from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from uuid import UUID

from wool.runtime.context.errors import WoolError

if TYPE_CHECKING:
    from wool.runtime.context.base import Context


_KIND_MESSAGES: dict[str, str] = {
    "thread": (
        "wool.ContextVar accessed from thread {current_thread} but chain "
        "{chain_id} is owned by thread {owning_thread}; an armed Wool "
        "context cannot be shared across OS threads in parallel. Use "
        "wool.to_thread to offload work onto a fresh, detached chain."
    ),
    "task": (
        "wool.ContextVar accessed by task {current_task!r} but chain "
        "{chain_id} is owned by task {owning_task!r}; an armed Wool chain "
        "cannot be entered by two tasks at once. Each task must run on "
        "its own chain — create child tasks the ordinary way (the task "
        "factory forks a fresh chain per task), or pass a fresh "
        "contextvars.copy_context() to each create_task instead of "
        "sharing one."
    ),
    "create_task": (
        "the same armed contextvars.Context was passed to create_task "
        "while an earlier task running in it is still live (chain "
        "{chain_id}). An armed context cannot be shared across "
        "concurrently-live tasks — both tasks would corrupt each other's "
        "Wool state through the single context it holds. Omit context= "
        "(the default copies the context per task) or pass a fresh "
        "contextvars.copy_context() to each task."
    ),
}


# public
class ChainContention(WoolError):
    """Raised when a Wool chain is entered by a thread or task that does
    not own it.

    Wool enforces strictly serial execution within a logical chain: at
    most one OS thread *and* one :class:`asyncio.Task` may run code
    under a given chain at a time. The guard has two dimensions — an
    OS-thread check and an asyncio-task check within a single thread —
    and engages only on an *armed* context (one carrying Wool chain
    state). It fires at the point a :class:`wool.ContextVar` is read
    or written, not at a boundary crossing; offloaded code that never
    touches a Wool variable is never flagged. An armed
    :class:`contextvars.Context` re-passed to
    :func:`asyncio.create_task` is also rejected up front (the factory
    installed by :func:`wool.install_task_factory` performs this
    rejection; see :mod:`wool.runtime.context.factory`) — the
    ``"create_task"`` kind below distinguishes that case.

    The supported way to run Wool-aware work on another OS thread is
    :func:`wool.to_thread`, which forks a fresh, detached chain for
    the worker.

    See ``wool/src/wool/runtime/context/README.md`` for the model
    context.

    :param chain_id:
        UUID of the chain whose ownership was violated.
    :param kind:
        ``"thread"`` for a cross-thread access, ``"task"`` for a
        cross-task access, ``"create_task"`` for an armed context
        re-passed to :func:`asyncio.create_task`.
    :param owning_thread:
        Owner thread identity, when *kind* is ``"thread"``.
    :param current_thread:
        Offending thread identity, when *kind* is ``"thread"``.
    :param owning_task:
        Owner task, when *kind* is ``"task"``.
    :param current_task:
        Offending task, when *kind* is ``"task"``.
    """

    chain_id: UUID
    kind: Literal["thread", "task", "create_task"]
    owning_thread: int | None
    current_thread: int | None
    owning_task: asyncio.Future[Any] | None
    current_task: asyncio.Future[Any] | None

    def __init__(
        self,
        *,
        chain_id: UUID,
        kind: Literal["thread", "task", "create_task"],
        owning_thread: int | None = None,
        current_thread: int | None = None,
        owning_task: asyncio.Future[Any] | None = None,
        current_task: asyncio.Future[Any] | None = None,
    ) -> None:
        # Validate ``kind`` explicitly so an unknown value raises a
        # typed ``ValueError`` from a known origin rather than a bare
        # ``KeyError`` from inside the exception's own constructor.
        # The Literal annotation guards static call sites; this guard
        # covers dynamic call sites (most notably ``__reduce__``-driven
        # cross-process reconstruction where a forward-compat receiver
        # might decode a ``kind`` value it does not know).
        if kind not in _KIND_MESSAGES:
            raise ValueError(
                f"unknown ChainContention kind: {kind!r}; "
                f"expected one of {sorted(_KIND_MESSAGES)}"
            )
        message = _KIND_MESSAGES[kind].format(
            chain_id=chain_id,
            owning_thread=owning_thread,
            current_thread=current_thread,
            owning_task=owning_task,
            current_task=current_task,
        )
        super().__init__(message)
        self.chain_id = chain_id
        self.kind = kind
        self.owning_thread = owning_thread
        self.current_thread = current_thread
        self.owning_task = owning_task
        self.current_task = current_task

    def __reduce__(self) -> tuple[Any, ...]:
        # ``ChainContention`` crosses the wire via
        # :func:`_safely_serialize_exception`. The default
        # ``BaseException.__reduce__`` pickles ``(type, args)`` where
        # ``args`` is the pre-formatted message — fine for the primary
        # ``serializer.dumps`` path, but the type-preserving fallback
        # rebuilds via ``cls(*exc.args)``, which our keyword-only
        # constructor rejects. ``__reduce__`` returning the structured
        # kwargs as a ``(cls, (), state)`` triple keeps both paths
        # intact: the structured fields ride the wire, and the message
        # is re-composed by ``__init__`` on the receiver.
        return (
            _reconstruct_chain_contention,
            (
                self.chain_id,
                self.kind,
                self.owning_thread,
                self.current_thread,
                self.owning_task,
                self.current_task,
            ),
        )


def _reconstruct_chain_contention(
    chain_id: UUID,
    kind: Literal["thread", "task", "create_task"],
    owning_thread: int | None,
    current_thread: int | None,
    owning_task: asyncio.Future[Any] | None,
    current_task: asyncio.Future[Any] | None,
) -> ChainContention:
    """Module-level constructor for :meth:`ChainContention.__reduce__`."""
    return ChainContention(
        chain_id=chain_id,
        kind=kind,
        owning_thread=owning_thread,
        current_thread=current_thread,
        owning_task=owning_task,
        current_task=current_task,
    )


def _assert_chain_owner(context: Context | None) -> None:
    """Raise :class:`ChainContention` if *context*'s chain is entered
    by a thread or task that does not own it.

    Runs both chain-contention checks in order — OS thread first,
    asyncio task second. The ordering is load-bearing: a cross-thread
    access fails on the thread guard before the task guard runs, so
    foreign-thread access never reaches :func:`asyncio.current_task`
    on a thread with no running loop.

    A no-op for unarmed contexts (*context* is ``None``). On the
    thread dimension, also a no-op when the calling thread is the
    chain's owner (the common case: tasks, callbacks, and timers on
    a single event loop all run on the loop's thread). On the task
    dimension, also a no-op when the chain has no live owner task
    (armed in synchronous code or a :func:`wool.to_thread` worker, or
    the owner has finished), or when the caller is not running inside
    a task at all — a bare event-loop callback or timer shares its
    scheduling scope's chain and runs serially on the loop thread.
    Calling outside a running event loop is treated the same as
    calling outside any task — the check is a no-op.

    The thread owner is identified by :func:`threading.get_ident`,
    whose integer the OS may reuse after a thread exits. A context
    that outlived its owning thread could therefore admit a later
    thread assigned the same identifier. For an event-loop chain, the
    owner is the loop's thread, which lives for the program's
    lifetime, so the window does not arise; a :func:`wool.to_thread`
    fork is owned by a shorter-lived worker thread, but that forked
    context is detached and dropped when the offload returns, so
    nothing observes it once the worker thread has exited.

    The task dimension catches the case the task factory's
    copy-on-fork cannot — two tasks handed the same
    :class:`contextvars.Context` while it was still unarmed, one of
    which later arms it: the second task to touch the chain does not
    own it and fails loudly.

    :param context:
        The active wool :class:`~wool.runtime.context.base.Context`, or
        ``None`` for an unarmed context.
    :raises ChainContention:
        If *context* is armed and the calling thread or running task is
        not its owner.
    """
    if context is None:
        return
    current_thread = threading.get_ident()
    if context._owning_thread != current_thread:
        raise ChainContention(
            chain_id=context.chain_id,
            kind="thread",
            owning_thread=context._owning_thread,
            current_thread=current_thread,
        )
    owning_task = _resolve_owning_task(context)
    if owning_task is None:
        return
    try:
        current_task = asyncio.current_task()
    except RuntimeError:
        return
    if current_task is None or current_task is owning_task:
        return
    raise ChainContention(
        chain_id=context.chain_id,
        kind="task",
        owning_task=owning_task,
        current_task=current_task,
    )


def _resolve_owning_task(context: Context) -> asyncio.Future[Any] | None:
    """Resolve *context*'s owning task, or ``None`` if it has no live owner.

    Returns ``None`` when the context was armed outside any task
    (synchronous code, or a :func:`wool.to_thread` worker thread), when
    the owning task has been garbage-collected, or when it has
    finished. In all three cases there is no live task to arbitrate
    against — the thread-owner check in :func:`_assert_chain_owner`
    and, for a finished owner, the unconditional owner re-stamp
    performed by :meth:`~wool.runtime.context.base.Context.mount`
    (invoked from :meth:`wool.ContextVar.set`) cover what remains.

    :param context:
        The active wool :class:`~wool.runtime.context.base.Context`.
        Always non-``None`` — :func:`_assert_chain_owner` short-circuits
        on an unarmed context before this helper runs.
    """
    ref = context._owning_task
    if ref is None:
        return None
    task = ref()
    if task is None or task.done():
        return None
    return task
