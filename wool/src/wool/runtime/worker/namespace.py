"""Context-id bookkeeping for dispatched task execution.

A wool.Context id identifies a logical execution chain that spans
caller and worker. Inside a single :class:`asyncio.Task` the id is
stable across sequential awaits; :func:`asyncio.create_task` /
:func:`asyncio.gather` children fork a fresh id on their first wool
interaction, mirroring stdlib ``contextvars.Context`` fork semantics.

Pieces:

- :data:`_wool_context_binding` — stdlib contextvar carrying the
  active wool.Context id paired with a :class:`weakref.ref` to the
  :class:`asyncio.Task` it was bound to. Identity comparison
  against the current task is how we detect implicit forks.
- :data:`_intended_context_id` — worker-side handoff: the dispatch
  handler plants the caller's id here; the first descendant task
  that calls :func:`_current_context_id` adopts it and clears the
  slot.
- :func:`_current_context_id` — the fork-detection primitive that
  backs :func:`wool.current_context`.
- :func:`adopt_context` — explicit adoption used by
  :meth:`wool.Context.run` when seeding a fresh stdlib Context in
  async code.
- :func:`activate` — context manager used by the dispatch handler
  to set the intended context id and bind the binding for its own
  task's duration.
"""

from __future__ import annotations

import asyncio
import contextvars
import uuid
import weakref
from contextlib import contextmanager
from typing import Any
from typing import Iterator


class _ContextBinding:
    """Pairs a wool.Context id with the asyncio.Task it was bound to.

    A binding is the primitive that lets :func:`_current_context_id`
    distinguish "still inside the same logical execution chain" from
    "forked into a child task." The wool.Context id is the logical
    identity; ``task_ref`` holds a :class:`weakref.ref` to the
    :class:`asyncio.Task` the id was originally anchored to.

    When stdlib copies the backing contextvar across an
    ``asyncio.create_task`` boundary, the binding's value propagates
    verbatim — but ``task_ref`` still points at the PARENT task. An
    identity mismatch between the binding's referent and the current
    task means we're inside a forked context and should mint a
    fresh wool.Context id.

    A ``weakref`` is used rather than :func:`id` because
    :func:`asyncio.create_task` only records weak references to
    tasks (see ``Lib/asyncio/tasks.py`` ``_scheduled_tasks`` and the
    asyncio docs' creating-tasks warning). A completed parent task
    may be garbage-collected while the stdlib Context carrying this
    binding stays alive in a child or in a captured snapshot.
    :func:`id` values are permitted to be reused across disjoint
    object lifetimes (Python docs, ``id`` builtin), so a post-GC
    address reuse could make a fresh descendant task appear
    identical to its parent and suppress fork detection. A
    ``weakref`` collapses to ``None`` once the referent is gone,
    which the fork-detection read treats as "not the same task."

    Stored in the :data:`_wool_context_binding` stdlib contextvar
    so it is recoverable from the current context.
    """

    __slots__ = ("context_id", "task_ref")

    def __init__(self, context_id: uuid.UUID, task: asyncio.Task[Any]) -> None:
        self.context_id = context_id
        self.task_ref: weakref.ref[asyncio.Task[Any]] = weakref.ref(task)


# Binding holding the active wool.Context id and the task it was
# established on. Inside the same asyncio Task, the id is constant
# and continues across sequential awaits. Crossing an
# asyncio.create_task boundary flips the current task,
# :func:`_current_context_id` detects the mismatch, and mints a
# fresh id — stdlib parity with ``contextvars.Context`` fork
# semantics.
_wool_context_binding: contextvars.ContextVar[_ContextBinding | None] = (
    contextvars.ContextVar("wool.namespace.wool_context_binding", default=None)
)


# Worker-side adoption mechanism. When a dispatch handler enters
# :func:`activate`, it sets this contextvar to the caller's wire
# context id. The first asyncio task descendant that calls
# :func:`_current_context_id` consumes the intended value (by
# writing ``None`` to its own local context) and establishes a
# binding on itself. Descendants of that task (via the user's own
# ``asyncio.create_task``) see ``None`` and mint fresh ids — stdlib
# fork semantics.
_intended_context_id: contextvars.ContextVar[uuid.UUID | None] = contextvars.ContextVar(
    "wool.namespace.intended_context_id", default=None
)


# Stable fallback context id for sync callers outside any asyncio
# task. Sharing a process-wide UUID across unrelated sync flows is
# the price for Tokens minted by ``var.set(...)`` in sync code to
# still match at ``var.reset(...)``. Per-call minting would make
# the Token context-id check reject every sync reset.
_process_default_context_id: uuid.UUID = uuid.uuid4()


def _bind_context_id(task: asyncio.Task[Any]) -> None:
    """Establish a wool.Context-id binding for *task*.

    Idempotent: if *task* already has a valid binding on the
    current stdlib ``Context``, returns without mutating state.
    Otherwise adopts a caller-planted :data:`_intended_context_id`
    if one is waiting (consuming it so descendants don't re-adopt),
    or mints a fresh :class:`uuid.UUID` and binds it.

    Caller is responsible for supplying a non-None task —
    typically by calling :func:`asyncio.current_task` inside a
    coroutine.

    :param task:
        The :class:`asyncio.Task` to bind the context id to. Must
        be non-None.
    :raises ValueError:
        If *task* is None.
    """
    if task is None:
        raise ValueError("_bind_context_id requires a non-None task")
    binding = _wool_context_binding.get(None)
    if binding is not None and binding.task_ref() is task:
        return
    intended = _intended_context_id.get(None)
    if intended is not None:
        _intended_context_id.set(None)
        _wool_context_binding.set(_ContextBinding(intended, task))
        return
    _wool_context_binding.set(_ContextBinding(uuid.uuid4(), task))


def _current_context_id() -> uuid.UUID:
    """Internal fork-detection primitive. Returns the UUID of the
    current execution context.

    Exposed publicly as :attr:`wool.Context.id` via
    :func:`wool.current_context`. Kept private because the primary
    user-facing abstraction is :class:`wool.Context` — the id is an
    attribute of a Context, not a top-level concept.

    Inside an asyncio task, delegates to :func:`_bind_context_id`
    to establish a binding on first call (adopting a waiting
    :data:`_intended_context_id` or minting fresh), then reads the
    binding back. Outside any asyncio task, returns
    :data:`_intended_context_id` if set — leaving it in place so
    subsequent reads in the same stdlib ``Context`` keep returning
    the same id — otherwise falls back to a process-wide default.
    The sync fallback is shared across unrelated sync flows in the
    same process, but stable — Tokens captured at ``set()`` must
    compare equal on ``reset()`` for the common sync
    ``var.set(...); var.reset(token)`` case to work.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None

    if task is None:
        intended = _intended_context_id.get(None)
        return intended if intended is not None else _process_default_context_id

    _bind_context_id(task)
    binding = _wool_context_binding.get(None)
    assert binding is not None
    return binding.context_id


def adopt_context(context_id: uuid.UUID) -> None:
    """Bind *context_id* to the current asyncio task.

    Used on the worker side when a dispatched task adopts the
    caller's context id instead of minting a fresh one. Must be
    called from within the asyncio task that will run user code.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    if task is None:
        return
    _wool_context_binding.set(_ContextBinding(context_id, task))


# ----------------------------------------------------------------------
# Activation
# ----------------------------------------------------------------------


@contextmanager
def activate(context_id: uuid.UUID) -> Iterator[None]:
    """Activate a dispatched task's context id for the duration of the block.

    Plants :data:`_intended_context_id` so the worker-loop sub-task
    adopts the caller's id on its first
    :func:`_current_context_id` call, and binds the context to the
    current (handler) task so the id is also recoverable from the
    handler's context.

    :param context_id:
        The wool.Context id to activate.
    """
    intended_token = _intended_context_id.set(context_id)

    try:
        handler_task = asyncio.current_task()
    except RuntimeError:
        handler_task = None
    handler_binding_token = None
    if handler_task is not None:
        handler_binding_token = _wool_context_binding.set(
            _ContextBinding(context_id, handler_task)
        )

    try:
        yield
    finally:
        if handler_binding_token is not None:
            _wool_context_binding.reset(handler_binding_token)
        _intended_context_id.reset(intended_token)
