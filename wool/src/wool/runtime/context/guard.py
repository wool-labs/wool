from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:
    from wool.runtime.context.snapshot import Snapshot


# public
class ConcurrentChainEntry(RuntimeError):
    """Raised when a Wool chain is entered by a thread or task that does
    not own it.

    Wool enforces strictly serial execution within a logical chain: at
    most one OS thread *and* one :class:`asyncio.Task` may run code
    under a given chain at a time. The guard has two dimensions —
    :func:`assert_chain_owner` arbitrates OS threads,
    :func:`assert_owner_task` arbitrates asyncio tasks within a single
    thread.

    The guard is **armed-gated** — it engages only on a context that
    carries a Wool snapshot. A context is armed by the first
    :meth:`wool.ContextVar.set` on it, *or* by a merge of incoming
    wire state (a worker receiving a dispatch frame, or a caller
    merging a routine's response). An *unarmed* context carries no
    chain and behaves as a plain :class:`contextvars.Context` — no
    chain UUID, no guard.

    The guard fires at the point a :class:`wool.ContextVar` is read
    or written (:meth:`~wool.ContextVar.get`,
    :meth:`~wool.ContextVar.set`, :meth:`~wool.ContextVar.reset`), not
    at the moment a thread or task boundary is crossed. Offloaded code
    that never touches a :class:`wool.ContextVar` is therefore never
    flagged; it never entered the chain in any observable way.

    The common cross-thread trigger is offloading work from an armed
    context to another OS thread without forking a fresh chain — for
    example :func:`asyncio.to_thread`, which copies the surrounding
    context (chain UUID and all) into the executor thread. The first
    :class:`wool.ContextVar` access from that thread runs the chain
    in genuine parallelism with the event loop and raises.
    :func:`wool.to_thread` is the supported alternative: it forks a
    fresh, detached chain for the worker thread so the offload never
    trips the guard.

    The cross-task trigger is two :class:`asyncio.Task` objects
    sharing one chain. The task factory forks every child task onto a
    fresh chain UUID, so siblings created the ordinary way never share
    a chain. The guard bites only when a :class:`contextvars.Context`
    is explicitly reused: the factory raises up front when an *armed*
    context already driving a live task is handed to a second
    ``create_task`` — that creation-time rejection is itself
    armed-gated, so sharing an *unarmed* context across tasks is
    permitted, exactly as stdlib asyncio permits it. If such an
    unarmed shared context is armed *later*, :func:`assert_owner_task`
    catches the second task the moment it touches a
    :class:`wool.ContextVar` on a chain another live task already
    owns.

    Cooperatively-scheduled work on the owning thread never trips the
    guard. Event-loop callbacks and timers scheduled on the loop
    thread inherit the scheduling scope's snapshot (a stdlib
    ``copy_context()``) and run on the loop's own thread with no
    running task, so they share the chain but can never run
    concurrently with its owner. The one exception is
    :meth:`loop.call_soon_threadsafe` called from another OS thread:
    it captures *that* thread's context, so a callback scheduled from
    a thread that has armed its own chain runs the chain off its
    owner thread and raises — by design, since the chain genuinely
    spans two threads. Child tasks never trip the guard either, for
    the opposite reason: the Wool task factory forks every child onto
    a fresh chain UUID, so a child never enters the parent's chain at
    all.
    """


def assert_chain_owner(snapshot: Snapshot | None) -> None:
    """Raise :class:`ConcurrentChainEntry` if *snapshot*'s chain is
    being accessed from a thread other than the one that owns it.

    Called from :meth:`wool.ContextVar.get`,
    :meth:`~wool.ContextVar.set`, and :meth:`~wool.ContextVar.reset`,
    so the guard fires when a Wool variable is touched, not when a
    thread boundary is crossed. A no-op when *snapshot* is ``None``
    (an unarmed context — no chain, no guard) or when the calling
    thread is the chain's owner (the common case: tasks, callbacks,
    and timers on a single event loop all run on the loop's thread).
    Genuine cross-thread access — an armed chain reached from an
    executor thread — fails loud.

    The owner is identified by :func:`threading.get_ident`, whose
    integer the OS may reuse after a thread exits. A snapshot that
    outlived its owning thread could therefore admit a later thread
    assigned the same identifier. For an event-loop chain the owner
    is the loop's thread, which lives for the program's lifetime, so
    the window does not arise; a :func:`wool.to_thread` fork is owned
    by a shorter-lived worker thread, but that forked snapshot is
    detached and dropped when the offload returns, so nothing
    observes it once the worker thread has exited.
    """
    if snapshot is None:
        return
    if snapshot.owner != threading.get_ident():
        raise ConcurrentChainEntry(
            "wool.ContextVar accessed from a thread that does not own its "
            "chain; an armed Wool context cannot be shared across OS threads "
            "in parallel. Use wool.to_thread to offload work onto a fresh, "
            "detached chain."
        )


def _resolve_owner_task(snapshot: Snapshot) -> asyncio.Future[Any] | None:
    """Resolve *snapshot*'s owner task, or ``None`` if it has no live owner.

    Returns ``None`` when the snapshot was armed outside any task
    (synchronous code, or a :func:`wool.to_thread` worker thread), when
    the owning task has been garbage-collected, or when it has
    finished. In all three cases there is no live task to arbitrate
    against — the thread-owner check (:func:`assert_chain_owner`) and,
    for a finished owner, the done-owner adoption in
    :meth:`wool.ContextVar.set` cover what remains.
    """
    ref = snapshot.owner_task
    if ref is None:
        return None
    task = ref()
    if task is None or task.done():
        return None
    return task


def assert_owner_task(snapshot: Snapshot | None) -> None:
    """Raise :class:`ConcurrentChainEntry` if *snapshot*'s chain is being
    entered by an :class:`asyncio.Task` other than the one that owns it.

    Complements :func:`assert_chain_owner`: that guard arbitrates OS
    threads, this one arbitrates asyncio tasks within a single thread.
    It catches the case the task factory's copy-on-fork cannot — two
    tasks handed the same :class:`contextvars.Context` while it was
    still unarmed, one of which later arms it: the second task to
    touch the chain runs a chain it does not own and fails loud.

    A no-op when *snapshot* is ``None`` (an unarmed context), when the
    chain has no live owner task (armed in synchronous code or a
    :func:`wool.to_thread` worker, or the owner has finished), or when
    the caller is not running inside a task at all — a bare event-loop
    callback or timer shares its scheduling scope's chain and runs
    serially on the loop thread, so it is never a concurrent runner.

    Called from :meth:`wool.ContextVar.get`,
    :meth:`~wool.ContextVar.set`, and :meth:`~wool.ContextVar.reset`
    immediately after :func:`assert_chain_owner`. That ordering is
    deliberate: :func:`assert_chain_owner` raises first on a thread
    that does not own the chain, so this function never reaches
    :func:`asyncio.current_task` on a thread with no running loop.
    """
    if snapshot is None:
        return
    owner_task = _resolve_owner_task(snapshot)
    if owner_task is None:
        return
    try:
        current = asyncio.current_task()
    except RuntimeError:
        return
    if current is None or current is owner_task:
        return
    raise ConcurrentChainEntry(
        "wool.ContextVar accessed from an asyncio task that does not own "
        "its chain; an armed Wool chain cannot be entered by two tasks at "
        "once. Each task must run on its own chain — create child tasks "
        "the ordinary way (the task factory forks a fresh chain per task), "
        "or pass a fresh contextvars.copy_context() to each create_task "
        "instead of sharing one."
    )
