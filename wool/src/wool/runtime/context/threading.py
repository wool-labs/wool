"""Wool-aware thread offload.

:func:`to_thread` is the supported way to run blocking work on a
worker thread from an armed Wool chain. It forks the caller's chain
onto a fresh, **detached** chain owned by the worker thread, then
runs the offloaded callable under the forked context — preserving
:class:`wool.ContextVar` bindings without sharing chain identity.

The fork is the keystone: plain :func:`asyncio.to_thread` copies the
caller's :class:`contextvars.Context` verbatim, leaving the worker
thread on the *same* logical Wool chain. The first
:class:`wool.ContextVar` access on the worker thread then trips the
chain-contention guard and raises :class:`wool.ChainContention`.
:func:`to_thread` here forks instead — a fresh chain UUID, owner-
stamped for the worker thread, with no merge-back path to the caller.

Lives in its own module (Q10) so :mod:`wool.runtime.context.factory`
retains its narrow charter — the task factory and its displacement
bookkeeping — and the thread-offload surface stays discoverable in
its own right.
"""

from __future__ import annotations

import asyncio
import contextvars
from typing import Any
from typing import Callable
from typing import TypeVar

from wool.runtime.context.base import current_context

T = TypeVar("T")


# public
async def to_thread(
    func: Callable[..., T],
    /,
    *args: Any,
    **kwargs: Any,
) -> T:
    """Offload a blocking call to a worker thread on a fresh Wool chain.

    Mirrors :func:`asyncio.to_thread` — runs *func* in the event
    loop's default executor and awaits the result — but additionally
    forks Wool chain state: the worker thread runs under a freshly
    minted, **detached** chain. The offloaded function sees a copy of
    the caller's :class:`wool.ContextVar` bindings under a new chain
    UUID owned by the worker thread; mutations it makes do not
    propagate back to the caller (no merge-back).

    When called from an unarmed context (no :class:`wool.ContextVar`
    ever set, no incoming wire context merged), the worker thread
    inherits the caller's plain :mod:`contextvars` context with no
    Wool chain — there is no chain to fork — and behaves exactly like
    :func:`asyncio.to_thread` plus a no-op.

    This is the supported way to run Wool-aware work in another OS
    thread. Plain :func:`asyncio.to_thread` from an armed context
    copies the caller's chain verbatim into the worker thread, placing
    a second runner on one chain in genuine parallelism; the worker
    thread's first :class:`wool.ContextVar` access then trips the
    chain-contention guard and raises
    :class:`wool.ChainContention`. Use this function instead.

    See ``wool/src/wool/runtime/context/README.md`` for the model
    context.

    **Re-handoff is undefined behaviour.** A :func:`to_thread` chain
    is owner-stamped for the worker thread and detached on purpose;
    handing the worker-side :class:`contextvars.Context` back to the
    caller and re-driving it (e.g. ``loop.create_task(coro,
    context=captured)``) is unsupported. The behaviour is not silently
    incorrect — it will fail loudly only at the chain's next
    :class:`wool.ContextVar` access — but the failure is not
    explicitly designed for and the diagnostic surfaces no clearer
    than the underlying guard raise.

    :param func:
        The blocking callable to offload.
    :param args:
        Positional arguments forwarded to *func*.
    :param kwargs:
        Keyword arguments forwarded to *func*.
    :returns:
        The value returned by *func*.
    """
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()

    def _run() -> T:
        # Runs inside the worker thread under the copied context. The
        # fork must happen here, on the executor thread, not before
        # copy_context() on the loop thread: Context._fork stamps
        # ``_owning_thread`` with threading.get_ident(), so forking any
        # earlier would mint the fork owned by the loop thread and the
        # worker's first wool.ContextVar access would self-trip the
        # guard. Forked here, the offloaded chain is detached and owned
        # by the worker, with no path back to the caller's chain. The
        # copied context is deliberately not registered in
        # ``_task_contexts`` (unlike a task-factory creation): it never
        # escapes ``_run``, so it can never be re-passed to a
        # create_task and need the guard.
        context = current_context()
        if context is not None:
            # Route the fork through mount so the forked chain's
            # owner-thread is stamped to this worker thread, not the
            # loop thread that called copy_context().
            context._fork().mount()
        return func(*args, **kwargs)

    return await loop.run_in_executor(None, ctx.run, _run)


__all__ = ["to_thread"]
