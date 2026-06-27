from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING
from typing import Any

from wool.runtime.context.exceptions import ChainContention

if TYPE_CHECKING:
    from wool.runtime.context.chain import Chain


def _assert_chain_owner(context: Chain | None) -> None:
    """Raise `ChainContention` if *context*'s chain is entered
    by a thread or task that does not own it.

    Runs both chain-contention checks in order — OS thread first,
    asyncio task second. The ordering is load-bearing: a cross-thread
    access fails on the thread guard before the task guard runs, so
    foreign-thread access never reaches `asyncio.current_task`
    on a thread with no running loop.

    A no-op for unarmed contexts (*context* is ``None``). On the
    thread dimension, also a no-op when the calling thread is the
    chain's owner (the common case: tasks, callbacks, and timers on
    a single event loop all run on the loop's thread). On the task
    dimension, also a no-op when the chain has no live owner task
    (armed in synchronous code or a `wool.to_thread` worker, or
    the owner has finished), or when the caller is not running inside
    a task at all — a bare event-loop callback or timer shares its
    scheduling scope's chain and runs serially on the loop thread.
    Calling outside a running event loop is treated the same as
    calling outside any task — the check is a no-op.

    The thread owner is identified by `threading.get_ident`,
    whose integer the OS may reuse after a thread exits. A context
    that outlived its owning thread could therefore admit a later
    thread assigned the same identifier. For an event-loop chain, the
    owner is the loop's thread, which lives for the program's
    lifetime, so the window does not arise; a `wool.to_thread`
    fork is owned by a shorter-lived worker thread, but that forked
    context is detached and dropped when the offload returns, so
    nothing observes it once the worker thread has exited.

    The task dimension catches the case the task factory's
    copy-on-fork cannot — two tasks handed the same
    `contextvars.Context` while it was still unarmed, one of
    which later arms it: the second task to touch the chain does not
    own it and fails loudly.

    :param context:
        The active wool `~wool.runtime.context.chain.Chain`, or
        ``None`` for an unarmed context.
    :raises ChainContention:
        If *context* is armed and the calling thread or running task is
        not its owner.
    """
    if context is None:
        return
    current_thread = threading.get_ident()
    if context.thread != current_thread:
        raise ChainContention(
            chain_id=context.id,
            kind="thread",
            owning_thread=context.thread,
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
        chain_id=context.id,
        kind="task",
        owning_task=owning_task,
        current_task=current_task,
    )


def _resolve_owning_task(context: Chain) -> asyncio.Future[Any] | None:
    """Resolve *context*'s owning task, or ``None`` if it has no live owner.

    Returns ``None`` when the context was armed outside any task
    (synchronous code, or a `wool.to_thread` worker thread), when
    the owning task has been garbage-collected, or when it has
    finished. In all three cases there is no live task to arbitrate
    against — the thread-owner check in `_assert_chain_owner`
    and, for a finished owner, the unconditional owner re-stamp
    performed by `~wool.runtime.context.chain.Chain.mount`
    (invoked from `wool.ContextVar.set`) cover what remains.

    :param context:
        The active wool `~wool.runtime.context.chain.Chain`.
        Always non-``None`` — `_assert_chain_owner` short-circuits
        on an unarmed context before this helper runs.
    """
    ref = context.task
    if ref is None:
        return None
    task = ref()
    if task is None or task.done():
        return None
    return task
