from __future__ import annotations

import asyncio
import contextvars
import logging
import warnings
import weakref
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Generator
from typing import TypeVar
from typing import cast

from wool.runtime.context.guard import ConcurrentChainEntry
from wool.runtime.context.snapshot import context_is_armed
from wool.runtime.context.snapshot import current_snapshot
from wool.runtime.context.snapshot import fork_snapshot
from wool.runtime.context.snapshot import install_snapshot

_log = logging.getLogger(__name__)

_loops_with_factory: weakref.WeakSet[asyncio.AbstractEventLoop] = weakref.WeakSet()

# Every task's running contextvars.Context, keyed by id() and mapped to
# the live task running under it. Lets the task factory detect a context
# shared across concurrently-live tasks (which would silently corrupt
# Wool state — see wool_factory). contextvars.Context is unhashable, so
# id() is the key; it is safe because while an entry exists the
# registered task pins its context alive, so the id cannot be reused.
# _release clears the entry when the task ends. wool_factory materialises
# the context for every task it creates and registers it here — not only
# explicitly-passed ones — so re-passing a live task's own context to a
# second create_task is caught.
_task_contexts: dict[int, "asyncio.Future[Any]"] = {}

T = TypeVar("T")


def _release(
    task: "asyncio.Future[Any]",
    key: int,
    coro: Coroutine[Any, Any, Any] | None,
    started: list[bool] | None,
) -> None:
    """Drop *key* from :data:`_task_contexts` once *task* is done.

    Removes the entry only if *task* is still the registered owner: a
    sequential reuse of the same context object — a fresh task created
    with it after this one finished but before this done-callback ran —
    may already hold the slot, and must not be evicted.

    When *task* wrapped its coroutine in :func:`_forked_scope` and was
    cancelled before the loop ever stepped it, the wrapper never ran and
    never awaited *coro*; *coro* is closed here so it does not leak a
    "coroutine was never awaited" :class:`RuntimeWarning` at GC.
    *started* is the wrapper's run flag; both *coro* and *started* are
    ``None`` for an unwrapped (unarmed) task, whose coroutine asyncio
    owns and closes itself.
    """
    if _task_contexts.get(key) is task:
        del _task_contexts[key]
    if coro is not None and started is not None and not started[0]:
        coro.close()


async def _forked_scope(coro: Coroutine[Any, Any, T], started: list[bool]) -> T:
    """Run *coro* under a freshly-forked chain.

    The Wool task factory wraps every *armed* child coroutine in this
    scope (an unarmed task runs its coroutine bare — there is no
    snapshot to fork, and wrapping would make the task's coroutine
    identity diverge from a plain asyncio task). Running inside the new
    task's own :class:`contextvars.Context`, it forks the inherited
    snapshot — minting a fresh chain UUID and adopting the running
    thread as owner — so a child task never shares its parent's chain.

    *started* is a one-element flag flipped to ``True`` the moment this
    scope is first stepped; the factory's :func:`_release` callback
    reads it to tell a wrapper that ran from one cancelled before its
    first step, whose *coro* was never awaited and must be closed.
    """
    started[0] = True
    snapshot = current_snapshot()
    if snapshot is not None:
        install_snapshot(fork_snapshot(snapshot))
    return await coro


def _default_task_factory(
    loop: asyncio.AbstractEventLoop,
    coro: Coroutine[Any, Any, Any],
    **kwargs: Any,
) -> asyncio.Task[Any]:
    """Fall-back task factory matching :meth:`AbstractEventLoop.create_task`.

    Used when no user factory is installed, so Wool's factory has a
    uniform inner layer to delegate to.
    """
    return asyncio.Task(coro, loop=loop, **kwargs)


def install_task_factory(
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """Install Wool's task factory on the given (or running) loop.

    Composes with an existing factory if one is set, so that asyncio
    child tasks created via ``create_task`` fork the parent's Wool
    snapshot onto a fresh chain. Idempotent — a subsequent call on a
    loop that already has the Wool-wrapped factory installed is a
    no-op. The first :meth:`wool.ContextVar.set` self-installs the
    factory on the running loop, so user code that touches Wool's API
    without first calling :func:`install_task_factory` still gets
    fork-on-task semantics for tasks created after that first set.

    **Ordering contract** — If a user installs their own task factory
    *after* Wool's, Wool's wrapping of child coroutines is dropped
    and copy-on-fork breaks silently for subsequently-created tasks.
    Install Wool's factory last (or compose manually) when other
    libraries also want a factory on the same loop.

    **Composed-factory contract** — When Wool composes with an existing
    factory, it forwards to that inner factory both ``context=`` and
    whatever keyword arguments the running loop handed Wool. Wool
    always supplies ``context=`` — it materialises each task's
    :class:`contextvars.Context` itself so the concurrent-entry guard
    can register it — and modern CPython additionally hands the factory
    ``name=``. A composed inner factory must therefore accept arbitrary
    ``**kwargs``. An inner factory written to the legacy two-argument
    ``(loop, coro)`` signature raises :class:`TypeError` under
    composition and is unsupported — this holds even on Python versions
    where bare stdlib would have called that factory without
    ``context=``, because Wool's guard requires the explicit context.
    """
    if loop is None:
        loop = asyncio.get_running_loop()

    existing = loop.get_task_factory()
    if existing is not None and getattr(existing, "__wool_wrapped__", False):
        _log.debug(f"Wool-composed task factory already installed on {loop}")
        return
    inner = existing if existing is not None else _default_task_factory

    def wool_factory(
        loop: asyncio.AbstractEventLoop,
        coro: Coroutine[Any, Any, Any] | Generator[Any, None, Any],
        *,
        context: contextvars.Context | None = None,
        **kwargs: Any,
    ) -> asyncio.Task[Any]:
        # Widen to ``Coroutine | Generator`` to satisfy typeshed's
        # ``_CoroutineLike[_T]`` contravariant parameter — the
        # ``Generator`` arm exists for pre-3.8 generator-coroutines
        # and is unreachable from asyncio's modern create_task path,
        # but the static type must accept it for ``wool_factory`` to
        # be a valid ``_TaskFactory``. Narrow back to ``Coroutine``
        # for the body.
        coro = cast(Coroutine[Any, Any, Any], coro)
        if context is None:
            # Materialise the context the task will run in here, rather
            # than letting asyncio.Task copy_context() internally — it
            # is the same call one frame up — so every task's context
            # is a known object the concurrent-entry guard registers
            # below. A fresh copy is unique and cannot alias a live
            # entry, so an ordinary task never trips the guard.
            context = contextvars.copy_context()
        else:
            # An *armed* contextvars.Context already driving a live task
            # must not be handed to a second create_task: Wool chain
            # state lives in the one ``_snapshot`` binding the context
            # holds, so two tasks running in it would read and write
            # each other's snapshot and silently corrupt both chains.
            # The rejection is armed-gated — an unarmed shared context
            # carries no chain to corrupt and is permitted, exactly as
            # stdlib asyncio permits it; if it is armed later, the
            # owner-task guard (assert_owner_task) catches the second
            # task then. Because every task's context is registered
            # below, this catches a context re-passed from any live task
            # — including one obtained via ``task.get_context()`` — not
            # only explicitly-created ones.
            owner = _task_contexts.get(id(context))
            if owner is not None and not owner.done() and context_is_armed(context):
                # Close the un-awaited coroutine to suppress the
                # "coroutine was never awaited" RuntimeWarning that
                # would otherwise leak at GC, then fail loud.
                coro.close()
                raise ConcurrentChainEntry(
                    "the same armed contextvars.Context was passed to "
                    "create_task while an earlier task running in it is "
                    "still live. An armed context cannot be shared across "
                    "concurrently-live tasks — both tasks would corrupt "
                    "each other's Wool state through the single snapshot "
                    "it holds. Omit context= (the default copies the "
                    "context per task) or pass a fresh "
                    "contextvars.copy_context() to each task."
                )
        kwargs["context"] = context
        # Wrap the coroutine in ``_forked_scope`` only when the creating
        # context is armed: an armed child must fork onto a fresh chain.
        # An unarmed task has no snapshot to fork, and wrapping it would
        # make Task.get_coro()/repr()/auto-name reflect ``_forked_scope``
        # instead of the user coroutine — a divergence from a plain
        # asyncio task — so it runs the coroutine bare.
        scope_coro: Coroutine[Any, Any, Any] | None
        started: list[bool] | None
        if current_snapshot() is None:
            scope_coro = None
            started = None
            # ``inner`` is statically typed to return ``Future | Task``
            # (the ``_TaskFactory`` protocol); a task factory in
            # practice always returns a ``Task``.
            task = inner(loop, coro, **kwargs)
        else:
            scope_coro = coro
            started = [False]
            task = inner(loop, _forked_scope(coro, started), **kwargs)
        key = id(context)
        _task_contexts[key] = task
        task.add_done_callback(
            lambda finished, k=key, c=scope_coro, s=started: _release(finished, k, c, s)
        )
        return task  # pyright: ignore[reportReturnType]

    wool_factory.__wool_wrapped__ = True  # pyright: ignore[reportFunctionMemberAccess]
    loop.set_task_factory(wool_factory)  # pyright: ignore[reportArgumentType]
    if existing is None:
        _log.debug(f"Wool task factory installed on {loop}")
    else:
        _log.debug(
            f"Wool task factory composed with existing factory {existing} on {loop}",
        )


def ensure_task_factory_installed() -> None:
    """Self-install Wool's task factory on the running loop if absent.

    Lets user code that touches Wool without first calling
    :func:`install_task_factory` still get fork-on-task semantics for
    tasks created after the first Wool API contact. No-ops in sync
    contexts (no running loop). The :data:`_loops_with_factory` weak
    set short-circuits the lookup to a single membership check after
    the first :func:`ensure_task_factory_installed` call on a given
    loop. A loop whose factory was installed by a direct
    :func:`install_task_factory` call is not in the set; the next
    :func:`ensure_task_factory_installed` call on it falls through
    once — :func:`install_task_factory` then no-ops on its
    ``__wool_wrapped__`` check — before the loop is added to the set.

    If a later call finds Wool's factory has been displaced from a
    loop it was previously installed on — a third-party factory
    installed after Wool's — it emits a :class:`RuntimeWarning`, since
    copy-on-fork is silently lost for tasks created after the
    displacement.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if loop in _loops_with_factory:
        # Wool's factory was installed on this loop earlier. If the
        # current factory is no longer Wool-wrapped, a third-party
        # factory was installed after Wool's and silently dropped
        # copy-on-fork for every task created since — a chain that no
        # longer forks per task is a latent correctness bug, so fail
        # loud rather than let it pass unnoticed.
        current = loop.get_task_factory()
        if not getattr(current, "__wool_wrapped__", False):
            warnings.warn(
                "Wool's task factory was displaced by a task factory "
                "installed after it; child tasks on this loop no longer "
                "fork onto fresh chains (copy-on-fork is lost). Install "
                "Wool's task factory last, or compose factories manually.",
                RuntimeWarning,
                stacklevel=2,
            )
        return
    install_task_factory(loop)
    _loops_with_factory.add(loop)


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

    This is the supported way to run Wool-aware work in another OS
    thread. Plain :func:`asyncio.to_thread` from an armed context
    copies the caller's chain verbatim into the worker thread, placing
    a second runner on one chain in genuine parallelism; the worker
    thread's first :class:`wool.ContextVar` access then trips the
    concurrent-entry guard and raises
    :class:`wool.ConcurrentChainEntry`. Use this function instead.

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
        # copy_context() on the loop thread: fork_snapshot stamps
        # ``owner`` with threading.get_ident(), so forking any earlier
        # would mint the fork owned by the loop thread and the worker's
        # first wool.ContextVar access would self-trip the guard. Forked
        # here, the offloaded chain is detached and owned by the worker,
        # with no path back to the caller's chain.
        snapshot = current_snapshot()
        if snapshot is not None:
            install_snapshot(fork_snapshot(snapshot))
        return func(*args, **kwargs)

    return await loop.run_in_executor(None, ctx.run, _run)
