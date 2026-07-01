from __future__ import annotations

import asyncio
import contextvars
import logging
import threading
import weakref
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Generator
from typing import TypeVar
from typing import cast

import wool
from wool.runtime.context.exceptions import ChainContention
from wool.runtime.context.exceptions import TaskFactoryDisplaced

_log = logging.getLogger(__name__)

T = TypeVar("T")


def context_is_armed(context: contextvars.Context) -> bool:
    """Return ``True`` if *context* carries a Wool chain.

    A `contextvars.Context` in which no `wool.ContextVar`
    has been set never holds the Wool-owned context variable at all — it
    is *unarmed* and behaves as a plain `contextvars.Context`.

    Inspects an *explicit* `contextvars.Context` (e.g., a
    ``copy_context()`` snapshot or a child task's materialised context),
    not the active one — the task factory consults it on a child's
    freshly copied context before scheduling it.
    """
    return wool.__chain__ in context


_loops_with_factory: weakref.WeakSet[asyncio.AbstractEventLoop] = weakref.WeakSet()

# Loops where Wool's factory has been observed displaced. Populated by
# the weakref.finalize callback on Wool's factory object (fires the
# moment the loop drops its reference inside loop.set_task_factory)
# and by ``_release``'s done-callback check (covers the corner where
# the third-party stash holds the reference but doesn't invoke it).
# Consulted by ``ensure_task_factory_installed`` (and by anything
# else that needs to surface displacement loudly to user code) so a
# user-Wool entry point flagged on a displaced loop raises
# `TaskFactoryDisplaced` regardless of whether mount is still
# the trigger path.
_displaced_loops: weakref.WeakSet[asyncio.AbstractEventLoop] = weakref.WeakSet()

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
_task_contexts: dict[int, asyncio.Future[Any]] = {}


class _PendingSentinel:
    """Sentinel marking a reserved-but-not-yet-populated context slot.

    Placed into ``_task_contexts`` by ``wool_factory`` *before*
    invoking the inner factory so two threads concurrently calling
    ``loop.create_task(coro, context=same_armed_ctx)`` cannot both
    pass the owner-check, race the registration, and silently share
    a chain. The first thread sees ``None``, reserves with the
    sentinel under the lock; the second thread sees the sentinel and
    treats it as a live owner (raising ``ChainContention``).
    """


# Module-level singleton — typed as Future for compatibility with
# _task_contexts's value type but checked via ``is _PENDING`` at the
# use site, so the nominal type is purely for the dict.
_PENDING: Any = _PendingSentinel()

# Guards (a) the read-modify-write in `install_task_factory`
# (``loop.get_task_factory()`` then ``loop.set_task_factory()``) so two
# threads cannot double-install on one loop, and (b) every read/write of
# `_task_contexts` so the per-task registration/release sequence
# stays consistent. Both critical sections are narrow (~5 lines each),
# so a single lock for both is cheaper than per-table sharding.
_lock = threading.Lock()


def _on_factory_collected(
    loop_ref: weakref.ref[asyncio.AbstractEventLoop],
) -> None:
    """``weakref.finalize`` callback for the Wool factory installed on a loop.

    Fires when Wool's factory object is garbage-collected. On CPython
    this is synchronous with the displacement: a third party calling
    ``loop.set_task_factory(other)`` drops the loop's last reference
    to Wool's factory, the refcount hits zero, and this callback runs
    immediately in the displacer's stack frame.

    Filters out legitimate teardown (loop closed / collected) so the
    callback fires only on actual displacement. Flags the loop in
    `_displaced_loops` so the next user-Wool entry point
    consulting the flag raises `TaskFactoryDisplaced` loudly to
    user code. Raises from finalize callbacks are caught by the GC
    machinery and printed as ``Exception ignored in:`` — not user-
    visible exceptions — so the flag-and-surface-later pattern is
    needed for a guaranteed user-facing raise.

    Only mark displacement when ``loop.is_running()``. In the
    finalize-during-shutdown window (between ``loop.close()``
    initiation and ``is_closed()`` returning True) a closing loop
    flagged as displaced logs at DEBUG rather than WARNING, since a
    non-running loop has no Wool API surface that could surface the
    flag anyway.
    """
    loop = loop_ref()
    if loop is None or loop.is_closed():
        return
    if not loop.is_running():
        _log.debug(
            "Wool's task factory finalize fired on non-running loop %r — "
            "treating as legitimate teardown rather than displacement.",
            loop,
        )
        return
    _displaced_loops.add(loop)
    _log.warning(
        "Wool's task factory has been displaced from %r; child tasks "
        "no longer fork onto fresh chains. The next wool.ContextVar "
        "access on this loop will raise TaskFactoryDisplaced.",
        loop,
    )


def _release(
    task: asyncio.Future[Any],
    key: int,
    coro: Coroutine[Any, Any, Any] | None,
) -> None:
    """Drop *key* from `_task_contexts` once *task* is done.

    Removes the entry only if *task* is still the registered owner: a
    sequential reuse of the same context object — a fresh task created
    with it after this one finished but before this done-callback ran —
    may already hold the slot, and must not be evicted.

    *coro* is the inner coroutine of a task whose coroutine the factory
    wrapped in `_forked_scope`. By the time this done-callback
    runs the task is finished, so *coro* is either already exhausted
    (the wrapper ran and awaited it to completion) or never started
    (the wrapper was cancelled before its first step). It is closed
    unconditionally: `close` is a no-op on an exhausted coroutine,
    and on a never-started one it suppresses the "coroutine was never
    awaited" `RuntimeWarning` that would otherwise leak at GC.
    *coro* is ``None`` for an unwrapped (unarmed) task, whose coroutine
    asyncio owns and closes itself.

    Also runs a displacement check as a backstop to the
    `_on_factory_collected` finalize callback: if a third party
    stashes Wool's factory reference but never calls it (so the
    finalize never fires) and creates new tasks via its own factory,
    eventually a pre-displacement Wool-tracked task completes — that
    completion runs ``_release`` on this loop and we observe that the
    current factory is no longer Wool-wrapped.
    """
    with _lock:
        if _task_contexts.get(key) is task:
            del _task_contexts[key]
    if coro is not None:
        # Close defensively. If ``coro.close()`` raises (e.g., a
        # third-party generator with a bug in ``close()``), the
        # ``_release`` body must continue so the displacement backstop
        # below still runs. Without this guard, asyncio would surface
        # the raise as "Exception in callback" and silently skip the
        # displacement check, defeating the backstop for the
        # stash-but-don't-call corner.
        try:
            coro.close()
        except (KeyboardInterrupt, SystemExit):  # pragma: no cover
            raise
        except BaseException:  # pragma: no cover
            _log.debug("coro.close() raised during _release; ignored", exc_info=True)
    try:
        loop = task.get_loop()
    except RuntimeError:  # pragma: no cover — orphaned future has no loop
        # ``get_loop`` raises on an orphaned future. Nothing to check.
        return
    if loop in _loops_with_factory and loop not in _displaced_loops:
        if not _is_wool_factory(loop.get_task_factory()):
            _displaced_loops.add(loop)
            _log.warning(
                "Wool's task factory has been displaced from %r "
                "(detected at task-completion time); the next "
                "wool.ContextVar access on this loop will raise "
                "TaskFactoryDisplaced.",
                loop,
            )


async def _forked_scope(coro: Coroutine[Any, Any, T]) -> T:
    """Run *coro* under a freshly-forked chain.

    The Wool task factory wraps every *armed* child coroutine in this
    scope (an unarmed task runs its coroutine bare — there is no
    context to fork, and wrapping would make the task's coroutine
    identity diverge from a plain asyncio task). Running inside the new
    task's own `contextvars.Context`, it forks the inherited
    context — minting a fresh chain UUID and adopting the running
    thread as owner — so a child task never shares its parent's chain.
    """
    # The factory only wraps an armed child in this scope (see the gate
    # at the wool_factory definition), so the chain is armed by
    # construction; a bare ``get`` raises ``LookupError`` loudly if the
    # gate-and-wrap drift ever breaks.
    context = wool.__chain__.get()
    # ``mount`` stamps owners and installs the task factory; fork
    # produces an owner-less Chain on a fresh chain id, so routing it
    # through mount is the canonical "make this fork live" step.
    context._fork().mount()
    return await coro


# Memoize per-factory `_is_wool_factory` outcomes, with built-in
# cycle detection. Wool's stamps (``__wool_wrapped__``,
# ``__wool_inner__``) are written once at install time and never
# mutated by Wool afterward, so the result is invariant under any
# code path Wool initiates. Caching short-circuits the steady-state
# hot path (``ensure_task_factory_installed`` fires from every
# ``wool.ContextVar.set()``'s ``mount``). A per-walk ``seen`` set
# defends against an adversarial third-party wrapper that points
# ``__wool_inner__`` back into the chain — without it, the walk
# spins indefinitely under the install lock and freezes every
# install attempt across the process.
_wool_factory_cache: weakref.WeakKeyDictionary[Callable[..., Any], bool] = (
    weakref.WeakKeyDictionary()
)


def _is_wool_factory(factory: Any) -> bool:
    """Return ``True`` if any layer of *factory*'s composition is wool.

    Walks the ``__wool_inner__`` chain Wool stamps on its wrapper —
    each Wool wrap remembers the factory it composed over via this
    attribute, terminated by ``None`` at the bottom. The chain lets the
    idempotency check in `install_task_factory` detect Wool even
    when buried under a third-party factory installed *over* a prior
    Wool install: ``wool → third-party → wool`` would otherwise pass
    the outer-only check and re-wrap into a double-fork hazard.

    Memoized via a `weakref.WeakKeyDictionary` and cycle-guarded
    by a per-walk ``seen`` set.
    """
    cached = _wool_factory_cache.get(factory)
    if cached is not None:
        return cached
    chain: list[Any] = []
    seen: set[int] = set()
    current: Any = factory
    result = False
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        # Mid-walk cache hit short-circuits.
        cached = _wool_factory_cache.get(current)
        if cached is not None:
            result = cached
            break
        if getattr(current, "__wool_wrapped__", False):
            result = True
            break
        current = getattr(current, "__wool_inner__", None)
    # Memoize every layer we walked. Layers that fall out (e.g., not
    # weakref-able) silently skip — the next call re-walks them.
    for f in chain:
        try:
            _wool_factory_cache[f] = result
        except TypeError:  # pragma: no cover — non-weakref-able layer; skip
            pass
    return result


# public
def install_task_factory(
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """Install Wool's task factory on the given (or running) loop.

    Composes with an existing factory if one is set, so that asyncio
    child tasks created via ``create_task`` fork the parent's Wool
    chain onto a fresh chain. Idempotent — a subsequent call on a
    loop that already has the Wool-wrapped factory installed is a
    no-op. The first `wool.ContextVar.set` self-installs the
    factory on the running loop, so user code that touches Wool's API
    without first calling `install_task_factory` still gets
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
    `contextvars.Context` itself so the chain-contention guard
    can register it. CPython's ``asyncio`` additionally hands the
    factory ``name=``; other conformant loops (uvloop, for one) apply
    the task name themselves and do not forward it — so a composed
    inner factory must accept arbitrary ``**kwargs`` and must not
    *depend* on ``name=`` being present. An inner factory written to
    the legacy two-argument ``(loop, coro)`` signature raises
    `TypeError` under composition and is unsupported — this
    holds even on Python versions where bare stdlib would have called
    that factory without ``context=``, because Wool's guard requires
    the explicit context.

    Installation is one-way — Wool does not provide an uninstall
    path; the wrapped factory stays on the loop until the loop is
    closed.

    See ``wool/src/wool/runtime/context/README.md`` for the model
    context.

    :param loop:
        The event loop to install the factory on. When ``None`` (the
        default), the running loop is resolved via
        `asyncio.get_running_loop`; calling with ``loop=None``
        outside a running loop raises `RuntimeError`. The call
        is idempotent — installing on a loop that already has the
        Wool-wrapped factory is a no-op — and composes with any
        existing factory by stamping ``__wool_inner__`` so the prior
        factory is invoked underneath Wool's wrapper.
    """
    if loop is None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as e:
            raise RuntimeError(
                "install_task_factory() with loop=None must run inside a "
                "running event loop, or pass loop= explicitly"
            ) from e

    # Guard the read-modify-write so two threads installing on the same
    # loop cannot both pass the wool-detection check and double-install
    # into a ``wool → wool`` composition. Today this is reactive
    # protection against a third-party factory installed lazily from
    # another thread; the lock is narrow enough that single-threaded
    # callers pay no measurable cost.
    with _lock:
        existing = loop.get_task_factory()
        if existing is not None and _is_wool_factory(existing):
            _log.debug(f"Wool-composed task factory already installed on {loop}")
            return
        # Inline default — a one-line passthrough to ``asyncio.Task``
        # that gives ``inner`` a uniform non-None value, avoiding a
        # named helper whose only purpose was to be that placeholder.
        inner = (
            existing
            if existing is not None
            else (lambda lp, cr, **kw: asyncio.Task(cr, loop=lp, **kw))
        )

        wool_factory = _build_wool_factory(inner)
        wool_factory.__wool_wrapped__ = True  # pyright: ignore[reportFunctionMemberAccess]
        wool_factory.__wool_inner__ = existing  # pyright: ignore[reportFunctionMemberAccess]
        loop.set_task_factory(wool_factory)  # pyright: ignore[reportArgumentType]
        # Register the loop for displacement monitoring — including loops
        # set up by a direct install_task_factory() call (e.g., worker-
        # process loops), not only those routed through
        # ensure_task_factory_installed.
        _loops_with_factory.add(loop)
        # Clear the displacement flag atomically with the
        # re-install so a recovery path (``install_task_factory(loop)``
        # after the displacement was detected) self-heals. Without
        # this discard, ``ensure_task_factory_installed`` would keep
        # short-circuiting on ``if loop in _displaced_loops:
        # raise TaskFactoryDisplaced(...)`` even after the user
        # explicitly puts Wool's factory back on top.
        _displaced_loops.discard(loop)
        # Detect displacement at the moment it happens. When a third
        # party calls ``loop.set_task_factory(other)`` without
        # composing through ``install_task_factory``, the loop drops
        # its reference to ``wool_factory`` and the refcount hits
        # zero synchronously (CPython); the ``weakref.finalize``
        # callback fires right there in the displacer's stack frame
        # and flags the loop so the next user-Wool entry point
        # raises. The ``loop.is_closed()`` filter avoids false
        # positives at legitimate loop shutdown.
        weakref.finalize(wool_factory, _on_factory_collected, weakref.ref(loop))
    if existing is None:
        _log.debug(f"Wool task factory installed on {loop}")
    else:
        _log.debug(
            f"Wool task factory composed with existing factory {existing} on {loop}",
        )


def _build_wool_factory(
    inner: Callable[..., asyncio.Future[Any]],
) -> Callable[..., asyncio.Task[Any]]:
    """Return Wool's task-factory closure composing over *inner*.

    Hoisted out of `install_task_factory` so the lock-held
    install body stays narrow. The returned closure captures *inner*
    by reference and exposes ``__wool_wrapped__`` / ``__wool_inner__``
    on itself; `install_task_factory` stamps them after this
    returns.

    *inner* is typed as ``Callable[..., asyncio.Future[Any]]`` to
    match typeshed's ``asyncio.events._TaskFactory`` protocol — what
    ``loop.get_task_factory()`` returns. Wool's own ``wool_factory``
    closure unconditionally materialises an `asyncio.Task`,
    so the outer return stays ``Task[Any]``.
    """

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
            # is a known object the chain-contention guard registers
            # below. A fresh copy is unique and cannot alias a live
            # entry, so an ordinary task never trips the guard.
            context = contextvars.copy_context()
            reserved_pending = False
        else:
            # An *armed* contextvars.Context already driving a live task
            # must not be handed to a second create_task: Wool chain
            # state lives in the one ``wool.__chain__`` binding the context
            # holds, so two tasks running in it would read and write
            # each other's context and silently corrupt both chains.
            # The rejection is armed-gated — an unarmed shared context
            # carries no chain to corrupt and is permitted, exactly as
            # stdlib asyncio permits it; if it is armed later, the
            # chain-owner guard (assert_chain_owner) catches the
            # second task then. Because every task's context is registered
            # below, this catches a context re-passed from any live task
            # — including one obtained via ``task.get_context()`` — not
            # only explicitly-created ones.
            #
            # Reserve the slot with a ``_PENDING`` sentinel
            # *inside the same locked section* as the owner check, so
            # two threads concurrently calling
            # ``loop.create_task(coro, context=same_armed_ctx)`` cannot
            # both pass the "owner is None" check and race the
            # registration. The first thread reserves; the second sees
            # the pending reservation and treats it as a live owner.
            armed_at_entry = context_is_armed(context)
            with _lock:
                owner = _task_contexts.get(id(context))
                if owner is None and armed_at_entry:
                    _task_contexts[id(context)] = _PENDING  # type: ignore[assignment]
                    reserved_pending = True
                else:
                    reserved_pending = False
            if owner is not None and (
                owner is _PENDING or (not owner.done() and armed_at_entry)
            ):
                # Close the un-awaited coroutine to suppress the
                # "coroutine was never awaited" RuntimeWarning that
                # would otherwise leak at GC, then fail loud.
                coro.close()
                # ``context[wool.__chain__]`` is the armed Wool Chain — by
                # the ``context_is_armed`` check above — so its
                # ``chain_id`` is the one being doubly-driven. Surface
                # it in the exception for diagnostics. Also
                # surface the owning task (the registered owner, if
                # any) and the current task so the structured
                # diagnostic fields are populated when available.
                wool_ctx = context[wool.__chain__]
                try:
                    current = asyncio.current_task()
                except RuntimeError:  # pragma: no cover — always under a running loop
                    current = None
                raise ChainContention(
                    chain_id=wool_ctx.id,
                    kind="create_task",
                    owning_task=owner if isinstance(owner, asyncio.Future) else None,
                    current_task=current,
                )
        kwargs["context"] = context
        # Wrap the coroutine in ``_forked_scope`` only when the creating
        # context is armed: an armed child must fork onto a fresh chain.
        # An unarmed task has no context to fork, and wrapping it would
        # make Task.get_coro()/repr()/auto-name reflect ``_forked_scope``
        # instead of the user coroutine — a divergence from a plain
        # asyncio task — so it runs the coroutine bare. An armed task
        # does take that get_coro()/repr() divergence: it is the
        # accepted cost of copy-on-fork, not an oversight — wrapping is
        # the only way to run the child under its own forked chain.
        scope_coro: Coroutine[Any, Any, Any] | None
        # Gate the wrap on the *materialised child*'s armed state, not
        # the caller's: when the caller is armed but explicitly passes
        # an unarmed ``contextvars.Context`` as ``context=``, only the
        # child's view matters — wrapping an unarmed child would make
        # ``Task.get_coro()`` / ``repr()`` reflect ``_forked_scope`` for
        # no reason (the inner fork would be a no-op).
        if not context_is_armed(context):
            scope_coro = None
            # ``inner`` is statically typed to return ``Future | Task``
            # (the ``_TaskFactory`` protocol); a task factory in
            # practice always returns a ``Task``.
            try:
                task = inner(loop, coro, **kwargs)
            except BaseException:
                # The user coroutine never reached a task — close it
                # to suppress the "coroutine was never awaited" warning
                # at GC, then re-raise. Also clean up the
                # _PENDING reservation if we made one, so the slot
                # doesn't pin forever.
                coro.close()
                if reserved_pending:  # pragma: no cover — no pending slot when unarmed
                    with _lock:
                        if _task_contexts.get(id(context)) is _PENDING:
                            del _task_contexts[id(context)]
                raise
        else:
            scope_coro = _forked_scope(coro)
            try:
                task = inner(loop, scope_coro, **kwargs)
            except BaseException:
                # ``inner`` raising leaves the wrapper coroutine (and
                # therefore the user coroutine inside it) unawaited.
                # Close both to suppress the "coroutine was never
                # awaited" warning at GC, symmetric with the proactive
                # close on the re-passed-armed-context branch above.
                scope_coro.close()
                coro.close()
                if reserved_pending:
                    with _lock:
                        if _task_contexts.get(id(context)) is _PENDING:
                            del _task_contexts[id(context)]
                raise
        key = id(context)
        # An eager inner factory can step the task to completion inside
        # the inner(...) call above, before this registration. That is
        # harmless: a re-passed already-done context is caught by the
        # ``not owner.done()`` check, not by registration ordering, and
        # the done-callback below still fires (add_done_callback on an
        # already-done task schedules via call_soon) to clear the slot.
        # Replace the ``_PENDING`` reservation we placed in the
        # owner check above with the real task; for non-reserved
        # contexts the entry is fresh.
        with _lock:
            _task_contexts[key] = task
        task.add_done_callback(
            lambda finished, k=key, c=scope_coro: _release(finished, k, c)
        )
        return task  # pyright: ignore[reportReturnType]

    return wool_factory


def ensure_task_factory_installed() -> None:
    """Self-install Wool's task factory on the running loop if absent.

    Lets user code that touches Wool without first calling
    `install_task_factory` still get fork-on-task semantics for
    tasks created after the first Wool API contact. No-ops in sync
    contexts (no running loop). The `_loops_with_factory` weak
    set short-circuits the lookup to a single membership check. Both
    `install_task_factory` and this function add the loop to the
    set, so a loop set up by a direct `install_task_factory`
    call — e.g., a worker-process loop — is displacement-monitored
    exactly like one self-installed here.

    If a later call finds Wool's factory has been displaced from a
    loop it was previously installed on — a third-party factory
    installed after Wool's — it raises `TaskFactoryDisplaced`,
    since copy-on-fork is silently lost for tasks created after the
    displacement. Displacement is detected via three converging
    paths: `_displaced_loops` (set by
    `_on_factory_collected`'s ``weakref.finalize`` callback the
    moment the loop drops Wool's factory reference, and by
    `_release`'s done-callback backstop), the post-displacement
    factory inspection at this site, and any direct re-check the
    user-Wool entry point performs.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if loop in _displaced_loops:
        # Flagged by ``_on_factory_collected`` (synchronous with the
        # ``loop.set_task_factory`` that displaced us on CPython) or
        # by ``_release`` (the stash-but-don't-call corner). Surface
        # loudly to user code regardless of whether this site's own
        # factory inspection also catches it.
        raise TaskFactoryDisplaced(
            "Wool's task factory was displaced by a task factory "
            "installed after it; child tasks on this loop no longer "
            "fork onto fresh chains (copy-on-fork is lost). Install "
            "Wool's task factory last, or compose factories manually."
        )
    if loop in _loops_with_factory:
        # Wool's factory was installed on this loop earlier. If the
        # current factory is no longer Wool-wrapped, a third-party
        # factory was installed after Wool's and silently dropped
        # copy-on-fork for every task created since — a chain that no
        # longer forks per task is a latent correctness bug, so fail
        # loud rather than let it pass unnoticed.
        current = loop.get_task_factory()
        if not _is_wool_factory(current):
            _displaced_loops.add(loop)
            raise TaskFactoryDisplaced(
                "Wool's task factory was displaced by a task factory "
                "installed after it; child tasks on this loop no longer "
                "fork onto fresh chains (copy-on-fork is lost). Install "
                "Wool's task factory last, or compose factories manually."
            )
        return
    install_task_factory(loop)
