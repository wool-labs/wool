from __future__ import annotations

import asyncio
import functools
import inspect
import logging
from dataclasses import dataclass
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Final
from typing import Generic
from typing import TypeVar
from typing import cast

#: Process-level interrupts — see `ResourcePool.expire_all` for their rank in a sweep.
_INTERRUPTS: Final = (KeyboardInterrupt, SystemExit)

T = TypeVar("T")

_log = logging.getLogger(__name__)


class Resource(Generic[T]):
    """Acquire one cached object for the duration of an ``async with`` block.

    This class can only be used once as an async context manager. After
    acquisition, it cannot be reacquired, and after release, it cannot be
    released again. The cached object may be any value, ``None`` and other
    falsy values included: the release is tied to the acquisition, not to
    the value acquired.

    :param pool:
        The `ResourcePool` this resource belongs to.
    :param key:
        The cache key for this resource.
    """

    def __init__(self, pool: ResourcePool[T], key):
        self._pool = pool
        self._key = key
        self._resource = None
        self._acquired = False
        self._released = False

    async def __aenter__(self) -> T:
        """
        Context manager entry - acquire resource.

        :returns:
            The cached resource object.
        :raises RuntimeError:
            If called on a resource that was previously acquired.
        """
        if self._acquired:
            raise RuntimeError(
                "Cannot re-acquire a resource that has already been acquired"
            )

        self._acquired = True
        try:
            self._resource = await self._pool.acquire(self._key)
            return cast(T, self._resource)
        except BaseException:
            self._acquired = False
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit - release resource.

        :param exc_type:
            Exception type if an exception occurred, None otherwise.
        :param exc_val:
            Exception value if an exception occurred, None otherwise.
        :param exc_tb:
            Exception traceback if an exception occurred, None otherwise.
        """
        await self._release()

    async def _release(self):
        """
        Release the resource.

        :raises RuntimeError:
            If attempting to release a resource that was not acquired or
            already released.
        """
        if not self._acquired:
            raise RuntimeError("Cannot release a resource that was not acquired")
        if self._released:
            raise RuntimeError(
                "Cannot release a resource that has already been released"
            )

        self._released = True
        await self._pool.release(self._key)


class ResourcePool(Generic[T]):
    """Cache objects by key with reference counting and TTL-based cleanup.

    Objects are created on-demand via a factory function (sync or async) and
    automatically cleaned up after all references are released and the TTL
    expires.

    **Loop affinity.** A pool serves one running event loop at a time. It
    binds to the first loop that uses it and rebinds when used from
    another loop once the bound loop is no longer running, dropping every
    cached entry without running its finalizer: a resource cannot be torn
    down from a loop other than the one that made it. Every dropped
    entry is reported at warning level, referenced and idle alike: a
    drop is not an expiry, so an idle entry the TTL would have closed
    is abandoned rather than finalized, and both counts name a resource
    that outlived every chance to finalize it. The record names the pool
    by its factory, and it is expected only of a loop that stopped with
    entries it did not clear: a loop that clears its pools before
    stopping leaves nothing to report. Using a bound pool from a second
    *running* loop raises `RuntimeError`, so tearing a pool down belongs
    to the loop that owns it.

    :param factory:
        Function to create new objects (sync or async). A coroutine a
        sync factory returns is awaited; any other awaitable is cached
        as the object itself.
    :param finalizer:
        Optional cleanup function (sync or async). It runs while the pool
        holds its internal lock, so it must not await any operation of
        *this* pool that mutates the cache — they all take the same lock
        and the call deadlocks, and reaching a resource through `get`
        counts; the read-only members are lock-free and safe, and so are
        the mutating members of a different pool, provided the
        finalizer-to-pool relation stays acyclic: two pools whose
        finalizers each mutate the other deadlock. An `Exception` it
        raises is contained — reported at warning level against the
        pool's factory name and the key, then suppressed — a
        `BaseException` propagates to whichever operation ran the
        finalizer (`expire_all` defers it to the end of its sweep), and
        the entry is evicted either way; a failure inside a cleanup the
        TTL spawned has no caller to propagate to and is reported the
        same way.
    :param ttl:
        Time-to-live in seconds after last reference is released.
    """

    @dataclass
    class CacheEntry:
        """Track a cached object and the lifecycle state the pool keeps for it.

        :param obj:
            The cached object.
        :param reference_count:
            Number of active references to this object.
        :param timer:
            Optional TTL timer scheduled when the reference count
            reaches zero; spawns the cleanup task once the TTL
            elapses.
        :param cleanup:
            Optional cleanup task created when the TTL timer fires.
        :param doomed:
            Whether the entry has been retired (see `ResourcePool.expire`
            for the retirement contract) and is finalized as soon as its
            reference count reaches zero. Cleared when the entry is
            re-acquired first — re-access resurrects, matching the pool's
            timer-cancellation semantics.
        """

        obj: Any
        reference_count: int
        timer: asyncio.TimerHandle | None = None
        cleanup: asyncio.Task | None = None
        doomed: bool = False

    @dataclass
    class Stats:
        """
        Statistics about the current state of the resource pool.

        :param total_entries:
            Total number of cached entries.
        :param referenced_entries:
            Number of entries currently being referenced (reference_count > 0).
        :param pending_cleanup:
            Number of keys in `ResourcePool.pending_cleanup`.
        """

        total_entries: int
        referenced_entries: int
        pending_cleanup: int

    def __init__(
        self,
        factory: Callable[[Any], T | Awaitable[T]],
        *,
        finalizer: Callable[[T], None | Awaitable[None]] | None = None,
        ttl: float = 0,
    ):
        self._factory = factory
        self._finalizer = finalizer
        self._ttl = ttl
        self._cache: dict[Any, ResourcePool.CacheEntry] = {}
        self._mutex: asyncio.Lock | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def __aenter__(self):
        """Async context manager entry.

        :returns:
            The ResourcePool instance itself.
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup all resources.

        :param exc_type:
            Exception type if an exception occurred, None otherwise.
        :param exc_val:
            Exception value if an exception occurred, None otherwise.
        :param exc_tb:
            Exception traceback if an exception occurred, None otherwise.
        """
        await self.clear()

    @property
    def stats(self) -> Stats:
        """
        Return cache statistics.

        .. note::
            This is synchronous for convenience, but should only be called
            when not concurrently modifying the cache.

        :returns:
            `ResourcePool.Stats` containing current statistics.
        """
        return self.Stats(
            total_entries=len(self._cache),
            referenced_entries=sum(
                1 for e in self._cache.values() if e.reference_count > 0
            ),
            pending_cleanup=len(self.pending_cleanup),
        )

    @property
    def pending_cleanup(self):
        """
        Map cache keys to their pending cleanup work.

        A pending entry holds either an unfired TTL timer or a
        cleanup task that has not finished.

        :returns:
            Dictionary mapping each such key to its pending TTL timer
            or cleanup task.
        """
        return {
            k: v.timer if v.timer is not None else v.cleanup
            for k, v in self._cache.items()
            if v.timer is not None or (v.cleanup is not None and not v.cleanup.done())
        }

    def get(self, key: Any) -> Resource[T]:
        """Return a single-use `Resource` for ``key``, entered with ``async with``.

        :param key:
            The cache key.
        :returns:
            The `Resource` for ``key``.
        """
        return Resource(self, key)

    async def acquire(self, key: Any) -> T:
        """Acquire a reference to the cached object, creating it on a miss.

        Creates a new object via the factory if not cached. Increments
        reference count and cancels any pending cleanup.

        :param key:
            The cache key.
        :returns:
            The cached or newly created object.
        """
        async with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                try:
                    await self._cancel_cleanup(entry)
                except BaseException:
                    # Interrupted after the cleanup task was cancelled:
                    # the entry is unreferenced with neither timer nor
                    # cleanup, so re-arm its TTL rather than orphan it.
                    if (
                        entry.reference_count == 0
                        and entry.timer is None
                        and self._ttl > 0
                    ):
                        self._arm_timer(key, entry)
                    raise
                self._cancel_timer(entry)
                entry.reference_count += 1
                entry.doomed = False
                return entry.obj
            else:
                # Cache miss - create new object
                created = self._factory(key)
                obj = cast(T, await created if asyncio.iscoroutine(created) else created)
                self._cache[key] = self.CacheEntry(obj=obj, reference_count=1)
                return obj

    async def release(self, key: Any) -> None:
        """Release a reference to the cached object.

        Decrements reference count. If count reaches 0, schedules cleanup
        after TTL expires (if TTL > 0); an entry retired by `expire` or
        `expire_all` is finalized here rather than deferred — see `expire`
        for the retirement contract. Releasing a key that is not cached is
        a silent no-op.

        A cancellation delivered while the release waits for the pool's
        lock does not abandon the decrement; the release completes and
        the caller still observes the cancellation.

        :param key:
            The cache key.
        :raises ValueError:
            If the key's reference count is already 0.

        .. rubric:: Implementation notes

        Finalizing inline rather than in a spawned task is what lets a
        release that lands while its loop is shutting down still close
        the resource: with nothing left to defer to, a task spawned there
        may never run — the closing loop would orphan it, and the
        resource with it.

        A cancellation delivered while this waits on a contended lock
        would abandon the decrement, i.e., the reference would be held
        forever and the entry never finalized, so the contended path
        runs shielded: the release completes on its own task after the
        caller has been cancelled. The uncontended path never suspends
        before the decrement, so it needs no shield and no task.
        """
        if self._lock.locked():
            await asyncio.shield(self._release(key))
        else:
            await self._release(key)

    async def expire(self, key: Any) -> None:
        """Treat *key* as TTL-expired now, finalizing it once unreferenced.

        Drops the pool's own retention of an entry — the retention that
        keeps it cached for reuse until its TTL fires — without touching
        the reference count callers hold through `acquire` and `release`.
        An unreferenced entry, including one already idling out its TTL,
        is finalized immediately; a referenced entry is marked and
        finalized by the release that drops its last reference, before
        that release returns, so in-flight users always drain first.
        Re-acquiring a marked entry before that release clears the mark —
        re-access resurrects, matching the pool's timer-cancellation
        semantics. Unlike `clear`, which tears the whole pool down
        regardless of reference count, this never finalizes a resource out
        from under an active reference. Expiring a key that is not cached
        is a silent no-op.

        :param key:
            The cache key to expire.
        """
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return
            await self._retire(key, entry)

    async def expire_all(self) -> None:
        """Treat every cached key as TTL-expired now.

        `expire` applied to every cached key at once (see `expire` for
        the per-key drain-first and resurrection semantics), with one
        difference: the sweep never stops early. It is the
        retirement primitive for a pool whose loop stays running.

        Retirement is all-or-nothing in reach, not in outcome: every
        cached key is retired even if finalizing one of them fails. A
        finalizer's `Exception` is contained as it is for every
        operation (see ``finalizer``), here per entry, so a failure does
        not end the sweep; any other `BaseException`, e.g., a cancelled
        teardown's `asyncio.CancelledError`, propagates once the sweep
        is over, not in place of it.

        A failure the ranking below does not re-raise or chain is logged
        at warning level and discarded, and a cancellation absorbed that
        way is uncancelled, so the caller's cancellation count is left
        as it found it.

        :raises asyncio.CancelledError:
            A cancellation delivered to a finalizer, re-raised once every
            remaining key has been retired.
        :raises KeyboardInterrupt:
            A process-level interrupt raised in a finalizer, re-raised
            likewise; it supersedes an earlier cancellation, which is
            chained as its ``__cause__``.
        :raises SystemExit:
            As `KeyboardInterrupt`.

        .. rubric:: Implementation notes

        The sweep deliberately outlives its own failures. This is the
        primitive a loop retires its resources through, and it is
        reached on teardown paths that are themselves cancellable, so
        abandoning the loop at the first `BaseException` would strand
        every key it had not reached yet, i.e., the leak the primitive
        exists to close, reappearing under cancellation. A delivered
        cancellation is consumed by the finalizer it lands in and the
        finalizers after it run uncancelled, so the sweep defers a single
        delivered ``cancel()`` by at most one finalizer, and uncancels
        what it absorbed so `asyncio.timeout` and `TaskGroup` see the
        count they expect.
        """
        async with self._lock:
            await self._sweep(self._retire)

    async def clear(self) -> None:
        """Finalize every cached entry and cancel pending cleanups.

        The teardown primitive: it force-finalizes regardless of reference
        count, which is correct when the pool itself is going away and
        there is nothing left to drain for. To retire keys while the pool
        stays in use, use `expire` or `expire_all`, which drain first. A
        finalizer that raises does not end the sweep: every key is
        reached, and an uncontained failure is re-raised afterwards under
        `expire_all`'s contract.

        :raises BaseException:
            As `expire_all`.
        """
        async with self._lock:
            await self._sweep(lambda key, _: self._cleanup(key))

    @property
    def _name(self) -> str:
        """Return the name the pool's log records carry: its factory's."""
        return (
            getattr(self._factory, "__qualname__", None) or type(self._factory).__name__
        )

    @property
    def _lock(self) -> asyncio.Lock:
        """Return the mutex serializing this pool on its bound loop.

        Binds the pool on first use and rebinds it when the running loop
        differs from the bound one and the bound one is no longer running
        — see the class docstring for what a rebind drops. Every method
        that touches ``_cache`` takes this lock first, so the loop check
        happens once per operation.

        :raises RuntimeError:
            If the pool is bound to another loop that is still running.

        .. rubric:: Implementation notes

        `asyncio.Lock` binds to a loop the first time it is *contended*
        — the uncontended acquire path never consults one — and never
        unbinds, so a single mutex built at construction would serve
        every uncontended caller and then raise for the first contender
        on any later loop. Liveness is `is_running`, not `is_closed`: a
        loop that has stopped but not yet closed cannot contend the
        mutex, and both the worker's loop rotation and the test fixtures
        stop a loop before closing it. `is_running` reads one attribute,
        so it is safe to call from another thread.
        """
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            if self._loop is not None and self._loop.is_running():
                raise RuntimeError(
                    f"ResourcePool({self._name}) is bound to another running "
                    "event loop; use one pool per loop"
                )
            self._rebind(loop)
        assert self._mutex is not None
        return self._mutex

    def _rebind(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind this pool to ``loop``, dropping what the previous loop left.

        Both counts are logged — see the class docstring for why an idle
        drop is a leak and not a deferred expiry.
        """
        if self._cache:
            referenced = sum(1 for e in self._cache.values() if e.reference_count > 0)
            idle = len(self._cache) - referenced
            _log.warning(
                "ResourcePool(%s) rebinding to a new event loop; dropping %d "
                "referenced and %d idle entries left by a loop that is no "
                "longer running (finalizers not run)",
                self._name,
                referenced,
                idle,
            )
            self._cache.clear()
        self._mutex = asyncio.Lock()
        self._loop = loop

    async def _release(self, key: Any) -> None:
        """Drop one reference under the lock — see `release`.

        :param key:
            The cache key.
        :raises ValueError:
            If the key's reference count is already 0.
        """
        async with self._lock:
            if key not in self._cache:
                return
            entry = self._cache[key]

            if entry.reference_count <= 0:
                raise ValueError(f"Reference count for key '{key}' is already 0")

            entry.reference_count -= 1

            if entry.reference_count <= 0:
                if entry.doomed or self._ttl <= 0:
                    # Inline — see the implementation notes on release.
                    await self._cleanup(key)
                else:
                    self._arm_timer(key, entry)

    def _arm_timer(self, key: Any, entry: ResourcePool.CacheEntry) -> None:
        """Schedule an unreferenced entry's TTL expiry on the bound loop.

        :param key:
            The cache key whose TTL to start.
        :param entry:
            The entry cached under ``key``.

        .. rubric:: Implementation notes

        Defers cleanup with a plain timer rather than a task parked on a
        TTL sleep: an unfired `asyncio.TimerHandle` is discarded silently
        at loop close, whereas a parked task is destroyed pending — and,
        if never started, its coroutine emits a "never awaited"
        `RuntimeWarning`.
        """
        loop = asyncio.get_running_loop()
        entry.timer = loop.call_later(self._ttl, self._expire, key)

    def _cancel_timer(self, entry: ResourcePool.CacheEntry) -> None:
        """
        Cancel an entry's pending TTL timer, if any.

        The timer always belongs to the bound loop, so a plain cancel
        suffices.

        :param entry:
            The cache entry whose timer to cancel.
        """
        if entry.timer is None:
            return
        timer, entry.timer = entry.timer, None
        timer.cancel()

    async def _cancel_cleanup(self, entry: ResourcePool.CacheEntry) -> None:
        """
        Cancel an entry's in-flight cleanup task, if any.

        The task is cancelled and waited for; its own cancellation is
        not re-raised here, while a cancellation of the current task is
        honored. The current task is left alone: on the expiry path this
        runs *inside* the entry's own cleanup task (`_finalize`), which
        must not cancel itself.

        :param entry:
            The cache entry whose cleanup task to cancel.
        """
        cleanup = entry.cleanup
        entry.cleanup = None
        if cleanup is None or cleanup.done() or cleanup is asyncio.current_task():
            return
        cleanup.cancel()
        await asyncio.wait({cleanup})

    def _expire(self, key: Any) -> None:
        """
        Spawn the cleanup task for an expired entry.

        Runs synchronously, as a timer callback, on the bound loop; see
        `_finalize` for how the spawned task tolerates a concurrent
        re-acquire. A timer that fires on a loop this pool has since
        left is ignored — its entry was dropped at the rebind.

        :param key:
            The cache key whose TTL elapsed.
        """
        if asyncio.get_running_loop() is not self._loop:
            return
        entry = self._cache.get(key)
        if entry is None:
            return
        entry.timer = None
        entry.cleanup = asyncio.get_running_loop().create_task(self._finalize(key))
        entry.cleanup.add_done_callback(functools.partial(self._report_cleanup, key))

    def _report_cleanup(self, key: Any, cleanup: asyncio.Task[None]) -> None:
        """Log a cleanup task that ended in a failure.

        A `BaseException` a finalizer raises inside a spawned cleanup has
        no caller to propagate to, so it is retrieved and reported here
        rather than left for the loop's unretrieved-exception hook.

        :param key:
            The cache key the cleanup was for.
        :param cleanup:
            The finished cleanup task.
        """
        if cleanup.cancelled():
            return
        error = cleanup.exception()
        if error is not None:
            _log.warning(
                "ResourcePool(%s) cleanup of key %r failed",
                self._name,
                key,
                exc_info=error,
            )

    async def _finalize(self, key: Any) -> None:
        """
        Clean up an expired entry if it is still unreferenced.

        Re-checks the reference count under the lock, so an entry
        re-acquired between TTL expiry and lock acquisition is left
        untouched; cancellation by a concurrent re-acquire is
        likewise tolerated as an expected outcome.

        :param key:
            The cache key to clean up.
        """
        try:
            async with self._lock:
                if key in self._cache:
                    entry = self._cache[key]
                    if entry.reference_count == 0:
                        await self._cleanup(key)

        except asyncio.CancelledError:
            pass

    async def _sweep(
        self, retire: Callable[[Any, ResourcePool.CacheEntry], Awaitable[None]]
    ) -> None:
        """Apply ``retire`` to every cached entry, deferring failures to the end.

        See `expire_all` for the contract this implements.

        .. warning::
            Must be called while holding the lock.

        :param retire:
            The per-entry retirement, given the key and its entry.
        """
        failure: BaseException | None = None
        superseded: BaseException | None = None
        absorbed = 0
        for key, entry in list(self._cache.items()):
            try:
                await retire(key, entry)
            except BaseException as error:
                if isinstance(error, asyncio.CancelledError):
                    absorbed += 1
                # Ranking — see expire_all's :raises: contract.
                if failure is None:
                    failure = error
                elif isinstance(error, _INTERRUPTS) and not isinstance(
                    failure, _INTERRUPTS
                ):
                    failure, superseded = error, failure
                else:
                    _log.warning(
                        "ResourcePool(%s) sweep discarding a later failure for key %r",
                        self._name,
                        key,
                        exc_info=error,
                    )
        if isinstance(failure, asyncio.CancelledError):
            absorbed -= 1
        # Uncancel what the sweep absorbed — see expire_all.
        task = asyncio.current_task()
        if task is not None:
            for _ in range(absorbed):
                task.uncancel()
        if failure is not None:
            if superseded is not None:
                raise failure from superseded
            raise failure

    async def _retire(self, key: Any, entry: ResourcePool.CacheEntry) -> None:
        """Retire one entry under `expire`'s drain-first contract.

        .. warning::
            Must be called while holding the lock.

        :param key:
            The cache key to retire.
        :param entry:
            The entry cached under ``key``.
        """
        if entry.reference_count <= 0:
            await self._cleanup(key)
        else:
            entry.doomed = True

    async def _cleanup(self, key: Any) -> None:
        """
        Remove entry from cache and call finalizer.

        .. warning::
            Must be called while holding the lock.

        :param key:
            The cache key to cleanup.
        """
        entry = self._cache[key]
        try:
            self._cancel_timer(entry)
            await self._cancel_cleanup(entry)
        finally:
            # Evict from the cache *unconditionally*, before and
            # regardless of how the finalizer exits. A finalized
            # resource must never remain cached: if the finalizer
            # raises — including ``CancelledError`` when cleanup runs
            # under a cancelled teardown, which is a ``BaseException``
            # and so escapes ``except Exception`` — the entry must
            # still be removed, or a later ``acquire`` hands back a
            # torn-down resource (e.g., a closed event loop). The
            # inner ``try`` lets the finalizer run for its side
            # effects while the outer ``finally`` guarantees eviction
            # and lets any cancellation propagate.
            try:
                if self._finalizer:
                    try:
                        result = self._finalizer(entry.obj)
                        if inspect.isawaitable(result):
                            await result
                    except Exception:
                        _log.warning(
                            "ResourcePool(%s) finalizer failed for key %r",
                            self._name,
                            key,
                            exc_info=True,
                        )
            finally:
                del self._cache[key]
