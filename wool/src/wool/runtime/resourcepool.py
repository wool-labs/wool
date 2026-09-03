from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Generic
from typing import TypeVar
from typing import cast

T = TypeVar("T")

_log = logging.getLogger(__name__)


class Resource(Generic[T]):
    """
    A single-use async context manager for resource acquisition.

    This class can only be used once as an async context manager. After
    acquisition, it cannot be reacquired, and after release, it cannot be
    released again.

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
        except Exception:
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
        if self._resource:
            await self._pool.release(self._key)


class ResourcePool(Generic[T]):
    """
    An asynchronous reference-counted cache with TTL-based cleanup.

    Objects are created on-demand via a factory function (sync or async) and
    automatically cleaned up after all references are released and the TTL
    expires.

    **Loop affinity.** A pool serves one running event loop at a time. It
    binds to the first loop that uses it and rebinds when used from
    another loop once the bound loop is no longer running, dropping every
    cached entry without running its finalizer: a resource cannot be torn
    down from a loop other than the one that made it. Entries still
    referenced when their loop stopped are reported at warning level,
    idle ones at debug. Using a bound pool from a second *running* loop
    raises `RuntimeError`, so tearing a pool down belongs to the loop that
    owns it.

    :param factory:
        Function to create new objects (sync or async).
    :param finalizer:
        Optional cleanup function (sync or async). It runs while the pool
        holds its internal lock, so it must not await `acquire`,
        `release`, `expire` or `clear` — those take the same lock and the
        call deadlocks; the read-only members are lock-free and safe. An
        `Exception` it raises is suppressed, a `BaseException` propagates
        to whichever operation ran the finalizer, and the entry is
        evicted either way.
    :param ttl:
        Time-to-live in seconds after last reference is released.
    """

    @dataclass
    class CacheEntry:
        """
        Internal cache entry tracking an object and its metadata.

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
            Whether `ResourcePool.expire` marked this entry for
            finalization as soon as its reference count reaches zero.
            Cleared when the entry is re-acquired first — re-access
            resurrects, matching the pool's timer-cancellation
            semantics.
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
                    "ResourcePool is bound to another running event loop; "
                    "use one pool per loop"
                )
            self._rebind(loop)
        assert self._mutex is not None
        return self._mutex

    def _rebind(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind this pool to ``loop``, dropping what the previous loop left.

        The dropped entries are never finalized: their finalizer would
        run against a loop that is no longer running. Entries still
        referenced when that loop stopped are a leak and are logged at
        warning level; idle entries are what the TTL would have
        discarded and are logged at debug level.
        """
        if self._cache:
            referenced = sum(1 for e in self._cache.values() if e.reference_count > 0)
            idle = len(self._cache) - referenced
            log = _log.warning if referenced else _log.debug
            log(
                "ResourcePool rebinding to a new event loop; dropping %d "
                "referenced and %d idle entries left by a loop that is no "
                "longer running (finalizers not run)",
                referenced,
                idle,
            )
            self._cache.clear()
        self._mutex = asyncio.Lock()
        self._loop = loop

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
        """
        Get a resource acquisition that can be awaited or used as context
        manager.

        :param key:
            The cache key.
        :returns:
            `Resource` that can be awaited or used with 'async with'.
        """
        return Resource(self, key)

    async def acquire(self, key: Any) -> T:
        """
        Internal acquire method - acquires a reference to the cached object.

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
                entry.reference_count += 1
                entry.doomed = False
                self._cancel_timer(entry)
                await self._cancel_cleanup(entry)
                return entry.obj
            else:
                # Cache miss - create new object
                obj = await self._await(self._factory, key)
                self._cache[key] = self.CacheEntry(obj=obj, reference_count=1)
                return obj

    async def release(self, key: Any) -> None:
        """
        Release a reference to the cached object.

        Decrements reference count. If count reaches 0, schedules cleanup
        after TTL expires (if TTL > 0); an entry retired by `expire` is
        finalized here rather than deferred — see `expire` for the
        retirement contract. Releasing a key that is not cached is a
        silent no-op.

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
                    # Inline rather than a spawned task: with nothing to
                    # defer to, a task spawned here may never be run.
                    await self._cleanup(key)
                else:
                    # Defer cleanup with a plain timer rather than a
                    # task parked on a TTL sleep: an unfired
                    # TimerHandle is discarded silently at loop close,
                    # whereas a parked task is destroyed pending —
                    # and, if never started, its coroutine emits a
                    # "never awaited" RuntimeWarning.
                    loop = asyncio.get_running_loop()
                    entry.timer = loop.call_later(self._ttl, self._expire, key)

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
            if entry.reference_count <= 0:
                # Also cancels any pending TTL timer or cleanup task.
                await self._cleanup(key)
            else:
                entry.doomed = True

    async def clear(self) -> None:
        """Finalize every cached entry and cancel pending cleanups.

        The teardown primitive: it force-finalizes regardless of reference
        count, which is correct when the pool itself is going away and
        there is nothing left to drain for. To retire a single key while
        the pool stays in use, use `expire`, which drains first.
        """
        async with self._lock:
            for key in list(self._cache.keys()):
                await self._cleanup(key)

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

        The task is cancelled and awaited. The current task is left
        alone: on the expiry path this runs *inside* the entry's own
        cleanup task (`_finalize`), which must not cancel itself.

        :param entry:
            The cache entry whose cleanup task to cancel.
        """
        cleanup = entry.cleanup
        entry.cleanup = None
        if cleanup is None or cleanup.done() or cleanup is asyncio.current_task():
            return
        cleanup.cancel()
        try:
            await cleanup
        except asyncio.CancelledError:
            pass

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
                        await self._await(self._finalizer, entry.obj)
                    except Exception:
                        pass
            finally:
                del self._cache[key]

    async def _await(self, func: Callable, *args) -> Any:
        """
        Call a function that might be sync or async.

        If the function is a coroutine function, await it. Otherwise, call it
        synchronously. If the result is a coroutine, await that as well.

        :param func:
            The function to call.
        :param args:
            Arguments to pass to the function.
        :returns:
            The result of the function call.
        """
        if asyncio.iscoroutinefunction(func):
            return await func(*args)
        else:
            result = func(*args)
            # Check if the result is a coroutine and await it if so
            if asyncio.iscoroutine(result):
                return await result
            return result
