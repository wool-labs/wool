from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Final
from typing import Generic
from typing import TypeVar
from typing import cast

T = TypeVar("T")


SENTINEL: Final = object()


class Resource(Generic[T]):
    """
    A single-use async context manager for resource acquisition.

    This class can only be used once as an async context manager. After
    acquisition, it cannot be reacquired, and after release, it cannot be
    released again.

    :param pool:
        The :py:class:`ResourcePool` this resource belongs to.
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

    :param factory:
        Function to create new objects (sync or async).
    :param finalizer:
        Optional cleanup function (sync or async).
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
        :param cleanup:
            Optional cleanup task scheduled when reference count reaches zero.
        """

        obj: Any
        reference_count: int
        cleanup: asyncio.Task | None = None

    @dataclass
    class Stats:
        """
        Statistics about the current state of the resource pool.

        :param total_entries:
            Total number of cached entries.
        :param referenced_entries:
            Number of entries currently being referenced (reference_count > 0).
        :param pending_cleanup:
            Number of cleanup tasks currently pending execution.
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
        self._lock = asyncio.Lock()

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
            :py:class:`ResourcePool.Stats` containing current statistics.
        """
        pending_cleanup = sum(
            1 for c in self.pending_cleanup.values() if c is not None and not c.done()
        )
        return self.Stats(
            total_entries=len(self._cache),
            referenced_entries=sum(
                1 for e in self._cache.values() if e.reference_count > 0
            ),
            pending_cleanup=pending_cleanup,
        )

    @property
    def pending_cleanup(self):
        """Dictionary of cache keys with pending cleanup tasks.

        :returns:
            Dictionary mapping cache keys to their cleanup tasks.
        """
        return {
            k: v.cleanup
            for k, v in self._cache.items()
            if v.cleanup is not None and not v.cleanup.done()
        }

    def get(self, key: Any) -> Resource[T]:
        """
        Get a resource acquisition that can be awaited or used as context
        manager.

        :param key:
            The cache key.
        :returns:
            :py:class:`Resource` that can be awaited or used with 'async with'.
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

                # Cancel pending cleanup task if it exists
                if entry.cleanup is not None and not entry.cleanup.done():
                    entry.cleanup.cancel()
                    try:
                        await entry.cleanup
                    except asyncio.CancelledError:
                        pass
                    entry.cleanup = None

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
        after TTL expires (if TTL > 0).

        :param key:
            The cache key.
        :raises KeyError:
            If key not in cache.
        """
        async with self._lock:
            if key not in self._cache:
                raise KeyError(f"Key '{key}' not found in cache")
            entry = self._cache[key]

            if entry.reference_count <= 0:
                raise ValueError(f"Reference count for key '{key}' is already 0")

            entry.reference_count -= 1

            if entry.reference_count <= 0:
                if self._ttl > 0:
                    # Schedule cleanup after TTL
                    entry.cleanup = asyncio.create_task(self._schedule_cleanup(key))
                else:
                    # Immediate cleanup
                    await self._cleanup(key)

    async def clear(self, key=SENTINEL) -> None:
        """Clear cache entries and cancel pending cleanups.

        :param key:
            Specific key to clear, or SENTINEL to clear all entries.
        """
        async with self._lock:
            # Clean up all entries
            if key is SENTINEL:
                keys = list(self._cache.keys())
            else:
                keys = [key]
            for key in keys:
                await self._cleanup(key)

    async def _schedule_cleanup(self, key: Any) -> None:
        """
        Schedule cleanup after TTL delay.

        Only cleans up if the reference count is still 0 when TTL expires.

        :param key:
            The cache key to schedule cleanup for.
        """
        try:
            await asyncio.sleep(self._ttl)

            async with self._lock:
                # Double-check conditions - reference might have been re-acquired
                if key in self._cache:
                    entry = self._cache[key]
                    if entry.reference_count == 0:
                        await self._cleanup(key)

        except asyncio.CancelledError:
            # Cleanup was cancelled due to new reference - this is expected
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
            # Cancel cleanup task if running
            if entry.cleanup is not None and not entry.cleanup.done():
                entry.cleanup.cancel()
                try:
                    await entry.cleanup
                except asyncio.CancelledError:
                    pass
        finally:
            # Call finalizer
            if self._finalizer:
                try:
                    await self._await(self._finalizer, entry.obj)
                except Exception:
                    pass
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
