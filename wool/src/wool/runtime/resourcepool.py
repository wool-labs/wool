from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import inspect
import logging
import threading
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

    Entering the block takes one reference on the object cached under
    ``key`` in the entering loop's partition of the pool (see
    `ResourcePool`), creating the object through the pool's factory on a
    miss and cancelling any cleanup its TTL had pending. Exiting drops
    that reference. When it was the last, the entry's TTL is armed, or,
    when the pool has no TTL or the entry was retired by
    `ResourcePool.expire`, the entry is finalized inline before the exit
    returns. A cancellation delivered during the exit does not abandon
    the reference: the release completes and the caller still observes
    the cancellation. The release lands only on the entry the
    acquisition took: when `ResourcePool.clear` has evicted that entry
    in between, the exit drops nothing, and an entry cached since under
    the same key is untouched.

    This class can only be used once as an async context manager. After
    acquisition, it cannot be reacquired, and after release, it cannot be
    released again. The cached object may be any value, ``None`` and other
    falsy values included: the release is tied to the acquisition, not to
    the value acquired. The block must exit on the loop that entered it:
    an exit on another loop is refused with `RuntimeError`, but the
    reference is not lost. The release is handed to the acquiring loop,
    which runs it as a task of its own, unless that loop has closed, in
    which case the entry is stranded and reported with its partition when
    the pool sweeps it.

    :param pool:
        The `ResourcePool` this resource belongs to.
    :param key:
        The cache key for this resource.
    """

    def __init__(self, pool: ResourcePool[T], key):
        self._pool = pool
        self._key = key
        self._resource = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._partition: _Partition[T] | None = None
        self._entry: ResourcePool.CacheEntry | None = None
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
            self._loop = asyncio.get_running_loop()
            self._partition = self._pool._partition()
            self._resource, self._entry = await self._partition.acquire(self._key)
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
            already released, or from a loop other than the one that
            acquired it; in the last case the release has already been
            handed to the acquiring loop when that loop is still open.
        """
        if not self._acquired:
            raise RuntimeError("Cannot release a resource that was not acquired")
        if self._released:
            raise RuntimeError(
                "Cannot release a resource that has already been released"
            )
        assert self._loop is not None and self._partition is not None
        assert self._entry is not None

        self._released = True
        if asyncio.get_running_loop() is not self._loop:
            outcome = (
                "the release was handed to the acquiring loop"
                if self._hand_back()
                else "the acquiring loop is closed, so the reference is stranded"
            )
            raise RuntimeError(
                f"ResourcePool({self._pool._name}) cannot release key "
                f"{self._key!r} on a loop other than the one that acquired it; "
                f"{outcome}"
            )
        await self._partition.release(self._key, self._entry)

    def _hand_back(self) -> bool:
        """Schedule the release on the acquiring loop, or report that it closed.

        :returns:
            Whether the release was scheduled.

        .. rubric:: Implementation notes

        The refusal in `_release` must not pin the reference it refuses
        to drop: the acquiring partition's count would stay inflated,
        the entry could never idle out, and its loop would strand it
        unfinalized when it stopped. The release therefore goes back to
        the loop that can run it through `asyncio.run_coroutine_threadsafe`,
        with `asyncio.AbstractEventLoop.is_closed` as the liveness check,
        the predicate asyncio's own cross-loop paths use, and the
        `RuntimeError` a loop closed in between raises caught as the same
        outcome. The coroutine is closed on that path so it is never
        reported as un-awaited.
        """
        assert self._loop is not None and self._partition is not None
        assert self._entry is not None
        if self._loop.is_closed():
            return False
        release = self._partition.release(self._key, self._entry)
        try:
            future = asyncio.run_coroutine_threadsafe(release, self._loop)
        except RuntimeError:
            release.close()
            return False
        future.add_done_callback(self._report_hand_back)
        return True

    def _report_hand_back(self, future: concurrent.futures.Future[None]) -> None:
        """Log a handed-back release that ended in a failure.

        :param future:
            The finished release.
        """
        if future.cancelled():
            return
        error = future.exception()
        if error is not None:
            _log.warning(
                "ResourcePool(%s) release of key %r handed to its acquiring loop failed",
                self._pool._name,
                self._key,
                exc_info=error,
            )


class ResourcePool(Generic[T]):
    """Cache objects by key with reference counting and TTL-based cleanup.

    Objects are created on-demand via a factory function (sync or async) and
    automatically cleaned up after all references are released and the TTL
    expires. A reference is taken and dropped through the `Resource` that
    `get` returns, which owns the reference contract.

    **Loop partitioning.** A pool serves any number of event loops at
    once, each through a private partition only that loop can reach:
    entries, reference counts, and TTL timers are never shared across
    loops, because a pooled resource can only be used and finalized on
    the loop that created it. `get`, `expire`, `expire_all`, `clear`,
    `stats`, and `pending_cleanup` therefore act on the calling loop's
    partition alone, and a process-wide clear does not exist. A partition
    is finalized by clearing it from its own loop before that loop
    closes. A partition whose loop has closed without doing so is swept
    by the next operation on any loop that mutates the pool: its entries
    are dropped without running their finalizers, referenced and idle
    alike, and the drop is reported at warning level, since a drop is not
    an expiry and a resource stranded that way is a leak whether or not
    anything still referenced it. A loop that has stopped without closing
    keeps its partition, since it can resume and finish what it started.
    The record names the pool by its factory, and it is expected only of
    a loop that closed with entries it did not clear: a loop that clears
    its pools before closing leaves nothing to report. `stats` and
    `pending_cleanup` are reads: they sweep nothing, register nothing,
    and log nothing.

    :param factory:
        Function to create new objects (sync or async). A coroutine a
        sync factory returns is awaited; any other awaitable is cached
        as the object itself.
    :param finalizer:
        Optional cleanup function (sync or async). It runs while the
        calling loop's partition holds its lock, so it must not await any
        operation of *this* pool that mutates the cache — on that loop
        they all take the same lock and the call deadlocks, and reaching
        a resource through `get` counts; the read-only members are
        lock-free and safe, and so are the mutating members of a
        different pool, provided the finalizer-to-pool relation stays
        acyclic: two pools whose finalizers each mutate the other
        deadlock. An `Exception` it
        raises is contained — reported at warning level against the
        pool's factory name and the key, then suppressed — a
        `BaseException` propagates to whichever operation ran the
        finalizer (`expire_all` defers it to the end of its sweep), and
        the entry is evicted either way; a finalizer interrupted by a
        cancellation is reported the same way before the cancellation
        propagates, since the resource it was closing is evicted without
        having been closed. A failure inside a cleanup the TTL spawned
        has no caller to propagate to and is reported likewise.
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
        self._partitions: dict[asyncio.AbstractEventLoop, _Partition[T]] = {}
        self._registry_lock = threading.Lock()

    async def __aenter__(self):
        """Async context manager entry.

        :returns:
            The ResourcePool instance itself.
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clear the exiting loop's partition on exit — see `clear`.

        The bracket is loop-scoped like every other operation: entries
        another loop cached survive it and are cleared from that loop.

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
        """Return statistics for the calling loop's partition.

        A read: it reports zeros for a loop that has no partition rather
        than creating one, and it neither sweeps nor logs.

        :returns:
            `ResourcePool.Stats` containing current statistics.
        :raises RuntimeError:
            If there is no running event loop.
        """
        partition = self._lookup()
        if partition is None:
            return ResourcePool.Stats(0, 0, 0)
        return partition.stats

    @property
    def pending_cleanup(self):
        """Map the calling loop's cache keys to their pending cleanup work.

        A pending entry holds either an unfired TTL timer or a
        cleanup task that has not finished, the one currently inside
        the entry's finalizer included. A read, as `stats` is.

        :returns:
            Dictionary mapping each such key to its pending TTL timer
            or cleanup task.
        :raises RuntimeError:
            If there is no running event loop.
        """
        partition = self._lookup()
        if partition is None:
            return {}
        return partition.pending_cleanup

    def get(self, key: Any) -> Resource[T]:
        """Return a single-use `Resource` for ``key``, entered with ``async with``.

        The only way to take a reference on a cached object; `Resource`
        owns the reference contract.

        :param key:
            The cache key.
        :returns:
            The `Resource` for ``key``.
        """
        return Resource(self, key)

    async def expire(self, key: Any) -> None:
        """Treat *key* as TTL-expired now, finalizing it once unreferenced.

        Drops the pool's own retention of an entry — the retention that
        keeps it cached for reuse until its TTL fires — without touching
        the reference count callers hold through `get`. An unreferenced
        entry, including one already idling out its TTL, is finalized
        immediately; a referenced entry is marked and finalized by the
        release that drops its last reference, before that release
        returns, so in-flight users always drain first. Re-acquiring a
        marked entry before that release clears the mark — re-access
        resurrects, matching the pool's timer-cancellation semantics.
        Unlike `clear`, which tears the loop's whole partition down
        regardless of reference count, this never finalizes a resource
        out from under an active reference. Expiring a key that is not
        cached is a silent no-op.

        :param key:
            The cache key to expire.
        """
        await self._partition().expire(key)

    async def expire_all(self) -> None:
        """Treat every key the calling loop cached as TTL-expired now.

        `expire` applied to every key in the calling loop's partition at
        once (see `expire` for the per-key drain-first and resurrection
        semantics), with one difference: the sweep never stops early. It
        is the retirement primitive for a loop that stays running.

        Retirement is all-or-nothing in reach, not in outcome: every
        cached key is retired even if finalizing one of them fails. A
        finalizer's `Exception` is contained as it is for every
        operation (see ``finalizer``), here per entry, so a failure does
        not end the sweep; any other `BaseException`, e.g., a cancelled
        teardown's `asyncio.CancelledError`, propagates once the sweep
        is over, not in place of it.

        A failure the ranking below does not re-raise or chain is logged
        at warning level and discarded. Every cancellation delivered to
        the sweeping task during the sweep is uncancelled except the one
        it re-raises, so the caller's cancellation count is left as the
        caller expects it.

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
        delivered ``cancel()`` by at most one finalizer. The uncancel
        accounting measures cancellations delivered to the task, by
        comparing `asyncio.Task.cancelling` before and after the sweep,
        rather than `asyncio.CancelledError` instances observed: a
        finalizer can raise one without the task having been cancelled,
        e.g., by awaiting a future a third party cancelled, and
        uncancelling for that would consume a genuine cancellation the
        caller is still owed. This is the accounting `asyncio.timeout`
        performs for the same reason.
        """
        await self._partition().expire_all()

    async def clear(self) -> None:
        """Finalize every entry the calling loop cached and cancel its cleanups.

        The teardown primitive: it force-finalizes regardless of reference
        count, which is correct when the loop's use of the pool is over
        and there is nothing left to drain for. It is loop-scoped: another
        loop's partition is untouched and must be cleared from that loop,
        the only place its finalizers can run. To retire keys while the
        loop stays in use, use `expire` or `expire_all`, which drain
        first. A finalizer that raises does not end the sweep: every key
        is reached, and an uncontained failure is re-raised afterwards
        under `expire_all`'s contract.

        :raises BaseException:
            As `expire_all`.
        """
        await self._partition().clear()

    @property
    def _name(self) -> str:
        """Return the name the pool's log records carry: its factory's."""
        return (
            getattr(self._factory, "__qualname__", None) or type(self._factory).__name__
        )

    def _lookup(self) -> _Partition[T] | None:
        """Return the running loop's partition, or ``None`` if it has none.

        The read path: no sweep, no creation.

        :raises RuntimeError:
            If there is no running event loop.
        """
        loop = asyncio.get_running_loop()
        with self._registry_lock:
            return self._partitions.get(loop)

    def _partition(self) -> _Partition[T]:
        """Return the running loop's partition, creating it on first use.

        The mutating path: every call also sweeps each partition whose
        loop has closed — see the class docstring for what a sweep drops
        and how it is reported.

        :raises RuntimeError:
            If there is no running event loop.

        .. rubric:: Implementation notes

        The registry is the only state shared across loops, so its lookup
        is the only cross-thread critical section: synchronous, linear in
        the number of loops that have touched the pool, and never held
        across an ``await``. It takes an explicit `threading.Lock` rather
        than relying on dict atomicity under the GIL, which free-threaded
        Python does not preserve. Sweeping on every mutating access
        rather than on a miss keeps the report prompt: a loop that closed
        without clearing is reported by the next such operation on any
        loop, not only by the arrival of a loop the pool has never seen.
        The sweep is skipped when the registry holds only the caller's
        own partition, the steady state of a single-loop process, since
        a running loop is never closed.

        Liveness is `asyncio.AbstractEventLoop.is_closed`, the predicate
        asyncio's own cross-loop paths use, and not
        `asyncio.AbstractEventLoop.is_running`: a loop between two
        ``run_until_complete`` calls is not running but can resume and
        finish what it started, and sweeping its partition from another
        thread would drop entries still referenced and race the owner's
        next access to its own cache. A closed loop can never resume, so
        nothing with a live owner is destroyed. Both predicates read one
        attribute, so either is safe to call from another thread.

        Stale partitions are popped under the lock and discarded after
        it is released: `_Partition.discard` logs, and logging takes the
        handler lock and can block on I/O, which no other loop's next
        pool operation should wait behind.
        """
        loop = asyncio.get_running_loop()
        stale: list[_Partition[T]] = []
        with self._registry_lock:
            if len(self._partitions) > 1 or loop not in self._partitions:
                stale = [
                    self._partitions.pop(owner)
                    for owner in list(self._partitions)
                    if owner.is_closed()
                ]
            partition = self._partitions.get(loop)
            if partition is None:
                partition = _Partition(self)
                self._partitions[loop] = partition
        for orphan in stale:
            orphan.discard()
        return partition


class _Partition(Generic[T]):
    """One event loop's share of a `ResourcePool`.

    Holds the cache, the `asyncio.Lock` serializing it, and the TTL
    timers for a single loop; the owning pool routes every operation
    here from that loop alone, and a `Resource` releases through the
    partition it acquired from. The caching semantics implemented here
    are documented on `ResourcePool`, which owns the contract, and the
    reference semantics on `Resource`.

    :param pool:
        The owning pool, whose factory, finalizer, TTL, and name this
        partition applies.
    """

    def __init__(self, pool: ResourcePool[T]):
        self._pool = pool
        self._cache: dict[Any, ResourcePool.CacheEntry] = {}
        self._lock = asyncio.Lock()

    @property
    def stats(self) -> ResourcePool.Stats:
        """Implement `ResourcePool.stats` for this partition."""
        return ResourcePool.Stats(
            total_entries=len(self._cache),
            referenced_entries=sum(
                1 for e in self._cache.values() if e.reference_count > 0
            ),
            pending_cleanup=len(self.pending_cleanup),
        )

    @property
    def pending_cleanup(self):
        """Implement `ResourcePool.pending_cleanup` for this partition."""
        return {
            k: v.timer if v.timer is not None else v.cleanup
            for k, v in self._cache.items()
            if v.timer is not None or (v.cleanup is not None and not v.cleanup.done())
        }

    async def acquire(self, key: Any) -> tuple[T, ResourcePool.CacheEntry]:
        """Take one reference on ``key``'s object — see `Resource`.

        :returns:
            The object and the entry the reference was taken on, which
            `release` needs back to tell the reference's entry from one
            cached since under the same key.
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
                        and self._pool._ttl > 0
                    ):
                        self._arm_timer(key, entry)
                    raise
                self._cancel_timer(entry)
                entry.reference_count += 1
                entry.doomed = False
                return entry.obj, entry
            else:
                # Cache miss - create new object
                created = self._pool._factory(key)
                obj = cast(T, await created if asyncio.iscoroutine(created) else created)
                entry = ResourcePool.CacheEntry(obj=obj, reference_count=1)
                self._cache[key] = entry
                return obj, entry

    async def release(self, key: Any, entry: ResourcePool.CacheEntry) -> None:
        """Drop one reference on ``key``'s object — see `Resource`.

        :param key:
            The cache key.
        :param entry:
            The entry the reference was taken on, as `acquire` returned
            it; a release whose entry is no longer the one cached drops
            nothing.

        .. rubric:: Implementation notes

        Runs shielded on every path. A cancellation delivered while the
        release waits on a contended lock would abandon the decrement,
        i.e., the reference would be held forever and the entry never
        finalized, so the release completes on its own task after the
        caller has been cancelled. There is no unshielded fast path: one
        keyed on `asyncio.Lock.locked` misses the window after a holder
        releases the lock and before the waiter it woke has taken it,
        during which the lock reads unlocked yet a fresh acquire parks
        behind that waiter. `asyncio.shield` on an inner that completes
        without suspending returns its result directly, so the
        uncontended case costs one task and no extra loop turn.
        """
        await asyncio.shield(self._release(key, entry))

    async def expire(self, key: Any) -> None:
        """Implement `ResourcePool.expire` for this partition."""
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return
            await self._retire(key, entry)

    async def expire_all(self) -> None:
        """Implement `ResourcePool.expire_all` for this partition."""
        async with self._lock:
            await self._sweep(self._retire)

    async def clear(self) -> None:
        """Implement `ResourcePool.clear` for this partition."""
        async with self._lock:
            await self._sweep(lambda key, _: self._cleanup(key))

    def discard(self) -> None:
        """Drop every entry without finalizing it, reporting what was lost.

        The path a partition takes when its loop is found to have
        closed: the finalizers cannot run without that loop, so the
        entries are abandoned and the drop is logged at warning level
        with the referenced and idle counts — see `ResourcePool` for why
        both count as leaks. Dropping the entries drops their timer
        handles, the last references the partition holds to its loop.
        """
        if not self._cache:
            return
        referenced = sum(1 for e in self._cache.values() if e.reference_count > 0)
        _log.warning(
            "ResourcePool(%s) dropping %d referenced and %d idle entries "
            "stranded by an event loop that closed without clearing its "
            "partition (finalizers not run)",
            self._pool._name,
            referenced,
            len(self._cache) - referenced,
        )
        self._cache.clear()

    async def _release(self, key: Any, entry: ResourcePool.CacheEntry) -> None:
        """Drop one reference under the lock — see `release`.

        :param key:
            The cache key.
        :param entry:
            The entry the reference was taken on.
        :raises ValueError:
            If the key's reference count is already 0, a state the
            `Resource` guards make unreachable and this keeps as an
            invariant.

        .. rubric:: Implementation notes

        Finalizing inline rather than in a spawned task is what lets a
        release that lands while its loop is shutting down still close
        the resource: with nothing left to defer to, a task spawned there
        may never run — the closing loop would orphan it, and the
        resource with it.

        The release is matched to its entry by identity rather than by
        key: a `clear` evicts an entry under its holders, and a holder's
        release after that must not decrement whatever entry a later
        acquire cached under the same key, which for a zero-TTL pool
        would finalize the later holder's live object under it.
        """
        async with self._lock:
            if self._cache.get(key) is not entry:
                return

            if entry.reference_count <= 0:
                raise ValueError(f"Reference count for key '{key}' is already 0")

            entry.reference_count -= 1

            if entry.reference_count <= 0:
                if entry.doomed or self._pool._ttl <= 0:
                    # Inline — see the implementation notes.
                    await self._cleanup(key)
                else:
                    self._arm_timer(key, entry)

    def _arm_timer(self, key: Any, entry: ResourcePool.CacheEntry) -> None:
        """Schedule an unreferenced entry's TTL expiry on this partition's loop.

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
        entry.timer = loop.call_later(self._pool._ttl, self._expire, key)

    def _cancel_timer(self, entry: ResourcePool.CacheEntry) -> None:
        """
        Cancel an entry's pending TTL timer, if any.

        The timer always belongs to this partition's loop, so a plain
        cancel suffices.

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
        honored. The current task is left alone, and left recorded on
        the entry: on the expiry path this runs *inside* the entry's own
        cleanup task (`_finalize`), which must not cancel itself, and
        which `pending_cleanup` keeps reporting until the entry is
        evicted.

        :param entry:
            The cache entry whose cleanup task to cancel.
        """
        cleanup = entry.cleanup
        if cleanup is None or cleanup.done() or cleanup is asyncio.current_task():
            return
        entry.cleanup = None
        cleanup.cancel()
        await asyncio.wait({cleanup})

    def _expire(self, key: Any) -> None:
        """
        Spawn the cleanup task for an expired entry.

        Runs synchronously, as a timer callback, on this partition's
        loop; see `_finalize` for how the spawned task tolerates a
        concurrent re-acquire. A timer whose entry has since been retired
        finds nothing cached and is ignored.

        :param key:
            The cache key whose TTL elapsed.
        """
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
                self._pool._name,
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
        task = asyncio.current_task()
        baseline = task.cancelling() if task is not None else 0
        failure: BaseException | None = None
        superseded: BaseException | None = None
        for key, entry in list(self._cache.items()):
            try:
                await retire(key, entry)
            except GeneratorExit:
                # The sweep's own coroutine is being closed, i.e., its
                # task was destroyed pending: there is no loop left to
                # retire the remaining keys on.
                raise
            except BaseException as error:
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
                        self._pool._name,
                        key,
                        exc_info=error,
                    )
        if task is not None:
            # Uncancel what the sweep absorbed, keeping the one it
            # re-raises — see expire_all.
            kept = 1 if isinstance(failure, asyncio.CancelledError) else 0
            for _ in range(max(0, task.cancelling() - baseline - kept)):
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

        A key no longer cached is ignored, so the call is idempotent.

        .. warning::
            Must be called while holding the lock.

        :param key:
            The cache key to cleanup.
        """
        entry = self._cache.get(key)
        if entry is None:
            return
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
                if self._pool._finalizer:
                    try:
                        result = self._pool._finalizer(entry.obj)
                        if inspect.isawaitable(result):
                            await result
                    except Exception:
                        _log.warning(
                            "ResourcePool(%s) finalizer failed for key %r",
                            self._pool._name,
                            key,
                            exc_info=True,
                        )
                    except asyncio.CancelledError:
                        # Reported here because the eviction below is
                        # the last the pool sees of the resource — see
                        # the ``finalizer`` parameter.
                        _log.warning(
                            "ResourcePool(%s) finalizer for key %r was interrupted "
                            "by a cancellation; the resource was evicted without "
                            "being closed",
                            self._pool._name,
                            key,
                        )
                        raise
            finally:
                self._cache.pop(key, None)
