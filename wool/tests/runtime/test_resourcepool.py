import asyncio
import contextlib
import gc
import logging
import threading
import time
import warnings
import weakref
from contextlib import AsyncExitStack
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies

from wool.runtime.resourcepool import Resource
from wool.runtime.resourcepool import ResourcePool


def make_resource(key):
    """Build a placeholder resource for ``key``.

    A module-level ``def`` so the pool's records name it
    ``ResourcePool(make_resource)``.
    """
    return f"obj-{key}"


#: Handles `_acquire` holds open, keyed by pool, loop, and key, so a test
#: can take and drop references non-lexically through the public surface.
_handles: dict[tuple[int, int, Any], list[AsyncExitStack]] = {}


async def _acquire(pool, key):
    """Take a reference on ``key`` through ``pool.get`` and return the object.

    The handle stays open until `_release` closes it on the same loop.
    """
    stack = AsyncExitStack()
    obj = await stack.enter_async_context(pool.get(key))
    _handles.setdefault((id(pool), id(asyncio.get_running_loop()), key), []).append(
        stack
    )
    return obj


async def _release(pool, key):
    """Drop the most recent reference `_acquire` took on ``key`` on this loop."""
    slot = (id(pool), id(asyncio.get_running_loop()), key)
    stack = _handles[slot].pop()
    if not _handles[slot]:
        del _handles[slot]
    await stack.aclose()


@pytest.fixture(autouse=True)
def _drop_handles():
    """Forget the handles a test left open."""
    yield
    _handles.clear()


async def _read(value):
    """Return an already-read value, so a property read can be awaited."""
    return value


async def _pool_stats(pool):
    """Read a pool's stats from the loop this coroutine runs on."""
    return pool.stats


async def _cache_idle_entry(pool, key):
    """Acquire and release ``key``, leaving it cached and idle."""
    async with pool.get(key):
        pass


async def _use_resource(resource):
    """Enter and immediately exit an already-built resource."""
    async with resource:
        pass


async def _close_stack_elsewhere(resource):
    """Exit ``resource`` from whichever loop runs this coroutine."""
    stack = AsyncExitStack()
    stack.push_async_exit(resource)
    await stack.aclose()


async def _poll_until(predicate, timeout=2.0):
    """Wait for ``predicate`` to hold, failing rather than hanging."""
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "the condition never held"
        await asyncio.sleep(0.01)


def _pool_records(caplog):
    """Return the records the pool's own logger emitted."""
    return [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]


@strategies.composite
def _loop_mixes(draw):
    """Draw a (running, stopped) split of one to four loops.

    At least one loop stays running, since a stopped loop cannot touch
    the pool.
    """
    total = draw(strategies.integers(1, 4))
    stopped = draw(strategies.integers(0, total - 1))
    return total - stopped, stopped


@strategies.composite
def factory_functions(draw):
    """Generate various factory function types with consistent interfaces."""
    factory_type = draw(
        strategies.sampled_from(
            [
                "sync_simple",
                "async_simple",
                "sync_lambda",
                "async_lambda",
                "callable",
                "awaitable",
            ]
        )
    )

    if factory_type == "sync_simple":

        def sync_factory(key):
            obj = Mock()
            obj.name = f"sync-{key}"
            obj.created_by = "sync_simple"
            return obj

        return sync_factory

    elif factory_type == "async_simple":

        async def async_factory(key):
            obj = Mock()
            obj.name = f"async-{key}"
            obj.created_by = "async_simple"
            return obj

        return async_factory

    elif factory_type == "sync_lambda":

        def sync_lambda_factory(key):
            return SimpleNamespace(name=f"lambda-{key}", created_by="sync_lambda")

        return lambda key: sync_lambda_factory(key)

    elif factory_type == "async_lambda":

        async def async_lambda_factory(key):
            return SimpleNamespace(name=f"async-lambda-{key}", created_by="async_lambda")

        return lambda key: async_lambda_factory(key)

    elif factory_type == "callable":

        class CallableLike:
            def __call__(self, key):
                return self.sync_factory(key)

            def sync_factory(self, key):
                obj = Mock()
                obj.name = f"callable-{key}"
                obj.created_by = "callable"
                return obj

        return CallableLike()

    elif factory_type == "awaitable":

        class AwaitableLike:
            def __init__(self, key) -> None:
                self.key = key

            def __await__(self):
                return self.async_factory().__await__()

            async def async_factory(self):
                obj = Mock()
                obj.name = f"awaitable-{self.key}"
                obj.created_by = "awaitable"
                return obj

        return AwaitableLike


@strategies.composite
def finalizer_functions(draw):
    """Generate various finalizer function types."""
    finalizer_type = draw(
        strategies.sampled_from(
            [
                None,
                "sync_simple",
                "async_simple",
                "sync_lambda",
                "async_lambda",
            ]
        )
    )

    if finalizer_type is None:
        return None

    elif finalizer_type == "sync_simple":

        def simple_sync_finalizer(obj):
            assert obj is not None

        return simple_sync_finalizer

    elif finalizer_type == "async_simple":

        async def simple_async_finalizer(obj):
            assert obj is not None

        return simple_async_finalizer

    elif finalizer_type == "sync_lambda":

        def sync_lambda_finalizer(obj):
            assert obj is not None

        return lambda obj: sync_lambda_finalizer(obj)

    elif finalizer_type == "async_lambda":

        async def async_lambda_finalizer(obj):
            assert obj is not None

        return lambda obj: async_lambda_finalizer(obj)


@pytest.fixture
def retired_entry_pool(mocker):
    """Build a long-TTL pool holding one entry retired while referenced.

    Returns the pool, its finalizer mock and its factory mock. The
    factory yields ``"first"`` then ``"second"``, so a test can prove
    eviction by acquiring again and getting the second object.
    """
    factory = mocker.Mock(side_effect=["first", "second"])
    finalizer = mocker.AsyncMock()
    pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)
    return pool, finalizer, factory


@pytest.fixture
def expiry_race_pool(mocker):
    """Build a short-TTL pool whose lock can be parked via a blocker key.

    Returns the pool, its finalizer mock, the list of factory calls,
    and the event that releases the parked ``blocker`` acquire.
    """
    release_blocker = asyncio.Event()
    factory_calls = []

    async def factory(key):
        factory_calls.append(key)
        if key == "blocker":
            await release_blocker.wait()
        return f"obj-{key}"

    finalizer = mocker.AsyncMock()
    pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=0.05)
    return pool, finalizer, factory_calls, release_blocker


async def _queue_behind_fired_cleanup(pool, factory_calls, queued_coroutine):
    """Race a fired TTL cleanup against an operation queued on the pool lock.

    Caches and releases ``expired`` so its TTL timer arms, parks an
    acquire of ``blocker`` inside its factory — the factory runs under
    the pool lock, so the lock stays held — then queues the given
    operation on the (FIFO) lock and waits for the timer to fire so
    its cleanup task queues behind that operation. Returns the blocker
    and queued-operation tasks.
    """
    async with pool.get("expired"):
        pass

    blocker_task = asyncio.create_task(_acquire(pool, "blocker"))

    async def blocker_parked():
        while "blocker" not in factory_calls:
            await asyncio.sleep(0)

    await asyncio.wait_for(blocker_parked(), timeout=2.0)

    queued_task = asyncio.create_task(queued_coroutine)

    async def cleanup_task_spawned():
        # The armed timer already counts as pending; wait until the
        # pending work is the fired timer's cleanup *task*.
        while not isinstance(pool.pending_cleanup.get("expired"), asyncio.Task):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(cleanup_task_spawned(), timeout=2.0)
    return blocker_task, queued_task


@pytest.fixture
def counting_factory():
    """Create a factory that counts how many times it's called."""

    class CountingFactory:
        def __init__(self):
            self.call_count = 0

        def __call__(self, _key):
            self.call_count += 1
            return f"resource-{self.call_count}"

    return CountingFactory()


class TestResourcePool:
    @staticmethod
    @strategies.composite
    def setup(draw, *, max_key_count=5):
        """Generate a ResourcePool with varied initial resource states.

        Creates a pool with 0-max_key_count resources using the public API
        to create realistic pool states for property-based testing.

        :param draw:
            The Hypothesis draw function for generating test data.
        :param max_key_count:
            Maximum number of keys to create resources for.
        :returns:
            An async function that when called returns a tuple of
            (ResourcePool, factory, list of resources, list of keys).
        """
        factory = draw(factory_functions())
        finalizer = draw(finalizer_functions())
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=0)
        created_resources = []
        keys = []

        async def setup():
            for i in range(draw(strategies.integers(0, max_key_count))):
                key = f"resource-{i}"
                keys.append(key)

                # Create the initial resource using public API and track it
                async with pool.get(key) as resource:
                    created_resources.append(resource)

                # The resource is now in the pool with TTL=0, so it should be immediately
                # cleaned up. We verify pool behavior through public interface

            return pool, factory, created_resources, keys

        return setup

    @pytest.mark.asyncio
    @settings(max_examples=50, deadline=None)
    @given(setup=setup())
    async def test_get_should_return_resource_instance(self, setup):
        """Test that get returns a Resource instance.

        Given:
            A pool with various initial resource states
        When:
            get() is called with a test key
        Then:
            Should return a Resource instance
        """
        # Arrange
        pool, _, _, _ = await setup()

        # Act
        resource_acquisition = pool.get("test-key")

        # Assert
        assert isinstance(resource_acquisition, Resource)

    @pytest.mark.asyncio
    async def test_release_should_decrement_reference_counts(self):
        """Test releasing resources decrements reference counts properly.

        Given:
            A pool with resources that have active references
        When:
            Resources are released via _release(pool, )
        Then:
            Should properly decrement ref counts or cleanup and remove resources
        """
        # Arrange - Create pool with TTL to keep resources after context exit
        mock_factory = Mock()
        pool = ResourcePool(factory=mock_factory, ttl=60)

        # Create some test resources
        test_keys = ["key1", "key2", "key3"]
        for i, key in enumerate(test_keys):
            mock_factory.return_value = f"resource-{i}"
            async with pool.get(key):
                pass  # Creates and caches the resource

        # Verify initial state
        assert pool.stats.total_entries == len(test_keys)
        assert pool.stats.referenced_entries == 0  # All released from context

        # Now manually acquire some resources to test release
        await _acquire(pool, "key1")
        await _acquire(pool, "key2")

        assert pool.stats.referenced_entries == 2

        # Act & assert
        await _release(pool, "key1")
        assert pool.stats.referenced_entries == 1

        await _release(pool, "key2")
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_expire_should_await_finalizer_when_it_returns_an_awaitable(self):
        """Test a finalizer's non-coroutine awaitable is awaited.

        Given:
            A pool whose synchronous finalizer returns an object that is
            awaitable but not a coroutine.
        When:
            An idle entry is expired.
        Then:
            It should await that object as part of the finalization.
        """
        # Arrange
        awaited = []

        class Completion:
            def __await__(self):
                awaited.append(True)
                yield from ()

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}",
            finalizer=lambda obj: Completion(),
            ttl=60,
        )
        async with pool.get("key"):
            pass

        # Act
        await pool.expire("key")

        # Assert
        assert awaited == [True]
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_acquire_should_cache_an_awaitable_object_as_is(self):
        """Test a factory's non-coroutine awaitable is the cached object.

        Given:
            A pool whose synchronous factory returns an object that is
            awaitable but not a coroutine.
        When:
            A key is acquired.
        Then:
            It should hand back that object itself rather than await it.
        """

        # Arrange
        class Handle:
            def __await__(self):
                raise AssertionError("the handle must not be awaited")
                yield

        handle = Handle()
        pool = ResourcePool(factory=lambda key: handle, ttl=60)

        # Act
        async with pool.get("key") as resource:
            # Assert
            assert resource is handle

    @pytest.mark.asyncio
    async def test_expire_should_log_warning_when_finalizer_raises(self, caplog):
        """Test a finalizer's contained failure is still reported.

        Given:
            A pool whose finalizer raises an Exception.
        When:
            An idle entry is expired.
        Then:
            It should evict the entry and log one warning naming the
            pool's factory and the key.
        """

        # Arrange
        def factory(key):
            return f"obj-{key}"

        async def finalizer(obj):
            raise RuntimeError("close failed")

        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)
        async with pool.get("key"):
            pass

        # Act
        with caplog.at_level(logging.WARNING, logger="wool.runtime.resourcepool"):
            await pool.expire("key")

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert len(records) == 1
        assert "factory" in records[0].getMessage()
        assert "'key'" in records[0].getMessage()
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_ttl_cleanup_should_log_warning_when_spawned_cleanup_fails(
        self, caplog
    ):
        """Test a failure inside a spawned cleanup task is reported.

        Given:
            A short-TTL pool whose finalizer raises a BaseException that
            is neither a cancellation nor a process-level interrupt, so
            the spawned cleanup ends with it and no caller receives it.
        When:
            An entry's TTL elapses and its cleanup task runs.
        Then:
            It should evict the entry and log one warning naming the
            pool's factory and the key, rather than leave the failure
            unretrieved.
        """

        # Arrange
        class Interrupt(BaseException):
            pass

        def factory(key):
            return f"obj-{key}"

        async def finalizer(obj):
            raise Interrupt()

        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=0.01)

        # Act
        with caplog.at_level(logging.WARNING, logger="wool.runtime.resourcepool"):
            async with pool.get("key"):
                pass
            # The report lands one tick after the eviction, so poll for
            # the record rather than the empty pool.
            deadline = time.monotonic() + 2.0
            while (
                not any(r.name == "wool.runtime.resourcepool" for r in caplog.records)
                and time.monotonic() < deadline
            ):
                await asyncio.sleep(0.01)

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert len(records) == 1
        assert "factory" in records[0].getMessage()
        assert "'key'" in records[0].getMessage()
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "ttl, retire_first",
        [(0, False), (60, True)],
        ids=["zero-ttl", "retired-while-referenced"],
    )
    async def test_finalizer_should_still_evict_entry_when_raising_base_exception(
        self, ttl, retire_first
    ):
        """Test a cancelled finalizer still evicts the cache entry.

        Given:
            A pool that finalizes inline — either because it has no TTL
            or because the entry was retired by ``expire`` while still
            referenced — whose finalizer raises ``CancelledError`` — a
            ``BaseException``, not an ``Exception`` — on its first call,
            modelling cleanup that runs under a cancelled teardown
        When:
            A resource is acquired and released, driving the inline
            cleanup whose finalizer raises
        Then:
            The ``CancelledError`` propagates, but the torn-down entry
            is still evicted, so the next acquire is a cache miss that
            builds a fresh resource via the factory rather than handing
            back the finalized one
        """

        # Arrange
        finalizer_calls = {"count": 0}

        async def finalizer(obj):
            finalizer_calls["count"] += 1
            if finalizer_calls["count"] == 1:
                # First cleanup runs under cancellation.
                raise asyncio.CancelledError()

        factory = Mock(
            side_effect=[
                SimpleNamespace(name="first"),
                SimpleNamespace(name="second"),
            ]
        )
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=ttl)

        # Act
        # Acquire then release: rc -> 0 drives inline cleanup, whose
        # finalizer raises CancelledError out of the release.
        with pytest.raises(asyncio.CancelledError):
            async with pool.get("key"):
                if retire_first:
                    await pool.expire("key")

        # Assert
        # The finalized resource must not survive in the cache.
        assert pool.stats.total_entries == 0
        # The next acquire is therefore a miss that builds a fresh
        # resource, never the torn-down one.
        async with pool.get("key") as resource:
            assert resource.name == "second"
        assert factory.call_count == 2

    def test_stats_should_raise_when_no_running_loop(self, mocker):
        """Test reading stats outside a running loop is an error.

        Given:
            A pool read from synchronous code, with no event loop
            running.
        When:
            The stats property is accessed.
        Then:
            It should raise RuntimeError, since a partition — and so
            the statistics describing one — only exists relative to a
            running loop.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)

        # Act & assert
        with pytest.raises(RuntimeError, match="no running event loop"):
            pool.stats

    def test_pending_cleanup_should_raise_when_no_running_loop(self, mocker):
        """Test reading pending cleanup outside a running loop is an error.

        Given:
            A pool read from synchronous code, with no event loop
            running.
        When:
            The pending_cleanup property is accessed.
        Then:
            It should raise RuntimeError, for the same reason stats
            does: the pending work belongs to a loop's partition.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)

        # Act & assert
        with pytest.raises(RuntimeError, match="no running event loop"):
            pool.pending_cleanup

    def test_acquire_should_isolate_entries_when_two_loops_run_concurrently(
        self, background_loops
    ):
        """Test one pool serves two live loops through separate partitions.

        Given:
            One pool and two event loops running concurrently on their
            own threads, with a factory slow enough for acquires to
            overlap.
        When:
            Each loop acquires the same key twice concurrently.
        Then:
            It should invoke the factory exactly once per loop, hand each
            loop its own object, and report one entry per loop, so
            entries never cross loops while acquires on one loop still
            serialize.
        """
        # Arrange
        objects = []

        async def factory(key):
            await asyncio.sleep(0.05)
            obj = object()
            objects.append(obj)
            return obj

        pool = ResourcePool(factory, ttl=60)
        handles = [background_loops() for _ in range(2)]

        async def acquire_twice():
            first, second = await asyncio.gather(
                _acquire(pool, "key"), _acquire(pool, "key")
            )
            entries = pool.stats.total_entries
            await pool.clear()
            return first, second, entries

        # Act
        futures = [handle.submit(acquire_twice()) for handle in handles]
        results = [future.result(timeout=5) for future in futures]

        # Assert
        assert len(objects) == 2
        assert all(first is second for first, second, _ in results)
        assert results[0][0] is not results[1][0]
        assert [entries for _, _, entries in results] == [1, 1]

    @given(loop_count=strategies.integers(2, 5))
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_acquire_should_isolate_partitions_when_many_loops_run_concurrently(
        self, background_loops, loop_count
    ):
        """Test partitions stay private however many loops share a pool.

        Given:
            One pool and any number of event loops, between two and
            five, running concurrently on their own threads.
        When:
            Every loop acquires the same key twice concurrently.
        Then:
            It should invoke the factory exactly once per loop, hand
            each loop a distinct object shared by both of that loop's
            acquires, and report exactly one entry to each loop.
        """
        # Arrange
        objects = []

        async def factory(key):
            await asyncio.sleep(0.01)
            obj = object()
            objects.append(obj)
            return obj

        pool = ResourcePool(factory, ttl=60)
        handles = [background_loops() for _ in range(loop_count)]

        async def acquire_twice():
            first, second = await asyncio.gather(
                _acquire(pool, "key"), _acquire(pool, "key")
            )
            entries = pool.stats.total_entries
            await pool.clear()
            return first, second, entries

        # Act
        try:
            futures = [handle.submit(acquire_twice()) for handle in handles]
            results = [future.result(timeout=10) for future in futures]
        finally:
            # Retire each example's loops eagerly; the fixture would
            # otherwise hold every loop of every example open.
            for handle in handles:
                handle.close()

        # Assert
        assert len(objects) == loop_count
        assert all(first is second for first, second, _ in results)
        assert len({id(first) for first, _, _ in results}) == loop_count
        assert [entries for _, _, entries in results] == [1] * loop_count

    def test_expire_all_should_finalize_only_current_loop_partition(
        self, mocker, background_loops
    ):
        """Test retirement stops at the calling loop's partition.

        Given:
            A pool holding one idle entry on an event loop still
            running on another thread and one idle entry on a second
            loop.
        When:
            expire_all is awaited on the second loop.
        Then:
            It should finalize that loop's entry alone, leaving the
            first loop's entry cached with its finalizer never run.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=lambda key: key, finalizer=finalizer, ttl=60)
        live = background_loops()

        async def cache_idle_entry(key):
            async with pool.get(key):
                pass

        live.run(cache_idle_entry("live"))

        async def retire_own_partition():
            await cache_idle_entry("own")
            await pool.expire_all()
            return pool.stats.total_entries

        async def live_stats():
            return pool.stats

        # Act
        remaining = asyncio.run(retire_own_partition())

        # Assert
        live_snapshot = live.run(live_stats())
        assert remaining == 0
        finalizer.assert_awaited_once_with("own")
        assert live_snapshot.total_entries == 1

    def test_clear_should_finalize_only_current_loop_partition(
        self, mocker, background_loops
    ):
        """Test clear leaves another running loop's entries alone.

        Given:
            A pool holding one referenced entry on an event loop that is
            still running on another thread, and one entry on a second
            loop.
        When:
            The pool is cleared from the second loop.
        Then:
            It should finalize the second loop's entry only: the first
            loop's entry stays cached and referenced with its finalizer
            never run, until that loop clears it.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=lambda key: key, finalizer=finalizer, ttl=60)
        live = background_loops()

        async def clear_own_entry():
            await _acquire(pool, "second")
            await pool.clear()
            return pool.stats.total_entries

        async def live_stats():
            return pool.stats

        live.run(_acquire(pool, "first"))

        # Act
        remaining = asyncio.run(clear_own_entry())

        # Assert
        live_snapshot = live.run(live_stats())
        assert remaining == 0
        finalizer.assert_awaited_once_with("second")
        assert live_snapshot.total_entries == 1
        assert live_snapshot.referenced_entries == 1

    def test_stats_should_count_only_current_loop_entries(self, background_loops):
        """Test stats describe the calling loop's partition alone.

        Given:
            A pool holding two entries on an event loop running on
            another thread and one entry on a second loop.
        When:
            stats is read on each loop.
        Then:
            It should report two entries to the first loop and one to
            the second.
        """
        # Arrange
        pool = ResourcePool(factory=lambda key: key, ttl=60)
        live = background_loops()

        async def acquire_many(*keys):
            for key in keys:
                await _acquire(pool, key)
            return pool.stats.total_entries

        # Act
        first = live.run(acquire_many("a", "b"))
        second = asyncio.run(acquire_many("c"))

        # Assert
        live.run(pool.clear())
        assert first == 2
        assert second == 1

    def test_acquire_should_sweep_partition_when_its_loop_closed(
        self, mocker, stranded_loop
    ):
        """Test a fresh loop starts from an empty partition.

        Given:
            A pool that cached and released an entry on an event loop
            that has since closed, leaving that entry's TTL timer behind.
        When:
            The same key is acquired from a fresh event loop.
        Then:
            It should sweep the closed loop's partition -- the entry and
            its timer gone, its finalizer never run -- invoke the factory
            again, and hold only the new entry with no pending cleanup.
        """
        # Arrange
        factory = mocker.Mock(return_value="obj")
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)

        async def acquire_and_release():
            async with pool.get("key"):
                pass
            return pool.pending_cleanup

        _, pending_before = stranded_loop(acquire_and_release())

        async def acquire_again():
            acquired = await _acquire(pool, "key")
            return acquired, pool.stats.total_entries, pool.pending_cleanup

        # Act
        acquired, entries, pending = asyncio.run(acquire_again())

        # Assert
        assert "key" in pending_before
        assert acquired == "obj"
        assert factory.call_count == 2
        finalizer.assert_not_awaited()
        assert entries == 1
        assert pending == {}

    def test_acquire_should_sweep_partition_when_its_loop_stopped_but_not_closed(
        self, mocker, stranded_loop
    ):
        """Test liveness is whether a loop runs, not whether it closed.

        Given:
            A pool holding an entry on an event loop that has stopped
            running but has not been closed.
        When:
            The same key is acquired from a second event loop.
        Then:
            It should sweep the stopped loop's partition and rebuild the
            resource on the new loop, since a loop that is not running
            cannot finalize what it cached.
        """
        # Arrange
        factory = mocker.Mock(return_value="obj")
        pool = ResourcePool(factory=factory, ttl=60)
        stranded_loop(_acquire(pool, "key"), close=False)

        # Act
        acquired = asyncio.run(_acquire(pool, "key"))

        # Assert
        assert acquired == "obj"
        assert factory.call_count == 2

    def test_acquire_should_not_warn_when_partition_cleared_before_loop_stopped(
        self, caplog, stranded_loop
    ):
        """Test a partition cleared by its own loop is swept silently.

        Given:
            A pool whose entry was cleared on its own event loop before
            that loop closed, leaving an empty partition in the
            registry.
        When:
            The pool is used from a fresh event loop.
        Then:
            It should sweep the empty partition without logging
            anything, because nothing was stranded and there is no leak
            to report.
        """

        # Arrange
        def make_resource(key):
            return "obj"

        pool = ResourcePool(factory=make_resource, ttl=60)

        async def acquire_and_clear():
            async with pool.get("key"):
                pass
            await pool.clear()

        stranded_loop(acquire_and_clear())

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(_acquire(pool, "other"))

        # Assert
        assert [r for r in caplog.records if r.name == "wool.runtime.resourcepool"] == []

    def test_acquire_should_warn_when_sweeping_stranded_referenced_entry(
        self, caplog, stranded_loop
    ):
        """Test a still-referenced stranded entry is reported as a leak.

        Given:
            A pool whose loop closed while an entry was still referenced.
        When:
            The pool is used from a fresh event loop.
        Then:
            It should log one WARNING from wool.runtime.resourcepool
            naming the pool by its factory and the referenced entry it
            dropped without finalizing.
        """

        # Arrange
        def make_resource(key):
            return "obj"

        pool = ResourcePool(factory=make_resource, ttl=60)
        stranded_loop(_acquire(pool, "key"))

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(_acquire(pool, "other"))

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert [r.levelno for r in records] == [logging.WARNING]
        assert "make_resource" in records[0].getMessage()
        assert "1 referenced and 0 idle" in records[0].getMessage()

    def test_acquire_should_warn_when_sweeping_stranded_idle_entry(
        self, caplog, stranded_loop
    ):
        """Test an idle stranded entry is reported as a leak too.

        Given:
            A pool whose loop closed with only an idle entry cached
            (released, awaiting its TTL).
        When:
            The pool is used from a fresh event loop.
        Then:
            It should log one WARNING from wool.runtime.resourcepool
            reporting the idle entry, since an entry the loop never
            finalized is a leak whether or not it was in use.
        """

        # Arrange
        def make_resource(key):
            return "obj"

        pool = ResourcePool(factory=make_resource, ttl=60)

        async def acquire_and_release():
            async with pool.get("key"):
                pass

        stranded_loop(acquire_and_release())

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(_acquire(pool, "other"))

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert [r.levelno for r in records] == [logging.WARNING]
        assert "0 referenced and 1 idle" in records[0].getMessage()

    def test_acquire_should_warn_once_per_partition_when_several_loops_stranded(
        self, caplog, background_loops
    ):
        """Test the sweep drains every stale partition, reporting each.

        Given:
            A pool that cached one entry on each of three event loops
            running concurrently on their own threads, all of which
            then stopped without clearing their partitions.
        When:
            The pool is used from a fresh event loop.
        Then:
            It should log exactly one WARNING per stranded partition,
            so the whole registry is drained on the first touch rather
            than one partition at a time.
        """

        # Arrange
        def make_resource(key):
            return "obj"

        pool = ResourcePool(factory=make_resource, ttl=60)
        handles = [background_loops() for _ in range(3)]
        for index, handle in enumerate(handles):
            handle.run(_acquire(pool, f"key-{index}"))
        for handle in handles:
            handle.close()

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(_acquire(pool, "other"))

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert [r.levelno for r in records] == [logging.WARNING] * 3
        assert all("1 referenced and 0 idle" in r.getMessage() for r in records)

    def test_release_should_fire_only_its_own_timer_when_its_loop_resumes(
        self, mocker, caplog, stranded_loop
    ):
        """Test a paused loop's TTL timer reaches only that loop's entry.

        Given:
            A pool that released an entry on one event loop, scheduling
            its TTL timer there, which then paused while a second loop
            acquired the same key and still holds it.
        When:
            The first loop resumes long enough for that timer to fire.
        Then:
            It should finalize the first loop's own idle entry and leave
            the second loop's entry untouched, still cached and still
            referenced, reporting nothing.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=0.05)

        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            first_loop, _ = stranded_loop(_cache_idle_entry(pool, "key"), close=False)
            second_loop, _ = stranded_loop(_acquire(pool, "key"), close=False)

            # Act
            first_loop.run_until_complete(
                _poll_until(lambda: finalizer.await_count == 1, timeout=2.0)
            )
            first = first_loop.run_until_complete(_pool_stats(pool))
            second = second_loop.run_until_complete(_pool_stats(pool))

        # Assert
        finalizer.assert_awaited_once_with("obj-key")
        assert first.total_entries == 0
        assert second.total_entries == 1
        assert second.referenced_entries == 1
        assert _pool_records(caplog) == []

    def test_acquire_should_sweep_stranded_partition_when_calling_loop_registered(
        self, caplog, background_loops, stranded_loop
    ):
        """Test a stranded partition is reported by a loop the pool already serves.

        Given:
            A pool already serving an event loop running on another
            thread, and a second loop that cached an entry and then
            closed without clearing it.
        When:
            The first loop, whose partition already exists, acquires
            another key.
        Then:
            It should sweep the closed loop's partition on that access
            and log one WARNING for it, rather than waiting for a loop
            the pool has never seen.
        """

        # Arrange
        def make_resource(key):
            return "obj"

        pool = ResourcePool(factory=make_resource, ttl=60)
        live = background_loops()
        live.run(_acquire(pool, "first"))
        stranded_loop(_acquire(pool, "stranded"))

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            live.run(_acquire(pool, "second"))

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert [r.levelno for r in records] == [logging.WARNING]
        assert "1 referenced and 0 idle" in records[0].getMessage()

    def test_acquire_should_serve_each_loop_when_two_race_the_registry(
        self, background_loops
    ):
        """Test the partition registry holds up under real contention.

        Given:
            One pool and two event loops on their own threads, each
            parked on a barrier immediately before its first acquire, so
            both reach the registry at the same instant.
        When:
            Both loops acquire the same key at once.
        Then:
            It should invoke the factory once per loop, hand each loop
            its own object, and report exactly one entry to each.
        """
        # Arrange
        barrier = threading.Barrier(2)
        objects = []
        objects_lock = threading.Lock()

        def factory(key):
            obj = object()
            with objects_lock:
                objects.append(obj)
            return obj

        pool = ResourcePool(factory, ttl=60)
        handles = [background_loops() for _ in range(2)]

        async def race():
            # Blocking the loop's own thread is what puts both threads
            # inside the registry lock's window together.
            barrier.wait(timeout=5)
            acquired = await _acquire(pool, "key")
            entries = pool.stats.total_entries
            await pool.clear()
            return acquired, entries

        # Act
        futures = [handle.submit(race()) for handle in handles]
        results = [future.result(timeout=10) for future in futures]

        # Assert
        assert len(objects) == 2
        assert results[0][0] is not results[1][0]
        assert [entries for _, entries in results] == [1, 1]

    @given(loop_count=strategies.integers(2, 4), acquires=strategies.integers(1, 8))
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_acquire_should_build_one_object_per_loop_for_any_concurrency(
        self, background_loops, loop_count, acquires
    ):
        """Test the partition lock and the registry compose at any width.

        Given:
            Any two to four event loops sharing one pool, each making
            between one and eight concurrent acquires of the same key.
        When:
            Every acquire runs, every reference is released, and each
            loop clears its own partition.
        Then:
            It should invoke the factory once per loop, hand all of a
            loop's acquires the same object, hand different loops
            different objects, and finalize each object exactly once.
        """
        # Arrange
        objects = []
        finalized = []
        records_lock = threading.Lock()

        async def factory(key):
            await asyncio.sleep(0)
            obj = object()
            with records_lock:
                objects.append(obj)
            return obj

        def finalizer(obj):
            with records_lock:
                finalized.append(obj)

        pool = ResourcePool(factory, finalizer=finalizer, ttl=60)
        handles = [background_loops() for _ in range(loop_count)]

        async def acquire_many():
            acquired = await asyncio.gather(
                *(_acquire(pool, "key") for _ in range(acquires))
            )
            for _ in range(acquires):
                await _release(pool, "key")
            await pool.clear()
            return acquired

        # Act
        try:
            futures = [handle.submit(acquire_many()) for handle in handles]
            results = [future.result(timeout=10) for future in futures]
        finally:
            # Retire each example's loops eagerly; the fixture would
            # otherwise hold every loop of every example open.
            for handle in handles:
                handle.close()

        # Assert
        assert len(objects) == loop_count
        assert all(len({id(obj) for obj in acquired}) == 1 for acquired in results)
        held = [acquired[0] for acquired in results]
        assert len({id(obj) for obj in held}) == loop_count
        assert sorted(id(obj) for obj in finalized) == sorted(id(obj) for obj in held)

    def test___aexit___should_clear_only_the_calling_loops_partition(
        self, mocker, background_loops
    ):
        """Test the pool's own context manager clears one partition.

        Given:
            A pool holding one referenced entry on an event loop still
            running on another thread.
        When:
            A second loop enters ``async with pool``, caches a key
            inside the block, and leaves it.
        Then:
            It should finalize the second loop's entry alone, leaving
            the first loop's entry cached and still referenced.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        live = background_loops()
        live.run(_acquire(pool, "live"))

        async def use_pool():
            async with pool:
                await _acquire(pool, "own")
            return pool.stats.total_entries

        # Act
        remaining = asyncio.run(use_pool())

        # Assert
        live_snapshot = live.run(_pool_stats(pool))
        assert remaining == 0
        finalizer.assert_awaited_once_with("obj-own")
        assert live_snapshot.total_entries == 1
        assert live_snapshot.referenced_entries == 1

    def test_clear_should_finalize_nothing_when_the_calling_loop_has_no_partition(
        self, mocker, background_loops
    ):
        """Test clearing from a loop that never reached the pool is a no-op.

        Given:
            A pool holding one referenced entry on an event loop still
            running on another thread, and a second loop that has never
            touched the pool.
        When:
            clear() is awaited on the second loop.
        Then:
            It should run no finalizer and report an empty partition,
            leaving the first loop's entry cached and referenced.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        live = background_loops()
        live.run(_acquire(pool, "live"))

        async def clear_nothing():
            await pool.clear()
            return pool.stats.total_entries

        # Act
        remaining = asyncio.run(clear_nothing())

        # Assert
        live_snapshot = live.run(_pool_stats(pool))
        assert remaining == 0
        finalizer.assert_not_awaited()
        assert live_snapshot.total_entries == 1
        assert live_snapshot.referenced_entries == 1

    def test_expire_all_should_finalize_nothing_when_the_loop_has_no_partition(
        self, mocker, background_loops
    ):
        """Test retiring from a loop that never reached the pool is a no-op.

        Given:
            A pool holding one referenced entry on an event loop still
            running on another thread, and a second loop that has never
            touched the pool.
        When:
            expire_all() is awaited on the second loop.
        Then:
            It should run no finalizer and report an empty partition,
            leaving the first loop's entry cached and referenced.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        live = background_loops()
        live.run(_acquire(pool, "live"))

        async def retire_nothing():
            await pool.expire_all()
            return pool.stats.total_entries

        # Act
        remaining = asyncio.run(retire_nothing())

        # Assert
        live_snapshot = live.run(_pool_stats(pool))
        assert remaining == 0
        finalizer.assert_not_awaited()
        assert live_snapshot.total_entries == 1
        assert live_snapshot.referenced_entries == 1

    def test_stats_should_leave_a_paused_loops_partition_intact(
        self, caplog, stranded_loop
    ):
        """Test a paused loop's partition survives another loop's read.

        Given:
            A pool holding one idle entry on an event loop that is
            neither running nor closed, paused between two
            run_until_complete calls.
        When:
            stats is read from a fresh loop.
        Then:
            It should report an empty partition to the fresh loop, log
            nothing, and leave the paused loop's entry cached for when
            it resumes, since a loop that can resume is alive.
        """
        # Arrange
        pool = ResourcePool(factory=make_resource, ttl=60)
        paused, _ = stranded_loop(_cache_idle_entry(pool, "key"), close=False)

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            fresh = asyncio.run(_pool_stats(pool))
            resumed = paused.run_until_complete(_pool_stats(pool))

        # Assert
        assert _pool_records(caplog) == []
        assert fresh.total_entries == 0
        assert resumed.total_entries == 1

    def test_acquire_should_find_its_entry_when_its_loop_resumes(
        self, mocker, caplog, stranded_loop
    ):
        """Test a paused loop resumes with its cache where it left it.

        Given:
            A pool whose paused loop cached an idle entry, and a fresh
            loop that has since read the pool and cached and cleared an
            entry of its own under the same key.
        When:
            The paused loop resumes and acquires the same key.
        Then:
            It should hand back the object it cached, calling the factory
            once for that loop, and report nothing.
        """
        # Arrange
        factory = mocker.Mock(side_effect=["first", "second"])
        pool = ResourcePool(factory=factory, ttl=60)
        paused, _ = stranded_loop(_cache_idle_entry(pool, "key"), close=False)

        async def read_use_and_clear():
            pool.stats
            await _cache_idle_entry(pool, "key")
            await pool.clear()

        asyncio.run(read_use_and_clear())
        caplog.clear()

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            acquired = paused.run_until_complete(_acquire(pool, "key"))
            resumed = paused.run_until_complete(_pool_stats(pool))

        # Assert
        assert acquired == "first"
        assert factory.call_count == 2
        assert resumed.total_entries == 1
        assert _pool_records(caplog) == []

    def test_get_should_cache_in_its_own_partition_when_factory_spans_a_pause(
        self,
    ):
        """Test an acquire suspended in its factory lands in the right partition.

        Given:
            A pool whose async factory parks until released, entered on a
            loop that pauses while the factory is parked, and a fresh
            loop that uses and clears the pool meanwhile.
        When:
            The paused loop resumes and the factory completes.
        Then:
            It should cache the object in the paused loop's partition,
            where a second acquire finds it without calling the factory
            again.
        """
        # Arrange
        gate = asyncio.Event()
        built = []

        async def factory(key):
            built.append(key)
            if key == "key":
                await gate.wait()
            return f"obj-{key}"

        async def use_and_clear():
            await _cache_idle_entry(pool, "other")
            await pool.clear()

        pool = ResourcePool(factory=factory, ttl=60)
        loop = asyncio.new_event_loop()
        stack = AsyncExitStack()
        try:
            entering = loop.create_task(stack.enter_async_context(pool.get("key")))
            loop.run_until_complete(_poll_until(lambda: built == ["key"]))
            asyncio.run(use_and_clear())
            gate.set()

            # Act
            acquired = loop.run_until_complete(entering)
            again = loop.run_until_complete(_acquire(pool, "key"))
            snapshot = loop.run_until_complete(_pool_stats(pool))

            # Assert
            assert acquired == again == "obj-key"
            assert built == ["key", "other"]
            assert snapshot.total_entries == 1
            assert snapshot.referenced_entries == 1
        finally:
            loop.run_until_complete(stack.aclose())
            loop.close()

    def test_get_should_finalize_once_when_a_runner_pauses_between_runs(
        self, mocker, background_loops
    ):
        """Test the Runner idiom keeps its references across its runs.

        Given:
            A zero-TTL pool, an asyncio.Runner that enters a resource in
            one run, and a background loop that uses and clears the pool
            between that run and the next.
        When:
            A second run on the same Runner exits the resource.
        Then:
            It should finalize the object exactly once, the reference
            having survived the pause between runs.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=0)
        stack = AsyncExitStack()
        other = background_loops()

        async def use_and_clear():
            await _cache_idle_entry(pool, "key")
            await pool.clear()

        # Act
        with asyncio.Runner() as runner:
            runner.run(stack.enter_async_context(pool.get("key")))
            for _ in range(20):
                other.run(use_and_clear())
            finalizer.reset_mock()
            runner.run(stack.aclose())

        # Assert
        finalizer.assert_awaited_once_with("obj-key")

    def test_release_should_finalize_when_its_loop_resumes_after_the_ttl(
        self, mocker, stranded_loop
    ):
        """Test a TTL outlives a pause no other loop interrupts.

        Given:
            A pool whose short-TTL entry was released on a loop that
            then paused for longer than the TTL, with no other loop
            touching the pool meanwhile.
        When:
            That loop resumes.
        Then:
            It should fire the timer on resume and finalize the entry
            exactly once, leaving the partition empty.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=0.05)
        paused, _ = stranded_loop(_cache_idle_entry(pool, "key"), close=False)
        time.sleep(0.1)

        # Act
        paused.run_until_complete(
            _poll_until(lambda: finalizer.await_count == 1, timeout=2.0)
        )

        # Assert
        finalizer.assert_awaited_once_with("obj-key")
        assert paused.run_until_complete(_pool_stats(pool)).total_entries == 0

    def test_release_should_not_report_a_failure_when_its_cleanup_outlives_a_pause(
        self, caplog, stranded_loop
    ):
        """Test a finalizer parked across a pause still ends quietly.

        Given:
            A pool whose TTL cleanup task is parked inside its finalizer
            on a loop that then paused, and whose pool a fresh loop has
            since read and used.
        When:
            The paused loop resumes and the finalizer completes.
        Then:
            It should finish without reporting anything, the entry gone
            from the resumed loop's partition and the fresh loop's own
            partition untouched.
        """
        # Arrange
        parked = asyncio.Event()
        gate = asyncio.Event()
        finished = asyncio.Event()

        async def finalizer(obj):
            parked.set()
            await gate.wait()
            finished.set()

        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=0.01)
        paused, _ = stranded_loop(_cache_idle_entry(pool, "key"), close=False)
        # Drive the loop only until the cleanup task has entered the
        # finalizer, so it is parked mid-cleanup when the loop stops.
        paused.run_until_complete(asyncio.wait_for(parked.wait(), timeout=2.0))

        async def resume():
            gate.set()
            await asyncio.wait_for(finished.wait(), timeout=2.0)
            # Let the cleanup task's done callback run before the loop
            # pauses again, so a failure it reports would be seen.
            await asyncio.sleep(0.05)
            return pool.stats

        async def read_and_use():
            before = pool.stats
            await pool.expire("none")
            return before

        # Act
        with caplog.at_level(logging.DEBUG):
            fresh = asyncio.run(read_and_use())
            resumed = paused.run_until_complete(resume())
            gc.collect()

        # Assert
        assert fresh.total_entries == 0
        assert resumed.total_entries == 0
        assert _pool_records(caplog) == []
        assert not [r for r in caplog.records if "never retrieved" in r.getMessage()]

    @pytest.mark.asyncio
    async def test_pending_cleanup_should_report_an_entry_inside_its_finalizer(self):
        """Test an entry mid-finalization is still pending, not vanished.

        Given:
            A short-TTL pool whose finalizer parks on a gate, holding
            one idle entry whose TTL has fired.
        When:
            stats and pending_cleanup are read while the finalizer is
            parked.
        Then:
            It should count the entry in total_entries and report its
            cleanup task as pending, until the gate opens and the entry
            is evicted.
        """
        # Arrange
        parked = asyncio.Event()
        gate = asyncio.Event()

        async def finalizer(obj):
            parked.set()
            await gate.wait()

        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=0.01)
        await _cache_idle_entry(pool, "key")
        await asyncio.wait_for(parked.wait(), timeout=2.0)

        # Act
        during = pool.stats
        pending = pool.pending_cleanup
        gate.set()
        await _poll_until(lambda: pool.stats.total_entries == 0)

        # Assert
        assert during.total_entries == 1
        assert during.pending_cleanup == 1
        assert isinstance(pending.get("key"), asyncio.Task)

    def test_acquire_should_name_the_pool_by_type_when_the_factory_has_no_qualname(
        self, caplog, counting_factory, stranded_loop
    ):
        """Test a callable object still gives the pool's records a name.

        Given:
            A pool whose factory is a callable object rather than a
            function, holding one entry stranded on a closed loop.
        When:
            The pool is used from a fresh loop.
        Then:
            It should name the pool by its factory's type in the record
            it logs, having no qualified name to fall back on.
        """
        # Arrange
        pool = ResourcePool(factory=counting_factory, ttl=60)
        stranded_loop(_acquire(pool, "key"))

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(_acquire(pool, "other"))

        # Assert
        records = _pool_records(caplog)
        assert [r.levelno for r in records] == [logging.WARNING]
        assert "ResourcePool(CountingFactory)" in records[0].getMessage()

    @pytest.mark.asyncio
    async def test_acquire_should_leave_the_partition_empty_when_the_factory_raises(
        self, mocker
    ):
        """Test a factory failure leaves a fresh partition usable.

        Given:
            A pool on a loop with no partition yet, whose factory raises
            on its first call and succeeds on the second.
        When:
            The key is acquired, the failure propagates, and the same
            key is acquired again.
        Then:
            It should propagate the failure, leave the new partition
            empty with no pending cleanup, and serve the retry.
        """
        # Arrange
        factory = mocker.Mock(side_effect=[RuntimeError("factory failed"), "obj"])
        pool = ResourcePool(factory=factory, ttl=60)

        # Act
        with pytest.raises(RuntimeError, match="factory failed"):
            await _acquire(pool, "key")

        # Assert
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup
        assert await _acquire(pool, "key") == "obj"

    @pytest.mark.parametrize(
        "operation",
        [
            pytest.param(lambda pool: pool.expire("fresh"), id="expire"),
            pytest.param(lambda pool: pool.expire_all(), id="expire_all"),
            pytest.param(lambda pool: pool.clear(), id="clear"),
            pytest.param(lambda pool: _use_resource(pool.get("fresh")), id="get"),
        ],
    )
    def test_entry_point_should_warn_once_when_a_partition_was_stranded(
        self, caplog, stranded_loop, operation
    ):
        """Test every mutating entry point sweeps, and reports, once.

        Given:
            A pool holding one referenced entry stranded on a closed
            loop.
        When:
            Any one of the pool's mutating operations runs on a fresh
            loop.
        Then:
            It should log exactly one WARNING for the stranded
            partition, whichever operation reached the pool first.
        """
        # Arrange
        pool = ResourcePool(factory=make_resource, ttl=60)
        stranded_loop(_acquire(pool, "key"))

        async def touch():
            await operation(pool)

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(touch())

        # Assert
        records = _pool_records(caplog)
        assert [r.levelno for r in records] == [logging.WARNING]
        assert "1 referenced and 0 idle" in records[0].getMessage()

    @pytest.mark.parametrize(
        "read",
        [
            pytest.param(lambda pool: pool.stats, id="stats"),
            pytest.param(lambda pool: pool.pending_cleanup, id="pending_cleanup"),
        ],
    )
    def test_stats_should_not_sweep_a_stranded_partition(
        self, caplog, stranded_loop, read
    ):
        """Test a read leaves a stranded partition for a mutating access.

        Given:
            A pool holding one referenced entry stranded on a closed
            loop.
        When:
            stats or pending_cleanup is read on a fresh loop, and the
            pool is then expired on that loop.
        Then:
            It should log nothing for the read and report the stranded
            partition once, from the expire that follows it.
        """
        # Arrange
        pool = ResourcePool(factory=make_resource, ttl=60)
        stranded_loop(_acquire(pool, "key"))

        async def read_then_expire():
            read(pool)
            after_read = list(_pool_records(caplog))
            await pool.expire("none")
            return after_read

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            after_read = asyncio.run(read_then_expire())

        # Assert
        assert after_read == []
        records = _pool_records(caplog)
        assert [r.levelno for r in records] == [logging.WARNING]
        assert "1 referenced and 0 idle" in records[0].getMessage()

    def test_stats_should_not_pin_the_reading_loop(self):
        """Test a read registers nothing for the loop that made it.

        Given:
            A pool an event loop only ever read stats from, then closed
            and dropped.
        When:
            A collection runs, with no further pool access.
        Then:
            It should have kept no reference to that loop, so the weakly
            referenced loop is collected.
        """
        # Arrange
        pool = ResourcePool(factory=make_resource, ttl=60)
        loop = asyncio.new_event_loop()
        reference = weakref.ref(loop)
        loop.run_until_complete(_pool_stats(pool))
        loop.close()

        # Act
        del loop
        gc.collect()

        # Assert
        assert reference() is None

    @given(referenced=strategies.integers(0, 4), idle=strategies.integers(0, 4))
    @settings(
        max_examples=25,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_acquire_should_report_the_stranded_counts_for_any_mix(
        self, caplog, stranded_loop, referenced, idle
    ):
        """Test the sweep's record counts whatever the loop stranded.

        Given:
            Any zero to four referenced and zero to four idle entries
            left on a loop that has closed.
        When:
            A fresh loop touches the pool.
        Then:
            It should stay silent when the partition is empty and
            otherwise log exactly one WARNING whose message reports the
            two counts against the pool's factory name.
        """
        # Arrange
        pool = ResourcePool(factory=make_resource, ttl=60)

        async def strand():
            # Clear first, so the partition exists even when the loop
            # strands nothing in it.
            await pool.clear()
            for index in range(referenced):
                await _acquire(pool, f"referenced-{index}")
            for index in range(idle):
                await _cache_idle_entry(pool, f"idle-{index}")

        stranded_loop(strand())
        caplog.clear()

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(_acquire(pool, "fresh"))

        # Assert
        records = _pool_records(caplog)
        if referenced == 0 and idle == 0:
            assert records == []
        else:
            assert [r.levelno for r in records] == [logging.WARNING]
            assert records[0].getMessage() == (
                f"ResourcePool(make_resource) dropping {referenced} referenced "
                f"and {idle} idle entries stranded by an event loop that "
                "closed without clearing its partition (finalizers not run)"
            )

    @given(
        operations=strategies.lists(
            strategies.tuples(
                strategies.integers(0, 1),
                strategies.sampled_from(["acquire", "release", "expire"]),
                strategies.sampled_from(["a", "b", "c"]),
            ),
            max_size=12,
        )
    )
    @settings(
        max_examples=15,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_release_should_keep_each_loops_bookkeeping_independent(
        self, background_loops, operations
    ):
        """Test two loops' books never leak into one another.

        Given:
            Any interleaving of acquire, release and expire over three
            shared keys, each step tagged to one of two live loops,
            where releases are applied only while that loop holds a
            reference.
        When:
            The sequence is applied step by step to a long-TTL pool.
        Then:
            It should keep each loop's counters, pending keys and
            finalized objects equal to an independent model of that
            loop alone, after every step.
        """
        # Arrange
        handles = [background_loops() for _ in range(2)]
        index_by_loop = {handle.loop: index for index, handle in enumerate(handles)}
        finalized = ([], [])
        finalized_lock = threading.Lock()

        def finalizer(obj):
            with finalized_lock:
                finalized[index_by_loop[asyncio.get_running_loop()]].append(obj)

        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        models = [{"references": {}, "doomed": set(), "finalized": []} for _ in range(2)]

        async def step(operation, key):
            if operation == "acquire":
                await _acquire(pool, key)
            elif operation == "expire":
                await pool.expire(key)
            else:
                await _release(pool, key)
            return pool.stats, sorted(pool.pending_cleanup)

        # Act & assert
        try:
            for index, operation, key in operations:
                model = models[index]
                references = model["references"]
                if operation == "acquire":
                    references[key] = references.get(key, 0) + 1
                    model["doomed"].discard(key)
                elif operation == "expire":
                    if key in references:
                        if references[key] > 0:
                            model["doomed"].add(key)
                        else:
                            del references[key]
                            model["finalized"].append(make_resource(key))
                elif references.get(key, 0) > 0:
                    references[key] -= 1
                    if references[key] == 0 and key in model["doomed"]:
                        model["doomed"].discard(key)
                        del references[key]
                        model["finalized"].append(make_resource(key))
                else:
                    # The pool raises on a release with no reference;
                    # the model only ever drives a legal sequence.
                    continue

                stats, pending = handles[index].run(step(operation, key))
                other = handles[1 - index].run(_pool_stats(pool))
                other_references = models[1 - index]["references"]
                assert stats.total_entries == len(references)
                assert stats.referenced_entries == sum(
                    1 for count in references.values() if count > 0
                )
                assert pending == sorted(
                    key for key, count in references.items() if count == 0
                )
                assert other.total_entries == len(other_references)
                assert finalized[index] == model["finalized"]
                assert finalized[1 - index] == models[1 - index]["finalized"]
        finally:
            # Retire each example's loops eagerly; the fixture would
            # otherwise hold every loop of every example open.
            for handle in handles:
                handle.close()

    @given(mix=_loop_mixes())
    @settings(
        max_examples=15,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_acquire_should_sweep_only_the_stopped_loops_for_any_mix(
        self, caplog, background_loops, mix
    ):
        """Test the sweep is selective however the loops are mixed.

        Given:
            Any one to four loops sharing a pool, each holding one
            referenced entry, of which any number but at least one is
            still running.
        When:
            A running loop that holds entries of its own acquires
            another key.
        Then:
            It should log exactly one WARNING per stopped loop and leave
            every running loop's entries, the sweeping loop's included,
            cached.
        """
        # Arrange
        running_count, stopped_count = mix
        pool = ResourcePool(factory=make_resource, ttl=60)
        running = [background_loops() for _ in range(running_count)]
        stopped = [background_loops() for _ in range(stopped_count)]
        try:
            for index, handle in enumerate(running + stopped):
                handle.run(_acquire(pool, f"key-{index}"))
            for handle in stopped:
                handle.close()
            caplog.clear()

            # Act
            with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
                running[0].run(_acquire(pool, "swept-by"))

            # Assert
            records = _pool_records(caplog)
            assert [r.levelno for r in records] == [logging.WARNING] * stopped_count
            assert all("1 referenced and 0 idle" in r.getMessage() for r in records)
            assert running[0].run(_pool_stats(pool)).total_entries == 2
            for handle in running[1:]:
                assert handle.run(_pool_stats(pool)).total_entries == 1
        finally:
            # Retire each example's loops eagerly; the fixture would
            # otherwise hold every loop of every example open.
            for handle in running + stopped:
                handle.close()

    def test_acquire_should_report_only_the_pool_it_is_called_on(
        self, caplog, stranded_loop
    ):
        """Test one pool's sweep never speaks for another's.

        Given:
            Two pools that each cached an entry on the same loop, which
            has since closed.
        When:
            A fresh loop touches the first pool, and later the second.
        Then:
            It should report the first pool's stranded entry alone on
            the first touch, the second pool's registry being reached
            only when that pool is used.
        """

        # Arrange
        def make_second(key):
            return "obj"

        first = ResourcePool(factory=make_resource, ttl=60)
        second = ResourcePool(factory=make_second, ttl=60)

        async def strand():
            await _acquire(first, "key")
            await _acquire(second, "key")

        stranded_loop(strand())

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(_acquire(first, "other"))
            first_records = _pool_records(caplog)
            caplog.clear()
            asyncio.run(_acquire(second, "other"))
            second_records = _pool_records(caplog)

        # Assert
        assert [r.levelno for r in first_records] == [logging.WARNING]
        assert "ResourcePool(make_resource)" in first_records[0].getMessage()
        assert [r.levelno for r in second_records] == [logging.WARNING]
        # A nested function's qualified name carries its enclosing scopes.
        assert ".make_second)" in second_records[0].getMessage()

    def test_clear_should_reach_only_this_loops_entries_of_a_finalizers_pool(
        self, background_loops
    ):
        """Test a finalizer reaching another pool stays on its own loop.

        Given:
            A pool whose finalizer retires a second pool, where that
            second pool holds one entry on the clearing loop and one on
            another loop still running.
        When:
            The first pool is cleared on the first loop.
        Then:
            It should finalize the second pool's entry on that loop and
            leave the other loop's entry cached, a finalizer reaching
            only as far as the loop it runs on.
        """
        # Arrange
        finalized = []
        finalized_lock = threading.Lock()

        def record(obj):
            with finalized_lock:
                finalized.append(obj)

        second = ResourcePool(factory=make_resource, finalizer=record, ttl=60)

        async def finalizer(obj):
            await second.expire_all()

        first = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        live = background_loops()
        live.run(_cache_idle_entry(second, "live"))

        async def clear_first():
            await _acquire(first, "key")
            await _cache_idle_entry(second, "own")
            await first.clear()
            return second.stats.total_entries

        # Act
        remaining = asyncio.run(clear_first())

        # Assert
        live_snapshot = live.run(_pool_stats(second))
        assert remaining == 0
        assert finalized == ["obj-own"]
        assert live_snapshot.total_entries == 1

    def test_acquire_should_drop_the_loop_reference_when_it_sweeps_a_partition(self):
        """Test a swept partition does not pin its loop forever.

        Given:
            A pool holding an entry stranded on a closed loop the
            registry is the last thing to reference.
        When:
            A fresh loop touches the pool and a collection runs.
        Then:
            It should have dropped the registry's reference, so the
            weakly referenced loop is collected.
        """
        # Arrange
        pool = ResourcePool(factory=make_resource, ttl=60)
        loop = asyncio.new_event_loop()
        loop.run_until_complete(_cache_idle_entry(pool, "key"))
        loop.close()
        reference = weakref.ref(loop)
        del loop
        gc.collect()
        # Guard: the registry is what is keeping the loop alive.
        assert reference() is not None

        # Act
        asyncio.run(_acquire(pool, "other"))
        gc.collect()

        # Assert
        assert reference() is None

    def test_release_should_leave_no_pending_task_when_loop_closes_before_ttl(
        self, mocker
    ):
        """Test release defers cleanup without parking a task on the loop.

        Given:
            A pool with a positive TTL whose resource is released on
            a dedicated event loop.
        When:
            The loop is closed and garbage-collected before the TTL
            elapses.
        Then:
            It should leave no pending task on the loop and emit no
            RuntimeWarning when the deferred cleanup is collected.
        """
        # Arrange
        loop = asyncio.new_event_loop()

        def create_release_and_drop():
            pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)

            async def acquire_release():
                async with pool.get("key"):
                    pass

            loop.run_until_complete(acquire_release())

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            create_release_and_drop()
            pending = asyncio.all_tasks(loop)
            loop.close()
            gc.collect()

        # Assert
        assert pending == set()
        assert not [w for w in caught if issubclass(w.category, RuntimeWarning)]

    @pytest.mark.asyncio
    async def test_acquire_should_cancel_in_flight_cleanup_when_reacquired_after_expiry(
        self, expiry_race_pool
    ):
        """Test acquire cancels a fired cleanup racing on the pool lock.

        Given:
            A pool whose expired key's TTL timer has fired while the
            pool lock is held by another key's acquire, so the
            spawned cleanup task and a queued re-acquire of the
            expired key both wait on the lock with the re-acquire
            first
        When:
            The lock holder completes and the queued re-acquire runs
        Then:
            It should cancel the in-flight cleanup, return the cached
            object without re-invoking the factory or finalizer, and
            leave the key without pending cleanup
        """
        # Arrange
        pool, finalizer, factory_calls, release_blocker = expiry_race_pool
        blocker_task, reacquire_task = await _queue_behind_fired_cleanup(
            pool, factory_calls, _acquire(pool, "expired")
        )

        # Act
        release_blocker.set()
        acquired = await reacquire_task
        await blocker_task

        # Assert
        assert acquired == "obj-expired"
        assert factory_calls.count("expired") == 1
        finalizer.assert_not_awaited()
        assert "expired" not in pool.pending_cleanup
        assert pool.stats.total_entries == 2

    @pytest.mark.asyncio
    async def test_acquire_should_report_nothing_when_it_cancels_a_fired_cleanup(
        self, caplog, mocker
    ):
        """Test a cleanup cancelled before it ran is not a failure.

        Given:
            A short-TTL pool holding one idle entry, and a caller
            spinning on the ready queue so that it re-acquires the key
            in the loop step after the fired timer spawned the cleanup
            task and before that task takes its first step.
        When:
            The re-acquire cancels the cleanup task and its done
            callback runs.
        Then:
            It should log nothing at all, a cleanup cancelled by a
            re-acquire being an expected outcome rather than a failure
            to report.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=0.01)
        await _cache_idle_entry(pool, "key")

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            # Spinning keeps this task at the head of the ready queue,
            # ahead of the cleanup task the timer callback appends
            # behind it.
            deadline = time.monotonic() + 2.0
            while not isinstance(pool.pending_cleanup.get("key"), asyncio.Task):
                assert time.monotonic() < deadline, "the TTL timer never fired"
                await asyncio.sleep(0)
            acquired = await _acquire(pool, "key")
            # The cancelled task's done callback lands a tick later.
            await asyncio.sleep(0.05)

        # Assert
        assert acquired == "obj-key"
        finalizer.assert_not_awaited()
        assert _pool_records(caplog) == []
        assert "key" not in pool.pending_cleanup

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "retire",
        [
            pytest.param(lambda pool: pool.expire("expired"), id="expire"),
            pytest.param(lambda pool: pool.expire_all(), id="expire_all"),
        ],
    )
    async def test_retirement_should_cancel_in_flight_cleanup_when_expired_after_ttl(
        self, expiry_race_pool, retire
    ):
        """Test retirement cancels a fired cleanup racing on the pool lock.

        Given:
            A pool whose expired key's TTL timer has fired while the
            pool lock is held by another key's acquire, so the spawned
            cleanup task and a queued retirement of that key both wait
            on the lock with the retirement first.
        When:
            The lock holder completes and the queued retirement runs.
        Then:
            It should cancel the in-flight cleanup, still run the
            finalizer exactly once, and evict the entry.
        """
        # Arrange
        pool, finalizer, factory_calls, release_blocker = expiry_race_pool
        blocker_task, retire_task = await _queue_behind_fired_cleanup(
            pool, factory_calls, retire(pool)
        )

        # Act
        release_blocker.set()
        await retire_task
        await blocker_task

        # Assert
        finalizer.assert_awaited_once_with("obj-expired")
        assert "expired" not in pool.pending_cleanup
        assert pool.stats.total_entries == 1

    @pytest.mark.asyncio
    @settings(max_examples=50, deadline=None)
    @given(
        operations=strategies.lists(
            strategies.tuples(
                strategies.sampled_from(["acquire", "release", "expire"]),
                strategies.sampled_from(["a", "b", "c"]),
            ),
            max_size=30,
        )
    )
    async def test_release_should_maintain_bookkeeping_invariants(self, operations):
        """Test acquire, release and expire keep bookkeeping consistent.

        Given:
            Any interleaved sequence of acquire, release and expire
            operations over a small key domain, where releases are
            applied only while a reference is held
        When:
            The sequence is applied step by step to a long-TTL pool
        Then:
            It should evict a retired key the instant the release that
            drops its last reference returns, keeping total entries,
            referenced entries, pending cleanup and the finalized
            objects equal to the model's at every step
        """
        # Arrange
        finalized = []
        pool = ResourcePool(
            factory=lambda key: f"obj-{key}",
            finalizer=finalized.append,
            ttl=60,
        )
        model_refcount = {}
        model_doomed = set()
        model_finalized = []

        # Act & assert
        for operation, key in operations:
            if operation == "acquire":
                await _acquire(pool, key)
                model_refcount[key] = model_refcount.get(key, 0) + 1
                model_doomed.discard(key)
            elif operation == "expire":
                if key in model_refcount:
                    if model_refcount[key] > 0:
                        model_doomed.add(key)
                    else:
                        del model_refcount[key]
                        model_finalized.append(f"obj-{key}")
                await pool.expire(key)
            elif model_refcount.get(key, 0) > 0:
                await _release(pool, key)
                model_refcount[key] -= 1
                if model_refcount[key] == 0 and key in model_doomed:
                    model_doomed.discard(key)
                    del model_refcount[key]
                    model_finalized.append(f"obj-{key}")

            stats = pool.stats
            assert stats.total_entries == len(model_refcount)
            assert stats.referenced_entries == sum(
                1 for count in model_refcount.values() if count > 0
            )
            assert set(pool.pending_cleanup) == {
                key for key, count in model_refcount.items() if count == 0
            }
            assert finalized == model_finalized

    @pytest.mark.asyncio
    async def test_clear_should_finalize_all_resources(self):
        """Test clearing the pool calls finalizer on all resources.

        Given:
            A pool with resources
        When:
            Clear is called without specific key
        Then:
            All resources should be finalized and cache cleared
        """
        # Arrange - Create pool with TTL to keep resources after context exit
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)

        # Create some resources
        test_resources = []
        for i in range(3):
            mock_resource = Mock(name=f"resource-{i}")
            test_resources.append(mock_resource)
            mock_factory.return_value = mock_resource
            async with pool.get(f"key-{i}"):
                pass  # Creates and caches the resource

        # Verify initial state
        assert pool.stats.total_entries == 3

        # Act
        await pool.clear()

        # Assert
        # All resources should be cleaned up and cache cleared
        assert pool.stats.total_entries == 0

        # Finalizer should have been called for all resources
        assert mock_finalizer.call_count == 3

    @pytest.mark.asyncio
    async def test_expire_should_leave_other_entries_when_key_expired(self):
        """Test expiring one key retires only that key.

        Given:
            A pool holding two unreferenced entries under a long TTL.
        When:
            One of them is expired.
        Then:
            It should finalize that entry alone, leaving the other cached
            and its resource untouched.
        """
        # Arrange
        finalized = []

        async def factory(key):
            return f"obj-{key}"

        async def finalizer(resource):
            finalized.append(resource)

        pool = ResourcePool(factory, finalizer=finalizer, ttl=3600)
        async with pool:
            await _acquire(pool, "key1")
            await _acquire(pool, "key2")
            await _release(pool, "key1")
            await _release(pool, "key2")
            assert pool.stats.total_entries == 2

            # Act
            await pool.expire("key1")

            # Assert
            assert finalized == ["obj-key1"]
            assert pool.stats.total_entries == 1
            assert "key2" in pool.pending_cleanup

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "retire",
        [
            pytest.param(lambda pool: pool.expire("idle"), id="expire"),
            pytest.param(lambda pool: pool.expire_all(), id="expire_all"),
        ],
    )
    async def test_retirement_should_finalize_only_the_idle_entry_when_one_is_held(
        self, mocker, retire
    ):
        """Test an idle entry is finalized at once and a referenced one left alone.

        Given:
            A long-TTL pool holding one idle entry awaiting its TTL and
            one still referenced.
        When:
            The idle key is expired, or expire_all() is awaited.
        Then:
            It should finalize the idle entry immediately, skipping its
            TTL, and leave the referenced one cached with no pending
            cleanup.
        """
        # Arrange
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=lambda key: key, finalizer=mock_finalizer, ttl=60)
        async with pool.get("idle"):
            pass
        await _acquire(pool, "held")

        # Act
        await retire(pool)

        # Assert
        mock_finalizer.assert_awaited_once_with("idle")
        assert pool.stats.total_entries == 1
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_should_not_finalize_while_referenced(self):
        """Test expiring a referenced entry leaves the in-flight user alone.

        Given:
            A long-TTL pool holding an entry with an active reference.
        When:
            expire() is called.
        Then:
            It should leave the resource unfinalized and the entry cached —
            an in-flight user is never torn out from under.
        """
        # Arrange
        mock_factory = Mock(return_value="resource")
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)
        await _acquire(pool, "key")

        # Act
        await pool.expire("key")

        # Assert
        mock_finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "retire",
        [
            pytest.param(lambda pool: pool.expire("key"), id="expire"),
            pytest.param(lambda pool: pool.expire_all(), id="expire_all"),
        ],
    )
    async def test_release_should_finalize_retired_entry_when_last_reference_released(
        self, retired_entry_pool, retire
    ):
        """Test a retired entry is finalized by the release that drains it.

        Given:
            A long-TTL pool whose only entry is referenced and has been
            retired.
        When:
            The last reference is released.
        Then:
            It should finalize the entry then, ending with an empty pool
            and no pending cleanup.
        """
        # Arrange
        pool, finalizer, _ = retired_entry_pool
        await _acquire(pool, "key")
        await retire(pool)

        # Act
        await _release(pool, "key")

        # Assert
        finalizer.assert_awaited_once_with("first")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_release_should_not_finalize_retired_entry_when_references_remain(
        self, retired_entry_pool
    ):
        """Test a retired entry survives a release that does not drain it.

        Given:
            A long-TTL pool holding an entry with two live references
            that has been retired by ``expire`` while referenced.
        When:
            Only one of the two references is released.
        Then:
            It should leave the resource unfinalized and the entry
            cached and still referenced, with no cleanup pending.
        """
        # Arrange
        pool, finalizer, _ = retired_entry_pool
        await _acquire(pool, "key")
        await _acquire(pool, "key")
        await pool.expire("key")

        # Act
        await _release(pool, "key")

        # Assert
        finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 1
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_release_should_finish_finalizing_when_cancelled_mid_finalizer(
        self, mocker
    ):
        """Test cancelling a release does not interrupt the finalizer it runs.

        Given:
            A long-TTL pool holding an entry retired by ``expire`` while
            still referenced, whose finalizer parks on a gate so the
            release is suspended inside it.
        When:
            The releasing task is cancelled while the finalizer is
            parked, and the gate then opens.
        Then:
            It should raise ``CancelledError`` to the releaser, let the
            finalizer complete on its own, and evict the entry, so a
            later acquire builds a fresh object.
        """
        # Arrange
        parked = asyncio.Event()
        gate = asyncio.Event()
        factory = mocker.Mock(side_effect=["first", "second"])

        async def finalizer(_):
            parked.set()
            await gate.wait()

        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)
        await _acquire(pool, "key")
        await pool.expire("key")
        release = asyncio.ensure_future(_release(pool, "key"))
        # Bounded: a regression that never enters the finalizer must
        # fail here rather than idle out the pool's own TTL.
        await asyncio.wait_for(parked.wait(), timeout=2.0)

        # Act
        release.cancel()
        with pytest.raises(asyncio.CancelledError):
            await release
        held = pool.stats.total_entries
        gate.set()
        await _poll_until(lambda: pool.stats.total_entries == 0)

        # Assert
        assert held == 1
        assert await _acquire(pool, "key") == "second"

    @pytest.mark.asyncio
    async def test_release_should_drop_reference_when_cancelled_waiting_on_the_lock(
        self,
    ):
        """Test a release cancelled while contending the lock still releases.

        Given:
            A pool of two entries whose sweep is parked inside the idle
            first entry's finalizer, holding the lock, while the second
            is still referenced.
        When:
            A release of the second entry, parked behind that lock, is
            cancelled, and the sweep then completes.
        Then:
            It should raise CancelledError to the releasing task yet still
            drop the reference, so the retired second entry is finalized
            rather than held forever.
        """
        # Arrange
        parked = asyncio.Event()
        gate = asyncio.Event()
        done = asyncio.Event()
        finalized = []

        async def finalizer(obj):
            finalized.append(obj)
            if obj == "obj-a":
                parked.set()
                await gate.wait()
            else:
                done.set()

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}", finalizer=finalizer, ttl=60
        )
        await _acquire(pool, "a")
        await _acquire(pool, "b")
        await _release(pool, "a")
        sweep = asyncio.ensure_future(pool.expire_all())
        await asyncio.wait_for(parked.wait(), timeout=2.0)
        release = asyncio.ensure_future(_release(pool, "b"))
        # The lock is held by the parked finalizer for the whole window,
        # so any number of ticks past the shield's entry leaves the
        # release waiting on it.
        for _ in range(5):
            await asyncio.sleep(0)

        # Act
        release.cancel()
        with pytest.raises(asyncio.CancelledError):
            await release
        gate.set()
        await sweep

        # Assert
        await asyncio.wait_for(done.wait(), timeout=2.0)
        assert finalized == ["obj-a", "obj-b"]
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_release_should_drop_reference_when_cancelled_behind_a_woken_waiter(
        self,
    ):
        """Test a release cancelled in the woken-waiter window still releases.

        Given:
            A pool whose sweep, parked inside an idle first entry's
            finalizer, holds the lock with a second acquire queued
            behind it, while a third entry is still referenced.
        When:
            The sweep is released and, in the same tick, a release of
            the third entry starts, so it runs after the lock is freed
            but before the queued acquire has taken it, and that release
            is then cancelled.
        Then:
            It should raise CancelledError to the releasing task yet still
            drop the reference, so the retired third entry is finalized
            rather than held forever.
        """
        # Arrange
        parked = asyncio.Event()
        gate = asyncio.Event()
        finalized = []

        async def finalizer(obj):
            finalized.append(obj)
            if obj == "obj-a":
                parked.set()
                await gate.wait()

        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        await _cache_idle_entry(pool, "a")
        await _acquire(pool, "b")
        sweep = asyncio.ensure_future(pool.expire_all())
        await asyncio.wait_for(parked.wait(), timeout=2.0)
        waiter = asyncio.ensure_future(_acquire(pool, "c"))
        for _ in range(3):
            await asyncio.sleep(0)

        # Act -- no suspension between the two, so the release is queued
        # behind the sweep's resumption and ahead of the waiter's.
        gate.set()
        release = asyncio.ensure_future(_release(pool, "b"))
        for _ in range(2):
            await asyncio.sleep(0)
        release.cancel()
        with pytest.raises(asyncio.CancelledError):
            await release
        await sweep
        await waiter

        # Assert
        await _poll_until(lambda: "obj-b" in finalized)
        assert finalized == ["obj-a", "obj-b"]
        await _release(pool, "c")

    def test_release_should_finalize_retired_entry_when_loop_ends_immediately(
        self, mocker, caplog
    ):
        """Test a release during shutdown closes the resource before returning.

        Given:
            A long-TTL pool holding an entry retired by ``expire`` while
            still referenced, on a loop that closes as soon as the last
            reference is released.
        When:
            That release is awaited and the loop is closed with no
            further iterations.
        Then:
            It should have run the finalizer to completion before
            returning, leaving no pending task on the loop and no
            destroyed-while-pending report from asyncio.
        """
        # Arrange
        closed = []

        # The suspension point is load-bearing: a finalizer that never
        # awaits would finish inside a single loop step, so this test
        # could not tell an inline finalize from deferred work the loop
        # happens to run before closing.
        async def finalizer(obj):
            await asyncio.sleep(0)
            closed.append(obj)

        pool = ResourcePool(
            factory=mocker.Mock(return_value="obj"), finalizer=finalizer, ttl=60
        )
        loop = asyncio.new_event_loop()

        async def acquire_and_retire():
            await _acquire(pool, "key")
            await pool.expire("key")

        loop.run_until_complete(acquire_and_retire())

        async def stats():
            return pool.stats

        # Act
        with caplog.at_level(logging.ERROR, logger="asyncio"):
            loop.run_until_complete(_release(pool, "key"))
            pending = asyncio.all_tasks(loop)
            entries = loop.run_until_complete(stats()).total_entries
            loop.close()
            gc.collect()

        # Assert
        assert closed == ["obj"]
        assert pending == set()
        assert entries == 0
        assert not [
            record
            for record in caplog.records
            if "Task was destroyed" in record.getMessage()
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "retire",
        [
            pytest.param(lambda pool: pool.expire("key"), id="expire"),
            pytest.param(lambda pool: pool.expire_all(), id="expire_all"),
        ],
    )
    async def test_acquire_should_clear_retirement_when_entry_reacquired(
        self, mocker, retire
    ):
        """Test re-acquiring a retired entry clears its retirement.

        Given:
            A long-TTL pool whose only entry is referenced and has been
            retired.
        When:
            The same key is acquired again and both references are
            released.
        Then:
            It should hand back the cached object without rebuilding it
            and keep it cached under its normal TTL schedule, the
            retirement cleared.
        """
        # Arrange
        mock_factory = mocker.Mock(return_value="resource")
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)
        await _acquire(pool, "key")
        await retire(pool)

        # Act
        acquired = await _acquire(pool, "key")
        await _release(pool, "key")
        await _release(pool, "key")

        # Assert
        assert acquired == "resource"
        assert mock_factory.call_count == 1
        mock_finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 1
        assert "key" in pool.pending_cleanup

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "retire",
        [
            pytest.param(lambda pool: pool.expire("missing"), id="expire"),
            pytest.param(lambda pool: pool.expire_all(), id="expire_all"),
        ],
    )
    async def test_retirement_should_not_raise_when_nothing_cached(self, mocker, retire):
        """Test retiring what is not cached is a no-op.

        Given:
            A pool that has never cached anything.
        When:
            expire() is awaited for an unknown key, or expire_all() on
            the empty pool.
        Then:
            It should return without raising, invoke no finalizer, and
            leave the pool empty with no pending cleanup.
        """
        # Arrange
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(
            factory=mocker.Mock(return_value="resource"),
            finalizer=mock_finalizer,
            ttl=60,
        )

        # Act
        await retire(pool)

        # Assert
        mock_finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_finalize_retired_entry_when_release_is_late(
        self, mocker
    ):
        """Test a late release finalizes the entry it was taken against.

        Given:
            A pool whose only entry is referenced and has been retired
            by expire_all().
        When:
            The outstanding reference is released and the same key is
            acquired again afterwards.
        Then:
            It should finalize the retired object exactly once on that
            release and build a fresh object for the new acquire, so a
            release landing after retirement can never finalize a
            resource handed out since.
        """
        # Arrange
        mock_factory = mocker.Mock(side_effect=["first", "second"])
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)
        await _acquire(pool, "key")
        await pool.expire_all()

        # Act
        await _release(pool, "key")
        reacquired = await _acquire(pool, "key")

        # Assert
        mock_finalizer.assert_awaited_once_with("first")
        assert mock_factory.call_count == 2
        assert reacquired == "second"
        assert pool.stats.referenced_entries == 1

    @pytest.mark.asyncio
    @given(reference_counts=strategies.lists(strategies.integers(0, 3), max_size=5))
    @settings(max_examples=25, deadline=None)
    async def test_expire_all_should_finalize_every_entry_exactly_once(
        self, reference_counts
    ):
        """Test retirement drains a whole pool without double finalizing.

        Given:
            Any pool of up to five distinct keys whose entries carry
            reference counts between zero and three.
        When:
            expire_all() is awaited and every outstanding reference is
            then released.
        Then:
            It should finalize each cached object exactly once and end
            with an empty pool holding no pending cleanup.
        """
        # Arrange
        finalized = []

        async def finalizer(obj):
            finalized.append(obj)

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}", finalizer=finalizer, ttl=60
        )
        keys = [f"key-{index}" for index in range(len(reference_counts))]
        for key, count in zip(keys, reference_counts):
            # One seeding reference caches the entry; the extra
            # acquires and the single release leave `count` behind.
            await _acquire(pool, key)
            for _ in range(count):
                await _acquire(pool, key)
            await _release(pool, key)

        # Act
        await pool.expire_all()
        for key, count in zip(keys, reference_counts):
            for _ in range(count):
                await _release(pool, key)

        # Assert
        assert sorted(finalized) == sorted(f"obj-{key}" for key in keys)
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_all_should_finalize_idle_entry_when_ttl_zero(self, mocker):
        """Test retirement holds for a pool with no idle grace at all.

        Given:
            A pool with no TTL holding one idle entry and one still
            referenced.
        When:
            expire_all() is awaited and the outstanding reference is
            then released.
        Then:
            It should finalize both without ever scheduling pending
            cleanup, since a zero TTL leaves nothing to defer to.
        """
        # Arrange
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=lambda key: key, finalizer=mock_finalizer, ttl=0)
        async with pool.get("idle"):
            pass
        await _acquire(pool, "held")

        # Act
        await pool.expire_all()
        await _release(pool, "held")

        # Assert
        assert sorted(c.args[0] for c in mock_finalizer.await_args_list) == [
            "held",
            "idle",
        ]
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("failures", "expected", "cause", "discarded"),
        [
            pytest.param(
                {"obj-b": RuntimeError("teardown failed")},
                None,
                None,
                0,
                id="exception-contained",
            ),
            pytest.param(
                {"obj-a": asyncio.CancelledError()},
                "obj-a",
                None,
                0,
                id="expire_all-cancelled",
            ),
            pytest.param(
                {"obj-a": asyncio.CancelledError(), "obj-b": asyncio.CancelledError()},
                "obj-a",
                None,
                1,
                id="first-cancellation-wins",
            ),
            pytest.param(
                {"obj-a": asyncio.CancelledError(), "obj-b": KeyboardInterrupt()},
                "obj-b",
                "obj-a",
                0,
                id="signal-outranks-cancellation",
            ),
            pytest.param(
                {"obj-a": SystemExit(), "obj-b": KeyboardInterrupt()},
                "obj-a",
                None,
                1,
                id="two-signals",
            ),
        ],
    )
    async def test_expire_all_should_finish_sweep_when_finalizer_fails(
        self, failures, expected, cause, discarded, caplog
    ):
        """Test a failing finalizer does not spare the entries after it.

        Given:
            A long-TTL pool of three idle entries whose finalizer raises
            the given failures: a contained Exception, a cancellation,
            two distinct cancellations, a cancellation followed by a
            process-level signal, or two signals.
        When:
            expire_all() is awaited.
        Then:
            It should attempt every finalizer, leave the pool empty with
            no pending cleanup, and propagate the expected failure
            afterwards: nothing for a contained Exception, the first
            cancellation otherwise, unless a signal arrived later, which
            chains the cancellation it superseded as its cause; a failure
            it neither re-raises nor chains is logged at warning level.
        """
        # Arrange
        attempted = []

        async def finalizer(obj):
            attempted.append(obj)
            if obj in failures:
                raise failures[obj]

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}", finalizer=finalizer, ttl=60
        )
        for key in ("a", "b", "c"):
            async with pool.get(key):
                pass
        expectation = (
            pytest.raises(type(failures[expected]))
            if expected is not None
            else contextlib.nullcontext()
        )

        # Act
        with (
            caplog.at_level(logging.WARNING, logger="wool.runtime.resourcepool"),
            expectation as raised,
        ):
            await pool.expire_all()

        # Assert
        if expected is not None:
            assert raised is not None and raised.value is failures[expected]
            assert raised.value.__cause__ is (failures[cause] if cause else None)
        assert attempted == ["obj-a", "obj-b", "obj-c"]
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup
        assert (
            sum("discarding" in record.getMessage() for record in caplog.records)
            == discarded
        )

    @pytest.mark.asyncio
    async def test_clear_should_finish_sweep_when_finalizer_cancelled(self, caplog):
        """Test a cancelled finalizer does not spare the entries after it under clear.

        Given:
            A long-TTL pool of three idle entries whose first finalizer
            raises a cancellation.
        When:
            clear() is awaited.
        Then:
            It should attempt every finalizer, leave the pool empty with
            no pending cleanup, and re-raise the cancellation afterwards
            without discarding anything.
        """
        # Arrange
        attempted = []

        async def finalizer(obj):
            attempted.append(obj)
            if obj == "obj-a":
                raise asyncio.CancelledError()

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}", finalizer=finalizer, ttl=60
        )
        for key in ("a", "b", "c"):
            async with pool.get(key):
                pass

        # Act
        with (
            caplog.at_level(logging.WARNING, logger="wool.runtime.resourcepool"),
            pytest.raises(asyncio.CancelledError),
        ):
            await pool.clear()

        # Assert
        assert attempted == ["obj-a", "obj-b", "obj-c"]
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup
        assert not [r for r in caplog.records if "discarding" in r.getMessage()]

    @pytest.mark.asyncio
    @given(
        outcomes=strategies.lists(
            strategies.sampled_from(["clean", "error", "cancel", "interrupt", "exit"]),
            min_size=1,
            max_size=5,
        )
    )
    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    async def test_expire_all_should_rank_failures_for_any_sequence_of_outcomes(
        self, outcomes, caplog
    ):
        """Test the sweep's failure ranking holds for any run of finalizer outcomes.

        Given:
            A long-TTL pool of one to five idle entries whose finalizers
            each finish cleanly, raise a contained Exception, raise a
            cancellation, raise KeyboardInterrupt, or raise SystemExit,
            in any order.
        When:
            expire_all() is awaited.
        Then:
            It should attempt every finalizer in cache order, leave the
            pool empty with no pending cleanup, raise the first
            uncontained failure unless a later process-level signal
            superseded it, chaining the superseded failure as its cause,
            log every other uncontained failure as discarded, and leave
            the task's cancellation count at zero.
        """
        # Arrange
        raised = {
            "error": RuntimeError,
            "cancel": asyncio.CancelledError,
            "interrupt": KeyboardInterrupt,
            "exit": SystemExit,
        }
        keys = [f"k{index}" for index in range(len(outcomes))]
        failures = {
            key: raised[outcome]()
            for key, outcome in zip(keys, outcomes)
            if outcome != "clean"
        }
        attempted = []

        async def finalizer(obj):
            attempted.append(obj)
            if obj in failures:
                raise failures[obj]

        pool = ResourcePool(factory=lambda key: key, finalizer=finalizer, ttl=60)
        for key in keys:
            async with pool.get(key):
                pass
        # The expectation, walked the way the ranking is documented: a
        # contained Exception never reaches the sweep, the first failure
        # latches, a later signal supersedes a latched cancellation, and
        # anything else is discarded.
        expected = cause = None
        discarded = 0
        for key, outcome in zip(keys, outcomes):
            if outcome in ("clean", "error"):
                continue
            if expected is None:
                expected = (key, outcome)
            elif outcome in ("interrupt", "exit") and expected[1] == "cancel":
                expected, cause = (key, outcome), expected
            else:
                discarded += 1
        expectation = (
            pytest.raises(type(failures[expected[0]]))
            if expected is not None
            else contextlib.nullcontext()
        )
        caplog.clear()

        # Act
        with (
            caplog.at_level(logging.WARNING, logger="wool.runtime.resourcepool"),
            expectation as raised_failure,
        ):
            await pool.expire_all()

        # Assert
        assert attempted == keys
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup
        if expected is not None:
            assert raised_failure is not None
            assert raised_failure.value is failures[expected[0]]
            assert raised_failure.value.__cause__ is (
                failures[cause[0]] if cause is not None else None
            )
        assert (
            sum(
                "discarding" in record.getMessage()
                for record in caplog.records
                if record.name == "wool.runtime.resourcepool"
            )
            == discarded
        )
        task = asyncio.current_task()
        assert task is not None
        assert task.cancelling() == 0

    @pytest.mark.asyncio
    async def test_expire_all_should_uncancel_when_a_signal_supersedes_a_cancellation(
        self,
    ):
        """Test an absorbed cancellation is uncancelled when a signal wins.

        Given:
            A long-TTL pool of two idle entries whose first finalizer
            parks until the sweeping task is cancelled and whose second
            raises KeyboardInterrupt.
        When:
            expire_all() runs in a task that is cancelled while parked.
        Then:
            It should raise the KeyboardInterrupt with the cancellation
            chained as its cause and leave the task's cancellation count
            at zero, since the cancellation it absorbed is not the
            failure it re-raised.
        """
        # Arrange
        parked = asyncio.Event()

        async def finalizer(obj):
            if obj == "obj-a":
                parked.set()
                await asyncio.Event().wait()
            raise KeyboardInterrupt()

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}", finalizer=finalizer, ttl=60
        )
        for key in ("a", "b"):
            async with pool.get(key):
                pass

        async def sweep():
            # Caught here: a KeyboardInterrupt escaping a task ends the loop.
            try:
                await pool.expire_all()
            except KeyboardInterrupt as error:
                task = asyncio.current_task()
                assert task is not None
                return error, task.cancelling()

        sweeping = asyncio.ensure_future(sweep())
        await asyncio.wait_for(parked.wait(), timeout=2.0)

        # Act
        sweeping.cancel()
        outcome = await sweeping

        # Assert
        assert outcome is not None
        error, cancelling = outcome
        assert isinstance(error.__cause__, asyncio.CancelledError)
        assert cancelling == 0
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_expire_all_should_preserve_cancellation_count_when_it_reraises(
        self,
    ):
        """Test a re-raised cancellation leaves the task's cancellation count alone.

        Given:
            A long-TTL pool of two idle entries whose first finalizer
            parks until the sweeping task is cancelled and whose second
            finalizes cleanly.
        When:
            expire_all() runs in a task that is cancelled while parked.
        Then:
            It should finish the sweep, re-raise the cancellation, and
            leave the task's cancellation count at one, since the
            cancellation it re-raises is the one it absorbed.
        """
        # Arrange
        parked = asyncio.Event()
        counts = []

        async def finalizer(obj):
            if obj == "obj-a":
                parked.set()
                await asyncio.Event().wait()

        pool = ResourcePool(
            factory=lambda key: f"obj-{key}", finalizer=finalizer, ttl=60
        )
        for key in ("a", "b"):
            async with pool.get(key):
                pass

        async def sweep():
            try:
                await pool.expire_all()
            except asyncio.CancelledError:
                task = asyncio.current_task()
                assert task is not None
                counts.append(task.cancelling())
                raise

        sweeping = asyncio.ensure_future(sweep())
        await asyncio.wait_for(parked.wait(), timeout=2.0)

        # Act
        sweeping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sweeping

        # Assert
        assert counts == [1]
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_expire_all_should_not_uncancel_for_a_cancellation_it_did_not_receive(
        self,
    ):
        """Test uncancel accounting counts deliveries, not CancelledErrors.

        Given:
            A long-TTL pool of two idle entries whose first finalizer
            parks until the sweeping task is cancelled, and whose second
            awaits a future a third party cancelled.
        When:
            expire_all() is awaited on a task that is cancelled once
            while the first finalizer is parked.
        Then:
            It should re-raise the delivered cancellation with the task's
            cancellation count still one, not consumed on account of the
            cancellation the second finalizer merely observed.
        """
        # Arrange
        parked = asyncio.Event()
        counts = []
        doomed_future = asyncio.get_running_loop().create_future()
        doomed_future.cancel()

        async def finalizer(obj):
            if obj == "obj-a":
                parked.set()
                await asyncio.Event().wait()
            else:
                await doomed_future

        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        for key in ("a", "b"):
            await _cache_idle_entry(pool, key)

        async def sweep():
            try:
                await pool.expire_all()
            except asyncio.CancelledError:
                task = asyncio.current_task()
                assert task is not None
                counts.append(task.cancelling())
                raise

        sweeping = asyncio.ensure_future(sweep())
        await asyncio.wait_for(parked.wait(), timeout=2.0)

        # Act
        sweeping.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sweeping

        # Assert
        assert counts == [1]
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_acquire_should_leave_entry_unreferenced_when_cancelled_mid_cleanup(
        self, expiry_race_pool
    ):
        """Test a cancelled acquire neither leaks a reference nor orphans the entry.

        Given:
            A pool whose expired key's TTL timer has fired while the
            pool lock is held by another key's acquire, so the spawned
            cleanup task and a queued acquire of that key both wait on
            the lock with the acquire first.
        When:
            The lock holder completes and the acquire is cancelled while
            it waits for the cleanup task it has just cancelled.
        Then:
            It should raise CancelledError, leave the entry cached and
            unreferenced with its TTL re-armed, and finalize it once
            that TTL elapses.
        """
        # Arrange
        pool, finalizer, factory_calls, release_blocker = expiry_race_pool
        blocker_task, acquire_task = await _queue_behind_fired_cleanup(
            pool, factory_calls, _acquire(pool, "expired")
        )

        # Act
        release_blocker.set()
        while "expired" in pool.pending_cleanup:
            await asyncio.sleep(0)
        acquire_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await acquire_task
        await blocker_task
        rearmed = isinstance(pool.pending_cleanup.get("expired"), asyncio.TimerHandle)
        await _release(pool, "blocker")
        referenced = pool.stats.referenced_entries
        await asyncio.sleep(0.1)

        # Assert
        assert rearmed
        assert referenced == 0
        assert finalizer.await_count == 2
        finalizer.assert_any_await("obj-expired")
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "retire",
        [
            pytest.param(lambda pool: pool.expire("key"), id="expire"),
            pytest.param(lambda pool: pool.expire_all(), id="expire_all"),
        ],
    )
    async def test_release_should_finalize_before_returning_when_entry_retired(
        self, mocker, retire
    ):
        """Test the release of a retired entry finalizes it inline.

        Given:
            A long-TTL pool holding a referenced entry retired by either
            expire() for its key or expire_all() for the whole pool.
        When:
            The last reference is released.
        Then:
            It should have awaited the finalizer and emptied the pool by
            the time release returns, spawning no task to do it later — a
            task would be orphaned by a loop that stops straight after
            the release.
        """
        # Arrange
        mock_finalizer = mocker.AsyncMock()
        pool = ResourcePool(
            factory=mocker.Mock(return_value="resource"),
            finalizer=mock_finalizer,
            ttl=60,
        )
        await _acquire(pool, "key")
        await retire(pool)
        tasks_before = asyncio.all_tasks()

        # Act
        await _release(pool, "key")

        # Assert
        mock_finalizer.assert_awaited_once_with("resource")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup
        assert asyncio.all_tasks() == tasks_before

    @pytest.mark.asyncio
    async def test_ttl_cleanup_should_schedule_resource_removal(self):
        """Test TTL-based cleanup schedules and executes properly.

        Given:
            A pool with TTL > 0
        When:
            A resource reference count reaches 0
        Then:
            Should schedule cleanup after TTL expires
        """
        # Arrange
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=0.1)

        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        key = "ttl-test"

        # Act
        # Acquire and immediately release
        async with pool.get(key) as resource:
            assert resource is mock_resource
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 1

        # Resource should still be in cache with cleanup deferred
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0
        assert pool.stats.pending_cleanup == 1
        mock_finalizer.assert_not_called()

        # Assert
        # Wait for cleanup to complete using polling with timeout
        start_time = time.time()
        while (key in pool.pending_cleanup) and (time.time() - start_time < 2.0):
            await asyncio.sleep(0.01)

        # Resource should now be cleaned up
        assert pool.stats.total_entries == 0
        mock_finalizer.assert_called_once_with(mock_resource)

    @pytest.mark.asyncio
    async def test_ttl_cleanup_should_be_cancelled_when_reacquired(self):
        """Test TTL cleanup is cancelled when resource is reacquired.

        Given:
            A pool with TTL > 0 and a scheduled cleanup
        When:
            The resource is reacquired before TTL expires
        Then:
            Cleanup should be cancelled and resource kept
        """
        # Arrange
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=0.1)

        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        key = "ttl-cancel-test"

        # Act
        # Acquire and release to schedule cleanup
        async with pool.get(key):
            pass

        # Should be scheduled for cleanup
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0
        assert pool.stats.pending_cleanup == 1

        # Reacquire the resource while cleanup is still waiting
        async with pool.get(key) as resource:
            # Assert - cleanup should be cancelled and resource reused
            assert resource is mock_resource
            assert pool.stats.referenced_entries == 1

        # After reacquisition and release, verify finalizer wasn't called
        # (which would indicate the original resource was preserved)
        mock_finalizer.assert_not_called()

        # Resource should still exist due to TTL
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_stats_should_return_accurate_counts(self):
        """Test stats method returns accurate cache statistics.

        Given:
            A pool with various resource states
        When:
            Stats property is accessed
        Then:
            Should return accurate counts for entries, references, and pending
            timers or tasks
        """
        # Arrange
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=0.1)

        # Guard: a fresh pool reports zero across all stats.
        stats = pool.stats
        assert stats.total_entries == 0
        assert stats.referenced_entries == 0
        assert stats.pending_cleanup == 0

        # Act
        # Add some resources
        mock_factory.side_effect = [Mock() for _ in range(3)]

        async with pool.get("key1"):  # ref_count = 1 while in context
            async with pool.get("key2"):  # ref_count = 1 while in context
                async with pool.get("key3"):  # will be released immediately
                    # Assert while all resources are active
                    stats = pool.stats
                    assert stats.total_entries == 3
                    assert stats.referenced_entries == 3  # All active
                    assert stats.pending_cleanup == 0  # None scheduled yet

    @pytest.mark.asyncio
    async def test_async_context_manager_should_clear_resources(self):
        """Test ResourcePool as async context manager clears all on exit.

        Given:
            A ResourcePool with resources
        When:
            Used as async context manager and then exited
        Then:
            Should clear all resources on exit
        """
        # Arrange
        mock_factory = Mock()
        mock_finalizer = AsyncMock()

        # Act & assert
        async with ResourcePool(factory=mock_factory, finalizer=mock_finalizer) as pool:
            mock_resource = Mock()
            mock_factory.return_value = mock_resource

            async with pool.get("test-key"):
                assert pool.stats.total_entries == 1

        # After context exit, cache should be cleared
        assert pool.stats.total_entries == 0
        mock_finalizer.assert_called_once_with(mock_resource)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ttl", [0, 0.1, 1, 1.1, 10, 10.1])
    async def test_ttl_should_schedule_cleanup_based_on_value(self, ttl):
        """Test specific TTL values defer or run cleanup accordingly.

        Given:
            A pool with specific TTL value
        When:
            A resource is acquired and released
        Then:
            It should finalize immediately for TTL 0 and defer
            cleanup for positive TTLs
        """
        # Arrange
        mock_factory = Mock(return_value=Mock(name="test-obj"))
        mock_finalizer = Mock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=ttl)

        # Act
        async with pool.get("test-key"):
            pass

        # Assert
        if ttl == 0:
            mock_finalizer.assert_called_once()
            assert pool.stats.total_entries == 0
            assert pool.stats.pending_cleanup == 0
        else:
            mock_finalizer.assert_not_called()
            assert pool.stats.total_entries == 1
            assert pool.stats.pending_cleanup == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "ttl, retire_first",
        [(0, False), (60, True)],
        ids=["zero-ttl", "retired-while-referenced"],
    )
    async def test_finalizer_should_catch_exception_and_remove_resource(
        self, ttl, retire_first
    ):
        """Test finalizer exceptions are caught and logged.

        Given:
            A pool that finalizes inline — either because it has no TTL
            or because the entry was retired by ``expire`` while still
            referenced — whose finalizer raises an exception
        When:
            Resource cleanup occurs
        Then:
            Exception should be caught and resource still removed
        """
        # Arrange
        mock_factory = Mock()

        async def failing_finalizer(_):
            raise ValueError("Finalizer failed")

        pool = ResourcePool(factory=mock_factory, finalizer=failing_finalizer, ttl=ttl)

        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        key = "test"

        # Act & assert
        # This should not raise despite finalizer failing
        async with pool.get(key):
            if retire_first:
                await pool.expire(key)

        # Resource should still be cleaned up
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_concurrent_should_maintain_consistency_when_acquire_release_same_key(
        self, counting_factory
    ):
        """Test concurrent operations on same key maintain consistency.

        Given:
            A resource pool with TTL
        When:
            Multiple coroutines acquire and release the same key concurrently
        Then:
            Resource pool should maintain consistency and not leak resources
        """
        # Arrange
        pool = ResourcePool(factory=counting_factory, ttl=0.1)

        # Act
        async def acquire_release_worker():
            async with pool.get("shared-key") as resource:
                await asyncio.sleep(0.01)  # Small delay to increase contention
                return resource

        # Run multiple concurrent workers
        tasks = [acquire_release_worker() for _ in range(10)]
        results = await asyncio.gather(*tasks)

        # Assert
        # All workers should get the same resource instance (cached)
        assert len(set(results)) == 1  # All got the same resource
        # Factory should only be called once despite concurrent access
        assert counting_factory.call_count == 1
        # Pool should be consistent after all operations
        assert pool.stats.total_entries <= 1  # 0 or 1 depending on TTL timing

    def test_get_should_return_a_resource_when_no_loop_is_running(self, mocker):
        """Test building a resource needs no loop of its own.

        Given:
            A pool reached from synchronous code, with no event loop
            running.
        When:
            get() is called for a key.
        Then:
            It should return a Resource without raising, the loop being
            bound where the resource is entered rather than where it is
            built.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)

        # Act
        resource = pool.get("key")

        # Assert
        assert isinstance(resource, Resource)

    @pytest.mark.asyncio
    async def test_get_should_handle_none_key(self):
        """Test resource pool handles None key appropriately.

        Given:
            A resource pool
        When:
            get() is called with None key
        Then:
            Should handle None key as a valid cache key
        """
        # Arrange
        mock_factory = Mock()
        mock_resource = Mock()
        mock_factory.return_value = mock_resource
        pool = ResourcePool(factory=mock_factory, ttl=0)

        # Act & assert
        # None should be treated as a valid key
        async with pool.get(None) as resource:
            assert resource is mock_resource

        # Resource should be cleaned up after use
        assert pool.stats.total_entries == 0


class TestResource:
    """Test suite for the Resource class."""

    @pytest.mark.asyncio
    async def test_context_manager_should_auto_release(self):
        """Test Resource as async context manager.

        Given:
            A Resource instance from a pool
        When:
            Used as async context manager
        Then:
            Should auto-acquire on enter and auto-release on exit
        """
        # Arrange
        mock_factory = Mock()
        mock_resource = Mock()
        mock_resource.name = "context-resource"
        mock_factory.return_value = mock_resource

        pool = ResourcePool(factory=mock_factory, ttl=0)

        # Act & assert
        # Use Resource as context manager
        async with pool.get("test-key") as resource:
            assert resource is mock_resource
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 1

        # Should be automatically cleaned up after context exit
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body_error",
        [None, KeyError("boom")],
        ids=["clean-exit", "body-raises"],
    )
    async def test_context_manager_should_finalize_when_retired_in_body(
        self, retired_entry_pool, body_error
    ):
        """Test exiting the context finalizes a resource retired inside it.

        Given:
            A long-TTL pool whose key is retired by ``expire`` from
            inside an ``async with pool.get(key)`` body while still
            referenced, where the body then either returns or raises.
        When:
            The context manager exits, normally or on the exceptional
            unwind.
        Then:
            It should have awaited the finalizer before the statement
            following the block runs, leaving the entry evicted with no
            cleanup pending and any original exception propagating
            unchanged.
        """
        # Arrange
        pool, finalizer, _ = retired_entry_pool
        guard = pytest.raises(KeyError, match="boom") if body_error else nullcontext()

        # Act & assert
        with guard:
            async with pool.get("key"):
                await pool.expire("key")
                # Guard: still referenced, so nothing is finalized yet.
                finalizer.assert_not_awaited()
                if body_error:
                    raise body_error

        finalizer.assert_awaited_once_with("first")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test___aenter___should_raise_when_entered_twice(self, mocker):
        """Test a resource is a single-use context manager.

        Given:
            A resource that has already been entered and exited once.
        When:
            The same resource is entered again.
        Then:
            It should raise RuntimeError rather than take a second
            reference against one acquisition.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=0)
        resource = pool.get("key")
        async with resource as acquired:
            assert acquired == "obj"

        # Act & assert
        with pytest.raises(RuntimeError, match="Cannot re-acquire a resource"):
            async with resource:
                pass

    @pytest.mark.asyncio
    async def test___aenter___should_propagate_when_factory_raises(self, mocker):
        """Test a failed acquisition leaves the resource enterable.

        Given:
            A resource for a key whose factory raises on its first call
            and succeeds on the second.
        When:
            The resource is entered, the failure propagates, and it is
            entered again.
        Then:
            It should propagate the factory's failure, cache nothing,
            and treat the second entry as the first real acquisition
            rather than refuse it as a re-acquire.
        """
        # Arrange
        factory = mocker.Mock(side_effect=[RuntimeError("factory failed"), "obj"])
        pool = ResourcePool(factory=factory, ttl=60)
        resource = pool.get("key")

        # Act
        with pytest.raises(RuntimeError, match="factory failed"):
            async with resource:
                pass

        # Assert
        assert pool.stats.total_entries == 0
        async with resource as acquired:
            assert acquired == "obj"

    @pytest.mark.asyncio
    async def test___aenter___should_allow_reentry_when_the_acquire_is_cancelled(self):
        """Test a cancelled entry leaves the resource enterable again.

        Given:
            A pool whose factory parks until released, and a Resource
            for a key it has never cached.
        When:
            A task entering the Resource is cancelled while the factory
            is parked, and the Resource is entered again once the
            factory is released.
        Then:
            It should leave the pool with nothing cached or referenced
            after the cancellation, and hand back the object on the
            second entry rather than refuse a re-acquire.
        """
        # Arrange
        parked = asyncio.Event()
        gate = asyncio.Event()

        async def factory(key):
            parked.set()
            await gate.wait()
            return f"obj-{key}"

        pool = ResourcePool(factory=factory, ttl=60)
        resource = pool.get("key")

        entering = asyncio.ensure_future(_use_resource(resource))
        await asyncio.wait_for(parked.wait(), timeout=2.0)

        # Act
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering
        gate.set()

        # Assert
        assert pool.stats.total_entries == 0
        assert pool.stats.referenced_entries == 0
        async with resource as obj:
            assert obj == "obj-key"

    def test___aenter___should_cache_in_the_entering_loops_partition(
        self, background_loops
    ):
        """Test entering a resource binds it to the loop that enters it.

        Given:
            A resource built from synchronous code, with no loop
            running, for a pool no loop has reached.
        When:
            It is entered and exited on an event loop running on
            another thread.
        Then:
            It should cache the object in that loop's partition alone,
            leaving the building loop with nothing.
        """
        # Arrange
        pool = ResourcePool(factory=make_resource, ttl=60)
        resource = pool.get("key")
        live = background_loops()

        async def use():
            async with resource as acquired:
                held = pool.stats
            return acquired, held, pool.stats

        # Act
        acquired, held, released = live.run(use())

        # Assert
        assert acquired == "obj-key"
        assert held.referenced_entries == 1
        assert released.total_entries == 1
        assert released.referenced_entries == 0
        assert asyncio.run(_pool_stats(pool)).total_entries == 0

    @pytest.mark.asyncio
    async def test___aexit___should_raise_when_never_entered(self, mocker):
        """Test exiting a resource that was never entered is an error.

        Given:
            A resource that has never been entered, pushed onto an exit
            stack so its exit runs without its entry.
        When:
            The stack is closed.
        Then:
            It should raise RuntimeError, a release being tied to an
            acquisition it never made.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)
        stack = AsyncExitStack()
        stack.push_async_exit(pool.get("key"))

        # Act & assert
        with pytest.raises(
            RuntimeError, match="Cannot release a resource that was not acquired"
        ):
            await stack.aclose()

    @pytest.mark.asyncio
    async def test___aexit___should_raise_when_already_released(self, mocker):
        """Test exiting a released resource a second time is an error.

        Given:
            A resource entered and exited once, then pushed onto an exit
            stack so its exit runs again.
        When:
            The stack is closed.
        Then:
            It should raise RuntimeError rather than drop a second
            reference for one acquisition.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)
        resource = pool.get("key")
        async with resource:
            pass
        stack = AsyncExitStack()
        stack.push_async_exit(resource)

        # Act & assert
        with pytest.raises(
            RuntimeError,
            match="Cannot release a resource that has already been released",
        ):
            await stack.aclose()

    def test___aexit___should_hand_the_release_back_when_exited_on_another_loop(
        self, mocker, background_loops
    ):
        """Test a cross-loop exit is refused but the reference is not lost.

        Given:
            A resource entered on an event loop running on another
            thread.
        When:
            The resource is exited on a second loop.
        Then:
            It should raise RuntimeError naming the pool and the key and
            saying the release was handed to the acquiring loop, which
            then drops the reference: the entry stays cached on that
            loop, unreferenced, with its TTL armed and its finalizer not
            run.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        live = background_loops()
        resource = pool.get("key")
        stack = AsyncExitStack()
        live.run(stack.enter_async_context(resource))

        # Act
        with pytest.raises(RuntimeError) as raised:
            asyncio.run(_close_stack_elsewhere(resource))
        live.run(_poll_until(lambda: pool.stats.referenced_entries == 0))

        # Assert
        assert str(raised.value) == (
            "ResourcePool(make_resource) cannot release key 'key' on a loop "
            "other than the one that acquired it; the release was handed to "
            "the acquiring loop"
        )
        live_snapshot = live.run(_pool_stats(pool))
        assert live_snapshot.total_entries == 1
        assert live_snapshot.pending_cleanup == 1
        finalizer.assert_not_awaited()

    def test___aexit___should_arm_the_ttl_when_the_stack_closes_on_its_own_loop(
        self, mocker, background_loops
    ):
        """Test the loop check passes for the loop that entered.

        Given:
            A resource entered on an event loop running on another
            thread, through an exit stack that same loop closes.
        When:
            The stack is closed there.
        Then:
            It should release the reference and leave the entry cached
            with its TTL armed, the refusal being reserved for another
            loop.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        live = background_loops()
        stack = AsyncExitStack()
        live.run(stack.enter_async_context(pool.get("key")))

        # Act
        live.run(stack.aclose())

        # Assert
        live_snapshot = live.run(_pool_stats(pool))
        assert live_snapshot.total_entries == 1
        assert live_snapshot.referenced_entries == 0
        assert live_snapshot.pending_cleanup == 1
        finalizer.assert_not_awaited()

    def test___aexit___should_raise_as_released_when_retried_after_a_refusal(
        self, mocker, background_loops
    ):
        """Test a refused exit consumes the resource's one release.

        Given:
            A resource entered on an event loop running on another
            thread, whose exit a second loop has already been refused
            and handed back.
        When:
            It is exited again on the loop that entered it.
        Then:
            It should raise RuntimeError as already released, the
            handed-back release having dropped the reference once.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        live = background_loops()
        resource = pool.get("key")
        stack = AsyncExitStack()
        live.run(stack.enter_async_context(resource))
        with pytest.raises(RuntimeError, match="cannot release"):
            asyncio.run(_close_stack_elsewhere(resource))
        live.run(_poll_until(lambda: pool.stats.referenced_entries == 0))

        # Act & assert
        with pytest.raises(RuntimeError, match="already been released"):
            live.run(stack.aclose())

        live_snapshot = live.run(_pool_stats(pool))
        assert live_snapshot.total_entries == 1
        assert live_snapshot.referenced_entries == 0
        finalizer.assert_not_awaited()

    def test___aexit___should_raise_when_the_exiting_loop_cached_the_same_key(
        self, background_loops
    ):
        """Test the refusal is about the resource, not the key.

        Given:
            A resource entered on an event loop running on another
            thread, and a second loop that independently cached and
            still holds the same key.
        When:
            The resource is exited on the second loop.
        Then:
            It should raise RuntimeError naming the pool and the key,
            leave the exiting loop's own reference count unchanged, and
            drop only the acquiring loop's, through the handed-back
            release.
        """
        # Arrange
        pool = ResourcePool(factory=make_resource, ttl=60)
        live = background_loops()
        resource = pool.get("key")
        stack = AsyncExitStack()
        live.run(stack.enter_async_context(resource))
        snapshots = []

        async def exit_where_the_key_is_cached():
            await _acquire(pool, "key")
            snapshots.append(pool.stats)
            try:
                await _close_stack_elsewhere(resource)
            finally:
                snapshots.append(pool.stats)

        # Act
        with pytest.raises(RuntimeError) as raised:
            asyncio.run(exit_where_the_key_is_cached())

        # Assert
        assert str(raised.value) == (
            "ResourcePool(make_resource) cannot release key 'key' on a loop "
            "other than the one that acquired it; the release was handed to "
            "the acquiring loop"
        )
        assert [(s.total_entries, s.referenced_entries) for s in snapshots] == [
            (1, 1),
            (1, 1),
        ]
        live.run(_poll_until(lambda: pool.stats.referenced_entries == 0))
        live_snapshot = live.run(_pool_stats(pool))
        assert live_snapshot.total_entries == 1
        assert live_snapshot.referenced_entries == 0

    def test___aexit___should_raise_as_released_when_released_before_the_loop_check(
        self, background_loops
    ):
        """Test the release guards are ordered, released before loop.

        Given:
            A resource entered and exited on an event loop running on
            another thread.
        When:
            It is exited once more from a second loop.
        Then:
            It should report the resource as already released rather
            than as released on the wrong loop, the double release
            being the more specific fault.
        """
        # Arrange
        pool = ResourcePool(factory=make_resource, ttl=60)
        live = background_loops()
        resource = pool.get("key")
        stack = AsyncExitStack()
        live.run(stack.enter_async_context(resource))
        live.run(stack.aclose())

        # Act & assert
        with pytest.raises(
            RuntimeError,
            match="Cannot release a resource that has already been released",
        ):
            asyncio.run(_close_stack_elsewhere(resource))

    def test___aexit___should_release_when_its_loop_resumes(
        self, mocker, caplog, stranded_loop
    ):
        """Test an outstanding resource survives its loop's pause.

        Given:
            A resource entered on a loop that then paused, and a fresh
            loop that read and used the pool meanwhile.
        When:
            The paused loop resumes and exits the resource.
        Then:
            It should release into the entry it acquired, leaving it
            cached and unreferenced with its finalizer not run, and
            report nothing.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=60)
        resource = pool.get("key")
        stack = AsyncExitStack()
        paused, _ = stranded_loop(stack.enter_async_context(resource), close=False)

        async def read_and_use():
            pool.stats
            await pool.expire("none")

        asyncio.run(read_and_use())
        caplog.clear()

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            paused.run_until_complete(stack.aclose())
            resumed = paused.run_until_complete(_pool_stats(pool))

        # Assert
        assert _pool_records(caplog) == []
        assert resumed.total_entries == 1
        assert resumed.referenced_entries == 0
        finalizer.assert_not_awaited()

    def test___aexit___should_finalize_once_when_its_loop_paused_while_held(
        self, mocker, stranded_loop
    ):
        """Test a held resource outlives a pause other loops use the pool through.

        Given:
            A zero-TTL pool and a resource entered on a loop that then
            paused, while a fresh loop read the pool and cached and
            cleared an entry of its own under the same key.
        When:
            The paused loop resumes and exits the resource.
        Then:
            It should finalize the object exactly once, leaving that
            loop's partition empty.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=0)
        resource = pool.get("key")
        stack = AsyncExitStack()
        paused, _ = stranded_loop(stack.enter_async_context(resource), close=False)

        async def read_use_and_clear():
            pool.stats
            await _cache_idle_entry(pool, "key")
            await pool.clear()

        asyncio.run(read_use_and_clear())
        finalizer.reset_mock()

        # Act
        paused.run_until_complete(stack.aclose())

        # Assert
        finalizer.assert_awaited_once_with("obj-key")
        assert paused.run_until_complete(_pool_stats(pool)).total_entries == 0

    def test___aexit___should_keep_a_second_holders_object_when_its_loop_paused(
        self, mocker, stranded_loop
    ):
        """Test a pause never lets one release finalize under another holder.

        Given:
            A zero-TTL pool and two resources for one key entered on a
            loop that then paused, while a fresh loop used the pool.
        When:
            The paused loop resumes and exits the first resource, then
            the second.
        Then:
            It should leave the object unfinalized and referenced after
            the first exit and finalize it once after the second.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=make_resource, finalizer=finalizer, ttl=0)
        first, second = AsyncExitStack(), AsyncExitStack()

        async def enter_both():
            await first.enter_async_context(pool.get("key"))
            await second.enter_async_context(pool.get("key"))

        paused, _ = stranded_loop(enter_both(), close=False)
        asyncio.run(pool.expire("none"))

        # Act
        paused.run_until_complete(first.aclose())
        after_first = paused.run_until_complete(_pool_stats(pool))
        awaited_after_first = finalizer.await_count
        paused.run_until_complete(second.aclose())

        # Assert
        assert (after_first.total_entries, after_first.referenced_entries) == (1, 1)
        assert awaited_after_first == 0
        finalizer.assert_awaited_once_with("obj-key")

    @pytest.mark.asyncio
    async def test___aexit___should_release_nothing_when_its_entry_was_cleared(
        self, mocker
    ):
        """Test a release lands on the entry it acquired, not on its key.

        Given:
            A zero-TTL pool, a resource entered for a key, the pool then
            cleared under it, and a second resource entered for the same
            key since.
        When:
            The first resource exits, then the second.
        Then:
            It should leave the second resource's object referenced and
            unfinalized after the first exit and finalize it once after
            the second, the first exit having dropped nothing.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        factory = mocker.Mock(side_effect=["first", "second"])
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=0)
        stale, live = AsyncExitStack(), AsyncExitStack()
        await stale.enter_async_context(pool.get("key"))
        await pool.clear()
        await live.enter_async_context(pool.get("key"))
        finalizer.reset_mock()

        # Act
        await stale.aclose()
        after_stale = pool.stats
        awaited_after_stale = finalizer.await_count
        await live.aclose()

        # Assert
        assert (after_stale.total_entries, after_stale.referenced_entries) == (1, 1)
        assert awaited_after_stale == 0
        finalizer.assert_awaited_once_with("second")

    @given(
        key=strategies.one_of(
            strategies.none(),
            strategies.booleans(),
            strategies.integers(),
            strategies.floats(allow_nan=False),
            strategies.text(),
            strategies.binary(),
            strategies.tuples(strategies.integers(), strategies.text()),
        )
    )
    @settings(
        max_examples=25,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test___aexit___should_raise_for_any_key_when_exited_on_another_loop(
        self, background_loops, key
    ):
        """Test the cross-loop refusal holds over the key domain.

        Given:
            Any hashable key — none, a boolean, an integer, a float,
            text, bytes, or a tuple — entered on an event loop running
            on another thread.
        When:
            The resource is exited on a second loop.
        Then:
            It should always raise RuntimeError and hand the release to
            the first loop, leaving its entry cached and unreferenced.
        """
        # Arrange
        pool = ResourcePool(factory=make_resource, ttl=60)
        live = background_loops()
        try:
            resource = pool.get(key)
            stack = AsyncExitStack()
            live.run(stack.enter_async_context(resource))

            # Act & assert
            with pytest.raises(RuntimeError, match="cannot release key"):
                asyncio.run(_close_stack_elsewhere(resource))

            live.run(_poll_until(lambda: pool.stats.referenced_entries == 0))
            live_snapshot = live.run(_pool_stats(pool))
            assert live_snapshot.total_entries == 1
            assert live_snapshot.referenced_entries == 0
        finally:
            # Retire each example's loop eagerly; the fixture would
            # otherwise hold every loop of every example open.
            live.close()

    @pytest.mark.asyncio
    @given(
        resource=strategies.sampled_from(
            [None, 0, 0.0, False, "", b"", (), [], {}, set()]
        )
    )
    @settings(max_examples=10, deadline=None)
    async def test___aexit___should_release_when_resource_is_falsy(self, resource):
        """Test a falsy resource is still released on context exit.

        Given:
            A zero-TTL pool whose factory yields a falsy object (e.g.,
            None, zero, False, or an empty string, bytes, tuple, list,
            dict, or set).
        When:
            The Resource is used as an async context manager and exits.
        Then:
            It should drop the reference and finalize the entry, so the
            release follows the acquisition rather than the value.
        """
        # Arrange
        finalized = []
        pool = ResourcePool(
            factory=lambda key: resource,
            finalizer=lambda obj: finalized.append(obj),
            ttl=0,
        )

        # Act
        async with pool.get("key") as acquired:
            referenced = pool.stats.referenced_entries

        # Assert
        assert acquired is resource
        assert referenced == 1
        assert pool.stats.total_entries == 0
        assert len(finalized) == 1
        assert finalized[0] is resource
