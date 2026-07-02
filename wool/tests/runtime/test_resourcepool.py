import asyncio
import gc
import time
import warnings
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from hypothesis import given
from hypothesis import strategies

from wool.runtime.resourcepool import Resource
from wool.runtime.resourcepool import ResourcePool


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
def mock_resource_factory():
    """Create a mock factory with consistent behavior."""
    factory = Mock()
    factory.return_value = Mock(name="test-resource")
    return factory


@pytest.fixture
def mock_finalizer():
    """Create a mock finalizer that tracks calls."""
    return AsyncMock()


@pytest.fixture
def resource_pool_immediate_cleanup(mock_resource_factory, mock_finalizer):
    """Create a resource pool with TTL=0 for immediate cleanup testing."""
    return ResourcePool(factory=mock_resource_factory, finalizer=mock_finalizer, ttl=0)


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

    blocker_task = asyncio.create_task(pool.acquire("blocker"))

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
            Resources are released via pool.release()
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
        await pool.acquire("key1")
        await pool.acquire("key2")

        assert pool.stats.referenced_entries == 2

        # Act & assert
        await pool.release("key1")
        assert pool.stats.referenced_entries == 1

        await pool.release("key2")
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_release_should_not_affect_existing_resources_when_key_nonexistent(
        self, counting_factory
    ):
        """Test releasing a nonexistent key is a silent no-op.

        Given:
            A pool with some existing resources
        When:
            Release is called with a nonexistent key
        Then:
            Should exit without affecting existing resources
        """
        # Arrange
        pool = ResourcePool(factory=counting_factory, ttl=1.0)

        # Create some resources to establish initial state
        keys = ["key1", "key2"]
        for key in keys:
            async with pool.get(key):
                pass  # Just acquire and release to populate cache

        initial_cache_size = pool.stats.total_entries

        # Act & assert
        # Try to release a nonexistent key
        await pool.release("nonexistent")

        # Should not affect existing resources
        assert pool.stats.total_entries == initial_cache_size
        # All keys should have zero references (since they were released)
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_release_should_raise_value_error_when_zero_reference_count(self):
        """Test releasing key with zero ref count raises ValueError.

        Given:
            A pool with a resource that has zero reference count
        When:
            Release is called on that key
        Then:
            Should raise ValueError indicating reference count is already
            zero
        """
        # Arrange
        # Create a new resource with unique key using a pool with TTL > 0
        # so the resource stays in cache after release
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        ttl_pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)

        unique_key = "test-zero-ref-count"
        mock_resource = Mock()
        mock_resource.name = unique_key
        mock_factory.return_value = mock_resource

        # Act & assert
        # Acquire and release once to get ref count to 0 (but stays in cache due to TTL)
        async with ttl_pool.get(unique_key):
            pass

        # Now try to release again - should raise ValueError
        with pytest.raises(
            ValueError,
            match=f"Reference count for key '{unique_key}' is already 0",
        ):
            await ttl_pool.release(unique_key)

    @pytest.mark.asyncio
    async def test_finalizer_should_still_evict_entry_when_raising_base_exception(self):
        """Test a cancelled finalizer still evicts the cache entry.

        Given:
            A ``ttl=0`` pool whose finalizer raises
            ``CancelledError`` — a ``BaseException``, not an
            ``Exception`` — on its first call, modelling cleanup that
            runs under a cancelled teardown
        When:
            A resource is acquired and released, driving immediate
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
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=0)

        # Act
        # Acquire then release: rc -> 0 drives immediate cleanup, whose
        # finalizer raises CancelledError out of the release.
        with pytest.raises(asyncio.CancelledError):
            async with pool.get("key"):
                pass

        # Assert
        # The finalized resource must not survive in the cache.
        assert pool.stats.total_entries == 0
        # The next acquire is therefore a miss that builds a fresh
        # resource, never the torn-down one.
        async with pool.get("key") as resource:
            assert resource.name == "second"
        assert factory.call_count == 2

    def test_acquire_should_cancel_cross_loop_timer_threadsafe(self, mocker):
        """Test acquire cancels a foreign-loop TTL timer cross-loop.

        Given:
            A pool whose cached entry has an unfired TTL timer
            scheduled on one event loop, which has since closed.
        When:
            The same key is re-acquired from a different event loop.
        Then:
            The timer should be cancelled on its own (now-closed) loop
            via call_soon_threadsafe, the resulting RuntimeError
            should be swallowed, the cached object should be returned,
            and the pending cleanup should be cleared.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)

        # Acquire and release on a dedicated loop so a TTL timer is
        # scheduled and bound to that loop, then close it.
        foreign_loop = asyncio.new_event_loop()

        async def schedule_cleanup_on_foreign_loop():
            async with pool.get("key"):
                pass

        foreign_loop.run_until_complete(schedule_cleanup_on_foreign_loop())
        pending_cleanup = pool.pending_cleanup["key"]
        foreign_loop.close()

        acquiring_loop = asyncio.new_event_loop()
        try:
            # Act
            acquired = acquiring_loop.run_until_complete(pool.acquire("key"))

            # Assert
            assert acquired == "obj"
            assert pool.pending_cleanup == {}
        finally:
            acquiring_loop.close()
            del pending_cleanup

    def test_clear_should_cancel_cross_loop_timer_threadsafe(self, mocker):
        """Test clear cancels a foreign-loop TTL timer cross-loop.

        Given:
            A pool whose cached entry has an unfired TTL timer
            scheduled on one event loop, which has since closed.
        When:
            The key is cleared from a different event loop.
        Then:
            The timer should be cancelled on its own (now-closed) loop
            via call_soon_threadsafe, the resulting RuntimeError
            should be swallowed, the finalizer should still run, and
            the entry should be evicted.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(
            factory=mocker.Mock(return_value="obj"), finalizer=finalizer, ttl=60
        )

        # Acquire and release on a dedicated loop so a TTL timer is
        # scheduled and bound to that loop, then close it.
        foreign_loop = asyncio.new_event_loop()

        async def schedule_cleanup_on_foreign_loop():
            async with pool.get("key"):
                pass

        foreign_loop.run_until_complete(schedule_cleanup_on_foreign_loop())
        pending_cleanup = pool.pending_cleanup["key"]
        foreign_loop.close()

        clearing_loop = asyncio.new_event_loop()
        try:
            # Act
            clearing_loop.run_until_complete(pool.clear("key"))

            # Assert
            finalizer.assert_awaited_once_with("obj")
            assert pool.stats.total_entries == 0
        finally:
            clearing_loop.close()
            del pending_cleanup

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
            pool, factory_calls, pool.acquire("expired")
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
    async def test_clear_should_cancel_in_flight_cleanup_when_cleared_after_expiry(
        self, expiry_race_pool
    ):
        """Test clear cancels a fired cleanup racing on the pool lock.

        Given:
            A pool whose expired key's TTL timer has fired while the
            pool lock is held by another key's acquire, so the
            spawned cleanup task and a queued clear of the expired
            key both wait on the lock with the clear first
        When:
            The lock holder completes and the queued clear runs
        Then:
            It should cancel the in-flight cleanup, still run the
            finalizer exactly once, and evict the entry
        """
        # Arrange
        pool, finalizer, factory_calls, release_blocker = expiry_race_pool
        blocker_task, clear_task = await _queue_behind_fired_cleanup(
            pool, factory_calls, pool.clear("expired")
        )

        # Act
        release_blocker.set()
        await clear_task
        await blocker_task

        # Assert
        finalizer.assert_awaited_once_with("obj-expired")
        assert "expired" not in pool.pending_cleanup
        assert pool.stats.total_entries == 1

    @pytest.mark.asyncio
    @given(
        operations=strategies.lists(
            strategies.tuples(
                strategies.sampled_from(["acquire", "release"]),
                strategies.sampled_from(["a", "b", "c"]),
            ),
            max_size=30,
        )
    )
    async def test_release_should_maintain_bookkeeping_invariants(self, operations):
        """Test acquire and release keep TTL bookkeeping consistent.

        Given:
            Any interleaved sequence of acquire and release
            operations over a small key domain, where releases are
            applied only while a reference is held
        When:
            The sequence is applied step by step to a long-TTL pool
        Then:
            It should keep total entries equal to the keys ever
            acquired, referenced entries equal to the keys with live
            references, and pending cleanup on exactly the keys whose
            references all released
        """
        # Arrange
        pool = ResourcePool(factory=lambda key: f"obj-{key}", ttl=60)
        model_refcount = {}

        # Act & assert
        for operation, key in operations:
            if operation == "acquire":
                await pool.acquire(key)
                model_refcount[key] = model_refcount.get(key, 0) + 1
            elif model_refcount.get(key, 0) > 0:
                await pool.release(key)
                model_refcount[key] -= 1

            stats = pool.stats
            assert stats.total_entries == len(model_refcount)
            assert stats.referenced_entries == sum(
                1 for count in model_refcount.values() if count > 0
            )
            assert set(pool.pending_cleanup) == {
                key for key, count in model_refcount.items() if count == 0
            }

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
    async def test_clear_should_remove_specific_resource_when_key_given(
        self, mock_finalizer
    ):
        """Test clearing a specific key from the pool.

        Given:
            A pool with multiple resources
        When:
            Clear is called with a specific key
        Then:
            Only that resource should be cleaned up and removed
        """
        # Arrange
        mock_factory = Mock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)

        # Create multiple resources
        mock_resource1 = Mock()
        mock_resource1.name = "resource1"
        mock_resource2 = Mock()
        mock_resource2.name = "resource2"

        mock_factory.side_effect = [mock_resource1, mock_resource2]

        # Acquire two resources using context managers to populate cache
        async with pool.get("key1"):
            pass  # Resource created and cached with TTL > 0
        async with pool.get("key2"):
            pass  # Resource created and cached with TTL > 0

        # Both resources should be in cache but not referenced (TTL keeps them)
        assert pool.stats.total_entries == 2
        assert pool.stats.referenced_entries == 0

        # Act
        # Clear only one specific key
        await pool.clear("key1")

        # Assert
        # Only key1 should be removed, key2 should remain
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0

        # Finalizer should have been called only for the cleared resource
        mock_finalizer.assert_called_once_with(mock_resource1)

    @pytest.mark.asyncio
    async def test_clear_should_raise_key_error_when_key_nonexistent(self):
        """Test clearing a non-existent key raises KeyError.

        Given:
            A pool with some resources
        When:
            Clear is called with a non-existent key
        Then:
            Should raise KeyError and not affect existing resources
        """
        # Arrange
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)

        # Create one resource
        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        async with pool.get("existing-key"):
            pass  # Resource created and cached with TTL > 0
        assert pool.stats.total_entries == 1

        # Act & assert
        # Try to clear a non-existent key - should raise KeyError
        with pytest.raises(KeyError):
            await pool.clear("nonexistent-key")

        # Should not affect existing resources
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0  # Not referenced after context exit

        # Finalizer should not have been called
        mock_finalizer.assert_not_called()

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
    async def test_finalizer_should_catch_exception_and_remove_resource(self):
        """Test finalizer exceptions are caught and logged.

        Given:
            A finalizer that raises an exception
        When:
            Resource cleanup occurs
        Then:
            Exception should be caught and resource still removed
        """
        # Arrange
        mock_factory = Mock()

        async def failing_finalizer(_):
            raise ValueError("Finalizer failed")

        pool = ResourcePool(factory=mock_factory, finalizer=failing_finalizer)

        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        key = "test"

        # Act & assert
        # This should not raise despite finalizer failing
        async with pool.get(key):
            pass

        # Resource should still be cleaned up
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_clear_should_raise_key_error_when_key_absent(self):
        """Test clear with non-existent key raises appropriate error.

        Given:
            A pool with a resource
        When:
            clear() is called with a non-existent key
        Then:
            Should raise KeyError for non-existent key
        """
        # Arrange
        mock_factory = Mock(return_value=Mock())
        pool = ResourcePool(factory=mock_factory, ttl=0)

        # Create one resource first
        async with pool.get("valid-key"):
            pass

        # Act & assert
        # Non-existent keys should raise KeyError
        with pytest.raises(KeyError):
            await pool.clear("nonexistent-key")

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

    @pytest.mark.asyncio
    async def test_resource_pool_should_cleanup_immediately_when_zero_ttl(
        self, resource_pool_immediate_cleanup
    ):
        """Test TTL=0 performs immediate cleanup as expected.

        Given:
            A resource pool with TTL=0
        When:
            A resource is acquired and released
        Then:
            Should perform immediate cleanup without scheduling
        """
        # Arrange
        pool = resource_pool_immediate_cleanup
        mock_resource = Mock()
        pool._factory.return_value = mock_resource

        # Act
        async with pool.get("test-key") as resource:
            # While in context, resource should exist
            assert resource is mock_resource
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 1

        # Assert
        # After context exit with TTL=0, should be immediately cleaned up
        assert pool.stats.total_entries == 0
        assert pool.stats.referenced_entries == 0
        assert pool.stats.pending_cleanup == 0  # No pending cleanup tasks
        pool._finalizer.assert_called_once_with(mock_resource)

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
    async def test_resource_should_have_no_manual_release_method(self):
        """Test Resource has no manual release method.

        Given:
            A Resource instance
        When:
            Checking for release method
        Then:
            Should not have a release method
        """
        # Arrange
        mock_factory = Mock()
        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        pool = ResourcePool(factory=mock_factory, ttl=0)

        resource_acquisition = pool.get("test-key")

        # Act & assert
        # Manual release method should not exist
        assert not hasattr(resource_acquisition, "release")

    @pytest.mark.asyncio
    async def test_resource_should_stay_cached_when_ttl_set(self):
        """Test Resource lifecycle with TTL keeps resource in cache.

        Given:
            A Resource instance with TTL pool
        When:
            Used as context manager
        Then:
            Should handle lifecycle correctly and resource stays cached due to TTL
        """
        # Arrange
        mock_factory = Mock()
        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        pool = ResourcePool(factory=mock_factory, ttl=60)  # Use TTL to keep resource

        resource_acquisition = pool.get("test-key")

        # Use as context manager
        async with resource_acquisition as resource:
            assert resource is mock_resource
            assert pool.stats.referenced_entries == 1

        # Resource should still exist due to TTL but no longer referenced
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_context_manager_should_handle_lifecycle(self):
        """Test using Resource only as context manager.

        Given:
            A Resource instance
        When:
            Used only as context manager
        Then:
            Should handle acquisition and release correctly
        """
        # Arrange
        mock_factory = Mock()
        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        pool = ResourcePool(factory=mock_factory, ttl=0)

        # Act & assert
        # Use only as context manager
        async with pool.get("test-key") as resource:
            assert resource is mock_resource
            assert pool.stats.referenced_entries == 1

        # After context exit, should be cleaned up (TTL=0)
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_acquire_should_raise_runtime_error_when_acquired_twice(self):
        """Test that re-acquiring the same Resource instance raises error.

        Given:
            A Resource that has been used as context manager once
        When:
            Attempting to use it as context manager again
        Then:
            Should raise RuntimeError
        """
        mock_factory = Mock()
        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        pool = ResourcePool(factory=mock_factory, ttl=0)
        resource_acquisition = pool.get("test-key")

        # First use as context manager
        async with resource_acquisition as resource:
            assert resource is mock_resource

        # Second use as context manager should fail
        with pytest.raises(RuntimeError, match="Cannot re-acquire a resource"):
            async with resource_acquisition:
                pass

    @pytest.mark.asyncio
    async def test_resource_context_should_propagate_acquire_exception(self):
        """Test Resource context manager handles acquire exceptions properly.

        Given:
            A Resource instance from a pool that fails during acquire
        When:
            Entering the context manager
        Then:
            Should propagate the exception and set _acquired to False
        """
        # Arrange
        mock_pool = AsyncMock()
        mock_pool.acquire.side_effect = RuntimeError("Acquire failed")

        resource = Resource(pool=mock_pool, key="test-key")

        # Act & assert
        with pytest.raises(RuntimeError, match="Acquire failed"):
            async with resource:
                pass

        # Verify _acquired was set to False during exception handling
        assert resource._acquired is False

    @pytest.mark.asyncio
    async def test_resource_context_should_raise_runtime_error_when_not_acquired(self):
        """Test Resource release when not acquired raises RuntimeError.

        Given:
            A Resource instance that was never acquired
        When:
            Attempting to exit context without entering properly
        Then:
            Should raise RuntimeError indicating resource was not acquired
        """
        # Arrange
        mock_pool = AsyncMock()
        resource = Resource(pool=mock_pool, key="test-key")

        # Act & assert - manually call __aexit__ without calling __aenter__
        with pytest.raises(
            RuntimeError, match="Cannot release a resource that was not acquired"
        ):
            await resource.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_resource_context_should_raise_runtime_error_when_already_released(
        self,
    ):
        """Test Resource release when already released raises RuntimeError.

        Given:
            A Resource instance that was already released
        When:
            Attempting to exit context again after normal usage
        Then:
            Should raise RuntimeError indicating resource was already released
        """
        # Arrange
        mock_pool = AsyncMock()
        mock_resource = Mock()
        mock_pool.acquire.return_value = mock_resource

        resource = Resource(pool=mock_pool, key="test-key")

        # Use normally once (which sets _released = True)
        async with resource:
            pass

        # Act & assert - manually call __aexit__ again
        with pytest.raises(
            RuntimeError,
            match="Cannot release a resource that has already been released",
        ):
            await resource.__aexit__(None, None, None)
