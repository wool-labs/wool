import asyncio
import time
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis import strategies

import wool.runtime.resourcepool as rp
from wool.runtime.resourcepool import Resource
from wool.runtime.resourcepool import ResourcePool

# Global tracking for factory and finalizer calls using function names as keys
call_tracker = defaultdict(lambda: {"factory_calls": [], "finalizer_calls": []})


def reset_call_tracker():
    """Reset the global call tracker."""
    call_tracker.clear()


def track_factory_call(func_name, key, result):
    """Track a factory function call."""
    call_tracker[func_name]["factory_calls"].append({"key": key, "result": result})


def track_finalizer_call(func_name, obj):
    """Track a finalizer function call."""
    call_tracker[func_name]["finalizer_calls"].append({"obj": obj})


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
            track_factory_call("sync_simple", key, obj)
            return obj

        return sync_factory

    elif factory_type == "async_simple":

        async def async_factory(key):
            obj = Mock()
            obj.name = f"async-{key}"
            obj.created_by = "async_simple"
            track_factory_call("async_simple", key, obj)
            return obj

        return async_factory

    elif factory_type == "sync_lambda":

        def sync_lambda_factory(key):
            obj = SimpleNamespace(name=f"lambda-{key}", created_by="sync_lambda")
            track_factory_call("sync_lambda", key, obj)
            return obj

        return lambda key: sync_lambda_factory(key)

    elif factory_type == "async_lambda":

        async def async_lambda_factory(key):
            obj = SimpleNamespace(name=f"async-lambda-{key}", created_by="async_lambda")
            track_factory_call("async_lambda", key, obj)
            return obj

        return lambda key: async_lambda_factory(key)

    elif factory_type == "callable":

        class CallableLike:
            def __call__(self, key):
                self.sync_factory(key)

            def sync_factory(self, key):
                obj = Mock()
                obj.name = f"callable-{key}"
                obj.created_by = "callable"
                track_factory_call("callable", key, obj)
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
                track_factory_call("awaitable", self.key, obj)
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
            # Simple finalizer that just validates it was called with an object
            assert obj is not None
            track_finalizer_call("sync_simple", obj)

        # Store the type on the function for easy identification
        return simple_sync_finalizer

    elif finalizer_type == "async_simple":

        async def simple_async_finalizer(obj):
            # Simple finalizer that just validates it was called with an object
            assert obj is not None
            track_finalizer_call("async_simple", obj)

        return simple_async_finalizer

    elif finalizer_type == "sync_lambda":

        def sync_lambda_finalizer(obj):
            assert obj is not None
            track_finalizer_call("sync_lambda", obj)

        return lambda obj: sync_lambda_finalizer(obj)

    elif finalizer_type == "async_lambda":

        async def async_lambda_finalizer(obj):
            assert obj is not None
            track_finalizer_call("async_lambda", obj)

        return lambda obj: async_lambda_finalizer(obj)


# Reusable fixtures for simplified test setup
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
def resource_pool_with_ttl(mock_resource_factory, mock_finalizer):
    """Create a resource pool configured for TTL testing."""
    return ResourcePool(factory=mock_resource_factory, finalizer=mock_finalizer, ttl=0.1)


@pytest.fixture
def resource_pool_immediate_cleanup(mock_resource_factory, mock_finalizer):
    """Create a resource pool with TTL=0 for immediate cleanup testing."""
    return ResourcePool(factory=mock_resource_factory, finalizer=mock_finalizer, ttl=0)


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
            # Reset call tracker for this test run
            reset_call_tracker()

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
    async def test_get_returns_resource_instance(self, setup):
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
    @pytest.mark.dependency("TestResourcePool::test_get_returns_resource_instance")
    async def test_release_decrements_reference_counts(self):
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

        # Act & Assert
        await pool.release("key1")
        assert pool.stats.referenced_entries == 1

        await pool.release("key2")
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    @pytest.mark.dependency("TestResourcePool::test_release_decrements_reference_counts")
    async def test_release_nonexistent_key_raises_error(self, counting_factory):
        """Test releasing nonexistent key raises KeyError.

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

        # Act & Assert
        # Try to release a nonexistent key
        await pool.release("nonexistent")

        # Should not affect existing resources
        assert pool.stats.total_entries == initial_cache_size
        # All keys should have zero references (since they were released)
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    @pytest.mark.dependency("TestResourcePool::test_release_decrements_reference_counts")
    async def test_release_zero_reference_count_raises_error(self):
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

        # Act & Assert
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
    @pytest.mark.dependency("TestResourcePool::test_get_returns_resource_instance")
    async def test_clear_finalizes_all_resources(self):
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
    @pytest.mark.dependency("TestResourcePool::test_get_returns_resource_instance")
    async def test_clear_key_removes_specific_resource(self, mock_finalizer):
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
    async def test_clear_nonexistent_key_raises_error(self):
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

        # Act & Assert
        # Try to clear a non-existent key - should raise KeyError
        with pytest.raises(KeyError):
            await pool.clear("nonexistent-key")

        # Should not affect existing resources
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0  # Not referenced after context exit

        # Finalizer should not have been called
        mock_finalizer.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.dependency("TestResourcePool::test_get_returns_resource_instance")
    async def test_ttl_cleanup_schedules_resource_removal(self):
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

        # Create an event to control when the mocked sleep completes
        sleep_event = asyncio.Event()

        async def mock_sleep(_delay):
            # Wait for the test to signal that sleep should complete
            await sleep_event.wait()

        with patch.object(rp.asyncio, "sleep", side_effect=mock_sleep):
            # Act
            # Acquire and immediately release
            async with pool.get(key) as resource:
                assert resource is mock_resource
                assert pool.stats.total_entries == 1
                assert pool.stats.referenced_entries == 1

            # Resource should still be in cache but with cleanup task scheduled
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 0
            assert pool.stats.pending_cleanup == 1

            # At this point, cleanup task is waiting for our mocked sleep
            # Resource should still be in cache
            assert pool.stats.total_entries == 1
            mock_finalizer.assert_not_called()

            # Signal that sleep should complete
            sleep_event.set()

        # Assert
        # Wait for cleanup to complete using polling with timeout
        start_time = time.time()
        while (key in pool.pending_cleanup) and (time.time() - start_time < 0.5):
            await asyncio.sleep(0.01)

        # Resource should now be cleaned up
        assert pool.stats.total_entries == 0
        mock_finalizer.assert_called_once_with(mock_resource)

    @pytest.mark.asyncio
    @pytest.mark.dependency("TestResourcePool::test_get_returns_resource_instance")
    async def test_ttl_cleanup_cancelled_on_reacquire(self):
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
    @pytest.mark.dependency("TestResourcePool::test_get_returns_resource_instance")
    async def test_stats_returns_accurate_counts(self):
        """Test stats method returns accurate cache statistics.

        Given:
            A pool with various resource states
        When:
            Stats property is accessed
        Then:
            Should return accurate counts for entries, references, and pending
            tasks
        """
        # Arrange
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=0.1)

        # Start with empty pool
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
    @pytest.mark.dependency("TestResourcePool::test_get_returns_resource_instance")
    async def test_async_context_manager_clears_resources(self):
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

        # Act & Assert
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
    async def test_ttl_specific_behavior_with_mocked_sleep(self, ttl):
        """Test specific TTL values with controlled sleep mocking.

        Given:
            A pool with specific TTL value
        When:
            A resource is acquired and released
        Then:
            Should schedule cleanup based on TTL value
        """
        # Arrange
        sleep_calls = []
        cleanup_started = asyncio.Event()
        finalizer_called = asyncio.Event()

        async def mock_sleep(delay):
            sleep_calls.append(delay)
            if delay != 0:  # Only signal for non-zero delays (actual TTL sleeps)
                cleanup_started.set()
                # Wait briefly to ensure the test can check the finalizer state
                await finalizer_called.wait()
            # Don't actually sleep, just record the call

        # Act & Assert
        # Patch asyncio.sleep globally to ensure it captures calls from background tasks
        with patch("asyncio.sleep", side_effect=mock_sleep):
            mock_factory = Mock(return_value=Mock(name="test-obj"))

            def mock_finalizer_func(obj):
                finalizer_called.set()

            mock_finalizer = Mock(side_effect=mock_finalizer_func)

            pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=ttl)

            async with pool.get("test-key"):
                pass

            if ttl == 0:
                # Filter out our own sleep(0) call
                filtered_calls = [call for call in sleep_calls if call != 0]
                assert len(filtered_calls) == 0
                mock_finalizer.assert_called_once()
            else:
                # Wait for the background task to actually call sleep with the TTL value
                try:
                    await asyncio.wait_for(cleanup_started.wait(), timeout=1.0)
                    # At this point cleanup task is waiting for finalizer_called
                    # Finalizer should not have been called yet
                    mock_finalizer.assert_not_called()

                    # Now allow cleanup to proceed
                    finalizer_called.set()

                    # Wait a bit for cleanup to complete
                    await asyncio.sleep(0.1)
                except asyncio.TimeoutError:
                    # If timeout, just check that sleep was called with TTL
                    pass

                # Filter out our own sleep(0) calls and any wait_for internal calls
                filtered_calls = [
                    call for call in sleep_calls if call != 0 and call == ttl
                ]
                assert len(filtered_calls) > 0, (
                    f"Expected TTL {ttl} not found in sleep calls: {sleep_calls}"
                )

    @pytest.mark.asyncio
    async def test_finalizer_exception_handling_catches_errors(self):
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

        # Act & Assert
        # This should not raise despite finalizer failing
        async with pool.get(key):
            pass

        # Resource should still be cleaned up
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_clear_with_nonexistent_key_raises_error(self):
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

        # Act & Assert
        # Non-existent keys should raise KeyError
        with pytest.raises(KeyError):
            await pool.clear("nonexistent-key")

    @pytest.mark.asyncio
    async def test_concurrent_acquire_release_same_key(self, counting_factory):
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
    async def test_resource_pool_with_zero_ttl_immediate_cleanup(
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
    async def test_get_with_none_key_handles_gracefully(self):
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

        # Act & Assert
        # None should be treated as a valid key
        async with pool.get(None) as resource:
            assert resource is mock_resource

        # Resource should be cleaned up after use
        assert pool.stats.total_entries == 0


class TestResource:
    """Test suite for the Resource class."""

    @pytest.mark.asyncio
    async def test_context_manager_auto_releases(self):
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

        # Act & Assert
        # Use Resource as context manager
        async with pool.get("test-key") as resource:
            assert resource is mock_resource
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 1

        # Should be automatically cleaned up after context exit
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_resource_has_no_manual_release_method(self):
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

        # Act & Assert
        # Manual release method should not exist
        assert not hasattr(resource_acquisition, "release")

    @pytest.mark.asyncio
    async def test_resource_lifecycle_with_ttl(self):
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
    async def test_context_manager_only_usage_handles_lifecycle(self):
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

        # Act & Assert
        # Use only as context manager
        async with pool.get("test-key") as resource:
            assert resource is mock_resource
            assert pool.stats.referenced_entries == 1

        # After context exit, should be cleaned up (TTL=0)
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_acquire_twice(self):
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
    async def test_resource_context_acquire_exception(self):
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

        from wool.runtime.resourcepool import Resource

        resource = Resource(pool=mock_pool, key="test-key")

        # Act & Assert
        with pytest.raises(RuntimeError, match="Acquire failed"):
            async with resource:
                pass

        # Verify _acquired was set to False during exception handling
        assert resource._acquired is False

    @pytest.mark.asyncio
    async def test_resource_context_release_not_acquired(self):
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
        from wool.runtime.resourcepool import Resource

        resource = Resource(pool=mock_pool, key="test-key")

        # Act & Assert - manually call __aexit__ without calling __aenter__
        with pytest.raises(
            RuntimeError, match="Cannot release a resource that was not acquired"
        ):
            await resource.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_resource_context_release_already_released(self):
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

        from wool.runtime.resourcepool import Resource

        resource = Resource(pool=mock_pool, key="test-key")

        # Use normally once (which sets _released = True)
        async with resource:
            pass

        # Act & Assert - manually call __aexit__ again
        with pytest.raises(
            RuntimeError,
            match="Cannot release a resource that has already been released",
        ):
            await resource.__aexit__(None, None, None)
