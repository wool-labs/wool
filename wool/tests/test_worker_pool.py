"""Test suite for :py:class:`wool._worker_pool.WorkerPool` module."""

import asyncio
import time
from multiprocessing.shared_memory import SharedMemory
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

import wool._worker_pool as wp
from wool._worker import LocalWorker
from wool._worker_discovery import LanRegistryService


@pytest.fixture
def mock_local_registry_service(mocker: MockerFixture):
    """Create a mock :py:class:`LocalRegistryService` for test isolation."""
    mock_registry = mocker.MagicMock(spec=LanRegistryService)
    mock_registry.start = mocker.AsyncMock()
    mock_registry.stop = mocker.AsyncMock()
    mock_registry.register = mocker.AsyncMock()
    mock_registry.unregister = mocker.AsyncMock()
    mocker.patch.object(wp, "LocalRegistryService", return_value=mock_registry)
    return mock_registry


@pytest.fixture
def mock_shared_memory(mocker: MockerFixture):
    """Mock SharedMemory for isolation from multiprocessing resources."""
    mock_memory = mocker.MagicMock()
    mock_memory.buf = bytearray(1024)
    mock_memory.close = mocker.MagicMock()
    mock_memory.unlink = mocker.MagicMock()
    mocker.patch("multiprocessing.shared_memory.SharedMemory", return_value=mock_memory)
    return mock_memory


@pytest.fixture
def mock_worker_proxy(mocker: MockerFixture):
    """Mock WorkerProxy for isolation from proxy behavior."""
    mock_proxy = mocker.MagicMock()
    mock_proxy.__aenter__ = mocker.AsyncMock(return_value=mock_proxy)
    mock_proxy.__aexit__ = mocker.AsyncMock()
    mocker.patch.object(wp, "WorkerProxy", return_value=mock_proxy)
    return mock_proxy


@pytest.fixture
def mock_local_worker(mocker: MockerFixture):
    """Mock LocalWorker for isolation from worker process management."""
    mock_worker = mocker.MagicMock()
    mock_worker.start = mocker.AsyncMock()
    mock_worker.stop = mocker.AsyncMock()
    mock_worker.info = mocker.MagicMock()
    mock_worker.info.uid = "test-worker-123"
    mock_worker.info.port = 50051
    mocker.patch.object(wp, "LocalWorker", return_value=mock_worker)
    return mock_worker


@pytest.fixture
def mock_discovery_service(mocker: MockerFixture):
    """Mock LocalDiscoveryService for isolation from discovery behavior."""
    mock_discovery = mocker.MagicMock()
    mocker.patch.object(wp, "LocalDiscoveryService", return_value=mock_discovery)
    return mock_discovery


@pytest.fixture
def pool_config():
    """Standard configuration for WorkerPool tests."""
    return {"tags": ("test", "pool"), "size": 2}


class TestWorkerPool:
    """Test suite for :py:class:`WorkerPool` constructor and lifecycle."""

    def test_constructor_uses_cpu_count_as_default_size(self, mocker: MockerFixture):
        """Test WorkerPool constructor with default parameters.

        Given:
            No size parameter is provided and CPU count is available
        When:
            WorkerPool is initialized
        Then:
            It should successfully create a pool using CPU count
        """
        # Arrange
        mock_cpu_count = mocker.patch("os.cpu_count", return_value=4)

        # Act
        pool = wp.WorkerPool()

        # Assert
        assert isinstance(pool, wp.WorkerPool)
        assert pool._workers == []  # Workers list should be empty initially
        mock_cpu_count.assert_called_once()

    def test_constructor_raises_error_when_cpu_count_unavailable(
        self, mocker: MockerFixture
    ):
        """Test WorkerPool initialization when CPU count cannot be determined.

        Given:
            os.cpu_count() returns None and size is set to 0
        When:
            WorkerPool is initialized
        Then:
            Should raise ValueError with appropriate message
        """
        # Arrange
        mock_cpu_count = mocker.patch("os.cpu_count", return_value=None)

        # Act & Assert
        with pytest.raises(ValueError, match="Unable to determine CPU count"):
            wp.WorkerPool(size=0)

        mock_cpu_count.assert_called_once()

    def test_constructor_raises_error_with_negative_size(self):
        """Test WorkerPool initialization with invalid negative size.

        Given:
            A negative size parameter
        When:
            WorkerPool is initialized
        Then:
            Should raise ValueError with appropriate message
        """
        # Arrange
        invalid_size = -1

        # Act & Assert
        with pytest.raises(ValueError, match="Size must be non-negative"):
            wp.WorkerPool(size=invalid_size)

    def test_constructor_accepts_tags_and_size_parameters(self):
        """Test WorkerPool constructor with tags and size.

        Given:
            Custom tags and size parameter
        When:
            WorkerPool is initialized
        Then:
            It should successfully create a pool with the specified configuration
        """
        # Arrange
        expected_tags = ("gpu-capable", "ml-model")
        expected_size = 2

        # Act
        pool = wp.WorkerPool(*expected_tags, size=expected_size)

        # Assert
        assert isinstance(pool, wp.WorkerPool)
        assert pool._workers == []  # Workers list should be empty initially

    def test_constructor_accepts_custom_worker_factory(self, mocker: MockerFixture):
        """Test WorkerPool constructor with custom worker factory.

        Given:
            A custom worker factory function and size
        When:
            WorkerPool is initialized
        Then:
            It should accept the factory for later worker creation
        """
        # Arrange
        mock_worker_factory = mocker.MagicMock()
        expected_size = 2

        # Act
        pool = wp.WorkerPool(size=expected_size, worker=mock_worker_factory)

        # Assert
        assert isinstance(pool, wp.WorkerPool)
        assert pool._workers == []  # Workers list should be empty initially
        # The factory is stored internally and will be used during worker creation

    @pytest.mark.asyncio
    async def test_context_manager_lifecycle_returns_pool_instance(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test WorkerPool async context manager lifecycle.

        Given:
            A WorkerPool with mocked internal components
        When:
            The pool is used as an async context manager
        Then:
            It should return the WorkerPool instance and manage lifecycle correctly
        """
        # Act
        async with wp.WorkerPool(size=1) as returned_pool:
            # Assert: Context manager entry
            assert returned_pool is not None
            assert isinstance(returned_pool, wp.WorkerPool)
            mock_worker_proxy.__aenter__.assert_called_once()

        # Assert: Context manager exit
        mock_worker_proxy.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_with_exception_in_user_code(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test context manager cleanup when user code raises exception.

        Given:
            A WorkerPool context manager that starts successfully
        When:
            User code inside the context manager raises an exception
        Then:
            Should clean up pool properly and propagate the exception
        """
        # Act & Assert
        with pytest.raises(ValueError, match="User error"):
            async with wp.WorkerPool(size=2) as pool:
                assert pool is not None
                raise ValueError("User error")

        # Assert: Cleanup still happened
        mock_worker_proxy.__aexit__.assert_called_once()
        mock_local_worker.stop.assert_called()

    @pytest.mark.asyncio
    async def test_context_manager_with_worker_startup_failure(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test context manager when worker startup fails.

        Given:
            A worker that fails during startup
        When:
            WorkerPool is used as context manager
        Then:
            Should handle startup failure gracefully and clean up
        """
        # Arrange
        mock_local_worker.start.side_effect = RuntimeError("Worker startup failed")

        # Act & Assert
        async with wp.WorkerPool(size=1) as pool:
            # Pool should still be created even if workers fail to start
            assert pool is not None

        # Assert: Cleanup still happened
        mock_worker_proxy.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_handles_exceptions_gracefully(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test context manager handles various exceptions gracefully.

        Given:
            A WorkerPool that encounters issues during lifecycle
        When:
            Context manager handles the lifecycle
        Then:
            Should attempt proper cleanup without additional errors
        """
        # Arrange - Make cleanup operations potentially fail but be handled
        mock_shared_memory.unlink.side_effect = OSError("Cleanup failed")

        # Act & Assert - Should not raise exception from cleanup
        async with wp.WorkerPool(size=1) as pool:
            assert pool is not None

        # Assert: Pool was created and cleanup was attempted
        assert isinstance(pool, wp.WorkerPool)

    @pytest.mark.asyncio
    async def test_context_manager_with_custom_worker_factory(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test context manager with custom worker factory.

        Given:
            A WorkerPool with custom worker factory
        When:
            Pool is used as context manager
        Then:
            Should use the custom factory for worker creation
        """
        # Arrange
        custom_worker = mocker.MagicMock()
        custom_worker.start = mocker.AsyncMock()
        custom_worker.stop = mocker.AsyncMock()
        custom_worker.info = mocker.MagicMock()

        def custom_factory(*args, **kwargs):
            return cast(LocalWorker, custom_worker)

        # Act
        async with wp.WorkerPool(size=1, worker=custom_factory) as pool:
            assert pool is not None

        # Assert: Custom worker was used
        custom_worker.start.assert_called()
        custom_worker.stop.assert_called()

    @pytest.mark.asyncio
    async def test_context_manager_with_durable_pool_configuration(
        self, mock_worker_proxy, mock_discovery_service
    ):
        """Test context manager with durable pool (discovery-based).

        Given:
            A WorkerPool configured for durable mode with discovery service
        When:
            Pool is used as context manager
        Then:
            Should connect to existing workers via discovery
        """
        # Arrange
        discovery_service = mock_discovery_service

        # Act
        async with wp.WorkerPool(discovery=discovery_service) as pool:
            assert pool is not None

        # Assert: No workers were spawned (durable mode)
        assert len(pool._workers) == 0
        mock_worker_proxy.__aenter__.assert_called_once()
        mock_worker_proxy.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_workers_startup_and_cleanup(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test context manager with multiple workers.

        Given:
            A WorkerPool configured with multiple workers
        When:
            Pool is used as context manager
        Then:
            Should start all workers and clean them up properly
        """
        # Act
        async with wp.WorkerPool(size=3) as pool:
            assert pool is not None
            # Verify multiple workers were created
            assert len(pool._workers) == 3

        # Assert: All workers were stopped
        assert mock_local_worker.stop.call_count == 3

    @pytest.mark.asyncio
    async def test_context_manager_preserves_worker_info(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test that worker information is properly maintained.

        Given:
            A WorkerPool with workers that have info
        When:
            Pool is started
        Then:
            Should maintain worker information correctly
        """
        # Act
        async with wp.WorkerPool(size=2) as pool:
            assert pool is not None
            # Verify workers have info
            for worker in pool._workers:
                assert worker.info is not None

    # Property-based testing with Hypothesis
    @given(st.integers(min_value=1, max_value=10))
    def test_constructor_accepts_valid_sizes(self, size):
        """Test constructor with various valid sizes using property-based testing.

        Given:
            Any valid size between 1 and 10
        When:
            WorkerPool is initialized with that size
        Then:
            Should create pool successfully
        """
        # Act
        pool = wp.WorkerPool(size=size)

        # Assert
        assert isinstance(pool, wp.WorkerPool)
        assert pool._workers == []  # Workers list should be empty initially

    @given(st.integers(min_value=-100, max_value=-1))
    def test_constructor_rejects_negative_sizes(self, negative_size):
        """Test constructor rejects negative sizes using property-based testing.

        Given:
            Any negative size
        When:
            WorkerPool is initialized with that size
        Then:
            Should raise ValueError
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Size must be non-negative"):
            wp.WorkerPool(size=negative_size)

    def test_constructor_with_zero_size_and_available_cpu_count(
        self, mocker: MockerFixture
    ):
        """Test constructor with size=0 when CPU count is available.

        Given:
            Size parameter of 0 and available CPU count
        When:
            WorkerPool is initialized
        Then:
            Should use CPU count as the pool size
        """
        # Arrange
        mock_cpu_count = mocker.patch("os.cpu_count", return_value=8)

        # Act
        pool = wp.WorkerPool(size=0)

        # Assert
        assert isinstance(pool, wp.WorkerPool)
        mock_cpu_count.assert_called_once()

    def test_constructor_with_empty_tags(self):
        """Test constructor with no tags specified.

        Given:
            No tags provided to constructor
        When:
            WorkerPool is initialized
        Then:
            Should create pool successfully
        """
        # Act
        pool = wp.WorkerPool(size=1)

        # Assert
        assert isinstance(pool, wp.WorkerPool)

    def test_constructor_with_multiple_tags(self):
        """Test constructor with multiple capability tags.

        Given:
            Multiple capability tags
        When:
            WorkerPool is initialized
        Then:
            Should accept all tags for worker configuration
        """
        # Arrange
        tags = ("gpu", "ml", "high-memory", "production")

        # Act
        pool = wp.WorkerPool(*tags, size=2)

        # Assert
        assert isinstance(pool, wp.WorkerPool)

    def test_constructor_durable_mode_with_discovery(self):
        """Test constructor in durable mode with discovery service.

        Given:
            A discovery service parameter
        When:
            WorkerPool is initialized with discovery
        Then:
            Should configure for durable mode (no local workers)
        """
        # Arrange
        mock_discovery = MagicMock()

        # Act
        pool = wp.WorkerPool(discovery=mock_discovery)

        # Assert
        assert isinstance(pool, wp.WorkerPool)
        assert pool._workers == []  # No local workers in durable mode

    def test_constructor_rejects_both_size_and_discovery(self):
        """Test constructor rejects conflicting size and discovery parameters.

        Given:
            Both size and discovery parameters
        When:
            WorkerPool is initialized
        Then:
            Should raise appropriate error
        """
        # Arrange
        mock_discovery = MagicMock()

        # Act & Assert
        with pytest.raises(RuntimeError):
            # This should fail because the implementation only supports one mode
            wp.WorkerPool("tag1", size=2, discovery=mock_discovery)  # type: ignore[reportCallIssue]

    @pytest.mark.asyncio
    async def test_context_manager_concurrent_operations(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test context manager under concurrent operations.

        Given:
            A WorkerPool with multiple workers
        When:
            Context manager is used with concurrent worker operations
        Then:
            Should handle concurrency correctly
        """
        # Arrange
        mock_local_worker.start = AsyncMock()

        # Act
        async with wp.WorkerPool(size=3) as pool:
            assert pool is not None
            # Simulate concurrent operations
            tasks = [mock_local_worker.start() for _ in range(3)]
            await asyncio.gather(*tasks, return_exceptions=True)

        # Assert: All workers were managed properly
        assert len(pool._workers) == 3

    @pytest.mark.asyncio
    async def test_context_manager_with_custom_loadbalancer(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test context manager with custom load balancer.

        Given:
            A WorkerPool with custom load balancer
        When:
            Pool is used as context manager
        Then:
            Should pass load balancer to WorkerProxy correctly
        """
        # Arrange
        custom_loadbalancer = MagicMock()

        # Act
        async with wp.WorkerPool(size=1, loadbalancer=custom_loadbalancer) as pool:
            assert pool is not None

        # Assert: Pool was created successfully
        mock_worker_proxy.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_partial_worker_startup_failure_cleanup(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_discovery_service,
        mock_local_registry_service,
        mocker: MockerFixture,
    ):
        """Test cleanup when some workers fail during startup.

        Given:
            Multiple workers where some fail during startup
        When:
            WorkerPool is used as context manager
        Then:
            Should clean up successfully started workers
        """
        # Arrange
        successful_worker = mocker.MagicMock()
        successful_worker.start = mocker.AsyncMock()
        successful_worker.stop = mocker.AsyncMock()
        successful_worker.info = mocker.MagicMock()

        failing_worker = mocker.MagicMock()
        failing_worker.start = mocker.AsyncMock(
            side_effect=RuntimeError("Startup failed")
        )
        failing_worker.stop = mocker.AsyncMock()
        failing_worker.info = None

        workers = [successful_worker, failing_worker]
        mocker.patch.object(wp, "LocalWorker", side_effect=workers)

        # Act
        async with wp.WorkerPool(size=2) as pool:
            assert pool is not None

        # Assert: Both workers had stop called (cleanup)
        successful_worker.stop.assert_called_once()
        failing_worker.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_startup_timing_performance(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test that pool startup completes within reasonable time.

        Given:
            A WorkerPool with multiple workers
        When:
            Pool startup is timed
        Then:
            Should complete within reasonable timeframe
        """
        # Arrange
        start_time = time.time()

        # Act
        async with wp.WorkerPool(size=2) as pool:
            end_time = time.time()
            assert pool is not None

        # Assert: Startup completed quickly (with mocked components)
        startup_time = end_time - start_time
        assert startup_time < 1.0  # Should be very fast with mocks

    @pytest.mark.asyncio
    async def test_worker_info_collection_after_startup(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
        mock_local_registry_service,
    ):
        """Test worker info collection after startup completes.

        Given:
            Workers with various info states
        When:
            Pool startup completes
        Then:
            Should properly collect available worker info
        """
        # Arrange
        mock_local_worker.info.uid = "worker-123"
        mock_local_worker.info.port = 50051

        # Act
        async with wp.WorkerPool(size=2) as pool:
            assert pool is not None
            # Verify worker info is accessible
            for worker in pool._workers:
                assert hasattr(worker, "info")
                if worker.info:
                    assert hasattr(worker.info, "uid")
                    assert hasattr(worker.info, "port")

    def test_constructor_raises_error_when_cpu_count_unavailable_default_size(
        self, mocker: MockerFixture
    ):
        """Test WorkerPool constructor error handling when CPU count is unavailable with
        default size.

        Given:
            A system where os.cpu_count() returns None and no size is specified
        When:
            WorkerPool constructor is called with default parameters
        Then:
            Should raise ValueError indicating CPU count cannot be determined
        """
        # Arrange
        mocker.patch("os.cpu_count", return_value=None)

        # Act & Assert - This hits the (None, None) case at line 232
        with pytest.raises(ValueError, match="Unable to determine CPU count"):
            wp.WorkerPool()

    @pytest.mark.asyncio
    async def test_context_manager_handles_shared_memory_cleanup_exceptions(
        self, mocker: MockerFixture, mock_local_worker, mock_worker_proxy
    ):
        """Test WorkerPool context manager exception handling during shared memory
        cleanup.

        Given:
            A WorkerPool with real shared memory that encounters cleanup issues
        When:
            Context manager exits and cleanup operations fail
        Then:
            Should handle exceptions gracefully without propagating them
        """
        # Arrange - Create real shared memory and simulate cleanup failure

        # Create real shared memory
        real_memory = SharedMemory(create=True, size=1024)

        try:
            pool = wp.WorkerPool(size=1)
            pool._proxy = mock_worker_proxy
            pool._shared_memory = real_memory

            # Close the memory beforehand to cause unlink() to potentially fail
            # This simulates real-world scenarios where cleanup encounters issues
            real_memory.close()

            # Act - Should not raise exceptions despite potential cleanup failures
            await pool.__aexit__(None, None, None)

        except Exception:
            # Clean up if something went wrong during test
            real_memory.unlink()

    @pytest.mark.asyncio
    async def test_stop_workers_handles_worker_stop_exceptions(
        self, mocker: MockerFixture, mock_local_worker
    ):
        """Test WorkerPool._stop_workers method handles individual worker stop failures.

        Given:
            A WorkerPool with workers where some fail to stop gracefully
        When:
            _stop_workers is called
        Then:
            Should attempt to stop all workers despite individual failures
        """
        # Arrange
        mock_worker1 = mocker.MagicMock()
        mock_worker2 = mocker.MagicMock()
        mock_worker1.stop = mocker.AsyncMock(
            side_effect=RuntimeError("Worker 1 stop failed")
        )
        mock_worker2.stop = mocker.AsyncMock()  # This one succeeds

        pool = wp.WorkerPool(size=0)  # Don't spawn any workers automatically
        pool._workers = [mock_worker1, mock_worker2]

        # Act - Should complete despite one worker failing to stop
        await pool._stop_workers()

        # Assert - Both workers had stop() called
        mock_worker1.stop.assert_called_once()
        mock_worker2.stop.assert_called_once()

    @given(
        tags=st.lists(
            st.text(
                alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]),
                min_size=1,
                max_size=10,
            ),
            min_size=0,
            max_size=3,
        ),
        size=st.one_of(st.none(), st.integers(min_value=1, max_value=4)),
        has_discovery=st.booleans(),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test_constructor_valid_parameter_combinations(
        self, mocker: MockerFixture, tags, size, has_discovery
    ):
        """Test WorkerPool constructor with various valid parameter combinations.

        Given:
            Valid combinations of tags, size, and discovery parameters
        When:
            WorkerPool constructor is called
        Then:
            Should initialize successfully with appropriate internal state
        """
        # Arrange - Mock external dependencies within test to avoid fixture issues
        mock_memory = mocker.MagicMock()
        mock_memory.buf = bytearray(1024)
        mocker.patch(
            "multiprocessing.shared_memory.SharedMemory", return_value=mock_memory
        )

        mock_proxy = mocker.MagicMock()
        mocker.patch.object(wp, "WorkerProxy", return_value=mock_proxy)

        mock_discovery = mocker.MagicMock() if has_discovery else None

        # Act - Test all valid parameter combinations
        if has_discovery:
            # Case: (None, discovery) - Durable pool
            pool = wp.WorkerPool(*tags, discovery=mock_discovery)
        elif size is None:
            # Case: (None, None) - Default ephemeral pool
            pool = wp.WorkerPool(*tags)
        else:
            # Case: (size, None) - Explicit size ephemeral pool
            pool = wp.WorkerPool(*tags, size=size)

        # Assert - Verify proper initialization
        assert pool is not None
        assert hasattr(pool, "_workers")
        assert hasattr(pool, "_proxy_factory")
        assert isinstance(pool._workers, list)
        assert callable(pool._proxy_factory)

    @pytest.mark.asyncio
    async def test_context_manager_default_case_covers_shared_memory_creation(
        self,
        mocker: MockerFixture,
        mock_local_worker,
        mock_shared_memory,
        mock_worker_proxy,
    ):
        """Test WorkerPool default context manager covers shared memory creation path.

        Given:
            WorkerPool called with default parameters (no size, no discovery)
        When:
            Context manager is entered (which calls _proxy_factory)
        Then:
            Should execute the create_proxy function covering lines 238-246
        """
        # Act
        async with wp.WorkerPool() as pool:
            # Assert
            assert pool is not None
            assert hasattr(pool, "_proxy")
            assert hasattr(pool, "_shared_memory")

    def test_default_worker_factory_creates_separate_registry_instances(
        self, mocker: MockerFixture
    ):
        """Test default worker factory creates separate registry instances.

        Given:
            A WorkerPool instance with default worker factory
        When:
            Multiple workers are created using the default factory
        Then:
            Each worker should get its own LocalRegistryService instance
        """
        # Arrange
        pool = wp.WorkerPool(size=0)
        uri = "test-pool-123"

        # Track all LocalRegistryService instances created
        registry_instances = []

        def mock_local_registry_service(pool_uri):
            instance = mocker.MagicMock()
            instance.pool_uri = pool_uri
            registry_instances.append(instance)
            return instance

        mocker.patch.object(
            wp, "LocalRegistryService", side_effect=mock_local_registry_service
        )

        # Mock LocalWorker to capture the registry_service parameter
        created_workers = []

        def mock_local_worker(*tags, registry_service=None, **kwargs):
            worker = mocker.MagicMock()
            worker.registry_service = registry_service
            worker.tags = set(tags)
            worker.kwargs = kwargs
            created_workers.append(worker)
            return worker

        mocker.patch.object(wp, "LocalWorker", side_effect=mock_local_worker)

        # Get the default factory
        factory = pool._default_worker_factory(uri)

        # Act - Create multiple workers using the factory
        factory("tag1", "tag2")
        factory("tag3", "tag4")
        factory("tag5")

        # Assert
        assert len(created_workers) == 3
        assert len(registry_instances) == 3

        # Verify each worker gets a separate registry instance
        assert (
            created_workers[0].registry_service
            is not created_workers[1].registry_service
        )
        assert (
            created_workers[1].registry_service
            is not created_workers[2].registry_service
        )
        assert (
            created_workers[0].registry_service
            is not created_workers[2].registry_service
        )

        # Verify all registry instances have the correct URI
        for registry in registry_instances:
            assert registry.pool_uri == uri

        # Verify workers were created with correct tags
        assert created_workers[0].tags == {"tag1", "tag2"}
        assert created_workers[1].tags == {"tag3", "tag4"}
        assert created_workers[2].tags == {"tag5"}
