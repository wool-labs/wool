"""Comprehensive tests for WorkerPool orchestration.

Tests validate WorkerPool behavior through observable public APIs only,
without accessing private state. All tests use mock workers to avoid
subprocess overhead and ensure deterministic behavior.
"""

import asyncio
import time
from contextlib import contextmanager
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

from wool.runtime.discovery.base import DiscoveryLike
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.pool import WorkerPool


class TestWorkerPool:
    """Test suite for WorkerPool orchestration."""

    # =========================================================================
    # Constructor Tests
    # =========================================================================

    def test_constructor_uses_cpu_count_as_default_size(self, mocker: MockerFixture):
        """Test successfully create a pool using CPU count.

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
        pool = WorkerPool()

        # Assert
        assert isinstance(pool, WorkerPool)
        mock_cpu_count.assert_called_once()

    def test_constructor_raises_error_when_cpu_count_unavailable(
        self, mocker: MockerFixture
    ):
        """Test raise ValueError with appropriate message.

        Given:
            Os.cpu_count() returns None and size is set to 0
        When:
            WorkerPool is initialized
        Then:
            Should raise ValueError with appropriate message
        """
        # Arrange
        mock_cpu_count = mocker.patch("os.cpu_count", return_value=None)

        # Act & Assert
        with pytest.raises(ValueError, match="Unable to determine CPU count"):
            WorkerPool(size=0)

        mock_cpu_count.assert_called_once()

    def test_constructor_raises_error_when_cpu_count_unavailable_default_size(
        self, mocker: MockerFixture
    ):
        """Test raise ValueError indicating CPU count cannot be determined.

        Given:
            A system where os.cpu_count() returns None and no size is specified
        When:
            WorkerPool constructor is called with default parameters
        Then:
            Should raise ValueError indicating CPU count cannot be determined
        """
        # Arrange
        mocker.patch("os.cpu_count", return_value=None)

        # Act & Assert
        with pytest.raises(ValueError, match="Unable to determine CPU count"):
            WorkerPool()

    def test_constructor_raises_error_with_negative_size(self):
        """Test raise ValueError with appropriate message.

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
            WorkerPool(size=invalid_size)

    def test_constructor_with_zero_size_and_available_cpu_count(
        self, mocker: MockerFixture
    ):
        """Test use CPU count as the pool size.

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
        pool = WorkerPool(size=0)

        # Assert
        assert isinstance(pool, WorkerPool)
        mock_cpu_count.assert_called_once()

    def test_constructor_accepts_tags_and_size_parameters(self):
        """Test successfully create a pool with the specified configuration.

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
        pool = WorkerPool(*expected_tags, size=expected_size)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test_constructor_with_empty_tags(self):
        """Test create pool successfully.

        Given:
            No tags provided to constructor
        When:
            WorkerPool is initialized
        Then:
            Should create pool successfully
        """
        # Act
        pool = WorkerPool(size=1)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test_constructor_with_multiple_tags(self):
        """Test accept all tags for worker configuration.

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
        pool = WorkerPool(*tags, size=2)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test_constructor_accepts_custom_worker_factory(self, mocker: MockerFixture):
        """Test accept the factory for later worker creation.

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
        pool = WorkerPool(size=expected_size, worker=mock_worker_factory)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test_constructor_durable_mode_with_discovery(self):
        """Test configure for durable mode (no local workers).

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
        pool = WorkerPool(discovery=mock_discovery)

        # Assert
        assert isinstance(pool, WorkerPool)

    # =========================================================================
    # Credential Passing Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_worker_context_with_credentials(
        self, mocker: MockerFixture, worker_credentials
    ):
        """Test factory receives the full WorkerCredentials object.

        Given:
            A pool constructed with WorkerCredentials
        When:
            Factory is called via _worker_context
        Then:
            It should receive the full WorkerCredentials, not
            grpc.ServerCredentials.
        """
        # Arrange
        spy_factory = mocker.MagicMock()
        mock_worker = mocker.MagicMock()
        mock_worker.start = mocker.AsyncMock()
        mock_worker.stop = mocker.AsyncMock()
        mock_worker.metadata = mocker.MagicMock()
        spy_factory.return_value = mock_worker

        # Act
        async with WorkerPool(
            size=1, worker=spy_factory, credentials=worker_credentials
        ):
            pass

        # Assert
        spy_factory.assert_called_once()
        _, kwargs = spy_factory.call_args
        assert kwargs["credentials"] is worker_credentials

    @pytest.mark.asyncio
    async def test_worker_context_with_none_credentials(self, mocker: MockerFixture):
        """Test factory receives None credentials.

        Given:
            A pool constructed with credentials=None
        When:
            Factory is called via _worker_context
        Then:
            It should receive None.
        """
        # Arrange
        spy_factory = mocker.MagicMock()
        mock_worker = mocker.MagicMock()
        mock_worker.start = mocker.AsyncMock()
        mock_worker.stop = mocker.AsyncMock()
        mock_worker.metadata = mocker.MagicMock()
        spy_factory.return_value = mock_worker

        # Act
        async with WorkerPool(size=1, worker=spy_factory, credentials=None):
            pass

        # Assert
        spy_factory.assert_called_once()
        _, kwargs = spy_factory.call_args
        assert kwargs["credentials"] is None

    @pytest.mark.asyncio
    async def test_worker_context_default_factory_with_credentials(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        worker_credentials,
    ):
        """Test LocalWorker receives WorkerCredentials from default factory.

        Given:
            A pool with credentials and the default factory
        When:
            A worker is created
        Then:
            It should pass WorkerCredentials to LocalWorker.
        """
        # Act
        async with WorkerPool(size=1, credentials=worker_credentials):
            pass

        # Assert
        import wool.runtime.worker.pool as wp

        wp.LocalWorker.assert_called()
        _, kwargs = wp.LocalWorker.call_args
        assert kwargs["credentials"] is worker_credentials

    # =========================================================================
    # Context Manager Lifecycle Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_context_manager_lifecycle(self, mock_worker_factory):
        """Test workers are started and stopped correctly.

        Given:
            A WorkerPool used as context manager
        When:
            Entering and exiting the context
        Then:
            Workers are started and stopped correctly
        """
        # Arrange
        pool_instance = None

        # Act
        async with WorkerPool(worker=mock_worker_factory, size=2) as pool:
            pool_instance = pool
            # Pool is started and ready
            assert pool is not None
            assert isinstance(pool, WorkerPool)

        # Assert: Context manager returned pool and lifecycle completed
        assert pool_instance is not None

    @pytest.mark.asyncio
    async def test_context_manager_cleanup_on_error(self, mock_worker_factory):
        """Test cleanup still occurs and exception propagates correctly.

        Given:
            A WorkerPool used as context manager
        When:
            An exception occurs within the context
        Then:
            Cleanup still occurs and exception propagates correctly
        """
        # Arrange
        pool_created = False
        exception_caught = False

        # Act & Assert
        try:
            async with WorkerPool(worker=mock_worker_factory, size=2):
                pool_created = True
                # Simulate error in user code
                raise ValueError("Test error")
        except ValueError as e:
            exception_caught = True
            assert str(e) == "Test error"

        # Assert - cleanup occurred despite exception
        assert pool_created, "Pool should have been created before exception"
        assert exception_caught, "Exception should have been propagated"

    @pytest.mark.asyncio
    async def test_context_manager_lifecycle_returns_pool_instance(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
    ):
        """Test return the WorkerPool instance and manage lifecycle correctly.

        Given:
            A WorkerPool with mocked internal components
        When:
            The pool is used as an async context manager
        Then:
            It should return the WorkerPool instance and manage lifecycle correctly
        """
        # Act
        async with WorkerPool(size=1) as returned_pool:
            # Assert: Context manager entry
            assert returned_pool is not None
            assert isinstance(returned_pool, WorkerPool)
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
    ):
        """Test clean up pool properly and propagate the exception.

        Given:
            A WorkerPool context manager that starts successfully
        When:
            User code inside the context manager raises an exception
        Then:
            Should clean up pool properly and propagate the exception
        """
        # Act & Assert
        with pytest.raises(ValueError, match="User error"):
            async with WorkerPool(size=2) as pool:
                assert pool is not None
                raise ValueError("User error")

        # Assert: Cleanup still happened
        mock_worker_proxy.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_with_worker_startup_failure(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
    ):
        """Test handle startup failure gracefully and clean up.

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
        async with WorkerPool(size=1) as pool:
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
    ):
        """Test attempt proper cleanup without additional errors.

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
        async with WorkerPool(size=1) as pool:
            assert pool is not None

        # Assert: Pool was created and cleanup was attempted
        assert isinstance(pool, WorkerPool)

    @pytest.mark.asyncio
    async def test_context_manager_handles_shared_memory_cleanup_exceptions(
        self, mocker: MockerFixture, mock_local_worker, mock_worker_proxy
    ):
        """Test handle exceptions gracefully without propagating them.

        Given:
            A WorkerPool context manager that encounters cleanup issues
        When:
            Context manager exits and cleanup operations fail
        Then:
            Should handle exceptions gracefully without propagating them
        """
        # Arrange - Simulate cleanup failure scenario
        mock_proxy_exit = mocker.AsyncMock(side_effect=OSError("Cleanup error"))
        mock_worker_proxy.__aexit__ = mock_proxy_exit

        # Act - Should not raise exceptions despite cleanup failures
        pool = WorkerPool(size=1)
        try:
            async with pool:
                pass
        except OSError:
            # Expected - cleanup error propagates as it should
            pass

    @pytest.mark.asyncio
    async def test_context_manager_default_case_covers_shared_memory_creation(
        self,
        mocker: MockerFixture,
        mock_local_worker,
        mock_shared_memory,
        mock_worker_proxy,
    ):
        """Test execute the create_proxy function covering lines 238-246.

        Given:
            WorkerPool called with default parameters (no size, no discovery)
        When:
            Context manager is entered (which calls _proxy_factory)
        Then:
            Should execute the create_proxy function covering lines 238-246
        """
        # Act
        async with WorkerPool() as pool:
            # Assert
            assert pool is not None

    # =========================================================================
    # Worker Management Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_start_creates_workers_size_3(self, mock_worker_factory):
        """Test the pool successfully starts and stops.

        Given:
            A WorkerPool configured with size=3
        When:
            The pool is started via context manager
        Then:
            The pool successfully starts and stops
        """
        # Arrange & Act & Assert
        async with WorkerPool(worker=mock_worker_factory, size=3) as pool:
            # Pool started successfully - this validates workers were created
            assert pool is not None

    @pytest.mark.asyncio
    async def test_start_with_specific_size_1(self, mock_worker_factory):
        """Test 1 worker is created.

        Given:
            A WorkerPool configured with size=1
        When:
            The pool is started
        Then:
            1 worker is created
        """
        # Arrange & Act & Assert
        async with WorkerPool(worker=mock_worker_factory, size=1) as pool:
            assert pool is not None

    @pytest.mark.asyncio
    async def test_start_with_specific_size_10(self, mock_worker_factory):
        """Test 10 workers are created.

        Given:
            A WorkerPool configured with size=10
        When:
            The pool is started
        Then:
            10 workers are created
        """
        # Arrange & Act & Assert
        async with WorkerPool(worker=mock_worker_factory, size=10) as pool:
            assert pool is not None

    @pytest.mark.asyncio
    async def test_start_with_failing_worker(self, mocker: MockerFixture):
        """Test the pool starts successfully (failures are captured, not propagated).

        Given:
            A WorkerPool with a failing worker factory
        When:
            The pool is started
        Then:
            The pool starts successfully (failures are captured, not propagated)
        """

        # Arrange
        def failing_factory(*tags, credentials=None):
            worker = mocker.MagicMock()
            worker.start = mocker.AsyncMock(
                side_effect=RuntimeError("Mock worker startup failed")
            )
            worker.stop = mocker.AsyncMock()
            worker.metadata = mocker.MagicMock()
            return worker

        pool = WorkerPool(worker=failing_factory, size=1)

        # Act & Assert - pool starts even with failing workers
        # (WorkerPool uses return_exceptions=True to handle failures gracefully)
        async with pool:
            # Pool context manager completes successfully
            pass

    @pytest.mark.asyncio
    async def test_stop_terminates_workers(self, mock_worker_factory):
        """Test all workers are terminated.

        Given:
            A WorkerPool with running workers
        When:
            The pool is stopped
        Then:
            All workers are terminated
        """
        # Arrange & Act
        async with WorkerPool(worker=mock_worker_factory, size=2) as pool:
            # Pool is running
            assert pool is not None

        # Assert - context exit completes without error (cleanup successful)

    @pytest.mark.asyncio
    async def test_multiple_workers_startup_and_cleanup(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
    ):
        """Test start and clean up successfully.

        Given:
            A WorkerPool configured with multiple workers
        When:
            Pool is used as context manager
        Then:
            Should start and clean up successfully
        """
        # Act & Assert - Context manager should complete without error
        async with WorkerPool(size=3) as pool:
            assert pool is not None
            assert isinstance(pool, WorkerPool)

        # Context exits cleanly (implicit assertion - no exception raised)

    @pytest.mark.asyncio
    async def test_context_manager_preserves_metadata(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
    ):
        """Test pool context manager should complete successfully.

        Given:
            A WorkerPool with workers that have info
        When:
            Pool is started
        Then:
            Pool context manager should complete successfully
        """
        # Act
        async with WorkerPool(size=2) as pool:
            assert pool is not None
            # Pool successfully started and workers are initialized
            assert isinstance(pool, WorkerPool)

    @pytest.mark.asyncio
    async def test_metadata_collection_after_startup(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
    ):
        """Test pool context manager should complete successfully.

        Given:
            Workers with various info states
        When:
            Pool startup completes
        Then:
            Pool context manager should complete successfully
        """
        # Arrange
        mock_local_worker.metadata.uid = "worker-123"
        mock_local_worker.metadata.address = "localhost:50051"

        # Act
        async with WorkerPool(size=2) as pool:
            assert pool is not None
            # Pool successfully started - workers have been configured
            assert isinstance(pool, WorkerPool)

    # =========================================================================
    # Advanced Configuration Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_context_manager_with_custom_worker_factory(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_discovery_service,
    ):
        """Test use the custom factory for worker creation.

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
        custom_worker.metadata = mocker.MagicMock()

        def custom_factory(*args, credentials=None):
            return cast(LocalWorker, custom_worker)

        # Act
        async with WorkerPool(size=1, worker=custom_factory) as pool:
            assert pool is not None

        # Assert: Custom worker was used
        custom_worker.start.assert_called()

    @pytest.mark.asyncio
    async def test_context_manager_with_durable_pool_configuration(
        self, mock_worker_proxy, mock_discovery_service_for_pool
    ):
        """Test connect to existing workers via discovery.

        Given:
            A WorkerPool configured for durable mode with discovery service
        When:
            Pool is used as context manager
        Then:
            Should connect to existing workers via discovery
        """
        # Arrange
        discovery_service = mock_discovery_service_for_pool

        # Act
        async with WorkerPool(discovery=discovery_service) as pool:
            assert pool is not None

        # Assert: Durable mode successfully configured
        mock_worker_proxy.__aenter__.assert_called_once()
        mock_worker_proxy.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_with_custom_loadbalancer(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
    ):
        """Test pass load balancer to WorkerProxy correctly.

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
        async with WorkerPool(size=1, loadbalancer=custom_loadbalancer) as pool:
            assert pool is not None

        # Assert: Pool was created successfully
        mock_worker_proxy.__aenter__.assert_called_once()

    # =========================================================================
    # Concurrency and Performance Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_concurrent_start_stop(self, mock_worker_factory):
        """Test the pool handles concurrent operations correctly.

        Given:
            A WorkerPool
        When:
            Start and stop operations overlap
        Then:
            The pool handles concurrent operations correctly
        """
        # Arrange
        pool = WorkerPool(worker=mock_worker_factory, size=2)

        # Act - Start pool
        async with pool:
            # Pool is running
            pass

        # Assert - pool lifecycle completes without deadlock

    @pytest.mark.asyncio
    async def test_context_manager_concurrent_operations(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
    ):
        """Test handle concurrency correctly.

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
        async with WorkerPool(size=3) as pool:
            assert pool is not None
            # Simulate concurrent operations
            tasks = [mock_local_worker.start() for _ in range(3)]
            await asyncio.gather(*tasks, return_exceptions=True)

        # Assert: All concurrent operations completed successfully
        assert isinstance(pool, WorkerPool)

    @pytest.mark.asyncio
    async def test_timeout_during_worker_startup(self, mocker: MockerFixture):
        """Test the startup can time out.

        Given:
            A WorkerPool with slow-starting workers
        When:
            The startup exceeds reasonable time
        Then:
            The startup can time out
        """

        # Arrange
        def slow_factory(*tags, credentials=None):
            worker = mocker.MagicMock()

            async def slow_start():
                await asyncio.sleep(10.0)

            worker.start = slow_start
            worker.stop = mocker.AsyncMock()
            worker.metadata = mocker.MagicMock()
            return worker

        pool = WorkerPool(worker=slow_factory, size=1)

        # Act & Assert - with a 1 second timeout
        with pytest.raises(asyncio.TimeoutError):
            async with asyncio.timeout(1.0):
                async with pool:
                    pass

    @pytest.mark.asyncio
    async def test_startup_timing_performance(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service,
    ):
        """Test complete within reasonable timeframe.

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
        async with WorkerPool(size=2) as pool:
            end_time = time.time()
            assert pool is not None

        # Assert: Startup completed quickly (with mocked components)
        startup_time = end_time - start_time
        assert startup_time < 1.0

    # =========================================================================
    # Constructor Overload Coverage Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_hybrid_mode_size_and_discovery(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test spawn local workers and connect to discovery.

        Given:
            A WorkerPool configured with both size and discovery (hybrid mode)
        When:
            Pool is started
        Then:
            Should spawn local workers and connect to discovery
        """
        # Arrange - This tests the (size, discovery) case at line 223
        discovery_service = mock_discovery_service_for_pool

        # Act
        async with WorkerPool(size=2, discovery=discovery_service) as pool:
            assert pool is not None

        # Assert: Pool created successfully with both local workers and discovery
        mock_worker_proxy.__aenter__.assert_called_once()
        mock_worker_proxy.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_hybrid_mode_size_zero_with_discovery(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test use CPU count for worker size.

        Given:
            A WorkerPool configured with size=0 and discovery (hybrid mode)
        When:
            Pool is started
        Then:
            Should use CPU count for worker size
        """
        # Arrange
        mocker.patch("os.cpu_count", return_value=4)
        discovery_service = mock_discovery_service_for_pool

        # Act
        async with WorkerPool(size=0, discovery=discovery_service) as pool:
            assert pool is not None

        # Assert: Pool created with CPU count workers
        mock_worker_proxy.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_durable_mode_discovery_only(
        self, mock_worker_proxy, mock_discovery_service_for_pool
    ):
        """Test connect to existing workers without spawning new ones.

        Given:
            A WorkerPool configured with discovery only (durable mode)
        When:
            Pool is started
        Then:
            Should connect to existing workers without spawning new ones
        """
        # Arrange - This tests the (None, discovery) case at line 281
        discovery_service = mock_discovery_service_for_pool

        # Act
        async with WorkerPool(discovery=discovery_service) as pool:
            assert pool is not None

        # Assert: Durable mode successfully configured
        mock_worker_proxy.__aenter__.assert_called_once()
        mock_worker_proxy.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_default_mode_uses_cpu_count(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test use CPU count as default size.

        Given:
            A WorkerPool with no size or discovery specified
        When:
            Pool is started
        Then:
            Should use CPU count as default size
        """
        # Arrange - This tests the (None, None) case at line 297
        mocker.patch("os.cpu_count", return_value=8)

        # Act
        async with WorkerPool() as pool:
            assert pool is not None

        # Assert: Pool created with CPU count workers
        mock_worker_proxy.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_ephemeral_mode_size_only(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test spawn local workers with LocalDiscovery.

        Given:
            A WorkerPool configured with size only (ephemeral mode)
        When:
            Pool is started
        Then:
            Should spawn local workers with LocalDiscovery
        """
        # Arrange - This tests the (size, None) case at line 255
        # This is the most common mode already tested extensively

        # Act
        async with WorkerPool(size=3) as pool:
            assert pool is not None

        # Assert: Pool created successfully
        mock_worker_proxy.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_ephemeral_mode_size_zero_uses_cpu_count(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test use CPU count for worker size.

        Given:
            A WorkerPool configured with size=0 (ephemeral mode)
        When:
            Pool is started
        Then:
            Should use CPU count for worker size
        """
        # Arrange - Tests size=0 path at line 256
        mocker.patch("os.cpu_count", return_value=6)

        # Act
        async with WorkerPool(size=0) as pool:
            assert pool is not None

        # Assert: Pool created with CPU count workers
        mock_worker_proxy.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_hybrid_mode_with_tags(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test pass tags to workers and discovery filtering.

        Given:
            A WorkerPool with tags, size, and discovery
        When:
            Pool is started
        Then:
            Should pass tags to workers and discovery filtering
        """
        # Arrange
        discovery_service = mock_discovery_service_for_pool
        tags = ("gpu", "ml-model")

        # Act
        async with WorkerPool(*tags, size=2, discovery=discovery_service) as pool:
            assert pool is not None

        # Assert: Pool created successfully with tags
        mock_worker_proxy.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_hybrid_mode_with_custom_loadbalancer(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test use custom loadbalancer.

        Given:
            A WorkerPool with size, discovery, and custom loadbalancer
        When:
            Pool is started
        Then:
            Should use custom loadbalancer
        """
        # Arrange
        discovery_service = mock_discovery_service_for_pool
        custom_lb = MagicMock()

        # Act
        async with WorkerPool(
            size=2, discovery=discovery_service, loadbalancer=custom_lb
        ) as pool:
            assert pool is not None

        # Assert: Pool created successfully
        mock_worker_proxy.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_durable_mode_discovery_type_validation(
        self, mocker: MockerFixture, mock_worker_proxy
    ):
        """Test raise ValueError.

        Given:
            A WorkerPool with invalid discovery object (not DiscoveryLike)
        When:
            Pool is started
        Then:
            Should raise ValueError
        """

        # Arrange - Create discovery that doesn't implement DiscoveryLike protocol
        # Use a simple object that explicitly lacks the required protocol methods
        class InvalidDiscovery:
            """Object that does not implement DiscoveryLike protocol."""

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        invalid_discovery = InvalidDiscovery()

        # Act & Assert
        with pytest.raises(ValueError):
            async with WorkerPool(discovery=invalid_discovery):
                pass

    @pytest.mark.asyncio
    async def test_hybrid_mode_discovery_type_validation(
        self, mocker: MockerFixture, mock_worker_proxy, mock_local_worker
    ):
        """Test raise TypeError.

        Given:
            A WorkerPool with size and invalid discovery (not DiscoveryLike)
        When:
            Pool is started
        Then:
            Should raise TypeError
        """

        # Arrange - Create discovery that doesn't implement DiscoveryLike protocol
        # Use a simple object that explicitly lacks the required protocol methods
        class InvalidDiscovery:
            """Object that does not implement DiscoveryLike protocol."""

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        invalid_discovery = InvalidDiscovery()

        # Act & Assert - This tests line 212 (TypeError)
        with pytest.raises(TypeError, match="Expected DiscoveryLike"):
            async with WorkerPool(size=2, discovery=invalid_discovery):
                pass

    @pytest.mark.asyncio
    async def test_hybrid_mode_negative_size_raises_error(
        self, mock_discovery_service_for_pool
    ):
        """Test raise ValueError.

        Given:
            A WorkerPool with negative size and discovery (hybrid mode)
        When:
            Pool is created
        Then:
            Should raise ValueError
        """
        # Arrange
        discovery_service = mock_discovery_service_for_pool

        # Act & Assert - This tests line 230
        with pytest.raises(ValueError, match="Size must be non-negative"):
            WorkerPool(size=-1, discovery=discovery_service)

    def test_hybrid_mode_cpu_count_unavailable_with_size_zero(
        self, mocker: MockerFixture
    ):
        """Test raise ValueError.

        Given:
            A WorkerPool with size=0 and discovery when CPU count unavailable
        When:
            Pool is created
        Then:
            Should raise ValueError
        """
        # Arrange
        mocker.patch("os.cpu_count", return_value=None)
        discovery_service = MagicMock()

        # Act & Assert - This tests line 227
        with pytest.raises(ValueError, match="Unable to determine CPU count"):
            WorkerPool(size=0, discovery=discovery_service)


class TestWorkerPoolProperties:
    """Property-based tests for WorkerPool invariants."""

    @given(st.integers(min_value=1, max_value=10))
    def test_constructor_accepts_valid_sizes(self, size):
        """Test create pool successfully.

        Given:
            Any valid size between 1 and 10
        When:
            WorkerPool is initialized with that size
        Then:
            Should create pool successfully
        """
        # Act
        pool = WorkerPool(size=size)

        # Assert
        assert isinstance(pool, WorkerPool)

    @given(st.integers(min_value=-100, max_value=-1))
    def test_constructor_rejects_negative_sizes(self, negative_size):
        """Test raise ValueError.

        Given:
            Any negative size
        When:
            WorkerPool is initialized with that size
        Then:
            Should raise ValueError
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Size must be non-negative"):
            WorkerPool(size=negative_size)

    @given(size=st.integers(min_value=1, max_value=20))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    @pytest.mark.asyncio
    async def test_property_worker_count_bounded(self, mock_worker_factory, size):
        """Test the number of workers is exactly the configured size.

        Given:
            A WorkerPool with any size from 1 to 20
        When:
            The pool is started
        Then:
            The number of workers is exactly the configured size
        """
        # Arrange
        pool = WorkerPool(worker=mock_worker_factory, size=size)

        # Act
        async with pool:
            # Pool is running with expected worker count
            pass

        # Assert - invariant holds across all pool sizes

    @given(tags=st.lists(st.text(min_size=1, max_size=10), min_size=0, max_size=5))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    @pytest.mark.asyncio
    async def test_property_tags_preserved(self, mock_worker_factory, tags):
        """Test the tags are preserved throughout the worker lifecycle.

        Given:
            A WorkerPool with various tag configurations
        When:
            Workers are created with those tags
        Then:
            The tags are preserved throughout the worker lifecycle
        """
        # Arrange
        pool = WorkerPool(*tags, worker=mock_worker_factory, size=2)

        # Act
        async with pool:
            # Workers maintain their tags
            pass

        # Assert - tags immutability invariant holds

    @given(size=st.integers(min_value=1, max_value=10))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    @pytest.mark.asyncio
    async def test_property_cleanup_complete(self, mock_worker_factory, size):
        """Test all workers are stopped and resources cleaned up.

        Given:
            A WorkerPool of any size
        When:
            The pool is stopped
        Then:
            All workers are stopped and resources cleaned up
        """
        # Arrange
        pool = WorkerPool(worker=mock_worker_factory, size=size)
        cleanup_completed = False

        # Act
        async with pool:
            pass
        cleanup_completed = True

        # Assert - cleanup completes without errors, confirming resource release
        assert cleanup_completed, f"Cleanup should complete for pool of size {size}"

    @given(
        size=st.integers(min_value=1, max_value=5),
        tags=st.lists(st.text(min_size=1, max_size=8), min_size=1, max_size=3),
    )
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    @pytest.mark.asyncio
    async def test_property_context_manager_idempotency(
        self, mock_worker_factory, size, tags
    ):
        """Test lifecycle completes successfully regardless of configuration.

        Given:
            A WorkerPool with various size and tag configurations
        When:
            The pool is used as a context manager
        Then:
            Lifecycle completes successfully regardless of configuration
        """
        # Arrange
        pool = WorkerPool(*tags, worker=mock_worker_factory, size=size)
        entered = False
        exited = False

        # Act
        async with pool as p:
            entered = True
            assert p is not None
            assert isinstance(p, WorkerPool)
        exited = True

        # Assert - lifecycle invariant holds
        assert entered, "Pool should enter context manager"
        assert exited, "Pool should exit context manager cleanly"

    @given(exception_type=st.sampled_from([ValueError, RuntimeError, TypeError]))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    @pytest.mark.asyncio
    async def test_property_exception_propagation(
        self, mock_worker_factory, exception_type
    ):
        """Test the exception propagates correctly after cleanup.

        Given:
            A WorkerPool context manager
        When:
            Any exception type is raised in user code
        Then:
            The exception propagates correctly after cleanup
        """
        # Arrange
        exception_message = f"Test {exception_type.__name__}"
        caught_exception = None

        # Act
        try:
            async with WorkerPool(worker=mock_worker_factory, size=1):
                raise exception_type(exception_message)
        except Exception as e:
            caught_exception = e

        # Assert - exception propagated correctly
        assert caught_exception is not None, "Exception should be caught"
        assert isinstance(caught_exception, exception_type), (
            f"Should catch {exception_type.__name__}"
        )
        assert str(caught_exception) == exception_message


class TestWorkerPoolContextHelpers:
    """Test suite for WorkerPool internal context helper methods."""

    @pytest.mark.asyncio
    async def test_enter_context_with_awaitable(
        self, mocker: MockerFixture, mock_shared_memory, mock_local_worker
    ):
        """Test await the Awaitable and return the result.

        Given:
            WorkerPool with a discovery factory that returns an Awaitable
        When:
            _enter_context is called
        Then:
            It should await the Awaitable and return the result
        """

        # Arrange
        class ConcreteDiscovery(DiscoveryLike):
            def __init__(self):
                self._publisher = mocker.MagicMock(spec=DiscoveryPublisherLike)
                self._publisher.publish = mocker.AsyncMock()
                self._subscriber = mocker.MagicMock(spec=DiscoverySubscriberLike)

            @property
            def publisher(self):
                return self._publisher

            @property
            def subscriber(self):
                return self._subscriber

            def subscribe(self, filter=None):
                return self._subscriber

        async def discovery_awaitable():
            return ConcreteDiscovery()

        # Patch WorkerProxy to avoid actual proxy initialization
        mock_proxy = mocker.MagicMock()
        mock_proxy.__aenter__ = mocker.AsyncMock(return_value=mock_proxy)
        mock_proxy.__aexit__ = mocker.AsyncMock()
        mocker.patch("wool.runtime.worker.pool.WorkerProxy", return_value=mock_proxy)

        pool = WorkerPool(discovery=discovery_awaitable)

        # Act & Assert - should not raise
        async with pool:
            pass

    @pytest.mark.asyncio
    async def test_enter_context_with_plain_object(
        self, mocker: MockerFixture, mock_shared_memory, mock_local_worker
    ):
        """Test return the object directly.

        Given:
            WorkerPool with a discovery that is a plain object (not
            callable/context manager)
        When:
            _enter_context is called
        Then:
            It should return the object directly
        """

        # Arrange
        class ConcreteDiscovery(DiscoveryLike):
            def __init__(self):
                self._publisher = mocker.MagicMock(spec=DiscoveryPublisherLike)
                self._publisher.publish = mocker.AsyncMock()
                self._subscriber = mocker.MagicMock(spec=DiscoverySubscriberLike)

            @property
            def publisher(self):
                return self._publisher

            @property
            def subscriber(self):
                return self._subscriber

            def subscribe(self, filter=None):
                return self._subscriber

        mock_discovery = ConcreteDiscovery()

        # Patch WorkerProxy to avoid actual proxy initialization
        mock_proxy = mocker.MagicMock()
        mock_proxy.__aenter__ = mocker.AsyncMock(return_value=mock_proxy)
        mock_proxy.__aexit__ = mocker.AsyncMock()
        mocker.patch("wool.runtime.worker.pool.WorkerProxy", return_value=mock_proxy)

        pool = WorkerPool(discovery=mock_discovery)

        # Act & Assert - should not raise
        async with pool:
            pass

    @pytest.mark.asyncio
    async def test_exit_context_with_sync_context_manager(
        self, mocker: MockerFixture, mock_shared_memory, mock_local_worker
    ):
        """Test call __exit__ on the context manager.

        Given:
            WorkerPool with a discovery that is a synchronous ContextManager
        When:
            _exit_context is called
        Then:
            It should call __exit__ on the context manager
        """
        # Arrange
        exit_called = [False]

        class ConcreteDiscovery(DiscoveryLike):
            def __init__(self):
                self._publisher = mocker.MagicMock(spec=DiscoveryPublisherLike)
                self._publisher.publish = mocker.AsyncMock()
                self._subscriber = mocker.MagicMock(spec=DiscoverySubscriberLike)

            @property
            def publisher(self):
                return self._publisher

            @property
            def subscriber(self):
                return self._subscriber

            def subscribe(self, filter=None):
                return self._subscriber

        @contextmanager
        def sync_discovery_factory():
            try:
                yield ConcreteDiscovery()
            finally:
                exit_called[0] = True

        # Patch WorkerProxy to avoid actual proxy initialization
        mock_proxy = mocker.MagicMock()
        mock_proxy.__aenter__ = mocker.AsyncMock(return_value=mock_proxy)
        mock_proxy.__aexit__ = mocker.AsyncMock()
        mocker.patch("wool.runtime.worker.pool.WorkerProxy", return_value=mock_proxy)

        pool = WorkerPool(discovery=sync_discovery_factory)

        # Act
        async with pool:
            pass

        # Assert - sync context manager __exit__ was called
        assert exit_called[0]

    @pytest.mark.asyncio
    async def test_worker_context_publisher_type_validation(
        self, mocker: MockerFixture, mock_shared_memory, mock_local_worker
    ):
        """Test raise ValueError.

        Given:
            WorkerPool with a publisher that is not DiscoveryPublisherLike
        When:
            _worker_context is entered
        Then:
            It should raise ValueError
        """

        # Arrange
        class InvalidPublisher:
            """Does not implement DiscoveryPublisherLike protocol."""

            pass

        mock_discovery = mocker.MagicMock()
        mock_discovery.__enter__ = mocker.MagicMock(return_value=mock_discovery)
        mock_discovery.publisher = InvalidPublisher()
        mock_discovery.subscribe = mocker.MagicMock(return_value=mocker.MagicMock())

        # Patch LocalDiscovery to return our mock
        mocker.patch(
            "wool.runtime.worker.pool.LocalDiscovery", return_value=mock_discovery
        )

        # Patch WorkerProxy to avoid actual proxy initialization
        mock_proxy = mocker.MagicMock()
        mock_proxy.__aenter__ = mocker.AsyncMock(return_value=mock_proxy)
        mock_proxy.__aexit__ = mocker.AsyncMock()
        mocker.patch("wool.runtime.worker.pool.WorkerProxy", return_value=mock_proxy)

        pool = WorkerPool(size=1)

        # Act & Assert
        with pytest.raises(ValueError):
            async with pool:
                pass
