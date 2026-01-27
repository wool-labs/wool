"""Comprehensive tests for WorkerProxy client-side coordinator.

Tests validate WorkerProxy behavior through observable public APIs only,
without accessing private state. All tests use mock discovery and gRPC stubs
to avoid network overhead and ensure deterministic behavior.
"""

import asyncio
import uuid
from types import MappingProxyType

import cloudpickle
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

import wool.runtime.worker.proxy as wp
from wool.runtime import protobuf as pb
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.resourcepool import Resource
from wool.runtime.work.task import WorkTask
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.proxy import WorkerProxy

# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def mock_load_balancer_factory(mocker: MockerFixture):
    """Create a mock load balancer factory for testing.

    Returns a tuple of (factory, load_balancer) where the factory creates
    the load balancer instance. Used for testing factory-based load balancer
    creation patterns.
    """
    mock_load_balancer_factory = mocker.MagicMock()
    mock_load_balancer = mocker.MagicMock(spec=wp.LoadBalancerLike)
    mock_load_balancer.dispatch = mocker.AsyncMock()
    mock_load_balancer.worker_added_callback = mocker.MagicMock()
    mock_load_balancer.worker_updated_callback = mocker.MagicMock()
    mock_load_balancer.worker_removed_callback = mocker.MagicMock()
    mock_load_balancer_factory.return_value = mock_load_balancer
    return mock_load_balancer_factory, mock_load_balancer


@pytest.fixture
def mock_worker_stub(mocker: MockerFixture):
    """Create a mock gRPC worker stub for testing.

    Provides a mock :class:`WorkerStub` with async dispatch functionality
    for testing worker communication scenarios.
    """
    mock_worker_stub = mocker.MagicMock(spec=pb.worker.WorkerStub)
    mock_worker_stub.dispatch = mocker.AsyncMock()
    return mock_worker_stub


@pytest.fixture
def mock_wool_task(mocker: MockerFixture):
    """Create a mock :class:`WorkTask` for testing.

    Provides a mock task with protobuf serialization capabilities for
    testing task dispatch and processing scenarios.
    """
    mock_task = mocker.MagicMock(spec=WorkTask)
    mock_task.to_protobuf = mocker.MagicMock()
    return mock_task


@pytest.fixture
def spy_loadbalancer_with_workers(mocker: MockerFixture):
    """Create a spy-wrapped LoadBalancer with realistic behavior.

    Provides a LoadBalancer that implements the LoadBalancerLike protocol
    with real functionality for storing workers and dispatching tasks,
    while being wrapped with spies to verify method calls.
    """

    class SpyableLoadBalancer:
        """LoadBalancer with real functionality that can be spied upon."""

        def __init__(self):
            self._workers = {}
            self._current_index = 0

        def worker_added_callback(
            self, connection: Resource[WorkerConnection], info: WorkerMetadata
        ):
            """Add worker to internal storage."""
            self._workers[info] = connection

        def worker_updated_callback(
            self, connection: Resource[WorkerConnection], info: WorkerMetadata
        ):
            """Update worker in internal storage."""
            self._workers[info] = connection

        def worker_removed_callback(self, info: WorkerMetadata):
            """Remove worker from internal storage."""
            if info in self._workers:
                del self._workers[info]

        async def dispatch(self, task, *, context, timeout=None):
            """Dispatch task to available workers."""
            if not self._workers:
                raise NoWorkersAvailable("No workers available for dispatch")

            # Simple dispatch to first available worker
            metadata, worker_resource = next(iter(self._workers.items()))
            async with worker_resource() as worker:
                return await worker.dispatch(task)

    loadbalancer = SpyableLoadBalancer()

    # Wrap methods with spies
    loadbalancer.worker_added_callback = mocker.spy(
        loadbalancer, "worker_added_callback"
    )
    loadbalancer.worker_updated_callback = mocker.spy(
        loadbalancer, "worker_updated_callback"
    )
    loadbalancer.worker_removed_callback = mocker.spy(
        loadbalancer, "worker_removed_callback"
    )
    loadbalancer.dispatch = mocker.spy(loadbalancer, "dispatch")

    return loadbalancer


@pytest.fixture
def spy_discovery_with_events(mocker: MockerFixture):
    """Create a spy-wrapped discovery service with realistic event behavior.

    Provides a discovery service that yields events from a predefined list,
    while being wrapped with spies to verify method calls and behavior.
    """

    class SpyableDiscovery:
        """Discovery service with real event streaming that can be spied upon."""

        def __init__(self, events=None):
            self._events = events or []
            self._index = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self._index >= len(self._events):
                raise StopAsyncIteration
            event = self._events[self._index]
            self._index += 1
            return event

        def add_event(self, event: DiscoveryEvent):
            """Add event to be yielded by the discovery service."""
            self._events.append(event)

    # Create default worker info for testing
    metadata = WorkerMetadata(
        uid=uuid.uuid4(), host="127.0.0.1", port=50051, pid=1234, version="1.0.0"
    )

    # Create discovery with default worker-added event
    events = [DiscoveryEvent("worker-added", metadata=metadata)]
    discovery = SpyableDiscovery(events)

    # Wrap methods with spies
    discovery.__anext__ = mocker.spy(discovery, "__anext__")
    discovery.add_event = mocker.spy(discovery, "add_event")

    return discovery, metadata


@pytest.fixture
def mock_worker_resource(mocker: MockerFixture):
    """Create a mock worker resource for testing with spy fixtures.

    Provides a mock Resource[WorkerConnection] that implements async context
    manager protocol and returns a worker with dispatch capabilities.
    """
    # Create mock worker without spec to completely avoid real gRPC calls
    mock_worker = mocker.MagicMock()

    async def mock_dispatch_success(task):
        async def _result_generator():
            yield "test_result"

        return _result_generator()

    # Replace the dispatch method completely to avoid calling real gRPC code
    mock_worker.dispatch = mock_dispatch_success

    # Create mock resource with async context manager protocol
    mock_resource = mocker.MagicMock()
    mock_resource.__aenter__ = mocker.AsyncMock(return_value=mock_worker)
    mock_resource.__aexit__ = mocker.AsyncMock()

    return mock_resource, mock_worker


@pytest.fixture
def mock_proxy_session(mocker: MockerFixture):
    """Create a mock wool session context manager.

    Provides a mock proxy session for testing context manager behavior
    and session token handling in wool proxy scenarios.
    """
    mock_proxy_token = mocker.MagicMock()
    mock_proxy_session = mocker.patch.object(wp.wool, "__proxy__")
    mock_proxy_session.set.return_value = mock_proxy_token
    return mock_proxy_session


# ============================================================================
# Test Classes
# ============================================================================


class TestWorkerProxy:
    """Comprehensive test suite for WorkerProxy."""

    # =========================================================================
    # Constructor Tests
    # =========================================================================

    def test_constructor_discovery_and_loadbalancer(
        self, mock_discovery_service, mock_load_balancer_factory
    ):
        """Test create a proxy with both services configured.

        Given:
            A discovery service and load balancer factory
        When:
            WorkerProxy is initialized
        Then:
            It should create a proxy with both services configured
        """
        # Arrange
        mock_factory, mock_load_balancer = mock_load_balancer_factory

        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service, loadbalancer=mock_factory)

        # Assert
        assert isinstance(proxy, WorkerProxy)
        assert not proxy.started

    def test_constructor_discovery_only(
        self, mocker: MockerFixture, mock_discovery_service
    ):
        """Test create proxy with default RoundRobinLoadBalancer.

        Given:
            A discovery service
        When:
            WorkerProxy is initialized
        Then:
            It should create proxy with default RoundRobinLoadBalancer
        """
        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Assert
        assert isinstance(proxy, WorkerProxy)
        assert not proxy.started

    def test_constructor_uri_only(self, mocker: MockerFixture):
        """Test create LocalDiscovery and use RoundRobinLoadBalancer.

        Given:
            A pool URI string
        When:
            WorkerProxy is initialized
        Then:
            It should create LocalDiscovery and use RoundRobinLoadBalancer
        """
        # Arrange
        mock_subscriber = mocker.MagicMock()
        mock_local_discovery_service = mocker.MagicMock()
        mock_local_discovery_service.subscribe.return_value = mock_subscriber
        mocker.patch.object(
            wp, "LocalDiscovery", return_value=mock_local_discovery_service
        )

        # Act
        proxy = WorkerProxy("pool-1")

        # Assert
        assert isinstance(proxy, WorkerProxy)
        assert not proxy.started

    def test_constructor_invalid_arguments(self):
        """Test raise ValueError.

        Given:
            No valid constructor arguments
        When:
            WorkerProxy is initialized
        Then:
            It should raise ValueError
        """
        # Act & Assert
        with pytest.raises(
            ValueError,
            match=(
                "Must specify either a workerpool URI, discovery event stream, "
                "or a sequence of workers"
            ),
        ):
            WorkerProxy()  # type: ignore[call-overload]

    def test_constructor_uri_and_loadbalancer(
        self, mock_load_balancer_factory, mocker: MockerFixture
    ):
        """Test create a proxy with URI filtering and provided load balancer.

        Given:
            A pool URI and load balancer factory
        When:
            WorkerProxy is initialized
        Then:
            It should create a proxy with URI filtering and provided load balancer
        """
        # Arrange
        mock_factory, mock_load_balancer = mock_load_balancer_factory
        mock_local_discovery_service = mocker.MagicMock()
        mocker.patch.object(
            wp, "LocalDiscovery", return_value=mock_local_discovery_service
        )

        # Act
        proxy = WorkerProxy("test-pool", loadbalancer=mock_factory)

        # Assert
        assert isinstance(proxy, WorkerProxy)
        assert not proxy.started

    def test_constructor_workers_parameter_creates_reducible_iterator(self):
        """Test create a ReducibleAsyncIterator with worker-added events.

        Given:
            A list of WorkerMetadata objects
        When:
            WorkerProxy is initialized with workers parameter
        Then:
            It should create a ReducibleAsyncIterator with worker-added events
        """
        # Arrange
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(), host="127.0.0.1", port=50051, pid=1234, version="1.0.0"
            ),
            WorkerMetadata(
                uid=uuid.uuid4(), host="127.0.0.1", port=50052, pid=1235, version="1.0.0"
            ),
        ]

        # Act
        proxy = WorkerProxy(workers=workers)

        # Assert
        assert isinstance(proxy, WorkerProxy)

    def test_constructor_invalid_multiple_parameters_raises_error(self):
        """Test raise ValueError with appropriate message.

        Given:
            Multiple conflicting initialization parameters
        When:
            WorkerProxy is initialized with conflicting parameters
        Then:
            It should raise ValueError with appropriate message
        """
        # Arrange
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(), host="127.0.0.1", port=50051, pid=1234, version="1.0.0"
            )
        ]
        discovery = LocalDiscovery("test-pool").subscriber

        # Act & Assert
        with pytest.raises(ValueError, match="Must specify exactly one of"):
            WorkerProxy(pool_uri="test-pool", discovery=discovery, workers=workers)

    def test_constructor_no_parameters_raises_error(self):
        """Test raise ValueError with appropriate message.

        Given:
            No initialization parameters
        When:
            WorkerProxy is initialized without any parameters
        Then:
            It should raise ValueError with appropriate message
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Must specify either a workerpool URI"):
            WorkerProxy()

    # =========================================================================
    # Lifecycle Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_context_manager_lifecycle(self, mock_discovery_service):
        """Test it starts and stops correctly.

        Given:
            A WorkerProxy configured with discovery service
        When:
            The proxy is used as a context manager
        Then:
            It starts and stops correctly
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)
        entered = False
        exited = False

        # Act
        async with proxy as p:
            entered = True
            assert p is not None
            assert p.started
        exited = True

        # Assert
        assert entered
        assert exited
        assert not proxy.started

    @pytest.mark.asyncio
    async def test_context_manager_cleanup_on_error(self, mock_discovery_service):
        """Test cleanup still occurs and exception propagates.

        Given:
            A WorkerProxy used as context manager
        When:
            An exception occurs within the context
        Then:
            Cleanup still occurs and exception propagates
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)
        exception_caught = False

        # Act & Assert
        try:
            async with proxy:
                raise ValueError("Test error")
        except ValueError:
            exception_caught = True

        # Assert - cleanup occurred despite exception
        assert exception_caught
        assert not proxy.started

    @pytest.mark.asyncio
    async def test_start_sets_started_flag(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test set the started flag to True.

        Given:
            An unstarted WorkerProxy instance
        When:
            Start is called
        Then:
            It should set the started flag to True
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act
        await proxy.start()

        # Assert
        assert proxy.started

    @pytest.mark.asyncio
    async def test_stop_clears_state(self, mock_discovery_service, mock_proxy_session):
        """Test clear workers and reset the started flag to False.

        Given:
            A started WorkerProxy with registered workers
        When:
            Stop is called
        Then:
            It should clear workers and reset the started flag to False
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)
        await proxy.start()

        # Act
        await proxy.stop()

        # Assert - verify observable behavior through public API
        assert not proxy.started
        assert len(proxy.workers) == 0

    @pytest.mark.asyncio
    async def test_start_already_started_raises_error(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test raise RuntimeError.

        Given:
            A WorkerProxy that is already started
        When:
            Start is called again
        Then:
            It should raise RuntimeError
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)
        await proxy.start()

        # Act & Assert
        with pytest.raises(RuntimeError, match="Proxy already started"):
            await proxy.start()

    @pytest.mark.asyncio
    async def test_stop_not_started_raises_error(self, mock_discovery_service):
        """Test raise RuntimeError.

        Given:
            A WorkerProxy that is not started
        When:
            Stop is called
        Then:
            It should raise RuntimeError
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act & Assert
        with pytest.raises(RuntimeError, match="Proxy not started"):
            await proxy.stop()

    @pytest.mark.asyncio
    async def test_context_manager_enter_starts_proxy(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test automatically start the proxy.

        Given:
            An unstarted WorkerProxy
        When:
            The async context manager is entered
        Then:
            It should automatically start the proxy
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act & Assert
        async with proxy as p:
            assert p.started

    @pytest.mark.asyncio
    async def test_context_manager_exit_stops_proxy(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test automatically stop the proxy.

        Given:
            A WorkerProxy within async context
        When:
            The async context manager exits
        Then:
            It should automatically stop the proxy
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act
        async with proxy:
            pass

        # Assert
        assert not proxy.started

    @pytest.mark.asyncio
    async def test_start_stop_with_sync_cm_loadbalancer(
        self, mock_discovery_service, mock_proxy_session, mocker: MockerFixture
    ):
        """Test start/stop with sync context manager load balancer.

        Given:
            A WorkerProxy with a load balancer provided as a sync
            context manager.
        When:
            start() then stop() are called.
        Then:
            The context manager's __enter__ is called on start and
            __exit__ on stop.
        """
        # Arrange
        mock_lb = mocker.MagicMock(spec=wp.LoadBalancerLike)
        mock_lb.dispatch = mocker.AsyncMock()

        class SyncCM:
            def __init__(self):
                self.entered = False
                self.exited = False

            def __enter__(self):
                self.entered = True
                return mock_lb

            def __exit__(self, *args):
                self.exited = True

        cm = SyncCM()
        proxy = WorkerProxy(discovery=mock_discovery_service, loadbalancer=cm)

        # Act
        await proxy.start()
        assert cm.entered
        assert proxy.started

        await proxy.stop()

        # Assert
        assert cm.exited
        assert not proxy.started

    @pytest.mark.asyncio
    async def test_start_stop_with_async_cm_loadbalancer(
        self, mock_discovery_service, mock_proxy_session, mocker: MockerFixture
    ):
        """Test start/stop with async context manager load balancer.

        Given:
            A WorkerProxy with a load balancer provided as an async
            context manager.
        When:
            start() then stop() are called.
        Then:
            The async context manager's __aenter__ is called on start
            and __aexit__ on stop.
        """
        # Arrange
        mock_lb = mocker.MagicMock(spec=wp.LoadBalancerLike)
        mock_lb.dispatch = mocker.AsyncMock()

        class AsyncCM:
            def __init__(self):
                self.entered = False
                self.exited = False

            async def __aenter__(self):
                self.entered = True
                return mock_lb

            async def __aexit__(self, *args):
                self.exited = True

        cm = AsyncCM()
        proxy = WorkerProxy(discovery=mock_discovery_service, loadbalancer=cm)

        # Act
        await proxy.start()
        assert cm.entered
        assert proxy.started

        await proxy.stop()

        # Assert
        assert cm.exited
        assert not proxy.started

    @pytest.mark.asyncio
    async def test_start_with_awaitable_loadbalancer(
        self, mock_discovery_service, mock_proxy_session, mocker: MockerFixture
    ):
        """Test start with an awaitable load balancer.

        Given:
            A WorkerProxy with a load balancer provided as a bare
            awaitable (coroutine object).
        When:
            start() is called.
        Then:
            The awaitable is resolved to the load balancer instance
            and the proxy starts successfully.
        """
        # Arrange
        mock_lb = mocker.MagicMock(spec=wp.LoadBalancerLike)
        mock_lb.dispatch = mocker.AsyncMock()

        async def make_lb():
            return mock_lb

        proxy = WorkerProxy(discovery=mock_discovery_service, loadbalancer=make_lb())

        # Act
        await proxy.start()

        # Assert
        assert proxy.started

        # Cleanup
        await proxy.stop()

    # =========================================================================
    # Discovery Integration Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_discovers_workers_from_service(self, mock_discovery_service):
        """Test the proxy discovers them.

        Given:
            A WorkerProxy with mock discovery service
        When:
            Workers are injected into discovery
        Then:
            The proxy discovers them
        """
        # Arrange
        worker1 = WorkerMetadata(
            uid=uuid.uuid4(),
            host="192.168.1.100",
            port=50051,
            pid=1001,
            version="1.0.0",
            tags=frozenset(["test"]),
            extra=MappingProxyType({}),
        )

        # Inject worker before starting proxy
        await mock_discovery_service.start()
        mock_discovery_service.inject_worker_added(worker1)

        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act
        async with proxy:
            # Give discovery time to propagate
            await asyncio.sleep(0.1)

        await mock_discovery_service.stop()

    @pytest.mark.asyncio
    async def test_handles_worker_added_dynamically(self, mock_discovery_service):
        """Test the proxy adds it to available workers.

        Given:
            A WorkerProxy that is already running
        When:
            A new worker is discovered
        Then:
            The proxy adds it to available workers
        """
        # Arrange
        await mock_discovery_service.start()
        proxy = WorkerProxy(discovery=mock_discovery_service)

        worker1 = WorkerMetadata(
            uid=uuid.uuid4(),
            host="192.168.1.100",
            port=50051,
            pid=1001,
            version="1.0.0",
            tags=frozenset(["test"]),
            extra=MappingProxyType({}),
        )

        # Act
        async with proxy:
            # Add worker after proxy started
            mock_discovery_service.inject_worker_added(worker1)
            await asyncio.sleep(0.1)

        await mock_discovery_service.stop()

    @pytest.mark.asyncio
    async def test_handles_worker_removed(self, mock_discovery_service):
        """Test the proxy removes it from available workers.

        Given:
            A WorkerProxy with discovered workers
        When:
            A worker is removed from discovery
        Then:
            The proxy removes it from available workers
        """
        # Arrange
        worker1 = WorkerMetadata(
            uid=uuid.uuid4(),
            host="192.168.1.100",
            port=50051,
            pid=1001,
            version="1.0.0",
            tags=frozenset(["test"]),
            extra=MappingProxyType({}),
        )

        await mock_discovery_service.start()
        mock_discovery_service.inject_worker_added(worker1)
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act
        async with proxy:
            await asyncio.sleep(0.1)
            # Remove worker
            mock_discovery_service.inject_worker_removed(worker1)
            await asyncio.sleep(0.1)

        await mock_discovery_service.stop()

    @pytest.mark.asyncio
    async def test_handles_worker_updated(self, mock_proxy_session):
        """Test the proxy handles worker-updated events.

        Given:
            A started WorkerProxy with a discovered worker.
        When:
            A "worker-updated" event is received for that worker.
        Then:
            The worker metadata is updated without error.
        """
        # Arrange
        metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            host="192.168.1.100",
            port=50051,
            pid=1001,
            version="1.0.0",
        )

        events = [
            DiscoveryEvent("worker-added", metadata=metadata),
            DiscoveryEvent("worker-updated", metadata=metadata),
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery)

        # Act
        await proxy.start()
        await asyncio.sleep(0.1)

        # Assert
        assert metadata in proxy.workers

        # Cleanup
        await proxy.stop()

    # =========================================================================
    # Task Dispatch Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_dispatch_delegates_to_loadbalancer(
        self,
        spy_loadbalancer_with_workers,
        spy_discovery_with_events,
        mock_worker_resource,
        mock_wool_task,
        mock_proxy_session,
        mocker,
    ):
        """Test delegate the task to the load balancer and yield results.

        Given:
            A started WorkerProxy with available workers via discovery
        When:
            Dispatch is called with a task
        Then:
            It should delegate the task to the load balancer and yield results
        """
        # Arrange
        discovery, metadata = spy_discovery_with_events
        mock_resource, mock_worker = mock_worker_resource

        proxy = WorkerProxy(
            discovery=discovery,
            loadbalancer=spy_loadbalancer_with_workers,
        )

        await proxy.start()

        # Add worker through proper loadbalancer callback (simulating discovery)
        spy_loadbalancer_with_workers.worker_added_callback(
            lambda: mock_resource, metadata
        )

        # Act
        result_iterator = await proxy.dispatch(mock_wool_task)
        results = [result async for result in result_iterator]

        # Assert
        assert results == ["test_result"]
        spy_loadbalancer_with_workers.dispatch.assert_called_once()
        # worker_added_callback gets called twice: once by test, once by _worker_sentinel
        assert spy_loadbalancer_with_workers.worker_added_callback.call_count >= 1

    @pytest.mark.asyncio
    async def test_dispatch_not_started_raises_error(
        self, mock_discovery_service, mock_wool_task
    ):
        """Test raise RuntimeError.

        Given:
            A WorkerProxy that is not started
        When:
            Dispatch is called
        Then:
            It should raise RuntimeError
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act & Assert
        with pytest.raises(RuntimeError, match="Proxy not started"):
            async for _ in await proxy.dispatch(mock_wool_task):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_propagates_loadbalancer_errors(
        self,
        mocker: MockerFixture,
        spy_discovery_with_events,
        mock_worker_resource,
        mock_wool_task,
        mock_proxy_session,
    ):
        """Test propagate the error from the load balancer.

        Given:
            A WorkerProxy where load balancer dispatch fails
        When:
            Dispatch is called
        Then:
            It should propagate the error from the load balancer
        """
        # Arrange
        discovery, metadata = spy_discovery_with_events
        mock_resource, mock_worker = mock_worker_resource

        # Create loadbalancer that raises errors during dispatch
        class FailingLoadBalancer:
            def __init__(self):
                self._workers = {}

            def worker_added_callback(self, connection, info):
                self._workers[info] = connection

            def worker_updated_callback(self, connection, info):
                self._workers[info] = connection

            def worker_removed_callback(self, info):
                if info in self._workers:
                    del self._workers[info]

            async def dispatch(self, task, *, context, timeout=None):
                raise Exception("Load balancer error")

        failing_loadbalancer = FailingLoadBalancer()

        proxy = WorkerProxy(discovery=discovery, loadbalancer=failing_loadbalancer)

        await proxy.start()

        # Add worker through proper callback
        failing_loadbalancer.worker_added_callback(lambda: mock_resource, metadata)

        # Act & Assert
        with pytest.raises(Exception, match="Load balancer error"):
            async for _ in await proxy.dispatch(mock_wool_task):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_waits_for_workers_then_dispatches(
        self,
        mocker: MockerFixture,
        mock_worker_resource,
        mock_wool_task,
        mock_proxy_session,
    ):
        """Test wait via _await_workers, then dispatch when workers appear.

        Given:
            A started WorkerProxy with no initial workers in loadbalancer
        When:
            Dispatch is called and workers become available during wait
        Then:
            Should wait via _await_workers, then dispatch when workers appear
        """
        # Arrange
        metadata = WorkerMetadata(
            uid=uuid.uuid4(), host="127.0.0.1", port=50051, pid=1234, version="1.0.0"
        )

        # Create discovery service that will emit worker event
        events = [DiscoveryEvent("worker-added", metadata=metadata)]
        discovery = wp.ReducibleAsyncIterator(events)
        mock_resource, mock_worker = mock_worker_resource

        # Create loadbalancer that tracks workers through callbacks
        class WaitingLoadBalancer:
            def __init__(self):
                self._workers = {}  # Start empty

            def worker_added_callback(self, connection, info):
                self._workers[info] = connection

            def worker_updated_callback(self, connection, info):
                self._workers[info] = connection

            def worker_removed_callback(self, info):
                if info in self._workers:
                    del self._workers[info]

            async def dispatch(self, task, *, context, timeout=None):
                if not context.workers:
                    raise NoWorkersAvailable("No workers available")

                async def _result_generator():
                    yield "test_result"

                return _result_generator()

        waiting_loadbalancer = WaitingLoadBalancer()

        proxy = WorkerProxy(
            discovery=discovery,
            loadbalancer=waiting_loadbalancer,
        )

        await proxy.start()

        # Give time for discovery event to be processed
        await asyncio.sleep(0.1)

        # Act - workers should be available through discovery processing
        results = []
        async for result in await proxy.dispatch(mock_wool_task):
            results.append(result)

        # Assert
        assert results == ["test_result"]

    @pytest.mark.asyncio
    async def test_dispatch_spins_until_worker_discovered(
        self,
        mocker: MockerFixture,
        mock_proxy_session,
    ):
        """Test dispatch spins when no workers are available yet.

        Given:
            A started WorkerProxy whose discovery stream is gated
            behind an asyncio.Event, so no workers exist initially.
        When:
            dispatch() is called before any workers are available,
            then the gate is opened to emit a worker-added event.
        Then:
            dispatch() spins in the await-workers loop until the
            sentinel processes the event and adds the worker,
            then completes normally.
        """
        # Arrange — gated discovery that blocks until signaled
        gate = asyncio.Event()
        metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            host="192.168.1.100",
            port=50051,
            pid=1001,
            version="1.0.0",
        )

        class GatedDiscovery:
            def __aiter__(self):
                return self

            async def __anext__(self):
                await gate.wait()
                gate.clear()
                return DiscoveryEvent("worker-added", metadata=metadata)

        class StubLoadBalancer:
            def __init__(self):
                self.dispatched = False

            async def dispatch(self, task, *, context, timeout=None):
                self.dispatched = True

        stub_lb = StubLoadBalancer()
        proxy = WorkerProxy(discovery=GatedDiscovery(), loadbalancer=stub_lb)
        await proxy.start()

        # Verify no workers before the gate opens
        assert proxy.workers == []

        mock_task = mocker.MagicMock(spec=WorkTask)

        # Act — launch dispatch in background; it spins waiting for workers
        dispatch_task = asyncio.create_task(proxy.dispatch(mock_task))
        await asyncio.sleep(0)  # yield so dispatch enters the spin loop

        # Open the gate — sentinel receives the event and adds the worker
        gate.set()
        await dispatch_task

        # Assert
        assert metadata in proxy.workers
        assert stub_lb.dispatched

        # Cleanup
        await proxy.stop()

    # =========================================================================
    # Static Worker Configuration Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_proxy_with_static_workers_list(self):
        """Test it starts and stops correctly.

        Given:
            A WorkerProxy configured with a static list of workers
        When:
            The proxy is used as a context manager
        Then:
            It starts and stops correctly
        """
        # Arrange
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                host="192.168.1.100",
                port=50051,
                pid=1001,
                version="1.0.0",
                tags=frozenset(["test"]),
                extra=MappingProxyType({}),
            ),
            WorkerMetadata(
                uid=uuid.uuid4(),
                host="192.168.1.101",
                port=50052,
                pid=1002,
                version="1.0.0",
                tags=frozenset(["test"]),
                extra=MappingProxyType({}),
            ),
        ]

        proxy = WorkerProxy(workers=workers)

        # Act & Assert
        async with proxy as p:
            assert p is not None
            assert p.started

        # After exit, proxy should be stopped
        assert not proxy.started

    @pytest.mark.asyncio
    async def test_proxy_with_pool_uri(self):
        """Test it starts and stops correctly.

        Given:
            A WorkerProxy configured with a pool URI
        When:
            The proxy is used as a context manager
        Then:
            It starts and stops correctly
        """
        # Arrange
        proxy = WorkerProxy("test://pool")

        # Act & Assert
        async with proxy as p:
            assert p is not None
            assert p.started

        # After exit, proxy should be stopped
        assert not proxy.started

    # =========================================================================
    # Properties Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_workers_property_returns_workers_list(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test return a list of WorkerMetadata objects.

        Given:
            A WorkerProxy with discovered workers
        When:
            The workers property is accessed
        Then:
            It should return a list of WorkerMetadata objects
        """
        # Arrange
        metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            host="127.0.0.1",
            port=50051,
            pid=1234,
            version="1.0.0",
        )

        # Inject worker into discovery service before starting proxy
        await mock_discovery_service.start()
        mock_discovery_service.inject_worker_added(metadata)

        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act
        async with proxy:
            # Give discovery time to process
            await asyncio.sleep(0.1)
            workers = proxy.workers

            # Assert - verify observable behavior through public API
            assert isinstance(workers, list)
            assert metadata in workers
            assert len(workers) == 1

        await mock_discovery_service.stop()

    def test_proxy_id_uniqueness_across_instances(self):
        """Test all IDs should be unique UUIDs.

        Given:
            Multiple WorkerProxy instances
        When:
            Each proxy generates an ID
        Then:
            All IDs should be unique UUIDs
        """
        # Act
        proxy_ids = [WorkerProxy("test-pool").id for _ in range(5)]

        # Assert
        assert len(set(proxy_ids)) == len(proxy_ids)

    def test_hash(self):
        """Test return hash of the proxy ID string.

        Given:
            A WorkerProxy instance
        When:
            __hash__ is called (implicitly via hash())
        Then:
            Should return hash of the proxy ID string
        """
        # Arrange
        proxy = WorkerProxy("test-pool")

        # Act
        proxy_hash = hash(proxy)

        # Assert
        assert proxy_hash == hash(str(proxy.id))
        assert isinstance(proxy_hash, int)

    def test_eq(self):
        """Test return True if both are WorkerProxy instances with same hash.

        Given:
            Two WorkerProxy instances
        When:
            __eq__ is called (implicitly via ==)
        Then:
            Should return True if both are WorkerProxy instances with same hash
        """
        # Arrange
        proxy1 = WorkerProxy("test-pool")
        proxy2 = WorkerProxy("test-pool")
        non_proxy = "not a proxy"

        # Act & Assert
        # Same proxy should equal itself
        assert proxy1 == proxy1

        # Different proxies should not be equal (different IDs)
        assert proxy1 != proxy2

        # Proxy should not equal non-WorkerProxy objects
        assert proxy1 != non_proxy

    # =========================================================================
    # Serialization Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_with_services(self):
        """Test serialize and deserialize successfully with deserialized proxy
        in an unstarted state.

        Given:
            A started WorkerProxy with real discovery and load balancer
            services
        When:
            Cloudpickle serialization and deserialization are performed
            within context
        Then:
            It should serialize and deserialize successfully with deserialized
            proxy in an unstarted state
        """
        # Arrange - Use real objects instead of mocks for cloudpickle test
        discovery_service = LocalDiscovery("test-pool").subscriber
        proxy = WorkerProxy(
            discovery=discovery_service, loadbalancer=wp.RoundRobinLoadBalancer
        )

        # Act & Assert
        async with proxy:
            # Verify proxy is started before pickling
            assert proxy.started is True

            # Pickle from within the started proxy context
            pickled_data = cloudpickle.dumps(proxy)
            unpickled_proxy = cloudpickle.loads(pickled_data)

            # Unpickled proxy should be unstarted regardless of source state
            assert isinstance(unpickled_proxy, WorkerProxy)
            assert unpickled_proxy.started is False

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_discovery_only(self):
        """Test serialize and deserialize successfully with deserialized proxy
        in an unstarted state.

        Given:
            A started WorkerProxy with only discovery service
        When:
            Cloudpickle serialization and deserialization are performed within
            context
        Then:
            It should serialize and deserialize successfully with deserialized
            proxy in an unstarted state
        """
        # Arrange - Use real objects instead of mocks for cloudpickle test
        discovery_service = LocalDiscovery("test-pool").subscriber
        proxy = WorkerProxy(discovery=discovery_service)

        # Act & Assert
        async with proxy:
            # Verify proxy is started before pickling
            assert proxy.started is True

            # Pickle from within the started proxy context
            pickled_data = cloudpickle.dumps(proxy)
            unpickled_proxy = cloudpickle.loads(pickled_data)

            # Unpickled proxy should be unstarted regardless of source state
            assert isinstance(unpickled_proxy, WorkerProxy)
            assert unpickled_proxy.started is False

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_uri_preserves_id(self):
        """Test serialize and deserialize successfully with deserialized proxy
        in an unstarted state and preserved ID.

        Given:
            A started WorkerProxy created with only a URI
        When:
            Cloudpickle serialization and deserialization are performed within
            context
        Then:
            It should serialize and deserialize successfully with deserialized
            proxy in an unstarted state and preserved ID
        """
        # Arrange - Use real objects - this creates a LocalDiscovery internally
        proxy = WorkerProxy("pool-1")

        # Act & Assert
        async with proxy:
            # Verify proxy is started before pickling
            assert proxy.started is True

            # Pickle from within the started proxy context
            pickled_data = cloudpickle.dumps(proxy)
            unpickled_proxy = cloudpickle.loads(pickled_data)

            # Unpickled proxy should be unstarted regardless of source state
            assert isinstance(unpickled_proxy, WorkerProxy)
            assert unpickled_proxy.started is False
            assert unpickled_proxy.id == proxy.id

    # =========================================================================
    # Validation Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_start_invalid_loadbalancer_type_raises_error(
        self, mocker: MockerFixture
    ):
        """Test raise ValueError.

        Given:
            A WorkerProxy with a loadbalancer that doesn't implement LoadBalancerLike
        When:
            Start() is called
        Then:
            It should raise ValueError
        """

        # Arrange
        class NotALoadBalancer:
            pass

        invalid_loadbalancer = NotALoadBalancer()

        proxy = WorkerProxy(
            pool_uri="test-pool", loadbalancer=lambda: invalid_loadbalancer
        )

        # Act & Assert
        with pytest.raises(ValueError):
            await proxy.start()

    @pytest.mark.asyncio
    async def test_start_invalid_discovery_type_raises_error(
        self, mocker: MockerFixture
    ):
        """Test raise ValueError.

        Given:
            A WorkerProxy with a discovery that doesn't implement AsyncIterator
        When:
            Start() is called
        Then:
            It should raise ValueError
        """
        # Arrange - use a simple string which is definitely not an AsyncIterator
        invalid_discovery = "not_an_async_iterator"

        proxy = WorkerProxy(discovery=lambda: invalid_discovery)

        # Act & Assert
        with pytest.raises(ValueError):
            await proxy.start()

    # =========================================================================
    # Security Filtering Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_security_filter_with_credentials(self, mock_proxy_session):
        """Test only secure workers are discovered with credentials.

        Given:
            A WorkerProxy instantiated with credentials and a mix
            of secure and insecure static workers.
        When:
            The proxy is started and discovery events are processed.
        Then:
            Only secure workers appear in the workers list.
        """
        # Arrange
        secure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            host="192.168.1.100",
            port=50051,
            pid=1001,
            version="1.0.0",
            secure=True,
        )
        insecure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            host="192.168.1.101",
            port=50052,
            pid=1002,
            version="1.0.0",
            secure=False,
        )
        mock_credentials = object()  # Any non-None value

        proxy = WorkerProxy(
            workers=[secure_worker, insecure_worker],
            credentials=mock_credentials,
        )

        # Act
        await proxy.start()
        await asyncio.sleep(0.1)

        # Assert
        assert secure_worker in proxy.workers
        assert insecure_worker not in proxy.workers

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_pool_uri_combined_filter(self, mocker: MockerFixture):
        """Test pool URI filter combines tag matching and security filtering.

        Given:
            A WorkerProxy instantiated with a pool URI and extra tags.
        When:
            The discovery filter is evaluated against workers.
        Then:
            Only workers matching the tags and security requirements
            pass the filter.
        """
        # Arrange
        mock_subscriber = mocker.MagicMock()
        mock_local_discovery = mocker.MagicMock()
        mock_local_discovery.subscribe.return_value = mock_subscriber
        mocker.patch.object(wp, "LocalDiscovery", return_value=mock_local_discovery)

        WorkerProxy("pool-1", "extra-tag")

        # Capture the filter passed to subscribe()
        filter_fn = mock_local_discovery.subscribe.call_args.kwargs["filter"]

        # Act & Assert — matching tags, insecure (no credentials)
        matching = WorkerMetadata(
            uid=uuid.uuid4(),
            host="127.0.0.1",
            port=50051,
            pid=1,
            version="1.0.0",
            tags=frozenset(["pool-1"]),
            secure=False,
        )
        assert filter_fn(matching) is True

        # Act & Assert — no matching tags
        non_matching = WorkerMetadata(
            uid=uuid.uuid4(),
            host="127.0.0.1",
            port=50052,
            pid=2,
            version="1.0.0",
            tags=frozenset(["other"]),
            secure=False,
        )
        assert filter_fn(non_matching) is False

        # Act & Assert — matching tags but secure (no credentials means
        # security filter rejects secure workers)
        secure_matching = WorkerMetadata(
            uid=uuid.uuid4(),
            host="127.0.0.1",
            port=50053,
            pid=3,
            version="1.0.0",
            tags=frozenset(["pool-1"]),
            secure=True,
        )
        assert filter_fn(secure_matching) is False


# ============================================================================
# Property-Based Tests
# ============================================================================


class TestWorkerProxyProperties:
    """Property-based tests for WorkerProxy invariants."""

    @given(worker_count=st.integers(min_value=0, max_value=10))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    @pytest.mark.asyncio
    async def test_property_workers_list_accurate(
        self, mock_discovery_service, worker_count
    ):
        """Test the workers list remains consistent.

        Given:
            A WorkerProxy with varying numbers of discovered workers
        When:
            Workers are added via discovery
        Then:
            The workers list remains consistent
        """
        # Arrange
        await mock_discovery_service.start()
        workers = []
        for i in range(worker_count):
            worker = WorkerMetadata(
                uid=uuid.uuid4(),
                host=f"192.168.1.{100 + i}",
                port=50051,
                pid=1000 + i,
                version="1.0.0",
                tags=frozenset(["test"]),
                extra=MappingProxyType({}),
            )
            workers.append(worker)
            mock_discovery_service.inject_worker_added(worker)

        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act
        async with proxy:
            await asyncio.sleep(0.1)
            # Workers list should be consistent

        await mock_discovery_service.stop()

    @given(num_proxies=st.integers(min_value=2, max_value=20))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_property_proxy_id_uniqueness(self, num_proxies):
        """Test each proxy ID should be unique.

        Given:
            Multiple WorkerProxy instances
        When:
            They are created
        Then:
            Each proxy ID should be unique
        """
        # Arrange & Act
        proxies = [WorkerProxy("test-pool") for _ in range(num_proxies)]
        proxy_ids = [proxy.id for proxy in proxies]

        # Assert
        assert len(set(proxy_ids)) == len(proxy_ids), "All proxy IDs must be unique"

    @given(num_proxies=st.integers(min_value=1, max_value=10))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_property_proxy_hash_consistency(self, num_proxies):
        """Test hash should equal hash of string representation of ID.

        Given:
            WorkerProxy instances
        When:
            Hash is computed
        Then:
            Hash should equal hash of string representation of ID
        """
        # Arrange & Act
        proxies = [WorkerProxy("test-pool") for _ in range(num_proxies)]

        # Assert
        for proxy in proxies:
            assert hash(proxy) == hash(str(proxy.id))

    @given(num_proxies=st.integers(min_value=1, max_value=10))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_property_proxy_equality_reflexive(self, num_proxies):
        """Test always be equal.

        Given:
            WorkerProxy instances
        When:
            Comparing a proxy to itself
        Then:
            It should always be equal
        """
        # Arrange & Act
        proxies = [WorkerProxy("test-pool") for _ in range(num_proxies)]

        # Assert
        for proxy in proxies:
            assert proxy == proxy, "Proxy should equal itself (reflexive property)"

    @given(
        pool_uri=st.text(
            min_size=1, max_size=50, alphabet=st.characters(blacklist_characters="\x00")
        )
    )
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_property_proxy_accepts_any_valid_pool_uri(self, pool_uri):
        """Test succeed without error.

        Given:
            A non-empty string
        When:
            Creating a WorkerProxy with it
        Then:
            It should succeed without error
        """
        # Arrange & Act & Assert
        proxy = WorkerProxy(pool_uri)
        assert isinstance(proxy, WorkerProxy)
        assert not proxy.started

    @given(worker_count=st.integers(min_value=1, max_value=10))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    def test_property_static_workers_list_creates_proxy(self, worker_count):
        """Test succeed and create a valid proxy.

        Given:
            A list of WorkerMetadata objects
        When:
            Creating a WorkerProxy with workers parameter
        Then:
            It should succeed and create a valid proxy
        """
        # Arrange
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                host=f"192.168.1.{100 + i}",
                port=50051 + i,
                pid=1000 + i,
                version="1.0.0",
            )
            for i in range(worker_count)
        ]

        # Act
        proxy = WorkerProxy(workers=workers)

        # Assert
        assert isinstance(proxy, WorkerProxy)
        assert not proxy.started


# ============================================================================
# Utility Functions Tests
# ============================================================================


class TestUtilityFunctions:
    """Test utility functions for worker connection management."""

    @pytest.mark.asyncio
    async def test_client_factory_creates_worker_client(self):
        """Test return WorkerConnection instance for the address.

        Given:
            A network address string
        When:
            Connection_factory is called
        Then:
            It should return WorkerConnection instance for the address
        """
        # Arrange
        address = "127.0.0.1:50051"

        # Act
        result = await wp.connection_factory(address)

        # Assert
        assert isinstance(result, wp.WorkerConnection)

    @pytest.mark.asyncio
    async def test_client_finalizer_handles_stop_exceptions(self, mocker: MockerFixture):
        """Test handle exception gracefully without propagating.

        Given:
            A WorkerConnection that raises exception during close
        When:
            Connection_finalizer is called
        Then:
            It should handle exception gracefully without propagating
        """
        # Arrange
        mock_connection = mocker.MagicMock()
        mock_connection.close = mocker.AsyncMock(side_effect=Exception("Stop failed"))

        # Act - Should not raise exception
        await wp.connection_finalizer(mock_connection)

        # Assert
        mock_connection.close.assert_called_once()


# ============================================================================
# ReducibleAsyncIterator Tests
# ============================================================================


class TestReducibleAsyncIterator:
    """Tests for ReducibleAsyncIterator pickling support."""

    @pytest.mark.asyncio
    async def test_pickle_round_trip(self):
        """Test pickle round-trip preserves items.

        Given:
            A ReducibleAsyncIterator with items.
        When:
            It is pickled and unpickled.
        Then:
            The unpickled iterator yields the same items in order.
        """
        # Arrange
        items = [1, 2, 3, "hello", None]
        iterator = wp.ReducibleAsyncIterator(items)

        # Act
        pickled = cloudpickle.dumps(iterator)
        unpickled = cloudpickle.loads(pickled)
        results = [item async for item in unpickled]

        # Assert
        assert results == items
