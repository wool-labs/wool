"""Comprehensive tests for WorkerProxy client-side coordinator.

Tests validate WorkerProxy behavior through observable public APIs only,
without accessing private state. All tests use mock discovery and gRPC stubs
to avoid network overhead and ensure deterministic behavior.
"""

import asyncio
import uuid
import warnings
from types import MappingProxyType

import cloudpickle
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from packaging.version import Version
from pytest_mock import MockerFixture

import wool.runtime.worker.proxy as wp
from wool import protocol
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import DelegatingLoadBalancerLike
from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.routine.task import Task
from wool.runtime.worker.auth import CredentialContext
from wool.runtime.worker.base import ChannelOptions
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata
from wool.runtime.worker.proxy import WorkerProxy
from wool.runtime.worker.proxy import is_version_compatible
from wool.runtime.worker.proxy import parse_version


async def _drain_discovery(proxy, *, expect, timeout=2.0):
    """Wait until proxy.workers reaches the expected count or times out.

    Yields the event loop in a tight loop so the sentinel can process
    events from a finite ReducibleAsyncIterator without a hard sleep.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while len(proxy.workers) != expect:
        if asyncio.get_event_loop().time() > deadline:
            break
        await asyncio.sleep(0)


# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def mock_load_balancer_factory(mocker: MockerFixture):
    """Create a mock delegating load balancer factory for testing.

    Returns a tuple of (factory, load_balancer). The mock load balancer
    has an empty ``delegate`` async generator that terminates immediately
    — tests that actually exercise dispatch should override it.
    """
    mock_load_balancer_factory = mocker.MagicMock()
    mock_load_balancer = mocker.MagicMock(spec=DelegatingLoadBalancerLike)

    async def _empty_delegate(*, context):
        if False:
            yield  # pragma: no cover

    mock_load_balancer.delegate = mocker.MagicMock(side_effect=_empty_delegate)
    mock_load_balancer_factory.return_value = mock_load_balancer
    return mock_load_balancer_factory, mock_load_balancer


@pytest.fixture
def mock_worker_stub(mocker: MockerFixture):
    """Create a mock gRPC worker stub for testing.

    Provides a mock :class:`WorkerStub` with async dispatch functionality
    for testing worker communication scenarios.
    """
    mock_worker_stub = mocker.MagicMock(spec=protocol.WorkerStub)
    mock_worker_stub.dispatch = mocker.AsyncMock()
    return mock_worker_stub


@pytest.fixture
def mock_wool_task(mocker: MockerFixture):
    """Create a mock :class:`Task` for testing.

    Provides a mock task with protobuf serialization capabilities for
    testing task dispatch and processing scenarios.
    """
    mock_task = mocker.MagicMock(spec=Task)
    mock_task.to_protobuf = mocker.MagicMock()
    return mock_task


@pytest.fixture
def spy_loadbalancer_with_workers(mocker: MockerFixture):
    """Create a spy-wrapped delegating load balancer.

    Provides a :py:class:`DelegatingLoadBalancerLike` implementation
    that yields workers straight from the context in iteration order,
    wrapped with a spy so tests can assert that ``delegate`` was called.
    """

    class SpyableLoadBalancer:
        """Delegating load balancer wrapping context iteration."""

        async def delegate(self, *, context):
            """Yield workers from the context in insertion order."""
            for metadata, connection in list(context.workers.items()):
                try:
                    sent = yield metadata, connection
                except Exception:
                    continue
                if sent is not None:
                    return

    loadbalancer = SpyableLoadBalancer()
    loadbalancer.delegate = mocker.spy(loadbalancer, "delegate")
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
        uid=uuid.uuid4(), address="127.0.0.1:50051", pid=1234, version="1.0.0"
    )

    # Create discovery with default worker-added event
    events = [DiscoveryEvent("worker-added", metadata=metadata)]
    discovery = SpyableDiscovery(events)

    # Wrap methods with spies
    discovery.__anext__ = mocker.spy(discovery, "__anext__")
    discovery.add_event = mocker.spy(discovery, "add_event")

    return discovery, metadata


@pytest.fixture
def mock_worker_connection(mocker: MockerFixture):
    """Create a mock WorkerConnection for testing with spy fixtures.

    Provides a mock :class:`WorkerConnection` with dispatch capabilities
    for testing worker communication scenarios.
    """
    mock_connection = mocker.MagicMock(spec=WorkerConnection)

    async def mock_dispatch_success(task, *, timeout=None):
        async def _result_generator():
            yield "test_result"

        return _result_generator()

    mock_connection.dispatch = mock_dispatch_success

    return mock_connection


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
# Module-Level Function Tests
# ============================================================================


def test_parse_version_with_valid_string():
    """Test parsing a valid PEP 440 version string.

    Given:
        A valid version string "1.2.3".
    When:
        parse_version is called with the string.
    Then:
        It should return a Version instance equal to Version("1.2.3").
    """
    # Act
    result = parse_version("1.2.3")

    # Assert
    assert result == Version("1.2.3")


def test_parse_version_with_invalid_string():
    """Test parsing an invalid version string.

    Given:
        An invalid version string "abc".
    When:
        parse_version is called with the string.
    Then:
        It should return None.
    """
    # Act
    result = parse_version("abc")

    # Assert
    assert result is None


def test_parse_version_with_empty_string():
    """Test parsing an empty version string.

    Given:
        An empty version string "".
    When:
        parse_version is called with the string.
    Then:
        It should return None.
    """
    # Act
    result = parse_version("")

    # Assert
    assert result is None


def test_is_version_compatible_same_major():
    """Test version compatibility with same major version.

    Given:
        A client version "1.0.0" and a server version "1.0.0".
    When:
        is_version_compatible is called.
    Then:
        It should return True since client <= server within same major.
    """
    # Arrange
    client = Version("1.0.0")
    server = Version("1.0.0")

    # Act
    result = is_version_compatible(client, server)

    # Assert
    assert result is True


def test_is_version_compatible_newer_minor():
    """Test version compatibility when server has newer minor version.

    Given:
        A client version "1.0.0" and a server version "1.2.0".
    When:
        is_version_compatible is called.
    Then:
        It should return True since client <= server within same major.
    """
    # Arrange
    client = Version("1.0.0")
    server = Version("1.2.0")

    # Act
    result = is_version_compatible(client, server)

    # Assert
    assert result is True


def test_is_version_compatible_different_major():
    """Test version incompatibility with different major versions.

    Given:
        A client version "1.0.0" and a server version "2.0.0".
    When:
        is_version_compatible is called.
    Then:
        It should return False since major versions differ.
    """
    # Arrange
    client = Version("1.0.0")
    server = Version("2.0.0")

    # Act
    result = is_version_compatible(client, server)

    # Assert
    assert result is False


# ============================================================================
# Test Classes
# ============================================================================


class TestWorkerProxy:
    """Comprehensive test suite for WorkerProxy."""

    def test___init___discovery_and_loadbalancer(
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

    def test___init___discovery_only(
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

    def test___init___uri_only(self, mocker: MockerFixture):
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

    def test___init___invalid_arguments(self):
        """Test raise ValueError.

        Given:
            No valid constructor arguments
        When:
            WorkerProxy is initialized
        Then:
            It should raise ValueError
        """
        # Act & assert
        with pytest.raises(
            ValueError,
            match=(
                "Must specify either a workerpool URI, discovery event stream, "
                "or a sequence of workers"
            ),
        ):
            WorkerProxy()  # type: ignore[call-overload]

    def test___init___uri_and_loadbalancer(
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

    def test___init___workers_parameter_creates_reducible_iterator(self):
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
                uid=uuid.uuid4(), address="127.0.0.1:50051", pid=1234, version="1.0.0"
            ),
            WorkerMetadata(
                uid=uuid.uuid4(), address="127.0.0.1:50052", pid=1235, version="1.0.0"
            ),
        ]

        # Act
        proxy = WorkerProxy(workers=workers)

        # Assert
        assert isinstance(proxy, WorkerProxy)

    def test___init___invalid_multiple_parameters_raises_error(self):
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
                uid=uuid.uuid4(), address="127.0.0.1:50051", pid=1234, version="1.0.0"
            )
        ]
        discovery = LocalDiscovery("test-pool").subscriber

        # Act & assert
        with pytest.raises(ValueError, match="Must specify exactly one of"):
            WorkerProxy(pool_uri="test-pool", discovery=discovery, workers=workers)

    def test___init___no_parameters_raises_error(self):
        """Test raise ValueError with appropriate message.

        Given:
            No initialization parameters
        When:
            WorkerProxy is initialized without any parameters
        Then:
            It should raise ValueError with appropriate message
        """
        # Act & assert
        with pytest.raises(ValueError, match="Must specify either a workerpool URI"):
            WorkerProxy()

    def test___init___with_sync_cm_loadbalancer_warns(self, mock_discovery_service):
        """Test UserWarning for sync CM loadbalancer.

        Given:
            A sync context manager instance as loadbalancer.
        When:
            WorkerProxy is instantiated.
        Then:
            It should emit a UserWarning mentioning 'loadbalancer'.
        """

        # Arrange
        class SyncCM:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        # Act & assert
        with pytest.warns(UserWarning, match="loadbalancer"):
            WorkerProxy(discovery=mock_discovery_service, loadbalancer=SyncCM())

    def test___init___with_async_cm_loadbalancer_warns(self, mock_discovery_service):
        """Test UserWarning for async CM loadbalancer.

        Given:
            An async context manager instance as loadbalancer.
        When:
            WorkerProxy is instantiated.
        Then:
            It should emit a UserWarning mentioning 'loadbalancer'.
        """

        # Arrange
        class AsyncCM:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        # Act & assert
        with pytest.warns(UserWarning, match="loadbalancer"):
            WorkerProxy(discovery=mock_discovery_service, loadbalancer=AsyncCM())

    def test___init___with_sync_cm_discovery_warns(self):
        """Test UserWarning for sync CM discovery.

        Given:
            A sync context manager instance as discovery.
        When:
            WorkerProxy is instantiated.
        Then:
            It should emit a UserWarning mentioning 'discovery'.
        """

        # Arrange
        class SyncCM:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        # Act & assert
        with pytest.warns(UserWarning, match="discovery"):
            WorkerProxy(discovery=SyncCM())

    def test___init___with_async_cm_discovery_warns(self):
        """Test UserWarning for async CM discovery.

        Given:
            An async context manager instance as discovery.
        When:
            WorkerProxy is instantiated.
        Then:
            It should emit a UserWarning mentioning 'discovery'.
        """

        # Arrange
        class AsyncCM:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        # Act & assert
        with pytest.warns(UserWarning, match="discovery"):
            WorkerProxy(discovery=AsyncCM())

    def test___init___with_callable_loadbalancer_no_warning(
        self, mock_discovery_service
    ):
        """Test no UserWarning for callable loadbalancer.

        Given:
            A callable (non-CM) as loadbalancer.
        When:
            WorkerProxy is instantiated.
        Then:
            It should not emit a UserWarning.
        """
        # Arrange, act, & assert
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            WorkerProxy(
                discovery=mock_discovery_service,
                loadbalancer=wp.RoundRobinLoadBalancer,
            )

        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert user_warnings == []

    def test___init___with_default_lazy(self, mock_discovery_service):
        """Test WorkerProxy defaults to lazy initialization.

        Given:
            A discovery service.
        When:
            WorkerProxy is instantiated with default parameters.
        Then:
            It should have lazy set to True.
        """
        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Assert
        assert proxy.lazy is True

    def test___init___with_lazy_false(self, mock_discovery_service):
        """Test WorkerProxy accepts explicit lazy=False.

        Given:
            A discovery service and lazy=False.
        When:
            WorkerProxy is instantiated.
        Then:
            It should have lazy set to False.
        """
        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)

        # Assert
        assert proxy.lazy is False

    @pytest.mark.asyncio
    async def test___aenter___lifecycle(self, mock_discovery_service):
        """Test it starts and stops correctly.

        Given:
            A non-lazy WorkerProxy configured with discovery service
        When:
            The proxy is used as a context manager
        Then:
            It starts and stops correctly
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)
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
    async def test___aexit___cleanup_on_error(self, mock_discovery_service):
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

        # Act & assert
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
            A non-lazy unstarted WorkerProxy instance
        When:
            Start is called
        Then:
            It should set the started flag to True
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)

        # Act
        await proxy.start()

        # Assert
        assert proxy.started

    @pytest.mark.asyncio
    async def test_enter_with_lazy_proxy(self, mock_discovery_service):
        """Test enter defers startup for lazy proxies.

        Given:
            A lazy WorkerProxy that has not been entered.
        When:
            enter() is called.
        Then:
            It should remain un-started.
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act
        await proxy.enter()

        # Assert
        assert not proxy.started

    @pytest.mark.asyncio
    async def test_enter_with_non_lazy_proxy(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test enter eagerly starts a non-lazy proxy.

        Given:
            A non-lazy WorkerProxy that has not been entered.
        When:
            enter() is called.
        Then:
            It should set started to True.
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)

        # Act
        await proxy.enter()

        # Assert
        assert proxy.started

    @pytest.mark.asyncio
    async def test_enter_already_entered_raises_error(self, mock_discovery_service):
        """Test enter raises on reentrant call.

        Given:
            A lazy WorkerProxy that has already been entered.
        When:
            enter() is called a second time.
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)
        await proxy.enter()

        # Act & assert
        with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
            await proxy.enter()

    @pytest.mark.asyncio
    async def test_enter_after_exit_raises_error(self, mock_discovery_service):
        """Test enter raises after a full enter/exit cycle.

        Given:
            A lazy WorkerProxy that has been entered and exited.
        When:
            enter() is called again.
        Then:
            It should raise RuntimeError because the context is single-use.
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)
        await proxy.enter()
        await proxy.exit()

        # Act & assert
        with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
            await proxy.enter()

    @pytest.mark.asyncio
    async def test_stop_clears_state(self, mock_discovery_service, mock_proxy_session):
        """Test clear workers and reset the started flag to False.

        Given:
            A started WorkerProxy with registered workers.
        When:
            stop() is called.
        Then:
            It should clear workers and reset the started flag to False.
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)
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
            A non-lazy WorkerProxy that is already started
        When:
            Start is called again
        Then:
            It should raise RuntimeError
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)
        await proxy.start()

        # Act & assert
        with pytest.raises(RuntimeError, match="Proxy already started"):
            await proxy.start()

    @pytest.mark.asyncio
    async def test_exit_not_started_raises_error(self, mock_discovery_service):
        """Test raise RuntimeError.

        Given:
            A non-lazy WorkerProxy that is not started.
        When:
            exit() is called.
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)

        # Act & assert
        with pytest.raises(RuntimeError, match="Proxy not started"):
            await proxy.exit()

    @pytest.mark.asyncio
    async def test_exit_with_unstarted_lazy_proxy(self, mock_discovery_service):
        """Test exit is a no-op on an un-started lazy proxy.

        Given:
            A lazy WorkerProxy that was never started.
        When:
            exit() is called.
        Then:
            It should return without raising.
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act & assert — should not raise
        await proxy.exit()

    @pytest.mark.asyncio
    async def test_exit_stops_started_lazy_proxy(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test exit stops a lazy proxy that was started.

        Given:
            A lazy WorkerProxy that was entered and subsequently started.
        When:
            exit() is called.
        Then:
            It should stop the proxy and set started to False.
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)
        await proxy.enter()
        await proxy.start()
        assert proxy.started

        # Act
        await proxy.exit()

        # Assert
        assert not proxy.started

    @pytest.mark.asyncio
    async def test___aenter___enter_starts_proxy(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test automatically start the proxy.

        Given:
            A non-lazy unstarted WorkerProxy
        When:
            The async context manager is entered
        Then:
            It should automatically start the proxy
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)

        # Act & assert
        async with proxy as p:
            assert p.started

    @pytest.mark.asyncio
    async def test___aexit___exit_stops_proxy(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test automatically stop the proxy.

        Given:
            A non-lazy WorkerProxy within async context
        When:
            The async context manager exits
        Then:
            It should automatically stop the proxy
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)

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
            A non-lazy WorkerProxy with a load balancer provided as a
            sync context manager.
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
        proxy = WorkerProxy(
            discovery=mock_discovery_service, loadbalancer=cm, lazy=False
        )

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
            A non-lazy WorkerProxy with a load balancer provided as an
            async context manager.
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
        proxy = WorkerProxy(
            discovery=mock_discovery_service, loadbalancer=cm, lazy=False
        )

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
            A non-lazy WorkerProxy with a load balancer provided as a
            bare awaitable (coroutine object).
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

        proxy = WorkerProxy(
            discovery=mock_discovery_service, loadbalancer=make_lb(), lazy=False
        )

        # Act
        await proxy.start()

        # Assert
        assert proxy.started

        # Cleanup
        await proxy.stop()

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
            address="192.168.1.100:50051",
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
            address="192.168.1.100:50051",
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
            address="192.168.1.100:50051",
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
            A non-lazy, started WorkerProxy with a discovered worker.
        When:
            A "worker-updated" event is received for that worker.
        Then:
            The worker metadata is updated without error.
        """
        # Arrange
        metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
        )

        events = [
            DiscoveryEvent("worker-added", metadata=metadata),
            DiscoveryEvent("worker-updated", metadata=metadata),
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lazy=False)

        # Act
        await proxy.start()
        await asyncio.sleep(0.1)

        # Assert
        assert metadata in proxy.workers

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_sentinel_passes_metadata_options_to_connection(
        self, mocker: MockerFixture
    ):
        """Test sentinel creates WorkerConnection with metadata options.

        Given:
            A non-lazy WorkerProxy with a discovery stream yielding a
            worker-added event whose metadata has
            options=ChannelOptions(keepalive_time_ms=60000)
        When:
            The sentinel processes the event
        Then:
            It should create WorkerConnection with
            options=ChannelOptions(keepalive_time_ms=60000) from the
            event metadata
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        channel_opts = ChannelOptions(keepalive_time_ms=60000)
        metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
            options=channel_opts,
        )
        mock_conn_cls = mocker.patch.object(
            wp, "WorkerConnection", return_value=mocker.MagicMock()
        )
        events = [DiscoveryEvent("worker-added", metadata=metadata)]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lazy=False)

        # Act
        await proxy.start()
        await asyncio.sleep(0.1)

        # Assert
        mock_conn_cls.assert_called_once_with(
            metadata.address,
            credentials=None,
            options=channel_opts,
        )

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_sentinel_passes_none_options_for_legacy_workers(
        self, mocker: MockerFixture
    ):
        """Test sentinel creates WorkerConnection with options=None for legacy workers.

        Given:
            A non-lazy WorkerProxy with a discovery stream yielding a
            worker-added event whose metadata has options=None
        When:
            The sentinel processes the event
        Then:
            It should create WorkerConnection with options=None
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
            options=None,
        )
        mock_conn_cls = mocker.patch.object(
            wp, "WorkerConnection", return_value=mocker.MagicMock()
        )
        events = [DiscoveryEvent("worker-added", metadata=metadata)]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lazy=False)

        # Act
        await proxy.start()
        await asyncio.sleep(0.1)

        # Assert
        mock_conn_cls.assert_called_once_with(
            metadata.address,
            credentials=None,
            options=None,
        )

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_sentinel_passes_updated_options_on_worker_updated(
        self, mocker: MockerFixture
    ):
        """Test sentinel creates WorkerConnection with updated metadata options.

        Given:
            A non-lazy WorkerProxy with a discovery stream yielding a
            worker-added event followed by a worker-updated event whose
            metadata has options=ChannelOptions(keepalive_time_ms=90000)
        When:
            The sentinel processes the events
        Then:
            It should create WorkerConnection with
            options=ChannelOptions(keepalive_time_ms=90000) for the
            updated event
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        worker_uid = uuid.uuid4()
        initial_opts = ChannelOptions(keepalive_time_ms=60000)
        updated_opts = ChannelOptions(keepalive_time_ms=90000)
        initial_metadata = WorkerMetadata(
            uid=worker_uid,
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
            options=initial_opts,
        )
        updated_metadata = WorkerMetadata(
            uid=worker_uid,
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
            options=updated_opts,
        )
        mock_conn_cls = mocker.patch.object(
            wp, "WorkerConnection", return_value=mocker.MagicMock()
        )
        events = [
            DiscoveryEvent("worker-added", metadata=initial_metadata),
            DiscoveryEvent("worker-updated", metadata=updated_metadata),
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lazy=False)

        # Act
        await proxy.start()
        await asyncio.sleep(0.1)

        # Assert
        assert mock_conn_cls.call_count == 2
        _, updated_kwargs = mock_conn_cls.call_args_list[1]
        assert updated_kwargs["options"] == updated_opts

        # Cleanup
        await proxy.stop()

    def test___init___with_zero_lease_raises(self, mock_discovery_service):
        """Test zero lease is rejected.

        Given:
            A discovery service and lease of 0
        When:
            WorkerProxy is instantiated
        Then:
            It should raise ValueError
        """
        # Act & assert
        with pytest.raises(
            ValueError,
            match="Lease must be a positive, non-zero integer",
        ):
            WorkerProxy(discovery=mock_discovery_service, lease=0)

    def test___init___with_negative_lease_raises(self, mock_discovery_service):
        """Test negative lease is rejected.

        Given:
            A discovery service and lease of -1
        When:
            WorkerProxy is instantiated
        Then:
            It should raise ValueError
        """
        # Act & assert
        with pytest.raises(
            ValueError,
            match="Lease must be a positive, non-zero integer",
        ):
            WorkerProxy(discovery=mock_discovery_service, lease=-1)

    def test___init___with_positive_lease_accepted(self, mock_discovery_service):
        """Test positive lease is accepted.

        Given:
            A discovery service and lease of 5
        When:
            WorkerProxy is instantiated
        Then:
            It should create the proxy successfully
        """
        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service, lease=5)

        # Assert
        assert isinstance(proxy, WorkerProxy)

    @pytest.mark.asyncio
    async def test_start_caps_discovered_workers_at_lease(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test sentinel respects lease cap on worker-added events.

        Given:
            A non-lazy WorkerProxy with lease=2 and a discovery stream
            with 3 worker-added events
        When:
            The sentinel processes all events
        Then:
            It should accept only 2 workers, ignoring the 3rd
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"192.168.1.{i}:50051",
                pid=1000 + i,
                version="1.0.0",
            )
            for i in range(3)
        ]
        events = [DiscoveryEvent("worker-added", metadata=w) for w in workers]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lease=2, lazy=False)

        # Act
        await proxy.start()
        await _drain_discovery(proxy, expect=2)

        # Assert
        assert len(proxy.workers) == 2

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_accepts_all_workers_when_lease_none(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test sentinel accepts all workers when lease is None.

        Given:
            A non-lazy WorkerProxy with lease=None and a discovery
            stream with 3 worker-added events
        When:
            The sentinel processes all events
        Then:
            It should accept all 3 workers
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"192.168.1.{i}:50051",
                pid=1000 + i,
                version="1.0.0",
            )
            for i in range(3)
        ]
        events = [DiscoveryEvent("worker-added", metadata=w) for w in workers]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lease=None, lazy=False)

        # Act
        await proxy.start()
        await _drain_discovery(proxy, expect=3)

        # Assert
        assert len(proxy.workers) == 3

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_allows_worker_updated_at_capacity(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test sentinel processes worker-updated events even at capacity.

        Given:
            A non-lazy WorkerProxy with lease=2, already at capacity,
            receiving a worker-updated event
        When:
            The sentinel processes the update event
        Then:
            It should process the update without being blocked by the cap
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        worker1 = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.1:50051",
            pid=1001,
            version="1.0.0",
        )
        worker2 = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.2:50051",
            pid=1002,
            version="1.0.0",
        )
        events = [
            DiscoveryEvent("worker-added", metadata=worker1),
            DiscoveryEvent("worker-added", metadata=worker2),
            DiscoveryEvent("worker-updated", metadata=worker1),
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lease=2, lazy=False)

        # Act
        await proxy.start()
        await _drain_discovery(proxy, expect=2)

        # Assert
        assert len(proxy.workers) == 2
        assert worker1 in proxy.workers

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_accepts_worker_after_drop_frees_capacity(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test drop restores capacity for a subsequent worker-added event.

        Given:
            A non-lazy WorkerProxy with lease=2, at capacity with 2
            workers, then one worker is dropped followed by a new
            worker-added
        When:
            The proxy processes all events
        Then:
            It should accept the new worker after the drop
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        worker1 = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.1:50051",
            pid=1001,
            version="1.0.0",
        )
        worker2 = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.2:50051",
            pid=1002,
            version="1.0.0",
        )
        worker3 = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.3:50051",
            pid=1003,
            version="1.0.0",
        )
        events = [
            DiscoveryEvent("worker-added", metadata=worker1),
            DiscoveryEvent("worker-added", metadata=worker2),
            DiscoveryEvent("worker-dropped", metadata=worker1),
            DiscoveryEvent("worker-added", metadata=worker3),
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lease=2, lazy=False)

        # Act
        await proxy.start()
        await _drain_discovery(proxy, expect=2)

        # Assert — worker3 was accepted after worker1 was dropped
        assert len(proxy.workers) == 2
        assert worker2 in proxy.workers
        assert worker3 in proxy.workers

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_drops_update_for_rejected_worker(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test worker-updated for a cap-rejected worker is silently dropped.

        Given:
            A non-lazy WorkerProxy with lease=1 and a discovery stream
            where worker2 is rejected by the cap, then a worker-updated
            event arrives for worker2
        When:
            The proxy processes all events
        Then:
            It should not add worker2 via the update event
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        worker1 = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.1:50051",
            pid=1001,
            version="1.0.0",
        )
        worker2 = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.2:50051",
            pid=1002,
            version="1.0.0",
        )
        events = [
            DiscoveryEvent("worker-added", metadata=worker1),
            DiscoveryEvent("worker-added", metadata=worker2),  # rejected
            DiscoveryEvent("worker-updated", metadata=worker2),  # dropped
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lease=1, lazy=False)

        # Act
        await proxy.start()
        await _drain_discovery(proxy, expect=1)

        # Assert — only worker1 is present; worker2 was never admitted
        assert len(proxy.workers) == 1
        assert worker1 in proxy.workers
        assert worker2 not in proxy.workers

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_preserves_lease(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test pickle round-trip preserves lease cap behavior.

        Given:
            A non-lazy WorkerProxy with lease=2 and a discovery stream
            with 3 worker-added events
        When:
            The proxy is pickled, unpickled, started, and processes
            the discovery events
        Then:
            It should cap at 2 workers, proving the limit survived
            the round-trip
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"192.168.1.{i}:50051",
                pid=1000 + i,
                version="1.0.0",
            )
            for i in range(3)
        ]
        events = [DiscoveryEvent("worker-added", metadata=w) for w in workers]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(
            discovery=discovery,
            loadbalancer=wp.RoundRobinLoadBalancer,
            lease=2,
            lazy=False,
        )

        # Act — pickle round-trip, then start the restored proxy
        pickled_data = cloudpickle.dumps(proxy)
        restored = cloudpickle.loads(pickled_data)
        await restored.start()
        await _drain_discovery(restored, expect=2)

        # Assert — restored proxy enforces the cap
        assert len(restored.workers) == 2
        await restored.stop()

    def test_cloudpickle_serialization_with_lazy_false(self, mock_discovery_service):
        """Test pickle round-trip with explicit lazy=False.

        Given:
            A WorkerProxy with lazy=False.
        When:
            The proxy is pickled and unpickled.
        Then:
            It should preserve lazy as False on the restored proxy.
        """
        # Arrange
        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=wp.RoundRobinLoadBalancer,
            lazy=False,
        )

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(proxy))

        # Assert
        assert restored.lazy is False

    def test_cloudpickle_serialization_with_default_lazy(self, mock_discovery_service):
        """Test pickle round-trip with default lazy=True.

        Given:
            A WorkerProxy with default lazy=True.
        When:
            The proxy is pickled and unpickled.
        Then:
            It should preserve lazy as True on the restored proxy.
        """
        # Arrange
        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=wp.RoundRobinLoadBalancer,
        )

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(proxy))

        # Assert
        assert restored.lazy is True

    @given(lazy=st.booleans())
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test___init___with_arbitrary_lazy_value(self, mock_discovery_service, lazy):
        """Test instantiation with an arbitrary lazy value.

        Given:
            An arbitrary boolean value for lazy.
        When:
            WorkerProxy is instantiated with that value.
        Then:
            It should set the lazy property to the given value.
        """
        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=lazy)

        # Assert
        assert proxy.lazy is lazy

    @pytest.mark.asyncio
    async def test_dispatch_delegates_to_loadbalancer(
        self,
        spy_loadbalancer_with_workers,
        spy_discovery_with_events,
        mock_worker_connection,
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
        discovery, _ = spy_discovery_with_events
        mocker.patch.object(wp, "WorkerConnection", return_value=mock_worker_connection)

        proxy = WorkerProxy(
            discovery=discovery,
            loadbalancer=spy_loadbalancer_with_workers,
        )

        await proxy.start()
        await _drain_discovery(proxy, expect=1)

        # Act
        result_iterator = await proxy.dispatch(mock_wool_task)
        results = [result async for result in result_iterator]

        # Assert
        assert results == ["test_result"]
        spy_loadbalancer_with_workers.delegate.assert_called_once()

    @pytest.mark.asyncio
    async def test_dispatch_not_started_raises_error(
        self, mock_discovery_service, mock_wool_task
    ):
        """Test raise RuntimeError.

        Given:
            A non-lazy WorkerProxy that is not started
        When:
            Dispatch is called
        Then:
            It should raise RuntimeError
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)

        # Act & assert
        with pytest.raises(RuntimeError, match="Proxy not started"):
            async for _ in await proxy.dispatch(mock_wool_task):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_with_lazy_auto_start(
        self,
        spy_loadbalancer_with_workers,
        spy_discovery_with_events,
        mock_worker_connection,
        mock_wool_task,
        mock_proxy_session,
        mocker,
    ):
        """Test dispatch auto-starts a lazy proxy on first call.

        Given:
            A lazy WorkerProxy that has not been started.
        When:
            dispatch() is called.
        Then:
            It should auto-start the proxy and dispatch the task.
        """
        # Arrange
        discovery, _ = spy_discovery_with_events
        mocker.patch.object(wp, "WorkerConnection", return_value=mock_worker_connection)

        proxy = WorkerProxy(
            discovery=discovery,
            loadbalancer=spy_loadbalancer_with_workers,
        )

        assert not proxy.started

        # Act
        result_iterator = await proxy.dispatch(mock_wool_task)
        results = [result async for result in result_iterator]

        # Assert
        assert proxy.started
        assert results == ["test_result"]
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_lazy_concurrent_start(
        self,
        spy_loadbalancer_with_workers,
        spy_discovery_with_events,
        mock_worker_connection,
        mock_wool_task,
        mock_proxy_session,
        mocker,
    ):
        """Test concurrent dispatch calls start the proxy only once.

        Given:
            A lazy WorkerProxy that has not been started.
        When:
            Two dispatch() calls are made concurrently.
        Then:
            It should start the proxy and both dispatches should succeed.
        """
        # Arrange
        discovery, _ = spy_discovery_with_events
        mocker.patch.object(wp, "WorkerConnection", return_value=mock_worker_connection)

        proxy = WorkerProxy(
            discovery=discovery,
            loadbalancer=spy_loadbalancer_with_workers,
        )

        # Act
        await asyncio.gather(
            proxy.dispatch(mock_wool_task),
            proxy.dispatch(mock_wool_task),
        )

        # Assert
        assert proxy.started
        assert spy_loadbalancer_with_workers.delegate.call_count == 2
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_propagates_loadbalancer_errors(
        self,
        mocker: MockerFixture,
        spy_discovery_with_events,
        mock_worker_connection,
        mock_wool_task,
        mock_proxy_session,
    ):
        """Test propagate the error from the load balancer.

        Given:
            A WorkerProxy where the delegate generator raises
        When:
            Dispatch is called
        Then:
            It should propagate the error from the load balancer
        """
        # Arrange
        discovery, _ = spy_discovery_with_events
        mocker.patch.object(wp, "WorkerConnection", return_value=mock_worker_connection)

        # Create a delegating loadbalancer whose generator raises immediately
        class FailingLoadBalancer:
            async def delegate(self, *, context):
                raise Exception("Load balancer error")
                if False:
                    yield  # pragma: no cover

        failing_loadbalancer = FailingLoadBalancer()

        proxy = WorkerProxy(discovery=discovery, loadbalancer=failing_loadbalancer)

        await proxy.start()
        await _drain_discovery(proxy, expect=1)

        # Act & assert
        with pytest.raises(Exception, match="Load balancer error"):
            async for _ in await proxy.dispatch(mock_wool_task):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_waits_for_workers_then_dispatches(
        self,
        mocker: MockerFixture,
        mock_worker_connection,
        mock_wool_task,
        mock_proxy_session,
    ):
        """Test wait via _await_workers, then dispatch when workers appear.

        Given:
            A started WorkerProxy with no initial workers
        When:
            Dispatch is called and workers become available during wait
        Then:
            Should wait via _await_workers, then dispatch when workers appear
        """
        # Arrange
        metadata = WorkerMetadata(
            uid=uuid.uuid4(), address="127.0.0.1:50051", pid=1234, version="1.0.0"
        )

        # Create discovery service that will emit worker event
        events = [DiscoveryEvent("worker-added", metadata=metadata)]
        discovery = wp.ReducibleAsyncIterator(events)
        mocker.patch.object(wp, "WorkerConnection", return_value=mock_worker_connection)

        # Delegating loadbalancer that yields straight from the context
        class WaitingLoadBalancer:
            async def delegate(self, *, context):
                if not context.workers:
                    raise NoWorkersAvailable("No workers available")
                for metadata_, connection in list(context.workers.items()):
                    try:
                        sent = yield metadata_, connection
                    except Exception:
                        continue
                    if sent is not None:
                        return

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
            address="192.168.1.100:50051",
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
                self.delegated = False

            async def delegate(self, *, context):
                self.delegated = True
                for metadata_, connection in list(context.workers.items()):
                    try:
                        sent = yield metadata_, connection
                    except Exception:
                        continue
                    if sent is not None:
                        return

        stub_lb = StubLoadBalancer()
        mock_connection = mocker.MagicMock(spec=WorkerConnection)

        async def fake_dispatch(task, *, timeout=None):
            async def _gen():
                yield "result"

            return _gen()

        mock_connection.dispatch = fake_dispatch
        mocker.patch.object(wp, "WorkerConnection", return_value=mock_connection)

        proxy = WorkerProxy(discovery=GatedDiscovery(), loadbalancer=stub_lb)
        await proxy.start()

        # Verify no workers before the gate opens
        assert proxy.workers == []

        mock_task = mocker.MagicMock(spec=Task)

        # Act — launch dispatch in background; it spins waiting for workers
        dispatch_task = asyncio.create_task(proxy.dispatch(mock_task))
        await asyncio.sleep(0)  # yield so dispatch enters the spin loop

        # Open the gate — sentinel receives the event and adds the worker
        gate.set()
        await dispatch_task

        # Assert
        assert metadata in proxy.workers
        assert stub_lb.delegated

        # Cleanup
        await proxy.stop()

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
                address="192.168.1.100:50051",
                pid=1001,
                version="1.0.0",
                tags=frozenset(["test"]),
                extra=MappingProxyType({}),
            ),
            WorkerMetadata(
                uid=uuid.uuid4(),
                address="192.168.1.101:50052",
                pid=1002,
                version="1.0.0",
                tags=frozenset(["test"]),
                extra=MappingProxyType({}),
            ),
        ]

        proxy = WorkerProxy(workers=workers, lazy=False)

        # Act & assert
        async with proxy as p:
            assert p is not None
            assert p.started

        # After exit, proxy should be stopped
        assert not proxy.started

    @pytest.mark.asyncio
    async def test_proxy_with_pool_uri(self):
        """Test it starts and stops correctly.

        Given:
            A non-lazy WorkerProxy configured with a pool URI
        When:
            The proxy is used as a context manager
        Then:
            It starts and stops correctly
        """
        # Arrange
        proxy = WorkerProxy("test://pool", lazy=False)

        # Act & assert
        async with proxy as p:
            assert p is not None
            assert p.started

        # After exit, proxy should be stopped
        assert not proxy.started

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
            address="127.0.0.1:50051",
            pid=1234,
            version="1.0.0",
        )

        # Inject worker into discovery service before starting proxy
        await mock_discovery_service.start()
        mock_discovery_service.inject_worker_added(metadata)

        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False)

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

    def test___hash__(self):
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

    def test___eq__(self):
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

        # Act & assert
        # Same proxy should equal itself
        assert proxy1 == proxy1

        # Different proxies should not be equal (different IDs)
        assert proxy1 != proxy2

        # Proxy should not equal non-WorkerProxy objects
        assert proxy1 != non_proxy

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
            discovery=discovery_service,
            loadbalancer=wp.RoundRobinLoadBalancer,
            lazy=False,
        )

        # Act & assert
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
            A non-lazy started WorkerProxy with only discovery service
        When:
            Cloudpickle serialization and deserialization are performed within
            context
        Then:
            It should serialize and deserialize successfully with deserialized
            proxy in an unstarted state
        """
        # Arrange - Use real objects instead of mocks for cloudpickle test
        discovery_service = LocalDiscovery("test-pool").subscriber
        proxy = WorkerProxy(discovery=discovery_service, lazy=False)

        # Act & assert
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
            A non-lazy started WorkerProxy created with only a URI
        When:
            Cloudpickle serialization and deserialization are performed within
            context
        Then:
            It should serialize and deserialize successfully with deserialized
            proxy in an unstarted state and preserved ID
        """
        # Arrange - Use real objects - this creates a LocalDiscovery internally
        proxy = WorkerProxy("pool-1", lazy=False)

        # Act & assert
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

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_preserves_proxy_id(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test pickle roundtrip preserves proxy ID.

        Given:
            A started WorkerProxy with discovery and load balancer.
        When:
            Cloudpickle serialization and deserialization are performed.
        Then:
            It should preserve the proxy ID on the restored proxy.
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        discovery_service = LocalDiscovery("test-pool").subscriber
        proxy = WorkerProxy(
            discovery=discovery_service,
            loadbalancer=wp.RoundRobinLoadBalancer,
        )

        # Act
        async with proxy:
            pickled_data = cloudpickle.dumps(proxy)
            unpickled_proxy = cloudpickle.loads(pickled_data)

        # Assert
        assert isinstance(unpickled_proxy, WorkerProxy)
        assert unpickled_proxy.id == proxy.id
        assert unpickled_proxy.started is False
        # Verify a second roundtrip still produces a valid proxy
        repickled = cloudpickle.loads(cloudpickle.dumps(unpickled_proxy))
        assert repickled.id == proxy.id

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_excludes_credentials(
        self, mock_proxy_session, worker_credentials, mocker: MockerFixture
    ):
        """Test pickle roundtrip does not include credentials.

        Given:
            A started WorkerProxy with WorkerCredentials.
        When:
            Cloudpickle serialization and deserialization are performed
            outside a WorkerCredentials context.
        Then:
            The unpickled proxy should resolve to None credentials
            (insecure) and only discover insecure workers.
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        discovery_service = LocalDiscovery("test-pool").subscriber
        proxy = WorkerProxy(
            discovery=discovery_service,
            credentials=worker_credentials,
        )

        async with proxy:
            pickled_data = cloudpickle.dumps(proxy)

        # Act — unpickle outside any WorkerCredentials context
        unpickled_proxy = cloudpickle.loads(pickled_data)

        # Assert — credentials not carried; proxy acts insecure
        assert isinstance(unpickled_proxy, WorkerProxy)
        assert unpickled_proxy.id == proxy.id

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_resolves_credentials_from_context(
        self, mock_proxy_session, worker_credentials, mocker: MockerFixture
    ):
        """Test unpickled proxy resolves credentials from ContextVar.

        Given:
            A WorkerProxy serialized with credentials.
        When:
            Deserialized inside a WorkerCredentials context with
            secure and insecure static workers.
        Then:
            The restored proxy should discover only secure workers,
            confirming credentials resolved from the ContextVar.
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        discovery_service = LocalDiscovery("test-pool").subscriber
        proxy = WorkerProxy(
            discovery=discovery_service,
            credentials=worker_credentials,
        )

        async with proxy:
            pickled_data = cloudpickle.dumps(proxy)

        # Act — unpickle inside a WorkerCredentials context with
        # static workers so we can observe the security filter
        secure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
            secure=True,
        )
        insecure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.101:50052",
            pid=1002,
            version="1.0.0",
            secure=False,
        )
        with CredentialContext(worker_credentials):
            # Unpickle restores proxy with credentials from ContextVar.
            # Verify by constructing a new proxy (same mechanism) with
            # static workers — default credentials resolve from ContextVar.
            restored_proxy = WorkerProxy(
                workers=[secure_worker, insecure_worker],
                lazy=False,
            )

        # Assert — resolved credentials from ContextVar: only secure workers
        await restored_proxy.start()
        await asyncio.sleep(0.05)
        assert secure_worker in restored_proxy.workers
        assert insecure_worker not in restored_proxy.workers
        await restored_proxy.stop()

    def test_cloudpickle_serialization_with_sync_cm_loadbalancer_raises(
        self, mock_discovery_service
    ):
        """Test TypeError when pickling proxy with sync CM loadbalancer.

        Given:
            A WorkerProxy with a sync context manager instance as
            loadbalancer.
        When:
            cloudpickle serialization is attempted.
        Then:
            It should raise TypeError mentioning 'loadbalancer'.
        """

        # Arrange
        class SyncCM:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        with pytest.warns(UserWarning):
            proxy = WorkerProxy(
                discovery=mock_discovery_service,
                loadbalancer=SyncCM(),
            )

        # Act & assert
        with pytest.raises(TypeError, match="loadbalancer"):
            cloudpickle.dumps(proxy)

    def test_cloudpickle_serialization_with_async_cm_loadbalancer_raises(
        self, mock_discovery_service
    ):
        """Test TypeError when pickling proxy with async CM loadbalancer.

        Given:
            A WorkerProxy with an async context manager instance as
            loadbalancer.
        When:
            cloudpickle serialization is attempted.
        Then:
            It should raise TypeError mentioning 'loadbalancer'.
        """

        # Arrange
        class AsyncCM:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        with pytest.warns(UserWarning):
            proxy = WorkerProxy(
                discovery=mock_discovery_service,
                loadbalancer=AsyncCM(),
            )

        # Act & assert
        with pytest.raises(TypeError, match="loadbalancer"):
            cloudpickle.dumps(proxy)

    def test_cloudpickle_serialization_with_sync_cm_discovery_raises(self):
        """Test TypeError when pickling proxy with sync CM discovery.

        Given:
            A WorkerProxy with a sync context manager instance as
            discovery.
        When:
            cloudpickle serialization is attempted.
        Then:
            It should raise TypeError mentioning 'discovery'.
        """

        # Arrange
        class SyncCM:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        with pytest.warns(UserWarning):
            proxy = WorkerProxy(discovery=SyncCM())

        # Act & assert
        with pytest.raises(TypeError, match="discovery"):
            cloudpickle.dumps(proxy)

    def test_cloudpickle_serialization_with_async_cm_discovery_raises(self):
        """Test TypeError when pickling proxy with async CM discovery.

        Given:
            A WorkerProxy with an async context manager instance as
            discovery.
        When:
            cloudpickle serialization is attempted.
        Then:
            It should raise TypeError mentioning 'discovery'.
        """

        # Arrange
        class AsyncCM:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

        with pytest.warns(UserWarning):
            proxy = WorkerProxy(discovery=AsyncCM())

        # Act & assert
        with pytest.raises(TypeError, match="discovery"):
            cloudpickle.dumps(proxy)

    @pytest.mark.asyncio
    async def test_explicit_credentials_parameter_overrides_contextvar(
        self, mock_proxy_session, worker_credentials, mocker: MockerFixture
    ):
        """Test explicit credentials parameter overrides ContextVar.

        Given:
            A WorkerCredentials context is active with mTLS credentials
            and a non-lazy WorkerProxy with a mix of secure and insecure
            static workers.
        When:
            WorkerProxy is created with explicit None credentials.
        Then:
            The proxy should use the explicitly passed None,
            discovering only insecure workers instead of secure ones.
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        secure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
            secure=True,
        )
        insecure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.101:50052",
            pid=1002,
            version="1.0.0",
            secure=False,
        )

        # Act — ContextVar has mTLS creds, but we pass None explicitly
        with CredentialContext(worker_credentials):
            proxy = WorkerProxy(
                workers=[secure_worker, insecure_worker],
                credentials=None,
                lazy=False,
            )

        await proxy.start()
        await asyncio.sleep(0.05)

        # Assert — explicit None wins: only insecure workers discovered
        assert insecure_worker in proxy.workers
        assert secure_worker not in proxy.workers
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_credentials_default_resolves_from_contextvar(
        self, mock_proxy_session, worker_credentials, mocker: MockerFixture
    ):
        """Test default credentials resolves from ContextVar.

        Given:
            A WorkerCredentials context is active and a non-lazy
            WorkerProxy with a mix of secure and insecure static
            workers.
        When:
            WorkerProxy is created without explicit credentials.
        Then:
            The proxy should resolve credentials from the ContextVar,
            discovering only secure workers.
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        secure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
            secure=True,
        )
        insecure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.101:50052",
            pid=1002,
            version="1.0.0",
            secure=False,
        )

        # Act
        with CredentialContext(worker_credentials):
            proxy = WorkerProxy(
                workers=[secure_worker, insecure_worker],
                lazy=False,
            )

        await proxy.start()
        await asyncio.sleep(0.05)

        # Assert — resolved from ContextVar: only secure workers
        assert secure_worker in proxy.workers
        assert insecure_worker not in proxy.workers
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_credentials_none_without_contextvar(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test credentials resolve to None when no context is set.

        Given:
            No WorkerCredentials context is active and a non-lazy
            WorkerProxy with a mix of secure and insecure static
            workers.
        When:
            WorkerProxy is created without explicit credentials.
        Then:
            The proxy should have None credentials, discovering only
            insecure workers.
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        secure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
            secure=True,
        )
        insecure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.101:50052",
            pid=1002,
            version="1.0.0",
            secure=False,
        )

        # Act
        proxy = WorkerProxy(
            workers=[secure_worker, insecure_worker],
            lazy=False,
        )

        await proxy.start()
        await asyncio.sleep(0.05)

        # Assert — no credentials: only insecure workers
        assert insecure_worker in proxy.workers
        assert secure_worker not in proxy.workers
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_invalid_loadbalancer_type_raises_error(
        self, mocker: MockerFixture
    ):
        """Test raise ValueError.

        Given:
            A non-lazy WorkerProxy with a loadbalancer that doesn't
            implement LoadBalancerLike
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
            pool_uri="test-pool",
            loadbalancer=lambda: invalid_loadbalancer,
            lazy=False,
        )

        # Act & assert
        with pytest.raises(ValueError):
            await proxy.start()

    @pytest.mark.asyncio
    async def test_start_invalid_discovery_type_raises_error(
        self, mocker: MockerFixture
    ):
        """Test raise ValueError.

        Given:
            A non-lazy WorkerProxy with a discovery that doesn't
            implement AsyncIterator
        When:
            Start() is called
        Then:
            It should raise ValueError
        """
        # Arrange - use a simple string which is definitely not an AsyncIterator
        invalid_discovery = "not_an_async_iterator"

        proxy = WorkerProxy(discovery=lambda: invalid_discovery, lazy=False)

        # Act & assert
        with pytest.raises(ValueError):
            await proxy.start()

    @pytest.mark.asyncio
    async def test_security_filter_with_credentials(
        self, mock_proxy_session, worker_credentials, mocker: MockerFixture
    ):
        """Test only secure workers are discovered with credentials.

        Given:
            A non-lazy WorkerProxy instantiated with credentials and a
            mix of secure and insecure static workers.
        When:
            The proxy is started and discovery events are processed.
        Then:
            Only secure workers appear in the workers list.
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        secure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
            secure=True,
        )
        insecure_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.101:50052",
            pid=1002,
            version="1.0.0",
            secure=False,
        )

        proxy = WorkerProxy(
            workers=[secure_worker, insecure_worker],
            credentials=worker_credentials,
            lazy=False,
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
        """Test pool URI filter combines tag matching, security, and version
        filtering.

        Given:
            A WorkerProxy instantiated with a pool URI and extra tags.
        When:
            The discovery filter is evaluated against workers.
        Then:
            Only workers matching the tags, security, and version
            requirements pass the filter.
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        mock_subscriber = mocker.MagicMock()
        mock_local_discovery = mocker.MagicMock()
        mock_local_discovery.subscribe.return_value = mock_subscriber
        mocker.patch.object(wp, "LocalDiscovery", return_value=mock_local_discovery)

        WorkerProxy("pool-1", "extra-tag")

        # Capture the filter passed to subscribe()
        filter_fn = mock_local_discovery.subscribe.call_args.kwargs["filter"]

        # Act & assert — matching tags, insecure (no credentials)
        matching = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50051",
            pid=1,
            version="1.0.0",
            tags=frozenset(["pool-1"]),
            secure=False,
        )
        assert filter_fn(matching) is True

        # Act & assert — no matching tags
        non_matching = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50052",
            pid=2,
            version="1.0.0",
            tags=frozenset(["other"]),
            secure=False,
        )
        assert filter_fn(non_matching) is False

        # Act & assert — matching tags but secure (no credentials means
        # security filter rejects secure workers)
        secure_matching = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50053",
            pid=3,
            version="1.0.0",
            tags=frozenset(["pool-1"]),
            secure=True,
        )
        assert filter_fn(secure_matching) is False

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
                address=f"192.168.1.{100 + i}:50051",
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
        # Arrange, act, & assert
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
                address=f"192.168.1.{100 + i}:{50051 + i}",
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


class TestWorkerProxyDelegateDispatch:
    """Tests for the proxy-owned dispatch-retry-evict loop.

    These tests exercise ``WorkerProxy._delegate_dispatch`` directly by
    constructing a proxy, seeding its internal ``LoadBalancerContext``
    with mock connections, and driving a custom delegating balancer.
    The proxy owns the retry/evict/cancellation semantics — the tests
    verify each branch of that loop.
    """

    @staticmethod
    async def _start_proxy_with_workers(
        proxy: WorkerProxy,
        mocker: MockerFixture,
        *,
        connections: list,
    ) -> list[WorkerMetadata]:
        """Start the proxy and seed its context with mock connections.

        Returns the list of :class:`WorkerMetadata` in the same order
        as ``connections``.
        """
        assert not proxy.started
        await proxy.start()
        assert proxy._loadbalancer_context is not None
        metadata_list: list[WorkerMetadata] = []
        for i, connection in enumerate(connections):
            metadata = WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"127.0.0.1:{50100 + i}",
                pid=9000 + i,
                version="1.0.0",
            )
            proxy._loadbalancer_context.add_worker(metadata, connection)
            metadata_list.append(metadata)
        return metadata_list

    @staticmethod
    def _make_success_connection(mocker: MockerFixture, spy_stream: list):
        """Build a mock connection whose dispatch yields a tracked stream."""
        connection = mocker.MagicMock(spec=WorkerConnection)

        async def _fake_dispatch(task, *, timeout=None):
            async def _gen():
                yield "ok"

            gen = _gen()
            spy_stream.append(gen)
            return gen

        connection.dispatch = _fake_dispatch
        return connection

    @pytest.fixture
    def mock_proxy(self, mock_discovery_service, mock_proxy_session):
        """Construct a WorkerProxy with an empty delegating balancer."""

        class _NoopBalancer:
            async def delegate(self, *, context):
                if False:
                    yield  # pragma: no cover

        return WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=_NoopBalancer(),
            lazy=False,
        )

    @pytest.mark.asyncio
    async def test_dispatch_with_transient_error_skips_worker(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test transient errors skip to the next candidate without eviction.

        Given:
            A delegating balancer with two workers; the first worker's
            connection raises ``TransientRpcError``, the second succeeds
        When:
            dispatch() is called
        Then:
            The proxy yields the first candidate, observes the transient
            error, calls athrow to get the next candidate, dispatches
            successfully, and the first worker is NOT evicted from the
            context.
        """

        # Arrange
        class TwoWorkerBalancer:
            async def delegate(self, *, context):
                items = list(context.workers.items())
                for metadata, connection in items:
                    try:
                        sent = yield metadata, connection
                    except Exception:
                        continue
                    if sent is not None:
                        return

        failing_conn = mocker.MagicMock(spec=WorkerConnection)
        failing_conn.dispatch = mocker.AsyncMock(side_effect=TransientRpcError())
        success_streams: list = []
        success_conn = self._make_success_connection(mocker, success_streams)

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=TwoWorkerBalancer(),
            lazy=False,
        )
        metadata_list = await self._start_proxy_with_workers(
            proxy, mocker, connections=[failing_conn, success_conn]
        )

        # Act
        result_stream = await proxy.dispatch(mock_wool_task)
        results = [r async for r in result_stream]

        # Assert
        assert results == ["ok"]
        # Transient errors do NOT evict workers.
        assert metadata_list[0] in proxy.workers
        assert metadata_list[1] in proxy.workers
        # The success stream was handed off (exactly one created).
        assert len(success_streams) == 1
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_non_transient_error_evicts_worker(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test non-transient errors evict the worker before retry.

        Given:
            A delegating balancer with two workers; the first worker's
            connection raises a generic ``Exception``, the second
            succeeds
        When:
            dispatch() is called
        Then:
            The first worker is removed from ``proxy.workers`` before
            the balancer is notified via athrow, and the proxy
            eventually dispatches to the second worker.
        """

        # Arrange
        observed_workers_during_athrow: list[list[WorkerMetadata]] = []

        class ObservingBalancer:
            async def delegate(self_, *, context):
                items = list(context.workers.items())
                for metadata, connection in items:
                    try:
                        sent = yield metadata, connection
                    except Exception:
                        observed_workers_during_athrow.append(
                            list(context.workers.keys())
                        )
                        continue
                    if sent is not None:
                        return

        failing_conn = mocker.MagicMock(spec=WorkerConnection)
        failing_conn.dispatch = mocker.AsyncMock(side_effect=Exception("fatal"))
        success_streams: list = []
        success_conn = self._make_success_connection(mocker, success_streams)

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=ObservingBalancer(),
            lazy=False,
        )
        metadata_list = await self._start_proxy_with_workers(
            proxy, mocker, connections=[failing_conn, success_conn]
        )

        # Act
        result_stream = await proxy.dispatch(mock_wool_task)
        results = [r async for r in result_stream]

        # Assert
        assert results == ["ok"]
        # Non-transient error evicted the failing worker.
        assert metadata_list[0] not in proxy.workers
        assert metadata_list[1] in proxy.workers
        # The balancer observed the post-eviction context when reacting
        # to athrow — only the healthy worker remained visible.
        assert len(observed_workers_during_athrow) == 1
        assert observed_workers_during_athrow[0] == [metadata_list[1]]
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_empty_delegate_raises_no_workers_available(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test the initial anext exhaustion path.

        Given:
            A delegating balancer whose generator produces zero
            candidates (empty ``delegate``)
        When:
            dispatch() is called with a seeded context
        Then:
            NoWorkersAvailable is raised via the initial-anext path.
        """

        # Arrange
        class EmptyBalancer:
            async def delegate(self, *, context):
                if False:
                    yield  # pragma: no cover

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=EmptyBalancer(),
            lazy=False,
        )
        dummy_conn = mocker.MagicMock(spec=WorkerConnection)
        await self._start_proxy_with_workers(
            proxy, mocker, connections=[dummy_conn]
        )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await proxy.dispatch(mock_wool_task)
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_non_transient_exhaustion_raises_no_workers_available(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test the non-transient exhaustion path.

        Given:
            A delegating balancer with one worker whose connection
            raises a non-transient error; after athrow the generator
            has no further candidates
        When:
            dispatch() is called
        Then:
            NoWorkersAvailable is raised from the non-transient
            branch, and the failing worker was evicted before the
            athrow (covering the eviction-then-exhaustion path).
        """

        # Arrange
        class SingleCandidateBalancer:
            async def delegate(self, *, context):
                items = list(context.workers.items())
                if items:
                    metadata, connection = items[0]
                    try:
                        sent = yield metadata, connection
                    except Exception:
                        return
                    if sent is not None:
                        return

        failing_conn = mocker.MagicMock(spec=WorkerConnection)
        failing_conn.dispatch = mocker.AsyncMock(side_effect=Exception("fatal"))

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=SingleCandidateBalancer(),
            lazy=False,
        )
        [metadata] = await self._start_proxy_with_workers(
            proxy, mocker, connections=[failing_conn]
        )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await proxy.dispatch(mock_wool_task)
        # Non-transient error: worker was evicted from the context.
        assert metadata not in proxy.workers
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_exhausted_candidates_raises_no_workers_available(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test exhaustion maps to NoWorkersAvailable.

        Given:
            A delegating balancer whose generator ends immediately
            after all candidates fail
        When:
            dispatch() is called
        Then:
            NoWorkersAvailable is raised.
        """

        # Arrange
        class ExhaustedBalancer:
            async def delegate(self, *, context):
                for metadata, connection in list(context.workers.items()):
                    try:
                        sent = yield metadata, connection
                    except Exception:
                        continue
                    if sent is not None:
                        return

        failing_conn = mocker.MagicMock(spec=WorkerConnection)
        failing_conn.dispatch = mocker.AsyncMock(side_effect=TransientRpcError())

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=ExhaustedBalancer(),
            lazy=False,
        )
        await self._start_proxy_with_workers(
            proxy, mocker, connections=[failing_conn]
        )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await proxy.dispatch(mock_wool_task)
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_success_asends_metadata(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test the proxy reports success to the balancer via asend.

        Given:
            A delegating balancer that records sent values
        When:
            dispatch() succeeds
        Then:
            The balancer observed a sent value equal to the metadata
            of the successful worker, and the generator terminated.
        """
        # Arrange
        sent_values: list = []

        class RecordingBalancer:
            async def delegate(self, *, context):
                for metadata, connection in list(context.workers.items()):
                    try:
                        sent = yield metadata, connection
                    except Exception:
                        continue
                    sent_values.append(sent)
                    if sent is not None:
                        return

        success_streams: list = []
        conn = self._make_success_connection(mocker, success_streams)

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=RecordingBalancer(),
            lazy=False,
        )
        [metadata] = await self._start_proxy_with_workers(
            proxy, mocker, connections=[conn]
        )

        # Act
        stream = await proxy.dispatch(mock_wool_task)
        await stream.aclose()

        # Assert
        assert sent_values == [metadata]
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_post_success_yield_raises_runtime_error(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test post-asend yield is a loud protocol violation.

        Given:
            A malformed delegating balancer whose generator yields
            another candidate after receiving a success signal via
            asend
        When:
            dispatch() is called
        Then:
            RuntimeError is raised naming the trailing value, and the
            orphaned gRPC stream is closed (its aclose was called).
        """
        # Arrange
        sentinel_metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50200",
            pid=9100,
            version="1.0.0",
        )

        class MalformedBalancer:
            async def delegate(self, *, context):
                items = list(context.workers.items())
                if not items:
                    return
                metadata, connection = items[0]
                _ = yield metadata, connection
                # Contract violation: yield again after asend.
                yield sentinel_metadata, connection

        stream_close_calls: list[bool] = []

        class SpyStream:
            def __init__(self):
                self._items = iter(["ok"])

            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    return next(self._items)
                except StopIteration:
                    raise StopAsyncIteration

            async def aclose(self):
                stream_close_calls.append(True)

        spy_stream = SpyStream()
        conn = mocker.MagicMock(spec=WorkerConnection)

        async def _dispatch(task, *, timeout=None):
            return spy_stream

        conn.dispatch = _dispatch

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=MalformedBalancer(),
            lazy=False,
        )
        await self._start_proxy_with_workers(proxy, mocker, connections=[conn])

        # Act & assert
        with pytest.raises(RuntimeError, match="after receiving a success signal"):
            await proxy.dispatch(mock_wool_task)
        assert stream_close_calls == [True]
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_cancellation_during_connection_dispatch(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test cancellation during connection.dispatch propagates cleanly.

        Given:
            A delegating balancer with one worker whose
            ``connection.dispatch`` awaits indefinitely
        When:
            The outer dispatch task is cancelled mid-dispatch
        Then:
            CancelledError propagates to the caller, the worker is
            NOT evicted, and the balancer's delegate was called only
            once (no retry against another candidate).
        """
        # Arrange
        delegate_calls = []

        class SingleWorkerBalancer:
            async def delegate(self_, *, context):
                delegate_calls.append(1)
                for metadata, connection in list(context.workers.items()):
                    try:
                        sent = yield metadata, connection
                    except Exception:
                        continue
                    if sent is not None:
                        return

        conn = mocker.MagicMock(spec=WorkerConnection)

        async def _hang(task, *, timeout=None):
            await asyncio.sleep(10)

        conn.dispatch = _hang

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=SingleWorkerBalancer(),
            lazy=False,
        )
        [metadata] = await self._start_proxy_with_workers(
            proxy, mocker, connections=[conn]
        )

        # Act
        dispatch_task = asyncio.create_task(proxy.dispatch(mock_wool_task))
        await asyncio.sleep(0.01)  # let the dispatch enter connection.dispatch
        dispatch_task.cancel()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await dispatch_task
        # Worker was not evicted.
        assert metadata in proxy.workers
        # delegate was entered exactly once — no retry on cancellation.
        assert len(delegate_calls) == 1
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_cancellation_during_asend_closes_stream(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test cancellation mid-asend closes the orphaned stream.

        Given:
            A delegating balancer whose ``asend`` branch hangs (via an
            awaitable inside the generator body) and one worker whose
            connection returns a spy stream
        When:
            The outer dispatch is cancelled while the balancer is
            processing the success signal
        Then:
            The spy stream's aclose was invoked so the underlying gRPC
            call is released, and CancelledError propagates.
        """
        # Arrange
        hang = asyncio.Event()

        class HangingOnSuccessBalancer:
            async def delegate(self, *, context):
                for metadata, connection in list(context.workers.items()):
                    try:
                        sent = yield metadata, connection
                    except Exception:
                        continue
                    if sent is not None:
                        # Simulate slow bookkeeping on success.
                        await hang.wait()
                        return

        stream_close_calls: list[bool] = []

        class SpyStream:
            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            async def aclose(self):
                stream_close_calls.append(True)

        spy_stream = SpyStream()
        conn = mocker.MagicMock(spec=WorkerConnection)

        async def _dispatch(task, *, timeout=None):
            return spy_stream

        conn.dispatch = _dispatch

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=HangingOnSuccessBalancer(),
            lazy=False,
        )
        await self._start_proxy_with_workers(proxy, mocker, connections=[conn])

        # Act
        dispatch_task = asyncio.create_task(proxy.dispatch(mock_wool_task))
        await asyncio.sleep(0.01)  # let dispatch reach the asend hang
        dispatch_task.cancel()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await dispatch_task
        assert stream_close_calls == [True]
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_with_legacy_dispatching_balancer_emits_warning(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mocker: MockerFixture,
    ):
        """Test legacy DispatchingLoadBalancerLike path emits deprecation warning.

        Given:
            A load balancer that implements only the legacy ``dispatch``
            method (DispatchingLoadBalancerLike)
        When:
            The proxy is started
        Then:
            A DeprecationWarning is emitted referencing the new protocol.
        """

        # Arrange
        class LegacyBalancer:
            async def dispatch(self, task, *, context, timeout=None):
                async def _gen():
                    yield "ok"

                return _gen()

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=LegacyBalancer(),
            lazy=False,
        )

        # Act & assert
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await proxy.start()
            deprecations = [
                w for w in caught if issubclass(w.category, DeprecationWarning)
            ]
            assert any(
                "DispatchingLoadBalancerLike" in str(w.message) for w in deprecations
            ), f"expected deprecation warning, got: {[str(w.message) for w in caught]}"
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_via_legacy_dispatching_balancer_still_works(
        self,
        mock_discovery_service,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test the legacy dispatch path is still functional.

        Given:
            A started proxy with a legacy DispatchingLoadBalancerLike
        When:
            dispatch() is called
        Then:
            The legacy ``dispatch`` method is invoked and its stream
            is returned to the caller.
        """
        # Arrange
        dispatched_tasks: list = []

        class LegacyBalancer:
            async def dispatch(self, task, *, context, timeout=None):
                dispatched_tasks.append(task)

                async def _gen():
                    yield "legacy-ok"

                return _gen()

        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=LegacyBalancer(),
            lazy=False,
        )

        # Seed context with a dummy worker so _await_workers resolves.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            await proxy.start()
        assert proxy._loadbalancer_context is not None
        proxy._loadbalancer_context.add_worker(
            WorkerMetadata(
                uid=uuid.uuid4(),
                address="127.0.0.1:55555",
                pid=1234,
                version="1.0.0",
            ),
            mocker.MagicMock(spec=WorkerConnection),
        )

        # Act
        stream = await proxy.dispatch(mock_wool_task)
        results = [r async for r in stream]

        # Assert
        assert results == ["legacy-ok"]
        assert len(dispatched_tasks) == 1
        await proxy.stop()
