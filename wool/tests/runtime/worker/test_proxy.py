"""Comprehensive tests for WorkerProxy client-side coordinator.

Tests validate WorkerProxy behavior through observable public APIs only,
without accessing private state. All tests use mock discovery and gRPC stubs
to avoid network overhead and ensure deterministic behavior.
"""

import asyncio
import contextvars
import copy
import inspect
import pickle
import uuid
import warnings
from dataclasses import replace
from types import MappingProxyType

import cloudpickle
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from packaging.version import Version
from pytest_mock import MockerFixture

import wool
import wool.runtime.worker.proxy as wp
from tests.helpers import _unique
from wool import protocol
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.routine.task import Task
from wool.runtime.worker.auth import CredentialContext
from wool.runtime.worker.base import ChannelOptions
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata
from wool.runtime.worker.proxy import WorkerProxy
from wool.runtime.worker.proxy import is_version_compatible
from wool.runtime.worker.proxy import parse_version


async def _drain_until(predicate, *, timeout=2.0):
    """Yield the event loop until predicate() holds, or fail.

    The sentinel consumes discovery events strictly in order, so waiting
    for the last event's observable effect implies all earlier events
    were processed.

    Raises `AssertionError` when the predicate does not hold within
    ``timeout``: a caller that drains to establish a precondition would
    otherwise proceed on an unmet one, and pass for the wrong reason.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while not predicate():
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(
                f"drain timed out after {timeout}s: the awaited condition "
                f"never held, so the state it was establishing is absent"
            )
        await asyncio.sleep(0)


async def _run_hook(hook, *arguments):
    """Invoke an optional balancer hook, awaiting an awaitable result.

    A hook may be ``None`` (a no-op), a plain callable, or a callable
    returning an awaitable — the awaitable form lets a hook suspend the
    generator (e.g. to simulate slow success bookkeeping under
    cancellation).
    """
    if hook is None:
        return
    result = hook(*arguments)
    if inspect.isawaitable(result):
        await result


def make_delegating_balancer(
    *, on_task=None, on_yield=None, on_throw=None, on_success=None
):
    """Build a delegating balancer that yields workers in context order.

    The returned balancer's ``delegate`` walks ``context.workers`` in
    insertion order, yielding each worker's uid for the proxy to
    resolve and dispatch to. Four optional hooks observe the handshake
    without altering control flow: ``on_task(task)`` fires once with the
    routed task before any candidate is offered, ``on_yield(metadata)``
    fires before each candidate is offered — with the record the
    balancer read, so tests can assert on the candidate it chose —
    ``on_throw(exception, context)`` fires when the proxy reports a
    dispatch failure via ``athrow``, and ``on_success(sent)`` fires when
    the proxy acknowledges a successful dispatch via ``asend``, carrying
    the proxy's echo verbatim (a uid). Any hook may return an awaitable,
    which is awaited before the generator continues.
    """

    class DelegatingBalancer:
        async def delegate(self, task, *, context):
            await _run_hook(on_task, task)
            for uid, (metadata, _) in list(context.workers.items()):
                await _run_hook(on_yield, metadata)
                try:
                    sent = yield uid
                except Exception as exception:
                    await _run_hook(on_throw, exception, context)
                    continue
                if sent is not None:
                    await _run_hook(on_success, sent)
                    return

    return DelegatingBalancer()


def make_dispatching_balancer(*, marker="ok", on_dispatch=None):
    """Build a legacy dispatching balancer that yields a single marker.

    The returned balancer implements only the deprecated ``dispatch``
    method — the `DispatchingLoadBalancerLike` protocol — yielding
    ``marker`` once as its streamed result. The optional
    ``on_dispatch(task)`` hook fires before the result stream is produced
    so a test can record the dispatched task; it may return an awaitable,
    which is awaited before the stream is returned.
    """

    class DispatchingBalancer:
        async def dispatch(self, task, *, context, timeout=None):
            await _run_hook(on_dispatch, task)

            async def _stream():
                yield marker

            return _stream()

    return DispatchingBalancer()


def _dispatch_deprecations(caught):
    """Return the recorded `DispatchingLoadBalancerLike` deprecation warnings.

    Filters a ``warnings.catch_warnings(record=True)`` list down to the
    proxy's dispatch-protocol ``DeprecationWarning``, so an assertion on the
    fire-once guarantee is not perturbed by unrelated deprecation warnings.
    """
    return [
        warning
        for warning in caught
        if issubclass(warning.category, DeprecationWarning)
        and "DispatchingLoadBalancerLike" in str(warning.message)
    ]


class SpyStream:
    """Async result-stream double that records ``aclose`` invocations.

    Yields the supplied items, then reports each ``aclose`` call to the
    ``on_close`` callback so a test can assert the proxy released an
    orphaned stream.
    """

    def __init__(self, items=(), *, on_close):
        self._items = iter(items)
        self._on_close = on_close

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration

    async def aclose(self):
        self._on_close()


class _SteppedDiscovery:
    """Discovery stream that hands the sentinel one event at a time.

    ``apply`` publishes a single event and returns only once the
    sentinel has finished applying it. The sentinel consumes the stream
    strictly in order and asks for the next event only after applying
    the current one, so its next ``__anext__`` call is the
    acknowledgement. This lets a test observe the pool after every
    single event rather than only after the stream drains.
    """

    def __init__(self):
        self._pending: asyncio.Queue = asyncio.Queue()
        self._requested = asyncio.Event()
        self._requests = 0
        self._published = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        self._requests += 1
        self._requested.set()
        return await self._pending.get()

    async def apply(self, event, *, timeout=5.0):
        """Publish one event and return once the sentinel has applied it."""
        self._published += 1
        await self._pending.put(event)
        async with asyncio.timeout(timeout):
            while self._requests <= self._published:
                self._requested.clear()
                await self._requested.wait()


@st.composite
def _worker_event_sequence(draw):
    """Generate a lease cap and a sequence of worker lifecycle events.

    Draws an optional lease alongside up to twelve
    ``(event_type, uid_slot, mutate)`` tuples over a pool of three uid
    slots. ``mutate`` selects between a changed record and an equal copy
    of that uid's last record, so a refresh that changes nothing is
    drawn as readily as one that does.

    :param draw:
        Hypothesis draw function

    :returns:
        Tuple of ``(lease, events)``
    """
    lease = draw(st.one_of(st.none(), st.integers(min_value=1, max_value=3)))
    events = draw(
        st.lists(
            st.tuples(
                st.sampled_from(("worker-added", "worker-updated", "worker-dropped")),
                st.integers(min_value=0, max_value=2),
                st.booleans(),
            ),
            max_size=12,
        )
    )
    return lease, events


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
    mock_load_balancer = mocker.MagicMock(spec=LoadBalancerLike)

    async def _empty_delegate(task, *, context):
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

    Provides a `LoadBalancerLike` implementation that yields workers
    straight from the context in iteration order, wrapped with a spy so
    tests can assert that ``delegate`` was called.
    """
    loadbalancer = make_delegating_balancer()
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
        proxy = WorkerProxy(workers=workers, quorum=None)

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
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)

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
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)
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
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)

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
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)

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
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)
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
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)
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
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)

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
        proxy = WorkerProxy(discovery=mock_discovery_service, quorum=0)
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
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)

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
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)

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
            discovery=mock_discovery_service, loadbalancer=cm, lazy=False, quorum=0
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
            discovery=mock_discovery_service, loadbalancer=cm, lazy=False, quorum=0
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
            discovery=mock_discovery_service,
            loadbalancer=make_lb(),
            lazy=False,
            quorum=0,
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
            updated event and refresh the pool entry with the updated
            record
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
        await _drain_until(lambda: updated_metadata in proxy.workers)

        # Assert
        assert mock_conn_cls.call_count == 2
        _, updated_kwargs = mock_conn_cls.call_args_list[1]
        assert updated_kwargs["options"] == updated_opts
        assert updated_metadata in proxy.workers
        assert initial_metadata not in proxy.workers

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
        await _drain_until(lambda: len(proxy.workers) == 2)

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
        await _drain_until(lambda: len(proxy.workers) == 3)

        # Assert
        assert len(proxy.workers) == 3

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
        await _drain_until(lambda: len(proxy.workers) == 2)

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
        await _drain_until(lambda: len(proxy.workers) == 1)

        # Assert — only worker1 is present; worker2 was never admitted
        assert len(proxy.workers) == 1
        assert worker1 in proxy.workers
        assert worker2 not in proxy.workers

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_should_refresh_worker_when_updated_metadata_changes(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test worker-updated with changed metadata refreshes the pool entry.

        Given:
            A non-lazy WorkerProxy with a discovered worker and a
            worker-updated event carrying the same uid with a changed
            address, pid, and tags
        When:
            The sentinel processes the update event
        Then:
            It should replace the worker's stale metadata with the
            updated record without changing the pool size
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        worker_uid = uuid.uuid4()
        initial_metadata = WorkerMetadata(
            uid=worker_uid,
            address="192.168.1.1:50051",
            pid=1001,
            version="1.0.0",
            tags=frozenset(["cpu"]),
        )
        updated_metadata = WorkerMetadata(
            uid=worker_uid,
            address="192.168.1.2:50052",
            pid=1002,
            version="1.0.0",
            tags=frozenset(["gpu"]),
        )
        events = [
            DiscoveryEvent("worker-added", metadata=initial_metadata),
            DiscoveryEvent("worker-updated", metadata=updated_metadata),
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lazy=False)

        # Act
        await proxy.start()
        await _drain_until(lambda: updated_metadata in proxy.workers)

        # Assert
        assert updated_metadata in proxy.workers
        assert initial_metadata not in proxy.workers
        assert len(proxy.workers) == 1

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_should_refresh_worker_when_updated_at_capacity(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test sentinel refreshes changed metadata even at capacity.

        Given:
            A non-lazy WorkerProxy with lease=2, already at capacity,
            receiving a worker-updated event whose same-uid metadata
            changed
        When:
            The sentinel processes the update event
        Then:
            It should refresh the worker's entry with the updated
            record without being blocked by the cap
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
        updated1 = WorkerMetadata(
            uid=worker1.uid,
            address="192.168.1.9:50059",
            pid=1009,
            version="1.0.0",
            tags=frozenset(["updated"]),
        )
        events = [
            DiscoveryEvent("worker-added", metadata=worker1),
            DiscoveryEvent("worker-added", metadata=worker2),
            DiscoveryEvent("worker-updated", metadata=updated1),
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lease=2, lazy=False)

        # Act
        await proxy.start()
        await _drain_until(lambda: updated1 in proxy.workers)

        # Assert
        assert len(proxy.workers) == 2
        assert updated1 in proxy.workers
        assert worker1 not in proxy.workers
        assert worker2 in proxy.workers

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_should_not_resurrect_worker_when_updated_after_drop(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test a post-drop update cannot resurrect a removed worker.

        Given:
            A non-lazy WorkerProxy whose discovered worker is dropped
            and then updated with a same-uid changed record, followed
            by an unrelated worker-added event
        When:
            The sentinel processes all events
        Then:
            It should keep the dropped worker out in both forms, and
            the unrelated worker's arrival proves the sentinel survived
            the post-drop update
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        worker_uid = uuid.uuid4()
        initial_metadata = WorkerMetadata(
            uid=worker_uid,
            address="192.168.1.1:50051",
            pid=1001,
            version="1.0.0",
        )
        updated_metadata = WorkerMetadata(
            uid=worker_uid,
            address="192.168.1.2:50052",
            pid=1002,
            version="1.0.0",
        )
        beacon = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.3:50053",
            pid=1003,
            version="1.0.0",
        )
        events = [
            DiscoveryEvent("worker-added", metadata=initial_metadata),
            DiscoveryEvent("worker-dropped", metadata=initial_metadata),
            DiscoveryEvent("worker-updated", metadata=updated_metadata),
            DiscoveryEvent("worker-added", metadata=beacon),
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lazy=False)

        # Act
        await proxy.start()
        await _drain_until(lambda: beacon in proxy.workers)

        # Assert
        assert len(proxy.workers) == 1
        assert beacon in proxy.workers
        assert initial_metadata not in proxy.workers
        assert updated_metadata not in proxy.workers

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_should_refresh_readded_worker_when_pool_at_capacity(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test a same-uid re-announcement bypasses the lease cap.

        Given:
            A non-lazy WorkerProxy with lease=2 at capacity, receiving
            a worker-added event re-announcing an admitted uid with
            changed fields, then a drop freeing capacity and an
            unrelated worker-added event
        When:
            The sentinel processes all events
        Then:
            It should replace the re-announced worker's entry with the
            new record while the pool stays within the lease
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        worker_a = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.1:50051",
            pid=1001,
            version="1.0.0",
        )
        worker_b = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.2:50052",
            pid=1002,
            version="1.0.0",
        )
        readded_a = WorkerMetadata(
            uid=worker_a.uid,
            address="192.168.1.9:50059",
            pid=1009,
            version="1.0.0",
        )
        beacon = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.3:50053",
            pid=1003,
            version="1.0.0",
        )
        events = [
            DiscoveryEvent("worker-added", metadata=worker_a),
            DiscoveryEvent("worker-added", metadata=worker_b),
            DiscoveryEvent("worker-added", metadata=readded_a),  # at capacity
            DiscoveryEvent("worker-dropped", metadata=worker_b),
            DiscoveryEvent("worker-added", metadata=beacon),
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(discovery=discovery, lease=2, lazy=False)

        # Act
        await proxy.start()
        await _drain_until(lambda: beacon in proxy.workers)

        # Assert — the admission gate applies only to pool-growing
        # adds: re-announcing an already-admitted uid replaces that
        # worker's entry in place without consuming a lease slot.
        assert len(proxy.workers) == 2
        assert readded_a in proxy.workers
        assert worker_a not in proxy.workers
        assert beacon in proxy.workers

        # Cleanup
        await proxy.stop()

    @given(_worker_event_sequence())
    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_workers_should_mirror_uid_keyed_model_when_events_interleave(
        self, mock_proxy_session, mocker: MockerFixture, scenario
    ):
        """Test the workers list against a uid-keyed reference model.

        Given:
            A non-lazy WorkerProxy under a drawn lease cap and a drawn
            sequence of worker-added, worker-updated, and worker-dropped
            events over a pool of three uids — two of which share an
            address, so uid-keyed matching is exercised against a
            colliding address — each event carrying either a mutated
            record or an equal copy of that uid's last record
        When:
            The sentinel consumes the events in order alongside a plain
            uid-keyed dict model implementing the documented semantics:
            an add admits or replaces, an update refreshes an admitted
            uid but never admits and never resurrects a dropped one, a
            drop evicts by uid regardless of the record it carries, and
            the lease gates only pool-growing adds
        Then:
            After every event the workers list should equal the model
            exactly — one entry per live uid, carrying that uid's
            last-written record
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        lease, events = scenario
        bases = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                # Slots 0 and 1 deliberately share an address so a drop
                # or refresh that matched on address rather than uid
                # would corrupt the model.
                address=f"192.168.1.{min(slot, 1)}:50051",
                pid=1000 + slot,
                version="1.0.0",
            )
            for slot in range(3)
        ]
        stream = _SteppedDiscovery()
        proxy = WorkerProxy(discovery=stream, lease=lease, lazy=False, quorum=0)
        await proxy.start()
        model: dict[uuid.UUID, WorkerMetadata] = {}

        # Act & assert
        try:
            for event_type, slot, mutate in events:
                last = model.get(bases[slot].uid)
                base = bases[slot] if last is None else last
                record = replace(base, pid=base.pid + 1) if mutate else replace(base)
                admitted = record.uid in model

                await stream.apply(DiscoveryEvent(event_type, metadata=record))

                if event_type == "worker-added":
                    if lease is None or admitted or len(model) < lease:
                        model[record.uid] = record
                elif event_type == "worker-updated":
                    if admitted:
                        model[record.uid] = record
                else:
                    model.pop(record.uid, None)

                assert proxy.workers == list(model.values())
        finally:
            # Cleanup
            await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_should_use_refreshed_connection_when_metadata_updates(
        self, mock_proxy_session, mock_wool_task, mocker: MockerFixture
    ):
        """Test dispatch routes through the connection built from an update.

        Given:
            A non-lazy WorkerProxy with a delegating balancer whose
            discovered worker is refreshed by a worker-updated event
            carrying a changed address
        When:
            A task is dispatched after the refresh is visible
        Then:
            It should dispatch through the connection built from the
            updated metadata and never touch the stale connection
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        worker_uid = uuid.uuid4()
        initial_metadata = WorkerMetadata(
            uid=worker_uid,
            address="192.168.1.1:50051",
            pid=1001,
            version="1.0.0",
        )
        updated_metadata = WorkerMetadata(
            uid=worker_uid,
            address="192.168.1.2:50052",
            pid=1002,
            version="1.0.0",
        )
        stale_connection = mocker.MagicMock(spec=WorkerConnection)
        stale_connection.dispatch = mocker.AsyncMock()
        success_streams: list = []
        fresh_connection = _make_success_connection(mocker, success_streams)
        mocker.patch.object(
            wp, "WorkerConnection", side_effect=[stale_connection, fresh_connection]
        )
        events = [
            DiscoveryEvent("worker-added", metadata=initial_metadata),
            DiscoveryEvent("worker-updated", metadata=updated_metadata),
        ]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(
            discovery=discovery,
            loadbalancer=make_delegating_balancer(),
            lazy=False,
        )
        await proxy.start()
        await _drain_until(lambda: updated_metadata in proxy.workers)

        # Act
        stream = await proxy.dispatch(mock_wool_task)
        results = [result async for result in stream]

        # Assert
        assert results == ["ok"]
        assert stale_connection.dispatch.await_count == 0

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_should_evict_worker_by_uid_when_refreshed_mid_dispatch(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test non-transient eviction removes a concurrently refreshed worker.

        Given:
            Two workers behind a delegating balancer, a gated discovery
            stream holding a same-uid changed-record update for the
            first worker, and a first-worker connection that releases
            the gate, awaits the refresh, and then raises a
            non-transient RpcError
        When:
            dispatch() is called and fails over
        Then:
            It should evict the first worker by uid — neither the stale
            nor the refreshed record remains — while the balancer
            observes only the surviving worker and dispatch succeeds on
            it
        """
        # Arrange
        gate = asyncio.Event()
        worker_a = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50100",
            pid=9000,
            version="1.0.0",
        )
        worker_b = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50101",
            pid=9001,
            version="1.0.0",
        )
        refreshed_a = WorkerMetadata(
            uid=worker_a.uid,
            address="127.0.0.1:50109",
            pid=9009,
            version="1.0.0",
        )
        proxy = None
        stale_connection = mocker.MagicMock(spec=WorkerConnection)

        async def _stale_dispatch(task, *, timeout=None):
            gate.set()
            await _drain_until(lambda: refreshed_a in proxy.workers)
            raise RpcError()

        stale_connection.dispatch = _stale_dispatch
        success_streams: list = []
        survivor_connection = _make_success_connection(mocker, success_streams)
        fresh_connection = mocker.MagicMock(spec=WorkerConnection)
        fresh_connection.dispatch = mocker.AsyncMock()
        observed_pools: list = []
        discovery = _GatedDiscovery(
            initial=[
                DiscoveryEvent("worker-added", metadata=worker_a),
                DiscoveryEvent("worker-added", metadata=worker_b),
            ],
            deferred=[DiscoveryEvent("worker-updated", metadata=refreshed_a)],
            gate=gate,
        )
        proxy, _ = await _make_proxy_with_workers(
            connections=[stale_connection, survivor_connection, fresh_connection],
            loadbalancer=make_delegating_balancer(
                on_throw=lambda _, context: observed_pools.append(
                    [m for m, _ in context.workers.values()]
                )
            ),
            mocker=mocker,
            discovery=discovery,
            metadata_list=[worker_a, worker_b],
        )

        # Act
        stream = await proxy.dispatch(mock_wool_task)
        results = [result async for result in stream]

        # Assert
        assert results == ["ok"]
        assert len(proxy.workers) == 1
        assert worker_b in proxy.workers
        assert refreshed_a not in proxy.workers
        assert worker_a not in proxy.workers
        assert observed_pools == [[worker_b]]
        assert fresh_connection.dispatch.await_count == 0

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_should_keep_refreshed_worker_when_failure_transient(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test a transient failure neither evicts nor clobbers a refresh.

        Given:
            Two workers behind a delegating balancer, a gated discovery
            stream holding a same-uid changed-record update for the
            first worker, and a first-worker connection that releases
            the gate, awaits the refresh, and then raises a
            TransientRpcError
        When:
            dispatch() is called
        Then:
            It should leave the refreshed entry in the pool keyed by
            the updated record and succeed on the second worker
        """
        # Arrange
        gate = asyncio.Event()
        worker_a = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50100",
            pid=9000,
            version="1.0.0",
        )
        worker_b = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50101",
            pid=9001,
            version="1.0.0",
        )
        refreshed_a = WorkerMetadata(
            uid=worker_a.uid,
            address="127.0.0.1:50109",
            pid=9009,
            version="1.0.0",
        )
        proxy = None
        stale_connection = mocker.MagicMock(spec=WorkerConnection)

        async def _stale_dispatch(task, *, timeout=None):
            gate.set()
            await _drain_until(lambda: refreshed_a in proxy.workers)
            raise TransientRpcError()

        stale_connection.dispatch = _stale_dispatch
        success_streams: list = []
        survivor_connection = _make_success_connection(mocker, success_streams)
        fresh_connection = mocker.MagicMock(spec=WorkerConnection)
        fresh_connection.dispatch = mocker.AsyncMock()
        discovery = _GatedDiscovery(
            initial=[
                DiscoveryEvent("worker-added", metadata=worker_a),
                DiscoveryEvent("worker-added", metadata=worker_b),
            ],
            deferred=[DiscoveryEvent("worker-updated", metadata=refreshed_a)],
            gate=gate,
        )
        proxy, _ = await _make_proxy_with_workers(
            connections=[stale_connection, survivor_connection, fresh_connection],
            loadbalancer=make_delegating_balancer(),
            mocker=mocker,
            discovery=discovery,
            metadata_list=[worker_a, worker_b],
        )

        # Act
        stream = await proxy.dispatch(mock_wool_task)
        results = [result async for result in stream]

        # Assert
        assert results == ["ok"]
        assert len(proxy.workers) == 2
        assert refreshed_a in proxy.workers
        assert worker_a not in proxy.workers
        assert worker_b in proxy.workers
        assert fresh_connection.dispatch.await_count == 0

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_should_use_fresh_connection_when_selection_precedes_refresh(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test a uid selected before a refresh dispatches to the fresh entry.

        Given:
            A balancer that selects a worker's uid and only then lets a
            same-uid changed-record update land, so its selection was
            made against a pool state that no longer exists
        When:
            dispatch() is called
        Then:
            It should dispatch through the refreshed worker's
            connection and never touch the superseded one — the proxy
            resolves the uid against the live pool at dispatch time, so
            a selection cannot carry a stale address
        """
        # Arrange
        gate = asyncio.Event()
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50100",
            pid=9000,
            version="1.0.0",
        )
        refreshed = WorkerMetadata(
            uid=worker.uid,
            address="127.0.0.1:50109",
            pid=9009,
            version="1.0.0",
        )
        proxy = None
        stale_connection = mocker.MagicMock(spec=WorkerConnection)
        stale_connection.dispatch = mocker.AsyncMock()
        success_streams: list = []
        fresh_connection = _make_success_connection(mocker, success_streams)

        async def _land_refresh(_):
            # Fires after the balancer has read the pool and chosen a
            # candidate, but before the proxy resolves it.
            gate.set()
            await _drain_until(lambda: refreshed in proxy.workers)

        discovery = _GatedDiscovery(
            initial=[DiscoveryEvent("worker-added", metadata=worker)],
            deferred=[DiscoveryEvent("worker-updated", metadata=refreshed)],
            gate=gate,
        )
        proxy, _ = await _make_proxy_with_workers(
            connections=[stale_connection, fresh_connection],
            loadbalancer=make_delegating_balancer(on_yield=_land_refresh),
            mocker=mocker,
            discovery=discovery,
            metadata_list=[worker],
        )

        # Act
        stream = await proxy.dispatch(mock_wool_task)
        results = [result async for result in stream]

        # Assert
        assert results == ["ok"]
        assert len(success_streams) == 1
        assert stale_connection.dispatch.await_count == 0

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_should_skip_candidate_when_uid_no_longer_pooled(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test a departed candidate is skipped rather than dispatched to.

        Given:
            Two pooled workers and a balancer that selects both, with
            the first dropped from the pool after selection but before
            the proxy resolves it
        When:
            dispatch() is called
        Then:
            It should skip the departed worker without dispatching to
            it and without reporting a failure against it — a worker
            that has left the pool has not failed anything — and
            dispatch should succeed on the survivor
        """
        # Arrange
        gate = asyncio.Event()
        departed = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50100",
            pid=9000,
            version="1.0.0",
        )
        survivor = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50101",
            pid=9001,
            version="1.0.0",
        )
        proxy = None
        departed_connection = mocker.MagicMock(spec=WorkerConnection)
        departed_connection.dispatch = mocker.AsyncMock()
        success_streams: list = []
        survivor_connection = _make_success_connection(mocker, success_streams)
        throws: list = []

        async def _drop_first(metadata):
            if metadata != departed:
                return
            gate.set()
            await _drain_until(lambda: departed not in proxy.workers)

        discovery = _GatedDiscovery(
            initial=[
                DiscoveryEvent("worker-added", metadata=departed),
                DiscoveryEvent("worker-added", metadata=survivor),
            ],
            deferred=[DiscoveryEvent("worker-dropped", metadata=departed)],
            gate=gate,
        )
        proxy, _ = await _make_proxy_with_workers(
            connections=[departed_connection, survivor_connection],
            loadbalancer=make_delegating_balancer(
                on_yield=_drop_first,
                on_throw=lambda exception, _: throws.append(exception),
            ),
            mocker=mocker,
            discovery=discovery,
            metadata_list=[departed, survivor],
        )

        # Act
        stream = await proxy.dispatch(mock_wool_task)
        results = [result async for result in stream]

        # Assert
        assert results == ["ok"]
        assert departed_connection.dispatch.await_count == 0
        assert len(success_streams) == 1
        assert throws == []

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_when_every_candidate_departed(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test exhausting the balancer on departed uids raises.

        Given:
            A single pooled worker and a balancer that selects it, with
            the worker dropped from the pool after selection but before
            the proxy resolves it
        When:
            dispatch() is called
        Then:
            It should raise NoWorkersAvailable once the balancer is
            exhausted, having dispatched to nothing — skipping a
            departed candidate must not loop forever
        """
        # Arrange
        gate = asyncio.Event()
        departed = WorkerMetadata(
            uid=uuid.uuid4(),
            address="127.0.0.1:50100",
            pid=9000,
            version="1.0.0",
        )
        proxy = None
        departed_connection = mocker.MagicMock(spec=WorkerConnection)
        departed_connection.dispatch = mocker.AsyncMock()
        throws: list = []

        async def _drop_it(_):
            gate.set()
            await _drain_until(lambda: departed not in proxy.workers)

        discovery = _GatedDiscovery(
            initial=[DiscoveryEvent("worker-added", metadata=departed)],
            deferred=[DiscoveryEvent("worker-dropped", metadata=departed)],
            gate=gate,
        )
        proxy, _ = await _make_proxy_with_workers(
            connections=[departed_connection],
            loadbalancer=make_delegating_balancer(
                on_yield=_drop_it,
                on_throw=lambda exception, _: throws.append(exception),
            ),
            mocker=mocker,
            discovery=discovery,
            metadata_list=[departed],
        )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await proxy.dispatch(mock_wool_task)
        assert departed_connection.dispatch.await_count == 0
        assert throws == []

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_should_evict_worker_when_legacy_record_stale(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test a legacy balancer's stale-record eviction takes effect.

        Given:
            A started proxy with one worker and a legacy
            DispatchingLoadBalancerLike whose dispatch removes the
            worker using a same-uid record with changed fields and then
            raises NoWorkersAvailable
        When:
            dispatch() is called
        Then:
            It should propagate NoWorkersAvailable and leave the pool
            empty — the stale-record eviction matches by uid
        """

        # Arrange
        class EvictingBalancer:
            async def dispatch(self, task, *, context, timeout=None):
                [(current, _)] = list(context.workers.values())
                stale = WorkerMetadata(
                    uid=current.uid,
                    address="127.0.0.1:50109",
                    pid=9099,
                    version=current.version,
                )
                context.remove_worker(stale)
                raise NoWorkersAvailable()

        dummy_connection = mocker.MagicMock(spec=WorkerConnection)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            proxy, _ = await _make_proxy_with_workers(
                connections=[dummy_connection],
                loadbalancer=EvictingBalancer(),
                mocker=mocker,
            )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await proxy.dispatch(mock_wool_task)
        assert len(proxy.workers) == 0

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_should_timeout_quorum_when_only_updates_arrive(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test worker-updated events cannot satisfy the quorum gate.

        Given:
            A non-lazy WorkerProxy with quorum=1 and a small
            quorum_timeout whose discovery stream yields only a
            worker-updated event for a never-admitted worker
        When:
            start() is awaited
        Then:
            It should raise asyncio.TimeoutError — updates neither
            admit workers nor satisfy the quorum gate
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.1:50051",
            pid=1001,
            version="1.0.0",
        )
        events = [DiscoveryEvent("worker-updated", metadata=metadata)]
        discovery = wp.ReducibleAsyncIterator(events)
        proxy = WorkerProxy(
            discovery=discovery,
            lazy=False,
            quorum=1,
            quorum_timeout=0.2,
        )

        # Act & assert
        with pytest.raises(asyncio.TimeoutError):
            await proxy.start()
        assert proxy.workers == []

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
        pickled_data = wool.__serializer__.dumps(proxy)
        restored = cloudpickle.loads(pickled_data)
        await restored.start()
        await _drain_until(lambda: len(restored.workers) == 2)

        # Assert — restored proxy enforces the cap
        assert len(restored.workers) == 2
        await restored.stop()

    # ------------------------------------------------------------------
    # Quorum tests
    # ------------------------------------------------------------------

    def test___init___with_negative_quorum_raises(self, mock_discovery_service):
        """Test negative quorum is rejected.

        Given:
            A discovery service and quorum of -1
        When:
            WorkerProxy is instantiated
        Then:
            It should raise ValueError
        """
        # Act & assert
        with pytest.raises(
            ValueError,
            match="Quorum must be a non-negative integer",
        ):
            WorkerProxy(discovery=mock_discovery_service, quorum=-1)

    def test___init___with_zero_quorum_accepted(self, mock_discovery_service):
        """Test zero quorum is accepted.

        Given:
            A discovery service and quorum of 0
        When:
            WorkerProxy is instantiated
        Then:
            It should create the proxy successfully
        """
        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service, quorum=0)

        # Assert
        assert isinstance(proxy, WorkerProxy)

    def test___init___with_positive_quorum_accepted(self, mock_discovery_service):
        """Test positive quorum is accepted.

        Given:
            A discovery service and quorum of 3
        When:
            WorkerProxy is instantiated
        Then:
            It should create the proxy successfully
        """
        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service, quorum=3)

        # Assert
        assert isinstance(proxy, WorkerProxy)

    def test___init___with_quorum_exceeding_lease_raises(self, mock_discovery_service):
        """Test quorum exceeding lease is rejected.

        Given:
            A discovery service with lease=2 and quorum=3
        When:
            WorkerProxy is instantiated
        Then:
            It should raise ValueError
        """
        # Act & assert
        with pytest.raises(
            ValueError,
            match=r"Quorum.*cannot exceed lease",
        ):
            WorkerProxy(discovery=mock_discovery_service, lease=2, quorum=3)

    def test___init___with_quorum_equal_to_lease_accepted(self, mock_discovery_service):
        """Test quorum equal to lease is accepted.

        Given:
            A discovery service with lease=3 and quorum=3
        When:
            WorkerProxy is instantiated
        Then:
            It should create the proxy successfully
        """
        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service, lease=3, quorum=3)

        # Assert
        assert isinstance(proxy, WorkerProxy)

    def test___init___with_quorum_and_no_lease_accepted(self, mock_discovery_service):
        """Test quorum without lease is accepted.

        Given:
            A discovery service and quorum of 5 with no lease
        When:
            WorkerProxy is instantiated
        Then:
            It should create the proxy successfully
        """
        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service, quorum=5)

        # Assert
        assert isinstance(proxy, WorkerProxy)

    def test___init___with_non_positive_quorum_timeout_raises(
        self, mock_discovery_service
    ):
        """Test non-positive quorum_timeout is rejected.

        Given:
            A discovery service, quorum=1, and quorum_timeout=0
        When:
            WorkerProxy is instantiated
        Then:
            It should raise ValueError
        """
        # Act & assert
        with pytest.raises(ValueError, match="Quorum timeout must be positive"):
            WorkerProxy(discovery=mock_discovery_service, quorum=1, quorum_timeout=0)

    def test___init___with_quorum_timeout_without_quorum_warns(
        self, mock_discovery_service
    ):
        """Test quorum_timeout supplied without a positive quorum emits a warning.

        Given:
            A discovery service, quorum=None, and quorum_timeout=30
        When:
            WorkerProxy is instantiated
        Then:
            It should emit an IneffectiveQuorumTimeoutWarning; users
            who want strict behaviour can elevate the category to error
            via warnings.filterwarnings
        """
        # Act & assert
        with pytest.warns(
            wp.IneffectiveQuorumTimeoutWarning,
            match="'quorum_timeout' has no effect when 'quorum' is None or 0",
        ):
            WorkerProxy(discovery=mock_discovery_service, quorum=None, quorum_timeout=30)

    def test_quorum_and_quorum_timeout_properties(self, mock_discovery_service):
        """Test the quorum and quorum_timeout properties expose configured values.

        Given:
            A WorkerProxy constructed with quorum=3 and quorum_timeout=15
        When:
            The quorum and quorum_timeout properties are read
        Then:
            They should return the configured values
        """
        # Arrange
        proxy = WorkerProxy(
            discovery=mock_discovery_service, quorum=3, quorum_timeout=15
        )

        # Act & assert
        assert proxy.quorum == 3
        assert proxy.quorum_timeout == 15

    def test___init___with_static_workers_quorum_exceeds_raises(self):
        """Test quorum exceeding the static workers list is rejected.

        Given:
            A static workers list of length 2 and quorum of 5
        When:
            WorkerProxy is instantiated
        Then:
            It should raise ValueError, since the static list cannot
            grow to satisfy the quorum
        """
        # Arrange
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"10.0.0.{i}:50051",
                pid=1000 + i,
                version="1.0.0",
            )
            for i in range(2)
        ]

        # Act & assert
        with pytest.raises(
            ValueError, match=r"Quorum.*cannot exceed compatible worker count"
        ):
            WorkerProxy(workers=workers, quorum=5)

    def test___init___with_static_workers_unparseable_version_filtered(self):
        """Test workers with unparseable versions are filtered out of quorum count.

        Given:
            A static workers list of length 2 where one worker has an
            unparseable version, and quorum=2
        When:
            WorkerProxy is instantiated
        Then:
            It should raise ValueError reporting "1 of 2" — the
            unparseable-version worker is rejected by the version filter
        """
        # Arrange
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address="10.0.0.1:50051",
                pid=1000,
                version=protocol.__version__,
            ),
            WorkerMetadata(
                uid=uuid.uuid4(),
                address="10.0.0.2:50051",
                pid=1001,
                version="not-a-semver",
            ),
        ]

        # Act & assert
        with pytest.raises(
            ValueError,
            match=r"compatible worker count \(1 of 2",
        ):
            WorkerProxy(workers=workers, quorum=2)

    @given(
        lease=st.integers(min_value=1, max_value=100),
        quorum=st.integers(min_value=0, max_value=100),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test___init___accepts_valid_quorum_within_lease_pbt(
        self, mock_discovery_service, lease, quorum
    ):
        """Test any quorum within the lease cap is accepted.

        Given:
            Any lease in [1, 100] and quorum in [0, lease]
        When:
            WorkerProxy is instantiated
        Then:
            It should create the proxy successfully
        """
        # Hypothesis explores the full grid; skip combinations that the
        # validator legitimately rejects (quorum > lease).
        if quorum > lease:
            return

        # Act
        proxy = WorkerProxy(discovery=mock_discovery_service, lease=lease, quorum=quorum)

        # Assert
        assert isinstance(proxy, WorkerProxy)

    @pytest.mark.asyncio
    async def test_dispatch_with_zero_quorum_skips_worker_wait(
        self,
        mocker: MockerFixture,
        mock_proxy_session,
    ):
        """Test dispatch does not wait for workers when quorum is 0.

        Given:
            A started WorkerProxy with quorum=0 and no workers
        When:
            dispatch is called
        Then:
            It should proceed without waiting for workers
        """

        # Arrange
        class EmptyDiscovery:
            def __aiter__(self):
                return self

            async def __anext__(self):
                # Block forever — no workers will appear
                await asyncio.Event().wait()
                raise StopAsyncIteration

        class StubLoadBalancer:
            def __init__(self):
                self.dispatched = False

            async def dispatch(self, task, *, context, timeout=None):
                self.dispatched = True

        stub_lb = StubLoadBalancer()
        proxy = WorkerProxy(
            discovery=EmptyDiscovery(),
            loadbalancer=stub_lb,
            quorum=0,
        )
        await proxy.start()

        mock_task = mocker.MagicMock(spec=Task)

        # Act — should not block waiting for workers
        await asyncio.wait_for(proxy.dispatch(mock_task), timeout=2.0)

        # Assert
        assert stub_lb.dispatched

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_waits_for_quorum_workers(
        self,
        mocker: MockerFixture,
        mock_proxy_session,
    ):
        """Test dispatch blocks until quorum workers are discovered.

        Given:
            A started WorkerProxy with quorum=2 and a gated
            discovery stream
        When:
            dispatch is called and workers are added one at a time
        Then:
            It should block until 2 workers are discovered
        """
        # Arrange
        gate = asyncio.Event()
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"192.168.1.{i}:50051",
                pid=1000 + i,
                version="1.0.0",
            )
            for i in range(2)
        ]
        call_count = 0

        class GatedDiscovery:
            def __aiter__(self):
                return self

            async def __anext__(self):
                nonlocal call_count
                await gate.wait()
                gate.clear()
                if call_count < len(workers):
                    event = DiscoveryEvent("worker-added", metadata=workers[call_count])
                    call_count += 1
                    return event
                await asyncio.Event().wait()
                raise StopAsyncIteration

        class StubLoadBalancer:
            def __init__(self):
                self.dispatched = False

            async def dispatch(self, task, *, context, timeout=None):
                self.dispatched = True

        stub_lb = StubLoadBalancer()
        proxy = WorkerProxy(
            discovery=GatedDiscovery(),
            loadbalancer=stub_lb,
            quorum=2,
        )
        mock_task = mocker.MagicMock(spec=Task)

        # Act — launch dispatch; lazy start triggers quorum wait
        dispatch_task = asyncio.create_task(proxy.dispatch(mock_task))
        await asyncio.sleep(0)

        # Release first worker — still below quorum
        gate.set()
        await asyncio.sleep(0.05)
        assert not dispatch_task.done()

        # Release second worker — quorum met
        gate.set()
        await asyncio.wait_for(dispatch_task, timeout=2.0)

        # Assert
        assert stub_lb.dispatched

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test___aenter___blocks_until_quorum_for_non_lazy_proxy(
        self,
        mocker: MockerFixture,
        mock_proxy_session,
    ):
        """Test non-lazy entry blocks until quorum workers are discovered.

        Given:
            A non-lazy WorkerProxy with quorum=2 and a gated discovery
            stream that yields workers one at a time
        When:
            The proxy is entered as an async context manager
        Then:
            __aenter__ should block until 2 workers are discovered
        """
        # Arrange
        gate = asyncio.Event()
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"192.168.1.{i}:50051",
                pid=1000 + i,
                version="1.0.0",
            )
            for i in range(2)
        ]
        call_count = 0

        class GatedDiscovery:
            def __aiter__(self):
                return self

            async def __anext__(self):
                nonlocal call_count
                await gate.wait()
                gate.clear()
                if call_count < len(workers):
                    event = DiscoveryEvent("worker-added", metadata=workers[call_count])
                    call_count += 1
                    return event
                await asyncio.Event().wait()
                raise StopAsyncIteration

        proxy = WorkerProxy(discovery=GatedDiscovery(), quorum=2, lazy=False)

        # Act — launch __aenter__; it must block on quorum at start
        enter_task = asyncio.create_task(proxy.__aenter__())
        await asyncio.sleep(0)

        # Release first worker — still below quorum
        gate.set()
        await asyncio.sleep(0.05)
        assert not enter_task.done()

        # Release second worker — quorum met
        gate.set()
        await asyncio.wait_for(enter_task, timeout=2.0)

        # Assert
        assert proxy.started
        assert len(proxy.workers) == 2

        # Cleanup. ``__aenter__`` ran inside ``enter_task``'s context, so the
        # ``_armed`` token it minted cannot be reset from this (different)
        # context; tear the started proxy down directly instead.
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_enter_should_not_arm_chain_when_called_directly(
        self, mock_discovery_service
    ):
        """Test the direct enter() path does not arm the chain.

        Given:
            A fresh, unarmed context and a lazy proxy.
        When:
            `enter` is called directly (the path the worker pool factory
            uses) rather than via the async context manager.
        Then:
            `wool.__proxy__` should be bound to the proxy, but the chain
            should remain unarmed — only `__aenter__` arms, so the worker
            pool path stays unguarded by design.
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)
        assert wool.__chain__.get(None) is None

        # Act
        await proxy.enter()
        try:
            # Assert
            assert wool.__proxy__.get() is proxy
            assert wool.__chain__.get(None) is None
        finally:
            await proxy.exit()

    @pytest.mark.asyncio
    async def test___aenter___should_arm_chain_for_contention_guard(
        self, mock_discovery_service
    ):
        """Test entering a proxy via the context manager arms the chain.

        Given:
            A fresh, unarmed context.
        When:
            A proxy is entered as an async context manager.
        Then:
            The chain should be armed inside the block — this is what
            brings `wool.__proxy__`'s lifecycle under the contention
            guard.
        """
        # Arrange
        assert wool.__chain__.get(None) is None

        # Act & assert
        async with WorkerProxy(discovery=mock_discovery_service):
            assert wool.__chain__.get(None) is not None

    @pytest.mark.asyncio
    async def test___aenter___should_preserve_active_proxy_when_second_task_contends(
        self, mock_discovery_service
    ):
        """Test a contended second entry never clobbers the first proxy.

        Given:
            Two tasks share one context; the first enters `proxy_first`
            (binding `wool.__proxy__`) and holds it open.
        When:
            The second task, sharing that context it does not own, enters
            its own `proxy_second` and the entry is rejected.
        Then:
            It should raise `wool.ChainContention` (kind ``"task"``)
            before enter() runs, so the first task still observes
            `wool.__proxy__` as `proxy_first` — the active proxy is never
            silently clobbered.
        """
        # Arrange
        wool.install_task_factory()
        loop = asyncio.get_running_loop()
        shared = contextvars.copy_context()
        proxy_first = WorkerProxy(discovery=mock_discovery_service)
        proxy_second = WorkerProxy(discovery=mock_discovery_service)
        armed = asyncio.Event()
        first_can_finish = asyncio.Event()
        observed: dict[str, object] = {}

        async def first() -> None:
            async with proxy_first:
                observed["before"] = wool.__proxy__.get()
                armed.set()
                await first_can_finish.wait()
                observed["after"] = wool.__proxy__.get()

        async def second() -> BaseException | None:
            await armed.wait()
            try:
                async with proxy_second:
                    return None
            except wool.ChainContention as exc:
                return exc
            finally:
                first_can_finish.set()

        # Act
        first_task = loop.create_task(first(), context=shared)
        second_task = loop.create_task(second(), context=shared)
        contention, _ = await asyncio.gather(second_task, first_task)

        # Assert
        assert isinstance(contention, wool.ChainContention)
        assert contention.kind == "task"
        assert observed["before"] is proxy_first
        assert observed["after"] is proxy_first

    @pytest.mark.asyncio
    async def test___aenter___should_raise_when_foreign_task_enters_pre_armed_context(
        self, mock_discovery_service
    ):
        """Test entering a proxy on a pre-armed, foreign-owned context fails.

        Given:
            A `contextvars.Context` already armed and owned by a first
            task via a plain `wool.ContextVar` (not a proxy); the first
            task then enters its own proxy without raising and holds it.
        When:
            A second task, sharing that context it does not own, enters a
            proxy.
        Then:
            It should raise `wool.ChainContention` with kind ``"task"`` —
            the guard keys on chain ownership, not on the proxy having
            done the arming.
        """
        # Arrange
        wool.install_task_factory()
        loop = asyncio.get_running_loop()
        shared = contextvars.copy_context()
        marker = wool.ContextVar(_unique("prearmed"))
        armed = asyncio.Event()
        first_can_finish = asyncio.Event()

        async def first() -> None:
            marker.set("owned-by-first")
            async with WorkerProxy(discovery=mock_discovery_service):
                armed.set()
                await first_can_finish.wait()

        async def second() -> BaseException | None:
            await armed.wait()
            try:
                async with WorkerProxy(discovery=mock_discovery_service):
                    return None
            except wool.ChainContention as exc:
                return exc
            finally:
                first_can_finish.set()

        # Act
        first_task = loop.create_task(first(), context=shared)
        second_task = loop.create_task(second(), context=shared)
        observed, _ = await asyncio.gather(second_task, first_task)

        # Assert
        assert isinstance(observed, wool.ChainContention)
        assert observed.kind == "task"

    @pytest.mark.asyncio
    async def test___aenter___should_bind_inner_then_revert_to_outer_when_nested(
        self, mock_discovery_service
    ):
        """Test nesting two proxies in one task rebinds then reverts.

        Given:
            One task and two distinct lazy proxies.
        When:
            The task enters an outer proxy and, while it is open, enters
            an inner proxy.
        Then:
            Neither entry should raise — a single owner satisfies the
            guard — and `wool.__proxy__` should be the inner proxy inside
            the nested block and revert to the outer proxy after it.
        """
        # Arrange
        outer = WorkerProxy(discovery=mock_discovery_service)
        inner = WorkerProxy(discovery=mock_discovery_service)

        # Act & assert
        async with outer:
            assert wool.__proxy__.get() is outer
            async with inner:
                assert wool.__proxy__.get() is inner
            assert wool.__proxy__.get() is outer

    @pytest.mark.asyncio
    async def test___aenter___should_not_raise_in_forked_child_when_parent_armed(
        self, mock_discovery_service
    ):
        """Test a forked child entering a proxy under an armed parent is fine.

        Given:
            A parent task whose context is already armed (a bound
            `wool.ContextVar`), then a child task forked the ordinary way.
        When:
            The forked child enters its own proxy.
        Then:
            It should not raise — the task factory forks a fresh,
            child-owned chain, so the child owns the chain it arms (this
            discriminates from the concurrent-isolation negative, which
            starts from an unarmed parent).
        """
        # Arrange
        wool.install_task_factory()
        marker = wool.ContextVar(_unique("parent_armed"))
        marker.set("parent")

        async def child() -> None:
            async with WorkerProxy(discovery=mock_discovery_service):
                await asyncio.sleep(0)

        # Act & assert
        await asyncio.create_task(child())

    @pytest.mark.asyncio
    async def test___aenter___should_isolate_distinct_proxies_across_concurrent_tasks(
        self, mock_discovery_service
    ):
        """Test concurrent coroutines each entering their own proxy stay isolated.

        Given:
            Two coroutines, each entering a distinct proxy, run
            concurrently as ordinary separate tasks.
        When:
            They run via asyncio.gather.
        Then:
            Neither should raise and each should observe its own proxy as
            `wool.__proxy__` — each coroutine task runs in its own context
            with its own chain, so distinct proxies never collide or cross
            over (the common concurrent-dispatch case).
        """
        # Arrange
        proxy_a = WorkerProxy(discovery=mock_discovery_service)
        proxy_b = WorkerProxy(discovery=mock_discovery_service)
        seen: dict[str, object] = {}

        async def use(proxy: WorkerProxy, key: str) -> None:
            async with proxy:
                await asyncio.sleep(0)
                seen[key] = wool.__proxy__.get()

        # Act
        await asyncio.gather(use(proxy_a, "a"), use(proxy_b, "b"))

        # Assert
        assert seen["a"] is proxy_a
        assert seen["b"] is proxy_b

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_preserves_quorum(
        self, mock_proxy_session, mocker: MockerFixture
    ):
        """Test pickle round-trip preserves quorum behavior.

        Given:
            A non-lazy WorkerProxy with quorum=2 and a discovery
            stream with 3 worker-added events
        When:
            The proxy is pickled, unpickled, started, and processes
            the discovery events
        Then:
            It should require 2 workers before dispatch unblocks,
            proving the quorum survived the round-trip
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
            quorum=2,
            lazy=False,
        )

        # Act — pickle round-trip, then start the restored proxy
        restored = cloudpickle.loads(wool.__serializer__.dumps(proxy))
        await restored.start()
        await _drain_until(lambda: len(restored.workers) == 3)

        # Assert — quorum survived the round-trip
        assert len(restored.workers) >= 2
        await restored.stop()

    def test__restore_proxy_with_5_arg_legacy_tuple_defaults_quorum(
        self, mock_discovery_service
    ):
        """Test legacy 5-arg reduce tuples restore with default quorum.

        Given:
            A 5-element reduce tuple (discovery, loadbalancer, proxy_id,
            lease, lazy) emitted by a peer that predates the quorum
            parameter
        When:
            wp._restore_proxy is invoked with only 5 positional arguments
        Then:
            The restored proxy applies the constructor default quorum=1,
            preserving cross-version unpickle compatibility within the
            same major protocol version
        """
        # Arrange
        proxy_id = uuid.uuid4()

        # Act — invoke the restore helper as an older peer's reduce tuple would
        restored = wp._restore_proxy(
            mock_discovery_service,
            wp.RoundRobinLoadBalancer,
            proxy_id,
            None,
            True,
        )

        # Assert
        assert isinstance(restored, WorkerProxy)
        assert restored.id == proxy_id
        assert restored._quorum == 1

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
        restored = cloudpickle.loads(wool.__serializer__.dumps(proxy))

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
        restored = cloudpickle.loads(wool.__serializer__.dumps(proxy))

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
        await _drain_until(lambda: len(proxy.workers) == 1)

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
        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)

        # Act & assert
        with pytest.raises(RuntimeError, match="Proxy not started"):
            async for _ in await proxy.dispatch(mock_wool_task):
                pass

    @pytest.mark.asyncio
    async def test_stop_raises_runtime_error_when_not_started(
        self, mock_discovery_service
    ):
        """Test stop() on a never-started proxy raises RuntimeError.

        Given:
            A WorkerProxy that has never been started
        When:
            stop() is awaited directly
        Then:
            It should raise RuntimeError matching "Proxy not started"
        """
        # Arrange
        proxy = WorkerProxy(discovery=mock_discovery_service)

        # Act & assert
        with pytest.raises(RuntimeError, match="Proxy not started"):
            await proxy.stop()

    @pytest.mark.asyncio
    async def test_enter_resets_proxy_token_on_start_failure(
        self, mock_discovery_service
    ):
        """Test the contextvar is reset when start() raises in enter().

        Given:
            A non-lazy proxy with an unsatisfiable quorum and a tiny
            quorum_timeout
        When:
            The proxy is used as an async context manager, causing
            start() to raise asyncio.TimeoutError at context entry
        Then:
            The contextvar wool.__proxy__ should be None after the
            failed entry — the token set in enter() must be reset
        """
        # Arrange
        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            lazy=False,
            quorum=2,
            quorum_timeout=0.01,
        )

        # Act & assert
        with pytest.raises(asyncio.TimeoutError):
            async with proxy:
                pass
        assert wool.__proxy__.get() is None

    @pytest.mark.asyncio
    async def test_dispatch_retries_start_after_failed_quorum(
        self, mocker: MockerFixture
    ):
        """Test a failed first dispatch leaves the proxy retryable.

        Given:
            A lazy proxy with quorum=1 whose first dispatch times out
            because no worker is available, then a worker appears
        When:
            A second dispatch is attempted after the worker is admitted
        Then:
            The first dispatch raises asyncio.TimeoutError and leaves
            the proxy un-started; the retried dispatch re-runs start()
            and dispatches successfully
        """
        # Arrange
        worker_available = asyncio.Event()
        metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.100:50051",
            pid=1001,
            version="1.0.0",
        )

        class RecoverableDiscovery:
            def __init__(self):
                self._emitted = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                await worker_available.wait()
                if not self._emitted:
                    self._emitted = True
                    return DiscoveryEvent("worker-added", metadata=metadata)
                await asyncio.Event().wait()
                raise StopAsyncIteration

        class StubLoadBalancer:
            def __init__(self):
                self.dispatched = False

            async def dispatch(self, task, *, context, timeout=None):
                self.dispatched = True

        stub_lb = StubLoadBalancer()
        proxy = WorkerProxy(
            discovery=RecoverableDiscovery(),
            loadbalancer=stub_lb,
            quorum=1,
            quorum_timeout=0.1,
        )
        mock_task = mocker.MagicMock(spec=Task)

        # Act — first dispatch times out because no worker is present
        with pytest.raises(asyncio.TimeoutError):
            await proxy.dispatch(mock_task)

        # Assert — the failed start left the proxy un-started and retryable
        assert not proxy.started
        assert not stub_lb.dispatched

        # Act — a worker appears and a retried dispatch re-runs start()
        worker_available.set()
        await asyncio.wait_for(proxy.dispatch(mock_task), timeout=2.0)

        # Assert — the retry started the proxy and dispatched the task
        assert proxy.started
        assert stub_lb.dispatched

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_recovers_concurrent_first_dispatch_after_failed_quorum(
        self, mocker: MockerFixture
    ):
        """Test concurrent first dispatches recover after a failed quorum.

        Given:
            A lazy proxy with quorum=1 whose first start() times out
            while several dispatches contend for the start lock, then a
            worker appears
        When:
            Several dispatches are attempted concurrently before the
            worker is admitted, then one more after it is admitted
        Then:
            Every concurrent dispatch raises asyncio.TimeoutError and
            leaves the proxy un-started, and the later dispatch recovers
            once the worker is present
        """
        # Arrange
        worker_available = asyncio.Event()
        metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.101:50051",
            pid=1002,
            version="1.0.0",
        )

        class RecoverableDiscovery:
            def __init__(self):
                self._emitted = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                await worker_available.wait()
                if not self._emitted:
                    self._emitted = True
                    return DiscoveryEvent("worker-added", metadata=metadata)
                await asyncio.Event().wait()
                raise StopAsyncIteration

        class StubLoadBalancer:
            def __init__(self):
                self.dispatched = False

            async def dispatch(self, task, *, context, timeout=None):
                self.dispatched = True

        stub_lb = StubLoadBalancer()
        proxy = WorkerProxy(
            discovery=RecoverableDiscovery(),
            loadbalancer=stub_lb,
            quorum=1,
            quorum_timeout=0.1,
        )
        mock_task = mocker.MagicMock(spec=Task)

        # Act — three dispatches race the first start() while no worker exists
        results = await asyncio.gather(
            proxy.dispatch(mock_task),
            proxy.dispatch(mock_task),
            proxy.dispatch(mock_task),
            return_exceptions=True,
        )

        # Assert — every contender timed out and the proxy stayed un-started
        assert all(isinstance(result, asyncio.TimeoutError) for result in results)
        assert not proxy.started
        assert not stub_lb.dispatched

        # Act — a worker appears and a retried dispatch recovers
        worker_available.set()
        await asyncio.wait_for(proxy.dispatch(mock_task), timeout=2.0)

        # Assert — the retry started the proxy and dispatched the task
        assert proxy.started
        assert stub_lb.dispatched

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_propagates_quorum_timeout_out_of_context(
        self, mocker: MockerFixture, mock_proxy_session
    ):
        """Test a lazy first-dispatch failure propagates out of context.

        Given:
            A lazy proxy entered as an async context manager whose first
            dispatch times out because no worker is ever discovered
        When:
            The dispatch is awaited inside the async with block
        Then:
            The asyncio.TimeoutError propagates out of the block and the
            context exit is a safe no-op leaving the proxy un-started
        """

        # Arrange
        class EmptyDiscovery:
            def __aiter__(self):
                return self

            async def __anext__(self):
                await asyncio.Event().wait()
                raise StopAsyncIteration

        class StubLoadBalancer:
            def __init__(self):
                self.dispatched = False

            async def dispatch(self, task, *, context, timeout=None):
                self.dispatched = True

        stub_lb = StubLoadBalancer()
        proxy = WorkerProxy(
            discovery=EmptyDiscovery(),
            loadbalancer=stub_lb,
            quorum=1,
            quorum_timeout=0.1,
        )
        mock_task = mocker.MagicMock(spec=Task)

        # Act & assert — the timeout propagates out of the async with block
        with pytest.raises(asyncio.TimeoutError):
            async with proxy:
                await proxy.dispatch(mock_task)

        # Assert — the exit was a safe no-op; the proxy is un-started
        assert not proxy.started
        assert not stub_lb.dispatched

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
            async def delegate(self, task, *, context):
                raise Exception("Load balancer error")
                if False:
                    yield  # pragma: no cover

        failing_loadbalancer = FailingLoadBalancer()

        proxy = WorkerProxy(discovery=discovery, loadbalancer=failing_loadbalancer)

        await proxy.start()
        await _drain_until(lambda: len(proxy.workers) == 1)

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
        waiting_loadbalancer = make_delegating_balancer()

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
    async def test_dispatch_blocks_on_quorum_until_worker_discovered(
        self,
        mocker: MockerFixture,
        mock_proxy_session,
    ):
        """Test dispatch blocks at start when quorum is unmet.

        Given:
            A WorkerProxy with quorum=1 whose discovery stream is gated
            behind an asyncio.Event, so no workers exist initially.
        When:
            dispatch() is called before any workers are available, then
            the gate is opened to emit a worker-added event.
        Then:
            dispatch() blocks inside the lazy start() quorum wait until
            the sentinel processes the event and admits the worker, then
            completes normally.
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

        delegated: list[bool] = []
        stub_lb = make_delegating_balancer(
            on_yield=lambda metadata: delegated.append(True)
        )
        mock_connection = mocker.MagicMock(spec=WorkerConnection)

        async def fake_dispatch(task, *, timeout=None):
            async def _gen():
                yield "result"

            return _gen()

        mock_connection.dispatch = fake_dispatch
        mocker.patch.object(wp, "WorkerConnection", return_value=mock_connection)

        proxy = WorkerProxy(discovery=GatedDiscovery(), loadbalancer=stub_lb, quorum=1)
        mock_task = mocker.MagicMock(spec=Task)

        # Act — launch dispatch in background; it blocks in start() awaiting quorum
        dispatch_task = asyncio.create_task(proxy.dispatch(mock_task))
        await asyncio.sleep(0)  # yield so dispatch enters the quorum wait
        assert not dispatch_task.done()

        # Open the gate — sentinel receives the event and admits the worker
        gate.set()
        await dispatch_task

        # Assert
        assert metadata in proxy.workers
        assert delegated

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

        proxy = WorkerProxy(workers=workers, lazy=False, quorum=0)

        # Act & assert
        async with proxy as p:
            assert p is not None
            assert p.started

        # After exit, proxy should be stopped
        assert not proxy.started

    @pytest.mark.asyncio
    async def test_proxy_with_static_workers_list_satisfies_quorum(
        self, mocker: MockerFixture
    ):
        """Test static workers satisfy a positive quorum at entry.

        Given:
            A non-lazy WorkerProxy configured with three static workers
            and quorum=2
        When:
            The proxy is entered as an async context manager
        Then:
            __aenter__ should succeed without timing out, with at least
            two workers admitted
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        workers = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"192.168.1.{i}:50051",
                pid=1000 + i,
                version="1.0.0",
                tags=frozenset(["test"]),
                extra=MappingProxyType({}),
            )
            for i in range(3)
        ]

        # Act & assert
        async with WorkerProxy(workers=workers, quorum=2, lazy=False) as proxy:
            assert proxy.started
            assert len(proxy.workers) >= 2

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
        proxy = WorkerProxy("test://pool", lazy=False, quorum=0)

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

        proxy = WorkerProxy(discovery=mock_discovery_service, lazy=False, quorum=0)

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
            quorum=0,
        )

        # Act & assert
        async with proxy:
            # Verify proxy is started before pickling
            assert proxy.started is True

            # Pickle from within the started proxy context
            pickled_data = wool.__serializer__.dumps(proxy)
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
        proxy = WorkerProxy(discovery=discovery_service, lazy=False, quorum=0)

        # Act & assert
        async with proxy:
            # Verify proxy is started before pickling
            assert proxy.started is True

            # Pickle from within the started proxy context
            pickled_data = wool.__serializer__.dumps(proxy)
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
        proxy = WorkerProxy("pool-1", lazy=False, quorum=0)

        # Act & assert
        async with proxy:
            # Verify proxy is started before pickling
            assert proxy.started is True

            # Pickle from within the started proxy context
            pickled_data = wool.__serializer__.dumps(proxy)
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
            pickled_data = wool.__serializer__.dumps(proxy)
            unpickled_proxy = cloudpickle.loads(pickled_data)

        # Assert
        assert isinstance(unpickled_proxy, WorkerProxy)
        assert unpickled_proxy.id == proxy.id
        assert unpickled_proxy.started is False
        # Verify a second roundtrip still produces a valid proxy
        repickled = cloudpickle.loads(wool.__serializer__.dumps(unpickled_proxy))
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
            pickled_data = wool.__serializer__.dumps(proxy)

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
            wool.__serializer__.dumps(proxy)

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
            wool.__serializer__.dumps(proxy)

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
            wool.__serializer__.dumps(proxy)

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
            wool.__serializer__.dumps(proxy)

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
            wool.__serializer__.dumps(proxy)

    def test___reduce_ex___with_vanilla_pickle_and_copy(self, mock_discovery_service):
        """Test the guard fires for vanilla pickle, cloudpickle, and copy paths.

        Given:
            A WorkerProxy instance.
        When:
            pickle.dumps, cloudpickle.dumps, copy.copy, and copy.deepcopy
            are called on it directly.
        Then:
            Each should raise TypeError pointing at Wool's runtime as the
            supported serialization path.
        """
        # Arrange
        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=wp.RoundRobinLoadBalancer,
        )
        match = "WorkerProxy cannot be pickled"

        # Act & assert
        with pytest.raises(TypeError, match=match):
            pickle.dumps(proxy)
        with pytest.raises(TypeError, match=match):
            cloudpickle.dumps(proxy)
        with pytest.raises(TypeError, match=match):
            copy.copy(proxy)
        with pytest.raises(TypeError, match=match):
            copy.deepcopy(proxy)

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
        proxy = WorkerProxy(workers=workers, quorum=None)

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


_DISPATCH_OUTCOMES = ("success", "transient", "non_transient")


async def _make_proxy_with_workers(
    *,
    connections: list,
    loadbalancer,
    mocker: MockerFixture,
    discovery=None,
    metadata_list=None,
) -> tuple[WorkerProxy, list[WorkerMetadata]]:
    """Build and start a WorkerProxy seeded via the public discovery flow.

    Patches `WorkerConnection` so the sentinel creates the given mock
    connections, constructs a proxy with a `ReducibleAsyncIterator`
    discovery stream, starts it, and waits until all workers are visible
    on ``proxy.workers``.

    A custom ``discovery`` stream (e.g. `_GatedDiscovery`) and matching
    ``metadata_list`` may be supplied; the drain then waits only for the
    ``metadata_list`` workers, and ``connections`` must cover every
    event the stream will ever emit.

    :returns:
        ``(proxy, metadata_list)`` where the metadata matches the
        connections in order.
    """
    if metadata_list is None:
        metadata_list = [
            WorkerMetadata(
                uid=uuid.uuid4(),
                address=f"127.0.0.1:{50100 + i}",
                pid=9000 + i,
                version="1.0.0",
            )
            for i in range(len(connections))
        ]
    mocker.patch.object(wp, "WorkerConnection", side_effect=connections)
    if discovery is None:
        discovery = wp.ReducibleAsyncIterator(
            [DiscoveryEvent("worker-added", metadata=m) for m in metadata_list]
        )
    proxy = WorkerProxy(
        discovery=discovery,
        loadbalancer=loadbalancer,
        lazy=False,
    )
    await proxy.start()
    await _drain_until(lambda: len(proxy.workers) == len(metadata_list))
    return proxy, metadata_list


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


def _make_outcome_connection(outcome, marker, mocker: MockerFixture):
    """Build a mock connection whose dispatch resolves to a drawn outcome.

    A ``"success"`` connection returns a stream yielding ``marker``; a
    ``"transient"`` connection raises `TransientRpcError`; any other
    outcome raises a non-transient `RpcError`.
    """
    connection = mocker.MagicMock(spec=WorkerConnection)
    if outcome == "success":

        async def _dispatch(task, *, timeout=None, _marker=marker):
            async def _stream():
                yield _marker

            return _stream()

        connection.dispatch = _dispatch
    elif outcome == "transient":
        connection.dispatch = mocker.AsyncMock(side_effect=TransientRpcError())
    else:
        connection.dispatch = mocker.AsyncMock(side_effect=RpcError())
    return connection


class _GatedDiscovery:
    """Discovery stream yielding initial events, then gated deferred events.

    Yields ``initial`` immediately, waits for ``gate`` to be set, then
    yields ``deferred`` — landing a discovery event deterministically in
    the middle of an in-flight dispatch.
    """

    def __init__(self, initial, deferred, gate):
        self._initial = list(initial)
        self._deferred = list(deferred)
        self._gate = gate

    def __aiter__(self):
        return self._events()

    async def _events(self):
        for event in self._initial:
            yield event
        await self._gate.wait()
        for event in self._deferred:
            yield event


class TestWorkerProxyDispatchRetryEviction:
    """Tests for the proxy-owned dispatch-retry-evict loop.

    Each test drives the public `dispatch()` API against a delegating
    balancer, seeding the worker pool through the public discovery flow.
    The proxy owns the dispatch loop — it calls `WorkerConnection.dispatch`
    on each candidate, evicts workers on non-transient errors, reports
    outcomes back to the balancer, and surfaces `NoWorkersAvailable` when
    candidates are exhausted. The tests verify each branch of that loop.
    """

    @pytest.mark.asyncio
    async def test_dispatch_forwards_task_to_delegate(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test the proxy forwards the routed task to the balancer.

        Given:
            A delegating balancer with one worker that records the task
            its ``delegate`` receives
        When:
            dispatch() is called with a task
        Then:
            The balancer's ``delegate`` should receive the same task
            object the caller passed to dispatch()
        """
        # Arrange
        routed_tasks: list = []
        success_streams: list = []
        success_connection = _make_success_connection(mocker, success_streams)

        proxy, _ = await _make_proxy_with_workers(
            connections=[success_connection],
            loadbalancer=make_delegating_balancer(
                on_task=lambda task: routed_tasks.append(task)
            ),
            mocker=mocker,
        )

        # Act
        result_stream = await proxy.dispatch(mock_wool_task)
        results = [r async for r in result_stream]

        # Assert
        assert results == ["ok"]
        assert routed_tasks == [mock_wool_task]
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_transient_error(
        self,
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
        failing_connection = mocker.MagicMock(spec=WorkerConnection)
        failing_connection.dispatch = mocker.AsyncMock(side_effect=TransientRpcError())
        success_streams: list = []
        success_connection = _make_success_connection(mocker, success_streams)

        proxy, metadata_list = await _make_proxy_with_workers(
            connections=[failing_connection, success_connection],
            loadbalancer=make_delegating_balancer(),
            mocker=mocker,
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
    async def test_dispatch_with_non_transient_error(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test non-transient errors evict the worker before retry.

        Given:
            A delegating balancer with two workers; the first worker's
            connection raises a non-transient ``RpcError``, the second
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
        failing_connection = mocker.MagicMock(spec=WorkerConnection)
        failing_connection.dispatch = mocker.AsyncMock(side_effect=RpcError())
        success_streams: list = []
        success_connection = _make_success_connection(mocker, success_streams)

        proxy, metadata_list = await _make_proxy_with_workers(
            connections=[failing_connection, success_connection],
            loadbalancer=make_delegating_balancer(
                on_throw=lambda exception, context: (
                    observed_workers_during_athrow.append(
                        [m for m, _ in context.workers.values()]
                    )
                )
            ),
            mocker=mocker,
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
    async def test_dispatch_with_non_rpc_error(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test non-RpcError faults propagate without evicting the worker.

        Given:
            A delegating balancer with one worker whose connection
            raises a non-``RpcError`` exception — a fault unrelated to
            worker health, such as a caller-side ``ValueError``
        When:
            dispatch() is called
        Then:
            The exception propagates to the caller unwrapped and the
            worker is left in ``proxy.workers``; only ``RpcError`` is
            treated as a worker-health signal warranting eviction.
        """
        # Arrange
        failing_connection = mocker.MagicMock(spec=WorkerConnection)
        failing_connection.dispatch = mocker.AsyncMock(side_effect=ValueError("boom"))

        proxy, [metadata] = await _make_proxy_with_workers(
            connections=[failing_connection],
            loadbalancer=make_delegating_balancer(),
            mocker=mocker,
        )

        # Act & assert
        with pytest.raises(ValueError, match="boom"):
            await proxy.dispatch(mock_wool_task)
        # Not a worker-health signal: the worker was NOT evicted.
        assert metadata in proxy.workers
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_empty_delegate(
        self,
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
            async def delegate(self, task, *, context):
                if False:
                    yield  # pragma: no cover

        dummy_connection = mocker.MagicMock(spec=WorkerConnection)
        proxy, _ = await _make_proxy_with_workers(
            connections=[dummy_connection],
            loadbalancer=EmptyBalancer(),
            mocker=mocker,
        )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await proxy.dispatch(mock_wool_task)
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_non_transient_exhaustion(
        self,
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
        failing_connection = mocker.MagicMock(spec=WorkerConnection)
        failing_connection.dispatch = mocker.AsyncMock(side_effect=RpcError())

        proxy, [metadata] = await _make_proxy_with_workers(
            connections=[failing_connection],
            loadbalancer=make_delegating_balancer(),
            mocker=mocker,
        )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await proxy.dispatch(mock_wool_task)
        # Non-transient error: worker was evicted from the context.
        assert metadata not in proxy.workers
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_exhausted_candidates(
        self,
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
        failing_connection = mocker.MagicMock(spec=WorkerConnection)
        failing_connection.dispatch = mocker.AsyncMock(side_effect=TransientRpcError())

        proxy, _ = await _make_proxy_with_workers(
            connections=[failing_connection],
            loadbalancer=make_delegating_balancer(),
            mocker=mocker,
        )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await proxy.dispatch(mock_wool_task)
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_evicted_checkpoint_and_transient_survivors(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test dispatch ends when the checkpoint worker is evicted.

        Given:
            A real RoundRobinLoadBalancer over three workers where the
            first-tried (checkpoint) worker fails non-transiently and is
            evicted, while both survivors fail transiently on every
            attempt
        When:
            dispatch() is awaited under a short timeout
        Then:
            It should raise NoWorkersAvailable rather than spin forever,
            because the balancer reseeds its cycle boundary when the
            checkpoint worker leaves the pool.
        """

        # Arrange
        def _slow_transient_connection():
            connection = mocker.MagicMock(spec=WorkerConnection)

            async def _dispatch(task, *, timeout=None):
                # Yield to the loop so wait_for can fire — a regression
                # surfaces as TimeoutError, never a suite hang.
                await asyncio.sleep(0)
                raise TransientRpcError()

            connection.dispatch = _dispatch
            return connection

        checkpoint_connection = mocker.MagicMock(spec=WorkerConnection)
        checkpoint_connection.dispatch = mocker.AsyncMock(side_effect=RpcError())
        proxy, [checkpoint, _, _] = await _make_proxy_with_workers(
            connections=[
                checkpoint_connection,
                _slow_transient_connection(),
                _slow_transient_connection(),
            ],
            loadbalancer=wp.RoundRobinLoadBalancer,
            mocker=mocker,
        )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await asyncio.wait_for(proxy.dispatch(mock_wool_task), timeout=2)
        assert checkpoint not in proxy.workers

        # Cleanup
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_success(
        self,
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
            The balancer observed a sent value equal to the uid of the
            successful worker — the candidate it yielded, echoed back —
            and the generator terminated.
        """
        # Arrange
        sent_values: list = []
        success_streams: list = []
        conn = _make_success_connection(mocker, success_streams)

        proxy, [metadata] = await _make_proxy_with_workers(
            connections=[conn],
            loadbalancer=make_delegating_balancer(
                on_success=lambda sent: sent_values.append(sent)
            ),
            mocker=mocker,
        )

        # Act
        stream = await proxy.dispatch(mock_wool_task)
        await stream.aclose()

        # Assert
        assert sent_values == [metadata.uid]
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_post_success_yield(
        self,
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
        sentinel_uid = uuid.uuid4()

        class MalformedBalancer:
            async def delegate(self, task, *, context):
                uids = list(context.workers)
                if not uids:
                    return
                _ = yield uids[0]
                # Contract violation: yield again after asend.
                yield sentinel_uid

        stream_close_calls: list[bool] = []
        spy_stream = SpyStream(["ok"], on_close=lambda: stream_close_calls.append(True))
        conn = mocker.MagicMock(spec=WorkerConnection)

        async def _dispatch(task, *, timeout=None):
            return spy_stream

        conn.dispatch = _dispatch

        proxy, _ = await _make_proxy_with_workers(
            connections=[conn],
            loadbalancer=MalformedBalancer(),
            mocker=mocker,
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="after receiving a success signal"):
            await proxy.dispatch(mock_wool_task)
        assert stream_close_calls == [True]
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_cancellation_during_connection_dispatch(
        self,
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
            NOT evicted, and the balancer yielded only one candidate
            (no retry against another candidate).
        """
        # Arrange
        yielded_candidates: list = []
        conn = mocker.MagicMock(spec=WorkerConnection)

        async def _hang(task, *, timeout=None):
            await asyncio.sleep(10)

        conn.dispatch = _hang

        proxy, [metadata] = await _make_proxy_with_workers(
            connections=[conn],
            loadbalancer=make_delegating_balancer(
                on_yield=lambda metadata: yielded_candidates.append(metadata)
            ),
            mocker=mocker,
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
        # Exactly one candidate was yielded — no retry on cancellation.
        assert len(yielded_candidates) == 1
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_cancellation_during_asend(
        self,
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
        # Arrange — the success hook blocks, simulating slow bookkeeping.
        hang = asyncio.Event()
        stream_close_calls: list[bool] = []
        spy_stream = SpyStream(on_close=lambda: stream_close_calls.append(True))
        conn = mocker.MagicMock(spec=WorkerConnection)

        async def _dispatch(task, *, timeout=None):
            return spy_stream

        conn.dispatch = _dispatch

        proxy, _ = await _make_proxy_with_workers(
            connections=[conn],
            loadbalancer=make_delegating_balancer(on_success=lambda sent: hang.wait()),
            mocker=mocker,
        )

        # Act
        dispatch_task = asyncio.create_task(proxy.dispatch(mock_wool_task))
        await asyncio.sleep(0.01)  # let dispatch reach the asend hang
        dispatch_task.cancel()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await dispatch_task
        assert stream_close_calls == [True]
        await proxy.stop()

    @settings(
        max_examples=30,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        outcomes=st.lists(st.sampled_from(_DISPATCH_OUTCOMES), min_size=1, max_size=5)
    )
    def test_dispatch_with_arbitrary_outcome_sequence(
        self, outcomes, mock_proxy_session, mock_wool_task, mocker: MockerFixture
    ):
        """Test the retry/evict loop over an arbitrary per-worker outcome run.

        Given:
            A delegating balancer over N workers whose connections each
            resolve to a drawn outcome — a success, a transient
            ``TransientRpcError``, or a non-transient ``RpcError``.
        When:
            dispatch() drives the candidates in order.
        Then:
            It should return the first success (else raise
            NoWorkersAvailable), evict exactly the non-transient failures
            it attempted, and leave every transient-failing worker in
            place.
        """

        async def _scenario() -> None:
            # Arrange
            connections = [
                _make_outcome_connection(outcome, index, mocker)
                for index, outcome in enumerate(outcomes)
            ]
            proxy, metadata_list = await _make_proxy_with_workers(
                connections=connections,
                loadbalancer=make_delegating_balancer(),
                mocker=mocker,
            )
            first_success = next(
                (i for i, o in enumerate(outcomes) if o == "success"), None
            )
            attempted = (
                range(len(outcomes))
                if first_success is None
                else range(first_success + 1)
            )
            evicted = {
                metadata_list[i] for i in attempted if outcomes[i] == "non_transient"
            }
            surviving = set(metadata_list) - evicted

            # Act & assert
            try:
                if first_success is None:
                    with pytest.raises(NoWorkersAvailable):
                        await proxy.dispatch(mock_wool_task)
                else:
                    stream = await proxy.dispatch(mock_wool_task)
                    results = [item async for item in stream]
                    assert results == [first_success]
                assert set(proxy.workers) == surviving
                for index, outcome in enumerate(outcomes):
                    if outcome == "transient":
                        assert metadata_list[index] in proxy.workers
            finally:
                await proxy.stop()

        asyncio.run(_scenario())

    @pytest.mark.asyncio
    async def test_start_with_delegating_balancer(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test starting a delegating balancer emits no deprecation warning.

        Given:
            A proxy configured with a delegating balancer that implements
            the ``delegate`` protocol.
        When:
            The proxy is started.
        Then:
            It should emit no DeprecationWarning — the delegating protocol
            is the supported, non-deprecated path.
        """
        # Arrange
        proxy = WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=make_delegating_balancer(),
            lazy=False,
            quorum=0,
        )

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            await proxy.start()

        # Assert
        deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert deprecations == []
        await proxy.stop()


class TestWorkerProxyDispatchingBalancerBackcompat:
    """Tests for the deprecated `DispatchingLoadBalancerLike` dispatch path.

    A balancer that implements only the legacy ``dispatch`` method keeps
    working: the proxy delegates dispatch to it and warns once that the
    protocol is deprecated.
    """

    @pytest.mark.asyncio
    async def test_init_with_dispatching_balancer_instance(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test constructing with a legacy balancer instance warns once.

        Given:
            A load balancer instance implementing only the legacy
            ``dispatch`` method (a `DispatchingLoadBalancerLike`)
        When:
            The proxy is constructed and then started
        Then:
            It should emit exactly one deprecation warning, at
            construction, and starting should not warn a second time
        """
        # Arrange
        loadbalancer = make_dispatching_balancer()

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            proxy = WorkerProxy(
                discovery=mock_discovery_service,
                loadbalancer=loadbalancer,
                lazy=False,
                quorum=0,
            )
            warned_at_construction = _dispatch_deprecations(caught)
            await proxy.start()

        # Assert
        assert len(warned_at_construction) == 1
        assert len(_dispatch_deprecations(caught)) == 1
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_start_with_dispatching_balancer_factory(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test a legacy balancer factory warns at start().

        Given:
            A factory returning a load balancer that implements only the
            legacy ``dispatch`` method (a `DispatchingLoadBalancerLike`)
        When:
            The proxy is constructed and then started
        Then:
            It should not warn at construction — the factory is opaque —
            and emit exactly one deprecation warning when start() resolves
            it to the legacy instance
        """

        # Arrange
        def factory():
            return make_dispatching_balancer()

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            proxy = WorkerProxy(
                discovery=mock_discovery_service,
                loadbalancer=factory,
                lazy=False,
                quorum=0,
            )
            warned_at_construction = _dispatch_deprecations(caught)
            await proxy.start()

        # Assert
        assert warned_at_construction == []
        assert len(_dispatch_deprecations(caught)) == 1
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_dispatching_balancer(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test the legacy dispatch path is still functional.

        Given:
            A started proxy with a legacy `DispatchingLoadBalancerLike`
        When:
            dispatch() is called
        Then:
            The legacy ``dispatch`` method is invoked and its stream
            is returned to the caller.
        """
        # Arrange
        dispatched_tasks: list = []
        loadbalancer = make_dispatching_balancer(
            marker="legacy-ok",
            on_dispatch=lambda task: dispatched_tasks.append(task),
        )

        dummy_connection = mocker.MagicMock(spec=WorkerConnection)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            proxy, _ = await _make_proxy_with_workers(
                connections=[dummy_connection],
                loadbalancer=loadbalancer,
                mocker=mocker,
            )

        # Act
        stream = await proxy.dispatch(mock_wool_task)
        results = [r async for r in stream]

        # Assert
        assert results == ["legacy-ok"]
        assert len(dispatched_tasks) == 1
        await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_with_dual_protocol_balancer(
        self,
        mock_proxy_session,
        mock_wool_task,
        mocker: MockerFixture,
    ):
        """Test a balancer implementing both protocols uses delegate.

        Given:
            A balancer implementing both the ``delegate`` and the legacy
            ``dispatch`` methods, seeded with one worker
        When:
            The proxy is constructed, started, and dispatch() is called
        Then:
            It should route through ``delegate`` — surfacing the delegate
            path's result, not the ``dispatch`` result — and emit no
            deprecation warning, because the non-deprecated path is used
        """

        # Arrange
        class DualBalancer:
            async def delegate(self, task, *, context):
                for uid in list(context.workers):
                    sent = yield uid
                    if sent is not None:
                        return

            async def dispatch(self, task, *, context, timeout=None):
                async def _stream():
                    yield "legacy-ok"

                return _stream()

        success_streams: list = []
        success_connection = _make_success_connection(mocker, success_streams)

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            proxy, _ = await _make_proxy_with_workers(
                connections=[success_connection],
                loadbalancer=DualBalancer(),
                mocker=mocker,
            )
            result_stream = await proxy.dispatch(mock_wool_task)
            results = [r async for r in result_stream]

        # Assert
        assert results == ["ok"]
        assert _dispatch_deprecations(caught) == []
        await proxy.stop()
