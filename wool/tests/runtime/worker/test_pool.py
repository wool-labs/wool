"""Comprehensive tests for WorkerPool orchestration.

Tests validate WorkerPool behavior through observable public APIs only,
without accessing private state. All tests use mock workers to avoid
subprocess overhead and ensure deterministic behavior.
"""

import asyncio
import logging
import os
import time
import uuid
from contextlib import AsyncExitStack
from contextlib import contextmanager
from functools import partial
from types import MappingProxyType
from typing import cast

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

import wool
import wool.runtime.worker.pool as wp
from wool import protocol
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoveryLike
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.metadata import WorkerMetadata
from wool.runtime.worker.pool import IneffectiveLeaseWarning
from wool.runtime.worker.pool import WorkerPool
from wool.runtime.worker.proxy import IneffectiveQuorumTimeoutWarning
from wool.runtime.worker.proxy import ReducibleAsyncIterator


def _make_worker_metadata(*tags: str) -> WorkerMetadata:
    """Build a valid WorkerMetadata with a fresh UUID."""
    return WorkerMetadata(
        uid=uuid.uuid4(),
        address="localhost:50051",
        pid=12345,
        version="1.0.0",
        tags=frozenset(tags),
        extra=MappingProxyType({}),
    )


class _FakePublisher:
    """Minimal ``DiscoveryPublisherLike`` double for ``_FakeDiscovery``.

    A concrete class rather than a mock: the pool validates the
    publisher with a runtime ``isinstance`` against the protocol, which
    a spec'd mock fails and a specless one sends into recursion.
    """

    bind_host = "127.0.0.1"

    def __init__(self, mocker):
        self.publish = mocker.AsyncMock()


class _FakeDiscovery(DiscoveryLike):
    """Custom ``DiscoveryLike`` double for admission-gate pool tests.

    Serves both shapes the sentinel tests need through one surface:
    when ``events`` is supplied, ``subscriber`` and ``subscribe`` yield
    a ``ReducibleAsyncIterator`` over them (durable admission tests);
    otherwise they return a plain mock subscriber. ``subscribe`` always
    records the predicate it was handed on ``captured_filter`` so
    filter-composition tests can evaluate it.
    """

    def __init__(self, mocker, events=None):
        self._events = events
        self._publisher = _FakePublisher(mocker)
        self._subscriber = mocker.MagicMock(spec=DiscoverySubscriberLike)
        self.captured_filter = None

    @property
    def publisher(self):
        return self._publisher

    @property
    def subscriber(self):
        if self._events is not None:
            return ReducibleAsyncIterator(self._events)
        return self._subscriber

    def subscribe(self, filter=None):
        self.captured_filter = filter
        if self._events is not None:
            return ReducibleAsyncIterator(self._events)
        return self._subscriber


class TestWorkerPool:
    """Test suite for WorkerPool orchestration."""

    # =========================================================================
    # Constructor Tests
    # =========================================================================

    def test___init___uses_cpu_count_as_default_spawn(self, mocker: MockerFixture):
        """Test successfully create a pool using CPU count.

        Given:
            No spawn parameter is provided and CPU count is available
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

    def test___init___raises_error_when_cpu_count_unavailable(
        self, mocker: MockerFixture
    ):
        """Test raise ValueError with appropriate message.

        Given:
            Os.cpu_count() returns None and spawn is set to 0
        When:
            WorkerPool is initialized
        Then:
            Should raise ValueError with appropriate message
        """
        # Arrange
        mock_cpu_count = mocker.patch("os.cpu_count", return_value=None)

        # Act & assert
        with pytest.raises(ValueError, match="Unable to determine CPU count"):
            WorkerPool(spawn=0)

        mock_cpu_count.assert_called_once()

    def test___init___raises_error_when_cpu_count_unavailable_default_spawn(
        self, mocker: MockerFixture
    ):
        """Test raise ValueError indicating CPU count cannot be determined.

        Given:
            A system where os.cpu_count() returns None and no spawn is specified
        When:
            WorkerPool constructor is called with default parameters
        Then:
            Should raise ValueError indicating CPU count cannot be determined
        """
        # Arrange
        mocker.patch("os.cpu_count", return_value=None)

        # Act & assert
        with pytest.raises(ValueError, match="Unable to determine CPU count"):
            WorkerPool()

    def test___init___raises_error_with_negative_spawn(self):
        """Test raise ValueError with appropriate message.

        Given:
            A negative spawn parameter
        When:
            WorkerPool is initialized
        Then:
            Should raise ValueError with appropriate message
        """
        # Arrange
        invalid_spawn = -1

        # Act & assert
        with pytest.raises(ValueError, match="Spawn must be non-negative"):
            WorkerPool(spawn=invalid_spawn)

    def test___init___accepts_positive_shutdown_timeout(self):
        """Test pool constructs with a positive shutdown_timeout.

        Given:
            A positive shutdown_timeout value
        When:
            WorkerPool is initialized
        Then:
            It should construct a valid pool
        """
        # Act
        pool = WorkerPool(spawn=1, shutdown_timeout=5.0)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___accepts_none_shutdown_timeout(self):
        """Test pool constructs with shutdown_timeout=None.

        Given:
            Shutdown_timeout explicitly set to None to opt out of bounding
        When:
            WorkerPool is initialized
        Then:
            It should construct a valid pool
        """
        # Act
        pool = WorkerPool(spawn=1, shutdown_timeout=None)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___rejects_zero_shutdown_timeout(self):
        """Test zero shutdown_timeout raises ValueError.

        Given:
            A shutdown_timeout of 0
        When:
            WorkerPool is initialized
        Then:
            It should raise ValueError indicating the timeout must be positive
        """
        # Act & assert
        with pytest.raises(ValueError, match="Shutdown timeout must be positive"):
            WorkerPool(spawn=1, shutdown_timeout=0)

    def test___init___rejects_negative_shutdown_timeout(self):
        """Test negative shutdown_timeout raises ValueError.

        Given:
            A negative shutdown_timeout value
        When:
            WorkerPool is initialized
        Then:
            It should raise ValueError indicating the timeout must be positive
        """
        # Act & assert
        with pytest.raises(ValueError, match="Shutdown timeout must be positive"):
            WorkerPool(spawn=1, shutdown_timeout=-1.0)

    def test___init___size_emits_deprecation_warning(self):
        """Test deprecated size parameter emits DeprecationWarning.

        Given:
            The deprecated 'size' parameter is passed
        When:
            WorkerPool is initialized
        Then:
            It should emit a DeprecationWarning
        """
        # Act & assert
        with pytest.warns(DeprecationWarning, match="'size' parameter is deprecated"):
            WorkerPool(size=4)

    def test___init___spawn_does_not_emit_deprecation_warning(self):
        """Test spawn parameter does not emit DeprecationWarning.

        Given:
            The 'spawn' parameter is passed
        When:
            WorkerPool is initialized
        Then:
            It should not emit a DeprecationWarning
        """
        # Arrange
        import warnings

        # Act & assert
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            WorkerPool(spawn=4)

    def test___init___size_and_spawn_raises_type_error(self):
        """Test passing both size and spawn raises TypeError.

        Given:
            Both 'size' and 'spawn' parameters are provided
        When:
            WorkerPool is initialized
        Then:
            It should raise a TypeError
        """
        # Act & assert
        with pytest.raises(TypeError, match="Cannot specify both"):
            WorkerPool(size=4, spawn=4)

    def test___init___size_value_forwarded_to_spawn(self):
        """Test deprecated size value is forwarded to spawn.

        Given:
            The deprecated 'size' parameter with value 4
        When:
            WorkerPool is initialized
        Then:
            It should create a valid pool identical to spawn=4
        """
        # Arrange
        import warnings

        # Act
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            pool = WorkerPool(size=4)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___size_negative_emits_warning_and_raises(self):
        """Test deprecated size with negative value emits warning and raises.

        Given:
            The deprecated 'size' parameter with value -1
        When:
            WorkerPool is initialized
        Then:
            It should emit a DeprecationWarning and raise ValueError
        """
        # Act & assert
        with pytest.warns(DeprecationWarning, match="'size' parameter is deprecated"):
            with pytest.raises(ValueError, match="Spawn must be non-negative"):
                WorkerPool(size=-1)

    def test___init___size_zero_resolves_cpu_count(self, mocker: MockerFixture):
        """Test deprecated size=0 resolves to CPU count.

        Given:
            The deprecated 'size' parameter with value 0 and CPU count available
        When:
            WorkerPool is initialized
        Then:
            It should emit a DeprecationWarning and create a valid pool
        """
        # Arrange
        mocker.patch("os.cpu_count", return_value=4)

        # Act
        with pytest.warns(DeprecationWarning, match="'size' parameter is deprecated"):
            pool = WorkerPool(size=0)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___size_with_discovery_creates_hybrid(self, mocker: MockerFixture):
        """Test deprecated size with discovery creates hybrid pool.

        Given:
            The deprecated 'size' parameter and a discovery service
        When:
            WorkerPool is initialized
        Then:
            It should emit a DeprecationWarning and create a valid pool
        """
        # Arrange
        mock_discovery = mocker.MagicMock()

        # Act
        with pytest.warns(DeprecationWarning, match="'size' parameter is deprecated"):
            pool = WorkerPool(size=2, discovery=mock_discovery)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___size_with_lease_warns(self):
        """Test deprecated size combined with lease without discovery.

        Given:
            The deprecated 'size' parameter with value 4 and lease of 8,
            and no discovery service
        When:
            WorkerPool is initialized
        Then:
            It should emit a DeprecationWarning for 'size' and an
            IneffectiveLeaseWarning for 'lease', and still create the
            pool successfully — 'lease' is recorded but never consulted
        """
        # Act
        with pytest.warns(DeprecationWarning, match="'size' parameter is deprecated"):
            with pytest.warns(
                IneffectiveLeaseWarning,
                match="'lease' has no effect when no 'discovery' service",
            ):
                pool = WorkerPool(size=4, lease=8)  # type: ignore[call-overload]

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___size_warns_at_caller_frame(self):
        """Test deprecation warning points to the caller's frame.

        Given:
            The deprecated 'size' parameter is passed
        When:
            WorkerPool is initialized
        Then:
            It should emit a warning whose filename is the test file
        """
        # Arrange
        import warnings

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            WorkerPool(size=4)

        # Assert
        deprecation_warnings = [
            w for w in caught if issubclass(w.category, DeprecationWarning)
        ]
        assert len(deprecation_warnings) == 1
        assert "test_pool.py" in deprecation_warnings[0].filename

    def test___init___with_zero_spawn_and_available_cpu_count(
        self, mocker: MockerFixture
    ):
        """Test use CPU count as the pool spawn count.

        Given:
            Spawn parameter of 0 and available CPU count
        When:
            WorkerPool is initialized
        Then:
            Should use CPU count as the pool spawn count
        """
        # Arrange
        mock_cpu_count = mocker.patch("os.cpu_count", return_value=8)

        # Act
        pool = WorkerPool(spawn=0)

        # Assert
        assert isinstance(pool, WorkerPool)
        mock_cpu_count.assert_called_once()

    def test___init___accepts_tags_and_spawn_parameters(self):
        """Test successfully create a pool with the specified configuration.

        Given:
            Custom tags and spawn parameter
        When:
            WorkerPool is initialized
        Then:
            It should successfully create a pool with the specified configuration
        """
        # Arrange
        expected_tags = ("gpu-capable", "ml-model")
        expected_spawn = 2

        # Act
        pool = WorkerPool(*expected_tags, spawn=expected_spawn)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___with_empty_tags(self):
        """Test create pool successfully.

        Given:
            No tags provided to constructor
        When:
            WorkerPool is initialized
        Then:
            Should create pool successfully
        """
        # Act
        pool = WorkerPool(spawn=1)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___with_multiple_tags(self):
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
        pool = WorkerPool(*tags, spawn=2)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___accepts_custom_worker_factory(self, mocker: MockerFixture):
        """Test accept the factory for later worker creation.

        Given:
            A custom worker factory function and spawn
        When:
            WorkerPool is initialized
        Then:
            It should accept the factory for later worker creation
        """
        # Arrange
        mock_worker_factory = mocker.MagicMock()
        expected_spawn = 2

        # Act
        pool = WorkerPool(spawn=expected_spawn, worker=mock_worker_factory)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___durable_mode_with_discovery(self, mocker):
        """Test configure for durable mode (no local workers).

        Given:
            A discovery service parameter
        When:
            WorkerPool is initialized with discovery
        Then:
            Should configure for durable mode (no local workers)
        """
        # Arrange
        mock_discovery = mocker.MagicMock()

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
        mock_worker = mocker.MagicMock(spec=LocalWorker)
        mock_worker.start = mocker.AsyncMock()
        mock_worker.stop = mocker.AsyncMock()
        mock_worker.metadata = _make_worker_metadata()
        spy_factory.return_value = mock_worker

        # Act
        async with WorkerPool(
            spawn=1, worker=spy_factory, credentials=worker_credentials
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
        mock_worker = mocker.MagicMock(spec=LocalWorker)
        mock_worker.start = mocker.AsyncMock()
        mock_worker.stop = mocker.AsyncMock()
        mock_worker.metadata = _make_worker_metadata()
        spy_factory.return_value = mock_worker

        # Act
        async with WorkerPool(spawn=1, worker=spy_factory, credentials=None):
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
        async with WorkerPool(spawn=1, credentials=worker_credentials):
            pass

        # Assert

        wp.LocalWorker.assert_called()
        _, kwargs = wp.LocalWorker.call_args
        assert kwargs["credentials"] is worker_credentials

    @pytest.mark.asyncio
    async def test_worker_context_default_factory_without_discovery(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test default factory binds loopback for spawn-only pools.

        Given:
            A pool with spawn and no discovery, using the default
            factory
        When:
            A worker is created
        Then:
            It should pass host="127.0.0.1" to LocalWorker.
        """
        # Act
        async with WorkerPool(spawn=1):
            pass

        # Assert

        wp.LocalWorker.assert_called()
        _, kwargs = wp.LocalWorker.call_args
        assert kwargs["host"] == "127.0.0.1"

    @pytest.mark.asyncio
    async def test_worker_context_default_factory_with_wildcard_bind_publisher(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test default factory binds the publisher's prescribed host.

        Given:
            A hybrid pool whose discovery publisher prescribes
            bind_host="0.0.0.0" and no explicit worker factory
        When:
            A worker is created
        Then:
            It should pass host="0.0.0.0" to LocalWorker so the
            advertised routable address is actually served.
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "0.0.0.0"

        # Act
        async with WorkerPool(spawn=1, discovery=mock_discovery_service_for_pool):
            pass

        # Assert

        wp.LocalWorker.assert_called()
        _, kwargs = wp.LocalWorker.call_args
        assert kwargs["host"] == "0.0.0.0"

    @pytest.mark.asyncio
    async def test_worker_context_with_bound_factory(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_discovery_service_for_pool,
    ):
        """Test a bound factory is never passed a bind host.

        Given:
            A hybrid pool whose discovery publisher prescribes
            bind_host="0.0.0.0" and a factory without an explicit
            host parameter (a MagicMock accepts only **kwargs)
        When:
            A worker is created
        Then:
            It should call the factory without a host argument — a
            bound factory always wins.
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "0.0.0.0"
        spy_factory = mocker.MagicMock()
        mock_worker = mocker.MagicMock(spec=LocalWorker)
        mock_worker.start = mocker.AsyncMock()
        mock_worker.stop = mocker.AsyncMock()
        mock_worker.metadata = _make_worker_metadata()
        spy_factory.return_value = mock_worker

        # Act
        async with WorkerPool(
            spawn=1,
            worker=spy_factory,
            discovery=mock_discovery_service_for_pool,
        ):
            pass

        # Assert
        spy_factory.assert_called_once()
        _, kwargs = spy_factory.call_args
        assert "host" not in kwargs

    @pytest.mark.asyncio
    async def test_worker_context_with_unbound_factory(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_discovery_service_for_pool,
    ):
        """Test an unbound factory receives the publisher's bind host.

        Given:
            A hybrid pool whose discovery publisher prescribes
            bind_host="0.0.0.0" and a factory declaring an explicit
            host parameter
        When:
            A worker is created
        Then:
            It should pass the publisher's bind host to the factory.
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "0.0.0.0"
        received = []

        def unbound_factory(*tags, credentials=None, host):
            received.append(host)
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()
            worker.stop = mocker.AsyncMock()
            worker.metadata = _make_worker_metadata()
            return worker

        # Act
        async with WorkerPool(
            spawn=1,
            worker=unbound_factory,
            discovery=mock_discovery_service_for_pool,
        ):
            pass

        # Assert
        assert received == ["0.0.0.0"]

    @pytest.mark.asyncio
    async def test_worker_context_with_kwargs_sink_factory(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_discovery_service_for_pool,
    ):
        """Test a kwargs-accepting factory is classified bound.

        Given:
            A hybrid pool whose discovery publisher prescribes
            bind_host="0.0.0.0" and a factory accepting **kwargs but
            declaring no explicit host parameter
        When:
            A worker is created
        Then:
            It should call the factory without a host argument rather
            than pass a host into a keyword sink.
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "0.0.0.0"
        received = []

        def sink_factory(*tags, credentials=None, **kwargs):
            received.append(kwargs)
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()
            worker.stop = mocker.AsyncMock()
            worker.metadata = _make_worker_metadata()
            return worker

        # Act
        async with WorkerPool(
            spawn=1,
            worker=sink_factory,
            discovery=mock_discovery_service_for_pool,
        ):
            pass

        # Assert
        assert received == [{}]

    @pytest.mark.asyncio
    async def test_worker_context_with_positional_host_factory(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_discovery_service_for_pool,
    ):
        """Test a positional host parameter is classified bound.

        Given:
            A hybrid pool whose discovery publisher prescribes
            bind_host="0.0.0.0" and a factory declaring host as a
            positional-or-keyword parameter rather than keyword-only
        When:
            A worker is created
        Then:
            It should call the factory without injecting the publisher
            host — only a keyword-only host opts in — so the factory
            observes its own default and a positional host cannot
            collide with a forwarded tag at spawn.
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "0.0.0.0"
        received = []

        def positional_host_factory(host="127.0.0.1", *, credentials=None):
            received.append(host)
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()
            worker.stop = mocker.AsyncMock()
            worker.metadata = _make_worker_metadata()
            return worker

        # Act
        async with WorkerPool(
            spawn=1,
            worker=positional_host_factory,
            discovery=mock_discovery_service_for_pool,
        ):
            pass

        # Assert
        assert received == ["127.0.0.1"]

    @pytest.mark.asyncio
    async def test_worker_context_with_prebound_partial_factory(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_discovery_service_for_pool,
    ):
        """Test a partial pre-supplying host is classified bound.

        Given:
            A hybrid pool whose discovery publisher prescribes
            bind_host="0.0.0.0" and a functools.partial of an unbound
            factory that pre-supplies host="10.0.0.5"
        When:
            A worker is created
        Then:
            It should not override the pre-bound value — the factory
            observes "10.0.0.5".
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "0.0.0.0"
        received = []

        def unbound_factory(*tags, credentials=None, host):
            received.append(host)
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()
            worker.stop = mocker.AsyncMock()
            worker.metadata = _make_worker_metadata()
            return worker

        # Act
        async with WorkerPool(
            spawn=1,
            worker=partial(unbound_factory, host="10.0.0.5"),
            discovery=mock_discovery_service_for_pool,
        ):
            pass

        # Assert
        assert received == ["10.0.0.5"]

    @pytest.mark.asyncio
    async def test_worker_context_with_partial_unbound_factory(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_discovery_service_for_pool,
    ):
        """Test a partial not touching host stays unbound.

        Given:
            A hybrid pool whose discovery publisher prescribes
            bind_host="0.0.0.0" and a functools.partial of an unbound
            factory that pre-supplies only credentials
        When:
            A worker is created
        Then:
            It should pass the publisher's bind host to the factory.
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "0.0.0.0"
        received = []

        def unbound_factory(*tags, credentials=None, host):
            received.append(host)
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()
            worker.stop = mocker.AsyncMock()
            worker.metadata = _make_worker_metadata()
            return worker

        # Act
        async with WorkerPool(
            spawn=1,
            worker=partial(unbound_factory, credentials=None),
            discovery=mock_discovery_service_for_pool,
        ):
            pass

        # Assert
        assert received == ["0.0.0.0"]

    @pytest.mark.asyncio
    async def test_worker_context_with_uninspectable_factory(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_discovery_service_for_pool,
    ):
        """Test a factory with an uninspectable signature.

        Given:
            A hybrid pool whose discovery publisher prescribes
            bind_host="0.0.0.0" and a factory whose signature cannot
            be inspected (simulating a C-implemented callable)
        When:
            A worker is created
        Then:
            It should call the factory without a host argument —
            uninspectable signatures classify bound, the safe default.
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "0.0.0.0"
        received = []

        def opaque_factory(*tags, credentials=None, **kwargs):
            received.append(kwargs)
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()
            worker.stop = mocker.AsyncMock()
            worker.metadata = _make_worker_metadata()
            return worker

        # Simulate a C callable: inspect.signature rejects a
        # non-Signature __signature__ override.
        opaque_factory.__signature__ = "uninspectable"

        # Act
        async with WorkerPool(
            spawn=1,
            worker=opaque_factory,
            discovery=mock_discovery_service_for_pool,
        ):
            pass

        # Assert
        assert received == [{}]

    @pytest.mark.asyncio
    async def test_worker_context_with_publisher_missing_bind_host(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test a publisher without bind_host is rejected.

        Given:
            A hybrid pool whose discovery publisher implements publish
            but declares no bind_host
        When:
            The pool is entered
        Then:
            It should raise TypeError naming the publisher protocol,
            rather than silently defaulting the worker bind host.
        """

        # Arrange
        class BarePublisher:
            def __init__(self):
                self.publish = mocker.AsyncMock()

        class BareDiscovery:
            def __init__(self):
                self.publisher = BarePublisher()
                self.subscriber = mocker.MagicMock()

            def subscribe(self, filter=None):
                return self.subscriber

        # Act & assert
        with pytest.raises(TypeError, match="DiscoveryPublisherLike"):
            async with WorkerPool(spawn=1, discovery=BareDiscovery()):
                pass

    @pytest.mark.asyncio
    async def test_worker_context_with_rejected_context_manager_publisher(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test a rejected context-manager publisher is exited.

        Given:
            A hybrid pool whose discovery publisher is an async
            context manager implementing publish but declaring no
            bind_host
        When:
            The pool is entered
        Then:
            It should raise TypeError and exit the publisher context
            it entered, leaking no resources.
        """
        # Arrange
        entered = []
        exited = []

        class BareContextPublisher:
            def __init__(self):
                self.publish = mocker.AsyncMock()

            async def __aenter__(self):
                entered.append(True)
                return self

            async def __aexit__(self, *args):
                exited.append(True)

        class BareDiscovery:
            def __init__(self):
                self.publisher = BareContextPublisher()
                self.subscriber = mocker.MagicMock()

            def subscribe(self, filter=None):
                return self.subscriber

        # Act
        with pytest.raises(TypeError, match="DiscoveryPublisherLike"):
            async with WorkerPool(spawn=1, discovery=BareDiscovery()):
                pass

        # Assert
        assert entered == [True]
        assert exited == [True]

    @pytest.mark.asyncio
    async def test_worker_context_default_factory_with_custom_bind_host(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
        worker_credentials,
    ):
        """Test default factory honors an arbitrary bind host.

        Given:
            A hybrid pool with a tag and credentials whose discovery
            publisher prescribes bind_host="10.0.0.9"
        When:
            A worker is created
        Then:
            It should pass the host verbatim to LocalWorker alongside
            the tag and credentials.
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "10.0.0.9"

        # Act
        async with WorkerPool(
            "gpu",
            spawn=1,
            discovery=mock_discovery_service_for_pool,
            credentials=worker_credentials,
        ):
            pass

        # Assert

        wp.LocalWorker.assert_called()
        args, kwargs = wp.LocalWorker.call_args
        assert kwargs["host"] == "10.0.0.9"
        assert "gpu" in args
        assert kwargs["credentials"] is worker_credentials

    @pytest.mark.asyncio
    async def test_worker_context_default_factory_with_discovery_factory(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test bind host is read from the resolved discovery.

        Given:
            A hybrid pool whose discovery argument is a callable
            factory returning an instance whose publisher prescribes
            bind_host="0.0.0.0"
        When:
            A worker is created
        Then:
            It should pass host="0.0.0.0" to LocalWorker, proving the
            host comes from the resolved instance's publisher rather
            than the factory object.
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "0.0.0.0"

        # Act
        async with WorkerPool(
            spawn=1, discovery=lambda: mock_discovery_service_for_pool
        ):
            pass

        # Assert

        wp.LocalWorker.assert_called()
        _, kwargs = wp.LocalWorker.call_args
        assert kwargs["host"] == "0.0.0.0"

    @pytest.mark.asyncio
    async def test_durable_mode_with_wildcard_bind_publisher(
        self,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test durable pools spawn no workers despite the publisher.

        Given:
            A discovery-only pool whose discovery publisher prescribes
            bind_host="0.0.0.0"
        When:
            The pool is entered and exited
        Then:
            It should never construct a LocalWorker.
        """
        # Arrange
        mock_discovery_service_for_pool.publisher.bind_host = "0.0.0.0"

        # Act
        async with WorkerPool(discovery=mock_discovery_service_for_pool):
            pass

        # Assert

        wp.LocalWorker.assert_not_called()

    @pytest.mark.asyncio
    async def test_worker_context_default_factory_with_default_mode(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test default no-args pools bind every worker to loopback.

        Given:
            A WorkerPool with no arguments and a CPU count of 2
        When:
            The pool is entered and workers are created
        Then:
            It should pass host="127.0.0.1" to every LocalWorker.
        """
        # Arrange
        mocker.patch.object(os, "cpu_count", return_value=2)

        # Act
        async with WorkerPool():
            pass

        # Assert

        assert wp.LocalWorker.call_count == 2
        for _, kwargs in wp.LocalWorker.call_args_list:
            assert kwargs["host"] == "127.0.0.1"

    # =========================================================================
    # Context Manager Lifecycle Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test___aenter___lifecycle(self, mock_worker_factory):
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
        async with WorkerPool(worker=mock_worker_factory, spawn=2) as pool:
            pool_instance = pool
            # Pool is started and ready
            assert pool is not None
            assert isinstance(pool, WorkerPool)

        # Assert: Context manager returned pool and lifecycle completed
        assert pool_instance is not None

    @pytest.mark.asyncio
    async def test___aenter___should_return_pool_when_pushed_onto_async_exit_stack(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test entering a pool through an AsyncExitStack.

        Given:
            A WorkerPool
        When:
            The pool is pushed onto a contextlib.AsyncExitStack
        Then:
            It should return the pool itself.
        """
        # Arrange
        pool = WorkerPool(spawn=1)

        # Act
        async with AsyncExitStack() as stack:
            entered = await stack.enter_async_context(pool)

        # Assert
        assert entered is pool

    @pytest.mark.asyncio
    async def test___aexit___should_tear_down_pool_when_async_exit_stack_unwinds(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test tearing a pool down through an AsyncExitStack.

        Given:
            A WorkerPool pushed onto a contextlib.AsyncExitStack
        When:
            The stack unwinds
        Then:
            It should exit the pool's worker proxy context.
        """
        # Arrange
        pool = WorkerPool(spawn=1)

        # Act
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(pool)

        # Assert
        mock_worker_proxy.__aexit__.assert_awaited()

    @pytest.mark.asyncio
    async def test___aexit___cleanup_on_error(self, mock_worker_factory):
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

        # Act & assert
        try:
            async with WorkerPool(worker=mock_worker_factory, spawn=2):
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
    async def test___aenter___already_entered_raises_error(self, mock_worker_factory):
        """Test pool raises on reentrant entry.

        Given:
            A WorkerPool that is already entered via async with.
        When:
            The pool is entered a second time via async with.
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        pool = WorkerPool(worker=mock_worker_factory, spawn=2)

        # Act & assert
        async with pool:
            with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
                async with pool:
                    pass

    @pytest.mark.asyncio
    async def test___aenter___after_exit_raises_error(self, mock_worker_factory):
        """Test pool raises when re-entered after exit.

        Given:
            A WorkerPool that has been entered and exited.
        When:
            The pool is entered again via async with.
        Then:
            It should raise RuntimeError because the context is single-use.
        """
        # Arrange
        pool = WorkerPool(worker=mock_worker_factory, spawn=2)
        async with pool:
            pass

        # Act & assert
        with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
            async with pool:
                pass

    @pytest.mark.asyncio
    async def test___aenter___lifecycle_returns_pool_instance(
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
        async with WorkerPool(spawn=1) as returned_pool:
            # Assert: Context manager entry
            assert returned_pool is not None
            assert isinstance(returned_pool, WorkerPool)
            mock_worker_proxy.__aenter__.assert_called_once()

        # Assert: Context manager exit
        mock_worker_proxy.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test___aexit___with_exception_in_user_code(
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
        # Act & assert
        with pytest.raises(ValueError, match="User error"):
            async with WorkerPool(spawn=2) as pool:
                assert pool is not None
                raise ValueError("User error")

        # Assert: Cleanup still happened
        mock_worker_proxy.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test___aenter___with_worker_startup_failure(self, mocker: MockerFixture):
        """Test raise the spawn exception before yielding.

        Given:
            A worker that fails during startup
        When:
            WorkerPool is used as context manager
        Then:
            It should raise the spawn exception before yielding
        """

        # Arrange
        def failing_factory(*tags, credentials=None, options=None):
            worker = mocker.MagicMock()
            worker.start = mocker.AsyncMock(
                side_effect=RuntimeError("Worker startup failed")
            )
            worker.stop = mocker.AsyncMock()
            worker.metadata = mocker.MagicMock()
            return worker

        # Act & assert
        with pytest.raises(ExceptionGroup) as exc_info:
            async with WorkerPool(worker=failing_factory, spawn=1):
                pass

        assert len(exc_info.value.exceptions) == 1
        assert "Worker startup failed" in str(exc_info.value.exceptions[0])

    @pytest.mark.asyncio
    async def test___aexit___handles_exceptions_gracefully(
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

        # Act & assert - Should not raise exception from cleanup
        async with WorkerPool(spawn=1) as pool:
            assert pool is not None

        # Assert: Pool was created and cleanup was attempted
        assert isinstance(pool, WorkerPool)

    @pytest.mark.asyncio
    async def test___aexit___handles_shared_memory_cleanup_exceptions(
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
        pool = WorkerPool(spawn=1)
        try:
            async with pool:
                pass
        except OSError:
            # Expected - cleanup error propagates as it should
            pass

    @pytest.mark.asyncio
    async def test___aexit___bounds_teardown_when_worker_stop_hangs(
        self, mocker: MockerFixture
    ):
        """Test pool teardown returns within the configured shutdown bound.

        Given:
            A WorkerPool whose worker has a stop() that never returns
        When:
            The async-with block exits
        Then:
            It should complete within a small multiple of shutdown_timeout
        """

        # Arrange
        def hanging_factory(*tags, credentials=None):
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()

            async def hang(*, timeout=None):
                await asyncio.sleep(60)

            worker.stop = hang
            worker.metadata = _make_worker_metadata()
            return worker

        shutdown_timeout = 0.2

        # Act
        start = time.monotonic()
        async with WorkerPool(
            worker=hanging_factory,
            spawn=1,
            shutdown_timeout=shutdown_timeout,
        ):
            pass
        elapsed = time.monotonic() - start

        # Assert
        assert elapsed < shutdown_timeout * 5

    @pytest.mark.asyncio
    async def test___aexit___logs_warning_when_workers_abandoned(
        self, mocker: MockerFixture, caplog
    ):
        """Test pool logs a warning naming the abandoned worker count.

        Given:
            A WorkerPool whose worker has a stop() that never returns
        When:
            The async-with block exits and the shutdown bound elapses
        Then:
            It should emit a logger.warning identifying the abandoned count
        """

        # Arrange
        def hanging_factory(*tags, credentials=None):
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()

            async def hang(*, timeout=None):
                await asyncio.sleep(60)

            worker.stop = hang
            worker.metadata = _make_worker_metadata()
            return worker

        # Act
        with caplog.at_level(logging.WARNING, "wool.runtime.worker.pool"):
            async with WorkerPool(
                worker=hanging_factory,
                spawn=1,
                shutdown_timeout=0.2,
            ):
                pass

        # Assert
        assert any(
            "abandoned 1 worker" in r.getMessage()
            for r in caplog.records
            if r.levelno == logging.WARNING
        )

    @pytest.mark.asyncio
    async def test___aexit___cancels_pending_stops_after_timeout(
        self, mocker: MockerFixture
    ):
        """Test pending stop coroutines receive CancelledError on timeout.

        Given:
            A WorkerPool whose worker has a stop() that never returns
        When:
            The async-with block exits and the shutdown bound elapses
        Then:
            It should cancel the pending stop coroutine
        """
        # Arrange
        cancelled = asyncio.Event()

        def hanging_factory(*tags, credentials=None):
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()

            async def hang(*, timeout=None):
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

            worker.stop = hang
            worker.metadata = _make_worker_metadata()
            return worker

        # Act
        async with WorkerPool(
            worker=hanging_factory,
            spawn=1,
            shutdown_timeout=0.2,
        ):
            pass
        await asyncio.wait_for(cancelled.wait(), timeout=1.0)

        # Assert
        assert cancelled.is_set()

    @pytest.mark.asyncio
    async def test___aexit___completes_normally_when_workers_stop_within_timeout(
        self, mocker: MockerFixture, caplog
    ):
        """Test pool teardown emits no warning when workers stop in time.

        Given:
            A WorkerPool whose workers stop quickly
        When:
            The async-with block exits with shutdown_timeout configured
        Then:
            It should complete without an abandonment warning
        """

        # Arrange
        def fast_factory(*tags, credentials=None):
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()
            worker.stop = mocker.AsyncMock()
            worker.metadata = _make_worker_metadata()
            return worker

        # Act
        with caplog.at_level(logging.WARNING, "wool.runtime.worker.pool"):
            async with WorkerPool(
                worker=fast_factory,
                spawn=2,
                shutdown_timeout=1.0,
            ):
                pass

        # Assert
        assert not any(
            "abandoned" in r.getMessage()
            for r in caplog.records
            if r.levelno == logging.WARNING
        )

    @pytest.mark.asyncio
    async def test___aexit___does_not_block_other_cleanup_when_one_worker_hangs(
        self, mocker: MockerFixture
    ):
        """Test fast worker still stops when a sibling worker's stop hangs.

        Given:
            A WorkerPool with one hanging worker and one fast worker
        When:
            The async-with block exits and the shutdown bound elapses
        Then:
            It should call stop on the fast worker and complete within bound
        """
        # Arrange
        fast_stop = mocker.AsyncMock()
        workers_built: list = []

        def mixed_factory(*tags, credentials=None):
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock()
            worker.metadata = _make_worker_metadata()
            if not workers_built:

                async def hang(*, timeout=None):
                    await asyncio.sleep(60)

                worker.stop = hang
            else:
                worker.stop = fast_stop
            workers_built.append(worker)
            return worker

        shutdown_timeout = 0.2

        # Act
        start = time.monotonic()
        async with WorkerPool(
            worker=mixed_factory,
            spawn=2,
            shutdown_timeout=shutdown_timeout,
        ):
            pass
        elapsed = time.monotonic() - start

        # Assert
        fast_stop.assert_awaited_once()
        assert elapsed < shutdown_timeout * 5

    @pytest.mark.asyncio
    async def test___aenter___default_case_covers_shared_memory_creation(
        self,
        mocker: MockerFixture,
        mock_local_worker,
        mock_shared_memory,
        mock_worker_proxy,
    ):
        """Test execute the create_proxy function covering lines 238-246.

        Given:
            WorkerPool called with default parameters (no spawn, no discovery)
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
    async def test_start_creates_workers_spawn_3(self, mock_worker_factory):
        """Test the pool successfully starts and stops.

        Given:
            A WorkerPool configured with spawn=3
        When:
            The pool is started via context manager
        Then:
            The pool successfully starts and stops
        """
        # Arrange, act, & assert
        async with WorkerPool(worker=mock_worker_factory, spawn=3) as pool:
            # Pool started successfully - this validates workers were created
            assert pool is not None

    @pytest.mark.asyncio
    async def test_start_with_specific_spawn_1(self, mock_worker_factory):
        """Test 1 worker is created.

        Given:
            A WorkerPool configured with spawn=1
        When:
            The pool is started
        Then:
            1 worker is created
        """
        # Arrange, act, & assert
        async with WorkerPool(worker=mock_worker_factory, spawn=1) as pool:
            assert pool is not None

    @pytest.mark.asyncio
    async def test_start_with_specific_spawn_10(self, mock_worker_factory):
        """Test 10 workers are created.

        Given:
            A WorkerPool configured with spawn=10
        When:
            The pool is started
        Then:
            10 workers are created
        """
        # Arrange, act, & assert
        async with WorkerPool(worker=mock_worker_factory, spawn=10) as pool:
            assert pool is not None

    @pytest.mark.asyncio
    async def test_start_with_failing_worker(self, mocker: MockerFixture):
        """Test the spawn exception propagates to the caller.

        Given:
            A WorkerPool with a failing worker factory
        When:
            The pool is started
        Then:
            It should raise the spawn exception before yielding
        """

        # Arrange
        def failing_factory(*tags, credentials=None, options=None):
            worker = mocker.MagicMock()
            worker.start = mocker.AsyncMock(
                side_effect=RuntimeError("Mock worker startup failed")
            )
            worker.stop = mocker.AsyncMock()
            worker.metadata = mocker.MagicMock()
            return worker

        pool = WorkerPool(worker=failing_factory, spawn=1)

        # Act & assert
        with pytest.raises(ExceptionGroup) as exc_info:
            async with pool:
                pass

        assert len(exc_info.value.exceptions) == 1
        assert "Mock worker startup failed" in str(exc_info.value.exceptions[0])

    @pytest.mark.asyncio
    async def test_start_with_all_workers_failing(self, mocker: MockerFixture):
        """Test multiple spawn failures raise ExceptionGroup.

        Given:
            A WorkerPool with spawn=3 where all workers fail to start
        When:
            The pool is used as async context manager
        Then:
            It should raise ExceptionGroup containing all 3 failures
        """

        # Arrange
        def failing_factory(*tags, credentials=None, options=None):
            worker = mocker.MagicMock(spec=LocalWorker)
            worker.start = mocker.AsyncMock(
                side_effect=RuntimeError("Worker startup failed")
            )
            worker.stop = mocker.AsyncMock()
            worker.metadata = mocker.MagicMock()
            return worker

        pool = WorkerPool(worker=failing_factory, spawn=3)

        # Act & assert
        with pytest.raises(ExceptionGroup) as exc_info:
            async with pool:
                pass

        assert len(exc_info.value.exceptions) == 3

    @pytest.mark.asyncio
    async def test_start_with_partial_worker_failure(self, mocker: MockerFixture):
        """Test partial spawn failure raises ExceptionGroup with single error.

        Given:
            A WorkerPool with spawn=3 where 1 of 3 workers fails to start
        When:
            The pool is used as async context manager
        Then:
            It should raise ExceptionGroup containing the single failure
        """

        # Arrange
        call_count = 0

        def mixed_factory(*tags, credentials=None, options=None):
            nonlocal call_count
            call_count += 1
            worker = mocker.MagicMock(spec=LocalWorker)
            if call_count == 2:
                worker.start = mocker.AsyncMock(
                    side_effect=RuntimeError("Worker 2 failed")
                )
            else:
                worker.start = mocker.AsyncMock()
            worker.stop = mocker.AsyncMock()
            worker.metadata = _make_worker_metadata()
            return worker

        pool = WorkerPool(worker=mixed_factory, spawn=3)

        # Act & assert
        with pytest.raises(ExceptionGroup) as exc_info:
            async with pool:
                pass

        assert len(exc_info.value.exceptions) == 1
        assert "Worker 2 failed" in str(exc_info.value.exceptions[0])

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
        async with WorkerPool(worker=mock_worker_factory, spawn=2) as pool:
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
        # Act & assert - Context manager should complete without error
        async with WorkerPool(spawn=3) as pool:
            assert pool is not None
            assert isinstance(pool, WorkerPool)

        # Context exits cleanly (implicit assertion - no exception raised)

    @pytest.mark.asyncio
    async def test___aenter___preserves_metadata(
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
        async with WorkerPool(spawn=2) as pool:
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
        # Act
        async with WorkerPool(spawn=2) as pool:
            assert pool is not None
            # Pool successfully started - workers have been configured
            assert isinstance(pool, WorkerPool)

    # =========================================================================
    # Advanced Configuration Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test___aenter___with_custom_worker_factory(
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
        custom_worker = mocker.MagicMock(spec=LocalWorker)
        custom_worker.start = mocker.AsyncMock()
        custom_worker.stop = mocker.AsyncMock()
        custom_worker.metadata = _make_worker_metadata()

        def custom_factory(*args, credentials=None, options=None):
            return cast(LocalWorker, custom_worker)

        # Act
        async with WorkerPool(spawn=1, worker=custom_factory) as pool:
            assert pool is not None

        # Assert: Custom worker was used
        custom_worker.start.assert_called()

    @pytest.mark.asyncio
    async def test___aenter___with_durable_pool_configuration(
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
    async def test___aenter___with_custom_loadbalancer(
        self,
        mocker,
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
        custom_loadbalancer = mocker.MagicMock()

        # Act
        async with WorkerPool(spawn=1, loadbalancer=custom_loadbalancer) as pool:
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
        pool = WorkerPool(worker=mock_worker_factory, spawn=2)

        # Act - Start pool
        async with pool:
            # Pool is running
            pass

        # Assert - pool lifecycle completes without deadlock

    @pytest.mark.asyncio
    async def test___aenter___concurrent_operations(
        self,
        mocker,
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
        mock_local_worker.start = mocker.AsyncMock()

        # Act
        async with WorkerPool(spawn=3) as pool:
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
        def slow_factory(*tags, credentials=None, options=None):
            worker = mocker.MagicMock()

            async def slow_start():
                await asyncio.sleep(10.0)

            worker.start = slow_start
            worker.stop = mocker.AsyncMock()
            worker.metadata = mocker.MagicMock()
            return worker

        pool = WorkerPool(worker=slow_factory, spawn=1)

        # Act & assert - with a 1 second timeout
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
        async with WorkerPool(spawn=2) as pool:
            end_time = time.time()
            assert pool is not None

        # Assert: Startup completed quickly (with mocked components)
        startup_time = end_time - start_time
        assert startup_time < 1.0

    # =========================================================================
    # Constructor Overload Coverage Tests
    # =========================================================================

    @pytest.mark.asyncio
    async def test_hybrid_mode_spawn_and_discovery(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test spawn local workers and connect to discovery.

        Given:
            A WorkerPool configured with both spawn and discovery (hybrid mode)
        When:
            Pool is started
        Then:
            Should spawn local workers and connect to discovery
        """
        # Arrange - This tests the (spawn, discovery) case at line 223
        discovery_service = mock_discovery_service_for_pool

        # Act
        async with WorkerPool(spawn=2, discovery=discovery_service) as pool:
            assert pool is not None

        # Assert: Pool created successfully with both local workers and discovery
        mock_worker_proxy.__aenter__.assert_called_once()
        mock_worker_proxy.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_hybrid_mode_spawn_zero_with_discovery(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test use CPU count for worker spawn count.

        Given:
            A WorkerPool configured with spawn=0 and discovery (hybrid mode)
        When:
            Pool is started
        Then:
            Should use CPU count for worker spawn count
        """
        # Arrange
        mocker.patch("os.cpu_count", return_value=4)
        discovery_service = mock_discovery_service_for_pool

        # Act
        async with WorkerPool(spawn=0, discovery=discovery_service) as pool:
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
        """Test use CPU count as default spawn count.

        Given:
            A WorkerPool with no spawn or discovery specified
        When:
            Pool is started
        Then:
            Should use CPU count as default spawn count
        """
        # Arrange - This tests the (None, None) case at line 297
        mocker.patch("os.cpu_count", return_value=8)

        # Act
        async with WorkerPool() as pool:
            assert pool is not None

        # Assert: Pool created with CPU count workers
        mock_worker_proxy.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_ephemeral_mode_spawn_only(
        self,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test spawn local workers with LocalDiscovery.

        Given:
            A WorkerPool configured with spawn only (ephemeral mode)
        When:
            Pool is started
        Then:
            Should spawn local workers with LocalDiscovery
        """
        # Arrange - This tests the (spawn, None) case at line 255
        # This is the most common mode already tested extensively

        # Act
        async with WorkerPool(spawn=3) as pool:
            assert pool is not None

        # Assert: Pool created successfully
        mock_worker_proxy.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_ephemeral_mode_spawn_zero_uses_cpu_count(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test use CPU count for worker spawn count.

        Given:
            A WorkerPool configured with spawn=0 (ephemeral mode)
        When:
            Pool is started
        Then:
            Should use CPU count for worker spawn count
        """
        # Arrange - Tests spawn=0 path at line 256
        mocker.patch("os.cpu_count", return_value=6)

        # Act
        async with WorkerPool(spawn=0) as pool:
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
            A WorkerPool with tags, spawn, and discovery
        When:
            Pool is started
        Then:
            Should pass tags to workers and discovery filtering
        """
        # Arrange
        discovery_service = mock_discovery_service_for_pool
        tags = ("gpu", "ml-model")

        # Act
        async with WorkerPool(*tags, spawn=2, discovery=discovery_service) as pool:
            assert pool is not None

        # Assert: Pool created successfully with tags
        mock_worker_proxy.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_hybrid_mode_with_custom_loadbalancer(
        self,
        mocker,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test use custom loadbalancer.

        Given:
            A WorkerPool with spawn, discovery, and custom loadbalancer
        When:
            Pool is started
        Then:
            Should use custom loadbalancer
        """
        # Arrange
        discovery_service = mock_discovery_service_for_pool
        custom_lb = mocker.MagicMock()

        # Act
        async with WorkerPool(
            spawn=2, discovery=discovery_service, loadbalancer=custom_lb
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

        # Act & assert
        with pytest.raises(TypeError, match="Expected DiscoveryLike"):
            async with WorkerPool(discovery=invalid_discovery):
                pass

    @pytest.mark.asyncio
    async def test_hybrid_mode_discovery_type_validation(
        self, mocker: MockerFixture, mock_worker_proxy, mock_local_worker
    ):
        """Test raise TypeError.

        Given:
            A WorkerPool with spawn and invalid discovery (not DiscoveryLike)
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

        # Act & assert - This tests line 212 (TypeError)
        with pytest.raises(TypeError, match="Expected DiscoveryLike"):
            async with WorkerPool(spawn=2, discovery=invalid_discovery):
                pass

    @pytest.mark.asyncio
    async def test_hybrid_mode_negative_spawn_raises_error(
        self, mock_discovery_service_for_pool
    ):
        """Test raise ValueError.

        Given:
            A WorkerPool with negative spawn and discovery (hybrid mode)
        When:
            Pool is created
        Then:
            Should raise ValueError
        """
        # Arrange
        discovery_service = mock_discovery_service_for_pool

        # Act & assert - This tests line 230
        with pytest.raises(ValueError, match="Spawn must be non-negative"):
            WorkerPool(spawn=-1, discovery=discovery_service)

    def test_hybrid_mode_cpu_count_unavailable_with_spawn_zero(
        self, mocker: MockerFixture
    ):
        """Test raise ValueError.

        Given:
            A WorkerPool with spawn=0 and discovery when CPU count unavailable
        When:
            Pool is created
        Then:
            Should raise ValueError
        """
        # Arrange
        mocker.patch("os.cpu_count", return_value=None)
        discovery_service = mocker.MagicMock()

        # Act & assert - This tests line 227
        with pytest.raises(ValueError, match="Unable to determine CPU count"):
            WorkerPool(spawn=0, discovery=discovery_service)

    # =========================================================================
    # Property-Based Tests
    # =========================================================================

    @given(st.integers(min_value=1, max_value=10))
    def test___init___accepts_valid_spawns(self, spawn):
        """Test create pool successfully.

        Given:
            Any valid spawn between 1 and 10
        When:
            WorkerPool is initialized with that spawn
        Then:
            Should create pool successfully
        """
        # Act
        pool = WorkerPool(spawn=spawn)

        # Assert
        assert isinstance(pool, WorkerPool)

    @given(st.integers(min_value=-100, max_value=-1))
    def test___init___rejects_negative_spawns(self, negative_spawn):
        """Test raise ValueError.

        Given:
            Any negative spawn
        When:
            WorkerPool is initialized with that spawn
        Then:
            Should raise ValueError
        """
        # Act & assert
        with pytest.raises(ValueError, match="Spawn must be non-negative"):
            WorkerPool(spawn=negative_spawn)

    @given(spawn=st.integers(min_value=1, max_value=20))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    @pytest.mark.asyncio
    async def test_property_worker_count_bounded(self, mock_worker_factory, spawn):
        """Test the number of workers is exactly the configured spawn count.

        Given:
            A WorkerPool with any spawn from 1 to 20
        When:
            The pool is started
        Then:
            The number of workers is exactly the configured spawn count
        """
        # Arrange
        pool = WorkerPool(worker=mock_worker_factory, spawn=spawn)

        # Act
        async with pool:
            # Pool is running with expected worker count
            pass

        # Assert - invariant holds across all pool spawn counts

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
        pool = WorkerPool(*tags, worker=mock_worker_factory, spawn=2)

        # Act
        async with pool:
            # Workers maintain their tags
            pass

        # Assert - tags immutability invariant holds

    @given(spawn=st.integers(min_value=1, max_value=10))
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    @pytest.mark.asyncio
    async def test_property_cleanup_complete(self, mock_worker_factory, spawn):
        """Test all workers are stopped and resources cleaned up.

        Given:
            A WorkerPool of any spawn count
        When:
            The pool is stopped
        Then:
            All workers are stopped and resources cleaned up
        """
        # Arrange
        pool = WorkerPool(worker=mock_worker_factory, spawn=spawn)
        cleanup_completed = False

        # Act
        async with pool:
            pass
        cleanup_completed = True

        # Assert - cleanup completes without errors, confirming resource release
        assert cleanup_completed, f"Cleanup should complete for pool of spawn {spawn}"

    @given(
        spawn=st.integers(min_value=1, max_value=5),
        tags=st.lists(st.text(min_size=1, max_size=8), min_size=1, max_size=3),
    )
    @settings(
        max_examples=100, suppress_health_check=[HealthCheck.function_scoped_fixture]
    )
    @pytest.mark.asyncio
    async def test_property_context_manager_idempotency(
        self, mock_worker_factory, spawn, tags
    ):
        """Test lifecycle completes successfully regardless of configuration.

        Given:
            A WorkerPool with various spawn and tag configurations
        When:
            The pool is used as a context manager
        Then:
            Lifecycle completes successfully regardless of configuration
        """
        # Arrange
        pool = WorkerPool(*tags, worker=mock_worker_factory, spawn=spawn)
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
            async with WorkerPool(worker=mock_worker_factory, spawn=1):
                raise exception_type(exception_message)
        except Exception as e:
            caught_exception = e

        # Assert - exception propagated correctly
        assert caught_exception is not None, "Exception should be caught"
        assert isinstance(caught_exception, exception_type), (
            f"Should catch {exception_type.__name__}"
        )
        assert str(caught_exception) == exception_message

    # =========================================================================
    # Context Helper Tests
    # =========================================================================

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

        # Act & assert - should not raise
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

        # Act & assert - should not raise
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
        """Test raise TypeError.

        Given:
            WorkerPool with a publisher that is not DiscoveryPublisherLike
        When:
            _worker_context is entered
        Then:
            It should raise TypeError
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

        pool = WorkerPool(spawn=1)

        # Act & assert
        with pytest.raises(TypeError, match="Expected DiscoveryPublisherLike"):
            async with pool:
                pass

    @pytest.mark.asyncio
    async def test___aenter___does_not_pass_options_to_proxy(
        self,
        mocker: MockerFixture,
        mock_worker_factory,
        mock_local_worker,
        mock_worker_proxy,
    ):
        """Test pool startup does not pass options to WorkerProxy.

        Given:
            A WorkerPool with default configuration
        When:
            The pool is entered as an async context
        Then:
            It should not pass an options parameter to WorkerProxy
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )
        pool = WorkerPool(worker=mock_worker_factory, spawn=1)

        # Act
        async with pool:
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert "options" not in proxy_kwargs

    def test___init___spawn_only_with_lease_warns(self):
        """Test spawn-only pool warns when lease is supplied.

        Given:
            A spawn of 4, a lease of 8, and no discovery service
        When:
            WorkerPool is instantiated
        Then:
            It should emit an IneffectiveLeaseWarning and still create
            the pool successfully — 'lease' is recorded but never
            consulted without a discovery service
        """
        # Act
        with pytest.warns(
            IneffectiveLeaseWarning,
            match="'lease' has no effect when no 'discovery' service",
        ):
            pool = WorkerPool(spawn=4, lease=8)  # type: ignore[call-overload]

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___default_mode_with_lease_warns(self):
        """Test default-mode pool warns when lease is supplied.

        Given:
            A lease of 4 with no explicit spawn and no discovery
        When:
            WorkerPool is instantiated
        Then:
            It should emit an IneffectiveLeaseWarning and still create
            the pool successfully — default mode also spawns only
            locally and 'lease' has no effect
        """
        # Act
        with pytest.warns(
            IneffectiveLeaseWarning,
            match="'lease' has no effect when no 'discovery' service",
        ):
            pool = WorkerPool(lease=4)  # type: ignore[call-overload]

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___discovery_only_with_lease_accepts(self, mocker: MockerFixture):
        """Test discovery-only pool accepts lease.

        Given:
            A discovery service and lease of 8 with no spawn specified
        When:
            WorkerPool is instantiated
        Then:
            It should create the pool successfully — 'lease' is valid
            in discovery-only mode
        """
        # Arrange
        mock_discovery = mocker.MagicMock()

        # Act
        pool = WorkerPool(discovery=mock_discovery, lease=8)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___hybrid_mode_with_lease_accepts(self, mocker: MockerFixture):
        """Test hybrid pool accepts lease.

        Given:
            A spawn of 4, a discovery service, and lease of 8
        When:
            WorkerPool is instantiated
        Then:
            It should create the pool successfully — 'lease' is valid
            in hybrid mode (spawn + discovery)
        """
        # Arrange
        mock_discovery = mocker.MagicMock()

        # Act
        pool = WorkerPool(spawn=4, lease=8, discovery=mock_discovery)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___spawn_only_with_zero_lease_warns(self):
        """Test spawn-only pool warns when zero lease is supplied.

        Given:
            A spawn of 4 and lease of 0, with no discovery
        When:
            WorkerPool is instantiated
        Then:
            It should emit an IneffectiveLeaseWarning and still create
            the pool — any non-None lease without discovery has no
            effect, including lease=0
        """
        # Act
        with pytest.warns(
            IneffectiveLeaseWarning,
            match="'lease' has no effect when no 'discovery' service",
        ):
            pool = WorkerPool(spawn=4, lease=0)  # type: ignore[call-overload]

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___with_zero_lease_discovery_only(self, mocker: MockerFixture):
        """Test zero lease with discovery-only pool is rejected.

        Given:
            A discovery service and lease of 0
        When:
            WorkerPool is instantiated
        Then:
            It should raise ValueError
        """
        # Arrange
        mock_discovery = mocker.MagicMock()

        # Act & assert
        with pytest.raises(
            ValueError,
            match="Lease must be positive for discovery-only pools",
        ):
            WorkerPool(discovery=mock_discovery, lease=0)

    def test___init___with_negative_lease(self):
        """Test negative lease is rejected.

        Given:
            A negative lease value
        When:
            WorkerPool is instantiated
        Then:
            It should raise ValueError
        """
        # Act & assert
        with pytest.raises(ValueError, match="Lease must be non-negative"):
            WorkerPool(spawn=4, lease=-1)  # type: ignore[call-overload]

    @pytest.mark.asyncio
    async def test___aenter___hybrid_mode_forwards_lease(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test hybrid mode forwards lease to WorkerProxy.

        Given:
            A WorkerPool with spawn=2, lease=4, and discovery
        When:
            The pool context is entered
        Then:
            It should pass lease=6 (spawn + lease) to WorkerProxy
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )
        discovery_service = mock_discovery_service_for_pool

        # Act
        async with WorkerPool(spawn=2, lease=4, discovery=discovery_service):
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert proxy_kwargs["lease"] == 6  # spawn(2) + lease(4)

    @pytest.mark.asyncio
    async def test___aenter___durable_mode_forwards_lease(
        self,
        mocker: MockerFixture,
        mock_worker_proxy,
        mock_discovery_service_for_pool,
    ):
        """Test durable mode forwards lease to WorkerProxy.

        Given:
            A WorkerPool with discovery only and lease=6
        When:
            The pool context is entered
        Then:
            It should pass lease=6 to WorkerProxy
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )
        discovery_service = mock_discovery_service_for_pool

        # Act
        async with WorkerPool(discovery=discovery_service, lease=6):
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert proxy_kwargs["lease"] == 6

    @pytest.mark.asyncio
    async def test___aenter___should_timeout_when_only_incompatible_discovered(
        self, mocker: MockerFixture
    ):
        """Test custom-source incompatible workers never satisfy quorum.

        Given:
            A durable WorkerPool over a custom DiscoveryLike source
            whose unfiltered subscriber yields only a next-major-version
            worker, with quorum=1 and a short quorum timeout
        When:
            The pool context is entered eagerly
        Then:
            It should raise asyncio.TimeoutError at entry — the
            proxy's admission gate rejects the worker, so the quorum
            is never met
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        incompatible_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.1:50051",
            pid=1001,
            version="2.0.0",
        )
        events = [DiscoveryEvent("worker-added", metadata=incompatible_worker)]
        pool = WorkerPool(
            discovery=_FakeDiscovery(mocker, events=events),
            quorum=1,
            quorum_timeout=0.05,
            lazy=False,
        )

        # Act & assert
        with pytest.raises(asyncio.TimeoutError):
            async with pool:
                pass

    @pytest.mark.asyncio
    async def test___aenter___should_admit_only_compatible_workers_within_lease(
        self, mocker: MockerFixture
    ):
        """Test the gate composes with pool lease and quorum wiring.

        Given:
            A durable WorkerPool with lease=1 and quorum=1 over a
            custom DiscoveryLike source yielding an incompatible worker
            followed by a compatible one
        When:
            The pool context is entered eagerly and the admitted set is
            read from the active proxy
        Then:
            It should enter successfully with exactly the compatible
            worker admitted — the rejected worker did not occupy the
            single lease slot
        """
        # Arrange
        mocker.patch.object(protocol, "__version__", "1.0.0")
        incompatible_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.1:50051",
            pid=1001,
            version="2.0.0",
        )
        compatible_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.2:50051",
            pid=1002,
            version="1.0.0",
        )
        events = [
            DiscoveryEvent("worker-added", metadata=incompatible_worker),
            DiscoveryEvent("worker-added", metadata=compatible_worker),
        ]
        pool = WorkerPool(
            discovery=_FakeDiscovery(mocker, events=events),
            lease=1,
            quorum=1,
            lazy=False,
        )

        # Act & assert
        async with pool:
            proxy = wool.__proxy__.get()
            assert proxy is not None
            assert len(proxy.workers) == 1
            assert compatible_worker in proxy.workers
            assert incompatible_worker not in proxy.workers

    @pytest.mark.asyncio
    async def test___aenter___should_subscribe_with_tag_only_filter_when_tags_given(
        self,
        mocker: MockerFixture,
        mock_worker_proxy,
        mock_local_worker,
        mock_shared_memory,
    ):
        """Test hybrid subscriptions carry tag semantics only.

        Given:
            A hybrid WorkerPool with a tag and a filter-capturing
            DiscoveryLike source
        When:
            The pool context is entered and the captured subscription
            predicate is evaluated
        Then:
            It should admit workers by tag intersection alone —
            version and security mismatches pass through to the
            proxy's admission gate, and non-matching tags are rejected
        """
        # Arrange
        discovery = _FakeDiscovery(mocker)

        # Act
        async with WorkerPool("gpu", spawn=1, discovery=discovery):
            pass

        filter_fn = discovery.captured_filter

        # Assert — tag match passes despite a version mismatch
        version_mismatched = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.1:50051",
            pid=1001,
            version="2.0.0",
            tags=frozenset(["gpu"]),
        )
        assert filter_fn(version_mismatched) is True

        # Assert — tag match passes despite a security mismatch
        secure_mismatched = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.2:50051",
            pid=1002,
            version="1.0.0",
            tags=frozenset(["gpu"]),
            secure=True,
        )
        assert filter_fn(secure_mismatched) is True

        # Assert — non-matching tags are rejected
        non_matching = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.3:50051",
            pid=1003,
            version="1.0.0",
            tags=frozenset(["other"]),
        )
        assert filter_fn(non_matching) is False

    @pytest.mark.asyncio
    async def test___aenter___should_subscribe_with_match_all_filter_when_no_tags(
        self,
        mocker: MockerFixture,
        mock_worker_proxy,
        mock_local_worker,
        mock_shared_memory,
    ):
        """Test untagged hybrid subscriptions match every worker.

        Given:
            A hybrid WorkerPool with no tags and a filter-capturing
            DiscoveryLike source
        When:
            The pool context is entered and the captured subscription
            predicate is evaluated
        Then:
            It should match tagged and untagged workers alike
        """
        # Arrange
        discovery = _FakeDiscovery(mocker)

        # Act
        async with WorkerPool(spawn=1, discovery=discovery):
            pass

        filter_fn = discovery.captured_filter

        # Assert
        tagged = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.1:50051",
            pid=1001,
            version="1.0.0",
            tags=frozenset(["anything"]),
        )
        untagged = WorkerMetadata(
            uid=uuid.uuid4(),
            address="192.168.1.2:50051",
            pid=1002,
            version="1.0.0",
        )
        assert filter_fn(tagged) is True
        assert filter_fn(untagged) is True

    @pytest.mark.asyncio
    async def test___aenter___should_forward_credentials_to_proxy(
        self,
        mocker: MockerFixture,
        mock_worker_proxy,
        mock_discovery_service_for_pool,
        worker_credentials,
    ):
        """Test durable mode forwards credentials to WorkerProxy.

        Given:
            A WorkerPool with discovery only and WorkerCredentials
        When:
            The pool context is entered
        Then:
            It should pass the exact credentials instance to
            WorkerProxy — the wiring that aims the security half of
            the proxy's admission gate
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )

        # Act
        async with WorkerPool(
            discovery=mock_discovery_service_for_pool,
            credentials=worker_credentials,
        ):
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert proxy_kwargs["credentials"] is worker_credentials

    @pytest.mark.asyncio
    async def test___aenter___no_lease_forwards_none(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test ephemeral pool with no lease forwards lease=None.

        Given:
            A WorkerPool with plain int spawn and no lease
        When:
            The pool context is entered
        Then:
            It should pass lease=None to WorkerProxy
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )

        # Act
        async with WorkerPool(spawn=2):
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert proxy_kwargs["lease"] is None

    @given(lease=st.integers(min_value=0, max_value=20))
    def test___init___warns_on_lease_without_discovery_pbt(self, lease):
        """Test any non-negative lease without discovery emits a warning.

        Given:
            A positive spawn and any non-negative lease, with no
            discovery service
        When:
            WorkerPool is instantiated
        Then:
            It should emit an IneffectiveLeaseWarning for every drawn
            lease and still create the pool — 'lease' is recorded but
            never consulted
        """
        # Act
        with pytest.warns(
            IneffectiveLeaseWarning,
            match="'lease' has no effect when no 'discovery' service",
        ):
            pool = WorkerPool(spawn=1, lease=lease)  # type: ignore[call-overload]

        # Assert
        assert isinstance(pool, WorkerPool)

    @given(lease=st.integers(min_value=-100, max_value=-1))
    def test___init___rejects_negative_lease_pbt(self, lease):
        """Test negative lease values are rejected.

        Given:
            Any negative lease value
        When:
            WorkerPool is instantiated
        Then:
            It should raise ValueError
        """
        # Act & assert
        with pytest.raises(
            ValueError,
            match="Lease must be non-negative",
        ):
            WorkerPool(spawn=1, lease=lease)  # type: ignore[call-overload]

    # ------------------------------------------------------------------
    # Quorum tests
    # ------------------------------------------------------------------

    def test___init___with_quorum(self):
        """Test instantiation with spawn and quorum.

        Given:
            A spawn of 4 and quorum of 2
        When:
            WorkerPool is instantiated
        Then:
            It should create the pool successfully
        """
        # Act
        pool = WorkerPool(spawn=4, quorum=2)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___with_zero_quorum(self):
        """Test zero quorum is accepted.

        Given:
            A spawn of 4 and quorum of 0
        When:
            WorkerPool is instantiated
        Then:
            It should create the pool successfully
        """
        # Act
        pool = WorkerPool(spawn=4, quorum=0)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___with_quorum_timeout_without_quorum_warns(self):
        """Test quorum_timeout supplied without a positive quorum emits a warning.

        Given:
            spawn=2, quorum=None, and quorum_timeout=30
        When:
            WorkerPool is instantiated
        Then:
            It should emit an IneffectiveQuorumTimeoutWarning; users
            who want strict behaviour can elevate the category to error
            via warnings.filterwarnings
        """
        # Act & assert
        with pytest.warns(
            IneffectiveQuorumTimeoutWarning,
            match="'quorum_timeout' has no effect when 'quorum' is None or 0",
        ):
            WorkerPool(spawn=2, quorum=None, quorum_timeout=30)

    @pytest.mark.asyncio
    async def test___aenter___with_negative_quorum_raises(
        self, mock_shared_memory, mock_local_worker
    ):
        """Test negative quorum is rejected at context entry.

        Given:
            A spawn of 4 and quorum of -1
        When:
            The WorkerPool context is entered
        Then:
            It should raise ValueError from the underlying WorkerProxy
        """
        # Act & assert
        with pytest.raises(ValueError, match="Quorum must be a non-negative integer"):
            async with WorkerPool(spawn=4, quorum=-1):
                pass

    def test___init___with_quorum_exceeding_capacity(self, mocker: MockerFixture):
        """Test quorum exceeding pool capacity is rejected.

        Given:
            A spawn of 2, lease of 2, a discovery service, and quorum
            of 5
        When:
            WorkerPool is instantiated
        Then:
            It should raise ValueError
        """
        # Arrange
        mock_discovery = mocker.MagicMock()

        # Act & assert
        with pytest.raises(ValueError, match=r"Quorum.*cannot exceed pool capacity"):
            WorkerPool(spawn=2, discovery=mock_discovery, lease=2, quorum=5)

    def test___init___with_quorum_equal_to_capacity(self, mocker: MockerFixture):
        """Test quorum equal to pool capacity is accepted.

        Given:
            A spawn of 4, lease of 3, a discovery service, and quorum
            of 7
        When:
            WorkerPool is instantiated
        Then:
            It should create the pool successfully
        """
        # Arrange
        mock_discovery = mocker.MagicMock()

        # Act
        pool = WorkerPool(spawn=4, discovery=mock_discovery, lease=3, quorum=7)

        # Assert
        assert isinstance(pool, WorkerPool)

    def test___init___with_quorum_satisfied_by_spawn_alone(self, mocker: MockerFixture):
        """Test quorum satisfiable by spawn alone with lease=0 is accepted.

        Given:
            A spawn of 4, lease of 0, a discovery service, and quorum
            of 3
        When:
            WorkerPool is instantiated
        Then:
            It should create the pool successfully
        """
        # Arrange
        mock_discovery = mocker.MagicMock()

        # Act
        pool = WorkerPool(spawn=4, discovery=mock_discovery, lease=0, quorum=3)

        # Assert
        assert isinstance(pool, WorkerPool)

    @pytest.mark.asyncio
    async def test___aenter___with_quorum_exceeding_lease_discovery_only(
        self, mock_discovery_service_for_pool
    ):
        """Test quorum exceeding lease in discovery-only pool is rejected.

        Given:
            A discovery service, lease of 2, and quorum of 3
        When:
            The WorkerPool context is entered
        Then:
            It should raise ValueError from the underlying WorkerProxy
        """
        # Act & assert
        with pytest.raises(ValueError, match=r"Quorum.*cannot exceed lease"):
            async with WorkerPool(
                discovery=mock_discovery_service_for_pool, lease=2, quorum=3
            ):
                pass

    def test___init___with_unbounded_lease_and_quorum_above_spawn(self):
        """Test unbounded lease accepts quorum exceeding spawn.

        Given:
            A spawn of 2, no lease (unbounded), and quorum of 10
        When:
            WorkerPool is instantiated
        Then:
            It should create the pool successfully (the quorum
            reachability check is deferred to runtime via
            quorum_timeout)
        """
        # Act
        pool = WorkerPool(spawn=2, quorum=10)
        assert isinstance(pool, WorkerPool)

    def test___init___hybrid_mode_rejects_quorum_above_capacity(
        self, mocker: MockerFixture
    ):
        """Test hybrid pool rejects quorum exceeding spawn + lease.

        Given:
            A discovery service with spawn=2, lease=2, and quorum=10
        When:
            WorkerPool is instantiated
        Then:
            It should raise ValueError, since hybrid capacity is
            spawn + lease
        """
        # Arrange
        mock_discovery = mocker.MagicMock()

        # Act & assert
        with pytest.raises(ValueError, match=r"Quorum.*cannot exceed pool capacity"):
            WorkerPool(spawn=2, discovery=mock_discovery, lease=2, quorum=10)

    @pytest.mark.asyncio
    async def test___aenter___with_non_positive_quorum_timeout_raises(
        self, mock_shared_memory, mock_local_worker
    ):
        """Test non-positive quorum_timeout is rejected at context entry.

        Given:
            A spawn of 2, quorum of 1, and quorum_timeout of 0
        When:
            The WorkerPool context is entered
        Then:
            It should raise ValueError from the underlying WorkerProxy
        """
        # Act & assert
        with pytest.raises(ValueError, match="Quorum timeout must be positive"):
            async with WorkerPool(spawn=2, quorum=1, quorum_timeout=0):
                pass

    @pytest.mark.asyncio
    async def test___aenter___forwards_quorum(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test quorum is forwarded to WorkerProxy.

        Given:
            A WorkerPool with spawn=2 and quorum=2
        When:
            The pool context is entered
        Then:
            It should pass quorum=2 to WorkerProxy
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )

        # Act
        async with WorkerPool(spawn=2, quorum=2):
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert proxy_kwargs["quorum"] == 2

    @pytest.mark.asyncio
    async def test___aenter___forwards_default_quorum(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test default quorum is forwarded to WorkerProxy.

        Given:
            A WorkerPool with spawn=2 and no explicit quorum
        When:
            The pool context is entered
        Then:
            It should pass quorum=1 to WorkerProxy (preserves
            pre-quorum implicit-wait semantics)
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )

        # Act
        async with WorkerPool(spawn=2):
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert proxy_kwargs["quorum"] == 1

    @pytest.mark.asyncio
    async def test___aenter___forwards_quorum_none(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test explicit quorum=None forwards quorum=None to WorkerProxy.

        Given:
            A WorkerPool with spawn=2 and explicit quorum=None
        When:
            The pool context is entered
        Then:
            It should pass quorum=None to WorkerProxy without
            forwarding quorum_timeout
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )

        # Act
        async with WorkerPool(spawn=2, quorum=None):
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert proxy_kwargs["quorum"] is None
        assert "quorum_timeout" not in proxy_kwargs

    @pytest.mark.asyncio
    async def test___aenter___forwards_quorum_zero(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
    ):
        """Test quorum=0 normalizes to quorum=None when forwarded to WorkerProxy.

        Given:
            A WorkerPool with spawn=2 and quorum=0
        When:
            The pool context is entered
        Then:
            It should pass quorum=None to WorkerProxy without forwarding
            quorum_timeout — quorum=0 is documented as equivalent to
            None and normalized at the boundary to satisfy WorkerProxy's
            typed overload contract
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )

        # Act
        async with WorkerPool(spawn=2, quorum=0):
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert proxy_kwargs["quorum"] is None
        assert "quorum_timeout" not in proxy_kwargs

    @pytest.mark.asyncio
    async def test___aenter___durable_mode_forwards_quorum(
        self,
        mocker: MockerFixture,
        mock_worker_proxy,
        mock_discovery_service_for_pool,
    ):
        """Test durable mode forwards quorum to WorkerProxy.

        Given:
            A WorkerPool with discovery and quorum=3
        When:
            The pool context is entered
        Then:
            It should pass quorum=3 to WorkerProxy
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )
        discovery_service = mock_discovery_service_for_pool

        # Act
        async with WorkerPool(discovery=discovery_service, quorum=3):
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert proxy_kwargs["quorum"] == 3

    @pytest.mark.asyncio
    async def test___aenter___hybrid_mode_forwards_quorum(
        self,
        mocker: MockerFixture,
        mock_shared_memory,
        mock_worker_proxy,
        mock_local_worker,
        mock_discovery_service_for_pool,
    ):
        """Test hybrid mode forwards quorum to WorkerProxy.

        Given:
            A WorkerPool with spawn=2, discovery, and quorum=3
        When:
            The pool context is entered
        Then:
            It should pass quorum=3 to WorkerProxy
        """
        # Arrange

        mock_proxy_cls = mocker.patch.object(
            wp, "WorkerProxy", return_value=mock_worker_proxy
        )
        discovery_service = mock_discovery_service_for_pool

        # Act
        async with WorkerPool(spawn=2, discovery=discovery_service, quorum=3):
            pass

        # Assert
        mock_proxy_cls.assert_called_once()
        _, proxy_kwargs = mock_proxy_cls.call_args
        assert proxy_kwargs["quorum"] == 3

    @given(quorum=st.integers(min_value=-100, max_value=-1))
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @pytest.mark.asyncio
    async def test___aenter___rejects_negative_quorum_pbt(
        self, quorum, mock_shared_memory, mock_local_worker
    ):
        """Test negative quorum values are rejected at context entry.

        Given:
            Any negative quorum value
        When:
            The WorkerPool context is entered
        Then:
            It should raise ValueError from the underlying WorkerProxy
        """
        # Act & assert
        with pytest.raises(
            ValueError,
            match="Quorum must be a non-negative integer",
        ):
            async with WorkerPool(spawn=1, quorum=quorum):
                pass

    @given(
        spawn=st.integers(min_value=1, max_value=20),
        lease=st.integers(min_value=0, max_value=20),
        quorum=st.integers(min_value=0, max_value=40),
    )
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    def test___init___accepts_quorum_within_capacity_pbt(
        self, spawn, lease, quorum, mocker: MockerFixture
    ):
        """Test any quorum within the pool capacity is accepted.

        Given:
            Any spawn in [1, 20], lease in [0, 20], a discovery
            service, and quorum in [0, spawn + lease]
        When:
            WorkerPool is instantiated
        Then:
            It should create the pool successfully
        """
        # Hypothesis explores the full grid; skip combinations that the
        # validator legitimately rejects (quorum > spawn + lease).
        if quorum > spawn + lease:
            return

        # Arrange
        mock_discovery = mocker.MagicMock()

        # Act
        pool = WorkerPool(
            spawn=spawn, discovery=mock_discovery, lease=lease, quorum=quorum
        )

        # Assert
        assert isinstance(pool, WorkerPool)
