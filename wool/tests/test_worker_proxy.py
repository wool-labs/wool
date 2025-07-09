import cloudpickle
import pytest
from pytest_mock import MockerFixture

import wool._worker_proxy as wp
from wool import _protobuf as pb
from wool._work import WoolTask
from wool._worker_discovery import DiscoveryService
from wool._worker_discovery import LanDiscoveryService


@pytest.fixture
def mock_lan_discovery_service(mocker: MockerFixture):
    """Create a mock :py:class:`LanDiscoveryService` for testing.

    Provides a mock discovery service with async methods and started state
    tracking for use in worker proxy tests.
    """
    mock_lan_discovery_service = mocker.MagicMock(spec=LanDiscoveryService)
    mock_lan_discovery_service.started = False
    mock_lan_discovery_service.start = mocker.AsyncMock()
    mock_lan_discovery_service.stop = mocker.AsyncMock()
    mock_lan_discovery_service.events = mocker.AsyncMock()
    return mock_lan_discovery_service


@pytest.fixture
def mock_discovery_service(mocker: MockerFixture):
    """Create a mock :py:class:`DiscoveryService` for testing.

    Provides a generic mock discovery service with async methods for use in
    worker proxy tests that don't require LAN-specific functionality.
    """
    mock_discovery_service = mocker.MagicMock(spec=DiscoveryService)
    mock_discovery_service.started = False
    mock_discovery_service.start = mocker.AsyncMock()
    mock_discovery_service.stop = mocker.AsyncMock()
    mock_discovery_service.events = mocker.AsyncMock()
    return mock_discovery_service


@pytest.fixture
def mock_load_balancer_factory(mocker: MockerFixture):
    """Create a mock load balancer factory for testing.

    Returns a tuple of (factory, load_balancer) where the factory creates
    the load balancer instance. Used for testing factory-based load balancer
    creation patterns.
    """
    mock_load_balancer_factory = mocker.MagicMock(spec=wp.LoadBalancerFactory)
    mock_load_balancer = mocker.MagicMock(spec=wp.LoadBalancerLike)
    mock_load_balancer.dispatch = mocker.AsyncMock()
    mock_load_balancer._workers = {}
    mock_load_balancer.worker_added_callback = mocker.MagicMock()
    mock_load_balancer.worker_updated_callback = mocker.MagicMock()
    mock_load_balancer.worker_removed_callback = mocker.MagicMock()
    mock_load_balancer_factory.return_value = mock_load_balancer
    return mock_load_balancer_factory, mock_load_balancer


@pytest.fixture
def mock_worker_stub(mocker: MockerFixture):
    """Create a mock gRPC worker stub for testing.

    Provides a mock :py:class:`WorkerStub` with async dispatch functionality
    for testing worker communication scenarios.
    """
    mock_worker_stub = mocker.MagicMock(spec=pb.worker.WorkerStub)
    mock_worker_stub.dispatch = mocker.AsyncMock()
    return mock_worker_stub


@pytest.fixture
def mock_wool_task(mocker: MockerFixture):
    """Create a mock :py:class:`WoolTask` for testing.

    Provides a mock task with protobuf serialization capabilities for
    testing task dispatch and processing scenarios.
    """
    mock_task = mocker.MagicMock(spec=WoolTask)
    mock_task.to_protobuf = mocker.MagicMock()
    return mock_task


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


class TestWorkerProxy:
    def test_constructor_discovery_and_loadbalancer(
        self, mock_discovery_service, mock_load_balancer_factory
    ):
        """Test :py:class:`WorkerProxy` initialization with discovery and
        load balancer.

        **Given:**
            A discovery service and load balancer factory.
        **When:**
            :py:class:`WorkerProxy` is initialized.
        **Then:**
            It should store both services correctly.
        """
        # Arrange
        mock_factory, mock_load_balancer = mock_load_balancer_factory

        # Act
        proxy = wp.WorkerProxy(
            discovery=mock_discovery_service, loadbalancer=mock_factory
        )

        # Assert
        assert proxy._loadbalancer == mock_factory
        assert proxy._discovery == mock_discovery_service
        assert proxy._started is False

    def test_constructor_discovery_only(
        self, mocker: MockerFixture, mock_discovery_service
    ):
        """Test :py:class:`WorkerProxy` initialization with discovery only.

        **Given:**
            A discovery service.
        **When:**
            :py:class:`WorkerProxy` is initialized.
        **Then:**
            It should create a :py:class:`RoundRobinLoadBalancer`.
        """
        # Arrange
        # (No additional arrangement needed)

        # Act
        proxy = wp.WorkerProxy(discovery=mock_discovery_service)

        # Assert
        assert proxy._loadbalancer == wp.RoundRobinLoadBalancer
        assert proxy._discovery == mock_discovery_service

    def test_constructor_uri_only(self, mocker: MockerFixture):
        """Test :py:class:`WorkerProxy` initialization with URI only.

        **Given:**
            A pool URI string.
        **When:**
            :py:class:`WorkerProxy` is initialized.
        **Then:**
            It should create :py:class:`LocalDiscoveryService` and use
            :py:class:`RoundRobinLoadBalancer`.
        """
        # Arrange
        mock_local_discovery_service = mocker.MagicMock()
        mocker.patch.object(
            wp, "LocalDiscoveryService", return_value=mock_local_discovery_service
        )

        # Act
        proxy = wp.WorkerProxy("pool-1")

        # Assert
        assert proxy._loadbalancer == wp.RoundRobinLoadBalancer
        assert proxy._discovery == mock_local_discovery_service

    def test_constructor_invalid_arguments(self):
        """Test :py:class:`WorkerProxy` initialization with invalid
        arguments.

        **Given:**
            No valid constructor arguments.
        **When:**
            :py:class:`WorkerProxy` is initialized.
        **Then:**
            It should raise :py:exc:`ValueError`.
        """
        # Act & Assert
        with pytest.raises(
            ValueError,
            match=(
                "Must specify either a workerpool URI, discovery event stream, "
                "or a sequence of workers"
            ),
        ):
            wp.WorkerProxy()  # type: ignore[call-overload]

    @pytest.mark.asyncio
    async def test_start_sets_started_flag(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test :py:class:`WorkerProxy` start method sets started flag.

        **Given:**
            An unstarted :py:class:`WorkerProxy` instance.
        **When:**
            :py:meth:`start` is called.
        **Then:**
            It should set the started flag to True.
        """
        # Arrange
        proxy = wp.WorkerProxy(discovery=mock_discovery_service)

        # Act
        await proxy.start()

        # Assert
        assert proxy._started

    @pytest.mark.asyncio
    async def test_stop_clears_state(self, mock_discovery_service, mock_proxy_session):
        """Test :py:class:`WorkerProxy` stop method clears state.

        **Given:**
            A started :py:class:`WorkerProxy` with registered workers.
        **When:**
            :py:meth:`stop` is called.
        **Then:**
            It should clear workers and reset the started flag to False.
        """
        # Arrange
        proxy = wp.WorkerProxy(discovery=mock_discovery_service)
        await proxy.start()

        # Add some workers to verify they get cleared
        from wool._worker_discovery import WorkerInfo

        worker_info = WorkerInfo(
            uid="worker-1",
            host="127.0.0.1",
            port=50051,
            pid=1234,
            version="1.0.0",
        )
        from unittest.mock import MagicMock

        mock_worker_stub = MagicMock()
        proxy._workers[worker_info] = mock_worker_stub

        # Act
        await proxy.stop()

        # Assert
        assert not proxy._started
        assert len(proxy._workers) == 0

    @pytest.mark.asyncio
    async def test_dispatch_delegates_to_loadbalancer(
        self,
        mocker: MockerFixture,
        mock_load_balancer_factory,
        mock_discovery_service,
        mock_wool_task,
        mock_proxy_session,
    ):
        """Test :py:class:`WorkerProxy` task dispatch delegates to load
        balancer.

        **Given:**
            A started :py:class:`WorkerProxy` with available workers.
        **When:**
            :py:meth:`dispatch` is called with a task.
        **Then:**
            It should delegate the task to the load balancer and yield results.
        """
        # Arrange
        mock_factory, mock_load_balancer = mock_load_balancer_factory
        mock_worker_info = mocker.MagicMock()
        mock_worker_stub = mocker.MagicMock()
        mock_load_balancer._workers = {mock_worker_info: mock_worker_stub}

        proxy = wp.WorkerProxy(
            discovery=mock_discovery_service,
            loadbalancer=mock_factory,
        )

        # Mock the await_workers method to avoid waiting
        mocker.patch.object(proxy, "_await_workers", new_callable=mocker.AsyncMock)

        await proxy.start()

        # Setup mock async iterator response
        mock_result = mocker.MagicMock()

        async def mock_dispatch_iterator(task):
            yield mock_result

        mock_load_balancer.dispatch = mock_dispatch_iterator

        # Act
        result_iterator = proxy.dispatch(mock_wool_task)
        results = [result async for result in result_iterator]

        # Assert
        assert results == [mock_result]

    def test_constructor_uri_and_loadbalancer(
        self, mock_load_balancer_factory, mocker: MockerFixture
    ):
        """Test :py:class:`WorkerProxy` initialization with URI and load
        balancer.

        **Given:**
            A pool URI and load balancer factory.
        **When:**
            :py:class:`WorkerProxy` is initialized.
        **Then:**
            It should create a proxy with URI filtering and the provided
            load balancer.
        """
        # Arrange
        mock_factory, mock_load_balancer = mock_load_balancer_factory
        mock_local_discovery_service = mocker.MagicMock()
        mocker.patch.object(
            wp, "LocalDiscoveryService", return_value=mock_local_discovery_service
        )

        # Act
        proxy = wp.WorkerProxy("test-pool", loadbalancer=mock_factory)

        # Assert
        assert proxy._loadbalancer == mock_factory
        assert proxy._started is False

    @pytest.mark.asyncio
    async def test_start_already_started_raises_error(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test :py:class:`WorkerProxy` start when already started raises
        error.

        **Given:**
            A :py:class:`WorkerProxy` that is already started.
        **When:**
            :py:meth:`start` is called again.
        **Then:**
            It should raise :py:exc:`RuntimeError`.
        """
        # Arrange
        proxy = wp.WorkerProxy(discovery=mock_discovery_service)
        await proxy.start()

        # Act & Assert
        with pytest.raises(RuntimeError, match="Proxy already started"):
            await proxy.start()

    @pytest.mark.asyncio
    async def test_stop_not_started_raises_error(self, mock_discovery_service):
        """Test :py:class:`WorkerProxy` stop when not started raises error.

        **Given:**
            A :py:class:`WorkerProxy` that is not started.
        **When:**
            :py:meth:`stop` is called.
        **Then:**
            It should raise :py:exc:`RuntimeError`.
        """
        # Arrange
        proxy = wp.WorkerProxy(discovery=mock_discovery_service)

        # Act & Assert
        with pytest.raises(RuntimeError, match="Proxy not started"):
            await proxy.stop()

    @pytest.mark.asyncio
    async def test_dispatch_not_started_raises_error(
        self, mock_discovery_service, mock_wool_task
    ):
        """Test :py:class:`WorkerProxy` dispatch when not started raises
        error.

        **Given:**
            A :py:class:`WorkerProxy` that is not started.
        **When:**
            :py:meth:`dispatch` is called.
        **Then:**
            It should raise :py:exc:`RuntimeError`.
        """
        # Arrange
        proxy = wp.WorkerProxy(discovery=mock_discovery_service)

        # Act & Assert
        with pytest.raises(RuntimeError, match="Proxy not started"):
            async for _ in proxy.dispatch(mock_wool_task):
                pass

    @pytest.mark.asyncio
    async def test_context_manager_enter_starts_proxy(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test :py:class:`WorkerProxy` async context manager entry starts
        proxy.

        **Given:**
            An unstarted :py:class:`WorkerProxy`.
        **When:**
            The async context manager is entered.
        **Then:**
            It should automatically start the proxy.
        """
        # Arrange
        proxy = wp.WorkerProxy(discovery=mock_discovery_service)

        # Act & Assert
        async with proxy as p:
            assert p._started

    @pytest.mark.asyncio
    async def test_context_manager_exit_stops_proxy(
        self, mock_discovery_service, mock_proxy_session
    ):
        """Test :py:class:`WorkerProxy` async context manager exit stops
        proxy.

        **Given:**
            A :py:class:`WorkerProxy` within async context.
        **When:**
            The async context manager exits.
        **Then:**
            It should automatically stop the proxy.
        """
        # Arrange
        proxy = wp.WorkerProxy(discovery=mock_discovery_service)

        # Act
        async with proxy:
            pass

        # Assert
        assert not proxy._started

    def test_workers_property_returns_workers_dict(
        self, mock_discovery_service, mocker: MockerFixture
    ):
        """Test :py:class:`WorkerProxy` workers property returns workers
        dictionary.

        **Given:**
            A :py:class:`WorkerProxy` with discovered workers.
        **When:**
            The workers property is accessed.
        **Then:**
            It should return a dictionary of :py:class:`WorkerInfo` objects.
        """
        # Arrange
        from wool._worker_discovery import WorkerInfo

        proxy = wp.WorkerProxy(discovery=mock_discovery_service)
        worker_info = WorkerInfo(
            uid="worker-1",
            host="127.0.0.1",
            port=50051,
            pid=1234,
            version="1.0.0",
        )
        mock_worker_stub = mocker.MagicMock()
        proxy._workers = {worker_info: mock_worker_stub}

        # Act
        workers = proxy.workers

        # Assert
        assert workers == {worker_info: mock_worker_stub}

    @pytest.mark.asyncio
    async def test_dispatch_propagates_loadbalancer_errors(
        self,
        mocker: MockerFixture,
        mock_load_balancer_factory,
        mock_discovery_service,
        mock_wool_task,
        mock_proxy_session,
    ):
        """Test :py:class:`WorkerProxy` dispatch propagates load balancer
        errors.

        **Given:**
            A :py:class:`WorkerProxy` where load balancer dispatch fails.
        **When:**
            :py:meth:`dispatch` is called.
        **Then:**
            It should propagate the error from the load balancer.
        """
        # Arrange
        mock_factory, mock_load_balancer = mock_load_balancer_factory
        mock_worker_info = mocker.MagicMock()
        mock_worker_stub = mocker.MagicMock()
        mock_load_balancer._workers = {mock_worker_info: mock_worker_stub}

        proxy = wp.WorkerProxy(
            discovery=mock_discovery_service, loadbalancer=mock_factory
        )

        # Mock the await_workers method to avoid waiting
        mocker.patch.object(proxy, "_await_workers", new_callable=mocker.AsyncMock)

        await proxy.start()

        # Setup mock async generator to raise error
        async def mock_failing_dispatch(task):
            raise Exception("Load balancer error")
            yield  # This line will never be reached

        mock_load_balancer.dispatch = mock_failing_dispatch

        # Act & Assert
        with pytest.raises(Exception, match="Load balancer error"):
            async for _ in proxy.dispatch(mock_wool_task):
                pass

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_with_services(self):
        """Test :py:class:`WorkerProxy` cloudpickle serialization with
        services.

        **Given:**
            A started :py:class:`WorkerProxy` with real discovery and load
            balancer services.
        **When:**
            cloudpickle serialization and deserialization are performed within
            the context.
        **Then:**
            It should serialize and deserialize successfully with the
            deserialized proxy in an unstarted state.
        """
        # Arrange
        # Use real objects instead of mocks for cloudpickle test
        from wool._worker_discovery import LocalDiscoveryService

        discovery_service = LocalDiscoveryService("test-pool")
        proxy = wp.WorkerProxy(
            discovery=discovery_service, loadbalancer=wp.RoundRobinLoadBalancer
        )

        # Act & Assert
        async with proxy:
            # Verify proxy is started before pickling
            assert proxy._started is True

            # Pickle from within the started proxy context
            pickled_data = cloudpickle.dumps(proxy)
            unpickled_proxy = cloudpickle.loads(pickled_data)

            # Unpickled proxy should be unstarted regardless of source state
            assert isinstance(unpickled_proxy, wp.WorkerProxy)
            assert unpickled_proxy._started is False

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_discovery_only(self):
        """Test :py:class:`WorkerProxy` cloudpickle serialization with
        discovery only.

        **Given:**
            A started :py:class:`WorkerProxy` with only discovery service.
        **When:**
            cloudpickle serialization and deserialization are performed within
            the context.
        **Then:**
            It should serialize and deserialize successfully with the
            deserialized proxy in an unstarted state.
        """
        # Arrange
        # Use real objects instead of mocks for cloudpickle test
        from wool._worker_discovery import LocalDiscoveryService

        discovery_service = LocalDiscoveryService("test-pool")
        proxy = wp.WorkerProxy(discovery=discovery_service)

        # Act & Assert
        async with proxy:
            # Verify proxy is started before pickling
            assert proxy._started is True

            # Pickle from within the started proxy context
            pickled_data = cloudpickle.dumps(proxy)
            unpickled_proxy = cloudpickle.loads(pickled_data)

            # Unpickled proxy should be unstarted regardless of source state
            assert isinstance(unpickled_proxy, wp.WorkerProxy)
            assert unpickled_proxy._started is False

    @pytest.mark.asyncio
    async def test_cloudpickle_serialization_uri_preserves_id(self):
        """Test :py:class:`WorkerProxy` cloudpickle serialization with URI
        preserves ID.

        **Given:**
            A started :py:class:`WorkerProxy` created with only a URI.
        **When:**
            cloudpickle serialization and deserialization are performed within
            the context.
        **Then:**
            It should serialize and deserialize successfully with the
            deserialized proxy in an unstarted state and preserved ID.
        """
        # Arrange
        # Use real objects - this creates a LocalDiscoveryService internally
        proxy = wp.WorkerProxy("pool-1")

        # Act & Assert
        async with proxy:
            # Verify proxy is started before pickling
            assert proxy._started is True

            # Pickle from within the started proxy context
            pickled_data = cloudpickle.dumps(proxy)
            unpickled_proxy = cloudpickle.loads(pickled_data)

            # Unpickled proxy should be unstarted regardless of source state
            assert isinstance(unpickled_proxy, wp.WorkerProxy)
            assert unpickled_proxy.started is False
            assert unpickled_proxy.id == proxy.id

    def test_proxy_id_uniqueness_across_instances(self):
        """Test :py:class:`WorkerProxy` ID uniqueness across instances.

        **Given:**
            Multiple :py:class:`WorkerProxy` instances.
        **When:**
            Each proxy generates an ID.
        **Then:**
            All IDs should be unique UUIDs.
        """
        # Arrange
        # (No arrangement needed)

        # Act
        proxy_ids = [wp.WorkerProxy("test-pool").id for _ in range(5)]

        # Assert
        assert len(set(proxy_ids)) == len(proxy_ids)


class TestRoundRobinLoadBalancer:
    def test_constructor_initializes_state(self):
        """Test :py:class:`RoundRobinLoadBalancer` initialization.

        **Given:**
            No constructor arguments (default initialization).
        **When:**
            :py:class:`RoundRobinLoadBalancer` is initialized.
        **Then:**
            It should initialize the internal state correctly with empty
            workers dictionary and zero index.
        """
        # Arrange
        # (No arrangement needed)

        # Act
        load_balancer = wp.RoundRobinLoadBalancer()

        # Assert
        assert hasattr(load_balancer, "_workers")
        assert hasattr(load_balancer, "_current_index")
        assert load_balancer._workers == {}
        assert load_balancer._current_index == 0

    @pytest.mark.asyncio
    async def test_dispatch_no_workers_raises_exception(self, mocker: MockerFixture):
        """Test :py:class:`RoundRobinLoadBalancer` task dispatch with no
        workers.

        **Given:**
            A :py:class:`RoundRobinLoadBalancer` with no available workers.
        **When:**
            :py:meth:`dispatch` is called.
        **Then:**
            It should raise :py:exc:`NoWorkersAvailable` exception.
        """
        # Arrange
        load_balancer = wp.RoundRobinLoadBalancer()
        mock_wool_task = mocker.MagicMock()
        mock_task_protobuf = mocker.MagicMock()
        mock_wool_task.to_protobuf.return_value = mock_task_protobuf

        # Act & Assert
        with pytest.raises(wp.NoWorkersAvailable):
            async for _ in load_balancer.dispatch(mock_wool_task):
                pass

    def test_cloudpickle_serialization_preserves_state(self):
        """Test :py:class:`RoundRobinLoadBalancer` cloudpickle serialization.

        **Given:**
            A :py:class:`RoundRobinLoadBalancer` instance.
        **When:**
            cloudpickle serialization and deserialization are performed.
        **Then:**
            It should serialize and deserialize successfully with proper state
            preservation.
        """
        # Arrange
        load_balancer = wp.RoundRobinLoadBalancer()

        # Act
        pickled_data = cloudpickle.dumps(load_balancer)
        unpickled_load_balancer = cloudpickle.loads(pickled_data)

        # Assert
        assert isinstance(unpickled_load_balancer, wp.RoundRobinLoadBalancer)
        assert hasattr(unpickled_load_balancer, "_workers")
        assert hasattr(unpickled_load_balancer, "_current_index")
        assert unpickled_load_balancer._current_index == 0


# class TestChannelCache:
#     @staticmethod
#     @strategies.composite
#     def setup(draw, *, max_address_count=5, max_reference_count=3):
#         """Generate a ChannelCache with varied initial channel states.

#         Creates a cache with 0-max_address_count channels, each acquired
#         1-max_reference_count times to create realistic cache states for
#         property-based testing.

#         :param draw:
#             The Hypothesis draw function for generating test data.
#         :param max_address_count:
#             Maximum number of addresses to create channels for.
#         :param max_reference_count:
#             Maximum number of times each channel can be acquired.
#         :returns:
#             An async function that when called returns a tuple of
#             (ChannelCache, list of mock channels, list of addresses).
#         """
#         cache = wp.ChannelCache()
#         mock_channels = []
#         addresses = []

#         async def setup() -> tuple[wp.ChannelCache, list, list]:
#             with patch.object(grpc.aio, 'insecure_channel') as mock_insecure_channel:
#                 for i in range(draw(strategies.integers(0, max_address_count))):
#                     address = f"127.0.0.1:{50051 + i}"
#                     addresses.append(address)

#                     mock_channel = Mock()
#                     mock_channel.close = AsyncMock()
#                     mock_channels.append(mock_channel)
#                     mock_insecure_channel.return_value = mock_channel

#                     acquire_count = draw(strategies.integers(1, max_reference_count))
#                     for _ in range(acquire_count):
#                         await cache.acquire(address)

#                     assert address in cache._channels
#                     assert cache._ref_counts[address] == acquire_count

#             return cache, mock_channels, addresses

#         return setup

#     @pytest.mark.asyncio
#     @given(setup=setup())
#     async def test_acquire(self, setup):
#         """Test acquiring channels with varied initial cache conditions.

#         Given:
#             A cache with various initial channel states
#         When:
#             New channels are acquired
#         Then:
#             Should properly create and reference count channels
#         """
#         cache, mock_channels, addresses = await setup()

#         # Test acquiring a new address
#         with patch.object(grpc.aio, 'insecure_channel') as mock_insecure_channel:
#             new_mock_channel = Mock()
#             new_mock_channel.close = AsyncMock()
#             mock_insecure_channel.return_value = new_mock_channel

#             new_address = "127.0.0.1:60000"
#             channel = await cache.acquire(new_address)

#             assert channel is new_mock_channel
#             assert new_address in cache._channels
#             assert cache._ref_counts[new_address] == 1
#             mock_insecure_channel.assert_called_once_with(new_address)

#         # Verify existing channels are still there
#         for address in addresses:
#             assert address in cache._channels
#             assert cache._ref_counts[address] > 0

#     @pytest.mark.asyncio
#     @pytest.mark.dependency("TestChannelCache::test_acquire")
#     @given(setup=setup(), data=strategies.data())
#     async def test_release(self, setup, data):
#         """Test releasing channels with varied initial cache conditions.

#         Given:
#             A cache with various channel states and ref counts
#         When:
#             Channels are released
#         Then:
#             Should properly decrement ref counts or close and remove channels
#         """
#         cache, mock_channels, addresses = await setup()

#         if addresses:
#             # Sample a random subset of addresses to release
#             addresses_to_release = data.draw(strategies.lists(
#                 strategies.sampled_from(addresses),
#                 min_size=0,
#                 max_size=len(addresses),
#                 unique=True
#             ))

#             for address in addresses_to_release:
#                 initial_ref_count = cache._ref_counts[address]
#                 await cache.release(address)

#                 if initial_ref_count > 1:
#                     # Should still be in cache with decremented count
#                     assert address in cache._channels
#                     assert cache._ref_counts[address] == initial_ref_count - 1
#                 else:
#                     # Should be removed from cache and channel closed
#                     assert address not in cache._channels
#                     assert address not in cache._ref_counts

#     @pytest.mark.asyncio
#     @pytest.mark.dependency("TestChannelCache::test_acquire")
#     @given(setup=setup())
#     async def test_release_nonexistent_address(self, setup):
#         """Test releasing nonexistent address does nothing.

#         Given:
#             A cache with various initial channel states
#         When:
#             Release is called with a nonexistent address
#         Then:
#             Should return without error and not affect existing channels
#         """
#         cache, mock_channels, addresses = await setup()
#         initial_channel_count = len(cache._channels)

#         # Try to release a nonexistent address
#         await cache.release("nonexistent:50051")

#         # Should not affect existing channels
#         assert len(cache._channels) == initial_channel_count
#         for address in addresses:
#             assert address in cache._channels

#     @pytest.mark.asyncio
#     @pytest.mark.dependency(
#         "TestChannelCache::test_acquire", "TestChannelCache::test_release"
#     )
#     @given(setup=setup(), data=strategies.data())
#     async def test_clear(self, setup, data):
#         """Test clearing the cache closes all channels.

#         Given:
#             A cache with various initial channel states
#         When:
#             Clear is called
#         Then:
#             All channels should be closed and cache cleared
#         """
#         cache, mock_channels, addresses = await setup()

#         if addresses:
#             # Randomly release some channels first
#             addresses_to_release = data.draw(strategies.lists(
#                 strategies.sampled_from(addresses),
#                 min_size=0,
#                 max_size=len(addresses),
#                 unique=True
#             ))

#             for address in addresses_to_release:
#                 await cache.release(address)

#         await cache.clear()

#         # All channels should be closed and cache cleared
#         assert len(cache._channels) == 0
#         assert len(cache._ref_counts) == 0

#         # All mock channels should have been closed
#         for mock_channel in mock_channels:
#             mock_channel.close.assert_called()
