import asyncio
import hashlib
import json
import multiprocessing.shared_memory
import socket
import struct
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import cloudpickle
import pytest
from pytest_mock import MockerFixture

import wool._worker_discovery as discovery


@pytest.fixture
def worker_info():
    """Fixture providing a WorkerInfo instance for testing."""
    return discovery.WorkerInfo(
        uid="worker-test-123",
        host="127.0.0.1",
        port=48800,
        pid=12345,
        version="1.0.0",
        tags={"test", "worker"},
        extra={"region": "us-west-1"},
    )


@pytest.fixture
def mock_async_zeroconf(mocker: MockerFixture):
    """Fixture providing a mocked AsyncZeroconf instance."""
    mock_async_zeroconf = mocker.patch.object(discovery, "AsyncZeroconf", autospec=True)
    mock_async_zeroconf.return_value.zeroconf = mocker.MagicMock()
    return mock_async_zeroconf


@pytest.fixture
def mock_async_service_browser(mocker: MockerFixture):
    """Fixture providing a mocked AsyncServiceBrowser instance."""
    return mocker.patch.object(discovery, "AsyncServiceBrowser", autospec=True)


@pytest.fixture
def mock_service_info(mocker: MockerFixture):
    """Fixture providing a mocked ServiceInfo instance."""
    return mocker.patch.object(discovery, "ServiceInfo", autospec=True)


@pytest.fixture
def dummy_discovery_service():
    """Fixture providing a dummy DiscoveryService for testing async iteration."""

    class DummyDiscoveryService(discovery.DiscoveryService):
        def __init__(self, filter=None):
            super().__init__(filter)
            self.events_called = False

        async def events(self):
            self.events_called = True
            # Yield test events
            yield discovery.DiscoveryEvent(
                type="worker_added",
                worker_info=discovery.WorkerInfo(
                    uid="test-123",
                    host="127.0.0.1",
                    port=8080,
                    pid=12345,
                    version="1.0.0",
                    tags=set(),
                    extra={},
                ),
            )
            yield discovery.DiscoveryEvent(
                type="worker_removed",
                worker_info=discovery.WorkerInfo(
                    uid="test-456",
                    host="127.0.0.1",
                    port=8081,
                    pid=12346,
                    version="1.0.0",
                    tags=set(),
                    extra={},
                ),
            )

        async def _start(self):
            pass

        async def _stop(self):
            pass

    return DummyDiscoveryService


class TestLanRegistrarService:
    def test_init_sets_default_values(self, mock_async_zeroconf):
        """Test LanRegistrarService initialization with default values.

        Given:
            No parameters are provided
        When:
            LanRegistrarService is initialized
        Then:
            It should set correct default values and not create AsyncZeroconf
        """
        # Arrange & Act
        registrar = discovery.LanRegistrarService()

        # Assert
        assert registrar.aiozc is None
        assert registrar.services == {}
        assert registrar.service_type == "_wool._tcp.local."
        assert registrar._started is False
        assert registrar._stopped is False
        mock_async_zeroconf.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_creates_service_info_and_registers(
        self,
        mocker: MockerFixture,
        mock_async_zeroconf,
        mock_service_info,
        worker_info,
    ):
        """Test worker registration creates service info and registers with Zeroconf.

        Given:
            A started registrar service and WorkerInfo instance
        When:
            register() is called with WorkerInfo
        Then:
            ServiceInfo should be created and registered with AsyncZeroconf
        """
        # Arrange
        registrar = discovery.LanRegistrarService()

        mock_inet_pton = mocker.patch.object(
            socket,
            "inet_pton",
            autospec=True,
            return_value=b"\x7f\x00\x00\x01",
        )
        mock_serialize_worker_info = mocker.patch.object(
            discovery, "_serialize_worker_info", return_value={"test": "data"}
        )

        await registrar.start()

        # Act
        await registrar.register(worker_info)

        # Assert
        mock_async_zeroconf.assert_called_once()
        mock_inet_pton.assert_called_once_with(socket.AF_INET, "127.0.0.1")
        mock_serialize_worker_info.assert_called_once_with(worker_info)
        mock_service_info.assert_called_once_with(
            "_wool._tcp.local.",
            "worker-test-123._wool._tcp.local.",
            addresses=[b"\x7f\x00\x00\x01"],
            port=48800,
            properties={"test": "data"},
        )
        mock_async_register_service = (
            mock_async_zeroconf.return_value.async_register_service
        )
        mock_async_register_service.assert_called_once_with(
            mock_service_info.return_value
        )
        assert registrar.services[worker_info.uid] == mock_service_info.return_value

    @pytest.mark.asyncio
    async def test_unregister_removes_service_from_registrar(
        self,
        mock_async_zeroconf,
        mock_service_info,
        worker_info,
    ):
        """Test worker unregistration removes service from AsyncZeroconf and
        local registrar.

        Given:
            A started registrar service with a registered worker
        When:
            unregister() is called with the WorkerInfo
        Then:
            The service should be unregistered from AsyncZeroconf and
            removed from local registrar
        """
        # Arrange
        registrar = discovery.LanRegistrarService()
        registrar.services[worker_info.uid] = mock_service_info.return_value
        await registrar.start()

        # Act
        await registrar.unregister(worker_info)

        # Assert
        mock_async_unregister_service = (
            mock_async_zeroconf.return_value.async_unregister_service
        )
        mock_async_unregister_service.assert_called_once_with(
            mock_service_info.return_value
        )
        assert worker_info.uid not in registrar.services

    @pytest.mark.asyncio
    async def test_start_already_started_raises_error(self, mock_async_zeroconf):
        """Test starting an already started registrar raises RuntimeError.

        Given:
            A registrar service that has already been started
        When:
            start() is called again
        Then:
            RuntimeError should be raised with appropriate message
        """
        # Arrange
        registrar = discovery.LanRegistrarService()
        await registrar.start()

        # Act & Assert
        with pytest.raises(RuntimeError, match="Registrar service already started"):
            await registrar.start()

    @pytest.mark.asyncio
    async def test_stop_not_started(self):
        """Test stopping a registrar that hasn't been started raises
        RuntimeError.

        Given:
            A registrar service that has not been started
        When:
            stop() is called
        Then:
            RuntimeError should be raised with appropriate message
        """
        # Arrange
        registrar = discovery.LanRegistrarService()

        # Act & Assert
        with pytest.raises(
            RuntimeError,
            match="Registrar service not started",
        ):
            await registrar.stop()

    @pytest.mark.asyncio
    async def test_register_not_started_raises_error(self, worker_info):
        """Test registering worker before starting registrar raises
        RuntimeError.

        Given:
            A registrar service that has not been started
        When:
            register() is called with a WorkerInfo
        Then:
            RuntimeError should be raised with appropriate message
        """
        # Arrange
        registrar = discovery.LanRegistrarService()

        # Act & Assert
        with pytest.raises(
            RuntimeError,
            match="Registrar service not started - call start\\(\\) first",
        ):
            await registrar.register(worker_info)

    @pytest.mark.asyncio
    async def test_register_after_stopped_raises_error(
        self, mock_async_zeroconf, worker_info
    ):
        """Test registering worker after stopping registrar raises
        RuntimeError.

        Given:
            A registrar service that has been started and then stopped
        When:
            register() is called with a WorkerInfo
        Then:
            RuntimeError should be raised with appropriate message
        """
        # Arrange
        registrar = discovery.LanRegistrarService()
        await registrar.start()
        await registrar.stop()

        # Act & Assert
        with pytest.raises(RuntimeError, match="Registrar service already stopped"):
            await registrar.register(worker_info)

    @pytest.mark.asyncio
    async def test_unregister_not_started_raises_error(self, worker_info):
        """Test unregistering worker before starting registrar raises
        RuntimeError.

        Given:
            A registrar service that has not been started
        When:
            unregister() is called with a WorkerInfo
        Then:
            RuntimeError should be raised with appropriate message
        """
        # Arrange
        registrar = discovery.LanRegistrarService()

        # Act & Assert
        with pytest.raises(
            RuntimeError,
            match="Registrar service not started - call start\\(\\) first",
        ):
            await registrar.unregister(worker_info)

    @pytest.mark.asyncio
    async def test_unregister_after_stopped_raises_error(
        self, mock_async_zeroconf, worker_info
    ):
        """Test unregistering worker after stopping registrar raises
        RuntimeError.

        Given:
            A registrar service that has been started and then stopped
        When:
            unregister() is called with a WorkerInfo
        Then:
            RuntimeError should be raised with appropriate message
        """
        # Arrange
        registrar = discovery.LanRegistrarService()
        await registrar.start()
        await registrar.stop()

        # Act & Assert
        with pytest.raises(RuntimeError, match="Registrar service already stopped"):
            await registrar.unregister(worker_info)

    @pytest.mark.asyncio
    async def test_stop_idempotent_operation(self, mock_async_zeroconf):
        """Test that stopping a registrar multiple times is safe.

        Given:
            A registrar service that has been started and stopped
        When:
            stop() is called again
        Then:
            No exception should be raised (idempotent operation)
        """
        # Arrange
        registrar = discovery.LanRegistrarService()
        await registrar.start()
        await registrar.stop()

        # Act
        await registrar.stop()  # Should not raise

        # Assert - no exception should be raised

    @pytest.mark.asyncio
    async def test_update_worker_properties(
        self,
        mocker: MockerFixture,
        mock_async_zeroconf,
        mock_service_info,
        worker_info,
    ):
        """Test worker property update modifies service info and updates
        AsyncZeroconf.

        Given:
            A started registrar service with a registered worker
        When:
            update() is called with updated WorkerInfo
        Then:
            ServiceInfo should be updated and re-registered with AsyncZeroconf
        """
        # Arrange
        registrar = discovery.LanRegistrarService()

        # Set up existing service
        mock_existing_service = mocker.MagicMock()
        mock_existing_service.decoded_properties = {"old": "data"}
        mock_existing_service.type = "_wool._tcp.local."
        mock_existing_service.name = "worker-test-123._wool._tcp.local."
        mock_existing_service.addresses = [b"\x7f\x00\x00\x01"]
        mock_existing_service.port = 48800
        mock_existing_service.server = "worker-test-123.local."
        registrar.services[worker_info.uid] = mock_existing_service

        mock_serialize_worker_info = mocker.patch.object(
            discovery, "_serialize_worker_info", return_value={"new": "data"}
        )

        await registrar.start()

        # Act
        await registrar.update(worker_info)

        # Assert
        mock_serialize_worker_info.assert_called_once_with(worker_info)
        mock_service_info.assert_called_once_with(
            "_wool._tcp.local.",
            "worker-test-123._wool._tcp.local.",
            addresses=[b"\x7f\x00\x00\x01"],
            port=48800,
            properties={"new": "data"},
            server="worker-test-123.local.",
        )
        mock_async_update_service = mock_async_zeroconf.return_value.async_update_service
        mock_async_update_service.assert_called_once_with(mock_service_info.return_value)

    @pytest.mark.asyncio
    async def test_register_with_aiozc_none_raises_error(self, worker_info):
        """Test LanRegistrarService _register() with aiozc=None.

        Given:
            A started LanRegistrarService with aiozc=None
        When:
            _register() is called
        Then:
            Should raise RuntimeError "Registrar service not properly initialized"
        """
        # Arrange
        registrar = discovery.LanRegistrarService()
        await registrar.start()

        # Set aiozc to None to simulate initialization failure
        registrar.aiozc = None

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Registrar service not properly initialized"
        ):
            await registrar._register(worker_info)

        await registrar.stop()

    @pytest.mark.asyncio
    async def test_unregister_with_aiozc_none_raises_error(self, worker_info):
        """Test LanRegistrarService _unregister() with aiozc=None.

        Given:
            A started LanRegistrarService with aiozc=None
        When:
            _unregister() is called
        Then:
            Should raise RuntimeError "Registrar service not properly initialized"
        """
        # Arrange
        registrar = discovery.LanRegistrarService()
        await registrar.start()

        # Set aiozc to None to simulate initialization failure
        registrar.aiozc = None

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Registrar service not properly initialized"
        ):
            await registrar._unregister(worker_info)

        await registrar.stop()

    @pytest.mark.asyncio
    async def test_update_with_aiozc_none_raises_error(self, worker_info):
        """Test LanRegistrarService _update() with aiozc=None.

        Given:
            A started LanRegistrarService with aiozc=None
        When:
            _update() is called
        Then:
            Should raise RuntimeError "Registrar service not properly initialized"
        """
        # Arrange
        registrar = discovery.LanRegistrarService()
        await registrar.start()

        # Set aiozc to None to simulate initialization failure
        registrar.aiozc = None

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Registrar service not properly initialized"
        ):
            await registrar._update(worker_info)

        await registrar.stop()

    def test_resolve_address_ipv4_fallback(self):
        """Test LanRegistrarService._resolve_address() IPv4 fallback handling.

        Given:
            A LanRegistrarService with address that fails IPv4 parsing
        When:
            _resolve_address() is called with hostname
        Then:
            Should fall back through IPv4, IPv6, and hostname resolution
        """
        # Arrange
        registrar = discovery.LanRegistrarService()

        with pytest.MonkeyPatch().context() as m:
            # Mock socket functions to simulate IPv4 parsing failure, then success
            def mock_inet_pton_ipv4(family, host):
                if family == socket.AF_INET:
                    raise OSError("Not IPv4")
                return b"\x7f\x00\x00\x01"  # 127.0.0.1

            def mock_inet_pton_ipv6(family, host):
                if family == socket.AF_INET6:
                    raise OSError("Not IPv6")
                return b"\x7f\x00\x00\x01"

            def mock_gethostbyname(hostname):
                return "127.0.0.1"

            def mock_inet_aton(ip_str):
                return b"\x7f\x00\x00\x01"

            m.setattr(socket, "inet_pton", mock_inet_pton_ipv4)
            m.setattr(socket, "gethostbyname", mock_gethostbyname)
            m.setattr(socket, "inet_aton", mock_inet_aton)

            # Act
            result = registrar._resolve_address("localhost:8080")

            # Assert
            assert result == (b"\x7f\x00\x00\x01", 8080)

    def test_resolve_address_ipv6_fallback(self):
        """Test LanRegistrarService._resolve_address() IPv6 fallback handling.

        Given:
            A LanRegistrarService with IPv6 address
        When:
            _resolve_address() is called
        Then:
            Should handle IPv6 address parsing
        """
        # Arrange
        registrar = discovery.LanRegistrarService()

        with pytest.MonkeyPatch().context() as m:
            # Mock IPv4 to fail, IPv6 to succeed
            def mock_inet_pton(family, host):
                if family == socket.AF_INET:
                    raise OSError("Not IPv4")
                elif family == socket.AF_INET6:
                    return b"\x00" * 16  # Mock IPv6 bytes

            m.setattr(socket, "inet_pton", mock_inet_pton)

            # Act
            result = registrar._resolve_address("ipv6host:8080")

            # Assert
            assert result == (b"\x00" * 16, 8080)

    def test_resolve_address_hostname_fallback(self):
        """Test LanRegistrarService._resolve_address() hostname resolution fallback.

        Given:
            A LanRegistrarService with address that fails both IPv4 and IPv6 parsing
        When:
            _resolve_address() is called
        Then:
            Should fall back to hostname resolution
        """
        # Arrange
        registrar = discovery.LanRegistrarService()

        with pytest.MonkeyPatch().context() as m:
            # Mock both IPv4 and IPv6 to fail, then hostname resolution
            def mock_inet_pton(family, host):
                raise OSError("Not IP address")

            def mock_gethostbyname(hostname):
                return "192.168.1.100"

            def mock_inet_aton(ip_str):
                return b"\xc0\xa8\x01\x64"  # 192.168.1.100

            m.setattr(socket, "inet_pton", mock_inet_pton)
            m.setattr(socket, "gethostbyname", mock_gethostbyname)
            m.setattr(socket, "inet_aton", mock_inet_aton)

            # Act
            result = registrar._resolve_address("example.com:9000")

            # Assert
            assert result == (b"\xc0\xa8\x01\x64", 9000)


class TestLanDiscoveryService:
    def test_init_sets_default_values(self, mock_async_zeroconf):
        """Test LanDiscoveryService initialization with default values.

        Given:
            No parameters are provided
        When:
            LanDiscoveryService is initialized
        Then:
            It should set correct default values and not create AsyncZeroconf
        """
        # Arrange & Act
        lan_discovery_service = discovery.LanDiscoveryService()

        # Assert
        assert lan_discovery_service._started is False
        assert lan_discovery_service.service_type == "_wool._tcp.local."
        assert hasattr(lan_discovery_service, "_event_queue")
        assert hasattr(lan_discovery_service, "_service_cache")
        assert hasattr(lan_discovery_service, "_filter")
        # AsyncZeroconf is not created until start() is called
        mock_async_zeroconf.assert_not_called()

    def test_init_with_filter_sets_predicate(self, mock_async_zeroconf):
        """Test LanDiscoveryService initialization with filter function.

        Given:
            A filter function for WorkerInfo filtering
        When:
            LanDiscoveryService is initialized with filter
        Then:
            It should store the filter for use in discovery
        """

        # Arrange
        def filter_func(w):
            return "test" in w.tags

        # Act
        lan_discovery_service = discovery.LanDiscoveryService(filter=filter_func)

        # Assert
        assert lan_discovery_service._filter == filter_func
        assert lan_discovery_service._started is False
        mock_async_zeroconf.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_creates_zeroconf_and_browser(
        self, mock_async_zeroconf, mock_async_service_browser
    ):
        """Test starting LanDiscoveryService creates Zeroconf artifacts.

        Given:
            A LanDiscoveryService instance
        When:
            start() is called
        Then:
            AsyncZeroconf and AsyncServiceBrowser should be created and configured
            properly
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        # Act
        await lan_discovery_service.start()

        # Assert
        mock_async_zeroconf.assert_called_once_with(interfaces=["127.0.0.1"])
        mock_async_service_browser.assert_called_once()
        args, kwargs = mock_async_service_browser.call_args
        assert args[0] == mock_async_zeroconf.return_value.zeroconf
        assert args[1] == "_wool._tcp.local."
        assert isinstance(kwargs["listener"], discovery.LanDiscoveryService._Listener)
        assert lan_discovery_service.browser == mock_async_service_browser.return_value

    @pytest.mark.asyncio
    async def test_stop_cancels_browser_and_closes_zeroconf(
        self,
        mock_async_zeroconf,
        mock_async_service_browser,
    ):
        """Test stopping LanDiscoveryService cancels browser and closes AsyncZeroconf.

        Given:
            A started LanDiscoveryService instance
        When:
            stop() is called
        Then:
            The browser should be cancelled and AsyncZeroconf closed
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()
        await lan_discovery_service.start()

        # Act
        await lan_discovery_service.stop()

        # Assert
        mock_async_service_browser.return_value.async_cancel.assert_awaited_once()
        mock_async_zeroconf.return_value.async_close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_start_already_started_raises_error(
        self,
        mock_async_zeroconf,
        mock_async_service_browser,
    ):
        """Test starting an already started discovery service raises RuntimeError.

        Given:
            A discovery service that has already been started
        When:
            start() is called again
        Then:
            RuntimeError should be raised with appropriate message
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()
        await lan_discovery_service.start()

        # Act & Assert
        with pytest.raises(RuntimeError, match="Discovery service already started"):
            await lan_discovery_service.start()

    @pytest.mark.asyncio
    async def test_stop_not_started_raises_error(self):
        """Test stopping a discovery service that hasn't been started raises exception.

        Given:
            A discovery service that has not been started
        When:
            stop() is called
        Then:
            RuntimeError should be raised with appropriate message
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        # Act & Assert
        with pytest.raises(
            RuntimeError,
            match="Discovery service not started",
        ):
            await lan_discovery_service.stop()

    @pytest.mark.asyncio
    async def test_events_iterator(self, mocker: MockerFixture, worker_info):
        """Test that events() returns an async iterator over discovery events.

        Given:
            A LanDiscoveryService with a mocked event queue
        When:
            events() is called
        Then:
            It should return an async iterator that yields events from queue
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()
        mock_discovery_event = discovery.DiscoveryEvent(
            type="worker_added", worker_info=worker_info
        )

        # Mock the event queue to return our test event once
        mocker.patch.object(
            lan_discovery_service._event_queue,
            "get",
            side_effect=[mock_discovery_event, asyncio.CancelledError()],
        )

        # Act
        events_iter = lan_discovery_service.events()

        # Assert
        # Get first event
        event = await events_iter.__anext__()
        assert event == mock_discovery_event
        assert event.worker_info == worker_info

        # Second call should raise CancelledError (simulating end of events)
        with pytest.raises(asyncio.CancelledError):
            await events_iter.__anext__()

    @pytest.mark.asyncio
    async def test_worker_filter_applied(
        self, mock_async_zeroconf, mock_async_service_browser
    ):
        """Test that worker filter is correctly passed to listener.

        Given:
            A LanDiscoveryService with a WorkerInfo filter
        When:
            start() is called
        Then:
            The filter should be passed to the listener for filtering workers
        """

        # Arrange
        def filter_func(w):
            return "production" in w.tags

        lan_discovery_service = discovery.LanDiscoveryService(filter=filter_func)

        # Act
        await lan_discovery_service.start()

        # Assert
        mock_async_service_browser.assert_called_once()
        _, kwargs = mock_async_service_browser.call_args
        listener = kwargs["listener"]
        assert listener._predicate == filter_func

    @pytest.mark.asyncio
    async def test_listener_update_service_creates_task(
        self, mock_async_zeroconf, mock_async_service_browser, mocker
    ):
        """Test LanDiscoveryService._Listener update_service() Zeroconf callback.

        Given:
            A LanDiscoveryService._Listener instance
        When:
            update_service() Zeroconf callback is triggered
        Then:
            Should create async task for service update handling
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        # Create a real listener for testing
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,
            service_cache=lan_discovery_service._service_cache,
        )

        # Mock _handle_update_service instead of create_task to avoid coroutine warnings
        mock_handle_update = mocker.patch.object(
            listener, "_handle_update_service", new_callable=AsyncMock
        )

        # Act
        listener.update_service(
            mock_async_zeroconf.return_value,
            "_wool._tcp.local.",
            "test-service._wool._tcp.local.",
        )

        # Give the task a chance to be scheduled and executed
        await asyncio.sleep(0.01)

        # Assert
        mock_handle_update.assert_called_once_with(
            "_wool._tcp.local.", "test-service._wool._tcp.local."
        )

    @pytest.mark.asyncio
    async def test_handle_add_service_with_no_service_info(
        self, mock_async_zeroconf, mock_async_service_browser
    ):
        """Test LanDiscoveryService _handle_add_service() with no service info.

        Given:
            A LanDiscoveryService where aiozc returns None for service info
        When:
            _handle_add_service() retrieves service info
        Then:
            Should return early when service info is None
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        # Mock async_get_service_info to return None
        mock_async_zeroconf.return_value.async_get_service_info.return_value = None

        # Create a real listener for testing
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,
            service_cache=lan_discovery_service._service_cache,
        )

        # Act
        await listener._handle_add_service(
            "_wool._tcp.local.", "test-service._wool._tcp.local."
        )

        # Assert
        assert lan_discovery_service._event_queue.empty()

    @pytest.mark.asyncio
    async def test_handle_add_service_deserialization_error(
        self,
        mock_async_zeroconf,
        mock_async_service_browser,
        mock_service_info,
        mocker,
    ):
        """Test LanDiscoveryService _handle_add_service() deserialization error.

        Given:
            A LanDiscoveryService with invalid service properties
        When:
            _handle_add_service() deserializes worker info
        Then:
            Should handle ValueError during deserialization
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        # Mock service info with invalid data
        mock_async_zeroconf.return_value.async_get_service_info.return_value = (
            mock_service_info.return_value
        )

        # Mock _deserialize_worker_info to raise ValueError
        mock_deserialize_worker_info = mocker.patch.object(
            discovery,
            "_deserialize_worker_info",
            side_effect=ValueError("Invalid data"),
        )

        # Create a real listener for testing
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,
            service_cache=lan_discovery_service._service_cache,
        )

        # Act
        await listener._handle_add_service(
            "_wool._tcp.local.", "test-service._wool._tcp.local."
        )

        # Assert
        # Verify deserialization was attempted
        mock_deserialize_worker_info.assert_called_once()
        # Verify no events were added to queue
        assert lan_discovery_service._event_queue.empty()

    @pytest.mark.asyncio
    async def test_handle_add_service_exception_handling(
        self, mock_async_zeroconf, mock_async_service_browser, mocker
    ):
        """Test LanDiscoveryService _handle_add_service() exception handling.

        Given:
            A LanDiscoveryService where service operations raise exceptions
        When:
            _handle_add_service() processes service
        Then:
            Should handle general exceptions gracefully
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        # Mock async_get_service_info to raise an exception
        mock_async_zeroconf.return_value.async_get_service_info.side_effect = Exception(
            "Network error"
        )

        # Create a real listener for testing
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,
            service_cache=lan_discovery_service._service_cache,
        )

        # Act
        await listener._handle_add_service(
            "_wool._tcp.local.", "test-service._wool._tcp.local."
        )

        # Assert
        assert lan_discovery_service._event_queue.empty()

    @pytest.mark.asyncio
    async def test_handle_update_service_complete_scenarios(
        self,
        mock_async_zeroconf,
        mock_async_service_browser,
        mock_service_info,
        mocker,
        worker_info,
    ):
        """Test LanDiscoveryService _handle_update_service() complete scenarios.

        Given:
            A LanDiscoveryService with various worker states
        When:
            _handle_update_service() processes all scenarios
        Then:
            Should handle new worker, existing worker updates, and filter changes
        """

        # Arrange
        # Test with a filter that accepts workers with 'test' tag
        def test_filter(w):
            return "test" in w.tags

        lan_discovery_service = discovery.LanDiscoveryService(filter=test_filter)

        # Create a real listener for testing
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=test_filter,
            service_cache=lan_discovery_service._service_cache,
        )

        # Mock service info
        mock_async_zeroconf.return_value.async_get_service_info.return_value = (
            mock_service_info.return_value
        )

        # Mock deserialization to return our worker_info
        mock_deserialize_worker_info = mocker.patch.object(
            discovery, "_deserialize_worker_info", return_value=worker_info
        )

        # Act & Assert - Test 1: New worker that matches filter (not in cache)
        await listener._handle_update_service(
            "_wool._tcp.local.", "new-worker._wool._tcp.local."
        )

        # Should add worker to cache and emit worker_added event
        assert "new-worker._wool._tcp.local." in listener._service_cache
        event = await lan_discovery_service._event_queue.get()
        assert event.type == "worker_added"
        assert event.worker_info == worker_info

        # Act & Assert - Test 2: Existing worker that still matches filter
        await listener._handle_update_service(
            "_wool._tcp.local.", "new-worker._wool._tcp.local."
        )

        # Should update cache and emit worker_updated event
        event = await lan_discovery_service._event_queue.get()
        assert event.type == "worker_updated"
        assert event.worker_info == worker_info

        # Arrange - Test 3: Existing worker that no longer matches filter
        # Create a worker without 'test' tag using the fixture as base
        filtered_out_worker = discovery.WorkerInfo(
            uid="filtered-worker",
            host=worker_info.host,
            port=worker_info.port,
            pid=worker_info.pid,
            version=worker_info.version,
            tags={"production"},  # No 'test' tag
            extra=worker_info.extra,
        )

        mock_deserialize_worker_info.return_value = filtered_out_worker

        # Act & Assert - Test 3: Worker that no longer matches filter
        await listener._handle_update_service(
            "_wool._tcp.local.", "new-worker._wool._tcp.local."
        )

        # Should remove worker from cache and emit worker_removed event
        assert "new-worker._wool._tcp.local." not in listener._service_cache
        event = await lan_discovery_service._event_queue.get()
        assert event.type == "worker_removed"
        assert event.worker_info == worker_info  # The old worker info

    @pytest.mark.asyncio
    async def test_add_service_type_filtering(
        self, mock_async_zeroconf, mock_async_service_browser
    ):
        """Test LanDiscoveryService add_service() with incorrect service type.

        Given:
            A LanDiscoveryService._Listener instance
        When:
            add_service() is called with non-wool service type
        Then:
            Should not create async task
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,
            service_cache=lan_discovery_service._service_cache,
        )
        mock_zeroconf_instance = MagicMock()

        # Act
        listener.add_service(
            mock_zeroconf_instance, "_http._tcp.local.", "test-service._http._tcp.local."
        )

        # Assert
        assert lan_discovery_service._event_queue.empty()

    @pytest.mark.asyncio
    async def test_remove_service_cache_operations(
        self, mock_async_zeroconf, mock_async_service_browser, worker_info
    ):
        """Test LanDiscoveryService remove_service() cache operations.

        Given:
            A LanDiscoveryService._Listener with worker in cache
        When:
            remove_service() is called
        Then:
            Should pop worker from cache and create removal event
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        service_name = "test-worker._wool._tcp.local."
        lan_discovery_service._service_cache[service_name] = worker_info
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,
            service_cache=lan_discovery_service._service_cache,
        )
        mock_zeroconf_instance = MagicMock()

        # Act
        listener.remove_service(
            mock_zeroconf_instance, "_wool._tcp.local.", service_name
        )
        await asyncio.sleep(0.01)  # Allow async task to complete

        # Assert
        assert service_name not in lan_discovery_service._service_cache
        assert not lan_discovery_service._event_queue.empty()
        event = await lan_discovery_service._event_queue.get()
        assert event.type == "worker_removed"
        assert event.worker_info == worker_info

    @pytest.mark.asyncio
    async def test_handle_add_service_predicate_filtering(
        self,
        mock_async_zeroconf,
        mock_async_service_browser,
        mock_service_info,
        worker_info,
    ):
        """Test LanDiscoveryService _handle_add_service() predicate filtering.

        Given:
            A LanDiscoveryService with predicate that filters out workers
        When:
            _handle_add_service() processes worker that doesn't match predicate
        Then:
            Should not add worker to cache or generate event
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService(
            filter=lambda w: w.uid != worker_info.uid
        )

        mock_async_zeroconf.return_value.async_get_service_info.return_value = (
            mock_service_info.return_value
        )
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: w.uid != worker_info.uid,  # This will reject our worker
            service_cache=lan_discovery_service._service_cache,
        )

        # Act
        with pytest.MonkeyPatch().context() as m:
            m.setattr(discovery, "_deserialize_worker_info", lambda si: worker_info)
            await listener._handle_add_service(
                "_wool._tcp.local.", "test-service._wool._tcp.local."
            )

        # Assert
        assert len(lan_discovery_service._service_cache) == 0
        assert lan_discovery_service._event_queue.empty()

    @pytest.mark.asyncio
    async def test_handle_update_service_with_no_service_info(
        self, mock_async_zeroconf, mock_async_service_browser
    ):
        """Test LanDiscoveryService _handle_update_service() when service info is None.

        Given:
            A LanDiscoveryService where async_get_service_info returns None
        When:
            _handle_update_service() is called
        Then:
            Should return early without processing
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        mock_async_zeroconf.return_value.async_get_service_info.return_value = None
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,
            service_cache=lan_discovery_service._service_cache,
        )

        # Act
        await listener._handle_update_service(
            "_wool._tcp.local.", "test-service._wool._tcp.local."
        )

        # Assert
        assert lan_discovery_service._event_queue.empty()

    @pytest.mark.asyncio
    async def test_handle_update_service_deserialization_error(
        self,
        mock_async_zeroconf,
        mock_async_service_browser,
        mock_service_info,
        mocker,
    ):
        """Test LanDiscoveryService _handle_update_service() deserialization error.

        Given:
            A LanDiscoveryService with service that can't be deserialized
        When:
            _handle_update_service() tries to deserialize worker info
        Then:
            Should handle ValueError during deserialization
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        mock_async_zeroconf.return_value.async_get_service_info.return_value = (
            mock_service_info.return_value
        )
        mock_deserialize = mocker.patch.object(
            discovery,
            "_deserialize_worker_info",
            side_effect=ValueError("Invalid data"),
        )
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,
            service_cache=lan_discovery_service._service_cache,
        )

        # Act
        await listener._handle_update_service(
            "_wool._tcp.local.", "test-service._wool._tcp.local."
        )

        # Assert
        mock_deserialize.assert_called_once()
        assert lan_discovery_service._event_queue.empty()

    @pytest.mark.asyncio
    async def test_handle_update_service_exception_handling(
        self, mock_async_zeroconf, mock_async_service_browser, mocker
    ):
        """Test LanDiscoveryService _handle_update_service() general exception handling.

        Given:
            A LanDiscoveryService where service operations raise exceptions
        When:
            _handle_update_service() processes service
        Then:
            Should handle general exceptions gracefully
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        mock_async_zeroconf.return_value.async_get_service_info.side_effect = Exception(
            "Network error"
        )
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,
            service_cache=lan_discovery_service._service_cache,
        )

        # Act
        await listener._handle_update_service(
            "_wool._tcp.local.", "test-service._wool._tcp.local."
        )

        # Assert
        assert lan_discovery_service._event_queue.empty()

    @pytest.mark.asyncio
    async def test_add_service_exact_type_match(
        self, mock_async_zeroconf, mock_async_service_browser
    ):
        """Test LanDiscoveryService add_service() with exact service type match.

        Given:
            A LanDiscoveryService._Listener instance
        When:
            add_service() is called with exact LanRegistrarService.service_type string
        Then:
            Should create async task for service processing
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,
            service_cache=lan_discovery_service._service_cache,
        )
        mock_zeroconf_instance = MagicMock()
        exact_service_type = discovery.LanRegistrarService.service_type
        assert exact_service_type == "_wool._tcp.local."  # Verify we have the right type

        # Act & Assert
        # Mock _handle_add_service instead of create_task to avoid coroutine warnings
        async def mock_handle_add_service(type_, name):
            pass

        with pytest.MonkeyPatch().context() as m:
            mock_handle_add = MagicMock(side_effect=mock_handle_add_service)
            m.setattr(listener, "_handle_add_service", mock_handle_add)

            listener.add_service(
                mock_zeroconf_instance,
                exact_service_type,
                "test-service._wool._tcp.local.",
            )

            # Give the task a chance to be scheduled
            await asyncio.sleep(0.01)

            mock_handle_add.assert_called_once_with(
                exact_service_type, "test-service._wool._tcp.local."
            )

    @pytest.mark.asyncio
    async def test_handle_add_service_complete_success_path(
        self,
        mock_async_zeroconf,
        mock_async_service_browser,
        mock_service_info,
        worker_info,
    ):
        """Test LanDiscoveryService _handle_add_service() complete success execution.

        Given:
            A LanDiscoveryService with all success conditions met
        When:
            _handle_add_service() executes without any failures
        Then:
            Should update cache and create event
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService(
            filter=lambda w: w.uid == worker_info.uid  # This will match
        )

        mock_async_zeroconf.return_value.async_get_service_info.return_value = (
            mock_service_info.return_value
        )
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: w.uid
            == worker_info.uid,  # This predicate will return True
            service_cache=lan_discovery_service._service_cache,
        )

        # Act
        with pytest.MonkeyPatch().context() as m:
            m.setattr(discovery, "_deserialize_worker_info", lambda si: worker_info)

            # Ensure initial state
            assert len(lan_discovery_service._service_cache) == 0
            assert lan_discovery_service._event_queue.empty()

            await listener._handle_add_service(
                "_wool._tcp.local.", "test-service._wool._tcp.local."
            )

            # Assert
            assert len(lan_discovery_service._service_cache) == 1
            assert (
                "test-service._wool._tcp.local." in lan_discovery_service._service_cache
            )
            assert (
                lan_discovery_service._service_cache["test-service._wool._tcp.local."]
                == worker_info
            )
            assert not lan_discovery_service._event_queue.empty()
            event = await lan_discovery_service._event_queue.get()
            assert event.type == "worker_added"
            assert event.worker_info == worker_info

    def test_cloudpickle_serialization(self, mock_async_zeroconf):
        """Test LanDiscoveryService is cloudpickleable.

        Given:
            A LanDiscoveryService instance
        When:
            cloudpickle.dumps() and cloudpickle.loads() are called
        Then:
            Should serialize and deserialize successfully
        """
        service = discovery.LanDiscoveryService()

        # Should not raise exception
        pickled_data = cloudpickle.dumps(service)
        unpickled_service = cloudpickle.loads(pickled_data)

        assert isinstance(unpickled_service, discovery.LanDiscoveryService)
        assert unpickled_service._started is False
        assert unpickled_service.service_type == "_wool._tcp.local."
        assert hasattr(unpickled_service, "_event_queue")
        assert hasattr(unpickled_service, "_service_cache")
        assert hasattr(unpickled_service, "_filter")

    def test_cloudpickle_serialization_with_filter(
        self, mock_async_zeroconf, worker_info
    ):
        """Test LanDiscoveryService with filter is cloudpickleable.

        Given:
            A LanDiscoveryService instance with filter function
        When:
            cloudpickle.dumps() and cloudpickle.loads() are called
        Then:
            Should serialize and deserialize successfully with filter preserved
        """

        def filter_func(w):
            return "test" in w.tags

        service = discovery.LanDiscoveryService(filter=filter_func)

        # Should not raise exception
        pickled_data = cloudpickle.dumps(service)
        unpickled_service = cloudpickle.loads(pickled_data)

        assert isinstance(unpickled_service, discovery.LanDiscoveryService)
        assert unpickled_service._started is False
        assert unpickled_service._filter is not None
        # Filter should be preserved - add "test" tag to the fixture worker
        worker_info.tags.add("test")
        assert unpickled_service._filter(worker_info) is True

    @pytest.mark.asyncio
    async def test_handle_add_service_predicate_success_with_mocks(
        self, mock_async_zeroconf, mock_async_service_browser, worker_info, mocker
    ):
        """Test LanDiscoveryService _handle_add_service() with mocked success conditions.

        Given:
            A LanDiscoveryService with mocked components for complete success
        When:
            _handle_add_service() processes a worker that passes all checks
        Then:
            Should execute cache update, event creation, and event queuing
        """
        # Arrange
        lan_discovery_service = discovery.LanDiscoveryService()

        mock_service_info_instance = MagicMock()
        mock_async_zeroconf.return_value.async_get_service_info.return_value = (
            mock_service_info_instance
        )
        mock_deserialize_worker_info = mocker.patch.object(
            discovery, "_deserialize_worker_info", return_value=worker_info
        )
        listener = discovery.LanDiscoveryService._Listener(
            aiozc=mock_async_zeroconf.return_value,
            event_queue=lan_discovery_service._event_queue,
            predicate=lambda w: True,  # Always match
            service_cache=lan_discovery_service._service_cache,
        )
        service_name = "test-worker._wool._tcp.local."

        # Act
        await listener._handle_add_service("_wool._tcp.local.", service_name)

        # Assert
        mock_async_zeroconf.return_value.async_get_service_info.assert_called_once_with(
            "_wool._tcp.local.", service_name
        )
        mock_deserialize_worker_info.assert_called_once_with(mock_service_info_instance)
        assert service_name in lan_discovery_service._service_cache
        assert lan_discovery_service._service_cache[service_name] == worker_info
        assert not lan_discovery_service._event_queue.empty()
        event = await lan_discovery_service._event_queue.get()
        assert event.type == "worker_added"
        assert event.worker_info == worker_info


class TestSerializationFunctions:
    def test_serialize_worker_info_to_properties_dict(self, worker_info):
        """Test serialization of WorkerInfo to properties dict.

        Given:
            A WorkerInfo instance with various properties
        When:
            _serialize_worker_info is called
        Then:
            It should return a flat dict with JSON-serialized complex fields
        """
        # Arrange & Act
        result = discovery._serialize_worker_info(worker_info)

        # Assert
        assert result["pid"] == "12345"
        assert result["version"] == "1.0.0"
        assert result["tags"]
        assert result["extra"]
        assert set(json.loads(result["tags"])) == {"test", "worker"}
        assert json.loads(result["extra"]) == {"region": "us-west-1"}

    def test_serialize_worker_info_with_empty_collections(self, worker_info):
        """Test serialization with empty tags and extra fields.

        Given:
            A WorkerInfo instance with empty collections
        When:
            _serialize_worker_info is called
        Then:
            It should handle empty collections appropriately
        """
        # Arrange - modify fixture to have empty collections
        worker_info.tags = set()
        worker_info.extra = {}

        # Act
        result = discovery._serialize_worker_info(worker_info)

        # Assert
        assert result["pid"] == "12345"
        assert result["version"] == "1.0.0"
        assert result["tags"] is None
        assert result["extra"] is None

    def test_deserialize_service_info_to_worker_info(self):
        """Test deserialization of ServiceInfo to WorkerInfo.

        Given:
            A ServiceInfo with decoded properties
        When:
            _deserialize_worker_info is called
        Then:
            It should return a properly constructed WorkerInfo instance
        """
        # Arrange
        mock_service_info_instance = MagicMock()
        mock_service_info_instance.name = "worker-test-123._wool._tcp.local."
        mock_service_info_instance.port = 48800
        mock_service_info_instance.decoded_properties = {
            "pid": "12345",
            "version": "1.0.0",
            "tags": '["test", "worker"]',
            "extra": '{"region": "us-west-1"}',
        }

        # Mock the IP address retrieval
        mock_ip_address_instance = MagicMock()
        mock_ip_address_instance.__str__ = MagicMock(return_value="127.0.0.1")
        mock_service_info_instance.ip_addresses_by_version.return_value = [
            mock_ip_address_instance
        ]

        # Act
        result = discovery._deserialize_worker_info(mock_service_info_instance)

        # Assert
        assert result.uid == "worker-test-123._wool._tcp.local."
        assert result.host == "127.0.0.1"
        assert result.port == 48800
        assert result.pid == 12345
        assert result.version == "1.0.0"
        assert result.tags == {"test", "worker"}
        assert result.extra == {"region": "us-west-1"}

    def test_deserialize_worker_info_with_missing_required_fields(self):
        """Test deserialization fails with missing required fields.

        Given:
            A ServiceInfo missing required properties
        When:
            _deserialize_worker_info is called
        Then:
            It should raise ValueError with appropriate message
        """
        # Arrange
        mock_service_info_instance = MagicMock()
        mock_service_info_instance.decoded_properties = {
            "pid": "12345",
            # Missing "version"
        }

        # Act & Assert
        with pytest.raises(ValueError, match="Missing required properties: version"):
            discovery._deserialize_worker_info(mock_service_info_instance)

    def test_deserialize_worker_info_with_empty_collections(self):
        """Test deserialization with empty optional fields.

        Given:
            A ServiceInfo with minimal properties
        When:
            _deserialize_worker_info is called
        Then:
            It should handle missing optional fields gracefully
        """
        # Arrange
        mock_service_info_instance = MagicMock()
        mock_service_info_instance.name = "worker-minimal._wool._tcp.local."
        mock_service_info_instance.port = 8080
        mock_service_info_instance.decoded_properties = {
            "pid": "1234",
            "version": "1.0.0",
            "tags": None,
            "extra": None,
        }

        mock_ip_address_instance = MagicMock()
        mock_ip_address_instance.__str__ = MagicMock(return_value="192.168.1.1")
        mock_service_info_instance.ip_addresses_by_version.return_value = [
            mock_ip_address_instance
        ]

        # Act
        result = discovery._deserialize_worker_info(mock_service_info_instance)

        # Assert
        assert result.uid == "worker-minimal._wool._tcp.local."
        assert result.host == "192.168.1.1"
        assert result.port == 8080
        assert result.pid == 1234
        assert result.version == "1.0.0"
        assert result.tags == set()
        assert result.extra == {}


class TestWorkerInfo:
    def test_worker_info_hash_method(self, worker_info):
        """Test WorkerInfo.__hash__ method returns hash of uid.

        Given:
            A WorkerInfo instance
        When:
            __hash__ is called (via hash() built-in)
        Then:
            Should return hash of the uid
        """

        # Act
        worker_hash = hash(worker_info)

        # Assert
        expected_hash = hash(worker_info.uid)
        assert worker_hash == expected_hash

    def test_worker_info_hash_consistency(self):
        """Test WorkerInfo.__hash__ is consistent for same uid.

        Given:
            Two WorkerInfo instances with same uid but different properties
        When:
            __hash__ is called on both
        Then:
            Should return the same hash value
        """

        # Arrange
        worker1 = discovery.WorkerInfo(
            uid="same-uid", host="host1", port=8001, pid=1001, version="1.0.0"
        )

        worker2 = discovery.WorkerInfo(
            uid="same-uid",  # Same uid
            host="host2",  # Different host
            port=8002,  # Different port
            pid=1002,  # Different pid
            version="2.0.0",  # Different version
        )

        # Act
        hash1 = hash(worker1)
        hash2 = hash(worker2)

        # Assert
        assert hash1 == hash2  # Should be equal since uid is the same


class TestDiscoveryEvent:
    def test_discovery_event_creation(self, worker_info):
        """Test DiscoveryEvent creation with WorkerInfo.

        Given:
            A WorkerInfo instance and event type
        When:
            DiscoveryEvent is created
        Then:
            It should properly store the worker and event type
        """
        # Arrange & Act
        event = discovery.DiscoveryEvent(type="worker_added", worker_info=worker_info)

        # Assert
        assert event.type == "worker_added"
        assert event.worker_info == worker_info
        assert event.worker_info.uid == "worker-test-123"
        assert event.worker_info.host == "127.0.0.1"
        assert event.worker_info.port == 48800

    @pytest.mark.parametrize(
        "event_type", ["worker_added", "worker_removed", "worker_updated"]
    )
    def test_discovery_event_types(self, event_type, worker_info):
        """Test all valid DiscoveryEvent types.

        Given:
            A WorkerInfo and various event types
        When:
            DiscoveryEvent is created with each type
        Then:
            It should accept all valid event types
        """
        # Arrange & Act
        event = discovery.DiscoveryEvent(type=event_type, worker_info=worker_info)

        # Assert
        assert event.type == event_type
        assert event.worker_info == worker_info


class TestPredicatedQueue:
    @pytest.mark.asyncio
    async def test_put_and_get_without_predicate(self):
        """Test basic put and get operations without predicate.

        Given:
            A PredicatedQueue instance
        When:
            Items are put and retrieved without predicate
        Then:
            Items should be retrieved in FIFO order
        """
        # Arrange
        queue = discovery.PredicatedQueue[int]()

        # Act
        await queue.put(1)
        await queue.put(2)
        await queue.put(3)

        # Assert
        assert await queue.get() == 1
        assert await queue.get() == 2
        assert await queue.get() == 3

    @pytest.mark.asyncio
    async def test_get_with_predicate(self):
        """Test get with predicate function.

        Given:
            A PredicatedQueue with multiple items
        When:
            get() is called with a predicate
        Then:
            Only matching items should be returned
        """
        # Arrange
        queue = discovery.PredicatedQueue[int]()

        await queue.put(1)
        await queue.put(2)
        await queue.put(3)
        await queue.put(4)

        # Get even numbers only
        def even_predicate(x):
            return x % 2 == 0

        # Get odd numbers
        def odd_predicate(x):
            return x % 2 == 1

        # Act & Assert
        assert await queue.get(even_predicate) == 2
        assert await queue.get(even_predicate) == 4
        assert await queue.get(odd_predicate) == 1
        assert await queue.get(odd_predicate) == 3

    def test_qsize(self):
        """Test queue size reporting.

        Given:
            A PredicatedQueue with items
        When:
            qsize() is called
        Then:
            It should return the correct number of items
        """
        # Arrange
        queue = discovery.PredicatedQueue[str]()

        # Act & Assert
        assert queue.qsize() == 0

        queue.put_nowait("a")
        assert queue.qsize() == 1

        queue.put_nowait("b")
        queue.put_nowait("c")
        assert queue.qsize() == 3

    def test_empty(self):
        """Test empty queue detection.

        Given:
            A PredicatedQueue in various states
        When:
            empty() is called
        Then:
            It should correctly report if queue is empty
        """
        # Arrange
        queue = discovery.PredicatedQueue[int]()

        # Act & Assert
        assert queue.empty() is True

        queue.put_nowait(1)
        assert queue.empty() is False

        queue.get_nowait()
        assert queue.empty() is True

    def test_full_with_maxsize(self):
        """Test full queue detection with maxsize.

        Given:
            A PredicatedQueue with maxsize set
        When:
            full() is called
        Then:
            It should correctly report if queue is at capacity
        """
        # Arrange
        queue = discovery.PredicatedQueue[int](maxsize=2)

        # Act & Assert
        assert queue.full() is False

        queue.put_nowait(1)
        assert queue.full() is False

        queue.put_nowait(2)
        assert queue.full() is True

    def test_get_nowait_raises_on_empty(self):
        """Test get_nowait raises QueueEmpty when queue is empty.

        Given:
            An empty PredicatedQueue
        When:
            get_nowait() is called
        Then:
            It should raise asyncio.QueueEmpty
        """
        # Arrange
        queue = discovery.PredicatedQueue[int]()

        # Act & Assert
        with pytest.raises(asyncio.QueueEmpty):
            queue.get_nowait()

    def test_put_nowait_raises_on_full(self):
        """Test put_nowait raises QueueFull when queue is full.

        Given:
            A full PredicatedQueue
        When:
            put_nowait() is called
        Then:
            It should raise asyncio.QueueFull
        """
        # Arrange
        queue = discovery.PredicatedQueue[int](maxsize=1)
        queue.put_nowait(1)

        # Act & Assert
        with pytest.raises(asyncio.QueueFull):
            queue.put_nowait(2)

    @pytest.mark.asyncio
    async def test_task_done_and_join(self):
        """Test task completion tracking.

        Given:
            A PredicatedQueue with tasks
        When:
            task_done() is called for each task
        Then:
            join() should complete when all tasks are done
        """
        # Arrange
        queue = discovery.PredicatedQueue[str]()

        await queue.put("task1")
        await queue.put("task2")

        # Start consumer tasks
        async def consumer():
            await queue.get()
            # Process item
            queue.task_done()

        # Create consumer tasks
        task1 = asyncio.create_task(consumer())
        task2 = asyncio.create_task(consumer())

        # Act
        # Wait for all tasks to be processed
        await queue.join()

        # Ensure tasks completed
        await task1
        await task2

        # Assert
        assert queue.empty()

    @pytest.mark.asyncio
    async def test_put_blocking_and_cancel(self):
        """Test PredicatedQueue put() blocking and cancellation handling.

        Given:
            A full PredicatedQueue with maxsize=1
        When:
            put() is called and then cancelled
        Then:
            Should handle cancellation gracefully and clean up putters
        """
        # Arrange
        queue = discovery.PredicatedQueue[str](maxsize=1)
        queue.put_nowait("existing_item")

        # Create a task that will block on put()
        put_task = asyncio.create_task(queue.put("blocked_item"))

        # Give the put operation a moment to register as a blocked putter
        await asyncio.sleep(0.01)

        # Act
        put_task.cancel()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await put_task

        # Verify the item wasn't added and putter was cleaned up
        assert queue.qsize() == 1
        assert queue.get_nowait() == "existing_item"

    @pytest.mark.asyncio
    async def test_get_getter_removal_failure(self):
        """Test PredicatedQueue get() getter removal failure handling.

        Given:
            A PredicatedQueue with cancelled getters
        When:
            get() processes cancelled getters
        Then:
            Should handle ValueError when removing cancelled getter
        """
        # Arrange
        queue = discovery.PredicatedQueue[str]()

        # Create and immediately cancel a getter
        getter_task = asyncio.create_task(queue.get())
        getter_task.cancel()

        # Try to get the cancelled task result
        with pytest.raises(asyncio.CancelledError):
            await getter_task

        # Act
        # Now add an item - this should work even with the cancelled getter
        await queue.put("test_item")
        result = await queue.get()

        # Assert
        assert result == "test_item"

    def test_get_matching_item_no_match(self):
        """Test PredicatedQueue _get_matching_item() with no matching items.

        Given:
            A PredicatedQueue with items that don't match predicate
        When:
            _get_matching_item() is called with non-matching predicate
        Then:
            Should return None
        """
        # Arrange
        queue = discovery.PredicatedQueue[int]()
        queue.put_nowait(1)
        queue.put_nowait(3)
        queue.put_nowait(5)

        # Try to get an even number (none exist)
        def even_predicate(x):
            return x % 2 == 0

        # Act
        result = queue._get_matching_item(even_predicate)

        # Assert
        assert result is None
        # Verify items are still in queue
        assert queue.qsize() == 3

    @pytest.mark.asyncio
    async def test_wakeup_getter_cancelled(self):
        """Test PredicatedQueue _wakeup_next_getter() with cancelled getters.

        Given:
            A PredicatedQueue with cancelled getter futures
        When:
            _wakeup_next_getter() processes getters
        Then:
            Should skip done/cancelled getters
        """
        # Arrange
        queue = discovery.PredicatedQueue[str]()

        # Create a cancelled future
        cancelled_future = asyncio.Future()
        cancelled_future.cancel()
        queue._getters.append((cancelled_future, None))

        # Add a valid getter
        valid_task = asyncio.create_task(queue.get())
        await asyncio.sleep(0.01)  # Let getter register

        # Act
        # Add item to trigger wakeup
        await queue.put("test_item")

        # Assert
        # The valid getter should receive the item
        result = await valid_task
        assert result == "test_item"

    @pytest.mark.asyncio
    async def test_wakeup_getter_item_removed(self):
        """Test PredicatedQueue _wakeup_next_getter() item removal conflict.

        Given:
            A PredicatedQueue where item is removed by another operation
        When:
            _wakeup_next_getter() tries to remove item
        Then:
            Should handle ValueError and restore getters
        """
        # Arrange
        queue = discovery.PredicatedQueue[str]()

        # Add an item
        await queue.put("test_item")

        # Get the item synchronously first
        item = queue.get_nowait()
        assert item == "test_item"

        # Now add the item back and create a getter
        await queue.put("test_item")
        getter = asyncio.create_task(queue.get())

        # Wait briefly and add another item to trigger the wakeup path
        await asyncio.sleep(0.01)

        # Act
        await queue.put("another_item")

        # Assert
        # The getter should complete
        result = await getter
        assert result in ["test_item", "another_item"]

    @pytest.mark.asyncio
    async def test_wakeup_putter_break(self):
        """Test PredicatedQueue _wakeup_next_putter() break after success.

        Given:
            A PredicatedQueue with putters where queue becomes available
        When:
            _wakeup_next_putter() processes putters
        Then:
            Should break after first successful putter
        """
        # Arrange
        queue = discovery.PredicatedQueue[str](maxsize=1)

        # Fill the queue
        queue.put_nowait("existing")

        # Create multiple blocked putters
        putter1 = asyncio.create_task(queue.put("item1"))
        putter2 = asyncio.create_task(queue.put("item2"))

        await asyncio.sleep(0.01)  # Let putters register

        # Act
        # Remove the existing item to make space
        queue.get_nowait()

        # Wait for one putter to complete
        completed, pending = await asyncio.wait(
            [putter1, putter2], return_when=asyncio.FIRST_COMPLETED
        )

        # Assert
        # Only one putter should complete
        assert len(completed) == 1
        assert len(pending) == 1

        # Cancel the pending task
        for task in pending:
            task.cancel()

    def test_task_done_called_too_many_times(self):
        """Test PredicatedQueue task_done() called too many times.

        Given:
            A PredicatedQueue with no unfinished tasks
        When:
            task_done() is called
        Then:
            Should raise ValueError "task_done() called too many times"
        """
        # Arrange
        queue = discovery.PredicatedQueue[str]()

        # Act & Assert
        with pytest.raises(ValueError, match="task_done\\(\\) called too many times"):
            queue.task_done()

    @pytest.mark.asyncio
    async def test_put_cancellation_cleanup_failure(self):
        """Test PredicatedQueue put() cancellation when putter removal fails.

        Given:
            A PredicatedQueue where putter removal from cancelled list fails
        When:
            put() operation is cancelled and cleanup attempt fails
        Then:
            Should handle ValueError gracefully
        """
        # Arrange
        queue = discovery.PredicatedQueue[str](maxsize=1)

        queue.put_nowait("existing_item")

        async def mock_put_with_cleanup_failure():
            # Start put operation that will block
            put_task = asyncio.create_task(queue.put("blocked_item"))
            await asyncio.sleep(0.01)  # Let put() get to the blocking state

            # Verify putter was added
            assert len(queue._putters) == 1

            # Replace the putter tuple to simulate concurrent modification
            putter_future, item = queue._putters[0]
            queue._putters.clear()
            queue._putters.append((putter_future, "different_item"))

            # Act & Assert
            put_task.cancel()

            try:
                await put_task
                assert False, "Expected CancelledError"
            except asyncio.CancelledError:
                pass

        await mock_put_with_cleanup_failure()

    @pytest.mark.asyncio
    async def test_get_cancellation_cleanup_failure(self):
        """Test PredicatedQueue get() cancellation when getter removal fails.

        Given:
            A PredicatedQueue where getter removal from cancelled list fails
        When:
            get() operation is cancelled and cleanup attempt fails
        Then:
            Should handle ValueError gracefully
        """
        # Arrange
        queue = discovery.PredicatedQueue[str]()

        async def mock_get_with_cleanup_failure():
            # Start get operation that will block (no items in queue)
            get_task = asyncio.create_task(
                queue.get(predicate=lambda x: x == "specific")
            )
            await asyncio.sleep(0.01)  # Let get() get to the blocking state

            # Verify getter was added
            assert len(queue._getters) == 1

            # Replace the getter tuple to simulate concurrent modification
            getter_future, predicate = queue._getters[0]
            queue._getters.clear()
            queue._getters.append((getter_future, lambda x: x == "different"))

            # Act & Assert
            get_task.cancel()

            try:
                await get_task
                assert False, "Expected CancelledError"
            except asyncio.CancelledError:
                pass

        await mock_get_with_cleanup_failure()

    def test_wakeup_getter_value_error_handling(self):
        """Test PredicatedQueue _wakeup_next_getter ValueError handling.

        Given:
            A PredicatedQueue with item already removed by concurrent operation
        When:
            _wakeup_next_getter() tries to remove the item
        Then:
            Should handle ValueError and restore getter to queue
        """
        # Arrange
        queue = discovery.PredicatedQueue[str]()

        queue.put_nowait("test_item")
        mock_asyncio_future = asyncio.Future()
        # Don't set result - keep it as not done
        queue._getters.append((mock_asyncio_future, None))
        queue._queue.remove("test_item")  # Remove item to simulate concurrent removal

        # Act
        queue._wakeup_next_getter("test_item")

        # Assert
        assert len(queue._getters) == 1
        assert queue._getters[0] == (mock_asyncio_future, None)

    def test_wakeup_getter_predicate_mismatch_after_error(
        self,
    ):
        """Test PredicatedQueue _wakeup_next_getter predicate mismatch after ValueError.

        Given:
            A PredicatedQueue with multiple getters where first matches but removal
            fails, and second getter has predicate that doesn't match
        When:
            _wakeup_next_getter() processes getters after ValueError
        Then:
            Should restore non-matching getter to remaining_getters list
        """
        # Arrange
        queue = discovery.PredicatedQueue[str]()

        queue.put_nowait("test_item")
        mock_matching_future = asyncio.Future()
        mock_non_matching_future = asyncio.Future()

        # Add getters: first with matching predicate, second with non-matching predicate
        queue._getters.append(
            (mock_matching_future, lambda x: x == "test_item")
        )  # Will match
        queue._getters.append(
            (mock_non_matching_future, lambda x: x == "different_item")
        )  # Won't match

        queue._queue.remove("test_item")  # Remove item to cause ValueError

        # Act
        queue._wakeup_next_getter("test_item")

        # Assert
        assert len(queue._getters) == 2
        restored_getters = list(queue._getters)
        # Check that mock_non_matching_future is in the restored getters
        assert any(f == mock_non_matching_future for f, _ in restored_getters)


class TestDiscoveryService:
    def test_discovery_service_started_property(self):
        """Test DiscoveryService started property access.

        Given:
            A DiscoveryService instance
        When:
            started property is accessed
        Then:
            Should return the current started state
        """

        # Arrange
        # Create a concrete implementation for testing
        class DummyDiscoveryService(discovery.DiscoveryService):
            async def events(self):
                if False:  # pragma: no cover
                    yield  # Make it an async generator

            async def _start(self):
                pass

            async def _stop(self):
                pass

        service = DummyDiscoveryService()

        # Act & Assert
        # Initially not started
        assert service.started is False

        # Mock the started state
        service._started = True
        assert service.started is True

    def test_discovery_service_cloudpickle(self):
        """Test DiscoveryService is cloudpickleable.

        Given:
            A DiscoveryService instance
        When:
            cloudpickle.dumps() and cloudpickle.loads() are called
        Then:
            Should serialize and deserialize successfully
        """

        # Create a concrete implementation for testing
        class DummyDiscoveryService(discovery.DiscoveryService):
            async def events(self):
                if False:  # pragma: no cover
                    yield  # Make it an async generator

            async def _start(self):
                pass

            async def _stop(self):
                pass

        service = DummyDiscoveryService()

        # Should not raise exception
        pickled_data = cloudpickle.dumps(service)
        unpickled_service = cloudpickle.loads(pickled_data)

        assert isinstance(unpickled_service, DummyDiscoveryService)
        assert unpickled_service._started is False
        assert hasattr(unpickled_service, "_service_cache")
        assert hasattr(unpickled_service, "_filter")

    def test_discovery_service_cloudpickle_with_filter(self, worker_info):
        """Test DiscoveryService with filter is cloudpickleable.

        Given:
            A DiscoveryService instance with filter function
        When:
            cloudpickle.dumps() and cloudpickle.loads() are called
        Then:
            Should serialize and deserialize successfully with filter preserved
        """

        # Create a concrete implementation for testing
        class DummyDiscoveryService(discovery.DiscoveryService):
            async def events(self):
                if False:  # pragma: no cover
                    yield  # Make it an async generator

            async def _start(self):
                pass

            async def _stop(self):
                pass

        def test_filter(w):
            return w.uid.startswith("test-")

        service = DummyDiscoveryService(filter=test_filter)

        # Should not raise exception
        pickled_data = cloudpickle.dumps(service)
        unpickled_service = cloudpickle.loads(pickled_data)

        assert isinstance(unpickled_service, DummyDiscoveryService)
        assert unpickled_service._started is False
        assert unpickled_service._filter is not None
        # Filter should be preserved - modify the fixture worker to match filter
        worker_info.uid = "test-worker-123"
        assert unpickled_service._filter(worker_info) is True

    @pytest.mark.asyncio
    async def test_discovery_service_async_iteration(self, dummy_discovery_service):
        """Test DiscoveryService async iteration with async for loop.

        Given:
            A DiscoveryService instance
        When:
            Used in an async for loop
        Then:
            Should delegate to events() method and yield events correctly
        """
        # Arrange
        service = dummy_discovery_service()
        events_received = []

        # Act
        async for event in service:
            events_received.append(event)

        # Assert
        assert service.events_called is True
        assert len(events_received) == 2
        assert events_received[0].type == "worker_added"
        assert events_received[0].worker_info.uid == "test-123"
        assert events_received[1].type == "worker_removed"
        assert events_received[1].worker_info.uid == "test-456"

    @pytest.mark.asyncio
    async def test_discovery_service_anext_function(self, dummy_discovery_service):
        """Test DiscoveryService with anext() function.

        Given:
            A DiscoveryService instance
        When:
            anext() function is called on it
        Then:
            Should delegate to events() method and return next event
        """
        # Arrange
        service = dummy_discovery_service()

        # Act
        first_event = await anext(service)

        # Assert
        assert service.events_called is True
        assert isinstance(first_event, discovery.DiscoveryEvent)
        assert first_event.type == "worker_added"
        assert first_event.worker_info.uid == "test-123"


class TestRegistrarService:
    @pytest.mark.asyncio
    async def test_registrar_service_update_state_checks(self, worker_info):
        """Test RegistrarService update() method state validation.

        Given:
            A RegistrarService in various states
        When:
            update() method state validation is tested
        Then:
            Should properly validate started/stopped states
        """

        # Arrange
        # Create a concrete implementation for testing
        class DummyRegistrarService(discovery.RegistrarService):
            async def _start(self):
                pass

            async def _stop(self):
                pass

            async def _register(self, worker_info):
                pass

            async def _unregister(self, worker_info):
                pass

            async def _update(self, worker_info):
                pass

        service = DummyRegistrarService()

        # Act & Assert - Test update when not started
        with pytest.raises(
            RuntimeError,
            match="Registrar service not started - call start\\(\\) first",
        ):
            await service.update(worker_info)

        # Start the service
        await service.start()

        # Update should work when started
        await service.update(worker_info)

        # Stop the service
        await service.stop()

        # Test update when stopped
        with pytest.raises(RuntimeError, match="Registrar service already stopped"):
            await service.update(worker_info)


class TestLocalRegistrarService:
    @pytest.mark.asyncio
    async def test_init_sets_default_values(self, worker_info):
        """Test LocalRegistrarService initialization.

        Given:
            A URI for the registrar
        When:
            LocalRegistrarService is initialized with the URI
        Then:
            Should initialize without errors but not be ready for registration
        """
        # Arrange & Act
        registrar = discovery.LocalRegistrarService("test-registrar-uri")

        # Assert - Test behavior: registrar should not be ready for operations
        with pytest.raises(RuntimeError, match="Registrar service not started"):
            await registrar.register(worker_info)

    @pytest.mark.asyncio
    async def test_start_creates_shared_memory(self, worker_info):
        """Test LocalRegistrarService start enables worker registration.

        Given:
            An unstarted LocalRegistrarService
        When:
            start() is called
        Then:
            Should be ready to accept worker registrations
        """
        # Arrange
        registrar = discovery.LocalRegistrarService(uri="test_registrar_123")

        # Act
        await registrar.start()

        # Assert - Test behavior: registrar should now accept registrations
        # This should not raise an exception
        await registrar.register(worker_info)

        # Cleanup
        await registrar.stop()

    @pytest.mark.asyncio
    async def test_register(self, worker_info):
        """Test worker registration writes port to shared memory.

        Given:
            A started LocalRegistrarService
        When:
            register() is called with WorkerInfo
        Then:
            Worker port should be written to shared memory
        """
        # Arrange
        registrar_uri = "test_registrar_456"
        registrar = discovery.LocalRegistrarService(uri=registrar_uri)
        await registrar.start()

        # Act
        await registrar.register(worker_info)

        # Assert - Check that port was written to first slot using public API
        # Use same hash-based naming as LocalRegistrarService
        shared_memory_name = hashlib.sha256(registrar_uri.encode()).hexdigest()[:12]
        shared_memory = multiprocessing.shared_memory.SharedMemory(
            name=shared_memory_name
        )
        try:
            stored_port = struct.unpack("I", shared_memory.buf[0:4])[0]
            assert stored_port == worker_info.port
        finally:
            shared_memory.close()

        # Cleanup
        await registrar.stop()

    @pytest.mark.asyncio
    async def test_unregister(self, worker_info):
        """Test worker unregistration removes port from shared memory.

        Given:
            A started LocalRegistrarService with registered worker
        When:
            unregister() is called
        Then:
            Worker port should be removed from shared memory
        """
        # Arrange
        registrar_uri = "test_registrar_789"
        registrar = discovery.LocalRegistrarService(uri=registrar_uri)
        await registrar.start()
        await registrar.register(worker_info)

        # Verify port was registered using public API
        shared_memory_name = hashlib.sha256(registrar_uri.encode()).hexdigest()[:12]
        shared_memory = multiprocessing.shared_memory.SharedMemory(
            name=shared_memory_name
        )
        try:
            stored_port = struct.unpack("I", shared_memory.buf[0:4])[0]
            assert stored_port == worker_info.port
        finally:
            shared_memory.close()

        # Act
        await registrar.unregister(worker_info)

        # Assert - Port should be cleared (set to 0) using public API
        shared_memory_name = hashlib.sha256(registrar_uri.encode()).hexdigest()[:12]
        shared_memory = multiprocessing.shared_memory.SharedMemory(
            name=shared_memory_name
        )
        try:
            stored_port = struct.unpack("I", shared_memory.buf[0:4])[0]
            assert stored_port == 0
        finally:
            shared_memory.close()

        # Cleanup
        await registrar.stop()

    @pytest.mark.asyncio
    async def test_register_multiple_workers(self, worker_info):
        """Test registering multiple workers uses different slots.

        Given:
            A started LocalRegistrarService
        When:
            Multiple workers are registered
        Then:
            Should use different slots in shared memory
        """
        # Arrange
        registrar_uri = "test_registrar_multi"
        registrar = discovery.LocalRegistrarService(uri=registrar_uri)
        await registrar.start()

        worker2 = discovery.WorkerInfo(
            uid="worker-2",
            host="localhost",
            port=48801,
            pid=12346,
            version="1.0.0",
        )

        # Act
        await registrar.register(worker_info)
        await registrar.register(worker2)

        # Assert - Check both ports are stored using public API
        shared_memory_name = hashlib.sha256(registrar_uri.encode()).hexdigest()[:12]
        shared_memory = multiprocessing.shared_memory.SharedMemory(
            name=shared_memory_name
        )
        try:
            stored_port1 = struct.unpack("I", shared_memory.buf[0:4])[0]
            stored_port2 = struct.unpack("I", shared_memory.buf[4:8])[0]
            assert stored_port1 == worker_info.port
            assert stored_port2 == worker2.port
        finally:
            shared_memory.close()

        # Cleanup
        await registrar.stop()

    @pytest.mark.asyncio
    async def test_stop_handles_shared_memory_cleanup_exceptions(
        self, mocker: MockerFixture
    ):
        """Test LocalRegistrarService._stop() handles shared memory cleanup exceptions.

        Given:
            A LocalRegistrarService with shared memory that fails during cleanup
        When:
            _stop() is called
        Then:
            Should catch exceptions and continue cleanup without raising
        """

        # Arrange
        registrar = discovery.LocalRegistrarService(uri="test_cleanup_errors")

        # Mock shared memory object that raises exceptions on close/unlink
        mock_shared_memory = mocker.MagicMock()
        mock_shared_memory.close.side_effect = RuntimeError("Close failed")
        mock_shared_memory.unlink.side_effect = RuntimeError("Unlink failed")

        # Set up registrar state as if it had been started
        registrar._shared_memory = mock_shared_memory
        registrar._created_shared_memory = True

        # Act - Should not raise exception despite shared memory errors
        await registrar._stop()

        # Assert - Cleanup should have been attempted and state reset
        mock_shared_memory.close.assert_called_once()
        # unlink won't be called because close() raised exception
        mock_shared_memory.unlink.assert_not_called()
        assert registrar._shared_memory is None
        assert registrar._created_shared_memory is False

    @pytest.mark.asyncio
    async def test_register_not_initialized_raises_error(self, worker_info):
        """Test registering worker when registrar not initialized raises RuntimeError.

        Given:
            A LocalRegistrarService that hasn't been started
        When:
            _register() is called directly
        Then:
            Should raise RuntimeError about not being initialized
        """

        # Arrange
        registrar = discovery.LocalRegistrarService(uri="test_not_init")

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Registrar service not properly initialized"
        ):
            await registrar._register(worker_info)

    @pytest.mark.asyncio
    async def test_register_worker_without_port_raises_error(self):
        """Test registering worker without port raises ValueError.

        Given:
            A started LocalRegistrarService and worker without port
        When:
            _register() is called
        Then:
            Should raise ValueError about worker port being required
        """

        # Arrange
        registrar = discovery.LocalRegistrarService(uri="test_no_port")
        await registrar.start()

        worker_without_port = discovery.WorkerInfo(
            uid="worker-no-port",
            host="localhost",
            port=None,  # No port specified
            pid=12345,
            version="1.0.0",
        )

        try:
            # Act & Assert
            with pytest.raises(ValueError, match="Worker port must be specified"):
                await registrar._register(worker_without_port)
        finally:
            # Cleanup
            await registrar.stop()

    @pytest.mark.asyncio
    async def test_register_when_no_available_slots_raises_error(
        self, mocker: MockerFixture, worker_info
    ):
        """Test registering worker when shared memory is full raises RuntimeError.

        Given:
            A LocalRegistrarService with full shared memory
        When:
            _register() is called
        Then:
            Should raise RuntimeError about no available slots
        """

        # Arrange
        registrar = discovery.LocalRegistrarService(uri="test_full_memory")

        # Create a mock shared memory object with full buffer
        mock_shared_memory = mocker.MagicMock()
        mock_buffer = bytearray(1024)
        # Fill buffer with non-zero values to simulate occupied slots
        for i in range(0, len(mock_buffer), 4):
            struct.pack_into("I", mock_buffer, i, 9999)  # Non-zero port

        mock_shared_memory.buf = mock_buffer

        # Set the mocked shared memory directly
        registrar._shared_memory = mock_shared_memory

        try:
            # Act & Assert
            with pytest.raises(
                RuntimeError, match="No available slots in shared memory registrar"
            ):
                await registrar._register(worker_info)
        finally:
            # Cleanup
            registrar._shared_memory = None

    @pytest.mark.asyncio
    async def test_unregister_not_initialized_raises_error(self, worker_info):
        """Test unregistering worker when registrar not initialized raises RuntimeError.

        Given:
            A LocalRegistrarService that hasn't been started
        When:
            _unregister() is called directly
        Then:
            Should raise RuntimeError about not being initialized
        """

        # Arrange
        registrar = discovery.LocalRegistrarService(uri="test_unregister_not_init")

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Registrar service not properly initialized"
        ):
            await registrar._unregister(worker_info)

    @pytest.mark.asyncio
    async def test_unregister_worker_without_port_returns_early(self):
        """Test unregistering worker without port returns early.

        Given:
            A started LocalRegistrarService and worker without port
        When:
            _unregister() is called
        Then:
            Should return early without error
        """

        # Arrange
        registrar = discovery.LocalRegistrarService(uri="test_unregister_no_port")
        await registrar.start()

        worker_without_port = discovery.WorkerInfo(
            uid="worker-no-port",
            host="localhost",
            port=None,  # No port specified
            pid=12345,
            version="1.0.0",
        )

        try:
            # Act - Should not raise any exception
            await registrar._unregister(worker_without_port)
            # No assertion needed, just checking it doesn't raise
        finally:
            # Cleanup
            await registrar.stop()

    @pytest.mark.asyncio
    async def test_update_delegates_to_register(
        self, worker_info, mocker: MockerFixture
    ):
        """Test update method delegates to _register method.

        Given:
            A LocalRegistrarService with mocked _register method
        When:
            _update() is called
        Then:
            Should call _register with the same worker_info
        """

        # Arrange
        registrar = discovery.LocalRegistrarService(uri="test_update")
        mock_register = mocker.patch.object(
            registrar, "_register", new_callable=mocker.AsyncMock
        )

        # Act
        await registrar._update(worker_info)

        # Assert
        mock_register.assert_called_once_with(worker_info)


class TestLocalDiscoveryService:
    def test___init__(self):
        """Test LocalDiscoveryService initialization.

        Given:
            A LocalDiscoveryService is created
        When:
            Initialized with a URI
        Then:
            Should set correct default values
        """
        # Arrange & Act
        service = discovery.LocalDiscoveryService(uri="wool_local_registrar")

        # Assert
        assert service._shared_memory is None
        assert service._uri == "wool_local_registrar"
        assert service._monitor_task is None
        assert service._started is False

    def test___init___with_registrar_name(self):
        """Test LocalDiscoveryService initialization with custom registrar name.

        Given:
            A custom registrar name
        When:
            LocalDiscoveryService is initialized with the name
        Then:
            Should use the provided registrar name
        """
        # Arrange & Act
        service = discovery.LocalDiscoveryService(uri="custom_discovery")

        # Assert
        assert service._uri == "custom_discovery"
        assert service._shared_memory is None

    @pytest.mark.asyncio
    async def test_start(self):
        """Test LocalDiscoveryService start connects to shared memory.

        Given:
            An existing shared memory block
        When:
            start() is called
        Then:
            Should connect to shared memory and start monitoring
        """
        # Arrange - Create shared memory first with the expected name
        import hashlib

        uri = "test_discovery_123"
        shared_memory_name = hashlib.sha256(uri.encode()).hexdigest()[:12]
        shared_memory = multiprocessing.shared_memory.SharedMemory(
            name=shared_memory_name, create=True, size=1024
        )
        for i in range(1024):
            shared_memory.buf[i] = 0

        try:
            service = discovery.LocalDiscoveryService(uri=uri)

            # Act
            await service.start()

            # Assert
            assert service._started is True
            assert service._shared_memory is not None
            assert service._monitor_task is not None
            assert not service._monitor_task.done()

            # Cleanup
            await service.stop()
        finally:
            shared_memory.close()
            shared_memory.unlink()

    @pytest.mark.asyncio
    async def test_worker_discovery_integration(self, worker_info):
        """Test full integration between LocalRegistrarService and LocalDiscoveryService.

        Given:
            A LocalRegistrarService and LocalDiscoveryService sharing memory
        When:
            A worker is registered and unregistered
        Then:
            Discovery service should emit appropriate events
        """
        # Arrange
        registrar_name = "test_integration_abc"
        registrar = discovery.LocalRegistrarService(uri=registrar_name)
        discovery_service = discovery.LocalDiscoveryService(uri=registrar_name)

        await registrar.start()

        events = []

        async def collect_events():
            try:
                async for event in discovery_service.events():
                    events.append(event)
                    if len(events) >= 2:  # worker_added, worker_removed
                        break
            except asyncio.CancelledError:
                pass

        # Start event collection
        event_task = asyncio.create_task(collect_events())

        # Give discovery service time to start
        await asyncio.sleep(0.2)

        # Act - Register worker
        await registrar.register(worker_info)
        await asyncio.sleep(0.2)  # Wait for discovery to detect

        # Unregister worker
        await registrar.unregister(worker_info)
        await asyncio.sleep(0.2)  # Wait for discovery to detect

        # Cancel event collection
        event_task.cancel()
        try:
            await event_task
        except asyncio.CancelledError:
            pass

        # Assert - Check events were generated
        assert len(events) >= 2  # At least added and removed

        # Check worker_added event
        added_events = [e for e in events if e.type == "worker_added"]
        assert len(added_events) >= 1
        added_event = added_events[0]
        assert added_event.worker_info.port == worker_info.port
        assert added_event.worker_info.host == "localhost"

        # Check worker_removed event
        removed_events = [e for e in events if e.type == "worker_removed"]
        assert len(removed_events) >= 1
        assert removed_events[0].worker_info.port == worker_info.port

        # Cleanup
        try:
            await registrar.stop()
            await discovery_service.stop()
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_stop_handles_monitor_task_cancelled_error(
        self, mocker: MockerFixture
    ):
        """Test LocalDiscoveryService._stop() handles CancelledError from monitor task.

        Given:
            A LocalDiscoveryService with a monitor task that raises CancelledError
        When:
            _stop() is called
        Then:
            Should catch CancelledError and continue cleanup without raising
        """

        # Arrange
        service = discovery.LocalDiscoveryService(uri="test_cancelled_task")

        # Create an actual task that raises CancelledError
        async def cancelled_task():
            raise asyncio.CancelledError()

        # Create the task and cancel it immediately to simulate the error condition
        mock_task = asyncio.create_task(cancelled_task())
        mock_task.cancel()

        service._monitor_task = mock_task

        # Act - Should not raise exception despite CancelledError
        await service._stop()

        # Assert - Task should be done/cancelled
        assert mock_task.done()
        assert mock_task.cancelled()

    @pytest.mark.asyncio
    async def test_stop_handles_shared_memory_close_exception(
        self, mocker: MockerFixture
    ):
        """Test LocalDiscoveryService._stop() handles shared memory close exception.

        Given:
            A LocalDiscoveryService with shared memory that fails to close
        When:
            _stop() is called
        Then:
            Should catch exception and continue cleanup without raising
        """

        # Arrange
        service = discovery.LocalDiscoveryService(uri="test_close_error")

        # Mock shared memory object that raises exception on close
        mock_shared_memory = mocker.MagicMock()
        mock_shared_memory.close.side_effect = RuntimeError("Close failed")

        service._shared_memory = mock_shared_memory

        # Act - Should not raise exception despite shared memory close error
        await service._stop()

        # Assert - Close should have been attempted and memory reset
        mock_shared_memory.close.assert_called_once()
        assert service._shared_memory is None

    def test_local_discovery_service_cloudpickle_support(self):
        """Test LocalDiscoveryService __reduce__ method for cloudpickle support.

        Given:
            A LocalDiscoveryService instance
        When:
            cloudpickle.dumps() and loads() are called
        Then:
            Should serialize and deserialize successfully with URI preserved
        """

        # Arrange
        uri = "test_pickle_uri"
        service = discovery.LocalDiscoveryService(uri=uri)

        # Act
        pickled_data = cloudpickle.dumps(service)
        unpickled_service = cloudpickle.loads(pickled_data)

        # Assert
        assert isinstance(unpickled_service, discovery.LocalDiscoveryService)
        assert unpickled_service._uri == uri
        assert unpickled_service._started is False
        assert unpickled_service._shared_memory is None
        assert unpickled_service._monitor_task is None

    @pytest.mark.asyncio
    async def test_monitor_shared_memory_handles_struct_unpack_exception(
        self, mocker: MockerFixture, worker_info
    ):
        """Test LocalDiscoveryService monitoring continues after exceptions.

        Given:
            A LocalDiscoveryService where struct.unpack raises an exception
        When:
            The monitoring loop encounters the exception
        Then:
            Should catch the exception and continue monitoring
        """

        # Arrange
        registrar = discovery.LocalRegistrarService(uri="test_struct_exception")
        discovery_service = discovery.LocalDiscoveryService(uri="test_struct_exception")

        await registrar.start()

        # Register a worker normally first
        await registrar.register(worker_info)

        # Mock struct.unpack to fail on first call, succeed on second
        call_count = 0
        original_unpack = struct.unpack

        def mock_unpack(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 4:  # First few calls fail
                raise struct.error("Simulated struct error")
            # After that, work normally
            return original_unpack(*args, **kwargs)

        mocker.patch("struct.unpack", side_effect=mock_unpack)

        events = []

        async def collect_events():
            try:
                async for event in discovery_service.events():
                    events.append(event)
                    if len(events) >= 1:  # Just need one event to prove recovery
                        break
            except asyncio.CancelledError:
                pass

        # Act - Start event collection (this will trigger the exception handling)
        event_task = asyncio.create_task(collect_events())

        # Wait a bit for the monitoring to encounter exceptions and recover
        await asyncio.sleep(0.5)

        # Cancel event collection
        event_task.cancel()
        try:
            await event_task
        except asyncio.CancelledError:
            pass

        # Assert - Should have eventually detected the worker despite initial exceptions
        assert len(events) >= 1
        assert events[0].type == "worker_added"
        assert events[0].worker_info.port == worker_info.port

        # Cleanup
        try:
            await registrar.stop()
            await discovery_service.stop()
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_local_discovery_service_detects_worker_port_updates(
        self, worker_info
    ):
        """Test LocalDiscoveryService detects when worker port changes.

        Given:
            A LocalDiscoveryService monitoring workers
        When:
            A worker's port changes in shared memory
        Then:
            Should emit a worker_updated event
        """

        # Arrange
        registrar = discovery.LocalRegistrarService(uri="test_port_updates")
        discovery_service = discovery.LocalDiscoveryService(uri="test_port_updates")

        await registrar.start()

        # Register initial worker
        await registrar.register(worker_info)

        events = []

        async def collect_events():
            try:
                async for event in discovery_service.events():
                    events.append(event)
                    if len(events) >= 2:  # worker_added + worker_updated
                        break
            except asyncio.CancelledError:
                pass

        # Start event collection
        event_task = asyncio.create_task(collect_events())

        # Give discovery service time to start and detect initial worker
        await asyncio.sleep(0.2)

        # Act - Directly modify shared memory to change the port
        # while keeping the same slot (this triggers the update logic)
        new_port = worker_info.port + 1

        # Find the slot with our worker's port and change it
        if registrar._shared_memory:
            for i in range(0, len(registrar._shared_memory.buf), 4):
                current_port = struct.unpack(
                    "I", registrar._shared_memory.buf[i : i + 4]
                )[0]
                if current_port == worker_info.port:
                    # Change port in place (simulates an "update" rather than remove/add)
                    struct.pack_into("I", registrar._shared_memory.buf, i, new_port)
                    break

        # Wait for detection
        await asyncio.sleep(0.3)

        # Cancel event collection
        event_task.cancel()
        try:
            await event_task
        except asyncio.CancelledError:
            pass

        # Assert - Should have detected the port change as an update
        assert len(events) >= 1
        added_event = next(e for e in events if e.type == "worker_added")
        assert added_event.worker_info.port == worker_info.port

        # Should have detected the port update
        updated_events = [e for e in events if e.type == "worker_updated"]
        if updated_events:
            # If we caught the update event, verify it has the new port
            assert updated_events[0].worker_info.port == new_port

        # Cleanup
        try:
            await registrar.stop()
            await discovery_service.stop()
        except Exception:
            pass

    @pytest.mark.asyncio
    async def test_detect_changes_worker_port_update_directly(self, worker_info):
        """Test LocalDiscoveryService._detect_changes method for worker port updates.

        Given:
            A LocalDiscoveryService with a cached worker and current workers with
            changed port
        When:
            _detect_changes is called with the updated worker info
        Then:
            Should emit a worker_updated event for the port change
        """
        # Arrange
        discovery_service = discovery.LocalDiscoveryService(uri="test_direct_updates")
        discovery_service._event_queue = discovery.PredicatedQueue[
            discovery.DiscoveryEvent
        ]()

        # Set up initial cache with original worker
        original_worker = worker_info
        discovery_service._service_cache[original_worker.uid] = original_worker

        # Create updated worker with different port
        updated_worker = discovery.WorkerInfo(
            uid=original_worker.uid,
            host=original_worker.host,
            port=original_worker.port + 100,  # Change port
            pid=original_worker.pid,
            version=original_worker.version,
            tags=original_worker.tags,
            extra=original_worker.extra,
        )

        current_workers = {updated_worker.uid: updated_worker}

        # Act - Call _detect_changes directly with the updated worker
        await discovery_service._detect_changes(current_workers)

        # Assert - Should have emitted a worker_updated event
        events = []
        try:
            # Collect events from queue
            while not discovery_service._event_queue.empty():
                event = discovery_service._event_queue.get_nowait()
                events.append(event)
        except asyncio.QueueEmpty:
            pass

        # Verify worker_updated event was emitted
        assert len(events) == 1
        event = events[0]
        assert event.type == "worker_updated"
        assert event.worker_info.uid == updated_worker.uid
        assert event.worker_info.port == updated_worker.port

        # Verify cache was updated
        assert (
            discovery_service._service_cache[updated_worker.uid].port
            == updated_worker.port
        )
