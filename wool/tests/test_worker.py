import uuid
from typing import Callable
from typing import Coroutine
from typing import cast

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

from wool import _worker as svc
from wool._worker import WorkerService
from wool._worker_proxy import WorkerProxy
from wool.core.discovery.base import WorkerInfo
from wool.core.protobuf.worker import WorkerStub
from wool.core.protobuf.worker import add_WorkerServicer_to_server


@pytest.fixture(scope="function")
def grpc_add_to_server():
    return add_WorkerServicer_to_server


@pytest.fixture(scope="function")
def grpc_servicer():
    return WorkerService()


@pytest.fixture(scope="function")
def grpc_stub_cls():
    return WorkerStub


@pytest.fixture
def mock_worker_process(mocker):
    """Create a mock worker process with default configuration."""
    mock_process = mocker.MagicMock()
    mock_process.address = "192.168.1.100:50051"
    mock_process.pid = 12345
    mock_process.start = mocker.MagicMock()
    mock_process.join = mocker.MagicMock()
    mock_process.kill = mocker.MagicMock()
    mock_process.is_alive.return_value = True
    return mock_process


@pytest.fixture
def configured_local_worker(mock_worker_process, mocker):
    """Create a LocalWorker with mocked dependencies."""
    mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)
    worker = svc.LocalWorker("tag1", "tag2")
    return worker


@pytest.fixture
def mock_grpc_server(mocker):
    """Create a mock gRPC server for testing."""
    mock_server = mocker.AsyncMock()
    mock_server.add_insecure_port.return_value = 50051
    mock_server.start = mocker.AsyncMock()
    mock_server.stop = mocker.AsyncMock()
    return mock_server


@pytest.fixture
def sample_worker_info():
    """Create a sample WorkerInfo for testing."""
    return WorkerInfo(
        uid="test-uid",
        host="192.168.1.100",
        port=50051,
        pid=12345,
        tags=frozenset({"tag1", "tag2"}),
        version="0.1.0",
    )


@pytest.fixture
def mock_worker_service(mocker):
    """Create a mock WorkerService for testing."""
    mock_service = mocker.MagicMock()
    mock_service._stopped = mocker.MagicMock()
    mock_service._stopping = mocker.MagicMock()
    mock_service._task_completed = mocker.MagicMock()
    mock_service._tasks = set()
    mock_service.stopped = mocker.MagicMock()
    mock_service.stopping = mocker.MagicMock()
    mock_service._stop = mocker.AsyncMock()  # This is actually async
    return mock_service


class TestLocalWorker:
    def test_address_property_when_process_has_address(self, mocker: MockerFixture):
        """Test :class:`LocalWorker` address property returns process address
        when available.

        Given:
            A :class:`LocalWorker` with a worker process that has an address
            set
        When:
            The address property is accessed
        Then:
            It should return the worker process address
        """
        # Arrange
        mock_worker_process = mocker.patch.object(svc, "WorkerProcess")
        mock_worker_process.return_value.address = "192.168.1.100:50051"
        mock_registrar = mocker.MagicMock()

        worker = svc.LocalWorker()

        # Act
        result = worker.address

        # Assert
        assert result == "192.168.1.100:50051"

    def test_address_property_when_process_has_no_address(self, mocker: MockerFixture):
        """Test LocalWorker address property returns None when process has no
        address.

        Given:
            A LocalWorker with a worker process that has no address set
        When:
            The address property is accessed
        Then:
            It should return None
        """
        # Arrange
        mock_worker_process = mocker.patch.object(svc, "WorkerProcess")
        mock_worker_process.return_value.address = None
        mock_registrar = mocker.MagicMock()

        worker = svc.LocalWorker()

        # Act
        result = worker.address

        # Assert
        assert result is None

    @pytest.mark.parametrize(
        "property_name,expected_value",
        [
            ("info", None),
            ("host", None),
            ("port", None),
        ],
    )
    def test_properties_return_none_when_worker_not_started(
        self, property_name: str, expected_value, mocker: MockerFixture
    ):
        """Test LocalWorker properties return None when worker not started.

        Given:
            A LocalWorker that has not been started
        When:
            A property is accessed
        Then:
            It should return None
        """
        # Arrange
        mock_registrar = mocker.MagicMock()
        worker = svc.LocalWorker()

        # Act
        result = getattr(worker, property_name)

        # Assert
        assert result == expected_value

    def test_extra_property_returns_extra_data(self, mocker: MockerFixture):
        """Test LocalWorker extra property returns extra data.

        Given:
            A LocalWorker with extra keyword arguments
        When:
            The extra property is accessed
        Then:
            It should return the extra data dictionary
        """
        # Arrange
        mock_registrar = mocker.MagicMock()
        worker = svc.LocalWorker("tag1", key1="value1", key2="value2")

        # Act
        result = worker.extra

        # Assert
        assert result == {"key1": "value1", "key2": "value2"}

    @pytest.mark.asyncio
    async def test_start_initializes_worker_process(
        self, configured_local_worker, mock_worker_process
    ):
        """Test :class:`LocalWorker` :meth:`_start` method initializes
        worker process.

        Given:
            A :class:`LocalWorker` instance with mocked worker process
        When:
            :meth:`_start` is called
        Then:
            It should start worker process and create WorkerInfo
        """
        # Act
        await configured_local_worker._start(timeout=60.0)

        # Assert
        # Verify that worker process start was called (via run_in_executor)
        mock_worker_process.start.assert_called_once_with(timeout=60.0)
        # Verify WorkerInfo was created
        assert configured_local_worker._info is not None
        assert isinstance(configured_local_worker._info.uid, uuid.UUID)
        assert configured_local_worker._info.host == "192.168.1.100"
        assert configured_local_worker._info.port == 50051
        assert configured_local_worker._info.pid == 12345
        assert "tag1" in configured_local_worker._info.tags
        assert "tag2" in configured_local_worker._info.tags

    @pytest.mark.asyncio
    async def test_stop_performs_graceful_shutdown_when_process_alive(
        self,
        configured_local_worker,
        mock_worker_process,
        mocker: MockerFixture,
    ):
        """Test LocalWorker _stop method performs graceful shutdown.

        Given:
            A LocalWorker instance with an alive process
        When:
            _stop() is called and process responds to gRPC stop request
        Then:
            It should call stub.stop() via gRPC
        """
        # Arrange
        await configured_local_worker._start(
            timeout=60.0
        )  # Need to start first to set _info

        # Mock the gRPC channel and stub
        mock_channel = mocker.MagicMock()
        mock_stub = mocker.MagicMock()
        mock_stub.stop = mocker.AsyncMock()

        mocker.patch.object(svc.grpc.aio, "insecure_channel", return_value=mock_channel)
        mocker.patch.object(svc.pb.worker, "WorkerStub", return_value=mock_stub)

        # Act
        await configured_local_worker._stop(timeout=None)

        # Assert
        # Check that the worker process was gracefully stopped via gRPC
        mock_stub.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_returns_early_when_process_not_alive(
        self, mocker: MockerFixture
    ):
        """Test LocalWorker _stop method when process is already dead.

        Given:
            A LocalWorker instance with a process that is not alive
        When:
            _stop() is called
        Then:
            It should return early without attempting to stop the process
        """
        # Arrange
        mock_lan_registrar = mocker.MagicMock()
        mock_lan_registrar.unregister = mocker.AsyncMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = "192.168.1.100:50051"
        mock_worker_process.is_alive.return_value = False
        mock_worker_process.pid = None  # Process is dead, no PID
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        # Mock the gRPC stub (should not be called)
        mock_stub = mocker.MagicMock()
        mock_stub.stop = mocker.AsyncMock()
        mocker.patch.object(svc.pb.worker, "WorkerStub", return_value=mock_stub)

        worker = svc.LocalWorker()
        # Set up the _registrar_service that would normally be set by start()
        worker._registrar_service = mock_lan_registrar
        # Need to manually set up _info for this test since process is dead
        worker._info = WorkerInfo(
            uid=worker.uid,
            host="192.168.1.100",
            port=50051,
            pid=0,
            version="test",
            tags=frozenset(worker.tags),
        )

        # Act
        await worker._stop(timeout=None)

        # Assert
        # Check that no gRPC stop was attempted (process not alive)
        mock_stub.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_handles_grpc_errors_gracefully(self, mocker: MockerFixture):
        """Test LocalWorker _stop method handles gRPC errors gracefully.

        Given:
            A LocalWorker instance where gRPC stub.stop() raises an error
        When:
            _stop() is called and gRPC call fails
        Then:
            It should handle the error without crashing
        """
        # Arrange
        mock_lan_registrar = mocker.MagicMock()
        mock_lan_registrar.register = mocker.AsyncMock()
        mock_lan_registrar.unregister = mocker.AsyncMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = "192.168.1.100:50051"
        mock_worker_process.is_alive.return_value = True
        mock_worker_process.pid = 12345
        mock_worker_process.start = mocker.MagicMock()
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        # Mock the gRPC stub to raise an error
        mock_channel = mocker.MagicMock()
        mock_stub = mocker.MagicMock()
        mock_stub.stop = mocker.AsyncMock(
            side_effect=Exception("gRPC connection failed")
        )

        mocker.patch.object(svc.grpc.aio, "insecure_channel", return_value=mock_channel)
        mocker.patch.object(svc.pb.worker, "WorkerStub", return_value=mock_stub)

        worker = svc.LocalWorker()
        # Set up the _registrar_service that would normally be set by start()
        worker._registrar_service = mock_lan_registrar
        await worker._start(timeout=60.0)  # Need to start first to set _info

        # Act & Assert
        # Should raise the exception from stub.stop()
        with pytest.raises(Exception, match="gRPC connection failed"):
            await worker._stop(timeout=None)

    @pytest.mark.asyncio
    async def test_stop_processes_worker_shutdown_independently(
        self, mocker: MockerFixture
    ):
        """Test LocalWorker _stop method handles worker process shutdown.

        Given:
            A LocalWorker instance with a running process
        When:
            _stop() is called
        Then:
            It should call gRPC stub.stop() to shutdown the worker
        """
        # Arrange
        mock_lan_registrar = mocker.MagicMock()
        mock_lan_registrar.register = mocker.AsyncMock()
        mock_lan_registrar.unregister = mocker.AsyncMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = "192.168.1.100:50051"
        mock_worker_process.is_alive.return_value = True
        mock_worker_process.pid = 12345
        mock_worker_process.start = mocker.MagicMock()
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        # Mock the gRPC channel and stub
        mock_channel = mocker.MagicMock()
        mock_stub = mocker.MagicMock()
        mock_stub.stop = mocker.AsyncMock()

        mocker.patch.object(svc.grpc.aio, "insecure_channel", return_value=mock_channel)
        mocker.patch.object(svc.pb.worker, "WorkerStub", return_value=mock_stub)

        worker = svc.LocalWorker()
        # Set up the _registrar_service that would normally be set by start()
        worker._registrar_service = mock_lan_registrar
        await worker._start(timeout=60.0)  # Need to start first to set _info

        # Act
        await worker._stop(timeout=None)

        # Assert
        # Verify worker process shutdown was handled via gRPC
        mock_worker_process.is_alive.assert_called_once()
        mock_stub.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_raises_error_when_already_started(self, mocker: MockerFixture):
        """Test LocalWorker start raises error when already started.

        Given:
            A LocalWorker that has already been started
        When:
            start() is called again
        Then:
            It should raise RuntimeError with "Worker has already been started"
        """
        # Arrange
        mock_lan_registrar = mocker.MagicMock()

        worker = svc.LocalWorker()
        worker._started = True  # Simulate already started

        # Act & Assert
        with pytest.raises(RuntimeError, match="Worker has already been started"):
            await worker.start()

    @pytest.mark.asyncio
    async def test_stop_raises_error_when_not_started(self, mocker: MockerFixture):
        """Test LocalWorker stop raises error when not started.

        Given:
            A LocalWorker that has never been started
        When:
            stop() is called
        Then:
            It should raise RuntimeError with "Worker has not been started"
        """
        # Arrange
        mock_lan_registrar = mocker.MagicMock()

        worker = svc.LocalWorker()
        # _started is False by default

        # Act & Assert
        with pytest.raises(RuntimeError, match="Worker has not been started"):
            await worker.stop()

    @pytest.mark.asyncio
    async def test_start_raises_error_when_no_address(self, mocker: MockerFixture):
        """Test LocalWorker start raises error when worker process has no
        address.

        Given:
            A LocalWorker with mocked WorkerProcess that returns None for
            address
        When:
            start() is called
        Then:
            It should raise RuntimeError
        """
        # Arrange
        mock_lan_registrar = mocker.MagicMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = None  # No address
        mock_worker_process.pid = 12345
        mock_worker_process.start = mocker.MagicMock()
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        worker = svc.LocalWorker()

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Worker process failed to start - no address"
        ):
            await worker._start(timeout=60.0)

    @pytest.mark.asyncio
    async def test_start_raises_error_when_no_pid(self, mocker: MockerFixture):
        """Test LocalWorker start raises error when worker process has no PID.

        Given:
            A LocalWorker with mocked WorkerProcess that returns None for pid
        When:
            start() is called
        Then:
            It should raise RuntimeError with "Worker process failed to start
            - no PID"
        """
        # Arrange
        mock_lan_registrar = mocker.MagicMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = "192.168.1.100:50051"
        mock_worker_process.pid = None  # No PID
        mock_worker_process.start = mocker.MagicMock()
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        worker = svc.LocalWorker()

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Worker process failed to start - no PID"
        ):
            await worker._start(timeout=60.0)

    @pytest.mark.asyncio
    async def test_stop_safely_handles_no_worker_info(self, mocker: MockerFixture):
        """Test LocalWorker _stop safely handles case when no WorkerInfo exists.

        Given:
            A LocalWorker with no WorkerInfo (_info is None)
        When:
            _stop() is called
        Then:
            It should handle the case gracefully without errors
        """
        # Arrange
        mock_lan_registrar = mocker.MagicMock()
        mock_lan_registrar.unregister = mocker.AsyncMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = None  # No address
        mock_worker_process.is_alive.return_value = False  # Process not alive
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        worker = svc.LocalWorker()
        # Don't call _start(), so _info remains None

        # Act - should not raise any exception
        await worker._stop(timeout=None)

    @pytest.mark.asyncio
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(timeout=st.floats(min_value=0.001, max_value=3600.0))
    async def test_worker_start_accepts_valid_timeouts(
        self, timeout: float, mocker: MockerFixture
    ):
        """Test Worker.start() accepts various valid timeout values.

        Given:
            A valid positive timeout value
        When:
            Worker.start() is called with that timeout
        Then:
            It should accept the timeout without raising ValueError
        """
        # Arrange
        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = "192.168.1.100:50051"
        mock_worker_process.pid = 12345
        mock_worker_process.start = mocker.MagicMock()
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        worker = svc.LocalWorker()

        # Act - should not raise ValueError
        await worker.start(timeout=timeout)

        # Assert
        assert worker._started is True

    @pytest.mark.asyncio
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(
        timeout=st.one_of(
            st.just(0.0),
            st.floats(
                min_value=-1000.0,
                max_value=-0.001,
                allow_nan=False,
                allow_infinity=False,
            ),
        )
    )
    async def test_start_rejects_non_positive_timeouts(
        self, timeout: float, mocker: MockerFixture
    ):
        """Test Worker.start() rejects non-positive timeout values.

        Given:
            A timeout value that is zero or negative
        When:
            start() is called with that timeout
        Then:
            It should raise ValueError with "Timeout must be positive"
        """
        # Arrange
        worker = svc.LocalWorker()

        # Act & Assert
        with pytest.raises(ValueError, match="Timeout must be positive"):
            await worker.start(timeout=timeout)

    @pytest.mark.asyncio
    async def test_worker_info_creation_with_invalid_address(
        self, mocker: MockerFixture
    ):
        """Test LocalWorker handles malformed worker process address.

        Given:
            A LocalWorker with worker process returning malformed address
        When:
            _start() is called
        Then:
            Should handle address parsing gracefully
        """
        # Arrange
        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = "invalid:address:format:too:many:colons"
        mock_worker_process.pid = 12345
        mock_worker_process.start = mocker.MagicMock()
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        worker = svc.LocalWorker()

        # Act & Assert
        with pytest.raises(ValueError):
            await worker._start(timeout=60.0)

    def test_worker_tags_modification_behavior(self):
        """Test LocalWorker tags behavior when modified.

        Given:
            A LocalWorker with initial tags
        When:
            Tags property is accessed and modified
        Then:
            Should reflect the changes (tags are mutable)
        """
        # Arrange
        worker = svc.LocalWorker("tag1", "tag2")
        original_tags = worker.tags.copy()

        # Act
        worker.tags.add("additional_tag")

        # Assert
        assert "additional_tag" in worker.tags
        assert len(worker.tags) == len(original_tags) + 1

    @pytest.mark.asyncio
    def test_init_sets_port_when_provided(self):
        """Test :class:`WorkerProcess` initialization with specific port.

        Given:
            A positive port number
        When:
            :class:`WorkerProcess` is initialized
        Then:
            It should set ``_port`` to the specified value
        """
        # Act
        process = svc.WorkerProcess(port=8080)

        # Assert
        assert process.port == 8080

    def test_init_raises_error_for_negative_port(self):
        """Test :class:`WorkerProcess` initialization raises error for
        negative port.

        Given:
            A negative port number
        When:
            :class:`WorkerProcess` is initialized
        Then:
            It should raise :exc:`ValueError` with appropriate message
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Port must be a positive integer"):
            svc.WorkerProcess(port=-1)

    def test_init_raises_error_for_blank_host(self):
        """Test WorkerProcess initialization raises error for blank host.

        Given:
            An empty host string
        When:
            WorkerProcess is initialized
        Then:
            It should raise ValueError with "Host must be a non-blank string"
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Host must be a non-blank string"):
            svc.WorkerProcess(host="")

    def test_host_property_returns_host(self):
        """Test WorkerProcess host property returns the configured host.

        Given:
            A WorkerProcess with a specific host
        When:
            The host property is accessed
        Then:
            It should return the configured host
        """
        # Arrange
        process = svc.WorkerProcess(host="192.168.1.100")

        # Act
        result = process.host

        # Assert
        assert result == "192.168.1.100"

    def test_address_property_with_port(self, mocker: MockerFixture):
        """Test WorkerProcess address property returns formatted address.

        Given:
            A WorkerProcess with a port set
        When:
            The address property is accessed
        Then:
            It should return formatted address string with localhost and port
        """
        # Arrange
        process = svc.WorkerProcess(port=50051)

        # Act
        result = process.address

        # Assert
        assert result == "127.0.0.1:50051"

    def test_address_property_no_port(self):
        """Test :class:`WorkerProcess` address property returns None when no
        port set.

        Given:
            A :class:`WorkerProcess` with no port set
        When:
            The address property is accessed
        Then:
            It should return None
        """
        # Arrange
        process = svc.WorkerProcess()

        # Act
        result = process.address

        # Assert
        assert result is None

    def test_worker_process_startup_timeout(self, mocker: MockerFixture):
        """Test WorkerProcess start method timeout handling.

        Given:
            A WorkerProcess instance with pipe that times out
        When:
            start() is called and pipe.poll() returns False (timeout)
        Then:
            It should terminate, join, and raise RuntimeError
        """
        # Arrange
        mock_super_start = mocker.patch("multiprocessing.Process.start")
        mock_terminate = mocker.patch("multiprocessing.Process.terminate")
        mock_join = mocker.patch("multiprocessing.Process.join")

        mock_get_port = mocker.MagicMock()
        mock_get_port.poll.return_value = False  # Simulate timeout

        process = svc.WorkerProcess()
        process._get_port = mock_get_port

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Worker process failed to start within 10.0 seconds"
        ):
            process.start(timeout=10.0)

        # Assert
        mock_super_start.assert_called_once()
        mock_terminate.assert_called_once()
        mock_join.assert_called_once()
        mock_get_port.poll.assert_called_once_with(timeout=10.0)

    def test_start(self, mocker: MockerFixture):
        """Test WorkerProcess start method initializes process and port.

        Given:
            A WorkerProcess instance with mocked parent start and pipe
        When:
            start() is called
        Then:
            It should call parent start, receive port from pipe, and close pipe
        """
        # Arrange
        mock_super_start = mocker.patch("multiprocessing.Process.start")
        mock_get_port = mocker.MagicMock()
        mock_get_port.poll.return_value = True  # Simulate successful start
        mock_get_port.recv.return_value = 50051
        mock_get_port.close = mocker.MagicMock()

        process = svc.WorkerProcess()
        process._get_port = mock_get_port

        # Act
        process.start()

        # Assert
        mock_super_start.assert_called_once()
        mock_get_port.recv.assert_called_once()
        mock_get_port.close.assert_called_once()
        assert process._port == 50051

    @pytest.mark.asyncio
    async def test_run_sets_up_grpc_server_correctly(self, mocker: MockerFixture):
        """Test WorkerProcess run method sets up gRPC server correctly.

        Given:
            A WorkerProcess instance with mocked gRPC components
        When:
            _serve() is called directly in an async context
        Then:
            It should create gRPC server, start it, send port, and wait for
            stop
        """
        # Arrange
        mock_server = mocker.MagicMock()
        mock_server.add_insecure_port.return_value = 50051
        mock_server.start = mocker.AsyncMock()
        mock_server.stop = mocker.AsyncMock()
        mocker.patch.object(svc.grpc.aio, "server", return_value=mock_server)

        mock_service = mocker.MagicMock()
        mock_service.stopped.wait = mocker.AsyncMock()
        mock_service._shutdown_grace_period = 60.0  # Set the actual value
        mocker.patch.object(svc, "WorkerService", return_value=mock_service)

        mock_add_to_server = mocker.patch.object(svc.pb, "add_to_server")
        mock_signal = mocker.patch.object(svc.signal, "signal")

        process = svc.WorkerProcess()
        mock_set_port = mocker.MagicMock()
        process._set_port = mock_set_port

        # Act
        await process._serve()

        # Assert
        mock_server.add_insecure_port.assert_called_once()
        mock_add_to_server.__getitem__.assert_called_once()
        mock_server.start.assert_called_once()
        mock_set_port.send.assert_called_once_with(50051)
        mock_set_port.close.assert_called_once()
        mock_service.stopped.wait.assert_called_once()
        # Verify that the service's _shutdown_grace_period was used
        assert mock_server.stop.call_count == 1
        call_args = mock_server.stop.call_args
        assert call_args[1]["grace"] == 60.0  # Default value
        # Verify signal handlers were set up and restored
        assert mock_signal.call_count == 4  # 2 setups + 2 restores

    @pytest.mark.asyncio
    async def test_worker_process_startup_failure(self, mocker: MockerFixture):
        """Test WorkerProcess proxy factory startup failure handling.

        Given:
            A WorkerProxy that is not started and proxy.start() fails
        When:
            proxy_factory is called with the proxy
        Then:
            It should try to start the proxy and propagate the exception
        """
        # Arrange
        mock_proxy = mocker.MagicMock()
        mock_proxy.started = False
        mock_proxy.start = mocker.AsyncMock(side_effect=RuntimeError("Start failed"))

        process = svc.WorkerProcess()

        # Extract the proxy_factory function by mocking the run method
        proxy_factory = cast(Callable[[WorkerProxy], Coroutine], None)

        def capture_proxy_factory(*args, factory=None, **kwargs):
            nonlocal proxy_factory
            # Find the ResourcePool call and extract the factory
            if factory:
                proxy_factory = factory

        mocker.patch.object(svc, "ResourcePool", side_effect=capture_proxy_factory)
        mocker.patch.object(svc.asyncio, "run")

        # Call run to set up the proxy_factory
        process.run()

        # Act & Assert
        assert proxy_factory is not None
        with pytest.raises(RuntimeError, match="Start failed"):
            await proxy_factory(mock_proxy)

        mock_proxy.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_worker_process_proxy_factory_already_started(
        self, mocker: MockerFixture
    ):
        """Test WorkerProcess proxy factory when proxy is already started.

        Given:
            A WorkerProxy that is already started
        When:
            proxy_factory is called with the proxy
        Then:
            It should return the proxy without calling start
        """
        # Arrange
        mock_proxy = mocker.MagicMock()
        mock_proxy.started = True
        mock_proxy.start = mocker.AsyncMock()

        process = svc.WorkerProcess()

        # Extract the proxy_factory function by mocking the run method
        proxy_factory = cast(Callable[[WorkerProxy], Coroutine], None)

        def capture_proxy_factory(*args, factory=None, **kwargs):
            nonlocal proxy_factory
            if factory:
                proxy_factory = factory

        mocker.patch.object(svc, "ResourcePool", side_effect=capture_proxy_factory)
        mocker.patch.object(svc.asyncio, "run")

        # Call run to set up the proxy_factory
        process.run()

        # Act
        assert proxy_factory is not None
        result = await proxy_factory(mock_proxy)

        # Assert
        assert result == mock_proxy
        mock_proxy.start.assert_not_called()

    @pytest.mark.asyncio
    async def test_worker_process_run_method(self, mocker: MockerFixture):
        """Test WorkerProcess proxy finalizer exception handling.

        Given:
            A WorkerProxy that raises exception during stop
        When:
            proxy_finalizer is called with the proxy
        Then:
            It should catch and suppress the exception
        """
        # Arrange
        mock_proxy = mocker.MagicMock()
        mock_proxy.stop = mocker.AsyncMock(side_effect=RuntimeError("Stop failed"))

        process = svc.WorkerProcess()

        # Extract the proxy_finalizer function by mocking the run method
        proxy_finalizer = cast(Callable[[WorkerProxy], Coroutine], None)

        def capture_proxy_finalizer(*args, **kwargs):
            nonlocal proxy_finalizer
            # Find the ResourcePool call and extract the finalizer
            if "finalizer" in kwargs:
                proxy_finalizer = kwargs["finalizer"]

        mocker.patch.object(svc, "ResourcePool", side_effect=capture_proxy_finalizer)
        mocker.patch.object(svc.asyncio, "run")

        # Call run to set up the proxy_finalizer
        process.run()

        # Act - should not raise exception
        assert proxy_finalizer is not None
        await proxy_finalizer(mock_proxy)  # Should not raise

        # Assert
        mock_proxy.stop.assert_called_once()

    def test_run_method_calls_serve_with_asyncio_run(self, mocker: MockerFixture):
        """Test WorkerProcess run method calls _serve with asyncio.run.

        Given:
            A WorkerProcess instance
        When:
            run() method is called directly
        Then:
            It should execute _serve() using asyncio.run
        """
        # Arrange
        process = svc.WorkerProcess()
        mock_asyncio_run = mocker.patch.object(svc.asyncio, "run")

        # Mock the _serve method to verify it gets called
        mock_serve = mocker.patch.object(
            process, "_serve", new_callable=mocker.AsyncMock
        )

        # Act
        process.run()

        # Assert
        # asyncio.run should be called once, and _serve should be called
        mock_asyncio_run.assert_called_once()
        mock_serve.assert_called_once()

    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(timeout=st.floats(min_value=0.001, max_value=3600.0))
    def test_worker_process_start_accepts_valid_timeouts(
        self, timeout: float, mocker: MockerFixture
    ):
        """Test WorkerProcess.start() accepts various valid timeout values.

        Given:
            A valid positive timeout value
        When:
            WorkerProcess.start() is called with that timeout
        Then:
            It should accept the timeout without raising ValueError
        """
        # Arrange
        mocker.patch("multiprocessing.Process.start")
        mock_get_port = mocker.MagicMock()
        mock_get_port.poll.return_value = True
        mock_get_port.recv.return_value = 50051
        mock_get_port.close = mocker.MagicMock()

        process = svc.WorkerProcess()
        process._get_port = mock_get_port

        # Act - should not raise ValueError
        process.start(timeout=timeout)

        # Assert
        assert process._port == 50051
        mock_get_port.poll.assert_called_once_with(timeout=timeout)

    @given(
        shutdown_grace_period=st.floats(min_value=0.001, max_value=3600.0),
        proxy_pool_ttl=st.floats(min_value=0.001, max_value=3600.0),
    )
    def test_worker_process_init_accepts_valid_timeout_parameters(
        self, shutdown_grace_period: float, proxy_pool_ttl: float
    ):
        """Test WorkerProcess.__init__() accepts various valid timeout values.

        Given:
            Valid positive values for shutdown_grace_period and proxy_pool_ttl
        When:
            WorkerProcess is initialized with these values
        Then:
            It should accept them without raising ValueError
        """
        # Act - should not raise ValueError
        process = svc.WorkerProcess(
            shutdown_grace_period=shutdown_grace_period,
            proxy_pool_ttl=proxy_pool_ttl,
        )

        # Assert
        assert process._shutdown_grace_period == shutdown_grace_period
        assert process._proxy_pool_ttl == proxy_pool_ttl

    @given(
        shutdown_grace_period=st.one_of(
            st.just(0.0),
            st.floats(
                min_value=-1000.0,
                max_value=-0.001,
                allow_nan=False,
                allow_infinity=False,
            ),
        )
    )
    def test_init_rejects_non_positive_shutdown_grace_period(
        self, shutdown_grace_period: float
    ):
        """Test WorkerProcess.__init__() rejects non-positive shutdown grace period.

        Given:
            A shutdown_grace_period that is zero or negative
        When:
            WorkerProcess is initialized
        Then:
            It should raise ValueError with "Shutdown grace period must be positive"
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Shutdown grace period must be positive"):
            svc.WorkerProcess(shutdown_grace_period=shutdown_grace_period)

    @given(
        proxy_pool_ttl=st.one_of(
            st.just(0.0),
            st.floats(
                min_value=-1000.0,
                max_value=-0.001,
                allow_nan=False,
                allow_infinity=False,
            ),
        )
    )
    def test_init_rejects_non_positive_proxy_pool_ttl(self, proxy_pool_ttl: float):
        """Test WorkerProcess.__init__() rejects non-positive proxy pool TTL.

        Given:
            A proxy_pool_ttl that is zero or negative
        When:
            WorkerProcess is initialized
        Then:
            It should raise ValueError with "Proxy pool TTL must be positive"
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Proxy pool TTL must be positive"):
            svc.WorkerProcess(proxy_pool_ttl=proxy_pool_ttl)

    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(
        timeout=st.one_of(
            st.just(0.0),
            st.floats(
                min_value=-1000.0,
                max_value=-0.001,
                allow_nan=False,
                allow_infinity=False,
            ),
        )
    )
    def test_start_rejects_non_positive_timeout(
        self, timeout: float, mocker: MockerFixture
    ):
        """Test WorkerProcess.start() rejects non-positive timeout values.

        Given:
            A timeout value that is zero or negative
        When:
            start() is called with that timeout
        Then:
            It should raise ValueError with "Timeout must be positive"
        """
        # Arrange
        process = svc.WorkerProcess()

        # Act & Assert
        with pytest.raises(ValueError, match="Timeout must be positive"):
            process.start(timeout=timeout)
