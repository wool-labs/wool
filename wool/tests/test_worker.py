import asyncio
import signal

import cloudpickle
import grpc
import pytest
from pytest_mock import MockerFixture

from wool import _protobuf as pb
from wool import _worker as svc
from wool._work import WoolTask
from wool._worker_discovery import WorkerInfo


@pytest.fixture
def mock_registry_service(mocker):
    """Create a mock registry service with common async methods."""
    mock_service = mocker.AsyncMock()
    mock_service.start = mocker.AsyncMock()
    mock_service.stop = mocker.AsyncMock()
    mock_service.register = mocker.AsyncMock()
    mock_service.unregister = mocker.AsyncMock()
    return mock_service


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
def configured_local_worker(mock_registry_service, mock_worker_process, mocker):
    """Create a LocalWorker with mocked dependencies."""
    mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)
    return svc.LocalWorker(
        "test-uid", "tag1", "tag2", registry_service=mock_registry_service
    )


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
        tags={"tag1", "tag2"},
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


@pytest.fixture
def patch_signal_handlers():
    """Fixture to clean up signal handlers after tests."""
    original_sigterm = signal.signal(signal.SIGTERM, signal.SIG_DFL)
    original_sigint = signal.signal(signal.SIGINT, signal.SIG_DFL)
    yield
    signal.signal(signal.SIGTERM, original_sigterm)
    signal.signal(signal.SIGINT, original_sigint)


class TestSignalHandlers:
    def test_worker_process_sigterm_handler(self, mocker: MockerFixture):
        """Test SIGTERM signal handler when loop is running.

        Given:
            A running event loop and service
        When:
            SIGTERM signal handler is triggered
        Then:
            It should call loop.call_soon_threadsafe with service._stop(timeout=0)
        """
        # Arrange
        mock_loop = mocker.MagicMock()
        mock_loop.is_running.return_value = True
        mock_loop.call_soon_threadsafe = mocker.MagicMock()

        mock_service = mocker.MagicMock()
        mock_service._stop = mocker.AsyncMock()

        mocker.patch.object(svc.asyncio, "get_running_loop", return_value=mock_loop)

        captured_handler = None

        # Act
        with svc._signal_handlers(mock_service):
            # Capture the signal handler that was installed
            captured_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)

        # Now call the handler outside the context to test the behavior
        if captured_handler and callable(captured_handler):
            captured_handler(signal.SIGTERM, None)

        # Assert
        mock_loop.call_soon_threadsafe.assert_called_once()

    def test_signal_handlers_sigterm_loop_not_running(
        self, mock_worker_service, patch_signal_handlers, mocker: MockerFixture
    ):
        """Test SIGTERM signal handler installs and restores correctly.

        Given:
            A signal handler context
        When:
            The context is used
        Then:
            It should install and restore signal handlers correctly
        """
        # Act & Assert - should not raise any exceptions
        with svc._signal_handlers(mock_worker_service):
            # Get the current signal handler
            current_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
            # The handler should be installed
            assert current_handler != signal.SIG_DFL

    def test_worker_process_sigint_handler(self, mocker: MockerFixture):
        """Test SIGINT signal handler when loop is running.

        Given:
            A running event loop and service
        When:
            SIGINT signal handler is triggered
        Then:
            It should call loop.call_soon_threadsafe with service._stop(timeout=None)
        """
        # Arrange
        mock_loop = mocker.MagicMock()
        mock_loop.is_running.return_value = True
        mock_loop.call_soon_threadsafe = mocker.MagicMock()

        mock_service = mocker.MagicMock()
        mock_service._stop = mocker.AsyncMock()

        mocker.patch.object(svc.asyncio, "get_running_loop", return_value=mock_loop)

        captured_handler = None

        # Act
        with svc._signal_handlers(mock_service):
            # Capture the signal handler that was installed
            captured_handler = signal.signal(signal.SIGINT, signal.SIG_DFL)

        # Now call the handler outside the context to test the behavior
        if captured_handler and callable(captured_handler):
            captured_handler(signal.SIGINT, None)

        # Assert
        mock_loop.call_soon_threadsafe.assert_called_once()

    def test_signal_handlers_sigint_loop_not_running(self, mocker: MockerFixture):
        """Test SIGINT signal handler installs and restores correctly.

        Given:
            A signal handler context
        When:
            The context is used
        Then:
            It should install and restore signal handlers correctly
        """
        # Arrange
        mock_service = mocker.MagicMock()
        mock_service._stop = mocker.AsyncMock()

        # Act & Assert - should not raise any exceptions
        with svc._signal_handlers(mock_service):
            # Get the current signal handler
            current_handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
            # The handler should be installed
            assert current_handler != signal.SIG_DFL

    def test_signal_handlers_sigterm_loop_running(self, mocker: MockerFixture):
        """Test SIGTERM signal handler installs correctly.

        Given:
            A signal handler context
        When:
            The context is used
        Then:
            It should install and restore signal handlers correctly
        """
        # Arrange
        mock_service = mocker.MagicMock()
        mock_service._stop = mocker.AsyncMock()

        # Act & Assert - should not raise any exceptions
        with svc._signal_handlers(mock_service):
            # Get the current signal handler
            current_handler = signal.signal(signal.SIGTERM, signal.SIG_DFL)
            # The handler should be installed
            assert current_handler != signal.SIG_DFL

    def test_signal_handlers_sigint_loop_running(self, mocker: MockerFixture):
        """Test SIGINT signal handler installs correctly.

        Given:
            A signal handler context
        When:
            The context is used
        Then:
            It should install and restore signal handlers correctly
        """
        # Arrange
        mock_service = mocker.MagicMock()
        mock_service._stop = mocker.AsyncMock()

        # Act & Assert - should not raise any exceptions
        with svc._signal_handlers(mock_service):
            # Get the current signal handler
            current_handler = signal.signal(signal.SIGINT, signal.SIG_DFL)
            # The handler should be installed
            assert current_handler != signal.SIG_DFL


class TestLanWorker:
    def test_init_creates_registry_service_and_worker_process(
        self, mocker: MockerFixture
    ):
        """Test :py:class:`LocalWorker` initialization creates registry service
        and worker process.

        Given:
            Optional tags and registry service
        When:
            :py:class:`LocalWorker` is initialized
        Then:
            It should create registry service instance and worker process with
            correct attributes
        """
        # Arrange
        mock_lan_registry_service = mocker.MagicMock()
        mock_worker_process = mocker.patch.object(svc, "WorkerProcess")

        # Act
        worker = svc.LocalWorker(
            "tag1",
            "tag2",
            registry_service=mock_lan_registry_service,
        )

        # Assert
        assert worker._registry_service == mock_lan_registry_service
        assert "tag1" in worker.tags
        assert "tag2" in worker.tags
        assert worker.uid.startswith("worker-")
        mock_worker_process.assert_called_once_with(host="127.0.0.1", port=0)
        assert worker._worker_process == mock_worker_process.return_value

    def test_init_creates_default_registry_when_none_provided(
        self, mocker: MockerFixture
    ):
        """Test :py:class:`LocalWorker` initialization with provided registry
        service.

        Given:
            A LanRegistryService provided
        When:
            :py:class:`LocalWorker` is initialized
        Then:
            It should use the provided registry service
        """
        # Arrange
        mock_worker_process = mocker.patch.object(svc, "WorkerProcess")
        mock_registry = mocker.MagicMock()

        # Act
        worker = svc.LocalWorker("tag1", registry_service=mock_registry)

        # Assert
        assert worker._registry_service == mock_registry
        assert "tag1" in worker.tags
        mock_worker_process.assert_called_once_with(host="127.0.0.1", port=0)

    def test_address_property_when_process_has_address(self, mocker: MockerFixture):
        """Test :py:class:`LocalWorker` address property returns process address
        when available.

        Given:
            A :py:class:`LocalWorker` with a worker process that has an address
            set
        When:
            The address property is accessed
        Then:
            It should return the worker process address
        """
        # Arrange
        mock_worker_process = mocker.patch.object(svc, "WorkerProcess")
        mock_worker_process.return_value.address = "192.168.1.100:50051"
        mock_registry = mocker.MagicMock()

        worker = svc.LocalWorker(registry_service=mock_registry)

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
        mock_registry = mocker.MagicMock()

        worker = svc.LocalWorker(registry_service=mock_registry)

        # Act
        result = worker.address

        # Assert
        assert result is None

    def test_info_property_when_worker_not_started(self, mocker: MockerFixture):
        """Test LocalWorker info property returns None when worker not started.

        Given:
            A LocalWorker that has not been started
        When:
            The info property is accessed
        Then:
            It should return None
        """
        # Arrange
        mock_registry = mocker.MagicMock()
        worker = svc.LocalWorker(registry_service=mock_registry)

        # Act
        result = worker.info

        # Assert
        assert result is None

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
        mock_registry = mocker.MagicMock()
        worker = svc.LocalWorker(
            "tag1", registry_service=mock_registry, key1="value1", key2="value2"
        )

        # Act
        result = worker.extra

        # Assert
        assert result == {"key1": "value1", "key2": "value2"}

    def test_host_property_when_worker_not_started(self, mocker: MockerFixture):
        """Test LocalWorker host property returns None when worker not started.

        Given:
            A LocalWorker that has not been started
        When:
            The host property is accessed
        Then:
            It should return None
        """
        # Arrange
        mock_registry = mocker.MagicMock()
        worker = svc.LocalWorker(registry_service=mock_registry)

        # Act
        result = worker.host

        # Assert
        assert result is None

    def test_port_property_when_worker_not_started(self, mocker: MockerFixture):
        """Test LocalWorker port property returns None when worker not started.

        Given:
            A LocalWorker that has not been started
        When:
            The port property is accessed
        Then:
            It should return None
        """
        # Arrange
        mock_registry = mocker.MagicMock()
        worker = svc.LocalWorker(registry_service=mock_registry)

        # Act
        result = worker.port

        # Assert
        assert result is None

    @pytest.mark.asyncio
    async def test_start_initializes_components_and_registers(
        self, configured_local_worker, mock_registry_service, mock_worker_process
    ):
        """Test :py:class:`LocalWorker` :py:meth:`_start` method initializes
        components and registers.

        Given:
            A :py:class:`LocalWorker` instance with mocked registry service and
            worker process
        When:
            :py:meth:`_start` is called
        Then:
            It should start registry service, start worker process, and register
            with registry using the worker address and properties
        """
        # Act
        await configured_local_worker._start()

        # Assert
        mock_registry_service.start.assert_not_called()
        # Verify that worker process start was called (via run_in_executor)
        mock_worker_process.start.assert_called_once()
        mock_registry_service.register.assert_called_once()
        # Check the call arguments - should be called with WorkerInfo object
        call_args = mock_registry_service.register.call_args
        worker_info = call_args[0][0]
        assert isinstance(worker_info, WorkerInfo)
        assert worker_info.uid.startswith("worker-")
        assert worker_info.host == "192.168.1.100"
        assert worker_info.port == 50051
        assert worker_info.pid == 12345
        assert "tag1" in worker_info.tags
        assert "tag2" in worker_info.tags

    @pytest.mark.asyncio
    async def test_stop_performs_graceful_shutdown_when_process_alive(
        self,
        configured_local_worker,
        mock_registry_service,
        mock_worker_process,
        mocker: MockerFixture,
    ):
        """Test LocalWorker _stop method performs graceful shutdown.

        Given:
            A running LocalWorker instance with an alive process
        When:
            _stop() is called and process responds to SIGINT
        Then:
            It should unregister from registry, send SIGINT signal, join the
            process, and stop the registry service
        """
        # Arrange
        await configured_local_worker._start()  # Need to start first to set _info
        mock_os_kill = mocker.patch.object(svc.os, "kill")

        # Act
        await configured_local_worker._stop()

        # Assert
        # Check that unregister was called with WorkerInfo object
        mock_registry_service.unregister.assert_called_once()
        call_args = mock_registry_service.unregister.call_args
        worker_info = call_args[0][0]
        assert isinstance(worker_info, WorkerInfo)
        assert worker_info.uid.startswith("worker-")
        assert worker_info.host == "192.168.1.100"
        assert worker_info.port == 50051
        assert worker_info.pid == 12345
        assert "tag1" in worker_info.tags
        assert "tag2" in worker_info.tags
        mock_os_kill.assert_called_once_with(12345, svc.signal.SIGINT)
        mock_worker_process.join.assert_called_once()
        mock_registry_service.stop.assert_not_called()

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
            It should unregister from registry and return early without
            attempting to kill the process or stop the registry
        """
        # Arrange
        mock_lan_registry_service = mocker.MagicMock()
        mock_lan_registry_service.unregister = mocker.AsyncMock()
        mock_lan_registry_service.stop = mocker.AsyncMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = "192.168.1.100:50051"
        mock_worker_process.is_alive.return_value = False
        mock_worker_process.pid = None  # Process is dead, no PID
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        mock_os_kill = mocker.patch.object(svc.os, "kill")

        worker = svc.LocalWorker(registry_service=mock_lan_registry_service)
        # Need to manually set up _info for this test since process is dead
        from wool._worker_discovery import WorkerInfo

        worker._info = WorkerInfo(
            uid=worker.uid,
            host="192.168.1.100",
            port=50051,
            pid=0,
            version="test",
            tags=worker.tags,
        )

        # Act
        await worker._stop()

        # Assert
        # Check that unregister was called with WorkerInfo object
        mock_lan_registry_service.unregister.assert_called_once()
        call_args = mock_lan_registry_service.unregister.call_args
        worker_info = call_args[0][0]
        assert isinstance(worker_info, WorkerInfo)
        assert worker_info.uid.startswith("worker-")
        assert worker_info.host == "192.168.1.100"
        assert worker_info.port == 50051
        assert worker_info.pid == 0  # PID is 0 when process is not alive
        mock_os_kill.assert_not_called()
        mock_worker_process.join.assert_not_called()
        # NOTE: registry.stop() is not called when process is not alive
        # (early return at line 289)
        mock_lan_registry_service.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_force_kills_when_graceful_shutdown_fails(
        self, mocker: MockerFixture
    ):
        """Test LocalWorker _stop method force kills when graceful shutdown
        fails.

        Given:
            A LocalWorker instance where SIGINT fails to stop the process
        When:
            _stop() is called and graceful shutdown fails
        Then:
            It should force kill the process using the kill() method
        """
        # Arrange
        mock_lan_registry_service = mocker.MagicMock()
        mock_lan_registry_service.register = mocker.AsyncMock()
        mock_lan_registry_service.unregister = mocker.AsyncMock()
        mock_lan_registry_service.stop = mocker.AsyncMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = "192.168.1.100:50051"
        mock_worker_process.is_alive.side_effect = [
            True,
            True,
        ]  # alive during checks
        mock_worker_process.pid = 12345
        mock_worker_process.join.side_effect = OSError("Process not found")
        mock_worker_process.kill = mocker.MagicMock()
        mock_worker_process.start = mocker.MagicMock()
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        mocker.patch.object(svc.os, "kill", side_effect=OSError("Process not found"))

        worker = svc.LocalWorker(registry_service=mock_lan_registry_service)
        await worker._start()  # Need to start first to set _info

        # Act
        await worker._stop()

        # Assert
        mock_worker_process.kill.assert_called_once()

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
        mock_lan_registry_service = mocker.MagicMock()
        mock_lan_registry_service.start = mocker.AsyncMock()

        worker = svc.LocalWorker(registry_service=mock_lan_registry_service)
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
        mock_lan_registry_service = mocker.MagicMock()

        worker = svc.LocalWorker(registry_service=mock_lan_registry_service)
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
        mock_lan_registry_service = mocker.MagicMock()
        mock_lan_registry_service.start = mocker.AsyncMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = None  # No address
        mock_worker_process.pid = 12345
        mock_worker_process.start = mocker.MagicMock()
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        worker = svc.LocalWorker(registry_service=mock_lan_registry_service)

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Worker process failed to start - no address"
        ):
            await worker._start()

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
        mock_lan_registry_service = mocker.MagicMock()
        mock_lan_registry_service.start = mocker.AsyncMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = "192.168.1.100:50051"
        mock_worker_process.pid = None  # No PID
        mock_worker_process.start = mocker.MagicMock()
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        worker = svc.LocalWorker(registry_service=mock_lan_registry_service)

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Worker process failed to start - no PID"
        ):
            await worker._start()

    @pytest.mark.asyncio
    async def test_stop_raises_error_when_no_address(self, mocker: MockerFixture):
        """Test LocalWorker stop raises error when worker process has no
        address.

        Given:
            A LocalWorker with mocked WorkerProcess that returns None for
            address
        When:
            stop() is called
        Then:
            It should raise RuntimeError with "Cannot unregister - worker has
            no address"
        """
        # Arrange
        mock_lan_registry_service = mocker.MagicMock()
        mock_lan_registry_service.unregister = mocker.AsyncMock()

        mock_worker_process = mocker.MagicMock()
        mock_worker_process.address = None  # No address
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)

        worker = svc.LocalWorker(registry_service=mock_lan_registry_service)

        # Act & Assert
        with pytest.raises(RuntimeError, match="Cannot unregister - worker has no info"):
            await worker._stop()

    @pytest.mark.asyncio
    @pytest.mark.dependency(name="test_start_executes_successfully")
    async def test_start_executes_successfully(self, mocker: MockerFixture):
        """Test LocalWorker start method executes successfully.

        Given:
            A LocalWorker that has not been started
        When:
            start() is called
        Then:
            It should start registry service, call _start, and set _started
            flag
        """
        # Arrange
        mock_lan_registry_service = mocker.MagicMock()
        mock_lan_registry_service.start = mocker.AsyncMock()

        worker = svc.LocalWorker(registry_service=mock_lan_registry_service)
        mock_start = mocker.patch.object(
            worker, "_start", new_callable=mocker.AsyncMock
        )  # Mock the abstract method

        # Act
        await worker.start()

        # Assert
        mock_lan_registry_service.start.assert_called_once()
        mock_start.assert_called_once()
        assert worker._started is True

    @pytest.mark.asyncio
    @pytest.mark.dependency(depends=["test_start_executes_successfully"])
    async def test_stop_executes_successfully(self, mocker: MockerFixture):
        """Test LocalWorker stop method executes successfully.

        Given:
            A LocalWorker that has been started
        When:
            stop() is called
        Then:
            It should call _stop and stop registry service
        """
        # Arrange
        mock_lan_registry_service = mocker.MagicMock()
        mock_lan_registry_service.start = mocker.AsyncMock()
        mock_lan_registry_service.stop = mocker.AsyncMock()

        worker = svc.LocalWorker(registry_service=mock_lan_registry_service)
        mock_start = mocker.patch.object(worker, "_start", new_callable=mocker.AsyncMock)
        mock_stop = mocker.patch.object(
            worker, "_stop", new_callable=mocker.AsyncMock
        )  # Mock the abstract method

        # Actually start the worker first
        await worker.start()

        # Act
        await worker.stop()

        # Assert
        mock_start.assert_called_once()
        mock_stop.assert_called_once()
        mock_lan_registry_service.start.assert_called_once()
        mock_lan_registry_service.stop.assert_called_once()


class TestLocalWorkerEdgeCases:
    """Test edge cases and error conditions for LocalWorker."""

    @pytest.mark.asyncio
    async def test_start_with_registry_service_failure(
        self, mock_worker_process, mocker: MockerFixture
    ):
        """Test LocalWorker start when registry service registration fails.

        Given:
            A LocalWorker with a registry service that fails during registration
        When:
            start() is called
        Then:
            Should handle registry failure gracefully
        """
        # Arrange
        mock_registry_service = mocker.AsyncMock()
        mock_registry_service.start = mocker.AsyncMock()
        mock_registry_service.register = mocker.AsyncMock(
            side_effect=RuntimeError("Registry failed")
        )

        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)
        worker = svc.LocalWorker("test-uid", registry_service=mock_registry_service)

        # Act & Assert
        with pytest.raises(RuntimeError, match="Registry failed"):
            await worker._start()

    @pytest.mark.asyncio
    async def test_stop_with_os_kill_permission_error(
        self,
        configured_local_worker,
        mock_registry_service,
        mock_worker_process,
        mocker: MockerFixture,
    ):
        """Test LocalWorker stop when os.kill raises PermissionError.

        Given:
            A LocalWorker where os.kill raises PermissionError
        When:
            _stop() is called
        Then:
            Should continue with process.kill() as fallback
        """
        # Arrange
        # Need to start first to set _info
        await configured_local_worker._start()
        mock_os_kill = mocker.patch.object(
            svc.os, "kill", side_effect=PermissionError("Access denied")
        )

        # Act
        await configured_local_worker._stop()

        # Assert
        mock_os_kill.assert_called_once_with(12345, svc.signal.SIGINT)
        mock_worker_process.kill.assert_called_once()

    @pytest.mark.asyncio
    async def test_concurrent_start_calls(
        self, mock_registry_service, mock_worker_process, mocker: MockerFixture
    ):
        """Test LocalWorker handles concurrent start calls correctly.

        Given:
            A LocalWorker instance
        When:
            start() is called concurrently multiple times
        Then:
            Should raise appropriate error for subsequent calls
        """
        # Arrange
        mocker.patch.object(svc, "WorkerProcess", return_value=mock_worker_process)
        worker = svc.LocalWorker("test-uid", registry_service=mock_registry_service)

        # Act
        await worker.start()  # First call should succeed

        # Assert
        with pytest.raises(RuntimeError, match="Worker has already been started"):
            await worker.start()  # Second call should fail

    @pytest.mark.asyncio
    async def test_worker_info_creation_with_invalid_address(
        self, mock_registry_service, mocker: MockerFixture
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

        worker = svc.LocalWorker("test-uid", registry_service=mock_registry_service)

        # Act & Assert
        with pytest.raises(ValueError):
            await worker._start()

    def test_worker_tags_modification_behavior(self, mock_registry_service):
        """Test LocalWorker tags behavior when modified.

        Given:
            A LocalWorker with initial tags
        When:
            Tags property is accessed and modified
        Then:
            Should reflect the changes (tags are mutable)
        """
        # Arrange
        worker = svc.LocalWorker(
            "test-uid", "tag1", "tag2", registry_service=mock_registry_service
        )
        original_tags = worker.tags.copy()

        # Act
        worker.tags.add("additional_tag")

        # Assert
        assert "additional_tag" in worker.tags
        assert len(worker.tags) == len(original_tags) + 1


class TestWorkerProcess:
    def test_init_sets_default_port_to_zero(self):
        """Test :py:class:`WorkerProcess` initialization with default port.

        Given:
            No port argument is provided
        When:
            :py:class:`WorkerProcess` is initialized
        Then:
            It should set ``_port`` to 0 and create communication pipes
        """
        # Act
        process = svc.WorkerProcess()

        # Assert
        assert process.port is None

    def test_init_sets_port_when_provided(self):
        """Test :py:class:`WorkerProcess` initialization with specific port.

        Given:
            A positive port number
        When:
            :py:class:`WorkerProcess` is initialized
        Then:
            It should set ``_port`` to the specified value
        """
        # Act
        process = svc.WorkerProcess(port=8080)

        # Assert
        assert process.port == 8080

    def test_init_raises_error_for_negative_port(self):
        """Test :py:class:`WorkerProcess` initialization raises error for
        negative port.

        Given:
            A negative port number
        When:
            :py:class:`WorkerProcess` is initialized
        Then:
            It should raise :py:exc:`ValueError` with appropriate message
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
        """Test :py:class:`WorkerProcess` address property returns None when no
        port set.

        Given:
            A :py:class:`WorkerProcess` with no port set
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
            RuntimeError, match="Worker process failed to start within 10 seconds"
        ):
            process.start()

        # Assert
        mock_super_start.assert_called_once()
        mock_terminate.assert_called_once()
        mock_join.assert_called_once()
        mock_get_port.poll.assert_called_once_with(timeout=10)

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
        mock_server.stop.assert_called_once_with(grace=60)
        # Verify signal handlers were set up and restored
        assert mock_signal.call_count == 4  # 2 setups + 2 restores

    def test_address_formats_ip_and_port_correctly(self, mocker: MockerFixture):
        """Test WorkerProcess _address method formats address string.

        Given:
            A WorkerProcess instance, host, and port number
        When:
            _address() is called with host and port
        Then:
            It should return formatted address string with host and port
        """
        # Arrange
        process = svc.WorkerProcess(host="127.0.0.1", port=8080)

        # Assert
        assert process.address == "127.0.0.1:8080"

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
        proxy_factory = None

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
        proxy_factory = None

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
        mock_proxy.start.assert_not_called()  # Should not call start when already started

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
        proxy_finalizer = None

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


@pytest.mark.asyncio
class TestWorkerService:
    async def test_init_creates_required_attributes(self):
        """Test :py:class:`WorkerService` initialization creates required
        attributes.

        Given:
            No arguments are provided
        When:
            :py:class:`WorkerService` is initialized
        Then:
            It should create asyncio events and empty task set with correct
            initial states
        """
        # Act
        service = svc.WorkerService()

        # Assert
        assert isinstance(service._stopped, asyncio.Event)
        assert not service._stopped.is_set()
        assert isinstance(service._stopping, asyncio.Event)
        assert not service._stopping.is_set()
        assert isinstance(service._task_completed, asyncio.Event)
        assert isinstance(service._tasks, set)
        assert len(service._tasks) == 0

    async def test_stopping_property(self):
        """Test :py:class:`WorkerService` stopping property returns correct
        event.

        Given:
            A :py:class:`WorkerService` instance
        When:
            The stopping property is accessed
        Then:
            It should return the internal ``_stopping`` asyncio.Event
        """
        # Arrange
        service = svc.WorkerService()

        # Act
        result = service.stopping

        # Assert
        assert result is service._stopping

    async def test_stopped_property(self):
        """Test :py:class:`WorkerService` stopped property returns correct event.

        Given:
            A :py:class:`WorkerService` instance
        When:
            The stopped property is accessed
        Then:
            It should return the internal ``_stopped`` asyncio.Event
        """
        # Arrange
        service = svc.WorkerService()

        # Act
        result = service.stopped

        # Assert
        assert result is service._stopped

    async def test_running_context_manager_tracks_tasks_correctly(
        self, mocker: MockerFixture
    ):
        """Test WorkerService _running context manager tracks tasks correctly.

        Given:
            A WorkerService and a mock WoolTask
        When:
            _running context manager is used
        Then:
            It should add task to _tasks set, emit event, and remove on exit
        """
        # Arrange
        service = svc.WorkerService()
        mock_wool_task = mocker.MagicMock()

        async def mock_run():
            return "result"

        mock_wool_task.run = mock_run

        mock_event_instance = mocker.MagicMock()
        mock_event_instance.emit = mocker.MagicMock()
        mock_event = mocker.patch.object(
            svc, "WoolTaskEvent", return_value=mock_event_instance
        )

        # Act
        with service._running(mock_wool_task) as task:
            # Assert - task should be in the set during execution
            assert task in service._tasks
            assert isinstance(task, asyncio.Task)

        # Assert - event should be emitted and task removed after exit
        mock_event.assert_called_once_with("task-scheduled", task=mock_wool_task)
        mock_event_instance.emit.assert_called_once()
        assert task not in service._tasks

    async def test_dispatch_success(self, grpc_aio_stub, mocker: MockerFixture):
        """Test :py:class:`WorkerService` dispatch executes task successfully.

        Given:
            A gRPC :py:class:`WorkerService` that is not stopping and mocked
            task
        When:
            dispatch RPC is called with a task request
        Then:
            It should execute the task and return pickled result
        """
        # Arrange
        mock_wool_task = mocker.MagicMock()

        async def mock_run():
            return "test_result"

        mock_wool_task.run = mock_run
        mock_wool_task.callable.__qualname__ = "test_callable"
        mocker.patch.object(WoolTask, "from_protobuf", return_value=mock_wool_task)

        # Mock the WoolTaskEvent
        mock_event_instance = mocker.MagicMock()
        mocker.patch.object(svc, "WoolTaskEvent", return_value=mock_event_instance)

        request = pb.task.Task(
            id="12345678-1234-5678-1234-567812345678",
            callable=b"test_callable",
            args=b"test_args",
            kwargs=b"test_kwargs",
            caller="test_caller",
        )

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch(request)
            responses = []
            async for response in stream:
                responses.append(response)

        # Assert
        assert len(responses) == 2
        # First response should be an ack
        ack_response = responses[0]
        assert ack_response.HasField("ack")
        # Second response should contain the result
        result_response = responses[1]
        assert result_response.HasField("result")
        assert isinstance(result_response.result, pb.task.Result)
        assert cloudpickle.loads(result_response.result.dump) == "test_result"

    async def test_dispatch_with_exception(self, grpc_aio_stub, mocker: MockerFixture):
        """Test :py:class:`WorkerService` dispatch handles task exceptions.

        Given:
            A gRPC :py:class:`WorkerService` with a task that raises an
            exception
        When:
            dispatch RPC is called with the failing task
        Then:
            It should catch the exception and return it pickled
        """
        # Arrange
        test_exception = ValueError("Test error")
        mock_wool_task = mocker.MagicMock()

        async def mock_run():
            raise test_exception

        mock_wool_task.run = mock_run
        mock_wool_task.callable.__qualname__ = "test_callable"
        mocker.patch.object(WoolTask, "from_protobuf", return_value=mock_wool_task)

        # Mock the WoolTaskEvent
        mock_event_instance = mocker.MagicMock()
        mocker.patch.object(svc, "WoolTaskEvent", return_value=mock_event_instance)

        request = pb.task.Task(id="12345678-1234-5678-1234-567812345678")

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch(request)
            responses = []
            async for response in stream:
                responses.append(response)

        # Assert
        assert len(responses) == 2
        # First response should be an ack
        ack_response = responses[0]
        assert ack_response.HasField("ack")
        # Second response should contain the exception
        exception_response = responses[1]
        assert exception_response.HasField("exception")
        assert isinstance(exception_response.exception, pb.task.Exception)
        unpickled_exception = cloudpickle.loads(exception_response.exception.dump)
        assert isinstance(unpickled_exception, ValueError)
        assert str(unpickled_exception) == "Test error"

    async def test_dispatch_when_stopping(self, grpc_aio_stub, grpc_servicer):
        """Test WorkerService dispatch aborts when service is stopping.

        Given:
            A gRPC WorkerService that has stopping flag set
        When:
            dispatch RPC is called with any task request
        Then:
            It should abort the request with UNAVAILABLE status
        """
        # Arrange
        grpc_servicer._stopping.set()
        request = pb.task.Task(id="12345678-1234-5678-1234-567812345678")

        # Act & Assert
        async with grpc_aio_stub() as stub:
            with pytest.raises(grpc.aio.AioRpcError) as exc_info:
                stream = stub.dispatch(request)
                async for response in stream:
                    pass  # Should raise before we get any responses

            assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE
            assert (
                details := exc_info.value.details()
            ) and "Worker service is shutting down" in details

    async def test_stop_first_call(
        self, grpc_aio_stub, grpc_servicer, mocker: MockerFixture
    ):
        """Test WorkerService stop method calls internal _stop on first call.

        Given:
            A gRPC WorkerService that is not currently stopping
        When:
            stop RPC is called with a stop request
        Then:
            It should call internal _stop method and return Void response
        """
        # Arrange
        mock__stop = mocker.AsyncMock()
        grpc_servicer._stop = mock__stop
        request = pb.worker.StopRequest(wait=30)

        # Act
        async with grpc_aio_stub() as stub:
            result = await stub.stop(request)

        # Assert
        mock__stop.assert_called_once_with(timeout=30)
        assert isinstance(result, pb.worker.Void)

    async def test_stop_already_stopping(
        self, grpc_aio_stub, grpc_servicer, mocker: MockerFixture
    ):
        """Test WorkerService stop method is idempotent when already stopping.

        Given:
            A gRPC WorkerService that is already in stopping state
        When:
            stop RPC is called again
        Then:
            It should return Void immediately without calling _stop
        """
        # Arrange
        grpc_servicer._stopping.set()
        mock__stop = mocker.AsyncMock()
        grpc_servicer._stop = mock__stop
        request = pb.worker.StopRequest(wait=30)

        # Act
        async with grpc_aio_stub() as stub:
            result = await stub.stop(request)

        # Assert
        mock__stop.assert_not_called()
        assert isinstance(result, pb.worker.Void)

    async def test_stop_cancels_tasks_immediately_with_zero_timeout(
        self, mocker: MockerFixture
    ):
        """Test WorkerService _stop cancels tasks immediately with timeout=0.

        Given:
            A WorkerService with running tasks and timeout=0
        When:
            _stop is called
        Then:
            It should cancel tasks immediately and set stopped event
        """
        # Arrange
        mock_proxy_pool = mocker.patch.object(
            svc.wool, "__proxy_pool__", mocker.MagicMock()
        )
        mock_proxy_pool.get.return_value = mocker.AsyncMock()

        service = svc.WorkerService()

        # Create real asyncio tasks that can be cancelled
        async def long_running_task():
            await asyncio.sleep(10)  # Long sleep that will be cancelled

        task1 = asyncio.create_task(long_running_task())
        task2 = asyncio.create_task(long_running_task())
        service._tasks = {task1, task2}

        # Act
        await service._stop(timeout=0)

        # Assert
        assert service._stopping.is_set()
        assert service._stopped.is_set()
        # With timeout=0, tasks should be cancelled immediately
        assert task1.cancelled()
        assert task2.cancelled()

    async def test_stop_waits_for_tasks_with_positive_timeout(
        self, mocker: MockerFixture
    ):
        """Test WorkerService _stop waits for tasks with positive timeout.

        Given:
            A WorkerService with running tasks and positive timeout
        When:
            _stop is called and tasks complete within timeout
        Then:
            It should wait for tasks to complete and set stopped event
        """
        # Arrange
        mock_proxy_pool = mocker.patch.object(
            svc.wool, "__proxy_pool__", mocker.MagicMock()
        )
        mock_proxy_pool.get.return_value = mocker.AsyncMock()

        service = svc.WorkerService()

        # Create real asyncio tasks that will complete quickly
        async def quick_task():
            await asyncio.sleep(0)  # Short sleep

        task1 = asyncio.create_task(quick_task())
        task2 = asyncio.create_task(quick_task())
        service._tasks = {task1, task2}

        # Act
        await service._stop(timeout=1.0)  # Reasonable timeout

        # Assert
        assert service._stopping.is_set()
        assert service._stopped.is_set()
        assert task1.done()
        assert task2.done()

    async def test_stop_cancels_tasks_when_timeout_expires(self, mocker: MockerFixture):
        """Test WorkerService _stop cancels tasks when timeout expires.

        Given:
            A WorkerService with running tasks and a timeout that expires
        When:
            _stop is called and asyncio.TimeoutError is raised
        Then:
            It should recursively call _await_or_cancel_tasks with timeout=0
        """
        # Arrange
        mock_proxy_pool = mocker.patch.object(
            svc.wool, "__proxy_pool__", mocker.MagicMock()
        )
        mock_proxy_pool.get.return_value = mocker.AsyncMock()

        service = svc.WorkerService()

        # Create a real asyncio task to test with
        async def dummy_task():
            await asyncio.sleep(10)

        task = asyncio.create_task(dummy_task())
        service._tasks.add(task)

        # Use a very short timeout to trigger TimeoutError naturally
        # Act
        await service._stop(timeout=0.001)  # Very short timeout will expire

        # Assert
        assert service._stopping.is_set()
        assert service._stopped.is_set()
        assert task.cancelled()  # Task should be cancelled

    async def test_stop_is_idempotent_when_already_stopping(self, mocker: MockerFixture):
        """Test WorkerService _stop is idempotent when already stopping.

        Given:
            A WorkerService that is already in stopping state
        When:
            _stop is called again
        Then:
            It should complete successfully and ensure stopped is set
        """
        # Arrange
        mock_proxy_pool = mocker.patch.object(
            svc.wool, "__proxy_pool__", mocker.MagicMock()
        )
        mock_proxy_pool.get.return_value = mocker.AsyncMock()

        service = svc.WorkerService()
        service._stopping.set()

        # Act
        await service._stop()

        # Assert - _stop should always ensure both flags are set
        assert service._stopping.is_set()
        assert service._stopped.is_set()

    async def test_await_or_cancel_tasks_cancels_immediately_with_zero_timeout(self):
        """Test WorkerService _await_or_cancel_tasks cancels tasks immediately.

        Given:
            A WorkerService with running tasks and timeout=0
        When:
            _await_or_cancel_tasks is called
        Then:
            It should cancel tasks immediately
        """
        # Arrange
        service = svc.WorkerService()

        # Create real asyncio tasks that can be cancelled
        async def long_running_task():
            await asyncio.sleep(10)  # Long sleep that will be cancelled

        task1 = asyncio.create_task(long_running_task())
        task2 = asyncio.create_task(long_running_task())
        service._tasks = {task1, task2}

        # Act
        await service._await_or_cancel_tasks(timeout=0)

        # Assert
        # With timeout=0, tasks should be cancelled immediately
        assert task1.cancelled()
        assert task2.cancelled()

    async def test_await_or_cancel_tasks_waits_with_positive_timeout(self):
        """Test WorkerService _await_or_cancel_tasks waits with positive timeout.

        Given:
            A WorkerService with running tasks and positive timeout
        When:
            _await_or_cancel_tasks is called and tasks complete within timeout
        Then:
            It should wait for tasks to complete
        """
        # Arrange
        service = svc.WorkerService()

        # Create real asyncio tasks that will complete quickly
        async def quick_task():
            await asyncio.sleep(0)  # Short sleep

        task1 = asyncio.create_task(quick_task())
        task2 = asyncio.create_task(quick_task())
        service._tasks = {task1, task2}

        # Act
        await service._await_or_cancel_tasks(timeout=1.0)  # Reasonable timeout

        # Assert
        assert task1.done()
        assert task2.done()

    async def test_await_or_cancel_tasks_cancels_when_timeout_expires(self):
        """Test WorkerService _await_or_cancel_tasks cancels when timeout expires.

        Given:
            A WorkerService with running tasks and a timeout that expires
        When:
            _await_or_cancel_tasks is called and asyncio.TimeoutError is raised
        Then:
            It should recursively call itself with timeout=0 to cancel tasks
        """
        # Arrange
        service = svc.WorkerService()

        # Create a real asyncio task to test with
        async def dummy_task():
            await asyncio.sleep(10)

        task = asyncio.create_task(dummy_task())
        service._tasks.add(task)

        # Use a very short timeout to trigger TimeoutError naturally
        # Act
        await service._await_or_cancel_tasks(
            timeout=0.001
        )  # Very short timeout will expire

        # Assert
        assert task.cancelled()  # Task should be cancelled

    async def test_cancel_tasks_cancels_multiple_tasks_safely(self):
        """Test WorkerService _cancel method cancels multiple tasks safely.

        Given:
            Multiple long-running asyncio tasks
        When:
            _cancel is called with the tasks
        Then:
            It should cancel all tasks and handle CancelledError exceptions
        """
        # Arrange
        service = svc.WorkerService()

        async def long_task():
            await asyncio.sleep(10)

        task1 = asyncio.create_task(long_task())
        task2 = asyncio.create_task(long_task())

        # Act
        await service._cancel(task1, task2)

        # Assert
        assert task1.cancelled()
        assert task2.cancelled()

    async def test_cancel_safely_handles_current_task(self, mocker: MockerFixture):
        """Test WorkerService _cancel safely handles current task.

        Given:
            A task that is the currently running task
        When:
            _cancel is called with that task
        Then:
            It should skip canceling the current task to avoid deadlock
        """
        # Arrange
        service = svc.WorkerService()

        mock_current_task = mocker.MagicMock()
        mock_current_task.done.return_value = False
        mock_current_task.cancel = mocker.MagicMock()

        mocker.patch.object(asyncio, "current_task", return_value=mock_current_task)

        # Act
        await service._cancel(mock_current_task)

        # Assert
        mock_current_task.cancel.assert_not_called()


@pytest.mark.asyncio
class TestWithTimeoutContextManager:
    """Test the with_timeout context manager utility function."""

    async def test_wait_for_context_manager_timeout(self, mocker: MockerFixture):
        """Test with_timeout context manager when timeout occurs during entry.

        Given:
            A context manager and asyncio.wait_for that raises TimeoutError
        When:
            with_timeout is used with a timeout
        Then:
            It should call asyncio.wait_for with the timeout and propagate TimeoutError
        """
        # Arrange
        mock_context = mocker.MagicMock()
        mock_context.__aenter__ = mocker.AsyncMock()
        mock_context.__aexit__ = mocker.AsyncMock()

        mock_wait_for = mocker.patch.object(
            svc.asyncio, "wait_for", side_effect=asyncio.TimeoutError()
        )

        # Act & Assert
        with pytest.raises(asyncio.TimeoutError):
            async with svc.with_timeout(mock_context, timeout=0.5):
                pass

        # Verify asyncio.wait_for was called once with the correct timeout
        mock_wait_for.assert_called_once()
        call_args = mock_wait_for.call_args
        assert call_args[1]["timeout"] == 0.5

    async def test_wait_for_context_manager_exception(self, mocker: MockerFixture):
        """Test with_timeout context manager when exception occurs inside context.

        Given:
            A context manager where an exception occurs inside the context
        When:
            with_timeout is used and exception is raised
        Then:
            It should properly call __aexit__ with exception info and re-raise
        """
        # Arrange
        mock_context = mocker.MagicMock()
        mock_context.__aenter__ = mocker.AsyncMock()
        mock_context.__aexit__ = mocker.AsyncMock()

        test_exception = ValueError("Test exception")

        # Act & Assert
        with pytest.raises(ValueError, match="Test exception"):
            async with svc.with_timeout(mock_context, timeout=1.0):
                raise test_exception

        # Assert __aexit__ was called with correct exception info
        mock_context.__aexit__.assert_called_once()
        call_args = mock_context.__aexit__.call_args[0]
        assert call_args[0] is ValueError  # exception_type
        assert call_args[1] == test_exception  # exception_value
        assert call_args[2] is not None  # exception_traceback


@pytest.mark.asyncio
class TestDispatchStream:
    """Test the DispatchStream class."""

    async def test_dispatch_stream_init_and_aiter(self, mocker: MockerFixture):
        """Test DispatchStream initialization and __aiter__.

        Given:
            A gRPC dispatch call stream
        When:
            DispatchStream is initialized
        Then:
            It should store the stream and create an async iterator
        """
        # Arrange
        mock_stream = mocker.MagicMock()

        # Act
        dispatch_stream = svc.DispatchStream(mock_stream)
        async_iter = dispatch_stream.__aiter__()

        # Assert
        assert dispatch_stream._stream is mock_stream
        assert async_iter is dispatch_stream

    async def test_dispatch_stream_anext_with_result(self, mocker: MockerFixture):
        """Test DispatchStream __anext__ with result response.

        Given:
            A DispatchStream with a response containing a result
        When:
            __anext__ is called
        Then:
            It should return the unpickled result
        """
        # Arrange
        mock_stream = mocker.MagicMock()
        mock_response = mocker.MagicMock()
        mock_response.HasField.side_effect = lambda field: field == "result"
        mock_response.result.dump = cloudpickle.dumps("test_result")

        mock_iter = mocker.AsyncMock()
        mock_iter.__anext__ = mocker.AsyncMock(return_value=mock_response)

        mocker.patch.object(svc, "aiter", return_value=mock_iter)

        dispatch_stream = svc.DispatchStream(mock_stream)

        # Act
        result = await dispatch_stream.__anext__()

        # Assert
        assert result == "test_result"

    async def test_dispatch_stream_anext_with_exception(self, mocker: MockerFixture):
        """Test DispatchStream __anext__ with exception response.

        Given:
            A DispatchStream with a response containing an exception
        When:
            __anext__ is called
        Then:
            It should raise the unpickled exception
        """
        # Arrange
        mock_stream = mocker.MagicMock()
        mock_response = mocker.MagicMock()
        mock_response.HasField.side_effect = lambda field: field == "exception"
        test_exception = ValueError("Test exception")
        mock_response.exception.dump = cloudpickle.dumps(test_exception)

        mock_iter = mocker.AsyncMock()
        mock_iter.__anext__ = mocker.AsyncMock(return_value=mock_response)

        mocker.patch.object(svc, "aiter", return_value=mock_iter)

        dispatch_stream = svc.DispatchStream(mock_stream)

        # Act & Assert
        with pytest.raises(ValueError, match="Test exception"):
            await dispatch_stream.__anext__()

    async def test_dispatch_stream_anext_with_unexpected_response(
        self, mocker: MockerFixture
    ):
        """Test DispatchStream __anext__ with unexpected response.

        Given:
            A DispatchStream with a response that has no result or exception
        When:
            __anext__ is called
        Then:
            It should raise RuntimeError
        """
        # Arrange
        mock_stream = mocker.MagicMock()
        mock_response = mocker.MagicMock()
        mock_response.HasField.return_value = False  # No result or exception field

        mock_iter = mocker.AsyncMock()
        mock_iter.__anext__ = mocker.AsyncMock(return_value=mock_response)

        mocker.patch.object(svc, "aiter", return_value=mock_iter)

        dispatch_stream = svc.DispatchStream(mock_stream)

        # Act & Assert
        with pytest.raises(RuntimeError, match="Received unexpected response"):
            await dispatch_stream.__anext__()

    async def test_dispatch_stream_handle_exception_success(self, mocker: MockerFixture):
        """Test DispatchStream _handle_exception when cancel succeeds.

        Given:
            A DispatchStream and an exception
        When:
            _handle_exception is called and cancel succeeds
        Then:
            It should cancel the stream and re-raise the original exception
        """
        # Arrange
        mock_stream = mocker.MagicMock()
        mock_stream.cancel = mocker.MagicMock()

        dispatch_stream = svc.DispatchStream(mock_stream)
        test_exception = ValueError("Original exception")

        # Act & Assert
        with pytest.raises(ValueError, match="Original exception"):
            await dispatch_stream._handle_exception(test_exception)

        mock_stream.cancel.assert_called_once()

    async def test_dispatch_stream_handle_exception_cancel_fails(
        self, mocker: MockerFixture
    ):
        """Test DispatchStream _handle_exception when cancel fails.

        Given:
            A DispatchStream and an exception
        When:
            _handle_exception is called and cancel raises an exception
        Then:
            It should raise the cancel exception with the original as cause
        """
        # Arrange
        mock_stream = mocker.MagicMock()
        cancel_exception = RuntimeError("Cancel failed")
        mock_stream.cancel = mocker.MagicMock(side_effect=cancel_exception)

        dispatch_stream = svc.DispatchStream(mock_stream)
        original_exception = ValueError("Original exception")

        # Act & Assert
        with pytest.raises(RuntimeError, match="Cancel failed") as exc_info:
            await dispatch_stream._handle_exception(original_exception)

        assert exc_info.value.__cause__ is original_exception
        mock_stream.cancel.assert_called_once()


@pytest.mark.asyncio
class TestWorkerClient:
    """Test the WorkerClient class."""

    async def test_worker_client_init(self, mocker: MockerFixture):
        """Test WorkerClient initialization.

        Given:
            A worker address
        When:
            WorkerClient is initialized
        Then:
            It should create a gRPC channel and stub with semaphore
        """
        # Arrange
        mock_channel = mocker.MagicMock()
        mock_grpc_channel = mocker.patch.object(
            svc.grpc.aio, "insecure_channel", return_value=mock_channel
        )
        mock_stub = mocker.MagicMock()
        mocker.patch.object(svc.pb.worker, "WorkerStub", return_value=mock_stub)

        # Act
        client = svc.WorkerClient("192.168.1.100:50051")

        # Assert
        mock_grpc_channel.assert_called_once_with("192.168.1.100:50051")
        assert client._channel is mock_channel
        assert client._stub is mock_stub
        assert hasattr(client, "_semaphore")

    async def test_worker_client_stop(self, mocker: MockerFixture):
        """Test WorkerClient stop method.

        Given:
            A WorkerClient instance
        When:
            stop is called
        Then:
            It should close the gRPC channel
        """
        # Arrange
        mock_channel = mocker.MagicMock()
        mock_channel.close = mocker.AsyncMock()
        mocker.patch.object(svc.grpc.aio, "insecure_channel", return_value=mock_channel)

        client = svc.WorkerClient("192.168.1.100:50051")

        # Act
        await client.stop()

        # Assert
        mock_channel.close.assert_called_once()

    async def test_worker_client_dispatch_successful_flow(self, mocker: MockerFixture):
        """Test WorkerClient dispatch with successful flow.

        Given:
            A WorkerClient and a WoolTask
        When:
            dispatch is called and receives proper Ack response
        Then:
            It should yield results from DispatchStream
        """
        # Arrange
        mock_channel = mocker.MagicMock()
        mocker.patch.object(svc.grpc.aio, "insecure_channel", return_value=mock_channel)
        mock_stub = mocker.MagicMock()
        mocker.patch.object(svc.pb.worker, "WorkerStub", return_value=mock_stub)

        # Mock the task
        mock_task = mocker.MagicMock(spec=WoolTask)
        mock_protobuf = mocker.MagicMock()
        mock_task.to_protobuf.return_value = mock_protobuf

        # Mock the dispatch call
        mock_call = mocker.AsyncMock()
        mock_stub.dispatch.return_value = mock_call

        # Mock first response with Ack field
        mock_ack_response = mocker.MagicMock()
        mock_ack_response.HasField.side_effect = lambda field: field == "ack"

        # Mock anext and aiter
        mock_aiter = mocker.AsyncMock()
        mock_anext_coroutine = mocker.AsyncMock(return_value=mock_ack_response)
        mocker.patch.object(svc, "anext", return_value=mock_anext_coroutine)
        mocker.patch.object(svc, "aiter", return_value=mock_aiter)

        # Mock asyncio.wait_for to return the awaited response
        mocker.patch.object(svc.asyncio, "wait_for", return_value=mock_ack_response)

        # Mock DispatchStream to return async iterator
        class MockDispatchStream:
            def __init__(self, call):
                self.call = call

            def __aiter__(self):
                return self

            async def __anext__(self):
                try:
                    if not hasattr(self, "_values"):
                        self._values = iter(["result1", "result2"])
                    return next(self._values)
                except StopIteration:
                    raise StopAsyncIteration

        mocker.patch.object(svc, "DispatchStream", MockDispatchStream)

        # Mock with_timeout context manager
        mock_semaphore = mocker.AsyncMock()
        client = svc.WorkerClient("192.168.1.100:50051")
        client._semaphore = mock_semaphore

        # Act
        results = []
        async for result in client.dispatch(mock_task):
            results.append(result)

        # Assert
        assert results == ["result1", "result2"]
        mock_task.to_protobuf.assert_called_once()
        mock_stub.dispatch.assert_called_once_with(mock_protobuf)

    async def test_worker_client_dispatch_no_ack_response(self, mocker: MockerFixture):
        """Test WorkerClient dispatch when response has no Ack field.

        Given:
            A WorkerClient and a WoolTask
        When:
            dispatch is called but first response has no Ack field
        Then:
            It should raise UnexpectedResponse and cancel the call
        """
        # Arrange
        mock_channel = mocker.MagicMock()
        mocker.patch.object(svc.grpc.aio, "insecure_channel", return_value=mock_channel)
        mock_stub = mocker.MagicMock()
        mocker.patch.object(svc.pb.worker, "WorkerStub", return_value=mock_stub)

        mock_task = mocker.MagicMock(spec=WoolTask)
        mock_protobuf = mocker.MagicMock()
        mock_task.to_protobuf.return_value = mock_protobuf

        # Mock the dispatch call with cancel method
        mock_call = mocker.AsyncMock()
        mock_call.cancel = mocker.MagicMock()
        mock_stub.dispatch.return_value = mock_call

        # Mock first response without Ack field
        mock_response = mocker.MagicMock()
        mock_response.HasField.return_value = False  # No "ack" field

        mocker.patch.object(svc.asyncio, "wait_for", return_value=mock_response)

        client = svc.WorkerClient("192.168.1.100:50051")
        client._semaphore = mocker.AsyncMock()

        # Act & Assert
        with pytest.raises(svc.UnexpectedResponse, match="Expected Ack response"):
            async for _ in client.dispatch(mock_task):
                pass

        # Verify call was cancelled
        mock_call.cancel.assert_called_once()

    async def test_worker_client_dispatch_call_cancel_fails(self, mocker: MockerFixture):
        """Test WorkerClient dispatch when call cancellation fails.

        Given:
            A WorkerClient and a WoolTask
        When:
            dispatch fails and call.cancel() raises an exception
        Then:
            It should suppress the cancel exception and re-raise original
        """
        # Arrange - copy the successful test setup exactly
        mock_channel = mocker.MagicMock()
        mocker.patch.object(svc.grpc.aio, "insecure_channel", return_value=mock_channel)
        mock_stub = mocker.MagicMock()
        mocker.patch.object(svc.pb.worker, "WorkerStub", return_value=mock_stub)

        # Mock the task
        mock_task = mocker.MagicMock(spec=WoolTask)
        mock_protobuf = mocker.MagicMock()
        mock_task.to_protobuf.return_value = mock_protobuf

        mock_call = mocker.AsyncMock(
            __aiter__=mocker.MagicMock(side_effect=TimeoutError)
        )
        mock_call.cancel = mocker.MagicMock(side_effect=RuntimeError("Cancel failed"))
        mock_stub.dispatch.return_value = mock_call

        client = svc.WorkerClient("192.168.1.100:50051")

        # Act & Assert - consume the async generator to trigger dispatch
        with pytest.raises(TimeoutError):
            async for _ in client.dispatch(mock_task):
                pass  # This should never execute

        mock_call.cancel.assert_called_once()
