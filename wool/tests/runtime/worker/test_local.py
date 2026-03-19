import grpc.aio
import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool import protocol
from wool.runtime.worker import local as local_module
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.base import WorkerLike
from wool.runtime.worker.base import WorkerOptions
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.process import WorkerProcess


class TestLocalWorker:
    """Test suite for LocalWorker."""

    def test___init___with_default_parameters(self):
        """Test LocalWorker initialization with default parameters.

        Given:
            No custom parameters
        When:
            LocalWorker is instantiated
        Then:
            It should initialize successfully with None address before start
        """
        # Act
        worker = LocalWorker()

        # Assert
        assert worker.address is None

    def test___init___with_custom_host_and_port(self):
        """Test LocalWorker initialization with custom host and port.

        Given:
            Custom host and port values
        When:
            LocalWorker is instantiated
        Then:
            It should initialize successfully
        """
        # Act
        worker = LocalWorker(host="0.0.0.0", port=50051)

        # Assert
        # Before start, address is None and metadata is not yet available
        assert worker.metadata is None

    def test___init___with_tags(self, worker_tags):
        """Test LocalWorker initialization with capability tags.

        Given:
            Worker capability tags
        When:
            LocalWorker is instantiated with tags
        Then:
            Tags should be stored in the worker
        """
        # Act
        worker = LocalWorker(*worker_tags)

        # Assert
        assert worker.tags == set(worker_tags)

    def test___init___with_extra_metadata(self, worker_extra):
        """Test LocalWorker initialization with extra metadata.

        Given:
            Extra metadata dictionary
        When:
            LocalWorker is instantiated with extra metadata
        Then:
            Metadata should be stored in the worker
        """
        # Act
        worker = LocalWorker(**worker_extra)

        # Assert
        assert worker.extra == worker_extra

    @given(
        grace_period=st.floats(min_value=1.0, max_value=300.0),
        ttl=st.floats(min_value=1.0, max_value=300.0),
    )
    def test___init___with_custom_timeouts(self, grace_period, ttl):
        """Test LocalWorker initialization with custom timeout parameters.

        Given:
            Custom shutdown grace period and proxy pool TTL
        When:
            LocalWorker is instantiated
        Then:
            It should initialize successfully
        """
        # Act
        worker = LocalWorker(shutdown_grace_period=grace_period, proxy_pool_ttl=ttl)

        # Assert
        # Timeouts are internal config - just verify construction succeeds
        assert worker.metadata is None
        assert worker.address is None

    def test___init___with_default_options(self, mocker):
        """Test default options are forwarded to WorkerProcess.

        Given:
            No options parameter.
        When:
            LocalWorker is instantiated.
        Then:
            WorkerProcess is called with options=None and credentials=None.
        """
        # Arrange
        MockWorkerProcess = mocker.patch.object(local_module, "WorkerProcess")

        # Act
        LocalWorker()

        # Assert
        MockWorkerProcess.assert_called_once()
        assert MockWorkerProcess.call_args.kwargs["options"] is None
        assert MockWorkerProcess.call_args.kwargs["credentials"] is None

    def test___init___with_custom_options(self, mocker):
        """Test custom WorkerOptions are forwarded to WorkerProcess.

        Given:
            A WorkerOptions instance with custom message sizes.
        When:
            LocalWorker is instantiated with that options parameter.
        Then:
            WorkerProcess is called with the same options instance.
        """
        # Arrange
        MockWorkerProcess = mocker.patch.object(local_module, "WorkerProcess")
        opts = WorkerOptions(
            max_receive_message_length=50 * 1024 * 1024,
            max_send_message_length=25 * 1024 * 1024,
        )

        # Act
        LocalWorker(options=opts)

        # Assert
        MockWorkerProcess.assert_called_once()
        assert MockWorkerProcess.call_args.kwargs["options"] is opts

    def test_implements_workerlike_protocol(self):
        """Test LocalWorker implements WorkerLike protocol.

        Given:
            A LocalWorker instance
        When:
            isinstance check is performed
        Then:
            It should be an instance of WorkerLike
        """
        # Act
        worker = LocalWorker()

        # Assert
        assert isinstance(worker, WorkerLike)

    def test_address_returns_none_before_start(self):
        """Test address property returns None before worker is started.

        Given:
            A LocalWorker that has not been started
        When:
            The address property is accessed
        Then:
            It should return None
        """
        # Act
        worker = LocalWorker()

        # Assert
        assert worker.address is None

    @pytest.mark.asyncio
    async def test_address_returns_value_after_start(self, mocker):
        """Test address property returns value after worker is started.

        Given:
            A LocalWorker that has been started with mocked WorkerProcess
        When:
            The address property is accessed
        Then:
            It should return the worker process address
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()

        # Act
        await worker.start()

        # Assert
        assert worker.address == "127.0.0.1:50051"

    @pytest.mark.asyncio
    async def test_start_calls_worker_process_start(self, mocker):
        """Test _start method calls WorkerProcess.start.

        Given:
            A LocalWorker with mocked WorkerProcess
        When:
            start() is called
        Then:
            It should call WorkerProcess.start in executor
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()

        # Act
        await worker.start(timeout=60.0)

        # Assert
        mock_process.start.assert_called_once_with(timeout=60.0)

    @pytest.mark.asyncio
    async def test_start_creates_metadata(self, mocker):
        """Test _start method creates WorkerMetadata.

        Given:
            A LocalWorker with tags and extra metadata
        When:
            start() is called
        Then:
            It should create WorkerMetadata with correct data
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "192.168.1.100:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker("gpu", "ml", region="us-west")

        # Act
        await worker.start()

        # Assert
        assert worker.metadata is not None
        assert worker.metadata.uid == worker.uid
        assert worker.metadata.address == "192.168.1.100:50051"
        assert worker.metadata.pid == 12345
        assert "gpu" in worker.metadata.tags
        assert "ml" in worker.metadata.tags
        assert worker.metadata.extra["region"] == "us-west"

    @pytest.mark.asyncio
    async def test_start_raises_error_if_no_address(self, mocker):
        """Test _start raises error if WorkerProcess has no address.

        Given:
            A LocalWorker where WorkerProcess.start doesn't set address
        When:
            start() is called
        Then:
            It should raise RuntimeError
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = None
        mock_process.pid = 12345
        mock_process.start.return_value = None

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()

        # Act & assert
        with pytest.raises(RuntimeError, match="no address"):
            await worker.start()

    @pytest.mark.asyncio
    async def test_start_raises_error_if_no_pid(self, mocker):
        """Test _start raises error if WorkerProcess has no PID.

        Given:
            A LocalWorker where WorkerProcess.start doesn't set PID
        When:
            start() is called
        Then:
            It should raise RuntimeError
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = None
        mock_process.start.return_value = None

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()

        # Act & assert
        with pytest.raises(RuntimeError, match="no PID"):
            await worker.start()

    @pytest.mark.asyncio
    async def test_start_parses_address_correctly(self, mocker):
        """Test _start correctly parses host and port from address.

        Given:
            A LocalWorker with various address formats
        When:
            start() is called
        Then:
            It should correctly parse host and port
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "0.0.0.0:8080"
        mock_process.pid = 99999
        mock_process.start.return_value = None

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()

        # Act
        await worker.start()

        # Assert
        assert worker.address == "0.0.0.0:8080"

    @pytest.mark.asyncio
    async def test_stop_sends_grpc_stop_request(self, mocker):
        """Test _stop sends gRPC stop request to worker process.

        Given:
            A started LocalWorker with alive process
        When:
            stop() is called
        Then:
            It should send gRPC stop request
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()
        await worker.start()

        mock_channel = mocker.MagicMock()
        mock_stub = mocker.MagicMock()
        mock_stub.stop = mocker.AsyncMock()

        mocker.patch.object(grpc.aio, "insecure_channel", return_value=mock_channel)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        # Act
        await worker.stop()

        # Assert
        mock_stub.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_does_nothing_if_process_not_alive(self, mocker):
        """Test _stop does nothing if worker process is not alive.

        Given:
            A started LocalWorker with dead process
        When:
            stop() is called
        Then:
            It should not attempt gRPC call
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = False

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()
        await worker.start()

        mock_channel_fn = mocker.patch.object(grpc.aio, "insecure_channel")

        # Act
        await worker.stop()

        # Assert
        mock_channel_fn.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_handles_grpc_errors_gracefully(self, mocker):
        """Test _stop handles gRPC errors without crashing.

        Given:
            A started LocalWorker where gRPC call fails
        When:
            stop() is called
        Then:
            It should handle the error gracefully
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()
        await worker.start()

        mock_channel = mocker.MagicMock()
        mock_stub = mocker.MagicMock()
        mock_stub.stop = mocker.AsyncMock(side_effect=Exception("gRPC error"))

        mocker.patch.object(grpc.aio, "insecure_channel", return_value=mock_channel)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        # Act & assert
        with pytest.raises(Exception, match="gRPC error"):
            await worker.stop()

    # === NEW CREDENTIAL TESTS ===

    def test___init___with_worker_credentials(self, worker_credentials):
        """Test LocalWorker with WorkerCredentials.

        Given:
            WorkerCredentials instance.
        When:
            LocalWorker is instantiated with credentials parameter.
        Then:
            It should construct successfully.
        """
        # Act
        worker = LocalWorker(credentials=worker_credentials)

        # Assert
        assert worker.metadata is None  # Not started yet, but construction succeeded

    def test___init___with_no_credentials(self):
        """Test LocalWorker with no credentials.

        Given:
            None as credentials parameter.
        When:
            LocalWorker is instantiated.
        Then:
            It should construct successfully.
        """
        # Act
        worker = LocalWorker(credentials=None)

        # Assert
        assert worker.metadata is None  # Not started yet, but construction succeeded

    @pytest.mark.asyncio
    async def test_metadata_secure_flag_with_mtls(self, mocker, worker_credentials):
        """Test metadata.secure flag with mTLS worker.

        Given:
            LocalWorker with WorkerCredentials
        When:
            Worker metadata is accessed after start
        Then:
            metadata.secure is True
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = False

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker(credentials=worker_credentials)

        # Act
        await worker.start()

        # Assert
        assert worker.metadata.secure is True

    @pytest.mark.asyncio
    async def test_metadata_secure_flag_without_credentials(self, mocker):
        """Test metadata.secure flag for insecure worker.

        Given:
            LocalWorker with no credentials
        When:
            Worker metadata is accessed after start
        Then:
            metadata.secure is False
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = False

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()

        # Act
        await worker.start()

        # Assert
        assert worker.metadata.secure is False

    @pytest.mark.asyncio
    async def test_stop_with_client_credentials_secure_connection(
        self, mocker, worker_credentials
    ):
        """Test secure self-connection for mTLS worker.

        Given:
            Running LocalWorker with WorkerCredentials (has client_credentials)
        When:
            stop() is called
        Then:
            Worker uses client_credentials to send secure gRPC stop request
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker(credentials=worker_credentials)
        await worker.start()

        mock_channel = mocker.MagicMock()
        mock_stub = mocker.MagicMock()
        mock_stub.stop = mocker.AsyncMock()

        mock_secure_channel = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        # Act
        await worker.stop()

        # Assert
        mock_secure_channel.assert_called_once()
        call_args = mock_secure_channel.call_args
        assert call_args[0][0] == "127.0.0.1:50051"

    @pytest.mark.asyncio
    async def test_stop_without_credentials_insecure_connection(self, mocker):
        """Test insecure self-connection for insecure worker.

        Given:
            Running LocalWorker with no credentials
        When:
            stop() is called
        Then:
            Worker uses insecure channel to send gRPC stop request
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()
        await worker.start()

        mock_channel = mocker.MagicMock()
        mock_stub = mocker.MagicMock()
        mock_stub.stop = mocker.AsyncMock()

        mock_insecure_channel = mocker.patch.object(
            grpc.aio, "insecure_channel", return_value=mock_channel
        )
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        # Act
        await worker.stop()

        # Assert
        mock_insecure_channel.assert_called_once_with("127.0.0.1:50051")

    @pytest.mark.asyncio
    async def test_stop_with_one_way_tls(self, mocker, worker_credentials_one_way):
        """Test one-way TLS worker behavior.

        Given:
            LocalWorker with WorkerCredentials where mutual=False
        When:
            Worker is started and stopped
        Then:
            Worker uses secure channel with one-way TLS credentials
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker(credentials=worker_credentials_one_way)
        await worker.start()

        assert worker.metadata is not None

        mock_channel = mocker.MagicMock()
        mock_stub = mocker.MagicMock()
        mock_stub.stop = mocker.AsyncMock()

        mock_secure_channel = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        # Act
        await worker.stop()

        # Assert
        mock_secure_channel.assert_called_once()
        mock_stub.stop.assert_called_once()

    @pytest.mark.parametrize("mutual", [True, False], ids=["mtls", "one_way_tls"])
    @pytest.mark.asyncio
    async def test_worker_lifecycle_with_credentials(
        self, mutual, mocker, test_certificates
    ):
        """Test complete worker lifecycle with both mTLS and one-way TLS.

        Given:
            WorkerCredentials with mutual=True (mTLS) or mutual=False (one-way TLS)
        When:
            LocalWorker is created, started, and stopped
        Then:
            Worker completes full lifecycle without errors
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=mutual
        )

        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.side_effect = [True, False]

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        mock_channel = mocker.MagicMock()
        mock_stub = mocker.MagicMock()
        mock_stub.stop = mocker.AsyncMock()
        mocker.patch.object(grpc.aio, "secure_channel", return_value=mock_channel)
        mocker.patch.object(grpc.aio, "insecure_channel", return_value=mock_channel)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        # Act
        worker = LocalWorker(credentials=creds)
        await worker.start()
        await worker.stop()

        # Assert
        assert worker.metadata is not None
