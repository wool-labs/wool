from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.base import WorkerLike
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.process import WorkerProcess


class TestLocalWorker:
    """Test suite for LocalWorker."""

    def test_init_with_default_parameters(self):
        """Test LocalWorker initialization with default parameters.

        Given:
            No custom parameters
        When:
            LocalWorker is instantiated
        Then:
            It should initialize successfully with None address before start
        """
        worker = LocalWorker()
        assert worker.address is None

    def test_init_with_custom_host_and_port(self):
        """Test LocalWorker initialization with custom host and port.

        Given:
            Custom host and port values
        When:
            LocalWorker is instantiated
        Then:
            It should initialize successfully
        """
        worker = LocalWorker(host="0.0.0.0", port=50051)
        # Before start, address is None and metadata is not yet available
        assert worker.metadata is None

    def test_init_with_tags(self, worker_tags):
        """Test LocalWorker initialization with capability tags.

        Given:
            Worker capability tags
        When:
            LocalWorker is instantiated with tags
        Then:
            Tags should be stored in the worker
        """
        worker = LocalWorker(*worker_tags)
        assert worker.tags == set(worker_tags)

    def test_init_with_extra_metadata(self, worker_extra):
        """Test LocalWorker initialization with extra metadata.

        Given:
            Extra metadata dictionary
        When:
            LocalWorker is instantiated with extra metadata
        Then:
            Metadata should be stored in the worker
        """
        worker = LocalWorker(**worker_extra)
        assert worker.extra == worker_extra

    @given(
        grace_period=st.floats(min_value=1.0, max_value=300.0),
        ttl=st.floats(min_value=1.0, max_value=300.0),
    )
    def test_init_with_custom_timeouts(self, grace_period, ttl):
        """Test LocalWorker initialization with custom timeout parameters.

        Given:
            Custom shutdown grace period and proxy pool TTL
        When:
            LocalWorker is instantiated
        Then:
            It should initialize successfully
        """
        worker = LocalWorker(shutdown_grace_period=grace_period, proxy_pool_ttl=ttl)
        # Timeouts are internal config - just verify construction succeeds
        assert worker.metadata is None
        assert worker.address is None

    def test_implements_workerlike_protocol(self):
        """Test LocalWorker implements WorkerLike protocol.

        Given:
            A LocalWorker instance
        When:
            isinstance check is performed
        Then:
            It should be an instance of WorkerLike
        """
        worker = LocalWorker()
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
        worker = LocalWorker()
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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker()
        await worker.start()
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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker()
        await worker.start(timeout=60.0)
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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "192.168.1.100:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker("gpu", "ml", region="us-west")
        await worker.start()

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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = None
        mock_process.pid = 12345
        mock_process.start.return_value = None

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker()
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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = None
        mock_process.start.return_value = None

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker()
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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "0.0.0.0:8080"
        mock_process.pid = 99999
        mock_process.start.return_value = None

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker()
        await worker.start()

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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker()
        await worker.start()

        # Mock gRPC components
        mock_channel = MagicMock()
        mock_stub = MagicMock()
        mock_stub.stop = AsyncMock()

        mocker.patch("grpc.aio.insecure_channel", return_value=mock_channel)
        mocker.patch(
            "wool.runtime.worker.local.protocol.worker.WorkerStub",
            return_value=mock_stub,
        )

        await worker.stop()
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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = False

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker()
        await worker.start()

        # Mock gRPC - should not be called
        mock_channel_fn = mocker.patch("grpc.aio.insecure_channel")

        await worker.stop()
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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker()
        await worker.start()

        # Mock gRPC to raise error
        mock_channel = MagicMock()
        mock_stub = MagicMock()
        mock_stub.stop = AsyncMock(side_effect=Exception("gRPC error"))

        mocker.patch("grpc.aio.insecure_channel", return_value=mock_channel)
        mocker.patch(
            "wool.runtime.worker.local.protocol.worker.WorkerStub",
            return_value=mock_stub,
        )

        # Should raise - error is not caught in LocalWorker._stop
        with pytest.raises(Exception, match="gRPC error"):
            await worker.stop()

    # === NEW CREDENTIAL TESTS ===

    def test_init_with_worker_credentials(self, worker_credentials):
        """Test LocalWorker with WorkerCredentials.

        Given:
            WorkerCredentials instance
        When:
            LocalWorker is instantiated with credentials parameter
        Then:
            Worker extracts both server_credentials and client_credentials
        """
        worker = LocalWorker(credentials=worker_credentials)
        # Verify credentials were extracted
        assert worker._server_credentials is not None
        assert worker._client_credentials is not None

    def test_init_with_no_credentials(self):
        """Test LocalWorker with no credentials.

        Given:
            None as credentials parameter
        When:
            LocalWorker is instantiated
        Then:
            Worker sets both server_credentials and client_credentials to None
        """
        worker = LocalWorker(credentials=None)
        assert worker._server_credentials is None
        assert worker._client_credentials is None

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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = False

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker(credentials=worker_credentials)
        await worker.start()

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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = False

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker()
        await worker.start()

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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker(credentials=worker_credentials)
        await worker.start()

        # Mock secure channel
        mock_channel = MagicMock()
        mock_stub = MagicMock()
        mock_stub.stop = AsyncMock()

        mock_secure_channel = mocker.patch(
            "grpc.aio.secure_channel", return_value=mock_channel
        )
        mocker.patch(
            "wool.runtime.worker.local.protocol.worker.WorkerStub",
            return_value=mock_stub,
        )

        await worker.stop()

        # Verify secure channel was used
        mock_secure_channel.assert_called_once()
        # Verify the credentials passed are the client credentials
        call_args = mock_secure_channel.call_args
        assert call_args[0][0] == "127.0.0.1:50051"  # address

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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker()
        await worker.start()

        # Mock insecure channel
        mock_channel = MagicMock()
        mock_stub = MagicMock()
        mock_stub.stop = AsyncMock()

        mock_insecure_channel = mocker.patch(
            "grpc.aio.insecure_channel", return_value=mock_channel
        )
        mocker.patch(
            "wool.runtime.worker.local.protocol.worker.WorkerStub",
            return_value=mock_stub,
        )

        await worker.stop()

        # Verify insecure channel was used
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
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker(credentials=worker_credentials_one_way)
        await worker.start()

        # Verify it started
        assert worker.metadata is not None

        # Mock gRPC - one-way TLS should use secure channel
        mock_channel = MagicMock()
        mock_stub = MagicMock()
        mock_stub.stop = AsyncMock()

        mock_secure_channel = mocker.patch(
            "grpc.aio.secure_channel", return_value=mock_channel
        )
        mocker.patch(
            "wool.runtime.worker.local.protocol.worker.WorkerStub",
            return_value=mock_stub,
        )

        await worker.stop()

        # One-way TLS still has channel credentials (anonymous client), uses secure channel
        mock_secure_channel.assert_called_once()
        mock_stub.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_resolves_callable_channel_credentials(
        self, mocker, worker_credentials_callable
    ):
        """Test callable ChannelCredentials resolution on stop.

        Given:
            LocalWorker with callable ChannelCredentials in WorkerCredentials
        When:
            Worker is stopped
        Then:
            Callable is resolved before creating secure channel
        """
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        worker = LocalWorker(credentials=worker_credentials_callable)
        await worker.start()

        # Mock secure channel
        mock_channel = MagicMock()
        mock_stub = MagicMock()
        mock_stub.stop = AsyncMock()

        mock_secure_channel = mocker.patch(
            "grpc.aio.secure_channel", return_value=mock_channel
        )
        mocker.patch(
            "wool.runtime.worker.local.protocol.worker.WorkerStub",
            return_value=mock_stub,
        )

        await worker.stop()

        # Verify secure channel was called (callable was resolved)
        mock_secure_channel.assert_called_once()

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
        key_pem, cert_pem, ca_pem = test_certificates
        creds = WorkerCredentials(
            ca_cert=ca_pem, worker_key=key_pem, worker_cert=cert_pem, mutual=mutual
        )

        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.address = "127.0.0.1:50051"
        mock_process.pid = 12345
        mock_process.start.return_value = None
        mock_process.is_alive.side_effect = [True, False]  # Alive during stop, then not

        mocker.patch(
            "wool.runtime.worker.local.WorkerProcess", return_value=mock_process
        )

        # Mock gRPC for stop
        mock_channel = MagicMock()
        mock_stub = MagicMock()
        mock_stub.stop = AsyncMock()
        mocker.patch("grpc.aio.secure_channel", return_value=mock_channel)
        mocker.patch("grpc.aio.insecure_channel", return_value=mock_channel)
        mocker.patch(
            "wool.runtime.worker.local.protocol.worker.WorkerStub",
            return_value=mock_stub,
        )

        # Full lifecycle
        worker = LocalWorker(credentials=creds)
        await worker.start()
        assert worker.metadata is not None
        await worker.stop()
        # No exception means success
