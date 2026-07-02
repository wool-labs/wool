import asyncio
from types import MappingProxyType
from uuid import uuid4

import grpc.aio
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool import protocol
from wool.runtime.worker import local as local_module
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.base import ChannelOptions
from wool.runtime.worker.base import WorkerLike
from wool.runtime.worker.base import WorkerOptions
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.metadata import WorkerMetadata
from wool.runtime.worker.process import WorkerProcess


def _make_metadata(address="127.0.0.1:50051", pid=12345, secure=False) -> WorkerMetadata:
    """Build a minimal WorkerMetadata for mock processes."""
    return WorkerMetadata(
        uid=uuid4(),
        address=address,
        pid=pid,
        version="1.0.0",
        secure=secure,
    )


def _make_started_process(mocker, *, alive=True):
    """Build a WorkerProcess mock and patch it into local_module."""
    mock_process = mocker.MagicMock(spec=WorkerProcess)
    mock_process.address = "127.0.0.1:50051"
    mock_process.metadata = _make_metadata()
    mock_process.start.return_value = None
    mock_process.is_alive.return_value = alive
    mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)
    return mock_process


def _mock_stop_channel(mocker, *, stop_side_effect=None):
    """Mock the insecure channel and stub behind the graceful stop RPC."""
    mock_channel = mocker.MagicMock()
    mock_channel.close = mocker.AsyncMock()
    mock_stub = mocker.MagicMock()
    mock_stub.stop = mocker.AsyncMock(side_effect=stop_side_effect)
    mocker.patch.object(grpc.aio, "insecure_channel", return_value=mock_channel)
    mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
    return mock_channel, mock_stub


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
            channel=ChannelOptions(
                max_receive_message_length=50 * 1024 * 1024,
                max_send_message_length=25 * 1024 * 1024,
            ),
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
        mock_process.metadata = _make_metadata()
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
        mock_process.metadata = _make_metadata()
        mock_process.start.return_value = None

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()

        # Act
        await worker.start(timeout=60.0)

        # Assert
        mock_process.start.assert_called_once_with(timeout=60.0)

    @pytest.mark.asyncio
    async def test_start_uses_metadata_from_process(self, mocker):
        """Test _start method uses WorkerMetadata from the process.

        Given:
            A LocalWorker with tags and extra metadata
        When:
            start() is called
        Then:
            It should use the WorkerMetadata returned by the process
        """
        # Arrange
        expected_metadata = WorkerMetadata(
            uid=uuid4(),
            address="192.168.1.100:50051",
            pid=12345,
            version="1.0.0",
            tags=frozenset({"gpu", "ml"}),
            extra=MappingProxyType({"region": "us-west"}),
        )
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.metadata = expected_metadata
        mock_process.start.return_value = None

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker("gpu", "ml", region="us-west")

        # Act
        await worker.start()

        # Assert
        assert worker.metadata is expected_metadata

    @pytest.mark.asyncio
    async def test_start_raises_error_if_no_metadata(self, mocker):
        """Test _start raises error if WorkerProcess has no metadata.

        Given:
            A LocalWorker where WorkerProcess.start does not
            populate metadata
        When:
            start() is called
        Then:
            It should raise RuntimeError
        """
        # Arrange
        mock_process = mocker.MagicMock(spec=WorkerProcess)
        mock_process.metadata = None
        mock_process.start.return_value = None

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()

        # Act & assert
        with pytest.raises(RuntimeError, match="no metadata"):
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
        mock_process.metadata = _make_metadata(address="0.0.0.0:8080", pid=99999)
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
        _make_started_process(mocker)
        _, mock_stub = _mock_stop_channel(mocker)

        worker = LocalWorker()
        await worker.start()

        # Act
        await worker.stop()

        # Assert
        mock_stub.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_should_reap_process_after_graceful_stop(self, mocker):
        """Test stop reaps the worker process after the graceful RPC.

        Given:
            A started LocalWorker whose graceful stop RPC succeeds
        When:
            stop() is called with a timeout
        Then:
            It should send a stop request carrying that timeout, then
            reap the worker process with it
        """
        # Arrange
        mock_process = _make_started_process(mocker)
        _, mock_stub = _mock_stop_channel(mocker)

        worker = LocalWorker()
        await worker.start()

        teardown = mocker.MagicMock()
        teardown.attach_mock(mock_stub.stop, "rpc_stop")
        teardown.attach_mock(mock_process.reap, "reap")

        # Act
        await worker.stop(timeout=12.5)

        # Assert
        mock_stub.stop.assert_awaited_once()
        request = mock_stub.stop.await_args.args[0]
        assert request.timeout == 12.5
        mock_process.reap.assert_called_once_with(12.5)
        # Reap must follow the graceful request: reap-first would park
        # in join with no stop ever asked of the worker, then SIGTERM
        # a healthy process.
        call_names = [name for name, _, _ in teardown.mock_calls]
        assert call_names.index("rpc_stop") < call_names.index("reap")

    @pytest.mark.asyncio
    @settings(
        max_examples=25,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(timeout=st.floats(min_value=0.125, max_value=4096.0, width=32))
    async def test_stop_should_bound_rpc_deadline_when_timeout_finite(
        self, mocker, timeout
    ):
        """Test any finite stop timeout yields a strictly larger deadline.

        Given:
            Any finite positive stop timeout
        When:
            stop() is called with it
        Then:
            It should forward the timeout in the stop request, bound
            the RPC by deadline = timeout + _STOP_RPC_MARGIN, and reap
            with the same timeout
        """
        # Arrange
        mock_process = _make_started_process(mocker)
        _, mock_stub = _mock_stop_channel(mocker)

        worker = LocalWorker()
        await worker.start()

        # Act
        await worker.stop(timeout=timeout)

        # Assert
        request = mock_stub.stop.await_args.args[0]
        assert request.timeout == timeout
        deadline = mock_stub.stop.await_args.kwargs["timeout"]
        assert deadline == timeout + local_module._STOP_RPC_MARGIN
        assert deadline > timeout
        mock_process.reap.assert_called_once_with(timeout)

    @pytest.mark.asyncio
    async def test_stop_should_reap_process_when_grpc_stop_fails(self, mocker):
        """Test stop reaps the worker process even if the RPC fails.

        Given:
            A started LocalWorker whose graceful stop RPC raises
        When:
            stop() is called
        Then:
            It should propagate the error and still reap the process
        """
        # Arrange
        mock_process = _make_started_process(mocker)
        mock_channel, _ = _mock_stop_channel(
            mocker, stop_side_effect=Exception("gRPC error")
        )

        worker = LocalWorker()
        await worker.start()

        # Act & assert
        with pytest.raises(Exception, match="gRPC error"):
            await worker.stop()

        mock_process.reap.assert_called_once_with(None)
        mock_channel.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_should_reap_process_when_not_alive(self, mocker):
        """Test stop reaps the worker process even when already dead.

        Given:
            A started LocalWorker whose process is no longer alive
        When:
            stop() is called
        Then:
            It should skip the RPC but still reap the process
        """
        # Arrange
        mock_process = _make_started_process(mocker, alive=False)

        worker = LocalWorker()
        await worker.start()

        mock_channel_fn = mocker.patch.object(grpc.aio, "insecure_channel")

        # Act
        await worker.stop()

        # Assert
        mock_channel_fn.assert_not_called()
        mock_process.reap.assert_called_once_with(None)

    @pytest.mark.asyncio
    async def test_stop_should_not_set_rpc_deadline_when_timeout_none(self, mocker):
        """Test stop leaves the graceful RPC unbounded without a timeout.

        Given:
            A started LocalWorker whose graceful stop RPC succeeds
        When:
            stop() is called without a timeout
        Then:
            It should send the stop request with no gRPC deadline and
            reap without a bound override
        """
        # Arrange
        mock_process = _make_started_process(mocker)
        _, mock_stub = _mock_stop_channel(mocker)

        worker = LocalWorker()
        await worker.start()

        # Act
        await worker.stop()

        # Assert
        mock_stub.stop.assert_awaited_once()
        assert mock_stub.stop.await_args.kwargs["timeout"] is None
        mock_process.reap.assert_called_once_with(None)

    @pytest.mark.asyncio
    async def test_stop_should_reap_process_when_cancelled_mid_rpc(self, mocker):
        """Test stop reaps the worker even when cancelled mid-RPC.

        Given:
            A started LocalWorker whose graceful stop RPC never
            completes
        When:
            The stop() task is cancelled while the RPC is in flight
        Then:
            It should raise CancelledError to the awaiter and still
            reap the worker process with the stop timeout
        """
        # Arrange
        mock_process = _make_started_process(mocker)

        worker = LocalWorker()
        await worker.start()

        rpc_started = asyncio.Event()

        async def hang_forever(request, timeout=None):
            rpc_started.set()
            await asyncio.Event().wait()

        _mock_stop_channel(mocker, stop_side_effect=hang_forever)

        # Act
        stop_task = asyncio.create_task(worker.stop(timeout=3.0))
        await rpc_started.wait()
        stop_task.cancel()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await stop_task
        mock_process.reap.assert_called_once_with(3.0)

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
        mock_process.metadata = _make_metadata(secure=True)
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
        mock_process.metadata = _make_metadata()
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
        mock_process.metadata = _make_metadata()
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker(credentials=worker_credentials)
        await worker.start()

        mock_channel = mocker.MagicMock()
        mock_channel.close = mocker.AsyncMock()
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
        mock_process.metadata = _make_metadata()
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker()
        await worker.start()

        mock_channel = mocker.MagicMock()
        mock_channel.close = mocker.AsyncMock()
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
        mock_process.metadata = _make_metadata()
        mock_process.start.return_value = None
        mock_process.is_alive.return_value = True

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        worker = LocalWorker(credentials=worker_credentials_one_way)
        await worker.start()

        assert worker.metadata is not None

        mock_channel = mocker.MagicMock()
        mock_channel.close = mocker.AsyncMock()
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
        mock_process.metadata = _make_metadata()
        mock_process.start.return_value = None
        mock_process.is_alive.side_effect = [True, False]

        mocker.patch.object(local_module, "WorkerProcess", return_value=mock_process)

        mock_channel = mocker.MagicMock()
        mock_channel.close = mocker.AsyncMock()
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

    def test___init___with_backpressure(self, mocker):
        """Test LocalWorker initialization with backpressure hook.

        Given:
            A callable backpressure hook
        When:
            LocalWorker is instantiated with backpressure=hook
        Then:
            It should forward the hook to WorkerProcess
        """
        # Arrange
        MockWorkerProcess = mocker.patch.object(local_module, "WorkerProcess")

        def hook(ctx):
            return ctx.active_task_count >= 4

        # Act
        LocalWorker(backpressure=hook)

        # Assert
        MockWorkerProcess.assert_called_once()
        assert MockWorkerProcess.call_args.kwargs["backpressure"] is hook
