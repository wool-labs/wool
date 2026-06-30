import asyncio
import logging
import os
import pickle
from typing import Callable
from typing import Coroutine
from uuid import uuid4

import cloudpickle
import grpc
import grpc.aio
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

import wool
from wool import protocol
from wool.runtime.context.exceptions import SerializationWarning
from wool.runtime.context.var import ContextVar
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.auth import WorkerCredentialsProvider
from wool.runtime.worker.base import ChannelOptions
from wool.runtime.worker.connection import HandshakeError
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import UnexpectedResponse
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.connection import clear_channel_pool

from .conftest import PicklableMock


def _secure_provider(identity: str | None = None) -> WorkerCredentialsProvider:
    """Build a static provider over dummy credential bytes.

    For tests that only need a secure (non-None) provider; gRPC defers PEM
    validation to the handshake, which these tests never reach.
    """
    credentials = WorkerCredentials(
        ca_cert=b"ca", worker_key=b"key", worker_cert=b"cert"
    )
    return WorkerCredentialsProvider(lambda: credentials, identity=identity)


class MyAppError(Exception):
    """Custom user exception subclass defined at module scope.

    Defined here (not inside a test) so cloudpickle can resolve the
    class on deserialization in tests that round-trip user-defined
    exception types through Nack.exception payloads.
    """


@pytest.fixture
def sample_task(mocker: MockerFixture):
    """Provides a mock :class:`Task` for testing.

    Creates a Task with a simple async function that returns a
    test value.
    """

    async def sample_task():
        return "test_result"

    mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

    return Task(
        id=uuid4(),
        callable=sample_task,
        args=(),
        kwargs={},
        proxy=mock_proxy,
    )


@pytest.fixture
def async_stream():
    """Provides a factory for converting iterables into async generators.

    Returns a function that takes any iterable and converts it into an
    async generator, making it easy to create mock gRPC response streams.
    """

    async def create_async_stream(iterable):
        """Convert an iterable into an async generator.

        Args:
            iterable: Any iterable (list, tuple, generator, etc.)
        """
        for item in iterable:
            if isinstance(item, Callable):
                item()
            elif isinstance(item, Coroutine):
                await item
            else:
                yield item

    return create_async_stream


@pytest.fixture
def mock_grpc_call(mocker: MockerFixture):
    """Provides a factory for creating mock gRPC call objects.

    Returns a function that creates a mock gRPC call with configurable
    stream iterator and cancel behavior.
    """

    def create_call(stream_iterator, cancel_raises=False):
        """Create a mock gRPC call object for bidi-streaming.

        Args:
            stream_iterator: The async iterator to wrap
            cancel_raises: If True, cancel() raises RuntimeError
        """
        mock_call = mocker.MagicMock()
        mock_call.__aiter__ = lambda _: stream_iterator
        mock_call.write = mocker.AsyncMock()
        mock_call.done_writing = mocker.AsyncMock()

        if cancel_raises:
            mock_call.cancel = mocker.MagicMock(
                side_effect=RuntimeError("cancel failed")
            )
        else:
            mock_call.cancel = mocker.MagicMock()

        return mock_call

    return create_call


class TestHandshakeError:
    """Test suite for the HandshakeError exception type."""

    def test___init___should_set_code_and_details(self):
        """Test HandshakeError records its gRPC fields.

        Given:
            A status code and details.
        When:
            HandshakeError is constructed.
        Then:
            It should expose the code and details.
        """
        # Act
        error = HandshakeError(grpc.StatusCode.UNAVAILABLE, "boom")

        # Assert
        assert error.code is grpc.StatusCode.UNAVAILABLE
        assert error.details == "boom"

    def test___init___should_be_non_transient_rpc_error(self):
        """Test HandshakeError sits in the worker-health hierarchy.

        Given:
            A HandshakeError instance.
        When:
            Its type relationships are inspected.
        Then:
            It should be an RpcError (so non-handshake call sites still catch
            it) but not a TransientRpcError — see HandshakeError for why the
            load balancer skips rather than evicts it.
        """
        # Arrange
        error = HandshakeError()

        # Act & assert
        assert isinstance(error, RpcError)
        assert not isinstance(error, TransientRpcError)

    def test_pickle_roundtrip(self):
        """Test HandshakeError survives a serialization roundtrip.

        Given:
            A HandshakeError carrying a code and details.
        When:
            It is pickled and cloudpickled and restored.
        Then:
            The code and details should survive — it is a wire-crossing type
            raised back to a calling process.
        """
        # Arrange
        error = HandshakeError(grpc.StatusCode.UNAVAILABLE, "boom")

        # Act & assert
        for restore in (
            pickle.loads(pickle.dumps(error)),
            cloudpickle.loads(cloudpickle.dumps(error)),
        ):
            assert restore.code is grpc.StatusCode.UNAVAILABLE
            assert restore.details == "boom"


class TestWorkerConnection:
    @pytest.mark.asyncio
    async def test_dispatch_should_yield_return_value_when_task_returns(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test task dispatch with successful acknowledgment and result.

        Given:
            A connection instance with capacity available
        When:
            A task that returns a value is dispatched and acknowledged
        Then:
            It should convert the task to protobuf, dispatch to the worker
            stub, and yield the return value yielded from the gRPC response stream
        """
        # Arrange
        to_protobuf_spy = mocker.spy(Task, "to_protobuf")

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("test_result"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        to_protobuf_spy.assert_called_once()
        mock_stub.dispatch.assert_called_once()
        assert len(results) == 1
        assert results[0] == "test_result"

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_exception_when_task_raises(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test task dispatch when task raises an exception.

        Given:
            A connection instance with capacity available
        When:
            A task that raises an exception is dispatched and acknowledged
        Then:
            It should convert the task to protobuf, dispatch to the worker
            stub, and raise the exception yielded from the gRPC response stream
        """
        # Arrange
        to_protobuf_spy = mocker.spy(Task, "to_protobuf")

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(
                    dump=cloudpickle.dumps(ValueError("task_error"))
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(ValueError, match="task_error"):
            async for _ in await connection.dispatch(sample_task):
                pass

        to_protobuf_spy.assert_called_once()
        mock_stub.dispatch.assert_called_once()

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_unexpected_response_when_not_acked(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test task dispatch when acknowledgment is not received.

        Given:
            A connection instance with capacity available
        When:
            A task is dispatched, but not acknowledged
        Then:
            It should raise UnexpectedResponse and cancel the call
        """
        # Arrange
        responses = (
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("unexpected"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(UnexpectedResponse, match="Expected 'ack' response"):
            async for _ in await connection.dispatch(sample_task):
                pass

        # Handshake-phase failure: ``_handshake`` raises before
        # ``_DispatchStream`` is constructed, so only the
        # AsyncExitStack's ``_safe_cancel`` callback fires on
        # unwind — exactly one cancel.
        assert mock_call.cancel.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "cancel_raises",
        [False, True],
        ids=["cancel_succeeds", "cancel_raises"],
    )
    async def test_dispatch_should_raise_unexpected_response_when_unexpected_after_ack(
        self,
        mocker: MockerFixture,
        sample_task,
        mock_grpc_call,
        async_stream,
        cancel_raises: bool,
    ):
        """Test task dispatch when stub yields an unexpected response after
        successful acknowledgement.

        Given:
            A connection instance with capacity available
        When:
            A task is dispatched and acknowledged, but the stub subsequently
            yields an unexpected response
        Then:
            It should raise UnexpectedResponse and attempt to cancel the call,
            regardless of whether call.cancel() raises an exception
        """
        # Arrange
        ack_resp = protocol.Response(ack=protocol.Ack())
        responses = (ack_resp, protocol.Response())
        mock_call = mock_grpc_call(async_stream(responses), cancel_raises=cancel_raises)

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(
            UnexpectedResponse, match="Expected 'result' or 'exception' response"
        ):
            async for _ in await connection.dispatch(sample_task):
                pass

        # Stream-phase failure: ``_DispatchStream._read_next``'s
        # ``except BaseException`` calls ``self._call.cancel()`` once
        # before re-raising, then the AsyncExitStack's ``_safe_cancel``
        # callback fires a second time on stack unwind — both are
        # internally guarded with ``except Exception: pass`` so the
        # cancel-raising parametrization does not change the count.
        assert mock_call.cancel.call_count == 2

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status_code",
        [
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.UNAVAILABLE,
        ],
    )
    async def test_dispatch_should_raise_transient_rpc_error_when_stub_raises_transient(
        self, mocker: MockerFixture, sample_task, status_code: grpc.StatusCode
    ):
        """Test task dispatch when stub raises a transient RPC error.

        Given:
            A connection instance with capacity available
        When:
            A task is dispatched and the stub raises a transient RPC error
        Then:
            It should raise TransientRpcError from the grpc.RpcError
        """

        # Arrange
        # Create a custom exception class that inherits from grpc.RpcError
        class MockRpcError(grpc.RpcError):
            def code(self):
                return status_code

            def details(self):
                return "mock error details"

        mock_rpc_error = MockRpcError()

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=mock_rpc_error)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(TransientRpcError):
            async for _ in await connection.dispatch(sample_task):
                pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status_code",
        [
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.NOT_FOUND,
            grpc.StatusCode.PERMISSION_DENIED,
            grpc.StatusCode.UNIMPLEMENTED,
        ],
    )
    async def test_dispatch_should_raise_rpc_error_when_stub_raises_nontransient(
        self, mocker: MockerFixture, sample_task, status_code: grpc.StatusCode
    ):
        """Test task dispatch when stub raises a non-transient RPC error.

        Given:
            A connection instance with capacity available
        When:
            A task is dispatched and the stub raises a non-transient RPC error
        Then:
            It should raise RpcError from the grpc.RpcError
        """

        # Arrange
        # Create a custom exception class that inherits from grpc.RpcError
        class MockRpcError(grpc.RpcError):
            def code(self):
                return status_code

            def details(self):
                return "mock error details"

        mock_rpc_error = MockRpcError()

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=mock_rpc_error)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(RpcError):
            async for _ in await connection.dispatch(sample_task):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_handshake_error_when_peer_unauthenticated(
        self, mocker: MockerFixture, sample_task
    ):
        """Test dispatch surfaces a rejected client certificate distinctly.

        Given:
            A secure connection whose stub raises UNAUTHENTICATED.
        When:
            A task is dispatched.
        Then:
            It should raise HandshakeError (the peer rejected the client's
            certificate).
        """

        # Arrange
        class MockRpcError(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAUTHENTICATED

            def details(self):
                return "peer certificate rejected"

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=MockRpcError())
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "localhost:50051",
            credentials=_secure_provider(),
            options=ChannelOptions(max_concurrent_streams=10),
        )

        # Act & assert
        with pytest.raises(HandshakeError) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
        assert exc_info.value.code is grpc.StatusCode.UNAUTHENTICATED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "secure, details, debug",
        [
            # Distinct secure-handshake flavors (CA rejection, expired cert,
            # generic TLS failure) all surface as the same HandshakeError.
            (True, "", "Ssl handshake: certificate verify failed"),
            (True, "certificate has expired", ""),
            (True, "", "tls handshake eof"),
            # An insecure client that reached a TLS-only worker.
            (False, "", "Ssl handshake failed"),
        ],
    )
    async def test_dispatch_should_raise_handshake_error_when_tls_handshake_fails(
        self,
        mocker: MockerFixture,
        sample_task,
        secure: bool,
        details: str,
        debug: str,
    ):
        """Test dispatch classifies a failed TLS handshake structurally.

        Given:
            A connection whose stub raises UNAVAILABLE carrying TLS evidence
            in its error text.
        When:
            A task is dispatched.
        Then:
            It should raise HandshakeError, whatever the TLS failure flavor —
            the failure is not sub-classified.
        """

        # Arrange
        class MockRpcError(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

            def details(self):
                return details

            def debug_error_string(self):
                return debug

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=MockRpcError())
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "localhost:50051",
            credentials=_secure_provider() if secure else None,
            options=ChannelOptions(max_concurrent_streams=10),
        )

        # Act & assert
        with pytest.raises(HandshakeError):
            async for _ in await connection.dispatch(sample_task):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_transient_error_when_unavailable_not_tls(
        self, mocker: MockerFixture, sample_task
    ):
        """Test dispatch keeps a plain unreachable worker transient.

        Given:
            A connection whose stub raises UNAVAILABLE with no TLS evidence
            in its error text.
        When:
            A task is dispatched.
        Then:
            It should raise TransientRpcError and not HandshakeError, so a
            genuinely unreachable worker is not mistaken for a handshake
            failure.
        """

        # Arrange
        class MockRpcError(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

            def details(self):
                return "failed to connect to all addresses"

            def debug_error_string(self):
                return "failed to connect to all addresses"

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=MockRpcError())
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "localhost:50051",
            credentials=_secure_provider(),
            options=ChannelOptions(max_concurrent_streams=10),
        )

        # Act & assert
        with pytest.raises(TransientRpcError):
            async for _ in await connection.dispatch(sample_task):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_should_redact_debug_string_from_handshake_details(
        self, mocker: MockerFixture, sample_task
    ):
        """Test the handshake error does not leak gRPC's debug string.

        Given:
            A handshake failure whose ``details()`` is empty and whose debug
            string carries an internal peer address and source paths.
        When:
            A task is dispatched.
        Then:
            The raised HandshakeError's details should be a fixed message,
            not the verbose gRPC debug blob, so internal topology does not
            ride into logs or across the wire.
        """

        # Arrange
        class MockRpcError(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

            def details(self):
                return ""

            def debug_error_string(self):
                return "Ssl handshake failed; peer 10.1.2.3:8443; src/core/tsi/ssl.cc"

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=MockRpcError())
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "localhost:50051",
            credentials=_secure_provider(),
            options=ChannelOptions(max_concurrent_streams=10),
        )

        # Act & assert
        with pytest.raises(HandshakeError) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
        assert "10.1.2.3" not in exc_info.value.details
        assert "src/core" not in exc_info.value.details
        assert "secure handshake failed" in exc_info.value.details

    @pytest.mark.asyncio
    async def test_dispatch_should_classify_hostname_verification_as_handshake_failure(
        self, mocker: MockerFixture, sample_task
    ):
        """Test a real gRPC hostname-verification failure is gated as a handshake.

        Given:
            A handshake failure whose text is gRPC's verbatim
            ``ssl_target_name_override`` mismatch ("Hostname Verification
            Check failed") — a drift canary for the broad handshake gate.
        When:
            A task is dispatched.
        Then:
            It should raise HandshakeError, so an identity mismatch stays
            diagnosable as a handshake failure and is not mistaken for plain
            unreachability.
        """

        # Arrange — verbatim text from a gRPC ssl_target_name_override
        # mismatch (captured from grpc.aio against a real worker).
        class MockRpcError(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.UNAVAILABLE

            def details(self):
                return (
                    "failed to connect to all addresses; last error: UNKNOWN: "
                    "ipv4:127.0.0.1:51127: Custom verification check failed with "
                    "error: UNAUTHENTICATED: Hostname Verification Check failed."
                )

            def debug_error_string(self):
                return self.details()

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=MockRpcError())
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "localhost:50051",
            credentials=_secure_provider(),
            options=ChannelOptions(max_concurrent_streams=10),
        )

        # Act & assert
        with pytest.raises(HandshakeError):
            async for _ in await connection.dispatch(sample_task):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_should_override_target_name_when_identity_configured(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch verifies the worker against a configured identity.

        Given:
            A connection whose provider carries an expected identity and a
            target that is a bare network address.
        When:
            A task is dispatched.
        Then:
            The secure channel should be built with a
            grpc.ssl_target_name_override option for the identity, so the
            certificate is verified against the identity rather than the
            dialed address.
        """
        # Arrange
        mock_channel = mocker.AsyncMock()
        secure_spy = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(
            return_value=mock_grpc_call(async_stream(responses))
        )
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "10.0.0.7:50051", credentials=_secure_provider(identity="wool-worker")
        )

        # Act
        results = [result async for result in await connection.dispatch(sample_task)]

        # Assert
        assert results == ["ok"]
        options = secure_spy.call_args.kwargs["options"]
        assert ("grpc.ssl_target_name_override", "wool-worker") in options

        # Cleanup
        await connection.close()

    @pytest.mark.asyncio
    async def test_dispatch_should_not_override_target_name_when_identity_none(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch leaves address verification intact without identity.

        Given:
            A connection whose provider carries no identity.
        When:
            A task is dispatched.
        Then:
            The secure channel should be built without a
            grpc.ssl_target_name_override option, preserving the legacy
            address-based verification.
        """
        # Arrange
        mock_channel = mocker.AsyncMock()
        secure_spy = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(
            return_value=mock_grpc_call(async_stream(responses))
        )
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection("10.0.0.7:50051", credentials=_secure_provider())

        # Act
        results = [result async for result in await connection.dispatch(sample_task)]

        # Assert
        assert results == ["ok"]
        option_keys = [key for key, _ in secure_spy.call_args.kwargs["options"]]
        assert "grpc.ssl_target_name_override" not in option_keys

        # Cleanup
        await connection.close()

    @pytest.mark.asyncio
    async def test_dispatch_should_override_target_name_from_bare_credentials_identity(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test a bare WorkerCredentials identity overrides the SAN target.

        Given:
            A connection built from a bare `WorkerCredentials` (not a provider)
            that carries an identity.
        When:
            A task is dispatched.
        Then:
            The secure channel should carry the grpc.ssl_target_name_override
            option for that identity — the ``credentials`` union coerces the
            bare value and its identity flows through.
        """
        # Arrange
        mock_channel = mocker.AsyncMock()
        secure_spy = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(
            return_value=mock_grpc_call(async_stream(responses))
        )
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "10.0.0.7:50051",
            credentials=WorkerCredentials(
                ca_cert=b"ca",
                worker_key=b"key",
                worker_cert=b"cert",
                identity="wool-worker",
            ),
        )

        # Act
        results = [result async for result in await connection.dispatch(sample_task)]

        # Assert
        assert results == ["ok"]
        options = secure_spy.call_args.kwargs["options"]
        assert ("grpc.ssl_target_name_override", "wool-worker") in options

        # Cleanup
        await connection.close()

    @pytest.mark.asyncio
    async def test_dispatch_should_reuse_channel_when_credentials_unchanged(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test unchanged credential material reuses one pooled channel.

        Given:
            A connection over a static provider, dispatched twice.
        When:
            The second dispatch runs with the credential material
            unchanged.
        Then:
            The secure channel should be built only once — the pooled
            channel is reused because the credentials value is
            unchanged.
        """
        # Arrange
        mock_channel = mocker.AsyncMock()
        secure_spy = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )

        def fresh_call(*args, **kwargs):
            responses = (
                protocol.Response(ack=protocol.Ack()),
                protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
            )
            return mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=fresh_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection("10.0.0.7:50051", credentials=_secure_provider())

        # Act
        async for _ in await connection.dispatch(sample_task):
            pass
        async for _ in await connection.dispatch(sample_task):
            pass

        # Assert
        assert secure_spy.call_count == 1

        # Cleanup
        await connection.close()

    @pytest.mark.asyncio
    async def test_dispatch_should_create_new_channel_when_credentials_rotated(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
        tmp_path,
        test_certificates,
    ):
        """Test rotated credential material yields a fresh channel.

        Given:
            A connection over a reloading file provider, dispatched once,
            after which the CA file is rotated on disk.
        When:
            A second task is dispatched.
        Then:
            A second secure channel should be built — the rotated material
            resolves to a different credentials value and a new pooled
            channel, while the original channel remains for in-flight work.
        """
        # Arrange — real PEM so the provider's validate-before-cache passes;
        # rotation appends ignored trailing bytes for a distinct credentials value.
        key_pem, cert_pem, ca_pem = test_certificates
        ca_path = tmp_path / "ca.pem"
        key_path = tmp_path / "key.pem"
        cert_path = tmp_path / "cert.pem"
        ca_path.write_bytes(ca_pem)
        key_path.write_bytes(key_pem)
        cert_path.write_bytes(cert_pem)
        provider = WorkerCredentialsProvider(
            lambda: WorkerCredentials.from_files(
                str(ca_path), str(key_path), str(cert_path)
            ),
            reloadable=True,
        )

        mock_channel = mocker.AsyncMock()
        secure_spy = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )

        def fresh_call(*args, **kwargs):
            responses = (
                protocol.Response(ack=protocol.Ack()),
                protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
            )
            return mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=fresh_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection("10.0.0.7:50051", credentials=provider)

        # Act
        async for _ in await connection.dispatch(sample_task):
            pass
        rotated_mtime = os.stat(ca_path).st_mtime_ns + 1_000_000_000
        ca_path.write_bytes(ca_pem + b"\n# rotated\n")
        os.utime(ca_path, ns=(rotated_mtime, rotated_mtime))
        async for _ in await connection.dispatch(sample_task):
            pass

        # Assert
        assert secure_spy.call_count == 2

        # Cleanup
        await connection.close()

    @pytest.mark.asyncio
    async def test_dispatch_should_finish_inflight_stream_after_rotation(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
        tmp_path,
        test_certificates,
    ):
        """Test an in-flight dispatch is not torn down by a rotation.

        Given:
            A connection over a reloading file provider, with a dispatch
            primed and its stream still open.
        When:
            The credential material is rotated and the open stream is then
            consumed to completion.
        Then:
            The stream should finish on its original channel — rotation is
            adopted at the next connection, never by interrupting work in
            flight — so no replacement channel is built for it.
        """
        # Arrange
        key_pem, cert_pem, ca_pem = test_certificates
        ca_path = tmp_path / "ca.pem"
        key_path = tmp_path / "key.pem"
        cert_path = tmp_path / "cert.pem"
        ca_path.write_bytes(ca_pem)
        key_path.write_bytes(key_pem)
        cert_path.write_bytes(cert_pem)
        provider = WorkerCredentialsProvider(
            lambda: WorkerCredentials.from_files(
                str(ca_path), str(key_path), str(cert_path)
            ),
            reloadable=True,
        )

        mock_channel = mocker.AsyncMock()
        secure_spy = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(
            return_value=mock_grpc_call(async_stream(responses))
        )
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection("10.0.0.7:50051", credentials=provider)

        # Act — prime the dispatch (builds the original channel), then rotate
        # the material before consuming the still-open stream.
        stream = await connection.dispatch(sample_task)
        rotated_mtime = os.stat(ca_path).st_mtime_ns + 1_000_000_000
        ca_path.write_bytes(ca_pem + b"\n# rotated\n")
        os.utime(ca_path, ns=(rotated_mtime, rotated_mtime))
        results = [result async for result in stream]

        # Assert
        assert results == ["ok"]
        assert secure_spy.call_count == 1

        # Cleanup
        await connection.close()

    @pytest.mark.asyncio
    async def test_dispatch_should_use_insecure_uds_when_self_dispatch_secure(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test a secure worker self-dispatches over the insecure loopback.

        Given:
            A connection with a secure provider whose target matches the
            current worker's own address and a UDS address is available.
        When:
            A task is dispatched.
        Then:
            It should route over the insecure UDS channel and never build a
            secure channel — the worker does not do TLS against itself.
        """
        # Arrange
        target = "localhost:50051"
        uds_target = "unix:/tmp/wool-test-secure-self.sock"
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address=target,
            pid=1,
            version="1.0.0",
        )
        wool.__worker_uds_address__ = uds_target

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(
            return_value=mock_grpc_call(async_stream(responses))
        )
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        mock_channel = mocker.AsyncMock()
        insecure_spy = mocker.patch.object(
            grpc.aio, "insecure_channel", return_value=mock_channel
        )
        secure_spy = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )
        connection = WorkerConnection(
            target, credentials=_secure_provider(identity="wool-worker")
        )

        # Act
        results = [result async for result in await connection.dispatch(sample_task)]

        # Assert
        assert results == ["ok"]
        uds_calls = [c for c in insecure_spy.call_args_list if c.args[0] == uds_target]
        assert len(uds_calls) >= 1
        secure_spy.assert_not_called()

    @pytest.mark.asyncio
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(timeout=st.floats(max_value=0.0, allow_nan=False, allow_infinity=False))
    async def test_dispatch_should_raise_when_timeout_not_positive(
        self, mocker: MockerFixture, timeout: float
    ):
        """Test task dispatch with invalid dispatch timeout value.

        Given:
            A connection instance with capacity available
        When:
            A task is dispatched with an invalid dispatch timeout (0 or negative)
        Then:
            It should raise ValueError and not attempt dispatch
        """
        # Arrange
        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        async def test_callable():
            return "test"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        work_task = Task(
            id=uuid4(),
            callable=test_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Act & assert
        with pytest.raises(ValueError, match="Dispatch timeout must be positive"):
            async for _ in await connection.dispatch(work_task, timeout=timeout):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_deadline_when_timeout_exceeded(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test task dispatch when dispatch timeout is exceeded.

        Given:
            A connection instance with capacity available
        When:
            A task is dispatched, but exceeds the dispatch timeout
        Then:
            It should raise TransientRpcError with code DEADLINE_EXCEEDED
            (local timeout is wrapped to align with the load-balancer
            contract — same handling as a server-side gRPC deadline)
            and cancel the call
        """
        # Arrange
        ack = protocol.Response(ack=protocol.Ack())
        responses = (asyncio.sleep(1000), ack)

        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(TransientRpcError) as exc_info:
            async for _ in await connection.dispatch(sample_task, timeout=0.001):
                pass
        assert exc_info.value.code is grpc.StatusCode.DEADLINE_EXCEEDED

        mock_call.cancel.assert_called()

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_deadline_when_concurrency_limit_reached(
        self, mocker: MockerFixture, sample_task, mock_grpc_call, async_stream
    ):
        """Test task dispatch when concurrency limit is reached.

        Given:
            A connection instance with explicit concurrency limit
        When:
            All concurrency slots are occupied and another task is dispatched
            with a timeout
        Then:
            It should raise TransientRpcError with code DEADLINE_EXCEEDED
            while waiting for an available slot — local timeouts are
            wrapped to align with the load-balancer contract
        """
        # Arrange
        responses = (
            protocol.Response(ack=protocol.Ack()),
            asyncio.sleep(10),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("done"))),
        )

        long_running_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=long_running_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=1)
        )

        async def consume_slot():
            async for _ in await connection.dispatch(sample_task):
                pass

        blocking_task = asyncio.create_task(consume_slot())

        # Wait briefly to ensure the first task has acquired the semaphore
        await asyncio.sleep(0.01)

        try:
            # Act & assert
            with pytest.raises(TransientRpcError) as exc_info:
                async for _ in await connection.dispatch(sample_task, timeout=0.01):
                    pass
            assert exc_info.value.code is grpc.StatusCode.DEADLINE_EXCEEDED

            assert mock_stub.dispatch.call_count == 1
        finally:
            blocking_task.cancel()
            try:
                await blocking_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_dispatch_should_release_semaphore_when_handshake_fails(
        self, mocker: MockerFixture, sample_task, mock_grpc_call, async_stream
    ):
        """Test :meth:`WorkerConnection.dispatch` releases the
        channel semaphore when the dispatch handshake fails after
        the permit has been acquired.

        Given:
            A connection with ``max_concurrent_streams=1`` and a
            mock gRPC call whose ``write`` raises a non-transient
            :class:`grpc.RpcError`.
        When:
            :meth:`WorkerConnection.dispatch` is awaited.
        Then:
            It should raise :class:`RpcError` and release the
            channel semaphore so the slot is available for the
            next dispatch on the same connection.
        """
        from wool.runtime.worker import connection as connection_module

        class MockRpcError(grpc.RpcError):
            def code(self):
                return grpc.StatusCode.INTERNAL

            def details(self):
                return "handshake write failed"

        responses = (protocol.Response(ack=protocol.Ack()),)
        mock_call = mock_grpc_call(async_stream(responses))
        mock_call.write = mocker.AsyncMock(side_effect=MockRpcError())

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        # max_concurrent_streams=1 so a leaked permit is observable
        # as a permanently-locked semaphore.
        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=1)
        )

        # Act
        with pytest.raises(RpcError):
            await connection.dispatch(sample_task)

        # Assert — the cached channel's semaphore must be released
        # so the next dispatch is not blocked. With
        # ``max_concurrent_streams=1`` a held permit means
        # ``locked() is True``; a released permit means
        # ``locked() is False``.
        entry = connection_module._channel_pool._cache.get(connection._key)
        assert entry is not None, "channel should be cached after dispatch acquire"
        channel = entry.obj
        assert not channel.semaphore.locked(), (
            "channel.semaphore must be released after handshake failure"
        )

    @pytest.mark.asyncio
    async def test_dispatch_should_release_semaphore_when_stream_acloses_early(
        self, mocker: MockerFixture, sample_task, mock_grpc_call, async_stream
    ):
        """Test that closing a primed stream before iterating any
        value releases the channel semaphore.

        Given:
            A connection with ``max_concurrent_streams=1`` and a
            successful dispatch that returns a primed but
            unconsumed stream.
        When:
            ``aclose()`` is called on the stream without iterating
            any value.
        Then:
            It should release the channel semaphore so a
            subsequent dispatch on the same connection has a
            permit available.
        """
        from wool.runtime.worker import connection as connection_module

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("unused"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=1)
        )

        # Act
        stream = await connection.dispatch(sample_task)
        await stream.aclose()

        # Assert
        entry = connection_module._channel_pool._cache.get(connection._key)
        assert entry is not None, "channel should be cached after dispatch"
        channel = entry.obj
        assert not channel.semaphore.locked(), (
            "channel.semaphore must be released after aclose, even "
            "when no value was iterated from the primed stream"
        )

    @pytest.mark.asyncio
    async def test_dispatch_should_propagate_unwrapped_when_task_encode_fails(
        self, mocker: MockerFixture, sample_task
    ):
        """Test that a caller-side task encode failure propagates
        in its original form rather than being wrapped as
        :class:`RpcError`.

        The :meth:`WorkerConnection.dispatch` contract treats only
        :class:`RpcError` as a worker-health concern; encode-side
        failures surface to the caller in their original form so
        the load balancer does not evict workers on a caller-side
        bug.

        Given:
            A connection and a task whose ``to_protobuf`` raises a
            non-RPC exception.
        When:
            :meth:`WorkerConnection.dispatch` is awaited.
        Then:
            It should propagate the original exception class
            unchanged, not wrap it as :class:`RpcError` or
            :class:`TransientRpcError`.
        """

        class EncodeError(Exception):
            pass

        mocker.patch.object(
            sample_task,
            "to_protobuf",
            side_effect=EncodeError("strict-mode encode failure"),
        )

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=1)
        )

        # Act & assert — if dispatch wrapped this as RpcError,
        # ``pytest.raises(EncodeError)`` would not match.
        with pytest.raises(EncodeError, match="strict-mode encode failure"):
            await connection.dispatch(sample_task)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "cancel_raises",
        [False, True],
        ids=["cancel_succeeds", "cancel_raises"],
    )
    async def test_dispatch_should_cancel_call_and_raise_when_cancelled_during_dispatch(
        self,
        mocker: MockerFixture,
        sample_task,
        mock_grpc_call,
        async_stream,
        cancel_raises: bool,
    ):
        """Test task cancellation during dispatch phase.

        Given:
            A connection instance with capacity available
        When:
            A task is dispatched and subsequently cancelled while waiting for
            acknowledgment
        Then:
            It should cancel the gRPC call and raise CancelledError, regardless
            of whether call.cancel() raises an exception
        """
        # Arrange
        stream_started = asyncio.Event()

        responses = (
            stream_started.set,
            asyncio.sleep(10),
            protocol.Response(ack=protocol.Ack()),
        )

        mock_call = mock_grpc_call(async_stream(responses), cancel_raises=cancel_raises)

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        async def run_dispatch():
            async for _ in await connection.dispatch(sample_task):
                pass

        task = asyncio.create_task(run_dispatch())
        await stream_started.wait()
        task.cancel()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await task

        mock_call.cancel.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "cancel_raises",
        [False, True],
        ids=["cancel_succeeds", "cancel_raises"],
    )
    async def test_dispatch_should_cancel_call_and_raise_when_cancelled_during_execution(
        self,
        mocker: MockerFixture,
        sample_task,
        mock_grpc_call,
        async_stream,
        cancel_raises: bool,
    ):
        """Test task cancellation during execution phase.

        Given:
            A connection instance with an active task
        When:
            A task is cancelled while streaming results
        Then:
            It should cancel the gRPC call gracefully. If call.cancel()
            succeeds, raise CancelledError. If call.cancel() raises, raise that
            exception chained from the original error.
        """
        # Arrange
        execution_started = asyncio.Event()

        responses = (
            protocol.Response(ack=protocol.Ack()),
            execution_started.set,
            asyncio.sleep(10),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("done"))),
        )

        mock_call = mock_grpc_call(async_stream(responses), cancel_raises=cancel_raises)

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        async def run_dispatch():
            async for _ in await connection.dispatch(sample_task):
                pass

        task = asyncio.create_task(run_dispatch())
        await execution_started.wait()
        task.cancel()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await task

        # The gRPC call must be cancelled. Both ``_read_next``'s
        # except-BaseException cleanup and ``_execute``'s outer
        # aclose path call ``call.cancel()`` — gRPC's cancel is
        # idempotent, so 1+ calls is correct.
        assert mock_call.cancel.called

    @pytest.mark.asyncio
    async def test_dispatch_should_release_channel_ref_when_cancelled_during_teardown(
        self, mocker: MockerFixture, sample_task, mock_grpc_call, async_stream
    ):
        """Test external cancellation during teardown releases the
        pooled channel reference.

        Given:
            A dispatched task whose result stream runs to exhaustion
            into teardown, with the process-wide channel pool's lock
            held so the release callback suspends, and the consuming
            task cancelled while parked in that suspended release.
        When:
            The lock is released and the cancelled task is awaited.
        Then:
            It should leave the channel pool with zero referenced
            entries — the dispatch-scope reference is released
            despite the caller's pending cancellation.
        """
        # Arrange
        from wool.runtime.worker import connection as connection_module

        # Contend the pool lock on a fresh instance so it does not
        # bind the process-global lock to this test's event loop.
        mocker.patch.object(connection_module._channel_pool, "_lock", asyncio.Lock())
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("done"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )
        stream = await connection.dispatch(sample_task)
        pool = connection_module._channel_pool

        # Act
        await pool._lock.acquire()
        lock_released = False
        try:

            async def consume():
                async for _ in stream:
                    pass

            task = asyncio.ensure_future(consume())
            for _ in range(5):
                await asyncio.sleep(0)
            task.cancel()
            pool._lock.release()
            lock_released = True
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            if not lock_released:
                pool._lock.release()

        # Assert
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_dispatch_should_release_channel_ref_when_worker_cancels(
        self, mocker: MockerFixture, sample_task, mock_grpc_call, async_stream
    ):
        """Test a worker-side CancelledError releases the pooled
        channel reference during teardown.

        Given:
            A dispatched task whose worker ships an
            ``asyncio.CancelledError`` on the response exception
            frame — which bumps the caller's pending-cancel state —
            with the process-wide channel pool's lock held so the
            release callback suspends.
        When:
            The lock is released and the consuming task is awaited.
        Then:
            It should leave the channel pool with zero referenced
            entries — the dispatch-scope reference is released
            despite the worker-induced pending cancellation.
        """
        # Arrange
        from wool.runtime.worker import connection as connection_module

        # Contend the pool lock on a fresh instance so it does not
        # bind the process-global lock to this test's event loop.
        mocker.patch.object(connection_module._channel_pool, "_lock", asyncio.Lock())
        cancellation = asyncio.CancelledError("worker self-raised cancel")
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(dump=cloudpickle.dumps(cancellation)),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )
        stream = await connection.dispatch(sample_task)
        pool = connection_module._channel_pool

        # Act
        await pool._lock.acquire()
        lock_released = False
        try:

            async def consume():
                async for _ in stream:
                    pass

            task = asyncio.ensure_future(consume())
            for _ in range(5):
                await asyncio.sleep(0)
            pool._lock.release()
            lock_released = True
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            if not lock_released:
                pool._lock.release()

        # Assert
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_dispatch_should_reraise_signal_when_teardown_raises_process_signal(
        self, mocker: MockerFixture, sample_task, mock_grpc_call, async_stream
    ):
        """Test a process signal raised during teardown reaches the caller.

        Given:
            A dispatched task run to completion where a resource-release
            step performed during teardown raises a process-exit signal
            (``SystemExit``).
        When:
            The caller awaits the stream's completion.
        Then:
            The ``SystemExit`` should propagate to the caller and the
            connection's pooled channel resources should still be
            released — the signal is captured off the teardown task and
            re-raised without leaking the pooled reference.
        """
        # Arrange
        from wool.runtime.worker import connection as connection_module

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("done"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))
        # A teardown-side release step (the gRPC call cancel) raises a
        # process-exit signal; the swallow guard only catches Exception,
        # so it escapes the teardown task.
        mock_call.cancel.side_effect = SystemExit("teardown signal")
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )
        pool = connection_module._channel_pool

        # Act & assert
        with pytest.raises(SystemExit, match="teardown signal"):
            async for _ in await connection.dispatch(sample_task):
                pass

        # The pooled reference is still released despite the signal.
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_dispatch_should_detach_teardown_when_release_exceeds_timeout(
        self, mocker: MockerFixture, sample_task, mock_grpc_call, async_stream, caplog
    ):
        """Test a wedged teardown unblocks the caller after the timeout.

        Given:
            A dispatched task whose teardown cannot complete promptly
            because a resource-release step is blocked.
        When:
            The caller awaits the stream's completion.
        Then:
            The caller should unblock within the teardown timeout rather
            than hang, the blocked release should be left to a detached
            task, and a timeout warning should be logged.
        """
        # Arrange
        from wool.runtime.worker import connection as connection_module

        # Contend the pool lock on a fresh instance, and shorten the
        # teardown budget so the wedge resolves quickly.
        mocker.patch.object(connection_module._channel_pool, "_lock", asyncio.Lock())
        mocker.patch.object(connection_module, "_TEARDOWN_TIMEOUT", 0.1)
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("done"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)
        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )
        stream = await connection.dispatch(sample_task)
        pool = connection_module._channel_pool

        # Act — hold the pool lock so the release wedges; the caller must
        # unblock at the timeout instead of hanging.
        await pool._lock.acquire()
        try:

            async def consume():
                async for _ in stream:
                    pass

            with caplog.at_level(
                logging.WARNING, logger="wool.runtime.worker.connection"
            ):
                await asyncio.wait_for(consume(), timeout=5)
        finally:
            pool._lock.release()
            # Let the detached teardown task finish now the lock is free.
            for _ in range(5):
                await asyncio.sleep(0)

        # Assert — the caller unblocked and the timeout was reported.
        assert any("teardown exceeded" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_close_should_be_idempotent(self, mocker: MockerFixture):
        """Test closing a connection is idempotent.

        Given:
            A connection instance
        When:
            Close is called multiple times
        Then:
            It should complete without error each time
        """
        # Arrange
        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert — should not raise on repeated calls
        await connection.close()
        await connection.close()

    @pytest.mark.asyncio
    async def test_close_should_not_raise_when_called_twice_after_uds_self_dispatch(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test calling :meth:`WorkerConnection.close` a second time
        after a UDS self-dispatch returns without raising.

        Given:
            A :class:`WorkerConnection` that dispatched once over UDS
            (so both TCP and UDS pool entries were primed and the
            connection records the UDS key) and was then closed once
            (clearing both pool entries).
        When:
            ``close()`` is awaited a second time.
        Then:
            It should return without raising — the second close
            observes that both the TCP and UDS pool entries are
            already vacant and absorbs the resulting ``KeyError``.
        """
        # Arrange
        target = "localhost:50051"
        uds_target = "unix:/tmp/wool-test-close-twice.sock"
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address=target,
            pid=1,
            version="1.0.0",
        )
        wool.__worker_uds_address__ = uds_target

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("result"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        mock_channel = mocker.AsyncMock()
        mocker.patch.object(grpc.aio, "insecure_channel", return_value=mock_channel)

        connection = WorkerConnection(
            target, options=ChannelOptions(max_concurrent_streams=10)
        )

        async for _ in await connection.dispatch(sample_task):
            pass

        await connection.close()

        # Act & assert — second close must not raise
        await connection.close()

    @pytest.mark.asyncio
    async def test_dispatch_should_yield_all_results_in_order_when_task_yields_multiple(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test task dispatch with multiple streaming results.

        Given:
            A connection with a task that yields multiple results
        When:
            Task is dispatched
        Then:
            Async iterator yields all results in order
        """
        # Arrange
        to_protobuf_spy = mocker.spy(Task, "to_protobuf")

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("result_1"))
            ),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("result_2"))
            ),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("result_3"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        to_protobuf_spy.assert_called_once()
        mock_stub.dispatch.assert_called_once()
        assert len(results) == 3
        assert results == ["result_1", "result_2", "result_3"]

    @pytest.mark.asyncio
    async def test_dispatch_should_swallow_cancel_error_when_stream_closes(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test the dispatch stream's close swallows a cancel-time error.

        Given:
            A dispatch in flight whose underlying gRPC call's
            ``cancel()`` is patched to raise an Exception.
        When:
            The result iterator is broken out of, triggering the
            ``_DispatchStream.close()`` path on unwind.
        Then:
            The break and the surrounding teardown should complete
            without surfacing the cancel-time exception — close is
            a cleanup site, so a raise from ``call.cancel()`` is
            swallowed defensively (cleanup-during-cleanup).
        """
        # Arrange
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("first"))),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("second"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))
        # The ``cancel`` swallow is on the inner ``_DispatchStream``
        # path; ``_DispatchStream.close()`` invokes ``self._call.cancel()``
        # whose raise is what we want surfaced into the except arm.
        mock_call.cancel = mocker.MagicMock(side_effect=RuntimeError("cancel boom"))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act — break early to drive the close() path.
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)
            break

        # Assert — early break completed without surfacing the cancel raise.
        assert results == ["first"]

    @pytest.mark.asyncio
    async def test_dispatch_should_stop_stream_when_iterator_breaks_early(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test iterator closed via break before completion.

        Given:
            A connection with a task yielding results
        When:
            Iterator is terminated early via break
        Then:
            Underlying gRPC call is cancelled and stream stops
        """
        # Arrange
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("result_1"))
            ),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("result_2"))
            ),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("result_3"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)
            if len(results) >= 2:
                break

        # Assert - only got first 2 results
        assert results == ["result_1", "result_2"]

    @pytest.mark.asyncio
    async def test_dispatch_should_accept_ack_when_version_present(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch accepts Ack with version field.

        Given:
            A mock worker returning Ack with version field
        When:
            dispatch() is called
        Then:
            Ack is accepted and stream is returned normally.
        """
        # Arrange
        responses = (
            protocol.Response(ack=protocol.Ack(version="1.0.0")),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("test_result"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        assert results == ["test_result"]

    @pytest.mark.asyncio
    async def test_dispatch_should_reraise_original_class_when_nack_carries_exception(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch re-raises the worker's exception class on Nack.

        Given:
            A worker that responds with a Nack whose exception field
            carries a cloudpickle dump of ValueError("bad task id")
        When:
            dispatch(task) is awaited and consumed via async iteration
        Then:
            It should raise ValueError with message "bad task id" (the
            original class, not RpcError); mock_call.cancel is invoked
        """
        # Arrange
        responses = (
            protocol.Response(
                nack=protocol.Nack(
                    exception=protocol.Message(
                        dump=cloudpickle.dumps(ValueError("bad task id"))
                    ),
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(ValueError, match="bad task id"):
            async for _ in await connection.dispatch(sample_task):
                pass

        mock_call.cancel.assert_called()

    @pytest.mark.asyncio
    async def test_dispatch_should_preserve_subclass_identity_when_nack_has_exception(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch preserves the exact subclass of the worker exception.

        Given:
            A Nack whose exception field carries a cloudpickle dump of a
            custom user exception subclass MyAppError defined at module
            scope
        When:
            dispatch(task) is awaited and consumed
        Then:
            It should raise an exception whose type is MyAppError
            (preserving subclass identity)
        """
        # Arrange
        responses = (
            protocol.Response(
                nack=protocol.Nack(
                    exception=protocol.Message(
                        dump=cloudpickle.dumps(MyAppError("app failure"))
                    ),
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        with pytest.raises(MyAppError) as excinfo:
            async for _ in await connection.dispatch(sample_task):
                pass

        # Assert
        assert type(excinfo.value) is MyAppError

    @pytest.mark.asyncio
    async def test_dispatch_should_suppress_implicit_chaining_when_reraising_nack(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch suppresses chaining when re-raising the worker exception.

        Given:
            A Nack whose exception field carries a cloudpickle dump of
            RuntimeError("boom")
        When:
            dispatch(task) is awaited and consumed inside
            pytest.raises(RuntimeError)
        Then:
            It should raise an exception whose __cause__ is None and
            whose __context__ is not a pickle/cloudpickle deserialization
            frame
        """
        # Arrange
        responses = (
            protocol.Response(
                nack=protocol.Nack(
                    exception=protocol.Message(
                        dump=cloudpickle.dumps(RuntimeError("boom"))
                    ),
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        with pytest.raises(RuntimeError) as excinfo:
            async for _ in await connection.dispatch(sample_task):
                pass

        # Assert
        assert excinfo.value.__cause__ is None
        context = excinfo.value.__context__
        if context is not None:
            ctx_module = type(context).__module__
            assert "pickle" not in ctx_module
            assert "cloudpickle" not in ctx_module

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_rpc_error_when_nack_dump_unpicklable(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch falls back to RpcError when the Nack dump is malformed.

        Given:
            A Nack whose exception.dump is the byte string b"not a valid
            pickle"
        When:
            dispatch(task) is awaited and consumed
        Then:
            It should raise RpcError whose details flag the malformed
            payload
        """
        # Arrange
        responses = (
            protocol.Response(
                nack=protocol.Nack(
                    exception=protocol.Message(dump=b"not a valid pickle"),
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(RpcError, match="malformed Nack payload"):
            async for _ in await connection.dispatch(sample_task):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_rpc_error_when_nack_payload_not_exception(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch falls back to RpcError when the Nack dump is not an exception.

        Given:
            A Nack whose exception field carries a cloudpickle dump of a
            non-BaseException value (the string "not an exception")
        When:
            dispatch(task) is awaited and consumed
        Then:
            It should raise RpcError with details flagging the malformed
            payload
        """
        # Arrange
        responses = (
            protocol.Response(
                nack=protocol.Nack(
                    exception=protocol.Message(
                        dump=cloudpickle.dumps("not an exception")
                    ),
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(RpcError, match="malformed Nack payload"):
            async for _ in await connection.dispatch(sample_task):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_rpc_error_when_nack_payload_base_exception(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch degrades non-Exception BaseException Nacks to RpcError.

        Given:
            A Nack whose exception field carries a cloudpickle dump of
            KeyboardInterrupt() (a BaseException that is not an
            Exception)
        When:
            dispatch(task) is awaited and consumed
        Then:
            It should raise RpcError rather than re-raise the
            BaseException, since Rejected.original is Exception-typed
            by contract and a worker shipping a non-Exception
            BaseException would be smuggling cancel/interrupt signals
            across the wire.
        """
        # Arrange
        responses = (
            protocol.Response(
                nack=protocol.Nack(
                    exception=protocol.Message(
                        dump=cloudpickle.dumps(KeyboardInterrupt())
                    ),
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(RpcError, match="malformed Nack payload"):
            async for _ in await connection.dispatch(sample_task):
                pass

    @pytest.mark.asyncio
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None)
    @given(
        exc_type=st.sampled_from(
            (
                ValueError,
                RuntimeError,
                TypeError,
                KeyError,
                LookupError,
                OSError,
                ArithmeticError,
                # PR #205 explicitly names ``ImportError`` and
                # ``SerializationWarning`` as parse-phase rejection
                # classes that ride the Nack-with-exception channel
                # (unloadable callable / strict-mode chain-manifest decode).
                # Both must round-trip class+message intact so the
                # caller's ``except ImportError`` / ``except
                # SerializationWarning`` keeps matching.
                ImportError,
                SerializationWarning,
            )
        ),
        message=st.text(
            alphabet=st.characters(min_codepoint=32, max_codepoint=126),
            max_size=64,
        ),
    )
    async def test_dispatch_should_roundtrip_exception_when_nack_carries_arbitrary(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
        exc_type,
        message,
    ):
        """Test dispatch round-trips arbitrary exception classes via Nack.

        Given:
            A Hypothesis-generated typed exception instance drawn
            from a representative sampling of parse-phase rejection
            classes (Exception subclasses including ImportError and
            SerializationWarning) paired with arbitrary printable
            text messages, dumped via cloudpickle.dumps and shipped
            as a Nack.exception.
        When:
            dispatch(task) is awaited and consumed.
        Then:
            The exception raised has the same type and str() as the
            generated exception.
        """
        # Arrange
        generated_exc = exc_type(message)
        responses = (
            protocol.Response(
                nack=protocol.Nack(
                    exception=protocol.Message(dump=cloudpickle.dumps(generated_exc)),
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        try:
            with pytest.raises(exc_type) as excinfo:
                async for _ in await connection.dispatch(sample_task):
                    pass

            # Assert
            assert type(excinfo.value) is type(generated_exc)
            assert str(excinfo.value) == str(generated_exc)
        finally:
            # Hypothesis re-runs the test body per example while the
            # module-level channel pool persists across iterations; the
            # autouse cleanup fires only at function teardown. Close
            # the connection to drop the cached channel so the next
            # example sees the freshly patched stub.
            await connection.close()

    @pytest.mark.asyncio
    async def test_dispatch_should_use_secure_channel_when_credentials_provided(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch uses secure channel when credentials are provided.

        Given:
            A WorkerConnection configured with SSL channel credentials
        When:
            A task is dispatched
        Then:
            It should create a secure gRPC channel instead of an
            insecure one
        """
        # Arrange
        mock_channel = mocker.AsyncMock()
        mock_secure = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("secure_result"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", credentials=_secure_provider())

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        mock_secure.assert_called_once()
        assert results == ["secure_result"]

        await connection.close()

    @pytest.mark.asyncio
    async def test_dispatch_should_propagate_athrow_to_worker_and_return_recovery(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test throwing an exception into the dispatch generator.

        Given:
            A connection with an active streaming dispatch
        When:
            An exception is thrown into the generator via athrow()
        Then:
            The exception is serialized and sent to the worker, and
            the recovery response from the worker is returned
        """
        # Arrange
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("first"))),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("recovered"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        gen = await connection.dispatch(sample_task)
        first = await anext(gen)
        recovered = await gen.athrow(ValueError("injected"))

        # Assert
        assert first == "first"
        assert recovered == "recovered"
        # task request + next request + throw request
        assert mock_call.write.call_count == 3

    @pytest.mark.asyncio
    async def test_stream_should_be_consumable_when_dispatch_returns(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch returns a usable stream after its scope exits.

        Given:
            A connection backed by the module-level channel pool.
        When:
            dispatch() returns a stream (releasing its own pool ref).
        Then:
            The stream is still consumable because _execute() holds
            its own pool reference.
        """
        # Arrange
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("value"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act — obtain the stream, then consume later
        stream = await connection.dispatch(sample_task)
        # dispatch() has returned; its pool ref is released

        results = [r async for r in stream]

        # Assert
        assert results == ["value"]

    @pytest.mark.asyncio
    async def test_stream_should_release_pool_ref_when_fully_consumed(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test consuming the full stream releases the channel.

        Given:
            A connection that dispatches a single-result task with a
            mock gRPC channel.
        When:
            The stream is fully consumed and close() is called.
        Then:
            The mock channel's close() is invoked, confirming no
            dangling pool references prevented finalization.
        """
        # Arrange
        mock_channel = mocker.AsyncMock()
        mocker.patch.object(grpc.aio, "insecure_channel", return_value=mock_channel)

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("done"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        async for _ in await connection.dispatch(sample_task):
            pass
        await connection.close()

        # Assert
        mock_channel.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_should_release_pool_ref_when_raised_mid_stream(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test an error mid-stream releases the channel.

        Given:
            A connection that dispatches a task whose stream raises
            an exception after acknowledgment, with a mock gRPC
            channel.
        When:
            The caller iterates, hits the exception, and calls
            close().
        Then:
            The mock channel's close() is invoked, confirming the
            pool reference was released despite the error.
        """
        # Arrange
        mock_channel = mocker.AsyncMock()
        mocker.patch.object(grpc.aio, "insecure_channel", return_value=mock_channel)

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(dump=cloudpickle.dumps(RuntimeError("boom")))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        with pytest.raises(RuntimeError, match="boom"):
            async for _ in await connection.dispatch(sample_task):
                pass
        await connection.close()

        # Assert
        mock_channel.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_should_invoke_channel_finalizer(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test that close() tears down the pooled gRPC channel.

        Given:
            A connection that has dispatched a task with a mock gRPC
            channel, populating the module-level channel pool.
        When:
            close() is called.
        Then:
            The mock channel's close() method is called via the pool
            finalizer.
        """
        # Arrange
        mock_channel = mocker.AsyncMock()
        mocker.patch.object(grpc.aio, "insecure_channel", return_value=mock_channel)

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("done"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        async for _ in await connection.dispatch(sample_task):
            pass

        # Act
        await connection.close()

        # Assert
        mock_channel.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_two_dispatches_should_share_one_channel_when_target_matches(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test two dispatches to the same target share one channel.

        Given:
            Two WorkerConnection instances with identical (target,
            credentials, limit).
        When:
            Both dispatch a task.
        Then:
            Only one _Channel is created in the pool (the factory is
            called once).
        """

        # Arrange
        def make_call():
            responses = (
                protocol.Response(ack=protocol.Ack()),
                protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
            )
            return mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=lambda: make_call())
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        conn_a = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )
        conn_b = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        async for _ in await conn_a.dispatch(sample_task):
            pass
        async for _ in await conn_b.dispatch(sample_task):
            pass

        # Assert — WorkerStub constructed only once (one _Channel)
        assert protocol.WorkerStub.call_count == 1

    @pytest.mark.asyncio
    async def test_dispatch_should_use_default_message_sizes_when_no_options(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch creates gRPC channel with default ChannelOptions.

        Given:
            A WorkerConnection with no options parameter.
        When:
            A task is dispatched.
        Then:
            It should create a gRPC channel with default ChannelOptions
            sizes.
        """
        # Arrange
        defaults = ChannelOptions()
        mock_channel = mocker.AsyncMock()
        mock_insecure = mocker.patch.object(
            grpc.aio, "insecure_channel", return_value=mock_channel
        )

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051")

        # Act
        async for _ in await connection.dispatch(sample_task):
            pass

        # Assert
        mock_insecure.assert_called_once()
        call_options = mock_insecure.call_args[1]["options"]
        assert (
            "grpc.max_receive_message_length",
            defaults.max_receive_message_length,
        ) in call_options
        assert (
            "grpc.max_send_message_length",
            defaults.max_send_message_length,
        ) in call_options

    @pytest.mark.asyncio
    async def test_dispatch_should_use_custom_message_sizes_when_options_provided(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch creates gRPC channel with custom ChannelOptions.

        Given:
            A WorkerConnection with custom ChannelOptions message sizes.
        When:
            A task is dispatched.
        Then:
            It should create a gRPC channel with the custom sizes.
        """
        # Arrange
        custom_options = ChannelOptions(
            max_receive_message_length=200 * 1024 * 1024,
            max_send_message_length=50 * 1024 * 1024,
        )
        mock_channel = mocker.AsyncMock()
        mock_insecure = mocker.patch.object(
            grpc.aio, "insecure_channel", return_value=mock_channel
        )

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", options=custom_options)

        # Act
        async for _ in await connection.dispatch(sample_task):
            pass

        # Assert
        mock_insecure.assert_called_once()
        call_options = mock_insecure.call_args[1]["options"]
        assert (
            "grpc.max_receive_message_length",
            200 * 1024 * 1024,
        ) in call_options
        assert (
            "grpc.max_send_message_length",
            50 * 1024 * 1024,
        ) in call_options

    @pytest.mark.asyncio
    async def test_dispatch_should_use_custom_keepalive_options_when_provided(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch creates gRPC channel with custom keepalive options.

        Given:
            A WorkerConnection with custom keepalive options
        When:
            A task is dispatched
        Then:
            It should create a gRPC channel whose options include all
            keepalive entries with the custom values
        """
        # Arrange
        opts = ChannelOptions(
            keepalive_time_ms=60000,
            keepalive_timeout_ms=10000,
            keepalive_permit_without_calls=False,
        )
        mock_channel = mocker.AsyncMock()
        mock_insecure = mocker.patch.object(
            grpc.aio, "insecure_channel", return_value=mock_channel
        )

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", options=opts)

        # Act
        async for _ in await connection.dispatch(sample_task):
            pass

        # Assert
        mock_insecure.assert_called_once()
        call_options = mock_insecure.call_args[1]["options"]
        assert ("grpc.keepalive_time_ms", 60000) in call_options
        assert ("grpc.keepalive_timeout_ms", 10000) in call_options
        assert ("grpc.keepalive_permit_without_calls", 0) in call_options

    @pytest.mark.asyncio
    async def test_dispatch_should_use_default_keepalive_options_when_no_options(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch includes default keepalive options in channel.

        Given:
            A WorkerConnection with default ChannelOptions
        When:
            A task is dispatched
        Then:
            It should include all keepalive options with default values
        """
        # Arrange
        opts = ChannelOptions()
        mock_channel = mocker.AsyncMock()
        mock_insecure = mocker.patch.object(
            grpc.aio, "insecure_channel", return_value=mock_channel
        )

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", options=opts)

        # Act
        async for _ in await connection.dispatch(sample_task):
            pass

        # Assert
        mock_insecure.assert_called_once()
        call_options = mock_insecure.call_args[1]["options"]
        assert ("grpc.keepalive_time_ms", 30000) in call_options
        assert ("grpc.keepalive_timeout_ms", 30000) in call_options
        assert ("grpc.keepalive_permit_without_calls", 1) in call_options

    @pytest.mark.asyncio
    async def test_dispatch_should_use_custom_transport_options_when_provided(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch creates gRPC channel with custom transport options.

        Given:
            A WorkerConnection with custom max_pings_without_data,
            max_concurrent_streams, and compression=Gzip
        When:
            A task is dispatched
        Then:
            It should create a gRPC channel whose options include all
            three transport entries with the custom values
        """
        # Arrange
        opts = ChannelOptions(
            max_pings_without_data=5,
            max_concurrent_streams=50,
            compression=grpc.Compression.Gzip,
        )
        mock_channel = mocker.AsyncMock()
        mock_insecure = mocker.patch.object(
            grpc.aio, "insecure_channel", return_value=mock_channel
        )

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", options=opts)

        # Act
        async for _ in await connection.dispatch(sample_task):
            pass

        # Assert
        mock_insecure.assert_called_once()
        call_options = mock_insecure.call_args[1]["options"]
        assert ("grpc.http2.max_pings_without_data", 5) in call_options
        assert ("grpc.max_concurrent_streams", 50) in call_options
        assert ("grpc.default_compression_algorithm", 2) in call_options

    @pytest.mark.asyncio
    async def test_dispatch_should_serialize_payload_with_cloudpickle_when_self_dispatch(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch serializes the payload through cloudpickle.

        Given:
            A WorkerConnection whose target matches the current
            worker's address
        When:
            dispatch() is called
        Then:
            It should serialize the task payload through cloudpickle —
            the same serialization path as cross-process dispatch — and
            round-trip the result.
        """
        # Arrange
        target = "localhost:50051"
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address=target,
            pid=1,
            version="1.0.0",
        )

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("result"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(target)

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        assert results == ["result"]
        first_write = mock_call.write.call_args_list[0][0][0]
        restored_callable = cloudpickle.loads(first_write.task.callable)
        assert asyncio.iscoroutinefunction(restored_callable)

    @pytest.mark.asyncio
    async def test_dispatch_should_use_uds_channel_when_uds_address_set(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch uses UDS channel when UDS address is set.

        Given:
            A WorkerConnection whose target matches the current
            worker's address and a UDS address is available
        When:
            dispatch() is called
        Then:
            It should create the gRPC channel with the UDS address
            and None credentials
        """
        # Arrange
        target = "localhost:50051"
        uds_target = "unix:/tmp/wool-test.sock"
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address=target,
            pid=1,
            version="1.0.0",
        )
        wool.__worker_uds_address__ = uds_target

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("result"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        mock_channel = mocker.AsyncMock()
        channel_spy = mocker.patch.object(
            grpc.aio, "insecure_channel", return_value=mock_channel
        )
        secure_spy = mocker.patch.object(
            grpc.aio, "secure_channel", return_value=mock_channel
        )

        connection = WorkerConnection(target)

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        assert results == ["result"]
        channel_spy.assert_called()
        uds_calls = [c for c in channel_spy.call_args_list if c.args[0] == uds_target]
        assert len(uds_calls) >= 1
        secure_spy.assert_not_called()

    @pytest.mark.asyncio
    async def test_dispatch_should_use_cross_process_path_when_address_mismatch(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch uses cloudpickle when addresses do not match.

        Given:
            A WorkerConnection whose target differs from the current
            worker's address
        When:
            dispatch() is called
        Then:
            It should dispatch over the cross-process path and yield
            the worker's result.
        """
        # Arrange
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address="10.0.0.1:50051",
            pid=1,
            version="1.0.0",
        )

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("grpc_result"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051")

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        assert results == ["grpc_result"]

    @pytest.mark.asyncio
    async def test_dispatch_should_cloudpickle_vars_when_self_dispatch_anext(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch __anext__ serializes forwarded vars via cloudpickle.

        Given:
            A WorkerConnection whose target matches the current
            worker's address and a ContextVar with a value set
        When:
            The dispatch stream's __anext__ writes a next-frame request
        Then:
            The vars in the written next-frame request should be
            serialized via cloudpickle
        """
        # Arrange
        target = "localhost:50051"
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address=target,
            pid=1,
            version="1.0.0",
        )

        var = ContextVar("conn_d_var", namespace="conn_d")
        var.set("test_value")

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("first"))),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("second"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            target, options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert — the next-frame request (second write) should carry
        # cloudpickle var bytes, longer than a 16-byte token
        assert len(results) == 2
        next_request = mock_call.write.call_args_list[1][0][0]
        emitted = {(e.namespace, e.name): e.value for e in next_request.context.vars}
        assert (var.namespace, var.name) in emitted
        assert len(emitted[(var.namespace, var.name)]) > 16

    @pytest.mark.asyncio
    async def test_dispatch_should_apply_back_propagated_vars_when_response_carries_vars(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch applies back-propagated response vars.

        Given:
            A WorkerConnection in self-dispatch mode and a response
            carrying vars serialized via cloudpickle
        When:
            The stream reads the response and applies the vars
        Then:
            The ContextVar value should be updated to the
            back-propagated value
        """
        # Arrange
        target = "localhost:50051"
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address=target,
            pid=1,
            version="1.0.0",
        )

        var = ContextVar("conn_e_var", namespace="conn_e")
        var.set("original")

        caller_chain = wool.__chain__.get().id

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("result")),
                context=protocol.ChainManifest(
                    id=caller_chain.hex,
                    vars=[
                        protocol.ContextVar(
                            namespace=var.namespace,
                            name=var.name,
                            value=cloudpickle.dumps("back_propagated"),
                        )
                    ],
                ),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            target, options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        assert results == ["result"]
        assert var.get() == "back_propagated"

    @pytest.mark.asyncio
    async def test_dispatch_should_back_propagate_routine_set_when_caller_unarmed(
        self, mocker: MockerFixture, async_stream, mock_grpc_call
    ):
        """Test a routine's set on a wool.ContextVar reaches an unarmed caller.

        Given:
            An unarmed caller (no wool.ContextVar bindings, no chain
            installed) dispatching a coroutine routine, and a response
            whose context carries a new value for a wool.ContextVar
            and a fresh chain id — the shape of a routine that
            performed the first ``var.set`` on the worker.
        When:
            The dispatch result is consumed.
        Then:
            * Before the dispatch the caller has no value for the var
              (``LookupError`` on bare ``get``) and no active chain.
            * After the dispatch the caller observes the routine-set
              value, and the caller's chain is armed with the worker's
              chain id — back-propagation arms the previously-unarmed
              caller via the apply-back leg of the wire.
        """
        # Arrange

        var = ContextVar("conn_set_back_var", namespace="conn_set_back")

        async def routine() -> None:
            return None

        wool_task = Task(
            id=uuid4(),
            callable=routine,
            args=(),
            kwargs={},
            proxy=PicklableMock(spec=WorkerProxyLike, id="test-proxy-id"),
        )

        worker_chain_id = uuid4()
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("done")),
                context=protocol.ChainManifest(
                    id=worker_chain_id.hex,
                    vars=[
                        protocol.ContextVar(
                            namespace=var.namespace,
                            name=var.name,
                            value=cloudpickle.dumps("routine-set-value"),
                        )
                    ],
                ),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Pre-dispatch baseline: caller is unarmed, var has no value.
        assert wool.__chain__.get(None) is None
        with pytest.raises(LookupError):
            var.get()

        # Act
        results = []
        async for result in await connection.dispatch(wool_task):
            results.append(result)

        # Assert — back-propagation applied: value visible, caller armed.
        assert results == ["done"]
        assert var.get() == "routine-set-value"
        armed = wool.__chain__.get(None)
        assert armed is not None
        assert armed.id == worker_chain_id

    @pytest.mark.asyncio
    async def test_async_gen_should_back_propagate_per_yield_when_caller_unarmed(
        self, mocker: MockerFixture, async_stream, mock_grpc_call
    ):
        """Test an async-gen routine's per-yield mutations reach an unarmed caller.

        Given:
            An unarmed caller dispatching an async-generator routine,
            and a response stream where each yield carries an updated
            chain manifest — the shape of an async generator that does
            ``var.set("step-N")`` on every iteration. All response
            frames carry the same chain id (the worker's cached
            chain), so the caller's apply-back stays on one chain
            across iterations.
        When:
            The caller iterates the result stream to exhaustion,
            reading the var after every yield.
        Then:
            * Before the dispatch the caller has no value for the var
              and no active chain.
            * Each per-yield snapshot equals the worker's most-recent
              ``var.set`` — the response-frame mount on every
              ``__anext__`` applies the latest binding.
            * After exhaustion the caller is armed with the worker's
              chain id and the var holds the final yield's value.
        """
        # Arrange

        var = ContextVar("conn_set_back_agen_var", namespace="conn_set_back_agen")

        async def streaming_routine():
            for _ in range(3):
                yield None

        wool_task = Task(
            id=uuid4(),
            callable=streaming_routine,
            args=(),
            kwargs={},
            proxy=PicklableMock(spec=WorkerProxyLike, id="test-proxy-id"),
        )

        worker_chain_id = uuid4()

        def _yield_response(value: str) -> protocol.Response:
            """Build a yield-shaped response carrying the per-step var set."""
            return protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps(value)),
                context=protocol.ChainManifest(
                    id=worker_chain_id.hex,
                    vars=[
                        protocol.ContextVar(
                            namespace=var.namespace,
                            name=var.name,
                            value=cloudpickle.dumps(value),
                        )
                    ],
                ),
            )

        responses = (
            protocol.Response(ack=protocol.Ack()),
            _yield_response("step-0"),
            _yield_response("step-1"),
            _yield_response("step-2"),
        )
        mock_call = mock_grpc_call(async_stream(responses))
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Pre-dispatch baseline: caller is unarmed, var has no value.
        assert wool.__chain__.get(None) is None
        with pytest.raises(LookupError):
            var.get()

        # Act — iterate the stream and snapshot var.get() after each yield.
        snapshots: list[str] = []
        async for _ in await connection.dispatch(wool_task):
            snapshots.append(var.get())

        # Assert — every per-yield snapshot matches the worker's
        # most-recent mutation; the caller is left armed on the
        # worker's chain with the final binding.
        assert snapshots == ["step-0", "step-1", "step-2"]
        assert var.get() == "step-2"
        armed = wool.__chain__.get(None)
        assert armed is not None
        assert armed.id == worker_chain_id

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_and_warn_when_corrupt_context_with_worker_exc(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test the caller-side response decoder surfaces both the
        worker-raised exception and a per-var context-decode failure
        as independent signals when a single frame carries both.

        Given:
            A worker response that carries both a worker-raised
            routine exception and a context payload whose var entry
            cannot be deserialized (modeling cross-version pickle
            skew or on-wire corruption of a single var value in the
            same frame as a routine failure)
        When:
            The caller iterates the dispatch stream
        Then:
            The caller raises the worker's routine exception, and a
            SerializationWarning naming the corrupt var key is also
            emitted — the corrupt var is skipped via the per-entry
            resilience contract; surviving context state still
            propagates and the worker's signal still surfaces
        """
        # Arrange
        target = "localhost:50051"
        var = ContextVar(
            "exception_with_corrupt_context_var",
            namespace="exception_with_corrupt_context",
        )
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(
                    dump=cloudpickle.dumps(ValueError("worker-side failure"))
                ),
                context=protocol.ChainManifest(
                    vars=[
                        protocol.ContextVar(
                            namespace=var.namespace,
                            name=var.name,
                            value=b"\x00not a valid pickle stream\x00",
                        )
                    ]
                ),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            target, options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.warns(SerializationWarning, match=var.name):
            with pytest.raises(ValueError, match="worker-side failure"):
                async for _ in await connection.dispatch(sample_task):
                    pass

    @pytest.mark.asyncio
    async def test_dispatch_should_yield_result_and_warn_when_result_context_corrupt(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test the caller-side response decoder delivers the routine's
        return value and emits a SerializationWarning when a result
        frame's accompanying context payload fails to deserialize.

        Given:
            A worker response that carries a successful routine
            ``result`` payload alongside a context whose var entry
            fails to deserialize
        When:
            The caller iterates the dispatch stream
        Then:
            The caller observes the routine's return value normally
            and a SerializationWarning is emitted — context
            propagation is ancillary state and a decode failure here
            never preempts the primary signal. Callers that prefer
            strict semantics can promote the warning to an exception
            via ``warnings.filterwarnings("error", category=...)``
        """
        # Arrange
        target = "localhost:50051"
        var = ContextVar(
            "result_with_corrupt_context_var",
            namespace="result_with_corrupt_context",
        )
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("worker_result")),
                context=protocol.ChainManifest(
                    vars=[
                        protocol.ContextVar(
                            namespace=var.namespace,
                            name=var.name,
                            value=b"\x00not a valid pickle stream\x00",
                        )
                    ]
                ),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            target, options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        results: list[object] = []
        with pytest.warns(SerializationWarning, match="Failed to deserialize"):
            async for value in await connection.dispatch(sample_task):
                results.append(value)

        # Assert
        assert results == ["worker_result"]

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_serialization_error_when_corrupt_context_strict(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test that a caller can opt into strict semantics by promoting
        SerializationWarning to an error.

        Given:
            The same response shape as the lenient-mode test (result
            + corrupt context var)
        When:
            The caller has installed
            ``warnings.filterwarnings("error", category=SerializationWarning)``
            for the duration of the dispatch
        Then:
            Iterating the dispatch raises a typed
            :class:`wool.ChainSerializationError` aggregating the promoted
            warnings on ``.warnings``. On a result frame the decode
            error IS the primary — the routine's value is dropped
            because a result cannot be trusted alongside a context
            that failed to apply (strict-mode "fail loud" contract).
        """
        # Arrange
        import wool

        target = "localhost:50051"
        var = ContextVar(
            "strict_corrupt_context_var",
            namespace="strict_corrupt_context",
        )
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("worker_result")),
                context=protocol.ChainManifest(
                    vars=[
                        protocol.ContextVar(
                            namespace=var.namespace,
                            name=var.name,
                            value=b"\x00not a valid pickle stream\x00",
                        )
                    ]
                ),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))
        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            target, options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        import warnings as _warnings

        with _warnings.catch_warnings():
            _warnings.simplefilter("error", category=SerializationWarning)
            with pytest.raises(wool.ChainSerializationError) as exc_info:
                async for _ in await connection.dispatch(sample_task):
                    pass
        assert len(exc_info.value.warnings) == 1
        assert isinstance(exc_info.value.warnings[0], SerializationWarning)

    @pytest.mark.asyncio
    async def test_dispatch_should_serialize_vars_with_cloudpickle_when_no_serializer(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test non-self-dispatch uses default cloudpickle for vars serialization.

        Given:
            A WorkerConnection whose target does not match the current
            worker's address and a ContextVar with a value set
        When:
            The dispatch stream writes requests — the initial
            :class:`TaskRequestFrame` ships pure dispatch metadata
            with no chain manifest, and the first
            :class:`NextRequestFrame` (sent by ``__anext__`` to pull
            the result) auto-captures the active chain
        Then:
            The first mid-stream :class:`NextRequestFrame` should
            carry the var with its value serialized via cloudpickle.
        """
        # Arrange
        var = ContextVar("conn_f_var", namespace="conn_f")
        var.set("cp_value")

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("result"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051",
            options=ChannelOptions(max_concurrent_streams=10),
        )

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert — the first NextRequest (write[1]) carries the var;
        # the initial TaskRequest (write[0]) is pure dispatch metadata.
        assert results == ["result"]
        next_request = mock_call.write.call_args_list[1][0][0]
        emitted = {(e.namespace, e.name): e.value for e in next_request.context.vars}
        assert (var.namespace, var.name) in emitted
        assert len(emitted[(var.namespace, var.name)]) > 16

    @pytest.mark.asyncio
    async def test_dispatch_should_roundtrip_vars_when_self_dispatch_mid_stream_request(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch cloudpickle-encodes vars on the first mid-stream request.

        Given:
            A WorkerConnection whose target matches the current
            worker's address and a ContextVar with a value set
        When:
            dispatch() sends the initial task request (pure dispatch
            metadata, no chain manifest) and the first
            :class:`NextRequestFrame` (which auto-captures the active
            chain)
        Then:
            The first mid-stream :class:`NextRequestFrame` should
            cloudpickle-encode the var so it round-trips back to the
            original value. Under the per-frame architecture the
            initial Task frame is intentionally manifest-free —
            mid-stream frames ship the per-step manifest.
        """
        # Arrange
        target = "localhost:50051"
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address=target,
            pid=1,
            version="1.0.0",
        )

        var = ContextVar("conn_cn1_var", namespace="conn_cn1")
        var.set("roundtrip_value")

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("result"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            target, options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert — the first NextRequest (write[1]) carries the var;
        # decoding it yields the original value.
        assert results == ["result"]
        next_request = mock_call.write.call_args_list[1][0][0]
        emitted = {(e.namespace, e.name): e.value for e in next_request.context.vars}
        assert (var.namespace, var.name) in emitted
        assert cloudpickle.loads(emitted[(var.namespace, var.name)]) == "roundtrip_value"

    @pytest.mark.asyncio
    async def test_dispatch_should_encode_callable_when_cross_process_initial_request(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test cross-process dispatch cloudpickle-encodes the task payload.

        Given:
            A WorkerConnection whose target does not match any worker
            (no worker metadata set)
        When:
            dispatch() sends the initial task request
        Then:
            It should cloudpickle-encode the task payload so the
            decoded callable is a coroutine function.
        """
        # Arrange
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("result"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        assert results == ["result"]
        first_write = mock_call.write.call_args_list[0][0][0]
        restored_callable = cloudpickle.loads(first_write.task.callable)
        assert asyncio.iscoroutinefunction(restored_callable)

    @pytest.mark.asyncio
    @settings(
        max_examples=50,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        value=st.one_of(
            st.text(),
            st.integers(),
            st.tuples(st.integers(), st.text()),
            st.dictionaries(st.text(), st.integers()),
        )
    )
    async def test_dispatch_should_roundtrip_arbitrary_var_when_self_dispatch_mid_stream(
        self,
        value,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch round-trips arbitrary ContextVar values
        on the first mid-stream request.

        Given:
            A self-dispatch WorkerConnection and any picklable
            ContextVar value drawn from text, ints, tuples, or dicts
        When:
            dispatch() writes the initial task request (no wire
            context) and the first NextRequest (which auto-captures
            the chain), and the var is decoded from the emitted
            ``next_request.context``
        Then:
            It should decode to a value equal to the original.
        """
        # Arrange — the channel pool and var_registry are process-wide
        # and not reset between Hypothesis examples; a uuid-unique
        # target keeps each example on its own pool key (so a cached
        # channel from a prior example cannot serve an exhausted mock
        # stream) and a uuid-unique var name keeps the registry key
        # collision-free.
        target = f"localhost-{uuid4().hex}:50051"
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address=target,
            pid=1,
            version="1.0.0",
        )

        var = ContextVar(f"conn_pbt_var_{uuid4().hex}", namespace="conn_pbt")
        var.set(value)

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("result"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            target, options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        assert results == ["result"]
        next_request = mock_call.write.call_args_list[1][0][0]
        emitted = {(e.namespace, e.name): e.value for e in next_request.context.vars}
        assert (var.namespace, var.name) in emitted
        assert cloudpickle.loads(emitted[(var.namespace, var.name)]) == value

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_unexpected_response_when_exc_payload_non_exc(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch wraps a non-Exception ``Response.exception``
        payload as an :class:`UnexpectedResponse` so the load
        balancer does not evict the worker for a routine-level fault.

        Given:
            A :class:`protocol.Response` whose ``exception`` field
            carries a cloudpickle dump of a non-Exception value
            (a bare string)
        When:
            ``dispatch(task)`` is awaited and the result iterator is
            consumed
        Then:
            It should raise :class:`UnexpectedResponse` (not
            :class:`RpcError`) whose details name the malformed
            payload type. :class:`UnexpectedResponse` is not a
            :class:`RpcError` subclass, so the load-balancer
            classification treats it as a caller-fault and does
            not evict the worker.
        """
        # Arrange
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(
                    dump=cloudpickle.dumps("not an exception"),
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(
            UnexpectedResponse, match="non-Exception payload"
        ) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
        # Belt-and-suspenders: the worker-eviction contract is
        # carried by ``RpcError``; a routine-level fault must not
        # surface as an ``RpcError`` subclass.
        assert not isinstance(exc_info.value, RpcError)

    @pytest.mark.asyncio
    async def test_dispatch_should_preserve_base_exception_payload_on_context(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch preserves a non-Exception BaseException payload
        on the wrapper's ``__context__``.

        Given:
            A :class:`protocol.Response` whose ``exception`` field
            carries a cloudpickle dump of a non-Exception
            :class:`BaseException` (a ``KeyboardInterrupt``).
        When:
            ``dispatch(task)`` is awaited and the result iterator is
            consumed.
        Then:
            It should raise :class:`UnexpectedResponse` (not
            :class:`RpcError`) with the original ``KeyboardInterrupt``
            preserved on ``__context__`` — a process-level signal is not
            smuggled across the wire as a raisable, but it is not lost.
        """
        # Arrange
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(
                    dump=cloudpickle.dumps(KeyboardInterrupt("boom")),
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(
            UnexpectedResponse, match="non-Exception payload"
        ) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
        assert isinstance(exc_info.value.__context__, KeyboardInterrupt)
        assert not isinstance(exc_info.value, RpcError)

    @pytest.mark.asyncio
    async def test_dispatch_should_propagate_cancelled_error_raw_when_worker_cancels(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch propagates a worker-side
        :class:`asyncio.CancelledError` raw rather than degrading it.

        Mirrors stdlib's ``await task`` semantics where a coroutine
        that self-raises :class:`asyncio.CancelledError` is
        indistinguishable from one that was externally cancelled —
        both transition the task to ``CANCELLED`` and the caller's
        ``await`` raises :class:`asyncio.CancelledError`. Wool's
        wire ships ``CancelledError`` on the ``Response.exception``
        frame; the caller must re-raise the same class so user code
        can ``except asyncio.CancelledError`` (and allow the
        cancellation to chain through the caller's own task as
        asyncio expects).

        Given:
            A :class:`protocol.Response` whose ``exception`` field
            carries a cloudpickle dump of
            :class:`asyncio.CancelledError`
        When:
            ``dispatch(task)`` is awaited and the result iterator is
            consumed
        Then:
            The caller's ``await`` should raise
            :class:`asyncio.CancelledError` raw — not
            :class:`UnexpectedResponse`, not :class:`RpcError`.
        """
        # Arrange
        cancellation = asyncio.CancelledError("worker self-raised cancel")
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(dump=cloudpickle.dumps(cancellation)),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Run the cancellable consumption in an inner task so the
        # dispatch path's ``current_task().cancel()`` lands on the
        # inner task and not the test's outer task. On Python 3.11
        # ``Task.uncancel()`` does not clear ``_must_cancel`` (only
        # the count), so the scheduled next-cycle ``CancelledError``
        # cannot be suppressed by the awaiter — wrapping isolates
        # it cleanly across 3.11/3.12/3.13.
        async def body():
            async for _ in await connection.dispatch(sample_task):
                pass

        wrapped = asyncio.ensure_future(body())

        # Act & assert
        with pytest.raises(asyncio.CancelledError) as exc_info:
            await wrapped
        # ``CancelledError`` must NOT be degraded to
        # ``UnexpectedResponse`` / ``RpcError`` — those would
        # silently break stdlib's cancellation-chaining contract.
        # Pin the EXACT class (not just isinstance) so a regression
        # that wraps the cancellation in a private subclass is also
        # caught — asyncio internals are sensitive to identity here.
        assert type(exc_info.value) is asyncio.CancelledError
        assert not isinstance(exc_info.value, UnexpectedResponse)
        assert not isinstance(exc_info.value, RpcError)

    @pytest.mark.asyncio
    async def test_dispatch_should_not_increment_cancelling_when_worker_cancels(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch does NOT increment ``current_task().cancelling()``
        on a worker-side CancelledError.

        Matches stdlib ``await task`` semantics: when the awaitee
        raises :class:`asyncio.CancelledError`, the awaiter's
        ``cancelling()`` count is **not** bumped. A caller that
        catches ``CancelledError`` and continues to ``await``
        something else (a recovery path) is therefore not
        re-interrupted at the next checkpoint — the wool-naive
        caller does not need to call
        ``current_task().uncancel()`` to absorb a phantom bump
        (F9).

        Given:
            A :class:`protocol.Response` whose ``exception`` field
            carries a cloudpickle dump of
            :class:`asyncio.CancelledError`
        When:
            ``dispatch(task)`` is awaited and the result iterator is
            consumed, and the resulting ``CancelledError`` is caught
        Then:
            ``current_task().cancelling()`` should remain ``0`` —
            the worker-shipped CancelledError propagates as-is and
            no synchronous bump of the awaiter's state happens.
        """
        # Arrange
        cancellation = asyncio.CancelledError("worker self-raised cancel")
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(dump=cloudpickle.dumps(cancellation)),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        observed: dict[str, int | None] = {"cancelling": None}

        async def body():
            try:
                async for _ in await connection.dispatch(sample_task):
                    pass
            except asyncio.CancelledError:
                current = asyncio.current_task()
                assert current is not None
                observed["cancelling"] = current.cancelling()

        await asyncio.ensure_future(body())

        # Assert
        assert observed["cancelling"] == 0, (
            "worker-shipped CancelledError must not bump the "
            "awaiter's cancelling() count — stdlib ``await task`` "
            "semantics keep it at 0"
        )

    @pytest.mark.asyncio
    async def test_dispatch_should_leave_task_cancelled_when_worker_cancels(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test re-raising a worker-side CancelledError ends the
        surrounding task in the CANCELLED state.

        Closes the loop with stdlib parity: a task that observes
        :class:`asyncio.CancelledError` and re-raises (without
        ``uncancel``) must end as cancelled — same as a
        locally-cancelled task. The ``cancelling()`` bump on the
        caller is what lets asyncio transition the task to
        ``CANCELLED`` on re-raise.

        Given:
            A :class:`protocol.Response` whose ``exception`` field
            carries a cloudpickle dump of
            :class:`asyncio.CancelledError`, awaited inside a
            wrapping :class:`asyncio.Task`
        When:
            The wrapping task observes ``CancelledError`` and
            re-raises without calling ``uncancel()``
        Then:
            It should end with ``task.cancelled() == True``.
        """
        # Arrange
        cancellation = asyncio.CancelledError("worker self-raised cancel")
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(dump=cloudpickle.dumps(cancellation)),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        async def body():
            async for _ in await connection.dispatch(sample_task):
                pass

        wrapped = asyncio.ensure_future(body())

        # Act & assert
        with pytest.raises(asyncio.CancelledError):
            await wrapped
        assert wrapped.cancelled()

    @pytest.mark.asyncio
    async def test_dispatch_should_propagate_raw_when_result_payload_malformed(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch lets a malformed ``Response.result`` payload
        deserialization error propagate with its original type.

        Given:
            A :class:`protocol.Response` whose ``result`` field
            carries bytes that cannot be deserialized
            (b"not a valid pickle stream").
        When:
            ``dispatch(task)`` is awaited and the result iterator is
            consumed.
        Then:
            It should raise the underlying serializer error
            (:class:`pickle.UnpicklingError` for cloudpickle's
            default deserializer) raw — no wrapper class, no
            indirection. The original exception type carries the
            diagnostic detail. Since it isn't an :class:`RpcError`
            subclass the load-balancer classification treats it as
            a caller-fault and does not evict the worker.
        """
        # Arrange
        import pickle

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=b"not a valid pickle stream"),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(pickle.UnpicklingError) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
        # The serializer error propagates raw — not wrapped in
        # UnexpectedResponse, not an RpcError subclass.
        assert not isinstance(exc_info.value, UnexpectedResponse)
        assert not isinstance(exc_info.value, RpcError)

    @pytest.mark.asyncio
    async def test_dispatch_should_propagate_raw_when_exception_payload_malformed(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch lets a malformed ``Response.exception``
        payload deserialization error propagate with its original
        type.

        Given:
            A :class:`protocol.Response` whose ``exception`` field
            carries bytes that cannot be deserialized
            (b"not a valid pickle stream").
        When:
            ``dispatch(task)`` is awaited and the result iterator is
            consumed.
        Then:
            It should raise the underlying serializer error
            (:class:`pickle.UnpicklingError` for cloudpickle's
            default deserializer) raw — no wrapper class, no
            indirection. The original exception type carries the
            diagnostic detail. Since it isn't an :class:`RpcError`
            subclass the load-balancer classification treats it as
            a caller-fault and does not evict the worker.
        """
        # Arrange
        import pickle

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(dump=b"not a valid pickle stream"),
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection(
            "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
        )

        # Act & assert
        with pytest.raises(pickle.UnpicklingError) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
        # The serializer error propagates raw — not wrapped in
        # UnexpectedResponse, not an RpcError subclass.
        assert not isinstance(exc_info.value, UnexpectedResponse)
        assert not isinstance(exc_info.value, RpcError)


@pytest.mark.asyncio
async def test_clear_channel_pool_should_close_cached_channels(
    mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
):
    """Test :func:`clear_channel_pool` closes every cached gRPC
    channel in the module-wide pool.

    Given:
        A populated module-wide channel pool — a successful
        dispatch through a :class:`WorkerConnection` has primed the
        cache for a particular key.
    When:
        :func:`clear_channel_pool` is awaited.
    Then:
        It should invoke the cached channel's ``close()`` method
        so the cached entry is torn down and subsequent dispatches
        would build a fresh channel.
    """
    # Arrange
    mock_channel = mocker.AsyncMock()
    mocker.patch.object(grpc.aio, "insecure_channel", return_value=mock_channel)

    responses = (
        protocol.Response(ack=protocol.Ack()),
        protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("ok"))),
    )
    mock_call = mock_grpc_call(async_stream(responses))

    mock_stub = mocker.MagicMock()
    mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
    mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

    connection = WorkerConnection(
        "localhost:50051", options=ChannelOptions(max_concurrent_streams=10)
    )

    async for _ in await connection.dispatch(sample_task):
        pass

    # Act
    await clear_channel_pool()

    # Assert
    mock_channel.close.assert_called_once()


@pytest.mark.asyncio
async def test_clear_channel_pool_should_not_raise_when_pool_empty():
    """Test :func:`clear_channel_pool` is a no-op when the pool is
    empty.

    Given:
        A module-wide channel pool with no cached entries (the
        autouse ``_clear_channel_pool`` fixture clears the pool
        between tests, so this test starts from an empty pool).
    When:
        :func:`clear_channel_pool` is awaited.
    Then:
        It should return without raising — clearing an empty pool
        is a legitimate operation and must not surface a
        ``KeyError`` or similar.
    """
    # Act & assert — must not raise
    await clear_channel_pool()
