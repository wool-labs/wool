import asyncio
import os
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
from wool.runtime.context import ContextDecodeWarning
from wool.runtime.context import ContextVar
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.worker.auth import FileCredentialProvider
from wool.runtime.worker.auth import StaticCredentialProvider
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.base import ChannelOptions
from wool.runtime.worker.connection import HandshakeError
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import UnexpectedResponse
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.connection import clear_channel_pool

from .conftest import PicklableMock


def _secure_provider(identity: str | None = None) -> StaticCredentialProvider:
    """Build a static provider over dummy credential bytes.

    For tests that only need a secure (non-None) provider; gRPC defers PEM
    validation to the handshake, which these tests never reach.
    """
    return StaticCredentialProvider(
        WorkerCredentials(ca_cert=b"ca", worker_key=b"key", worker_cert=b"cert"),
        identity=identity,
    )


class MyAppError(Exception):
    """Custom user exception subclass defined at module scope.

    Defined here (not inside a test) so cloudpickle can resolve the
    class on deserialization in tests that round-trip user-defined
    exception types through Nack.exception payloads.
    """


class _StrictRejectingException(Exception):
    """Module-level exception that rejects every best-effort write
    the dispatch handler's strict-mode side channels attempt.

    Defined at module scope so cloudpickle can resolve the class on
    deserialization when this exception ships across the wire on
    :class:`protocol.Response`'s ``exception`` field.

    ``add_note`` raises :class:`AttributeError` so the PEP 678 note
    path inside :meth:`WorkerConnection._read_next`'s exception arm
    hits the ``except (AttributeError, TypeError)`` swallow.

    Arbitrary attribute writes — including
    ``__wool_context_warnings__`` — raise :class:`AttributeError` so
    the programmatic side-channel write hits the ``except
    AttributeError`` swallow. The ``args``/``__cause__``/
    ``__context__``/``__traceback__``/``__suppress_context__``/
    ``__notes__`` slots are explicitly allowed so the standard
    ``BaseException`` machinery (and cloudpickle's restore via
    ``__setstate__``) keeps working.
    """

    _ALLOWED = frozenset(
        {
            "args",
            "__cause__",
            "__context__",
            "__traceback__",
            "__suppress_context__",
            "__notes__",
        }
    )

    def __setattr__(self, name, value):
        if name in self._ALLOWED:
            object.__setattr__(self, name, value)
        else:
            raise AttributeError(
                f"{type(self).__name__!r} object does not accept "
                f"arbitrary attribute writes: {name!r}"
            )

    def add_note(self, _note):
        raise AttributeError(f"{type(self).__name__!r} object does not accept add_note")


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

    def test___init___should_set_reason_code_and_details(self):
        """Test HandshakeError records its classification and gRPC fields.

        Given:
            A status code, details, and a reason.
        When:
            HandshakeError is constructed.
        Then:
            It should expose the reason, code, and details.
        """
        # Act
        error = HandshakeError(
            grpc.StatusCode.UNAVAILABLE,
            "boom",
            reason=HandshakeError.Reason.CERT_VERIFY,
        )

        # Assert
        assert error.reason is HandshakeError.Reason.CERT_VERIFY
        assert error.code is grpc.StatusCode.UNAVAILABLE
        assert error.details == "boom"

    def test___init___should_be_non_transient_rpc_error(self):
        """Test HandshakeError sits in the worker-health hierarchy.

        Given:
            A HandshakeError instance.
        When:
            Its type relationships are inspected.
        Then:
            It should be an RpcError (so the load balancer evicts it) but
            not a TransientRpcError (so it is not treated as a retryable
            hiccup).
        """
        # Arrange
        error = HandshakeError(reason=HandshakeError.Reason.TLS_HANDSHAKE)

        # Act & assert
        assert isinstance(error, RpcError)
        assert not isinstance(error, TransientRpcError)


class TestWorkerConnection:
    @pytest.mark.asyncio
    async def test_dispatch_task_that_returns(
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
    async def test_dispatch_task_that_raises(
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
    async def test_dispatch_no_ack(
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
    async def test_dispatch_unexpected_response(
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
    async def test_dispatch_transient_rpc_error(
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
    async def test_dispatch_nontransient_rpc_error(
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
            It should raise HandshakeError with the PEER_UNAUTHENTICATED
            reason.
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
            provider=_secure_provider(),
            options=ChannelOptions(max_concurrent_streams=10),
        )

        # Act & assert
        with pytest.raises(HandshakeError) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
        assert exc_info.value.reason is HandshakeError.Reason.PEER_UNAUTHENTICATED

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "secure, details, debug, expected_reason",
        [
            (
                True,
                "",
                "Ssl handshake: certificate verify failed",
                HandshakeError.Reason.CERT_VERIFY,
            ),
            (
                True,
                "certificate has expired",
                "",
                HandshakeError.Reason.CERT_VERIFY,
            ),
            (
                True,
                "",
                "No match found for server name: wool-worker",
                HandshakeError.Reason.IDENTITY_MISMATCH,
            ),
            (
                True,
                "",
                "wrong version number",
                HandshakeError.Reason.PLAINTEXT_VS_ENCRYPTED,
            ),
            (
                True,
                "",
                "tls handshake eof",
                HandshakeError.Reason.TLS_HANDSHAKE,
            ),
            (
                False,
                "",
                "Ssl handshake failed",
                HandshakeError.Reason.PLAINTEXT_VS_ENCRYPTED,
            ),
        ],
    )
    async def test_dispatch_should_raise_handshake_error_when_tls_handshake_fails(
        self,
        mocker: MockerFixture,
        sample_task,
        secure: bool,
        details: str,
        debug: str,
        expected_reason: HandshakeError.Reason,
    ):
        """Test dispatch classifies a failed TLS handshake distinctly.

        Given:
            A connection whose stub raises UNAVAILABLE carrying TLS evidence
            in its error text.
        When:
            A task is dispatched.
        Then:
            It should raise HandshakeError with the reason matching the
            evidence.
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
            provider=_secure_provider() if secure else None,
            options=ChannelOptions(max_concurrent_streams=10),
        )

        # Act & assert
        with pytest.raises(HandshakeError) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
        assert exc_info.value.reason is expected_reason

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
            provider=_secure_provider(),
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
            provider=_secure_provider(),
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
            "10.0.0.7:50051", provider=_secure_provider(identity="wool-worker")
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
        connection = WorkerConnection("10.0.0.7:50051", provider=_secure_provider())

        # Act
        results = [result async for result in await connection.dispatch(sample_task)]

        # Assert
        assert results == ["ok"]
        option_keys = [key for key, _ in secure_spy.call_args.kwargs["options"]]
        assert "grpc.ssl_target_name_override" not in option_keys

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
            channel is reused because the credential fingerprint is
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
        connection = WorkerConnection("10.0.0.7:50051", provider=_secure_provider())

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
    ):
        """Test rotated credential material yields a fresh channel.

        Given:
            A connection over a reloading file provider, dispatched once,
            after which the CA file is rotated on disk.
        When:
            A second task is dispatched.
        Then:
            A second secure channel should be built — the rotated material
            resolves to a new fingerprint and a new pooled channel, while
            the original channel remains for in-flight work.
        """
        # Arrange
        ca_path = tmp_path / "ca.pem"
        key_path = tmp_path / "key.pem"
        cert_path = tmp_path / "cert.pem"
        ca_path.write_bytes(b"ca-v1")
        key_path.write_bytes(b"key")
        cert_path.write_bytes(b"cert")
        provider = FileCredentialProvider(str(ca_path), str(key_path), str(cert_path))

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
        connection = WorkerConnection("10.0.0.7:50051", provider=provider)

        # Act
        async for _ in await connection.dispatch(sample_task):
            pass
        rotated_mtime = os.stat(ca_path).st_mtime_ns + 1_000_000_000
        ca_path.write_bytes(b"ca-v2-rotated")
        os.utime(ca_path, ns=(rotated_mtime, rotated_mtime))
        async for _ in await connection.dispatch(sample_task):
            pass

        # Assert
        assert secure_spy.call_count == 2

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
            target, provider=_secure_provider(identity="wool-worker")
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
    async def test_dispatch_invalid_timeout(self, mocker: MockerFixture, timeout: float):
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
    async def test_dispatch_exceeds_timeout(
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
    async def test_dispatch_exceeds_limit(
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
    async def test_dispatch_releases_semaphore_when_handshake_fails(
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
    async def test_dispatch_releases_semaphore_when_stream_acloses_before_iteration(
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
    async def test_dispatch_propagates_task_encode_failure_unwrapped(
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
    async def test_dispatch_cancelled_during_dispatch(
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
    async def test_dispatch_cancelled_during_execution(
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
    async def test_dispatch_cancelled_during_teardown_releases_channel_ref(
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
    async def test_dispatch_response_exception_with_cancelled_error_releases_channel_ref(
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
    async def test_close_idempotent(self, mocker: MockerFixture):
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
    async def test_close_called_twice_after_uds_self_dispatch(
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
    async def test_dispatch_task_that_yields_multiple_results(
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
    async def test_dispatch_stream_early_close(
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
    async def test_dispatch_with_version_in_ack(
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
    async def test_dispatch_nack_with_exception_reraises_original_class(
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
    async def test_dispatch_nack_with_exception_preserves_subclass_identity(
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
    async def test_dispatch_nack_with_exception_suppresses_implicit_chaining(
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
    async def test_dispatch_nack_with_unpicklable_exception_falls_back_to_rpc_error(
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
    async def test_dispatch_nack_with_non_exception_payload_falls_back_to_rpc_error(
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
    async def test_dispatch_nack_with_base_exception_falls_back_to_rpc_error(
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
                # ``ContextDecodeWarning`` as parse-phase rejection
                # classes that ride the Nack-with-exception channel
                # (unloadable callable / strict-mode context decode).
                # Both must round-trip class+message intact so the
                # caller's ``except ImportError`` / ``except
                # ContextDecodeWarning`` keeps matching.
                ImportError,
                ContextDecodeWarning,
            )
        ),
        message=st.text(
            alphabet=st.characters(min_codepoint=32, max_codepoint=126),
            max_size=64,
        ),
    )
    async def test_dispatch_nack_with_arbitrary_exception_roundtrips(
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
            ContextDecodeWarning) paired with arbitrary printable
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
    async def test_dispatch_with_secure_channel(
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
        mock_secure = mocker.patch("grpc.aio.secure_channel", return_value=mock_channel)

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

        connection = WorkerConnection("localhost:50051", provider=_secure_provider())

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        mock_secure.assert_called_once()
        assert results == ["secure_result"]

        await connection.close()

    @pytest.mark.asyncio
    async def test_dispatch_athrow_propagates_to_worker(
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
    async def test_stream_usable_after_dispatch_returns(
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
    async def test_stream_consumption_releases_pool_ref(
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
        mocker.patch("grpc.aio.insecure_channel", return_value=mock_channel)

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
    async def test_error_mid_stream_releases_pool_ref(
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
        mocker.patch("grpc.aio.insecure_channel", return_value=mock_channel)

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
    async def test_close_invokes_channel_finalizer(
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
        mocker.patch("grpc.aio.insecure_channel", return_value=mock_channel)

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
    async def test_two_dispatches_share_one_channel(
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
    async def test_dispatch_with_default_options(
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
    async def test_dispatch_with_custom_options(
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
    async def test_dispatch_with_custom_keepalive_options(
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
    async def test_dispatch_with_default_keepalive_options(
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
    async def test_dispatch_with_custom_transport_options(
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
    async def test_dispatch_with_self_dispatch(
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
    async def test_dispatch_with_self_dispatch_over_uds(
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
    async def test_dispatch_with_address_mismatch(
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
    async def test_dispatch_self_dispatch_anext_sends_vars(
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
    async def test_dispatch_self_dispatch_with_response_vars(
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

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("result")),
                context=protocol.Context(
                    vars=[
                        protocol.ContextVar(
                            namespace=var.namespace,
                            name=var.name,
                            value=cloudpickle.dumps("back_propagated"),
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
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        assert results == ["result"]
        assert var.get() == "back_propagated"

    @pytest.mark.asyncio
    async def test_dispatch_with_corrupt_response_context_and_worker_exception(
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
            ContextDecodeWarning naming the corrupt var key is also
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
                context=protocol.Context(
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
        with pytest.warns(ContextDecodeWarning, match=var.name):
            with pytest.raises(ValueError, match="worker-side failure"):
                async for _ in await connection.dispatch(sample_task):
                    pass

    @pytest.mark.asyncio
    async def test_dispatch_with_corrupt_response_context_and_result_frame(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test the caller-side response decoder delivers the routine's
        return value and emits a ContextDecodeWarning when a result
        frame's accompanying context payload fails to deserialize.

        Given:
            A worker response that carries a successful routine
            ``result`` payload alongside a context whose var entry
            fails to deserialize
        When:
            The caller iterates the dispatch stream
        Then:
            The caller observes the routine's return value normally
            and a ContextDecodeWarning is emitted — context
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
                context=protocol.Context(
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
        with pytest.warns(ContextDecodeWarning, match="Failed to deserialize"):
            async for value in await connection.dispatch(sample_task):
                results.append(value)

        # Assert
        assert results == ["worker_result"]

    @pytest.mark.asyncio
    async def test_dispatch_with_corrupt_response_context_and_result_frame_strict(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test that a caller can opt into strict semantics by promoting
        ContextDecodeWarning to an error.

        Given:
            The same response shape as the lenient-mode test (result
            + corrupt context var)
        When:
            The caller has installed
            ``warnings.filterwarnings("error", category=ContextDecodeWarning)``
            for the duration of the dispatch
        Then:
            Iterating the dispatch raises a :class:`BaseExceptionGroup`
            whose sole peer is the promoted
            :class:`ContextDecodeWarning` — wool emits decode failures
            uniformly through the group shape so caller code stays
            symmetric across single- and multi-peer cases (e.g.
            decode failure alongside a worker exception). The opt-in
            strict mode lets callers treat ancillary failures as
            fatal without changing wool's wire-protocol defaults.
        """
        # Arrange
        target = "localhost:50051"
        var = ContextVar(
            "strict_corrupt_context_var",
            namespace="strict_corrupt_context",
        )
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=cloudpickle.dumps("worker_result")),
                context=protocol.Context(
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
            _warnings.simplefilter("error", category=ContextDecodeWarning)
            with pytest.raises(BaseExceptionGroup) as exc_info:
                async for _ in await connection.dispatch(sample_task):
                    pass
        assert len(exc_info.value.exceptions) == 1
        assert isinstance(exc_info.value.exceptions[0], ContextDecodeWarning)

    @pytest.mark.asyncio
    async def test_dispatch_without_serializer_uses_cloudpickle_for_vars(
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
            The dispatch stream writes requests
        Then:
            The vars in each request should be serialized via
            cloudpickle
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

        # Assert — the initial request vars should be cloudpickle bytes
        assert results == ["result"]
        initial_request = mock_call.write.call_args_list[0][0][0]
        emitted = {(e.namespace, e.name): e.value for e in initial_request.context.vars}
        assert (var.namespace, var.name) in emitted
        assert len(emitted[(var.namespace, var.name)]) > 16

    @pytest.mark.asyncio
    async def test_dispatch_self_dispatch_initial_request_roundtrips_vars(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch cloudpickle-encodes vars on the initial request.

        Given:
            A WorkerConnection whose target matches the current
            worker's address and a ContextVar with a value set
        When:
            dispatch() sends the initial task request
        Then:
            It should cloudpickle-encode the initial request's context
            vars so they round-trip back to the original value.
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

        # Assert — the initial request (first write) carries the var
        # cloudpickle-encoded; decoding it yields the original value.
        assert results == ["result"]
        initial_request = mock_call.write.call_args_list[0][0][0]
        emitted = {(e.namespace, e.name): e.value for e in initial_request.context.vars}
        assert (var.namespace, var.name) in emitted
        assert cloudpickle.loads(emitted[(var.namespace, var.name)]) == "roundtrip_value"

    @pytest.mark.asyncio
    async def test_dispatch_cross_process_initial_request_encodes_callable(
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
    async def test_dispatch_self_dispatch_initial_request_var_roundtrip_property(
        self,
        value,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch round-trips arbitrary ContextVar values.

        Given:
            A self-dispatch WorkerConnection and any picklable
            ContextVar value drawn from text, ints, tuples, or dicts
        When:
            dispatch() writes the initial request and the var is
            decoded from the emitted protocol.Context
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
        initial_request = mock_call.write.call_args_list[0][0][0]
        emitted = {(e.namespace, e.name): e.value for e in initial_request.context.vars}
        assert (var.namespace, var.name) in emitted
        assert cloudpickle.loads(emitted[(var.namespace, var.name)]) == value

    @pytest.mark.asyncio
    async def test_dispatch_response_exception_with_non_exception_payload_falls_back_to_unexpected_response(
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
    async def test_dispatch_response_exception_with_cancelled_error_propagates_as_cancelled_error(
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
    async def test_dispatch_response_exception_with_cancelled_error_increments_caller_cancelling(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch increments current_task().cancelling() on
        a worker-side CancelledError.

        Mirrors stdlib's local-cancel state shape: a caller catching
        :class:`asyncio.CancelledError` from a wool routine must
        observe ``current_task().cancelling() > 0`` — the same shape
        it would see for a local cancel — so idiomatic
        ``if cancelling() > 0: raise`` re-raise gates and
        ``current_task().uncancel()`` absorbers behave identically
        regardless of whether the cancel originated locally or on
        the worker. ``uncancel()`` must also decrement the count
        back to zero per asyncio's documented contract.

        Given:
            A :class:`protocol.Response` whose ``exception`` field
            carries a cloudpickle dump of
            :class:`asyncio.CancelledError`
        When:
            ``dispatch(task)`` is awaited and the result iterator is
            consumed, and the resulting ``CancelledError`` is caught
        Then:
            It should observe ``current_task().cancelling() > 0``
            synchronously with the catch, and ``uncancel()`` should
            decrement the count back to ``0``.
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

        observed: dict[str, int | None] = {
            "cancelling": None,
            "post_uncancel": None,
        }

        # Run the cancellable consumption in an inner task so the
        # observations happen on a task we control. On Python 3.11
        # ``Task.uncancel()`` only decrements the counter without
        # clearing ``_must_cancel`` — the inner task therefore
        # finalises as cancelled even though ``body()`` returned a
        # value. The outer ``await wrapped`` consumes that
        # cancellation, isolating the test runner's task.
        async def body():
            try:
                async for _ in await connection.dispatch(sample_task):
                    pass
            except asyncio.CancelledError:
                current = asyncio.current_task()
                assert current is not None
                observed["cancelling"] = current.cancelling()
                observed["post_uncancel"] = current.uncancel()

        wrapped = asyncio.ensure_future(body())

        # Act
        try:
            await wrapped
        except asyncio.CancelledError:
            # On Python 3.11 the scheduled cancel still fires after
            # body() returns; on 3.12+ uncancel() suppresses it.
            # Either outcome is fine — we assert on the observations
            # captured inside the except arm.
            pass

        # Assert
        assert observed["cancelling"] is not None and observed["cancelling"] > 0
        assert observed["post_uncancel"] == 0

    @pytest.mark.asyncio
    async def test_dispatch_response_exception_with_cancelled_error_propagates_to_task_cancelled_state(
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
    async def test_dispatch_response_exception_with_decode_failures_swallows_note_write_rejection(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test the strict-mode note/attribute write attempts are
        swallowed for slotted exception classes that reject them.

        Given:
            A :class:`Response` whose ``exception`` payload is a
            ``_StrictRejectingException`` (rejects ``add_note`` with
            :class:`AttributeError` and arbitrary attribute writes
            including ``__wool_context_warnings__`` with
            :class:`AttributeError`) AND whose ``context`` decode
            raises a :class:`BaseExceptionGroup` of
            :class:`ContextDecodeWarning` peers (strict-mode
            promotion)
        When:
            ``dispatch(task)`` is awaited and the result iterator is
            consumed
        Then:
            It should raise the ``_StrictRejectingException``
            unchanged — the failed note/attribute writes are
            swallowed under ``except (AttributeError, TypeError)``
            and ``except AttributeError`` respectively, so the
            routine's primary signal still ships.
        """
        from wool.runtime.context import Context

        # Arrange — patch Context.from_protobuf to raise the
        # strict-mode decode group on each call. The exception arm
        # in _read_next gets ``decode_failures`` populated and then
        # tries to attach them via add_note and a sidecar attribute.
        peer = ContextDecodeWarning("var-1 unencodable")

        def encode_with_strict_failure(cls, *args, **kwargs):
            raise BaseExceptionGroup("strict-mode encode group", [peer])

        mocker.patch.object(
            Context,
            "from_protobuf",
            classmethod(encode_with_strict_failure),
        )

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                exception=protocol.Message(
                    dump=cloudpickle.dumps(_StrictRejectingException("primary signal")),
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

        # Act & assert — the routine's primary signal type is
        # preserved; no stray AttributeError leaks from the
        # swallowed note/attribute writes.
        with pytest.raises(_StrictRejectingException) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass

        assert "primary signal" in str(exc_info.value)
        # The sidecar attribute was never set because the class
        # rejects arbitrary writes — the swallow is the assertion.
        assert not hasattr(exc_info.value, "__wool_context_warnings__")

    @pytest.mark.asyncio
    async def test_dispatch_response_result_with_malformed_payload_falls_back_to_unexpected_response(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch wraps a malformed ``Response.result`` payload
        as :class:`UnexpectedResponse` so the load balancer does not
        evict the worker for what is typically caller-side version
        skew on a shared result class.

        Given:
            A :class:`protocol.Response` whose ``result`` field
            carries bytes that cannot be deserialized
            (b"not a valid pickle stream")
        When:
            ``dispatch(task)`` is awaited and the result iterator is
            consumed
        Then:
            It should raise :class:`UnexpectedResponse` whose
            message names the malformed result payload, chained
            from the underlying deserialization error via
            ``__cause__``; :class:`UnexpectedResponse` is not an
            :class:`RpcError` subclass so the load-balancer
            classification treats it as a caller-fault and does
            not evict the worker.
        """
        # Arrange
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
        with pytest.raises(
            UnexpectedResponse, match="malformed result payload"
        ) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
        # Belt-and-suspenders: the worker-eviction contract is
        # carried by ``RpcError``; a malformed-result degradation
        # must not surface as an ``RpcError`` subclass.
        assert not isinstance(exc_info.value, RpcError)
        # The underlying deserialization error is preserved on
        # ``__cause__`` for diagnostic chains.
        assert exc_info.value.__cause__ is not None

    @pytest.mark.asyncio
    async def test_dispatch_response_exception_with_malformed_payload_falls_back_to_unexpected_response(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch wraps a malformed ``Response.exception``
        payload as :class:`UnexpectedResponse` so the load balancer
        does not evict the worker for what is typically caller-side
        version skew on a shared exception class.

        Given:
            A :class:`protocol.Response` whose ``exception`` field
            carries bytes that cannot be deserialized
            (b"not a valid pickle stream")
        When:
            ``dispatch(task)`` is awaited and the result iterator is
            consumed
        Then:
            It should raise :class:`UnexpectedResponse` whose
            message names the malformed exception payload, with the
            underlying deserialization error preserved on
            ``__cause__`` and ``__suppress_context__`` set so the
            implicit context chain is suppressed.
            :class:`UnexpectedResponse` is not an :class:`RpcError`
            subclass so the load-balancer classification treats it
            as a caller-fault and does not evict the worker.
        """
        # Arrange
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
        with pytest.raises(
            UnexpectedResponse, match="malformed exception payload"
        ) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
        # Belt-and-suspenders: a routine-time decode mismatch must
        # not surface as an ``RpcError`` subclass.
        assert not isinstance(exc_info.value, RpcError)
        # Manual ``__cause__`` chaining preserves the original
        # pickle/import failure for diagnostics; the implicit
        # context chain is suppressed via ``__suppress_context__``.
        assert exc_info.value.__cause__ is not None
        assert exc_info.value.__suppress_context__ is True


@pytest.mark.asyncio
async def test_clear_channel_pool_tears_down_cached_channels(
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
async def test_clear_channel_pool_with_empty_pool_returns_without_raising():
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
