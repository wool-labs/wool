"""Unit tests for :class:`wool.runtime.worker.interceptor.VersionInterceptor`.

These tests exercise the interceptor's version-checking arms by invoking
``intercept_service`` directly with mocked ``continuation`` /
``handler_call_details`` / ``ServicerContext`` doubles. The interceptor's
happy path (well-formed envelope, compatible version) is covered end-to-end
by ``tests/runtime/worker/test_service.py``; the three failure arms below
are reachable only via injected wire shapes, so they live as unit tests
that route directly through the handler the interceptor returns.
"""

from __future__ import annotations

import grpc
import pytest
from pytest_mock import MockerFixture

from wool import protocol
from wool.runtime.worker.interceptor import VersionInterceptor


class _FakeServicerContext:
    """Minimal :class:`grpc.aio.ServicerContext` test double.

    ``intercept_service`` only calls ``await context.abort(code, details)``,
    which in production raises an internal ``AbortError`` and tears the
    stream down. The fake records the call and raises a synthetic
    :class:`grpc.RpcError` so the test can capture the abort metadata.
    """

    class _Abort(grpc.RpcError):
        def __init__(self, code, details):
            super().__init__()
            self._code = code
            self._details = details

        def code(self):
            return self._code

        def details(self):
            return self._details

    def __init__(self):
        self.abort_calls: list[tuple[grpc.StatusCode, str]] = []

    async def abort(self, code: grpc.StatusCode, details: str) -> None:
        self.abort_calls.append((code, details))
        raise self._Abort(code, details)


async def _yield(first_bytes: bytes):
    """Build an async iterator that yields one bytes object."""
    yield first_bytes
    # No further frames — the interceptor only reads the first frame
    # before deciding whether to abort or chain to the original handler.


async def _passthrough_stream(request_iterator, context):
    """Real async-generator stand-in for the wrapped handler.

    Mirrors the shape ``intercept_service`` expects of the inner
    handler's ``stream_stream``: consume the request iterator, yield
    response bytes. The four failure-arm tests abort before reaching
    this, so it is a no-op stub; keeping it as a real async generator
    (rather than an :class:`AsyncMock`) guards against future
    happy-path additions silently producing a ``TypeError`` from a
    ``MagicMock`` not being iterable.
    """
    async for _ in request_iterator:
        yield b""


async def _resolve_handler(
    mocker: MockerFixture,
    method: str = "/wool.runtime.protobuf.wire.Worker/dispatch",
):
    """Drive :meth:`intercept_service` and return the wrapped handler."""
    interceptor = VersionInterceptor()
    inner_handler = mocker.MagicMock()
    inner_handler.stream_stream = _passthrough_stream
    inner_handler.request_deserializer = lambda b: b
    inner_handler.response_serializer = lambda b: b

    continuation = mocker.AsyncMock(return_value=inner_handler)
    handler_call_details = mocker.MagicMock()
    handler_call_details.method = method

    wrapped = await interceptor.intercept_service(continuation, handler_call_details)
    return wrapped, inner_handler


class TestVersionInterceptor:
    """Tests for :class:`VersionInterceptor`."""

    @pytest.mark.asyncio
    async def test_intercept_service_passes_through_non_dispatch_methods(
        self, mocker: MockerFixture
    ):
        """Test the interceptor returns the inner handler unchanged for non-dispatch methods.

        Given:
            A :class:`VersionInterceptor` and a continuation whose
            ``handler_call_details.method`` does not end in
            ``/dispatch``
        When:
            ``intercept_service`` is awaited
        Then:
            It should return the inner handler exactly — no
            version-check wrapper is installed.
        """
        # Arrange
        interceptor = VersionInterceptor()
        inner_handler = mocker.MagicMock()
        inner_handler.stream_stream = _passthrough_stream
        inner_handler.request_deserializer = lambda b: b
        inner_handler.response_serializer = lambda b: b
        continuation = mocker.AsyncMock(return_value=inner_handler)
        details = mocker.MagicMock()
        details.method = "/wool.runtime.protobuf.wire.Worker/stop"

        # Act
        result = await interceptor.intercept_service(continuation, details)

        # Assert
        assert result is inner_handler

    @pytest.mark.asyncio
    async def test_intercept_service_aborts_on_malformed_version_envelope(
        self, mocker: MockerFixture
    ):
        """Test the interceptor aborts when the task envelope is malformed.

        Given:
            A :class:`VersionInterceptor` mounted on a ``dispatch``
            method and a first request whose ``TaskEnvelope.version``
            field cannot be parsed (simulated by patching
            ``TaskEnvelope.ParseFromString`` to raise)
        When:
            The caller writes the malformed first request
        Then:
            The server should abort with
            :data:`grpc.StatusCode.FAILED_PRECONDITION` and a
            ``"Failed to parse version envelope"`` detail.
        """
        # Arrange
        wrapped, _ = await _resolve_handler(mocker)
        first_request = protocol.Request(task=protocol.Task())
        first_bytes = first_request.SerializeToString()
        ctx = _FakeServicerContext()
        mocker.patch.object(
            protocol.TaskEnvelope,
            "ParseFromString",
            side_effect=Exception("malformed"),
        )

        # Act
        with pytest.raises(_FakeServicerContext._Abort):
            async for _ in wrapped.stream_stream(_yield(first_bytes), ctx):
                pass

        # Assert

        assert ctx.abort_calls
        code, details = ctx.abort_calls[0]
        assert code is grpc.StatusCode.FAILED_PRECONDITION
        assert "Failed to parse version envelope" in details

    @pytest.mark.asyncio
    async def test_intercept_service_aborts_on_malformed_request_bytes(
        self, mocker: MockerFixture
    ):
        """Test the interceptor aborts on a malformed first request frame.

        Given:
            A :class:`VersionInterceptor` mounted on a ``dispatch``
            method and a first frame consisting of bytes that do not
            decode as a valid :class:`protocol.Request` (raises
            ``google.protobuf.message.DecodeError`` from
            ``Request.ParseFromString``)
        When:
            The caller writes the malformed first frame
        Then:
            The server should abort with
            :data:`grpc.StatusCode.FAILED_PRECONDITION` and a
            ``"Failed to parse version envelope"`` detail — not leak
            the underlying ``DecodeError`` as ``StatusCode.UNKNOWN``.
        """
        # Arrange
        wrapped, _ = await _resolve_handler(mocker)
        # Bytes that are not a valid protobuf wire encoding for a
        # ``Request`` message. The 0xff prefix gives a tag with a
        # field number / wire type combination ``Request`` does not
        # define, which ``ParseFromString`` rejects with
        # ``google.protobuf.message.DecodeError``.
        malformed_bytes = b"\xff\xff\xff\xff\xff"
        ctx = _FakeServicerContext()

        # Act
        with pytest.raises(_FakeServicerContext._Abort):
            async for _ in wrapped.stream_stream(_yield(malformed_bytes), ctx):
                pass

        # Assert

        assert ctx.abort_calls
        code, details = ctx.abort_calls[0]
        assert code is grpc.StatusCode.FAILED_PRECONDITION
        assert "Failed to parse version envelope" in details

    @pytest.mark.asyncio
    async def test_intercept_service_aborts_on_unparseable_version(
        self, mocker: MockerFixture
    ):
        """Test the interceptor aborts on an unparseable client version.

        Given:
            A :class:`VersionInterceptor` and a first request whose
            ``TaskEnvelope.version`` is a non-empty unparseable
            string (e.g. ``"not-a-version"``)
        When:
            The caller writes the first request
        Then:
            The server should abort with
            :data:`grpc.StatusCode.FAILED_PRECONDITION` and a detail
            naming both the client and worker versions.
        """
        # Arrange
        wrapped, _ = await _resolve_handler(mocker)
        first_request = protocol.Request(task=protocol.Task(version="not-a-version"))
        first_bytes = first_request.SerializeToString()
        ctx = _FakeServicerContext()

        # Act
        with pytest.raises(_FakeServicerContext._Abort):
            async for _ in wrapped.stream_stream(_yield(first_bytes), ctx):
                pass

        # Assert

        assert ctx.abort_calls
        code, details = ctx.abort_calls[0]
        assert code is grpc.StatusCode.FAILED_PRECONDITION
        assert "Unparseable version" in details
        assert "not-a-version" in details
        assert protocol.__version__ in details

    @pytest.mark.asyncio
    async def test_intercept_service_aborts_on_incompatible_major_version(
        self, mocker: MockerFixture
    ):
        """Test the interceptor aborts on an incompatible major version.

        Given:
            A :class:`VersionInterceptor` and a first request whose
            ``TaskEnvelope.version`` is parseable but on a different
            major (e.g. ``"99.0.0"`` against the worker's
            ``protocol.__version__``)
        When:
            The caller writes the first request
        Then:
            The server should abort with
            :data:`grpc.StatusCode.FAILED_PRECONDITION` and a detail
            naming both versions.
        """
        # Arrange
        wrapped, _ = await _resolve_handler(mocker)
        # Use a future major so the incompatibility is independent of
        # the worker's current version (under SemVer the worker
        # rejects any client whose major exceeds its own).
        first_request = protocol.Request(task=protocol.Task(version="99.0.0"))
        first_bytes = first_request.SerializeToString()
        ctx = _FakeServicerContext()

        # Act
        with pytest.raises(_FakeServicerContext._Abort):
            async for _ in wrapped.stream_stream(_yield(first_bytes), ctx):
                pass

        # Assert

        assert ctx.abort_calls
        code, details = ctx.abort_calls[0]
        assert code is grpc.StatusCode.FAILED_PRECONDITION
        assert "Incompatible version" in details
        assert "99.0.0" in details
        assert protocol.__version__ in details
