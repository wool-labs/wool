import asyncio
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
from wool.runtime.serializer import PassthroughSerializer
from wool.runtime.worker.base import ChannelOptions
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import UnexpectedResponse
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.connection import clear_channel_pool

from .conftest import PicklableMock


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

        _resp_ser = PassthroughSerializer()
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=_resp_ser.dumps("result"))),
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

        credentials = grpc.ssl_channel_credentials()
        connection = WorkerConnection("localhost:50051", credentials=credentials)

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
        """Test self-dispatch sends protobuf with serializer field set.

        Given:
            A WorkerConnection whose target matches the current
            worker's address
        When:
            dispatch() is called
        Then:
            It should set the serializer field on the protobuf
            Task and not call cloudpickle.dumps for payload fields
        """
        # Arrange
        target = "localhost:50051"
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address=target,
            pid=1,
            version="1.0.0",
        )

        _resp_ser = PassthroughSerializer()
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=_resp_ser.dumps("result"))),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        spy = mocker.patch.object(cloudpickle, "dumps", wraps=cloudpickle.dumps)

        connection = WorkerConnection(target)

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        assert results == ["result"]
        first_write = mock_call.write.call_args_list[0][0][0]
        assert first_write.task.HasField("serializer")
        pickled_objects = [c.args[0] for c in spy.call_args_list]
        assert sample_task.callable not in pickled_objects
        assert sample_task.args not in pickled_objects
        assert sample_task.proxy not in pickled_objects

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

        _resp_ser = PassthroughSerializer()
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=_resp_ser.dumps("result"))),
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

        spy = mocker.patch.object(cloudpickle, "dumps", wraps=cloudpickle.dumps)

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
        first_write = mock_call.write.call_args_list[0][0][0]
        assert first_write.task.HasField("serializer")
        pickled_objects = [c.args[0] for c in spy.call_args_list]
        assert sample_task.callable not in pickled_objects

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
            It should use cloudpickle and not set the serializer
            field
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
        first_write = mock_call.write.call_args_list[0][0][0]
        assert not first_write.task.HasField("serializer")

    @pytest.mark.asyncio
    async def test_dispatch_without_worker_metadata(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch uses cloudpickle when not in a worker process.

        Given:
            A WorkerConnection and no worker metadata set
            (non-worker process)
        When:
            dispatch() is called
        Then:
            It should use cloudpickle and not set the serializer
            field
        """
        # Arrange
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
        first_write = mock_call.write.call_args_list[0][0][0]
        assert not first_write.task.HasField("serializer")

    @pytest.mark.asyncio
    async def test_dispatch_self_dispatch_anext_sends_vars_via_passthrough(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch __anext__ serializes vars via PassthroughSerializer.dumps.

        Given:
            A WorkerConnection whose target matches the current
            worker's address and a ContextVar with a value set
        When:
            The dispatch stream's __anext__ writes a next-frame request
        Then:
            The vars in the written request should be serialized via
            PassthroughSerializer.dumps (16-byte UUID tokens), not
            cloudpickle
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

        _resp_ser = PassthroughSerializer()
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=_resp_ser.dumps("first"))),
            protocol.Response(result=protocol.Message(dump=_resp_ser.dumps("second"))),
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
        # 16-byte passthrough tokens as var values, not cloudpickle bytes
        assert len(results) == 2
        next_request = mock_call.write.call_args_list[1][0][0]
        emitted = {(e.namespace, e.name): e.value for e in next_request.context.vars}
        assert (var.namespace, var.name) in emitted
        assert len(emitted[(var.namespace, var.name)]) == 16

    @pytest.mark.asyncio
    async def test_dispatch_self_dispatch_with_response_vars_via_passthrough(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch applies response vars via passthrough loads.

        Given:
            A WorkerConnection in self-dispatch mode and a response
            carrying vars serialized via PassthroughSerializer.dumps
        When:
            The stream reads the response and applies the vars
        Then:
            The ContextVar value should be updated to the value
            round-tripped through the passthrough serializer
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

        # Build a passthrough-serialized var value for the response
        serializer = PassthroughSerializer()
        pt_bytes = serializer.dumps("back_propagated")

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(
                result=protocol.Message(dump=serializer.dumps("result")),
                context=protocol.Context(
                    vars=[
                        protocol.ContextVar(
                            namespace=var.namespace,
                            name=var.name,
                            value=pt_bytes,
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
            cloudpickle (not passthrough 16-byte tokens)
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

        # Assert — the initial request vars should be cloudpickle bytes,
        # which are longer than a 16-byte passthrough token
        assert results == ["result"]
        initial_request = mock_call.write.call_args_list[0][0][0]
        emitted = {(e.namespace, e.name): e.value for e in initial_request.context.vars}
        assert (var.namespace, var.name) in emitted
        assert len(emitted[(var.namespace, var.name)]) > 16

    @pytest.mark.asyncio
    async def test_dispatch_self_dispatch_initial_request_includes_passthrough_vars(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test self-dispatch initial request serializes vars via passthrough.

        Given:
            A WorkerConnection whose target matches the current
            worker's address and a ContextVar with a value set
        When:
            dispatch() sends the initial task request
        Then:
            The vars map on the initial request should contain
            16-byte passthrough tokens (UUID bytes), not cloudpickle
        """
        # Arrange
        target = "localhost:50051"
        wool.__worker_metadata__ = wool.WorkerMetadata(
            uid=uuid4(),
            address=target,
            pid=1,
            version="1.0.0",
        )

        var = ContextVar("conn_g_var", namespace="conn_g")
        var.set("initial_value")

        _resp_ser = PassthroughSerializer()
        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=_resp_ser.dumps("done"))),
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

        # Assert — initial request (first write) should carry passthrough vars
        assert results == ["done"]
        initial_request = mock_call.write.call_args_list[0][0][0]
        emitted = {(e.namespace, e.name): e.value for e in initial_request.context.vars}
        assert (var.namespace, var.name) in emitted
        # Passthrough tokens are exactly 16 bytes (UUID bytes)
        assert len(emitted[(var.namespace, var.name)]) == 16

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

        # Act & assert
        with pytest.raises(asyncio.CancelledError) as exc_info:
            async for _ in await connection.dispatch(sample_task):
                pass
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
