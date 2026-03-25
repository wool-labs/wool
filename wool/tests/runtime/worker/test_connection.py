import asyncio
from typing import Callable
from typing import Coroutine
from uuid import uuid4

import cloudpickle
import grpc
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

import wool
from wool import protocol
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.worker.base import WorkerOptions
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import UnexpectedResponse
from wool.runtime.worker.connection import WorkerConnection

from .conftest import PicklableMock


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
    @given(limit=st.integers(max_value=0))
    async def test___init___with_invalid_limit(self, limit: int):
        """Test WorkerConnection initialization with invalid limit.

        Given:
            An invalid limit value (0 or negative)
        When:
            WorkerConnection is instantiated
        Then:
            It should raise ValueError with appropriate message
        """
        # Act & assert
        with pytest.raises(ValueError, match="Limit must be positive"):
            WorkerConnection("localhost:50051", limit=limit)

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & assert
        with pytest.raises(UnexpectedResponse, match="Expected 'ack' response"):
            async for _ in await connection.dispatch(sample_task):
                pass

        mock_call.cancel.assert_called_once()

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

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & assert
        with pytest.raises(
            UnexpectedResponse, match="Expected 'result' or 'exception' response"
        ):
            async for _ in await connection.dispatch(sample_task):
                pass

        mock_call.cancel.assert_called()

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        connection = WorkerConnection("localhost:50051", limit=10)

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
        connection = WorkerConnection("localhost:50051", limit=10)

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
            It should raise TimeoutError and cancel the call
        """
        # Arrange
        ack = protocol.Response(ack=protocol.Ack())
        responses = (asyncio.sleep(1000), ack)

        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & assert
        with pytest.raises(TimeoutError):
            async for _ in await connection.dispatch(sample_task, timeout=0.001):
                pass

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
            It should raise TimeoutError while waiting for an available slot
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

        connection = WorkerConnection("localhost:50051", limit=1)

        async def consume_slot():
            async for _ in await connection.dispatch(sample_task):
                pass

        blocking_task = asyncio.create_task(consume_slot())

        # Wait briefly to ensure the first task has acquired the semaphore
        await asyncio.sleep(0.01)

        try:
            # Act & assert
            with pytest.raises(TimeoutError):
                async for _ in await connection.dispatch(sample_task, timeout=0.01):
                    pass

            assert mock_stub.dispatch.call_count == 1
        finally:
            blocking_task.cancel()
            try:
                await blocking_task
            except asyncio.CancelledError:
                pass

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        mock_call.cancel.assert_called_once()

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
        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & assert — should not raise on repeated calls
        await connection.close()
        await connection.close()

    @pytest.mark.asyncio
    async def test_close_clears_uds_pool_entry(
        self,
        mocker: MockerFixture,
        sample_task,
        async_stream,
        mock_grpc_call,
    ):
        """Test close clears UDS channel pool entry after self-dispatch.

        Given:
            A WorkerConnection that has dispatched over UDS
        When:
            close() is called
        Then:
            It should clear both the TCP and UDS pool entries
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
        mocker.patch.object(grpc.aio, "insecure_channel", return_value=mock_channel)

        connection = WorkerConnection(target, limit=10)

        async for _ in await connection.dispatch(sample_task):
            pass

        from wool.runtime.worker import connection as connection_module

        clear_spy = mocker.patch.object(
            connection_module._channel_pool, "clear", mocker.AsyncMock()
        )

        # Act
        await connection.close()

        # Assert
        cleared_keys = [c.args[0] for c in clear_spy.call_args_list]
        assert len(cleared_keys) == 2
        assert (target, None, 10, connection._options) in cleared_keys
        assert (uds_target, None, 10, connection._options) in cleared_keys

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)

        # Assert
        assert results == ["test_result"]

    @pytest.mark.asyncio
    async def test_dispatch_nack_raises_rpc_error(
        self, mocker: MockerFixture, sample_task, async_stream, mock_grpc_call
    ):
        """Test dispatch raises RpcError on Nack response.

        Given:
            A mock worker returning Nack with version mismatch reason
        When:
            dispatch() is called
        Then:
            It should raise RpcError with the rejection reason.
        """
        # Arrange
        responses = (
            protocol.Response(
                nack=protocol.Nack(
                    reason="Incompatible version: client=2.0.0, worker=1.0.0"
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(protocol, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & assert
        with pytest.raises(RpcError, match="Task rejected by worker"):
            async for _ in await connection.dispatch(sample_task):
                pass

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        connection = WorkerConnection("localhost:50051", limit=10)

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

        conn_a = WorkerConnection("localhost:50051", limit=10)
        conn_b = WorkerConnection("localhost:50051", limit=10)

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
        """Test dispatch creates gRPC channel with default WorkerOptions.

        Given:
            A WorkerConnection with no options parameter.
        When:
            A task is dispatched.
        Then:
            It should create a gRPC channel with default WorkerOptions
            sizes.
        """
        # Arrange
        defaults = WorkerOptions()
        mock_channel = mocker.AsyncMock()
        mock_insecure = mocker.patch(
            "grpc.aio.insecure_channel", return_value=mock_channel
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
        """Test dispatch creates gRPC channel with custom WorkerOptions.

        Given:
            A WorkerConnection with custom WorkerOptions message sizes.
        When:
            A task is dispatched.
        Then:
            It should create a gRPC channel with the custom sizes.
        """
        # Arrange
        custom_options = WorkerOptions(
            max_receive_message_length=200 * 1024 * 1024,
            max_send_message_length=50 * 1024 * 1024,
        )
        mock_channel = mocker.AsyncMock()
        mock_insecure = mocker.patch(
            "grpc.aio.insecure_channel", return_value=mock_channel
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

        responses = (
            protocol.Response(ack=protocol.Ack()),
            protocol.Response(result=protocol.Message(dump=cloudpickle.dumps("result"))),
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
