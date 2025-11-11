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

from wool.core import protobuf as pb
from wool.core.work import WorkTask
from wool.core.worker.connection import RpcError
from wool.core.worker.connection import TransientRpcError
from wool.core.worker.connection import UnexpectedResponse
from wool.core.worker.connection import WorkerConnection


@pytest.fixture
def mock_task(mocker: MockerFixture):
    """Provides a mock :class:`WorkTask` for testing.

    Creates a WorkTask with a simple async function that returns a
    test value.
    """

    async def sample_task():
        return "test_result"

    mock_proxy = mocker.MagicMock()
    mock_proxy.id = "test-proxy-id"

    return WorkTask(
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
        """Create a mock gRPC call object.

        Args:
            stream_iterator: The async iterator to wrap
            cancel_raises: If True, cancel() raises RuntimeError
        """
        mock_call = mocker.MagicMock()
        mock_call.__aiter__ = lambda _: stream_iterator

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
    async def test_init_invalid_limit(self, limit: int):
        """Test WorkerConnection initialization with invalid limit.

        Given:
            An invalid limit value (0 or negative)
        When:
            WorkerConnection is instantiated
        Then:
            It should raise ValueError with appropriate message
        """
        # Act & Assert
        with pytest.raises(ValueError, match="Limit must be positive"):
            WorkerConnection("localhost:50051", limit=limit)

    @pytest.mark.asyncio
    async def test_dispatch_task_that_returns(
        self, mocker: MockerFixture, mock_task, async_stream, mock_grpc_call
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
        to_protobuf_spy = mocker.spy(WorkTask, "to_protobuf")

        responses = (
            pb.worker.Response(ack=pb.worker.Ack()),
            pb.worker.Response(
                result=pb.task.Result(dump=cloudpickle.dumps("test_result"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act
        results = []
        async for result in await connection.dispatch(mock_task):
            results.append(result)

        # Assert
        to_protobuf_spy.assert_called_once()
        mock_stub.dispatch.assert_called_once()
        assert len(results) == 1
        assert results[0] == "test_result"

    @pytest.mark.asyncio
    async def test_dispatch_task_that_raises(
        self, mocker: MockerFixture, mock_task, async_stream, mock_grpc_call
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
        to_protobuf_spy = mocker.spy(WorkTask, "to_protobuf")

        responses = (
            pb.worker.Response(ack=pb.worker.Ack()),
            pb.worker.Response(
                exception=pb.task.Exception(
                    dump=cloudpickle.dumps(ValueError("task_error"))
                )
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & Assert
        with pytest.raises(ValueError, match="task_error"):
            async for _ in await connection.dispatch(mock_task):
                pass

        to_protobuf_spy.assert_called_once()
        mock_stub.dispatch.assert_called_once()

    @pytest.mark.asyncio
    async def test_dispatch_no_ack(
        self, mocker: MockerFixture, mock_task, async_stream, mock_grpc_call
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
            pb.worker.Response(
                result=pb.task.Result(dump=cloudpickle.dumps("unexpected"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & Assert
        with pytest.raises(UnexpectedResponse, match="Expected 'ack' response"):
            async for _ in await connection.dispatch(mock_task):
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
        mock_task,
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
        responses = (pb.worker.Response(ack=pb.worker.Ack()), pb.worker.Response())
        mock_call = mock_grpc_call(async_stream(responses), cancel_raises=cancel_raises)

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & Assert
        with pytest.raises(
            UnexpectedResponse, match="Expected 'result' or 'exception' response"
        ):
            async for _ in await connection.dispatch(mock_task):
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
        self, mocker: MockerFixture, mock_task, status_code: grpc.StatusCode
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

        mock_rpc_error = MockRpcError()

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=mock_rpc_error)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & Assert
        with pytest.raises(TransientRpcError):
            async for _ in await connection.dispatch(mock_task):
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
        self, mocker: MockerFixture, mock_task, status_code: grpc.StatusCode
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

        mock_rpc_error = MockRpcError()

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(side_effect=mock_rpc_error)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & Assert
        with pytest.raises(RpcError):
            async for _ in await connection.dispatch(mock_task):
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

        async def sample_task():
            return "test"

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        mock_task = WorkTask(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Act & Assert
        with pytest.raises(ValueError, match="Dispatch timeout must be positive"):
            async for _ in await connection.dispatch(mock_task, timeout=timeout):
                pass

    @pytest.mark.asyncio
    async def test_dispatch_exceeds_timeout(
        self, mocker: MockerFixture, mock_task, async_stream, mock_grpc_call
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
        responses = (asyncio.sleep(1000), pb.worker.Response(ack=pb.worker.Ack()))

        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & Assert
        with pytest.raises(TimeoutError):
            async for _ in await connection.dispatch(mock_task, timeout=0.001):
                pass

        mock_call.cancel.assert_called()

    @pytest.mark.asyncio
    async def test_dispatch_exceeds_limit(
        self, mocker: MockerFixture, mock_task, mock_grpc_call, async_stream
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
            pb.worker.Response(ack=pb.worker.Ack()),
            asyncio.sleep(10),
            pb.worker.Response(result=pb.task.Result(dump=cloudpickle.dumps("done"))),
        )

        long_running_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=long_running_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=1)

        async def consume_slot():
            async for _ in await connection.dispatch(mock_task):
                pass

        blocking_task = asyncio.create_task(consume_slot())

        # Wait briefly to ensure the first task has acquired the semaphore
        await asyncio.sleep(0.01)

        try:
            # Act & Assert
            with pytest.raises(TimeoutError):
                async for _ in await connection.dispatch(mock_task, timeout=0.01):
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
        mock_task,
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
            pb.worker.Response(ack=pb.worker.Ack()),
        )

        mock_call = mock_grpc_call(async_stream(responses), cancel_raises=cancel_raises)

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act
        async def run_dispatch():
            async for _ in await connection.dispatch(mock_task):
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
        mock_task,
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
            pb.worker.Response(ack=pb.worker.Ack()),
            execution_started.set,
            asyncio.sleep(10),
            pb.worker.Response(result=pb.task.Result(dump=cloudpickle.dumps("done"))),
        )

        mock_call = mock_grpc_call(async_stream(responses), cancel_raises=cancel_raises)

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act
        async def run_dispatch():
            async for _ in await connection.dispatch(mock_task):
                pass

        task = asyncio.create_task(run_dispatch())
        await execution_started.wait()
        task.cancel()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await task

        mock_call.cancel.assert_called_once()

    @pytest.mark.asyncio
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(call_count=st.integers(min_value=1, max_value=5))
    async def test_close(self, mocker: MockerFixture, call_count: int):
        """Test closing a connection with multiple invocations.

        Given:
            A connection instance
        When:
            Close is called one or more times
        Then:
            It should close the connection's channel without error
        """
        # Arrange
        connection = WorkerConnection("localhost:50051", limit=10)
        close_spy = mocker.spy(connection._channel, "close")

        # Act
        for _ in range(call_count):
            await connection.close()

        # Assert
        assert close_spy.call_count == call_count
