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

from wool.runtime import protobuf as pb
from wool.runtime.work.task import Task
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import UnexpectedResponse
from wool.runtime.worker.connection import WorkerConnection


@pytest.fixture
def sample_task(mocker: MockerFixture):
    """Provides a mock :class:`Task` for testing.

    Creates a Task with a simple async function that returns a
    test value.
    """

    async def sample_task():
        return "test_result"

    mock_proxy = mocker.MagicMock()
    mock_proxy.id = "test-proxy-id"

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
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & Assert
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
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & Assert
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

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        work_task = Task(
            id=uuid4(),
            callable=test_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Act & Assert
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
        responses = (asyncio.sleep(1000), pb.worker.Response(ack=pb.worker.Ack()))

        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act & Assert
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
            async for _ in await connection.dispatch(sample_task):
                pass

        blocking_task = asyncio.create_task(consume_slot())

        # Wait briefly to ensure the first task has acquired the semaphore
        await asyncio.sleep(0.01)

        try:
            # Act & Assert
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
            pb.worker.Response(ack=pb.worker.Ack()),
        )

        mock_call = mock_grpc_call(async_stream(responses), cancel_raises=cancel_raises)

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

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
            pb.worker.Response(ack=pb.worker.Ack()),
            pb.worker.Response(
                result=pb.task.Result(dump=cloudpickle.dumps("result_1"))
            ),
            pb.worker.Response(
                result=pb.task.Result(dump=cloudpickle.dumps("result_2"))
            ),
            pb.worker.Response(
                result=pb.task.Result(dump=cloudpickle.dumps("result_3"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

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
            pb.worker.Response(ack=pb.worker.Ack()),
            pb.worker.Response(
                result=pb.task.Result(dump=cloudpickle.dumps("result_1"))
            ),
            pb.worker.Response(
                result=pb.task.Result(dump=cloudpickle.dumps("result_2"))
            ),
            pb.worker.Response(
                result=pb.task.Result(dump=cloudpickle.dumps("result_3"))
            ),
        )
        mock_call = mock_grpc_call(async_stream(responses))

        mock_stub = mocker.MagicMock()
        mock_stub.dispatch = mocker.MagicMock(return_value=mock_call)
        mocker.patch.object(pb.worker, "WorkerStub", return_value=mock_stub)

        connection = WorkerConnection("localhost:50051", limit=10)

        # Act
        results = []
        async for result in await connection.dispatch(sample_task):
            results.append(result)
            if len(results) >= 2:
                break

        # Assert - only got first 2 results
        assert results == ["result_1", "result_2"]


class TestDispatchStream:
    """Tests for _DispatchStream class."""

    @pytest.mark.asyncio
    async def test_init(self, mocker: MockerFixture):
        """Test _DispatchStream instantiation.

        Given:
            A gRPC call object
        When:
            _DispatchStream is instantiated
        Then:
            Creates iterator wrapping the call
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        mock_call = mocker.MagicMock()

        # Act
        stream = _DispatchStream(mock_call)

        # Assert
        assert stream._call == mock_call
        assert not stream._closed

    @pytest.mark.asyncio
    async def test_aiter(self, mocker: MockerFixture):
        """Test _DispatchStream used in async for loop.

        Given:
            A _DispatchStream instance
        When:
            Used in async for loop
        Then:
            Returns self as async iterator
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        mock_call = mocker.MagicMock()
        stream = _DispatchStream(mock_call)

        # Act
        result = stream.__aiter__()

        # Assert
        assert result is stream

    @pytest.mark.asyncio
    async def test_anext_with_result(self, mocker: MockerFixture):
        """Test _DispatchStream __anext__ with response containing result.

        Given:
            A _DispatchStream with response containing result
        When:
            __anext__ is called
        Then:
            Returns deserialized result value
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        async def mock_iter():
            yield pb.worker.Response(
                result=pb.task.Result(dump=cloudpickle.dumps("test_value"))
            )

        mock_call = mocker.MagicMock()
        mock_call.__aiter__ = lambda _: mock_iter()
        stream = _DispatchStream(mock_call)

        # Act
        result = await stream.__anext__()

        # Assert
        assert result == "test_value"

    @pytest.mark.asyncio
    async def test_anext_with_exception(self, mocker: MockerFixture):
        """Test _DispatchStream __anext__ with response containing exception.

        Given:
            A _DispatchStream with response containing exception
        When:
            __anext__ is called
        Then:
            Raises the deserialized exception
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        test_exception = ValueError("test error")

        async def mock_iter():
            yield pb.worker.Response(
                exception=pb.task.Exception(dump=cloudpickle.dumps(test_exception))
            )

        mock_call = mocker.MagicMock()
        mock_call.__aiter__ = lambda _: mock_iter()
        stream = _DispatchStream(mock_call)

        # Act & Assert
        with pytest.raises(ValueError, match="test error"):
            await stream.__anext__()

    @pytest.mark.asyncio
    async def test_anext_with_unexpected_response(self, mocker: MockerFixture):
        """Test _DispatchStream __anext__ with neither result nor exception.

        Given:
            A _DispatchStream with response containing neither result nor exception
        When:
            __anext__ is called
        Then:
            Raises UnexpectedResponse with descriptive message
        """
        # Arrange
        from wool.runtime.worker.connection import UnexpectedResponse
        from wool.runtime.worker.connection import _DispatchStream

        async def mock_iter():
            yield pb.worker.Response()  # Empty response

        mock_call = mocker.MagicMock()
        mock_call.__aiter__ = lambda _: mock_iter()
        mock_call.cancel = mocker.MagicMock()
        stream = _DispatchStream(mock_call)

        # Act & Assert
        with pytest.raises(
            UnexpectedResponse, match="Expected 'result' or 'exception' response"
        ):
            await stream.__anext__()

        # Should have cancelled the call
        mock_call.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_anext_when_closed(self, mocker: MockerFixture):
        """Test _DispatchStream __anext__ when already closed.

        Given:
            A _DispatchStream that has been closed
        When:
            __anext__ is called
        Then:
            Raises StopAsyncIteration
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        mock_call = mocker.MagicMock()
        stream = _DispatchStream(mock_call)
        stream._closed = True

        # Act & Assert
        with pytest.raises(StopAsyncIteration):
            await stream.__anext__()

    @pytest.mark.asyncio
    async def test_anext_with_iteration_exception(self, mocker: MockerFixture):
        """Test _DispatchStream __anext__ when iteration raises exception.

        Given:
            A _DispatchStream where iteration raises exception
        When:
            __anext__ encounters error
        Then:
            Cancels underlying call and re-raises exception
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        async def mock_iter():
            raise RuntimeError("Iteration failed")
            yield  # unreachable

        mock_call = mocker.MagicMock()
        mock_call.__aiter__ = lambda _: mock_iter()
        mock_call.cancel = mocker.MagicMock()
        stream = _DispatchStream(mock_call)

        # Act & Assert
        with pytest.raises(RuntimeError, match="Iteration failed"):
            await stream.__anext__()

        # Should have cancelled the call
        mock_call.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_anext_cancel_exception_suppressed(self, mocker: MockerFixture):
        """Test _DispatchStream __anext__ when call.cancel() fails.

        Given:
            A _DispatchStream where call.cancel() raises
        When:
            Exception occurs during iteration
        Then:
            Suppresses cancel exception and re-raises original
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        async def mock_iter():
            raise RuntimeError("Original error")
            yield  # unreachable

        mock_call = mocker.MagicMock()
        mock_call.__aiter__ = lambda _: mock_iter()
        mock_call.cancel = mocker.MagicMock(side_effect=Exception("Cancel failed"))
        stream = _DispatchStream(mock_call)

        # Act & Assert - should raise original error, not cancel error
        with pytest.raises(RuntimeError, match="Original error"):
            await stream.__anext__()

    @pytest.mark.asyncio
    async def test_aclose(self, mocker: MockerFixture):
        """Test _DispatchStream aclose() method.

        Given:
            A _DispatchStream that is not closed
        When:
            aclose() is called
        Then:
            Cancels underlying call and sets closed flag
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        mock_call = mocker.MagicMock()
        mock_call.cancel = mocker.MagicMock()
        stream = _DispatchStream(mock_call)

        # Act
        await stream.aclose()

        # Assert
        assert stream._closed
        mock_call.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_aclose_idempotent(self, mocker: MockerFixture):
        """Test _DispatchStream aclose() when already closed.

        Given:
            A _DispatchStream that is already closed
        When:
            aclose() is called
        Then:
            Returns immediately without error (idempotent)
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        mock_call = mocker.MagicMock()
        mock_call.cancel = mocker.MagicMock()
        stream = _DispatchStream(mock_call)
        stream._closed = True

        # Act
        await stream.aclose()

        # Assert - cancel should not be called again
        mock_call.cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_aclose_cancel_exception_suppressed(self, mocker: MockerFixture):
        """Test _DispatchStream aclose() when call.cancel() raises.

        Given:
            A _DispatchStream where call.cancel() raises
        When:
            aclose() is called
        Then:
            Suppresses cancel exception and completes cleanup
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        mock_call = mocker.MagicMock()
        mock_call.cancel = mocker.MagicMock(side_effect=Exception("Cancel failed"))
        stream = _DispatchStream(mock_call)

        # Act - should not raise
        await stream.aclose()

        # Assert
        assert stream._closed

    @pytest.mark.asyncio
    async def test_asend_not_implemented(self, mocker: MockerFixture):
        """Test _DispatchStream asend() raises NotImplementedError.

        Given:
            A _DispatchStream instance
        When:
            asend() is called with any value
        Then:
            Raises NotImplementedError with descriptive message
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        mock_call = mocker.MagicMock()
        stream = _DispatchStream(mock_call)

        # Act & Assert
        with pytest.raises(NotImplementedError, match="asend\\(\\) is not supported"):
            await stream.asend("value")

    @pytest.mark.asyncio
    async def test_athrow_not_implemented(self, mocker: MockerFixture):
        """Test _DispatchStream athrow() raises NotImplementedError.

        Given:
            A _DispatchStream instance
        When:
            athrow() is called with exception info
        Then:
            Raises NotImplementedError with descriptive message
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        mock_call = mocker.MagicMock()
        stream = _DispatchStream(mock_call)

        # Act & Assert
        with pytest.raises(NotImplementedError, match="athrow\\(\\) is not supported"):
            await stream.athrow(ValueError, ValueError("test"))

    @pytest.mark.asyncio
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(response_count=st.integers(min_value=0, max_value=5))
    async def test_property_stream_cleanup(self, mocker: MockerFixture, response_count):
        """Property test: _DispatchStream with iteration and early termination.

        Given:
            Any _DispatchStream with valid responses
        When:
            Iteration and early termination via aclose()
        Then:
            Stream stops cleanly without resource leaks
        """
        # Arrange
        from wool.runtime.worker.connection import _DispatchStream

        async def mock_iter():
            for i in range(response_count):
                yield pb.worker.Response(
                    result=pb.task.Result(dump=cloudpickle.dumps(i))
                )

        mock_call = mocker.MagicMock()
        mock_call.__aiter__ = lambda _: mock_iter()
        mock_call.cancel = mocker.MagicMock()
        stream = _DispatchStream(mock_call)

        # Act - iterate partway and close
        results = []
        async for result in stream:
            results.append(result)
            if len(results) >= 2:
                break

        await stream.aclose()

        # Assert
        assert stream._closed
        if response_count > 0:
            assert len(results) <= min(2, response_count)
