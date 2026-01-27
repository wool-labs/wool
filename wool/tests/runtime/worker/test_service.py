import asyncio
from contextlib import asynccontextmanager
from typing import Final
from uuid import uuid4

import cloudpickle
import grpc
import pytest
from grpc import StatusCode
from pytest_mock import MockerFixture

from wool.runtime import protobuf as pb
from wool.runtime.protobuf.worker import WorkerStub
from wool.runtime.protobuf.worker import add_WorkerServicer_to_server
from wool.runtime.work.task import WorkTask
from wool.runtime.work.task import WorkTaskEvent
from wool.runtime.worker.service import WorkerService
from wool.runtime.worker.service import _ReadOnlyEvent


@pytest.fixture(scope="function")
def grpc_add_to_server():
    return add_WorkerServicer_to_server


@pytest.fixture(scope="function")
def grpc_servicer():
    return WorkerService()


@pytest.fixture(scope="function")
def grpc_stub_cls():
    return WorkerStub


# Global event for controlling test task execution
_control_event: asyncio.Event | None = None


def _get_control_event() -> asyncio.Event:
    """Get the global control event for test tasks."""
    assert _control_event
    return _control_event


async def _controllable_task():
    """Task function that waits on the global control event."""
    await _get_control_event().wait()
    return "task_completed"


@pytest.fixture
@asynccontextmanager
async def service_fixture(mocker: MockerFixture, grpc_aio_stub):
    """Provides a WorkerService with a dispatched task that waits on an event.

    This fixture returns a tuple of (service, event, stub) where:
    - service: WorkerService instance with a controllable task dispatched
    - event: asyncio.Event that controls task completion
    - stub: gRPC stub for interacting with the service

    The task remains active until the event is set. On cleanup, if the event
    is still unset, it will be set and the service will be stopped.
    """
    global _control_event

    service = WorkerService()
    _control_event = asyncio.Event()

    mock_proxy = mocker.MagicMock()
    mock_proxy.id = "test-proxy-id"

    wool_task = WorkTask(
        id=uuid4(),
        callable=_controllable_task,
        args=(),
        kwargs={},
        proxy=mock_proxy,
    )

    request = wool_task.to_protobuf()

    # Start the service and dispatch the task
    async with grpc_aio_stub(servicer=service) as stub:
        stream = stub.dispatch(request)

        # Wait for the ack to confirm task is dispatched
        async for response in stream:
            assert response.HasField("ack")
            break

        try:
            yield service, _control_event, stub
        finally:
            # Cleanup: ensure task completes and service stops
            if _control_event and not _control_event.is_set():
                _control_event.set()
            if not service.stopped.is_set():
                stop_request = pb.worker.StopRequest(timeout=1)
                await stub.stop(stop_request)
            _control_event = None


class TestReadOnlyEvent:
    """Tests for :class:`_ReadOnlyEvent` wrapper."""

    unset_event: Final = asyncio.Event()
    set_event: Final = asyncio.Event()
    set_event.set()

    @pytest.mark.parametrize("event", (unset_event, set_event))
    def test_init(self, event):
        """Test :class:`_ReadOnlyEvent` initialization.

        Given:
            An :class:`asyncio.Event` instance
        When:
            :class:`_ReadOnlyEvent` is instantiated with the event
        Then:
            It should wrap the event successfully
        """
        # Act
        read_only_event = _ReadOnlyEvent(event)

        # Assert
        assert read_only_event.is_set() == event.is_set()

        with pytest.raises(AttributeError):
            getattr(read_only_event, "set")

        with pytest.raises(AttributeError):
            getattr(read_only_event, "clear")

    def test_is_set(self, mocker: MockerFixture):
        """Test :class:`_ReadOnlyEvent.is_set` calls :meth:`~asyncio.Event.is_set` on its
        underlying event.

        Given:
            A :class:`_ReadOnlyEvent` instance wrapping an event
        When:
            :meth:`~_ReadOnlyEvent.is_set` is called
        Then:
            :meth:`~asyncio.Event.is_set` is called on the underlying event
        """
        # Arrange
        event = asyncio.Event()
        is_set_spy = mocker.spy(event, "is_set")
        read_only_event = _ReadOnlyEvent(event)

        # Act
        read_only_event.is_set()

        # Assert
        is_set_spy.assert_called_once()

    @pytest.mark.asyncio
    async def test_wait(self, mocker: MockerFixture):
        """Test :class:`_ReadOnlyEvent.wait` calls :meth:`~asyncio.Event.wait` on its
        underlying event.

        Given:
            A :class:`_ReadOnlyEvent` instance wrapping an event
        When:
            :meth:`~_ReadOnlyEvent.wait` is called
        Then:
            :meth:`~asyncio.Event.wait` is called and awaited on the underlying event
        """
        # Arrange
        event = asyncio.Event()
        event.set()
        wait_spy = mocker.patch.object(event, "wait", mocker.AsyncMock(wraps=event.wait))
        read_only_event = _ReadOnlyEvent(event)

        # Act
        await read_only_event.wait()

        # Assert
        wait_spy.assert_awaited_once_with()


class TestWorkerService:
    def test_init(self):
        """Test :class:`WorkerService` initialization.

        Given:
            No preconditions
        When:
            :class:`WorkerService` is instantiated
        Then:
            It should initialize successfully and expose its stopping and stopped events
        """
        # Act
        service = WorkerService()

        # Assert
        assert service.stopping is not None
        assert service.stopped is not None
        assert not service.stopping.is_set()
        assert not service.stopped.is_set()

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_init")
    async def test_dispatch_task_that_returns(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch successfully executes task that returns
        a value.

        Given:
            A gRPC :class:`WorkerService` that is not stopping or stopped
        When:
            Dispatch RPC is called with a task that returns a value
        Then:
            It should emit task-scheduled event and return task result
        """

        # Arrange
        async def sample_task():
            return "test_result"

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        wool_task = WorkTask(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = wool_task.to_protobuf()

        emit_spy = mocker.spy(WorkTaskEvent, "emit")

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch(request)
            responses = [r async for r in stream]

        # Assert
        ack, reponse = responses
        assert ack.HasField("ack")
        assert reponse.HasField("result")
        assert cloudpickle.loads(reponse.result.dump) == "test_result"

        # Verify
        emit_spy.assert_called()
        event_calls = [call[0][0] for call in emit_spy.call_args_list]
        scheduled_events = [e for e in event_calls if e.type == "task-scheduled"]
        assert len(scheduled_events) == 1

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_init")
    async def test_dispatch_task_that_raises(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch successfully executes task
        that raises an exception.

        Given:
            A gRPC :class:`WorkerService` that is not stopping or stopped
        When:
            Dispatch RPC is called with a task that raises an exception
        Then:
            It should emit task-scheduled event and return task exception as the result
        """

        # Arrange
        async def failing_task():
            raise ValueError("test_exception")

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        wool_task = WorkTask(
            id=uuid4(),
            callable=failing_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = wool_task.to_protobuf()

        emit_spy = mocker.spy(WorkTaskEvent, "emit")

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch(request)
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("exception")
        exception = cloudpickle.loads(response.exception.dump)
        assert isinstance(exception, ValueError)
        assert str(exception) == "test_exception"
        emit_spy.assert_called()
        event_calls = [call[0][0] for call in emit_spy.call_args_list]
        scheduled_events = [e for e in event_calls if e.type == "task-scheduled"]
        assert len(scheduled_events) == 1

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_init")
    async def test_dispatch_while_stopping(
        self, service_fixture, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService` dispatch aborts when stopping.

        Given:
            A :class:`WorkerService` with an active task, transitioning to stopping state
        When:
            stop is called and another dispatch RPC is attempted
        Then:
            It should abort the new dispatch with UNAVAILABLE status without emitting
            any task events
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            emit_spy = mocker.spy(WorkTaskEvent, "emit")
            initial_emit_count = emit_spy.call_count

            # Initiate stop (service enters stopping state)
            # Use wait=0 to immediately cancel tasks, but we'll check before tasks finish
            stop_task = asyncio.ensure_future(
                stub.stop(pb.worker.StopRequest(timeout=5))
            )

            await asyncio.wait_for(service.stopping.wait(), 1)
            assert not service.stopped.is_set()

            # Create a new task to dispatch
            async def another_task():
                return "should_not_execute"

            mock_proxy = mocker.MagicMock()
            mock_proxy.id = "test-proxy-id-2"

            wool_task = WorkTask(
                id=uuid4(),
                callable=another_task,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )

            request = wool_task.to_protobuf()

            # Act & Assert
            with pytest.raises(grpc.RpcError) as exc_info:
                stream = stub.dispatch(request)
                async for _ in stream:
                    pass

            assert exc_info.value.code() == StatusCode.UNAVAILABLE

            # Verify no task-scheduled event was emitted for the new task
            scheduled_events = [
                call
                for call in emit_spy.call_args_list[initial_emit_count:]
                if call[0][0].type == "task-scheduled"
            ]
            assert len(scheduled_events) == 0

            # Cleanup: let the original task complete so stop can finish
            event.set()
            await stop_task

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_init")
    async def test_dispatch_while_stopped(self, service_fixture, mocker: MockerFixture):
        """Test :class:`WorkerService` dispatch aborts when stopped.

        Given:
            A :class:`WorkerService` that has been stopped
        When:
            dispatch RPC is called with a task request
        Then:
            It should abort with UNAVAILABLE status without emitting any task events
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Complete the task and stop the service
            event.set()
            await stub.stop(pb.worker.StopRequest(timeout=5))

            # Verify service is stopped
            assert service.stopping.is_set()
            assert service.stopped.is_set()

            emit_spy = mocker.spy(WorkTaskEvent, "emit")
            initial_emit_count = emit_spy.call_count

            # Create a new task to dispatch
            async def another_task():
                return "should_not_execute"

            mock_proxy = mocker.MagicMock()
            mock_proxy.id = "test-proxy-id-2"

            wool_task = WorkTask(
                id=uuid4(),
                callable=another_task,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )

            request = wool_task.to_protobuf()

            # Act & Assert
            with pytest.raises(grpc.RpcError) as exc_info:
                stream = stub.dispatch(request)
                async for _ in stream:
                    pass

            assert exc_info.value.code() == StatusCode.UNAVAILABLE

            # Verify no task-scheduled event was emitted
            scheduled_events = [
                call
                for call in emit_spy.call_args_list[initial_emit_count:]
                if call[0][0].type == "task-scheduled"
            ]
            assert len(scheduled_events) == 0

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_dispatch_task_that_returns")
    async def test_stop_and_cancel(
        self,
        grpc_aio_stub,
        grpc_servicer,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
    ):
        """Test :class:`WorkerService` stop method gracefully shuts down.

        Given:
            A running :class:`WorkerService` with active tasks
        When:
            stop RPC is called with a timeout ("wait") of 0
        Then:
            It should cancel its tasks immediately, signal stopped state and
            call proxy_pool.clear()
        """

        # Arrange - Create a task that would take a long time
        async def long_running_task():
            await asyncio.sleep(10)
            return "should_not_complete"

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        wool_task = WorkTask(
            id=uuid4(),
            callable=long_running_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = wool_task.to_protobuf()

        # Act
        async with grpc_aio_stub() as stub:
            # Start the task dispatch stream
            stream = stub.dispatch(request)
            async for response in stream:
                assert response.HasField("ack")
                break

            # Stop with 0 second timeout in order to cancel tasks immediately
            stop_request = pb.worker.StopRequest(timeout=0)
            stop_result = await stub.stop(stop_request)

            # Assert
            async for response in stream:
                assert response.HasField("exception")
                exception = cloudpickle.loads(response.exception.dump)
                assert isinstance(exception, asyncio.CancelledError)
                break

            assert isinstance(stop_result, pb.worker.Void)
            assert grpc_servicer.stopping.is_set()
            assert grpc_servicer.stopped.is_set()
            mock_worker_proxy_cache.clear.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_dispatch_task_that_returns")
    async def test_stop_and_wait(
        self,
        grpc_aio_stub,
        grpc_servicer,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
    ):
        """Test :class:`WorkerService` stop method gracefully shuts down.

        Given:
            A running :class:`WorkerService` with active tasks
        When:
            stop RPC is called with a positive timeout ("wait")
        Then:
            It should await its tasks, signal stopped state and call
            proxy_pool.clear()
        """

        # Arrange
        async def quick_task():
            return "completed"

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        wool_task = WorkTask(
            id=uuid4(),
            callable=quick_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = wool_task.to_protobuf()

        # Act
        async with grpc_aio_stub() as stub:
            # Start the task dispatch stream
            stream = stub.dispatch(request)
            async for response in stream:
                assert response.HasField("ack")
                break

            # Stop with 5 second timeout to allow tasks to complete
            stop_request = pb.worker.StopRequest(timeout=5)
            stop_result = await stub.stop(stop_request)

            # Assert
            async for response in stream:
                assert response.HasField("result")
                result = cloudpickle.loads(response.result.dump)
                assert result == "completed"
                break

            assert isinstance(stop_result, pb.worker.Void)
            assert grpc_servicer.stopping.is_set()
            assert grpc_servicer.stopped.is_set()
            mock_worker_proxy_cache.clear.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_dispatch_task_that_returns")
    async def test_stop_while_idle(
        self, grpc_aio_stub, grpc_servicer, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` stop method gracefully shuts down.

        Given:
            A running :class:`WorkerService` with no active tasks
        When:
            stop RPC is called
        Then:
            It should signal stopped state and call proxy_pool.clear()
        """
        # Arrange
        stop_request = pb.worker.StopRequest(timeout=10)

        # Act
        async with grpc_aio_stub() as stub:
            stop_result = await asyncio.wait_for(stub.stop(stop_request), 1)

        # Assert - Verify behavior through public API
        assert isinstance(stop_result, pb.worker.Void)
        assert grpc_servicer.stopping.is_set()
        assert grpc_servicer.stopped.is_set()
        # Verify proxy_pool.clear() was called
        mock_worker_proxy_cache.clear.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_stop_while_idle")
    async def test_stop_while_stopping(self, service_fixture, mock_worker_proxy_cache):
        """Test :class:`WorkerService` stop is idempotent.

        Given:
            A :class:`WorkerService` that is already stopping
        When:
            stop RPC is called again
        Then:
            It should return immediately without error
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Initiate stop (service enters stopping state)
            stop_task = asyncio.ensure_future(
                stub.stop(pb.worker.StopRequest(timeout=5))
            )

            # Wait for service to enter stopping state
            await asyncio.wait_for(service.stopping.wait(), 1)
            assert not service.stopped.is_set()

            # Act - Call stop again while already stopping
            result = await stub.stop(pb.worker.StopRequest(timeout=1))

            # Assert - Verify behavior
            assert isinstance(result, pb.worker.Void)
            assert service.stopping.is_set()

            # Cleanup - let the original task complete so first stop can finish
            event.set()
            await stop_task

            # Verify service is now fully stopped
            assert service.stopped.is_set()

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_stop_while_idle")
    async def test_stop_while_stopped(self, service_fixture, mock_worker_proxy_cache):
        """Test :class:`WorkerService` stop when already stopped.

        Given:
            A :class:`WorkerService` that is already stopped
        When:
            stop RPC is called again
        Then:
            It should return immediately without error
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Complete the task and stop the service
            event.set()
            await stub.stop(pb.worker.StopRequest(timeout=5))

            # Verify service is fully stopped
            assert service.stopping.is_set()
            assert service.stopped.is_set()

            # Act - Call stop again while already stopped
            result = await stub.stop(pb.worker.StopRequest(timeout=1))

            # Assert - Verify behavior
            assert isinstance(result, pb.worker.Void)
            assert service.stopping.is_set()
            assert service.stopped.is_set()

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_dispatch_task_that_returns")
    async def test_dispatch_async_generator_task(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with async generator yields multiple results.

        Given:
            A service receiving task with async generator callable
        When:
            dispatch() is called and iterated
        Then:
            Yields acknowledgment, then multiple result responses from generator in order
        """

        # Arrange
        async def test_generator():
            for i in range(3):
                yield f"gen_value_{i}"

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        wool_task = WorkTask(
            id=uuid4(),
            callable=test_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = wool_task.to_protobuf()

        emit_spy = mocker.spy(WorkTaskEvent, "emit")

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch(request)
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 4  # 1 ack + 3 results
        assert responses[0].HasField("ack")

        for i, response in enumerate(responses[1:]):
            assert response.HasField("result")
            result = cloudpickle.loads(response.result.dump)
            assert result == f"gen_value_{i}"

        # Verify task-scheduled event was emitted
        emit_spy.assert_called()
        event_calls = [call[0][0] for call in emit_spy.call_args_list]
        scheduled_events = [e for e in event_calls if e.type == "task-scheduled"]
        assert len(scheduled_events) == 1

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_dispatch_task_that_returns")
    async def test_dispatch_async_generator_raises_during_iteration(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with async generator that raises yields exception.

        Given:
            A service with async generator task that raises during iteration
        When:
            dispatch() is called and iterated
        Then:
            Yields ack, then exception response containing the raised exception
        """

        # Arrange
        async def failing_generator():
            yield "first_value"
            raise ValueError("Generator error")

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        wool_task = WorkTask(
            id=uuid4(),
            callable=failing_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = wool_task.to_protobuf()

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch(request)
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 3  # 1 ack + 1 result + 1 exception
        assert responses[0].HasField("ack")
        assert responses[1].HasField("result")
        assert cloudpickle.loads(responses[1].result.dump) == "first_value"
        assert responses[2].HasField("exception")
        exception = cloudpickle.loads(responses[2].exception.dump)
        assert isinstance(exception, ValueError)
        assert str(exception) == "Generator error"

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_dispatch_task_that_returns")
    async def test_dispatch_async_generator_completes_normally(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with async generator completes cleanly.

        Given:
            A service with async generator task
        When:
            dispatch() is called and fully consumed
        Then:
            Yields acknowledgment, all results, then stream ends cleanly
        """

        # Arrange
        async def test_generator():
            for i in range(2):
                yield i

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        wool_task = WorkTask(
            id=uuid4(),
            callable=test_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = wool_task.to_protobuf()

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch(request)
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 3  # 1 ack + 2 results
        assert responses[0].HasField("ack")
        assert responses[1].HasField("result")
        assert cloudpickle.loads(responses[1].result.dump) == 0
        assert responses[2].HasField("result")
        assert cloudpickle.loads(responses[2].result.dump) == 1

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_dispatch_task_that_returns")
    async def test_dispatch_async_generator_empty(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with empty async generator.

        Given:
            A service with async generator task yielding zero values
        When:
            dispatch() is called and iterated
        Then:
            Yields acknowledgment, then immediately ends without result responses
        """

        # Arrange
        async def empty_generator():
            return
            yield  # unreachable, but makes it a generator

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        wool_task = WorkTask(
            id=uuid4(),
            callable=empty_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = wool_task.to_protobuf()

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch(request)
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1  # only ack
        assert responses[0].HasField("ack")

    @pytest.mark.asyncio
    @pytest.mark.dependency("test_dispatch_task_that_returns")
    async def test_dispatch_coroutine_for_comparison(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with coroutine task for comparison with async generator.

        Given:
            A service receiving task with coroutine callable
        When:
            dispatch() is called and iterated
        Then:
            Yields acknowledgment, then single result response
        """

        # Arrange
        async def test_coroutine():
            return "coroutine_result"

        mock_proxy = mocker.MagicMock()
        mock_proxy.id = "test-proxy-id"

        wool_task = WorkTask(
            id=uuid4(),
            callable=test_coroutine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = wool_task.to_protobuf()

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch(request)
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 2  # 1 ack + 1 result
        assert responses[0].HasField("ack")
        assert responses[1].HasField("result")
        result = cloudpickle.loads(responses[1].result.dump)
        assert result == "coroutine_result"
