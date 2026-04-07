import asyncio
import concurrent.futures
import threading
from contextlib import asynccontextmanager
from typing import Final
from uuid import uuid4

import cloudpickle
import grpc
import pytest
from grpc import StatusCode
from hypothesis import HealthCheck
from hypothesis import assume
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

import wool
from wool import protocol
from wool.protocol import WorkerStub
from wool.protocol import add_WorkerServicer_to_server
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.worker.interceptor import VersionInterceptor
from wool.runtime.worker.service import WorkerService
from wool.runtime.worker.service import _ReadOnlyEvent

from .conftest import PicklableMock


@pytest.fixture(scope="function")
def grpc_interceptors():
    return [VersionInterceptor()]


@pytest.fixture(scope="function")
def grpc_add_to_server():
    return add_WorkerServicer_to_server


@pytest.fixture(scope="function")
def grpc_servicer():
    service = WorkerService()
    yield service
    for entry in service._loop_pool._cache.values():
        WorkerService._destroy_worker_loop(entry.obj)


@pytest.fixture(scope="function")
def grpc_stub_cls():
    return WorkerStub


# Global event for controlling test task execution.
# Uses threading.Event (not asyncio.Event) because the controllable
# task runs on the worker loop while the test sets the event from
# the main loop. threading.Event is thread-safe across event loops.
_control_event: threading.Event | None = None


def _get_control_event() -> threading.Event:
    """Get the global control event for test tasks."""
    assert _control_event
    return _control_event


async def _controllable_task():
    """Task function that waits on the global control event."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _get_control_event().wait)
    return "task_completed"


@pytest.fixture
@asynccontextmanager
async def service_fixture(mocker: MockerFixture, grpc_aio_stub):
    """Provides a WorkerService with a dispatched task that waits on an event.

    This fixture returns a tuple of (service, event, stub) where:
    - service: WorkerService instance with a controllable task dispatched
    - event: threading.Event that controls task completion
    - stub: gRPC stub for interacting with the service

    The task remains active until the event is set. On cleanup, if the event
    is still unset, it will be set and the service will be stopped.
    """
    global _control_event

    service = WorkerService()
    _control_event = threading.Event()

    mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

    wool_task = Task(
        id=uuid4(),
        callable=_controllable_task,
        args=(),
        kwargs={},
        proxy=mock_proxy,
    )

    request = protocol.Request(task=wool_task.to_protobuf())

    # Start the service and dispatch the task
    async with grpc_aio_stub(servicer=service) as stub:
        stream = stub.dispatch()
        await stream.write(request)
        await stream.done_writing()

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
                stop_request = protocol.StopRequest(timeout=1)
                await stub.stop(stop_request)
            _control_event = None


class TestReadOnlyEvent:
    """Tests for :class:`_ReadOnlyEvent` wrapper."""

    unset_event: Final = asyncio.Event()
    set_event: Final = asyncio.Event()
    set_event.set()

    @pytest.mark.parametrize("event", (unset_event, set_event))
    def test___init___wraps_asyncio_event(self, event):
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

    def test_is_set_delegates_to_underlying_event(self, mocker: MockerFixture):
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
    async def test_wait_delegates_to_underlying_event(self, mocker: MockerFixture):
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
    def test___init___with_defaults(self):
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
            It should return the task result
        """

        # Arrange
        async def sample_task():
            return "test_result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, reponse = responses
        assert ack.HasField("ack")
        assert reponse.HasField("result")
        assert cloudpickle.loads(reponse.result.dump) == "test_result"

    @pytest.mark.asyncio
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
            It should return the task exception as the result
        """

        # Arrange
        async def failing_task():
            raise ValueError("test_exception")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=failing_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("exception")
        exception = cloudpickle.loads(response.exception.dump)
        assert isinstance(exception, ValueError)
        assert str(exception) == "test_exception"

    @pytest.mark.asyncio
    async def test_dispatch_while_stopping(
        self, service_fixture, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService` dispatch aborts when stopping.

        Given:
            A :class:`WorkerService` with an active task, transitioning to stopping state
        When:
            stop is called and another dispatch RPC is attempted
        Then:
            It should abort the new dispatch with UNAVAILABLE status
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Initiate stop (service enters stopping state)
            # Use wait=0 to immediately cancel tasks, but we'll check before tasks finish
            stop_task = asyncio.ensure_future(stub.stop(protocol.StopRequest(timeout=5)))

            await asyncio.wait_for(service.stopping.wait(), 1)
            assert not service.stopped.is_set()

            # Create a new task to dispatch
            async def another_task():
                return "should_not_execute"

            mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id-2")

            wool_task = Task(
                id=uuid4(),
                callable=another_task,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )

            request = protocol.Request(task=wool_task.to_protobuf())

            # Act & assert
            with pytest.raises(grpc.RpcError) as exc_info:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                async for _ in stream:
                    pass

            assert exc_info.value.code() == StatusCode.UNAVAILABLE

            # Cleanup: let the original task complete so stop can finish
            event.set()
            await stop_task

    @pytest.mark.asyncio
    async def test_dispatch_while_stopped(self, service_fixture, mocker: MockerFixture):
        """Test :class:`WorkerService` dispatch aborts when stopped.

        Given:
            A :class:`WorkerService` that has been stopped
        When:
            dispatch RPC is called with a task request
        Then:
            It should abort with UNAVAILABLE status
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Complete the task and stop the service
            event.set()
            await stub.stop(protocol.StopRequest(timeout=5))

            # Assert service is stopped
            assert service.stopping.is_set()
            assert service.stopped.is_set()

            # Create a new task to dispatch
            async def another_task():
                return "should_not_execute"

            mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id-2")

            wool_task = Task(
                id=uuid4(),
                callable=another_task,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )

            request = protocol.Request(task=wool_task.to_protobuf())

            # Act & assert
            with pytest.raises(grpc.RpcError) as exc_info:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                async for _ in stream:
                    pass

            assert exc_info.value.code() == StatusCode.UNAVAILABLE

    @pytest.mark.asyncio
    async def test_dispatch_non_async_callable(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch rejects non-async callable.

        Given:
            A gRPC :class:`WorkerService` that is not stopping or stopped
        When:
            Dispatch RPC is called with a task whose callable is a synchronous function
        Then:
            It should abort with an error status
        """

        # Arrange
        def sync_function():
            return "sync_result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sync_function,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act & assert
        async with grpc_aio_stub() as stub:
            with pytest.raises(grpc.RpcError) as exc_info:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                async for _ in stream:
                    pass

            assert exc_info.value.code() == StatusCode.UNKNOWN

    @pytest.mark.asyncio
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

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=long_running_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            # Start the task dispatch stream
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            async for response in stream:
                assert response.HasField("ack")
                break

            # Stop with 0 second timeout in order to cancel tasks immediately
            stop_request = protocol.StopRequest(timeout=0)
            stop_result = await stub.stop(stop_request)

            # Assert
            async for response in stream:
                assert response.HasField("exception")
                exception = cloudpickle.loads(response.exception.dump)
                assert isinstance(exception, asyncio.CancelledError)
                break

            assert isinstance(stop_result, protocol.Void)
            assert grpc_servicer.stopping.is_set()
            assert grpc_servicer.stopped.is_set()
            mock_worker_proxy_cache.clear.assert_called_once()

    @pytest.mark.asyncio
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

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=quick_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            # Start the task dispatch stream
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            async for response in stream:
                assert response.HasField("ack")
                break

            # Stop with 5 second timeout to allow tasks to complete
            stop_request = protocol.StopRequest(timeout=5)
            stop_result = await stub.stop(stop_request)

            # Assert
            async for response in stream:
                assert response.HasField("result")
                result = cloudpickle.loads(response.result.dump)
                assert result == "completed"
                break

            assert isinstance(stop_result, protocol.Void)
            assert grpc_servicer.stopping.is_set()
            assert grpc_servicer.stopped.is_set()
            mock_worker_proxy_cache.clear.assert_called_once()

    @pytest.mark.asyncio
    async def test_dispatch_task_that_self_cancels(
        self,
        grpc_aio_stub,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
    ):
        """Test :class:`WorkerService` dispatch handles task that cancels itself.

        Given:
            A gRPC :class:`WorkerService` that is not stopping or stopped
        When:
            Dispatch RPC is called with a task that cancels itself on the worker loop
        Then:
            It should return an exception response containing a CancelledError
        """

        # Arrange
        async def self_cancelling_task():
            asyncio.current_task().cancel()
            await asyncio.sleep(0)
            return "should_not_return"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=self_cancelling_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("exception")
        exception = cloudpickle.loads(response.exception.dump)
        assert isinstance(exception, asyncio.CancelledError)

    @pytest.mark.asyncio
    async def test_stop_timeout_then_cancel(
        self, service_fixture, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` stop cancels tasks after timeout expires.

        Given:
            A :class:`WorkerService` with an active task that outlasts the stop timeout
        When:
            stop RPC is called with a small positive timeout
        Then:
            It should wait for the timeout, then cancel remaining tasks and signal
            stopped state
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Act - stop with a very short timeout; the controllable task
            # won't complete because the event is never set in time
            stop_request = protocol.StopRequest(timeout=0.05)
            stop_result = await stub.stop(stop_request)

            # Assert
            assert isinstance(stop_result, protocol.Void)
            assert service.stopping.is_set()
            assert service.stopped.is_set()

    @pytest.mark.asyncio
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
        stop_request = protocol.StopRequest(timeout=10)

        # Act
        async with grpc_aio_stub() as stub:
            stop_result = await asyncio.wait_for(stub.stop(stop_request), 1)

        # Assert - Verify behavior through public API
        assert isinstance(stop_result, protocol.Void)
        assert grpc_servicer.stopping.is_set()
        assert grpc_servicer.stopped.is_set()
        # Assert proxy_pool.clear() was called
        mock_worker_proxy_cache.clear.assert_called_once()

    @pytest.mark.asyncio
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
            stop_task = asyncio.ensure_future(stub.stop(protocol.StopRequest(timeout=5)))

            # Wait for service to enter stopping state
            await asyncio.wait_for(service.stopping.wait(), 1)
            assert not service.stopped.is_set()

            # Act - Call stop again while already stopping
            result = await stub.stop(protocol.StopRequest(timeout=1))

            # Assert - Verify behavior
            assert isinstance(result, protocol.Void)
            assert service.stopping.is_set()

            # Cleanup - let the original task complete so first stop can finish
            event.set()
            await stop_task

            # Assert service is now fully stopped
            assert service.stopped.is_set()

    @pytest.mark.asyncio
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
            await stub.stop(protocol.StopRequest(timeout=5))

            # Assert service is fully stopped
            assert service.stopping.is_set()
            assert service.stopped.is_set()

            # Act - Call stop again while already stopped
            result = await stub.stop(protocol.StopRequest(timeout=1))

            # Assert - Verify behavior
            assert isinstance(result, protocol.Void)
            assert service.stopping.is_set()
            assert service.stopped.is_set()

    @pytest.mark.asyncio
    async def test_stop_negative_timeout_waits_indefinitely(
        self, service_fixture, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` stop with negative timeout waits indefinitely.

        Given:
            A :class:`WorkerService` with an active task
        When:
            stop RPC is called with a negative timeout
        Then:
            It should treat the negative timeout as an indefinite wait and stop
            only after the task completes
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Initiate stop with negative timeout (treated as indefinite wait)
            stop_task = asyncio.ensure_future(
                stub.stop(protocol.StopRequest(timeout=-1))
            )

            # Wait for service to enter stopping state
            await asyncio.wait_for(service.stopping.wait(), 1)
            assert not service.stopped.is_set()

            # Let the task complete so stop can finish
            event.set()

            # Act & assert
            stop_result = await asyncio.wait_for(stop_task, 5)
            assert isinstance(stop_result, protocol.Void)
            assert service.stopped.is_set()

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_task(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with async generator yields multiple results.

        Given:
            A service receiving task with async generator callable
        When:
            The client sends Request(next=Void()) for each yield
        Then:
            Yields acknowledgment, then multiple result responses from generator in order
        """

        # Arrange
        async def test_generator():
            for i in range(3):
                yield f"gen_value_{i}"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=test_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Send next requests and collect results
            results = []
            for _ in range(3):
                await stream.write(next_request)
                response = await anext(aiter(stream))
                assert response.HasField("result")
                results.append(cloudpickle.loads(response.result.dump))

            # Send one more next to exhaust the generator
            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        assert results == ["gen_value_0", "gen_value_1", "gen_value_2"]
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_raises_during_iteration(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with async generator that raises yields exception.

        Given:
            A service with async generator task that raises during iteration
        When:
            The client sends next requests and the generator raises
        Then:
            Yields ack, the first result, then exception response containing the raised exception
        """

        # Arrange
        async def failing_generator():
            yield "first_value"
            raise ValueError("Generator error")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=failing_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # First next: yields "first_value"
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "first_value"

            # Second next: generator raises
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("exception")
            exception = cloudpickle.loads(response.exception.dump)
            assert isinstance(exception, ValueError)
            assert str(exception) == "Generator error"

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_completes_normally(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with async generator completes cleanly.

        Given:
            A service with async generator task
        When:
            The client sends next requests until exhaustion
        Then:
            Yields acknowledgment, all results, then stream ends cleanly
        """

        # Arrange
        async def test_generator():
            for i in range(2):
                yield i

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=test_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Read first result
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == 0

            # Read second result
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == 1

            # Exhaust the generator
            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]
            assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_empty(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with empty async generator.

        Given:
            A service with async generator task yielding zero values
        When:
            The client sends a next request
        Then:
            Yields acknowledgment, then stream ends without result responses
        """

        # Arrange
        async def empty_generator():
            return
            yield  # unreachable, but makes it a generator

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=empty_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Send next - generator is empty, should end
            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        assert len(remaining) == 0

    @pytest.mark.asyncio
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

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=test_coroutine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 2  # 1 ack + 1 result
        assert responses[0].HasField("ack")
        assert responses[1].HasField("result")
        result = cloudpickle.loads(responses[1].result.dump)
        assert result == "coroutine_result"

    @pytest.mark.asyncio
    async def test_stop_cancels_async_generator_task(
        self,
        grpc_aio_stub,
        grpc_servicer,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
    ):
        """Test :class:`WorkerService` stop cancels an active async generator task.

        Given:
            A running :class:`WorkerService` with an active async generator task
        When:
            stop RPC is called with a timeout of 0
        Then:
            It should cancel the async generator and signal stopped state
        """

        # Arrange
        async def blocking_generator():
            yield "first_value"
            await asyncio.sleep(100)
            yield "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=blocking_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            async for response in stream:
                assert response.HasField("ack")
                break

            # Send next to advance generator and read first result
            await stream.write(next_request)
            async for response in stream:
                assert response.HasField("result")
                assert cloudpickle.loads(response.result.dump) == "first_value"
                break

            # Stop immediately to cancel the generator
            stop_request = protocol.StopRequest(timeout=0)
            stop_result = await stub.stop(stop_request)

            # Assert
            assert isinstance(stop_result, protocol.Void)
            assert grpc_servicer.stopping.is_set()
            assert grpc_servicer.stopped.is_set()

    @pytest.mark.asyncio
    async def test_run_on_worker_done_callback_tolerates_already_cancelled_future(
        self,
        mocker: MockerFixture,
    ):
        """Done callback must not raise on already-done future.

        Given:
            A task dispatched to the worker loop, where the bridging
            :class:`~concurrent.futures.Future` is cancelled before the
            worker task's done callback fires
        When:
            The worker task completes normally and fires its done
            callback on the already-cancelled future
        Then:
            The callback exits early without raising
            :exc:`~concurrent.futures.InvalidStateError`.
        """
        service = WorkerService()
        invalid_state_errors: list[Exception] = []

        # Spy on set_result and set_exception to detect
        # InvalidStateError that would occur without the
        # future.done() guard in the _done callback.
        _og_set_result = concurrent.futures.Future.set_result
        _og_set_exception = concurrent.futures.Future.set_exception

        def detecting_set_result(self, result):
            try:
                return _og_set_result(self, result)
            except concurrent.futures.InvalidStateError as e:
                invalid_state_errors.append(e)

        def detecting_set_exception(self, exception):
            try:
                return _og_set_exception(self, exception)
            except concurrent.futures.InvalidStateError as e:
                invalid_state_errors.append(e)

        mocker.patch.object(
            concurrent.futures.Future, "set_result", detecting_set_result
        )
        mocker.patch.object(
            concurrent.futures.Future, "set_exception", detecting_set_exception
        )

        # Patch wrap_future to cancel the bridging future immediately
        # after wrapping. This deterministically simulates the race
        # where Task.cancel() propagates through wrap_future's
        # _chain_future to cancel the concurrent.futures.Future
        # before the worker task's done callback fires.
        _og_wrap_future = asyncio.wrap_future

        def cancelling_wrap_future(future, *, loop=None):
            asyncio_future = _og_wrap_future(future, loop=loop)
            future.cancel()
            return asyncio_future

        mocker.patch("asyncio.wrap_future", cancelling_wrap_future)

        async def sample_task():
            return "completed"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        work_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        with pytest.raises(asyncio.CancelledError):
            await service._run_on_worker(work_task)

        # Allow the worker task's done callback to fire on the
        # worker loop.
        await asyncio.sleep(0.2)

        assert not invalid_state_errors, (
            "InvalidStateError raised in _done callback — "
            "the future.done() guard may have been removed"
        )

        # Cleanup worker loops
        for entry in service._loop_pool._cache.values():
            WorkerService._destroy_worker_loop(entry.obj)

    @pytest.mark.asyncio
    async def test_dispatch_with_version_in_ack(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch returns Ack with protocol version.

        Given:
            A running WorkerService
        When:
            A task is dispatched with a compatible version
        Then:
            The Ack response contains the protocol version.
        """

        # Arrange
        async def sample_task():
            return "test_result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack_response = responses[0]
        assert ack_response.HasField("ack")
        assert ack_response.ack.version == protocol.__version__

    @pytest.mark.asyncio
    async def test_dispatch_with_empty_client_version(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch rejects tasks with empty version field.

        Given:
            A running WorkerService with the version interceptor
        When:
            A task with empty version field is dispatched
        Then:
            The worker responds with a Nack citing unparseable
            version.
        """

        # Arrange
        async def sample_task():
            return "test_result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        request.task.ClearField("version")

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        assert responses[0].HasField("nack")
        assert "Unparseable version" in responses[0].nack.reason

    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        local_major=st.integers(min_value=0, max_value=100),
        client_major=st.integers(min_value=0, max_value=100),
    )
    @pytest.mark.asyncio
    async def test_dispatch_with_incompatible_major_version(
        self,
        grpc_aio_stub,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
        local_major,
        client_major,
    ):
        """Test dispatch yields Nack for incompatible major version.

        Given:
            Two semver-like version strings with different major
            versions
        When:
            A task is dispatched with the client version through the
            version interceptor
        Then:
            The dispatch yields a Nack with a reason citing
            incompatible, empty, or unparseable version.
        """
        assume(local_major != client_major)

        # Arrange
        mocker.patch.object(protocol, "__version__", f"{local_major}.0.0")

        async def sample_task():
            return "should_not_execute"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        # Override version field to simulate incompatible client
        request.task.version = f"{client_major}.0.0"

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        assert responses[0].HasField("nack")
        assert "Incompatible version" in responses[0].nack.reason

    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        major=st.integers(min_value=0, max_value=100),
        local_minor=st.integers(min_value=0, max_value=99),
        client_minor=st.integers(min_value=1, max_value=100),
    )
    @pytest.mark.asyncio
    async def test_dispatch_with_newer_client_same_major(
        self,
        grpc_aio_stub,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
        major,
        local_minor,
        client_minor,
    ):
        """Test dispatch yields Nack when client is newer than worker.

        Given:
            A worker with version X.a.0 and a client with version
            X.b.0 where b > a (same major, no forward compatibility)
        When:
            A task is dispatched through the version interceptor
        Then:
            The dispatch yields a Nack with a reason citing
            incompatible version.
        """
        assume(client_minor > local_minor)

        # Arrange
        mocker.patch.object(protocol, "__version__", f"{major}.{local_minor}.0")

        async def sample_task():
            return "should_not_execute"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        request.task.version = f"{major}.{client_minor}.0"

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        assert responses[0].HasField("nack")
        assert "Incompatible version" in responses[0].nack.reason

    @pytest.mark.asyncio
    async def test_dispatch_with_unparseable_client_version(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch rejects tasks with unparseable version.

        Given:
            A running WorkerService with the version interceptor
        When:
            A task with unparseable version string is dispatched
        Then:
            The worker responds with a Nack citing unparseable
            version.
        """

        # Arrange
        async def sample_task():
            return "should_not_execute"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        request.task.version = "not-a-version"

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        assert responses[0].HasField("nack")
        assert "Unparseable version" in responses[0].nack.reason

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_with_send(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() forwards send requests into async generator via asend().

        Given:
            A service receiving an async generator task
        When:
            The client sends Request(next=...) then Request(send=...)
            frames after the initial Task
        Then:
            The generator receives the sent values via asend() and
            yields responses driven by those values.
        """

        # Arrange
        async def echo_generator():
            value = yield "ready"
            while value is not None:
                value = yield f"echo:{value}"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=echo_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Send next to get first yield ("ready")
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "ready"

            # Send "hello" and read response
            msg = protocol.Request(
                send=protocol.Message(dump=cloudpickle.dumps("hello"))
            )
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "echo:hello"

            # Send "world" and read response
            msg = protocol.Request(
                send=protocol.Message(dump=cloudpickle.dumps("world"))
            )
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "echo:world"

            # Send None to terminate the generator
            msg = protocol.Request(send=protocol.Message(dump=cloudpickle.dumps(None)))
            await stream.write(msg)
            await stream.done_writing()

            # Stream should end cleanly
            remaining = [r async for r in stream]
            assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_send_then_close(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() handles client closing stream after sends.

        Given:
            A service receiving an async generator task with send support
        When:
            The client sends values then closes the write side
        Then:
            The server does not advance the generator further after
            the stream closes.
        """

        # Arrange
        async def counting_generator():
            count = 0
            while count < 5:
                received = yield count
                if received is not None:
                    count = received
                else:
                    count += 1

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=counting_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Send next to get first yield (0)
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == 0

            # Send 2 to jump the counter
            msg = protocol.Request(send=protocol.Message(dump=cloudpickle.dumps(2)))
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == 2

            # Close the write side; server should not advance further
            await stream.done_writing()

            # Stream should end cleanly (no more responses)
            remaining = [r async for r in stream]
            assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_pull_only_async_generator(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with a pull-only async generator (no send type).

        Given:
            A service receiving a pull-only async generator task
        When:
            The client sends Request(next=Void()) for each yield
        Then:
            Each next request produces the next yielded value in order
        """

        # Arrange
        async def pull_only():
            yield "alpha"
            yield "beta"
            yield "gamma"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=pull_only,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Pull each value
            results = []
            for _ in range(3):
                await stream.write(next_request)
                response = await anext(aiter(stream))
                assert response.HasField("result")
                results.append(cloudpickle.loads(response.result.dump))

            # Exhaust
            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        assert results == ["alpha", "beta", "gamma"]
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_pull_only_async_generator_partial_consumption(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with partial consumption of a pull-only async generator.

        Given:
            A service with a pull-only async generator yielding 5 values
        When:
            The client sends 2 next requests then closes the stream
        Then:
            Only 2 result responses are received and the generator is
            cleaned up
        """

        # Arrange
        async def five_values():
            for i in range(5):
                yield i

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=five_values,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Pull only 2 values
            results = []
            for _ in range(2):
                await stream.write(next_request)
                response = await anext(aiter(stream))
                assert response.HasField("result")
                results.append(cloudpickle.loads(response.result.dump))

            # Close the stream without exhausting
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        assert results == [0, 1]
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_interleaved_next_and_send(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with interleaved next and send requests.

        Given:
            A service with an async generator that accepts send values
        When:
            The client alternates between next and send requests
        Then:
            next advances with asend(None), send advances with the
            provided value
        """

        # Arrange
        async def accumulator():
            total = 0
            while True:
                received = yield total
                if received is not None:
                    total += received
                else:
                    total += 1

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=accumulator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # next -> yields 0 (initial total)
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert cloudpickle.loads(response.result.dump) == 0

            # send 10 -> yields 10
            msg = protocol.Request(send=protocol.Message(dump=cloudpickle.dumps(10)))
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert cloudpickle.loads(response.result.dump) == 10

            # next (asend(None)) -> yields 11 (total + 1)
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert cloudpickle.loads(response.result.dump) == 11

            # send 5 -> yields 16
            msg = protocol.Request(send=protocol.Message(dump=cloudpickle.dumps(5)))
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert cloudpickle.loads(response.result.dump) == 16

            await stream.done_writing()

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_throw_terminates(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with a throw request that terminates the generator.

        Given:
            A service with an async generator task
        When:
            The client sends a throw request with a ValueError
        Then:
            The generator receives the exception and the server yields
            an exception response
        """

        # Arrange
        async def simple_generator():
            yield "first"
            yield "second"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=simple_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Get first yield
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "first"

            # Throw ValueError into the generator
            throw_request = protocol.Request(
                throw=protocol.Message(dump=cloudpickle.dumps(ValueError("injected")))
            )
            await stream.write(throw_request)
            response = await anext(aiter(stream))
            assert response.HasField("exception")
            exception = cloudpickle.loads(response.exception.dump)
            assert isinstance(exception, ValueError)
            assert str(exception) == "injected"

    @pytest.mark.asyncio
    async def test_dispatch_streaming_with_proxy_and_dispatch_context(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() sets proxy and do_dispatch(False) for streaming.

        Given:
            A proxy pool configured in wool.__proxy_pool__ and an
            async generator task that reports do_dispatch() and
            wool.__proxy__ via yielded values
        When:
            dispatch() is called and the generator is advanced via
            next requests
        Then:
            It should execute with do_dispatch set to False during
            advancement, and wool.__proxy__ set to the pool's proxy.
        """

        # Arrange — generator yields its observed context
        async def capturing_generator():
            from wool.runtime.routine.task import do_dispatch as _dd

            yield {
                "do_dispatch": _dd(),
                "has_proxy": wool.__proxy__.get() is not None,
            }

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=capturing_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            result = cloudpickle.loads(response.result.dump)

            await stream.write(next_request)
            await stream.done_writing()
            [r async for r in stream]

        # Assert
        assert result["do_dispatch"] is False
        assert result["has_proxy"] is True

    @pytest.mark.asyncio
    async def test_dispatch_streaming_without_proxy_pool(
        self, grpc_aio_stub, mocker: MockerFixture
    ):
        """Test dispatch() streaming works without a proxy pool.

        Given:
            No proxy pool configured in wool.__proxy_pool__ (default
            None) and an async generator task
        When:
            dispatch() is called and the generator is advanced via
            next requests
        Then:
            It should execute normally without error, with
            wool.__proxy__ remaining None.
        """

        # Arrange — generator yields its observed proxy state
        async def capturing_generator():
            yield {"has_proxy": wool.__proxy__.get() is not None}

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=capturing_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            result = cloudpickle.loads(response.result.dump)

            await stream.write(next_request)
            await stream.done_writing()
            [r async for r in stream]

        # Assert
        assert result["has_proxy"] is False

    @pytest.mark.asyncio
    async def test_dispatch_streaming_proxy_cleanup_on_error(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() cleans up proxy context on generator error.

        Given:
            A proxy pool configured in wool.__proxy_pool__ and an
            async generator task that raises after one yield
        When:
            dispatch() is called and the generator raises
        Then:
            It should return the exception and clean up the proxy
            context (call __aexit__).
        """

        # Arrange
        async def failing_generator():
            yield "before_error"
            raise ValueError("generator_error")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=failing_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # First next: yields "before_error"
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "before_error"

            # Second next: generator raises
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("exception")
            exception = cloudpickle.loads(response.exception.dump)
            assert isinstance(exception, ValueError)
            assert str(exception) == "generator_error"

            await stream.done_writing()
            [r async for r in stream]

        # Assert — proxy context manager was cleaned up
        mock_worker_proxy_cache.get.return_value.__aexit__.assert_called()

    @pytest.mark.asyncio
    async def test_dispatch_streaming_asend_with_proxy_context(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() forwards asend() within do_dispatch(False) context.

        Given:
            A proxy pool configured in wool.__proxy_pool__ and an
            async generator task that echoes sent values and reports
            do_dispatch state
        When:
            dispatch() is called with send requests containing
            non-None payloads
        Then:
            It should forward each sent value to the generator via
            asend() with do_dispatch set to False, and yield back
            the generator's responses.
        """

        # Arrange — generator echoes sent values and reports dispatch flag
        async def echo_with_capture():
            from wool.runtime.routine.task import do_dispatch as _dd

            value = yield "ready"
            while value is not None:
                value = yield {"echo": value, "do_dispatch": _dd()}

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=echo_with_capture,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # First next to get "ready"
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "ready"

            # Send "hello"
            msg = protocol.Request(
                send=protocol.Message(dump=cloudpickle.dumps("hello"))
            )
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            result1 = cloudpickle.loads(response.result.dump)

            # Send "world"
            msg = protocol.Request(
                send=protocol.Message(dump=cloudpickle.dumps("world"))
            )
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            result2 = cloudpickle.loads(response.result.dump)

            # Send None to terminate
            msg = protocol.Request(send=protocol.Message(dump=cloudpickle.dumps(None)))
            await stream.write(msg)
            await stream.done_writing()
            [r async for r in stream]

        # Assert
        assert result1 == {"echo": "hello", "do_dispatch": False}
        assert result2 == {"echo": "world", "do_dispatch": False}

    @pytest.mark.asyncio
    async def test_dispatch_streaming_athrow_with_proxy_context(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() forwards athrow() within do_dispatch(False) context.

        Given:
            A proxy pool configured in wool.__proxy_pool__ and an
            async generator task that catches a specific exception
        When:
            dispatch() is called with a throw request
        Then:
            It should forward the exception via athrow() with
            do_dispatch set to False, and yield the recovery value
            along with the observed context.
        """

        # Arrange — generator catches ValueError and reports context
        async def resilient_generator():
            from wool.runtime.routine.task import do_dispatch as _dd

            try:
                yield "waiting"
            except ValueError:
                yield {
                    "do_dispatch": _dd(),
                    "has_proxy": wool.__proxy__.get() is not None,
                }

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=resilient_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Get first yield
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "waiting"

            # Throw ValueError
            throw_request = protocol.Request(
                throw=protocol.Message(dump=cloudpickle.dumps(ValueError("test")))
            )
            await stream.write(throw_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            result = cloudpickle.loads(response.result.dump)

            # Exhaust generator
            await stream.write(next_request)
            await stream.done_writing()
            [r async for r in stream]

        # Assert
        assert result["do_dispatch"] is False
        assert result["has_proxy"] is True

    def test___init___with_backpressure_hook(self):
        """Test WorkerService initialization with a backpressure hook.

        Given:
            A callable backpressure hook
        When:
            WorkerService is instantiated with backpressure=hook
        Then:
            It should initialize successfully with stopping and stopped events unset
        """

        # Arrange
        def hook(ctx):
            return False

        # Act
        service = WorkerService(backpressure=hook)

        # Assert
        assert not service.stopping.is_set()
        assert not service.stopped.is_set()

    @pytest.mark.asyncio
    async def test_dispatch_with_sync_backpressure_accepting(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch succeeds when sync backpressure hook returns False.

        Given:
            A :class:`WorkerService` with a sync backpressure hook that returns False
        When:
            Dispatch RPC is called
        Then:
            It should accept the task and return the result normally
        """

        # Arrange
        async def sample_task():
            return "accepted"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        def hook(ctx):
            return False

        service = WorkerService(backpressure=hook)

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("result")
        assert cloudpickle.loads(response.result.dump) == "accepted"

    @pytest.mark.asyncio
    async def test_dispatch_with_sync_backpressure_rejecting(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch aborts when sync backpressure hook returns True.

        Given:
            A :class:`WorkerService` with a sync backpressure hook that returns True
        When:
            Dispatch RPC is called
        Then:
            It should reject the task with RESOURCE_EXHAUSTED status
        """

        # Arrange
        async def sample_task():
            return "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        def hook(ctx):
            return True

        service = WorkerService(backpressure=hook)

        # Act & assert
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            with pytest.raises(grpc.RpcError) as exc_info:
                async for _ in stream:
                    pass
            assert exc_info.value.code() == StatusCode.RESOURCE_EXHAUSTED

    @pytest.mark.asyncio
    async def test_dispatch_with_async_backpressure_rejecting(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch aborts when async backpressure hook returns True.

        Given:
            A :class:`WorkerService` with an async backpressure hook that returns True
        When:
            Dispatch RPC is called
        Then:
            It should reject the task with RESOURCE_EXHAUSTED status
        """

        # Arrange
        async def sample_task():
            return "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        async def async_hook(ctx):
            return True

        service = WorkerService(backpressure=async_hook)

        # Act & assert
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            with pytest.raises(grpc.RpcError) as exc_info:
                async for _ in stream:
                    pass
            assert exc_info.value.code() == StatusCode.RESOURCE_EXHAUSTED

    @pytest.mark.asyncio
    async def test_dispatch_with_async_backpressure_accepting(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch succeeds when async backpressure hook returns False.

        Given:
            A :class:`WorkerService` with an async backpressure hook that returns False
        When:
            Dispatch RPC is called
        Then:
            It should accept the task and return the result normally
        """

        # Arrange
        async def sample_task():
            return "async_accepted"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        async def async_hook(ctx):
            return False

        service = WorkerService(backpressure=async_hook)

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("result")
        assert cloudpickle.loads(response.result.dump) == "async_accepted"

    @pytest.mark.asyncio
    async def test_dispatch_with_backpressure_receiving_context(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test backpressure hook receives correct context.

        Given:
            A :class:`WorkerService` with a backpressure hook that captures its argument
        When:
            Dispatch RPC is called
        Then:
            It should pass a BackpressureContext with active_task_count and task fields
        """
        # Arrange
        from wool.runtime.worker.service import BackpressureContext

        async def sample_task():
            return "result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        captured = []

        def hook(ctx):
            captured.append(ctx)
            return False

        service = WorkerService(backpressure=hook)

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            [r async for r in stream]

        # Assert
        assert len(captured) == 1
        ctx = captured[0]
        assert isinstance(ctx, BackpressureContext)
        assert ctx.active_task_count == 0
        assert ctx.task.id == wool_task.id

    @pytest.mark.asyncio
    async def test_dispatch_with_backpressure_and_active_tasks(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test backpressure hook sees correct active task count.

        Given:
            A :class:`WorkerService` with one active task already dispatched
        When:
            A second dispatch RPC is called with a backpressure hook
        Then:
            It should see active_task_count == 1
        """
        # Arrange
        global _control_event
        _control_event = threading.Event()

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        first_task = Task(
            id=uuid4(),
            callable=_controllable_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=first_task.to_protobuf())

        async def second_fn():
            return "second"

        second_task = Task(
            id=uuid4(),
            callable=second_fn,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        second_request = protocol.Request(task=second_task.to_protobuf())

        captured_count = []

        def hook(ctx):
            captured_count.append(ctx.active_task_count)
            return False

        service = WorkerService(backpressure=hook)

        # Act
        try:
            async with grpc_aio_stub(servicer=service) as stub:
                # Dispatch first task (blocks on control event)
                stream1 = stub.dispatch()
                await stream1.write(first_request)
                await stream1.done_writing()
                # Wait for ack to confirm first task is tracked
                async for response in stream1:
                    assert response.HasField("ack")
                    break

                # Dispatch second task — hook should see 1 active task
                stream2 = stub.dispatch()
                await stream2.write(second_request)
                await stream2.done_writing()
                [r async for r in stream2]

                # Release first task
                _control_event.set()
                [r async for r in stream1]
        finally:
            if _control_event and not _control_event.is_set():
                _control_event.set()
            _control_event = None

        # Assert — first dispatch sees 0 active, second sees 1
        assert captured_count == [0, 1]

    @pytest.mark.asyncio
    async def test_dispatch_with_backpressure_hook_raising_exception(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch surfaces error when backpressure hook raises.

        Given:
            A :class:`WorkerService` with a backpressure hook that
            raises RuntimeError
        When:
            Dispatch RPC is called
        Then:
            It should propagate the error as a gRPC failure
        """

        # Arrange
        async def sample_task():
            return "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        def hook(ctx):
            raise RuntimeError("hook exploded")

        service = WorkerService(backpressure=hook)

        # Act & assert
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            with pytest.raises(grpc.RpcError):
                async for _ in stream:
                    pass
