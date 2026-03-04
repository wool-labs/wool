import asyncio
import logging
from typing import Callable
from uuid import uuid4

import cloudpickle
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from wool import protocol
from wool.runtime.event import Event
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import TaskEvent
from wool.runtime.routine.task import TaskException
from wool.runtime.routine.task import current_task


class PicklableProxy:
    """A simple picklable proxy for testing serialization."""

    def __init__(self):
        self.id = uuid4()

    async def dispatch(self, *args, **kwargs):
        """Mock dispatch method."""

        async def _stream():
            yield "result"

        return _stream()


class TestTask:
    """Tests for Task class."""

    def test_init_emits_task_created_event(
        self, sample_task, event_spy, clear_event_handlers
    ):
        """Test Task instantiation emits task-created event.

        Given:
            Valid parameters for a Task
        When:
            Task is instantiated
        Then:
            A "task-created" event is emitted
        """
        # Arrange
        wool.TaskEvent._handlers["task-created"] = [event_spy]

        # Act
        task = sample_task()

        # Wait for handler thread to process scheduled handlers
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 1
        event, timestamp, context = event_spy.calls[0]
        assert event.type == "task-created"
        assert event.task.id == task.id
        assert isinstance(timestamp, int)
        assert timestamp > 0

    @pytest.mark.asyncio
    async def test_init_sets_caller_in_nested_context(
        self, sample_task, sample_async_callable, mock_proxy, clear_event_handlers
    ):
        """Test Task sets caller field in nested task context.

        Given:
            A Task is instantiated within another task's context
        When:
            Task is created
        Then:
            The caller field is set to the parent task's ID
        """
        # Arrange
        parent_task = sample_task()

        # Act
        async def create_nested_task():
            # Set parent task in context
            from wool.runtime.routine.task import _current_task

            token = _current_task.set(parent_task)
            try:
                nested_task = sample_task()
                return nested_task
            finally:
                _current_task.reset(token)

        nested_task = await create_nested_task()

        # Assert
        assert nested_task.caller == parent_task.id

    def test_init_caller_none_outside_task_context(
        self, sample_task, clear_event_handlers
    ):
        """Test Task caller field is None outside task context.

        Given:
            A Task is instantiated outside any task context
        When:
            Task is created
        Then:
            The caller field remains None
        """
        # Arrange & Act
        task = sample_task()

        # Assert
        assert task.caller is None

    @pytest.mark.asyncio
    async def test_context_manager_entry(self, sample_task, clear_event_handlers):
        """Test Task context manager entry.

        Given:
            A Task is used as a context manager
        When:
            Context is entered using `with` statement
        Then:
            Context manager provides access to task execution
        """
        # Arrange
        task = sample_task()

        # Act
        with task as run_method:
            # Assert
            assert callable(run_method)
            # We should not test which specific method is returned (private detail)

    @pytest.mark.asyncio
    async def test_context_manager_normal_exit(self, sample_task, clear_event_handlers):
        """Test Task context manager exits normally.

        Given:
            A Task context manager exits normally
        When:
            `with` statement completes without exception
        Then:
            Context exits cleanly without suppressing exceptions
        """
        # Arrange
        task = sample_task()

        # Act & Assert (no exception should be raised)
        with task:
            pass

    @pytest.mark.asyncio
    async def test_context_manager_exception_handling(
        self, sample_task, clear_event_handlers
    ):
        """Test Task context manager exception handling.

        Given:
            A Task context manager and an exception is raised within
            the context
        When:
            Exception occurs inside `with` block
        Then:
            TaskException is created and attached to task, exception
            propagates normally
        """
        # Arrange
        task = sample_task()

        # Act & Assert
        async def run_with_exception():
            with pytest.raises(ValueError, match="test error"):
                with task:
                    # Create a real asyncio task context
                    await asyncio.sleep(0)
                    raise ValueError("test error")

        await run_with_exception()

        # Assert that exception was captured
        assert task.exception is not None
        assert task.exception.type == "ValueError"
        assert any("test error" in line for line in task.exception.traceback)

    @pytest.mark.asyncio
    async def test_from_protobuf_all_fields(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """Test from_protobuf with all fields populated.

        Given:
            A protobuf Task message with all fields populated
        When:
            `from_protobuf` is called
        Then:
            Returns a Task with all fields correctly deserialized
        """
        # Arrange
        task_id = uuid4()
        caller_id = uuid4()
        args = (1, 2, 3)
        kwargs = {"key": "value"}

        pb_task = protocol.task.Task(
            version="0.1.0",
            id=str(task_id),
            callable=cloudpickle.dumps(sample_async_callable),
            args=cloudpickle.dumps(args),
            kwargs=cloudpickle.dumps(kwargs),
            caller=str(caller_id),
            proxy=cloudpickle.dumps(picklable_proxy),
            proxy_id=str(picklable_proxy.id),
            timeout=30,
            tag="test_tag",
        )

        # Act
        task = Task.from_protobuf(pb_task)

        # Assert
        assert task.id == task_id
        assert callable(task.callable)
        assert task.callable.__name__ == sample_async_callable.__name__
        assert task.args == args
        assert task.kwargs == kwargs
        assert task.caller == caller_id
        assert task.proxy.id == picklable_proxy.id
        assert task.timeout == 30
        assert task.tag == "test_tag"

    @pytest.mark.asyncio
    async def test_from_protobuf_empty_optionals(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """Test from_protobuf with optional fields empty.

        Given:
            A protobuf Task message with optional fields empty
        When:
            `from_protobuf` is called
        Then:
            Returns a Task with None/0 for empty optional fields
        """
        # Arrange
        task_id = uuid4()
        args = ()
        kwargs = {}

        pb_task = protocol.task.Task(
            id=str(task_id),
            callable=cloudpickle.dumps(sample_async_callable),
            args=cloudpickle.dumps(args),
            kwargs=cloudpickle.dumps(kwargs),
            caller="",
            proxy=cloudpickle.dumps(picklable_proxy),
            proxy_id=str(picklable_proxy.id),
            timeout=0,
            tag="",
        )

        # Act
        task = Task.from_protobuf(pb_task)

        # Assert
        assert task.id == task_id
        assert task.caller is None
        assert task.timeout == 0
        assert task.tag is None

    def test_to_protobuf_all_fields(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """Test to_protobuf with all fields populated.

        Given:
            A Task instance with all fields populated
        When:
            `to_protobuf` is called
        Then:
            Returns a protobuf Task with all fields correctly serialized
        """
        # Arrange
        caller_id = uuid4()
        task_id = uuid4()
        task = Task(
            id=task_id,
            callable=sample_async_callable,
            args=(1, 2),
            kwargs={"key": "value"},
            proxy=picklable_proxy,
            caller=caller_id,
            timeout=30,
            tag="test_tag",
        )

        # Act
        pb_task = task.to_protobuf()

        # Assert
        assert pb_task.version != ""
        assert pb_task.id == str(task.id)
        deserialized_callable = cloudpickle.loads(pb_task.callable)
        assert callable(deserialized_callable)
        assert deserialized_callable.__name__ == task.callable.__name__
        assert cloudpickle.loads(pb_task.args) == task.args
        assert cloudpickle.loads(pb_task.kwargs) == task.kwargs
        assert pb_task.caller == str(caller_id)
        deserialized_proxy = cloudpickle.loads(pb_task.proxy)
        assert deserialized_proxy.id == task.proxy.id
        assert pb_task.proxy_id == str(task.proxy.id)
        assert pb_task.timeout == 30
        assert pb_task.tag == "test_tag"

    def test_to_protobuf_none_optionals(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """Test to_protobuf with optional fields as None/0.

        Given:
            A Task instance with optional fields as None/0
        When:
            `to_protobuf` is called
        Then:
            Returns a protobuf Task with empty strings/0 for optional
            fields
        """
        # Arrange
        task = Task(
            id=uuid4(),
            callable=sample_async_callable,
            args=(),
            kwargs={},
            proxy=picklable_proxy,
            caller=None,
            timeout=0,
            tag=None,
        )

        # Act
        pb_task = task.to_protobuf()

        # Assert
        assert pb_task.caller == ""
        assert pb_task.timeout == 0
        assert pb_task.tag == ""

    def test_to_protobuf_with_version_field(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """Test to_protobuf includes protocol version in version field.

        Given:
            A Task instance
        When:
            to_protobuf() is called
        Then:
            The protobuf Task contains the protocol version in the
            version field.
        """
        # Arrange
        task = Task(
            id=uuid4(),
            callable=sample_async_callable,
            args=(),
            kwargs={},
            proxy=picklable_proxy,
        )

        # Act
        pb_task = task.to_protobuf()

        # Assert
        assert pb_task.version == protocol.__version__

    @settings(
        max_examples=50,
        deadline=None,
    )
    @given(
        version=st.from_regex(r"\d{1,3}\.\d{1,3}(rc\d{1,3}|\.\d{1,3})", fullmatch=True),
    )
    def test_from_protobuf_with_version_roundtrip(self, version):
        """Test version field round-trips through protobuf serialization.

        Given:
            Any PEP 440-like version string
        When:
            A protobuf Task with that version is serialized
        Then:
            The version field is preserved on the wire.
        """
        # Arrange
        proxy = PicklableProxy()

        async def test_callable():
            return "result"

        pb_task = protocol.task.Task(
            version=version,
            id=str(uuid4()),
            callable=cloudpickle.dumps(test_callable),
            args=cloudpickle.dumps(()),
            kwargs=cloudpickle.dumps({}),
            caller="",
            proxy=cloudpickle.dumps(proxy),
            proxy_id=str(proxy.id),
            timeout=0,
            tag="",
        )

        # Act — serialize to bytes and parse back
        wire_bytes = pb_task.SerializeToString()
        parsed = protocol.task.Task()
        parsed.ParseFromString(wire_bytes)

        # Assert
        assert parsed.version == version

    @pytest.mark.asyncio
    async def test_dispatch_successful_execution(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """Test Task.dispatch executes successfully.

        Given:
            A Task with a valid proxy pool in context
        When:
            `run` is called
        Then:
            Executes the callable with args/kwargs and returns the result
        """

        # Arrange
        async def test_callable(x, y=0):
            return x + y

        task = sample_task(
            callable=test_callable,
            args=(5,),
            kwargs={"y": 3},
        )

        # Act
        result = await task.dispatch()

        # Assert
        assert result == 8

    @pytest.mark.asyncio
    async def test_dispatch_emits_task_completed_event_on_success(
        self,
        sample_task,
        mock_worker_proxy_cache,
        event_spy,
        clear_event_handlers,
    ):
        """Test Task.dispatch emits task-completed event on success.

        Given:
            A Task completes execution successfully
        When:
            The task finishes without error
        Then:
            A "task-completed" event is emitted.
        """
        # Arrange
        wool.TaskEvent._handlers["task-completed"] = [event_spy]

        async def test_callable():
            return 42

        task = sample_task(callable=test_callable)

        # Act
        task_handle = asyncio.create_task(task.dispatch())
        result = await task_handle

        # Wait for the done callback to be invoked
        await asyncio.sleep(0.01)

        # Assert
        assert result == 42
        assert len(event_spy.calls) == 1
        event, timestamp, context = event_spy.calls[0]
        assert event.type == "task-completed"
        assert event.task.id == task.id
        assert event.task.exception is None

    @pytest.mark.asyncio
    async def test_dispatch_emits_task_completed_event_on_error(
        self,
        sample_task,
        mock_worker_proxy_cache,
        event_spy,
        clear_event_handlers,
    ):
        """Test Task.dispatch emits task-completed event on error.

        Given:
            A Task completes execution with an exception
        When:
            The task finishes with an error
        Then:
            A "task-completed" event is emitted with exception
            information attached to the task.
        """
        # Arrange
        wool.TaskEvent._handlers["task-completed"] = [event_spy]

        async def test_callable():
            raise ValueError("Test error")

        task = sample_task(callable=test_callable)

        # Act - Create a task so the done callback can be invoked
        async def run_task():
            try:
                await task.dispatch()
            except ValueError:
                pass  # Expected

        task_handle = asyncio.create_task(run_task())
        await task_handle

        # Wait for the callback to be invoked
        await asyncio.sleep(0.01)

        # Assert the event was emitted
        assert len(event_spy.calls) == 1
        event, timestamp, context = event_spy.calls[0]
        assert event.type == "task-completed"
        assert event.task.id == task.id
        assert event.task.exception is not None

    @pytest.mark.asyncio
    async def test_dispatch_without_proxy_pool_raises_error(
        self, sample_task, clear_event_handlers
    ):
        """Test Task.dispatch without proxy pool raises error.

        Given:
            Task.dispatch is called without proxy pool in context
        When:
            run() is invoked
        Then:
            RuntimeError is raised
        """
        # Arrange
        task = sample_task()

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="No proxy pool available for task execution"
        ):
            await task.dispatch()

    @pytest.mark.asyncio
    async def test_context_manager_with_multiple_exception_types(
        self, sample_task, clear_event_handlers
    ):
        """Test Task context manager with various exception types.

        Given:
            Task context manager with various exception types
        When:
            Different exception types are raised in context
        Then:
            All exception types are correctly captured and formatted
        """
        # Arrange & Act
        test_cases = [
            (ValueError, "value error"),
            (KeyError, "key error"),
            (RuntimeError, "runtime error"),
        ]

        for exception_type, message in test_cases:
            task = sample_task()

            async def run_with_exception():
                with pytest.raises(exception_type, match=message):
                    with task:
                        await asyncio.sleep(0)
                        raise exception_type(message)

            await run_with_exception()

            # Assert
            assert task.exception is not None
            assert task.exception.type == exception_type.__name__
            assert any(message in line for line in task.exception.traceback)

    def test_to_protobuf_with_unpicklable_callable_fails(
        self, picklable_proxy, clear_event_handlers
    ):
        """Test Task with unpicklable callable fails serialization.

        Given:
            Task with unpicklable callable
        When:
            to_protobuf() is called
        Then:
            Pickling fails appropriately
        """
        # Arrange - Create a mock callable that will fail pickling
        # Use a lambda which cloudpickle can normally handle, but wrap it
        # in a way that makes it unpicklable
        import _thread

        # _thread.lock objects cannot be pickled
        unpicklable_obj = _thread.allocate_lock()

        # Create a callable that contains the unpicklable object
        async def unpicklable_callable():
            # Reference the lock to make the function unpicklable
            return unpicklable_obj

        # Add a __qualname__ to avoid AttributeError in emit
        unpicklable_callable.__qualname__ = "unpicklable_callable"

        task = Task(
            id=uuid4(),
            callable=unpicklable_callable,
            args=(),
            kwargs={},
            proxy=picklable_proxy,
        )

        # Act & Assert
        with pytest.raises((TypeError, AttributeError)):
            task.to_protobuf()

    @settings(max_examples=50, deadline=None)
    @given(
        task_id=st.uuids(),
        timeout=st.integers(min_value=0, max_value=3600),
        caller_id=st.one_of(st.none(), st.uuids()),
        tag=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    )
    @pytest.mark.asyncio
    async def test_serialization_roundtrip(
        self,
        task_id,
        timeout,
        caller_id,
        tag,
    ):
        """Property-based test: Task serialization round-trip.

        Given:
            Any Task with valid picklable data
        When:
            from_protobuf(to_protobuf(task)) is called
        Then:
            Should equal the original task in all public attributes
        """

        # Arrange - Create test data inline to avoid fixture issues
        async def test_callable():
            return "result"

        proxy = PicklableProxy()
        args = (1, "test", [1, 2, 3])
        kwargs = {"key": "value", "number": 42}

        original_task = Task(
            id=task_id,
            callable=test_callable,
            args=args,
            kwargs=kwargs,
            proxy=proxy,
            timeout=timeout,
            caller=caller_id,
            tag=tag,
        )

        # Act
        pb_task = original_task.to_protobuf()
        deserialized_task = Task.from_protobuf(pb_task)

        # Assert
        assert deserialized_task.id == original_task.id
        assert callable(deserialized_task.callable)
        assert deserialized_task.callable.__name__ == original_task.callable.__name__
        assert deserialized_task.args == original_task.args
        assert deserialized_task.kwargs == original_task.kwargs
        assert deserialized_task.caller == original_task.caller
        assert deserialized_task.proxy.id == original_task.proxy.id
        assert deserialized_task.timeout == original_task.timeout
        assert deserialized_task.tag == original_task.tag

    # Async Generator Tests via dispatch()

    @pytest.mark.asyncio
    async def test_dispatch_with_async_generator_callable(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """Test dispatch() with async generator yields all values in order.

        Given:
            A Task with async generator callable and valid proxy pool
        When:
            dispatch() is called and iterated
        Then:
            Yields all values from the async generator callable in order
        """

        # Arrange
        async def test_generator():
            for i in range(3):
                yield f"value_{i}"

        task = sample_task(callable=test_generator)

        # Act
        results = []
        async for value in task.dispatch():
            results.append(value)

        # Assert
        assert results == ["value_0", "value_1", "value_2"]

    @pytest.mark.asyncio
    async def test_dispatch_with_coroutine_callable(
        self,
        sample_task: Callable[..., Task],
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """Test dispatch() with coroutine returns the result.

        Given:
            A Task with coroutine callable and valid proxy pool
        When:
            dispatch() is called and awaited
        Then:
            Returns the result from the coroutine callable
        """

        # Arrange
        async def test_coroutine():
            return "coroutine_result"

        task = sample_task(callable=test_coroutine)

        # Act
        result = await task.dispatch()

        # Assert
        assert result == "coroutine_result"

    @pytest.mark.asyncio
    async def test_dispatch_with_invalid_callable_raises_error(
        self,
        sample_task,
        clear_event_handlers,
    ):
        """Test dispatch() with neither coroutine nor async generator raises ValueError.

        Given:
            A Task with neither coroutine nor async generator
        When:
            dispatch() is called
        Then:
            Raises ValueError with expected message
        """

        # Arrange
        def not_async():
            return "not async"

        task = sample_task(callable=not_async)

        # Act & Assert
        with pytest.raises(
            ValueError, match="Expected routine to be coroutine or async generator"
        ):
            _ = task.dispatch()

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_without_proxy_pool_raises_error(
        self,
        sample_task,
        clear_event_handlers,
    ):
        """Test dispatch() with async generator without proxy pool raises RuntimeError.

        Given:
            A Task with async generator callable
        When:
            dispatch() is called without proxy pool in context
        Then:
            Raises RuntimeError with message "No proxy pool available for task execution"
        """

        # Arrange
        async def test_generator():
            yield "value"

        task = sample_task(callable=test_generator)

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="No proxy pool available for task execution"
        ):
            async for _ in task.dispatch():
                pass

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_raises_during_iteration(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """Test dispatch() with async generator that raises propagates exception.

        Given:
            A Task with async generator that raises during iteration
        When:
            dispatch() is called and iterated
        Then:
            Exception propagates to caller and generator is cleaned up via aclose()
        """

        # Arrange
        async def failing_generator():
            yield "first"
            raise ValueError("Generator error")

        task = sample_task(callable=failing_generator)

        # Act & Assert
        results = []
        with pytest.raises(ValueError, match="Generator error"):
            async for value in task.dispatch():
                results.append(value)

        # Verify we got the first value before the exception
        assert results == ["first"]

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_early_termination(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """Test dispatch() async iterator terminated early via break.

        Given:
            A Task with async generator callable
        When:
            dispatch() async iterator is terminated early via break
        Then:
            Iteration stops after receiving expected values
        """

        # Arrange
        async def test_generator():
            for i in range(10):
                yield f"value_{i}"

        task = sample_task(callable=test_generator)

        # Act
        results = []
        async for value in task.dispatch():
            results.append(value)
            if len(results) >= 2:
                break

        # Assert - verify we only got the first 2 values
        assert results == ["value_0", "value_1"]

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_multiple_values(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """Test dispatch() fully consumed yields all values in correct order.

        Given:
            A Task with async generator that yields multiple values
        When:
            dispatch() is fully consumed via async for
        Then:
            All yielded values are received in correct order
        """

        # Arrange
        async def multi_value_generator():
            for i in range(5):
                await asyncio.sleep(0)
                yield i * 10

        task = sample_task(callable=multi_value_generator)

        # Act
        results = []
        async for value in task.dispatch():
            results.append(value)

        # Assert
        assert results == [0, 10, 20, 30, 40]

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_empty(
        self,
        sample_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """Test dispatch() with async generator that yields zero values.

        Given:
            A Task with async generator that yields zero values
        When:
            dispatch() is called and consumed
        Then:
            Completes immediately without yielding any values
        """

        # Arrange
        async def empty_generator():
            return
            yield  # unreachable, but makes it a generator

        task = sample_task(callable=empty_generator)

        # Act
        results = []
        async for value in task.dispatch():
            results.append(value)

        # Assert - verify no values were yielded
        assert results == []

    @pytest.mark.asyncio
    async def test_context_manager_returns_stream_for_async_generator(
        self,
        sample_task,
        clear_event_handlers,
    ):
        """Test context manager returns callable for async generator.

        Given:
            A Task with an async generator callable
        When:
            Context manager is entered using `with` statement
        Then:
            Returns callable that can be used (private _stream method)
        """

        # Arrange
        async def test_generator():
            yield "value"

        task = sample_task(callable=test_generator)

        # Act
        with task as run_method:
            # Assert
            assert callable(run_method)
            # We should not call the returned method directly
            # (it's the private _stream method), just verify it's callable

    @pytest.mark.asyncio
    async def test_context_manager_with_invalid_callable_raises_error(
        self,
        sample_task,
        clear_event_handlers,
    ):
        """Test context manager with invalid callable raises ValueError.

        Given:
            A Task with neither coroutine nor async generator callable
        When:
            Context manager is entered using `with` statement
        Then:
            Raises ValueError with expected message
        """

        # Arrange
        def not_async():
            return "not async"

        task = sample_task(callable=not_async)

        # Act & Assert
        with pytest.raises(
            ValueError, match="Expected coroutine function or async generator function"
        ):
            with task:
                pass

    @pytest.mark.asyncio
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(value_count=st.integers(min_value=0, max_value=10))
    async def test_property_async_generator_correctness(
        self,
        value_count,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """Property test: Any async generator yielding N values returns all N in order.

        Given:
            Any async generator yielding N values (N = 0-10)
        When:
            Dispatched via dispatch() and fully consumed
        Then:
            All N values are received in order
        """

        # Arrange
        async def test_generator():
            for i in range(value_count):
                yield i

        proxy = PicklableProxy()
        task = Task(
            id=uuid4(),
            callable=test_generator,
            args=(),
            kwargs={},
            proxy=proxy,
        )

        # Act
        results = []
        async for value in task.dispatch():
            results.append(value)

        # Assert
        assert len(results) == value_count
        assert results == list(range(value_count))


class TestTaskException:
    """Tests for TaskException class."""

    def test_init(self):
        """Test TaskException instantiation.

        Given:
            Valid exception type and traceback
        When:
            TaskException is instantiated
        Then:
            Object is created with correct attributes
        """
        # Arrange
        exception_type = "ValueError"
        traceback_lines = ["line1", "line2", "line3"]

        # Act
        exception = TaskException(
            type=exception_type,
            traceback=traceback_lines,
        )

        # Assert
        assert exception.type == exception_type
        assert exception.traceback == traceback_lines


class TestTaskEvent:
    """Tests for TaskEvent class."""

    def test_init(self, sample_task, clear_event_handlers):
        """Test TaskEvent instantiation.

        Given:
            Valid event type and task
        When:
            TaskEvent is instantiated
        Then:
            Event object is created with correct attributes
        """
        # Arrange
        task = sample_task()

        # Act
        event = TaskEvent("task-created", task=task)

        # Assert
        assert event.type == "task-created"
        assert event.task == task

    def test_handler_decorator_registration(self, clear_event_handlers):
        """Test handler decorator registration.

        Given:
            A handler function is decorated with `@TaskEvent.handler`
        When:
            Handler decorator is applied with event types
        Then:
            Handler is registered for all specified event types and
            original function is returned
        """
        # Arrange
        call_log = []

        # Act
        @TaskEvent.handler("task-created", "task-completed")
        def test_handler(event, timestamp, context=None):
            call_log.append((event, timestamp, context))

        # Assert - handler is registered
        assert "task-created" in TaskEvent._handlers
        assert "task-completed" in TaskEvent._handlers
        assert test_handler in TaskEvent._handlers["task-created"]
        assert test_handler in TaskEvent._handlers["task-completed"]

        # Assert - original function is returned
        assert callable(test_handler)

    def test_emit_with_handlers(self, sample_task, event_spy, clear_event_handlers):
        """Test emit calls registered handlers.

        Given:
            An event with registered handlers
        When:
            `emit` is called
        Then:
            All registered handlers are called with event and timestamp
        """
        # Arrange
        task = sample_task()
        event = TaskEvent("task-created", task=task)
        TaskEvent._handlers["task-created"] = [event_spy]

        # Act
        event.emit()

        # Wait for handler thread to process scheduled handlers
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 1
        emitted_event, timestamp, context = event_spy.calls[0]
        assert emitted_event == event
        assert isinstance(timestamp, int)
        assert timestamp > 0

    def test_emit_without_handlers(self, sample_task, clear_event_handlers):
        """Test emit without handlers completes normally.

        Given:
            An event with no registered handlers
        When:
            `emit` is called
        Then:
            No handlers are called, execution completes normally
        """
        # Arrange
        task = sample_task()
        event = TaskEvent("task-created", task=task)

        # Act & Assert (no exception should be raised)
        event.emit()

    def test_emit_handler_exception_isolated(
        self, sample_task, clear_event_handlers, caplog
    ):
        """Test emit handler exception isolation via handler thread.

        Given:
            Event handler that raises an exception
        When:
            emit() calls a failing handler
        Then:
            Exception is logged, does not propagate to emit() caller
        """
        # Arrange
        task = sample_task()
        event = TaskEvent("task-created", task=task)

        def failing_handler(event, timestamp, context=None):
            raise ValueError("Handler failed")

        TaskEvent._handlers["task-created"] = [failing_handler]

        # Act - emit does not raise
        with caplog.at_level(logging.ERROR):
            event.emit()

            # Wait for handler thread to process scheduled handlers
            Event.flush()

        # Assert - exception was logged, not propagated
        assert any(
            "Exception in event handler" in record.message for record in caplog.records
        )

    @settings(max_examples=20, deadline=None)
    @given(
        event_types_to_register=st.sets(
            st.sampled_from(
                [
                    "task-created",
                    "task-scheduled",
                    "task-started",
                    "task-stopped",
                    "task-completed",
                ]
            ),
            min_size=1,
            max_size=6,
        ),
        num_handlers=st.integers(min_value=1, max_value=5),
    )
    def test_handler_registration_and_emission(
        self,
        event_types_to_register,
        num_handlers,
    ):
        """Property-based test: TaskEvent handler registration and emission.

        Given:
            Any set of event types and any number of handlers
        When:
            Handlers are registered and events are emitted
        Then:
            All handlers should be called exactly once for their registered
            event types
        """

        # Arrange - Create test data inline
        async def test_callable():
            return "result"

        proxy = PicklableProxy()

        # Clear handlers before test
        saved_handlers = wool.TaskEvent._handlers.copy()
        wool.TaskEvent._handlers.clear()

        try:
            task = Task(
                id=uuid4(),
                callable=test_callable,
                args=(),
                kwargs={},
                proxy=proxy,
            )
            handler_calls = {i: [] for i in range(num_handlers)}

            # Create and register handlers
            for i in range(num_handlers):

                def create_handler(handler_id):
                    def handler(event, timestamp, context=None):
                        handler_calls[handler_id].append((event, timestamp, context))

                    return handler

                handler = create_handler(i)
                for event_type in event_types_to_register:
                    TaskEvent._handlers.setdefault(event_type, []).append(handler)

            # Act - emit events for all registered types
            for event_type in event_types_to_register:
                event = TaskEvent(event_type, task=task)
                event.emit()

            # Wait for handler thread to process all scheduled handlers
            Event.flush()

            # Assert - each handler should be called once per event type
            for i in range(num_handlers):
                assert len(handler_calls[i]) == len(event_types_to_register)
        finally:
            # Restore handlers
            wool.TaskEvent._handlers = saved_handlers


class TestCurrentTask:
    """Tests for current_task() function."""

    @pytest.mark.asyncio
    async def test_current_task_within_context(
        self, sample_task, mock_worker_proxy_cache, clear_event_handlers
    ):
        """Test current_task() returns task within context.

        Given:
            Execution is within a task context
        When:
            `current_task()` is called
        Then:
            Returns the current Task instance
        """

        # Arrange
        async def test_callable():
            return current_task()

        task = sample_task(callable=test_callable)

        # Act
        result = await task.dispatch()

        # Assert
        assert result == task

    def test_current_task_outside_context(self):
        """Test current_task() returns None outside context.

        Given:
            Execution is outside any task context
        When:
            `current_task()` is called
        Then:
            Returns None
        """
        # Arrange & Act
        result = current_task()

        # Assert
        assert result is None

    @settings(max_examples=10, deadline=None)
    @given(nesting_depth=st.integers(min_value=1, max_value=5))
    @pytest.mark.asyncio
    async def test_nested_context_tracking(self, nesting_depth):
        """Property-based test: Nested task context tracking.

        Given:
            Any depth of nested task execution
        When:
            Tasks are nested
        Then:
            current_task() should always return the innermost active task,
            and caller chains should be properly maintained
        """
        # Arrange
        from wool.runtime.routine.task import _current_task

        async def test_callable():
            return "result"

        proxy = PicklableProxy()
        tasks = []

        async def create_nested_tasks(depth):
            if depth == 0:
                # Verify current_task() returns the innermost task
                assert current_task() == tasks[-1]
                return

            # Create a new task
            task = Task(
                id=uuid4(),
                callable=test_callable,
                args=(),
                kwargs={},
                proxy=proxy,
            )
            tasks.append(task)

            # Set as current task
            token = _current_task.set(task)
            try:
                await create_nested_tasks(depth - 1)
            finally:
                _current_task.reset(token)

        # Act
        await create_nested_tasks(nesting_depth)

        # Assert - verify caller chain
        for i in range(1, len(tasks)):
            # Each task's caller should be the previous task's ID
            # (except the first one which has no caller)
            if i > 0:
                assert tasks[i].caller == tasks[i - 1].id
