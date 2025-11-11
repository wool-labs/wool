import asyncio
from uuid import uuid4

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from wool.runtime import protobuf as pb
from wool.runtime.work import WorkTask
from wool.runtime.work import WorkTaskEvent
from wool.runtime.work import WorkTaskException
from wool.runtime.work import current_task


class PicklableProxy:
    """A simple picklable proxy for testing serialization."""

    def __init__(self):
        self.id = uuid4()

    async def dispatch(self, *args, **kwargs):
        """Mock dispatch method."""

        async def _stream():
            yield "result"

        return _stream()


class TestWorkerTask:
    """Tests for WorkTask class."""

    @pytest.mark.asyncio
    async def test_init_emits_task_created_event(
        self, mock_task, event_spy, clear_event_handlers
    ):
        """Test WorkTask instantiation emits task-created event.

        Given:
            Valid parameters for a WorkTask
        When:
            WorkTask is instantiated
        Then:
            A "task-created" event is emitted
        """
        # Arrange
        wool.WorkTaskEvent._handlers["task-created"] = [event_spy]

        # Act
        task = mock_task()

        # Wait for event loop to process scheduled handlers
        await asyncio.sleep(0)

        # Assert
        assert len(event_spy.calls) == 1
        event, timestamp, context = event_spy.calls[0]
        assert event.type == "task-created"
        assert event.task.id == task.id
        assert isinstance(timestamp, int)
        assert timestamp > 0

    @pytest.mark.asyncio
    async def test_init_sets_caller_in_nested_context(
        self, mock_task, sample_async_callable, mock_proxy, clear_event_handlers
    ):
        """Test WorkTask sets caller field in nested task context.

        Given:
            A WorkTask is instantiated within another task's context
        When:
            WorkTask is created
        Then:
            The caller field is set to the parent task's ID
        """
        # Arrange
        parent_task = mock_task()

        # Act
        async def create_nested_task():
            # Set parent task in context
            from wool.runtime.work.task import _current_task

            token = _current_task.set(parent_task)
            try:
                nested_task = mock_task()
                return nested_task
            finally:
                _current_task.reset(token)

        nested_task = await create_nested_task()

        # Assert
        assert nested_task.caller == parent_task.id

    def test_init_caller_none_outside_task_context(
        self, mock_task, clear_event_handlers
    ):
        """Test WorkTask caller field is None outside task context.

        Given:
            A WorkTask is instantiated outside any task context
        When:
            WorkTask is created
        Then:
            The caller field remains None
        """
        # Arrange & Act
        task = mock_task()

        # Assert
        assert task.caller is None

    @pytest.mark.asyncio
    async def test_context_manager_entry(self, mock_task, clear_event_handlers):
        """Test WorkTask context manager entry.

        Given:
            A WorkTask is used as a context manager
        When:
            Context is entered using `with` statement
        Then:
            Context manager provides access to task execution
        """
        # Arrange
        task = mock_task()

        # Act
        with task as run_method:
            # Assert
            assert callable(run_method)
            assert run_method == task.run

    @pytest.mark.asyncio
    async def test_context_manager_normal_exit(self, mock_task, clear_event_handlers):
        """Test WorkTask context manager exits normally.

        Given:
            A WorkTask context manager exits normally
        When:
            `with` statement completes without exception
        Then:
            Context exits cleanly without suppressing exceptions
        """
        # Arrange
        task = mock_task()

        # Act & Assert (no exception should be raised)
        with task:
            pass

    @pytest.mark.asyncio
    async def test_context_manager_exception_handling(
        self, mock_task, clear_event_handlers
    ):
        """Test WorkTask context manager exception handling.

        Given:
            A WorkTask context manager and an exception is raised within
            the context
        When:
            Exception occurs inside `with` block
        Then:
            WorkTaskException is created and attached to task, exception
            propagates normally
        """
        # Arrange
        task = mock_task()

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
            Returns a WorkTask with all fields correctly deserialized
        """
        # Arrange
        task_id = uuid4()
        caller_id = uuid4()
        args = (1, 2, 3)
        kwargs = {"key": "value"}

        pb_task = pb.task.Task(
            id=str(task_id),
            callable=cloudpickle.dumps(sample_async_callable),
            args=cloudpickle.dumps(args),
            kwargs=cloudpickle.dumps(kwargs),
            caller=str(caller_id),
            proxy=cloudpickle.dumps(picklable_proxy),
            proxy_id=str(picklable_proxy.id),
            timeout=30,
            filename="test.py",
            function="test_function",
            line_no=42,
            tag="test_tag",
        )

        # Act
        task = WorkTask.from_protobuf(pb_task)

        # Assert
        assert task.id == task_id
        assert callable(task.callable)
        assert task.callable.__name__ == sample_async_callable.__name__
        assert task.args == args
        assert task.kwargs == kwargs
        assert task.caller == caller_id
        assert task.proxy.id == picklable_proxy.id
        assert task.timeout == 30
        assert task.filename == "test.py"
        assert task.function == "test_function"
        assert task.line_no == 42
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
            Returns a WorkTask with None/0 for empty optional fields
        """
        # Arrange
        task_id = uuid4()
        args = ()
        kwargs = {}

        pb_task = pb.task.Task(
            id=str(task_id),
            callable=cloudpickle.dumps(sample_async_callable),
            args=cloudpickle.dumps(args),
            kwargs=cloudpickle.dumps(kwargs),
            caller="",
            proxy=cloudpickle.dumps(picklable_proxy),
            proxy_id=str(picklable_proxy.id),
            timeout=0,
            filename="",
            function="",
            line_no=0,
            tag="",
        )

        # Act
        task = WorkTask.from_protobuf(pb_task)

        # Assert
        assert task.id == task_id
        assert task.caller is None
        assert task.timeout == 0
        assert task.filename is None
        assert task.function is None
        assert task.line_no is None
        assert task.tag is None

    def test_to_protobuf_all_fields(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """Test to_protobuf with all fields populated.

        Given:
            A WorkTask instance with all fields populated
        When:
            `to_protobuf` is called
        Then:
            Returns a protobuf Task with all fields correctly serialized
        """
        # Arrange
        caller_id = uuid4()
        task_id = uuid4()
        task = WorkTask(
            id=task_id,
            callable=sample_async_callable,
            args=(1, 2),
            kwargs={"key": "value"},
            proxy=picklable_proxy,
            caller=caller_id,
            timeout=30,
            filename="test.py",
            function="test_function",
            line_no=42,
            tag="test_tag",
        )

        # Act
        pb_task = task.to_protobuf()

        # Assert
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
        assert pb_task.filename == "test.py"
        assert pb_task.function == "test_function"
        assert pb_task.line_no == 42
        assert pb_task.tag == "test_tag"

    def test_to_protobuf_none_optionals(
        self, sample_async_callable, picklable_proxy, clear_event_handlers
    ):
        """Test to_protobuf with optional fields as None/0.

        Given:
            A WorkTask instance with optional fields as None/0
        When:
            `to_protobuf` is called
        Then:
            Returns a protobuf Task with empty strings/0 for optional
            fields
        """
        # Arrange
        task = WorkTask(
            id=uuid4(),
            callable=sample_async_callable,
            args=(),
            kwargs={},
            proxy=picklable_proxy,
            caller=None,
            timeout=0,
            filename=None,
            function=None,
            line_no=None,
            tag=None,
        )

        # Act
        pb_task = task.to_protobuf()

        # Assert
        assert pb_task.caller == ""
        assert pb_task.timeout == 0
        assert pb_task.filename == ""
        assert pb_task.function == ""
        assert pb_task.line_no == 0
        assert pb_task.tag == ""

    @pytest.mark.asyncio
    async def test_run_successful_execution(
        self,
        mock_task,
        mock_worker_proxy_cache,
        clear_event_handlers,
    ):
        """Test WorkTask.run executes successfully.

        Given:
            A WorkTask with a valid proxy pool in context
        When:
            `run` is called
        Then:
            Executes the callable with args/kwargs and returns the result
        """

        # Arrange
        async def test_callable(x, y=0):
            return x + y

        task = mock_task(
            callable=test_callable,
            args=(5,),
            kwargs={"y": 3},
        )

        # Act
        result = await task.run()

        # Assert
        assert result == 8

    @pytest.mark.asyncio
    async def test_run_emits_task_completed_event(
        self,
        mock_task,
        mock_worker_proxy_cache,
        event_spy,
        clear_event_handlers,
    ):
        """Test WorkTask.run emits task-completed event on exception.

        Given:
            A WorkTask completes execution with an exception
        When:
            The task finishes with an error
        Then:
            A "task-completed" event is emitted
        """
        # Arrange
        wool.WorkTaskEvent._handlers["task-completed"] = [event_spy]

        async def test_callable():
            raise ValueError("Test error")

        task = mock_task(callable=test_callable)

        # Act - Create a task so the done callback can be invoked
        async def run_task():
            try:
                await task.run()
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

    @pytest.mark.asyncio
    async def test_run_without_proxy_pool_raises_error(
        self, mock_task, clear_event_handlers
    ):
        """Test WorkTask.run without proxy pool raises error.

        Given:
            WorkTask.run is called without proxy pool in context
        When:
            run() is invoked
        Then:
            RuntimeError is raised
        """
        # Arrange
        task = mock_task()

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="No proxy pool available for task execution"
        ):
            await task.run()

    @pytest.mark.asyncio
    async def test_context_manager_with_multiple_exception_types(
        self, mock_task, clear_event_handlers
    ):
        """Test WorkTask context manager with various exception types.

        Given:
            WorkTask context manager with various exception types
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
            task = mock_task()

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
        """Test WorkTask with unpicklable callable fails serialization.

        Given:
            WorkTask with unpicklable callable
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

        task = WorkTask(
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
        filename=st.one_of(st.none(), st.text(min_size=1, max_size=50)),
        function=st.one_of(st.none(), st.text(min_size=1, max_size=50)),
        line_no=st.one_of(st.none(), st.integers(min_value=1, max_value=10000)),
        tag=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    )
    @pytest.mark.asyncio
    async def test_serialization_roundtrip(
        self,
        task_id,
        timeout,
        caller_id,
        filename,
        function,
        line_no,
        tag,
    ):
        """Property-based test: WorkTask serialization round-trip.

        Given:
            Any WorkTask with valid picklable data
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

        original_task = WorkTask(
            id=task_id,
            callable=test_callable,
            args=args,
            kwargs=kwargs,
            proxy=proxy,
            timeout=timeout,
            caller=caller_id,
            filename=filename,
            function=function,
            line_no=line_no,
            tag=tag,
        )

        # Act
        pb_task = original_task.to_protobuf()
        deserialized_task = WorkTask.from_protobuf(pb_task)

        # Assert
        assert deserialized_task.id == original_task.id
        assert callable(deserialized_task.callable)
        assert deserialized_task.callable.__name__ == original_task.callable.__name__
        assert deserialized_task.args == original_task.args
        assert deserialized_task.kwargs == original_task.kwargs
        assert deserialized_task.caller == original_task.caller
        assert deserialized_task.proxy.id == original_task.proxy.id
        assert deserialized_task.timeout == original_task.timeout
        assert deserialized_task.filename == original_task.filename
        assert deserialized_task.function == original_task.function
        assert deserialized_task.line_no == original_task.line_no
        assert deserialized_task.tag == original_task.tag


class TestWorkerTaskException:
    """Tests for WorkTaskException class."""

    def test_init(self):
        """Test WorkTaskException instantiation.

        Given:
            Valid exception type and traceback
        When:
            WorkTaskException is instantiated
        Then:
            Object is created with correct attributes
        """
        # Arrange
        exception_type = "ValueError"
        traceback_lines = ["line1", "line2", "line3"]

        # Act
        exception = WorkTaskException(
            type=exception_type,
            traceback=traceback_lines,
        )

        # Assert
        assert exception.type == exception_type
        assert exception.traceback == traceback_lines


class TestWorkerTaskEvent:
    """Tests for WorkTaskEvent class."""

    def test_init(self, mock_task, clear_event_handlers):
        """Test WorkTaskEvent instantiation.

        Given:
            Valid event type and task
        When:
            WorkTaskEvent is instantiated
        Then:
            Event object is created with correct attributes
        """
        # Arrange
        task = mock_task()

        # Act
        event = WorkTaskEvent("task-created", task=task)

        # Assert
        assert event.type == "task-created"
        assert event.task == task

    def test_handler_decorator_registration(self, clear_event_handlers):
        """Test handler decorator registration.

        Given:
            A handler function is decorated with `@WorkTaskEvent.handler`
        When:
            Handler decorator is applied with event types
        Then:
            Handler is registered for all specified event types and
            original function is returned
        """
        # Arrange
        call_log = []

        # Act
        @WorkTaskEvent.handler("task-created", "task-completed")
        def test_handler(event, timestamp, context=None):
            call_log.append((event, timestamp, context))

        # Assert - handler is registered
        assert "task-created" in WorkTaskEvent._handlers
        assert "task-completed" in WorkTaskEvent._handlers
        assert test_handler in WorkTaskEvent._handlers["task-created"]
        assert test_handler in WorkTaskEvent._handlers["task-completed"]

        # Assert - original function is returned
        assert callable(test_handler)

    @pytest.mark.asyncio
    async def test_emit_with_handlers(self, mock_task, event_spy, clear_event_handlers):
        """Test emit calls registered handlers.

        Given:
            An event with registered handlers
        When:
            `emit` is called
        Then:
            All registered handlers are called with event and timestamp
        """
        # Arrange
        task = mock_task()
        event = WorkTaskEvent("task-created", task=task)
        WorkTaskEvent._handlers["task-created"] = [event_spy]

        # Act
        event.emit()

        # Wait for event loop to process scheduled handlers
        await asyncio.sleep(0)

        # Assert
        assert len(event_spy.calls) == 1
        emitted_event, timestamp, context = event_spy.calls[0]
        assert emitted_event == event
        assert isinstance(timestamp, int)
        assert timestamp > 0

    def test_emit_without_handlers(self, mock_task, clear_event_handlers):
        """Test emit without handlers completes normally.

        Given:
            An event with no registered handlers
        When:
            `emit` is called
        Then:
            No handlers are called, execution completes normally
        """
        # Arrange
        task = mock_task()
        event = WorkTaskEvent("task-created", task=task)

        # Act & Assert (no exception should be raised)
        event.emit()

    @pytest.mark.asyncio
    async def test_emit_handler_exception_isolated(
        self, mock_task, clear_event_handlers
    ):
        """Test emit handler exception isolation via call_soon.

        Given:
            Event handler that raises an exception
        When:
            emit() calls a failing handler
        Then:
            Exception is caught by event loop, does not propagate to emit() caller
        """
        # Arrange
        task = mock_task()
        event = WorkTaskEvent("task-created", task=task)
        exception_caught = []

        def failing_handler(event, timestamp, context=None):
            raise ValueError("Handler failed")

        WorkTaskEvent._handlers["task-created"] = [failing_handler]

        # Set up event loop exception handler to catch the exception
        loop = asyncio.get_event_loop()
        old_exception_handler = loop.get_exception_handler()

        def exception_handler(loop, context):
            exception_caught.append(context["exception"])

        loop.set_exception_handler(exception_handler)

        try:
            # Act - emit does not raise
            event.emit()

            # Wait for event loop to process scheduled handlers
            await asyncio.sleep(0)

            # Assert - exception was caught by event loop, not propagated
            assert len(exception_caught) == 1
            assert isinstance(exception_caught[0], ValueError)
            assert str(exception_caught[0]) == "Handler failed"
        finally:
            # Restore original exception handler
            loop.set_exception_handler(old_exception_handler)

    @pytest.mark.asyncio
    @settings(max_examples=20, deadline=None)
    @given(
        event_types_to_register=st.sets(
            st.sampled_from(
                [
                    "task-created",
                    "task-queued",
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
    async def test_handler_registration_and_emission(
        self,
        event_types_to_register,
        num_handlers,
    ):
        """Property-based test: WorkTaskEvent handler registration and emission.

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
        saved_handlers = wool.WorkTaskEvent._handlers.copy()
        wool.WorkTaskEvent._handlers.clear()

        try:
            task = WorkTask(
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
                    WorkTaskEvent._handlers.setdefault(event_type, []).append(handler)

            # Act - emit events for all registered types
            for event_type in event_types_to_register:
                event = WorkTaskEvent(event_type, task=task)
                event.emit()

            # Wait for event loop to process all scheduled handlers
            await asyncio.sleep(0)

            # Assert - each handler should be called once per event type
            for i in range(num_handlers):
                assert len(handler_calls[i]) == len(event_types_to_register)
        finally:
            # Restore handlers
            wool.WorkTaskEvent._handlers = saved_handlers


class TestCurrentTask:
    """Tests for current_task() function."""

    @pytest.mark.asyncio
    async def test_current_task_within_context(
        self, mock_task, mock_worker_proxy_cache, clear_event_handlers
    ):
        """Test current_task() returns task within context.

        Given:
            Execution is within a task context
        When:
            `current_task()` is called
        Then:
            Returns the current WorkTask instance
        """

        # Arrange
        async def test_callable():
            return current_task()

        task = mock_task(callable=test_callable)

        # Act
        result = await task.run()

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
        from wool.runtime.work.task import _current_task

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
            task = WorkTask(
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
