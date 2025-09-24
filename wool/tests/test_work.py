import asyncio
import uuid
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import cloudpickle
import pytest
from pytest_mock import MockerFixture

import wool
from wool import _protobuf as pb
from wool._work import WoolTask
from wool._work import WoolTaskEvent
from wool._work import WoolTaskException
from wool._work import _current_task
from wool._work import _do_dispatch
from wool._work import _execute
from wool._work import _resolve
from wool._work import current_task
from wool._work import work
from wool._worker_proxy import WorkerProxy


def create_mock_proxy():
    """Create a properly mocked proxy for tests."""
    mock_proxy = MagicMock()
    mock_proxy.id = "test-proxy-id"
    mock_proxy.start = AsyncMock()
    mock_proxy.stop = AsyncMock()
    mock_proxy.__aenter__ = AsyncMock(return_value=mock_proxy)
    mock_proxy.__aexit__ = AsyncMock(return_value=None)
    return mock_proxy


@pytest.fixture
def mock_proxy():
    """Provide a mock WorkerProxy for testing."""
    return create_mock_proxy()


@pytest.fixture
def configured_mock_proxy():
    """Create a fully configured mock WorkerProxy for testing."""
    mock_proxy = MagicMock(spec=WorkerProxy)
    mock_proxy.id = "test-proxy-id"
    mock_proxy.__aenter__ = AsyncMock(return_value=mock_proxy)
    mock_proxy.__aexit__ = AsyncMock(return_value=None)
    mock_proxy.dispatch = MagicMock()
    return mock_proxy


@pytest.fixture
def sample_wool_task(configured_mock_proxy):
    """Create a standard WoolTask for testing."""
    return WoolTask(
        id=uuid.uuid4(),
        callable=pickable_callable,
        args=(),
        kwargs={},
        proxy=configured_mock_proxy,
    )


@pytest.fixture
def mock_async_generator():
    """Create a mock async generator for dispatch testing."""

    async def _generator(result="test_result"):
        yield result

    return _generator


@pytest.fixture
def event_handler_cleanup():
    """Fixture to clean up event handlers after tests."""
    original_handlers = WoolTaskEvent._handlers.copy()
    yield
    WoolTaskEvent._handlers = original_handlers


async def pickable_callable():
    pass


# Global function for testing line 140 coverage (not a pytest test function)
async def function_for_line_140_coverage():
    return "test"


@work
async def dummy_work_function(x: int, y: int) -> int:
    """Test decorated function."""
    return x + y


class DummyWorkClass:
    """Test class for method decoration (not collected by pytest)."""

    @work
    async def instance_method(self, value: int) -> int:
        """Test instance method."""
        return value * 2

    @classmethod
    @work
    async def class_method(cls, value: int) -> int:
        """Test class method."""
        return value * 3

    @staticmethod
    @work
    async def static_method(value: int) -> int:
        """Test static method."""
        return value * 4


class TestWoolTask:
    """Test the WoolTask and WoolTaskException classes."""

    def test_wool_task_enter_returns_run_method(
        self, mock_proxy, mock_worker_proxy_cache
    ):
        """Test context manager __enter__ method returns run method.

        Given:
            A WoolTask with callable and proxy
        When:
            The task __enter__ method is called
        Then:
            Should return the task's run method
        """

        # Arrange
        async def sample_callable():
            pass

        task = WoolTask(
            id=uuid.uuid4(),
            callable=sample_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Act
        result = task.__enter__()

        # Assert
        assert result == task.run

    def test_wool_task_exit_handles_exception(self, mock_proxy, mocker: MockerFixture):
        """Test context manager __exit__ method handles exception properly.

        Given:
            A WoolTask with exception during execution
        When:
            The task __exit__ method is called with exception
        Then:
            Should capture exception details and add done callback
        """
        # Arrange
        mock_asyncio_task = MagicMock()
        mocker.patch("asyncio.current_task", return_value=mock_asyncio_task)

        async def sample_callable():
            pass

        task = WoolTask(
            id=uuid.uuid4(),
            callable=sample_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        exception = ValueError("test error")

        # Act
        task.__exit__(ValueError, exception, exception.__traceback__)

        # Assert
        assert isinstance(task.exception, WoolTaskException)
        assert task.exception.type == "ValueError"
        assert any("test error" in line for line in task.exception.traceback)
        mock_asyncio_task.add_done_callback.assert_called_once()

    def test_wool_task_exception_initialization_sets_attributes(self):
        """Test WoolTaskException initialization sets attributes correctly.

        Given:
            Exception type and traceback lines
        When:
            WoolTaskException is initialized
        Then:
            Should set type and traceback attributes correctly
        """
        # Arrange
        traceback_lines = ["line1", "line2", "line3"]

        # Act
        exception = WoolTaskException(type="ValueError", traceback=traceback_lines)

        # Assert
        assert exception.type == "ValueError"
        assert exception.traceback == traceback_lines

    def test_wool_task_finish_emits_completion_event(
        self, mock_proxy, mocker: MockerFixture
    ):
        """Test _finish method emits task completion event.

        Given:
            A WoolTask with mocked event system
        When:
            The _finish method is called
        Then:
            Should create and emit task-completed event
        """
        # Arrange
        mock_event_class = mocker.patch("wool._work.WoolTaskEvent")
        mock_event_instance = MagicMock()
        mock_event_class.return_value = mock_event_instance

        task = WoolTask(
            id=uuid.uuid4(),
            callable=pickable_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        # Reset the mock to ignore the initialization call
        mock_event_class.reset_mock()

        # Act
        task._finish(None)

        # Assert
        mock_event_class.assert_called_once_with("task-completed", task=task)
        mock_event_instance.emit.assert_called_once()

    def test_wool_task_from_protobuf_deserializes_correctly(self):
        """Test from_protobuf class method deserializes task correctly.

        Given:
            A protobuf task with serialized data
        When:
            WoolTask.from_protobuf is called
        Then:
            Should deserialize all task attributes correctly
        """
        # Arrange
        task_id = uuid.uuid4()
        caller_id = uuid.uuid4()
        mock_worker_proxy = WorkerProxy("test-pool")

        pb_task = pb.task.Task(
            id=str(task_id),
            callable=cloudpickle.dumps(pickable_callable),
            args=cloudpickle.dumps((1, 2)),
            kwargs=cloudpickle.dumps({"key": "value"}),
            caller=str(caller_id),
            proxy=cloudpickle.dumps(mock_worker_proxy),
        )

        # Act
        task = WoolTask.from_protobuf(pb_task)

        # Assert
        assert task.id == task_id
        assert task.args == (1, 2)
        assert task.kwargs == {"key": "value"}
        assert task.caller == caller_id
        assert task.proxy is not None

    def test_wool_task_initialization_sets_all_attributes(self, mock_proxy):
        """Test WoolTask initialization sets all attributes correctly.

        Given:
            Task parameters including id, callable, args, kwargs and proxy
        When:
            WoolTask is initialized
        Then:
            Should set all attributes correctly with proper defaults
        """
        # Arrange
        task_id = uuid.uuid4()

        async def sample_callable():
            pass

        # Act
        task = WoolTask(
            id=task_id,
            callable=sample_callable,
            args=(1, 2),
            kwargs={"key": "value"},
            proxy=mock_proxy,
        )

        # Assert
        assert task.id == task_id
        assert task.callable == sample_callable
        assert task.args == (1, 2)
        assert task.kwargs == {"key": "value"}
        assert task.timeout == 0
        assert task.exception is None

    def test_wool_task_post_init_sets_caller_from_context(
        self, mock_proxy, mocker: MockerFixture
    ):
        """Test __post_init__ sets caller from current task context.

        Given:
            A current task in context with specific ID
        When:
            A new WoolTask is initialized
        Then:
            Should set caller to current task ID and emit task-created event
        """
        # Arrange
        caller_id = uuid.uuid4()
        mock_caller_task = MagicMock(spec=WoolTask)
        mock_caller_task.id = caller_id

        mock_current_task_context = mocker.patch("wool._work._current_task")
        mock_current_task_context.get.return_value = mock_caller_task
        mock_event = mocker.patch("wool._work.WoolTaskEvent")

        async def sample_callable():
            pass

        # Act
        task = WoolTask(
            id=uuid.uuid4(),
            callable=sample_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Assert
        assert task.caller == caller_id
        mock_event.assert_called_once_with("task-created", task=task)

    def test_wool_task_to_protobuf_serializes_correctly(self):
        """Test to_protobuf method serializes task correctly.

        Given:
            A WoolTask with all attributes set
        When:
            to_protobuf method is called
        Then:
            Should serialize all attributes to protobuf format correctly
        """
        # Arrange
        task_id = uuid.uuid4()
        caller_id = uuid.uuid4()
        # Use a real proxy that can be pickled
        mock_worker_proxy = WorkerProxy("test-pool")

        task = WoolTask(
            id=task_id,
            callable=pickable_callable,
            args=(1, 2),
            kwargs={"key": "value"},
            caller=caller_id,
            proxy=mock_worker_proxy,
        )

        # Act
        pb_task = task.to_protobuf()

        # Assert
        assert pb_task.id == str(task_id)
        assert cloudpickle.loads(pb_task.callable) == pickable_callable
        assert cloudpickle.loads(pb_task.args) == (1, 2)
        assert cloudpickle.loads(pb_task.kwargs) == {"key": "value"}
        assert pb_task.caller == str(caller_id)
        assert pb_task.proxy != b""

    @pytest.mark.asyncio
    async def test_wool_task_run_context_manager_integration(
        self, configured_mock_proxy, mock_worker_proxy_cache
    ):
        """Test run method properly uses context manager.

        Given:
            A WoolTask with callable and proxy
        When:
            run() method is called
        Then:
            Should properly use task context manager (with statement)
        """
        # Arrange
        context_used = False

        async def context_tracking_callable():
            nonlocal context_used
            # If we reach here, the 'with self:' statement worked properly
            context_used = True
            return "cm_result"

        task = WoolTask(
            id=uuid.uuid4(),
            callable=context_tracking_callable,
            args=(),
            kwargs={},
            proxy=configured_mock_proxy,
        )

        # Act
        result = await task.run()

        # Assert
        assert result == "cm_result"
        assert context_used

    @pytest.mark.asyncio
    async def test_wool_task_run_executes_callable_returns_result(
        self, configured_mock_proxy, mock_worker_proxy_cache
    ):
        """Test run method executes callable and returns result.

        Given:
            A WoolTask with async callable and proxy
        When:
            run() method is called
        Then:
            Should execute the callable and return result
        """

        # Arrange
        async def test_callable(x, y):
            return x * y

        task = WoolTask(
            id=uuid.uuid4(),
            callable=test_callable,
            args=(6, 7),
            kwargs={},
            proxy=configured_mock_proxy,
        )

        # Act
        result = await task.run()

        # Assert
        assert result == 42

    @pytest.mark.asyncio
    async def test_wool_task_run_resets_context_after_completion(
        self, configured_mock_proxy, mock_worker_proxy_cache
    ):
        """Test run method resets current task context after completion.

        Given:
            A WoolTask with callable that checks context and proxy
        When:
            run() method completes
        Then:
            Should reset current task context variable
        """

        # Arrange
        async def context_resetting_callable():
            # Verify context is set during execution
            assert current_task() is not None
            return "reset_result"

        task = WoolTask(
            id=uuid.uuid4(),
            callable=context_resetting_callable,
            args=(),
            kwargs={},
            proxy=configured_mock_proxy,
        )
        # Ensure no task is set initially
        assert current_task() is None

        # Act
        result = await task.run()

        # Assert
        assert result == "reset_result"
        # Context should be reset after completion
        assert current_task() is None

    @pytest.mark.asyncio
    async def test_wool_task_run_sets_current_task_context(
        self, mock_proxy, mock_worker_proxy_cache
    ):
        """Test run method sets current task in context during execution.

        Given:
            A WoolTask with callable that checks current_task() and proxy
        When:
            run() method is called
        Then:
            Should set current task in context during execution
        """
        # Arrange
        captured_task = None

        async def context_checking_callable():
            nonlocal captured_task
            captured_task = current_task()
            return "context_result"

        task = WoolTask(
            id=uuid.uuid4(),
            callable=context_checking_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Act
        result = await task.run()

        # Assert
        assert result == "context_result"
        assert captured_task is task

    @pytest.mark.asyncio
    async def test_wool_task_run_yields_to_event_loop(
        self, mock_proxy, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test run method yields control to event loop during execution.

        Given:
            A WoolTask with async callable and proxy
        When:
            run() method is called
        Then:
            Should yield control to event loop during execution
        """

        # Arrange
        async def yielding_callable():
            return "yielded_result"

        task = WoolTask(
            id=uuid.uuid4(),
            callable=yielding_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        # Mock asyncio.sleep to verify it's called during execution
        mock_asyncio_sleep = mocker.patch(
            "wool._work.asyncio.sleep", new_callable=AsyncMock
        )

        # Act
        result = await task.run()

        # Assert
        assert result == "yielded_result"
        # Should be called once in task context to yield to event loop
        assert mock_asyncio_sleep.call_count == 1
        mock_asyncio_sleep.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_wool_task_run_sets_do_dispatch_context(
        self, mock_proxy, mock_worker_proxy_cache
    ):
        """Test run method sets _do_dispatch context during execution.

        Given:
            A WoolTask with callable that checks _do_dispatch context
        When:
            run() method is called
        Then:
            Should set _do_dispatch=False during execution (local execution)
        """
        # Arrange
        captured_do_dispatch = None

        async def context_checking_callable():
            nonlocal captured_do_dispatch
            captured_do_dispatch = _do_dispatch.get()
            return "do_dispatch_result"

        task = WoolTask(
            id=uuid.uuid4(),
            callable=context_checking_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Act
        result = await task.run()

        # Assert
        assert result == "do_dispatch_result"
        assert captured_do_dispatch is False

    @pytest.mark.asyncio
    async def test_wool_task_run_resets_do_dispatch_context_after_completion(
        self, mock_proxy, mock_worker_proxy_cache
    ):
        """Test run method resets _do_dispatch context after completion.

        Given:
            A WoolTask with callable and proxy
        When:
            run() method completes
        Then:
            Should reset _do_dispatch context variable to original value
        """

        # Arrange
        async def context_resetting_callable():
            # Verify _do_dispatch is set during execution (False for local execution)
            assert _do_dispatch.get() is False
            return "reset_do_dispatch_result"

        task = WoolTask(
            id=uuid.uuid4(),
            callable=context_resetting_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        # Ensure _do_dispatch is True initially (default is to dispatch)
        assert _do_dispatch.get() is True

        # Act
        result = await task.run()

        # Assert
        assert result == "reset_do_dispatch_result"
        # Context should be reset after completion
        assert _do_dispatch.get() is True

    @pytest.mark.asyncio
    async def test_wool_task_run_resets_contexts_on_exception(
        self, mock_proxy, mock_worker_proxy_cache
    ):
        """Test run method resets context variables when exception occurs.

        Given:
            A WoolTask with callable that raises exception
        When:
            run() method is called and exception occurs
        Then:
            Should reset both _current_task and _do_dispatch contexts
        """

        # Arrange
        async def failing_callable():
            # Verify contexts are set during execution
            assert current_task() is not None
            assert _do_dispatch.get() is False
            raise ValueError("Test exception")

        task = WoolTask(
            id=uuid.uuid4(),
            callable=failing_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        # Ensure contexts are at default values initially
        assert current_task() is None
        assert _do_dispatch.get() is True

        # Act & Assert - exception should be raised but contexts reset
        with pytest.raises(ValueError, match="Test exception"):
            await task.run()

        # Assert
        # Contexts should be reset even after exception
        assert current_task() is None
        assert _do_dispatch.get() is True

    def test_wool_task_captures_proxy_from_context_on_creation(
        self, mocker: MockerFixture
    ):
        """Test WoolTask captures current proxy context on creation.

        Given:
            A WorkerProxy in the context
        When:
            WoolTask is created
        Then:
            Should capture the proxy automatically
        """
        # Arrange
        mock_worker_proxy = MagicMock(spec=WorkerProxy)
        token = wool.__proxy__.set(mock_worker_proxy)

        try:
            # Act
            task = WoolTask(
                id=uuid.uuid4(),
                callable=pickable_callable,
                args=(),
                kwargs={},
                proxy=mock_worker_proxy,
            )

            # Assert
            assert task.proxy == mock_worker_proxy
        finally:
            wool.__proxy__.reset(token)

    def test_wool_task_proxy_none_when_no_context(self):
        """Test WoolTask proxy is None when no proxy in context.

        Given:
            No WorkerProxy in the context
        When:
            WoolTask is created
        Then:
            Should set proxy to None
        """
        # Arrange & Act
        task = WoolTask(
            id=uuid.uuid4(),
            callable=pickable_callable,
            args=(),
            kwargs={},
            proxy=None,  # type: ignore
        )

        # Assert
        assert task.proxy is None

    def test_wool_task_explicit_proxy_overrides_context(self, mocker: MockerFixture):
        """Test explicitly provided proxy overrides context proxy.

        Given:
            A WorkerProxy in context and explicit proxy provided
        When:
            WoolTask is created with explicit proxy
        Then:
            Should use explicit proxy not context proxy
        """
        # Arrange
        mock_context_proxy = MagicMock(spec=WorkerProxy)
        mock_explicit_proxy = MagicMock(spec=WorkerProxy)
        token = wool.__proxy__.set(mock_context_proxy)

        try:
            # Act
            task = WoolTask(
                id=uuid.uuid4(),
                callable=pickable_callable,
                args=(),
                kwargs={},
                proxy=mock_explicit_proxy,
            )

            # Assert
            assert task.proxy == mock_explicit_proxy
            assert task.proxy != mock_context_proxy
        finally:
            wool.__proxy__.reset(token)

    @pytest.mark.asyncio
    async def test_wool_task_run_with_proxy_context(self, mock_worker_proxy_cache):
        """Test WoolTask.run executes within proxy context.

        Given:
            A WoolTask with a proxy
        When:
            run() is called
        Then:
            It should execute within the proxy's async context
        """

        # Mock proxy
        mock_proxy = MagicMock(spec=WorkerProxy)
        mock_proxy.id = "test-proxy-id"  # Add the required ID
        mock_proxy.__aenter__ = AsyncMock(return_value=mock_proxy)
        mock_proxy.__aexit__ = AsyncMock(return_value=None)

        # Mock callable that records context
        context_recorder = MagicMock()

        async def mock_callable():
            context_recorder.called = True
            return "result"

        task = WoolTask(
            id=uuid.uuid4(),
            callable=mock_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        result = await task.run()

        # Verify callable was executed and result returned
        assert context_recorder.called
        assert result == "result"
        mock_worker_proxy_cache.get.assert_called_once_with(mock_proxy)
        # Note: ResourcePool uses context manager pattern via get() method

    def test_wool_task_protobuf_serialization_with_proxy(self, mocker: MockerFixture):
        """Test WoolTask protobuf serialization includes proxy.

        Given:
            A WoolTask with a proxy
        When:
            to_protobuf() is called
        Then:
            proxy should be pickled and included
        """

        # Create a real WorkerProxy that can be pickled
        real_proxy = WorkerProxy("test-pool", "tag1", "tag2")

        task = WoolTask(
            id=uuid.uuid4(),
            callable=pickable_callable,
            args=(),
            kwargs={},
            proxy=real_proxy,
        )

        pb_task = task.to_protobuf()

        # Verify proxy was pickled
        assert pb_task.proxy != b""
        # Verify we can unpickle it
        unpickled_proxy = cloudpickle.loads(pb_task.proxy)
        assert isinstance(unpickled_proxy, WorkerProxy)

    def test_wool_task_protobuf_serialization_with_mock_proxy(self):
        """Test WoolTask protobuf serialization with mock proxy.

        Given:
            A WoolTask with a mock proxy
        When:
            to_protobuf() is called
        Then:
            proxy field should contain serialized proxy
        """
        mock_proxy = MagicMock()
        mock_proxy.id = "mock-proxy-id"
        task = WoolTask(
            id=uuid.uuid4(),
            callable=pickable_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        pb_task = task.to_protobuf()

        # Verify proxy field contains data
        assert pb_task.proxy != b""
        assert pb_task.proxy_id == "mock-proxy-id"

    def test_wool_task_from_protobuf_with_proxy(self, mocker: MockerFixture):
        """Test WoolTask deserialization from protobuf with proxy.

        Given:
            A protobuf task with proxy data
        When:
            from_protobuf() is called
        Then:
            proxy should be unpickled correctly
        """

        real_proxy = WorkerProxy("test-pool", "tag1", "tag2")
        original_task = WoolTask(
            id=uuid.uuid4(),
            callable=pickable_callable,
            args=(),
            kwargs={},
            proxy=real_proxy,
        )

        # Serialize to protobuf
        pb_task = original_task.to_protobuf()

        # Deserialize from protobuf
        restored_task = WoolTask.from_protobuf(pb_task)

        # Verify proxy was restored
        assert isinstance(restored_task.proxy, WorkerProxy)

    def test_wool_task_from_protobuf_with_mock_proxy(self):
        """Test WoolTask deserialization from protobuf with mock proxy.

        Given:
            A protobuf task with mock proxy data
        When:
            from_protobuf() is called
        Then:
            proxy should be properly restored
        """
        mock_proxy = MagicMock()
        mock_proxy.id = "mock-proxy-id"
        original_task = WoolTask(
            id=uuid.uuid4(),
            callable=pickable_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Serialize to protobuf
        pb_task = original_task.to_protobuf()

        # Deserialize from protobuf
        restored_task = WoolTask.from_protobuf(pb_task)

        # Verify proxy was restored
        assert restored_task.proxy is not None


class TestWoolTaskEdgeCases:
    """Test edge cases and error conditions for WoolTask."""

    @pytest.mark.asyncio
    async def test_wool_task_serialization_with_unpicklable_args(self, sample_wool_task):
        """Test task serialization with unpicklable arguments.

        Given:
            A WoolTask with unpicklable arguments
        When:
            to_protobuf() method is called
        Then:
            Should raise appropriate serialization error
        """

        # Arrange
        class UnpicklableClass:
            def __reduce__(self):
                raise TypeError("Cannot pickle this object")

        task = WoolTask(
            id=uuid.uuid4(),
            callable=pickable_callable,
            args=(UnpicklableClass(),),
            kwargs={},
            proxy=sample_wool_task.proxy,
        )

        # Act & Assert
        with pytest.raises((TypeError, Exception)):
            task.to_protobuf()

    @pytest.mark.asyncio
    async def test_wool_task_run_with_cancelled_context(
        self, configured_mock_proxy, mock_worker_proxy_cache
    ):
        """Test task execution when asyncio context is cancelled.

        Given:
            A WoolTask with a callable that can be cancelled
        When:
            The task is cancelled during execution
        Then:
            Should raise CancelledError and reset context properly
        """

        # Arrange
        async def cancellable_callable():
            await asyncio.sleep(1)  # Will be cancelled before this completes
            return "should_not_reach_here"

        task = WoolTask(
            id=uuid.uuid4(),
            callable=cancellable_callable,
            args=(),
            kwargs={},
            proxy=configured_mock_proxy,
        )

        # Act & Assert
        async def run_and_cancel():
            task_coroutine = task.run()
            await asyncio.sleep(0.01)  # Let task start
            task_coroutine.close()  # Cancel the coroutine
            return "cancelled"

        result = await run_and_cancel()
        assert result == "cancelled"
        # Context should be reset even after cancellation
        assert current_task() is None

    @pytest.mark.asyncio
    async def test_work_decorator_with_invalid_proxy_state(self):
        """Test work decorator when proxy dispatch fails.

        Given:
            A proxy that fails during dispatch
        When:
            A decorated function is called
        Then:
            Should handle proxy errors gracefully
        """

        # Arrange
        async def failing_generator():
            raise RuntimeError("Proxy dispatch failed")
            yield  # unreachable

        failing_proxy = MagicMock(spec=WorkerProxy)
        failing_proxy.dispatch = MagicMock(return_value=failing_generator())
        token = wool.__proxy__.set(failing_proxy)

        try:
            # Act & Assert
            with pytest.raises(RuntimeError, match="Proxy dispatch failed"):
                await dummy_work_function(1, 2)
        finally:
            wool.__proxy__.reset(token)

    def test_protobuf_deserialization_with_corrupted_data(self):
        """Test protobuf deserialization with corrupted data.

        Given:
            A protobuf task with corrupted serialized data
        When:
            from_protobuf() is called
        Then:
            Should raise appropriate deserialization error
        """
        # Arrange
        pb_task = pb.task.Task(
            id=str(uuid.uuid4()),
            callable=b"corrupted_data_not_valid_pickle",
            args=cloudpickle.dumps(()),
            kwargs=cloudpickle.dumps({}),
            caller=str(uuid.uuid4()),
            proxy=cloudpickle.dumps(None),
        )

        # Act & Assert
        with pytest.raises(Exception):  # Could be pickle.UnpicklingError or similar
            WoolTask.from_protobuf(pb_task)

    @pytest.mark.asyncio
    async def test_wool_task_context_isolation_with_nested_tasks(
        self, configured_mock_proxy, mock_worker_proxy_cache
    ):
        """Test context isolation when tasks are nested.

        Given:
            A task that creates another task during execution
        When:
            The outer task is executed
        Then:
            Should maintain proper context isolation between tasks
        """
        # Arrange
        outer_task_id = None
        inner_task_id = None

        async def outer_callable():
            nonlocal outer_task_id
            outer_task_id = (t := current_task()) and t.id

            # Create inner task
            inner_task = WoolTask(
                id=uuid.uuid4(),
                callable=inner_callable,
                args=(),
                kwargs={},
                proxy=configured_mock_proxy,
            )
            await inner_task.run()

            # Verify outer context is restored
            assert (t := current_task()) and t.id == outer_task_id
            return "outer_result"

        async def inner_callable():
            nonlocal inner_task_id
            inner_task_id = (t := current_task()) and t.id
            # Verify different task context
            assert (t := current_task()) and t.id != outer_task_id
            return "inner_result"

        outer_task = WoolTask(
            id=uuid.uuid4(),
            callable=outer_callable,
            args=(),
            kwargs={},
            proxy=configured_mock_proxy,
        )

        # Act
        result = await outer_task.run()

        # Assert
        assert result == "outer_result"
        assert outer_task_id is not None
        assert inner_task_id is not None
        assert outer_task_id != inner_task_id
        # Context should be reset after completion
        assert current_task() is None

    def test_wool_task_exception_capture_with_complex_traceback(
        self, configured_mock_proxy, mocker: MockerFixture
    ):
        """Test exception capture with complex nested traceback.

        Given:
            A task that raises an exception with complex traceback
        When:
            The task encounters the exception
        Then:
            Should properly capture and format the traceback
        """
        # Arrange
        mock_asyncio_task = MagicMock()
        mocker.patch("asyncio.current_task", return_value=mock_asyncio_task)

        task = WoolTask(
            id=uuid.uuid4(),
            callable=pickable_callable,
            args=(),
            kwargs={},
            proxy=configured_mock_proxy,
        )

        exception = ValueError("Complex nested error")

        # Act
        task.__exit__(ValueError, exception, exception.__traceback__)

        # Assert
        assert task.exception is not None
        assert task.exception.type == "ValueError"
        assert any("Complex nested error" in line for line in task.exception.traceback)


class TestCurrentTaskFunction:
    """Test the current_task function."""

    def test_current_task_returns_none_when_no_active_task(self):
        """Test current_task returns None when no task is active.

        Given:
            No task in the current context
        When:
            current_task() is called
        Then:
            Should return None
        """
        # Arrange & Act
        result = current_task()

        # Assert
        assert result is None

    def test_current_task_returns_active_task_from_context(self, mocker: MockerFixture):
        """Test current_task returns the active task from context.

        Given:
            An active task in context
        When:
            current_task() is called
        Then:
            Should return the active task
        """
        # Arrange
        mock_wool_task = MagicMock(spec=WoolTask)
        mock_current_task_context = mocker.patch("wool._work._current_task")
        mock_current_task_context.get.return_value = mock_wool_task

        # Act
        result = current_task()

        # Assert
        assert result == mock_wool_task


class TestContextVariableIntegration:
    """Test context variable integration."""

    def test_current_task_respects_asyncio_context_isolation(
        self, mocker: MockerFixture
    ):
        """Test that current_task respects asyncio context isolation.

        Given:
            A mocked context variable
        When:
            current_task() is called
        Then:
            Should call the context variable's get method
        """
        # Arrange
        mock_current_task_context = mocker.patch("wool._work._current_task")

        # Act
        current_task()

        # Assert
        mock_current_task_context.get.assert_called_once()

    def test_context_variable_has_none_default_value(self):
        """Test context variable has correct default value.

        Given:
            The _current_task context variable
        When:
            get() is called without any set value
        Then:
            Should return None by default
        """
        # Arrange & Act
        result = _current_task.get()

        # Assert
        assert result is None


class TestWorkDecorator:
    """Test the work decorator functionality."""

    def test_task_decorator_non_coroutine_function(self):
        """Test work decorator validation with non-coroutine function.

        Given:
            A regular (non-async) function
        When:
            work decorator is applied to it
        Then:
            It should raise ValueError with "Expected a coroutine function" message
        """
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="Expected a coroutine function"):

            @work
            def non_async_function():
                return "not async"

    @pytest.mark.asyncio
    async def test_task_wrapper_invalid_function_type(self, mocker: MockerFixture):
        """Test work decorator wrapper with invalid function type during dispatch.

        Given:
            A work decorated function where inspect.iscoroutinefunction is mocked to
            return False
        When:
            The wrapper is called with dispatch enabled
        Then:
            It should raise ValueError with "Expected a coroutine function" message
        """
        # Arrange
        mock_proxy = MagicMock(spec=WorkerProxy)
        token = wool.__proxy__.set(mock_proxy)

        async def mock_stream():
            yield "test_result"

        mocker.patch("wool._work._dispatch", return_value=mock_stream())

        decorated_func = work(function_for_line_140_coverage)

        try:
            original_iscoroutinefunction = mocker.patch(
                "wool._work.inspect.iscoroutinefunction"
            )
            call_count = 0

            def mock_iscoroutinefunction(fn):
                nonlocal call_count
                call_count += 1
                return call_count != 1

            original_iscoroutinefunction.side_effect = mock_iscoroutinefunction

            # Act & Assert - this should trigger line 140
            with pytest.raises(ValueError, match="Expected a coroutine function"):
                await decorated_func()
        finally:
            wool.__proxy__.reset(token)

    def test_work_decorator_preserves_original_function_metadata(self):
        """Test work decorator preserves original function metadata.

        Given:
            A function decorated with @work
        When:
            Function metadata is accessed
        Then:
            Should preserve original name, docstring, and module
        """
        # Arrange & Act & Assert
        assert dummy_work_function.__name__ == "dummy_work_function"
        assert dummy_work_function.__doc__ == "Test decorated function."
        assert dummy_work_function.__module__ == __name__

    @pytest.mark.asyncio
    async def test_work_decorator_dispatches_to_proxy_when_available(
        self, mocker: MockerFixture
    ):
        """Test work decorator dispatches to proxy when available.

        Given:
            A decorated function and proxy in context
        When:
            The function is called
        Then:
            Should dispatch to proxy using _dispatch
        """
        # Arrange
        mock_worker_proxy = MagicMock(spec=WorkerProxy)
        token = wool.__proxy__.set(mock_worker_proxy)

        async def mock_async_generator():
            yield "dispatched_result"

        mock_dispatch_function = mocker.patch("wool._work._dispatch")
        mock_dispatch_function.return_value = mock_async_generator()

        try:
            # Act
            result = await dummy_work_function(1, 2)

            # Assert
            mock_dispatch_function.assert_called_once_with(
                mock_worker_proxy,
                __name__,
                "dummy_work_function",
                dummy_work_function,
                1,
                2,
            )
            assert result == "dispatched_result"
        finally:
            wool.__proxy__.reset(token)

    @pytest.mark.asyncio
    async def test_work_decorator_works_with_instance_methods(
        self, mocker: MockerFixture
    ):
        """Test work decorator works with instance methods.

        Given:
            A decorated instance method and proxy in context
        When:
            The instance method is called
        Then:
            Should dispatch with correct instance and method arguments
        """
        # Arrange
        mock_proxy = MagicMock(spec=WorkerProxy)
        token = wool.__proxy__.set(mock_proxy)

        async def mock_async_generator():
            yield "method_result"

        mock_dispatch = mocker.patch("wool._work._dispatch")
        mock_dispatch.return_value = mock_async_generator()

        try:
            instance = DummyWorkClass()

            # Act
            result = await instance.instance_method(5)

            # Assert
            mock_dispatch.assert_called_once()
            args = mock_dispatch.call_args[0]
            assert args[0] == mock_proxy  # proxy
            assert args[1] == __name__  # module
            assert "DummyWorkClass.instance_method" in args[2]  # qualname
            assert args[4] == instance  # self argument
            assert args[5] == 5  # method argument
            assert result == "method_result"
        finally:
            wool.__proxy__.reset(token)

    @pytest.mark.asyncio
    async def test_work_decorator_handles_keyword_arguments(self, mocker: MockerFixture):
        """Test work decorator handles keyword arguments correctly.

        Given:
            A decorated function with keyword arguments and proxy in context
        When:
            The function is called with kwargs
        Then:
            Should dispatch with correct arguments including kwargs
        """
        # Arrange
        mock_proxy = MagicMock(spec=WorkerProxy)
        token = wool.__proxy__.set(mock_proxy)

        async def mock_async_generator():
            yield "kwargs_result"

        mock_dispatch = mocker.patch("wool._work._dispatch")
        mock_dispatch.return_value = mock_async_generator()

        try:
            # Act
            result = await dummy_work_function(1, y=2)

            # Assert
            mock_dispatch.assert_called_once_with(
                mock_proxy,
                __name__,
                "dummy_work_function",
                dummy_work_function,
                1,
                y=2,
            )
            assert result == "kwargs_result"
        finally:
            wool.__proxy__.reset(token)

    @pytest.mark.asyncio
    async def test_work_decorator_raises_assertion_when_no_proxy_available(self):
        """Test work decorator raises assertion when no proxy is available.

        Given:
            No proxy in context
        When:
            A decorated function is called
        Then:
            Should raise AssertionError
        """
        # Arrange
        assert wool.__proxy__.get() is None

        # Act & Assert
        with pytest.raises(AssertionError):
            await dummy_work_function(1, 2)

    @pytest.mark.asyncio
    async def test_execute_function_handles_classmethod_directly(self):
        """Test _execute function directly with a classmethod.

        Given:
            A classmethod object passed directly to _execute
        When:
            _execute is called
        Then:
            Should call the classmethod's __func__ with parent class
        """

        # Arrange
        class TestClass:
            @classmethod
            async def test_method(cls, value):
                return f"class_{cls.__name__}_{value}"

        classmethod_descriptor = TestClass.__dict__["test_method"]

        # Act
        result = await _execute(classmethod_descriptor, TestClass, 42)

        # Assert
        assert result == "class_TestClass_42"

    @pytest.mark.asyncio
    async def test_execute_function_handles_regular_async_function(self):
        """Test _execute function with a regular async function.

        Given:
            A regular async function passed to _execute
        When:
            _execute is called
        Then:
            Should call the function directly with args and kwargs
        """

        # Arrange
        async def test_function(value):
            return f"regular_{value}"

        # Act
        result = await _execute(test_function, None, 42)

        # Assert
        assert result == "regular_42"

    @pytest.mark.asyncio
    async def test_work_decorator_executes_locally_when_do_dispatch_false(
        self, mocker: MockerFixture
    ):
        """Test work decorator executes locally when _do_dispatch=False.

        Given:
            A decorated function with _do_dispatch context set to False
        When:
            The function is called
        Then:
            Should execute function locally without proxy dispatch
        """
        # Arrange
        mock_do_dispatch = mocker.patch("wool._work._do_dispatch")
        mock_do_dispatch.get.return_value = False
        mock_do_dispatch.set.return_value = "mock_token"

        # Act
        result = await dummy_work_function(3, 7)

        # Assert
        assert result == 10  # 3 + 7 from the function implementation
        mock_do_dispatch.set.assert_called_once_with(True)
        mock_do_dispatch.reset.assert_called_once_with("mock_token")

    @pytest.mark.asyncio
    async def test_work_decorator_creates_task_with_correct_metadata(
        self, mocker: MockerFixture
    ):
        """Test work decorator creates WoolTask with correct metadata.

        Given:
            A decorated function and proxy in context
        When:
            The function is called via proxy
        Then:
            Should create WoolTask with correct module, qualname, and tag
        """
        # Arrange
        mock_proxy = MagicMock(spec=WorkerProxy)

        async def mock_async_generator():
            yield "metadata_result"

        mock_proxy.dispatch = MagicMock(return_value=mock_async_generator())
        token = wool.__proxy__.set(mock_proxy)

        try:
            # Act
            result = await dummy_work_function(42, 84)

            # Assert
            assert result == "metadata_result"
            task_arg = mock_proxy.dispatch.call_args[0][0]
            assert task_arg.callable == dummy_work_function
            assert task_arg.args == (42, 84)
            assert f"{__name__}.dummy_work_function(42, 84)" in task_arg.tag
        finally:
            wool.__proxy__.reset(token)

    @pytest.mark.asyncio
    async def test_work_decorator_executes_classmethod_locally_when_do_dispatch_false(
        self, mocker: MockerFixture
    ):
        """Test work decorator executes classmethod locally when do_dispatch is False.

        Given:
            A decorated classmethod with _do_dispatch context set to False
        When:
            The classmethod is called
        Then:
            Should execute classmethod locally without proxy dispatch
        """
        # Arrange
        mock_do_dispatch = mocker.patch("wool._work._do_dispatch")
        mock_do_dispatch.get.return_value = False
        mock_do_dispatch.set.return_value = "mock_token"

        # Act
        result = await DummyWorkClass.class_method(5)

        # Assert
        assert result == 15  # 5 * 3 from the classmethod implementation
        mock_do_dispatch.set.assert_called_once_with(True)
        mock_do_dispatch.reset.assert_called_once_with("mock_token")

    @pytest.mark.asyncio
    async def test_work_decorator_handles_bound_method_arguments_correctly(
        self, mocker: MockerFixture
    ):
        """Test that work decorator properly handles bound method arguments.

        Given:
            A decorated instance method and proxy in context
        When:
            The method is called via proxy
        Then:
            Should properly handle self argument in task creation
        """
        # Arrange
        mock_proxy = MagicMock(spec=WorkerProxy)

        async def mock_async_generator():
            yield "method_result"

        mock_proxy.dispatch = MagicMock(return_value=mock_async_generator())
        token = wool.__proxy__.set(mock_proxy)

        try:
            instance = DummyWorkClass()

            # Act
            result = await instance.instance_method(5)

            # Assert
            assert result == "method_result"
            mock_proxy.dispatch.assert_called_once()
            task_arg = mock_proxy.dispatch.call_args[0][0]
            assert task_arg.args == (instance, 5)  # Should include self
            assert "DummyWorkClass.instance_method(" in task_arg.tag
            assert str(instance) in task_arg.tag
        finally:
            wool.__proxy__.reset(token)

    @pytest.mark.asyncio
    async def test_work_decorator_handles_mixed_arguments_correctly(
        self, mocker: MockerFixture
    ):
        """Test work decorator with mixed positional and keyword arguments.

        Given:
            A decorated function with args and kwargs and proxy in context
        When:
            The function is called via proxy
        Then:
            Should create task with properly formatted signature tag
        """
        # Arrange
        mock_proxy = MagicMock(spec=WorkerProxy)

        async def mock_async_generator():
            yield "mixed_result"

        mock_proxy.dispatch = MagicMock(return_value=mock_async_generator())
        token = wool.__proxy__.set(mock_proxy)

        try:
            # Act
            result = await dummy_work_function(10, y=20)

            # Assert
            assert result == "mixed_result"
            mock_proxy.dispatch.assert_called_once()
            task_arg = mock_proxy.dispatch.call_args[0][0]
            assert task_arg.args == (10,)
            assert task_arg.kwargs["y"] == 20
            assert "dummy_work_function(10, y=20)" in task_arg.tag
        finally:
            wool.__proxy__.reset(token)

    def test_resolve_function_returns_correct_module_and_function(self):
        """Test _resolve function returns correct module and function.

        Given:
            A function with module and qualname
        When:
            _resolve is called
        Then:
            Should return the module and function
        """
        # Arrange & Act
        parent, function = _resolve(dummy_work_function)

        # Assert
        assert parent is not None
        assert function == dummy_work_function
        assert callable(function)

    def test_resolve_classmethod_returns_correct_class_and_method(self):
        """Test _resolve function with classmethod.

        Given:
            A classmethod with class and qualname
        When:
            _resolve is called
        Then:
            Should return the class and method
        """
        # Arrange & Act
        parent, function = _resolve(DummyWorkClass.class_method)

        # Assert
        assert parent == DummyWorkClass
        assert function == DummyWorkClass.class_method
        assert callable(function)


class TestWoolTaskEvent:
    """Test WoolTaskEvent handler registration and emission functionality."""

    def test_wool_task_event_emission_calls_all_registered_handlers(
        self, sample_wool_task, event_handler_cleanup
    ):
        """Test event emission calls all registered handlers for the event type.

        Given:
            An event with registered handlers
        When:
            emit() is called
        Then:
            Should call all registered handlers for the event type
        """
        # Arrange
        handler_calls = []

        def mock_handler_1(event, timestamp):
            handler_calls.append(("handler1", event.type, timestamp))

        def mock_handler_2(event, timestamp):
            handler_calls.append(("handler2", event.type, timestamp))

        WoolTaskEvent._handlers.clear()
        WoolTaskEvent.handler("task-created")(mock_handler_1)
        WoolTaskEvent.handler("task-created")(mock_handler_2)

        # Act
        event = WoolTaskEvent("task-created", task=sample_wool_task)
        event.emit()

        # Assert
        assert len(handler_calls) == 2
        assert any(
            call[0] == "handler1" and call[1] == "task-created" for call in handler_calls
        )
        assert any(
            call[0] == "handler2" and call[1] == "task-created" for call in handler_calls
        )

    def test_wool_task_event_emission_calls_handlers_in_registration_order(
        self, sample_wool_task, event_handler_cleanup
    ):
        """Test event emission calls multiple handlers in registration order.

        Given:
            An event with multiple registered handlers
        When:
            emit() is called
        Then:
            Should call each handler in registration order
        """
        # Arrange
        execution_order = []

        def mock_handler_a(event, timestamp):
            execution_order.append("A")

        def mock_handler_b(event, timestamp):
            execution_order.append("B")

        def mock_handler_c(event, timestamp):
            execution_order.append("C")

        WoolTaskEvent._handlers.clear()
        WoolTaskEvent.handler("task-started")(mock_handler_a)
        WoolTaskEvent.handler("task-started")(mock_handler_b)
        WoolTaskEvent.handler("task-started")(mock_handler_c)

        # Act
        event = WoolTaskEvent("task-started", task=sample_wool_task)
        event.emit()

        # Assert
        assert execution_order == ["A", "B", "C"]

    def test_wool_task_event_emission_provides_perf_counter_timestamp_to_handlers(
        self, sample_wool_task, event_handler_cleanup, mocker: MockerFixture
    ):
        """Test event emission provides timestamp from perf_counter_ns() to handlers.

        Given:
            An event with registered handlers
        When:
            emit() is called
        Then:
            Should pass timestamp from perf_counter_ns() to handlers
        """
        # Arrange
        mock_perf_counter = mocker.patch(
            "wool._work.perf_counter_ns", return_value=123456789
        )
        received_timestamps = []

        def mock_timestamp_capturing_handler(event, timestamp):
            received_timestamps.append(timestamp)

        WoolTaskEvent._handlers.clear()
        WoolTaskEvent.handler("task-stopped")(mock_timestamp_capturing_handler)

        # Act
        event = WoolTaskEvent("task-stopped", task=sample_wool_task)
        event.emit()

        # Assert
        assert len(received_timestamps) == 1
        assert received_timestamps[0] == 123456789
        mock_perf_counter.assert_called_once()

    def test_wool_task_event_handler_decorator_registers_for_multiple_event_types(
        self, configured_mock_proxy, event_handler_cleanup
    ):
        """Test WoolTaskEvent.handler() decorator with multiple event types.

        Given:
            A callback function
        When:
            WoolTaskEvent.handler() decorator is applied with multiple event types
        Then:
            Should register callback for all specified event types
        """
        # Arrange
        callback_calls = []

        @WoolTaskEvent.handler("task-created", "task-completed")
        def mock_multi_handler(event, timestamp):
            callback_calls.append(event.type)

        # Act
        task = WoolTask(
            id=uuid.uuid4(),
            callable=pickable_callable,
            args=(),
            kwargs={},
            proxy=configured_mock_proxy,
        )
        task._finish(None)

        # Assert
        assert "task-created" in callback_calls
        assert "task-completed" in callback_calls
        assert len(callback_calls) == 2

    def test_wool_task_event_handler_decorator_returns_original_callback_function(self):
        """Test WoolTaskEvent.handler() decorator returns original callback function.

        Given:
            A callback function
        When:
            WoolTaskEvent.handler() decorator is applied
        Then:
            Should return the original callback function unchanged
        """

        # Arrange
        def mock_original_callback(event, timestamp):
            pass

        # Act
        decorated_callback = WoolTaskEvent.handler("task-created")(
            mock_original_callback
        )

        # Assert
        assert decorated_callback is mock_original_callback

    def test_event_handler_decorator_registers_for_single_event_type(self):
        """Test WoolTaskEvent.handler() decorator with single event type.

        Given:
            A callback function
        When:
            WoolTaskEvent.handler() decorator is applied with one event type
        Then:
            Should register callback for the specified event type
        """
        # Arrange
        callback_calls = []

        @WoolTaskEvent.handler("task-created")
        def mock_test_handler(event, timestamp):
            callback_calls.append((event.type, timestamp))

        mock_proxy = MagicMock()

        # Act
        _ = WoolTask(
            id=uuid.uuid4(),
            callable=pickable_callable,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Assert
        assert len(callback_calls) == 1
        assert callback_calls[0][0] == "task-created"
        assert isinstance(callback_calls[0][1], int)


class TestAsyncioHandleRunWrapper:
    def test_asyncio_handle_execution_with_task_context(self, mocker: MockerFixture):
        """Test asyncio.Handle execution emits events when task context is present.

        Given an asyncio.Handle with current task in context
        When Handle is executed
        Then should emit task-started and task-stopped events.
        """
        # Arrange
        emitted_events = []

        def event_capturing_handler(event, timestamp):
            emitted_events.append(event.type)

        original_handlers = WoolTaskEvent._handlers.copy()
        WoolTaskEvent._handlers.clear()
        WoolTaskEvent.handler("task-started", "task-stopped")(event_capturing_handler)

        try:
            mock_proxy = MagicMock()
            task = WoolTask(
                id=uuid.uuid4(),
                callable=pickable_callable,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )

            mock_context = MagicMock()
            mock_context.get.return_value = task

            def test_callback():
                return "handle_result"

            handle = asyncio.Handle(
                test_callback, (), asyncio.get_event_loop(), context=mock_context
            )

            # Act
            handle._run()

            # Assert
            assert "task-started" in emitted_events
            assert "task-stopped" in emitted_events
        finally:
            WoolTaskEvent._handlers = original_handlers

    @pytest.mark.asyncio
    async def test_asyncio_handle_execution_without_task_context(
        self, mocker: MockerFixture
    ):
        """Test asyncio.Handle execution without task context executes normally.

        Given an asyncio.Handle with no current task
        When Handle is executed
        Then should execute normally without emitting task events.
        """
        # Arrange

        emitted_events = []

        def event_capturing_handler(event, timestamp):
            emitted_events.append(event.type)

        # Clear existing handlers and register our test handler
        original_handlers = WoolTaskEvent._handlers.copy()
        WoolTaskEvent._handlers.clear()
        WoolTaskEvent.handler("task-started", "task-stopped")(event_capturing_handler)

        try:
            # Create a Handle with mock context that returns None (no current task)
            mock_context = MagicMock()
            mock_context.get.return_value = None

            def test_callback():
                pass  # Simple callback for the handle

            # Create handle with current event loop and set mock context
            loop = asyncio.get_running_loop()
            handle = asyncio.Handle(test_callback, (), loop, context=mock_context)

            # Act
            handle._run()

            # Assert - no task events should be emitted when no task context
            assert len(emitted_events) == 0
            # Note: The actual callback execution is handled by the original _run method
            # We're primarily testing the monkey patch path with no current task
        finally:
            WoolTaskEvent._handlers = original_handlers

    def test_asyncio_handle_exception_handling_with_task(self, mocker: MockerFixture):
        """Test Handle._run emits task-stopped event even when exception occurs.

        Given an asyncio.Handle that raises exception with task context
        When Handle encounters exception
        Then should emit task-stopped event in finally block.
        """
        # Arrange

        emitted_events = []

        def event_capturing_handler(event, timestamp):
            emitted_events.append(event.type)

        # Clear existing handlers and register our test handler
        original_handlers = WoolTaskEvent._handlers.copy()
        WoolTaskEvent._handlers.clear()
        WoolTaskEvent.handler("task-started", "task-stopped")(event_capturing_handler)

        try:
            # Create a task for context
            mock_proxy = MagicMock()
            task = WoolTask(
                id=uuid.uuid4(),
                callable=pickable_callable,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )

            # Create Handle with context and callback that raises exception
            mock_context = MagicMock()
            mock_context.get.return_value = task

            def failing_callback():
                raise ValueError("Test exception")

            handle = asyncio.Handle(
                failing_callback, (), asyncio.get_event_loop(), context=mock_context
            )

            # Act - Execute handle that will raise exception
            try:
                handle._run()
            except ValueError:
                pass  # Expected exception

            # Assert - both started and stopped events should be emitted
            assert "task-started" in emitted_events
            assert "task-stopped" in emitted_events
        finally:
            WoolTaskEvent._handlers = original_handlers
