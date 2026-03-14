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

from wool import protocol
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import TaskException
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.routine.task import current_task
from wool.runtime.routine.task import do_dispatch


class _PicklableProxy:
    """A simple picklable proxy for tests that cannot use fixtures."""

    def __init__(self):
        self.id = uuid4()

    async def dispatch(self, *args, **kwargs):
        async def _stream():
            yield "result"

        return _stream()


# --- Protocol conformance test classes ---


class TestWorkerProxyLike:
    """Tests for :py:class:`WorkerProxyLike` protocol conformance."""

    def test_positive_conformance(self, sample_task):
        """Test that a conforming proxy is accepted by Task.

        Given:
            A class that implements the ``id`` property and async
            ``dispatch`` method required by
            :py:class:`WorkerProxyLike`.
        When:
            An instance is passed as the ``proxy`` argument to
            :py:class:`Task`.
        Then:
            It should instantiate successfully and the proxy's
            methods should be callable.
        """

        # Arrange
        class ConformingProxy:
            @property
            def id(self):
                return uuid4()

            async def dispatch(self, task, *, timeout=None):
                yield "result"

        proxy = ConformingProxy()

        # Act
        task = sample_task(proxy=proxy)

        # Assert
        assert task.proxy is proxy
        assert hasattr(task.proxy, "id")
        assert callable(task.proxy.dispatch)

    def test_negative_conformance(self, sample_async_callable):
        """Test that a non-conforming proxy is rejected by Task.

        Given:
            A class that has an ``id`` property but is missing the
            ``dispatch`` method required by
            :py:class:`WorkerProxyLike`.
        When:
            An instance is passed as the ``proxy`` argument to
            :py:class:`Task`.
        Then:
            It should raise ``TypeError``.
        """

        # Arrange
        class NonConformingProxy:
            @property
            def id(self):
                return uuid4()

        proxy = NonConformingProxy()

        # Act & assert
        with pytest.raises(TypeError, match="proxy must conform to WorkerProxyLike"):
            Task(
                id=uuid4(),
                callable=sample_async_callable,
                args=(),
                kwargs={},
                proxy=proxy,
            )


# --- Module-level do_dispatch tests ---


def test_do_dispatch_with_default_context():
    """Test do_dispatch returns True with no active context.

    Given:
        No ``do_dispatch`` context manager is active.
    When:
        ``do_dispatch()`` is called without arguments.
    Then:
        It should return ``True``.
    """
    # Arrange, act, & assert
    assert do_dispatch() is True


def test_do_dispatch_with_false_flag():
    """Test do_dispatch returns False inside a False context.

    Given:
        A ``do_dispatch(False)`` context manager is active.
    When:
        ``do_dispatch()`` is called inside the context.
    Then:
        It should return ``False`` inside the context and
        ``True`` after exiting.
    """
    # Act & assert
    with do_dispatch(False):
        assert not do_dispatch()
    assert do_dispatch()


def test_do_dispatch_with_nested_contexts():
    """Test do_dispatch tracks the innermost nested context.

    Given:
        Nested ``do_dispatch`` context managers with outer
        ``True`` and inner ``False``.
    When:
        ``do_dispatch()`` is called at each level.
    Then:
        It should return the value matching the innermost active
        context and restore correctly on exit.
    """
    # Arrange, act, & assert
    with do_dispatch(True):
        assert do_dispatch() is True
        with do_dispatch(False):
            assert do_dispatch() is False
        assert do_dispatch() is True
    assert do_dispatch() is True


# --- Module-level current_task tests ---


@pytest.mark.asyncio
async def test_current_task_inside_task_context(sample_task, mock_worker_proxy_cache):
    """Test current_task returns the active Task during dispatch.

    Given:
        Execution is within a task context via ``dispatch()``.
    When:
        ``current_task()`` is called inside the task callable.
    Then:
        It should return the current :py:class:`Task` instance.
    """

    # Arrange
    async def test_callable():
        return current_task()

    task = sample_task(callable=test_callable)

    # Act
    result = await task.dispatch()

    # Assert
    assert result == task


def test_current_task_outside_task_context():
    """Test current_task returns None outside any task context.

    Given:
        Execution is outside any task context.
    When:
        ``current_task()`` is called.
    Then:
        It should return ``None``.
    """
    # Arrange & Act
    result = current_task()

    # Assert
    assert result is None


@pytest.mark.asyncio
async def test_current_task_with_nested_task_contexts(sample_task):
    """Test nested task contexts set caller to the outer task.

    Given:
        An outer task entered via ``with outer_task:``.
    When:
        An inner task is created inside the outer task's context.
    Then:
        It should set the inner task's ``caller`` to the outer
        task's ``id``.
    """
    # Arrange
    outer_task = sample_task()

    # Act
    with outer_task:
        inner_task = sample_task()

    # Assert
    assert inner_task.caller == outer_task.id


@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(depth=st.integers(min_value=2, max_value=5))
@pytest.mark.asyncio
async def test_current_task_with_variable_nesting_depth(depth, sample_task):
    """Test nested task context tracking at variable depth.

    Given:
        A nesting depth between 2 and 5.
    When:
        That many tasks are entered via nested ``with task:``
        blocks, each created inside the prior task's context.
    Then:
        It should maintain correct ``current_task()`` at each
        level and set each inner task's ``caller`` to the outer
        task's ID.
    """
    # Arrange — create first task outside any context
    from contextlib import ExitStack

    tasks = [sample_task()]

    # Act & assert — build nested contexts iteratively
    with ExitStack() as stack:
        stack.enter_context(tasks[0])
        assert current_task() is tasks[0]
        assert tasks[0].caller is None

        for i in range(1, depth):
            # Create inside prior task's context so __post_init__
            # picks up the caller
            task = sample_task()
            tasks.append(task)
            stack.enter_context(task)
            assert current_task() is task
            assert task.caller == tasks[i - 1].id

    # After exiting all contexts, current_task should be None
    assert current_task() is None


# --- TestTask ---


class TestTask:
    """Tests for :py:class:`Task`."""

    @pytest.mark.asyncio
    async def test___post_init___inside_task_context(self, sample_task):
        """Test post-init sets caller inside a task context.

        Given:
            An outer task entered via ``with outer_task:``.
        When:
            A new task is created inside the outer task's context.
        Then:
            It should set the inner task's ``caller`` to the outer
            task's ``id``.
        """
        # Arrange
        outer_task = sample_task()

        # Act
        with outer_task:
            inner_task = sample_task()

        # Assert
        assert inner_task.caller == outer_task.id

    def test___post_init___outside_task_context(self, sample_task):
        """Test post-init leaves caller as None without context.

        Given:
            No task context is active.
        When:
            A :py:class:`Task` is instantiated.
        Then:
            It should leave the ``caller`` field as ``None``.
        """
        # Arrange & Act
        task = sample_task()

        # Assert
        assert task.caller is None

    @pytest.mark.asyncio
    async def test___enter___with_coroutine_callable(self, sample_task):
        """Test __enter__ returns a callable for coroutine tasks.

        Given:
            A :py:class:`Task` with a coroutine callable.
        When:
            The task is used as a context manager.
        Then:
            It should return a callable from ``__enter__``.
        """
        # Arrange
        task = sample_task()

        # Act
        with task as run_method:
            # Assert
            assert callable(run_method)

    @pytest.mark.asyncio
    async def test___enter___with_async_generator(self, sample_task):
        """Test __enter__ returns a callable for generator tasks.

        Given:
            A :py:class:`Task` with an async generator callable.
        When:
            The task is used as a context manager.
        Then:
            It should return a callable from ``__enter__``.
        """

        # Arrange
        async def test_generator():
            yield "value"

        task = sample_task(callable=test_generator)

        # Act
        with task as run_method:
            # Assert
            assert callable(run_method)

    @pytest.mark.asyncio
    async def test___enter___with_invalid_callable(self, sample_task):
        """Test __enter__ raises ValueError for non-async callable.

        Given:
            A :py:class:`Task` with neither a coroutine nor an
            async generator callable.
        When:
            The task is used as a context manager.
        Then:
            It should raise ``ValueError``.
        """

        # Arrange
        def not_async():
            return "not async"

        task = sample_task(callable=not_async)

        # Act & assert
        with pytest.raises(
            ValueError,
            match="Expected coroutine function or async generator function",
        ):
            with task:
                pass

    @pytest.mark.asyncio
    async def test___exit___without_exception(self, sample_task):
        """Test __exit__ completes cleanly without exceptions.

        Given:
            A :py:class:`Task` context manager.
        When:
            The ``with`` block completes without exception.
        Then:
            It should exit cleanly.
        """
        # Arrange
        task = sample_task()

        # Act & assert (no exception should be raised)
        with task:
            pass

    @pytest.mark.asyncio
    async def test___exit___with_value_error(self, sample_task):
        """Test __exit__ captures ValueError as TaskException.

        Given:
            A :py:class:`Task` context manager.
        When:
            A ``ValueError`` is raised inside the ``with`` block.
        Then:
            It should attach a :py:class:`TaskException` to the
            task and propagate the exception.
        """
        # Arrange
        task = sample_task()

        # Act & assert
        async def run_with_exception():
            with pytest.raises(ValueError, match="test error"):
                with task:
                    await asyncio.sleep(0)
                    raise ValueError("test error")

        await run_with_exception()

        # Assert that exception was captured
        assert task.exception is not None
        assert task.exception.type == "ValueError"
        assert any("test error" in line for line in task.exception.traceback)

    @pytest.mark.asyncio
    async def test___exit___with_runtime_error(self, sample_task):
        """Test __exit__ captures RuntimeError as TaskException.

        Given:
            A :py:class:`Task` context manager.
        When:
            A ``RuntimeError`` is raised inside the ``with``
            block.
        Then:
            It should attach a :py:class:`TaskException` with the
            correct type and propagate the exception.
        """
        # Arrange
        task = sample_task()

        # Act
        async def run_with_exception():
            with pytest.raises(RuntimeError, match="runtime error"):
                with task:
                    await asyncio.sleep(0)
                    raise RuntimeError("runtime error")

        await run_with_exception()

        # Assert
        assert task.exception is not None
        assert task.exception.type == "RuntimeError"
        assert any("runtime error" in line for line in task.exception.traceback)

    @settings(max_examples=50, deadline=None)
    @given(
        task_id=st.uuids(),
        timeout=st.integers(min_value=0, max_value=3600),
        caller_id=st.one_of(st.none(), st.uuids()),
        tag=st.one_of(st.none(), st.text(min_size=1, max_size=100)),
    )
    @pytest.mark.asyncio
    async def test_to_protobuf_with_picklable_proxy(
        self,
        task_id,
        timeout,
        caller_id,
        tag,
    ):
        """Test protobuf round-trip preserves all public attributes.

        Given:
            A :py:class:`Task` with valid picklable data.
        When:
            ``from_protobuf(to_protobuf(task))`` is called.
        Then:
            It should produce a deserialized task equal to the
            original in all public attributes.
        """

        # Arrange
        async def test_callable():
            return "result"

        proxy = _PicklableProxy()
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

    @pytest.mark.asyncio
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(value_count=st.integers(min_value=0, max_value=10))
    async def test_dispatch_with_async_generator(
        self,
        value_count,
        mock_worker_proxy_cache,
    ):
        """Test dispatch yields all values from an async generator.

        Given:
            An async generator yielding *N* values
            (0 <= N <= 10).
        When:
            Dispatched via ``dispatch()`` and fully consumed.
        Then:
            It should receive all *N* values in order.
        """

        # Arrange
        async def test_generator():
            for i in range(value_count):
                yield i

        proxy = _PicklableProxy()
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

    @pytest.mark.asyncio
    async def test_from_protobuf_all_fields(
        self, sample_async_callable, picklable_proxy
    ):
        """Test from_protobuf deserializes all fields correctly.

        Given:
            A protobuf Task message with all fields populated.
        When:
            ``from_protobuf`` is called.
        Then:
            It should return a :py:class:`Task` with all fields
            correctly deserialized.
        """
        # Arrange
        task_id = uuid4()
        caller_id = uuid4()
        args = (1, 2, 3)
        kwargs = {"key": "value"}

        pb_task = protocol.Task(
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
        self, sample_async_callable, picklable_proxy
    ):
        """Test from_protobuf handles empty optional fields.

        Given:
            A protobuf Task message with optional fields empty.
        When:
            ``from_protobuf`` is called.
        Then:
            It should return a :py:class:`Task` with ``None`` or
            ``0`` for empty optional fields.
        """
        # Arrange
        task_id = uuid4()
        args = ()
        kwargs = {}

        pb_task = protocol.Task(
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

    def test_to_protobuf_all_fields(self, sample_async_callable, picklable_proxy):
        """Test to_protobuf serializes all fields correctly.

        Given:
            A :py:class:`Task` instance with all fields populated.
        When:
            ``to_protobuf()`` is called.
        Then:
            It should return a protobuf Task with all fields
            correctly serialized.
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

    def test_to_protobuf_none_optionals(self, sample_async_callable, picklable_proxy):
        """Test to_protobuf serializes None optionals as defaults.

        Given:
            A :py:class:`Task` instance with optional fields as
            ``None`` or ``0``.
        When:
            ``to_protobuf()`` is called.
        Then:
            It should return a protobuf Task with empty strings
            and ``0`` for optional fields.
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
        self, sample_async_callable, picklable_proxy
    ):
        """Test to_protobuf includes the protocol version.

        Given:
            A :py:class:`Task` instance.
        When:
            ``to_protobuf()`` is called.
        Then:
            It should include the protocol version in the
            ``version`` field.
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
        """Test protobuf round-trip preserves the version field.

        Given:
            Any PEP 440-like version string.
        When:
            A protobuf Task with that version is serialized and
            deserialized.
        Then:
            It should preserve the version field on the wire.
        """
        # Arrange
        proxy = _PicklableProxy()

        async def test_callable():
            return "result"

        pb_task = protocol.Task(
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

        # Act
        wire_bytes = pb_task.SerializeToString()
        parsed = protocol.Task()
        parsed.ParseFromString(wire_bytes)

        # Assert
        assert parsed.version == version

    @pytest.mark.asyncio
    async def test_dispatch_successful_execution(
        self,
        sample_task,
        mock_worker_proxy_cache,
    ):
        """Test dispatch executes the callable and returns result.

        Given:
            A :py:class:`Task` with a valid proxy pool in context.
        When:
            ``dispatch()`` is called.
        Then:
            It should execute the callable and return the result.
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
    async def test_dispatch_without_proxy_pool_raises_error(self, sample_task):
        """Test dispatch raises RuntimeError without a proxy pool.

        Given:
            No proxy pool is set in the context.
        When:
            ``dispatch()`` is called.
        Then:
            It should raise ``RuntimeError``.
        """
        # Arrange
        task = sample_task()

        # Act & assert
        with pytest.raises(
            RuntimeError,
            match="No proxy pool available for task execution",
        ):
            await task.dispatch()

    def test_to_protobuf_with_unpicklable_callable_fails(self, picklable_proxy):
        """Test to_protobuf fails with an unpicklable callable.

        Given:
            A :py:class:`Task` with an unpicklable callable.
        When:
            ``to_protobuf()`` is called.
        Then:
            It should fail with ``TypeError`` or
            ``AttributeError``.
        """
        # Arrange
        import _thread

        unpicklable_obj = _thread.allocate_lock()

        async def unpicklable_callable():
            return unpicklable_obj

        unpicklable_callable.__qualname__ = "unpicklable_callable"

        task = Task(
            id=uuid4(),
            callable=unpicklable_callable,
            args=(),
            kwargs={},
            proxy=picklable_proxy,
        )

        # Act & assert
        with pytest.raises((TypeError, AttributeError)):
            task.to_protobuf()

    @pytest.mark.asyncio
    async def test_dispatch_with_async_generator_callable(
        self,
        sample_task,
        mock_worker_proxy_cache,
    ):
        """Test dispatch yields all async generator values in order.

        Given:
            A :py:class:`Task` with an async generator callable
            and a valid proxy pool.
        When:
            ``dispatch()`` is called and iterated.
        Then:
            It should yield all values from the async generator in
            order.
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
    ):
        """Test dispatch returns the coroutine result.

        Given:
            A :py:class:`Task` with a coroutine callable and a
            valid proxy pool.
        When:
            ``dispatch()`` is called and awaited.
        Then:
            It should return the result from the coroutine.
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
    ):
        """Test dispatch raises ValueError for non-async callable.

        Given:
            A :py:class:`Task` with a non-async callable.
        When:
            ``dispatch()`` is called.
        Then:
            It should raise ``ValueError``.
        """

        # Arrange
        def not_async():
            return "not async"

        task = sample_task(callable=not_async)

        # Act & assert
        with pytest.raises(
            ValueError,
            match="Expected routine to be coroutine or async generator",
        ):
            _ = task.dispatch()

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_without_proxy_pool_raises_error(
        self,
        sample_task,
    ):
        """Test dispatch raises RuntimeError for generator without pool.

        Given:
            A :py:class:`Task` with an async generator callable
            and no proxy pool in context.
        When:
            ``dispatch()`` is called.
        Then:
            It should raise ``RuntimeError``.
        """

        # Arrange
        async def test_generator():
            yield "value"

        task = sample_task(callable=test_generator)

        # Act & assert
        with pytest.raises(
            RuntimeError,
            match="No proxy pool available for task execution",
        ):
            async for _ in task.dispatch():
                pass

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_raises_during_iteration(
        self,
        sample_task,
        mock_worker_proxy_cache,
    ):
        """Test dispatch propagates generator exceptions to caller.

        Given:
            A :py:class:`Task` with an async generator that raises
            during iteration.
        When:
            ``dispatch()`` is called and iterated.
        Then:
            It should propagate the exception to the caller.
        """

        # Arrange
        async def failing_generator():
            yield "first"
            raise ValueError("Generator error")

        task = sample_task(callable=failing_generator)

        # Act & assert
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
    ):
        """Test dispatch stops iteration on early break.

        Given:
            A :py:class:`Task` with an async generator callable.
        When:
            The async iterator is terminated early via ``break``.
        Then:
            It should stop after receiving the expected values.
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

        # Assert
        assert results == ["value_0", "value_1"]

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_multiple_values(
        self,
        sample_task,
        mock_worker_proxy_cache,
    ):
        """Test dispatch receives multiple yielded values in order.

        Given:
            A :py:class:`Task` with an async generator that yields
            multiple values.
        When:
            ``dispatch()`` is fully consumed.
        Then:
            It should receive all yielded values in correct order.
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
    ):
        """Test dispatch completes without values for empty generator.

        Given:
            A :py:class:`Task` with an async generator that yields
            zero values.
        When:
            ``dispatch()`` is called and consumed.
        Then:
            It should complete immediately without yielding any
            values.
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

        # Assert
        assert results == []


# --- TestTaskException ---


class TestTaskException:
    """Tests for :py:class:`TaskException`."""

    def test___init___with_type_and_traceback(self):
        """Test TaskException stores type and traceback correctly.

        Given:
            A valid exception type string and traceback lines.
        When:
            :py:class:`TaskException` is instantiated.
        Then:
            It should store the correct ``type`` and ``traceback``
            attributes.
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
