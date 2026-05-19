import _thread
import asyncio
import pickle
from typing import AsyncGenerator
from typing import Callable
from typing import Coroutine
from typing import cast
from uuid import uuid4

import cloudpickle
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from wool import protocol
from wool.runtime.context import RuntimeContext
from wool.runtime.context import dispatch_timeout
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import TaskException
from wool.runtime.routine.task import current_task
from wool.runtime.routine.task import do_dispatch
from wool.runtime.routine.task import routine_scope


class _PicklableProxy:
    """A simple picklable proxy for tests that cannot use fixtures."""

    def __init__(self):
        self.id = uuid4()

    async def dispatch(self, *args, **kwargs):
        async def _stream():
            yield "result"

        return _stream()


def _restore_picklable_proxy(proxy_id):
    """Reconstruct a _PicklableProxy with the supplied id."""
    proxy = _PicklableProxy()
    proxy.id = proxy_id
    return proxy


def _arbitrary_payloads():
    """Recursive Hypothesis strategy covering nested cloudpickle-picklable values."""
    primitives = st.one_of(
        st.none(),
        st.booleans(),
        st.text(),
        st.integers(),
        st.binary(),
        st.floats(allow_nan=False),
    )
    return st.recursive(
        primitives,
        lambda children: st.one_of(
            st.lists(children),
            st.tuples(children, children),
            st.dictionaries(st.text(), children),
        ),
        max_leaves=20,
    )


class _GuardedProxy(_PicklableProxy):
    """Proxy test double that adopts the Wool pickling protocol.

    Defines __wool_reduce__ for serialization through Wool's pickler and
    a __reduce_ex__ guard that raises TypeError to model a guarded type.
    Reconstruction returns a plain _PicklableProxy with the original id
    preserved, mirroring the production WorkerProxy reduce contract.
    """

    def __wool_reduce__(self):
        return (_restore_picklable_proxy, (self.id,))

    def __reduce_ex__(self, *_):
        raise TypeError("_GuardedProxy cannot be pickled via vanilla pickle/cloudpickle")


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
    async with routine_scope(task) as routine:
        result = await cast(Coroutine, routine)

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

    def test___post_init___without_explicit_context(self, sample_async_callable):
        """Test post-init auto-captures the caller's RuntimeContext.

        Given:
            A dispatch_timeout set in the caller's context and a Task
            constructed without an explicit runtime_context argument.
        When:
            The Task is constructed.
        Then:
            ``task.runtime_context`` is a RuntimeContext that, when
            entered in a fresh scope, restores the caller's
            dispatch_timeout.
        """
        # Arrange
        outer_token = dispatch_timeout.set(1.25)
        try:
            # Act
            task = Task(
                id=uuid4(),
                callable=sample_async_callable,
                args=(),
                kwargs={},
                proxy=_PicklableProxy(),
            )
        finally:
            dispatch_timeout.reset(outer_token)

        # Assert
        assert task.runtime_context is not None
        with task.runtime_context:
            assert dispatch_timeout.get() == 1.25

    @pytest.mark.asyncio
    async def test___enter___with_coroutine_callable(self, sample_task):
        """Test __enter__ returns a callable for coroutine tasks.

        Given:
            A :py:class:`Task` with a coroutine callable.
        When:
            The task is used as a context manager.
        Then:
            It should bind :data:`_current_task` to *self* for the
            duration of the ``with`` block.
        """
        # Arrange
        task = sample_task()

        # Act
        with task:
            # Assert
            assert current_task() is task

    @pytest.mark.asyncio
    async def test___enter___with_async_generator(self, sample_task):
        """Test __enter__ binds ``_current_task`` for an async-gen task.

        Given:
            A :py:class:`Task` with an async generator callable.
        When:
            The task is used as a context manager.
        Then:
            It should bind :data:`_current_task` to *self* for the
            duration of the ``with`` block.
        """

        # Arrange
        async def test_generator():
            yield "value"

        task = sample_task(callable=test_generator)

        # Act
        with task:
            # Assert
            assert current_task() is task

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
        task_msg = original_task.to_protobuf()
        deserialized_task = Task.from_protobuf(task_msg)

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

    def test_to_protobuf_round_trip_produces_distinct_objects(self):
        """Test the protobuf round-trip returns independent copies.

        Given:
            A :py:class:`Task` with picklable callable, args, kwargs,
            and proxy.
        When:
            The task is round-tripped via
            ``Task.from_protobuf(task.to_protobuf())``.
        Then:
            The restored ``callable``, ``args``, ``kwargs``, and
            ``proxy`` are each distinct objects from the originals —
            cloudpickle round-trips produce copies.
        """

        # Arrange
        async def test_callable():
            return "result"

        proxy = _PicklableProxy()
        args = (1, "test", [1, 2, 3])
        kwargs = {"key": "value"}
        original = Task(
            id=uuid4(),
            callable=test_callable,
            args=args,
            kwargs=kwargs,
            proxy=proxy,
        )

        # Act
        restored = Task.from_protobuf(original.to_protobuf())

        # Assert
        assert restored.callable is not original.callable
        assert restored.args is not original.args
        assert restored.kwargs is not original.kwargs
        assert restored.proxy is not original.proxy

    def test_to_protobuf_round_trip_copies_mutable_args(self):
        """Test the protobuf round-trip yields independent mutable args.

        Given:
            A :py:class:`Task` whose args contain a mutable list.
        When:
            The task is round-tripped and the restored arg is
            mutated.
        Then:
            The original arg is unaffected — the round-trip produced
            an independent copy, not a shared reference.
        """

        # Arrange
        async def test_callable():
            return "result"

        original_list = [1, 2, 3]
        original = Task(
            id=uuid4(),
            callable=test_callable,
            args=(original_list,),
            kwargs={},
            proxy=_PicklableProxy(),
        )

        # Act
        restored = Task.from_protobuf(original.to_protobuf())
        restored.args[0].append(4)

        # Assert
        assert restored.args[0] == [1, 2, 3, 4]
        assert original_list == [1, 2, 3]

    def test_to_protobuf_with_uncloudpicklable_arg_fails(self, picklable_proxy):
        """Test to_protobuf raises for a non-cloudpicklable positional arg.

        Given:
            A :py:class:`Task` whose ``args`` tuple contains a value
            that cloudpickle cannot serialize (a thread lock).
        When:
            ``to_protobuf()`` is called.
        Then:
            It should raise a pickling error.
        """

        # Arrange
        async def test_callable():
            return "result"

        task = Task(
            id=uuid4(),
            callable=test_callable,
            args=(_thread.allocate_lock(),),
            kwargs={},
            proxy=picklable_proxy,
        )

        # Act & assert
        with pytest.raises((TypeError, pickle.PicklingError)):
            task.to_protobuf()

    def test_to_protobuf_with_uncloudpicklable_kwarg_fails(self, picklable_proxy):
        """Test to_protobuf raises for a non-cloudpicklable keyword arg.

        Given:
            A :py:class:`Task` whose ``kwargs`` dict contains a value
            that cloudpickle cannot serialize (a thread lock).
        When:
            ``to_protobuf()`` is called.
        Then:
            It should raise a pickling error.
        """

        # Arrange
        async def test_callable():
            return "result"

        task = Task(
            id=uuid4(),
            callable=test_callable,
            args=(),
            kwargs={"lock": _thread.allocate_lock()},
            proxy=picklable_proxy,
        )

        # Act & assert
        with pytest.raises((TypeError, pickle.PicklingError)):
            task.to_protobuf()

    def test_to_protobuf_omits_serializer_field(
        self, sample_async_callable, picklable_proxy
    ):
        """Test to_protobuf produces a message without a serializer field.

        Given:
            A fully-populated :py:class:`Task`.
        When:
            ``to_protobuf()`` is called and the message is inspected.
        Then:
            The message has no ``serializer`` field.
        """
        # Arrange
        task = Task(
            id=uuid4(),
            callable=sample_async_callable,
            args=(1, 2),
            kwargs={"key": "value"},
            proxy=picklable_proxy,
            caller=uuid4(),
            timeout=30,
            tag="test_tag",
        )

        # Act
        task_msg = task.to_protobuf()

        # Assert
        field_names = [f.name for f in task_msg.DESCRIPTOR.fields]
        assert "serializer" not in field_names
        with pytest.raises(ValueError):
            task_msg.HasField("serializer")

    @settings(max_examples=50, deadline=None)
    @given(payload=_arbitrary_payloads())
    def test_to_protobuf_round_trip_copies_arbitrary_payloads(self, payload):
        """Test the protobuf round-trip copies arbitrary nested payloads.

        Given:
            Any nested cloudpicklable structure used as a
            :py:class:`Task`'s ``args`` and ``kwargs``.
        When:
            The task is round-tripped via
            ``Task.from_protobuf(task.to_protobuf())``.
        Then:
            The restored ``args`` and ``kwargs`` equal the originals
            yet are non-identical objects — the copy invariant holds
            for arbitrary payloads.
        """

        # Arrange
        async def test_callable():
            return "result"

        args = (payload,)
        kwargs = {"payload": payload}
        original = Task(
            id=uuid4(),
            callable=test_callable,
            args=args,
            kwargs=kwargs,
            proxy=_PicklableProxy(),
        )

        # Act
        restored = Task.from_protobuf(original.to_protobuf())

        # Assert
        assert restored.args == original.args
        assert restored.kwargs == original.kwargs
        assert restored.args is not original.args
        assert restored.kwargs is not original.kwargs

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
        async with routine_scope(task) as routine:
            async for value in cast(AsyncGenerator, routine):
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

        task_msg = protocol.Task(
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
        task = Task.from_protobuf(task_msg)

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

        task_msg = protocol.Task(
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
        task = Task.from_protobuf(task_msg)

        # Assert
        assert task.id == task_id
        assert task.caller is None
        assert task.timeout == 0
        assert task.tag is None

    @pytest.mark.asyncio
    async def test_from_protobuf_with_runtime_context(
        self, sample_async_callable, picklable_proxy
    ):
        """Test from_protobuf reads the RuntimeContext submessage.

        Given:
            A protobuf Task carrying an optional RuntimeContext
            submessage with a dispatch_timeout value.
        When:
            ``from_protobuf`` is called.
        Then:
            The reconstructed Task's ``context`` attribute is a
            RuntimeContext whose dispatch_timeout reflects the wire
            value.
        """
        # Arrange
        task_msg = protocol.Task(
            version=protocol.__version__,
            id=str(uuid4()),
            callable=cloudpickle.dumps(sample_async_callable),
            args=cloudpickle.dumps(()),
            kwargs=cloudpickle.dumps({}),
            caller="",
            proxy=cloudpickle.dumps(picklable_proxy),
            proxy_id=str(picklable_proxy.id),
            timeout=0,
            tag="",
            runtime_context=protocol.RuntimeContext(dispatch_timeout=12.5),
        )

        # Act
        task = Task.from_protobuf(task_msg)

        # Assert
        assert task.runtime_context is not None
        with task.runtime_context:
            assert dispatch_timeout.get() == 12.5

    @pytest.mark.asyncio
    async def test_dispatch_with_dispatch_timeout_on_coroutine(
        self, mock_worker_proxy_cache
    ):
        """Test coroutine dispatch applies context dispatch_timeout.

        Given:
            A coroutine Task with a RuntimeContext carrying
            ``dispatch_timeout=7.5`` and the process-wide
            dispatch_timeout ContextVar at its default.
        When:
            ``task.dispatch()`` completes.
        Then:
            The coroutine reads the applied dispatch_timeout value of
            ``7.5`` from :py:data:`wool.runtime.context.dispatch_timeout`.
        """
        # Arrange
        captured: list[float | None] = []

        async def capture_timeout():
            captured.append(dispatch_timeout.get())

        task = Task(
            id=uuid4(),
            callable=capture_timeout,
            args=(),
            kwargs={},
            proxy=_PicklableProxy(),
            runtime_context=RuntimeContext(dispatch_timeout=7.5),
        )

        # Act
        async with routine_scope(task) as routine:
            await cast(Coroutine, routine)

        # Assert
        assert captured == [7.5]

    @pytest.mark.asyncio
    async def test_dispatch_with_dispatch_timeout_on_async_generator(
        self, mock_worker_proxy_cache
    ):
        """Test async-gen dispatch applies context dispatch_timeout each iteration.

        Given:
            An async-generator Task with a RuntimeContext carrying
            ``dispatch_timeout=3.0`` yielding twice, and the
            process-wide dispatch_timeout ContextVar at its default.
        When:
            ``task.dispatch()`` is iterated to completion.
        Then:
            Each iteration's captured value is ``3.0``, confirming the
            context is entered on every frame.
        """
        # Arrange
        captured: list[float | None] = []

        async def capture_timeout_stream():
            captured.append(dispatch_timeout.get())
            yield 1
            captured.append(dispatch_timeout.get())
            yield 2

        task = Task(
            id=uuid4(),
            callable=capture_timeout_stream,
            args=(),
            kwargs={},
            proxy=_PicklableProxy(),
            runtime_context=RuntimeContext(dispatch_timeout=3.0),
        )

        # Act
        async with routine_scope(task) as routine:
            async for _ in cast(AsyncGenerator, routine):
                pass

        # Assert
        assert captured == [3.0, 3.0]

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
        task_msg = task.to_protobuf()

        # Assert
        assert task_msg.version != ""
        assert task_msg.id == str(task.id)
        deserialized_callable = cloudpickle.loads(task_msg.callable)
        assert callable(deserialized_callable)
        assert deserialized_callable.__name__ == task.callable.__name__
        assert cloudpickle.loads(task_msg.args) == task.args
        assert cloudpickle.loads(task_msg.kwargs) == task.kwargs
        assert task_msg.caller == str(caller_id)
        deserialized_proxy = cloudpickle.loads(task_msg.proxy)
        assert deserialized_proxy.id == task.proxy.id
        assert task_msg.proxy_id == str(task.proxy.id)
        assert task_msg.timeout == 30
        assert task_msg.tag == "test_tag"

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
        task_msg = task.to_protobuf()

        # Assert
        assert task_msg.caller == ""
        assert task_msg.timeout == 0
        assert task_msg.tag == ""

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
        task_msg = task.to_protobuf()

        # Assert
        assert task_msg.version == protocol.__version__

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

        task_msg = protocol.Task(
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
        wire_bytes = task_msg.SerializeToString()
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
        async with routine_scope(task) as routine:
            result = await cast(Coroutine, routine)

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
            It should raise ``RuntimeError`` from :func:`routine_scope`'s
            single-source-of-truth precondition (callers no longer
            duplicate the check at their entry points).
        """
        # Arrange
        task = sample_task()

        # Act & assert
        with pytest.raises(
            RuntimeError,
            match="wool.__proxy_pool__ is not initialized",
        ):
            async with routine_scope(task) as routine:
                await cast(Coroutine, routine)

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
        async with routine_scope(task) as routine:
            async for value in cast(AsyncGenerator, routine):
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
        async with routine_scope(task) as routine:
            result = await cast(Coroutine, routine)

        # Assert
        assert result == "coroutine_result"

    @pytest.mark.asyncio
    async def test_routine_scope_with_invalid_callable(
        self,
        sample_task,
        mock_worker_proxy_cache,
    ):
        """Test routine_scope raises ValueError for a non-async callable.

        Given:
            A :py:class:`Task` whose callable is neither a coroutine
            function nor an async generator function.
        When:
            :func:`routine_scope` is entered for the task.
        Then:
            It should raise :class:`ValueError`.
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
            async with routine_scope(task):
                pass

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
            It should raise ``RuntimeError`` from :func:`routine_scope`'s
            single-source-of-truth precondition.
        """

        # Arrange
        async def test_generator():
            yield "value"

        task = sample_task(callable=test_generator)

        # Act & assert
        with pytest.raises(
            RuntimeError,
            match="wool.__proxy_pool__ is not initialized",
        ):
            async with routine_scope(task) as routine:
                async for _ in cast(AsyncGenerator, routine):
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
            async with routine_scope(task) as routine:
                async for value in cast(AsyncGenerator, routine):
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
        async with routine_scope(task) as routine:
            async for value in cast(AsyncGenerator, routine):
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
        async with routine_scope(task) as routine:
            async for value in cast(AsyncGenerator, routine):
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
        async with routine_scope(task) as routine:
            async for value in cast(AsyncGenerator, routine):
                results.append(value)

        # Assert
        assert results == []

    def test_to_protobuf_with_guarded_proxy(self, sample_async_callable):
        """Test to_protobuf serializes a guarded proxy via wool.__serializer__.

        Given:
            A Task whose proxy adopts the Wool pickling protocol.
        When:
            to_protobuf() is called.
        Then:
            It should serialize the proxy via wool.__serializer__, which
            honors __wool_reduce__ instead of tripping the __reduce_ex__
            guard.
        """
        # Arrange
        proxy = _GuardedProxy()
        task = Task(
            id=uuid4(),
            callable=sample_async_callable,
            args=(),
            kwargs={},
            proxy=proxy,
        )

        # Act
        task_msg = task.to_protobuf()

        # Assert
        assert task_msg.proxy
        assert task_msg.proxy_id == str(proxy.id)

    def test_from_protobuf_with_guarded_proxy(self, sample_async_callable):
        """Test the to_protobuf / from_protobuf round-trip with a guarded proxy.

        Given:
            A Task whose proxy is a guarded type.
        When:
            The Task is serialized via to_protobuf() and deserialized via
            from_protobuf().
        Then:
            The restored Task's proxy and proxy_id should match the
            original.
        """
        # Arrange
        proxy = _GuardedProxy()
        original = Task(
            id=uuid4(),
            callable=sample_async_callable,
            args=(),
            kwargs={},
            proxy=proxy,
        )

        # Act
        restored = Task.from_protobuf(original.to_protobuf())

        # Assert
        assert restored.id == original.id
        assert isinstance(restored.proxy, _PicklableProxy)
        assert restored.proxy.id == proxy.id

    def test_from_protobuf_with_cloudpickle_fields(
        self, sample_async_callable, picklable_proxy
    ):
        """Test from_protobuf deserializes cloudpickle-encoded payload fields.

        Given:
            A protobuf Task whose payload fields are cloudpickle-encoded.
        When:
            ``from_protobuf()`` is called.
        Then:
            It should deserialize all fields correctly using cloudpickle.
        """
        # Arrange
        task_id = uuid4()
        args = (1, 2, 3)
        kwargs = {"key": "value"}

        task_msg = protocol.Task(
            version="0.1.0",
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
        task = Task.from_protobuf(task_msg)

        # Assert
        assert task.id == task_id
        assert task.args == args
        assert task.kwargs == kwargs

    def test_from_protobuf_with_invalid_payload_fails(self, picklable_proxy):
        """Test from_protobuf raises for a non-cloudpickle payload.

        Given:
            A :py:class:`protocol.Task` message whose ``callable``
            field holds bytes that are not a valid cloudpickle
            payload.
        When:
            ``Task.from_protobuf()`` is called.
        Then:
            It should raise an unpickling error.
        """
        # Arrange
        task_msg = protocol.Task(
            version="0.1.0",
            id=str(uuid4()),
            callable=b"not-a-pickle",
            args=cloudpickle.dumps(()),
            kwargs=cloudpickle.dumps({}),
            caller="",
            proxy=cloudpickle.dumps(picklable_proxy),
            proxy_id=str(picklable_proxy.id),
            timeout=0,
            tag="",
        )

        # Act & assert
        with pytest.raises((pickle.UnpicklingError, EOFError, ValueError, TypeError)):
            Task.from_protobuf(task_msg)


class TestRoutineScope:
    """Tests for :func:`wool.runtime.routine.task.routine_scope`."""

    @pytest.mark.asyncio
    async def test_routine_scope_with_null_runtime_context_asserts(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test :func:`routine_scope` asserts a non-None runtime context.

        ``Task.__post_init__`` always seeds ``runtime_context`` from
        ``RuntimeContext.get_current()``; a Task that bypasses
        ``__post_init__`` (impossible in normal use) and surfaces
        with ``runtime_context = None`` is a broken-by-construction
        invariant. The assertion surfaces the regression loudly
        rather than papering over it.

        Given:
            A Task whose ``runtime_context`` has been cleared after
            construction (bypassing ``__post_init__``)
        When:
            :func:`routine_scope` is entered
        Then:
            It should raise :class:`AssertionError`.
        """

        # Arrange
        async def trivial_routine():
            return "ran"

        task = sample_task(callable=trivial_routine)
        task.runtime_context = None

        # Act & assert
        with pytest.raises(AssertionError):
            async with routine_scope(task):
                pass

    @pytest.mark.asyncio
    async def test_routine_scope_without_proxy_pool(self, sample_task):
        """Test routine_scope raises when wool.__proxy_pool__ is unset.

        Given:
            ``wool.__proxy_pool__`` has no value set in the current
            context and a Task with a coroutine callable.
        When:
            ``routine_scope(task)`` is entered via ``async with``.
        Then:
            It should raise ``RuntimeError`` whose message starts
            with "wool.__proxy_pool__ is not initialized".
        """
        # Arrange
        task = sample_task()

        # Act & assert
        with pytest.raises(
            RuntimeError, match=r"^wool\.__proxy_pool__ is not initialized"
        ):
            async with routine_scope(task):
                pass

    @pytest.mark.asyncio
    async def test_routine_scope_with_coroutine_callable(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope yields an awaitable coroutine for coroutine tasks.

        Given:
            A Task whose callable is a coroutine function returning
            a sentinel and an active proxy pool.
        When:
            ``routine_scope(task)`` is entered and the yielded routine is
            awaited.
        Then:
            It should yield a coroutine object that resolves to the
            callable's return value.
        """

        # Arrange
        async def coro_callable():
            return "coro_result"

        task = sample_task(callable=coro_callable)

        # Act
        async with routine_scope(task) as routine:
            assert asyncio.iscoroutine(routine)
            result = await cast(Coroutine, routine)

        # Assert
        assert result == "coro_result"

    @pytest.mark.asyncio
    async def test_routine_scope_with_async_generator_callable(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope yields an iterable async generator for stream tasks.

        Given:
            A Task whose callable is an async-generator function that
            yields three values, plus an active proxy pool.
        When:
            ``routine_scope(task)`` is entered and the yielded routine is
            iterated.
        Then:
            It should yield an async generator producing the three
            values in order.
        """

        # Arrange
        async def gen_callable():
            yield 1
            yield 2
            yield 3

        task = sample_task(callable=gen_callable)

        # Act
        results = []
        async with routine_scope(task) as routine:
            async for value in cast(AsyncGenerator, routine):
                results.append(value)

        # Assert
        assert results == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_routine_scope_establishes_task_scope(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope binds current_task and disables dispatch routing.

        Given:
            A Task whose callable records ``current_task()`` and
            ``do_dispatch()`` inside the scope.
        When:
            The routine runs inside ``routine_scope()``.
        Then:
            It should observe ``current_task()`` is the task and
            ``do_dispatch()`` is False inside; both should return to
            the outer values after the CM exits.
        """
        # Arrange
        observed: dict[str, object] = {}

        async def record_scope():
            observed["task"] = current_task()
            observed["do_dispatch"] = do_dispatch()

        task = sample_task(callable=record_scope)
        outer_task_before = current_task()
        outer_dispatch_before = do_dispatch()

        # Act
        async with routine_scope(task) as routine:
            await cast(Coroutine, routine)

        # Assert
        assert observed["task"] is task
        assert observed["do_dispatch"] is False
        assert current_task() is outer_task_before
        assert do_dispatch() is outer_dispatch_before

    @pytest.mark.asyncio
    async def test_routine_scope_resets_proxy_token_on_exit(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope restores wool.__proxy__ on exit.

        Given:
            A Task whose callable records ``wool.__proxy__`` inside
            the scope; ``wool.__proxy__`` defaults to None outside.
        When:
            The routine runs inside ``routine_scope()`` and the CM is
            exited.
        Then:
            It should bind a non-None proxy inside the scope and
            restore ``wool.__proxy__`` to its prior value (None) on
            exit.
        """
        # Arrange
        observed: dict[str, object] = {}

        async def record_proxy():
            observed["proxy"] = wool.__proxy__.get()

        task = sample_task(callable=record_proxy)
        outer_proxy_before = wool.__proxy__.get()

        # Act
        async with routine_scope(task) as routine:
            await cast(Coroutine, routine)

        # Assert
        assert observed["proxy"] is not None
        assert wool.__proxy__.get() is outer_proxy_before
        assert wool.__proxy__.get() is None

    @pytest.mark.asyncio
    async def test_routine_scope_applies_runtime_context(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope applies the Task's RuntimeContext.

        Given:
            A Task with ``runtime_context=RuntimeContext(
            dispatch_timeout=4.5)`` and a coroutine that records
            the active dispatch_timeout value.
        When:
            The routine runs inside ``routine_scope()``.
        Then:
            It should observe ``dispatch_timeout == 4.5`` inside the
            scope.
        """
        # Arrange
        observed: dict[str, object] = {}

        async def record_timeout():
            observed["dispatch_timeout"] = dispatch_timeout.get()

        task = sample_task(
            callable=record_timeout,
            runtime_context=RuntimeContext(dispatch_timeout=4.5),
        )

        # Act
        async with routine_scope(task) as routine:
            await cast(Coroutine, routine)

        # Assert
        assert observed["dispatch_timeout"] == 4.5

    @pytest.mark.asyncio
    async def test_routine_scope_aclose_unconsumed_async_gen(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope acloses an async generator never iterated.

        Given:
            A Task whose callable is an async generator that yields
            once; the caller exits ``routine_scope()`` without iterating any
            value.
        When:
            ``async with routine_scope(task) as routine: pass`` runs (no
            iteration).
        Then:
            It should aclose the routine on exit (the generator's
            ``ag_frame`` becomes None) without raising.
        """

        # Arrange
        async def gen_callable():
            yield 1

        task = sample_task(callable=gen_callable)

        # Act
        async with routine_scope(task) as routine:
            captured_routine = routine

        # Assert — ag_frame is None after the generator has been closed
        assert captured_routine.ag_frame is None

    @pytest.mark.asyncio
    async def test_routine_scope_swallows_generator_exit_during_aclose(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope exits cleanly when the routine reacts to GeneratorExit.

        Given:
            An async-generator routine whose body catches and
            re-raises ``GeneratorExit`` on cleanup.
        When:
            The caller exits ``routine_scope()`` after iterating once.
        Then:
            It should swallow the GeneratorExit and exit the CM
            cleanly without surfacing an exception to the caller.
        """

        # Arrange
        async def naughty_gen():
            try:
                yield 1
                yield 2
            except GeneratorExit:
                raise

        task = sample_task(callable=naughty_gen)

        # Act — exit scoped after one iteration.  No exception
        # should escape the ``async with`` block on teardown.
        async with routine_scope(task) as routine:
            await cast(Coroutine, routine).__aiter__().__anext__()

        # Assert — control reached this point without raising; the
        # routine's GeneratorExit handling did not preempt teardown.
        assert routine.ag_frame is None

    @pytest.mark.asyncio
    async def test_routine_scope_propagates_internal_cancelled_during_aclose(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope propagates routine-internal CancelledError on aclose.

        Wool matches stdlib ``await agen.aclose()`` semantics:
        when a routine catches :class:`GeneratorExit` and raises
        :class:`asyncio.CancelledError` during its cleanup, the
        exception propagates from :func:`routine_scope`'s exit handler
        unchanged. Paired stdlib parity test
        :meth:`test_stdlib_aclose_propagates_internal_cancelled`
        pins the stdlib behavior so a future stdlib change
        signals that wool's parity needs revisiting.

        Given:
            An async-generator routine that raises
            ``asyncio.CancelledError`` during its aclose finally
            while the awaiting task's ``cancelling()`` count is 0
            (no external cancel pending).
        When:
            The caller exits ``routine_scope()`` after iterating once.
        Then:
            It should propagate the routine's
            :class:`asyncio.CancelledError` out of the
            ``async with`` block, matching stdlib semantics.
        """

        # Arrange
        async def naughty_gen():
            try:
                yield 1
                yield 2
            except GeneratorExit:
                # Routine raises CancelledError during aclose unwind;
                # the awaiting task is NOT being externally cancelled
                # (cancelling() == 0).
                raise asyncio.CancelledError()

        task = sample_task(callable=naughty_gen)

        # Act + Assert
        with pytest.raises(asyncio.CancelledError):
            async with routine_scope(task) as routine:
                it = routine.__aiter__()
                await it.__anext__()

    @pytest.mark.asyncio
    async def test_routine_scope_propagates_external_cancellation_during_aclose(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope re-raises CancelledError when externally cancelled.

        Given:
            An async-generator routine that raises
            ``asyncio.CancelledError`` during aclose while the
            awaiting task is being externally cancelled (the
            generator marks its own task as cancelling before
            re-raising, so ``current_task().cancelling()`` is
            greater than 0 when scoped's exit handler runs).
        When:
            The surrounding task is cancelled mid-iteration.
        Then:
            It should re-raise CancelledError so the surrounding
            task ends as cancelled.
        """

        # Arrange
        async def naughty_gen():
            try:
                yield 1
                yield 2
            except GeneratorExit:
                # Mark the awaiting task as being externally
                # cancelled, then raise CancelledError.  This puts
                # the awaiting task's cancelling() count above 0
                # so routine_scope() must re-raise rather than swallow.
                current = asyncio.current_task()
                assert current is not None
                current.cancel()
                raise asyncio.CancelledError()

        task = sample_task(callable=naughty_gen)

        async def body():
            async with routine_scope(task) as routine:
                await cast(Coroutine, routine).__aiter__().__anext__()

        # Wrap body() in its own asyncio.Task so the simulated
        # cancellation lands on that task and the test task can
        # cleanly observe the resulting CancelledError.
        wrapped = asyncio.ensure_future(body())

        # Act & assert
        with pytest.raises(asyncio.CancelledError):
            await wrapped

    @pytest.mark.asyncio
    async def test_routine_scope_propagates_runtime_error_when_routine_yields_during_ge(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope propagates the synthesized RuntimeError when
        a routine yields during ``GeneratorExit`` handling.

        Wool matches stdlib ``await agen.aclose()`` semantics:
        when a routine catches :class:`GeneratorExit` and yields
        a value (a protocol violation), CPython synthesizes
        ``RuntimeError("async generator ignored GeneratorExit")``
        from ``aclose``. Wool propagates this unchanged. Paired
        stdlib parity test
        :meth:`test_stdlib_aclose_raises_runtime_error_when_yielding_during_ge`
        pins the stdlib behavior.

        Given:
            An async-generator routine that catches
            :class:`GeneratorExit` and yields a value (protocol
            violation).
        When:
            The caller exits ``routine_scope()`` after iterating once.
        Then:
            It should propagate
            ``RuntimeError("async generator ignored
            GeneratorExit")`` out of the ``async with`` block.
        """

        async def yielding_gen():
            try:
                yield 1
                yield 2
            except GeneratorExit:
                yield "rude"

        task = sample_task(callable=yielding_gen)

        # Act + Assert
        with pytest.raises(RuntimeError, match="ignored GeneratorExit"):
            async with routine_scope(task) as routine:
                it = routine.__aiter__()
                await it.__anext__()

    @pytest.mark.asyncio
    async def test_routine_scope_with_coroutine_does_not_aclose(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope does not invoke aclose teardown for coroutines.

        Given:
            A Task whose callable is a coroutine that records normal
            completion (no GeneratorExit raised in cleanup).
        When:
            ``routine_scope()`` exits cleanly after awaiting the coroutine.
        Then:
            It should not invoke any async-generator aclose path —
            coroutines bypass the teardown branch and complete
            naturally without GeneratorExit.
        """
        # Arrange
        events: list[str] = []

        async def coro_callable():
            try:
                events.append("enter")
                return "done"
            except GeneratorExit:
                events.append("generator_exit")
                raise
            finally:
                events.append("finally")

        task = sample_task(callable=coro_callable)

        # Act
        async with routine_scope(task) as routine:
            result = await cast(Coroutine, routine)

        # Assert
        assert result == "done"
        assert "generator_exit" not in events
        assert events == ["enter", "finally"]

    @pytest.mark.asyncio
    async def test_routine_scope_propagates_caller_body_exception(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope propagates exceptions raised in the caller body.

        Given:
            A coroutine Task and a caller body that raises
            ``ValueError("boom")`` inside the ``async with`` block.
        When:
            The exception escapes the ``async with routine_scope(task):``
            block.
        Then:
            It should propagate ``ValueError("boom")`` to the caller.
        """

        # Arrange
        async def coro_callable():
            return "ok"

        task = sample_task(callable=coro_callable)

        # Act & assert
        with pytest.raises(ValueError, match="boom"):
            async with routine_scope(task) as routine:
                # Drain the coroutine to avoid an "unawaited
                # coroutine" warning, then raise from the body.
                await cast(Coroutine, routine)
                raise ValueError("boom")

        # Assert — the proxy pool's __aexit__ was invoked. With
        # plain ``async with proxy_pool.get(...)`` the dunder
        # protocol invokes ``__aexit__`` with the exc triple, but
        # we no longer test the specific arg shape (the contract
        # we care about is propagation, not the proxy's exc-info
        # access).
        aexit_mock = mock_worker_proxy_cache.get.return_value.__aexit__
        assert aexit_mock.await_count >= 1

    @pytest.mark.asyncio
    async def test_routine_scope_propagates_routine_exception_transparently(
        self, sample_task, mock_worker_proxy_cache
    ):
        """Test routine_scope propagates routine-raised exceptions unchanged.

        Given:
            An async-generator routine that yields a value and then
            raises ``RuntimeError`` on the next iteration.
        When:
            The caller iterates ``routine_scope()`` past the first value.
        Then:
            It should propagate the routine's exception unchanged.
        """

        # Arrange
        async def gen_callable():
            yield 1
            raise RuntimeError("routine-failure")

        task = sample_task(callable=gen_callable)

        # Act & assert
        results: list[int] = []
        with pytest.raises(RuntimeError, match="routine-failure"):
            async with routine_scope(task) as routine:
                async for value in cast(AsyncGenerator, routine):
                    results.append(value)

        # Assert — the first yielded value made it through
        assert results == [1]


class TestRuntimeContext:
    """Tests for :py:class:`RuntimeContext`."""

    def test___enter___and___exit___apply_inner_and_restore_outer_dispatch_timeout(self):
        """Test RuntimeContext sets and restores dispatch_timeout.

        Given:
            An outer dispatch_timeout value.
        When:
            A RuntimeContext block with a different dispatch_timeout
            is entered and exited.
        Then:
            Inside the block the stdlib ContextVar reflects the inner
            value; after exit the outer value is restored.
        """
        # Arrange
        outer_token = dispatch_timeout.set(2.0)
        try:
            # Act & Assert
            with RuntimeContext(dispatch_timeout=7.0):
                assert dispatch_timeout.get() == 7.0
            assert dispatch_timeout.get() == 2.0
        finally:
            dispatch_timeout.reset(outer_token)

    def test___enter___when_dispatch_timeout_unset(self):
        """Test an empty RuntimeContext does not touch dispatch_timeout.

        Given:
            A dispatch_timeout value set outside the block.
        When:
            A RuntimeContext with no dispatch_timeout argument is
            entered.
        Then:
            The stdlib ContextVar value is unchanged inside and after.
        """
        # Arrange
        outer_token = dispatch_timeout.set(5.0)
        try:
            # Act & Assert
            with RuntimeContext():
                assert dispatch_timeout.get() == 5.0
            assert dispatch_timeout.get() == 5.0
        finally:
            dispatch_timeout.reset(outer_token)

    def test_get_current_with_dispatch_timeout_set(self):
        """Test get_current snapshots the current dispatch_timeout.

        Given:
            A dispatch_timeout value set in the current context.
        When:
            ``RuntimeContext.get_current()`` is called.
        Then:
            Entering the returned context inside a fresh scope
            restores the captured value.
        """
        # Arrange
        outer_token = dispatch_timeout.set(4.0)
        try:
            # Act
            captured = RuntimeContext.get_current()
        finally:
            dispatch_timeout.reset(outer_token)

        # Assert
        with captured:
            assert dispatch_timeout.get() == 4.0

    def test_to_protobuf_with_dispatch_timeout_set(self):
        """Test to_protobuf serializes dispatch_timeout on the wire.

        Given:
            A RuntimeContext with a dispatch_timeout value.
        When:
            ``to_protobuf`` is called.
        Then:
            The protobuf message's ``dispatch_timeout`` field is set
            and ``HasField`` returns True.
        """
        # Act
        pb = RuntimeContext(dispatch_timeout=6.0).to_protobuf()

        # Assert
        assert pb.HasField("dispatch_timeout")
        assert pb.dispatch_timeout == 6.0

    def test_to_protobuf_when_dispatch_timeout_unset(self):
        """Test to_protobuf falls back to the current var when unset.

        Given:
            A RuntimeContext constructed without ``dispatch_timeout``
            and an outer context with the var set.
        When:
            ``to_protobuf`` is called.
        Then:
            The serialized ``dispatch_timeout`` equals the currently-
            set value on the stdlib ContextVar.
        """
        # Arrange
        outer_token = dispatch_timeout.set(9.25)
        try:
            # Act
            pb = RuntimeContext().to_protobuf()
        finally:
            dispatch_timeout.reset(outer_token)

        # Assert
        assert pb.HasField("dispatch_timeout")
        assert pb.dispatch_timeout == 9.25

    def test_to_protobuf_without_value(self):
        """Test to_protobuf omits dispatch_timeout when it is None.

        Given:
            A RuntimeContext constructed with ``dispatch_timeout=None``.
        When:
            ``to_protobuf`` is called.
        Then:
            The protobuf message omits the ``dispatch_timeout`` field.
        """
        # Act
        pb = RuntimeContext(dispatch_timeout=None).to_protobuf()

        # Assert
        assert not pb.HasField("dispatch_timeout")

    def test_from_protobuf_roundtrip(self):
        """Test from_protobuf reconstructs a usable RuntimeContext.

        Given:
            A protobuf RuntimeContext with a dispatch_timeout.
        When:
            ``from_protobuf`` is called and the result is entered.
        Then:
            The stdlib dispatch_timeout ContextVar reflects the wire
            value inside the block.
        """
        # Arrange
        pb = protocol.RuntimeContext(dispatch_timeout=8.5)

        # Act
        rc = RuntimeContext.from_protobuf(pb)

        # Assert
        with rc:
            assert dispatch_timeout.get() == 8.5


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
