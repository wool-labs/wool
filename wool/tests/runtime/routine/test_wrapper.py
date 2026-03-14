import asyncio
from inspect import getsourcelines
from inspect import isasyncgen

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

from wool.runtime.routine.task import do_dispatch
from wool.runtime.routine.wrapper import routine


@routine
async def foo(x, y):
    """Test function for dispatch tests."""
    return x + y


assert foo.__qualname__ == "foo"
assert foo.__module__ == "runtime.routine.test_wrapper"


@routine
async def bar():
    """Test function with no arguments."""
    return "async_result"


assert bar.__qualname__ == "bar"
assert bar.__module__ == "runtime.routine.test_wrapper"


@routine
async def foo_gen(x):
    """Async generator test function."""
    for i in range(x):
        yield i


assert foo_gen.__qualname__ == "foo_gen"
assert foo_gen.__module__ == "runtime.routine.test_wrapper"


@routine
async def echo_send(n):
    """Async generator that echoes sent values back."""
    value = yield "ready"
    for _ in range(n):
        value = yield f"echo:{value}"


@routine
async def accumulator():
    """Async generator that accumulates sent integers."""
    total = 0
    total = yield total
    while True:
        total = yield total


@routine
async def stop_on_signal():
    """Async generator that stops when sent 'stop'."""
    hint = yield "waiting"
    while hint != "stop":
        hint = yield f"got:{hint}"


class Resettable(Exception):
    """Custom exception for athrow tests."""


@routine
async def resilient_counter(start):
    """Async generator that resets its counter on Resettable."""
    count = start
    while True:
        try:
            _ = yield count
        except Resettable:
            count = 0
            continue
        count += 1


@routine
async def fragile_gen():
    """Async generator that does not handle thrown exceptions."""
    yield "one"
    yield "two"


@routine
async def catch_and_return():
    """Async generator that catches a throw and then returns."""
    try:
        yield "before"
    except Resettable:
        return


class Foo:
    """Test class for instance and class method tests."""

    @routine
    async def foo(self, x):
        """Instance method."""
        return x * 2

    assert foo.__qualname__ == "Foo.foo"
    assert foo.__module__ == "runtime.routine.test_wrapper"

    @routine
    @classmethod
    async def bar(cls, x):
        """Class method."""
        return x * 3

    assert bar.__qualname__ == "Foo.bar"
    assert bar.__module__ == "runtime.routine.test_wrapper"

    @routine
    @staticmethod
    async def baz(x):
        """Static method."""
        return x * 4

    assert baz.__qualname__ == "Foo.baz"
    assert baz.__module__ == "runtime.routine.test_wrapper"

    @routine
    async def foo_gen(self, x):
        """Async generator instance method."""
        for i in range(x):
            yield i * 2

    assert foo_gen.__qualname__ == "Foo.foo_gen"
    assert foo_gen.__module__ == "runtime.routine.test_wrapper"

    @routine
    @classmethod
    async def bar_gen(cls, x):
        """Async generator class method."""
        for i in range(x):
            yield i * 3

    assert bar_gen.__qualname__ == "Foo.bar_gen"
    assert bar_gen.__module__ == "runtime.routine.test_wrapper"

    @routine
    @staticmethod
    async def baz_gen(x):
        """Async generator static method."""
        for i in range(x):
            yield i * 4

    assert baz_gen.__qualname__ == "Foo.baz_gen"
    assert baz_gen.__module__ == "runtime.routine.test_wrapper"


@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(function_type=st.sampled_from(["function", "generator", "class"]))
def test_routine_with_invalid_types(function_type):
    """Test @routine rejects invalid callable types.

    Given:
        The @routine decorator applied to invalid function types
        (non-coroutine, sync generator, class).
    When:
        The decorator is applied.
    Then:
        It should raise ``ValueError``.
    """
    # Arrange, act, & assert
    with pytest.raises(
        ValueError, match="Expected a coroutine function or async generator function"
    ):
        match function_type:
            case "function":

                @routine  # type: ignore[arg-type]
                def invalid_foo(x: int):
                    return x * 2

            case "generator":

                @routine  # type: ignore[arg-type]
                def invalid_bar(x: int):
                    yield x * 3

            case "class":

                @routine  # type: ignore[arg-type]
                class InvalidFoo: ...


@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    test_case=st.sampled_from(
        [
            ("function", lambda: foo(5, 3), 8, False),
            ("method", lambda: Foo().foo(5), 10, False),
            ("classmethod", lambda: Foo.bar(5), 15, False),
            ("staticmethod", lambda: Foo.baz(5), 20, False),
            ("async-gen-function", lambda: foo_gen(3), [0, 1, 2], True),
            ("async-gen-method", lambda: Foo().foo_gen(3), [0, 2, 4], True),
            ("async-gen-classmethod", lambda: Foo.bar_gen(3), [0, 3, 6], True),
            ("async-gen-staticmethod", lambda: Foo.baz_gen(3), [0, 4, 8], True),
        ]
    ),
    use_dispatch=st.booleans(),
)
@pytest.mark.asyncio
async def test_routine_with_various_function_types(
    test_case,
    use_dispatch,
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test @routine dispatch and local execution across function types.

    Given:
        The @routine decorator applied to different function types (module
        function, instance method, classmethod, staticmethod, async generators)
        with do_dispatch set to either True or False.
    When:
        The decorated function is called with appropriate arguments.
    Then:
        The task is either dispatched (when do_dispatch=True) or
        executed locally (when do_dispatch=False) and the result
        is returned correctly for all function types.
    """
    # Arrange
    _, call_factory, expected, is_async_gen = test_case

    if use_dispatch:
        # Test dispatch path
        if is_async_gen:

            async def mock_dispatch_stream():
                for value in expected:
                    yield value

            # proxy.dispatch() is async and returns an async generator
            mock_proxy_context.dispatch = mocker.AsyncMock(
                return_value=mock_dispatch_stream()
            )

            # Act - await to get the async generator, then collect all yielded values
            result = []
            gen = call_factory()
            async for value in gen:
                result.append(value)

            # Assert
            assert result == expected
            mock_proxy_context.dispatch.assert_called_once()
        else:

            async def mock_dispatch_stream():
                yield expected

            # proxy.dispatch() is async and returns an async generator
            mock_proxy_context.dispatch = mocker.AsyncMock(
                return_value=mock_dispatch_stream()
            )

            # Act
            result = await call_factory()

            # Assert
            assert result == expected
            mock_proxy_context.dispatch.assert_called_once()
    else:
        # Test execution path
        from wool.runtime.routine.task import do_dispatch

        with do_dispatch(False):
            # Act
            if is_async_gen:
                result = []
                gen_or_coro = call_factory()
                # Check if it's already an async generator or needs awaiting
                if isasyncgen(gen_or_coro):
                    gen = gen_or_coro
                else:
                    gen = await gen_or_coro
                async for value in gen:
                    result.append(value)
            else:
                result = await call_factory()

            # Assert
            assert result == expected


@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    args=st.lists(
        st.one_of(
            st.integers(),
            st.text(min_size=0, max_size=20),
            st.lists(st.integers(), max_size=5),
        ),
        min_size=0,
        max_size=2,  # Reduced for function signature
    ),
)
@pytest.mark.asyncio
async def test_routine_with_various_arguments(
    args,
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test @routine preserves arguments through dispatch.

    Given:
        The @routine decorator with any valid combination of positional
        arguments.
    When:
        The decorated function is called with these arguments.
    Then:
        All values are preserved through execution.
    """
    # Arrange
    expected_result = (
        sum(args) if len(args) == 2 and all(isinstance(x, int) for x in args) else 0
    )

    async def mock_dispatch_stream():
        yield expected_result

    async def mock_dispatch(*fargs, **fkwargs):
        return mock_dispatch_stream()

    mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

    # Act & assert
    if len(args) == 2 and all(isinstance(x, int) for x in args):
        result = await foo(*args)
        assert result == expected_result
        mock_proxy_context.dispatch.assert_called_once()


@pytest.mark.asyncio
async def test_routine_with_multi_value_stream(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test stream with multiple values returns final value.

    Given:
        A decorated async function returns multiple values via stream.
    When:
        Wrapped function is called and awaited.
    Then:
        The final value from the execution stream is returned.
    """

    # Arrange
    async def mock_dispatch_stream():
        yield "first"
        yield "second"
        yield "final"

    async def mock_dispatch(*args, **kwargs):
        return mock_dispatch_stream()

    mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

    # Act
    result = await bar()

    # Assert
    assert result == "final"


@pytest.mark.asyncio
async def test_routine_with_empty_stream(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test empty result handling.

    Given:
        A decorated async function with no return value.
    When:
        Wrapped function is called and awaited.
    Then:
        The result is ``None``.
    """

    # Arrange
    async def mock_dispatch_stream():
        # Empty stream - no yields
        return
        yield  # Never reached

    async def mock_dispatch(*args, **kwargs):
        return mock_dispatch_stream()

    mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

    # Act
    result = await bar()

    # Assert
    assert result is None


@pytest.mark.asyncio
async def test_routine_with_stream_error(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test stream error handling.

    Given:
        A decorated function whose execution stream raises an
        exception.
    When:
        The decorated function is called and awaited.
    Then:
        The exception propagates to the caller.
    """

    # Arrange
    async def mock_dispatch_stream():
        raise ValueError("Stream error")
        yield  # Never reached

    async def mock_dispatch(*args, **kwargs):
        return mock_dispatch_stream()

    mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

    # Act & assert
    with pytest.raises(ValueError, match="Stream error"):
        await bar()


@pytest.mark.asyncio
async def test_routine_with_aclose_during_iteration(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test async generator cleanup on aclose.

    Given:
        An async generator decorated with @routine.
    When:
        The generator is closed via aclose() before exhaustion.
    Then:
        The underlying stream's aclose() is called for cleanup.
    """

    # Arrange
    class MockStream:
        def __init__(self):
            self.aclose_called = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            # Yield a few values then wait
            if not hasattr(self, "_count"):
                self._count = 0
            if self._count < 3:
                self._count += 1
                return self._count
            # Block forever on subsequent calls
            await asyncio.Event().wait()

        async def aclose(self):
            self.aclose_called = True

    mock_stream = MockStream()

    async def mock_dispatch(*args, **kwargs):
        return mock_stream

    mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

    # Act
    gen = foo_gen(10)
    result = []
    async for value in gen:
        result.append(value)
        if len(result) == 2:
            # Close early
            await gen.aclose()
            break

    # Assert
    assert result == [1, 2]
    assert mock_stream.aclose_called


@pytest.mark.asyncio
async def test_routine_with_exception_during_iteration(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test async generator cleanup on exception.

    Given:
        An async generator that raises an exception during iteration.
    When:
        The exception is raised.
    Then:
        The underlying stream's aclose() is called for cleanup.
    """

    # Arrange
    class MockStream:
        def __init__(self):
            self.aclose_called = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not hasattr(self, "_count"):
                self._count = 0
            self._count += 1
            if self._count <= 2:
                return self._count
            raise ValueError("Stream error during iteration")

        async def aclose(self):
            self.aclose_called = True

    mock_stream = MockStream()

    async def mock_dispatch(*args, **kwargs):
        return mock_stream

    mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

    # Act & assert
    gen = foo_gen(10)
    result = []
    with pytest.raises(ValueError, match="Stream error during iteration"):
        async for value in gen:
            result.append(value)

    # Assert cleanup happened
    assert result == [1, 2]
    assert mock_stream.aclose_called


@pytest.mark.asyncio
async def test_routine_with_task_cancellation(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test async generator cleanup on task cancellation.

    Given:
        An async generator being consumed in a task.
    When:
        The task is cancelled.
    Then:
        The underlying stream's aclose() is called for cleanup.
    """

    # Arrange
    class MockStream:
        def __init__(self):
            self.aclose_called = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not hasattr(self, "_count"):
                self._count = 0
            self._count += 1
            if self._count <= 2:
                return self._count
            # Block forever
            await asyncio.Event().wait()

        async def aclose(self):
            self.aclose_called = True

    mock_stream = MockStream()

    async def mock_dispatch(*args, **kwargs):
        return mock_stream

    mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

    # Act
    async def consume_generator():
        result = []
        async for value in foo_gen(10):
            result.append(value)
        return result

    task = asyncio.create_task(consume_generator())
    # Wait a bit to let it consume some values
    await asyncio.sleep(0.01)
    # Cancel the task
    task.cancel()

    # Assert
    with pytest.raises(asyncio.CancelledError):
        await task

    # Assert cleanup happened
    assert mock_stream.aclose_called


# ── Bilateral streaming tests (BS-xxx) ───────────────────────


@pytest.mark.asyncio
async def test_routine_with_asend_single_value():
    """Test asend forwards a single value to the generator.

    Given:
        A @routine-decorated async generator that echoes sent
        values.
    When:
        asend() is called with a non-None value after the first
        __anext__().
    Then:
        It should yield back the sent value.
    """
    with do_dispatch(False):
        # Arrange
        gen = echo_send(1)

        # Act
        first = await gen.__anext__()
        echoed = await gen.asend("hello")

        # Assert
        assert first == "ready"
        assert echoed == "echo:hello"
        await gen.aclose()


@pytest.mark.asyncio
async def test_routine_with_asend_multiple_values():
    """Test asend forwards multiple values in sequence.

    Given:
        A @routine-decorated async generator that accumulates
        sent integers.
    When:
        asend() is called multiple times in sequence.
    Then:
        It should forward each sent value and yield updated
        state.
    """
    with do_dispatch(False):
        # Arrange
        gen = accumulator()

        # Act
        initial = await gen.__anext__()
        r1 = await gen.asend(10)
        r2 = await gen.asend(20)
        r3 = await gen.asend(30)

        # Assert
        assert initial == 0
        assert r1 == 10
        assert r2 == 20
        assert r3 == 30
        await gen.aclose()


@pytest.mark.asyncio
async def test_routine_with_asend_causing_exhaustion():
    """Test asend causing generator exhaustion.

    Given:
        A @routine-decorated async generator that stops when
        sent "stop".
    When:
        asend() is called with "stop".
    Then:
        It should raise StopAsyncIteration after the generator
        returns.
    """
    with do_dispatch(False):
        # Arrange
        gen = stop_on_signal()
        await gen.__anext__()  # "waiting"

        # Act
        got = await gen.asend("continue")
        assert got == "got:continue"

        # Act & assert
        with pytest.raises(StopAsyncIteration):
            await gen.asend("stop")


@pytest.mark.asyncio
async def test_routine_with_anext_implicit_none():
    """Test __anext__() works as implicit None send.

    Given:
        A @routine-decorated async generator yielding its first
        value.
    When:
        __anext__() is called (no prior send).
    Then:
        It should yield the first value with None as the implicit
        send.
    """
    with do_dispatch(False):
        # Arrange
        gen = echo_send(2)

        # Act
        first = await gen.__anext__()
        second = await gen.__anext__()

        # Assert
        assert first == "ready"
        assert second == "echo:None"
        await gen.aclose()


@pytest.mark.asyncio
async def test_routine_with_athrow_handled_exception():
    """Test athrow with an exception the generator catches.

    Given:
        A @routine-decorated async generator that catches
        Resettable and yields a recovery value.
    When:
        athrow() is called with Resettable.
    Then:
        It should yield the recovery value instead of propagating
        the exception.
    """
    with do_dispatch(False):
        # Arrange
        gen = resilient_counter(5)
        v1 = await gen.__anext__()
        assert v1 == 5

        # Act
        recovered = await gen.athrow(Resettable)

        # Assert
        assert recovered == 0
        await gen.aclose()


@pytest.mark.asyncio
async def test_routine_with_athrow_unhandled_exception():
    """Test athrow with an exception the generator does not catch.

    Given:
        A @routine-decorated async generator that does not catch
        the thrown exception.
    When:
        athrow() is called with an unhandled exception.
    Then:
        It should propagate the exception to the caller.
    """
    with do_dispatch(False):
        # Arrange
        gen = fragile_gen()
        await gen.__anext__()  # "one"

        # Act & assert
        with pytest.raises(Resettable):
            await gen.athrow(Resettable)


@pytest.mark.asyncio
async def test_routine_with_athrow_causing_return():
    """Test athrow causing the generator to return.

    Given:
        A @routine-decorated async generator that catches a
        thrown exception and then returns.
    When:
        athrow() is called causing the generator to return after
        handling.
    Then:
        It should raise StopAsyncIteration.
    """
    with do_dispatch(False):
        # Arrange
        gen = catch_and_return()
        v = await gen.__anext__()
        assert v == "before"

        # Act & assert
        with pytest.raises(StopAsyncIteration):
            await gen.athrow(Resettable)


@pytest.mark.asyncio
async def test_routine_with_aclose_partial_consumption():
    """Test aclose on a partially consumed generator.

    Given:
        A @routine-decorated async generator that is partially
        consumed.
    When:
        aclose() is called before exhaustion.
    Then:
        It should close cleanly and subsequent __anext__() should
        raise StopAsyncIteration.
    """
    with do_dispatch(False):
        # Arrange
        gen = echo_send(5)
        await gen.__anext__()  # "ready"

        # Act
        await gen.aclose()

        # Assert
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()


@pytest.mark.asyncio
async def test_routine_with_interleaved_send_and_throw():
    """Test interleaving asend and athrow calls.

    Given:
        A @routine-decorated async generator with bilateral
        protocol.
    When:
        asend() is called, then athrow(), then asend() again.
    Then:
        It should correctly alternate between send and throw
        forwarding.
    """
    with do_dispatch(False):
        # Arrange
        gen = resilient_counter(0)
        v = await gen.__anext__()
        assert v == 0

        # Act — send to advance
        v = await gen.asend(None)
        assert v == 1
        v = await gen.asend(None)
        assert v == 2

        # Act — throw to reset
        v = await gen.athrow(Resettable)
        assert v == 0

        # Act — send again after reset
        v = await gen.asend(None)
        assert v == 1

        # Assert final state
        await gen.aclose()


@pytest.mark.asyncio
async def test_routine_with_async_for_iteration():
    """Test async for iteration still works after refactor.

    Given:
        A @routine-decorated async generator using async for
        iteration.
    When:
        The generator is consumed via async for (not asend or
        athrow).
    Then:
        It should yield all values identically to the pre-change
        behavior.
    """
    with do_dispatch(False):
        # Act
        result = []
        async for value in foo_gen(5):
            result.append(value)

        # Assert
        assert result == [0, 1, 2, 3, 4]


# ── Source metadata tag tests (SM-xxx) ────────────────────────


@pytest.mark.asyncio
async def test_routine_with_coroutine_tag(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test coroutine dispatch tag uses source metadata format.

    Given:
        A @routine-decorated coroutine function.
    When:
        The function is called and dispatched.
    Then:
        The task tag should match
        ``"{module}.{qualname}:{lineno}"``.
    """
    # Arrange
    _, expected_lineno = getsourcelines(foo.__wrapped__)

    async def mock_stream():
        yield 42

    mock_proxy_context.dispatch = mocker.AsyncMock(return_value=mock_stream())

    # Act
    await foo(1, 2)

    # Assert
    task = mock_proxy_context.dispatch.call_args[0][0]
    expected = f"{foo.__module__}.{foo.__qualname__}:{expected_lineno}"
    assert task.tag == expected


@pytest.mark.asyncio
async def test_routine_with_async_generator_tag(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test async generator dispatch tag uses source metadata format.

    Given:
        A @routine-decorated async generator function.
    When:
        The function is called and dispatched.
    Then:
        The task tag should match
        ``"{module}.{qualname}:{lineno}"``.
    """
    # Arrange
    _, expected_lineno = getsourcelines(foo_gen.__wrapped__)

    async def mock_stream():
        yield 0
        yield 1

    mock_proxy_context.dispatch = mocker.AsyncMock(return_value=mock_stream())

    # Act
    result = []
    async for value in foo_gen(2):
        result.append(value)

    # Assert
    task = mock_proxy_context.dispatch.call_args[0][0]
    expected = f"{foo_gen.__module__}.{foo_gen.__qualname__}:{expected_lineno}"
    assert task.tag == expected


@pytest.mark.asyncio
async def test_routine_with_large_arguments(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test tag stays small regardless of argument size.

    Given:
        A @routine-decorated coroutine called with a large
        argument.
    When:
        The function is dispatched.
    Then:
        The task tag length should remain under 200 characters.
    """

    # Arrange
    async def mock_stream():
        yield 0

    mock_proxy_context.dispatch = mocker.AsyncMock(return_value=mock_stream())

    # Act
    await foo(b"\x00" * 1_000_000, 0)

    # Assert
    task = mock_proxy_context.dispatch.call_args[0][0]
    assert len(task.tag) < 200


@pytest.mark.asyncio
async def test_routine_with_source_line_in_tag(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test tag contains the correct source line number.

    Given:
        A @routine-decorated function.
    When:
        The function is dispatched.
    Then:
        The task tag should end with the source line number
        from ``inspect.getsourcelines``.
    """
    # Arrange
    _, expected_lineno = getsourcelines(foo.__wrapped__)

    async def mock_stream():
        yield 42

    mock_proxy_context.dispatch = mocker.AsyncMock(return_value=mock_stream())

    # Act
    await foo(1, 2)

    # Assert
    task = mock_proxy_context.dispatch.call_args[0][0]
    assert task.tag.endswith(f":{expected_lineno}")


@pytest.mark.asyncio
async def test_routine_with_instance_method_tag(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test instance method tag includes class in qualname.

    Given:
        A @routine-decorated instance method.
    When:
        The method is called and dispatched.
    Then:
        The task tag should include the class-qualified name.
    """

    # Arrange
    async def mock_stream():
        yield 10

    mock_proxy_context.dispatch = mocker.AsyncMock(return_value=mock_stream())

    # Act
    await Foo().foo(5)

    # Assert
    task = mock_proxy_context.dispatch.call_args[0][0]
    assert "Foo.foo:" in task.tag


@pytest.mark.asyncio
async def test_routine_with_classmethod_tag(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test classmethod tag includes class in qualname.

    Given:
        A @routine-decorated classmethod.
    When:
        The method is called and dispatched.
    Then:
        The task tag should include the class-qualified name.
    """

    # Arrange
    async def mock_stream():
        yield 15

    mock_proxy_context.dispatch = mocker.AsyncMock(return_value=mock_stream())

    # Act
    await Foo.bar(5)

    # Assert
    task = mock_proxy_context.dispatch.call_args[0][0]
    assert "Foo.bar:" in task.tag


@pytest.mark.asyncio
async def test_routine_with_staticmethod_tag(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test staticmethod tag includes class in qualname.

    Given:
        A @routine-decorated staticmethod.
    When:
        The method is called and dispatched.
    Then:
        The task tag should include the class-qualified name.
    """

    # Arrange
    async def mock_stream():
        yield 20

    mock_proxy_context.dispatch = mocker.AsyncMock(return_value=mock_stream())

    # Act
    await Foo.baz(5)

    # Assert
    task = mock_proxy_context.dispatch.call_args[0][0]
    assert "Foo.baz:" in task.tag


@pytest.mark.asyncio
async def test_routine_with_async_generator_method_tag(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test async generator method tag includes class in qualname.

    Given:
        A @routine-decorated async generator instance method.
    When:
        The method is called and dispatched.
    Then:
        The task tag should include the class-qualified name.
    """

    # Arrange
    async def mock_stream():
        yield 0
        yield 2

    mock_proxy_context.dispatch = mocker.AsyncMock(return_value=mock_stream())

    # Act
    async for _ in Foo().foo_gen(2):
        pass

    # Assert
    task = mock_proxy_context.dispatch.call_args[0][0]
    assert "Foo.foo_gen:" in task.tag


def test_routine_with_distinct_definitions():
    """Test different functions produce distinct tag line numbers.

    Given:
        Multiple @routine-decorated functions defined on
        different lines.
    When:
        Their source line numbers are inspected via
        ``inspect.getsourcelines``.
    Then:
        Each should have a distinct value.
    """
    # Arrange
    _, foo_lineno = getsourcelines(foo.__wrapped__)
    _, bar_lineno = getsourcelines(bar.__wrapped__)
    _, gen_lineno = getsourcelines(foo_gen.__wrapped__)

    # Act & assert
    assert foo_lineno != bar_lineno
    assert bar_lineno != gen_lineno
    assert foo_lineno != gen_lineno


@pytest.mark.asyncio
async def test_routine_with_keyword_arguments(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test keyword arguments are not included in the tag.

    Given:
        A @routine-decorated coroutine called with keyword
        arguments.
    When:
        The function is dispatched.
    Then:
        The task tag should not contain ``=``.
    """

    # Arrange
    async def mock_stream():
        yield 42

    mock_proxy_context.dispatch = mocker.AsyncMock(return_value=mock_stream())

    # Act
    await foo(x=1, y=2)

    # Assert
    task = mock_proxy_context.dispatch.call_args[0][0]
    assert "=" not in task.tag


@pytest.mark.asyncio
async def test_routine_with_no_arguments(
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Test no-argument call produces identical tag format.

    Given:
        A @routine-decorated coroutine with no parameters.
    When:
        The function is called with no arguments.
    Then:
        The task tag should match
        ``"{module}.{qualname}:{lineno}"``.
    """
    # Arrange
    _, expected_lineno = getsourcelines(bar.__wrapped__)

    async def mock_stream():
        yield "async_result"

    mock_proxy_context.dispatch = mocker.AsyncMock(return_value=mock_stream())

    # Act
    await bar()

    # Assert
    task = mock_proxy_context.dispatch.call_args[0][0]
    expected = f"{bar.__module__}.{bar.__qualname__}:{expected_lineno}"
    assert task.tag == expected
