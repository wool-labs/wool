import asyncio
from inspect import isasyncgen

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

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


class TestRoutine:
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
    async def test(
        self,
        test_case,
        use_dispatch,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Property-based test: Routine decorator with various function types.

        Given:
            The @routine decorator applied to different function types (module
            function, instance method, classmethod, staticmethod, async generators)
            with do_dispatch set to either True or False
        When:
            The decorated function is called with appropriate arguments
        Then:
            Task is either dispatched (when do_dispatch=True) or executed
            locally (when do_dispatch=False) and result is returned
            correctly for all function types, including async generators
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
    async def test_work_decorator_with_various_arguments(
        self,
        args,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Property-based test: Routine decorator with various argument patterns.

        Given:
            The @routine decorator with any valid combination of positional
            arguments
        When:
            The decorated function is called with these arguments
        Then:
            All values should be preserved through execution
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

    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(function_type=st.sampled_from(["function", "generator", "class"]))
    def test_invalid_wrapper(self, function_type):
        """Property-based test: @routine decorator validation.

        Given:
            The @routine decorator applied to invalid function types
            (non-coroutine, sync generator, class)
        When:
            Decorator is applied
        Then:
            ValueError is raised with appropriate message
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

    @pytest.mark.asyncio
    async def test_stream_multiple_values(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test stream with multiple values returns final value.

        Given:
            A decorated async function returns multiple values via stream
        When:
            Wrapped function is called and awaited
        Then:
            Returns the final value from the execution stream
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
    async def test_stream_empty(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test empty result handling.

        Given:
            A decorated async function with no return value
        When:
            Wrapped function is called and awaited
        Then:
            Returns None
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
    async def test_stream_error_handling(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test stream error handling.

        Given:
            A decorated function whose execution stream raises an
            exception
        When:
            The decorated function is called and awaited
        Then:
            Exception propagates to caller
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
    async def test_async_generator_aclose_cleanup(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test async generator cleanup on aclose.

        Given:
            An async generator decorated with @routine
        When:
            The generator is closed via aclose() before exhaustion
        Then:
            The underlying stream's aclose() is called for cleanup
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
    async def test_async_generator_exception_cleanup(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test async generator cleanup on exception.

        Given:
            An async generator that raises an exception during iteration
        When:
            The exception is raised
        Then:
            The underlying stream's aclose() is called for cleanup
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
    async def test_async_generator_cancellation_cleanup(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test async generator cleanup on task cancellation.

        Given:
            An async generator being consumed in a task
        When:
            The task is cancelled
        Then:
            The underlying stream's aclose() is called for cleanup
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
