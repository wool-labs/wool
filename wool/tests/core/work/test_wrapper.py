"""Tests for wrapper.py module."""

import asyncio
import inspect

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

import wool
from wool.core.work import work
from wool.core.work.wrapper import _do_dispatch
from wool.core.work.wrapper import execute_as_worker


# Module-level test functions for @work decorator tests
@work
async def module_test_func(x, y):
    """Test function for dispatch tests."""
    return x + y


@work
async def module_test_func_no_args():
    """Test function with no arguments."""
    return "async_result"


class TestClass:
    """Test class for instance and class method tests."""

    @work
    async def method(self, x):
        """Instance method."""
        return x * 2

    @classmethod
    @work
    async def class_method(cls, x):
        """Class method."""
        return x * 3


class TestWorkDecorator:
    """Tests for @work decorator."""

    def test_decorator_application(self):
        """Test @work decorator application.

        Given:
            A valid async function
        When:
            `@work` decorator is applied
        Then:
            Returns a wrapped function that dispatches to worker pool
        """

        # Arrange
        async def test_func():
            return "result"

        # Act
        decorated = work(test_func)

        # Assert
        assert callable(decorated)
        assert decorated.__name__ == test_func.__name__
        assert decorated.__module__ == test_func.__module__

    @pytest.mark.asyncio
    async def test_dispatch_execution(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test decorated function dispatch execution.

        Given:
            A decorated async function with proxy available in context
        When:
            Wrapped function is called
        Then:
            Task is dispatched to worker pool and returns result via
            coroutine
        """

        # Arrange - Mock the dispatch to return a simple result
        async def mock_dispatch_stream():
            yield 8

        async def mock_dispatch(*args, **kwargs):
            return mock_dispatch_stream()

        mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

        # Act
        result = await module_test_func(5, 3)

        # Assert
        assert result == 8
        mock_proxy_context.dispatch.assert_called_once()

    @pytest.mark.asyncio
    async def test_local_execution_within_worker(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test local execution within worker.

        Given:
            A decorated async function called from within a worker (via
            execute_as_worker)
        When:
            Wrapped function is called
        Then:
            Executes locally and returns result without re-dispatching
        """
        # Arrange & Act - execute as worker
        executor = execute_as_worker(module_test_func)
        result = await executor(5, 3)

        # Assert
        assert result == 8
        # Dispatch should not have been called
        mock_proxy_context.dispatch.assert_not_called()

    @pytest.mark.asyncio
    async def test_coroutine_result_handling(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test coroutine result handling.

        Given:
            A decorated async function with proxy set (coroutine)
        When:
            Wrapped function is called and awaited
        Then:
            Returns the result of the async execution
        """

        # Arrange - Mock the dispatch to return result
        async def mock_dispatch_stream():
            yield "async_result"

        async def mock_dispatch(*args, **kwargs):
            return mock_dispatch_stream()

        mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

        # Act
        result = await module_test_func_no_args()

        # Assert
        assert result == "async_result"

    def test_non_coroutine_validation(self):
        """Test non-coroutine validation during decoration.

        Given:
            A decorated function that is not a coroutine
        When:
            @work decorator is applied
        Then:
            Raises ValueError
        """
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="Expected a coroutine function"):

            @work
            def not_async():
                return "result"

    @pytest.mark.asyncio
    async def test_instance_method_execution(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test instance method execution.

        Given:
            A decorated instance method is called with instance and args
        When:
            Wrapped function is invoked
        Then:
            Method executes successfully and returns expected result
        """
        # Arrange
        instance = TestClass()

        # Mock the dispatch to return result
        async def mock_dispatch_stream():
            yield 10

        async def mock_dispatch(*args, **kwargs):
            return mock_dispatch_stream()

        mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

        # Act
        result = await instance.method(5)

        # Assert
        assert result == 10

    @pytest.mark.asyncio
    async def test_classmethod_execution(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test classmethod execution.

        Given:
            A decorated classmethod is called with class and args
        When:
            Wrapped function is invoked
        Then:
            Classmethod executes successfully and returns expected result
        """

        # Arrange - Mock the dispatch to return result
        async def mock_dispatch_stream():
            yield 15

        async def mock_dispatch(*args, **kwargs):
            return mock_dispatch_stream()

        mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

        # Act
        result = await TestClass.class_method(5)

        # Assert
        assert result == 15

    @pytest.mark.asyncio
    async def test_module_function_execution(
        self,
        mocker: MockerFixture,
        mock_proxy_context,
    ):
        """Test module-level function execution.

        Given:
            A decorated module-level function is called with args
        When:
            Wrapped function is invoked
        Then:
            Function executes successfully and returns expected result
        """

        # Arrange - Mock the dispatch to return result
        async def mock_dispatch_stream():
            yield 8

        async def mock_dispatch(*args, **kwargs):
            return mock_dispatch_stream()

        mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

        # Act
        result = await module_test_func(5, 3)

        # Assert
        assert result == 8

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

        # Arrange - Mock the dispatch to return multiple values
        async def mock_dispatch_stream():
            yield "first"
            yield "second"
            yield "final"

        async def mock_dispatch(*args, **kwargs):
            return mock_dispatch_stream()

        mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

        # Act
        result = await module_test_func_no_args()

        # Assert
        assert result == "final"

    @pytest.mark.asyncio
    async def test_empty_result_handling(
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

        # Arrange - Mock the dispatch to return empty stream
        async def mock_dispatch_stream():
            # Empty stream - no yields
            return
            yield  # This line is never reached but needed for generator syntax

        async def mock_dispatch(*args, **kwargs):
            return mock_dispatch_stream()

        mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

        # Act
        result = await module_test_func_no_args()

        # Assert
        assert result is None

    def test_decorator_on_async_generator_raises(self):
        """Test @work decorator on async generator raises error.

        Given:
            @work decorator applied to an async generator
        When:
            Decorator is applied
        Then:
            ValueError is raised because async generators are not supported
        """
        # Arrange & Act & Assert
        # Async generators are NOT coroutine functions, so the decorator
        # should reject them
        with pytest.raises(ValueError, match="Expected a coroutine function"):

            @work
            async def async_gen():
                yield 1
                yield 2

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

        # Arrange - Mock the dispatch to raise an exception
        async def mock_dispatch_stream():
            raise ValueError("Stream error")
            yield  # Never reached

        async def mock_dispatch(*args, **kwargs):
            return mock_dispatch_stream()

        mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

        # Act & Assert
        with pytest.raises(ValueError, match="Stream error"):
            await module_test_func_no_args()


class TestExecuteAsWorker:
    """Tests for execute_as_worker() function."""

    @pytest.mark.asyncio
    async def test_execute_without_redispatch(self):
        """Test execute_as_worker executes without redispatch.

        Given:
            An async callable is wrapped with execute_as_worker
        When:
            The wrapped executor is called and awaited
        Then:
            Executes the callable and returns result without dispatching
            to worker pool
        """

        # Arrange
        async def test_callable(x, y):
            return x * y

        # Act
        executor = execute_as_worker(test_callable)
        result = await executor(5, 3)

        # Assert
        assert result == 15

    @pytest.mark.asyncio
    async def test_execute_exception_propagation(self):
        """Test execute_as_worker exception propagation.

        Given:
            An async callable wrapped with execute_as_worker that raises
            an exception
        When:
            The wrapped executor is called
        Then:
            Exception is propagated to caller
        """

        # Arrange
        async def test_callable():
            raise ValueError("Test error")

        # Act
        executor = execute_as_worker(test_callable)

        # Assert
        with pytest.raises(ValueError, match="Test error"):
            await executor()


# Property-Based Tests


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
        max_size=2,  # Reduced for module_test_func signature
    ),
)
@pytest.mark.asyncio
async def test_work_decorator_with_various_arguments(
    args,
    mocker: MockerFixture,
    mock_proxy_context,
):
    """Property-based test: Work decorator with various argument patterns.

    Given:
        The @work decorator with any valid combination of positional
        arguments
    When:
        The decorated function is called with these arguments
    Then:
        All values should be preserved through execution
    """
    # Arrange - Mock the dispatch to return the arguments
    expected_result = (
        sum(args) if len(args) == 2 and all(isinstance(x, int) for x in args) else 0
    )

    async def mock_dispatch_stream():
        yield expected_result

    async def mock_dispatch(*fargs, **fkwargs):
        return mock_dispatch_stream()

    mock_proxy_context.dispatch = mocker.MagicMock(side_effect=mock_dispatch)

    # Act & Assert - Only test with valid argument counts
    if len(args) == 2 and all(isinstance(x, int) for x in args):
        result = await module_test_func(*args)
        assert result == expected_result
        # Verify dispatch was called
        mock_proxy_context.dispatch.assert_called_once()
