"""Shared fixtures for work subpackage tests."""

import asyncio
from typing import Callable
from typing import Coroutine
from uuid import uuid4

import pytest
from pytest_mock import MockerFixture

import wool
from wool.runtime.work import WorkTask


class PicklableProxy:
    """A simple picklable proxy for testing serialization."""

    def __init__(self):
        self.id = uuid4()

    async def dispatch(self, *args, **kwargs):
        """Mock dispatch method."""

        async def _stream():
            yield "result"

        return _stream()


@pytest.fixture
def mock_proxy(mocker: MockerFixture):
    """Provides a mock WorkerProxy for testing.

    Creates a mock proxy with the minimal interface needed for task
    execution and dispatch.
    """
    mock_proxy = mocker.MagicMock()
    mock_proxy.id = uuid4()

    # Mock dispatch to return an async generator
    async def mock_dispatch_stream(*args, **kwargs):
        yield "result"

    mock_proxy.dispatch = mocker.MagicMock(return_value=mock_dispatch_stream())

    return mock_proxy


@pytest.fixture
def picklable_proxy():
    """Provides a picklable proxy for serialization testing."""
    return PicklableProxy()


@pytest.fixture
def mock_proxy_context(mock_proxy):
    """Provides a mock proxy in context for decorated function testing.

    Sets the wool.__proxy__ context variable and returns a token
    for cleanup.
    """
    token = wool.__proxy__.set(mock_proxy)
    yield mock_proxy
    wool.__proxy__.reset(token)


@pytest.fixture
def sample_async_callable():
    """Provides a simple async callable for testing."""

    async def _callable(*args, **kwargs):
        return "test_result"

    return _callable


@pytest.fixture
def sample_task(
    mocker: MockerFixture, sample_async_callable: Callable[..., Coroutine], mock_proxy
) -> Callable[..., WorkTask]:
    """Provides a factory for creating mock WorkTask instances.

    Returns a function that creates WorkTask instances with customizable
    parameters for testing.
    """

    def _create_task(**overrides) -> WorkTask:
        defaults = {
            "id": uuid4(),
            "callable": sample_async_callable,
            "args": (),
            "kwargs": {},
            "proxy": mock_proxy,
            "timeout": 0,
            "caller": None,
            "exception": None,
            "filename": None,
            "function": None,
            "line_no": None,
            "tag": None,
        }
        defaults.update(overrides)

        task = WorkTask(**defaults)
        return task

    return _create_task


@pytest.fixture
def event_spy():
    """Provides a spy for tracking event emissions.

    Returns a function that can be used as an event handler and tracks
    all calls made to it.
    """
    calls = []

    def spy(event, timestamp, context=None):
        calls.append((event, timestamp, context))

    spy.calls = calls
    spy.reset = lambda: calls.clear()

    return spy


@pytest.fixture
def clear_event_handlers():
    """Clears all event handlers before and after each test.

    Ensures test isolation by preventing handler pollution between tests.
    """
    saved_handlers = wool.WorkTaskEvent._handlers.copy()
    wool.WorkTaskEvent._handlers.clear()

    yield

    wool.WorkTaskEvent._handlers = saved_handlers


# Picklable test functions for decorator tests
async def picklable_async_function(x: int) -> int:
    """A picklable async function for testing @work decorator."""
    return x * 2


async def picklable_async_function_no_return():
    """A picklable async function with no return value."""
    await asyncio.sleep(0)


async def picklable_async_function_raises():
    """A picklable async function that raises an exception."""
    raise ValueError("Test exception")


class PicklableTestClass:
    """A picklable class for testing instance and class methods."""

    @wool.work
    async def instance_method(self, x: int) -> int:
        """A picklable instance method."""
        return x * 3

    @classmethod
    @wool.work
    async def class_method(cls, x: int) -> int:
        """A picklable class method."""
        return x * 4
