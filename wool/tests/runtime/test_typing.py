import pytest
from pytest_mock import MockerFixture

from wool.runtime.typing import resolved


@pytest.mark.asyncio
async def test_resolved_should_yield_object_when_dependency_is_plain():
    """Test a bare instance is yielded as is.

    Given:
        A plain object that is neither awaitable, callable, nor a
        context manager.
    When:
        It is resolved.
    Then:
        It should yield that same object.
    """
    # Arrange
    dependency = object()

    # Act
    async with resolved(dependency) as obj:
        # Assert
        assert obj is dependency


@pytest.mark.asyncio
async def test_resolved_should_await_dependency_when_awaitable():
    """Test an awaitable is awaited and its result yielded.

    Given:
        A coroutine that resolves to a sentinel object.
    When:
        It is resolved.
    Then:
        It should yield the coroutine's result.
    """
    # Arrange
    sentinel = object()

    async def dependency():
        return sentinel

    # Act
    async with resolved(dependency()) as obj:
        # Assert
        assert obj is sentinel


@pytest.mark.asyncio
async def test_resolved_should_recurse_when_dependency_is_callable():
    """Test a callable factory is called and its product resolved.

    Given:
        A zero-argument callable returning an async context manager.
    When:
        It is resolved.
    Then:
        It should yield what the manager yields and exit it afterwards.
    """
    # Arrange
    events = []

    class Manager:
        async def __aenter__(self):
            events.append("enter")
            return "obj"

        async def __aexit__(self, *args):
            events.append("exit")

    # Act
    async with resolved(Manager) as obj:
        assert obj == "obj"

    # Assert
    assert events == ["enter", "exit"]


@pytest.mark.asyncio
async def test_resolved_should_exit_sync_manager_with_exception_when_block_raises():
    """Test a sync context manager receives the block's exception on exit.

    Given:
        A sync context manager recording the exception info its exit
        receives.
    When:
        The resolved block raises.
    Then:
        It should exit the manager with that exception and re-raise it.
    """
    # Arrange
    exits = []

    class Manager:
        def __enter__(self):
            return "obj"

        def __exit__(self, *args):
            exits.append(args)

    error = ValueError("boom")

    # Act
    with pytest.raises(ValueError):
        async with resolved(Manager()):
            raise error

    # Assert
    assert len(exits) == 1
    assert exits[0][0] is ValueError
    assert exits[0][1] is error


@pytest.mark.asyncio
async def test_resolved_should_exit_async_manager_with_none_when_block_completes():
    """Test an async context manager exits cleanly after a normal block.

    Given:
        An async context manager recording its exit arguments.
    When:
        The resolved block completes without raising.
    Then:
        It should exit the manager with no exception info.
    """
    # Arrange
    exits = []

    class Manager:
        async def __aenter__(self):
            return "obj"

        async def __aexit__(self, *args):
            exits.append(args)

    # Act
    async with resolved(Manager()):
        pass

    # Assert
    assert exits == [(None, None, None)]


@pytest.mark.asyncio
async def test_resolved_should_reraise_when_manager_suppresses_exception():
    """Test a suppressing manager cannot swallow the block's failure.

    Given:
        An async context manager whose exit returns True.
    When:
        The resolved block raises.
    Then:
        It should still propagate the exception, ignoring the
        manager's verdict.
    """

    # Arrange
    class Manager:
        async def __aenter__(self):
            return "obj"

        async def __aexit__(self, *args):
            return True

    # Act & assert
    with pytest.raises(ValueError, match="boom"):
        async with resolved(Manager()):
            raise ValueError("boom")


@pytest.mark.asyncio
async def test_resolved_should_propagate_error_when_manager_exit_raises(
    mocker: MockerFixture,
):
    """Test an error raised by the manager's own exit propagates.

    Given:
        A sync context manager whose exit raises.
    When:
        The resolved block completes normally.
    Then:
        It should propagate the exit's error.
    """
    # Arrange
    manager = mocker.MagicMock()
    manager.__enter__ = mocker.Mock(return_value="obj")
    manager.__exit__ = mocker.Mock(side_effect=RuntimeError("exit failed"))

    # Act & assert
    with pytest.raises(RuntimeError, match="exit failed"):
        async with resolved(manager):
            pass


@pytest.mark.asyncio
async def test_resolved_should_enter_manager_when_dependency_is_also_callable():
    """Test the context-manager form wins over the callable form.

    Given:
        An object that is both a sync context manager and callable.
    When:
        It is resolved.
    Then:
        It should enter the manager for the block and never call the
        object.
    """
    # Arrange
    events = []

    class Both:
        def __enter__(self):
            events.append("enter")
            return "obj"

        def __exit__(self, *args):
            events.append("exit")

        def __call__(self):
            events.append("call")
            return "called"

    # Act
    async with resolved(Both()) as obj:
        assert obj == "obj"

    # Assert
    assert events == ["enter", "exit"]


@pytest.mark.asyncio
async def test_resolved_should_prefer_sync_protocol_when_manager_is_both():
    """Test a manager implementing both protocols is entered synchronously.

    Given:
        An object that is both a sync and an async context manager.
    When:
        It is resolved.
    Then:
        It should enter and exit the sync protocol and never touch the
        async one.
    """
    # Arrange
    events = []

    class Both:
        def __enter__(self):
            events.append("enter")
            return "obj"

        def __exit__(self, *args):
            events.append("exit")

        async def __aenter__(self):
            events.append("aenter")
            return "obj"

        async def __aexit__(self, *args):
            events.append("aexit")

    # Act
    async with resolved(Both()) as obj:
        assert obj == "obj"

    # Assert
    assert events == ["enter", "exit"]
