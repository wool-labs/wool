import pytest

from wool.runtime.typing import resolved


class _Recording:
    """Record how a context manager is entered and exited."""

    def __init__(self, obj="obj", *, suppress=False, error=None):
        self.obj = obj
        self.suppress = suppress
        self.error = error
        self.events = []
        self.exits = []

    def _enter(self):
        self.events.append("enter")
        return self.obj

    def _exit(self, args):
        self.events.append("exit")
        self.exits.append(args)
        if self.error is not None:
            raise self.error
        return self.suppress


class _RecordingManager(_Recording):
    """A sync context manager that records its exits."""

    def __enter__(self):
        return self._enter()

    def __exit__(self, *args):
        return self._exit(args)


class _AsyncRecordingManager(_Recording):
    """An async context manager that records its exits."""

    async def __aenter__(self):
        return self._enter()

    async def __aexit__(self, *args):
        return self._exit(args)


_MANAGERS = [
    pytest.param(_RecordingManager, id="sync"),
    pytest.param(_AsyncRecordingManager, id="async"),
]


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
    manager = _AsyncRecordingManager()

    # Act
    async with resolved(lambda: manager) as obj:
        assert obj == "obj"

    # Assert
    assert manager.events == ["enter", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("manager_type", _MANAGERS)
async def test_resolved_should_exit_manager_with_exception_when_block_raises(
    manager_type,
):
    """Test a context manager receives the block's exception on exit.

    Given:
        A sync or async context manager recording the exception info
        its exit receives.
    When:
        The resolved block raises.
    Then:
        It should exit the manager with that exception and re-raise it.
    """
    # Arrange
    manager = manager_type()
    error = ValueError("boom")

    # Act
    with pytest.raises(ValueError):
        async with resolved(manager):
            raise error

    # Assert
    assert len(manager.exits) == 1
    assert manager.exits[0][0] is ValueError
    assert manager.exits[0][1] is error


@pytest.mark.asyncio
@pytest.mark.parametrize("manager_type", _MANAGERS)
async def test_resolved_should_exit_manager_with_none_when_block_completes(
    manager_type,
):
    """Test a context manager exits cleanly after a normal block.

    Given:
        A sync or async context manager recording its exit arguments.
    When:
        The resolved block completes without raising.
    Then:
        It should exit the manager with no exception info.
    """
    # Arrange
    manager = manager_type()

    # Act
    async with resolved(manager):
        pass

    # Assert
    assert manager.exits == [(None, None, None)]


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
    manager = _AsyncRecordingManager(suppress=True)

    # Act & assert
    with pytest.raises(ValueError, match="boom"):
        async with resolved(manager):
            raise ValueError("boom")


@pytest.mark.asyncio
async def test_resolved_should_propagate_error_when_manager_exit_raises():
    """Test an error raised by the manager's own exit propagates.

    Given:
        A sync context manager whose exit raises.
    When:
        The resolved block completes normally.
    Then:
        It should propagate the exit's error.
    """
    # Arrange
    manager = _RecordingManager(error=RuntimeError("exit failed"))

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
    class Both(_RecordingManager):
        def __call__(self):
            self.events.append("call")
            return "called"

    manager = Both()

    # Act
    async with resolved(manager) as obj:
        assert obj == "obj"

    # Assert
    assert manager.events == ["enter", "exit"]


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
    class Both(_RecordingManager):
        async def __aenter__(self):
            self.events.append("aenter")
            return self.obj

        async def __aexit__(self, *args):
            self.events.append("aexit")

    manager = Both()

    # Act
    async with resolved(manager) as obj:
        assert obj == "obj"

    # Assert
    assert manager.events == ["enter", "exit"]


@pytest.mark.asyncio
async def test_resolved_should_yield_object_when_expect_satisfied():
    """Test a resolved object of the expected type is yielded.

    Given:
        An async context manager yielding a string, resolved with
        ``expect=str``.
    When:
        It is resolved.
    Then:
        It should yield the string and exit the manager cleanly.
    """
    # Arrange
    manager = _AsyncRecordingManager()

    # Act
    async with resolved(manager, expect=str) as obj:
        assert obj == "obj"

    # Assert
    assert manager.exits == [(None, None, None)]


@pytest.mark.asyncio
async def test_resolved_should_raise_type_error_when_expect_not_satisfied():
    """Test a resolved object of the wrong type fails after entry.

    Given:
        An async context manager yielding a string, resolved with
        ``expect=int``.
    When:
        It is resolved.
    Then:
        It should raise TypeError naming the expected type and exit the
        manager with that failure.
    """
    # Arrange
    manager = _AsyncRecordingManager()

    # Act
    with pytest.raises(TypeError, match="Expected int"):
        async with resolved(manager, expect=int):
            pass

    # Assert
    assert manager.events == ["enter", "exit"]
    assert manager.exits[0][0] is TypeError


@pytest.mark.asyncio
async def test_resolved_should_name_every_type_when_expect_is_a_tuple():
    """Test a wrong-typed object fails against every type of a tuple ``expect``.

    Given:
        An async context manager yielding a string, resolved with
        ``expect=(int, float)``.
    When:
        It is resolved.
    Then:
        It should raise TypeError naming both expected types and exit
        the manager with that failure.
    """
    # Arrange
    manager = _AsyncRecordingManager()

    # Act
    with pytest.raises(TypeError, match=r"Expected int \| float"):
        async with resolved(manager, expect=(int, float)):
            pass

    # Assert
    assert manager.events == ["enter", "exit"]
    assert manager.exits[0][0] is TypeError


@pytest.mark.asyncio
async def test_resolved_should_yield_object_when_expect_tuple_satisfied():
    """Test a resolved object matching one type of a tuple ``expect`` is yielded.

    Given:
        An async context manager yielding a string, resolved with
        ``expect=(str, bytes)``.
    When:
        It is resolved.
    Then:
        It should yield the string and exit the manager cleanly.
    """
    # Arrange
    manager = _AsyncRecordingManager()

    # Act
    async with resolved(manager, expect=(str, bytes)) as obj:
        assert obj == "obj"

    # Assert
    assert manager.exits == [(None, None, None)]
