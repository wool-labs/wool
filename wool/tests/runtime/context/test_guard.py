import asyncio
import pickle
import threading
import uuid
import weakref
from uuid import uuid4

import pytest

import wool
from tests.helpers import scoped_context
from wool.runtime.context import ChainContention
from wool.runtime.context import ContextVar
from wool.runtime.context.base import Context
from wool.runtime.context.guard import _assert_chain_owner


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class TestChainContention:
    def test_chain_contention_is_runtime_error_subclass(self):
        """Test ChainContention is a RuntimeError subclass.

        Given:
            The wool.ChainContention exception class.
        When:
            Its subclass relationship to RuntimeError is checked.
        Then:
            It should be a subclass of RuntimeError.
        """
        # Arrange, act, & assert
        assert issubclass(ChainContention, RuntimeError)

    @pytest.mark.asyncio
    async def test_plain_to_thread_from_armed_context(self):
        """Test plain asyncio.to_thread touching a ContextVar trips the guard.

        Given:
            An armed context whose chain is owned by the loop thread.
        When:
            asyncio.to_thread offloads a function that reads a
            wool.ContextVar from a different OS thread.
        Then:
            It should raise wool.ChainContention directing the
            caller to wool.to_thread.
        """
        # Arrange
        var = ContextVar(_unique("plain_to_thread"))

        def touch() -> str:
            return var.get()

        # Act & assert
        with scoped_context():
            var.set("armed")
            with pytest.raises(ChainContention, match="wool.to_thread"):
                await asyncio.to_thread(touch)

    @pytest.mark.asyncio
    async def test_plain_to_thread_set_from_armed_context(self):
        """Test plain asyncio.to_thread setting a ContextVar trips the guard.

        Given:
            An armed context owned by the loop thread.
        When:
            asyncio.to_thread offloads a function that sets a
            wool.ContextVar from a worker thread.
        Then:
            It should raise wool.ChainContention directing the
            caller to wool.to_thread.
        """
        # Arrange
        var = ContextVar(_unique("plain_to_thread_set"))

        def mutate() -> None:
            var.set("from-thread")

        # Act & assert
        with scoped_context():
            var.set("armed")
            with pytest.raises(ChainContention, match="wool.to_thread"):
                await asyncio.to_thread(mutate)

    @pytest.mark.asyncio
    async def test_plain_to_thread_with_unarmed_context(self):
        """Test plain asyncio.to_thread touching a ContextVar in an unarmed context.

        Given:
            An unarmed context — no chain, no guard.
        When:
            asyncio.to_thread offloads a function that reads a
            wool.ContextVar with a default.
        Then:
            It should not raise — an unarmed context behaves as plain
            contextvars.
        """
        # Arrange
        var = ContextVar(_unique("plain_unarmed"), default="d")

        def touch() -> str:
            return var.get()

        # Act
        with scoped_context():
            result = await asyncio.to_thread(touch)

        # Assert
        assert result == "d"

    @pytest.mark.asyncio
    async def test_wool_to_thread_does_not_raise(self):
        """Test wool.to_thread offloading is the supported alternative.

        Given:
            An armed context.
        When:
            wool.to_thread offloads a function that reads a
            wool.ContextVar.
        Then:
            It should not raise — wool.to_thread forks a fresh,
            detached chain owned by the worker thread.
        """
        # Arrange
        var = ContextVar(_unique("wool_to_thread_ok"))

        def touch() -> str:
            return var.get()

        # Act
        with scoped_context():
            var.set("armed")
            result = await wool.to_thread(touch)

        # Assert
        assert result == "armed"

    @pytest.mark.asyncio
    async def test_callbacks_on_owning_thread_do_not_raise(self):
        """Test event-loop callbacks on the owning thread never trip the guard.

        Given:
            An armed context owned by the loop thread.
        When:
            A loop.call_soon callback reads a wool.ContextVar.
        Then:
            It should not raise — cooperative work on the owning thread
            shares the chain but never runs in parallel.
        """
        # Arrange
        var = ContextVar(_unique("callback_owner"))
        loop = asyncio.get_running_loop()
        observed: asyncio.Future[str] = loop.create_future()

        def callback() -> None:
            observed.set_result(var.get())

        # Act
        with scoped_context():
            var.set("armed")
            loop.call_soon(callback)
            result = await observed

        # Assert
        assert result == "armed"

    @pytest.mark.asyncio
    async def test_timer_callbacks_on_owning_thread_do_not_raise(self):
        """Test event-loop timers on the owning thread never trip the guard.

        Given:
            An armed context owned by the loop thread.
        When:
            A loop.call_later timer callback reads a wool.ContextVar.
        Then:
            It should not raise — the guard exemption covers timers as
            well as immediate callbacks; a cooperatively-scheduled timer
            on the owning thread shares the chain but never runs in
            parallel with it.
        """
        # Arrange
        var = ContextVar(_unique("timer_owner"))
        loop = asyncio.get_running_loop()
        observed: asyncio.Future[str] = loop.create_future()

        def callback() -> None:
            observed.set_result(var.get())

        # Act
        with scoped_context():
            var.set("armed")
            loop.call_later(0, callback)
            result = await observed

        # Assert
        assert result == "armed"


class TestAssertOwnerTask:
    def test_assert_owning_task_with_a_none_context(self):
        """Test assert_owning_task is a no-op for a None context.

        Given:
            A None context — an unarmed context.
        When:
            assert_owning_task is called with it.
        Then:
            It should return without raising — an unarmed context
            carries no chain and no task-level guard.
        """
        # Arrange, act, & assert
        assert _assert_chain_owner(None) is None

    def test_assert_owning_task_with_no_running_loop(self):
        """Test assert_owning_task is a no-op when called off any event loop.

        Given:
            An armed context owned by a still-pending asyncio task,
            evaluated from synchronous code with no running event loop.
        When:
            assert_owning_task is called.
        Then:
            It should return without raising — with no running loop
            there is no concurrent task to arbitrate against.
        """
        # Arrange — a pending, never-stepped task held strongly so
        # the owner-check sees a live, not-done owner.
        loop = asyncio.new_event_loop()
        try:

            async def _never_runs() -> None:  # pragma: no cover — never stepped
                return None

            _owning_task = loop.create_task(_never_runs())
            context = Context(
                chain_id=uuid4(),
                _owning_thread=threading.get_ident(),
                _owning_task=weakref.ref(_owning_task),
            )

            # Act & assert — no running loop in this synchronous frame.
            assert _assert_chain_owner(context) is None
        finally:
            # Drain the pending task so its coroutine is closed and no
            # "coroutine was never awaited" RuntimeWarning leaks.
            _owning_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                loop.run_until_complete(_owning_task)
            loop.close()

    @pytest.mark.asyncio
    async def test_assert_owning_task_with_a_done_owning_task(self):
        """Test assert_owning_task is a no-op when the owner task has finished.

        Given:
            An armed context whose owner task has already run to
            completion.
        When:
            assert_owning_task is called from a different running task.
        Then:
            It should return without raising — a finished owner is no
            longer a live concurrent runner.
        """

        # Arrange
        async def _finished() -> None:
            return None

        _owning_task = asyncio.create_task(_finished())
        await _owning_task
        context = Context(
            chain_id=uuid4(),
            _owning_thread=threading.get_ident(),
            _owning_task=weakref.ref(_owning_task),
        )

        # Act & assert
        assert _assert_chain_owner(context) is None

    @pytest.mark.asyncio
    async def test_assert_owning_task_with_a_live_foreign_owning_task(self):
        """Test assert_owning_task raises when a live foreign task owns the chain.

        Given:
            An armed context owned by a still-live asyncio task other
            than the running one.
        When:
            assert_owning_task is called from the running task.
        Then:
            It should raise wool.ChainContention — two tasks
            cannot enter one armed chain at once.
        """
        # Arrange — a live owner task blocked on an event.
        release = asyncio.Event()

        async def _owner() -> None:
            await release.wait()

        _owning_task = asyncio.create_task(_owner())
        await asyncio.sleep(0)  # Let the owner task start.
        context = Context(
            chain_id=uuid4(),
            _owning_thread=threading.get_ident(),
            _owning_task=weakref.ref(_owning_task),
        )

        # Act & assert
        try:
            with pytest.raises(ChainContention):
                _assert_chain_owner(context)
        finally:
            release.set()
            await _owning_task

    @pytest.mark.asyncio
    async def test_assert_owning_task_exception_carries_structured_fields(self):
        """Test the cross-task ChainContention carries structured identity.

        Given:
            An armed context owned by a live foreign asyncio task,
            evaluated from another running task on the same thread.
        When:
            assert_owning_task raises wool.ChainContention.
        Then:
            The exception should expose kind="task", the chain id, the
            owning task, and the current task; the message should
            interpolate the chain id and both task identities for
            diagnostics.
        """
        # Arrange
        release = asyncio.Event()

        async def _owner() -> None:
            await release.wait()

        _owning_task = asyncio.create_task(_owner())
        await asyncio.sleep(0)
        chain_id = uuid4()
        context = Context(
            chain_id=chain_id,
            _owning_thread=threading.get_ident(),
            _owning_task=weakref.ref(_owning_task),
        )

        # Act — capture the message while both tasks are still in
        # their raise-time states (the owner is pending, the current
        # task is running) so the repr() interpolation matches.
        current = asyncio.current_task()
        try:
            with pytest.raises(ChainContention) as excinfo:
                _assert_chain_owner(context)
            exc = excinfo.value
            message = str(exc)
        finally:
            release.set()
            await _owning_task

        # Assert
        assert exc.kind == "task"
        assert exc.chain_id == chain_id
        assert exc.owning_task is _owning_task
        assert exc.current_task is current
        assert str(chain_id) in message
        assert _owning_task.get_name() in message
        assert current is not None
        assert current.get_name() in message

    @pytest.mark.asyncio
    async def test_assert_owning_task_with_the_running_task_as_owner(self):
        """Test assert_owning_task is a no-op when the running task owns the chain.

        Given:
            An armed context whose owner task is the currently running
            task.
        When:
            assert_owning_task is called.
        Then:
            It should return without raising — the chain's own owner is
            never a concurrent runner.
        """
        # Arrange
        context = Context(
            chain_id=uuid4(),
            _owning_thread=threading.get_ident(),
            _owning_task=weakref.ref(asyncio.current_task()),
        )

        # Act & assert
        assert _assert_chain_owner(context) is None


class TestAssertOwningThreadStructuredException:
    def test_assert_owning_thread_exception_carries_structured_fields(self):
        """Test the cross-thread ChainContention carries structured identity.

        Given:
            An armed context whose ``_owning_thread`` is a fabricated
            value that cannot match the current thread.
        When:
            assert_owning_thread raises wool.ChainContention.
        Then:
            The exception should expose kind="thread", the chain id, the
            owning thread ident, and the current thread ident; the
            message should interpolate the chain id and both thread
            identities for diagnostics.
        """
        # Arrange — fabricate a foreign owner-thread ident
        chain_id = uuid4()
        current_ident = threading.get_ident()
        foreign_ident = current_ident + 1
        context = Context(
            chain_id=chain_id,
            _owning_thread=foreign_ident,
        )

        # Act
        with pytest.raises(ChainContention) as excinfo:
            _assert_chain_owner(context)

        # Assert
        exc = excinfo.value
        assert exc.kind == "thread"
        assert exc.chain_id == chain_id
        assert exc.owning_thread == foreign_ident
        assert exc.current_thread == current_ident
        assert str(chain_id) in str(exc)
        assert str(foreign_ident) in str(exc)
        assert str(current_ident) in str(exc)


class TestChainContentionPickling:
    def test_chain_contention_round_trips_through_pickle(self):
        """Test ChainContention survives a pickle round-trip.

        Given:
            A ChainContention raised with structured kwargs.
        When:
            The exception is pickled and unpickled.
        Then:
            The restored instance should carry the same chain id, kind,
            and identity fields, and reconstruct an equivalent message.
        """
        # Arrange
        chain_id = uuid4()
        exc = ChainContention(
            chain_id=chain_id,
            kind="thread",
            owning_thread=12345,
            current_thread=67890,
        )

        # Act
        restored = pickle.loads(pickle.dumps(exc))

        # Assert
        assert isinstance(restored, ChainContention)
        assert restored.chain_id == chain_id
        assert restored.kind == "thread"
        assert restored.owning_thread == 12345
        assert restored.current_thread == 67890
        assert str(restored) == str(exc)

    def test_chain_contention_create_task_kind_message(self):
        """Test the create_task kind interpolates the chain id.

        Given:
            A ChainContention with kind="create_task" raised by the task
            factory when an armed context is re-passed to create_task.
        When:
            The exception's string form is inspected.
        Then:
            The message should mention create_task and interpolate the
            chain id, and the exception's structured fields should
            reflect the create_task kind.
        """
        # Arrange
        chain_id = uuid4()

        # Act
        exc = ChainContention(chain_id=chain_id, kind="create_task")

        # Assert
        assert exc.kind == "create_task"
        assert exc.chain_id == chain_id
        assert "create_task" in str(exc)
        assert str(chain_id) in str(exc)
