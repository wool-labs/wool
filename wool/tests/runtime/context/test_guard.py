import asyncio
import contextvars
import threading
import uuid

import pytest

import wool
from tests.helpers import scoped_context
from wool.runtime.context.exceptions import ChainContention
from wool.runtime.context.factory import install_task_factory
from wool.runtime.context.var import ContextVar


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


def test_get_should_not_raise_when_owner_task_is_pending_off_loop():
    """Test reading an armed var off any loop on the owning thread is a no-op.

    Given:
        An armed context whose chain is owned by a still-pending asyncio
        task, with its contextvars.Context captured while that task is
        live, and the driving loop subsequently stopped.
    When:
        A wool.ContextVar is read inside that captured context from
        synchronous code on the owning thread, with no running event
        loop.
    Then:
        It should return the armed value without raising — with no
        running loop there is no concurrent task to arbitrate against.
    """
    # Arrange — arm the chain inside a task that stays pending so its
    # owner reference is live (not done) when the off-loop read runs.
    # A dedicated loop is driven to the arming point and then left idle
    # (not closed) so the off-loop read runs on the owning thread with
    # no loop running in its frame.
    var = ContextVar(_unique("pending_off_loop"))
    captured: dict[str, object] = {}
    loop = asyncio.new_event_loop()

    async def _arm_and_block(armed: asyncio.Event) -> None:
        var.set("armed")
        captured["context"] = contextvars.copy_context()
        armed.set()
        await asyncio.Future()  # never completes — keep the owner live

    async def _drive() -> asyncio.Task[None]:
        armed = asyncio.Event()
        task = loop.create_task(_arm_and_block(armed))
        await armed.wait()
        return task

    owning_task = loop.run_until_complete(_drive())
    context = captured["context"]
    assert isinstance(context, contextvars.Context)

    # Act — the loop has stopped (run_until_complete returned) but the
    # owner task is still pending; read synchronously inside the
    # captured armed context with no running loop in this frame.
    try:
        result = context.run(var.get)
    finally:
        # Drain the pending task so its coroutine is closed and no
        # "coroutine was never awaited" RuntimeWarning leaks.
        owning_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            loop.run_until_complete(owning_task)
        loop.close()

    # Assert
    assert result == "armed"


def test_get_should_not_raise_when_owner_task_is_done():
    """Test reading an armed var whose owner task has finished is a no-op.

    Given:
        An armed context whose owner task has already run to completion,
        with its contextvars.Context captured.
    When:
        A wool.ContextVar is read inside that captured context from
        synchronous code on the owning thread.
    Then:
        It should return the armed value without raising — a finished
        owner is no longer a live concurrent runner.
    """
    # Arrange — drive an arming coroutine to completion so its owner
    # task is done, then capture its armed context.
    var = ContextVar(_unique("done_owner"))
    captured: dict[str, object] = {}
    loop = asyncio.new_event_loop()

    async def _arm() -> None:
        var.set("armed")
        captured["context"] = contextvars.copy_context()

    try:
        loop.run_until_complete(_arm())
    finally:
        loop.close()
    context = captured["context"]
    assert isinstance(context, contextvars.Context)

    # Act — the owner task is now done; read synchronously inside the
    # captured armed context on the owning thread.
    result = context.run(var.get)

    # Assert
    assert result == "armed"


class TestChainContention:
    @pytest.mark.asyncio
    async def test_get_should_raise_contention_when_plain_to_thread_from_armed_context(
        self,
    ):
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
    async def test_set_should_raise_contention_when_plain_to_thread_from_armed_context(
        self,
    ):
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
    async def test_get_should_carry_structured_fields_when_cross_thread_contention(self):
        """Test a cross-thread ChainContention carries structured identity.

        Given:
            An armed context owned by the loop thread.
        When:
            asyncio.to_thread offloads a function that reads a
            wool.ContextVar from a worker thread, tripping the guard.
        Then:
            The exception should expose kind="thread", the chain id, the
            owning thread ident, and the offending worker-thread ident;
            the message should interpolate the chain id and both thread
            identities for diagnostics.
        """
        # Arrange
        var = ContextVar(_unique("thread_fields"))
        observed: dict[str, int] = {}

        def touch() -> str:
            observed["worker_thread"] = threading.get_ident()
            return var.get()

        # Act
        with scoped_context():
            var.set("armed")
            owning_thread = threading.get_ident()
            chain = wool.__chain__.get(None)
            with pytest.raises(ChainContention) as excinfo:
                await asyncio.to_thread(touch)

        # Assert
        exc = excinfo.value
        assert chain is not None
        assert exc.kind == "thread"
        assert exc.chain_id == chain.id
        assert exc.owning_thread == owning_thread
        assert exc.current_thread == observed["worker_thread"]
        message = str(exc)
        assert str(chain.id) in message
        assert str(owning_thread) in message
        assert str(observed["worker_thread"]) in message

    @pytest.mark.asyncio
    async def test_get_should_carry_structured_fields_when_cross_task_contention(self):
        """Test a cross-task ChainContention carries structured identity.

        Given:
            One unarmed contextvars.Context handed to two tasks; the
            first arms it with a wool.ContextVar.set and the second
            then touches a wool.ContextVar — the cross-task path the
            task factory's copy-on-fork cannot catch.
        When:
            The second task touches the armed chain it does not own.
        Then:
            It should raise wool.ChainContention exposing kind="task",
            the chain id, the owning (first) task, and the current
            (second) task; the message should interpolate the chain id
            and both task identities for diagnostics.
        """
        # Arrange — share one unarmed context between two tasks. Sharing
        # an unarmed context is permitted exactly as stdlib allows; the
        # first task arms it, then the second task fails loud.
        install_task_factory()
        var = ContextVar(_unique("task_fields"))
        shared = contextvars.copy_context()
        loop = asyncio.get_running_loop()
        armed = asyncio.Event()
        first_can_finish = asyncio.Event()
        identities: dict[str, object] = {}

        async def first() -> None:
            var.set("armed-by-first")
            identities["owning_task"] = asyncio.current_task()
            identities["chain"] = wool.__chain__.get(None)
            armed.set()
            await first_can_finish.wait()

        async def second() -> BaseException | None:
            await armed.wait()
            identities["current_task"] = asyncio.current_task()
            try:
                var.get("fallback")
            except ChainContention as exc:
                return exc
            finally:
                first_can_finish.set()
            return None

        # Act — both tasks are handed the SAME context object.
        first_task = loop.create_task(first(), context=shared)
        second_task = loop.create_task(second(), context=shared)
        observed, _ = await asyncio.gather(second_task, first_task)

        # Assert
        assert isinstance(observed, ChainContention)
        chain = identities["chain"]
        assert chain is not None
        assert observed.kind == "task"
        assert observed.chain_id == chain.id
        assert observed.owning_task is identities["owning_task"]
        assert observed.current_task is identities["current_task"]
        message = str(observed)
        assert str(chain.id) in message

    @pytest.mark.asyncio
    async def test_get_should_not_raise_when_plain_to_thread_from_unarmed_context(self):
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
    async def test_get_should_not_raise_when_wool_to_thread_from_armed_context(self):
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
    async def test_get_should_not_raise_when_callback_on_owning_thread(self):
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
    async def test_get_should_not_raise_when_timer_callback_on_owning_thread(self):
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
