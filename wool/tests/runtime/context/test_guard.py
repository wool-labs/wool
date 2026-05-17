import asyncio
import uuid

import pytest

import wool
from tests.helpers import scoped_context
from wool.runtime.context import ConcurrentChainEntry
from wool.runtime.context import ContextVar


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class TestConcurrentChainEntry:
    def test_concurrent_chain_entry_is_runtime_error_subclass(self):
        """Test ConcurrentChainEntry is a RuntimeError subclass.

        Given:
            The wool.ConcurrentChainEntry exception class.
        When:
            Its subclass relationship to RuntimeError is checked.
        Then:
            It should be a subclass of RuntimeError.
        """
        # Arrange, act, & assert
        assert issubclass(ConcurrentChainEntry, RuntimeError)

    @pytest.mark.asyncio
    async def test_plain_to_thread_from_armed_context(self):
        """Test plain asyncio.to_thread touching a ContextVar trips the guard.

        Given:
            An armed context whose chain is owned by the loop thread.
        When:
            asyncio.to_thread offloads a function that reads a
            wool.ContextVar from a different OS thread.
        Then:
            It should raise wool.ConcurrentChainEntry directing the
            caller to wool.to_thread.
        """
        # Arrange
        var = ContextVar(_unique("plain_to_thread"))

        def touch() -> str:
            return var.get()

        # Act & assert
        with scoped_context():
            var.set("armed")
            with pytest.raises(ConcurrentChainEntry, match="wool.to_thread"):
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
            It should raise wool.ConcurrentChainEntry directing the
            caller to wool.to_thread.
        """
        # Arrange
        var = ContextVar(_unique("plain_to_thread_set"))

        def mutate() -> None:
            var.set("from-thread")

        # Act & assert
        with scoped_context():
            var.set("armed")
            with pytest.raises(ConcurrentChainEntry, match="wool.to_thread"):
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
