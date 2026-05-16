"""Stdlib parity pins for ``wool.ContextVar`` behavior across OS-thread offload.

These tests pin the boundary between cooperative loop work (which
shares a chain safely) and genuine OS-thread parallelism (which must
not). They cover the offload edges:

- :meth:`loop.run_in_executor` — carries NO ``contextvars.Context`` of
  its own, matching stdlib; a bare executor callable sees neither the
  caller's stdlib variables nor a Wool chain. This holds for both
  thread-pool and process-pool executors.
- :func:`asyncio.to_thread` — copies the caller's
  :class:`contextvars.Context` (chain UUID and all) into the worker
  thread; touching a :class:`wool.ContextVar` from an armed context
  then trips :class:`wool.ConcurrentChainEntry`.
- :func:`wool.to_thread` — the supported alternative; forks a fresh
  detached chain owned by the worker thread, with no merge-back.

Also pins armed-gating: an unarmed context behaves as a plain
:class:`contextvars.Context` and incurs no guard. Every test
additionally runs under both the default ``asyncio`` loop and uvloop,
via the ``event_loop_policy`` fixture in ``conftest.py``.
"""

import asyncio
import contextvars
import threading
import uuid
from concurrent.futures import ProcessPoolExecutor

import pytest

import wool
from tests.helpers import context_is_unarmed
from wool.runtime.context import ConcurrentChainEntry
from wool.runtime.context import ContextVar
from wool.runtime.context import current_snapshot

pytestmark = pytest.mark.stdlib_parity


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class TestRunInExecutorParity:
    @pytest.mark.asyncio
    async def test_stdlib_contextvar_absent_in_run_in_executor(self):
        """Test a bare run_in_executor callable does not see a stdlib ContextVar.

        Given:
            A plain contextvars.ContextVar set in the caller scope.
        When:
            loop.run_in_executor offloads a bare callable that reads it
            with a fallback.
        Then:
            It should observe the fallback — run_in_executor carries no
            contextvars.Context of its own.
        """
        # Arrange
        var: contextvars.ContextVar[str] = contextvars.ContextVar(_unique("std_rie"))
        var.set("caller")
        loop = asyncio.get_running_loop()

        # Act
        observed = await loop.run_in_executor(None, lambda: var.get("<fallback>"))

        # Assert
        assert observed == "<fallback>"

    @pytest.mark.asyncio
    async def test_wool_contextvar_absent_in_run_in_executor(self):
        """Test a bare run_in_executor callable carries no Wool chain.

        Given:
            A wool.ContextVar set in an armed caller scope.
        When:
            loop.run_in_executor offloads a bare callable reading
            current_snapshot.
        Then:
            It should observe None — bare run_in_executor carries no
            Wool context, matching stdlib.
        """
        # Arrange
        var = ContextVar(_unique("wool_rie"))
        var.set("caller")  # Arm the context.
        loop = asyncio.get_running_loop()

        # Act
        observed = await loop.run_in_executor(None, current_snapshot)

        # Assert
        assert observed is None

    @pytest.mark.asyncio
    async def test_run_in_executor_with_a_process_pool_carries_no_wool_context(self):
        """Test a bare process-pool run_in_executor callable carries no Wool chain.

        Given:
            A wool.ContextVar set in an armed caller scope.
        When:
            loop.run_in_executor offloads a bare callable to a
            ProcessPoolExecutor that reads current_snapshot.
        Then:
            It should observe no snapshot in the worker process — a bare
            run_in_executor carries no Wool context across a process
            boundary either, matching the thread-pool path and stdlib.
        """
        # Arrange
        var = ContextVar(_unique("wool_rie_proc"))
        var.set("caller")  # Arm the context.
        loop = asyncio.get_running_loop()

        # Act
        with ProcessPoolExecutor(max_workers=1) as pool:
            observed = await loop.run_in_executor(pool, context_is_unarmed)

        # Assert
        assert observed is True


class TestAsyncioToThreadParity:
    @pytest.mark.asyncio
    async def test_stdlib_contextvar_visible_in_asyncio_to_thread(self):
        """Test a stdlib ContextVar value is visible inside asyncio.to_thread.

        Given:
            A plain contextvars.ContextVar set in the caller scope.
        When:
            asyncio.to_thread offloads a function reading it.
        Then:
            It should observe the caller's value — asyncio.to_thread
            copies the caller's contextvars.Context.
        """
        # Arrange
        var: contextvars.ContextVar[str] = contextvars.ContextVar(_unique("std_a2t"))
        var.set("caller")

        # Act
        observed = await asyncio.to_thread(var.get)

        # Assert
        assert observed == "caller"

    @pytest.mark.asyncio
    async def test_plain_to_thread_from_armed_context(self):
        """Test asyncio.to_thread touching a wool.ContextVar trips the guard.

        Given:
            An armed context whose chain is owned by the loop thread.
        When:
            asyncio.to_thread offloads a function that reads a
            wool.ContextVar from a worker thread.
        Then:
            It should raise wool.ConcurrentChainEntry — the copied chain
            reaches an OS thread that does not own it.
        """
        # Arrange
        var = ContextVar(_unique("a2t_guard"))
        var.set("armed")

        # Act & assert
        with pytest.raises(
            ConcurrentChainEntry, match="cannot be shared across OS threads"
        ):
            await asyncio.to_thread(var.get)

    @pytest.mark.asyncio
    async def test_asyncio_to_thread_with_unarmed_context(self):
        """Test asyncio.to_thread touching a wool.ContextVar in an unarmed context.

        Given:
            An unarmed context — no chain, no guard.
        When:
            asyncio.to_thread offloads a function reading a
            wool.ContextVar with a default.
        Then:
            It should not raise — an unarmed context behaves as a
            plain contextvars.Context.
        """
        # Arrange
        var = ContextVar(_unique("a2t_unarmed"), default="d")

        # Act
        observed = await asyncio.to_thread(var.get)

        # Assert
        assert observed == "d"


class TestWoolToThreadParity:
    @pytest.mark.asyncio
    async def test_wool_to_thread_carries_value_without_guard(self):
        """Test wool.to_thread carries the caller's value without tripping the guard.

        Given:
            An armed context with a wool.ContextVar set.
        When:
            wool.to_thread offloads a function reading it.
        Then:
            It should observe the caller's value and not raise
            wool.ConcurrentChainEntry.
        """
        # Arrange
        var = ContextVar(_unique("w2t_value"))
        var.set("caller")

        # Act
        observed = await wool.to_thread(var.get)

        # Assert
        assert observed == "caller"

    @pytest.mark.asyncio
    async def test_wool_to_thread_with_unarmed_context(self):
        """Test wool.to_thread from an unarmed context does not raise.

        Given:
            An unarmed context — no wool.ContextVar has been set.
        When:
            wool.to_thread offloads a function reading a wool.ContextVar
            with a default.
        Then:
            It should return the default and not raise — an unarmed
            context incurs no guard.
        """
        # Arrange
        var = ContextVar(_unique("w2t_unarmed"), default="default")

        # Act
        observed = await wool.to_thread(var.get)

        # Assert
        assert observed == "default"

    @pytest.mark.asyncio
    async def test_wool_to_thread_runs_on_fresh_detached_chain(self):
        """Test wool.to_thread runs the offload on a fresh detached chain.

        Given:
            An armed context whose chain id is known.
        When:
            wool.to_thread offloads a function reading
            current_snapshot().chain_id.
        Then:
            It should differ from the caller's chain id.
        """
        # Arrange
        var = ContextVar(_unique("w2t_chain"))
        var.set("x")  # Arm the context.
        caller = current_snapshot()
        assert caller is not None

        def read_chain() -> uuid.UUID:
            snapshot = current_snapshot()
            assert snapshot is not None
            return snapshot.chain_id

        # Act
        offloaded_chain = await wool.to_thread(read_chain)

        # Assert
        assert offloaded_chain != caller.chain_id

    @pytest.mark.asyncio
    async def test_wool_to_thread_runs_on_worker_thread(self):
        """Test wool.to_thread runs the offloaded function off the loop thread.

        Given:
            An armed context and the running loop's thread id.
        When:
            wool.to_thread offloads a function reading its own thread
            id.
        Then:
            It should differ from the loop thread's id.
        """
        # Arrange
        var = ContextVar(_unique("w2t_thread"))
        var.set("x")  # Arm the context.
        loop_thread = threading.get_ident()

        # Act
        offloaded_thread = await wool.to_thread(threading.get_ident)

        # Assert
        assert offloaded_thread != loop_thread

    @pytest.mark.asyncio
    async def test_wool_to_thread_with_mutation(self):
        """Test wool.to_thread inherits a copy of the caller's bindings.

        Given:
            An armed context with a wool.ContextVar set.
        When:
            wool.to_thread offloads a function that reads the variable,
            sets it, and reads it again.
        Then:
            It should observe the caller's value first (the detached
            chain inherited a copy of the caller's bindings), then its
            own mutation — while the caller's value stays unchanged, so
            the mutation does not merge back.
        """
        # Arrange
        var = ContextVar(_unique("w2t_no_merge"))
        var.set("caller")

        def mutate() -> tuple[str, str]:
            before = var.get()
            var.set("thread")
            after = var.get()
            return before, after

        # Act
        before, after = await wool.to_thread(mutate)

        # Assert
        assert before == "caller"
        assert after == "thread"
        assert var.get() == "caller"


class TestArmedGating:
    @pytest.mark.asyncio
    async def test_unarmed_context_has_no_snapshot(self):
        """Test an unarmed context carries no snapshot.

        Given:
            A fresh context where no wool.ContextVar has been set.
        When:
            current_snapshot is read.
        Then:
            It should return None — the context behaves as a plain
            contextvars.Context.
        """
        # Arrange, act, & assert
        assert current_snapshot() is None

    @pytest.mark.asyncio
    async def test_first_set_arms_the_context(self):
        """Test the first wool.ContextVar.set arms the context.

        Given:
            An unarmed context.
        When:
            A wool.ContextVar is set for the first time.
        Then:
            It should return a snapshot from current_snapshot.
        """
        # Arrange
        var = ContextVar(_unique("arm_first_set"))

        # Act
        var.set("x")

        # Assert
        assert current_snapshot() is not None
