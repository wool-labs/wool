"""Stdlib parity pins for ``wool.ContextVar`` propagation into loop callbacks.

A :class:`wool.ContextVar` value rides in a single wool-owned stdlib
:class:`contextvars.ContextVar`, so it propagates with stdlib
visibility into every event-loop scheduling edge that snapshots a
:class:`contextvars.Context`: :meth:`loop.call_soon`,
:meth:`call_soon_threadsafe`, :meth:`call_later`, :meth:`call_at`,
:meth:`loop.add_reader` / :meth:`add_writer`,
:meth:`add_signal_handler`, and :meth:`Future.add_done_callback`.

The value-propagation tests take the ``make_var`` fixture and run once
per variable type (:class:`contextvars.ContextVar` and
:class:`wool.ContextVar`), so a single assertion proves the two behave
identically. The ``wool``-only tests additionally pin chain identity:
unlike child tasks, callbacks share the scheduling scope's chain (no
fork) — they run cooperatively on the owning thread.

Every test additionally runs under both the default ``asyncio`` loop
and uvloop, via the ``event_loop_policy`` fixture in ``conftest.py``.
"""

import asyncio
import os
import signal
import socket
import threading
import uuid
from collections.abc import Callable
from typing import TypeVar

import pytest

from wool.runtime.context import ContextVar
from wool.runtime.context import current_snapshot

pytestmark = pytest.mark.stdlib_parity

_T = TypeVar("_T")

# Loose upper bound for awaiting a callback's result. A conformant callback
# fires near-instantly; the timeout is only a backstop so that a callback
# which never resolves the future (for example, one that was never scheduled)
# fails the test fast instead of hanging the suite.
_CALLBACK_TIMEOUT = 5.0


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


def _resolve(future: asyncio.Future[_T], produce: Callable[[], _T]) -> None:
    """Route ``produce()``'s result or exception onto ``future``.

    Used as a loop callback so that a failure inside ``produce()`` — such
    as a ``LookupError`` from a variable that failed to propagate — surfaces
    as a failure on ``future`` instead of being swallowed by the loop's
    exception handler, which would leave the awaiting test hanging.
    """
    if future.done():
        return
    try:
        future.set_result(produce())
    except Exception as exc:
        future.set_exception(exc)


def _scope_chain_id() -> uuid.UUID:
    """Return the active snapshot's chain id, asserting the scope is armed."""
    snapshot = current_snapshot()
    assert snapshot is not None
    return snapshot.chain_id


class TestCallSoonParity:
    @pytest.mark.asyncio
    async def test_call_soon_propagates_a_scoped_value(self, make_var):
        """Test a context variable propagates into a call_soon callback.

        Given:
            A context variable set in the scheduling scope.
        When:
            A loop.call_soon callback reads it.
        Then:
            It should observe the scheduling scope's value, identically
            for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("cs")
        var.set("scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()

        # Act
        loop.call_soon(_resolve, done, var.get)
        observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == "scope"

    @pytest.mark.asyncio
    async def test_call_soon_preserves_the_scope_chain(self):
        """Test a call_soon callback shares the scheduling scope's chain.

        Given:
            A wool.ContextVar set in an armed scheduling scope.
        When:
            A loop.call_soon callback reads current_snapshot().chain_id.
        Then:
            It should observe the scheduling scope's chain id — a
            cooperatively-scheduled callback shares the chain, it is
            not forked onto a fresh one.
        """
        # Arrange
        var = ContextVar(_unique("cs_chain"))
        var.set("scope")
        scope_chain = _scope_chain_id()
        loop = asyncio.get_running_loop()
        done: asyncio.Future[uuid.UUID] = loop.create_future()

        # Act
        loop.call_soon(_resolve, done, _scope_chain_id)
        observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == scope_chain

    @pytest.mark.asyncio
    async def test_call_soon_threadsafe_from_a_foreign_thread(self, make_var):
        """Test call_soon_threadsafe captures the scheduling thread's context.

        Given:
            A context variable set on the loop thread and a separate OS
            thread that never entered that scope.
        When:
            The foreign thread schedules a callback via
            loop.call_soon_threadsafe that reads the variable with a
            fallback.
        Then:
            It should observe the fallback — call_soon_threadsafe
            captures the scheduling thread's context, not the loop
            thread's, identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("cst")
        var.set("loop-scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()

        def schedule_from_foreign_thread() -> None:
            loop.call_soon_threadsafe(_resolve, done, lambda: var.get("fallback"))

        # Act
        worker = threading.Thread(target=schedule_from_foreign_thread)
        worker.start()
        worker.join(timeout=_CALLBACK_TIMEOUT)
        observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == "fallback"


class TestCallLaterParity:
    @pytest.mark.asyncio
    async def test_call_later_propagates_a_scoped_value(self, make_var):
        """Test a context variable propagates into a call_later callback.

        Given:
            A context variable set in the scheduling scope.
        When:
            A loop.call_later callback reads it.
        Then:
            It should observe the scheduling scope's value, identically
            for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("cl")
        var.set("scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()

        # Act
        loop.call_later(0, _resolve, done, var.get)
        observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == "scope"

    @pytest.mark.asyncio
    async def test_call_at_propagates_a_scoped_value(self, make_var):
        """Test a context variable propagates into a call_at callback.

        Given:
            A context variable set in the scheduling scope.
        When:
            A loop.call_at callback reads it.
        Then:
            It should observe the scheduling scope's value, identically
            for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("cat")
        var.set("scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()

        # Act
        loop.call_at(loop.time(), _resolve, done, var.get)
        observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == "scope"

    @pytest.mark.asyncio
    async def test_call_later_preserves_the_scope_chain(self):
        """Test a call_later timer callback shares the scheduling scope's chain.

        Given:
            A wool.ContextVar set in an armed scheduling scope.
        When:
            A loop.call_later callback reads current_snapshot().chain_id.
        Then:
            It should observe the scheduling scope's chain id — a
            cooperatively-scheduled timer shares the chain, it is not
            forked onto a fresh one.
        """
        # Arrange
        var = ContextVar(_unique("cl_chain"))
        var.set("scope")
        scope_chain = _scope_chain_id()
        loop = asyncio.get_running_loop()
        done: asyncio.Future[uuid.UUID] = loop.create_future()

        # Act
        loop.call_later(0, _resolve, done, _scope_chain_id)
        observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == scope_chain


class TestAddReaderWriterParity:
    @pytest.mark.asyncio
    async def test_add_reader_propagates_a_scoped_value(self, make_var):
        """Test a context variable propagates into an add_reader callback.

        Given:
            A context variable set in the scheduling scope and a socket
            pair with data ready to read.
        When:
            A loop.add_reader callback reads it.
        Then:
            It should observe the scheduling scope's value, identically
            for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("rd")
        var.set("scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()
        reader, writer = socket.socketpair()
        writer.send(b"x")

        def on_readable() -> None:
            loop.remove_reader(reader.fileno())
            _resolve(done, var.get)

        # Act
        loop.add_reader(reader.fileno(), on_readable)
        try:
            observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)
        finally:
            reader.close()
            writer.close()

        # Assert
        assert observed == "scope"

    @pytest.mark.asyncio
    async def test_add_writer_propagates_a_scoped_value(self, make_var):
        """Test a context variable propagates into an add_writer callback.

        Given:
            A context variable set in the scheduling scope and a socket
            that is writable.
        When:
            A loop.add_writer callback reads it.
        Then:
            It should observe the scheduling scope's value, identically
            for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("wr")
        var.set("scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()
        left, right = socket.socketpair()

        def on_writable() -> None:
            loop.remove_writer(left.fileno())
            _resolve(done, var.get)

        # Act
        loop.add_writer(left.fileno(), on_writable)
        try:
            observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)
        finally:
            left.close()
            right.close()

        # Assert
        assert observed == "scope"

    @pytest.mark.asyncio
    async def test_add_reader_preserves_the_scope_chain(self):
        """Test an add_reader callback shares the scheduling scope's chain.

        Given:
            A wool.ContextVar set in an armed scheduling scope and a
            socket pair with data ready to read.
        When:
            A loop.add_reader callback reads current_snapshot().chain_id.
        Then:
            It should observe the scheduling scope's chain id — a
            cooperatively-scheduled I/O callback shares the chain, it is
            not forked onto a fresh one.
        """
        # Arrange
        var = ContextVar(_unique("rd_chain"))
        var.set("scope")
        scope_chain = _scope_chain_id()
        loop = asyncio.get_running_loop()
        done: asyncio.Future[uuid.UUID] = loop.create_future()
        reader, writer = socket.socketpair()
        writer.send(b"x")

        def on_readable() -> None:
            loop.remove_reader(reader.fileno())
            _resolve(done, _scope_chain_id)

        # Act
        loop.add_reader(reader.fileno(), on_readable)
        try:
            observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)
        finally:
            reader.close()
            writer.close()

        # Assert
        assert observed == scope_chain

    @pytest.mark.asyncio
    async def test_add_reader_observes_the_registration_time_snapshot(self, make_var):
        """Test an add_reader callback observes the registration-time value.

        Given:
            A context variable set to one value before add_reader is
            called, then mutated to a different value after registration
            but before the callback fires.
        When:
            A loop.add_reader callback reads the variable.
        Then:
            It should observe the registration-time value, not the
            post-mutation value — add_reader copies the context at
            registration, identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("rd_snap")
        var.set("at-registration")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()
        reader, writer = socket.socketpair()
        writer.send(b"x")

        def on_readable() -> None:
            loop.remove_reader(reader.fileno())
            _resolve(done, var.get)

        loop.add_reader(reader.fileno(), on_readable)
        var.set("after-mutation")

        # Act
        try:
            observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)
        finally:
            reader.close()
            writer.close()

        # Assert
        assert observed == "at-registration"

    @pytest.mark.asyncio
    async def test_add_writer_observes_the_registration_time_snapshot(self, make_var):
        """Test an add_writer callback observes the registration-time value.

        Given:
            A context variable set to one value before add_writer is
            called, then mutated to a different value after registration
            but before the callback fires.
        When:
            A loop.add_writer callback reads the variable.
        Then:
            It should observe the registration-time value, not the
            post-mutation value — add_writer copies the context at
            registration, identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("wr_snap")
        var.set("at-registration")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()
        left, right = socket.socketpair()

        def on_writable() -> None:
            loop.remove_writer(left.fileno())
            _resolve(done, var.get)

        loop.add_writer(left.fileno(), on_writable)
        var.set("after-mutation")

        # Act
        try:
            observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)
        finally:
            left.close()
            right.close()

        # Assert
        assert observed == "at-registration"


class TestAddSignalHandlerParity:
    @pytest.mark.asyncio
    async def test_add_signal_handler_propagates_a_scoped_value(self, make_var):
        """Test a context variable propagates into a signal handler.

        Given:
            A context variable set in the scheduling scope and a SIGUSR1
            handler registered via loop.add_signal_handler.
        When:
            SIGUSR1 is raised and the handler reads the variable.
        Then:
            It should observe the scheduling scope's value, identically
            for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("sig")
        var.set("scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()

        def on_signal() -> None:
            loop.remove_signal_handler(signal.SIGUSR1)
            _resolve(done, var.get)

        # Act
        loop.add_signal_handler(signal.SIGUSR1, on_signal)
        os.kill(os.getpid(), signal.SIGUSR1)
        observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == "scope"

    @pytest.mark.asyncio
    async def test_add_signal_handler_preserves_the_scope_chain(self):
        """Test a signal handler shares the scheduling scope's chain.

        Given:
            A wool.ContextVar set in an armed scheduling scope and a
            SIGUSR1 handler registered via loop.add_signal_handler.
        When:
            SIGUSR1 is raised and the handler reads
            current_snapshot().chain_id.
        Then:
            It should observe the scheduling scope's chain id — a
            cooperatively-scheduled signal handler shares the chain, it
            is not forked onto a fresh one.
        """
        # Arrange
        var = ContextVar(_unique("sig_chain"))
        var.set("scope")
        scope_chain = _scope_chain_id()
        loop = asyncio.get_running_loop()
        done: asyncio.Future[uuid.UUID] = loop.create_future()

        def on_signal() -> None:
            loop.remove_signal_handler(signal.SIGUSR1)
            _resolve(done, _scope_chain_id)

        # Act
        loop.add_signal_handler(signal.SIGUSR1, on_signal)
        os.kill(os.getpid(), signal.SIGUSR1)
        observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == scope_chain


class TestFutureDoneCallbackParity:
    @pytest.mark.asyncio
    async def test_add_done_callback_propagates_a_scoped_value(self, make_var):
        """Test a context variable propagates into a done callback.

        Given:
            A context variable set in the scope that adds the callback.
        When:
            Future.add_done_callback fires after the future resolves.
        Then:
            It should observe the scheduling scope's value, identically
            for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("dc")
        var.set("scope")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        observed_future: asyncio.Future[str] = loop.create_future()
        future.add_done_callback(lambda _: _resolve(observed_future, var.get))

        # Act
        future.set_result(None)
        observed = await asyncio.wait_for(observed_future, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == "scope"

    @pytest.mark.asyncio
    async def test_add_done_callback_preserves_the_scope_chain(self):
        """Test a done callback shares the scheduling scope's chain.

        Given:
            A wool.ContextVar set in the armed scope that adds the
            callback.
        When:
            Future.add_done_callback fires and reads
            current_snapshot().chain_id.
        Then:
            It should observe the scheduling scope's chain id — a
            cooperatively-scheduled done callback shares the chain, it
            is not forked onto a fresh one.
        """
        # Arrange
        var = ContextVar(_unique("dc_chain"))
        var.set("scope")
        scope_chain = _scope_chain_id()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        observed_future: asyncio.Future[uuid.UUID] = loop.create_future()
        future.add_done_callback(lambda _: _resolve(observed_future, _scope_chain_id))

        # Act
        future.set_result(None)
        observed = await asyncio.wait_for(observed_future, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == scope_chain
