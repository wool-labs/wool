"""Stdlib parity pins for ``wool.ContextVar`` propagation into loop callbacks.

A :class:`wool.ContextVar` value rides in a single wool-owned stdlib
:class:`contextvars.ContextVar`, so it propagates with stdlib
visibility into every event-loop scheduling edge that contexts a
:class:`contextvars.Context`: :meth:`loop.call_soon`,
:meth:`call_soon_threadsafe`, :meth:`call_later`, :meth:`call_at`,
:meth:`loop.add_reader` / :meth:`add_writer`,
:meth:`add_signal_handler`, and :meth:`Future.add_done_callback`.

The value-propagation tests take the ``make_var`` fixture and run once
per variable type (:class:`contextvars.ContextVar` and
:class:`wool.ContextVar`), so a single assertion proves the two behave
identically. They are additionally parametrized over the scheduling
edge, so one body pins every callback type. The ``wool``-only tests
additionally pin chain identity: unlike child tasks, callbacks share
the scheduling scope's chain (no fork) — they run cooperatively on the
owning thread.

Every test additionally runs under both the default ``asyncio`` loop
and uvloop, via the ``event_loop_policy`` fixture in ``conftest.py``.
"""

import asyncio
import contextlib
import os
import signal
import socket
import threading
import uuid
from collections.abc import Callable
from collections.abc import Iterator
from typing import TypeVar

import pytest

from wool.runtime.context import ContextVar
from wool.runtime.context import current_context

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
    except BaseException as exc:  # noqa: BLE001 — route every failure to the future
        future.set_exception(exc)


def _scope_chain_id() -> uuid.UUID:
    """Return the active context's chain id, asserting the scope is armed."""
    context = current_context()
    assert context is not None
    return context.chain_id


class _CallbackScheduler:
    """One event-loop scheduling edge wired to drive a single callback.

    Each instance knows how to register *callback* (a zero-argument
    function) on its scheduling edge and how to tear that registration
    down afterward. Built fresh per test by :func:`scheduler`, so a
    single parametrized test body pins every callback type.
    """

    def __init__(self, name: str):
        self.name = name

    @contextlib.contextmanager
    def schedule(
        self, loop: asyncio.AbstractEventLoop, callback: Callable[[], None]
    ) -> Iterator[None]:
        """Register *callback* on this edge; clean up on block exit."""
        raise NotImplementedError  # pragma: no cover — overridden per edge


class _CallSoonScheduler(_CallbackScheduler):
    @contextlib.contextmanager
    def schedule(self, loop, callback):
        loop.call_soon(callback)
        yield


class _CallLaterScheduler(_CallbackScheduler):
    @contextlib.contextmanager
    def schedule(self, loop, callback):
        loop.call_later(0, callback)
        yield


class _CallAtScheduler(_CallbackScheduler):
    @contextlib.contextmanager
    def schedule(self, loop, callback):
        loop.call_at(loop.time(), callback)
        yield


class _AddReaderScheduler(_CallbackScheduler):
    @contextlib.contextmanager
    def schedule(self, loop, callback):
        reader, writer = socket.socketpair()
        writer.send(b"x")

        def on_readable() -> None:
            loop.remove_reader(reader.fileno())
            callback()

        loop.add_reader(reader.fileno(), on_readable)
        try:
            yield
        finally:
            reader.close()
            writer.close()


class _AddWriterScheduler(_CallbackScheduler):
    @contextlib.contextmanager
    def schedule(self, loop, callback):
        left, right = socket.socketpair()

        def on_writable() -> None:
            loop.remove_writer(left.fileno())
            callback()

        loop.add_writer(left.fileno(), on_writable)
        try:
            yield
        finally:
            left.close()
            right.close()


class _AddSignalHandlerScheduler(_CallbackScheduler):
    @contextlib.contextmanager
    def schedule(self, loop, callback):
        def on_signal() -> None:
            loop.remove_signal_handler(signal.SIGUSR1)
            callback()

        loop.add_signal_handler(signal.SIGUSR1, on_signal)
        os.kill(os.getpid(), signal.SIGUSR1)
        yield


class _DoneCallbackScheduler(_CallbackScheduler):
    @contextlib.contextmanager
    def schedule(self, loop, callback):
        future: asyncio.Future[None] = loop.create_future()
        future.add_done_callback(lambda _: callback())
        future.set_result(None)
        yield


_SCHEDULERS = [
    _CallSoonScheduler("call_soon"),
    _CallLaterScheduler("call_later"),
    _CallAtScheduler("call_at"),
    _AddReaderScheduler("add_reader"),
    _AddWriterScheduler("add_writer"),
    _AddSignalHandlerScheduler("add_signal_handler"),
    _DoneCallbackScheduler("add_done_callback"),
]


@pytest.fixture(params=_SCHEDULERS, ids=lambda s: s.name)
def scheduler(request) -> _CallbackScheduler:
    """Return one event-loop scheduling edge to pin under parity.

    Parametrizes a callback-propagation test over every loop edge that
    contexts a :class:`contextvars.Context` —
    ``call_soon``/``call_later``/``call_at``,
    ``add_reader``/``add_writer``, ``add_signal_handler``, and
    ``Future.add_done_callback`` — so one test body pins them all.
    """
    return request.param


class TestCallbackValuePropagationParity:
    @pytest.mark.asyncio
    async def test_callback_propagates_a_scoped_value(self, scheduler, make_var):
        """Test a context variable propagates into a loop callback.

        Given:
            A context variable set in the scheduling scope.
        When:
            A callback scheduled on the loop edge under test reads it.
        Then:
            It should observe the scheduling scope's value, identically
            for a stdlib and a wool variable and for every loop edge.
        """
        # Arrange
        var = make_var("cb_value")
        var.set("scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()

        # Act
        with scheduler.schedule(loop, lambda: _resolve(done, var.get)):
            observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == "scope"

    @pytest.mark.asyncio
    async def test_callback_observes_the_registration_time_context(
        self, scheduler, make_var
    ):
        """Test a loop callback observes the registration-time value.

        Given:
            A context variable set to one value before the callback is
            registered, then mutated to a different value after
            registration but before the callback fires.
        When:
            A callback on the loop edge under test reads the variable.
        Then:
            It should observe the registration-time value, not the
            post-mutation value — every loop edge copies the context at
            registration, identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("cb_snap")
        var.set("at-registration")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()

        # Act
        with scheduler.schedule(loop, lambda: _resolve(done, var.get)):
            var.set("after-mutation")
            observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == "at-registration"

    @pytest.mark.asyncio
    async def test_callback_preserves_the_scope_chain(self, scheduler):
        """Test a loop callback shares the scheduling scope's chain.

        Given:
            A wool.ContextVar set in an armed scheduling scope.
        When:
            A callback on the loop edge under test reads
            current_context().chain_id.
        Then:
            It should observe the scheduling scope's chain id — a
            cooperatively-scheduled callback shares the chain, it is
            not forked onto a fresh one, for every loop edge.
        """
        # Arrange
        var = ContextVar(_unique("cb_chain"))
        var.set("scope")
        scope_chain = _scope_chain_id()
        loop = asyncio.get_running_loop()
        done: asyncio.Future[uuid.UUID] = loop.create_future()

        # Act
        with scheduler.schedule(loop, lambda: _resolve(done, _scope_chain_id)):
            observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == scope_chain

    @pytest.mark.asyncio
    async def test_callback_set_does_not_leak_to_the_scheduling_scope(
        self, scheduler, make_var
    ):
        """Test a callback's set does not leak back to the scheduling scope.

        This is Pitfall 2: the callback runs in a copy_context() copy,
        so its write stays in that copy.

        Given:
            A context variable set in the scheduling scope.
        When:
            A callback on the loop edge under test sets the variable to
            a new value.
        Then:
            The scheduling scope should still observe its original
            value — native contextvars copy-on-write isolates the
            callback's write, identically for a stdlib and a wool
            variable and for every loop edge.
        """
        # Arrange
        var = make_var("cb_isolate")
        var.set("scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[None] = loop.create_future()

        def mutate() -> None:
            var.set("callback-local")

        # Act
        with scheduler.schedule(loop, lambda: _resolve(done, mutate)):
            await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert var.get() == "scope"

    @pytest.mark.asyncio
    async def test_callback_lookup_error_surfaces_on_the_future(
        self, scheduler, make_var
    ):
        """Test a LookupError raised inside a callback surfaces correctly.

        Given:
            A context variable with no value and no default.
        When:
            A callback on the loop edge under test reads it.
        Then:
            The read's :class:`LookupError` should surface as the
            awaiting future's exception, identically for a stdlib and a
            wool variable and for every loop edge.
        """
        # Arrange
        var = make_var("cb_lookup")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[object] = loop.create_future()

        # Act & assert
        with scheduler.schedule(loop, lambda: _resolve(done, var.get)):
            with pytest.raises(LookupError):
                await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)


class TestCallSoonThreadsafeParity:
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

    @pytest.mark.asyncio
    async def test_call_soon_threadsafe_from_the_setting_thread(self, make_var):
        """Test call_soon_threadsafe from the setting thread sees the value.

        Given:
            A context variable set on the loop thread.
        When:
            The loop thread itself schedules a callback via
            loop.call_soon_threadsafe that reads the variable.
        Then:
            It should observe the loop scope's value — scheduling from
            the thread that set the variable carries that thread's
            context, identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("cst_self")
        var.set("loop-scope")
        loop = asyncio.get_running_loop()
        done: asyncio.Future[str] = loop.create_future()

        # Act
        loop.call_soon_threadsafe(_resolve, done, var.get)
        observed = await asyncio.wait_for(done, timeout=_CALLBACK_TIMEOUT)

        # Assert
        assert observed == "loop-scope"
