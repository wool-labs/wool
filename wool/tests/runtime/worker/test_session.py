"""Unit tests for :mod:`wool.runtime.worker.session`.

These tests exercise :class:`Rejected` and :class:`DispatchSession` through
their public surface (constructor, ``async with``, ``async for``,
:meth:`drain`, :meth:`cancel`, public attributes ``task``/``context``/
``serializer``). Worker-loop interactions use the
``new_event_loop + threading.Thread + install_task_factory`` pattern
so the handler can drive a real :func:`scoped` routine across loops.
"""

from __future__ import annotations

import asyncio
import threading
from uuid import uuid4

import pytest
from pytest_mock import MockerFixture

import wool
from tests.runtime.worker.conftest import PicklableMock
from wool import protocol
from wool.runtime.context import Context
from wool.runtime.context import install_task_factory
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.serializer import PassthroughSerializer
from wool.runtime.serializer import _passthrough_pool
from wool.runtime.worker.session import DispatchSession
from wool.runtime.worker.session import Rejected

# Module-level routines (must be picklable for cloudpickle transport).


async def _coro_returning(value):
    return value


async def _coro_returning_default():
    return "coroutine_value"


async def _gen_yielding(values):
    for v in values:
        yield v


async def _gen_default_two():
    yield "a"
    yield "b"


async def _gen_three():
    yield 1
    yield 2
    yield 3


class _CustomRoutineError(Exception):
    """Distinct exception raised by routines under test."""


async def _gen_raises_mid_stream():
    yield "before"
    raise _CustomRoutineError("mid-stream failure")


async def _slow_gen():
    while True:
        await asyncio.sleep(60)
        yield "never"


def _sync_callable():
    return "not_async"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task(callable_obj, *, serializer=None):
    """Build a :class:`Task` whose proxy is a picklable mock."""
    mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
    return Task(
        id=uuid4(),
        callable=callable_obj,
        args=(),
        kwargs={},
        proxy=mock_proxy,
    )


def _request_for(task: Task, *, serializer=None) -> protocol.Request:
    """Build a first-frame Request carrying *task*'s protobuf."""
    return protocol.Request(task=task.to_protobuf(serializer=serializer))


def _next_request() -> protocol.Request:
    """Build a follow-up ``next`` request frame."""
    return protocol.Request(next=protocol.Void())


async def _stream(*requests):
    """Build an async iterator over *requests*."""
    for r in requests:
        yield r


@pytest.fixture
def worker_loop():
    """Spin up a real worker loop on a daemon thread.

    Mirrors the install_task_factory + threading.Thread pattern used in
    test_service.py so :func:`scoped` can run on a separate loop.
    """
    loop = asyncio.new_event_loop()
    install_task_factory(loop)
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        if not loop.is_closed():
            loop.close()


# ---------------------------------------------------------------------------
# Rejected
# ---------------------------------------------------------------------------


class TestRejected:
    """Tests for :class:`Rejected`."""

    def test___init___with_arbitrary_exception(self):
        """Test :class:`Rejected` wraps an arbitrary exception.

        Given:
            An arbitrary :class:`Exception` with a non-empty message
        When:
            :class:`Rejected` is constructed around it
        Then:
            It should expose the same instance on ``.original`` and
            stringify as ``"<TypeName>: <message>"``.
        """
        # Arrange
        original = ValueError("malformed task id")

        # Act
        rejected = Rejected(original)

        # Assert
        assert rejected.original is original
        assert str(rejected) == "ValueError: malformed task id"

    def test___init___with_custom_exception_subclass(self):
        """Test :class:`Rejected` preserves a custom subclass and ``str()``.

        Given:
            An exception subclass with a custom ``__str__``
        When:
            :class:`Rejected` is constructed around it
        Then:
            It should include the subclass ``__name__`` and the custom
            ``str()`` output verbatim in the message.
        """

        # Arrange
        class _Boom(RuntimeError):
            def __str__(self):  # noqa: D401 — test fixture
                return "custom-rendered"

        original = _Boom()

        # Act
        rejected = Rejected(original)

        # Assert
        assert rejected.original is original
        assert str(rejected) == "_Boom: custom-rendered"


# ---------------------------------------------------------------------------
# DispatchSession
# ---------------------------------------------------------------------------


class TestDispatchSession:
    """Tests for :class:`DispatchSession`."""

    # -- construction -----------------------------------------------------

    def test___init___with_request_stream_and_worker_loop(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :class:`DispatchSession` constructor leaves public attrs unset.

        Given:
            A request stream and a worker loop
        When:
            :class:`DispatchSession` is constructed
        Then:
            It should be created without raising and ``task`` /
            ``context`` / ``serializer`` should be unset before
            ``__aenter__``.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        # Act
        handler = DispatchSession(stream, worker_loop)

        # Assert
        assert isinstance(handler, DispatchSession)
        # Class-level annotations declare ``task`` and ``context``;
        # they are only populated on enter so accessing them before
        # enter raises AttributeError.
        with pytest.raises(AttributeError):
            handler.task  # noqa: B018
        with pytest.raises(AttributeError):
            handler.context  # noqa: B018
        # ``serializer`` is initialized in ``__init__`` to the
        # default cloudpickle so any pre-negotiation parse failure
        # (StopAsyncIteration, malformed task id, bad serializer
        # hint) has a sane serializer in scope for ``Rejected`` to
        # ship the dumped exception. ``__aenter__`` overwrites
        # with the negotiated serializer on successful parse.
        assert handler.serializer is wool.__serializer__

    # -- streaming property ----------------------------------------------

    @pytest.mark.asyncio
    async def test_streaming_with_async_generator_task(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :attr:`streaming` reflects an async-generator task.

        Given:
            A handler past :meth:`__aenter__` with an async-generator
            routine as the parsed Task
        When:
            :attr:`streaming` is read
        Then:
            It should be ``True``.
        """
        # Arrange
        task = _make_task(_gen_default_two)
        stream = _stream(_request_for(task))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            # Assert
            assert handler.streaming is True

    @pytest.mark.asyncio
    async def test_streaming_with_coroutine_task(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :attr:`streaming` reflects a coroutine task.

        Given:
            A handler past :meth:`__aenter__` with a coroutine routine
            as the parsed Task
        When:
            :attr:`streaming` is read
        Then:
            It should be ``False``.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            # Assert
            assert handler.streaming is False

    # -- __aenter__ -------------------------------------------------------

    @pytest.mark.asyncio
    async def test___aenter___with_coroutine_task_and_cloudpickle(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aenter__` populates public attrs for a
        cloudpickle-encoded coroutine task.

        Given:
            A request stream whose first frame is a valid coroutine
            :class:`Task` with cloudpickle serialization
        When:
            The handler is entered via ``async with``
        Then:
            It should populate ``handler.task``, ``handler.context``,
            and set ``handler.serializer`` to ``wool.__serializer__``.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            # Assert
            assert isinstance(handler.task, Task)
            assert handler.task.callable is _coro_returning_default
            assert isinstance(handler.context, Context)
            assert handler.serializer is wool.__serializer__

    @pytest.mark.asyncio
    async def test___aenter___with_passthrough_serializer_self_dispatch(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aenter__` acquires/releases a passthrough-pool
        entry on self-dispatch.

        Given:
            A request stream whose first frame is a Task encoded with
            :class:`PassthroughSerializer`
        When:
            The handler is entered then exited via ``async with``
        Then:
            It should set ``handler.serializer`` to a
            :class:`PassthroughSerializer` and the pool's
            ``referenced_entries`` count should return to baseline on
            exit.
        """
        # Arrange
        baseline = _passthrough_pool.stats.referenced_entries
        serializer = PassthroughSerializer()
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task, serializer=serializer))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            assert isinstance(handler.serializer, PassthroughSerializer)
            assert _passthrough_pool.stats.referenced_entries == baseline + 1

        # Assert
        assert _passthrough_pool.stats.referenced_entries == baseline

    @pytest.mark.asyncio
    async def test___aenter___with_sync_callable_task(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aenter__` rejects a non-async callable.

        Given:
            A request stream whose first frame is a Task wrapping a
            sync (non-async) callable
        When:
            The handler is entered via ``async with``
        Then:
            It should raise :class:`Rejected` whose ``.original`` is a
            :class:`ValueError` mentioning "coroutine function or
            async generator function".
        """
        # Arrange
        task = _make_task(_sync_callable)
        stream = _stream(_request_for(task))

        # Act & assert
        with pytest.raises(Rejected) as exc_info:
            async with DispatchSession(stream, worker_loop):
                pass

        assert isinstance(exc_info.value.original, ValueError)
        assert "coroutine function or async generator function" in str(
            exc_info.value.original
        )

    @pytest.mark.asyncio
    async def test___aenter___with_malformed_task_id(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aenter__` rejects a malformed task id.

        Given:
            A request stream whose first frame is a Task with a
            malformed (non-UUID) ``task.id``
        When:
            The handler is entered via ``async with``
        Then:
            It should raise :class:`Rejected` whose ``.original`` is a
            :class:`ValueError` from UUID parsing.
        """
        # Arrange — build a real first-frame Task and then mutate its
        # id field to a non-UUID string. Mutating the wire payload
        # avoids reaching into private symbols on the Python side.
        task = _make_task(_coro_returning_default)
        request = _request_for(task)
        request.task.id = "not-a-uuid"
        stream = _stream(request)

        # Act & assert
        with pytest.raises(Rejected) as exc_info:
            async with DispatchSession(stream, worker_loop):
                pass

        assert isinstance(exc_info.value.original, ValueError)

    @pytest.mark.asyncio
    async def test___aenter___with_empty_request_stream(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aenter__` rejects an empty request stream.

        Given:
            A request stream that completes without yielding any frame
        When:
            The handler is entered via ``async with``
        Then:
            It should raise :class:`Rejected` whose ``.original`` is a
            :class:`ValueError` mentioning "empty request stream".
        """

        # Arrange — async generator that yields nothing.
        async def empty_stream():
            if False:
                yield  # pragma: no cover — make this an async generator

        # Act & assert
        with pytest.raises(Rejected) as exc_info:
            async with DispatchSession(empty_stream(), worker_loop):
                pass

        assert isinstance(exc_info.value.original, ValueError)
        assert "empty request stream" in str(exc_info.value.original)

    @pytest.mark.asyncio
    async def test___aenter___with_first_frame_wrong_oneof(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aenter__` rejects a first frame whose payload
        oneof is not ``task``.

        Given:
            A request stream whose first frame carries a ``next``
            payload (the iteration verb) instead of a ``task`` payload
        When:
            The handler is entered via ``async with``
        Then:
            It should raise :class:`Rejected` whose ``.original`` is a
            :class:`ValueError` indicating the first request must
            carry a Task.
        """
        # Arrange — a ``next`` request as the first frame.
        stream = _stream(_next_request())

        # Act & assert
        with pytest.raises(Rejected) as exc_info:
            async with DispatchSession(stream, worker_loop):
                pass

        assert isinstance(exc_info.value.original, ValueError)
        assert "first request must carry a Task" in str(exc_info.value.original)

    @pytest.mark.asyncio
    async def test___aenter___with_cancelled_request_iterator(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aenter__` propagates ``CancelledError`` raw.

        Given:
            A request stream that raises :class:`asyncio.CancelledError`
            on first read
        When:
            The handler is entered via ``async with``
        Then:
            It should propagate :class:`asyncio.CancelledError` raw,
            not wrapped in :class:`Rejected`.
        """

        # Arrange
        async def cancelling_stream():
            raise asyncio.CancelledError()
            yield  # pragma: no cover — make this an async generator

        # Act & assert
        with pytest.raises(asyncio.CancelledError):
            async with DispatchSession(cancelling_stream(), worker_loop):
                pass

    @pytest.mark.asyncio
    async def test___aenter___with_decode_group_of_exceptions(
        self,
        worker_loop,
        mock_worker_proxy_cache,
        mocker: MockerFixture,
    ):
        """Test :meth:`__aenter__` wraps an Exception-only decode
        :class:`BaseExceptionGroup` as :class:`Rejected`.

        Given:
            A request stream whose first frame is well-formed but
            :meth:`Context.from_protobuf` raises a
            :class:`BaseExceptionGroup` of :class:`Exception` peers
        When:
            The handler is entered via ``async with``
        Then:
            It should raise :class:`Rejected` whose ``.original`` is a
            :class:`BaseExceptionGroup` labeled
            "request context decode failed".
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        # Patch Context.from_protobuf to raise an Exception-only group.
        # The constructor downgrades Exception-only groups to
        # ExceptionGroup so the parse-phase ``except Exception`` arm
        # routes it through Rejected.
        peer = ValueError("decode peer")
        eg = BaseExceptionGroup("simulated decode failure", [peer])
        from wool.runtime.context import base as ctx_base

        mocker.patch.object(
            ctx_base.Context,
            "from_protobuf",
            classmethod(lambda cls, *a, **kw: (_ for _ in ()).throw(eg)),
        )

        # Act & assert
        with pytest.raises(Rejected) as exc_info:
            async with DispatchSession(stream, worker_loop):
                pass

        original = exc_info.value.original
        assert isinstance(original, BaseExceptionGroup)
        assert "request context decode failed" in original.message

    @pytest.mark.asyncio
    async def test___aenter___with_decode_group_with_non_exception_peer(
        self,
        worker_loop,
        mock_worker_proxy_cache,
        mocker: MockerFixture,
    ):
        """Test :meth:`__aenter__` propagates a true
        :class:`BaseExceptionGroup` raw.

        Given:
            A request stream whose first frame triggers a
            :class:`BaseExceptionGroup` containing a non-Exception peer
            (e.g. :class:`asyncio.CancelledError`) from
            :meth:`Context.from_protobuf`
        When:
            The handler is entered via ``async with``
        Then:
            It should propagate the :class:`BaseExceptionGroup` raw
            (not as :class:`Rejected`).
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        # A group with a non-Exception peer stays a
        # BaseExceptionGroup (no auto-downgrade) and so falls through
        # the parse-phase ``except Exception`` arm.
        peer = asyncio.CancelledError()
        eg = BaseExceptionGroup("simulated decode failure", [peer])
        from wool.runtime.context import base as ctx_base

        mocker.patch.object(
            ctx_base.Context,
            "from_protobuf",
            classmethod(lambda cls, *a, **kw: (_ for _ in ()).throw(eg)),
        )

        # Act & assert
        with pytest.raises(BaseExceptionGroup) as exc_info:
            async with DispatchSession(stream, worker_loop):
                pass

        # The raised group should carry the rewrap label and the
        # original non-Exception peer, not be wrapped in Rejected.
        assert not isinstance(exc_info.value, Rejected)
        assert "request context decode failed" in exc_info.value.message

    # -- __aiter__ --------------------------------------------------------

    @pytest.mark.asyncio
    async def test___aiter___with_coroutine_routine_yields_single_response(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aiter__` yields exactly one response for a
        coroutine routine.

        Given:
            A handler past :meth:`__aenter__` with a coroutine routine
            returning a value
        When:
            The handler is iterated via ``async for``
        Then:
            It should yield exactly one response whose result is the
            coroutine's return value.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            results = [r async for r in handler]

        # Assert
        assert len(results) == 1
        assert results[0].result == "coroutine_value"

    @pytest.mark.asyncio
    async def test___aiter___with_async_generator_task_yields_per_request(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aiter__` yields one response per caller request
        for an async-generator routine.

        Given:
            A request stream whose first frame is a valid async-generator
            Task plus N caller-driven ``next`` requests
        When:
            The handler is entered and iterated via ``async for``
        Then:
            It should yield N responses carrying the corresponding
            generator values.
        """
        # Arrange
        task = _make_task(_gen_three)
        stream = _stream(
            _request_for(task),
            _next_request(),
            _next_request(),
            _next_request(),
        )

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            results = [r async for r in handler]

        # Assert
        assert [r.result for r in results] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test___aiter___with_async_generator_yields_per_caller_request(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aiter__` drives a multi-step async generator
        through caller requests.

        Given:
            A handler past :meth:`__aenter__` with an async-generator
            routine producing N values, with N caller-driven ``next``
            requests
        When:
            The handler is iterated via ``async for``
        Then:
            It should yield N responses with the corresponding values.
        """
        # Arrange — exercise N=2 to distinguish from coroutine path
        task = _make_task(_gen_default_two)
        stream = _stream(
            _request_for(task),
            _next_request(),
            _next_request(),
        )

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            results = [r async for r in handler]

        # Assert
        assert [r.result for r in results] == ["a", "b"]

    @pytest.mark.asyncio
    async def test___aiter___called_twice_returns_same_iterator(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aiter__` is idempotent across calls.

        Given:
            A handler past :meth:`__aenter__`
        When:
            ``aiter(handler)`` is invoked twice
        Then:
            It should return the same iterator instance on the second
            call.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            first = aiter(handler)
            second = aiter(handler)

            # Assert
            assert first is second

            # Drain so __aexit__ unwinds promptly.
            async for _ in first:
                pass

    @pytest.mark.asyncio
    async def test___aiter___with_routine_raising_mid_stream(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aiter__` propagates a routine's mid-stream
        exception raw.

        Given:
            A handler driving an async-generator routine that raises a
            custom exception mid-stream
        When:
            The iterator is consumed until the exception
        Then:
            The exception should propagate raw out of the iterator.
        """
        # Arrange
        task = _make_task(_gen_raises_mid_stream)
        stream = _stream(
            _request_for(task),
            _next_request(),
            _next_request(),
        )

        # Act & assert
        with pytest.raises(_CustomRoutineError, match="mid-stream failure"):
            async with DispatchSession(stream, worker_loop) as handler:
                async for _ in handler:
                    pass

    @pytest.mark.asyncio
    async def test___aiter___with_streaming_eof_closes_request_queue(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aiter__` exits cleanly when the request stream
        ends after the initial Task frame.

        Given:
            A handler past :meth:`__aenter__` whose request stream ends
            after the initial Task frame
        When:
            The iterator is fully consumed via ``async for``
        Then:
            It should exit cleanly and ``__aexit__`` returns promptly
            (the request queue is closed eagerly on EOF).
        """
        # Arrange — async-generator routine + only the Task frame
        task = _make_task(_gen_default_two)
        stream = _stream(_request_for(task))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            results = [r async for r in handler]

        # Assert — no caller-driven `next` was sent, so no values are
        # produced; the iterator exits cleanly when the request stream
        # ends. The async-with's __aexit__ returning is itself the
        # "drain returns promptly" assertion — the test would hang
        # otherwise.
        assert results == []

    # -- __aexit__ --------------------------------------------------------

    @pytest.mark.asyncio
    async def test___aexit___after_cancel_before_aiter(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aexit__` does not raise after a pre-iter cancel.

        Given:
            A handler that received :meth:`cancel` before any
            :meth:`__aiter__` call, then is iterated (which raises
            :class:`asyncio.CancelledError`) and exited
        When:
            The handler is exited via ``async with`` after the
            cancellation has been observed
        Then:
            ``__aexit__`` should return without raising.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            await handler.cancel()
            # Iteration surfaces the cancellation as
            # :class:`asyncio.CancelledError`, matching stdlib's
            # ``await task`` semantics — capture so the async-with's
            # __aexit__ runs naturally.
            with pytest.raises(asyncio.CancelledError):
                [r async for r in handler]

    @pytest.mark.asyncio
    async def test___aexit___swallows_worker_exception(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aexit__` does not re-raise a worker exception.

        Given:
            A handler whose worker raised an exception during the
            routine
        When:
            The handler is exited via ``async with`` after iteration
            already drained the exception out
        Then:
            It should not re-raise the worker exception (silently
            swallowed).
        """
        # Arrange
        task = _make_task(_gen_raises_mid_stream)
        stream = _stream(
            _request_for(task),
            _next_request(),
            _next_request(),
        )

        # Act — iterate, capture the exception so it does not abort
        # the async-with, then __aexit__ runs naturally.
        captured: list[BaseException] = []
        async with DispatchSession(stream, worker_loop) as handler:
            try:
                async for _ in handler:
                    pass
            except _CustomRoutineError as e:
                captured.append(e)

        # Assert
        assert len(captured) == 1
        assert isinstance(captured[0], _CustomRoutineError)

    # -- drain ------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_drain_called_twice_is_idempotent(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`drain` returns promptly across repeat calls.

        Given:
            A handler past :meth:`__aenter__` with no :meth:`__aiter__`
            ever invoked
        When:
            :meth:`drain` is awaited twice
        Then:
            Both calls should return promptly without raising.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        async with DispatchSession(stream, worker_loop) as handler:
            # Act
            await asyncio.wait_for(handler.drain(), timeout=2.0)
            await asyncio.wait_for(handler.drain(), timeout=2.0)

            # Assert — bounded wait_for ensures both returned promptly.

    @pytest.mark.asyncio
    async def test_drain_propagates_external_cancellation(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`drain` propagates an externally-injected
        cancellation.

        Given:
            A handler driving a long-running async-generator routine,
            awaited inside a task that is itself cancelled
        When:
            :meth:`drain` is awaited while the awaiting task has
            ``cancelling() > 0``
        Then:
            It should propagate :class:`asyncio.CancelledError`.
        """

        # Arrange
        task = _make_task(_slow_gen)
        stream = _stream(_request_for(task), _next_request())

        ready = asyncio.Event()
        observed: list[BaseException] = []

        async def driver():
            handler = DispatchSession(stream, worker_loop)
            await handler.__aenter__()
            try:
                # Kick the worker by starting iteration but bail out
                # without consuming, so drain awaits a still-running
                # routine.
                aiter(handler)
                ready.set()
                try:
                    await handler.drain()
                except BaseException as e:
                    observed.append(e)
                    raise
            finally:
                # Best-effort exit-stack unwind — the worker loop
                # owns the slow_gen so we let the fixture teardown
                # cancel the loop.
                try:
                    await handler.__aexit__(None, None, None)
                except BaseException:
                    pass

        driver_task = asyncio.create_task(driver())
        await ready.wait()
        # Yield once so the driver can hit its drain await.
        await asyncio.sleep(0)

        # Act
        driver_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await driver_task

        # Assert
        assert any(isinstance(e, asyncio.CancelledError) for e in observed), (
            "drain should have propagated CancelledError when the "
            "awaiting task was being cancelled"
        )

    @pytest.mark.asyncio
    async def test_drain_swallows_worker_cancellation(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`drain` swallows a worker-side cancellation.

        Given:
            A handler whose worker task was cancelled (its done
            callback set ``worker_done`` to
            :class:`asyncio.CancelledError`) but the awaiting task is
            not itself being cancelled
        When:
            :meth:`drain` is awaited
        Then:
            It should swallow the worker's cancellation and return
            cleanly.
        """
        # Arrange — drive a long-running generator, cancel the worker
        # task on its own loop, then call drain from the main loop.
        task = _make_task(_slow_gen)
        stream = _stream(_request_for(task), _next_request())

        async with DispatchSession(stream, worker_loop) as handler:
            iterator = aiter(handler)

            # Wait for the worker driver to be scheduled — _worker_done
            # is the public-equivalent observable populated by
            # _schedule_worker. We poll without referencing it in
            # the assert body itself to keep the test focused on
            # the drain behavior.
            for _ in range(200):
                if handler._worker_done is not None:
                    break
                await asyncio.sleep(0.01)

            # Cancel all worker-side tasks except the loop driver.
            def _cancel_workers():
                for t in asyncio.all_tasks(loop=worker_loop):
                    t.cancel()

            worker_loop.call_soon_threadsafe(_cancel_workers)

            # Give the worker time to settle; the response queue gets
            # closed by the done-callback so the iterator exits.
            try:
                async for _ in iterator:
                    pass
            except BaseException:
                pass

            # Act — drain on the awaiting task (not currently
            # cancelling) should swallow the worker's CancelledError.
            await asyncio.wait_for(handler.drain(), timeout=2.0)

            # Assert — control reached here without raising.

    @pytest.mark.asyncio
    async def test_drain_tolerates_closed_worker_loop(self, mock_worker_proxy_cache):
        """Test :meth:`drain` returns when the worker loop is closed.

        Given:
            A handler whose worker has been scheduled, then the worker
            loop is closed
        When:
            :meth:`drain` is awaited
        Then:
            It should return without raising.
        """
        # Arrange — manage the worker loop lifecycle in-test so we
        # can close it after __aenter__ but before __aiter__.
        loop = asyncio.new_event_loop()
        install_task_factory(loop)
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()

        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        handler = DispatchSession(stream, loop)
        await handler.__aenter__()

        # Close the worker loop. With no _worker_done assigned (no
        # __aiter__ yet), drain's short-circuit path is exercised.
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()

        # Act
        try:
            await asyncio.wait_for(handler.drain(), timeout=2.0)
            # Assert — drain returned without raising.
        finally:
            # Best-effort cleanup of the exit stack so the
            # passthrough-pool entry (if any) is released.
            try:
                await handler._stack.aclose()
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_drain_returns_when_worker_create_task_fails(
        self, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test :meth:`drain` returns when ``_start`` fails to
        create the worker task on the worker loop.

        Regression test for A8. Pre-fix, ``_start`` (scheduled
        via :meth:`asyncio.AbstractEventLoop.call_soon_threadsafe`
        from :meth:`_schedule_worker`) called
        :meth:`asyncio.AbstractEventLoop.create_task` without a
        try/except. If the worker loop closed between the
        scheduling and ``_start``'s execution — or any other
        late-loop-closure / task-factory failure — ``create_task``
        raised and the exception went to the loop's default
        handler. ``worker_done`` was never set, so :meth:`drain`
        awaited a future that would never resolve and hung
        indefinitely. Post-fix, ``_start`` catches creation
        failures and settles ``worker_done`` with the exception
        so :meth:`drain` unblocks.

        Given:
            A handler whose worker loop's
            :meth:`asyncio.AbstractEventLoop.create_task` raises
            (simulating a late-loop-closure or task-factory
            failure).
        When:
            :meth:`_schedule_worker` runs ``_start`` on the
            worker loop and :meth:`drain` is awaited.
        Then:
            :meth:`drain` should return within a short timeout —
            pre-fix it hung indefinitely on an unresolved
            ``worker_done`` future.
        """
        # Arrange — manage the worker loop in-test so we can
        # patch its ``create_task``.
        loop = asyncio.new_event_loop()
        install_task_factory(loop)
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()

        try:
            task = _make_task(_coro_returning_default)
            stream = _stream(_request_for(task))
            handler = DispatchSession(stream, loop)
            await handler.__aenter__()

            # Patch the worker loop's create_task to fail. The
            # patch is observed inside ``_start`` when it runs
            # on the worker loop.
            mocker.patch.object(
                loop,
                "create_task",
                side_effect=RuntimeError(
                    "simulated late-loop-closure / task-factory failure"
                ),
            )

            # Trigger scheduling. This calls
            # ``call_soon_threadsafe(_start)``; _start later runs
            # on the worker loop where the patched create_task
            # raises.
            handler._schedule_worker()

            # Wait briefly for the worker loop to execute _start.
            await asyncio.sleep(0.1)

            # Act + Assert — drain must return quickly. Pre-fix
            # it hangs because worker_done was never settled.
            await asyncio.wait_for(handler.drain(), timeout=2.0)
        finally:
            try:
                await handler._stack.aclose()
            except Exception:
                pass
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)
            if not loop.is_closed():
                loop.close()

    # -- cancel -----------------------------------------------------------

    @pytest.mark.asyncio
    async def test_cancel_before_aenter_is_safe(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`cancel` invoked before :meth:`__aenter__` is safe.

        Given:
            A fresh :class:`DispatchSession` (pre-``__aenter__``)
        When:
            :meth:`cancel` is awaited
        Then:
            It should not raise; subsequent :meth:`__aenter__`
            succeeds and :meth:`__aiter__` surfaces the cancellation
            as :class:`asyncio.CancelledError`, matching stdlib's
            ``await task`` semantics where a pre-cancelled awaitable
            raises ``CancelledError`` on its first await.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))
        handler = DispatchSession(stream, worker_loop)

        # Act — cancel before enter
        await handler.cancel()

        # Assert — enter succeeds; iteration raises CancelledError;
        # __aexit__ runs cleanly after the cancellation is observed.
        async with handler:
            with pytest.raises(asyncio.CancelledError):
                [r async for r in handler]

    @pytest.mark.asyncio
    async def test_cancel_after_iteration_is_safe(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`cancel` invoked after iteration is a no-op.

        Given:
            A handler whose iterator already completed naturally
        When:
            :meth:`cancel` is awaited
        Then:
            It should not raise and not re-schedule any work.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        async with DispatchSession(stream, worker_loop) as handler:
            results = [r async for r in handler]
            assert len(results) == 1

            # Act — cancel after natural completion
            await handler.cancel()

            # Assert — control reached here without raising.

    @pytest.mark.asyncio
    async def test_cancel_during_iteration_from_different_task(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`cancel` from another task surfaces
        :class:`asyncio.CancelledError` on the active iterator.

        Given:
            A handler iterating an async-generator routine, with
            :meth:`cancel` invoked from another task
        When:
            The iterator is awaited
        Then:
            The iterator should raise :class:`asyncio.CancelledError`,
            matching stdlib's ``await task`` semantics where
            ``task.cancel()`` from any source produces the same
            observable, and no values should have been yielded.
        """
        # Arrange
        task = _make_task(_slow_gen)
        stream = _stream(_request_for(task), _next_request())

        captured: dict = {}
        results: list = []

        async def driver():
            async with DispatchSession(stream, worker_loop) as handler:
                captured["handler"] = handler
                async for r in handler:
                    results.append(r)

        # Act
        driver_task = asyncio.create_task(driver())

        # Wait until the iterator is suspended on response_queue.get.
        for _ in range(500):
            if "handler" in captured and captured["handler"]._worker_done is not None:
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("iterator never reached the suspended state")

        await captured["handler"].cancel()

        # Driver task surfaces the cancellation naturally — the
        # iterator raises CancelledError, the async-with's
        # __aexit__ drains the worker (whose worker_task was also
        # cancelled by :meth:`cancel`), and the exception
        # propagates out of ``driver``.
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(driver_task, timeout=5.0)

        # Assert — the iterator yielded no values before raising.
        assert results == []

    @pytest.mark.asyncio
    async def test_cancel_during_suspended_iteration_unblocks_iterator(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`cancel` unblocks an iterator suspended on a
        response and surfaces :class:`asyncio.CancelledError`.

        Given:
            A handler whose iterator is suspended awaiting a response
        When:
            :meth:`cancel` is awaited from a different task
        Then:
            The iterator's next await should raise
            :class:`asyncio.CancelledError`, matching stdlib's
            ``await task`` semantics where ``task.cancel()`` from
            any source produces the same observable.
        """
        # Arrange
        task = _make_task(_slow_gen)
        stream = _stream(_request_for(task), _next_request())

        captured: dict = {}

        async def driver():
            async with DispatchSession(stream, worker_loop) as handler:
                captured["handler"] = handler
                try:
                    await anext(aiter(handler))
                except asyncio.CancelledError:
                    captured["cancelled"] = True
                    raise

        # Act
        driver_task = asyncio.create_task(driver())

        # Wait for the iterator to be suspended.
        for _ in range(500):
            if "handler" in captured and captured["handler"]._worker_done is not None:
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("iterator never reached the suspended state")

        # Cancel from this task while driver_task is suspended.
        await captured["handler"].cancel()

        # Wait for the driver to observe CancelledError. Once
        # observed, the driver re-raises so the surrounding
        # async-with's __aexit__ runs — drain blocks on the
        # slow_gen until the worker task's own cancellation
        # propagates, so cancel the driver task to ensure
        # bounded cleanup.
        for _ in range(500):
            if "cancelled" in captured:
                break
            await asyncio.sleep(0.01)

        driver_task.cancel()
        try:
            await asyncio.wait_for(driver_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

        # Assert — anext observed CancelledError.
        assert captured.get("cancelled") is True

    @pytest.mark.asyncio
    async def test_cancel_called_twice_is_idempotent(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`cancel` is idempotent across repeat calls.

        Given:
            A handler past :meth:`__aenter__`
        When:
            :meth:`cancel` is awaited twice
        Then:
            The second call should be a no-op and not raise.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        async with DispatchSession(stream, worker_loop) as handler:
            # Act
            await handler.cancel()
            await handler.cancel()

            # Assert — control reached here without raising.

