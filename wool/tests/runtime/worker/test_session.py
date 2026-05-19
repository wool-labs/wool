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
from wool.protocol import WorkerStub
from wool.protocol import add_WorkerServicer_to_server
from wool.runtime.context import Context
from wool.runtime.context import install_task_factory
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.worker.interceptor import VersionInterceptor
from wool.runtime.worker.service import WorkerService
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


async def _slow_coro():
    """Coroutine that sleeps long enough for tests to observe it
    suspended on the worker loop. ``asyncio.sleep`` is picklable
    (cloudpickle handles bound async builtins); capturing a
    :class:`threading.Event` in a closure would not be.
    """
    await asyncio.sleep(60)
    return "never"


async def _gen_yielding_unpicklable():
    """Async generator that yields a non-cloudpickle-serializable
    object so the dispatch handler's :meth:`_Response.to_protobuf`
    raises when encoding the response on the main loop —
    forcing the handler into its terminal-exception clause.
    """
    yield threading.Lock()


class _RoutineFailure(Exception):
    """Picklable, distinct routine-side exception used by tests that
    need to distinguish the routine's primary signal from other
    failures in the dispatch flow.
    """


async def _coro_raising_routine_failure():
    """Coroutine routine that raises :class:`_RoutineFailure`."""
    raise _RoutineFailure("routine signal")


def _sync_callable():
    return "not_async"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task(callable_obj):
    """Build a :class:`Task` whose proxy is a picklable mock."""
    mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
    return Task(
        id=uuid4(),
        callable=callable_obj,
        args=(),
        kwargs={},
        proxy=mock_proxy,
    )


def _request_for(task: Task) -> protocol.Request:
    """Build a first-frame Request carrying *task*'s protobuf."""
    return protocol.Request(task=task.to_protobuf())


def _next_request() -> protocol.Request:
    """Build a follow-up ``next`` request frame."""
    return protocol.Request(next=protocol.Void())


async def _stream(*requests):
    """Build an async iterator over *requests*."""
    for r in requests:
        yield r


# ---------------------------------------------------------------------------
# gRPC fixtures — required by tests that exercise the dispatch flow end-to-end
# through ``grpc_aio_stub``. The pytest-grpc-aio plugin expects each test
# module that uses ``grpc_aio_stub`` to provide these overrides.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def grpc_interceptors():
    return [VersionInterceptor()]


@pytest.fixture(scope="function")
def grpc_add_to_server():
    return add_WorkerServicer_to_server


@pytest.fixture(scope="function")
def grpc_servicer():
    service = WorkerService()
    yield service
    for entry in service._loop_pool._cache.values():
        service._destroy_worker_loop(entry.obj)


@pytest.fixture(scope="function")
def grpc_stub_cls():
    return WorkerStub


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
        # All dispatch serializes through cloudpickle; this one
        # serializer covers the payload, the context snapshots, and
        # any ``Rejected`` dumped exception (including pre-parse
        # failures such as StopAsyncIteration or a malformed frame).
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
            coroutine's return value and whose context carries the
            post-step :class:`protocol.Context` snapshot.
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
        # The unified driver MUST emit a post-step context snapshot on
        # every successful step (issue #187 motivation: "snapshot-encode
        # duplicated across both paths"). The presence of an ``id`` on
        # the response's :class:`protocol.Context` proves a snapshot
        # was actually populated (a missing snapshot would surface as
        # the empty default).
        assert results[0].context is not None
        assert results[0].context.id

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

            # Cancel every task running on the worker loop. The
            # cancellation cascades into the routine task and the
            # session's ``_on_done`` callback, which closes the
            # response queue and settles ``worker_done``.
            def _cancel_workers():
                for t in asyncio.all_tasks(loop=worker_loop):
                    t.cancel()

            worker_loop.call_soon_threadsafe(_cancel_workers)

            # Give the worker time to settle; the response queue gets
            # closed by the done-callback so the iterator exits.
            # Narrow the catch to ``CancelledError`` — the only
            # exception this scenario can legitimately produce — so
            # any unrelated regression that surfaces a different
            # exception class is not silently absorbed.
            try:
                async for _ in iterator:
                    pass
            except asyncio.CancelledError:
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
            # Best-effort cleanup of the exit stack.
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

    @pytest.mark.asyncio
    async def test___aiter___streaming_rewraps_mid_stream_context_decode_failure(
        self, worker_loop, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test mid-stream context decode failures are re-wrapped
        with a labeled message so the dispatch handler can
        distinguish them from initial-frame decode failures.

        Given:
            A streaming session whose first ``next`` request decodes
            cleanly but whose second ``next`` request's context
            decode raises a :class:`BaseExceptionGroup` (operator
            promoted :class:`ContextDecodeWarning` to an exception)
        When:
            The caller iterates the session past the first response
        Then:
            The error should surface as a :class:`BaseExceptionGroup`
            (or :class:`ExceptionGroup` after constructor downgrade)
            labeled "mid-stream request context decode failed" so
            the dispatch handler can distinguish it from
            initial-frame decode failures that share the same peer
            type.
        """
        from wool.runtime.context import base as ctx_base

        # Arrange — streaming routine that yields per ``next``.
        task = _make_task(_gen_three)
        stream = _stream(
            _request_for(task),
            _next_request(),
            _next_request(),
        )

        # Counter-based patch: let the initial __aenter__ decode
        # and the first per-step decode succeed; the third call
        # (second mid-stream decode) raises a
        # ``BaseExceptionGroup`` of Exception-only peers.
        original = ctx_base.Context.from_protobuf
        calls = {"n": 0}
        peer = ValueError("decode peer")

        def fake_from_protobuf(cls, proto_ctx, *, serializer):
            calls["n"] += 1
            if calls["n"] >= 3:
                raise BaseExceptionGroup("simulated decode failure", [peer])
            return original.__func__(cls, proto_ctx, serializer=serializer)

        mocker.patch.object(
            ctx_base.Context,
            "from_protobuf",
            classmethod(fake_from_protobuf),
        )

        # Act — iterate; the second step raises the re-wrapped
        # group, which surfaces out of the iterator.
        captured: list[BaseException] = []
        async with DispatchSession(stream, worker_loop) as handler:
            try:
                async for _ in handler:
                    pass
            except BaseException as e:
                captured.append(e)

        # Assert
        assert len(captured) == 1
        eg = captured[0]
        assert isinstance(eg, BaseExceptionGroup)
        assert "mid-stream request context decode failed" in eg.message

    @pytest.mark.asyncio
    async def test___aenter___propagates_keyboard_interrupt_during_aclose(
        self, worker_loop, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test the safe-aclose helper does not swallow
        :class:`KeyboardInterrupt` during cleanup.

        Given:
            A handler whose ``__aenter__`` is failing (empty request
            stream → :class:`Rejected`) and whose cleanup
            ``_stack.aclose()`` raises :class:`KeyboardInterrupt`
            (simulating a Ctrl-C landing mid-cleanup)
        When:
            The handler is entered
        Then:
            :class:`KeyboardInterrupt` should propagate raw out of
            the helper rather than being swallowed by the
            ``except Exception`` arm.
        """

        # Arrange — an empty stream forces __aenter__ to raise
        # StopAsyncIteration and route through the safe-aclose
        # error path.
        async def empty_stream():
            if False:
                yield  # pragma: no cover

        handler = DispatchSession(empty_stream(), worker_loop)

        # Patch the exit stack's aclose so the cleanup raises
        # KeyboardInterrupt — the safe-aclose helper must re-raise
        # this rather than swallow it under ``except Exception``.
        mocker.patch.object(
            handler._stack,
            "aclose",
            side_effect=KeyboardInterrupt("simulated Ctrl-C"),
        )

        # Act & assert
        with pytest.raises(KeyboardInterrupt):
            await handler.__aenter__()

    @pytest.mark.asyncio
    async def test___aiter___streaming_breaks_on_cancel_between_requests(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test the streaming pump observes a mid-pump cancel and
        breaks before pushing the next request to the worker.

        Given:
            A streaming session whose request stream is gated so the
            test controls when the next request frame arrives at the
            pump
        When:
            The caller cancels the session, then releases the gate so
            the next request arrives at the pump's
            ``async for protobuf_request in self._request_iterator``
        Then:
            The pump should observe ``self._cancelled is True`` at
            the top of the loop and break — the request is not
            pushed to the worker.
        """
        # Arrange — async generator that yields one value per
        # ``next``, so the pump reaches the top of the loop again
        # (awaiting the next caller request) after yielding the
        # first response.
        task = _make_task(_gen_three)

        first_yielded = asyncio.Event()
        gate = asyncio.Event()
        observed: dict = {}

        async def gated_stream():
            yield _request_for(task)
            yield _next_request()
            # Pump is now awaiting the next request — hold here
            # until the test cancels and releases the gate.
            await first_yielded.wait()
            await gate.wait()
            observed["resumed"] = True
            yield _next_request()

        async def driver():
            async with DispatchSession(gated_stream(), worker_loop) as handler:
                observed["handler"] = handler
                async for response in handler:
                    observed.setdefault("responses", []).append(response)
                    first_yielded.set()

        # Act
        driver_task = asyncio.create_task(driver())

        # Wait for the first response so we know the pump has
        # looped back to the top awaiting the next request.
        for _ in range(500):
            if first_yielded.is_set():
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("first response never arrived")

        # Cancel while the pump is suspended on the next request,
        # then release the gate so the pump's next-request branch
        # observes ``self._cancelled is True`` and breaks.
        await observed["handler"].cancel()
        gate.set()

        # Give the gated stream a chance to resume and yield the
        # next request, so the pump's ``if self._cancelled: break``
        # branch fires.
        for _ in range(500):
            if observed.get("resumed"):
                break
            await asyncio.sleep(0.01)

        # Cleanup — drain the driver so the test does not leak.
        driver_task.cancel()
        try:
            await driver_task
        except asyncio.CancelledError:
            pass

        # Assert
        assert observed.get("resumed") is True
        # The pump observed ``_cancelled`` at the top of its loop
        # and broke without forwarding the third request to the
        # worker. The generator yields ``1, 2, 3``; we only ever
        # consumed two responses (one before cancel, none after).
        # A regression that bypassed the ``if self._cancelled:
        # break`` check would forward the third request, the
        # worker would advance the generator to ``3``, and the
        # response would appear in ``observed["responses"]``.
        assert len(observed.get("responses", [])) == 1
        assert observed["responses"][0].result == 1

    @pytest.mark.asyncio
    async def test_cancel_tolerates_closed_worker_loop(
        self, worker_loop, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :meth:`cancel` swallows ``RuntimeError`` from a torn-down
        worker loop.

        Given:
            A handler whose worker loop has been closed after the
            worker task was scheduled (e.g. graceful shutdown
            landing between two main-loop pumps)
        When:
            :meth:`cancel` is awaited
        Then:
            It should return cleanly without raising — the
            ``call_soon_threadsafe`` ``RuntimeError("Event loop is
            closed")`` is swallowed because the dispatch is no
            longer serviceable.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        async with DispatchSession(stream, worker_loop) as handler:
            # Bind a mock worker task without scheduling a real one
            # so we exercise the ``call_soon_threadsafe`` branch of
            # cancel() directly.
            handler._worker_task = mocker.MagicMock()
            # Patch the worker loop's ``call_soon_threadsafe`` to
            # simulate a torn-down loop ("Event loop is closed").
            mocker.patch.object(
                worker_loop,
                "call_soon_threadsafe",
                side_effect=RuntimeError("Event loop is closed"),
            )

            # Act
            await handler.cancel()

            # Assert — cancel returned without raising; the
            # RuntimeError was swallowed.
            assert handler._cancelled is True

    # -- Migrated from test_service.py::TestWorkerService (F17) -----------

    @pytest.mark.asyncio
    async def test___aenter___preserves_parse_error_when_aclose_raises(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aenter__` preserves the original parse error as
        :class:`Rejected` even when ``_stack.aclose()`` raises during
        cleanup.

        Regression test: pre-fix, the parse-phase ``except Exception as
        e: await self._stack.aclose(); raise Rejected(e) from None``
        ran ``aclose`` un-guarded. If the stack's exit chain raised
        (e.g. a registered resource's ``__aexit__`` failing), the new
        exception replaced ``e`` and ``Rejected(e)`` was never
        constructed — the dispatch handler's Nack-with-exception
        channel observed the cleanup failure instead of the typed
        parse error. The fix swallows aclose failures so the parse
        error always reaches the caller.

        Given:
            A request that fails parse-phase validation (a non-async
            callable) AND a stack whose ``aclose`` raises during the
            resulting cleanup
        When:
            :meth:`__aenter__` is invoked
        Then:
            It should raise :class:`Rejected` whose ``original``
            attribute carries the parse-phase :class:`ValueError`, not
            the simulated aclose failure.
        """
        # Arrange — a non-async callable triggers a ValueError in the
        # __aenter__ validation step. ``_stack.aclose`` is patched
        # below to raise, so the cleanup-failure path is exercised
        # regardless of what the stack contains.
        task = _make_task(_sync_callable)
        stream = _stream(_request_for(task))
        handler = DispatchSession(stream, worker_loop)

        # Patch ``_stack.aclose`` directly: the underlying
        # :class:`AsyncExitStack` is not exposed through any public
        # hook, so the cleanup-failure path can only be exercised by
        # replacing this attribute. The substitution mirrors the
        # operational failure mode (a registered resource's
        # ``__aexit__`` raising).
        async def raising_aclose():
            raise RuntimeError("simulated aclose failure during cleanup")

        handler._stack.aclose = raising_aclose

        # Act + Assert
        with pytest.raises(Rejected) as exc_info:
            await handler.__aenter__()

        assert isinstance(exc_info.value.original, ValueError), (
            f"Rejected.original must carry the parse-phase ValueError, "
            f"not the aclose failure — observed "
            f"{type(exc_info.value.original).__name__}"
        )
        assert "Expected coroutine function" in str(exc_info.value.original)

    @pytest.mark.asyncio
    async def test___aiter___defers_worker_scheduling(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aiter__` defers worker scheduling until the
        first iteration rather than scheduling eagerly inside
        :meth:`__aenter__`.

        Regression test for the race between dispatch's backpressure
        hook and the worker for :meth:`Context._guard` ownership.
        Pre-fix :meth:`__aenter__` scheduled the worker eagerly; with
        a backpressure hook that yielded the main loop while holding
        ``attached(handler.context)``, the worker thread would race
        to acquire the same Context's guard and spuriously raise
        ``RuntimeError("wool.Context is already running...")``. The
        invariant tested here — :meth:`__aenter__` is parse-only —
        guarantees no contention regardless of how long any
        post-parse main-loop work holds the Context.

        Given:
            A :class:`DispatchSession` constructed around a parsed
            dispatch request and a real worker loop
        When:
            :meth:`__aenter__` completes (parse phase done) and
            :meth:`__aiter__` is first called (lazy-schedule fires)
        Then:
            It should leave the worker driver unscheduled after
            :meth:`__aenter__`, schedule it on the first
            :meth:`__aiter__` call, and deliver the routine's result
            through the iterator.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        async with DispatchSession(stream, worker_loop) as handler:
            # ``_worker_done`` is the private marker created by
            # :meth:`_schedule_worker`. Probed directly because no
            # public observable can witness "worker not yet
            # scheduled" without producing or consuming a Response —
            # which itself would force scheduling. The marker is the
            # narrowest stand-in.
            assert handler._worker_done is None, (
                "DispatchSession.__aenter__ must defer worker scheduling"
            )

            # Act
            iterator = aiter(handler)

            # Assert
            assert handler._worker_done is not None, (
                "DispatchSession.__aiter__ must schedule the worker on first call"
            )
            response = await anext(iterator)
            assert response.result == "coroutine_value"

    @pytest.mark.asyncio
    async def test___aiter___swallows_request_queue_runtime_error_mid_stream(
        self, worker_loop, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :meth:`_iterate` swallows :class:`RuntimeError` from
        ``_RequestQueue.put`` so a closed worker loop mid-stream
        terminates the iterator cleanly instead of surfacing the
        loop-teardown error as a routine failure.

        Regression test: pre-fix, the streaming branch of
        :meth:`_iterate` called ``request_queue.put(protobuf_request)``
        un-guarded. :meth:`_RequestQueue.put` schedules onto the
        worker loop via ``call_soon_threadsafe``; if the worker loop
        has been torn down (graceful shutdown teardown landing
        between two main-loop pumps), put raises ``RuntimeError(
        "Event loop is closed")``. The unguarded propagation
        surfaced the runtime error out of :meth:`_iterate` — but the
        routine never failed; the worker loop did. The fix mirrors
        :meth:`drain`'s pattern: catch ``RuntimeError`` at the put
        site and break cleanly.

        Given:
            A streaming session with :meth:`_RequestQueue.put` patched
            to succeed on the first call and raise ``RuntimeError``
            on the second
        When:
            The iterator is driven for the second response — the
            patched put triggers the runtime error mid-stream
        Then:
            The iterator should terminate cleanly without surfacing
            a synthetic :class:`RuntimeError` as a routine failure.
        """
        from wool.runtime.worker.session import _RequestQueue

        # Arrange — patching :class:`_RequestQueue.put` is justified
        # here: the "real boundary" alternative (closing the worker
        # loop mid-stream) has a semantic obstacle. Closing the
        # worker loop while the worker task is suspended on
        # ``request_queue.get`` leaves the worker-completion future
        # ``_worker_done`` unresolved (the ``_on_done`` callback
        # never fires), hanging the subsequent :meth:`drain` call
        # from ``__aexit__``. The patch injects the exact failure
        # mode the fix targets — a ``call_soon_threadsafe`` raise on
        # a torn-down loop — without leaking the worker task.
        call_count = 0
        original_put = _RequestQueue.put

        def patched_put(self, request):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise RuntimeError("simulated closed worker loop during put")
            return original_put(self, request)

        mocker.patch.object(_RequestQueue, "put", patched_put)

        task = _make_task(_gen_default_two)
        stream = _stream(
            _request_for(task),
            _next_request(),
            _next_request(),
        )

        # Act
        responses: list = []
        async with DispatchSession(stream, worker_loop) as handler:
            try:
                async for response in handler:
                    responses.append(response)
            except RuntimeError as e:
                if "simulated closed worker loop" in str(e):
                    pytest.fail(
                        "_iterate must catch RuntimeError from "
                        "request_queue.put and terminate the stream "
                        "cleanly; the unguarded raise surfaced "
                        "the synthetic loop-teardown error as a "
                        "routine failure"
                    )
                raise

        # Assert — iterator terminated cleanly. The first put
        # succeeded so one response was yielded; the second put
        # raised and was swallowed.
        assert len(responses) == 1
        assert responses[0].result == "a"

    @pytest.mark.asyncio
    async def test___aexit___drains_on_terminal_exception(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`DispatchSession.drain` is called both from the
        dispatch handler's terminal-exception clause and from
        :meth:`__aexit__`'s exit-stack unwind when the response-
        encode fails.

        Regression test for the cross-loop race when the dispatch's
        terminal-exception clause is reached while the worker is
        still alive. Main-loop handler-level failures (e.g.
        ``response.to_protobuf`` raising on dump) reach the except
        clause with the worker mid-``_step`` mutating ``work_ctx``.
        Without an explicit drain before the snapshot,
        ``handler.context.to_protobuf(...)`` reads ``_data`` while
        the worker writes it. The fix calls
        :meth:`DispatchSession.drain` from dispatch's terminal-
        exception clause; :meth:`__aexit__` also calls drain (via
        its exit stack, idempotent) so the spy observes two calls
        with the fix versus one without.

        Given:
            A streaming dispatch whose routine yields a
            non-cloudpickle-serializable value — forcing
            ``_Response.to_protobuf`` to raise on the wire-frame
            dump and routing dispatch into its terminal-exception
            clause while the worker is still alive
        When:
            Dispatch reaches its terminal-exception clause
        Then:
            It should call :meth:`DispatchSession.drain` at least
            twice — once from the except clause, once from
            :meth:`__aexit__`'s exit-stack unwind.
        """
        # Arrange — un-picklable yield value triggers the dispatch
        # handler's ``response.to_protobuf`` to raise on the main
        # loop while the worker is still alive on its loop.
        task = _make_task(_gen_yielding_unpicklable)
        first_request = protocol.Request(task=task.to_protobuf())
        next_request = protocol.Request(
            next=protocol.Void(),
            context=protocol.Context(id=uuid4().hex),
        )

        drain_spy = mocker.spy(DispatchSession, "drain")

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(first_request)
            ack = await anext(aiter(stream))
            assert ack.HasField("ack")

            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert — dispatch yields exactly one terminal-exception
        # Response after the Ack.
        assert len(remaining) == 1
        terminal = remaining[0]
        assert terminal.HasField("exception")

        # Drain must have been called at least twice: once from the
        # dispatch handler's terminal-exception clause (the fix) and
        # once from :meth:`__aexit__`'s exit-stack unwind. Without
        # the fix, drain is called only from ``__aexit__`` and the
        # context snapshot races the still-alive worker.
        assert drain_spy.call_count >= 2, (
            f"Expected dispatch's terminal-exception clause to call "
            f"DispatchSession.drain before snapshotting "
            f"DispatchSession.context (plus __aexit__'s call); "
            f"observed {drain_spy.call_count} call(s)."
        )

    @pytest.mark.asyncio
    async def test___aexit___does_not_mask_routine_exception_when_cancel_raises(
        self,
        grpc_aio_stub,
        mock_worker_proxy_cache,
        mocker: MockerFixture,
    ):
        """Test the dispatch handler's inner ``finally``
        ``session.cancel()`` swallows exceptions so the routine's
        primary signal reaches the terminal-exception clause intact.

        Regression test: pre-fix, the inner ``finally: await
        handler.cancel()`` ran without a guard. If :meth:`cancel`
        raised, Python's ``finally`` semantics replaced the in-
        flight ``async for`` exception with the cancel-time
        exception. The terminal-exception clause then shipped the
        cancel failure with the routine's primary signal demoted to
        ``__context__``. The fix wraps the cancel call in a
        ``try/except BaseException: pass`` so cancel-time failures
        cannot mask the routine signal.

        Given:
            A dispatch whose routine fails with a distinguished
            :class:`_RoutineFailure`, and :class:`DispatchSession.cancel`
            patched to raise a different exception type
        When:
            The dispatch handler reaches its inner ``finally`` and
            the terminal-exception clause builds the failure
            response
        Then:
            It should ship the routine's exception, not the cancel-
            time exception.
        """
        import cloudpickle

        # Arrange — patching :class:`DispatchSession.cancel` is the
        # SUT-of-this-file's own public method; the substitution
        # injects the inner-finally failure mode the regression
        # targets. No cleaner real-boundary trigger exists.
        class _CancelFailure(Exception):
            pass

        async def raising_cancel(self):
            raise _CancelFailure("cancel failure must not mask routine")

        mocker.patch.object(DispatchSession, "cancel", raising_cancel)

        task = _make_task(_coro_raising_routine_failure)
        first_request = protocol.Request(task=task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(first_request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, terminal = responses
        assert ack.HasField("ack")
        assert terminal.HasField("exception"), (
            "dispatch must ship a terminal-exception Response"
        )
        shipped = cloudpickle.loads(terminal.exception.dump)
        assert isinstance(shipped, _RoutineFailure), (
            f"terminal Response must carry the routine's primary "
            f"exception, not the cancel-time failure — pre-fix, "
            f"observed {type(shipped).__name__}"
        )

    @pytest.mark.asyncio
    async def test_drain_with_closed_worker_loop_pre_schedule(
        self, mock_worker_proxy_cache
    ):
        """Test :meth:`drain` returns promptly when the worker loop is
        closed before :meth:`_schedule_worker` can install the
        worker task's done-callback.

        Regression test for the indefinite hang in :meth:`drain` when
        :meth:`_schedule_worker` partially completes. Pre-fix, the
        ``self._worker_done = ...`` assignment ran before
        ``call_soon_threadsafe(_start)``; if the worker loop was
        closed in that window, ``call_soon_threadsafe`` raised
        ``RuntimeError("Event loop is closed")``, no ``_on_done``
        callback was ever registered, and :meth:`drain`'s
        ``await asyncio.wrap_future(self._worker_done)`` blocked
        forever. The fix assigns ``self._worker_done`` only after
        the schedule succeeds, so :meth:`drain`'s
        ``if self._worker_done is not None`` short-circuits to a
        no-op when scheduling fails.

        Given:
            A :class:`DispatchSession` that has completed parse-
            phase :meth:`__aenter__` against a real worker loop
        When:
            The worker loop is closed before :meth:`__aiter__` calls
            :meth:`_schedule_worker`, then :meth:`drain` is invoked
        Then:
            It should return within a bounded timeout rather than
            blocking on a ``worker_done`` future that will never be
            resolved.
        """
        # Arrange — manage the worker loop in-test so we can close
        # it after __aenter__ but before __aiter__. The
        # ``worker_loop`` fixture is unsafe here: its teardown would
        # re-stop the already-closed loop.
        loop = asyncio.new_event_loop()
        install_task_factory(loop)
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()

        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        handler = DispatchSession(stream, loop)
        await handler.__aenter__()

        # Close the worker loop so the next ``call_soon_threadsafe``
        # raises ``RuntimeError("Event loop is closed")``
        # synchronously inside :meth:`_schedule_worker`, exercising
        # the partial-schedule path :meth:`drain` must tolerate.
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()

        # Act
        try:
            with pytest.raises(RuntimeError):
                aiter(handler)

            # Assert — drain must return quickly; pre-fix it awaits
            # ``self._worker_done`` indefinitely because no
            # ``_on_done`` callback was ever installed on the closed
            # loop's worker task.
            await asyncio.wait_for(handler.drain(), timeout=2.0)
        finally:
            try:
                await handler._stack.aclose()
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_cancel_before_aiter(self, worker_loop, mock_worker_proxy_cache):
        """Test :meth:`cancel` invoked before :meth:`__aiter__` short-
        circuits worker scheduling and surfaces
        :class:`asyncio.CancelledError` on the iterator's first
        :meth:`anext`.

        Regression test for the race window where :meth:`cancel`
        was a no-op when ``self._iterator`` was ``None``, but
        :meth:`__aiter__` would still schedule the worker on its
        first call afterwards. With the flag-based :meth:`cancel`,
        the ``_cancelled`` flag pre-empts :meth:`_schedule_worker`
        and the iterator's first :meth:`anext` raises
        :class:`asyncio.CancelledError` — mirroring stdlib's
        ``await task`` semantics where a pre-cancelled awaitable
        raises ``CancelledError`` on its first await — without ever
        starting the worker.

        Given:
            A :class:`DispatchSession` past parse phase but with no
            iteration started
        When:
            :meth:`cancel` is called and then :meth:`__aiter__` is
            invoked
        Then:
            It should not schedule the worker (``_request_queue`` /
            ``_worker_done`` remain ``None``) and the iterator's
            first ``anext`` should raise
            :class:`asyncio.CancelledError`.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        async with DispatchSession(stream, worker_loop) as handler:
            # Act
            await handler.cancel()
            iterator = aiter(handler)

            # Assert — worker was not scheduled. The two private
            # markers stand in for "no scheduling occurred"; no
            # public observable can witness the absence of
            # scheduling without forcing it.
            assert handler._request_queue is None, (
                "cancel() before __aiter__ must short-circuit "
                "_schedule_worker — _request_queue should remain None"
            )
            assert handler._worker_done is None, (
                "cancel() before __aiter__ must short-circuit "
                "_schedule_worker — _worker_done should remain None"
            )

            # The iterator surfaces the cancellation immediately.
            with pytest.raises(asyncio.CancelledError):
                await anext(iterator)

    @pytest.mark.asyncio
    async def test_cancel_from_different_task(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`cancel` is safe to call from a task other than
        the one currently driving the iterator.

        Regression test for the pre-fix :meth:`cancel` invoking
        ``aclose()`` on the iterator, which raised
        ``RuntimeError("asynchronous generator is already
        running")`` when the iterator was being driven by another
        task. :meth:`WorkerService._cancel` (graceful shutdown)
        calls :meth:`cancel` on every handler from a task other
        than each handler's driver, so the error fired on every
        in-flight dispatch during shutdown — only invisible because
        ``gather(return_exceptions=True)`` swallowed the
        :class:`RuntimeError`. The flag-based :meth:`cancel` cannot
        raise this error because it does not invoke ``aclose``.

        Given:
            A :class:`DispatchSession` whose iterator is mid-flight
            (driven by another task)
        When:
            :meth:`cancel` is called from a separate task
        Then:
            It should not raise — and the iterator's driver task
            should observe end-of-stream cleanly.
        """
        # Arrange — module-level coroutine that sleeps long enough
        # for the test to observe the iterator suspended at
        # ``response_queue.get`` while another task calls
        # :meth:`cancel`.
        task = _make_task(_slow_coro)
        stream = _stream(_request_for(task))

        captured: dict = {}

        async def driver():
            async with DispatchSession(stream, worker_loop) as handler:
                captured["handler"] = handler
                async for _ in handler:
                    pass

        driver_task = asyncio.create_task(driver())

        # Wait until the worker has been scheduled — proves
        # ``__aiter__`` ran and ``_iterate`` is suspended at
        # ``response_queue.get`` waiting on the slow coroutine.
        # Polling ``_worker_done`` matches the existing pattern in
        # this test class for "iterator suspended" detection
        # (see :func:`test_cancel_during_iteration_from_different_task`).
        while "handler" not in captured or captured["handler"]._worker_done is None:
            await asyncio.sleep(0)

        # Act — cancel from this task while driver_task drives the
        # iterator. Pre-fix, this raised ``RuntimeError(
        # "asynchronous generator is already running")``; the flag-
        # based fix returns cleanly.
        await captured["handler"].cancel()

        # Cleanup — driver_task is now blocked in __aexit__'s drain
        # waiting on the slow coroutine. Cancel it so the test does
        # not stall — drain is registered on the exit stack and
        # unwinds cleanly when the wait is interrupted.
        driver_task.cancel()
        try:
            await asyncio.wait_for(driver_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
