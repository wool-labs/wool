"""Unit tests for :mod:`wool.runtime.worker.session`.

These tests exercise :class:`Rejected` and :class:`DispatchSession` through
their public surface (constructor, ``async with``, ``async for``,
:meth:`drain`, :meth:`cancel`, public attributes ``task``/``decoded``/
``serializer``). Worker-loop interactions use the
``new_event_loop + threading.Thread + install_task_factory`` pattern
so the handler can drive a real :func:`scoped` routine across loops.
"""

from __future__ import annotations

import asyncio
import gc
import threading
import weakref
from uuid import uuid4

import pytest
from pytest_mock import MockerFixture

import wool
from tests.runtime.worker.conftest import PicklableMock
from wool import protocol
from wool.protocol import WorkerStub
from wool.protocol import add_WorkerServicer_to_server
from wool.runtime.context.factory import install_task_factory
from wool.runtime.context.manifest import ChainManifest
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


# Module-level Event so the routine and the test share one instance
# under cloudpickle transport — the routine is pickled by reference and
# resolves this global on the worker side (a closure-captured Event
# would not pickle; see ``_slow_coro``). The routine sets it on entering
# its blocking step so a cancel-mid-step test knows the worker is
# suspended *inside* ``_drive_step``'s per-step ``await`` before it
# cancels.
_STEP_BLOCKING = threading.Event()


async def _gen_blocks_in_step():
    """Async generator whose first step signals then blocks forever, so
    a cancel preempts the in-flight per-step task rather than racing the
    routine's completion."""
    _STEP_BLOCKING.set()
    await asyncio.sleep(3600)
    yield "never"


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


# Unarmed-worker regression fixture — a module-level wool.ContextVar
# with a default plus a coroutine routine that offloads, via *plain*
# asyncio.to_thread, a function that reads it. When the routine is
# dispatched with no caller Wool state, the worker chain stays
# unarmed, so the offload runs as a plain contextvars context and the
# read returns the default instead of tripping ChainContention.
# Module-level so the routine and the var are picklable for cloudpickle
# transport.
_UNARMED_WORKER_VAR: wool.ContextVar[str] = wool.ContextVar(
    "unarmed_worker_var", default="unset-default"
)


def _read_unarmed_worker_var() -> str:
    """Read the unarmed-worker regression var — runs in the offload thread."""
    return _UNARMED_WORKER_VAR.get()


async def _coro_offloads_plain_to_thread():
    """Coroutine routine that offloads a wool.ContextVar read via
    plain asyncio.to_thread.
    """
    return await asyncio.to_thread(_read_unarmed_worker_var)


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


def _next_request(*, with_context: bool = False) -> protocol.Request:
    """Build a follow-up ``next`` request frame.

    When *with_context* is True, sets an empty (but present)
    ``context`` field on the wire so tests that exercise the
    chain-manifest decode path see a present-but-empty wire frame
    rather than the absent-field path that lazy-wire-frame semantics
    short-circuit.
    """
    request = protocol.Request(next=protocol.Void())
    if with_context:
        request.context.CopyFrom(protocol.ChainManifest())
    return request


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
    # The autouse ``_reap_worker_loops`` fixture (see conftest) reaps
    # any worker loop a dispatch left warm; see `_WORKER_LOOP_TTL` for
    # why the pool keeps one warm.


@pytest.fixture(scope="function")
def grpc_stub_cls():
    return WorkerStub


# ---------------------------------------------------------------------------
# Rejected
# ---------------------------------------------------------------------------


class TestRejected:
    """Tests for :class:`Rejected`."""

    def test___init___should_expose_original_and_stringify(self):
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

    def test___init___should_preserve_subclass_name_and_str_when_custom_exception(
        self,
    ):
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

    def test___init___should_leave_public_attrs_unset(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :class:`DispatchSession` constructor leaves public attrs unset.

        Given:
            A request stream and a worker loop
        When:
            :class:`DispatchSession` is constructed
        Then:
            It should be created without raising and ``task`` /
            ``decoded`` / ``serializer`` should be unset before
            ``__aenter__``.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        # Act
        handler = DispatchSession(stream, worker_loop)

        # Assert
        assert isinstance(handler, DispatchSession)
        # Class-level annotations declare ``task`` and ``decoded``;
        # they are only populated on enter so accessing them before
        # enter raises AttributeError.
        with pytest.raises(AttributeError):
            handler.task  # noqa: B018
        with pytest.raises(AttributeError):
            handler.decoded  # noqa: B018
        # All dispatch serializes through cloudpickle; this one
        # serializer covers the payload, the context contexts, and
        # any ``Rejected`` dumped exception (including pre-parse
        # failures such as StopAsyncIteration or a malformed frame).
        assert handler.serializer is wool.__serializer__

    # -- streaming property ----------------------------------------------

    @pytest.mark.asyncio
    async def test_streaming_should_return_true_when_async_generator_task(
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
    async def test_streaming_should_return_false_when_coroutine_task(
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
    async def test___aenter___should_populate_public_attrs_when_coroutine_task(
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
            It should populate ``handler.task``, ``handler.decoded``,
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
            assert isinstance(handler.decoded, ChainManifest)
            assert handler.serializer is wool.__serializer__

    @pytest.mark.asyncio
    async def test___aenter___should_populate_public_attrs_when_async_generator_task(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aenter__` populates public attrs for a
        cloudpickle-encoded async-generator task.

        Given:
            A request stream whose first frame is a valid
            async-generator :class:`Task` with cloudpickle
            serialization
        When:
            The handler is entered via ``async with``
        Then:
            It should populate ``handler.task`` and ``handler.decoded``,
            set ``handler.serializer`` to ``wool.__serializer__``, and
            mark ``handler.streaming`` as ``True``.
        """
        # Arrange
        task = _make_task(_gen_default_two)
        stream = _stream(_request_for(task))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            # Assert
            assert isinstance(handler.task, Task)
            assert handler.task.callable is _gen_default_two
            assert isinstance(handler.decoded, ChainManifest)
            assert handler.serializer is wool.__serializer__
            assert handler.streaming is True

    @pytest.mark.asyncio
    async def test___aenter___should_raise_rejected_when_sync_callable(
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
    async def test___aenter___should_raise_rejected_when_malformed_task_id(
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
    async def test___aenter___should_raise_rejected_when_empty_request_stream(
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
    async def test___aenter___should_raise_rejected_when_first_frame_not_task(
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
    async def test___aenter___should_propagate_cancelled_when_iterator_cancelled(
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
    async def test___aenter___should_wrap_context_decode_error_in_rejected(
        self,
        worker_loop,
        mock_worker_proxy_cache,
        mocker: MockerFixture,
    ):
        """Test :meth:`__aenter__` wraps a pre-Ack
        :class:`ChainSerializationError` in :class:`Rejected` for Nack
        transport.

        Given:
            A request stream whose first frame is well-formed but
            :func:`Chain.from_protobuf` raises a
            :class:`ChainSerializationError` aggregating per-var warnings
            (strict mode).
        When:
            The handler is entered via ``async with``.
        Then:
            It should raise :class:`Rejected` whose ``.original`` is
            the :class:`ChainSerializationError` itself — the routine
            does not run; the dispatch handler ships the error via
            the Nack channel; the caller catches the same error class
            symmetrically.
        """
        # Arrange
        import wool

        task = _make_task(_coro_returning_default)
        # Carry a present (but minimal) chain manifest so the lazy-wire-
        # frame receiver actually invokes
        # ``ChainManifest.from_protobuf``.
        request = _request_for(task)
        request.context.CopyFrom(protocol.ChainManifest())
        stream = _stream(request)

        peer = wool.SerializationWarning("decode peer")
        err = wool.ChainSerializationError(peer)
        from wool.runtime.context.manifest import ChainManifest

        mocker.patch.object(
            ChainManifest,
            "from_protobuf",
            classmethod(lambda cls, *a, **kw: (_ for _ in ()).throw(err)),
        )

        # Act & assert
        with pytest.raises(Rejected) as exc_info:
            async with DispatchSession(stream, worker_loop):
                pass

        original = exc_info.value.original
        assert original is err
        assert isinstance(original, wool.ChainSerializationError)
        assert len(original.warnings) == 1
        assert original.warnings[0] is peer

    @pytest.mark.asyncio
    async def test___aenter___should_propagate_base_exception_when_decode_raises_base(
        self,
        worker_loop,
        mock_worker_proxy_cache,
        mocker: MockerFixture,
    ):
        """Test :meth:`__aenter__` propagates a non-Exception raise
        from decode raw (no Rejected wrap).

        Given:
            A request stream whose first frame triggers an
            :class:`asyncio.CancelledError` raised directly from
            :meth:`ChainManifest.from_protobuf` (e.g. cancellation
            arriving mid-decode).
        When:
            The handler is entered via ``async with``.
        Then:
            It should propagate the :class:`asyncio.CancelledError`
            raw — the parse-phase ``except Exception`` arm does not
            catch :class:`BaseException` subclasses; Nack is the wrong
            channel for cancellation/interrupt signals.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        # Carry a present chain manifest so the lazy-wire-frame receiver
        # actually invokes ``ChainManifest.from_protobuf``.
        request = _request_for(task)
        request.context.CopyFrom(protocol.ChainManifest())
        stream = _stream(request)

        cancelled = asyncio.CancelledError()
        from wool.runtime.context.manifest import ChainManifest

        mocker.patch.object(
            ChainManifest,
            "from_protobuf",
            classmethod(lambda cls, *a, **kw: (_ for _ in ()).throw(cancelled)),
        )

        # Act & assert
        with pytest.raises(asyncio.CancelledError) as exc_info:
            async with DispatchSession(stream, worker_loop):
                pass

        assert not isinstance(exc_info.value, Rejected)
        assert exc_info.value is cancelled

    # -- __aiter__ --------------------------------------------------------

    @pytest.mark.asyncio
    async def test___aiter___should_yield_single_response_when_coroutine_routine(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aiter__` yields exactly one response for a
        coroutine routine.

        Given:
            A handler past :meth:`__aenter__` with a coroutine routine
            returning a value, dispatched with no caller Wool context
            state
        When:
            The handler is iterated via ``async for``
        Then:
            It should yield exactly one response whose result is the
            coroutine's return value and whose chain manifest is a stateless
            :class:`protocol.ChainManifest` — a stateless dispatch leaves the
            worker chain unarmed, so the post-step chain manifest carries no
            variable entries.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            results = [r async for r in handler]

        # Assert
        assert len(results) == 1
        assert results[0].payload == "coroutine_value"
        # Under lazy-wire-frame semantics the unarmed worker omits
        # the optional chain-manifest field entirely — the encode site
        # reads wool.__chain__.get(), which raises LookupError on an
        # unarmed worker, and maps that to a None wire chain manifest.
        assert results[0].chain_manifest is None

    @pytest.mark.asyncio
    async def test___aiter___should_yield_default_when_stateless_dispatch(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test a stateless dispatch leaves the worker chain unarmed.

        Given:
            A coroutine routine dispatched with no caller wool.ContextVar
            state, whose body offloads a wool.ContextVar read through
            plain asyncio.to_thread
        When:
            The handler is iterated via ``async for``
        Then:
            It should yield one response whose result is the variable's
            constructor default — the worker chain stayed unarmed
            (context_has_state gating), so the plain to_thread
            offload ran as a bare contextvars context and never tripped
            wool.ChainContention.
        """
        # Arrange
        task = _make_task(_coro_offloads_plain_to_thread)
        stream = _stream(_request_for(task))

        # Act
        async with DispatchSession(stream, worker_loop) as handler:
            results = [r async for r in handler]

        # Assert
        assert len(results) == 1
        assert results[0].payload == "unset-default"

    @pytest.mark.asyncio
    async def test___aiter___should_yield_response_per_request_when_async_generator(
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
        assert [r.payload for r in results] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test___aiter___should_drive_multi_step_generator_through_caller_requests(
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
        assert [r.payload for r in results] == ["a", "b"]

    @pytest.mark.asyncio
    async def test___aiter___should_return_same_iterator_when_called_twice(
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
    async def test___aiter___should_propagate_exception_when_routine_raises_mid_stream(
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
    async def test___aiter___should_exit_cleanly_when_request_stream_ends(
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

    @pytest.mark.asyncio
    async def test___aiter___should_reclaim_worker_task_on_natural_completion(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test a completed worker driver task is reclaimed by
        refcounting alone when a coroutine routine runs to natural
        completion.

        Given:
            A handler driving a coroutine routine, with the session
            strongly retained for the whole test (modelling a completed
            session a separate holder keeps alive) and automatic cyclic
            GC disabled
        When:
            The dispatch runs to natural (non-cancelled) completion and
            the last external reference to the worker driver task is
            dropped
        Then:
            It should let refcounting reclaim the worker driver task — a
            weakref to it clears without a forced ``gc.collect()`` and
            without automatic collection — proving the completed task's
            done-callback severed the ``session -> worker task ->
            coroutine -> session`` cycle so the retained session no
            longer pins the task (and its per-fork contexts) through the
            reference cycle until the next GC pass.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))
        handler = DispatchSession(stream, worker_loop)
        await handler.__aenter__()
        try:
            iterator = aiter(handler)

            # Capture the worker driver task as a public observable while
            # it is suspended on the request queue, before the dispatch
            # drives it — ``asyncio.all_tasks`` exposes the scheduled
            # worker task and the driver runs ``_run``, so its coroutine
            # qualname distinguishes it from any in-flight per-step task.
            driver = None
            for _ in range(500):
                drivers = [
                    t
                    for t in asyncio.all_tasks(loop=worker_loop)
                    if "_run" in t.get_coro().__qualname__
                ]
                if drivers:
                    driver = drivers[0]
                    break
                await asyncio.sleep(0.01)
            assert driver is not None, "worker driver task was never scheduled"
            worker_task_ref = weakref.ref(driver)
            del driver, drivers

            # Act — with automatic GC disabled, only refcounting can
            # reclaim the finished task. Drive the coroutine to natural
            # completion, release the produced responses, and drain so the
            # driver returns normally (its done-callback drops the
            # session's reference to it).
            gc.disable()
            try:
                results = [r async for r in iterator]
                assert len(results) == 1
                assert results[0].payload == "coroutine_value"
                del results
                await asyncio.wait_for(handler.drain(), timeout=2.0)

                # Assert — the session is still strongly held; if it kept
                # pointing at the task (the pre-fix cycle) the weakref
                # would survive with GC off. The fix drops that reference
                # on natural completion, so refcounting clears the weakref.
                for _ in range(200):
                    if worker_task_ref() is None:
                        break
                    await asyncio.sleep(0.01)
                assert worker_task_ref() is None, (
                    "a retained session must not pin its naturally "
                    "completed worker driver task — refcounting should "
                    "reclaim it without a forced gc.collect() or an "
                    "automatic collection"
                )
            finally:
                gc.enable()
        finally:
            await handler.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test___aiter___should_reclaim_worker_task_without_forced_gc_when_retained(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test a completed worker driver task is reclaimed by
        refcounting alone even while the session is retained.

        Given:
            A handler whose worker driver task is live on the worker
            loop, with the session strongly retained for the whole test
            (modelling a completed session a separate holder keeps
            alive) and automatic cyclic GC disabled
        When:
            The dispatch completes and the last external reference to
            the worker driver task is dropped
        Then:
            It should let refcounting reclaim the worker driver task
            immediately — a weakref to it clears without a forced
            ``gc.collect()`` and without automatic collection — proving
            the retained session no longer pins the task (and its
            per-fork contexts) through the reference cycle until the
            next GC pass.
        """
        # Arrange — a never-returning coroutine keeps the worker driver
        # task live on the worker loop long enough to capture a public
        # handle to it via ``asyncio.all_tasks``.
        task = _make_task(_slow_coro)
        stream = _stream(_request_for(task))
        handler = DispatchSession(stream, worker_loop)
        await handler.__aenter__()
        try:
            iterator = aiter(handler)
            pull = asyncio.ensure_future(anext(iterator))

            # Capture the worker driver task as a public observable —
            # ``asyncio.all_tasks`` exposes the scheduled worker task and
            # the driver runs ``_run``, so its coroutine qualname
            # distinguishes it from any in-flight per-step task.
            driver = None
            for _ in range(500):
                drivers = [
                    t
                    for t in asyncio.all_tasks(loop=worker_loop)
                    if "_run" in t.get_coro().__qualname__
                ]
                if drivers:
                    driver = drivers[0]
                    break
                await asyncio.sleep(0.01)
            assert driver is not None, "worker driver task was never scheduled"
            worker_task_ref = weakref.ref(driver)
            del driver, drivers

            # Act — with automatic GC disabled, only refcounting can
            # reclaim the finished task. Complete the dispatch (cancel
            # drives the worker driver task to completion, whose
            # done-callback drops the session's reference to it) and
            # release the last external handle to the iterator's pull.
            gc.disable()
            try:
                await handler.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await pull
                del pull
                await asyncio.wait_for(handler.drain(), timeout=2.0)

                # Assert — the session is still strongly held; if it kept
                # pointing at the task (the pre-fix cycle) the weakref
                # would survive with GC off. The fix drops that
                # reference, so refcounting clears the weakref.
                for _ in range(200):
                    if worker_task_ref() is None:
                        break
                    await asyncio.sleep(0.01)
                assert worker_task_ref() is None, (
                    "a retained session must not pin its completed worker "
                    "driver task — refcounting should reclaim it without a "
                    "forced gc.collect() or an automatic collection"
                )
            finally:
                gc.enable()

            # The session was strongly referenced throughout, so the
            # reclamation above is attributable to the severed cycle, not
            # to the session itself being collected.
            assert handler is not None
        finally:
            await handler.__aexit__(None, None, None)

    # -- __aexit__ --------------------------------------------------------

    @pytest.mark.asyncio
    async def test___aexit___should_not_raise_when_cancelled_before_aiter(
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
    async def test___aexit___should_not_reraise_worker_exception(
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

    @pytest.mark.asyncio
    async def test___aexit___should_surface_cancelled_error_when_drain_raises(
        self, worker_loop, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :meth:`__aexit__` unwinds the exit stack even when the
        registered :meth:`drain` callback raises ``CancelledError``.

        Given:
            A :class:`DispatchSession` whose :meth:`drain` is patched
            to raise :class:`asyncio.CancelledError`
        When:
            :meth:`__aenter__` succeeds and the ``async with`` block
            exits
        Then:
            It should surface :class:`asyncio.CancelledError` out of
            :meth:`__aexit__` while still invoking the registered
            :meth:`drain` callback — the exit-stack unwind finishes
            without hanging or raising a different error.
        """
        # Arrange — patching :class:`DispatchSession.drain` is the
        # SUT-of-this-file's own public method; the substitution
        # injects the cleanup-time re-raise that the exit-stack
        # registration must tolerate. No cleaner real-boundary
        # trigger exists. The flag records that the registered
        # callback actually ran during the unwind.
        drain_ran = False

        async def raising_drain(self):
            nonlocal drain_ran
            drain_ran = True
            raise asyncio.CancelledError("simulated drain re-raise")

        mocker.patch.object(DispatchSession, "drain", raising_drain)

        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        handler = DispatchSession(stream, worker_loop)
        await handler.__aenter__()

        # Act & assert — the unwind runs the registered drain
        # callback and surfaces its CancelledError; the bounded
        # wait_for proves the unwind completed rather than hung.
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(handler.__aexit__(None, None, None), timeout=5.0)

        assert drain_ran, (
            "__aexit__ must run the drain callback registered on the "
            "exit stack even though it raises CancelledError"
        )

    @pytest.mark.asyncio
    async def test___aexit___should_run_drain_when_caller_cancelled_mid_teardown(
        self, worker_loop, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :meth:`__aexit__` runs the registered :meth:`drain`
        callback to completion when the caller task is cancelled
        mid-teardown.

        Given:
            A :class:`DispatchSession` past :meth:`__aenter__` driving
            a long-running routine, awaited inside a task that is
            cancelled while suspended in the ``async with`` block exit
        When:
            The caller task is cancelled mid-:meth:`__aexit__`
        Then:
            It should run the registered :meth:`drain` callback to
            completion (shielded unwind) before
            :class:`asyncio.CancelledError` propagates.
        """
        # Arrange — patching :class:`DispatchSession.drain` is the
        # SUT-of-this-file's own public method; the substitution
        # parks the teardown on a controllable suspension so the
        # caller task can be cancelled while the exit-stack unwind
        # is in flight, then delegates to the real drain so the
        # long-running worker routine is genuinely torn down.
        original_drain = DispatchSession.drain
        drain_entered = asyncio.Event()
        drain_completed = False
        proceed = asyncio.Event()

        task = _make_task(_slow_gen)
        stream = _stream(_request_for(task), _next_request())

        async def parked_drain(self):
            nonlocal drain_completed
            drain_entered.set()
            await proceed.wait()
            # Cancel the still-running worker routine, then run the
            # real drain so the callback completes for real.
            await self.cancel()
            await original_drain(self)
            drain_completed = True

        mocker.patch.object(DispatchSession, "drain", parked_drain)

        observed: list[BaseException] = []

        async def caller():
            handler = DispatchSession(stream, worker_loop)
            await handler.__aenter__()
            # Kick the worker so a real routine is in flight, then
            # exit — __aexit__'s shielded unwind suspends inside the
            # parked drain callback.
            aiter(handler)
            try:
                await handler.__aexit__(None, None, None)
            except BaseException as e:
                observed.append(e)
                raise

        # Act
        caller_task = asyncio.ensure_future(caller())
        await drain_entered.wait()
        caller_task.cancel()
        # Let the cancellation register against the shielded unwind.
        for _ in range(5):
            await asyncio.sleep(0)
        # Release the parked drain so the shielded callback finishes.
        proceed.set()
        with pytest.raises(asyncio.CancelledError):
            await caller_task

        # Assert — the registered drain callback ran to completion
        # despite the mid-teardown cancel, and CancelledError still
        # propagated to the caller.
        assert drain_completed, (
            "the shielded exit-stack unwind must run the registered "
            "drain callback to completion even when the caller task "
            "is cancelled mid-teardown"
        )
        assert any(isinstance(e, asyncio.CancelledError) for e in observed)

    # -- drain ------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_drain_should_return_promptly_when_called_twice(
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
    async def test_drain_should_propagate_cancelled_error_when_awaiting_task_cancelled(
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
    async def test_drain_should_swallow_worker_cancellation(
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
        # via the public :meth:`cancel` (which cancels the worker
        # task on its own loop), then call drain from the main loop.
        task = _make_task(_slow_gen)
        stream = _stream(_request_for(task), _next_request())

        async with DispatchSession(stream, worker_loop) as handler:
            iterator = aiter(handler)

            # Wait for the worker driver task to appear on the worker
            # loop — ``asyncio.all_tasks`` is a public observable of
            # the scheduled worker task, so ``cancel`` below has a
            # live worker task to cancel on its own loop.
            for _ in range(200):
                if asyncio.all_tasks(loop=worker_loop):
                    break
                await asyncio.sleep(0.01)

            # Cancel via the public surface. ``cancel`` schedules the
            # worker task's cancellation on the worker loop; the
            # cascade settles the worker-completion future with
            # ``CancelledError`` and closes the response queue, so the
            # iterator exits.
            await handler.cancel()

            # Drive the iterator to exit. Narrow the catch to
            # ``CancelledError`` — the only exception this scenario can
            # legitimately produce — so any unrelated regression that
            # surfaces a different exception class is not silently
            # absorbed.
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
    async def test_drain_should_return_when_worker_loop_closed(
        self, mock_worker_proxy_cache
    ):
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

        # Close the worker loop before any __aiter__, so drain's
        # short-circuit path (no worker scheduled) is exercised.
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        loop.close()

        # Act
        try:
            await asyncio.wait_for(handler.drain(), timeout=2.0)
            # Assert — drain returned without raising.
        finally:
            # Best-effort public teardown.
            try:
                await handler.__aexit__(None, None, None)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_drain_should_return_when_worker_create_task_fails(
        self, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test :meth:`drain` returns when ``_start`` fails to
        create the worker task on the worker loop.

        Regression test. Pre-fix, ``_start`` (scheduled
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
            The first :meth:`__aiter__` schedules ``_start`` on the
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

            # Trigger lazy scheduling through the public iteration
            # entry point. ``__aiter__`` calls ``_schedule_worker``,
            # which schedules ``_start`` via
            # ``call_soon_threadsafe``; _start later runs on the
            # worker loop where the patched create_task raises.
            aiter(handler)

            # Wait briefly for the worker loop to execute _start.
            await asyncio.sleep(0.1)

            # Act + Assert — drain must return quickly. Pre-fix
            # it hangs because worker_done was never settled.
            await asyncio.wait_for(handler.drain(), timeout=2.0)
        finally:
            try:
                await handler.__aexit__(None, None, None)
            except Exception:
                pass
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)
            if not loop.is_closed():
                loop.close()

    # -- cancel -----------------------------------------------------------

    @pytest.mark.asyncio
    async def test_cancel_should_not_raise_when_called_before_aenter(
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
    async def test_cancel_should_not_raise_when_called_after_iteration(
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
    async def test_cancel_should_raise_cancelled_on_iterator_when_different_task(
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

        # Wait until the worker driver task is scheduled on the worker
        # loop — ``asyncio.all_tasks`` is a public observable of the
        # running worker task, which (for the never-yielding
        # ``_slow_gen``) implies the iterator is suspended on the
        # response.
        for _ in range(500):
            if "handler" in captured and asyncio.all_tasks(loop=worker_loop):
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
    async def test_cancel_should_raise_cancelled_error_when_racing_worker_scheduling(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`cancel` racing worker scheduling still cancels
        the worker.

        Given:
            A handler whose first :meth:`__aiter__` has queued the
            worker driver onto the worker loop, with :meth:`cancel`
            invoked in the same main-loop turn — before the worker
            loop has run the queued driver
        When:
            The iterator's first :meth:`anext` is awaited
        Then:
            It should raise :class:`asyncio.CancelledError` and yield
            no value — the driver's own start-time cancellation
            re-check observes the flag and cancels the freshly
            created worker task, so a routine never runs to natural
            completion after a cancel that raced its scheduling.
        """
        # Arrange — a long-running routine so a missed start-time
        # cancellation would otherwise let the worker run for real.
        task = _make_task(_slow_coro)
        stream = _stream(_request_for(task))

        async with DispatchSession(stream, worker_loop) as handler:
            # ``__aiter__`` runs ``_schedule_worker`` synchronously,
            # queuing the worker driver onto the worker loop via
            # ``call_soon_threadsafe``. ``cancel`` is synchronous up to
            # its return, so calling it before any ``await`` yields the
            # main loop sets the cancel flag while the queued driver
            # has not yet run on the worker loop — the race the driver
            # guards against at start time.
            iterator = aiter(handler)
            await handler.cancel()

            # Act & assert — the iterator surfaces the cancellation and
            # the routine never produces a value.
            with pytest.raises(asyncio.CancelledError):
                await anext(iterator)

    @pytest.mark.asyncio
    async def test_cancel_should_raise_cancelled_error_when_iterator_suspended(
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

        # Wait for the worker driver task to be scheduled on the
        # worker loop — ``asyncio.all_tasks`` is a public observable
        # of the running worker task; for the never-yielding
        # ``_slow_gen`` this implies the iterator is suspended.
        for _ in range(500):
            if "handler" in captured and asyncio.all_tasks(loop=worker_loop):
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
    async def test_cancel_should_not_raise_when_called_twice(
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
    async def test_cancel_should_preempt_routine_when_suspended_mid_step(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`cancel` preempts a routine suspended inside a step.

        Given:
            A handler whose streaming routine has been forwarded its
            first request and is suspended inside ``_drive_step``'s
            per-step ``await`` (a long sleep).
        When:
            :meth:`cancel` is awaited.
        Then:
            It should preempt the routine rather than let it run to
            natural completion — the iterator surfaces
            :class:`asyncio.CancelledError` and :meth:`drain` returns
            promptly instead of awaiting the routine's full sleep.
        """
        # Arrange — forward the first Next to the worker. ``anext``
        # forwards the request (the put precedes the response await)
        # then suspends on the response that never arrives, so the
        # worker enters its first step. The routine sets a module-level
        # Event the moment it is inside that step.
        _STEP_BLOCKING.clear()
        task = _make_task(_gen_blocks_in_step)
        stream = _stream(_request_for(task), _next_request())

        async with DispatchSession(stream, worker_loop) as handler:
            iterator = aiter(handler)
            pull = asyncio.ensure_future(anext(iterator))
            try:
                # Wait until the routine signals it is suspended *inside*
                # the step (it sets the Event right before its in-step
                # ``await``), so the cancel below lands while the
                # per-step task is in flight. Awaiting the blocking wait
                # in the default executor keeps the main loop free to
                # pump ``pull``'s request forward to the worker.
                entered = await asyncio.get_running_loop().run_in_executor(
                    None, _STEP_BLOCKING.wait, 5.0
                )
                assert entered, "worker never entered the blocking step"

                # Act — cancel while the step task is still in flight.
                await handler.cancel()

                # Assert — the suspended pull surfaces cancellation, and
                # drain returns promptly because the step task was
                # cancelled rather than awaited to its full sleep.
                with pytest.raises(asyncio.CancelledError):
                    await pull
                await asyncio.wait_for(handler.drain(), timeout=2.0)
            finally:
                if not pull.done():
                    pull.cancel()

    @pytest.mark.asyncio
    async def test___aiter___should_propagate_context_error_when_mid_stream_decode_fails(
        self, worker_loop, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test mid-stream chain-manifest decode failures propagate as
        :class:`ChainSerializationError` through the routine-exception
        channel.

        Given:
            A streaming session whose first ``next`` request decodes
            cleanly but whose second ``next`` request's chain-manifest
            decode raises a :class:`ChainSerializationError` aggregating
            per-var warnings (operator promoted
            :class:`SerializationWarning` to an exception).
        When:
            The caller iterates the session past the first response.
        Then:
            The :class:`ChainSerializationError` should surface
            unmolested out of the iterator — under the strict-mode
            "fail loud" contract the worker ships it via the
            routine-exception channel; the caller's existing
            ``except ChainSerializationError`` catches without migrating
            to ``except*``.
        """
        # Arrange — streaming routine that yields per ``next``. The
        # mid-stream next-requests carry a present-but-empty chain manifest so
        # ``RequestFrame.from_protobuf`` routes into
        # ``ChainManifest.from_protobuf`` (and hence the patched
        # mock) under lazy-wire-frame semantics — without a present
        # field, the decode path short-circuits before the mock can
        # fire. ``__aenter__``'s initial decode does not hit the mock
        # either because the task-frame fixture omits the optional
        # context field; so the first patched call corresponds to the
        # first mid-stream decode.
        import wool

        task = _make_task(_gen_three)
        stream = _stream(
            _request_for(task),
            _next_request(with_context=True),
            _next_request(with_context=True),
        )

        # Counter-based patch: let the first per-step decode succeed;
        # the second mid-stream decode raises a ChainSerializationError.
        from wool.runtime.context.manifest import ChainManifest

        original = ChainManifest.from_protobuf
        calls = {"n": 0}
        peer = wool.SerializationWarning("decode peer")

        def fake_from_protobuf(cls, wire, *, serializer=None):
            calls["n"] += 1
            if calls["n"] >= 2:
                raise wool.ChainSerializationError(peer)
            return original(wire, serializer=serializer)

        mocker.patch.object(
            ChainManifest,
            "from_protobuf",
            classmethod(fake_from_protobuf),
        )

        # Act — iterate; the second step raises ChainSerializationError,
        # which surfaces out of the iterator.
        captured: list[BaseException] = []
        async with DispatchSession(stream, worker_loop) as handler:
            try:
                async for _ in handler:
                    pass
            except BaseException as e:
                captured.append(e)

        # Assert: typed ChainSerializationError carrying the original
        # warning on .warnings.
        assert len(captured) == 1
        primary = captured[0]
        assert isinstance(primary, wool.ChainSerializationError)
        assert primary.warnings == (peer,)

    @pytest.mark.asyncio
    async def test___aiter___should_break_pump_when_cancelled_between_requests(
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
        assert observed["responses"][0].payload == 1

    @pytest.mark.asyncio
    async def test_cancel_should_return_when_worker_loop_closed(
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
        # Arrange — drive a real dispatch to completion so the worker
        # driver task is genuinely scheduled (and recorded) on the
        # worker loop. Once it has run, ``cancel`` still routes through
        # the worker-task-cancel branch (the task reference is set), so
        # the patched ``call_soon_threadsafe`` exercises the
        # closed-loop swallow rather than the early-return path that a
        # never-scheduled worker would take.
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        async with DispatchSession(stream, worker_loop) as handler:
            # Consume the result so the worker driver task settles —
            # this avoids the teardown blocking on a live routine while
            # still leaving the worker-task reference in place for
            # cancel to act on.
            results = [r async for r in handler]
            assert results[0].payload == "coroutine_value"

            # Patch the worker loop's ``call_soon_threadsafe`` to
            # simulate a torn-down loop ("Event loop is closed") — the
            # boundary cancel must schedule across. This affects only
            # the worker-task-cancel path; the response queue close
            # rides the main loop.
            mocker.patch.object(
                worker_loop,
                "call_soon_threadsafe",
                side_effect=RuntimeError("Event loop is closed"),
            )

            # Act & assert — cancel returns without raising; the
            # ``call_soon_threadsafe`` RuntimeError is swallowed.
            await handler.cancel()

    # -- Migrated from test_service.py::TestWorkerService -----------------

    @pytest.mark.asyncio
    async def test___aiter___should_defer_worker_scheduling(
        self, worker_loop, mock_worker_proxy_cache
    ):
        """Test :meth:`__aiter__` defers worker scheduling until the
        first iteration rather than scheduling eagerly inside
        :meth:`__aenter__`.

        Regression test for the race between dispatch's backpressure
        hook and the worker for chain ownership. Pre-fix
        :meth:`__aenter__` scheduled the worker eagerly; with a
        backpressure hook that yielded the main loop while reading the
        decoded chain manifest, the worker thread would race to re-stamp the
        same chain's owner. The invariant tested here —
        :meth:`__aenter__` is parse-only — guarantees no contention
        regardless of how long any post-parse main-loop work holds the
        chain.

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
            # The worker driver runs as a task on the worker loop;
            # ``asyncio.all_tasks`` is the public observable of
            # whether it has been scheduled. After parse-only
            # ``__aenter__`` (no ``__aiter__`` yet) the worker loop
            # carries no driver task.
            assert not asyncio.all_tasks(loop=worker_loop), (
                "DispatchSession.__aenter__ must defer worker scheduling"
            )

            # Act
            iterator = aiter(handler)

            # Assert — the first ``__aiter__`` schedules the worker
            # driver task on the worker loop. Scheduling crosses
            # loops via ``call_soon_threadsafe``, so poll until the
            # driver task appears.
            for _ in range(500):
                if asyncio.all_tasks(loop=worker_loop):
                    break
                await asyncio.sleep(0.01)
            else:
                pytest.fail(
                    "DispatchSession.__aiter__ must schedule the worker on first call"
                )
            response = await anext(iterator)
            assert response.payload == "coroutine_value"

    @pytest.mark.asyncio
    async def test_streaming_should_end_stream_when_put_to_closed_worker_loop(
        self, mock_worker_proxy_cache
    ):
        """Test forwarding a request after the worker loop closed ends
        the stream cleanly.

        Given:
            A streaming dispatch whose worker loop is torn down after
            the worker is scheduled but before the next request is
            forwarded — the graceful-shutdown race where the loop pool
            reclaims the worker loop between two main-loop pumps.
        When:
            The session forwards the next request onto the closed loop.
        Then:
            ``_RequestQueue.put`` should surface the closed loop as the
            typed ``_WorkerLoopClosed`` signal and the iterator should
            end with no responses, rather than shipping the transport
            teardown as a routine failure.
        """
        # Arrange — a dedicated worker loop we can close
        # deterministically. It is never run, so ``__aenter__`` (on the
        # main loop) and the scheduling in ``__aiter__`` (a queued,
        # never-executed ``call_soon`` on this loop) leave no worker
        # task to orphan. The session is driven without ``async with``:
        # closing the worker loop leaves the worker-completion future
        # unresolved, so the registered ``drain`` teardown would block
        # awaiting it — the production graceful-shutdown path tolerates
        # this, and the closed loop plus pending future are GC-clean.
        worker_loop = asyncio.new_event_loop()
        try:
            task = _make_task(_gen_default_two)
            stream = _stream(_request_for(task), _next_request())
            handler = DispatchSession(stream, worker_loop)
            await handler.__aenter__()
            iterator = aiter(handler)

            # Act — tear the worker loop down before the next request is
            # forwarded.
            worker_loop.close()
            responses = [response async for response in iterator]
        finally:
            if not worker_loop.is_closed():
                worker_loop.close()

        # Assert — the closed-loop put surfaced the typed signal and the
        # stream ended cleanly with no responses.
        assert responses == []

    @pytest.mark.asyncio
    async def test_coroutine_should_end_stream_when_put_to_closed_worker_loop(
        self, mock_worker_proxy_cache
    ):
        """Test forwarding a coroutine's prime after the worker loop
        closed ends the stream cleanly.

        Given:
            A coroutine dispatch whose worker loop is torn down after
            the worker is scheduled but before the prime request is
            forwarded — the graceful-shutdown race where the loop pool
            reclaims the worker loop between two main-loop pumps.
        When:
            The session forwards the prime request onto the closed loop.
        Then:
            ``_RequestQueue.put`` should surface the closed loop as the
            typed ``_WorkerLoopClosed`` signal and the iterator should
            end with no responses, rather than shipping the transport
            teardown as a routine failure.
        """
        # Arrange — a dedicated worker loop we can close deterministically.
        # As with the streaming twin, the session is driven without
        # ``async with`` so closing the worker loop (leaving the
        # worker-completion future unresolved) does not block the
        # registered ``drain`` teardown.
        worker_loop = asyncio.new_event_loop()
        try:
            task = _make_task(_coro_returning_default)
            stream = _stream(_request_for(task), _next_request())
            handler = DispatchSession(stream, worker_loop)
            await handler.__aenter__()
            iterator = aiter(handler)

            # Act — tear the worker loop down before the prime is forwarded.
            worker_loop.close()
            responses = [response async for response in iterator]
        finally:
            if not worker_loop.is_closed():
                worker_loop.close()

        # Assert — the closed-loop put surfaced the typed signal and the
        # stream ended cleanly with no responses.
        assert responses == []

    @pytest.mark.asyncio
    async def test___aexit___should_call_drain_twice_on_terminal_exception(
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
        clause with the worker mid-``_step`` mutating the work
        chain. Without an explicit drain before reading
        ``session._final_wire_chain_manifest``, the handler would read the
        worker-published chain manifest before the worker task has
        finished encoding it inside its own Chain. The fix calls
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
            context=protocol.ChainManifest(id=uuid4().hex),
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
        # read of ``session._final_wire_chain_manifest`` races the
        # still-alive worker.
        assert drain_spy.call_count >= 2, (
            f"Expected dispatch's terminal-exception clause to call "
            f"DispatchSession.drain before reading "
            f"DispatchSession._final_wire_chain_manifest (plus __aexit__'s "
            f"call); observed {drain_spy.call_count} call(s)."
        )

    @pytest.mark.asyncio
    async def test___aexit___should_ship_routine_exception_when_cancel_raises(
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
    async def test_drain_should_return_when_worker_loop_closed_pre_schedule(
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
                await handler.__aexit__(None, None, None)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_cancel_should_short_circuit_scheduling_when_called_before_aiter(
        self, worker_loop, mock_worker_proxy_cache
    ):
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
            It should not schedule the worker driver task on the
            worker loop and the iterator's first ``anext`` should
            raise :class:`asyncio.CancelledError`.
        """
        # Arrange
        task = _make_task(_coro_returning_default)
        stream = _stream(_request_for(task))

        async with DispatchSession(stream, worker_loop) as handler:
            # Act
            await handler.cancel()
            iterator = aiter(handler)

            # Give any cross-loop scheduling a chance to land so the
            # absence below is meaningful rather than merely early.
            await asyncio.sleep(0.05)

            # Assert — the cancel short-circuit means no worker driver
            # task is ever scheduled on the worker loop.
            # ``asyncio.all_tasks`` is the public observable of that
            # absence.
            assert not asyncio.all_tasks(loop=worker_loop), (
                "cancel() before __aiter__ must short-circuit worker "
                "scheduling — no driver task should run on the worker loop"
            )

            # The iterator surfaces the cancellation immediately.
            with pytest.raises(asyncio.CancelledError):
                await anext(iterator)

    @pytest.mark.asyncio
    async def test_cancel_should_not_raise_when_called_from_different_task(
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

        # Wait until the worker driver task is scheduled on the
        # worker loop — ``asyncio.all_tasks`` is the public observable
        # of the running worker task. For the never-returning
        # ``_slow_coro`` this proves ``__aiter__`` ran and ``_iterate``
        # is suspended at ``response_queue.get``.
        while "handler" not in captured or not asyncio.all_tasks(loop=worker_loop):
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
