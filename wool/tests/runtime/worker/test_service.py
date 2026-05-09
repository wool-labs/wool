import asyncio
import threading
from contextlib import asynccontextmanager
from uuid import uuid4

import cloudpickle
import grpc
import pytest
from grpc import StatusCode
from hypothesis import HealthCheck
from hypothesis import assume
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

import wool
from wool import protocol
from wool.protocol import WorkerStub
from wool.protocol import add_WorkerServicer_to_server
from wool.runtime.context import install_task_factory
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.worker.interceptor import VersionInterceptor
from wool.runtime.worker.service import WorkerService
from wool.runtime.worker.session import DispatchSession

from .conftest import PicklableMock


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


# Global event for controlling test task execution.
# Uses threading.Event (not asyncio.Event) because the controllable
# task runs on the worker loop while the test sets the event from
# the main loop. threading.Event is thread-safe across event loops.
_control_event: threading.Event | None = None


def _get_control_event() -> threading.Event:
    """Get the global control event for test tasks."""
    assert _control_event
    return _control_event


async def _controllable_task():
    """Task function that waits on the global control event."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _get_control_event().wait)
    return "task_completed"


# Cross-loop side-channel for A1 regression: routine on the worker
# loop signals via this threading.Event when it observes
# CancelledError; the test asserts on it from the main loop.
_a1_cancellation_observed: threading.Event | None = None

# Cross-loop side-channel for the stop+cancel regression tests
# (``test_stop_and_cancel`` and ``test_stop_and_cancel_streaming_routine``).
# The routine on the worker loop signals via this :class:`threading.Event`
# when it observes :class:`asyncio.CancelledError`; the test asserts on it
# from the main loop. Separate from ``_a1_cancellation_observed`` so the
# tests do not interfere when running concurrently or in arbitrary order.
_stop_cancellation_observed: threading.Event | None = None

# Side-channel used by the stop+cancel regression tests to confirm
# the routine has actually started running before the test sends
# ``stop``. Without this barrier the test races
# :meth:`DispatchSession.__aiter__`'s lazy worker scheduling: on
# slower Python versions/runtimes, ``stop`` can land before the
# worker task is created, so ``session.cancel()`` has no
# ``_worker_task`` to cancel and the routine never observes
# cancellation. The routine sets this event as its first statement,
# the test waits for it, then sends ``stop`` knowing the routine is
# suspended in its long sleep.
_stop_routine_started: threading.Event | None = None


class _AttributeRejectingRoutineError(Exception):
    """Module-level exception class for the A4 regression test.

    Overrides ``__setattr__`` to raise :class:`AttributeError` for
    arbitrary attribute writes — modeling exception types whose
    storage layout (e.g., ``__slots__`` derived from a slotted
    parent, or C-extension types with custom attribute machinery)
    rejects the dispatch handler's structured side-channel write.
    Read-only-via-``__init_subclass__`` patterns and frozen
    dataclass exceptions hit the same shape.

    The override forwards the standard ``args``/``__cause__``/
    ``__context__``/``__traceback__``/``__notes__`` slots so
    ``BaseException`` machinery and PEP 678 ``add_note`` continue
    to work — only arbitrary attribute writes (like
    ``__wool_context_warnings__``) raise.
    """

    _ALLOWED = frozenset(
        {
            "args",
            "__cause__",
            "__context__",
            "__traceback__",
            "__suppress_context__",
            "__notes__",
        }
    )

    def __setattr__(self, name, value):
        if name in self._ALLOWED:
            object.__setattr__(self, name, value)
        else:
            raise AttributeError(
                f"{type(self).__name__!r} object does not accept "
                f"arbitrary attribute writes: {name!r}"
            )


async def _a1_long_routine():
    """Module-level routine for the A1 regression test.

    Defined at module level so cloudpickle can serialize the
    callable for dispatch. Sleeps long enough that the test will
    have given up; signals the global event if interrupted by
    :class:`asyncio.CancelledError`.
    """
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        if _a1_cancellation_observed is not None:
            _a1_cancellation_observed.set()
        raise
    return "should_not_complete"


async def _stop_long_coroutine():
    """Module-level coroutine for the stop+cancel regression test.

    Signals :data:`_stop_routine_started` so the test can wait for
    the routine to actually start before sending ``stop`` (avoids
    racing :meth:`DispatchSession.__aiter__`'s lazy worker
    scheduling), then sleeps long enough that the test will have
    given up; signals :data:`_stop_cancellation_observed` if
    interrupted by :class:`asyncio.CancelledError`. Defined at
    module level so cloudpickle can serialize the callable for
    dispatch.
    """
    if _stop_routine_started is not None:
        _stop_routine_started.set()
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        if _stop_cancellation_observed is not None:
            _stop_cancellation_observed.set()
        raise
    return "should_not_complete"


async def _stop_streaming_routine():
    """Module-level async generator for the stop+cancel streaming
    regression test.

    Signals :data:`_stop_routine_started`, yields one value, then
    sleeps long enough that the test will have given up; signals
    :data:`_stop_cancellation_observed` if interrupted by
    :class:`asyncio.CancelledError` or :class:`GeneratorExit`.
    Defined at module level so cloudpickle can serialize the
    callable for dispatch.

    Both exception types signal cancellation from the worker side:
    operator-preempt cancels the worker driver task; depending on
    where the routine is suspended (mid-await vs at a yield) the
    teardown path either propagates :class:`asyncio.CancelledError`
    through the await or injects :class:`GeneratorExit` via
    :func:`routine_scope`'s ``aclose``. Either is a valid
    observation of cancellation reaching the routine.
    """
    if _stop_routine_started is not None:
        _stop_routine_started.set()
    try:
        yield 0
        await asyncio.sleep(30)
    except (asyncio.CancelledError, GeneratorExit):
        if _stop_cancellation_observed is not None:
            _stop_cancellation_observed.set()
        raise


@pytest.fixture
@asynccontextmanager
async def service_fixture(mocker: MockerFixture, grpc_aio_stub):
    """Provides a WorkerService with a dispatched task that waits on an event.

    This fixture returns a tuple of (service, event, stub) where:
    - service: WorkerService instance with a controllable task dispatched
    - event: threading.Event that controls task completion
    - stub: gRPC stub for interacting with the service

    The task remains active until the event is set. On cleanup, if the event
    is still unset, it will be set and the service will be stopped.
    """
    global _control_event

    service = WorkerService()
    _control_event = threading.Event()

    mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

    wool_task = Task(
        id=uuid4(),
        callable=_controllable_task,
        args=(),
        kwargs={},
        proxy=mock_proxy,
    )

    request = protocol.Request(task=wool_task.to_protobuf())

    # Start the service and dispatch the task
    async with grpc_aio_stub(servicer=service) as stub:
        stream = stub.dispatch()
        await stream.write(request)
        await stream.done_writing()

        # Wait for the ack to confirm task is dispatched
        async for response in stream:
            assert response.HasField("ack")
            break

        try:
            yield service, _control_event, stub
        finally:
            # Cleanup: ensure task completes and service stops
            if _control_event and not _control_event.is_set():
                _control_event.set()
            if not service.stopped.is_set():
                stop_request = protocol.StopRequest(timeout=1)
                await stub.stop(stop_request)
            _control_event = None


class TestWorkerService:
    def test___init___with_defaults(self):
        """Test :class:`WorkerService` initialization.

        Given:
            No preconditions
        When:
            :class:`WorkerService` is instantiated
        Then:
            It should initialize successfully and expose its stopping and stopped events
        """
        # Act
        service = WorkerService()

        # Assert
        assert service.stopping is not None
        assert service.stopped is not None
        assert not service.stopping.is_set()
        assert not service.stopped.is_set()

    @pytest.mark.asyncio
    async def test_dispatch_task_that_returns(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch successfully executes task that returns
        a value.

        Given:
            A gRPC :class:`WorkerService` that is not stopping or stopped
        When:
            Dispatch RPC is called with a task that returns a value
        Then:
            It should return the task result
        """

        # Arrange
        async def sample_task():
            return "test_result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, reponse = responses
        assert ack.HasField("ack")
        assert reponse.HasField("result")
        assert cloudpickle.loads(reponse.result.dump) == "test_result"

    @pytest.mark.asyncio
    async def test_dispatch_task_that_raises(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch successfully executes task
        that raises an exception.

        Given:
            A gRPC :class:`WorkerService` that is not stopping or stopped
        When:
            Dispatch RPC is called with a task that raises an exception
        Then:
            It should return the task exception as the result
        """

        # Arrange
        async def failing_task():
            raise ValueError("test_exception")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=failing_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("exception")
        assert response.HasField("context")
        exception = cloudpickle.loads(response.exception.dump)
        assert isinstance(exception, ValueError)
        assert str(exception) == "test_exception"

    @pytest.mark.asyncio
    async def test_dispatch_with_corrupt_context_under_strict_filter(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch under strict mode for
        :class:`ContextDecodeWarning` when the caller's context
        carries a corrupt var payload.

        Given:
            A dispatch Request whose ``context.vars`` map carries a
            corrupt byte payload, and the worker-side warning filter
            promotes :class:`ContextDecodeWarning` to an exception
            (modeling
            ``warnings.filterwarnings("error", category=...)`` set
            via ``PYTHONWARNINGS`` or programmatic config in the
            worker subprocess)
        When:
            The dispatch RPC is invoked with that request
        Then:
            It should reply with exactly one :class:`Nack` response
            (no preceding Ack) whose ``exception`` field decodes to
            a :class:`BaseExceptionGroup` carrying the promoted
            :class:`ContextDecodeWarning` as its sole peer — so
            worker-side strict mode preserves the same uniform
            group shape that caller-side strict mode produces, and
            the leaf class identity remains addressable via
            ``except*`` regardless of peer cardinality.
        """
        import warnings as _warnings

        # Arrange
        async def sample_task():
            return "should_not_execute"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        namespace = f"strict_corrupt_{uuid4().hex}"
        var: wool.ContextVar[str] = wool.ContextVar("x", namespace=namespace)
        context_pb = protocol.Context(id=uuid4().hex)
        context_pb.vars.add(
            namespace=var.namespace,
            name=var.name,
            value=b"\x00not a valid pickle stream\x00",
        )
        request = protocol.Request(
            task=wool_task.to_protobuf(),
            context=context_pb,
        )

        # Act
        with _warnings.catch_warnings():
            _warnings.simplefilter("error", category=wool.ContextDecodeWarning)
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        nack = responses[0]
        assert nack.HasField("nack")
        assert nack.nack.HasField("exception")
        raised = cloudpickle.loads(nack.nack.exception.dump)
        assert isinstance(raised, BaseExceptionGroup)
        assert len(raised.exceptions) == 1
        peer = raised.exceptions[0]
        assert isinstance(peer, wool.ContextDecodeWarning)
        assert "Failed to deserialize" in str(peer)

    @pytest.mark.asyncio
    async def test_dispatch_with_malformed_task_id(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch when the wire-shipped
        task id cannot be parsed as a UUID.

        Given:
            A dispatch Request whose ``task.id`` field is a
            non-hex / non-UUID string, so ``UUID(request.task.id)``
            raises :class:`ValueError` inside the parse phase
        When:
            The dispatch RPC is invoked with that request
        Then:
            It should reply with exactly one :class:`Nack` response
            (no preceding Ack) whose ``exception`` field decodes to
            the original :class:`ValueError`, surfacing the actual
            parse-failure class to the caller rather than an opaque
            gRPC error.
        """

        # Arrange
        async def sample_task():
            return "should_not_execute"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        task_pb = wool_task.to_protobuf()
        task_pb.id = "not-a-valid-uuid"
        request = protocol.Request(task=task_pb)

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        nack = responses[0]
        assert nack.HasField("nack")
        assert nack.nack.HasField("exception")
        raised = cloudpickle.loads(nack.nack.exception.dump)
        assert isinstance(raised, ValueError)

    @pytest.mark.asyncio
    async def test_dispatch_with_corrupt_task_callable(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch when the wire-shipped
        task callable bytes cannot be deserialized by cloudpickle.

        Given:
            A dispatch Request whose ``task.callable`` field carries
            corrupt bytes, so :meth:`Task.from_protobuf` raises
            during cloudpickle.loads inside the parse phase
        When:
            The dispatch RPC is invoked with that request
        Then:
            It should reply with exactly one :class:`Nack` response
            (no preceding Ack) whose ``exception`` field decodes to
            the underlying cloudpickle / unpickling error, surfacing
            the actual parse-failure class to the caller.
        """

        # Arrange
        async def sample_task():
            return "should_not_execute"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        task_pb = wool_task.to_protobuf()
        task_pb.callable = b"\x00not a valid pickle stream\x00"
        request = protocol.Request(task=task_pb)

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        nack = responses[0]
        assert nack.HasField("nack")
        assert nack.nack.HasField("exception")
        raised = cloudpickle.loads(nack.nack.exception.dump)
        assert isinstance(raised, Exception)

    @pytest.mark.asyncio
    async def test_dispatch_with_corrupt_context_var_value(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch runs the routine when a
        caller-shipped ``request.context.vars`` entry cannot be
        deserialized, falling back to a fresh empty context and
        emitting a :class:`ContextDecodeWarning`.

        Given:
            A dispatch Request whose ``context.vars`` map carries a
            known var key bound to a corrupt byte payload (not a
            valid pickle stream) — modeling cross-version pickle
            skew or wire corruption of a single var value
        When:
            The dispatch RPC is invoked with that request
        Then:
            The routine still runs and returns its value, a
            :class:`ContextDecodeWarning` is emitted on the worker,
            and the response is delivered normally — context
            propagation is ancillary state and a decode failure here
            does not preempt the primary signal
        """

        # Arrange
        async def sample_task():
            return "routine_ran"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        namespace = f"corrupt_val_{uuid4().hex}"
        var: wool.ContextVar[str] = wool.ContextVar("x", namespace=namespace)
        context_pb = protocol.Context(id=uuid4().hex)
        context_pb.vars.add(
            namespace=var.namespace,
            name=var.name,
            value=b"\x00not a valid pickle stream\x00",
        )
        request = protocol.Request(
            task=wool_task.to_protobuf(),
            context=context_pb,
        )

        # Act
        with pytest.warns(wool.ContextDecodeWarning, match="Failed to deserialize"):
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                responses = [r async for r in stream]

        # Assert
        result_responses = [r for r in responses if r.HasField("result")]
        assert len(result_responses) == 1
        assert cloudpickle.loads(result_responses[0].result.dump) == "routine_ran"

    @pytest.mark.asyncio
    async def test_dispatch_with_malformed_context_id(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch runs the routine when the
        caller's ``request.context.id`` is not a valid hex UUID,
        falling back to a fresh empty context and emitting a
        :class:`ContextDecodeWarning`.

        Given:
            A dispatch Request whose ``context.id`` field is a
            non-hex string (e.g., ``"not-a-uuid"``)
        When:
            The dispatch RPC is invoked with that request
        Then:
            The routine still runs and returns its value, a
            :class:`ContextDecodeWarning` is emitted, and the
            response is delivered normally — malformed wire context
            is treated as ancillary state lost, not a request
            rejection
        """

        # Arrange
        async def sample_task():
            return "routine_ran"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        # carries_state requires a non-empty vars list for
        # from_protobuf to even attempt parsing the id, so seed a
        # ContextVar entry carrying a consumed-token id alongside
        # the malformed Context id.
        bad_ctx = protocol.Context(id="not-a-uuid")
        bad_ctx.vars.add(namespace="", name="", consumed_tokens=[uuid4().hex])
        task_pb = wool_task.to_protobuf()
        request = protocol.Request(task=task_pb, context=bad_ctx)

        # Act
        with pytest.warns(wool.ContextDecodeWarning):
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                responses = [r async for r in stream]

        # Assert
        result_responses = [r for r in responses if r.HasField("result")]
        assert len(result_responses) == 1
        assert cloudpickle.loads(result_responses[0].result.dump) == "routine_ran"

    @pytest.mark.asyncio
    async def test_dispatch_streaming_with_mid_stream_corrupt_context(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch continues an async-generator
        iteration when a mid-stream frame carries a corrupt context,
        emitting a :class:`ContextDecodeWarning` instead of failing
        the dispatch.

        Given:
            An async-generator dispatch where the first ``next``
            frame carries a valid context and yields successfully,
            but the second ``next`` frame carries a state-bearing
            ``context`` whose serialized var payload is corrupt
        When:
            The caller sends the second request and consumes the
            stream
        Then:
            The generator's second yield is delivered as a normal
            result frame and a :class:`ContextDecodeWarning` is
            emitted on the worker — the corrupt mid-stream context
            is treated as ancillary state lost rather than a
            terminal failure
        """

        # Arrange
        async def streamer():
            for i in range(5):
                yield f"value_{i}"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streamer,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        good_next = protocol.Request(
            next=protocol.Void(),
            context=protocol.Context(id=uuid4().hex),
        )
        bad_ctx = protocol.Context(id=uuid4().hex)
        bad_ctx.vars.add(
            namespace="test",
            name="corrupt_key",
            value=b"\x00\x01garbage_not_pickle",
        )
        bad_next = protocol.Request(next=protocol.Void(), context=bad_ctx)

        # Act
        async def drive():
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(first_request)
                ack = await anext(aiter(stream))
                assert ack.HasField("ack")

                await stream.write(good_next)
                first = await anext(aiter(stream))
                assert first.HasField("result")
                assert cloudpickle.loads(first.result.dump) == "value_0"

                await stream.write(bad_next)
                second = await anext(aiter(stream))
                await stream.done_writing()
                return second

        with pytest.warns(wool.ContextDecodeWarning):
            second = await asyncio.wait_for(drive(), timeout=5.0)

        # Assert
        assert second.HasField("result")
        assert cloudpickle.loads(second.result.dump) == "value_1"

    @pytest.mark.asyncio
    async def test_dispatch_streaming_with_unpicklable_worker_mutation(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch delivers the next yield
        when a worker-side snapshot serialization fails between
        iterations, emitting a :class:`ContextDecodeWarning` instead
        of failing the dispatch.

        Given:
            An async-generator routine that, between yields, sets a
            :class:`wool.ContextVar` to a value whose ``__reduce__``
            raises — the wool back-prop snapshot
            (``Context.to_protobuf``) on the next iteration cannot
            serialize the var
        When:
            The caller drives the generator past the unpicklable
            assignment
        Then:
            The next yield is delivered as a normal result frame
            with an empty wire context, and a
            :class:`ContextDecodeWarning` is emitted on the worker —
            the snapshot failure is ancillary state and does not
            preempt the routine's primary signal
        """
        # Arrange
        namespace = f"unpicklable_mut_{uuid4().hex}"
        var = wool.ContextVar("trap", namespace=namespace)

        class _Unpicklable:
            def __reduce__(self):
                raise TypeError("synthetic unpicklable")

        async def streamer():
            yield "first"
            var.set(_Unpicklable())
            yield "second"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streamer,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async def drive():
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(first_request)
                ack = await anext(aiter(stream))
                assert ack.HasField("ack")

                await stream.write(next_request)
                first = await anext(aiter(stream))
                assert first.HasField("result")
                assert cloudpickle.loads(first.result.dump) == "first"

                await stream.write(next_request)
                second = await anext(aiter(stream))
                await stream.done_writing()
                return second

        with pytest.warns(wool.ContextDecodeWarning):
            second = await asyncio.wait_for(drive(), timeout=5.0)

        # Assert
        assert second.HasField("result")
        assert cloudpickle.loads(second.result.dump) == "second"

    @pytest.mark.asyncio
    async def test_dispatch_with_unpicklable_worker_mutation(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch delivers the routine's
        return value on the coroutine path when a worker-side
        snapshot serialization fails, emitting a
        :class:`ContextDecodeWarning` instead of failing the
        dispatch.

        Given:
            A coroutine routine that sets a :class:`wool.ContextVar`
            to a value whose ``__reduce__`` raises before
            returning — the wool back-prop snapshot
            (``Context.to_protobuf``) in the done-callback cannot
            serialize the post-run state
        When:
            The caller dispatches the routine
        Then:
            The routine's return value is delivered as a normal
            result frame with an empty wire context, and a
            :class:`ContextDecodeWarning` is emitted on the worker
        """
        # Arrange
        namespace = f"unpicklable_coro_{uuid4().hex}"
        var = wool.ContextVar("trap", namespace=namespace)

        class _Unpicklable:
            def __reduce__(self):
                raise TypeError("synthetic unpicklable")

        async def coroutine():
            var.set(_Unpicklable())
            return "ok"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=coroutine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async def drive():
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                return [r async for r in stream]

        with pytest.warns(wool.ContextDecodeWarning):
            responses = await asyncio.wait_for(drive(), timeout=5.0)

        # Assert
        result_responses = [r for r in responses if r.HasField("result")]
        assert len(result_responses) == 1
        assert cloudpickle.loads(result_responses[0].result.dump) == "ok"

    @pytest.mark.asyncio
    async def test_dispatch_with_routine_raise_and_unpicklable_mutation(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch ships the routine
        exception bare with the worker-side snapshot failure
        attached as PEP 678 ``__notes__`` and a structured
        ``__wool_context_warnings__`` attribute when both occur
        in the same done-callback on the coroutine path under
        strict mode.

        Given:
            A coroutine routine that sets a :class:`wool.ContextVar`
            to a value whose ``__reduce__`` raises and then itself
            raises an unrelated exception, with the worker-side
            warnings filter promoting :class:`ContextDecodeWarning`
            to an exception — both the routine's failure and the
            wool back-prop snapshot's failure occur in the same
            done-callback
        When:
            The caller dispatches the routine
        Then:
            The dispatch ships the routine exception's type bare
            (so the caller's existing ``except RoutineError``
            keeps catching), with the snapshot encode failure
            attached via PEP 678 ``__notes__`` (visible in
            tracebacks) and a ``__wool_context_warnings__``
            attribute (programmatic access to the
            :class:`ContextDecodeWarning` peers naming the
            offending var)
        """
        import warnings as _warnings

        # Arrange
        namespace = f"unpicklable_chain_{uuid4().hex}"
        var = wool.ContextVar("trap", namespace=namespace)

        class _Unpicklable:
            def __reduce__(self):
                raise TypeError("synthetic unpicklable")

        async def coroutine():
            var.set(_Unpicklable())
            raise ValueError("routine failure")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=coroutine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async def drive():
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                return [r async for r in stream]

        with _warnings.catch_warnings():
            _warnings.simplefilter("error", category=wool.ContextDecodeWarning)
            responses = await asyncio.wait_for(drive(), timeout=5.0)

        # Assert
        exc_responses = [r for r in responses if r.HasField("exception")]
        assert len(exc_responses) == 1
        raised = cloudpickle.loads(exc_responses[0].exception.dump)

        # The routine exception type is preserved; caller's
        # existing ``except ValueError`` keeps catching.
        assert isinstance(raised, ValueError), (
            f"wire must ship the routine's ValueError bare, not "
            f"a wrapper group — observed {type(raised).__name__}"
        )
        assert "routine failure" in str(raised)

        # PEP 678 notes carry the warning(s) for traceback
        # diagnostic.
        assert hasattr(raised, "__notes__")
        notes_text = "\n".join(raised.__notes__)
        assert "synthetic unpicklable" in notes_text, (
            f"snapshot failure must appear in __notes__; observed: {raised.__notes__}"
        )

        # __wool_context_warnings__ provides structured access.
        warnings = raised.__wool_context_warnings__
        snapshot_warnings = [
            w
            for w in warnings
            if isinstance(w, wool.ContextDecodeWarning)
            and "synthetic unpicklable" in str(w)
        ]
        assert len(snapshot_warnings) == 1, (
            "snapshot ContextDecodeWarning must appear in __wool_context_warnings__"
        )

    @pytest.mark.asyncio
    async def test_dispatch_with_attribute_rejecting_routine_exception_under_strict_mode(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch ships a routine
        exception whose class rejects arbitrary attribute writes
        unchanged under strict mode.

        Regression test for A4. Pre-fix,
        ``e.__wool_context_warnings__ = warnings`` raised
        :class:`AttributeError` for exception classes that reject
        arbitrary attribute writes (e.g., overridden
        ``__setattr__``, or layouts that disable ``__dict__``) —
        converting the routine's primary signal into a stray
        :class:`AttributeError` shipped to the caller. Post-fix,
        the assignment is best-effort: PEP 678 ``__notes__``
        carries the warnings (always available) and the structured
        attribute is silently skipped when the exception class
        does not support the write.

        Given:
            A coroutine routine that sets a :class:`wool.ContextVar`
            to a value whose ``__reduce__`` raises (forcing the
            wool snapshot encode failure path) and then raises an
            exception whose ``__setattr__`` rejects arbitrary
            attribute writes, with worker-side strict mode
            promoting :class:`wool.ContextDecodeWarning` to an
            exception.
        When:
            The caller dispatches the routine.
        Then:
            The wire ships the routine exception type unchanged
            with PEP 678 ``__notes__`` carrying the warnings;
            ``__wool_context_warnings__`` is not present (the
            best-effort attribute set silently skipped). Pre-fix
            the wire shipped an :class:`AttributeError` from the
            failed attribute write instead.
        """
        import warnings as _warnings

        # Arrange
        namespace = f"slotted_chain_{uuid4().hex}"
        var = wool.ContextVar("trap", namespace=namespace)

        class _Unpicklable:
            def __reduce__(self):
                raise TypeError("synthetic unpicklable")

        async def coroutine():
            var.set(_Unpicklable())
            raise _AttributeRejectingRoutineError("attribute-rejecting routine failure")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=coroutine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async def drive():
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                return [r async for r in stream]

        with _warnings.catch_warnings():
            _warnings.simplefilter("error", category=wool.ContextDecodeWarning)
            responses = await asyncio.wait_for(drive(), timeout=5.0)

        # Assert
        exc_responses = [r for r in responses if r.HasField("exception")]
        assert len(exc_responses) == 1
        raised = cloudpickle.loads(exc_responses[0].exception.dump)

        # The attribute-rejecting routine exception type is
        # preserved.
        assert isinstance(raised, _AttributeRejectingRoutineError), (
            f"wire must ship the routine's "
            f"_AttributeRejectingRoutineError unchanged, not an "
            f"AttributeError from the failed side-channel write — "
            f"observed {type(raised).__name__}"
        )
        assert "attribute-rejecting routine failure" in str(raised)

        # __notes__ carries the warning (always available — it's
        # part of BaseException's API regardless of __slots__).
        notes_text = "\n".join(getattr(raised, "__notes__", []))
        assert "synthetic unpicklable" in notes_text, (
            f"snapshot failure must appear in __notes__; observed: "
            f"{getattr(raised, '__notes__', None)}"
        )

        # __wool_context_warnings__ is not set on
        # attribute-rejecting exception types — the best-effort
        # attribute write was skipped.
        assert not hasattr(raised, "__wool_context_warnings__"), (
            "attribute-rejecting exception classes cannot accept "
            "arbitrary attribute writes; the best-effort set "
            "should be skipped"
        )

    @pytest.mark.asyncio
    async def test_dispatch_streaming_with_routine_raise_and_unpicklable_mutation(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch ships the routine
        exception bare with the worker-side snapshot failure
        attached as PEP 678 ``__notes__`` and
        ``__wool_context_warnings__`` when both occur in the
        same iteration on the streaming path under strict mode.
        Symmetric with the coroutine path's contract.

        Given:
            An async-generator routine that yields once
            successfully, then sets a :class:`wool.ContextVar`
            to a value whose ``__reduce__`` raises and itself
            raises an unrelated exception on the next iteration,
            with the worker-side warnings filter promoting
            :class:`ContextDecodeWarning` to an exception — both
            the routine's failure and the back-prop snapshot's
            failure occur in the same iteration
        When:
            The caller drives the generator past the yielded
            value and into the failing iteration
        Then:
            The dispatch ships the routine exception type bare
            with the snapshot encode failure attached via
            PEP 678 ``__notes__`` and a structured
            ``__wool_context_warnings__`` attribute, symmetric
            with the coroutine path's contract
        """
        import warnings as _warnings

        # Arrange
        namespace = f"unpicklable_stream_chain_{uuid4().hex}"
        var = wool.ContextVar("trap", namespace=namespace)

        class _Unpicklable:
            def __reduce__(self):
                raise TypeError("synthetic unpicklable")

        async def streamer():
            yield "first"
            var.set(_Unpicklable())
            raise ValueError("routine failure")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streamer,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async def drive():
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(first_request)
                ack = await anext(aiter(stream))
                assert ack.HasField("ack")

                await stream.write(next_request)
                first = await anext(aiter(stream))
                assert first.HasField("result")
                assert cloudpickle.loads(first.result.dump) == "first"

                await stream.write(next_request)
                await stream.done_writing()
                return [r async for r in stream]

        with _warnings.catch_warnings():
            _warnings.simplefilter("error", category=wool.ContextDecodeWarning)
            responses = await asyncio.wait_for(drive(), timeout=5.0)

        # Assert
        exc_responses = [r for r in responses if r.HasField("exception")]
        assert len(exc_responses) == 1
        raised = cloudpickle.loads(exc_responses[0].exception.dump)

        # The routine exception type is preserved.
        assert isinstance(raised, ValueError), (
            f"wire must ship the routine's ValueError bare, not "
            f"a wrapper group — observed {type(raised).__name__}"
        )
        assert "routine failure" in str(raised)

        # PEP 678 notes carry the warning for traceback diagnostic.
        assert hasattr(raised, "__notes__")
        notes_text = "\n".join(raised.__notes__)
        assert "synthetic unpicklable" in notes_text, (
            f"snapshot failure must appear in __notes__; observed: {raised.__notes__}"
        )

        # __wool_context_warnings__ provides structured access.
        warnings = raised.__wool_context_warnings__
        snapshot_warnings = [
            w
            for w in warnings
            if isinstance(w, wool.ContextDecodeWarning)
            and "synthetic unpicklable" in str(w)
        ]
        assert len(snapshot_warnings) == 1, (
            "snapshot ContextDecodeWarning must appear in __wool_context_warnings__"
        )

    @pytest.mark.asyncio
    async def test_dispatch_streaming_when_update_raises(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService` dispatch surfaces unhandled
        iteration-body errors as a terminal error rather than hanging
        the streaming dispatch.

        Given:
            An async-generator dispatch where the second ``next``
            frame carries a state-bearing context, but
            ``Context.update`` is patched to raise on invocation —
            the unprotected merge that would otherwise strand the
            worker task
        When:
            The caller sends the second request and consumes the
            stream
        Then:
            The dispatch must terminate within the asyncio timeout
            window with the synthetic error surfaced as an exception
            Response — the iteration-body catch-all guarantees
            that any exception escaping the precise handlers is
            still pushed to the result queue
        """
        # Arrange
        from wool.runtime.context import Context

        async def streamer():
            for i in range(5):
                yield f"value_{i}"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streamer,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        good_next = protocol.Request(
            next=protocol.Void(),
            context=protocol.Context(id=uuid4().hex),
        )
        # State-bearing context (carries_state True) so the worker
        # invokes update on receive.
        bad_ctx = protocol.Context(id=uuid4().hex)
        bad_ctx.vars.add(namespace="", name="", consumed_tokens=[uuid4().hex])
        bad_next = protocol.Request(next=protocol.Void(), context=bad_ctx)

        mocker.patch.object(
            Context,
            "update",
            side_effect=RuntimeError("synthetic update failure"),
        )

        # Act
        async def drive():
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(first_request)
                ack = await anext(aiter(stream))
                assert ack.HasField("ack")

                await stream.write(good_next)
                first = await anext(aiter(stream))
                assert first.HasField("result")
                assert cloudpickle.loads(first.result.dump) == "value_0"

                await stream.write(bad_next)
                await stream.done_writing()
                return [r async for r in stream]

        responses = await asyncio.wait_for(drive(), timeout=5.0)

        # Assert
        assert any(r.HasField("exception") for r in responses)
        exc_response = next(r for r in responses if r.HasField("exception"))
        raised = cloudpickle.loads(exc_response.exception.dump)
        assert isinstance(raised, RuntimeError)
        assert "synthetic update failure" in str(raised)

    @pytest.mark.asyncio
    async def test_dispatch_streaming_surfaces_pre_loop_setup_failure(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService` streaming dispatch surfaces a
        worker-task setup failure as a terminal exception frame
        rather than hanging the caller.

        Given:
            An async-generator dispatch where the worker task's
            ``RuntimeContext.__enter__`` is patched to raise — the
            failure precedes the worker's request-queue loop, so the
            worker pushes nothing to the result queue
        When:
            The caller sends the task and a follow-up ``next`` frame
        Then:
            The dispatch terminates within the asyncio timeout window
            with the synthetic exception surfaced as a Response —
            the main loop's ``result_queue.get()`` is unblocked by
            the done-callback, and the finally yields a terminal
            outcome built from the worker's exception rather than
            silently swallowing it.
        """
        # Arrange
        from wool.runtime.context import RuntimeContext

        async def streamer():
            yield "unreachable"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streamer,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(
            next=protocol.Void(),
            context=protocol.Context(id=uuid4().hex),
        )

        mocker.patch.object(
            RuntimeContext,
            "__enter__",
            side_effect=RuntimeError("synthetic pre-loop failure"),
        )

        # Act
        async def drive():
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(first_request)
                ack = await anext(aiter(stream))
                assert ack.HasField("ack")

                await stream.write(next_request)
                await stream.done_writing()
                return [r async for r in stream]

        responses = await asyncio.wait_for(drive(), timeout=5.0)

        # Assert
        assert any(r.HasField("exception") for r in responses), (
            "Pre-loop worker failure must surface as an exception "
            "frame rather than a silent stream end"
        )
        exc_response = next(r for r in responses if r.HasField("exception"))
        raised = cloudpickle.loads(exc_response.exception.dump)
        assert isinstance(raised, RuntimeError)
        assert "synthetic pre-loop failure" in str(raised)

    @pytest.mark.asyncio
    async def test_dispatch_streaming_with_teardown_failure_after_completion(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` streaming dispatch when the
        async generator's teardown raises after the primary signal
        has already reached the caller.

        Given:
            An async-generator dispatch whose generator's ``finally``
            block raises a non-cancellation exception, so
            ``gen.aclose()`` re-raises during worker teardown after
            the routine yielded its value
        When:
            The caller consumes the routine's output, closes the
            stream, and exhausts the response iterator
        Then:
            It should deliver the routine's yielded value unmodified
            and append no trailing exception response — the gRPC
            stream is not double-framed when the primary signal
            already reached the caller.
        """

        # Arrange
        async def streamer():
            try:
                yield "outcome_a"
            finally:
                raise RuntimeError("synthetic teardown failure")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streamer,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(
            next=protocol.Void(),
            context=protocol.Context(id=uuid4().hex),
        )

        # Act
        async def drive():
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(first_request)
                ack = await anext(aiter(stream))
                assert ack.HasField("ack")

                await stream.write(next_request)
                first = await anext(aiter(stream))
                assert first.HasField("result")
                assert cloudpickle.loads(first.result.dump) == "outcome_a"

                await stream.done_writing()
                return [r async for r in stream]

        remaining = await asyncio.wait_for(drive(), timeout=5.0)

        # Assert
        assert len(remaining) == 0, (
            "Teardown failure must not produce a trailing exception "
            "frame when the primary signal already streamed"
        )

    @pytest.mark.asyncio
    async def test_dispatch_while_stopping(
        self, service_fixture, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService` dispatch aborts when stopping.

        Given:
            A :class:`WorkerService` with an active task, transitioning to stopping state
        When:
            stop is called and another dispatch RPC is attempted
        Then:
            It should abort the new dispatch with UNAVAILABLE status
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Initiate stop (service enters stopping state)
            # Use wait=0 to immediately cancel tasks, but we'll check before tasks finish
            stop_task = asyncio.ensure_future(stub.stop(protocol.StopRequest(timeout=5)))

            await asyncio.wait_for(service.stopping.wait(), 1)
            assert not service.stopped.is_set()

            # Create a new task to dispatch
            async def another_task():
                return "should_not_execute"

            mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id-2")

            wool_task = Task(
                id=uuid4(),
                callable=another_task,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )

            request = protocol.Request(task=wool_task.to_protobuf())

            # Act & assert
            with pytest.raises(grpc.RpcError) as exc_info:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                async for _ in stream:
                    pass

            assert exc_info.value.code() == StatusCode.UNAVAILABLE

            # Cleanup: let the original task complete so stop can finish
            event.set()
            await stop_task

    @pytest.mark.asyncio
    async def test_dispatch_with_stop_arriving_between_entry_gate_and_tracking(
        self,
        grpc_aio_stub,
        grpc_servicer,
        mock_worker_proxy_cache,
        mocker: MockerFixture,
    ):
        """Test :class:`WorkerService.dispatch` aborts ``UNAVAILABLE`` when
        the ``_stopping`` event is set after the entry-gate check but before
        the session is registered in the docket.

        Regression test for the ``_tracked`` check-to-register race window.
        ``WorkerService.dispatch`` checks ``_stopping`` on entry and again
        on docket registration; without the second check, a concurrent
        :meth:`_stop` between the gate and registration would admit a
        session that :meth:`_preempt` never sees, leaving it to be torn
        down indirectly by loop-pool teardown rather than the explicit
        cancel path.

        Given:
            A :class:`WorkerService` whose ``_stopping.is_set`` returns
            ``False`` on the entry-gate check and ``True`` on the
            ``_tracked`` check, simulating a stop arrival in the race
            window
        When:
            A dispatch RPC arrives and progresses past the entry gate
        Then:
            ``_tracked`` should abort the RPC with ``UNAVAILABLE`` and
            cancel the session through the explicit path — the caller
            observes ``RpcError(UNAVAILABLE)``.
        """
        # Arrange — replace ``_stopping.is_set`` with a side-effect that
        # returns False on the first call (the dispatch handler's entry
        # gate at line 353) and True on subsequent calls (the
        # ``_tracked`` CM's check at line 785). The dispatch must
        # progress past the gate to reach ``_tracked`` so the racy
        # arm fires.
        is_set_calls = iter([False, True, True, True, True, True])
        mocker.patch.object(
            grpc_servicer._stopping, "is_set", side_effect=lambda: next(is_set_calls)
        )

        async def sample_task():
            return "should_not_execute"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        # Act & assert
        async with grpc_aio_stub() as stub:
            with pytest.raises(grpc.RpcError) as exc_info:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                async for _ in stream:
                    pass

        # The ``_tracked`` racy arm aborted with UNAVAILABLE — caller
        # observes the same status code as the regular stopping path,
        # so existing client retry logic does not need a special case.
        assert exc_info.value.code() == StatusCode.UNAVAILABLE

    @pytest.mark.asyncio
    async def test_dispatch_while_stopped(self, service_fixture, mocker: MockerFixture):
        """Test :class:`WorkerService` dispatch aborts when stopped.

        Given:
            A :class:`WorkerService` that has been stopped
        When:
            dispatch RPC is called with a task request
        Then:
            It should abort with UNAVAILABLE status
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Complete the task and stop the service
            event.set()
            await stub.stop(protocol.StopRequest(timeout=5))

            # Assert service is stopped
            assert service.stopping.is_set()
            assert service.stopped.is_set()

            # Create a new task to dispatch
            async def another_task():
                return "should_not_execute"

            mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id-2")

            wool_task = Task(
                id=uuid4(),
                callable=another_task,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )

            request = protocol.Request(task=wool_task.to_protobuf())

            # Act & assert
            with pytest.raises(grpc.RpcError) as exc_info:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                async for _ in stream:
                    pass

            assert exc_info.value.code() == StatusCode.UNAVAILABLE

    @pytest.mark.asyncio
    async def test_dispatch_with_sync_callable(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` dispatch when the task's
        callable is a plain synchronous function (not a coroutine
        function or async-generator function).

        Given:
            A dispatch Request whose task's callable is a synchronous
            function — unschedulable on the worker loop
        When:
            The dispatch RPC is invoked with that request
        Then:
            It should reply with exactly one :class:`Nack` response
            (no preceding Ack) whose ``exception`` field decodes to
            a :class:`ValueError` describing the routine-type
            violation.
        """

        # Arrange
        def sync_function():
            return "sync_result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sync_function,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        nack = responses[0]
        assert nack.HasField("nack")
        assert nack.nack.HasField("exception")
        raised = cloudpickle.loads(nack.nack.exception.dump)
        assert isinstance(raised, ValueError)
        assert "coroutine function or async generator function" in str(raised)

    @pytest.mark.asyncio
    async def test_stop_and_cancel(
        self,
        grpc_aio_stub,
        grpc_servicer,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
    ):
        """Test :class:`WorkerService` stop pre-empts an active
        coroutine routine.

        Verifies the operator-preempt contract on the routine side
        via a side-channel :class:`threading.Event`. In production,
        the worker subprocess exits after stop and the gRPC
        connection drops — callers do not observe a terminal
        ``CancelledError`` wire frame, they observe an
        :class:`RpcError` (transport-closed). Asserting on a wire
        frame after stop tests an in-process-only artifact (the
        gRPC server stays alive in the fixture); asserting on the
        routine's own observation of cancellation is the
        production-realistic check.

        Given:
            A running :class:`WorkerService` with an active
            coroutine awaiting a long sleep
        When:
            stop RPC is called with a timeout of 0
        Then:
            The routine should observe :class:`asyncio.CancelledError`
            within the test's budget — operator-preempt cancels the
            worker driver task on its loop, propagating cancellation
            into the routine's :func:`asyncio.sleep`. The service
            should signal stopped state and call
            :meth:`proxy_pool.clear`.
        """
        global _stop_cancellation_observed, _stop_routine_started
        _stop_cancellation_observed = threading.Event()
        _stop_routine_started = threading.Event()
        try:
            mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
            wool_task = Task(
                id=uuid4(),
                callable=_stop_long_coroutine,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )
            request = protocol.Request(task=wool_task.to_protobuf())

            # Act
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(request)
                ack = await anext(aiter(stream))
                assert ack.HasField("ack")

                # Wait for the routine to actually start before
                # sending stop. Without this barrier the test races
                # the dispatch session's lazy worker scheduling: on
                # slower Python versions/CI runners, stop can land
                # before ``_worker_task`` is created, so
                # ``session.cancel()`` has nothing to cancel and
                # the routine never observes :class:`CancelledError`.
                loop = asyncio.get_running_loop()
                started = await loop.run_in_executor(
                    None, _stop_routine_started.wait, 10.0
                )
                assert started, (
                    "routine must start within 10s of dispatch; "
                    "if it does not, the test is racing the "
                    "dispatch session's lazy worker scheduling"
                )

                stop_request = protocol.StopRequest(timeout=0)
                stop_result = await stub.stop(stop_request)

                # Off-loop wait so the main loop can pump
                # cross-loop cancellation work while we wait.
                observed = await loop.run_in_executor(
                    None, _stop_cancellation_observed.wait, 10.0
                )

            # Assert
            assert observed, (
                "operator-preempt with timeout=0 must cancel "
                "the routine within 10s; the routine signals via "
                "_stop_cancellation_observed when its sleep "
                "raises CancelledError"
            )
            assert isinstance(stop_result, protocol.Void)
            assert grpc_servicer.stopping.is_set()
            assert grpc_servicer.stopped.is_set()
            mock_worker_proxy_cache.clear.assert_called_once()
        finally:
            _stop_cancellation_observed = None
            _stop_routine_started = None

    @pytest.mark.asyncio
    async def test_stop_and_cancel_streaming_routine(
        self,
        grpc_aio_stub,
        grpc_servicer,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
    ):
        """Test :class:`WorkerService` stop pre-empts an active
        async-generator routine mid-stream.

        Verifies the operator-preempt contract on the routine side
        via a side-channel :class:`threading.Event`. See the
        coroutine variant's docstring for why we assert on the
        routine's observation of cancellation rather than on a
        terminal wire frame.

        Given:
            A running :class:`WorkerService` with an active
            async-generator task suspended between yields
        When:
            stop RPC is called with a timeout of 0
        Then:
            The routine should observe :class:`asyncio.CancelledError`
            within the test's budget — operator-preempt cancels the
            worker driver task on its loop, propagating cancellation
            into the routine's :func:`asyncio.sleep` between yields.
        """
        global _stop_cancellation_observed, _stop_routine_started
        _stop_cancellation_observed = threading.Event()
        # The streaming variant does not need to wait on
        # ``_stop_routine_started`` — consuming the first yielded
        # value proves the routine is running — but the event is
        # initialized here so the shared module-level routine can
        # signal it unconditionally without an ``if`` branch in the
        # routine body.
        _stop_routine_started = threading.Event()
        try:
            mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
            wool_task = Task(
                id=uuid4(),
                callable=_stop_streaming_routine,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )
            request = protocol.Request(task=wool_task.to_protobuf())

            # Act
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(request)

                # Consume Ack and the first yielded value so the
                # worker is mid-`asyncio.sleep(30)` when we stop.
                ack = await anext(aiter(stream))
                assert ack.HasField("ack")
                await stream.write(protocol.Request(next=protocol.Void()))
                first = await anext(aiter(stream))
                assert first.HasField("result")
                assert cloudpickle.loads(first.result.dump) == 0

                stop_request = protocol.StopRequest(timeout=0)
                await stub.stop(stop_request)

                loop = asyncio.get_running_loop()
                observed = await loop.run_in_executor(
                    None, _stop_cancellation_observed.wait, 10.0
                )

            # Assert
            assert observed, (
                "operator-preempt with timeout=0 must cancel the "
                "streaming routine mid-stream within 10s; the "
                "routine signals via _stop_cancellation_observed "
                "when its sleep raises CancelledError"
            )
        finally:
            _stop_cancellation_observed = None
            _stop_routine_started = None

    def test_destroy_worker_loop_thread_closes_its_own_loop(self):
        """Test the worker thread closes its loop after
        ``run_forever`` returns.

        Regression test for A2. Pre-fix,
        :meth:`_destroy_worker_loop` called ``loop.close()`` from
        the caller's thread after a 5s join, racing the worker
        thread's still-active ``run_forever`` and raising
        ``RuntimeError("Cannot close a running event loop")``
        whenever the join timed out. Post-fix, the close moved
        into the worker thread's target (``_run_then_close``)
        where ``run_forever`` has already returned by the time
        ``loop.close`` runs — the race is structurally
        impossible.

        Given:
            A worker loop+thread created by
            :meth:`_create_worker_loop`.
        When:
            :meth:`_destroy_worker_loop` schedules shutdown with
            a generous ``_stop_timeout`` so the join completes
            cleanly.
        Then:
            The thread exits and the loop is closed — closed by
            the worker thread's own ``finally`` clause, not by
            the caller-side finalizer.
        """
        service = WorkerService()
        loop, thread = WorkerService._create_worker_loop(key=None)
        service._stop_timeout = 5.0

        service._destroy_worker_loop((loop, thread))

        thread.join(timeout=5)
        assert not thread.is_alive(), (
            "worker thread should exit shortly after "
            "_destroy_worker_loop schedules shutdown"
        )
        assert loop.is_closed(), (
            "loop should be closed by the worker thread's target "
            "(_run_then_close finally clause), not by the "
            "caller-side _destroy_worker_loop"
        )

    def test_destroy_worker_loop_does_not_raise_when_shutdown_hangs(self):
        """Test :meth:`_destroy_worker_loop` returns cleanly even
        when the worker loop's shutdown cannot complete within
        the join budget.

        Regression test for A2. Pre-fix, a task that swallowed
        :class:`asyncio.CancelledError` made ``_shutdown``'s
        gather hang indefinitely; ``thread.join(timeout=5)``
        timed out, ``loop.close()`` then ran on a still-active
        loop and raised
        ``RuntimeError("Cannot close a running event loop")``.
        Post-fix, the join budget comes from the StopRequest's
        timeout (``self._stop_timeout``), the close happens on
        the worker thread (after ``run_forever`` returns), and
        ``_destroy_worker_loop`` is a non-blocking schedule when
        ``_stop_timeout=0``.

        Given:
            A worker loop hosting a task that swallows
            :class:`asyncio.CancelledError` indefinitely (so
            ``_shutdown``'s gather never completes), and a
            ``_stop_timeout`` of 0 (no synchronous wait).
        When:
            :meth:`_destroy_worker_loop` is called.
        Then:
            It should not raise. The daemon thread continues
            past the call; process exit reaps it.
        """
        service = WorkerService()
        loop, thread = WorkerService._create_worker_loop(key=None)

        async def _cancellation_swallower():
            while True:
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    # Defy cancellation — _shutdown's gather
                    # will never complete, exercising the
                    # timed-out-join branch.
                    pass

        asyncio.run_coroutine_threadsafe(_cancellation_swallower(), loop)

        service._stop_timeout = 0
        # Pre-fix: would join for 5s, then raise RuntimeError on
        # ``loop.close()``. Post-fix: schedules shutdown,
        # returns without joining (timeout=0), no close call on
        # this side.
        service._destroy_worker_loop((loop, thread))
        # If we reached here without raising, the regression
        # is fixed. The thread is still alive because the
        # swallower task keeps the loop running; daemon=True
        # reaps it at process exit.

    @pytest.mark.asyncio
    async def test_stop_and_wait(
        self,
        grpc_aio_stub,
        grpc_servicer,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
    ):
        """Test :class:`WorkerService` stop method gracefully shuts down.

        Given:
            A running :class:`WorkerService` with active tasks
        When:
            stop RPC is called with a positive timeout ("wait")
        Then:
            It should await its tasks, signal stopped state and call
            proxy_pool.clear()
        """

        # Arrange
        async def quick_task():
            return "completed"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=quick_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            # Start the task dispatch stream
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            async for response in stream:
                assert response.HasField("ack")
                break

            # Stop with 5 second timeout to allow tasks to complete
            stop_request = protocol.StopRequest(timeout=5)
            stop_result = await stub.stop(stop_request)

            # Assert
            async for response in stream:
                assert response.HasField("result")
                result = cloudpickle.loads(response.result.dump)
                assert result == "completed"
                break

            assert isinstance(stop_result, protocol.Void)
            assert grpc_servicer.stopping.is_set()
            assert grpc_servicer.stopped.is_set()
            mock_worker_proxy_cache.clear.assert_called_once()

    @pytest.mark.asyncio
    async def test_dispatch_task_that_self_cancels(
        self,
        grpc_aio_stub,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
    ):
        """Test :class:`WorkerService` dispatch handles task that cancels itself.

        Given:
            A gRPC :class:`WorkerService` that is not stopping or stopped
        When:
            Dispatch RPC is called with a task that cancels itself on the worker loop
        Then:
            It should return an exception response containing a CancelledError
        """

        # Arrange
        async def self_cancelling_task():
            asyncio.current_task().cancel()
            await asyncio.sleep(0)
            return "should_not_return"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=self_cancelling_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("exception")
        exception = cloudpickle.loads(response.exception.dump)
        assert isinstance(exception, asyncio.CancelledError)

    @pytest.mark.asyncio
    async def test_dispatch_client_cancellation_propagates_to_routine(
        self,
        grpc_aio_stub,
        mock_worker_proxy_cache,
    ):
        """Test :meth:`WorkerService.dispatch` cancels the worker
        routine when the client cancels mid-stream.

        Regression test for A1. Pre-fix,
        :meth:`DispatchSession.cancel` only set ``_cancelled = True``
        and pushed ``_EOS`` on the response queue; the worker
        driver task itself was never cancelled. A routine
        mid-``_step`` (e.g. ``await asyncio.sleep(...)``) ran to
        natural completion regardless of whether the caller had
        gone away. Post-fix, :meth:`cancel` schedules
        ``self._worker_task.cancel`` on the worker loop, so a
        compute-bound or sleeping routine receives a
        :class:`asyncio.CancelledError` and unwinds rather than
        holding the worker until shutdown.

        Given:
            A dispatched coroutine routine sleeping for 30
            seconds — long enough that the test will have given
            up and asserted before it could complete naturally.
            The routine signals observation of
            :class:`asyncio.CancelledError` via a cross-loop
            ``threading.Event``.
        When:
            The gRPC client cancels the dispatch stream while the
            routine is mid-``await asyncio.sleep``.
        Then:
            The routine observes :class:`asyncio.CancelledError`
            within a short timeout — pre-fix this assertion timed
            out because :meth:`cancel` left the worker driver
            task running.
        """
        global _a1_cancellation_observed
        _a1_cancellation_observed = threading.Event()
        try:
            mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
            wool_task = Task(
                id=uuid4(),
                callable=_a1_long_routine,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )
            request = protocol.Request(task=wool_task.to_protobuf())

            # Act
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(request)
                ack = await anext(aiter(stream))
                assert ack.HasField("ack")

                # Client-side cancel — simulates a caller that
                # has given up (network drop, deadline, explicit
                # cancel). The server-side dispatch generator's
                # ``async for response in handler`` raises
                # CancelledError, the finally calls
                # handler.cancel(), and (post-fix) the worker
                # driver task is cancelled — propagating
                # CancelledError into the routine's ``sleep``.
                stream.cancel()

                # Wait for the routine to observe
                # CancelledError. Off-loop wait so we don't block
                # the main loop pumping gRPC's cleanup. The budget
                # has to absorb the full cross-loop propagation
                # chain (client cancel → gRPC server-side handler
                # task cancel → main-loop `await response_queue.get`
                # raises → handler's except clause runs
                # `session.cancel()` → cross-loop
                # `call_soon_threadsafe(worker_task.cancel)` →
                # worker loop wakes and cancels the routine task →
                # `asyncio.sleep` raises). The chain is sub-second
                # on a quiescent loopback, but heavily-loaded shared
                # CI runners can take an order of magnitude longer
                # to deliver each hop. 10s is generous enough to
                # absorb that variability while still failing fast
                # if the chain is actually broken (regression would
                # see the routine sleep the full 30s).
                loop = asyncio.get_running_loop()
                observed = await loop.run_in_executor(
                    None, _a1_cancellation_observed.wait, 10.0
                )

            # Assert
            assert observed, (
                "Expected the routine to observe "
                "asyncio.CancelledError within 10s of "
                "stream.cancel(); pre-fix the routine slept for "
                "the full 30s because DispatchSession.cancel only set "
                "a flag and the worker driver task was never "
                "cancelled."
            )
        finally:
            _a1_cancellation_observed = None

    @pytest.mark.asyncio
    async def test_stop_timeout_then_cancel(
        self, service_fixture, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` stop cancels tasks after timeout expires.

        Given:
            A :class:`WorkerService` with an active task that outlasts the stop timeout
        When:
            stop RPC is called with a small positive timeout
        Then:
            It should wait for the timeout, then cancel remaining tasks and signal
            stopped state
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Act - stop with a very short timeout; the controllable task
            # won't complete because the event is never set in time
            stop_request = protocol.StopRequest(timeout=0.05)
            stop_result = await stub.stop(stop_request)

            # Assert
            assert isinstance(stop_result, protocol.Void)
            assert service.stopping.is_set()
            assert service.stopped.is_set()

    @pytest.mark.asyncio
    async def test_stop_while_idle(
        self, grpc_aio_stub, grpc_servicer, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` stop method gracefully shuts down.

        Given:
            A running :class:`WorkerService` with no active tasks
        When:
            stop RPC is called
        Then:
            It should signal stopped state and call proxy_pool.clear()
        """
        # Arrange
        stop_request = protocol.StopRequest(timeout=10)

        # Act
        async with grpc_aio_stub() as stub:
            stop_result = await asyncio.wait_for(stub.stop(stop_request), 1)

        # Assert - Verify behavior through public API
        assert isinstance(stop_result, protocol.Void)
        assert grpc_servicer.stopping.is_set()
        assert grpc_servicer.stopped.is_set()
        # Assert proxy_pool.clear() was called
        mock_worker_proxy_cache.clear.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_while_stopping(self, service_fixture, mock_worker_proxy_cache):
        """Test :class:`WorkerService` stop is idempotent.

        Given:
            A :class:`WorkerService` that is already stopping
        When:
            stop RPC is called again
        Then:
            It should return immediately without error
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Initiate stop (service enters stopping state)
            stop_task = asyncio.ensure_future(stub.stop(protocol.StopRequest(timeout=5)))

            # Wait for service to enter stopping state
            await asyncio.wait_for(service.stopping.wait(), 1)
            assert not service.stopped.is_set()

            # Act - Call stop again while already stopping
            result = await stub.stop(protocol.StopRequest(timeout=1))

            # Assert - Verify behavior
            assert isinstance(result, protocol.Void)
            assert service.stopping.is_set()

            # Cleanup - let the original task complete so first stop can finish
            event.set()
            await stop_task

            # Assert service is now fully stopped
            assert service.stopped.is_set()

    @pytest.mark.asyncio
    async def test_stop_while_stopped(self, service_fixture, mock_worker_proxy_cache):
        """Test :class:`WorkerService` stop when already stopped.

        Given:
            A :class:`WorkerService` that is already stopped
        When:
            stop RPC is called again
        Then:
            It should return immediately without error
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Complete the task and stop the service
            event.set()
            await stub.stop(protocol.StopRequest(timeout=5))

            # Assert service is fully stopped
            assert service.stopping.is_set()
            assert service.stopped.is_set()

            # Act - Call stop again while already stopped
            result = await stub.stop(protocol.StopRequest(timeout=1))

            # Assert - Verify behavior
            assert isinstance(result, protocol.Void)
            assert service.stopping.is_set()
            assert service.stopped.is_set()

    @pytest.mark.asyncio
    async def test_stop_negative_timeout_waits_indefinitely(
        self, service_fixture, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` stop with negative timeout waits indefinitely.

        Given:
            A :class:`WorkerService` with an active task
        When:
            stop RPC is called with a negative timeout
        Then:
            It should treat the negative timeout as an indefinite wait and stop
            only after the task completes
        """
        # Arrange
        async with service_fixture as (service, event, stub):
            # Initiate stop with negative timeout (treated as indefinite wait)
            stop_task = asyncio.ensure_future(
                stub.stop(protocol.StopRequest(timeout=-1))
            )

            # Wait for service to enter stopping state
            await asyncio.wait_for(service.stopping.wait(), 1)
            assert not service.stopped.is_set()

            # Let the task complete so stop can finish
            event.set()

            # Act & assert
            stop_result = await asyncio.wait_for(stop_task, 5)
            assert isinstance(stop_result, protocol.Void)
            assert service.stopped.is_set()

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_task(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with async generator yields multiple results.

        Given:
            A service receiving task with async generator callable
        When:
            The client sends Request(next=Void()) for each yield
        Then:
            Yields acknowledgment, then multiple result responses from generator in order
        """

        # Arrange
        async def test_generator():
            for i in range(3):
                yield f"gen_value_{i}"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=test_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Send next requests and collect results
            results = []
            for _ in range(3):
                await stream.write(next_request)
                response = await anext(aiter(stream))
                assert response.HasField("result")
                results.append(cloudpickle.loads(response.result.dump))

            # Send one more next to exhaust the generator
            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        assert results == ["gen_value_0", "gen_value_1", "gen_value_2"]
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_streaming_with_dispatch_timeout(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` streaming dispatch restores the
        caller's ``dispatch_timeout`` for every iteration of an
        async-generator routine.

        Given:
            An async-generator :class:`Task` whose
            :class:`RuntimeContext` carries a non-default
            ``dispatch_timeout`` and whose routine reads
            ``wool.runtime.context.dispatch_timeout.get()`` on each
            iteration
        When:
            The caller drives the generator across multiple ``next``
            frames via ``WorkerService.dispatch``
        Then:
            Every yielded value equals the caller-supplied
            ``dispatch_timeout`` — confirming that
            ``_stream_from_worker`` enters ``work_task.runtime_context`` for
            the lifetime of the generator. Regression guard for #176,
            where the prior code dropped the context after the first
            ``__enter__`` and left ``dispatch_timeout`` at its default
            on subsequent frames.
        """
        # Arrange
        from wool.runtime.context import RuntimeContext

        async def capture_timeout():
            from wool.runtime.context import dispatch_timeout

            yield dispatch_timeout.get()
            yield dispatch_timeout.get()
            yield dispatch_timeout.get()

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=capture_timeout,
            args=(),
            kwargs={},
            proxy=mock_proxy,
            runtime_context=RuntimeContext(dispatch_timeout=2.5),
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async def drive():
            async with grpc_aio_stub() as stub:
                stream = stub.dispatch()
                await stream.write(first_request)
                ack = await anext(aiter(stream))
                assert ack.HasField("ack")

                captured: list[float | None] = []
                for _ in range(3):
                    await stream.write(next_request)
                    response = await anext(aiter(stream))
                    assert response.HasField("result")
                    captured.append(cloudpickle.loads(response.result.dump))

                await stream.done_writing()
                async for _ in stream:
                    pass
                return captured

        captured = await asyncio.wait_for(drive(), timeout=5.0)

        # Assert
        assert captured == [2.5, 2.5, 2.5]

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_raises_during_iteration(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with async generator that raises yields exception.

        Given:
            A service with async generator task that raises during iteration
        When:
            The client sends next requests and the generator raises
        Then:
            Yields ack, the first result, then an exception response
            containing the raised exception
        """

        # Arrange
        async def failing_generator():
            yield "first_value"
            raise ValueError("Generator error")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=failing_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # First next: yields "first_value"
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "first_value"

            # Second next: generator raises
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("exception")
            assert response.HasField("context")
            exception = cloudpickle.loads(response.exception.dump)
            assert isinstance(exception, ValueError)
            assert str(exception) == "Generator error"

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_completes_normally(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with async generator completes cleanly.

        Given:
            A service with async generator task
        When:
            The client sends next requests until exhaustion
        Then:
            Yields acknowledgment, all results, then stream ends cleanly
        """

        # Arrange
        async def test_generator():
            for i in range(2):
                yield i

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=test_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Read first result
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == 0

            # Read second result
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == 1

            # Exhaust the generator
            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]
            assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_empty(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with empty async generator.

        Given:
            A service with async generator task yielding zero values
        When:
            The client sends a next request
        Then:
            Yields acknowledgment, then stream ends without result responses
        """

        # Arrange
        async def empty_generator():
            return
            yield  # unreachable, but makes it a generator

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=empty_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Send next - generator is empty, should end
            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_coroutine_for_comparison(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with coroutine task for comparison with async generator.

        Given:
            A service receiving task with coroutine callable
        When:
            dispatch() is called and iterated
        Then:
            Yields acknowledgment, then single result response
        """

        # Arrange
        async def test_coroutine():
            return "coroutine_result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=test_coroutine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 2  # 1 ack + 1 result
        assert responses[0].HasField("ack")
        assert responses[1].HasField("result")
        result = cloudpickle.loads(responses[1].result.dump)
        assert result == "coroutine_result"

    @pytest.mark.asyncio
    async def test_stop_cancels_async_generator_task(
        self,
        grpc_aio_stub,
        grpc_servicer,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
    ):
        """Test :class:`WorkerService` stop cancels an active async generator task.

        Given:
            A running :class:`WorkerService` with an active async generator task
        When:
            stop RPC is called with a timeout of 0
        Then:
            It should cancel the async generator and signal stopped state
        """

        # Arrange
        async def blocking_generator():
            yield "first_value"
            await asyncio.sleep(100)
            yield "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=blocking_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            async for response in stream:
                assert response.HasField("ack")
                break

            # Send next to advance generator and read first result
            await stream.write(next_request)
            async for response in stream:
                assert response.HasField("result")
                assert cloudpickle.loads(response.result.dump) == "first_value"
                break

            # Stop immediately to cancel the generator
            stop_request = protocol.StopRequest(timeout=0)
            stop_result = await stub.stop(stop_request)

            # Assert
            assert isinstance(stop_result, protocol.Void)
            assert grpc_servicer.stopping.is_set()
            assert grpc_servicer.stopped.is_set()

    @pytest.mark.asyncio
    async def test_dispatch_with_version_in_ack(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch returns Ack with protocol version.

        Given:
            A running WorkerService
        When:
            A task is dispatched with a compatible version
        Then:
            The Ack response contains the protocol version.
        """

        # Arrange
        async def sample_task():
            return "test_result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack_response = responses[0]
        assert ack_response.HasField("ack")
        assert ack_response.ack.version == protocol.__version__

    @pytest.mark.asyncio
    async def test_dispatch_with_empty_client_version(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch rejects tasks with empty version field.

        Given:
            A running WorkerService with the version interceptor
        When:
            A task with empty version field is dispatched
        Then:
            The worker responds with a Nack citing unparseable
            version.
        """

        # Arrange
        async def sample_task():
            return "test_result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        request.task.ClearField("version")

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        assert responses[0].HasField("nack")
        assert "Unparseable version" in responses[0].nack.reason

    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        local_major=st.integers(min_value=0, max_value=100),
        client_major=st.integers(min_value=0, max_value=100),
    )
    @pytest.mark.asyncio
    async def test_dispatch_with_incompatible_major_version(
        self,
        grpc_aio_stub,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
        local_major,
        client_major,
    ):
        """Test dispatch yields Nack for incompatible major version.

        Given:
            Two semver-like version strings with different major
            versions
        When:
            A task is dispatched with the client version through the
            version interceptor
        Then:
            The dispatch yields a Nack with a reason citing
            incompatible, empty, or unparseable version.
        """
        assume(local_major != client_major)

        # Arrange
        mocker.patch.object(protocol, "__version__", f"{local_major}.0.0")

        async def sample_task():
            return "should_not_execute"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        # Override version field to simulate incompatible client
        request.task.version = f"{client_major}.0.0"

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        assert responses[0].HasField("nack")
        assert "Incompatible version" in responses[0].nack.reason

    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        major=st.integers(min_value=0, max_value=100),
        local_minor=st.integers(min_value=0, max_value=99),
        client_minor=st.integers(min_value=1, max_value=100),
    )
    @pytest.mark.asyncio
    async def test_dispatch_with_newer_client_same_major(
        self,
        grpc_aio_stub,
        mocker: MockerFixture,
        mock_worker_proxy_cache,
        major,
        local_minor,
        client_minor,
    ):
        """Test dispatch yields Nack when client is newer than worker.

        Given:
            A worker with version X.a.0 and a client with version
            X.b.0 where b > a (same major, no forward compatibility)
        When:
            A task is dispatched through the version interceptor
        Then:
            The dispatch yields a Nack with a reason citing
            incompatible version.
        """
        assume(client_minor > local_minor)

        # Arrange
        mocker.patch.object(protocol, "__version__", f"{major}.{local_minor}.0")

        async def sample_task():
            return "should_not_execute"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        request.task.version = f"{major}.{client_minor}.0"

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        assert responses[0].HasField("nack")
        assert "Incompatible version" in responses[0].nack.reason

    @pytest.mark.asyncio
    async def test_dispatch_with_unparseable_client_version(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch rejects tasks with unparseable version.

        Given:
            A running WorkerService with the version interceptor
        When:
            A task with unparseable version string is dispatched
        Then:
            The worker responds with a Nack citing unparseable
            version.
        """

        # Arrange
        async def sample_task():
            return "should_not_execute"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        request.task.version = "not-a-version"

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        assert responses[0].HasField("nack")
        assert "Unparseable version" in responses[0].nack.reason

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_with_send(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() forwards send requests into async generator via asend().

        Given:
            A service receiving an async generator task
        When:
            The client sends Request(next=...) then Request(send=...)
            frames after the initial Task
        Then:
            The generator receives the sent values via asend() and
            yields responses driven by those values.
        """

        # Arrange
        async def echo_generator():
            value = yield "ready"
            while value is not None:
                value = yield f"echo:{value}"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=echo_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Send next to get first yield ("ready")
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "ready"

            # Send "hello" and read response
            msg = protocol.Request(
                send=protocol.Message(dump=cloudpickle.dumps("hello"))
            )
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "echo:hello"

            # Send "world" and read response
            msg = protocol.Request(
                send=protocol.Message(dump=cloudpickle.dumps("world"))
            )
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "echo:world"

            # Send None to terminate the generator
            msg = protocol.Request(send=protocol.Message(dump=cloudpickle.dumps(None)))
            await stream.write(msg)
            await stream.done_writing()

            # Stream should end cleanly
            remaining = [r async for r in stream]
            assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_send_then_close(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() handles client closing stream after sends.

        Given:
            A service receiving an async generator task with send support
        When:
            The client sends values then closes the write side
        Then:
            The server does not advance the generator further after
            the stream closes.
        """

        # Arrange
        async def counting_generator():
            count = 0
            while count < 5:
                received = yield count
                if received is not None:
                    count = received
                else:
                    count += 1

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=counting_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Send next to get first yield (0)
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == 0

            # Send 2 to jump the counter
            msg = protocol.Request(send=protocol.Message(dump=cloudpickle.dumps(2)))
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == 2

            # Close the write side; server should not advance further
            await stream.done_writing()

            # Stream should end cleanly (no more responses)
            remaining = [r async for r in stream]
            assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_pull_only_async_generator(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with a pull-only async generator (no send type).

        Given:
            A service receiving a pull-only async generator task
        When:
            The client sends Request(next=Void()) for each yield
        Then:
            Each next request produces the next yielded value in order
        """

        # Arrange
        async def pull_only():
            yield "alpha"
            yield "beta"
            yield "gamma"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=pull_only,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Pull each value
            results = []
            for _ in range(3):
                await stream.write(next_request)
                response = await anext(aiter(stream))
                assert response.HasField("result")
                results.append(cloudpickle.loads(response.result.dump))

            # Exhaust
            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        assert results == ["alpha", "beta", "gamma"]
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_pull_only_async_generator_partial_consumption(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with partial consumption of a pull-only async generator.

        Given:
            A service with a pull-only async generator yielding 5 values
        When:
            The client sends 2 next requests then closes the stream
        Then:
            Only 2 result responses are received and the generator is
            cleaned up
        """

        # Arrange
        async def five_values():
            for i in range(5):
                yield i

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=five_values,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Pull only 2 values
            results = []
            for _ in range(2):
                await stream.write(next_request)
                response = await anext(aiter(stream))
                assert response.HasField("result")
                results.append(cloudpickle.loads(response.result.dump))

            # Close the stream without exhausting
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        assert results == [0, 1]
        assert len(remaining) == 0

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_interleaved_next_and_send(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with interleaved next and send requests.

        Given:
            A service with an async generator that accepts send values
        When:
            The client alternates between next and send requests
        Then:
            next advances with asend(None), send advances with the
            provided value
        """

        # Arrange
        async def accumulator():
            total = 0
            while True:
                received = yield total
                if received is not None:
                    total += received
                else:
                    total += 1

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=accumulator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # next -> yields 0 (initial total)
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert cloudpickle.loads(response.result.dump) == 0

            # send 10 -> yields 10
            msg = protocol.Request(send=protocol.Message(dump=cloudpickle.dumps(10)))
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert cloudpickle.loads(response.result.dump) == 10

            # next (asend(None)) -> yields 11 (total + 1)
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert cloudpickle.loads(response.result.dump) == 11

            # send 5 -> yields 16
            msg = protocol.Request(send=protocol.Message(dump=cloudpickle.dumps(5)))
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert cloudpickle.loads(response.result.dump) == 16

            await stream.done_writing()

    @pytest.mark.asyncio
    async def test_dispatch_async_generator_throw_terminates(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() with a throw request that terminates the generator.

        Given:
            A service with an async generator task
        When:
            The client sends a throw request with a ValueError
        Then:
            The generator receives the exception and the server yields
            an exception response
        """

        # Arrange
        async def simple_generator():
            yield "first"
            yield "second"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=simple_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            # Read ack
            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Get first yield
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "first"

            # Throw ValueError into the generator
            throw_request = protocol.Request(
                throw=protocol.Message(dump=cloudpickle.dumps(ValueError("injected")))
            )
            await stream.write(throw_request)
            response = await anext(aiter(stream))
            assert response.HasField("exception")
            exception = cloudpickle.loads(response.exception.dump)
            assert isinstance(exception, ValueError)
            assert str(exception) == "injected"

    @pytest.mark.asyncio
    async def test_dispatch_streaming_with_proxy_and_dispatch_context(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() sets proxy and do_dispatch(False) for streaming.

        Given:
            A proxy pool configured in wool.__proxy_pool__ and an
            async generator task that reports do_dispatch() and
            wool.__proxy__ via yielded values
        When:
            dispatch() is called and the generator is advanced via
            next requests
        Then:
            It should execute with do_dispatch set to False during
            advancement, and wool.__proxy__ set to the pool's proxy.
        """

        # Arrange — generator yields its observed context
        async def capturing_generator():
            from wool.runtime.routine.task import do_dispatch as _dd

            yield {
                "do_dispatch": _dd(),
                "has_proxy": wool.__proxy__.get() is not None,
            }

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=capturing_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            result = cloudpickle.loads(response.result.dump)

            await stream.write(next_request)
            await stream.done_writing()
            [r async for r in stream]

        # Assert
        assert result["do_dispatch"] is False
        assert result["has_proxy"] is True

    @pytest.mark.asyncio
    async def test_dispatch_streaming_without_proxy_pool(self, grpc_aio_stub):
        """Test :class:`WorkerService` streaming dispatch when
        :data:`wool.__proxy_pool__` is not configured on the worker.

        Given:
            A worker process where :data:`wool.__proxy_pool__` is
            unset — the worker cannot lease a proxy and therefore
            cannot bind :data:`wool.__proxy__` for the routine
        When:
            The dispatch RPC is invoked
        Then:
            It should reply with a terminal exception Response
            carrying the assertion failure raised by
            :func:`scoped` — proxy-less execution is broken by
            construction (no nested-dispatch capability) and the
            handler surfaces the precondition violation rather
            than silently running without a proxy.
        """

        # Arrange — routine that would observe proxy state if it
        # ran; the assertion fires before the routine starts.
        async def capturing_generator():
            yield {"has_proxy": wool.__proxy__.get() is not None}

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=capturing_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            ack = await anext(aiter(stream))
            assert ack.HasField("ack")

            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        assert len(remaining) == 1
        terminal = remaining[0]
        assert terminal.HasField("exception")
        raised = cloudpickle.loads(terminal.exception.dump)
        assert isinstance(raised, RuntimeError)
        assert "wool.__proxy_pool__ is not initialized" in str(raised)

    @pytest.mark.asyncio
    async def test_dispatch_streaming_proxy_cleanup_on_error(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() cleans up proxy context on generator error.

        Given:
            A proxy pool configured in wool.__proxy_pool__ and an
            async generator task that raises after one yield
        When:
            dispatch() is called and the generator raises
        Then:
            It should return the exception and clean up the proxy
            context (call __aexit__).
        """

        # Arrange
        async def failing_generator():
            yield "before_error"
            raise ValueError("generator_error")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=failing_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # First next: yields "before_error"
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "before_error"

            # Second next: generator raises
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("exception")
            exception = cloudpickle.loads(response.exception.dump)
            assert isinstance(exception, ValueError)
            assert str(exception) == "generator_error"

            await stream.done_writing()
            [r async for r in stream]

        # Assert — proxy context manager was cleaned up
        mock_worker_proxy_cache.get.return_value.__aexit__.assert_called()

    @pytest.mark.asyncio
    async def test_dispatch_streaming_asend_with_proxy_context(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() forwards asend() within do_dispatch(False) context.

        Given:
            A proxy pool configured in wool.__proxy_pool__ and an
            async generator task that echoes sent values and reports
            do_dispatch state
        When:
            dispatch() is called with send requests containing
            non-None payloads
        Then:
            It should forward each sent value to the generator via
            asend() with do_dispatch set to False, and yield back
            the generator's responses.
        """

        # Arrange — generator echoes sent values and reports dispatch flag
        async def echo_with_capture():
            from wool.runtime.routine.task import do_dispatch as _dd

            value = yield "ready"
            while value is not None:
                value = yield {"echo": value, "do_dispatch": _dd()}

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=echo_with_capture,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # First next to get "ready"
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "ready"

            # Send "hello"
            msg = protocol.Request(
                send=protocol.Message(dump=cloudpickle.dumps("hello"))
            )
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            result1 = cloudpickle.loads(response.result.dump)

            # Send "world"
            msg = protocol.Request(
                send=protocol.Message(dump=cloudpickle.dumps("world"))
            )
            await stream.write(msg)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            result2 = cloudpickle.loads(response.result.dump)

            # Send None to terminate
            msg = protocol.Request(send=protocol.Message(dump=cloudpickle.dumps(None)))
            await stream.write(msg)
            await stream.done_writing()
            [r async for r in stream]

        # Assert
        assert result1 == {"echo": "hello", "do_dispatch": False}
        assert result2 == {"echo": "world", "do_dispatch": False}

    @pytest.mark.asyncio
    async def test_dispatch_streaming_athrow_with_proxy_context(
        self, grpc_aio_stub, mocker: MockerFixture, mock_worker_proxy_cache
    ):
        """Test dispatch() forwards athrow() within do_dispatch(False) context.

        Given:
            A proxy pool configured in wool.__proxy_pool__ and an
            async generator task that catches a specific exception
        When:
            dispatch() is called with a throw request
        Then:
            It should forward the exception via athrow() with
            do_dispatch set to False, and yield the recovery value
            along with the observed context.
        """

        # Arrange — generator catches ValueError and reports context
        async def resilient_generator():
            from wool.runtime.routine.task import do_dispatch as _dd

            try:
                yield "waiting"
            except ValueError:
                yield {
                    "do_dispatch": _dd(),
                    "has_proxy": wool.__proxy__.get() is not None,
                }

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        wool_task = Task(
            id=uuid4(),
            callable=resilient_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            # Get first yield
            await stream.write(next_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            assert cloudpickle.loads(response.result.dump) == "waiting"

            # Throw ValueError
            throw_request = protocol.Request(
                throw=protocol.Message(dump=cloudpickle.dumps(ValueError("test")))
            )
            await stream.write(throw_request)
            response = await anext(aiter(stream))
            assert response.HasField("result")
            result = cloudpickle.loads(response.result.dump)

            # Exhaust generator
            await stream.write(next_request)
            await stream.done_writing()
            [r async for r in stream]

        # Assert
        assert result["do_dispatch"] is False
        assert result["has_proxy"] is True

    def test___init___with_backpressure_hook(self):
        """Test WorkerService initialization with a backpressure hook.

        Given:
            A callable backpressure hook
        When:
            WorkerService is instantiated with backpressure=hook
        Then:
            It should initialize successfully with stopping and stopped events unset
        """

        # Arrange
        def hook(ctx):
            return False

        # Act
        service = WorkerService(backpressure=hook)

        # Assert
        assert not service.stopping.is_set()
        assert not service.stopped.is_set()

    @pytest.mark.asyncio
    async def test_dispatch_with_caller_context_var_and_backpressure_hook(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test the backpressure hook observes caller-shipped ContextVar
        values when it evaluates admission.

        Given:
            A :class:`WorkerService` whose backpressure hook reads a
            :class:`wool.ContextVar` to decide admission, and a
            dispatch Request whose ``context.vars`` carries a value
            for that var
        When:
            The dispatch RPC is invoked
        Then:
            The hook should observe the caller-shipped value (not
            ``LookupError``), because the dispatch handler scopes the
            caller-state Context via ``Context.run`` for the duration
            of hook evaluation — the handler does not install it
            against the main-loop task and so does not leak ownership
            across the main/worker boundary
        """
        # Arrange
        namespace = f"bp_ctxvar_{uuid4().hex}"
        tenant = wool.ContextVar("tenant", namespace=namespace)
        observed: list[str] = []

        def hook(ctx):
            observed.append(tenant.get("<unset>"))
            return False

        async def sample_task():
            return "accepted"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        context_pb = protocol.Context(id=uuid4().hex)
        context_pb.vars.add(
            namespace=tenant.namespace,
            name=tenant.name,
            value=cloudpickle.dumps("acme-corp"),
        )
        request = protocol.Request(task=wool_task.to_protobuf(), context=context_pb)

        service = WorkerService(backpressure=hook)

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("result")
        assert cloudpickle.loads(response.result.dump) == "accepted"
        assert observed == ["acme-corp"]

    @pytest.mark.asyncio
    async def test_dispatch_async_backpressure_hook_observes_caller_context_vars(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test an async backpressure hook observes caller-shipped
        ContextVar values across its await suspension.

        Given:
            A :class:`WorkerService` whose backpressure hook is
            ``async def`` and reads a :class:`wool.ContextVar` after
            an ``await asyncio.sleep(0)`` checkpoint, and a dispatch
            Request whose ``context.vars`` carries a value for that
            var
        When:
            The dispatch RPC is invoked
        Then:
            The hook should observe the caller-shipped value after
            the suspension — the dispatch handler must keep the
            caller-state Context attached across the await of the
            hook coroutine, not just the synchronous body that
            constructs it
        """
        # Arrange
        namespace = f"async_bp_ctxvar_{uuid4().hex}"
        tenant = wool.ContextVar("tenant", namespace=namespace)
        observed: list[str] = []

        async def hook(ctx):
            await asyncio.sleep(0)
            observed.append(tenant.get("<unset>"))
            return False

        async def sample_task():
            return "accepted"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        context_pb = protocol.Context(id=uuid4().hex)
        context_pb.vars.add(
            namespace=tenant.namespace,
            name=tenant.name,
            value=cloudpickle.dumps("acme-corp"),
        )
        request = protocol.Request(task=wool_task.to_protobuf(), context=context_pb)

        service = WorkerService(backpressure=hook)

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("result")
        assert cloudpickle.loads(response.result.dump) == "accepted"
        assert observed == ["acme-corp"]

    @pytest.mark.asyncio
    async def test_dispatch_with_sync_backpressure_accepting(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch succeeds when sync backpressure hook returns False.

        Given:
            A :class:`WorkerService` with a sync backpressure hook that returns False
        When:
            Dispatch RPC is called
        Then:
            It should accept the task and return the result normally
        """

        # Arrange
        async def sample_task():
            return "accepted"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        def hook(ctx):
            return False

        service = WorkerService(backpressure=hook)

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("result")
        assert cloudpickle.loads(response.result.dump) == "accepted"

    @pytest.mark.asyncio
    async def test_dispatch_with_sync_backpressure_rejecting(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch aborts when sync backpressure hook returns True.

        Given:
            A :class:`WorkerService` with a sync backpressure hook that returns True
        When:
            Dispatch RPC is called
        Then:
            It should reject the task with RESOURCE_EXHAUSTED status
        """

        # Arrange
        async def sample_task():
            return "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        def hook(ctx):
            return True

        service = WorkerService(backpressure=hook)

        # Act & assert
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            with pytest.raises(grpc.RpcError) as exc_info:
                async for _ in stream:
                    pass
            assert exc_info.value.code() == StatusCode.RESOURCE_EXHAUSTED

    @pytest.mark.asyncio
    async def test_dispatch_with_async_backpressure_rejecting(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch aborts when async backpressure hook returns True.

        Given:
            A :class:`WorkerService` with an async backpressure hook that returns True
        When:
            Dispatch RPC is called
        Then:
            It should reject the task with RESOURCE_EXHAUSTED status
        """

        # Arrange
        async def sample_task():
            return "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        async def async_hook(ctx):
            return True

        service = WorkerService(backpressure=async_hook)

        # Act & assert
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            with pytest.raises(grpc.RpcError) as exc_info:
                async for _ in stream:
                    pass
            assert exc_info.value.code() == StatusCode.RESOURCE_EXHAUSTED

    @pytest.mark.asyncio
    async def test_dispatch_with_async_backpressure_accepting(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch succeeds when async backpressure hook returns False.

        Given:
            A :class:`WorkerService` with an async backpressure hook that returns False
        When:
            Dispatch RPC is called
        Then:
            It should accept the task and return the result normally
        """

        # Arrange
        async def sample_task():
            return "async_accepted"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        async def async_hook(ctx):
            return False

        service = WorkerService(backpressure=async_hook)

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("result")
        assert cloudpickle.loads(response.result.dump) == "async_accepted"

    @pytest.mark.asyncio
    async def test_task_handler_defers_worker_scheduling_to_first_iteration(
        self, mock_worker_proxy_cache
    ):
        """Test :class:`DispatchSession` schedules the worker driver on
        the first :meth:`__aiter__` call rather than eagerly inside
        :meth:`__aenter__`.

        Regression test for the race between dispatch's backpressure
        hook and the worker for :meth:`Context._guard` ownership.
        Pre-fix :meth:`__aenter__` scheduled the worker eagerly;
        with a backpressure hook that yielded the main loop while
        holding ``attached(handler.context)``, the worker thread
        would race to acquire the same Context's guard via
        :func:`_wool_scoped` and spuriously raise
        ``RuntimeError("wool.Context is already running...")``. The
        invariant tested here — :meth:`__aenter__` is parse-only —
        guarantees no contention regardless of how long any
        post-parse main-loop work holds the Context. Verified via
        the ``_worker_done`` future, which the handler creates as
        the marker for "worker scheduled" inside
        :meth:`_schedule_worker`.

        Given:
            A :class:`DispatchSession` constructed around a parsed
            dispatch request and a real worker loop
        When:
            :meth:`__aenter__` completes (parse phase done) and
            :meth:`__aiter__` is first called (lazy-schedule fires)
        Then:
            It should leave the worker driver unscheduled after
            :meth:`__aenter__`, schedule it on the first
            :meth:`__aiter__` call, and deliver the routine's
            result through the iterator.
        """

        # Arrange
        async def sample_task():
            return "lazy_scheduled"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        async def request_stream():
            yield request

        worker_loop = asyncio.new_event_loop()
        install_task_factory(worker_loop)
        worker_thread = threading.Thread(target=worker_loop.run_forever, daemon=True)
        worker_thread.start()

        # Act & assert
        try:
            async with DispatchSession(request_stream(), worker_loop) as handler:
                # Lazy-schedule invariant: parse phase is complete but
                # the worker has not been scheduled. ``_worker_done``
                # is the marker — :meth:`_schedule_worker` creates the
                # future before submitting the worker task.
                assert handler._worker_done is None, (
                    "DispatchSession.__aenter__ must defer worker "
                    "scheduling to first __aiter__"
                )

                iterator = aiter(handler)
                assert handler._worker_done is not None, (
                    "DispatchSession.__aiter__ must schedule the worker on first call"
                )

                response = await anext(iterator)
                assert response.result == "lazy_scheduled"
        finally:
            worker_loop.call_soon_threadsafe(worker_loop.stop)
            worker_thread.join(timeout=5)
            worker_loop.close()

    @pytest.mark.asyncio
    async def test_dispatch_drains_handler_on_terminal_exception_path(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService` dispatch drains the worker
        before snapshotting :attr:`DispatchSession.context` for the
        terminal-exception response.

        Regression test for the cross-loop race when the dispatch's
        terminal-exception clause is reached while the worker is
        still alive on its loop. Routine-time and pre-stream worker
        failures arrive with the worker already finalized (its
        ``_on_done`` callback set ``worker_done`` before
        :meth:`_ResponseQueue.get` raised), but main-loop
        handler-level failures (e.g. ``response.to_protobuf``
        raising on dump) and gRPC-level cancellation reach the
        except clause with the worker mid-``_step`` calling
        ``work_ctx.update()`` / ``work_ctx.to_protobuf()``. Without
        an explicit drain before the snapshot,
        ``handler.context.to_protobuf(...)`` reads ``_data`` while
        the worker writes it. The fix calls
        :meth:`DispatchSession.drain` from dispatch's terminal-
        exception clause before the snapshot; :meth:`__aexit__`
        also calls drain (idempotent), so the spy observes two
        calls with the fix versus one without.

        Given:
            A streaming dispatch whose first
            ``response.to_protobuf`` invocation raises — forcing
            dispatch into its terminal-exception clause while the
            worker is still alive
        When:
            Dispatch reaches its terminal-exception clause
        Then:
            It should call :meth:`DispatchSession.drain` before
            snapshotting :attr:`DispatchSession.context` for the
            terminal Response — the spy observes drain at least
            twice (once from the except clause, once from
            :meth:`__aexit__`).
        """
        from wool.runtime.worker import session as handler_module

        # Arrange
        async def streamer():
            yield "value_a"
            yield "value_b"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streamer,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(
            next=protocol.Void(),
            context=protocol.Context(id=uuid4().hex),
        )

        drain_spy = mocker.spy(DispatchSession, "drain")

        # Force the dispatch into its terminal-exception clause
        # while the worker is still alive: make the FIRST
        # ``_Response.to_protobuf`` call raise. The dispatch's
        # iteration body raises before yielding any wire frames;
        # the inner finally calls handler.cancel() (which only
        # closes the main-loop iterator); the outer except builds
        # the terminal-exception Response from handler.context. At
        # this point — without the fix — the worker is still
        # mid-``_step`` because cancel did not drain it.
        original_to_protobuf = handler_module._Response.to_protobuf

        def failing_to_protobuf(self, *, serializer):
            raise RuntimeError("synthetic dump failure")

        mocker.patch.object(
            handler_module._Response,
            "to_protobuf",
            failing_to_protobuf,
        )

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(first_request)
            ack = await anext(aiter(stream))
            assert ack.HasField("ack")

            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        # The dispatch must yield exactly one terminal-exception
        # Response after the Ack (its except clause's output).
        assert len(remaining) == 1
        terminal = remaining[0]
        assert terminal.HasField("exception")

        # And drain must have been called at least twice: once
        # from the dispatch handler's terminal-exception clause
        # (the fix) and once from DispatchSession.__aexit__ (the
        # baseline cleanup that always runs). Without the fix,
        # drain is called only from __aexit__ — count == 1 — and
        # the snapshot at handler.context.to_protobuf(...) races
        # the still-alive worker.
        assert drain_spy.call_count >= 2, (
            f"Expected dispatch's terminal-exception clause to call "
            f"handler.drain() before snapshotting handler.context "
            f"(plus __aexit__'s call); observed {drain_spy.call_count} "
            f"call(s). Without this drain, the snapshot races the "
            f"still-alive worker mutating _data via _step."
        )

        # Restore so trailing teardown does not raise inside
        # mocker's revert.
        mocker.stop(
            mocker.patch.object(
                handler_module._Response,
                "to_protobuf",
                original_to_protobuf,
            )
        )

    @pytest.mark.asyncio
    async def test_task_handler_aexit_releases_passthrough_pool_when_drain_raises(
        self, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`DispatchSession.__aexit__` releases the
        passthrough-pool entry even when :meth:`drain` raises.

        Regression test for the resource leak in
        :meth:`__aexit__`'s no-``try/finally`` pattern. Pre-fix,
        ``await self.drain(); _stack.__aexit__(...)`` skipped the
        stack unwind whenever :meth:`drain` raised — most notably
        when drain re-raised ``CancelledError`` on the
        ``current.cancelling() > 0`` path during graceful
        shutdown, where the worker task was cancelled (its
        ``_on_done`` callback set ``worker_done`` with
        ``CancelledError``) and the dispatch task awaiting drain
        was also being cancelled. The
        ``_passthrough_pool.get(task_id)`` entry registered on the
        self-dispatch path leaked. The fix registers drain as an
        async exit-stack callback in :meth:`__aenter__` so the
        unwind always runs, regardless of drain's outcome.

        Given:
            A :class:`DispatchSession` driving a passthrough
            self-dispatch, with :meth:`drain` patched to raise
            :class:`asyncio.CancelledError`
        When:
            :meth:`__aenter__` succeeds (acquiring a
            ``_passthrough_pool`` entry) and :meth:`__aexit__` is
            invoked
        Then:
            It should release the passthrough-pool entry — the
            pool's ``referenced_entries`` count must return to its
            baseline despite drain raising.
        """
        from wool.runtime.serializer import PassthroughSerializer
        from wool.runtime.serializer import _passthrough_pool

        # Arrange
        baseline = _passthrough_pool.stats.referenced_entries
        serializer = PassthroughSerializer()

        # Patch drain at the class level BEFORE __aenter__ runs so
        # the fix's ``push_async_callback(self.drain)`` registers
        # the raising version on the exit stack.
        async def raising_drain(self):
            raise asyncio.CancelledError("simulated drain re-raise")

        mocker.patch.object(DispatchSession, "drain", raising_drain)

        async def quick_task():
            return "done"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=quick_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf(serializer=serializer))

        async def request_stream():
            yield request

        worker_loop = asyncio.new_event_loop()
        install_task_factory(worker_loop)
        worker_thread = threading.Thread(target=worker_loop.run_forever, daemon=True)
        worker_thread.start()

        try:
            handler = DispatchSession(request_stream(), worker_loop)
            await handler.__aenter__()

            assert _passthrough_pool.stats.referenced_entries == baseline + 1, (
                "__aenter__ should acquire a passthrough-pool entry on self-dispatch"
            )

            # Act
            with pytest.raises(asyncio.CancelledError):
                await handler.__aexit__(None, None, None)

            # Assert
            assert _passthrough_pool.stats.referenced_entries == baseline, (
                "DispatchSession.__aexit__ must release the "
                "passthrough-pool entry even when drain raises — "
                "pre-fix, drain's re-raise skipped _stack.__aexit__ "
                "and leaked the entry"
            )
        finally:
            worker_loop.call_soon_threadsafe(worker_loop.stop)
            worker_thread.join(timeout=5)
            if not worker_loop.is_closed():
                worker_loop.close()

    @pytest.mark.asyncio
    async def test_task_handler_drain_returns_when_worker_loop_closed_pre_schedule(
        self, mock_worker_proxy_cache
    ):
        """Test :meth:`DispatchSession.drain` returns promptly when
        the worker loop is closed before :meth:`_schedule_worker`
        can install the worker task's done-callback.

        Regression test for the indefinite hang in :meth:`drain`
        when :meth:`_schedule_worker` partially completes. Pre-fix,
        the ``self._worker_done = ...`` assignment ran before
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
            A :class:`DispatchSession` that has completed parse-phase
            :meth:`__aenter__` against a real worker loop
        When:
            The worker loop is closed before :meth:`__aiter__`
            calls :meth:`_schedule_worker`, then :meth:`drain` is
            invoked
        Then:
            It should return within a bounded timeout rather than
            blocking on a ``worker_done`` future that will never
            be resolved.
        """

        # Arrange
        async def sample_task():
            return "never_runs"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        async def request_stream():
            yield request

        worker_loop = asyncio.new_event_loop()
        install_task_factory(worker_loop)
        worker_thread = threading.Thread(target=worker_loop.run_forever, daemon=True)
        worker_thread.start()

        handler = DispatchSession(request_stream(), worker_loop)
        await handler.__aenter__()

        # Close the worker loop so that the next
        # ``call_soon_threadsafe`` raises
        # ``RuntimeError("Event loop is closed")`` synchronously
        # inside :meth:`_schedule_worker`, exercising the
        # partial-schedule path :meth:`drain` must tolerate.
        worker_loop.call_soon_threadsafe(worker_loop.stop)
        worker_thread.join(timeout=5)
        worker_loop.close()

        # Act
        try:
            with pytest.raises(RuntimeError):
                aiter(handler)

            # Assert
            # drain must return quickly — without the fix, it
            # awaits ``self._worker_done`` indefinitely because no
            # ``_on_done`` callback was ever installed on the
            # closed loop's worker task.
            await asyncio.wait_for(handler.drain(), timeout=2.0)
        finally:
            try:
                await handler._stack.aclose()
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_task_handler_cancel_before_aiter_short_circuits_schedule(
        self, mock_worker_proxy_cache
    ):
        """Test :meth:`DispatchSession.cancel` invoked before
        :meth:`__aiter__` short-circuits worker scheduling and
        surfaces :class:`asyncio.CancelledError` on the iterator's
        first :meth:`anext`.

        Regression test for the F7(a) race window: pre-fix,
        :meth:`cancel` was a no-op when ``self._iterator`` was
        ``None``, but :meth:`__aiter__` would still schedule the
        worker on its first call afterwards. With the flag-based
        :meth:`cancel`, the ``_cancelled`` flag pre-empts
        :meth:`_schedule_worker` and the iterator's first
        :meth:`anext` raises :class:`asyncio.CancelledError`
        (mirroring stdlib's ``await task`` semantics where a
        pre-cancelled awaitable raises ``CancelledError`` on its
        first await) without ever starting the worker.

        Given:
            A :class:`DispatchSession` past parse phase but with no
            iteration started
        When:
            :meth:`cancel` is called and then :meth:`__aiter__` is
            invoked
        Then:
            It should not schedule the worker
            (``_request_queue`` / ``_worker_done`` remain
            ``None``) and the iterator's first ``anext`` should
            raise :class:`asyncio.CancelledError`.
        """

        # Arrange
        async def sample_task():
            return "never_runs"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        async def request_stream():
            yield request

        worker_loop = asyncio.new_event_loop()
        install_task_factory(worker_loop)
        worker_thread = threading.Thread(target=worker_loop.run_forever, daemon=True)
        worker_thread.start()

        try:
            handler = DispatchSession(request_stream(), worker_loop)
            await handler.__aenter__()

            # Act: cancel before __aiter__
            await handler.cancel()
            iterator = aiter(handler)

            # Assert: worker was not scheduled
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
        finally:
            await handler.__aexit__(None, None, None)
            worker_loop.call_soon_threadsafe(worker_loop.stop)
            worker_thread.join(timeout=5)
            if not worker_loop.is_closed():
                worker_loop.close()

    @pytest.mark.asyncio
    async def test_task_handler_cancel_from_different_task_does_not_raise(
        self, mock_worker_proxy_cache
    ):
        """Test :meth:`DispatchSession.cancel` is safe to call from a
        task other than the one currently driving the iterator.

        Regression test for F7(b): pre-fix, :meth:`cancel`
        invoked ``aclose()`` on the iterator, which raised
        ``RuntimeError("asynchronous generator is already
        running")`` when the iterator was being driven by another
        task. :meth:`WorkerService._cancel` (graceful shutdown)
        calls :meth:`cancel` on every handler from a task other
        than each handler's driver, so the error fired on every
        in-flight dispatch during shutdown — only invisible
        because ``gather(return_exceptions=True)`` swallowed the
        :class:`RuntimeError`. The flag-based :meth:`cancel`
        cannot raise this error because it does not invoke
        ``aclose``.

        Given:
            A :class:`DispatchSession` whose iterator is mid-flight
            (driven by another task)
        When:
            :meth:`cancel` is called from a separate task
        Then:
            It should not raise — and the iterator's driver task
            should observe end-of-stream cleanly.
        """

        # Arrange — coroutine routine that sleeps long enough for
        # the test to observe the iterator suspended at
        # ``response_queue.get`` while another task calls
        # :meth:`cancel`. ``asyncio.sleep`` is picklable
        # (cloudpickle handles bound async builtins); a
        # ``threading.Event`` captured in the closure would not
        # be.
        async def slow_task():
            await asyncio.sleep(60)
            return "never"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=slow_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        async def request_stream():
            yield request

        worker_loop = asyncio.new_event_loop()
        install_task_factory(worker_loop)
        worker_thread = threading.Thread(target=worker_loop.run_forever, daemon=True)
        worker_thread.start()

        captured: dict = {}

        async def driver():
            async with DispatchSession(request_stream(), worker_loop) as handler:
                captured["handler"] = handler
                async for _ in handler:
                    pass

        try:
            driver_task = asyncio.create_task(driver())

            # Wait until the worker has been scheduled — this
            # proves __aiter__ ran and _iterate is suspended at
            # ``response_queue.get`` waiting on slow_task. Poll
            # the handler's ``_worker_done`` marker rather than
            # capturing a ``threading.Event`` in the routine
            # closure (which would block ``cloudpickle.dumps``
            # because :class:`threading.Event` holds a thread
            # lock).
            while "handler" not in captured or captured["handler"]._worker_done is None:
                await asyncio.sleep(0)

            # Act: cancel from this task while driver_task drives
            # the iterator. Pre-fix, this raised
            # ``RuntimeError("asynchronous generator is already
            # running")``; the flag-based fix returns cleanly. If
            # control reaches the next line, F7(b) is satisfied —
            # cancel did not raise.
            await captured["handler"].cancel()
        finally:
            # Cleanup: driver_task is now blocked in __aexit__'s
            # drain waiting on slow_task's 60s sleep. Cancel it
            # so the test does not stall — drain is registered on
            # the exit stack (F1 fix) and unwinds cleanly when
            # the wait is interrupted.
            driver_task.cancel()
            try:
                await asyncio.wait_for(driver_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            worker_loop.call_soon_threadsafe(worker_loop.stop)
            worker_thread.join(timeout=5)
            if not worker_loop.is_closed():
                worker_loop.close()

    @pytest.mark.asyncio
    async def test_dispatch_finally_cancel_failure_does_not_mask_routine_exception(
        self,
        grpc_aio_stub,
        mock_worker_proxy_cache,
        mocker: MockerFixture,
    ):
        """Test the dispatch handler's inner ``finally``
        ``handler.cancel()`` swallows exceptions so the routine's
        primary signal reaches the terminal-exception clause
        intact.

        Regression test for F8: pre-fix, the inner ``finally:
        await handler.cancel()`` ran without a guard. If
        :meth:`cancel` raised, Python's ``finally`` semantics
        replaced the in-flight ``async for`` exception with the
        cancel-time exception. The terminal-exception clause then
        shipped the cancel failure with the routine's primary
        signal demoted to ``__context__``. The fix wraps the
        cancel call in a ``try/except BaseException: pass`` so
        cancel-time failures cannot mask the routine signal.

        Given:
            A dispatch whose routine fails with a distinguished
            exception, and :meth:`DispatchSession.cancel` patched to
            raise a different exception
        When:
            The dispatch handler reaches its inner ``finally``
            and the terminal-exception clause builds the failure
            response
        Then:
            It should ship the routine's exception, not the
            cancel-time exception.
        """

        # Arrange
        class _RoutineFailure(Exception):
            pass

        class _CancelFailure(Exception):
            pass

        async def failing_task():
            raise _RoutineFailure("routine signal")

        async def raising_cancel(self):
            raise _CancelFailure("cancel failure must not mask routine")

        mocker.patch.object(DispatchSession, "cancel", raising_cancel)

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=failing_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())

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
            "terminal Response must carry the routine's primary "
            "exception, not the cancel-time failure — pre-fix, "
            f"observed {type(shipped).__name__}"
        )

    @pytest.mark.asyncio
    async def test_task_handler_aenter_preserves_parse_error_when_aclose_raises(
        self, mock_worker_proxy_cache
    ):
        """Test :meth:`DispatchSession.__aenter__` preserves the
        original parse error as :class:`Rejected` even when
        ``_stack.aclose()`` raises during cleanup.

        Regression test for F4: pre-fix, the parse-phase
        ``except Exception as e: await self._stack.aclose();
        raise Rejected(e) from None`` ran ``aclose`` un-guarded.
        If the stack's exit chain raised (e.g., a registered
        resource's ``__aexit__`` failing), the new exception
        replaced ``e`` and ``Rejected(e)`` was never constructed
        — the dispatch handler's Nack-with-exception channel
        observed the cleanup failure instead of the typed parse
        error. The fix swallows aclose failures so the parse
        error always reaches the caller.

        Given:
            A request that fails parse-phase validation (a
            non-async callable) AND a stack whose ``aclose``
            raises during the resulting cleanup
        When:
            :meth:`__aenter__` is invoked
        Then:
            It should raise :class:`Rejected` whose ``original``
            attribute carries the parse-phase ValueError, not
            the simulated aclose failure.
        """
        from wool.runtime.serializer import PassthroughSerializer
        from wool.runtime.worker.session import Rejected

        # Arrange — non-async callable triggers a ValueError in
        # __aenter__'s validation step at line 535-541, AFTER the
        # passthrough-pool entry has been pushed onto the stack
        # at line 516. Without a passthrough serializer the
        # entry is skipped and the test would pass trivially
        # because the stack would be empty when aclose runs.
        serializer = PassthroughSerializer()

        def not_async():
            return "sync"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=not_async,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf(serializer=serializer))

        async def request_stream():
            yield request

        worker_loop = asyncio.new_event_loop()
        install_task_factory(worker_loop)
        worker_thread = threading.Thread(target=worker_loop.run_forever, daemon=True)
        worker_thread.start()

        try:
            handler = DispatchSession(request_stream(), worker_loop)

            # Replace the stack's ``aclose`` with a raising
            # version. The patch survives ``__aenter__``'s call
            # to ``stack.__aenter__`` and fires when the parse
            # block hits its ``except Exception`` arm.
            async def raising_aclose():
                raise RuntimeError("simulated aclose failure during cleanup")

            handler._stack.aclose = raising_aclose

            # Act + Assert
            with pytest.raises(Rejected) as exc_info:
                await handler.__aenter__()

            assert isinstance(exc_info.value.original, ValueError), (
                f"Rejected.original must carry the parse-phase "
                f"ValueError, not the aclose failure — observed "
                f"{type(exc_info.value.original).__name__}"
            )
            assert "Expected coroutine function" in str(exc_info.value.original), (
                "Rejected.original must be the actual parse-phase validation error"
            )
        finally:
            worker_loop.call_soon_threadsafe(worker_loop.stop)
            worker_thread.join(timeout=5)
            if not worker_loop.is_closed():
                worker_loop.close()

    @pytest.mark.asyncio
    async def test_dispatch_ships_stop_async_iteration_raw_for_coroutine_routine(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test the wire surfaces :class:`StopAsyncIteration` raw
        when a coroutine routine raises it at the top level —
        matching stdlib ``await coro()`` semantics.

        Regression test for F5. Pre-fix, the wire shipped
        :class:`RuntimeError` because :meth:`DispatchSession._iterate`
        is an async generator: when the worker's
        ``_ResponseQueue.get`` re-raised the routine's
        :class:`StopAsyncIteration` inside _iterate's body, PEP
        525 converted it to ``RuntimeError("async generator
        raised StopAsyncIteration")`` at the asyncgen boundary
        before the dispatch handler's terminal-exception clause
        could ship it. The fix unwraps that RuntimeError's
        ``__cause__`` for non-streaming routines so the wire
        carries the original SAI — what the caller's
        ``await routine()`` would receive in stdlib if the
        coroutine had been local.

        Given:
            A coroutine routine that raises :class:`StopAsyncIteration`
        When:
            The dispatch RPC ships its terminal-exception
            Response
        Then:
            It should carry :class:`StopAsyncIteration`, not
            :class:`RuntimeError`.
        """

        async def coro_raising_sai():
            raise StopAsyncIteration("from coroutine")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=coro_raising_sai,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())

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
        assert isinstance(shipped, StopAsyncIteration), (
            "wire must surface coroutine-raised StopAsyncIteration "
            "raw — pre-fix PEP 525 in _iterate's asyncgen layer "
            "converted it to RuntimeError; observed "
            f"{type(shipped).__name__}"
        )
        assert shipped.args == ("from coroutine",), (
            "the original SAI's args must survive the unwrap"
        )

    @pytest.mark.asyncio
    async def test_dispatch_ships_runtime_error_for_async_generator_raising_sai(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test the wire surfaces :class:`RuntimeError` (not
        :class:`StopAsyncIteration`) when an async generator
        routine raises :class:`StopAsyncIteration` from its body
        — matching stdlib ``async for x in agen()`` semantics
        (PEP 525).

        Companion to the coroutine StopAsyncIteration regression
        test (F5). The fix targeted the coroutine path (unwrap
        PEP 525's auto-conversion); the async generator path was
        already correct because the user's asyncgen runtime does
        the conversion before the worker ever sees SAI — the
        dispatch handler observes a :class:`RuntimeError` from
        ``gen.asend`` and ships it. This test pins the desired
        behavior so a future change to the unwrap logic does not
        accidentally widen and corrupt the asyncgen contract.

        Given:
            An async generator routine whose body raises
            :class:`StopAsyncIteration` mid-iteration
        When:
            The dispatch RPC ships its terminal-exception
            Response
        Then:
            It should carry :class:`RuntimeError` whose
            ``__cause__`` preserves the original SAI.
        """

        async def agen_raising_sai():
            raise StopAsyncIteration("from agen")
            yield 1

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=agen_raising_sai,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(
            next=protocol.Void(),
            context=protocol.Context(id=uuid4().hex),
        )

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(first_request)
            ack = await anext(aiter(stream))
            assert ack.HasField("ack")
            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        assert len(remaining) == 1, (
            "expect exactly one terminal-exception Response after "
            f"the Ack; observed {len(remaining)}"
        )
        terminal = remaining[0]
        assert terminal.HasField("exception")
        shipped = cloudpickle.loads(terminal.exception.dump)
        assert isinstance(shipped, RuntimeError), (
            "wire must surface RuntimeError for async generator "
            "raising StopAsyncIteration (PEP 525 stdlib semantics) "
            f"— observed {type(shipped).__name__}"
        )
        assert isinstance(shipped.__cause__, StopAsyncIteration), (
            "the synthesized RuntimeError must preserve the "
            "original SAI on ``__cause__`` (PEP 525)"
        )
        assert shipped.__cause__.args == ("from agen",)

    @pytest.mark.asyncio
    async def test_dispatch_swallows_request_queue_runtime_error_mid_stream(
        self,
        grpc_aio_stub,
        mock_worker_proxy_cache,
        mocker: MockerFixture,
    ):
        """Test :class:`DispatchSession._iterate` swallows
        :class:`RuntimeError` from ``_RequestQueue.put`` so a
        closed worker loop mid-stream terminates the dispatch
        cleanly instead of being misattributed as a routine
        failure on the wire.

        Regression test for F9. Pre-fix, the streaming branch
        of :meth:`_iterate` called
        ``request_queue.put(protobuf_request)`` un-guarded.
        :meth:`_RequestQueue.put` schedules onto the worker loop
        via ``call_soon_threadsafe``; if the worker loop has
        been torn down (graceful shutdown teardown landing
        between two main-loop pumps), put raises
        ``RuntimeError("Event loop is closed")``. The
        unguarded propagation surfaced the runtime error out of
        :meth:`_iterate`, into the dispatch handler's
        terminal-exception clause, and onto the wire as a
        routine exception — but the routine never failed; the
        worker loop did. The fix mirrors :meth:`drain`'s
        pattern: catch ``RuntimeError`` at the put site and
        break cleanly, so the stream terminates without a
        synthetic terminal Response.

        Given:
            A streaming dispatch with :meth:`_RequestQueue.put`
            patched to succeed on the first call and raise
            ``RuntimeError`` on the second
        When:
            The dispatch RPC sends a Task, then a first ``next``
            request (succeeds), then a second ``next`` request
            (would trigger the patched RuntimeError)
        Then:
            The wire stream should not carry a terminal Response
            whose exception is the synthetic ``RuntimeError`` —
            the dispatch must terminate cleanly without
            misattributing the worker-loop teardown as a routine
            failure.
        """
        from wool.runtime.worker.session import _RequestQueue

        # Arrange — succeed once, then simulate a closed loop.
        call_count = 0
        original_put = _RequestQueue.put

        def patched_put(self, request):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise RuntimeError("simulated closed worker loop during put")
            return original_put(self, request)

        mocker.patch.object(_RequestQueue, "put", patched_put)

        async def streaming():
            yield "first"
            yield "second"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streaming,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(
            next=protocol.Void(),
            context=protocol.Context(id=uuid4().hex),
        )

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(first_request)
            ack = await anext(aiter(stream))
            assert ack.HasField("ack")

            # First next: put succeeds — caller receives "first".
            await stream.write(next_request)
            first_response = await anext(aiter(stream))
            assert first_response.HasField("result"), (
                "first iteration should yield a result"
            )

            # Second next: put raises. Pre-fix, the dispatch
            # handler's terminal-exception clause ships the
            # RuntimeError. Post-fix, _iterate breaks cleanly
            # and the stream terminates without a synthetic
            # exception Response.
            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert — no terminal Response should carry the
        # synthetic ``RuntimeError`` masquerading as a routine
        # failure.
        for response in remaining:
            if response.HasField("exception"):
                shipped = cloudpickle.loads(response.exception.dump)
                if isinstance(
                    shipped, RuntimeError
                ) and "simulated closed worker loop" in str(shipped):
                    raise AssertionError(
                        "dispatch shipped a synthetic worker-loop "
                        "RuntimeError as a routine exception — "
                        "_iterate must catch RuntimeError from "
                        "request_queue.put and terminate the stream "
                        "cleanly instead"
                    )

    def test_safely_serialize_exception_falls_back_for_unpicklable_exception(
        self,
    ):
        """Test :func:`_safely_serialize_exception` synthesizes a
        :class:`RuntimeError` describing the original when the
        first ``dumps`` raises a pickle-failure exception.

        Pins F10's contract: the helper catches the *expected*
        un-picklable failure modes
        (:class:`pickle.PickleError`, :class:`TypeError`,
        :class:`AttributeError`) and falls back to a stdlib
        :class:`RuntimeError` describing the original. The
        fallback always succeeds because :class:`RuntimeError`
        is trivially picklable for both
        :class:`CloudpickleSerializer` (cloudpickle) and
        :class:`PassthroughSerializer` (which never fails — it
        stashes by reference and returns a token).
        """
        from wool.runtime.worker.service import _safely_serialize_exception

        class _OriginalError(Exception):
            pass

        # A serializer whose first ``dumps`` raises TypeError
        # (the stdlib signal for un-picklable C types like
        # thread locks) and whose second ``dumps`` succeeds —
        # mirroring cloudpickle's behavior on un-picklable
        # input followed by the synthesized RuntimeError.
        class FlakySerializer:
            def __init__(self):
                self.calls = 0

            def dumps(self, obj):
                self.calls += 1
                if self.calls == 1:
                    raise TypeError("cannot pickle '_thread.lock' object")
                return cloudpickle.dumps(obj)

            def loads(self, data):
                return cloudpickle.loads(data)

        serializer = FlakySerializer()
        result = _safely_serialize_exception(
            serializer, _OriginalError("with un-picklable state")
        )

        assert serializer.calls == 2, (
            "first dumps should raise un-picklable, second should "
            "succeed for the synthesized RuntimeError"
        )
        loaded = cloudpickle.loads(result)
        assert isinstance(loaded, RuntimeError), (
            "fallback must synthesize a stdlib RuntimeError so the "
            "wire carries a typed exception (not a generic gRPC "
            f"stream error); observed {type(loaded).__name__}"
        )
        assert "_OriginalError" in str(loaded), (
            "fallback message must name the original exception "
            "type so the caller can correlate"
        )
        assert "with un-picklable state" in str(loaded), (
            "fallback message must include the original args so the caller has context"
        )

    def test_safely_serialize_exception_propagates_unexpected_serializer_failure(
        self,
    ):
        """Test :func:`_safely_serialize_exception` propagates
        non-pickle exceptions raised by ``dumps`` rather than
        silently rerouting through the fallback.

        Pins F10's narrowed catch: the helper only handles the
        expected un-picklable failure modes
        (:class:`pickle.PickleError`, :class:`TypeError`,
        :class:`AttributeError`); anything else (e.g., a broken
        serializer raising :class:`ValueError`) is a genuine bug
        that must surface, not be silently rerouted to the
        synthetic-RuntimeError fallback.

        The first ``dumps`` raises :class:`ValueError`; the
        second would succeed if reached. Pre-fix, the bare
        ``except Exception`` caught the ValueError, the fallback
        ran, and the helper returned bytes — silently absorbing
        the bug. Post-fix, the narrow catch lets the ValueError
        propagate.
        """
        from wool.runtime.worker.service import _safely_serialize_exception

        class FlakySerializer:
            def __init__(self):
                self.calls = 0

            def dumps(self, obj):
                self.calls += 1
                if self.calls == 1:
                    raise ValueError("unexpected serializer failure")
                return cloudpickle.dumps(obj)

            def loads(self, data):
                return cloudpickle.loads(data)

        serializer = FlakySerializer()
        with pytest.raises(ValueError, match="unexpected serializer failure"):
            _safely_serialize_exception(serializer, Exception("e"))
        assert serializer.calls == 1, (
            "fallback must NOT run when the first dumps raises a "
            "non-pickle-failure exception — pre-fix's bare "
            "``except Exception`` rerouted unexpected bugs through "
            "the synthetic-RuntimeError fallback, masking them"
        )

    @pytest.mark.asyncio
    async def test_dispatch_attaches_strict_mode_context_warnings_as_notes(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test the dispatch handler attaches strict-mode
        :class:`ContextDecodeWarning` peers to the routine's
        exception via PEP 678 ``__notes__`` and a
        ``__wool_context_warnings__`` attribute, preserving the
        routine exception's type.

        Regression test for the user-facing contract pinned by
        F11's redesign. Pre-redesign, when a routine failed AND
        ``handler.context.to_protobuf`` raised (only possible
        when the operator promoted :class:`ContextDecodeWarning`
        to an exception via
        ``warnings.filterwarnings("error",
        category=ContextDecodeWarning)``), :func:`merge_exceptions`
        wrapped the routine failure and the encode peers in a
        :class:`BaseExceptionGroup` — forcing strict-mode users
        to migrate their existing ``except RoutineError`` clauses
        to ``except*`` or ``except ExceptionGroup``. The redesign
        attaches peers to the routine exception via PEP 678
        notes (visible in tracebacks) and a
        ``__wool_context_warnings__`` attribute (programmatic
        access), so existing exception-handling code keeps
        working unchanged.

        Given:
            A coroutine routine that raises a custom exception,
            and ``handler.context.to_protobuf`` patched to raise
            a :class:`BaseExceptionGroup` of synthetic
            :class:`ContextDecodeWarning` peers (simulating
            strict-mode encode failure)
        When:
            The dispatch RPC ships its terminal-exception
            Response
        Then:
            It should ship the routine's exception type bare
            (not wrapped in any group), with the warnings
            attached as ``__notes__`` and
            ``__wool_context_warnings__``.
        """
        from wool.runtime.context import Context
        from wool.runtime.context import ContextDecodeWarning

        class _RoutineFailure(Exception):
            pass

        async def failing_task():
            raise _RoutineFailure("primary signal")

        original_to_protobuf = Context.to_protobuf

        def encode_with_strict_failure(self, *args, **kwargs):
            raise BaseExceptionGroup(
                "strict-mode context encode failure",
                [
                    ContextDecodeWarning("var-1 unencodable"),
                    ContextDecodeWarning("var-2 unencodable"),
                ],
            )

        mocker.patch.object(Context, "to_protobuf", encode_with_strict_failure)

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=failing_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(first_request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Restore so the gRPC fixture's teardown does not
        # explode on Context.to_protobuf calls during cleanup.
        mocker.patch.object(Context, "to_protobuf", original_to_protobuf)

        # Assert
        ack, terminal = responses
        assert ack.HasField("ack")
        assert terminal.HasField("exception")
        shipped = cloudpickle.loads(terminal.exception.dump)

        # The routine's exception type is preserved — caller's
        # existing ``except _RoutineFailure`` continues to catch.
        assert isinstance(shipped, _RoutineFailure), (
            f"wire must ship the routine's exception type bare, "
            f"not a wrapper group — observed {type(shipped).__name__}"
        )
        assert shipped.args == ("primary signal",)

        # PEP 678 notes carry the warnings as human-readable
        # diagnostic — they show up in tracebacks naturally.
        assert hasattr(shipped, "__notes__"), (
            "shipped exception must have __notes__ populated"
        )
        notes_text = "\n".join(shipped.__notes__)
        assert "var-1 unencodable" in notes_text, (
            "first ContextDecodeWarning must appear in "
            f"__notes__; observed: {shipped.__notes__}"
        )
        assert "var-2 unencodable" in notes_text, (
            "second ContextDecodeWarning must appear in "
            f"__notes__; observed: {shipped.__notes__}"
        )

        # __wool_context_warnings__ provides structured access
        # for programmatic inspection.
        assert hasattr(shipped, "__wool_context_warnings__"), (
            "shipped exception must carry __wool_context_warnings__"
        )
        warnings = shipped.__wool_context_warnings__
        assert len(warnings) == 2
        assert all(isinstance(w, ContextDecodeWarning) for w in warnings)
        assert {str(w) for w in warnings} == {
            "var-1 unencodable",
            "var-2 unencodable",
        }

    @pytest.mark.asyncio
    async def test_task_handler_aenter_rejects_first_frame_with_wrong_oneof(
        self, mock_worker_proxy_cache
    ):
        """Test :meth:`DispatchSession.__aenter__` raises
        :class:`Rejected` with a clear oneof-violation message
        when the first frame's ``payload`` is not a Task.

        Regression test for F12. Pre-fix, ``UUID(request.task.id)``
        ran unconditionally; for a non-Task first frame
        ``request.task`` was the default-empty Task and
        ``UUID("")`` raised :class:`ValueError`. The dispatch
        handler caught it via ``except Exception`` and rewrapped
        as :class:`Rejected`, but the cause string named a UUID
        parse error rather than the underlying protocol
        violation. The fix validates the oneof variant first.

        Given:
            A request stream whose first frame carries a ``next``
            payload (not a Task)
        When:
            :meth:`__aenter__` is invoked
        Then:
            It should raise :class:`Rejected` whose ``original``
            attribute is a :class:`ValueError` whose message
            names the oneof violation.
        """
        from wool.runtime.worker.session import Rejected

        wrong_first_frame = protocol.Request(next=protocol.Void())

        async def request_stream():
            yield wrong_first_frame

        worker_loop = asyncio.new_event_loop()
        install_task_factory(worker_loop)
        worker_thread = threading.Thread(target=worker_loop.run_forever, daemon=True)
        worker_thread.start()

        try:
            handler = DispatchSession(request_stream(), worker_loop)
            with pytest.raises(Rejected) as exc_info:
                await handler.__aenter__()

            assert isinstance(exc_info.value.original, ValueError), (
                f"Rejected.original must be ValueError; observed "
                f"{type(exc_info.value.original).__name__}"
            )
            message = str(exc_info.value.original).lower()
            assert "payload" in message and "task" in message, (
                "Rejected.original message must reference the "
                "payload oneof violation, not a UUID parse failure"
            )
        finally:
            worker_loop.call_soon_threadsafe(worker_loop.stop)
            worker_thread.join(timeout=5)
            if not worker_loop.is_closed():
                worker_loop.close()

    @pytest.mark.asyncio
    async def test_task_handler_aenter_rejects_empty_request_stream(
        self, mock_worker_proxy_cache
    ):
        """Test :meth:`DispatchSession.__aenter__` raises
        :class:`Rejected` (not bare
        :class:`StopAsyncIteration`) when the request stream is
        empty.

        Regression test for F16. Pre-fix, an empty request
        stream caused ``await anext(...)`` to raise
        :class:`StopAsyncIteration`; the bare sentinel escaped
        :meth:`__aenter__` raw and risked PEP 479 conversion to
        :class:`RuntimeError` at any surrounding async-generator
        boundary. The fix converts to a typed
        :class:`Rejected` so the dispatch handler ships a clear
        protocol-level rejection via the Nack channel.

        Given:
            A request stream that yields no frames
        When:
            :meth:`__aenter__` is invoked
        Then:
            It should raise :class:`Rejected` whose ``original``
            is a :class:`ValueError` naming the empty-stream
            condition.
        """
        from wool.runtime.worker.session import Rejected

        async def empty_stream():
            if False:
                yield

        worker_loop = asyncio.new_event_loop()
        install_task_factory(worker_loop)
        worker_thread = threading.Thread(target=worker_loop.run_forever, daemon=True)
        worker_thread.start()

        try:
            handler = DispatchSession(empty_stream(), worker_loop)
            with pytest.raises(Rejected) as exc_info:
                await handler.__aenter__()

            assert isinstance(exc_info.value.original, ValueError), (
                f"Rejected.original must be ValueError; observed "
                f"{type(exc_info.value.original).__name__}"
            )
            assert "empty" in str(exc_info.value.original).lower(), (
                "Rejected.original message must name the "
                "empty-stream condition for a clear protocol error"
            )
        finally:
            worker_loop.call_soon_threadsafe(worker_loop.stop)
            worker_thread.join(timeout=5)
            if not worker_loop.is_closed():
                worker_loop.close()

    @pytest.mark.asyncio
    async def test_dispatch_with_backpressure_receiving_context(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test backpressure hook receives correct context.

        Given:
            A :class:`WorkerService` with a backpressure hook that captures its argument
        When:
            Dispatch RPC is called
        Then:
            It should pass a BackpressureContext with active_task_count and task fields
        """
        # Arrange
        from wool.runtime.worker.service import BackpressureContext

        async def sample_task():
            return "result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        captured = []

        def hook(ctx):
            captured.append(ctx)
            return False

        service = WorkerService(backpressure=hook)

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            [r async for r in stream]

        # Assert
        assert len(captured) == 1
        ctx = captured[0]
        assert isinstance(ctx, BackpressureContext)
        assert ctx.active_task_count == 0
        assert ctx.task.id == wool_task.id

    @pytest.mark.asyncio
    async def test_dispatch_with_backpressure_and_active_tasks(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test backpressure hook sees correct active task count.

        Given:
            A :class:`WorkerService` with one active task already dispatched
        When:
            A second dispatch RPC is called with a backpressure hook
        Then:
            It should see active_task_count == 1
        """
        # Arrange
        global _control_event
        _control_event = threading.Event()

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")

        first_task = Task(
            id=uuid4(),
            callable=_controllable_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=first_task.to_protobuf())

        async def second_fn():
            return "second"

        second_task = Task(
            id=uuid4(),
            callable=second_fn,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        second_request = protocol.Request(task=second_task.to_protobuf())

        captured_count = []

        def hook(ctx):
            captured_count.append(ctx.active_task_count)
            return False

        service = WorkerService(backpressure=hook)

        # Act
        try:
            async with grpc_aio_stub(servicer=service) as stub:
                # Dispatch first task (blocks on control event)
                stream1 = stub.dispatch()
                await stream1.write(first_request)
                await stream1.done_writing()
                # Wait for ack to confirm first task is tracked
                async for response in stream1:
                    assert response.HasField("ack")
                    break

                # Dispatch second task — hook should see 1 active task
                stream2 = stub.dispatch()
                await stream2.write(second_request)
                await stream2.done_writing()
                [r async for r in stream2]

                # Release first task
                _control_event.set()
                [r async for r in stream1]
        finally:
            if _control_event and not _control_event.is_set():
                _control_event.set()
            _control_event = None

        # Assert — first dispatch sees 0 active, second sees 1
        assert captured_count == [0, 1]

    @pytest.mark.asyncio
    async def test_dispatch_with_backpressure_hook_raising_exception(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch surfaces error when backpressure hook raises.

        Given:
            A :class:`WorkerService` with a backpressure hook that
            raises RuntimeError
        When:
            Dispatch RPC is called
        Then:
            It should propagate the error as a gRPC failure
        """

        # Arrange
        async def sample_task():
            return "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        def hook(ctx):
            raise RuntimeError("hook exploded")

        service = WorkerService(backpressure=hook)

        # Act & assert
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            with pytest.raises(grpc.RpcError):
                async for _ in stream:
                    pass

    @pytest.mark.asyncio
    async def test_dispatch_with_caller_vars_for_coroutine_task(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test dispatch applies caller-side vars before running a coroutine.

        Given:
            A Request carrying a non-empty ``vars`` map (a
            wool.ContextVar set on the caller, serialized via _dumps)
            and a coroutine Task that reads the var
        When:
            The dispatch RPC is invoked end-to-end via the gRPC stub
        Then:
            The Response's result equals the caller-side var value —
            the worker applied the wire-shipped snapshot before running
            the task.
        """
        # Arrange
        var = wool.ContextVar("srv001_caller_var", namespace="test_srv_vars")

        async def reader_task():
            return var.get()

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=reader_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(
            task=wool_task.to_protobuf(),
            context=protocol.Context(
                vars=[
                    protocol.ContextVar(
                        namespace=var.namespace,
                        name=var.name,
                        value=cloudpickle.dumps("caller-side-value"),
                    )
                ]
            ),
        )

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, response = responses
        assert ack.HasField("ack")
        assert response.HasField("result")
        assert cloudpickle.loads(response.result.dump) == "caller-side-value"

    @pytest.mark.asyncio
    async def test_dispatch_with_per_frame_caller_vars_for_async_gen(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test streaming dispatch applies per-frame vars before each asend.

        Given:
            An async-generator Task whose frames yield the current
            value of a caller-side wool.ContextVar, with subsequent
            next Requests carrying updated ``vars`` maps each iteration
        When:
            The stream is iterated with changing ``vars`` on each frame
        Then:
            Each response's result reflects the per-frame ``vars``
            applied on the worker — forward-propagation is honored at
            every streaming frame, not just the first.
        """
        # Arrange
        var = wool.ContextVar("srv002_frame_var", namespace="test_srv_vars")

        async def streaming_task():
            while True:
                yield var.get()

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streaming_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        initial_request = protocol.Request(
            task=wool_task.to_protobuf(),
            context=protocol.Context(
                vars=[
                    protocol.ContextVar(
                        namespace=var.namespace,
                        name=var.name,
                        value=cloudpickle.dumps("first"),
                    )
                ]
            ),
        )

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(initial_request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            results: list = []
            for frame_value in ("first", "second", "third"):
                await stream.write(
                    protocol.Request(
                        next=protocol.Void(),
                        context=protocol.Context(
                            vars=[
                                protocol.ContextVar(
                                    namespace=var.namespace,
                                    name=var.name,
                                    value=cloudpickle.dumps(frame_value),
                                )
                            ],
                        ),
                    )
                )
                response = await anext(aiter(stream))
                assert response.HasField("result")
                results.append(cloudpickle.loads(response.result.dump))

            await stream.done_writing()
            async for _ in stream:
                pass

        # Assert
        assert results == ["first", "second", "third"]

    @pytest.mark.asyncio
    async def test_dispatch_with_routine_raising_cancelled_during_aclose(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService` streaming dispatch ends
        cleanly when the routine raises CancelledError during
        ``aclose`` on a natural-end iteration.

        :func:`scoped` propagates aclose-time exceptions (matching
        stdlib ``await agen.aclose()`` semantics — see the unit
        tests in ``tests/runtime/routine/test_task.py`` for direct
        coverage). For natural-end iteration, the consumer's
        ``_iterate`` has already returned by the time the worker
        runs aclose, and :meth:`drain` swallows the worker-side
        :class:`asyncio.CancelledError` when the dispatch task
        itself isn't being cancelled. Net wire-level result:
        clean stream end with no terminal exception response.

        Given:
            A streaming async-generator routine whose teardown
            handler catches :class:`GeneratorExit` during aclose
            and re-raises as :class:`asyncio.CancelledError`, plus
            a dispatch that ends naturally (caller closes the
            stream without invoking service.stop).
        When:
            The dispatch RPC is invoked, advanced by one ``next``,
            then the caller closes the request stream.
        Then:
            The wire stream should end cleanly (no terminal
            exception response) — drain's swallow on
            ``cancelling() == 0`` keeps the worker-side
            CancelledError off the wire after natural iteration
            end.
        """

        async def teardown_cancelling_generator():
            try:
                yield "first"
                yield "never"
            except GeneratorExit:
                raise asyncio.CancelledError() from None

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=teardown_cancelling_generator,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)

            ack = await anext(aiter(stream))
            assert ack.HasField("ack")

            await stream.write(next_request)
            first = await anext(aiter(stream))
            assert first.HasField("result")
            assert cloudpickle.loads(first.result.dump) == "first"

            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert — clean stream end. The worker-side
        # CancelledError raised by aclose propagates out of
        # scoped (verified by unit tests on scoped) but drain
        # swallows it on the natural-end path.
        assert remaining == []

    @pytest.mark.asyncio
    async def test_dispatch_with_passthrough_loads_for_streaming_self_dispatch(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test streaming self-dispatch applies PassthroughSerializer.loads for vars.

        Given:
            An async-generator Task configured with a
            PassthroughSerializer whose streaming next-frames carry
            var updates serialized via PassthroughSerializer.dumps
        When:
            The stream is iterated with changing vars on each frame
        Then:
            Each response reflects the updated value — proving that
            the worker-side _apply_vars uses PassthroughSerializer.loads
            rather than cloudpickle.loads for passthrough self-dispatch
            frames.
        """
        from wool.runtime.serializer import PassthroughSerializer

        # Arrange
        var = wool.ContextVar(
            "passthrough_stream_var", namespace="test_passthrough_stream"
        )
        serializer = PassthroughSerializer()

        async def streaming_task():
            while True:
                yield var.get()

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streaming_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        initial_request = protocol.Request(
            task=wool_task.to_protobuf(serializer=serializer),
            context=protocol.Context(
                vars=[
                    protocol.ContextVar(
                        namespace=var.namespace,
                        name=var.name,
                        value=serializer.dumps("alpha"),
                    )
                ]
            ),
        )

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(initial_request)

            response = await anext(aiter(stream))
            assert response.HasField("ack")

            results: list = []
            for frame_value in ("alpha", "bravo", "charlie"):
                await stream.write(
                    protocol.Request(
                        next=protocol.Void(),
                        context=protocol.Context(
                            vars=[
                                protocol.ContextVar(
                                    namespace=var.namespace,
                                    name=var.name,
                                    value=serializer.dumps(frame_value),
                                )
                            ],
                        ),
                    )
                )
                response = await anext(aiter(stream))
                assert response.HasField("result")
                results.append(serializer.loads(response.result.dump))

            await stream.done_writing()
            async for _ in stream:
                pass

        # Assert
        assert results == ["alpha", "bravo", "charlie"]

    @pytest.mark.asyncio
    async def test_dispatch_attaches_strict_mode_warnings_for_single_peer(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService.dispatch` attaches a single bare
        :class:`ContextDecodeWarning` to the routine's exception via
        ``__notes__`` and ``__wool_context_warnings__``.

        Given:
            A coroutine routine that succeeds AND
            :meth:`Context.to_protobuf` patched to raise a single bare
            :class:`ContextDecodeWarning` (not a group)
        When:
            The dispatch RPC ships its terminal-exception Response
        Then:
            It should ship the routine's exception with ``__notes__``
            containing the single warning and
            ``__wool_context_warnings__`` of length 1.
        """
        from wool.runtime.context import Context
        from wool.runtime.context import ContextDecodeWarning

        # Arrange
        async def succeeding_task():
            return "ok"

        original_to_protobuf = Context.to_protobuf
        single_warning = ContextDecodeWarning("single bare peer")

        def encode_with_single_failure(self, *args, **kwargs):
            raise single_warning

        mocker.patch.object(Context, "to_protobuf", encode_with_single_failure)

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=succeeding_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Restore so the gRPC fixture's teardown does not explode on
        # subsequent Context.to_protobuf calls during cleanup.
        mocker.patch.object(Context, "to_protobuf", original_to_protobuf)

        # Assert
        ack, terminal = responses
        assert ack.HasField("ack")
        assert terminal.HasField("exception")
        shipped = cloudpickle.loads(terminal.exception.dump)
        assert hasattr(shipped, "__notes__")
        notes_text = "\n".join(shipped.__notes__)
        assert "single bare peer" in notes_text
        assert hasattr(shipped, "__wool_context_warnings__")
        warnings = shipped.__wool_context_warnings__
        assert len(warnings) == 1
        assert isinstance(warnings[0], ContextDecodeWarning)
        assert str(warnings[0]) == "single bare peer"

    @pytest.mark.asyncio
    async def test_dispatch_attaches_strict_mode_warnings_on_async_generator_path(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService.dispatch` attaches strict-mode
        warning peers to the routine's exception on the
        async-generator path.

        Given:
            An async-generator routine that raises a custom exception
            mid-stream AND :meth:`Context.to_protobuf` patched to
            raise a :class:`BaseExceptionGroup` of two
            :class:`ContextDecodeWarning` peers
        When:
            The dispatch RPC ships its terminal-exception Response
            after one successful yield
        Then:
            It should ship the routine's exception type bare with
            both warnings on ``__notes__`` and
            ``__wool_context_warnings__`` of length 2.
        """
        from wool.runtime.context import Context
        from wool.runtime.context import ContextDecodeWarning

        # Arrange
        class _RoutineFailure(Exception):
            pass

        async def streamer():
            yield "first"
            raise _RoutineFailure("mid-stream signal")

        original_to_protobuf = Context.to_protobuf
        # Let the first per-yield encode succeed so the streamer
        # delivers ``"first"`` over the wire; subsequent invocations
        # (including the dispatch handler's terminal-exception
        # snapshot) raise the strict-mode encode group.
        call_count = {"n": 0}

        def encode_with_strict_failure(self, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return original_to_protobuf(self, *args, **kwargs)
            raise BaseExceptionGroup(
                "strict-mode encode failure",
                [
                    ContextDecodeWarning("agen-peer-1"),
                    ContextDecodeWarning("agen-peer-2"),
                ],
            )

        mocker.patch.object(Context, "to_protobuf", encode_with_strict_failure)

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streamer,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(first_request)
            ack = await anext(aiter(stream))
            assert ack.HasField("ack")

            await stream.write(next_request)
            first = await anext(aiter(stream))
            assert first.HasField("result")
            assert cloudpickle.loads(first.result.dump) == "first"

            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Restore so the gRPC fixture's teardown does not explode on
        # subsequent Context.to_protobuf calls during cleanup.
        mocker.patch.object(Context, "to_protobuf", original_to_protobuf)

        # Assert
        terminals = [r for r in remaining if r.HasField("exception")]
        assert len(terminals) == 1
        shipped = cloudpickle.loads(terminals[0].exception.dump)
        assert isinstance(shipped, _RoutineFailure)
        assert hasattr(shipped, "__notes__")
        notes_text = "\n".join(shipped.__notes__)
        assert "agen-peer-1" in notes_text
        assert "agen-peer-2" in notes_text
        warnings = shipped.__wool_context_warnings__
        assert len(warnings) == 2
        assert all(isinstance(w, ContextDecodeWarning) for w in warnings)

    @pytest.mark.asyncio
    async def test_dispatch_does_not_unwrap_runtime_error_with_unrelated_cause(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService.dispatch` ships
        :class:`RuntimeError` raw when its ``__cause__`` is not a
        :class:`StopAsyncIteration`.

        Given:
            A coroutine routine raising
            ``RuntimeError("not async iter")`` whose ``__cause__`` is
            NOT a :class:`StopAsyncIteration`
        When:
            The dispatch RPC ships its terminal-exception Response
        Then:
            It should ship the :class:`RuntimeError` raw (not
            unwrapped to ``__cause__``).
        """

        # Arrange
        async def raising_task():
            try:
                raise ValueError("underlying cause")
            except ValueError as cause:
                raise RuntimeError("not async iter") from cause

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=raising_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, terminal = responses
        assert ack.HasField("ack")
        assert terminal.HasField("exception")
        shipped = cloudpickle.loads(terminal.exception.dump)
        assert isinstance(shipped, RuntimeError)
        assert not isinstance(shipped, ValueError)
        assert "not async iter" in str(shipped)

    @pytest.mark.asyncio
    async def test_dispatch_does_not_unwrap_runtime_error_for_async_generator(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService.dispatch` keeps
        :class:`RuntimeError` un-unwrapped on the async-generator
        path even when ``__cause__`` is a
        :class:`StopAsyncIteration`.

        Given:
            An async-generator routine that raises a PEP 525
            :class:`RuntimeError` whose ``__cause__`` is a
            :class:`StopAsyncIteration`
        When:
            The dispatch RPC ships its terminal-exception Response
        Then:
            It should ship the :class:`RuntimeError` un-unwrapped —
            the wire payload is :class:`RuntimeError`, not
            :class:`StopAsyncIteration`.
        """

        # Arrange
        async def agen_raising_sai():
            raise StopAsyncIteration("from agen")
            yield 1

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=agen_raising_sai,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(first_request)
            ack = await anext(aiter(stream))
            assert ack.HasField("ack")

            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        terminals = [r for r in remaining if r.HasField("exception")]
        assert len(terminals) == 1
        shipped = cloudpickle.loads(terminals[0].exception.dump)
        assert isinstance(shipped, RuntimeError)
        assert not isinstance(shipped, StopAsyncIteration)

    @pytest.mark.asyncio
    async def test_dispatch_drains_handler_on_terminal_exception_path_for_coroutine(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService.dispatch` drains the handler on
        the coroutine terminal-exception path before yielding the
        terminal Response.

        Given:
            A coroutine dispatch whose first
            ``Response.to_protobuf`` invocation raises (forcing the
            terminal-exception clause while the worker is still alive
            on its loop)
        When:
            The dispatch RPC reaches its terminal-exception clause
        Then:
            It should call :meth:`DispatchSession.drain` at least twice
            (verified via spy) before yielding the terminal Response.
        """
        from wool.runtime.worker import session as handler_module

        # Arrange
        async def succeeding_coroutine():
            return "value"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=succeeding_coroutine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        drain_spy = mocker.spy(DispatchSession, "drain")

        original_to_protobuf = handler_module._Response.to_protobuf

        def failing_to_protobuf(self, *, serializer):
            raise RuntimeError("synthetic dump failure")

        mocker.patch.object(
            handler_module._Response,
            "to_protobuf",
            failing_to_protobuf,
        )

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, terminal = responses
        assert ack.HasField("ack")
        assert terminal.HasField("exception")
        assert drain_spy.call_count >= 2, (
            f"Expected dispatch's terminal-exception clause to call "
            f"handler.drain() before snapshotting handler.context "
            f"(plus __aexit__'s call); observed {drain_spy.call_count} "
            f"call(s)."
        )

        # Restore so trailing teardown does not raise inside
        # mocker's revert.
        mocker.stop(
            mocker.patch.object(
                handler_module._Response,
                "to_protobuf",
                original_to_protobuf,
            )
        )

    @pytest.mark.asyncio
    async def test_dispatch_skips_backpressure_evaluation_when_no_hook(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService.dispatch` skips backpressure
        evaluation entirely when no hook is configured.

        Given:
            A :class:`WorkerService` with no backpressure hook
        When:
            ``dispatch`` is invoked with a normally-completing
            coroutine task
        Then:
            The handler skips backpressure evaluation entirely; the
            response sequence is (Ack, result).
        """

        # Arrange
        async def sample_task():
            return "no_hook_result"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())
        service = WorkerService()
        assert service._backpressure is None  # sanity: no hook configured

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 2
        ack, result = responses
        assert ack.HasField("ack")
        assert result.HasField("result")
        assert cloudpickle.loads(result.result.dump) == "no_hook_result"

    @pytest.mark.asyncio
    async def test_dispatch_with_rejecting_backpressure_leaves_no_docket_entry(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService.dispatch` leaves no docket entry
        when backpressure rejects the task.

        Given:
            A backpressure hook that returns ``True`` for the very
            first task
        When:
            A single dispatch RPC is invoked
        Then:
            It should abort with ``RESOURCE_EXHAUSTED`` and the
            docket has no entry afterwards (verifiable via
            ``service.stop()`` completing cleanly with no waited
            entries).
        """

        # Arrange
        async def sample_task():
            return "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        def hook(ctx):
            return True

        service = WorkerService(backpressure=hook)

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            with pytest.raises(grpc.RpcError) as exc_info:
                async for _ in stream:
                    pass

            # The docket must be empty after a backpressure-rejected
            # dispatch — stop with timeout=0 should complete cleanly
            # without waiting for or cancelling any tracked entries.
            stop_result = await asyncio.wait_for(
                stub.stop(protocol.StopRequest(timeout=0)), timeout=2
            )

        # Assert
        assert exc_info.value.code() == StatusCode.RESOURCE_EXHAUSTED
        assert isinstance(stop_result, protocol.Void)
        assert service.stopping.is_set()
        assert service.stopped.is_set()

    def test_backpressure_context_is_frozen(self):
        """Test :class:`BackpressureContext` rejects mutation of its
        fields after construction.

        Given:
            A :class:`BackpressureContext` instance with assigned
            ``active_task_count`` and ``task``
        When:
            The caller attempts to mutate
            ``ctx.active_task_count = 5``
        Then:
            It should raise :class:`dataclasses.FrozenInstanceError`.
        """
        import dataclasses

        from wool.runtime.worker.service import BackpressureContext

        # Arrange
        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="frozen-test")
        task = Task(
            id=uuid4(),
            callable=lambda: None,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        ctx = BackpressureContext(active_task_count=0, task=task)

        # Act & assert
        with pytest.raises(dataclasses.FrozenInstanceError):
            ctx.active_task_count = 5

    def test_backpressure_like_runtime_checkable(self):
        """Test :class:`BackpressureLike` accepts callables and
        rejects non-callables under :func:`isinstance`.

        Given:
            A sync callable ``def hook(ctx): ...``, an async callable
            ``async def hook(ctx): ...``, and a non-callable object
        When:
            Each is checked with ``isinstance(x, BackpressureLike)``
        Then:
            The two callables pass; the non-callable fails.
        """
        from wool.runtime.worker.service import BackpressureLike

        # Arrange
        def sync_hook(ctx):
            return False

        async def async_hook(ctx):
            return False

        non_callable = object()

        # Act & assert
        assert isinstance(sync_hook, BackpressureLike)
        assert isinstance(async_hook, BackpressureLike)
        assert not isinstance(non_callable, BackpressureLike)

    def test_stopping_and_stopped_reflect_lifecycle(self):
        """Test :attr:`WorkerService.stopping` and
        :attr:`WorkerService.stopped` reflect the service lifecycle
        through their ``is_set()`` accessor.

        Given:
            A new :class:`WorkerService` and accesses to the
            ``stopping`` and ``stopped`` properties
        When:
            The properties are read pre/post-stop
        Then:
            Each access returns a wrapper whose ``is_set()`` reflects
            ``False`` initially and ``True`` after stop.
        """
        # Arrange
        service = WorkerService()

        # Act & assert (pre-stop)
        assert service.stopping.is_set() is False
        assert service.stopped.is_set() is False

        # Drive the stop path on the current loop directly via the
        # public RPC entry.
        async def _drive_stop():
            await service.stop(protocol.StopRequest(timeout=0), None)

        asyncio.run(_drive_stop())

        # Assert (post-stop)
        assert service.stopping.is_set() is True
        assert service.stopped.is_set() is True

    def test_stopping_wrapper_does_not_expose_mutators(self):
        """Test the :attr:`WorkerService.stopping` wrapper exposes
        only read access — calling mutators raises
        :class:`AttributeError`.

        Given:
            A :class:`WorkerService` whose ``stopping`` accessor is
            exposed
        When:
            The caller attempts to call ``.set()`` or ``.clear()`` on
            the returned wrapper
        Then:
            It should raise :class:`AttributeError` (the read-only
            wrapper does not expose mutators).
        """
        # Arrange
        service = WorkerService()
        wrapper = service.stopping

        # Act & assert
        with pytest.raises(AttributeError):
            wrapper.set()
        with pytest.raises(AttributeError):
            wrapper.clear()

    @pytest.mark.asyncio
    async def test_stop_with_empty_docket_completes_cleanly(self, grpc_aio_stub):
        """Test :meth:`WorkerService.stop` with ``timeout=0`` and an
        empty docket completes cleanly without a configured proxy
        pool.

        Given:
            A :class:`WorkerService.stop` invocation with
            ``timeout=0`` while the docket is empty AND
            :data:`wool.__proxy_pool__` is unset
        When:
            The stop RPC is invoked
        Then:
            It should complete without error, set ``stopping`` and
            ``stopped``, and a subsequent dispatch returns
            ``UNAVAILABLE``.
        """

        # Arrange
        async def sample_task():
            return "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        service = WorkerService()
        # Sanity: the autouse _clear_proxy_context fixture leaves the
        # proxy pool unset for this test (no mock_worker_proxy_cache).
        assert wool.__proxy_pool__.get() is None

        # Act
        async with grpc_aio_stub(servicer=service) as stub:
            stop_result = await asyncio.wait_for(
                stub.stop(protocol.StopRequest(timeout=0)), timeout=2
            )

            # Assert
            assert isinstance(stop_result, protocol.Void)
            assert service.stopping.is_set()
            assert service.stopped.is_set()

            request = protocol.Request(task=wool_task.to_protobuf())
            with pytest.raises(grpc.RpcError) as exc_info:
                stream = stub.dispatch()
                await stream.write(request)
                await stream.done_writing()
                async for _ in stream:
                    pass
            assert exc_info.value.code() == StatusCode.UNAVAILABLE

    @pytest.mark.asyncio
    async def test_stop_clears_loop_pool_when_proxy_pool_clear_raises(
        self, grpc_aio_stub, mocker: MockerFixture
    ):
        """Test :meth:`WorkerService.stop` still sets
        :attr:`stopped` when the proxy-pool's ``clear`` coroutine
        raises — the loop-pool clear runs in the ``finally``.

        Given:
            A :class:`WorkerService.stop` invocation while the
            proxy-pool ``clear`` coroutine raises
        When:
            The stop RPC is invoked
        Then:
            It should still set ``stopped`` (the loop-pool clear runs
            in the ``finally``); the raised proxy-pool exception
            surfaces to the caller.
        """
        from wool.runtime.resourcepool import ResourcePool

        # Arrange
        mock_pool = mocker.MagicMock(spec=ResourcePool)
        mock_pool.clear = mocker.AsyncMock(
            side_effect=RuntimeError("synthetic proxy-pool clear failure")
        )
        token = wool.__proxy_pool__.set(mock_pool)

        service = WorkerService()

        try:
            # Act & assert — the proxy-pool exception propagates
            # through the gRPC layer; the caller observes an
            # ``RpcError`` while the underlying service still
            # transitioned to stopped.
            async with grpc_aio_stub(servicer=service) as stub:
                with pytest.raises(grpc.RpcError):
                    await asyncio.wait_for(
                        stub.stop(protocol.StopRequest(timeout=0)),
                        timeout=2,
                    )

            # Assert — stopped event is set even when proxy-pool
            # clear raised, because the finally-block always runs
            # the loop-pool clear and sets the event.
            assert service.stopping.is_set()
            assert service.stopped.is_set()
            mock_pool.clear.assert_called_once()
        finally:
            wool.__proxy_pool__.reset(token)

    @pytest.mark.asyncio
    async def test_dispatch_ships_synthesized_runtime_error_for_unpicklable_routine_exception(  # noqa: E501
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService.dispatch` ships a synthesized
        stdlib :class:`RuntimeError` for an un-picklable routine
        exception.

        Given:
            A coroutine routine that raises an un-picklable exception
        When:
            The dispatch RPC ships its terminal-exception Response
        Then:
            It should ship a synthesized stdlib :class:`RuntimeError`
            whose message names the original exception type and args.
        """

        # Arrange
        class _UnpicklableError(Exception):
            def __reduce__(self):
                raise TypeError("cannot pickle _UnpicklableError")

        async def raising_task():
            raise _UnpicklableError("unpicklable signal")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=raising_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, terminal = responses
        assert ack.HasField("ack")
        assert terminal.HasField("exception")
        shipped = cloudpickle.loads(terminal.exception.dump)
        assert type(shipped) is RuntimeError
        assert "_UnpicklableError" in str(shipped)
        assert "unpicklable signal" in str(shipped)

    @pytest.mark.asyncio
    async def test_dispatch_streaming_ships_synthesized_runtime_error_for_unpicklable_exception(  # noqa: E501
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService.dispatch` ships a synthesized
        :class:`RuntimeError` and a valid context snapshot when an
        async-generator raises an un-picklable exception after one
        yield.

        Given:
            An async-generator routine emitting one yield then
            raising an un-picklable exception
        When:
            The dispatch RPC ships its terminal-exception Response
            after the first result
        Then:
            The terminal Response carries a synthesized
            :class:`RuntimeError` and a valid context snapshot.
        """

        # Arrange
        class _UnpicklableError(Exception):
            def __reduce__(self):
                raise TypeError("cannot pickle _UnpicklableError")

        async def streamer():
            yield "first"
            raise _UnpicklableError("agen unpicklable signal")

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=streamer,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        first_request = protocol.Request(task=wool_task.to_protobuf())
        next_request = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(first_request)
            ack = await anext(aiter(stream))
            assert ack.HasField("ack")

            await stream.write(next_request)
            first = await anext(aiter(stream))
            assert first.HasField("result")
            assert cloudpickle.loads(first.result.dump) == "first"

            await stream.write(next_request)
            await stream.done_writing()
            remaining = [r async for r in stream]

        # Assert
        terminals = [r for r in remaining if r.HasField("exception")]
        assert len(terminals) == 1
        terminal = terminals[0]
        assert terminal.HasField("context")
        shipped = cloudpickle.loads(terminal.exception.dump)
        assert type(shipped) is RuntimeError
        assert "_UnpicklableError" in str(shipped)
        assert "agen unpicklable signal" in str(shipped)

    @pytest.mark.asyncio
    async def test_dispatch_with_unpicklable_exception_whose_str_raises(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService.dispatch` ships a synthesized
        :class:`RuntimeError` containing only the exception class name
        when the routine's exception is un-picklable AND its ``__str__``
        raises.

        Given:
            A coroutine routine that raises an exception whose direct
            pickle fails, whose ``cls(*args)`` reconstruction also pickles
            with the un-picklable arg, AND whose ``__str__`` raises
        When:
            The dispatch RPC ships its terminal-exception Response
        Then:
            It should ship a synthesized :class:`RuntimeError` whose
            message carries only the class name (the message-with-args
            f-string fallback short-circuited because ``__str__``
            raised), exercising the defensive
            ``except Exception: message = cls_name`` arm of the
            three-tier serializer fallback.
        """

        # Arrange — design the exception so all three serializer tiers
        # exercise the deepest defensive path:
        #   tier 1 (dumps(exc))            → fails: args carries an
        #                                    un-picklable payload.
        #   tier 2 (dumps(cls(*args)))     → fails: same args.
        #   tier 3 (f"{cls_name}: {exc!s}") → fails: __str__ raises.
        #   ⇒ fallback message = cls_name only.
        class _Unpicklable:
            def __reduce__(self):
                raise TypeError("synthetic unpicklable payload")

        class _BadStrError(Exception):
            def __str__(self):
                raise RuntimeError("synthetic __str__ failure")

        async def raising_task():
            raise _BadStrError(_Unpicklable())

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=raising_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        ack, terminal = responses
        assert ack.HasField("ack")
        assert terminal.HasField("exception")
        shipped = cloudpickle.loads(terminal.exception.dump)
        # Synthesized stdlib RuntimeError, NOT the original _BadStrError
        # (un-picklable) and NOT a class-name-plus-message string
        # (``__str__`` raises, so the f-string fallback never composes
        # the message portion).
        assert type(shipped) is RuntimeError
        # Message body is exactly the class name (``cls_name``), with no
        # ``: <message>`` suffix — confirming the defensive
        # ``except Exception: message = cls_name`` arm fired.
        assert shipped.args == ("_BadStrError",)

    @pytest.mark.asyncio
    async def test_backpressure_with_truthy_non_bool_return_rejects(
        self, grpc_aio_stub, mock_worker_proxy_cache
    ):
        """Test :class:`WorkerService.dispatch` rejects the task when
        the backpressure hook returns a truthy non-bool value.

        Given:
            A :class:`BackpressureLike` hook that returns a non-bool
            truthy value (e.g., a non-empty string)
        When:
            The dispatch RPC is invoked
        Then:
            It should reject the task with ``RESOURCE_EXHAUSTED``.
        """

        # Arrange
        async def sample_task():
            return "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        def truthy_string_hook(ctx):
            return "reject"

        service = WorkerService(backpressure=truthy_string_hook)

        # Act & assert
        async with grpc_aio_stub(servicer=service) as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            with pytest.raises(grpc.RpcError) as exc_info:
                async for _ in stream:
                    pass
            assert exc_info.value.code() == StatusCode.RESOURCE_EXHAUSTED

    @pytest.mark.asyncio
    async def test_dispatch_rejects_empty_request_stream(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService.dispatch` replies with a single
        :class:`Nack` when the request stream is empty.

        Given:
            A dispatch call whose request stream yields no frames
        When:
            The dispatch RPC is consumed
        Then:
            It should respond with a single :class:`Nack` whose
            exception decodes to a :class:`ValueError` naming the
            empty-stream rejection.
        """

        # Arrange — neuter the version interceptor so the dispatch
        # handler observes the empty stream directly. Pass-through
        # ``intercept_service`` returns the underlying handler
        # unchanged, exposing the dispatch handler's Nack contract
        # for empty streams to the wire.
        async def passthrough(self, continuation, handler_call_details):
            return await continuation(handler_call_details)

        mocker.patch.object(VersionInterceptor, "intercept_service", passthrough)

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        nack = responses[0]
        assert nack.HasField("nack")
        assert nack.nack.HasField("exception")
        raised = cloudpickle.loads(nack.nack.exception.dump)
        assert isinstance(raised, ValueError)
        assert "empty" in str(raised).lower()

    @pytest.mark.asyncio
    async def test_dispatch_rejects_first_frame_with_wrong_oneof(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService.dispatch` replies with a single
        :class:`Nack` when the first frame's payload is the wrong
        oneof variant.

        Given:
            A dispatch call whose first frame's payload is the wrong
            oneof variant (``next`` instead of ``task``)
        When:
            The dispatch RPC is consumed
        Then:
            It should respond with a single :class:`Nack` whose
            exception decodes to a :class:`ValueError` naming the
            payload oneof violation.
        """

        # Arrange — neuter the version interceptor so the dispatch
        # handler observes the wrong-oneof frame directly. The
        # interceptor would otherwise intercept the empty Task on a
        # ``next``-payload first frame and reply with an unparseable-
        # version Nack before the dispatch handler's oneof check
        # could fire.
        async def passthrough(self, continuation, handler_call_details):
            return await continuation(handler_call_details)

        mocker.patch.object(VersionInterceptor, "intercept_service", passthrough)
        wrong_first_frame = protocol.Request(next=protocol.Void())

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(wrong_first_frame)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Assert
        assert len(responses) == 1
        nack = responses[0]
        assert nack.HasField("nack")
        assert nack.nack.HasField("exception")
        raised = cloudpickle.loads(nack.nack.exception.dump)
        assert isinstance(raised, ValueError)
        message = str(raised).lower()
        assert "payload" in message and "task" in message

    @pytest.mark.asyncio
    async def test_dispatch_nack_with_unpicklable_rejected_original(
        self, grpc_aio_stub, mock_worker_proxy_cache, mocker: MockerFixture
    ):
        """Test :class:`WorkerService.dispatch` ships a synthesized
        :class:`RuntimeError` for the Nack ``exception`` payload when
        :attr:`Rejected.original` is itself un-picklable.

        Given:
            A :class:`WorkerService.dispatch` whose
            :attr:`Rejected.original` is itself an un-picklable
            exception
        When:
            The dispatch RPC ships its :class:`Nack` Response
        Then:
            The ``Nack.exception`` decodes to the synthesized stdlib
            :class:`RuntimeError`.
        """
        from wool.runtime.worker import session as handler_module

        # Arrange
        class _UnpicklableRejected(Exception):
            def __reduce__(self):
                raise TypeError("cannot pickle _UnpicklableRejected")

        async def sample_task():
            return "should_not_reach"

        mock_proxy = PicklableMock(spec=WorkerProxyLike, id="test-proxy-id")
        wool_task = Task(
            id=uuid4(),
            callable=sample_task,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        request = protocol.Request(task=wool_task.to_protobuf())

        original_aenter = handler_module.DispatchSession.__aenter__

        async def failing_aenter(self):
            await self._stack.__aenter__()
            raise handler_module.Rejected(
                _UnpicklableRejected("unpicklable parse failure")
            )

        mocker.patch.object(handler_module.DispatchSession, "__aenter__", failing_aenter)

        # Act
        async with grpc_aio_stub() as stub:
            stream = stub.dispatch()
            await stream.write(request)
            await stream.done_writing()
            responses = [r async for r in stream]

        # Restore so trailing teardown does not raise.
        mocker.patch.object(
            handler_module.DispatchSession, "__aenter__", original_aenter
        )

        # Assert
        assert len(responses) == 1
        nack = responses[0]
        assert nack.HasField("nack")
        assert nack.nack.HasField("exception")
        shipped = cloudpickle.loads(nack.nack.exception.dump)
        assert type(shipped) is RuntimeError
        assert "_UnpicklableRejected" in str(shipped)
        assert "unpicklable parse failure" in str(shipped)
