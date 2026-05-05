from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from contextlib import AsyncExitStack
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
from inspect import isasyncgen
from inspect import isasyncgenfunction
from inspect import isawaitable
from inspect import iscoroutinefunction
from typing import Any
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Awaitable
from typing import Final
from typing import Literal
from typing import Protocol
from typing import assert_never
from typing import cast
from typing import runtime_checkable
from uuid import UUID

from grpc import StatusCode
from grpc.aio import ServicerContext

import wool
from wool import protocol
from wool.runtime.context import Context
from wool.runtime.context import attached
from wool.runtime.context import install_task_factory
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import _unpickle_serializer
from wool.runtime.routine.task import do_dispatch
from wool.runtime.serializer import PassthroughSerializer
from wool.runtime.serializer import Serializer
from wool.runtime.serializer import _passthrough_pool

_log = logging.getLogger(__name__)

# Sentinel to mark end of async generator stream
_STREAM_END: Final = object()


@dataclass
class _WorkerOutcome:
    """Result of a worker-loop task execution.

    Always carries the post-run :class:`protocol.Context` so the
    handler can ship caller-visible mutations on the Response
    regardless of whether the routine returned normally or raised.
    """

    result: Any = None
    exception: BaseException | None = None
    context: protocol.Context = field(default_factory=protocol.Context)


def _response_from_outcome(
    outcome: _WorkerOutcome,
    serializer: Serializer,
) -> protocol.Response:
    """Build a :class:`protocol.Response` frame from a worker outcome.

    Picks the ``exception`` or ``result`` oneof based on which field
    is set, serializes the payload, and attaches the worker's context
    (ID + var snapshot) on the response.
    """
    if outcome.exception is not None:
        return protocol.Response(
            exception=protocol.Message(dump=serializer.dumps(outcome.exception)),
            context=outcome.context,
        )
    return protocol.Response(
        result=protocol.Message(dump=serializer.dumps(outcome.result)),
        context=outcome.context,
    )


def _merge_exceptions(
    primary: BaseException,
    secondary: BaseException,
) -> BaseExceptionGroup:
    """Merge *primary* and *secondary* into a single
    :class:`BaseExceptionGroup`, flattening either side when it is
    already a group so callers see one flat peer structure.

    When *primary* is a :class:`BaseExceptionGroup`, peers are
    appended via :meth:`BaseExceptionGroup.derive`, preserving the
    original group's message and metadata. When *primary* is not a
    group, the returned group is labelled
    ``"wool dispatch primary signal with ancillary failure"`` — a
    fixed string covering every site that hits this helper today.
    """
    secondary_peers = (
        list(secondary.exceptions)
        if isinstance(secondary, BaseExceptionGroup)
        else [secondary]
    )
    if isinstance(primary, BaseExceptionGroup):
        return primary.derive([*primary.exceptions, *secondary_peers])
    return BaseExceptionGroup(
        "wool dispatch primary signal with ancillary failure",
        [primary, *secondary_peers],
    )


def _safely_serialize_exception(
    serializer: Serializer,
    exc: BaseException,
) -> bytes:
    """Serialize *exc*, falling back to a synthesized
    :class:`RuntimeError` if dumping the original raises.

    Prevents unpicklable exception state from converting a
    wool-class failure on the wire into a generic gRPC stream
    error on the caller side.
    """
    try:
        return serializer.dumps(exc)
    except Exception:
        return serializer.dumps(RuntimeError(f"{type(exc).__name__}: {exc!s}"))


def _outcome_from(
    *,
    value: Any = None,
    routine_exc: BaseException | None,
    context: Context,
    serializer: Serializer,
) -> _WorkerOutcome:
    """Build a :class:`_WorkerOutcome` for a routine step, including
    the post-step context snapshot.

    Centralizes the primary-signal preservation contract — the
    result-or-exception fork plus snapshot bundling — so each step
    builds an outcome the same way regardless of dispatch shape. When
    encoding fails alongside a *routine_exc* in flight, the two are
    merged via :func:`_merge_exceptions` so the snapshot never
    preempts or demotes the primary signal. Encode failures alone
    (no routine exception) become the outcome's exception with an
    empty context patch. Either way the failure does not escape this
    function, so callers can rely on every code path producing a
    :class:`_WorkerOutcome`.
    """
    try:
        wire_context = context.to_protobuf(serializer=serializer)
    except Exception as context_exc:
        if routine_exc is not None:
            merged = _merge_exceptions(routine_exc, context_exc)
            return _WorkerOutcome(exception=merged, context=protocol.Context())
        return _WorkerOutcome(exception=context_exc, context=protocol.Context())
    if routine_exc is not None:
        return _WorkerOutcome(exception=routine_exc, context=wire_context)
    return _WorkerOutcome(result=value, context=wire_context)


# public
@dataclass(frozen=True)
class BackpressureContext:
    """Snapshot of worker state provided to backpressure hooks.

    :param active_task_count:
        Number of tasks currently executing on this worker.
    :param task:
        The incoming :class:`~wool.runtime.routine.task.Task` being
        evaluated for admission.
    """

    active_task_count: int
    task: Task


# public
@runtime_checkable
class BackpressureLike(Protocol):
    """Protocol for backpressure hooks.

    A backpressure hook determines whether an incoming task should be
    rejected. Return ``True`` to **reject** the task (apply
    backpressure) or ``False`` to **accept** it.

    When a task is rejected the worker responds with gRPC
    ``RESOURCE_EXHAUSTED``, which the load balancer treats as
    transient and skips to the next worker.

    Pass ``None`` (the default) to accept all tasks unconditionally.

    The hook runs after the caller's wire-shipped ContextVar snapshot
    is applied to the handler's context, so a hook that reads a
    :class:`wool.ContextVar` (e.g., a tenant id) observes the caller's
    value for that dispatch. This enables tenant- or request-scoped
    admission decisions without plumbing values through the
    :class:`BackpressureContext` explicitly.

    Both sync and async implementations are supported::

        def sync_hook(ctx: BackpressureContext) -> bool:
            return ctx.active_task_count >= 4


        async def async_hook(ctx: BackpressureContext) -> bool:
            return ctx.active_task_count >= 4
    """

    def __call__(self, ctx: BackpressureContext) -> bool | Awaitable[bool]:
        """Evaluate whether to reject the incoming task.

        :param ctx:
            Snapshot of the worker's current state and the incoming
            task.
        :returns:
            ``True`` to reject the task, ``False`` to accept it.
        """
        ...


class _Task:
    def __init__(self, task: asyncio.Task):
        self._work = task

    async def cancel(self):
        self._work.cancel()
        await self._work


class _AsyncGen:
    def __init__(self, task: AsyncGenerator):
        self._work = task

    async def cancel(self):
        await self._work.aclose()


class _ReadOnlyEvent:
    """A read-only wrapper around :class:`asyncio.Event`.

    Provides access to check if an event is set and wait for it to be
    set, but prevents external code from setting or clearing the event.

    :param event:
        The underlying :class:`asyncio.Event` to wrap.
    """

    def __init__(self, event: asyncio.Event):
        self._event = event

    def is_set(self) -> bool:
        """Check if the event is set.

        :returns:
            ``True`` if the event is set, ``False`` otherwise.
        """
        return self._event.is_set()

    async def wait(self) -> None:
        """Wait until the underlying event is set."""
        await self._event.wait()


class WorkerService(protocol.WorkerServicer):
    """gRPC service for task execution.

    Implements the worker gRPC interface for receiving and executing
    tasks. Runs tasks in the current asyncio event loop and streams
    results back to the client.

    Handles graceful shutdown by rejecting new tasks while allowing
    in-flight tasks to complete. Exposes :attr:`stopping` and
    :attr:`stopped` events for lifecycle monitoring.

    :param backpressure:
        Optional admission control hook. See
        :class:`BackpressureLike`. ``None`` (default) accepts all
        tasks unconditionally.
    """

    _docket: set[_Task | _AsyncGen]
    _stopped: asyncio.Event
    _stopping: asyncio.Event
    _task_completed: asyncio.Event
    _loop_pool: ResourcePool[tuple[asyncio.AbstractEventLoop, threading.Thread]]

    def __init__(self, *, backpressure: BackpressureLike | None = None):
        self._stopped = asyncio.Event()
        self._stopping = asyncio.Event()
        self._task_completed = asyncio.Event()
        self._docket = set()
        self._backpressure = backpressure
        self._loop_pool = ResourcePool(
            factory=self._create_worker_loop,
            finalizer=self._destroy_worker_loop,
            ttl=0,
        )

    @property
    def stopping(self) -> _ReadOnlyEvent:
        """Read-only event signaling that the service is stopping.

        :returns:
            A :class:`_ReadOnlyEvent`.
        """
        return _ReadOnlyEvent(self._stopping)

    @property
    def stopped(self) -> _ReadOnlyEvent:
        """Read-only event signaling that the service has stopped.

        :returns:
            A :class:`_ReadOnlyEvent`.
        """
        return _ReadOnlyEvent(self._stopped)

    async def dispatch(
        self,
        request_iterator: AsyncIterator[protocol.Request],
        context: ServicerContext,
    ) -> AsyncIterator[protocol.Response]:
        """Execute a task in the current event loop.

        Reads the first :class:`~wool.protocol.Request` from
        the bidirectional stream to obtain the :class:`Task`, then
        schedules it for execution. For async generators, subsequent
        ``Message`` frames are forwarded into the generator via
        ``asend()``.

        **Context serialization failures (worker-side).**
        Wire context is **ancillary state** under wool's protocol
        contract: a failure to serialize the post-run snapshot or to
        deserialize an incoming context (initial request or
        mid-stream frame) is non-fatal in non-strict mode but fatal
        in strict mode. Both modes emit a
        :class:`wool.ContextDecodeWarning` for each failure.

        *Non-strict mode (default).* The routine still runs — with a
        fresh empty context as fallback when initial-frame
        deserialization fails — and the back-propagated snapshot is
        replaced with an empty context when post-run serialization
        fails. A snapshot serialization failure that coincides with
        a routine exception rides back as peers in a
        :class:`BaseExceptionGroup` (extending an existing group
        when the routine exception is already grouped) rather than
        as nested causes, so the caller observes both signals at
        the same level.

        *Strict mode* (e.g.,
        ``PYTHONWARNINGS=error::wool.ContextDecodeWarning``). The
        warning promotes to an exception. The dispatch handler
        catches :class:`wool.ContextDecodeWarning` raised before the
        routine starts and ships it via the routine-exception
        channel — the routine does not run — so the caller catches
        the same warning class symmetrically with caller-side strict
        mode rather than seeing a generic gRPC error. Promotions
        raised after the routine starts surface through the
        existing routine-exception machinery.

        :param request_iterator:
            The incoming bidirectional request stream.
        :param context:
            The :class:`grpc.aio.ServicerContext` for this request.
        :yields:
            First yields an Ack Response when task processing begins,
            then yields Response(s) containing the task result(s).

        """
        if self._stopping.is_set():
            await context.abort(
                StatusCode.UNAVAILABLE, "Worker service is shutting down"
            )

        request = await anext(aiter(request_iterator))

        # Resolve the handler's serializer once. Self-dispatch
        # acquires a per-task PassthroughSerializer from the
        # process-wide pool; the caller side acquires the same
        # instance from the same key, so prune-at-loads on either
        # side bounds the shared keep-alive set. The pool's
        # reference-counted cleanup evicts the entry when both sides
        # release, so error-path keep-alive entries are not retained
        # across dispatch boundaries.
        task_id = UUID(request.task.id)
        is_passthrough = False
        if request.task.HasField("serializer"):
            sniff = _unpickle_serializer(request.task.serializer)
            is_passthrough = isinstance(sniff, PassthroughSerializer)

        async with AsyncExitStack() as stack:
            serializer: Serializer
            if is_passthrough:
                serializer = await stack.enter_async_context(
                    _passthrough_pool.get(task_id)
                )
            else:
                serializer = wool.__serializer__

            # Build the caller-state Context first, then briefly
            # install it as the active scope so any pickled
            # wool.ContextVar/Token in the task's args/kwargs
            # reconstitutes against worker_context rather than lazily
            # registering a Context on the dispatch handler. The
            # attach is detached before the worker task is created,
            # so the handler scope does not retain ownership of a
            # Context the worker loop will later mutate.
            #
            # Context decode is best-effort: a decode failure does
            # not preempt the dispatch. The task still runs, with a
            # fresh empty Context as fallback, and a
            # ContextDecodeWarning surfaces the inconsistency.
            try:
                try:
                    worker_context = Context.from_protobuf(
                        request.context, serializer=serializer
                    )
                except BaseExceptionGroup as eg:
                    raise BaseExceptionGroup(
                        "request context decode failed",
                        list(eg.exceptions),
                    ) from None

                with attached(worker_context, guarded=False):
                    wool_task = Task.from_protobuf(request.task)
            except Exception as e:
                # Wool-layer decode failures (strict-mode
                # ContextDecodeWarning, cloudpickle errors on the
                # task callable, ImportError on a missing module on
                # the worker) ride the response-frame exception
                # channel. The protobuf parsed cleanly, so the wire
                # envelope is acknowledged; the cloudpickle layer
                # then surfaces the failure to the caller as the
                # actual exception class — the same shape a failed
                # nested dispatch would take.
                yield protocol.Response(ack=protocol.Ack(version=protocol.__version__))
                yield protocol.Response(
                    exception=protocol.Message(
                        dump=_safely_serialize_exception(serializer, e)
                    ),
                )
                return

            if self._backpressure is not None:
                backpressure = self._backpressure
                with attached(worker_context):
                    decision = backpressure(
                        BackpressureContext(
                            active_task_count=len(self._docket),
                            task=wool_task,
                        )
                    )
                    if isawaitable(decision):
                        decision = await decision
                if decision:
                    await context.abort(
                        StatusCode.RESOURCE_EXHAUSTED,
                        "Task rejected by backpressure hook",
                    )

            with self._tracker(
                wool_task,
                request_iterator,
                worker_context,
                serializer=serializer,
            ) as task:
                ack = protocol.Ack(version=protocol.__version__)
                yield protocol.Response(ack=ack)
                try:
                    if isasyncgen(task):
                        try:
                            async for outcome in task:
                                yield _response_from_outcome(outcome, serializer)
                                if outcome.exception is not None:
                                    return
                        finally:
                            # Close the streaming generator on every
                            # exit path so its own ``finally`` drains
                            # the worker-loop task before control
                            # returns. Without this the generator is
                            # abandoned mid-yield on the early-return
                            # and exception paths, leaving the worker
                            # free to mutate ``worker_context`` while the
                            # fallback branch below snapshots it.
                            await task.aclose()
                    elif isinstance(task, asyncio.Task):
                        outcome: _WorkerOutcome = await task
                        yield _response_from_outcome(outcome, serializer)
                except (Exception, asyncio.CancelledError) as e:
                    # Handler-level failure or cancellation. Routine
                    # exceptions never reach this branch — they are
                    # reified into outcome frames upstream by
                    # ``_outcome_from``, which also absorbs
                    # snapshot-encode failures into the same outcome.
                    # ``worker_context`` is safe to read here: the worker-
                    # loop task has finished mutating it before
                    # control reaches this except clause.
                    try:
                        wire_context = worker_context.to_protobuf(serializer=serializer)
                    except Exception as encode_exc:
                        # Handler-level exception coincided with a
                        # snapshot encode failure; merge them as
                        # peers and ship the group through the
                        # exception channel with no context patch.
                        merged = _merge_exceptions(e, encode_exc)
                        yield protocol.Response(
                            exception=protocol.Message(
                                dump=_safely_serialize_exception(serializer, merged)
                            ),
                        )
                    else:
                        yield protocol.Response(
                            exception=protocol.Message(
                                dump=_safely_serialize_exception(serializer, e)
                            ),
                            context=wire_context,
                        )

    async def stop(
        self,
        request: protocol.StopRequest,
        context: ServicerContext | None,
    ) -> protocol.Void:
        """Stop the worker service and its thread.

        Gracefully shuts down the worker thread and signals the server
        to stop accepting new requests. This method is idempotent and
        can be called multiple times safely.

        :param request:
            The protobuf stop request containing the wait timeout.
        :param context:
            The :class:`grpc.aio.ServicerContext` for this request.
        :returns:
            An empty protobuf response indicating completion.
        """
        if self._stopping.is_set():
            return protocol.Void()
        await self._stop(timeout=request.timeout)
        return protocol.Void()

    @staticmethod
    def _create_worker_loop(
        key,
    ) -> tuple[asyncio.AbstractEventLoop, threading.Thread]:
        """Create a new event loop running on a dedicated daemon thread.

        :param key:
            The :class:`ResourcePool` cache key (unused).
        :returns:
            A tuple of the event loop and the thread running it.
        """
        loop = asyncio.new_event_loop()
        install_task_factory(loop)
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        return loop, thread

    @staticmethod
    def _destroy_worker_loop(
        loop_thread: tuple[asyncio.AbstractEventLoop, threading.Thread],
    ) -> None:
        """Stop the worker event loop and join its thread.

        Cancels all pending tasks on the worker loop before stopping,
        ensuring cleanup code runs in each task's own context.

        :param loop_thread:
            A tuple of the event loop and the thread running it.
        """
        loop, thread = loop_thread

        async def _shutdown():
            current = asyncio.current_task()
            tasks = [t for t in asyncio.all_tasks() if t is not current and not t.done()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            loop.stop()

        loop.call_soon_threadsafe(lambda: loop.create_task(_shutdown()))
        thread.join(timeout=5)
        loop.close()

    async def _run_on_worker(
        self,
        work_task: Task,
        worker_context: Context,
        serializer: Serializer,
    ) -> _WorkerOutcome:
        """Run a task on the shared worker event loop.

        :param work_task:
            The :class:`Task` instance to execute.
        :param worker_context:
            The dispatch handler's local :class:`~wool.runtime.context.Context`,
            passed by reference. The worker-loop task is the
            exclusive mutator for the duration of the routine, so
            the done-callback's snapshot reflects the post-run state
            for the response frame.
        :param serializer:
            Negotiated serializer for response var snapshots.
        """
        future: concurrent.futures.Future = concurrent.futures.Future()
        worker_task = None

        async with self._loop_pool.get("worker") as (worker_loop, _):

            def _schedule():
                nonlocal worker_task
                task = worker_loop.create_task(
                    work_task._run(),
                    context=worker_context,  # pyright: ignore[reportArgumentType]
                )
                worker_task = task

                def _done(t: asyncio.Task):
                    if future.done():
                        return
                    if t.cancelled():
                        future.cancel()
                        return
                    try:
                        routine_exc = t.exception()
                        outcome = _outcome_from(
                            value=t.result() if routine_exc is None else None,
                            routine_exc=routine_exc,
                            context=worker_context,
                            serializer=serializer,
                        )
                        future.set_result(outcome)
                    except BaseException as e:  # pragma: no cover
                        # Defensive outer guard: any unanticipated
                        # escape from the body above still resolves
                        # ``future`` so the dispatch handler does
                        # not hang on ``wrap_future``.
                        if not future.done():
                            future.set_exception(e)

                task.add_done_callback(_done)

            worker_loop.call_soon_threadsafe(_schedule)

            try:
                return await asyncio.wrap_future(future)
            except asyncio.CancelledError:
                if worker_task is not None:
                    worker_loop.call_soon_threadsafe(worker_task.cancel)
                    # Wait for the worker task to unwind before letting
                    # CancelledError bubble up. This guarantees that by
                    # the time the dispatch handler's fallback path
                    # reads worker_context._data, the worker loop is no
                    # longer mutating it — no cross-loop race on the
                    # dict iterator. Swallow any outcome from this
                    # follow-up await; the worker's result is not used
                    # on the cancellation path.
                    try:
                        await asyncio.wrap_future(future)
                    except (Exception, asyncio.CancelledError):
                        pass
                raise

    async def _stream_from_worker(
        self,
        work_task: Task,
        request_iterator: AsyncIterator[protocol.Request],
        worker_context: Context,
        serializer: Serializer,
    ):
        """Run a streaming task on the shared worker event loop.

        Offloads async generator execution to a dedicated worker event
        loop. Client requests (``next``, ``send``, ``throw``) are read
        from *request_iterator* on the main loop, forwarded to the
        worker loop via a queue, and the resulting values are returned
        to the main loop for yielding.

        Each frame is surfaced as a :class:`_WorkerOutcome`. Success
        frames carry ``result`` and ``context``; an exception raised
        by the routine yields a terminal frame with ``exception`` and
        ``context`` populated — the caller is responsible for
        emitting the error Response and terminating the stream. The
        generator does not raise routine errors itself so the
        worker-side context snapshot taken at the mutation point is
        never dropped.

        :param work_task:
            The :class:`Task` instance containing an async
            generator.
        :param request_iterator:
            The incoming bidirectional request stream for reading
            client-driven iteration commands.
        :param worker_context:
            The dispatch handler's local :class:`~wool.runtime.context.Context`,
            passed by reference. Main never scopes it against its own
            task after the backpressure hook returns, so the
            worker-loop task is the exclusive mutator for the
            generator's lifetime. Every per-frame snapshot reflects
            its live state.
        :param serializer:
            Negotiated serializer for response var snapshots and
            Message bodies.
        :yields:
            :class:`_WorkerOutcome` frames — one per successful
            routine yield, plus one terminal error frame if the
            routine raises.
        """
        main_loop = asyncio.get_running_loop()
        request_queue: asyncio.Queue = asyncio.Queue()
        result_queue: asyncio.Queue = asyncio.Queue()
        worker_done: concurrent.futures.Future = concurrent.futures.Future()

        async with self._loop_pool.get("worker") as (worker_loop, _):

            async def worker_dispatch():
                proxy_pool = wool.__proxy_pool__.get()
                proxy_context_manager = (
                    proxy_pool.get(work_task.proxy) if proxy_pool else None
                )
                proxy = (
                    await proxy_context_manager.__aenter__()
                    if proxy_context_manager
                    else None
                )
                try:
                    token = wool.__proxy__.set(proxy) if proxy else None
                    try:
                        assert work_task.runtime_context is not None
                        with work_task.runtime_context, work_task:
                            # RuntimeContext restores dispatch_timeout from the
                            # wire; Task.__enter__ sets _current_task for
                            # nested dispatch. Both __enter__ calls run once
                            # here but the generator below is driven across
                            # many ``command`` loop iterations — dispatch_timeout
                            # and _current_task therefore remain set for the
                            # full generator lifespan, matching the coroutine
                            # path's single-enter contract via ``Task._run``.
                            gen = work_task.callable(*work_task.args, **work_task.kwargs)

                            try:
                                while True:
                                    command = await request_queue.get()
                                    if command is _STREAM_END:
                                        break
                                    action, payload, caller_wire_context = cast(
                                        tuple[
                                            Literal["next", "send", "throw"],
                                            Any,
                                            protocol.Context,
                                        ],
                                        command,
                                    )
                                    # Outer guard: every exit from this
                                    # iteration body must push to result_queue
                                    # so the main loop's await never hangs on
                                    # a silently-failed worker task.
                                    try:
                                        try:
                                            incoming_context = Context.from_protobuf(
                                                caller_wire_context,
                                                serializer=serializer,
                                            )
                                        except BaseExceptionGroup as eg:
                                            raise BaseExceptionGroup(
                                                "mid-stream request context "
                                                "decode failed",
                                                list(eg.exceptions),
                                            ) from None
                                        if incoming_context.has_state():
                                            worker_context.update(incoming_context)
                                        try:
                                            with do_dispatch(False):
                                                match action:
                                                    case "next":
                                                        value = await gen.asend(None)
                                                    case "send":
                                                        value = await gen.asend(payload)
                                                    case "throw":
                                                        value = await gen.athrow(
                                                            type(payload), payload
                                                        )
                                                    case _:  # pragma: no cover
                                                        assert_never(action)
                                            routine_exc: BaseException | None = None
                                        except StopAsyncIteration:
                                            main_loop.call_soon_threadsafe(
                                                result_queue.put_nowait, _STREAM_END
                                            )
                                            return
                                        except BaseException as e:
                                            routine_exc = e
                                            value = None
                                        outcome = _outcome_from(
                                            value=value,
                                            routine_exc=routine_exc,
                                            context=worker_context,
                                            serializer=serializer,
                                        )
                                        main_loop.call_soon_threadsafe(
                                            result_queue.put_nowait, outcome
                                        )
                                        if routine_exc is not None:
                                            return
                                    except BaseException as e:
                                        # Anything escaping the iteration
                                        # body's except clauses (e.g.
                                        # Context.update raising) still
                                        # surfaces as a terminal error frame.
                                        _log.warning(
                                            "Aborting streaming dispatch on "
                                            "unhandled error: %s",
                                            e,
                                        )
                                        outcome = _outcome_from(
                                            routine_exc=e,
                                            context=worker_context,
                                            serializer=serializer,
                                        )
                                        main_loop.call_soon_threadsafe(
                                            result_queue.put_nowait, outcome
                                        )
                                        return
                            finally:
                                try:
                                    await gen.aclose()
                                except (asyncio.CancelledError, GeneratorExit):
                                    # During shutdown the aclose() may be
                                    # cancelled or exit before the generator
                                    # finishes its own teardown. Log and
                                    # swallow so cleanup continues.
                                    _log.warning(
                                        "wool routine generator interrupted during "
                                        "aclose on teardown",
                                        exc_info=True,
                                    )
                    finally:
                        if token is not None:
                            wool.__proxy__.reset(token)
                finally:
                    if proxy_context_manager is not None:
                        await proxy_context_manager.__aexit__(None, None, None)

            def _start_worker():
                task = worker_loop.create_task(
                    worker_dispatch(),
                    context=worker_context,  # pyright: ignore[reportArgumentType]
                )

                def _on_done(t: asyncio.Task):
                    if not worker_done.done():
                        if t.cancelled():
                            worker_done.cancel()
                        else:
                            exc = t.exception()
                            if exc is not None:
                                worker_done.set_exception(exc)
                            else:
                                worker_done.set_result(None)
                    # Wake any pending ``result_queue.get()`` so the
                    # main loop can observe worker termination — the
                    # happy path arrives via the outcome already
                    # pushed; the setup-failure path needs this nudge
                    # to escape an otherwise-indefinite await on an
                    # outcome that will never arrive.
                    main_loop.call_soon_threadsafe(result_queue.put_nowait, _STREAM_END)

                task.add_done_callback(_on_done)

            worker_loop.call_soon_threadsafe(_start_worker)

            streamed_outcome = False
            try:
                async for request in request_iterator:
                    caller_wire_context = request.context
                    try:
                        match request.WhichOneof("payload"):
                            case "next":
                                worker_loop.call_soon_threadsafe(
                                    request_queue.put_nowait,
                                    ("next", None, caller_wire_context),
                                )
                            case "send":
                                # Decode under ``attached(worker_context)`` so any
                                # pickled ``wool.ContextVar``/``Token`` in the
                                # payload reconstitutes against ``worker_context``
                                # rather than lazily registering a Context on
                                # the dispatch handler's transient task.
                                # Mirrors the initial-frame discipline at
                                # ``Task.from_protobuf`` above. Decode plumbing
                                # — opt out of the single-task guard since the
                                # worker task already holds it on ``worker_context``.
                                with attached(worker_context, guarded=False):
                                    value = serializer.loads(request.send.dump)
                                worker_loop.call_soon_threadsafe(
                                    request_queue.put_nowait,
                                    ("send", value, caller_wire_context),
                                )
                            case "throw":
                                with attached(worker_context, guarded=False):
                                    exc = serializer.loads(request.throw.dump)
                                worker_loop.call_soon_threadsafe(
                                    request_queue.put_nowait,
                                    ("throw", exc, caller_wire_context),
                                )
                            case (
                                _
                            ):  # pragma: no cover — defensive default for proto oneof
                                continue
                    except Exception as e:  # pragma: no cover
                        # Defensive guard for a mid-stream send/throw
                        # decode failure: the wool-layer cloudpickle
                        # decode of the payload failed. Ship via
                        # the response-frame exception channel so the
                        # caller observes the actual decode exception
                        # rather than a generic gRPC error; the outer
                        # ``finally`` pushes ``_STREAM_END`` to tear
                        # down ``worker_dispatch``.
                        try:
                            snapshot = worker_context.to_protobuf(serializer=serializer)
                        except Exception as encode_exc:
                            merged = _merge_exceptions(e, encode_exc)
                            yield protocol.Response(
                                exception=protocol.Message(
                                    dump=serializer.dumps(merged)
                                ),
                            )
                        else:
                            yield protocol.Response(
                                exception=protocol.Message(dump=serializer.dumps(e)),
                                context=snapshot,
                            )
                        return

                    result = await result_queue.get()

                    if result is _STREAM_END:
                        break
                    # Worker loop puts ``_WorkerOutcome`` on the queue
                    # directly (built via ``_outcome_from``). A terminal
                    # error frame rides back as the outcome's exception
                    # field; the dispatch handler reads it from the
                    # yielded outcome and short-circuits the stream,
                    # so this generator does not need its own check.
                    yield result
                    streamed_outcome = True
            finally:
                worker_loop.call_soon_threadsafe(request_queue.put_nowait, _STREAM_END)
                # Wait for ``worker_dispatch`` to consume ``_STREAM_END``
                # and finish unwinding (user generator's ``aclose`` plus
                # proxy cleanup) before this generator returns. After
                # this await, no worker-loop task is mutating
                # ``worker_context``, so the dispatch handler's fallback path
                # can snapshot it without racing a cross-loop writer.
                # Mirrors the coroutine path's drain in ``_run_on_worker``.
                try:
                    await asyncio.wrap_future(worker_done)
                except asyncio.CancelledError:
                    pass
                except Exception as worker_exc:
                    if streamed_outcome:
                        # Teardown failure after the primary signal
                        # already reached the caller; surface only as
                        # a log to avoid double-framing the gRPC
                        # stream with a trailing exception frame.
                        _log.warning(
                            "wool worker teardown failed after streaming completion",
                            exc_info=worker_exc,
                        )
                    else:
                        # Worker raised before any outcome was
                        # streamed (e.g., proxy/runtime-context setup
                        # raised pre-loop). Surface as a terminal
                        # ``_WorkerOutcome`` so the dispatch handler
                        # ships it through the exception channel and
                        # the caller observes the actual failure
                        # rather than a hang.
                        yield _outcome_from(
                            routine_exc=worker_exc,
                            context=worker_context,
                            serializer=serializer,
                        )

    @contextmanager
    def _tracker(
        self,
        work_task: Task,
        request_iterator: AsyncIterator[protocol.Request],
        worker_context: Context,
        serializer: Serializer,
    ):
        """Context manager for tracking running tasks.

        Manages the lifecycle of a task execution, adding it to the
        active tasks set. Ensures proper cleanup when the task
        completes or fails.

        :param work_task:
            The :class:`Task` instance to execute and track.
        :param request_iterator:
            The incoming bidirectional request stream.
        :param worker_context:
            The dispatch handler's local :class:`~wool.runtime.context.Context`,
            passed by reference. Since main does not scope it against
            its own task after backpressure, the worker-loop task is
            the exclusive mutator — no cross-loop race.
        :param serializer:
            Negotiated serializer for var snapshots and Message
            bodies. Cloudpickle for cross-process dispatch;
            :class:`PassthroughSerializer` for self-dispatch.
        :yields:
            The :class:`asyncio.Task` or async generator for the
            Wool task.

        """
        if iscoroutinefunction(work_task.callable):
            task = asyncio.create_task(
                self._run_on_worker(work_task, worker_context, serializer)
            )
            watcher = _Task(task)
        elif isasyncgenfunction(work_task.callable):
            task = self._stream_from_worker(
                work_task, request_iterator, worker_context, serializer
            )
            watcher = _AsyncGen(task)
        else:
            raise ValueError("Expected coroutine function or async generator function")

        self._docket.add(watcher)
        try:
            yield task
        finally:
            self._docket.discard(watcher)

    async def _stop(self, *, timeout: float | None = 0) -> None:
        if timeout is not None and timeout < 0:
            timeout = None
        self._stopping.set()
        await self._await_or_cancel(timeout=timeout)
        try:
            if proxy_pool := wool.__proxy_pool__.get():
                await proxy_pool.clear()
            from wool.runtime.discovery import __subscriber_pool__

            if subscriber_pool := __subscriber_pool__.get():
                await subscriber_pool.clear()
        finally:
            await self._loop_pool.clear()
            self._stopped.set()

    async def _await_or_cancel(self, *, timeout: float | None = 0) -> None:
        """Drain or cancel in-flight tasks in the docket.

        Waits for running tasks to complete or cancels them depending
        on the timeout value.

        :param timeout:
            Maximum time to wait for tasks to complete. If 0 (default),
            tasks are canceled immediately. If None, waits indefinitely.
            If a positive number, waits for that many seconds before
            canceling tasks.

        .. note::
            If a timeout occurs while waiting for tasks to complete,
            the method recursively calls itself with a timeout of 0
            to cancel all remaining tasks immediately.
        """
        if self._docket and timeout == 0:
            await self._cancel()
        elif self._docket:
            try:
                await asyncio.wait_for(self._await(), timeout=timeout)
            except asyncio.TimeoutError:
                return await self._await_or_cancel(timeout=0)

    async def _await(self):
        while self._docket:
            await asyncio.sleep(0)

    async def _cancel(self):
        """Cancel all tracked tasks in the docket.

        Cancels every entry in :attr:`_docket` and waits for them to
        finish, handling cancellation exceptions gracefully.
        """
        await asyncio.gather(*(w.cancel() for w in self._docket), return_exceptions=True)
