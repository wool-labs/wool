from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
from inspect import isasyncgen
from inspect import isasyncgenfunction
from inspect import iscoroutinefunction
from typing import Any
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Awaitable
from typing import Final
from typing import Protocol
from typing import assert_never
from typing import cast
from typing import runtime_checkable

import cloudpickle
from grpc import StatusCode
from grpc.aio import ServicerContext

import wool
from wool import protocol
from wool.runtime.context import Context
from wool.runtime.context import carries_state
from wool.runtime.context import create_bound_task
from wool.runtime.context import current_context
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

        work_task = Task.from_protobuf(request.task)

        # Resolve the handler's serializer once. Self-dispatch
        # acquires a per-task PassthroughSerializer from the
        # process-wide pool; the caller side acquires the same
        # instance from the same key, so prune-at-loads on either
        # side bounds the shared keep-alive set. The pool's
        # reference-counted cleanup evicts the entry when both sides
        # release, so error-path keep-alive entries are not retained
        # across dispatch boundaries.
        is_passthrough = False
        if request.task.HasField("serializer"):
            sniff = _unpickle_serializer(request.task.serializer)
            is_passthrough = isinstance(sniff, PassthroughSerializer)
        serializer: Serializer
        if is_passthrough:
            serializer = await _passthrough_pool.acquire(work_task.id)
        else:
            serializer = cast(Serializer, cloudpickle)

        try:
            # Build the caller-state Context as a local object on the
            # main handler task. It is *not* installed in the scope
            # registry here — main loop must not own a Context that
            # the worker loop will later mutate. The backpressure
            # hook scopes work_ctx transiently via ``Context.run`` so
            # hooks that read wool.ContextVars see the caller's
            # state; after the handoff, ``create_bound_task``
            # registers work_ctx against the worker task and the
            # worker loop becomes the exclusive mutator.
            try:
                work_ctx = Context.from_protobuf(request.context, serializer=serializer)
            except ValueError as e:
                _log.warning("Rejecting dispatch with corrupt context payload: %s", e)
                await context.abort(
                    StatusCode.INVALID_ARGUMENT,
                    f"Context payload is corrupt: {e}",
                )
                assert_never(  # pragma: no cover
                    "ServicerContext.abort returned unexpectedly"
                )

            if self._backpressure is not None:

                def _evaluate_backpressure():
                    return self._backpressure(
                        BackpressureContext(
                            active_task_count=len(self._docket),
                            task=work_task,
                        )
                    )

                decision = await work_ctx.run_async(_evaluate_backpressure)
                if decision:
                    await context.abort(
                        StatusCode.RESOURCE_EXHAUSTED,
                        "Task rejected by backpressure hook",
                    )

            with self._tracker(
                work_task,
                request_iterator,
                work_ctx,
                serializer=serializer,
            ) as task:
                ack = protocol.Ack(version=protocol.__version__)
                yield protocol.Response(ack=ack)
                try:
                    if isasyncgen(task):
                        async for outcome in task:
                            yield _response_from_outcome(outcome, serializer)
                            if outcome.exception is not None:
                                return
                    elif isinstance(task, asyncio.Task):
                        outcome: _WorkerOutcome = await task
                        yield _response_from_outcome(outcome, serializer)
                except (Exception, asyncio.CancelledError) as e:
                    # Cancellation of the awaited coroutine task or
                    # an unexpected handler-level failure bubbles
                    # here. Routine errors from the streaming path
                    # are surfaced through the outcome frame above,
                    # so this branch does not see them. work_ctx is
                    # safe to read here because the tracker's cancel
                    # path has drained the worker task — no loop is
                    # mutating it at this point.
                    yield protocol.Response(
                        exception=protocol.Message(dump=serializer.dumps(e)),
                        context=_snapshot_with_fallback(
                            work_ctx, serializer=serializer, primary_exc=e
                        ),
                    )
        finally:
            if is_passthrough:
                await _passthrough_pool.release(work_task.id)

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
        work_ctx: Context,
        serializer: Serializer,
    ) -> _WorkerOutcome:
        """Run a task on the shared worker event loop.

        :param work_task:
            The :class:`Task` instance to execute.
        :param work_ctx:
            The dispatch handler's local :class:`~wool.runtime.context.Context`,
            passed by reference. Main never scopes it against its own
            task after the backpressure hook returns, so the worker-loop
            task is the exclusive mutator for the duration of the
            routine. The done-callback's snapshot reflects the
            post-run state for the response frame.
        :param serializer:
            Negotiated serializer for response var snapshots.
        """
        future: concurrent.futures.Future = concurrent.futures.Future()
        worker_task = None

        async with self._loop_pool.get("worker") as (worker_loop, _):

            def _schedule():
                nonlocal worker_task
                task = create_bound_task(worker_loop, work_task._run(), work_ctx)
                worker_task = task

                def _done(t: asyncio.Task):
                    if future.done():
                        return
                    if t.cancelled():
                        future.cancel()
                        return
                    ctx_pb = work_ctx.to_protobuf(serializer=serializer)
                    exc = t.exception()
                    if exc is not None:
                        future.set_result(_WorkerOutcome(exception=exc, context=ctx_pb))
                    else:
                        future.set_result(
                            _WorkerOutcome(result=t.result(), context=ctx_pb)
                        )

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
                    # reads work_ctx._data, the worker loop is no
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
        work_ctx: Context,
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
        :param work_ctx:
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

        async with self._loop_pool.get("worker") as (worker_loop, _):

            async def worker_dispatch():
                proxy_pool = wool.__proxy_pool__.get()
                proxy_ctx = proxy_pool.get(work_task.proxy) if proxy_pool else None
                proxy = await proxy_ctx.__aenter__() if proxy_ctx else None
                token = wool.__proxy__.set(proxy) if proxy else None
                try:
                    assert work_task.context is not None
                    with work_task.context, work_task:
                        # RuntimeContext restores dispatch_timeout from the
                        # wire; Task.__enter__ sets _current_task for
                        # nested dispatch. Both __enter__ calls run once
                        # here but the generator below is driven across
                        # many ``cmd`` loop iterations — dispatch_timeout
                        # and _current_task therefore remain set for the
                        # full generator lifespan, matching the coroutine
                        # path's single-enter contract via ``Task._run``.
                        # Load-bearing for the regression guard in #176.
                        gen = work_task.callable(*work_task.args, **work_task.kwargs)

                        def _snapshot_or_empty() -> protocol.Context:
                            # Best-effort snapshot for error frames — if
                            # the snapshot itself raises (e.g. a worker
                            # mutation produced an unpicklable value), we
                            # ship an empty Context rather than re-enter
                            # the failure that brought us here.
                            try:
                                return current_context().to_protobuf(
                                    serializer=serializer
                                )
                            except Exception:
                                return protocol.Context()

                        try:
                            while True:
                                cmd = await request_queue.get()
                                if cmd is _STREAM_END:
                                    break
                                action, payload, caller_ctx = cmd
                                # Outer guard: every exit from this
                                # iteration body must push to result_queue
                                # so the main loop's await never hangs on
                                # a silently-failed worker task.
                                try:
                                    if caller_ctx is not None and carries_state(
                                        caller_ctx
                                    ):
                                        try:
                                            incoming = Context.from_protobuf(
                                                caller_ctx, serializer=serializer
                                            )
                                        except ValueError as e:
                                            # Forward-prop precise: caller's
                                            # context payload failed to decode.
                                            _log.warning(
                                                "Rejecting mid-stream frame "
                                                "with corrupt context: %s",
                                                e,
                                            )
                                            main_loop.call_soon_threadsafe(
                                                result_queue.put_nowait,
                                                ("error", e, _snapshot_or_empty()),
                                            )
                                            return
                                        current_context().update(incoming)
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
                                                case _:
                                                    continue
                                    except StopAsyncIteration:
                                        main_loop.call_soon_threadsafe(
                                            result_queue.put_nowait, _STREAM_END
                                        )
                                        return
                                    except BaseException as e:
                                        tag, outcome_value = "error", e
                                        terminate = True
                                    else:
                                        tag, outcome_value = "value", value
                                        terminate = False
                                    # current_context() resolves to work_ctx
                                    # because _start_worker registered the task
                                    # in the context registry.
                                    try:
                                        ctx_pb = current_context().to_protobuf(
                                            serializer=serializer
                                        )
                                    except TypeError as e:
                                        # Back-prop precise: worker-side
                                        # snapshot failed (e.g. a routine
                                        # set() an unpicklable value).
                                        # Symmetric with the forward-prop
                                        # precise handler above.
                                        _log.warning(
                                            "Aborting mid-stream dispatch: "
                                            "worker-side context snapshot "
                                            "failed: %s",
                                            e,
                                        )
                                        main_loop.call_soon_threadsafe(
                                            result_queue.put_nowait,
                                            ("error", e, protocol.Context()),
                                        )
                                        return
                                    main_loop.call_soon_threadsafe(
                                        result_queue.put_nowait,
                                        (tag, outcome_value, ctx_pb),
                                    )
                                    if terminate:
                                        return
                                except BaseException as e:
                                    # Catch-all: any exception escaping
                                    # the precise handlers above (e.g.
                                    # Context.update raising) still
                                    # surfaces as a terminal error frame.
                                    _log.warning(
                                        "Aborting streaming dispatch on "
                                        "unhandled error: %s",
                                        e,
                                    )
                                    main_loop.call_soon_threadsafe(
                                        result_queue.put_nowait,
                                        ("error", e, _snapshot_or_empty()),
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
                    if proxy_ctx is not None:
                        await proxy_ctx.__aexit__(None, None, None)

            def _start_worker():
                create_bound_task(worker_loop, worker_dispatch(), work_ctx)

            worker_loop.call_soon_threadsafe(_start_worker)

            try:
                async for request in request_iterator:
                    caller_ctx = request.context
                    match request.WhichOneof("payload"):
                        case "next":
                            worker_loop.call_soon_threadsafe(
                                request_queue.put_nowait,
                                ("next", None, caller_ctx),
                            )
                        case "send":
                            value = serializer.loads(request.send.dump)
                            worker_loop.call_soon_threadsafe(
                                request_queue.put_nowait,
                                ("send", value, caller_ctx),
                            )
                        case "throw":
                            exc = serializer.loads(request.throw.dump)
                            worker_loop.call_soon_threadsafe(
                                request_queue.put_nowait,
                                ("throw", exc, caller_ctx),
                            )
                        case _:
                            continue

                    result = await result_queue.get()

                    if result is _STREAM_END:
                        break
                    tag, payload, ctx_pb = result
                    if tag == "error":
                        # Terminal error frame: surface the exception
                        # alongside the worker-side snapshot so the
                        # caller can ship worker mutations on the
                        # error Response. Do not raise — raising would
                        # let the snapshot fall on the floor and the
                        # dispatch handler's fallback snapshot never
                        # saw the worker's mutations.
                        yield _WorkerOutcome(exception=payload, context=ctx_pb)
                        return
                    yield _WorkerOutcome(result=payload, context=ctx_pb)
            finally:
                worker_loop.call_soon_threadsafe(request_queue.put_nowait, _STREAM_END)

    @contextmanager
    def _tracker(
        self,
        work_task: Task,
        request_iterator: AsyncIterator[protocol.Request],
        work_ctx: Context,
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
        :param work_ctx:
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
                self._run_on_worker(work_task, work_ctx, serializer)
            )
            watcher = _Task(task)
        elif isasyncgenfunction(work_task.callable):
            task = self._stream_from_worker(
                work_task, request_iterator, work_ctx, serializer
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
