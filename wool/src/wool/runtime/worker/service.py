from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
import uuid
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
from typing import Protocol
from typing import runtime_checkable

import cloudpickle
from grpc import StatusCode
from grpc.aio import ServicerContext

import wool
from wool import protocol
from wool.runtime.context import Context
from wool.runtime.context import _task_contexts
from wool.runtime.context import _task_contexts_lock
from wool.runtime.context import apply_vars
from wool.runtime.context import dumps
from wool.runtime.context import resolve_context
from wool.runtime.context import snapshot_vars
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.routine.task import PassthroughSerializer
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import _unpickle_serializer
from wool.runtime.routine.task import do_dispatch
from wool.runtime.worker import namespace as _namespace

_log = logging.getLogger(__name__)

# Sentinel to mark end of async generator stream
_STREAM_END: Final = object()


@dataclass
class _WorkerOutcome:
    """Result of a worker-loop task execution.

    Always carries the post-run context snapshot so the handler can
    ship caller-visible mutations on the Response regardless of
    whether the routine returned normally or raised.
    """

    result: Any = None
    exception: BaseException | None = None
    snapshot: dict[str, bytes] = field(default_factory=dict)


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

        # Extract the context id from the first Request, unpickle
        # the Task, then activate the id and apply the caller's
        # wire-shipped var snapshot to the handler's context. The
        # snapshot is keyed by each var's "namespace:name";
        # apply_vars resolves each key via the process-wide
        # ContextVar registry and calls .set() on the local instance.
        context_hex = request.context_id
        context_id = uuid.UUID(hex=context_hex) if context_hex else uuid.uuid4()

        work_task = Task.from_protobuf(request.task)

        _is_passthrough = False
        if request.task.HasField("serializer"):
            _ser = _unpickle_serializer(request.task.serializer)
            _is_passthrough = isinstance(_ser, PassthroughSerializer)

        response_context_hex = context_hex or context_id.hex
        with _namespace.activate(context_id):
            if request.vars:
                apply_vars(
                    dict(request.vars),
                    loads_param=(
                        PassthroughSerializer.loads if _is_passthrough else None
                    ),
                )

            if self._backpressure is not None:
                decision = self._backpressure(
                    BackpressureContext(
                        active_task_count=len(self._docket),
                        task=work_task,
                    )
                )
                if isawaitable(decision):
                    decision = await decision
                if decision:
                    await context.abort(
                        StatusCode.RESOURCE_EXHAUSTED,
                        "Task rejected by backpressure hook",
                    )

            handler_ctx = resolve_context()
            # Message bodies use the negotiated serializer for parity
            # with vars and the Task payload. The PassthroughSerializer
            # instance must outlive every dumps call (it strong-refs
            # the stored objects via self._keys), so bind it to a local
            # that lives for the duration of the handler.
            _msg_ser = PassthroughSerializer() if _is_passthrough else None
            _msg_dumps = _msg_ser.dumps if _msg_ser else dumps
            with self._tracker(
                work_task,
                request_iterator,
                handler_ctx,
                passthrough=_is_passthrough,
            ) as task:
                ack = protocol.Ack(version=protocol.__version__)
                yield protocol.Response(ack=ack)
                try:
                    if isasyncgen(task):
                        async for outcome in task:
                            if outcome.exception is not None:
                                exception = protocol.Message(
                                    dump=_msg_dumps(outcome.exception)
                                )
                                yield protocol.Response(
                                    exception=exception,
                                    vars=outcome.snapshot,
                                    context_id=response_context_hex,
                                )
                                return
                            result = protocol.Message(dump=_msg_dumps(outcome.result))
                            yield protocol.Response(
                                result=result,
                                vars=outcome.snapshot,
                                context_id=response_context_hex,
                            )
                    elif isinstance(task, asyncio.Task):
                        outcome: _WorkerOutcome = await task
                        if outcome.exception is not None:
                            exception = protocol.Message(
                                dump=_msg_dumps(outcome.exception)
                            )
                            yield protocol.Response(
                                exception=exception,
                                vars=outcome.snapshot,
                                context_id=response_context_hex,
                            )
                        else:
                            result = protocol.Message(dump=_msg_dumps(outcome.result))
                            yield protocol.Response(
                                result=result,
                                vars=outcome.snapshot,
                                context_id=response_context_hex,
                            )
                except (Exception, asyncio.CancelledError) as e:
                    # Cancellation of the awaited coroutine task or
                    # an unexpected handler-level failure bubbles
                    # here. Routine errors from the streaming path
                    # are surfaced through the outcome frame above,
                    # so this branch does not see them. Best-effort
                    # snapshot from the handler context since no
                    # frame-level snapshot is available at this point.
                    exception = protocol.Message(dump=_msg_dumps(e))
                    _fallback_ser = PassthroughSerializer() if _is_passthrough else None
                    ctx_snapshot = snapshot_vars(
                        dumps_param=_fallback_ser.dumps if _fallback_ser else None
                    )
                    yield protocol.Response(
                        exception=exception,
                        vars=ctx_snapshot,
                        context_id=response_context_hex,
                    )

    async def stop(
        self, request: protocol.StopRequest, context: ServicerContext | None
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
        from wool.runtime.context import install_task_factory

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
        handler_ctx: Context,
        passthrough: bool = False,
    ) -> _WorkerOutcome:
        """Run a task on the shared worker event loop.

        :param work_task:
            The :class:`Task` instance to execute.
        :param handler_ctx:
            The handler's :class:`~wool.runtime.context.Context`,
            captured before ``_tracker`` forks a worker task.
            Preserves the activated context id and any vars applied
            by the dispatch handler so the worker-loop copy starts
            from the correct snapshot.
        :param passthrough:
            If ``True``, use :class:`PassthroughSerializer` for
            response var snapshots (self-dispatch optimization that
            avoids redundant cloudpickle round-trips).
        """
        worker_ctx = handler_ctx.copy()
        future: concurrent.futures.Future = concurrent.futures.Future()
        worker_task = None
        _resp_ser = PassthroughSerializer() if passthrough else None

        async with self._loop_pool.get("worker") as (worker_loop, _):

            def _schedule():
                nonlocal worker_task
                task = asyncio.Task(work_task._run(), loop=worker_loop)
                worker_task = task
                with _task_contexts_lock:
                    _task_contexts[task] = worker_ctx

                def _done(t: asyncio.Task):
                    if future.done():
                        return
                    if t.cancelled():
                        future.cancel()
                        return
                    snapshot = snapshot_vars(
                        worker_ctx,
                        dumps_param=_resp_ser.dumps if _resp_ser else None,
                    )
                    exc = t.exception()
                    if exc is not None:
                        future.set_result(
                            _WorkerOutcome(exception=exc, snapshot=snapshot)
                        )
                    else:
                        future.set_result(
                            _WorkerOutcome(result=t.result(), snapshot=snapshot)
                        )

                task.add_done_callback(_done)

            worker_loop.call_soon_threadsafe(_schedule)

            try:
                return await asyncio.wrap_future(future)
            except asyncio.CancelledError:
                if worker_task is not None:
                    worker_loop.call_soon_threadsafe(worker_task.cancel)
                raise

    async def _stream_from_worker(
        self,
        work_task: Task,
        request_iterator: AsyncIterator[protocol.Request],
        handler_ctx: Context,
        passthrough: bool = False,
    ):
        """Run a streaming task on the shared worker event loop.

        Offloads async generator execution to a dedicated worker event
        loop. Client requests (``next``, ``send``, ``throw``) are read
        from *request_iterator* on the main loop, forwarded to the
        worker loop via a queue, and the resulting values are returned
        to the main loop for yielding.

        Each frame is surfaced as a :class:`_WorkerOutcome`. Success
        frames carry ``result`` and ``snapshot``; an exception raised
        by the routine yields a terminal frame with ``exception`` and
        ``snapshot`` populated — the caller is responsible for
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
        :param handler_ctx:
            The handler's :class:`~wool.runtime.context.Context`,
            captured before the tracker forks. Preserves the
            activated context id and applied vars so the worker-loop
            copy starts from the correct snapshot.
        :param passthrough:
            If ``True``, use :class:`PassthroughSerializer` for
            response var snapshots (self-dispatch optimization).
        :yields:
            :class:`_WorkerOutcome` frames — one per successful
            routine yield, plus one terminal error frame if the
            routine raises.
        """
        main_loop = asyncio.get_running_loop()
        worker_ctx = handler_ctx.copy()
        _resp_ser = PassthroughSerializer() if passthrough else None
        _respdumps = _resp_ser.dumps if _resp_ser else None
        _msg_loads = PassthroughSerializer.loads if passthrough else cloudpickle.loads
        request_queue: asyncio.Queue = asyncio.Queue()
        result_queue: asyncio.Queue = asyncio.Queue()

        async with self._loop_pool.get("worker") as (worker_loop, _):

            async def worker_dispatch():
                proxy_pool = wool.__proxy_pool__.get()
                proxy_ctx = proxy_pool.get(work_task.proxy) if proxy_pool else None
                proxy = await proxy_ctx.__aenter__() if proxy_ctx else None
                token = wool.__proxy__.set(proxy) if proxy else None
                try:
                    with work_task:  # sets _current_task for nested dispatch
                        gen = work_task.callable(*work_task.args, **work_task.kwargs)
                        try:
                            while True:
                                cmd = await request_queue.get()
                                if cmd is _STREAM_END:
                                    break
                                action, payload, caller_ctx = cmd
                                if caller_ctx:
                                    apply_vars(
                                        caller_ctx,
                                        loads_param=(
                                            PassthroughSerializer.loads
                                            if passthrough
                                            else None
                                        ),
                                    )
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
                                    # resolve_context() resolves to
                                    # worker_ctx here because _start_worker
                                    # registered the task in _task_contexts.
                                    ctx_snapshot = snapshot_vars(dumps_param=_respdumps)
                                    main_loop.call_soon_threadsafe(
                                        result_queue.put_nowait,
                                        ("error", e, ctx_snapshot),
                                    )
                                    return
                                else:
                                    # resolve_context() resolves to
                                    # worker_ctx here because _start_worker
                                    # registered the task in _task_contexts.
                                    ctx_snapshot = snapshot_vars(dumps_param=_respdumps)
                                    main_loop.call_soon_threadsafe(
                                        result_queue.put_nowait,
                                        ("value", value, ctx_snapshot),
                                    )
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
                task = asyncio.Task(worker_dispatch(), loop=worker_loop)
                with _task_contexts_lock:
                    _task_contexts[task] = worker_ctx

            worker_loop.call_soon_threadsafe(_start_worker)

            try:
                async for request in request_iterator:
                    caller_ctx = dict(request.vars) if request.vars else {}
                    match request.WhichOneof("payload"):
                        case "next":
                            worker_loop.call_soon_threadsafe(
                                request_queue.put_nowait,
                                ("next", None, caller_ctx),
                            )
                        case "send":
                            value = _msg_loads(request.send.dump)
                            worker_loop.call_soon_threadsafe(
                                request_queue.put_nowait,
                                ("send", value, caller_ctx),
                            )
                        case "throw":
                            exc = _msg_loads(request.throw.dump)
                            worker_loop.call_soon_threadsafe(
                                request_queue.put_nowait,
                                ("throw", exc, caller_ctx),
                            )
                        case _:
                            continue

                    result = await result_queue.get()

                    if result is _STREAM_END:
                        break
                    tag, payload, ctx_snapshot = result
                    if tag == "error":
                        # Terminal error frame: surface the exception
                        # alongside the worker-side snapshot so the
                        # caller can ship worker mutations on the
                        # error Response. Do not raise — raising would
                        # let the snapshot fall on the floor and the
                        # dispatch handler's fallback snapshot never
                        # saw the worker's mutations.
                        yield _WorkerOutcome(exception=payload, snapshot=ctx_snapshot)
                        return
                    yield _WorkerOutcome(result=payload, snapshot=ctx_snapshot)
            finally:
                worker_loop.call_soon_threadsafe(request_queue.put_nowait, _STREAM_END)

    @contextmanager
    def _tracker(
        self,
        work_task: Task,
        request_iterator: AsyncIterator[protocol.Request],
        handler_ctx: Context,
        passthrough: bool = False,
    ):
        """Context manager for tracking running tasks.

        Manages the lifecycle of a task execution, adding it to the
        active tasks set. Ensures proper cleanup when the task
        completes or fails.

        :param work_task:
            The :class:`Task` instance to execute and track.
        :param request_iterator:
            The incoming bidirectional request stream.
        :param handler_ctx:
            The handler's :class:`~wool.runtime.context.Context`,
            captured before the tracker forks a worker task. Preserves
            the activated context id and any vars applied by the
            dispatch handler so the worker-loop copy starts from the
            correct snapshot.
        :param passthrough:
            If ``True``, use :class:`PassthroughSerializer` for var
            snapshots on response frames (self-dispatch optimization
            that avoids redundant cloudpickle round-trips).
        :yields:
            The :class:`asyncio.Task` or async generator for the
            wool task.

        """
        if iscoroutinefunction(work_task.callable):
            task = asyncio.create_task(
                self._run_on_worker(work_task, handler_ctx, passthrough)
            )
            watcher = _Task(task)
        elif isasyncgenfunction(work_task.callable):
            task = self._stream_from_worker(
                work_task, request_iterator, handler_ctx, passthrough
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
        on the timeout value. Called by :meth:`_stop` as part of the
        shutdown sequence.

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
