from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from inspect import isasyncgen
from inspect import isasyncgenfunction
from inspect import isawaitable
from inspect import iscoroutinefunction
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Awaitable
from typing import Protocol
from typing import runtime_checkable

import cloudpickle
from grpc import StatusCode
from grpc.aio import ServicerContext

import wool
from wool import protocol
from wool.runtime.cache import ReferenceCountedCache
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import do_dispatch

# Sentinel to mark end of async generator stream
_SENTINEL = object()


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
    _loop_pool: ReferenceCountedCache[tuple[asyncio.AbstractEventLoop, threading.Thread]]

    def __init__(self, *, backpressure: BackpressureLike | None = None):
        self._stopped = asyncio.Event()
        self._stopping = asyncio.Event()
        self._task_completed = asyncio.Event()
        self._docket = set()
        self._backpressure = backpressure
        self._loop_pool = ReferenceCountedCache(
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

        response = await anext(aiter(request_iterator))
        work_task = Task.from_protobuf(response.task)

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

        with self._tracker(work_task, request_iterator) as task:
            ack = protocol.Ack(version=protocol.__version__)
            yield protocol.Response(ack=ack)
            try:
                if isasyncgen(task):
                    async for result in task:
                        result = protocol.Message(dump=cloudpickle.dumps(result))
                        yield protocol.Response(result=result)
                elif isinstance(task, asyncio.Task):
                    result = protocol.Message(dump=cloudpickle.dumps(await task))
                    yield protocol.Response(result=result)
            except (Exception, asyncio.CancelledError) as e:
                exception = protocol.Message(dump=cloudpickle.dumps(e))
                yield protocol.Response(exception=exception)

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
            The :class:`ReferenceCountedCache` cache key (unused).
        :returns:
            A tuple of the event loop and the thread running it.
        """
        loop = asyncio.new_event_loop()
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

    async def _run_on_worker(self, work_task: Task):
        """Run a task on the shared worker event loop.

        Offloads task execution to a dedicated worker event loop to
        prevent blocking the main gRPC event loop. Context variables
        are propagated from the calling context to the worker loop.

        :param work_task:
            The :class:`Task` instance to execute.
        :returns:
            The result of the task execution.
        """
        ctx = contextvars.copy_context()
        future: concurrent.futures.Future = concurrent.futures.Future()
        worker_task = None

        async with self._loop_pool.get("worker") as (worker_loop, _):

            def _schedule():
                nonlocal worker_task
                task = worker_loop.create_task(work_task._run(), context=ctx)
                worker_task = task

                def _done(t: asyncio.Task):
                    if future.done():
                        return
                    if t.cancelled():
                        future.cancel()
                    elif t.exception() is not None:
                        future.set_exception(t.exception())
                    else:
                        future.set_result(t.result())

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
    ):
        """Run a streaming task on the shared worker event loop.

        Offloads async generator execution to a dedicated worker event
        loop. Client requests (``next``, ``send``, ``throw``) are read
        from *request_iterator* on the main loop, forwarded to the
        worker loop via a queue, and the resulting values are returned
        to the main loop for yielding.

        :param work_task:
            The :class:`Task` instance containing an async
            generator.
        :param request_iterator:
            The incoming bidirectional request stream for reading
            client-driven iteration commands.
        :yields:
            Values yielded by the async generator, streamed from
            the worker loop.
        """
        main_loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        request_queue: asyncio.Queue = asyncio.Queue()
        result_queue: asyncio.Queue = asyncio.Queue()

        async with self._loop_pool.get("worker") as (worker_loop, _):

            async def worker_dispatch():
                proxy_pool = wool.__proxy_pool__.get()
                proxy_ctx = proxy_pool.get(work_task.proxy) if proxy_pool else None
                proxy = await proxy_ctx.__aenter__() if proxy_ctx else None
                token = wool.__proxy__.set(proxy) if proxy else None
                try:
                    gen = work_task.callable(*work_task.args, **work_task.kwargs)
                    try:
                        while True:
                            cmd = await request_queue.get()
                            if cmd is _SENTINEL:
                                break
                            action, payload = cmd
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
                                    result_queue.put_nowait, _SENTINEL
                                )
                                return
                            except BaseException as e:
                                main_loop.call_soon_threadsafe(
                                    result_queue.put_nowait, ("error", e)
                                )
                                return
                            else:
                                main_loop.call_soon_threadsafe(
                                    result_queue.put_nowait,
                                    ("value", value),
                                )
                    finally:
                        await gen.aclose()
                finally:
                    if token is not None:
                        wool.__proxy__.reset(token)
                    if proxy_ctx is not None:
                        await proxy_ctx.__aexit__(None, None, None)

            worker_loop.call_soon_threadsafe(
                lambda: worker_loop.create_task(worker_dispatch(), context=ctx)
            )

            try:
                async for request in request_iterator:
                    match request.WhichOneof("payload"):
                        case "next":
                            worker_loop.call_soon_threadsafe(
                                request_queue.put_nowait,
                                ("next", None),
                            )
                        case "send":
                            value = cloudpickle.loads(request.send.dump)
                            worker_loop.call_soon_threadsafe(
                                request_queue.put_nowait,
                                ("send", value),
                            )
                        case "throw":
                            exc = cloudpickle.loads(request.throw.dump)
                            worker_loop.call_soon_threadsafe(
                                request_queue.put_nowait,
                                ("throw", exc),
                            )
                        case _:
                            continue

                    result = await result_queue.get()

                    if result is _SENTINEL:
                        break
                    tag, payload = result
                    if tag == "error":
                        raise payload
                    yield payload
            finally:
                worker_loop.call_soon_threadsafe(request_queue.put_nowait, _SENTINEL)

    @contextmanager
    def _tracker(
        self,
        work_task: Task,
        request_iterator: AsyncIterator[protocol.Request],
    ):
        """Context manager for tracking running tasks.

        Manages the lifecycle of a task execution, adding it to the
        active tasks set. Ensures proper cleanup when the task
        completes or fails.

        Tasks are executed in a thread pool to prevent blocking the
        main gRPC event loop, allowing the worker to remain responsive
        to health checks and new task dispatches.

        :param work_task:
            The :class:`Task` instance to execute and track.
        :param request_iterator:
            The incoming bidirectional request stream.
        :yields:
            The :class:`asyncio.Task` or async generator for the
            wool task.

        """
        if iscoroutinefunction(work_task.callable):
            # Regular async function -> run on worker loop
            task = asyncio.create_task(self._run_on_worker(work_task))
            watcher = _Task(task)
        elif isasyncgenfunction(work_task.callable):
            # Async generator -> stream from worker loop via queue
            task = self._stream_from_worker(work_task, request_iterator)
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
        """Stop the worker service gracefully.

        Gracefully shuts down the worker service by canceling or waiting
        for running tasks. This method is idempotent and can be called
        multiple times safely.

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
