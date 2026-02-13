from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import threading
from contextlib import contextmanager
from inspect import isasyncgen
from inspect import isasyncgenfunction
from inspect import iscoroutinefunction
from typing import AsyncGenerator
from typing import AsyncIterator

import cloudpickle
from grpc import StatusCode
from grpc.aio import ServicerContext

import wool
from wool.runtime import protobuf as pb
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.work.task import Task
from wool.runtime.work.task import WorkTaskEvent

# Sentinel to mark end of async generator stream
_SENTINEL = object()


class _Task:
    def __init__(self, work: asyncio.Task):
        self._work = work

    async def cancel(self):
        self._work.cancel()
        await self._work


class _AsyncGen:
    def __init__(self, work: AsyncGenerator):
        self._work = work

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


class WorkerService(pb.worker.WorkerServicer):
    """gRPC service for task execution.

    Implements the worker gRPC interface for receiving and executing
    tasks. Runs tasks in the current asyncio event loop and streams
    results back to the client.

    Handles graceful shutdown by rejecting new tasks while allowing
    in-flight tasks to complete. Exposes :attr:`stopping` and
    :attr:`stopped` events for lifecycle monitoring.
    """

    _docket: set[_Task | _AsyncGen]
    _stopped: asyncio.Event
    _stopping: asyncio.Event
    _task_completed: asyncio.Event
    _loop_pool: ResourcePool[tuple[asyncio.AbstractEventLoop, threading.Thread]]

    def __init__(self):
        self._stopped = asyncio.Event()
        self._stopping = asyncio.Event()
        self._task_completed = asyncio.Event()
        self._docket = set()
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
        self, request: pb.task.Task, context: ServicerContext
    ) -> AsyncIterator[pb.worker.Response]:
        """Execute a task in the current event loop.

        Deserializes the incoming task into a :class:`Task`
        instance, schedules it for execution in the current asyncio
        event loop, and yields responses for acknowledgment and result.

        :param request:
            The protobuf task message containing the serialized task
            data.
        :param context:
            The :class:`grpc.aio.ServicerContext` for this request.
        :yields:
            First yields an Ack Response when task processing begins,
            then yields a Response containing the task result.

        .. note::
            Emits a :class:`WorkTaskEvent` when the task is
            scheduled for execution.
        """
        if self._stopping.is_set():
            await context.abort(
                StatusCode.UNAVAILABLE, "Worker service is shutting down"
            )

        with self._tracker(Task.from_protobuf(request)) as work:
            yield pb.worker.Response(ack=pb.worker.Ack())
            try:
                if isasyncgen(work):
                    async for result in work:
                        result = pb.task.Result(dump=cloudpickle.dumps(result))
                        yield pb.worker.Response(result=result)
                elif isinstance(work, asyncio.Task):
                    result = pb.task.Result(dump=cloudpickle.dumps(await work))
                    yield pb.worker.Response(result=result)
            except (Exception, asyncio.CancelledError) as e:
                exception = pb.task.Exception(dump=cloudpickle.dumps(e))
                yield pb.worker.Response(exception=exception)

    async def stop(
        self, request: pb.worker.StopRequest, context: ServicerContext | None
    ) -> pb.worker.Void:
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
            return pb.worker.Void()
        await self._stop(timeout=request.timeout)
        return pb.worker.Void()

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

    async def _stream_from_worker(self, work_task: Task):
        """Run a streaming task on the shared worker event loop.

        Offloads async generator execution to a dedicated worker event loop
        and streams results back to the main event loop via an async queue.
        Context variables are propagated from the calling context to the worker loop.

        :param work_task:
            The :class:`Task` instance containing an async
            generator.
        :yields:
            Values yielded by the async generator, streamed from
            the worker loop.
        """
        main_loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        result_queue: asyncio.Queue = asyncio.Queue()
        exception_holder: list = [None]

        async with self._loop_pool.get("worker") as (worker_loop, _):

            async def collect_results():
                try:
                    async for value in work_task._stream():
                        main_loop.call_soon_threadsafe(result_queue.put_nowait, value)
                except Exception as e:
                    exception_holder[0] = e
                finally:
                    if main_loop.is_running():
                        main_loop.call_soon_threadsafe(
                            result_queue.put_nowait, _SENTINEL
                        )

            coro = collect_results()
            worker_loop.call_soon_threadsafe(
                lambda: worker_loop.create_task(coro, context=ctx)
            )

            while True:
                value = await result_queue.get()
                if value is _SENTINEL:
                    break
                yield value

            if exception_holder[0]:
                raise exception_holder[0]

    @contextmanager
    def _tracker(self, work_task: Task):
        """Context manager for tracking running tasks.

        Manages the lifecycle of a task execution, adding it to the
        active tasks set and emitting appropriate events. Ensures
        proper cleanup when the task completes or fails.

        Tasks are executed in a thread pool to prevent blocking the
        main gRPC event loop, allowing the worker to remain responsive
        to health checks and new task dispatches.

        :param work_task:
            The :class:`Task` instance to execute and track.
        :yields:
            The :class:`asyncio.Task` or async generator for the wool task.

        .. note::
            Emits a :class:`WorkTaskEvent` with type "task-scheduled"
            when the task begins execution.
        """
        WorkTaskEvent("task-scheduled", task=work_task).emit()

        if iscoroutinefunction(work_task.callable):
            # Regular async function -> run on worker loop
            work = asyncio.create_task(self._run_on_worker(work_task))
            watcher = _Task(work)
        elif isasyncgenfunction(work_task.callable):
            # Async generator -> stream from worker loop via queue
            work = self._stream_from_worker(work_task)
            watcher = _AsyncGen(work)
        else:
            raise ValueError("Expected coroutine function or async generator function")

        self._docket.add(watcher)
        try:
            yield work
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
        """Cancel multiple tasks safely.

        Cancels the provided tasks while performing safety checks to
        avoid canceling the current task or already completed tasks.
        Waits for all cancelled tasks to complete in parallel and handles
        cancellation exceptions.

        :param tasks:
            The :class:`asyncio.Task` instances to cancel.

        .. note::
            This method performs the following safety checks:
            - Avoids canceling the current task (would cause deadlock)
            - Only cancels tasks that are not already done
            - Properly handles :exc:`asyncio.CancelledError`
              exceptions.
        """
        await asyncio.gather(*(w.cancel() for w in self._docket), return_exceptions=True)
