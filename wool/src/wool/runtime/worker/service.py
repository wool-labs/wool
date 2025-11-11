from __future__ import annotations

import asyncio
from contextlib import contextmanager
from typing import AsyncIterator

import cloudpickle
from grpc import StatusCode
from grpc.aio import ServicerContext

import wool
from wool.runtime import protobuf as pb
from wool.runtime.work import WorkTask
from wool.runtime.work import WorkTaskEvent


class ReadOnlyEvent:
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

    _tasks: set[asyncio.Task]
    _stopped: asyncio.Event
    _stopping: asyncio.Event
    _task_completed: asyncio.Event

    def __init__(self):
        self._stopped = asyncio.Event()
        self._stopping = asyncio.Event()
        self._task_completed = asyncio.Event()
        self._tasks = set()

    @property
    def stopping(self) -> ReadOnlyEvent:
        """Read-only event signaling that the service is stopping.

        :returns:
            A :class:`ReadOnlyEvent`.
        """
        return ReadOnlyEvent(self._stopping)

    @property
    def stopped(self) -> ReadOnlyEvent:
        """Read-only event signaling that the service has stopped.

        :returns:
            A :class:`ReadOnlyEvent`.
        """
        return ReadOnlyEvent(self._stopped)

    async def dispatch(
        self, request: pb.task.Task, context: ServicerContext
    ) -> AsyncIterator[pb.worker.Response]:
        """Execute a task in the current event loop.

        Deserializes the incoming task into a :class:`WorkTask`
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

        with self._tracker(WorkTask.from_protobuf(request)) as task:
            try:
                yield pb.worker.Response(ack=pb.worker.Ack())
                try:
                    result = pb.task.Result(dump=cloudpickle.dumps(await task))
                    yield pb.worker.Response(result=result)
                except Exception as e:
                    exception = pb.task.Exception(dump=cloudpickle.dumps(e))
                    yield pb.worker.Response(exception=exception)
            except asyncio.CancelledError as e:
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

    @contextmanager
    def _tracker(self, wool_task: WorkTask):
        """Context manager for tracking running tasks.

        Manages the lifecycle of a task execution, adding it to the
        active tasks set and emitting appropriate events. Ensures
        proper cleanup when the task completes or fails.

        :param wool_task:
            The :class:`WorkTask` instance to execute and track.
        :yields:
            The :class:`asyncio.Task` created for the wool task.

        .. note::
            Emits a :class:`WorkTaskEvent` with type "task-scheduled"
            when the task begins execution.
        """
        WorkTaskEvent("task-scheduled", task=wool_task).emit()
        task = asyncio.create_task(wool_task.run())
        self._tasks.add(task)
        try:
            yield task
        finally:
            self._tasks.remove(task)

    async def _stop(self, *, timeout: float | None = 0) -> None:
        self._stopping.set()
        await self._await_or_cancel_tasks(timeout=timeout)
        try:
            if proxy_pool := wool.__proxy_pool__.get():
                await proxy_pool.clear()
        finally:
            self._stopped.set()

    async def _await_or_cancel_tasks(self, *, timeout: float | None = 0) -> None:
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
        if self._tasks and timeout == 0:
            await self._cancel(*self._tasks)
        elif self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                return await self._await_or_cancel_tasks(timeout=0)

    async def _cancel(self, *tasks: asyncio.Task):
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
        current = asyncio.current_task()
        to_cancel = [task for task in tasks if not task.done() and task != current]
        for task in to_cancel:
            task.cancel()
        if to_cancel:
            await asyncio.gather(*to_cancel, return_exceptions=True)
