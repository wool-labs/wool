from __future__ import annotations

import asyncio
import logging
import traceback
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import Context
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from types import TracebackType
from typing import TYPE_CHECKING
from typing import ContextManager
from typing import Coroutine
from typing import Dict
from typing import Literal
from typing import Protocol
from typing import SupportsInt
from typing import Tuple
from typing import overload
from uuid import UUID

import cloudpickle

import wool
from wool.runtime import protobuf as pb
from wool.runtime.event import Event
from wool.runtime.typing import AsyncCallable

if TYPE_CHECKING:
    from wool.runtime.worker.proxy import WorkerProxy

Args = Tuple
Kwargs = Dict
Timeout = SupportsInt
Timestamp = SupportsInt


_do_dispatch: ContextVar[bool] = ContextVar("_do_dispatch", default=True)


@contextmanager
def _do_dispatch_context_manager(flag: bool, /):
    token = _do_dispatch.set(flag)
    try:
        yield
    finally:
        _do_dispatch.reset(token)


@overload
def do_dispatch() -> bool: ...


@overload
def do_dispatch(flag: bool, /) -> ContextManager[None]: ...


def do_dispatch(flag: bool | None = None, /) -> bool | ContextManager[None]:
    if flag is None:
        return _do_dispatch.get()
    else:
        return _do_dispatch_context_manager(flag)


# public
@dataclass
class WorkTask:
    """
    Represents a distributed task to be executed in the worker pool.

    Each task encapsulates a function call along with its arguments and
    execution context. Tasks are created when decorated functions are
    invoked and contain all necessary information for remote execution
    including serialization, routing, and result handling.

    :param id:
        Unique identifier for this task instance.
    :param callable:
        The asynchronous function to execute.
    :param args:
        Positional arguments for the function.
    :param kwargs:
        Keyword arguments for the function.
    :param proxy:
        Worker proxy for task dispatch and routing.
    :param timeout:
        Task timeout in seconds (0 means no timeout).
    :param caller:
        UUID of the calling task if this is a nested task.
    :param exception:
        Exception information if task execution failed.
    :param filename:
        Source filename where the task was defined.
    :param function:
        Name of the function being executed.
    :param line_no:
        Line number where the task was defined.
    :param tag:
        Optional descriptive tag for the task.
    """

    id: UUID
    callable: AsyncCallable
    args: Args
    kwargs: Kwargs
    proxy: WorkerProxy
    timeout: Timeout = 0
    caller: UUID | None = None
    exception: WorkTaskException | None = None
    filename: str | None = None
    function: str | None = None
    line_no: int | None = None
    tag: str | None = None

    def __post_init__(self, **kwargs):
        """
        Initialize the task and emit a "task-created" event.

        Sets up the task context including caller tracking and event emission
        for monitoring and debugging purposes.

        :param kwargs:
            Additional keyword arguments (unused).
        """
        if caller := _current_task.get():
            self.caller = caller.id
        WorkTaskEvent("task-created", task=self).emit()

    def __enter__(self) -> Callable[[], Coroutine]:
        """
        Enter the task context for execution.

        :returns:
            The task's run method as a callable coroutine.
        """
        logging.debug(f"Entering {self.__class__.__name__} with ID {self.id}")
        self._task_token = _current_task.set(self)
        return self.run

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: TracebackType | None,
    ):
        """Exit the task context and handle any exceptions.

        Captures exception information for later processing and allows
        exceptions to propagate normally for proper error handling.

        :param exception_type:
            Type of exception that occurred, if any.
        :param exception_value:
            Exception instance that occurred, if any.
        :param exception_traceback:
            Traceback of the exception, if any.
        :returns:
            False to allow exceptions to propagate.
        """
        logging.debug(f"Exiting {self.__class__.__name__} with ID {self.id}")
        if exception_value:
            this = asyncio.current_task()
            assert this
            self.exception = WorkTaskException(
                exception_type.__qualname__,
                traceback=[
                    y
                    for x in traceback.format_exception(
                        exception_type, exception_value, exception_traceback
                    )
                    for y in x.split("\n")
                ],
            )
            this.add_done_callback(self._finish, context=Context())
        _current_task.reset(self._task_token)
        # Return False to allow exceptions to propagate
        return False

    @classmethod
    def from_protobuf(cls, task: pb.task.Task) -> WorkTask:
        return cls(
            id=UUID(task.id),
            callable=cloudpickle.loads(task.callable),
            args=cloudpickle.loads(task.args),
            kwargs=cloudpickle.loads(task.kwargs),
            caller=UUID(task.caller) if task.caller else None,
            proxy=cloudpickle.loads(task.proxy),
            timeout=task.timeout if task.timeout else 0,
            filename=task.filename if task.filename else None,
            function=task.function if task.function else None,
            line_no=task.line_no if task.line_no else None,
            tag=task.tag if task.tag else None,
        )

    def to_protobuf(self) -> pb.task.Task:
        return pb.task.Task(
            id=str(self.id),
            callable=cloudpickle.dumps(self.callable),
            args=cloudpickle.dumps(self.args),
            kwargs=cloudpickle.dumps(self.kwargs),
            caller=str(self.caller) if self.caller else "",
            proxy=cloudpickle.dumps(self.proxy),
            proxy_id=str(self.proxy.id),
            timeout=self.timeout if self.timeout else 0,
            filename=self.filename if self.filename else "",
            function=self.function if self.function else "",
            line_no=self.line_no if self.line_no else 0,
            tag=self.tag if self.tag else "",
        )

    async def run(self) -> Coroutine:
        """
        Execute the task's callable with its arguments in proxy context.

        :returns:
            A coroutine representing the routine execution.
        :raises RuntimeError:
            If no proxy pool is available for task execution.
        """
        proxy_pool = wool.__proxy_pool__.get()
        if not proxy_pool:
            raise RuntimeError("No proxy pool available for task execution")
        async with proxy_pool.get(self.proxy) as proxy:
            # Set the proxy in context variable for nested task dispatch
            token = wool.__proxy__.set(proxy)
            try:
                work = self._with_self(self.callable)
                return await work(*self.args, **self.kwargs)
            finally:
                wool.__proxy__.reset(token)

    def _finish(self, _):
        WorkTaskEvent("task-completed", task=self).emit()

    def _with_self(self, fn: AsyncCallable) -> AsyncCallable:
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            with self:
                await asyncio.sleep(0)
                with do_dispatch(False):
                    return await fn(*args, **kwargs)

        return wrapper


# public
@dataclass
class WorkTaskException:
    """
    Represents an exception that occurred during distributed task execution.

    Captures exception information from remote task execution for proper
    error reporting and debugging. The exception details are serialized
    and transmitted back to the calling context.

    :param type:
        Qualified name of the exception class.
    :param traceback:
        List of formatted traceback lines from the exception.
    """

    type: str
    traceback: list[str]


# public
class WorkTaskEvent(Event):
    """
    Represents a lifecycle event for a distributed task.

    Events are emitted at key points during task execution and can be used
    for monitoring, debugging, and performance analysis. Event handlers can
    be registered to respond to specific event types.

    :param type:
        The type of task event (e.g., "task-created", "task-scheduled").
    :param task:
        The :class:`WorkTask` instance associated with this event.
    """

    task: WorkTask

    def __init__(self, type: WorkTaskEventType, /, task: WorkTask) -> None:
        super().__init__(type)
        self.task = task

    def emit(self, context=None):
        """
        Emit the task event, invoking all registered handlers for the
        event type.

        Handlers are called with the event instance, timestamp, and
        optional context metadata.

        :param context:
            Optional metadata passed to handlers (e.g., emitter
            identification).
        """
        logging.debug(
            f"Emitting {self.type} event for "
            f"task {self.task.id} "
            f"({self.task.callable.__qualname__})"
        )
        # Delegate to base class Event.emit()
        super().emit(context)


# public
WorkTaskEventType = Literal[
    "task-created",
    "task-queued",
    "task-scheduled",
    "task-started",
    "task-stopped",
    "task-completed",
]
"""
Defines the types of events that can occur during the lifecycle of a Wool
task.

- "task-created":
    Emitted when a task is created.
- "task-queued":
    Emitted when a task is added to the queue.
- "task-scheduled":
    Emitted when a task is scheduled for execution in a worker's event
    loop.
- "task-started":
    Emitted when a task starts execution.
- "task-stopped":
    Emitted when a task stops execution.
- "task-completed":
    Emitted when a task completes execution.
"""


# public
class WorkTaskEventHandler(Protocol):
    """
    Protocol for WorkTaskEvent callback functions.
    """

    def __call__(
        self, event: WorkTaskEvent, timestamp: Timestamp, context=None
    ) -> None: ...


_current_task: ContextVar[WorkTask | None] = ContextVar("_current_task", default=None)


# public
def current_task() -> WorkTask | None:
    """
    Get the current task from the context variable if we are inside a task
    context, otherwise return None.

    :returns:
        The current task or None if no task is active.
    """
    return _current_task.get()


def _run(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        if current_task := self._context.get(_current_task):
            WorkTaskEvent("task-started", task=current_task).emit()
            try:
                result = fn(self, *args, **kwargs)
            finally:
                WorkTaskEvent("task-stopped", task=current_task).emit()
            return result
        else:
            return fn(self, *args, **kwargs)

    return wrapper


asyncio.Handle._run = _run(asyncio.Handle._run)
