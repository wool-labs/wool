from __future__ import annotations

import asyncio
import inspect
import logging
import traceback
from collections.abc import Callable
from contextvars import Context
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from sys import modules
from time import perf_counter_ns
from types import ModuleType
from types import TracebackType
from typing import TYPE_CHECKING
from typing import Coroutine
from typing import Dict
from typing import Literal
from typing import ParamSpec
from typing import Protocol
from typing import SupportsInt
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4

import cloudpickle

import wool
from wool import _protobuf as pb
from wool._typing import PassthroughDecorator

if TYPE_CHECKING:
    from wool._worker_proxy import WorkerProxy

AsyncCallable = Callable[..., Coroutine]
C = TypeVar("C", bound=AsyncCallable)

Args = Tuple
Kwargs = Dict
Timeout = SupportsInt
Timestamp = SupportsInt


# public
def work(fn: C) -> C:
    """Decorator to declare an asynchronous function as remotely executable.

    Converts an asynchronous function into a distributed task that can be
    executed by a worker pool. When the decorated function is invoked, it
    is dispatched to the worker pool associated with the current worker
    pool session context.

    :param fn:
        The asynchronous function to convert into a distributed routine.
    :returns:
        The decorated function that dispatches to the worker pool when
        called.
    :raises ValueError:
        If the decorated function is not a coroutine function.

    .. note::
        Decorated functions behave like regular coroutines and can
        be awaited and cancelled normally. Task execution occurs
        transparently across the distributed worker pool.

    Best practices and considerations for designing tasks:

    1. **Picklability**: Arguments and return values must be picklable for
       serialization between processes. Avoid unpicklable objects like
       open file handles, database connections, or lambda functions.
       Custom objects should implement ``__getstate__`` and
       ``__setstate__`` methods if needed.

    2. **Synchronization**: Tasks are not guaranteed to execute on the
       same process between invocations. Standard :mod:`asyncio`
       synchronization primitives will not work across processes.
       Use file-based or other distributed synchronization utilities.

    3. **Statelessness**: Design tasks to be stateless and idempotent.
       Avoid global variables or shared mutable state to ensure
       predictable behavior and enable safe retries.

    4. **Cancellation**: Task cancellation behaves like standard Python
       coroutine cancellation and is properly propagated across the
       distributed system.

    5. **Error propagation**: Unhandled exceptions raised within tasks are
       transparently propagated to the caller as they would be normally.

    6. **Performance**: Minimize argument and return value sizes to reduce
       serialization overhead. For large datasets, consider using shared
       memory or passing references instead of the data itself.

    Example usage:

    .. code-block:: python

        import wool


        @wool.work
        async def fibonacci(n: int) -> int:
            if n <= 1:
                return n
            return await fibonacci(n - 1) + await fibonacci(n - 2)


        async def main():
            async with wool.WorkerPool():
                result = await fibonacci(10)
    """
    if not inspect.iscoroutinefunction(fn):
        raise ValueError("Expected a coroutine function")

    @wraps(fn)
    def wrapper(*args, **kwargs) -> Coroutine:
        # Handle static and class methods in a picklable way.
        parent, function = _resolve(fn)
        assert parent is not None
        assert callable(function)

        if _do_dispatch.get():
            proxy = wool.__proxy__.get()
            assert proxy
            stream = _dispatch(
                proxy,
                wrapper.__module__,
                wrapper.__qualname__,
                function,
                *args,
                **kwargs,
            )
            if inspect.iscoroutinefunction(fn):
                return _stream_to_coroutine(stream)
            else:
                raise ValueError("Expected a coroutine function")
        else:
            return _execute(fn, parent, *args, **kwargs)

    return cast(C, wrapper)


routine = work


def _dispatch(
    proxy: WorkerProxy,
    module: str,
    qualname: str,
    function: AsyncCallable,
    *args,
    **kwargs,
):
    # Skip self argument if function is a method.
    args = args[1:] if hasattr(function, "__self__") else args
    signature = ", ".join(
        (
            *(repr(v) for v in args),
            *(f"{k}={repr(v)}" for k, v in kwargs.items()),
        )
    )
    task = WoolTask(
        id=uuid4(),
        callable=function,
        args=args,
        kwargs=kwargs,
        tag=f"{module}.{qualname}({signature})",
        proxy=proxy,
    )
    return proxy.dispatch(task)


async def _execute(fn: AsyncCallable, parent, *args, **kwargs):
    token = _do_dispatch.set(True)
    try:
        if isinstance(fn, classmethod):
            return await fn.__func__(parent, *args, **kwargs)
        else:
            return await fn(*args, **kwargs)
    finally:
        _do_dispatch.reset(token)


async def _stream_to_coroutine(stream):
    result = None
    async for result in stream:
        continue
    return result


# public
def current_task() -> WoolTask | None:
    """
    Get the current task from the context variable if we are inside a task
    context, otherwise return None.

    :returns:
        The current task or None if no task is active.
    """
    return _current_task.get()


# public
@dataclass
class WoolTask:
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
    exception: WoolTaskException | None = None
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
        WoolTaskEvent("task-created", task=self).emit()

    def __enter__(self) -> Callable[[], Coroutine]:
        """
        Enter the task context for execution.

        :returns:
            The task's run method as a callable coroutine.
        """
        logging.debug(f"Entering {self.__class__.__name__} with ID {self.id}")
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
            self.exception = WoolTaskException(
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
        # Return False to allow exceptions to propagate
        return False

    @classmethod
    def from_protobuf(cls, task: pb.task.Task) -> WoolTask:
        return cls(
            id=UUID(task.id),
            callable=cloudpickle.loads(task.callable),
            args=cloudpickle.loads(task.args),
            kwargs=cloudpickle.loads(task.kwargs),
            caller=UUID(task.caller) if task.caller else None,
            proxy=cloudpickle.loads(task.proxy),
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
        )

    async def run(self) -> Coroutine:
        """
        Execute the task's callable with its arguments in proxy context.

        :returns:
            A coroutine representing the routine execution.
        :raises RuntimeError:
            If no proxy is available for task execution.
        """
        proxy_pool = wool.__proxy_pool__.get()
        assert proxy_pool
        async with proxy_pool.get(self.proxy) as proxy:
            # Set the proxy in context variable for nested task dispatch
            token = wool.__proxy__.set(proxy)
            try:
                work = self._with_self(self.callable)
                return await work(*self.args, **self.kwargs)
            finally:
                wool.__proxy__.reset(token)

    def _finish(self, _):
        WoolTaskEvent("task-completed", task=self).emit()

    def _with_self(self, fn: AsyncCallable) -> AsyncCallable:
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            with self:
                current_task_token = _current_task.set(self)
                # Do not re-submit this task, execute it locally
                local_token = _do_dispatch.set(False)
                # Yield to event loop with context set
                await asyncio.sleep(0)
                try:
                    result = await fn(*args, **kwargs)
                    return result
                finally:
                    _current_task.reset(current_task_token)
                    _do_dispatch.reset(local_token)

        return wrapper


# public
@dataclass
class WoolTaskException:
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
class WoolTaskEvent:
    """
    Represents a lifecycle event for a distributed task.

    Events are emitted at key points during task execution and can be used
    for monitoring, debugging, and performance analysis. Event handlers can
    be registered to respond to specific event types.

    :param type:
        The type of task event (e.g., "task-created", "task-scheduled").
    :param task:
        The :py:class:`WoolTask` instance associated with this event.
    """

    type: WoolTaskEventType
    task: WoolTask

    _handlers: dict[str, list[WoolTaskEventCallback]] = {}

    def __init__(self, type: WoolTaskEventType, /, task: WoolTask) -> None:
        """
        Initialize a WoolTaskEvent instance.

        :param type:
            The type of the task event.
        :param task:
            The :py:class:`WoolTask` instance associated with the event.
        """
        self.type = type
        self.task = task

    @classmethod
    def handler(
        cls, *event_types: WoolTaskEventType
    ) -> PassthroughDecorator[WoolTaskEventCallback]:
        """
        Register a handler function for specific task event types.

        :param event_types:
            The event types to handle.
        :returns:
            A decorator to register the handler function.
        """

        def _handler(
            fn: WoolTaskEventCallback,
        ) -> WoolTaskEventCallback:
            for event_type in event_types:
                cls._handlers.setdefault(event_type, []).append(fn)
            return fn

        return _handler

    def emit(self):
        """
        Emit the task event, invoking all registered handlers for the
        event type.

        Handlers are called with the event instance and a timestamp.

        :raises Exception:
            If any handler raises an exception.
        """
        logging.debug(
            f"Emitting {self.type} event for "
            f"task {self.task.id} "
            f"({self.task.callable.__qualname__})"
        )
        if handlers := self._handlers.get(self.type):
            timestamp = perf_counter_ns()
            for handler in handlers:
                handler(self, timestamp)


# public
WoolTaskEventType = Literal[
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
class WoolTaskEventCallback(Protocol):
    """
    Protocol for WoolTaskEvent callback functions.
    """

    def __call__(self, event: WoolTaskEvent, timestamp: Timestamp) -> None: ...


_do_dispatch: ContextVar[bool] = ContextVar("_do_dispatch", default=True)
_current_task: ContextVar[WoolTask | None] = ContextVar("_current_task", default=None)


def _run(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        if current_task := self._context.get(_current_task):
            WoolTaskEvent("task-started", task=current_task).emit()
            try:
                result = fn(self, *args, **kwargs)
            finally:
                WoolTaskEvent("task-stopped", task=current_task).emit()
            return result
        else:
            return fn(self, *args, **kwargs)

    return wrapper


asyncio.Handle._run = _run(asyncio.Handle._run)


P = ParamSpec("P")
R = TypeVar("R")


def _resolve(
    method: Callable[P, R],
) -> Tuple[Type | ModuleType | None, Callable[P, R]]:
    scope = modules[method.__module__]
    parent = None
    for name in method.__qualname__.split("."):
        parent = scope
        scope = getattr(scope, name)
        assert scope
    assert isinstance(parent, (Type, ModuleType))
    return parent, cast(Callable[P, R], scope)
