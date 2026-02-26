from __future__ import annotations

import asyncio
import logging
import traceback
import types
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import contextmanager
from contextvars import Context
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from inspect import isasyncgenfunction
from inspect import iscoroutinefunction
from types import TracebackType
from typing import Any
from typing import AsyncGenerator
from typing import ContextManager
from typing import Coroutine
from typing import Dict
from typing import Generic
from typing import Literal
from typing import Protocol
from typing import SupportsInt
from typing import Tuple
from typing import TypeAlias
from typing import TypeVar
from typing import cast
from typing import overload
from uuid import UUID

import cloudpickle

import wool
from wool.runtime import protobuf as pb
from wool.runtime.event import Event
from wool.runtime.resourcepool import ResourcePool

# Sentinel for worker-level globals
WORKER: str = "__worker__"

# Default TTL for namespace cleanup (5 minutes)
NAMESPACE_TTL: float = 300.0


def _create_namespace_globals(namespace: str) -> _IsolatedGlobals:
    """Factory function to create _IsolatedGlobals for a namespace.

    Creates an _IsolatedGlobals that falls back to an empty dict for reads.
    The actual callable's __globals__ aren't available at creation time,
    so reads of module-level names won't work. Named namespaces are primarily
    useful for sharing mutable state between tasks, not for accessing
    module-level imports (those should be accessed via the callable's closure
    or passed as arguments).
    """
    return _IsolatedGlobals({})


# Registry stores shared _IsolatedGlobals instances, keyed by namespace name.
# All tasks using the same namespace share the same _IsolatedGlobals instance,
# so globals set by one task are visible to others.
_namespace_registry: ResourcePool[_IsolatedGlobals] = ResourcePool(
    factory=_create_namespace_globals,
    ttl=NAMESPACE_TTL,
)

Args = Tuple
Kwargs = Dict
Timeout = SupportsInt
Timestamp = SupportsInt
Routine: TypeAlias = Coroutine | AsyncGenerator
W = TypeVar("W", bound=Routine)


class _IsolatedGlobals(dict):
    """A dict subclass that provides overlay semantics for function globals.

    Writes go to this dict directly, while reads fall through to the
    original globals if not found. This enables namespace isolation for
    task execution.

    For named namespaces, the same _IsolatedGlobals instance is shared
    across all tasks using that namespace, so writes are visible to all.

    .. note::
        This type must be a ``dict`` subclass, not a ``MutableMapping``.
        Python's ``STORE_GLOBAL`` bytecode uses ``PyDict_SetItem`` at the
        C level, which requires an actual ``dict`` instance. Using a
        ``MutableMapping`` (like ``ChainMap``) as a function's
        ``__globals__`` would bypass ``__setitem__`` and fail.

    :param original_globals:
        The original function's __globals__ dict to fall back to for reads.
    """

    def __init__(self, original_globals: dict):
        super().__init__()
        self._original = original_globals

    def __getitem__(self, key):
        # First check local overlay
        try:
            return super().__getitem__(key)
        except KeyError:
            pass
        # Then check original globals
        return self._original[key]

    def __contains__(self, key):
        return super().__contains__(key) or key in self._original

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


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
class WorkerProxyLike(Protocol):
    """Protocol defining the interface required by Task for proxy objects.

    This allows both the actual WorkerProxy and test doubles to be used
    with Task without requiring inheritance.
    """

    @property
    def id(self) -> UUID: ...

    async def dispatch(
        self, task: Task, *, timeout: float | None = None
    ) -> AsyncGenerator: ...


# public
@dataclass
class Task(Generic[W]):
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
        Proxy object for task dispatch and routing (satisfies WorkerProxyLike).
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
    :param namespace:
        Controls global namespace isolation for task execution:

        - ``None`` (default): Ephemeral isolated globals. Each invocation
          gets a fresh isolated globals dict, preventing global state
          leakage between task invocations.
        - ``"name"``: Named namespace. Tasks with the same namespace string
          share globals, enabling patterns like ``@lru_cache`` across
          related tasks.
        - ``wool.WORKER``: Worker-level globals. The task runs in the
          shared worker namespace with no isolation.
    """

    id: UUID
    callable: Callable[..., W]
    args: Args
    kwargs: Kwargs
    proxy: WorkerProxyLike
    timeout: Timeout = 0
    caller: UUID | None = None
    exception: TaskException | None = None
    filename: str | None = None
    function: str | None = None
    line_no: int | None = None
    tag: str | None = None
    namespace: str | None = None

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
        TaskEvent("task-created", task=self).emit()

    def __enter__(self) -> Callable[[], Coroutine | AsyncGenerator]:
        """
        Enter the task context for execution.

        :returns:
            The task's run method as a callable coroutine.
        """
        logging.debug(f"Entering {self.__class__.__name__} with ID {self.id}")
        self._task_token = _current_task.set(self)
        if iscoroutinefunction(self.callable):
            return self._run
        elif isasyncgenfunction(self.callable):
            return self._stream
        else:
            raise ValueError("Expected coroutine function or async generator function")

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
        this = asyncio.current_task()
        assert this
        if exception_value:
            self.exception = TaskException(
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
    def from_protobuf(cls, task: pb.task.Task) -> Task:
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
            namespace=task.namespace if task.namespace else None,
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
            timeout=int(self.timeout) if self.timeout else 0,
            filename=self.filename if self.filename else "",
            function=self.function if self.function else "",
            line_no=self.line_no if self.line_no else 0,
            tag=self.tag if self.tag else "",
            namespace=self.namespace if self.namespace else "",
        )

    def dispatch(self) -> W:
        if isasyncgenfunction(self.callable):
            return cast(W, self._stream())
        elif iscoroutinefunction(self.callable):
            return cast(W, self._run())
        else:
            raise ValueError("Expected routine to be coroutine or async generator")

    @asynccontextmanager
    async def _prepare_callable(self) -> AsyncIterator[Callable[..., W]]:
        """Prepare the callable with appropriate globals based on namespace.

        Yields the callable to execute, managing namespace lifecycle for
        shared namespaces.

        :yields:
            The callable configured with the appropriate globals dict.
        """
        if self.namespace is None:
            # Ephemeral: fresh isolated globals each invocation
            callable_globals = _IsolatedGlobals(self.callable.__globals__)
            yield types.FunctionType(
                self.callable.__code__,
                callable_globals,
                self.callable.__name__,
                self.callable.__defaults__,
                self.callable.__closure__,
            )
        elif self.namespace == WORKER:
            # Worker globals: use original directly
            yield self.callable
        else:
            # Shared namespace: use shared globals from pool.
            # All tasks with the same namespace share the same
            # _IsolatedGlobals instance, so STORE_GLOBAL writes
            # are visible across tasks.
            async with _namespace_registry.get(self.namespace) as namespace:
                # Merge this callable's globals into the shared namespace.
                # Keys already in namespace take precedence, allowing
                # imports from different modules to accumulate.
                for key, value in self.callable.__globals__.items():
                    if key not in namespace:
                        namespace[key] = value
                yield types.FunctionType(
                    self.callable.__code__,
                    namespace,
                    self.callable.__name__,
                    self.callable.__defaults__,
                    self.callable.__closure__,
                )

    async def _run(self):
        """
        Execute the task's callable with its arguments in proxy context.

        :returns:
            The result of executing the callable.
        :raises RuntimeError:
            If no proxy pool available for task execution.
        """
        assert iscoroutinefunction(self.callable), "Expected coroutine function"
        proxy_pool = wool.__proxy_pool__.get()
        if not proxy_pool:
            raise RuntimeError("No proxy pool available for task execution")
        async with proxy_pool.get(self.proxy) as proxy:
            # Set the proxy in context variable for nested task dispatch
            token = wool.__proxy__.set(proxy)
            try:
                with self:
                    with do_dispatch(False):
                        await asyncio.sleep(0)  # Release the event loop
                        async with self._prepare_callable() as callable_to_run:
                            return await callable_to_run(*self.args, **self.kwargs)
            finally:
                wool.__proxy__.reset(token)

    async def _stream(self):
        """
        Execute the task's callable with its arguments in proxy context.

        :returns:
            An async generator that yields values from the callable.
        :raises RuntimeError:
            If no proxy pool is available for task execution.
        """
        assert isasyncgenfunction(self.callable), "Expected async generator function"
        proxy_pool = wool.__proxy_pool__.get()
        if not proxy_pool:
            raise RuntimeError("No proxy pool available for task execution")
        async with proxy_pool.get(self.proxy) as proxy:
            await asyncio.sleep(0)
            async with self._prepare_callable() as callable_to_run:
                gen = callable_to_run(*self.args, **self.kwargs)
                try:
                    while True:
                        token = wool.__proxy__.set(proxy)
                        try:
                            with self:
                                with do_dispatch(False):
                                    try:
                                        result = await anext(gen)
                                    except StopAsyncIteration:
                                        break
                        finally:
                            wool.__proxy__.reset(token)
                        yield result
                finally:
                    await gen.aclose()

    def _finish(self, _):
        TaskEvent("task-completed", task=self).emit()


# public
@dataclass
class TaskException:
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
class TaskEvent(Event):
    """
    Represents a lifecycle event for a distributed task.

    Events are emitted at key points during task execution and can be used
    for monitoring, debugging, and performance analysis. Event handlers can
    be registered to respond to specific event types.

    :param type:
        The type of task event (e.g., "task-created", "task-scheduled").
    :param task:
        The :class:`Task` instance associated with this event.
    """

    task: Task

    def __init__(self, type: TaskEventType, /, task: Task) -> None:
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
TaskEventType = Literal[
    "task-created",
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
class TaskEventHandler(Protocol):
    """
    Protocol for :py:class:`TaskEvent` callback functions.
    """

    def __call__(self, event: TaskEvent, timestamp: Timestamp, context=None) -> None: ...


_current_task: ContextVar[Task | None] = ContextVar("_current_task", default=None)


# public
def current_task() -> Task | None:
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
            TaskEvent("task-started", task=current_task).emit()
            try:
                result = fn(self, *args, **kwargs)
            finally:
                TaskEvent("task-stopped", task=current_task).emit()
            return result
        else:
            return fn(self, *args, **kwargs)

    return wrapper


asyncio.Handle._run = _run(asyncio.Handle._run)
