from __future__ import annotations

import asyncio
import logging
import traceback
from collections.abc import Callable
from contextvars import Context
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from sys import modules
from types import ModuleType
from types import TracebackType
from typing import Coroutine
from typing import Dict
from typing import ParamSpec
from typing import Protocol
from typing import SupportsInt
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4

import wool
from wool._event import TaskEvent
from wool._future import Future
from wool._pool import WorkerPoolSession

AsyncCallable = Callable[..., Coroutine]
C = TypeVar("C", bound=AsyncCallable)

Args = Tuple
Kwargs = Dict
Timeout = SupportsInt
Timestamp = SupportsInt


# PUBLIC
def task(fn: C) -> C:
    """
    A decorator to declare an asynchronous function as remotely executable by a
    worker pool. When the wrapped function is invoked, it is dispatched to the
    worker pool associated with the current worker pool session context.

    Tasks behave like coroutines, meaning they can be awaited as well as
    cancelled.

    :param fn: The task function.
    :return: A Wool task declaration.

    **Best practices and considerations for designing tasks:**

    1. **Picklability**:
        - Task arguments and return values must be picklable, as they are
            serialized and transferred between processes. Avoid passing
            unpicklable objects such as open file handles, database
            connections, or lambda functions.
        - Ensure that any custom objects used as arguments or return values
            implement the necessary methods for pickling (e.g.,
            ``__getstate__`` and ``__setstate__``).

    2. **Synchronization**:
        - Tasks are not guaranteed to execute on the same process between
            invocations. Each invocation may run on a different worker process.
        - Standard ``asyncio`` synchronization primitives (e.g.,
            ``asyncio.Lock``) will not behave as expected in a multi-process
            environment, as they are designed for single-process applications.
            Use the specialized ``wool.locking`` synchronization primitives to
            achieve inter-worker and inter-pool synchronization.

    3. **Statelessness and idempotency**:
        - Design tasks to be stateless and idemptoent. Avoid relying on global
            variables or shared mutable state. This ensures predictable
            behavior, avoids race conditions, and enables safe retries.

    4. **Cancellation**:
        - Task cancellation and propagation thereof mimics that of standard
            Python coroutines.

    5. **Error propagation**:
        - Wool makes every effort to execute tasks transparently to the user,
            and this includes error propagation. Unhandled exceptions raised
            within a task will be propagated to the caller as they would
            normally.

    6. **Performance**:
        - Minimize the size of arguments and return values to reduce
            serialization overhead.
        - For large datasets, consider using shared memory or passing
            references (e.g., file paths) instead of transferring the entire
            data.

    **Usage**::

    .. code-block:: python

        import wool


        @wool.task
        async def foo(...):
            ...
    """

    @wraps(fn)
    def wrapper(
        *args,
        __wool_remote__: bool = False,
        __wool_session__: WorkerPoolSession | None = None,
        **kwargs,
    ) -> Coroutine:
        # Handle static and class methods in a picklable way.
        parent, function = _resolve(fn)
        assert parent is not None
        assert callable(function)

        if __wool_remote__:
            # The caller is a worker, run the task locally...
            return _execute(fn, parent, *args, **kwargs)
        else:
            # Otherwise, submit the task to the pool.
            return _put(
                __wool_session__ or wool.__wool_session__.get(),
                wrapper.__module__,
                wrapper.__qualname__,
                function,
                *args,
                **kwargs,
            )

    return cast(C, wrapper)


def _put(
    session: WorkerPoolSession,
    module: str,
    qualname: str,
    function: AsyncCallable,
    *args,
    **kwargs,
) -> Coroutine:
    if not session.connected:
        session.connect()

    # Skip self argument if function is a method.
    _args = args[1:] if hasattr(function, "__self__") else args
    signature = ", ".join(
        (
            *(repr(v) for v in _args),
            *(f"{k}={repr(v)}" for k, v in kwargs.items()),
        )
    )

    # We don't want the remote worker to resubmit this task to the
    # pool, so we set the `__wool_remote__` flag to true.
    kwargs["__wool_remote__"] = True

    task = Task(
        id=uuid4(),
        callable=function,
        args=args,
        kwargs=kwargs,
        tag=f"{module}.{qualname}({signature})",
    )
    assert isinstance(session, WorkerPoolSession)
    future: Future = session.put(task)

    @wraps(function)
    async def coroutine(future: Future):
        try:
            while not future.done():
                await asyncio.sleep(0)
            else:
                return future.result()
        except ConnectionResetError as e:
            raise asyncio.CancelledError from e
        except asyncio.CancelledError:
            if not future.done():
                logging.debug("Cancelling...")
                future.cancel()
            raise

    return coroutine(future)


def _execute(fn: AsyncCallable, parent, *args, **kwargs):
    if isinstance(fn, classmethod):
        return fn.__func__(parent, *args, **kwargs)
    else:
        return fn(*args, **kwargs)


# PUBLIC
def current_task() -> Task | None:
    """
    Get the current task from the context variable if we are inside a task
    context, otherwise return None.

    :return: The current task or None if no task is active.
    """
    return _current_task.get()


# PUBLIC
@dataclass
class Task:
    """
    Represents a task to be executed in the worker pool.

    :param id: The unique identifier for the task.
    :param callable: The asynchronous function to execute.
    :param args: Positional arguments for the function.
    :param kwargs: Keyword arguments for the function.
    :param timeout: The timeout for the task in seconds.
        Defaults to 0 (no timeout).
    :param caller: The ID of the calling task, if any.
    :param exception: The exception raised during task execution, if any.
    :param filename: The filename where the task was defined.
    :param function: The name of the function being executed.
    :param line_no: The line number where the task was defined.
    :param tag: An optional tag for the task.
    """

    id: UUID
    callable: AsyncCallable
    args: Args
    kwargs: Kwargs
    timeout: Timeout = 0
    caller: UUID | None = None
    exception: TaskException | None = None
    filename: str | None = None
    function: str | None = None
    line_no: int | None = None
    tag: str | None = None

    def __post_init__(self, **kwargs):
        """
        Initialize the task and emit a "task-created" event.

        :param kwargs: Additional keyword arguments.
        """
        if caller := _current_task.get():
            self.caller = caller.id
        TaskEvent("task-created", task=self).emit()

    def __enter__(self) -> Callable[[], Coroutine]:
        """
        Enter the context of the task.

        :return: The task's run method.
        """
        logging.debug(f"Entering {self.__class__.__name__} with ID {self.id}")
        return self.run

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: TracebackType | None,
    ):
        logging.debug(f"Exiting {self.__class__.__name__} with ID {self.id}")
        if exception_value:
            this = asyncio.current_task()
            assert this
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

    def _finish(self, _):
        TaskEvent("task-completed", task=self).emit()

    def run(self) -> Coroutine:
        """
        Execute the task's callable with its arguments.

        :return: A coroutine representing the task execution.
        """
        work = self._with_task(self.callable)
        return work(*self.args, **self.kwargs)

    def _with_task(self, fn: AsyncCallable) -> AsyncCallable:
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            with self:
                token = _current_task.set(self)
                # Yield to event loop with context set
                await asyncio.sleep(0)
                result = await fn(*args, **kwargs)
                _current_task.reset(token)
                return result

        return wrapper


# PUBLIC
@dataclass
class TaskException:
    """
    Represents an exception raised during task execution.

    :param type: The type of the exception.
    :param traceback: The traceback of the exception.
    """

    type: str
    traceback: list[str]


# PUBLIC
class TaskEventCallback(Protocol):
    """
    Protocol for WoolTaskEvent callback functions.
    """

    def __call__(self, event: TaskEvent, timestamp: Timestamp) -> None: ...


_current_task: ContextVar[Task | None] = ContextVar(
    "_current_task", default=None
)


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
