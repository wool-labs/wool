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
from types import TracebackType
from typing import Any
from typing import Coroutine
from typing import ParamSpec
from typing import Protocol
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4

import wool
from wool._event import WoolTaskEvent
from wool._future import WoolFuture
from wool._pool import PoolSession

AsyncCallable = Callable[..., Coroutine]
C = TypeVar("C", bound=AsyncCallable)

Args = tuple
Kwargs = dict
Timeout = int
Timestamp = int


def task(fn: C) -> C:
    """
    A decorator to declare an asynchronous function as remotely executable by a
    worker pool. When the wrapped function is called, it is dispatched to the
    worker pool associated with the current worker pool session.

    :param fn: The function to be decorated.
    :return: The decorated function.
    """

    @wraps(fn)
    def wrapper(
        *args,
        __wool_remote__: bool = False,
        __wool_session__: PoolSession | None = None,
        **kwargs,
    ) -> Coroutine:
        """
        Wrapper function for the task decorator.

        :param args: Positional arguments for the function.
        :param __wool_remote__: Flag indicating if the task is remote.
        :param __wool_session__: The session for the worker pool.
        :param kwargs: Keyword arguments for the function.
        :return: A coroutine representing the task execution.
        """
        # Handle static and class methods in a picklable way.
        parent, function = resolve(fn)
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
    session: PoolSession,
    module: str,
    qualname: str,
    function: AsyncCallable,
    *args,
    **kwargs,
) -> Coroutine:
    """
    Submit a task to the worker pool.

    :param session: The session to use for submitting the task.
    :param module: The module containing the task function.
    :param qualname: The qualified name of the task function.
    :param function: The asynchronous function to execute.
    :param args: Positional arguments for the task function.
    :param kwargs: Keyword arguments for the task function.
    :return: A coroutine representing the task execution.
    """
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

    task = WoolTask(
        id=uuid4(),
        callable=function,
        args=args,
        kwargs=kwargs,
        tag=f"{module}.{qualname}({signature})",
    )
    assert isinstance(session, PoolSession)
    future: WoolFuture = session.put(task)

    async def coroutine(future: WoolFuture):
        """
        Coroutine to handle task execution.

        :param future: The future representing the task.
        :return: The result of the task execution.
        """
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
    """
    Execute a task function with the given arguments.

    :param fn: The asynchronous function to execute.
    :param parent: The parent context or object.
    :param args: Positional arguments for the function.
    :param kwargs: Keyword arguments for the function.
    """
    if isinstance(fn, classmethod):
        return fn.__func__(parent, *args, **kwargs)
    else:
        return fn(*args, **kwargs)


# PUBLIC
def current_task() -> WoolTask | None:
    """
    Get the current task from the context variable if we are inside a task
    context, otherwise return None.

    :return: The current task or None if no task is active.
    """
    return _current_task.get()


# PUBLIC
@dataclass
class WoolTask:
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
    exception: WoolTaskException | None = None
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
        WoolTaskEvent("task-created", task=self).emit()

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

    def _finish(self, _):
        """
        Emit a "task-completed" event when the task finishes.

        :param _: Placeholder for the callback argument.
        """
        WoolTaskEvent("task-completed", task=self).emit()

    def run(self) -> Coroutine:
        """
        Execute the task's callable with its arguments.

        :return: A coroutine representing the task execution.
        """
        work = self._with_task(self.callable)
        return work(*self.args, **self.kwargs)

    def _with_task(self, fn: AsyncCallable) -> AsyncCallable:
        """
        Wrap the task's callable with context management.

        :param fn: The asynchronous function to execute.
        :return: The wrapped function.
        """
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
class WoolTaskEventCallback(Protocol):
    """
    Protocol for WoolTaskEvent callback functions.
    """
    def __call__(self, event: WoolTaskEvent, timestamp: Timestamp) -> None: ...


# PUBLIC
@dataclass
class WoolTaskException:
    """
    Represents an exception raised during task execution.

    :param type: The type of the exception.
    :param traceback: The traceback of the exception.
    """
    type: str
    traceback: list[str]


_current_task: ContextVar[WoolTask | None] = ContextVar(
    "_current_task", default=None
)


def _run(fn):
    """
    Wrap the asyncio Handle._run method to emit task events.

    :param fn: The original _run method.
    :return: The wrapped _run method.
    """
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


def resolve(method: Callable[P, R]) -> tuple[Any, Callable[P, R]]:
    """
    Make static and class methods picklable from within their decorators.

    :param method: The method to resolve.
    :return: A tuple containing the parent and the resolved method.
    """
    scope = modules[method.__module__]
    parent = None
    for name in method.__qualname__.split("."):
        parent = scope
        scope = getattr(scope, name)
    return parent, cast(Callable[P, R], scope)
