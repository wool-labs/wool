from __future__ import annotations

import asyncio
import logging
import traceback
from collections.abc import Callable
from contextvars import Context, ContextVar
from dataclasses import dataclass
from functools import wraps
from sys import modules
from time import perf_counter_ns
from types import TracebackType
from typing import (
    Any,
    Coroutine,
    Literal,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)
from uuid import UUID, uuid4

import wool
from wool._future import WoolFuture
from wool._pool import WoolClient
from wool._typing import PassthroughDecorator

AsyncCallable = Callable[..., Coroutine]
C = TypeVar("C", bound=AsyncCallable)

Args = tuple
Kwargs = dict
Timeout = int
Timestamp = int


def task(fn: C) -> C:
    """
    A decorator to declare a function as remotely executed using a worker pool.
    This decorator allows a function to be executed either locally or remotely
    using a worker pool. If a worker pool is provided, the function will be
    submitted to the pool for remote execution. If no pool is provided, the
    function will be executed locally.
    The decorator also handles the case where the function is being executed
    within a worker, ensuring that it does not resubmit itself to the pool.
    """

    @wraps(fn)
    def wrapper(*args, __wool_remote__: bool = False, **kwargs) -> Coroutine:
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
                wool.__wool_client__.get(),
                wrapper.__module__,
                wrapper.__qualname__,
                function,
                *args,
                **kwargs,
            )

    return cast(C, wrapper)


def _put(
    client: WoolClient,
    module: str,
    qualname: str,
    function: AsyncCallable,
    *args,
    **kwargs,
) -> Coroutine:
    assert client.connected

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
    assert isinstance(client, WoolClient)
    future: WoolFuture = client.put(task)

    async def coroutine(future):
        try:
            while not future.done():
                await asyncio.sleep(0)
            else:
                try:
                    return future.result()
                except Exception as e:
                    raise Exception().with_traceback(e.__traceback__) from e
        except asyncio.CancelledError:
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
def current_task() -> WoolTask | None:
    """
    Get the current task from the context variable if we are inside a task
    context, otherwise return None.
    """
    return _current_task.get()


# PUBLIC
@dataclass
class WoolTask:
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
        if caller := _current_task.get():
            self.caller = caller.id
        WoolTaskEvent("task-created", task=self).emit()

    def __enter__(self) -> Callable[[], Coroutine]:
        logging.info(f"Entering {self.__class__.__name__} with ID {self.id}")
        WoolTaskEvent("task-queued", task=self).emit()
        return self.run

    def __exit__(
        self,
        exception_type: type,
        exception_value: Exception,
        exception_traceback: TracebackType,
    ):
        logging.info(f"Exiting {self.__class__.__name__} with ID {self.id}")
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
        WoolTaskEvent("task-completed", task=self).emit()

    def run(self) -> Coroutine:
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
class WoolTaskEvent:
    """
    Task events are emitted when a task is created, queued, started, stopped,
    and completed. Tasks can be started and stopped multiple times by the event
    loop. The cumulative time between start and stop events can be used to
    determine a task's CPU utilization.
    """

    type: WoolTaskEventType
    task: WoolTask

    _handlers: dict[str, list[WoolTaskEventCallback]] = {}

    def __init__(self, type: WoolTaskEventType, /, task: WoolTask) -> None:
        self.type = type
        self.task = task

    @classmethod
    def handler(
        cls, *event_types: WoolTaskEventType
    ) -> PassthroughDecorator[WoolTaskEventCallback]:
        def _handler(
            fn: WoolTaskEventCallback,
        ) -> WoolTaskEventCallback:
            for event_type in event_types:
                cls._handlers.setdefault(event_type, []).append(fn)
            return fn

        return _handler

    def emit(self):
        logging.debug(f"Emitting {self.type} event for task {self.task.id}")
        if handlers := self._handlers.get(self.type):
            timestamp = perf_counter_ns()
            for handler in handlers:
                handler(self, timestamp)


# PUBLIC
WoolTaskEventType = Literal[
    "task-created",
    "task-queued",
    "task-started",
    "task-stopped",
    "task-completed",
]


# PUBLIC
class WoolTaskEventCallback(Protocol):
    def __call__(self, event: WoolTaskEvent, timestamp: Timestamp) -> None: ...


# PUBLIC
@dataclass
class WoolTaskException:
    type: str
    traceback: list[str]


_current_task: ContextVar[WoolTask | None] = ContextVar(
    "_current_task", default=None
)


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


def resolve(method: Callable[P, R]) -> tuple[Any, Callable[P, R]]:
    """
    Make static and class methods picklable from within their decorators.
    """
    scope = modules[method.__module__]
    parent = None
    for name in method.__qualname__.split("."):
        parent = scope
        scope = getattr(scope, name)
    return parent, cast(Callable[P, R], scope)


@task
async def ping():
    logging.debug("Ping!")
