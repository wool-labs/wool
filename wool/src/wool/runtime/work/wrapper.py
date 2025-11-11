"""Task decorator and dispatch logic.

This module contains the @work decorator and related functions for converting
async functions into distributed tasks.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from functools import wraps
from sys import modules
from types import ModuleType
from typing import TYPE_CHECKING
from typing import Coroutine
from typing import ParamSpec
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import cast
from uuid import uuid4

import wool
from wool.runtime import context as ctx
from wool.runtime.typing import AsyncCallable
from wool.runtime.work.task import WorkTask
from wool.runtime.work.task import do_dispatch

if TYPE_CHECKING:
    from wool.runtime.worker.proxy import WorkerProxy

C = TypeVar("C", bound=AsyncCallable)
P = ParamSpec("P")
R = TypeVar("R")


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

        if do_dispatch():
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
    task = WorkTask(
        id=uuid4(),
        callable=function,
        args=args,
        kwargs=kwargs,
        tag=f"{module}.{qualname}({signature})",
        proxy=proxy,
    )
    return proxy.dispatch(task, timeout=ctx.dispatch_timeout.get())


async def _execute(fn: AsyncCallable, parent, *args, **kwargs):
    with do_dispatch(True):
        if isinstance(fn, classmethod):
            return await fn.__func__(parent, *args, **kwargs)
        else:
            return await fn(*args, **kwargs)


async def _stream_to_coroutine(stream):
    result = None
    async for result in await stream:
        continue
    return result


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
