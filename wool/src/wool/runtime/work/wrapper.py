from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from inspect import isasyncgen
from inspect import isasyncgenfunction
from inspect import iscoroutinefunction
from sys import modules
from types import ModuleType
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import Coroutine
from typing import ParamSpec
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import cast
from uuid import uuid4

import wool
from wool.runtime import context as ctx
from wool.runtime.work.task import WorkTask
from wool.runtime.work.task import do_dispatch

if TYPE_CHECKING:
    from wool.runtime.worker.proxy import WorkerProxy

C = TypeVar("C", bound=Callable[..., Coroutine | AsyncGenerator])
P = ParamSpec("P")
R = TypeVar("R")


# public
def work(fn: C) -> C:
    """Decorator to declare an asynchronous function as remotely executable.

    Converts an asynchronous function or async generator into a distributed
    task that can be executed by a worker pool. When the decorated function
    is invoked, it is dispatched to the worker pool associated with the
    current worker pool session context.

    :param fn:
        The asynchronous function or async generator to convert into a
        distributed routine.
    :returns:
        The decorated function that dispatches to the worker pool when
        called.
    :raises ValueError:
        If the decorated function is not a coroutine function or async
        generator function.

    .. note::
        Decorated functions behave like regular coroutines or async
        generators and can be awaited, iterated over, and cancelled
        normally. Task execution occurs transparently across the
        distributed worker pool.

    **Async Generator Support:**

    For async generators, the decorator supports pull-based streaming
    where the worker pauses at each ``yield`` until the client requests
    the next value. This enables efficient processing of large or
    infinite sequences without server-side buffering.

    - **Supported operations**: ``__anext__()``, ``aclose()``
    - **Unsupported operations**: ``asend()``, ``athrow()`` (raise
      :exc:`NotImplementedError`)
    - **Cleanup**: Calling ``aclose()`` or breaking out of iteration
      properly cancels the remote worker task

    Best practices and considerations for designing tasks:

    1. **Picklability**: Arguments, return values, and yielded values
       must be picklable for serialization between processes. Avoid
       unpicklable objects like open file handles, database connections,
       or lambda functions. Custom objects should implement
       ``__getstate__`` and ``__setstate__`` methods if needed.

    2. **Synchronization**: Tasks are not guaranteed to execute on the
       same process between invocations. Standard :mod:`asyncio`
       synchronization primitives will not work across processes.
       Use file-based or other distributed synchronization utilities.

    3. **Statelessness**: Design tasks to be stateless and idempotent.
       Avoid global variables or shared mutable state to ensure
       predictable behavior and enable safe retries.

    4. **Cancellation**: Task cancellation behaves like standard Python
       coroutine cancellation and is properly propagated across the
       distributed system. For async generators, cancellation or calling
       ``aclose()`` triggers proper cleanup.

    5. **Error propagation**: Unhandled exceptions raised within tasks are
       transparently propagated to the caller as they would be normally.

    6. **Performance**: Minimize argument, return value, and yielded value
       sizes to reduce serialization overhead. For large datasets,
       consider using shared memory or passing references instead of the
       data itself.

    Example usage with coroutines:

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

    Example usage with async generators:

    .. code-block:: python

        import wool


        @wool.work
        async def count_to(n: int):
            for i in range(1, n + 1):
                yield i


        async def main():
            async with wool.WorkerPool():
                async for value in count_to(10):
                    print(value)  # Prints 1, 2, 3, ..., 10
    """
    # Check if function is a coroutine or async generator
    is_valid = (
        iscoroutinefunction(fn)
        or isasyncgenfunction(fn)
        or (
            isinstance(fn, (classmethod, staticmethod))
            and (iscoroutinefunction(fn.__func__) or isasyncgenfunction(fn.__func__))
        )
    )
    if not is_valid:
        raise ValueError("Expected a coroutine function or async generator function")

    if isinstance(fn, (classmethod, staticmethod)):
        wrapped_fn = fn.__func__
    else:
        wrapped_fn = fn

    if isasyncgenfunction(wrapped_fn):

        @wraps(wrapped_fn)
        async def async_generator_wrapper(*args, **kwargs):
            # Handle static and class methods in a picklable way.
            parent, function = _resolve(fn)
            assert parent is not None
            assert callable(function)

            if do_dispatch():
                proxy = wool.__proxy__.get()
                assert proxy
                stream = await _dispatch(
                    proxy,
                    async_generator_wrapper.__module__,
                    async_generator_wrapper.__qualname__,
                    function,
                    *args,
                    **kwargs,
                )
            else:
                stream = _stream(fn, parent, *args, **kwargs)
                assert isasyncgen(stream)

            try:
                async for result in stream:
                    yield result
            finally:
                await stream.aclose()

        return cast(C, async_generator_wrapper)

    else:

        @wraps(wrapped_fn)
        async def coroutine_wrapper(*args, **kwargs):
            # Handle static and class methods in a picklable way.
            parent, function = _resolve(fn)
            assert parent is not None
            assert callable(function)

            if do_dispatch():
                proxy = wool.__proxy__.get()
                assert proxy
                stream = await _dispatch(
                    proxy,
                    coroutine_wrapper.__module__,
                    coroutine_wrapper.__qualname__,
                    function,
                    *args,
                    **kwargs,
                )
                coro = _stream_to_coroutine(stream)
            else:
                coro = _execute(fn, parent, *args, **kwargs)

            return await coro

        return cast(C, coroutine_wrapper)


routine = work


def _dispatch(
    proxy: WorkerProxy,
    module: str,
    qualname: str,
    function: Callable[..., Coroutine | AsyncGenerator],
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


async def _stream(fn, parent, *args, **kwargs):
    if isinstance(fn, classmethod):
        gen = fn.__func__(parent, *args, **kwargs)
    elif isinstance(fn, staticmethod):
        gen = fn.__func__(*args, **kwargs)
    else:
        gen = fn(*args, **kwargs)
    while True:
        with do_dispatch(True):
            try:
                result = await anext(gen)
            except StopAsyncIteration:
                break

        yield result


async def _execute(fn, parent, *args, **kwargs):
    with do_dispatch(True):
        if isinstance(fn, classmethod):
            return await fn.__func__(parent, *args, **kwargs)
        elif isinstance(fn, staticmethod):
            return await fn.__func__(*args, **kwargs)
        else:
            return await fn(*args, **kwargs)


async def _stream_to_coroutine(stream):
    result = None
    async for result in stream:
        continue
    return result


def _resolve(
    method: Callable[P, R] | classmethod | staticmethod,
) -> Tuple[Type | ModuleType | None, Callable[P, R]]:
    scope = modules[method.__module__]
    parent = None
    for name in method.__qualname__.split("."):
        parent = scope
        scope = getattr(scope, name)
        assert scope
    assert isinstance(parent, (Type, ModuleType))
    return parent, cast(Callable[P, R], scope)
