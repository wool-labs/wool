from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from inspect import getsourcelines
from inspect import isasyncgen
from inspect import isasyncgenfunction
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import Coroutine
from typing import TypeVar
from typing import cast
from uuid import uuid4

import wool
from wool.runtime.context import dispatch_timeout
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import do_dispatch

if TYPE_CHECKING:
    from wool.runtime.worker.proxy import WorkerProxy

C = TypeVar("C", bound=Callable[..., Coroutine | AsyncGenerator])


# public
def routine(fn: C) -> C:
    """Decorator to declare an asynchronous function as remotely executable.

    Converts an asynchronous coroutine or generator into a distributed
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
        If the decorated function is a ``classmethod`` or
        ``staticmethod`` descriptor (apply ``@wool.routine`` before
        the descriptor decorator), or if the decorated function is
        not a coroutine function or async generator function.

    .. note::
        Decorated functions behave like regular coroutines or async
        generators and can be awaited, iterated over, and cancelled
        normally. Task execution occurs transparently across the
        distributed worker pool.

    .. note::
        When using ``@wool.routine`` on a ``@classmethod`` or
        ``@staticmethod``, apply ``@wool.routine`` first (innermost)
        and the descriptor decorator second (outermost). The routine
        decorator operates on the raw function and cannot unwrap
        descriptor objects.

        .. code-block:: python

            class MyService:
                @classmethod
                @wool.routine
                async def fetch(cls, key: str) -> bytes: ...

                @staticmethod
                @wool.routine
                async def ping() -> bool: ...

    **Async Generator Support:**

    For async generators, the decorator supports pull-based streaming
    where the worker pauses at each ``yield`` until the client requests
    the next value. This enables efficient processing of large or
    infinite sequences without server-side buffering.

    - **Supported operations**:
      ``anext()``, ``asend()``, ``athrow()``, ``aclose()``
    - **Cleanup**:
      Calling ``aclose()`` or breaking out of iteration properly cancels
      the remote worker task

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

        import asyncio, wool


        @wool.routine
        async def fibonacci(n: int) -> int:
            if n <= 1:
                return n
            async with asyncio.TaskGroup() as tg:
                a = tg.create_task(fibonacci(n - 1))
                b = tg.create_task(fibonacci(n - 2))
            return a.result() + b.result()


        async def main():
            async with wool.WorkerPool():
                print(await fibonacci(10))  # 34

    Example usage with async generators:

    .. code-block:: python

        import wool


        @wool.routine
        async def fibonacci_series(n: int):
            a, b = 0, 1
            for _ in range(n):
                yield a
                a, b = b, a + b


        async def main():
            async with wool.WorkerPool():
                async for value in fibonacci_series(10):
                    print(value)  # 0, 1, 1, 2, 3, 5, 8, 13, 21, 34
    """
    if isinstance(fn, (classmethod, staticmethod)):
        raise ValueError(
            "@wool.routine must be applied before @classmethod/@staticmethod"
        )

    if not iscoroutinefunction(fn) and not isasyncgenfunction(fn):
        raise ValueError("Expected a coroutine function or async generator function")

    try:
        _, lineno = getsourcelines(fn)
    except OSError:
        lineno = 0

    if isasyncgenfunction(fn):

        @wraps(fn)
        async def async_generator_wrapper(*args, **kwargs):
            if do_dispatch():
                proxy = wool.__proxy__.get()
                assert proxy
                stream = await _dispatch(
                    proxy,
                    async_generator_wrapper.__module__,
                    async_generator_wrapper.__qualname__,
                    lineno,
                    fn,
                    *args,
                    **kwargs,
                )
            else:
                stream = _stream(fn, *args, **kwargs)
                assert isasyncgen(stream)

            try:
                sent = None
                result = await stream.__anext__()
                while True:
                    try:
                        sent = yield result
                    except GeneratorExit:
                        await stream.aclose()
                        return
                    except BaseException as exc:
                        result = await stream.athrow(exc)
                    else:
                        if sent is None:
                            result = await stream.__anext__()
                        else:
                            result = await stream.asend(sent)
            except StopAsyncIteration:
                return
            finally:
                await stream.aclose()

        return cast(C, async_generator_wrapper)

    else:

        @wraps(fn)
        async def coroutine_wrapper(*args, **kwargs):
            if do_dispatch():
                proxy = wool.__proxy__.get()
                assert proxy
                stream = await _dispatch(
                    proxy,
                    coroutine_wrapper.__module__,
                    coroutine_wrapper.__qualname__,
                    lineno,
                    fn,
                    *args,
                    **kwargs,
                )
                return await _stream_to_coroutine(stream)
            else:
                return await _execute(fn, *args, **kwargs)

        return cast(C, coroutine_wrapper)


def _dispatch(
    proxy: WorkerProxy,
    module: str,
    qualname: str,
    lineno: int,
    function: Callable[..., Coroutine | AsyncGenerator],
    *args,
    **kwargs,
):
    # Skip self argument if function is a method.
    args = args[1:] if hasattr(function, "__self__") else args
    task = Task(
        id=uuid4(),
        callable=function,
        args=args,
        kwargs=kwargs,
        tag=f"{module}.{qualname}:{lineno}",
        proxy=proxy,
    )
    return proxy.dispatch(task, timeout=dispatch_timeout.get())


async def _stream(fn, *args, **kwargs):
    gen = fn(*args, **kwargs)
    try:
        sent = None
        with do_dispatch(True):
            result = await gen.__anext__()
        while True:
            try:
                sent = yield result
            except GeneratorExit:
                await gen.aclose()
                return
            except BaseException as exc:
                with do_dispatch(True):
                    result = await gen.athrow(exc)
            else:
                with do_dispatch(True):
                    if sent is None:
                        result = await gen.__anext__()
                    else:
                        result = await gen.asend(sent)
    except StopAsyncIteration:
        return
    finally:
        await gen.aclose()


async def _execute(fn, *args, **kwargs):
    with do_dispatch(True):
        return await fn(*args, **kwargs)


async def _stream_to_coroutine(stream):
    return await anext(stream, None)
