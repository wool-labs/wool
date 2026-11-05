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
from wool.runtime.context.runtime import dispatch_timeout
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

    **Dispatch cleanup:**

    A completed dispatch releases its transport resources before control
    returns to the caller — for a coroutine routine when the ``await``
    returns, for an async generator on exhaustion or ``aclose()``.
    Release is never deferred to garbage collection, so a routine
    awaited on a loop that stops immediately afterwards strands nothing
    on that loop.

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

    **Chain propagation and decode-failure semantics:**

    Routines run on the worker under the caller's Wool chain: the
    caller's :class:`wool.ContextVar` chain manifest is decoded from the
    dispatch frame and the routine runs under it, with post-run
    mutations shipped back on the response. Wool treats this
    propagation as **ancillary state** — separate from the routine's
    primary signal (its return value or raised exception):

    - **Primary signal preservation.** A failure to decode the chain
      manifest (e.g., cross-version pickle skew on a single
      variable value) never preempts the routine's outcome. The
      result is still returned; a routine exception is still raised.
    - **Visibility via warning.** Every ancillary decode failure emits
      a :class:`wool.SerializationWarning` on the side that observed
      the failure. Under the default warning filter the routine
      exception's type is preserved — ``except RoutineError:`` still
      matches as written — and the warnings remain visible in the
      Python warnings stream.
    - **Strict mode (opt-in).** Promote the warning to an exception
      to treat ancillary failures as fatal::

          import warnings
          import wool

          warnings.filterwarnings("error", category=wool.SerializationWarning)

      In strict mode the chain-manifest decode aggregates the per-entry
      warnings into a single :class:`wool.ChainSerializationError` (a
      :class:`wool.WoolError` subclass, catchable via
      :class:`wool.SerializationError`, with the warnings on
      :attr:`~wool.ChainSerializationError.warnings`):

      * **Result frames** lose the routine's return value — the
        :class:`~wool.ChainSerializationError` raises in place of the
        primary signal so the caller observes every bad entry, not
        just the first.
      * **Exception frames** still preserve the routine exception as
        the primary signal; the :class:`~wool.ChainSerializationError`
        rides on it as ``__cause__`` via ``raise routine_exc from
        decode_err``. The routine exception's class is preserved so
        ``except RoutineError:`` keeps matching — no migration to
        ``except*`` required.

      Callers that want both result preservation *and* failure
      visibility should instead use ``warnings.catch_warnings(record=True)``
      to capture warnings without globally promoting them.

      Strict mode applies symmetrically on the worker side via the
      standard ``PYTHONWARNINGS`` environment variable, which
      ``multiprocessing`` propagates to spawned worker subprocesses
      by default::

          PYTHONWARNINGS = "error::wool.SerializationWarning"

      When the worker promotes the warning, wool ships the resulting
      :class:`wool.ChainSerializationError` back through the
      routine-exception channel so the caller catches the same error
      class — no :class:`grpc.aio.AioRpcError` to special-case.

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

    .. rubric:: Implementation notes

    The returned wrapper carries a ``__wool_dispatch__`` attribute
    holding ``(entrypoint, lineno)``, where the entrypoint is the
    dispatch function matching this routine's call shape — one
    returning a coroutine, or one returning an async generator. It is
    the pair `dispatch_through` needs to route this routine through a
    proxy its caller names rather than the ambient one. Nothing in the
    ambient dispatch path consults it; a routine call routes on the
    `~wool.runtime.routine.task.do_dispatch` flag. Its absence is also
    what marks a callable as undispatchable, which an explicit dispatch
    needs because it is handed a callable rather than invoking one: an
    undecorated coroutine function is callable and awaitable, so
    without the attribute it would run in the caller's own process
    while the call reported a dispatch.

    A tuple rather than a bound hook. The attribute travels in the
    wrapper's ``__dict__``, which cloudpickle ships for the shapes that
    serialize by value (a closure, a ``classmethod``'s function), and a
    closure over the wrapper would carry its own code object into every
    such dispatch. Both members pickle to almost nothing: the
    entrypoint is importable and ``lineno`` is an ``int``. The shapes
    that serialize by reference carry no ``__dict__`` at all, so they
    pay nothing either way.

    A bound ``classmethod`` or ``staticmethod`` forwards attribute
    lookup to the underlying function, so the decorator ordering
    documented above carries the attribute through.
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
                    async_generator_wrapper,
                    *args,
                    **kwargs,
                )
            else:
                stream = _stream(fn, *args, **kwargs)
                assert isasyncgen(stream)

            # Relayed here rather than through `_dispatch_stream`, whose
            # loop this repeats — see that function for why the three
            # relays in this module cannot be folded into one.
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

        # Set rather than assigned: `functools.wraps` returns a typed
        # shim that declares no such attribute.
        setattr(async_generator_wrapper, "__wool_dispatch__", (_dispatch_stream, lineno))

        return cast(C, async_generator_wrapper)

    else:

        @wraps(fn)
        async def coroutine_wrapper(*args, **kwargs):
            if do_dispatch():
                proxy = wool.__proxy__.get()
                assert proxy
                return await _dispatch_coroutine(
                    proxy,
                    coroutine_wrapper.__module__,
                    coroutine_wrapper.__qualname__,
                    lineno,
                    coroutine_wrapper,
                    *args,
                    **kwargs,
                )
            else:
                return await _execute(fn, *args, **kwargs)

        # Set rather than assigned: `functools.wraps` returns a typed
        # shim that declares no such attribute.
        setattr(coroutine_wrapper, "__wool_dispatch__", (_dispatch_coroutine, lineno))

        return cast(C, coroutine_wrapper)


def dispatch_through(proxy: WorkerProxy, routine: Callable, /, *args, **kwargs):
    """Dispatch ``routine`` through ``proxy`` rather than the ambient one.

    The entry point behind `wool.WorkerPool.dispatch`, which owns the
    user-facing contract. A routine resolves its proxy from
    `wool.__proxy__` when called; this routes one call through the proxy
    its caller names instead, by handing that proxy to the same dispatch
    path the routine would have taken, so an explicit dispatch and an
    ambient call differ in nothing but which proxy serves them.

    ``proxy`` and ``routine`` are positional-only, so no keyword this
    forwards is shadowed by the signature.

    :param proxy:
        The `WorkerProxy` to route through.
    :param routine:
        The `routine`-decorated function to dispatch. A bound method or
        classmethod is accepted and carries its receiver.
    :returns:
        Whatever calling ``routine`` returns — a coroutine for a
        coroutine routine, an async generator for an async generator
        routine.
    :raises TypeError:
        If ``routine`` carries no ``__wool_dispatch__``, which is to say
        it is not `routine`-decorated. See `routine` for what the
        attribute holds and why its absence is the marker.

    .. rubric:: Implementation notes

    A bound callable carries its receiver out of band, where an ambient
    call carries it in the argument list: calling ``obj.method(a, b)``
    invokes the decorator's wrapper as ``wrapper(obj, a, b)``. Unbinding
    restores that shape, which matters because `_dispatch` drops a
    leading argument for anything carrying ``__self__`` — a branch the
    ambient path keeps dead by only ever handing it the plain wrapper.
    Passing a bound callable straight through would both trip that
    branch and ship the receiver twice.
    """
    spec = getattr(routine, "__wool_dispatch__", None)
    if spec is None:
        raise TypeError(
            f"Expected a @wool.routine-decorated callable, got "
            f"{getattr(routine, '__qualname__', routine)!r}; an "
            "undecorated callable cannot be dispatched"
        )
    # Read through `getattr`: `Callable` declares neither member, and
    # anything carrying a `__self__` carries a `__func__` beside it.
    if (receiver := getattr(routine, "__self__", None)) is not None:
        args = (receiver, *args)
        routine = getattr(routine, "__func__")
    entrypoint, lineno = spec
    return entrypoint(
        proxy,
        routine.__module__,
        routine.__qualname__,
        lineno,
        routine,
        *args,
        **kwargs,
    )


async def _dispatch_coroutine(
    proxy: WorkerProxy,
    module: str,
    qualname: str,
    lineno: int,
    function: Callable[..., Coroutine | AsyncGenerator],
    *args,
    **kwargs,
):
    """Dispatch a coroutine routine and take its single result frame."""
    stream = await _dispatch(proxy, module, qualname, lineno, function, *args, **kwargs)
    return await _stream_to_coroutine(stream)


async def _dispatch_stream(
    proxy: WorkerProxy,
    module: str,
    qualname: str,
    lineno: int,
    function: Callable[..., Coroutine | AsyncGenerator],
    *args,
    **kwargs,
):
    """Dispatch an async generator routine and relay its result stream.

    Dispatches nothing until first iteration, which is where a routine
    call dispatches too.

    .. rubric:: Implementation notes

    This module relays the async-generator protocol in three places —
    here, in `routine`'s async generator wrapper, and in `_stream` — and
    they cannot be folded into one. Python has no asynchronous
    ``yield from``, so an async generator cannot delegate its protocol
    to another without repeating the loop, and the wrapper has to be an
    async generator function in its own right: `Task.callable` is the
    wrapper, and the worker decides between streaming and unary dispatch
    by asking `inspect.isasyncgenfunction` about it.

    This loop and the wrapper's are identical; `_stream`'s is not. That
    one scopes every step in ``do_dispatch(True)`` so a routine the
    user's body calls dispatches rather than running inline, and the
    scope has to sit inside each step rather than around the loop: an
    async generator has no context of its own, so a ``with`` spanning
    the ``yield`` would leak the flag to the consumer and reset its
    token in whichever context happened to drive the next step.
    """
    stream = await _dispatch(proxy, module, qualname, lineno, function, *args, **kwargs)
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
    # The per-step `do_dispatch` scoping is what distinguishes this
    # relay from the other two — see `_dispatch_stream`.
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
    """Take a coroutine routine's single result frame and close the stream.

    .. rubric:: Implementation notes

    A coroutine dispatch yields exactly one result frame, leaving the
    dispatch generator suspended at its yield. Closing it in a
    ``finally`` — on every exit path, not only when a frame is taken —
    drives the teardown `WorkerConnection._execute` pins on its exit
    stack, binding release to this call returning rather than to the
    event loop's async-generator finalizer, which a loop that stops
    first never runs.
    """
    try:
        return await anext(stream, None)
    finally:
        await stream.aclose()
