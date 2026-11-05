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
    """Convert an async function into a routine a worker pool executes.

    A routine is dispatched rather than called. Invocation resolves the
    `WorkerProxy` ambient in the caller's context and ships the callable,
    its arguments, and the caller's Wool chain to a worker that pool
    selects; the body runs in the worker's process and its outcome
    streams back. `wool.WorkerPool.dispatch` names a pool explicitly for
    one call instead of reading it from context.

    The call site is unchanged by the decorator. A coroutine routine is
    awaited, an async generator routine is iterated, and both cancel
    through the same mechanisms as their undecorated counterparts.

    **Guarantees.** Cancellation crosses the boundary: cancelling the
    awaiting task cancels the routine on the worker, as does ``aclose()``
    or an abandoned iteration on an async generator. An unhandled
    exception raised in the routine body is re-raised at the call site.

    **Constraints.** The boundary is a process boundary crossed by
    pickle, which bounds what a routine may accept, return, and yield,
    and what it may assume about where it runs.

    1. **Serializability.** Arguments, return values, and yielded values
       are pickled. Open file handles, live connections, and lambdas are
       not picklable; a custom type crosses only if it is, which may
       require ``__getstate__`` and ``__setstate__``.
    2. **Process affinity.** Successive invocations are not guaranteed to
       land on the same worker, so `asyncio` synchronization primitives
       do not compose across them and in-process global state does not
       persist between them. A routine that is stateless and idempotent
       is therefore also safely retryable. Synchronization that must hold
       across invocations needs a file-based or otherwise distributed
       primitive.
    3. **Payload cost.** Every argument, return value, and yielded value
       pays serialization and transport proportional to its size. A large
       dataset costs less as a reference the worker resolves, e.g., a
       path, a key, or a shared-memory handle, than as a value shipped
       per call.

    **Descriptor ordering.** The decorator operates on a raw function and
    cannot unwrap a descriptor, so on a ``classmethod`` or
    ``staticmethod`` it must be applied innermost, beneath the descriptor
    decorator. The reverse order raises `ValueError`.

    **Async generators.** An async generator routine streams pull-based:
    the worker suspends at each ``yield`` until the consumer requests the
    next value, so it buffers nothing on the routine's behalf and an
    unbounded sequence costs what the consumer draws from it. The whole
    protocol crosses the boundary: ``anext()``, ``asend()``, ``athrow()``
    and ``aclose()`` each act on the remote generator.

    **Resource release.** A completed dispatch releases its transport
    resources before control returns to the caller: on the ``await`` for
    a coroutine routine, on exhaustion or ``aclose()`` for an async
    generator. Release is never deferred to garbage collection or to the
    event loop's async-generator finalizer, so a routine awaited on a
    loop that stops immediately afterwards strands nothing on that loop.

    **Chain propagation.** A routine runs on the worker under the
    caller's Wool chain: the caller's `wool.ContextVar` manifest is
    decoded from the dispatch frame, the body runs under it, and the
    mutations the body makes ship back on the response. Wool classifies
    this propagation as *ancillary state*, distinct from the routine's
    *primary signal* — its return value, or the exception it raised — and
    that distinction decides what a failure to decode the manifest costs,
    e.g., under cross-version pickle skew on one variable's value.

    Under the default warning filter the primary signal survives. A
    result is still returned and a routine exception is still raised with
    its class intact, while each failed entry emits a
    `wool.SerializationWarning` on the side that observed it, so the
    failures stay visible in the Python warnings stream without
    displacing the outcome.

    Promoting that warning makes ancillary failures fatal::

        import warnings
        import wool

        warnings.filterwarnings("error", category=wool.SerializationWarning)

    The decode then aggregates its per-entry warnings into a single
    `wool.ChainSerializationError`, a `wool.WoolError` subclass catchable
    as `wool.SerializationError` and carrying the warnings on
    `~wool.ChainSerializationError.warnings`. The frame kind decides
    where it surfaces.

    - **Result frames** lose the return value. The
      `wool.ChainSerializationError` raises in place of the primary
      signal, which is what lets the caller observe every bad entry
      rather than only the first.
    - **Exception frames** keep the routine exception as the primary
      signal and attach the `wool.ChainSerializationError` to it as
      ``__cause__``. The routine exception's class is preserved, so
      ``except RoutineError:`` matches as written and no ``except*``
      migration is required.

    A caller that wants the result and the failure record both should
    capture the warnings with ``warnings.catch_warnings(record=True)``
    rather than promote them globally.

    Strict mode applies symmetrically on the worker through the standard
    ``PYTHONWARNINGS`` variable, which ``multiprocessing`` propagates to
    spawned workers by default::

        PYTHONWARNINGS = "error::wool.SerializationWarning"

    A worker that promotes the warning ships the resulting
    `wool.ChainSerializationError` back through the routine-exception
    channel, so the caller catches the class it would have caught locally
    and has no `grpc.aio.AioRpcError` to special-case.

    :param fn:
        The coroutine function or async generator function to convert.
    :returns:
        A wrapper carrying ``fn``'s signature that dispatches the call
        rather than executing it in the caller's process.
    :raises ValueError:
        If ``fn`` is a ``classmethod`` or ``staticmethod`` descriptor, or
        is neither a coroutine function nor an async generator function.

    A coroutine routine, dispatched by awaiting it:

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

    An async generator routine, dispatched on first iteration:

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

    Routines on a class, with the descriptor ordering applied:

    .. code-block:: python

        class MyService:
            @classmethod
            @wool.routine
            async def fetch(cls, key: str) -> bytes: ...

            @staticmethod
            @wool.routine
            async def ping() -> bool: ...

    .. rubric:: Implementation notes

    The wrapper carries a ``__wool_dispatch__`` attribute holding
    ``(entrypoint, lineno)``. The entrypoint is the dispatch function
    matching the routine's call shape, one returning a coroutine or one
    returning an async generator; ``lineno`` is the routine's source line,
    which `_dispatch` folds into the task tag. `dispatch_through` needs
    the pair to route a call through a proxy its caller names rather than
    the ambient one, and reads it off the callable because it is handed
    the callable rather than invoking it. The ambient path consults
    neither member; a routine call routes on the
    `~wool.runtime.routine.task.do_dispatch` flag.

    The attribute's absence is what marks a callable undispatchable. An
    undecorated coroutine function is callable and awaitable, so an
    explicit dispatch that accepted one would run it in the caller's own
    process while reporting a dispatch.

    The spec is a tuple in the wrapper's ``__dict__`` rather than a
    closure over the wrapper, because cloudpickle ships that ``__dict__``
    for every shape that serializes by value, e.g., a closure or a
    ``classmethod``'s underlying function, and a closure over the wrapper
    would carry its own code object into each such dispatch. Both members
    cost almost nothing to pickle: the entrypoint is importable and
    ``lineno`` is an ``int``. Shapes that serialize by reference carry no
    ``__dict__`` at all, so they pay nothing either way.

    A bound ``classmethod`` or ``staticmethod`` forwards attribute lookup
    to the underlying function, which is what carries the attribute
    through the descriptor ordering documented above.
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
