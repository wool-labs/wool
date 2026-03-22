from __future__ import annotations

import asyncio
import functools
import logging
import traceback
import weakref
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from inspect import isasyncgenfunction
from inspect import iscoroutinefunction
from types import TracebackType
from typing import Any
from typing import AsyncGenerator
from typing import ContextManager
from typing import Coroutine
from typing import Dict
from typing import Generic
from typing import Protocol
from typing import SupportsInt
from typing import Tuple
from typing import TypeAlias
from typing import TypeVar
from typing import cast
from typing import overload
from typing import runtime_checkable
from uuid import UUID
from uuid import uuid4

import cloudpickle

import wool
from wool import protocol
from wool.runtime.context import RuntimeContext

Args = Tuple
Kwargs = Dict
Timeout = SupportsInt
Routine: TypeAlias = Coroutine | AsyncGenerator
W = TypeVar("W", bound=Routine)


# public
@runtime_checkable
class Serializer(Protocol):
    """Protocol for pluggable serialization of Task payload fields."""

    def dumps(self, obj: Any) -> bytes: ...

    def loads(self, data: bytes) -> Any: ...


class _PassthroughKey:
    """Weak-referenceable key for the passthrough store."""

    __slots__ = ("__weakref__", "token")

    def __init__(self, token: UUID | None = None):
        self.token = token if token is not None else uuid4()

    def __hash__(self):
        return hash(self.token)

    def __eq__(self, other):
        if not isinstance(other, _PassthroughKey):
            return super().__eq__(other)
        return self.token == other.token


class PassthroughSerializer:
    """In-process serializer that avoids pickling entirely.

    Each instance acts as a scope guard for one dispatch.
    ``dumps`` creates a weakly-referenceable key, stores the object
    in a module-level :class:`~weakref.WeakKeyDictionary`, and
    retains a strong reference to the key on ``self``.  When the
    serializer goes out of scope the keys are garbage-collected and
    the weak-dict entries are removed automatically.

    ``loads`` is static — it reconstructs the key from the bytes
    token in the protobuf message and pops the entry from the store.

    All instances hash and compare equal so that
    ``_pickle_serializer``'s LRU cache hits on every call.
    """

    def __init__(self):
        self._keys: list[_PassthroughKey] = []

    def __hash__(self):
        return hash(PassthroughSerializer)

    def __eq__(self, other):
        # Required by _pickle_serializer's lru_cache: all instances
        # share the same hash, so __eq__ must confirm the match for
        # the cache to hit instead of treating each instance as a
        # separate key.
        return isinstance(other, PassthroughSerializer)

    def __reduce__(self):
        return (PassthroughSerializer, ())

    def dumps(self, obj: Any) -> bytes:
        key = _PassthroughKey()
        self._keys.append(key)
        _passthrough_store[key] = obj
        return key.token.bytes

    @staticmethod
    def loads(data: bytes) -> Any:
        key = _PassthroughKey(UUID(bytes=data))
        return _passthrough_store.pop(key)


_passthrough_store: weakref.WeakKeyDictionary[_PassthroughKey, Any] = (
    weakref.WeakKeyDictionary()
)


@functools.lru_cache(maxsize=8)
def _pickle_serializer(s: Serializer) -> bytes:
    return cloudpickle.dumps(s)


@functools.lru_cache(maxsize=8)
def _unpickle_serializer(data: bytes) -> Serializer:
    return cloudpickle.loads(data)


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
@runtime_checkable
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
    :param context:
        RuntimeContext snapshot captured at task creation time.
        Propagated over the wire so workers can restore the caller's
        runtime settings (e.g. dispatch_timeout) before execution.
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
    context: RuntimeContext | None = None

    def __post_init__(self, **kwargs):
        """
        Initialize the task and set up caller tracking.

        :param kwargs:
            Additional keyword arguments (unused).
        """
        if not isinstance(self.proxy, WorkerProxyLike):
            raise TypeError(
                f"proxy must conform to WorkerProxyLike, got {type(self.proxy).__name__}"
            )
        if caller := _current_task.get():
            self.caller = caller.id
        if self.context is None:
            self.context = RuntimeContext.get_current()

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
        _current_task.reset(self._task_token)
        # Return False to allow exceptions to propagate
        return False

    @classmethod
    def from_protobuf(cls, task: protocol.Task) -> Task:
        """Deserialize a Task from a protobuf message.

        .. note::
            The payload's serializer is unpickled and cached for subsequent calls.

        :param task:
            A protobuf ``Task`` message.
        :returns:
            A :class:`Task` instance with all fields restored.
        """
        context = (
            RuntimeContext.from_protobuf(task.context)
            if task.HasField("context")
            else None
        )
        if task.HasField("serializer"):
            s = _unpickle_serializer(task.serializer)
            loads = s.loads
            if isinstance(s, PassthroughSerializer):
                proxy_loads = s.loads
            else:
                proxy_loads = cloudpickle.loads
        else:
            loads = cloudpickle.loads
            proxy_loads = cloudpickle.loads
        return cls(
            id=UUID(task.id),
            callable=loads(task.callable),
            args=loads(task.args),
            kwargs=loads(task.kwargs),
            caller=UUID(task.caller) if task.caller else None,
            proxy=proxy_loads(task.proxy),
            timeout=task.timeout if task.timeout else 0,
            tag=task.tag if task.tag else None,
            context=context,
        )

    def to_protobuf(self, serializer: Serializer | None = None) -> protocol.Task:
        """Serialize this Task to a protobuf message.

        The serializer itself is pickled via :func:`_pickle_serializer`
        which uses an LRU cache keyed on the serializer instance.
        :class:`PassthroughSerializer` instances all hash and compare
        equal, so repeated calls hit the cache and avoid redundant
        pickling.

        :param serializer:
            Optional serializer for the callable and its arguments. When ``None`` (the
            default), ``cloudpickle`` is used and the protobuf ``serializer`` field is
            left unset. When provided, the serializer is pickled into the ``serializer``
            field so that :meth:`from_protobuf` can use it on the receiving side.

            .. note::
                If specified, the serializer is pickled and cached for subsequent calls.
        :returns:
            A protobuf ``Task`` message.
        """
        dumps = serializer.dumps if serializer is not None else cloudpickle.dumps
        proxy_dumps = (
            dumps if isinstance(serializer, PassthroughSerializer) else cloudpickle.dumps
        )
        task_msg = protocol.Task(
            version=protocol.__version__,
            id=str(self.id),
            callable=dumps(self.callable),
            args=dumps(self.args),
            kwargs=dumps(self.kwargs),
            caller=str(self.caller) if self.caller else "",
            proxy=proxy_dumps(self.proxy),
            proxy_id=str(self.proxy.id),
            timeout=int(self.timeout) if self.timeout else 0,
            tag=self.tag if self.tag else "",
            context=self.context.to_protobuf() if self.context else None,
        )
        if serializer is not None:
            task_msg.serializer = _pickle_serializer(serializer)
        return task_msg

    def dispatch(self) -> W:
        if isasyncgenfunction(self.callable):
            return cast(W, self._stream())
        elif iscoroutinefunction(self.callable):
            return cast(W, self._run())
        else:
            raise ValueError("Expected routine to be coroutine or async generator")

    async def _run(self):
        """
        Execute the task's callable with its arguments in proxy context.

        :returns:
            The result of executing the callable.
        :raises RuntimeError:
            If no proxy pool is available for task execution.
        """
        assert iscoroutinefunction(self.callable), "Expected coroutine function"
        proxy_pool = wool.__proxy_pool__.get()
        if not proxy_pool:
            raise RuntimeError("No proxy pool available for task execution")
        async with proxy_pool.get(self.proxy) as proxy:
            # Set the proxy in context variable for nested task dispatch
            token = wool.__proxy__.set(proxy)
            try:
                assert self.context is not None
                with self.context:
                    with self:
                        with do_dispatch(False):
                            await asyncio.sleep(0)
                            return await self.callable(*self.args, **self.kwargs)
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
            gen = self.callable(*self.args, **self.kwargs)
            try:
                while True:
                    # Set the proxy in context variable for nested task dispatch
                    token = wool.__proxy__.set(proxy)
                    try:
                        assert self.context is not None
                        with self.context:
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
