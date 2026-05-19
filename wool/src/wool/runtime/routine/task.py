from __future__ import annotations

import asyncio
import logging
import traceback
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from inspect import isasyncgen
from inspect import isasyncgenfunction
from inspect import iscoroutinefunction
from types import TracebackType
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
from typing import overload
from typing import runtime_checkable
from uuid import UUID

import cloudpickle

import wool
from wool import protocol
from wool.runtime.context import RuntimeContext

Args = Tuple
Kwargs = Dict
Timeout = SupportsInt
Routine: TypeAlias = Coroutine | AsyncGenerator
W = TypeVar("W", bound=Routine)


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
    """Read or scope the dispatch-routing flag.

    Called with no argument, returns the current flag value. Called
    with a *flag* argument, returns a context manager that sets the
    flag for the duration of its ``with`` block and restores the
    prior value on exit. The flag governs whether the surrounding
    ``@routine`` invocation routes through the worker pool (``True``,
    the default) or runs locally (``False``); ``with do_dispatch(False):``
    is the standard way to opt a nested call out of dispatch.
    """
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
    """Represents a distributed task to be executed in the worker pool.

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
        Proxy object for task dispatch and routing (satisfies
        :class:`WorkerProxyLike`).
    :param timeout:
        Task timeout in seconds (0 means no timeout).
    :param caller:
        UUID of the calling task if this is a nested task.
    :param exception:
        Exception information if task execution failed.
    :param tag:
        Descriptive label identifying the call site, formatted as
        ``module.qualname:lineno`` by the ``@routine`` wrapper.
    :param runtime_context:
        Snapshot of the active :class:`RuntimeContext` at construction
        time, captured by :meth:`__post_init__` if not supplied. Ships
        with the dispatch frame so the worker side can restore wire
        defaults (notably ``dispatch_timeout``) for the routine's
        execution.
    """

    id: UUID
    callable: Callable[..., W]
    args: Args
    kwargs: Kwargs
    proxy: WorkerProxyLike
    timeout: Timeout = 0
    caller: UUID | None = None
    exception: TaskException | None = None
    tag: str | None = None
    runtime_context: RuntimeContext | None = None

    def __post_init__(self):
        """Validate the proxy, capture the calling task's id, and seed
        a :class:`RuntimeContext` snapshot if one was not supplied.

        The runtime-context seed lets a Task built outside an active
        :class:`RuntimeContext` scope still ship the wire defaults
        when it dispatches.
        """
        if not isinstance(self.proxy, WorkerProxyLike):
            raise TypeError(
                f"proxy must conform to WorkerProxyLike, got {type(self.proxy).__name__}"
            )
        if caller := _current_task.get():
            self.caller = caller.id
        if self.runtime_context is None:
            self.runtime_context = RuntimeContext.get_current()

    def __enter__(self) -> Task:
        """Enter the task context.

        Bind this task to the current context. On exit, re-binds
        the calling task and records any propagating exception
        on :attr:`exception` for wire transport.
        """
        logging.debug(f"Entering {self.__class__.__name__} with ID {self.id}")
        self._task_token = _current_task.set(self)
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: TracebackType | None,
    ):
        """Exit the task context and capture exception state.

        Re-binds the calling task, if any, to the current context
        and records any propagating exception on :attr:`exception`
        for wire transport.

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
        return False

    @classmethod
    def from_protobuf(cls, task: protocol.Task) -> Task:
        """Deserialize a Task from a protobuf message.

        The payload fields are deserialized with :func:`cloudpickle.loads`
        — payloads produced by
        :class:`~wool.runtime.serializer.CloudpickleSerializer` are
        standard reduce tuples that stock unpickling executes natively.

        :param task:
            A :class:`protocol.Task` message.
        :returns:
            A :class:`Task` instance with all fields restored.
        """
        loads = cloudpickle.loads
        runtime_context = (
            RuntimeContext.from_protobuf(task.runtime_context)
            if task.HasField("runtime_context")
            else None
        )
        return cls(
            id=UUID(task.id),
            callable=loads(task.callable),
            args=loads(task.args),
            kwargs=loads(task.kwargs),
            caller=UUID(task.caller) if task.caller else None,
            proxy=loads(task.proxy),
            timeout=task.timeout if task.timeout else 0,
            tag=task.tag if task.tag else None,
            runtime_context=runtime_context,
        )

    def to_protobuf(self) -> protocol.Task:
        """Serialize this Task to a protobuf message.

        The callable, arguments, and proxy are serialized with
        :data:`wool.__serializer__`.

        :returns:
            A :class:`protocol.Task` message.
        """
        dumps = wool.__serializer__.dumps
        return protocol.Task(
            version=protocol.__version__,
            id=str(self.id),
            callable=dumps(self.callable),
            args=dumps(self.args),
            kwargs=dumps(self.kwargs),
            caller=str(self.caller) if self.caller else "",
            proxy=dumps(self.proxy),
            proxy_id=str(self.proxy.id),
            timeout=int(self.timeout) if self.timeout else 0,
            tag=self.tag if self.tag else "",
            runtime_context=(
                self.runtime_context.to_protobuf() if self.runtime_context else None
            ),
        )


# public
@dataclass
class TaskException:
    """Represents an exception that occurred during distributed task execution.

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


@asynccontextmanager
async def routine_scope(task: Task) -> AsyncGenerator[Coroutine | AsyncGenerator]:
    """Establish the execution scope around *task*'s Wool routine.

    Validates that ``task.callable`` is either a coroutine
    function or an async generator function, acquires a proxy
    from :data:`wool.__proxy_pool__`, binds :data:`wool.__proxy__`
    for nested dispatch, and yields the routine ready for iteration.
    On exit, close the callable as needed.

    .. warning::

       The :data:`wool.__proxy_pool__` context variable must be
       initialized before entering this scope; entering without a
       configured pool raises :class:`RuntimeError`.

    :param task:
        The :class:`Task` whose routine is to be executed. Its
        ``callable`` must be an async function or async generator
        function; its ``runtime_context`` must be populated (the
        caller-side constructor seeds it via
        :meth:`Task.__post_init__`).
    :yields:
        The constructed routine — a :class:`Coroutine` for async
        functions or an :class:`AsyncGenerator` for async generator
        functions. The caller drives it via ``await routine`` or
        ``routine.asend``/``athrow``/``aclose``; the scope handles
        teardown on exit.
    :raises ValueError:
        If ``task.callable`` is neither a coroutine function nor
        an async generator function.
    :raises RuntimeError:
        If :data:`wool.__proxy_pool__` is not initialized at scope
        entry.
    """
    if not (iscoroutinefunction(task.callable) or isasyncgenfunction(task.callable)):
        raise ValueError("Expected coroutine function or async generator function")
    proxy_pool = wool.__proxy_pool__.get()
    if proxy_pool is None:
        raise RuntimeError("wool.__proxy_pool__ is not initialized")
    assert task.runtime_context is not None
    async with proxy_pool.get(task.proxy) as proxy:
        token = wool.__proxy__.set(proxy)
        try:
            with task.runtime_context, task, do_dispatch(False):
                routine = task.callable(*task.args, **task.kwargs)
                try:
                    await asyncio.sleep(0)
                    yield routine
                finally:
                    if isasyncgen(routine):
                        await routine.aclose()
                    else:
                        routine.close()
        finally:
            wool.__proxy__.reset(token)
