from __future__ import annotations

import asyncio
import functools
import logging
import threading
import time
from contextvars import ContextVar
from contextvars import Token
from typing import TYPE_CHECKING
from typing import Coroutine
from typing import TypeVar
from uuid import UUID
from weakref import WeakValueDictionary

import wool
from wool._future import Future
from wool._future import fulfill
from wool._future import poll
from wool._manager import Manager

if TYPE_CHECKING:
    from wool._task import AsyncCallable
    from wool._task import Task
    from wool._typing import Positive
    from wool._typing import Zero


def command(fn):
    """
    Decorator to wrap a function with connection checks and automatic
    reconnection logic.

    :param fn: The function to wrap.
    :return: The wrapped function.
    """

    @functools.wraps(fn)
    def wrapper(self: BaseSession, *args, **kwargs):
        if not self.connected:
            raise RuntimeError("Client not connected to manager")
        assert self.manager
        try:
            return fn(self, *args, **kwargs)
        except (ConnectionRefusedError, ConnectionResetError):
            logging.warning(
                f"Connection to manager at {self._address} lost. "
                "Attempting to reconnect..."
            )
            self.connect()
            logging.debug(
                f"Reconnected to manager at {self._address}. "
                "Retrying command..."
            )
            return fn(self, *args, **kwargs)

    return wrapper


class BaseSession:
    """
    Base class for managing a session with a worker pool.

    Provides methods to connect to the manager and shut down the worker pool.
    """

    def __init__(
        self,
        address: tuple[str, int],
        *,
        authkey: bytes | None = None,
    ):
        """
        Initialize the session with the specified address and authentication
        key.

        :param address: The address of the manager (host, port).
        :param authkey: Optional authentication key for the manager.
        """
        self._address: tuple[str, int] = address
        self._authkey: bytes | None = authkey
        self._manager: Manager | None = None

    @property
    def manager(self) -> Manager | None:
        """
        Get the manager instance for the session.

        :return: The manager instance.
        """
        return self._manager

    @property
    def connected(self) -> bool:
        """
        Check if the session is connected to the manager.

        :return: True if connected, False otherwise.
        """
        return self._manager is not None

    def connect(
        self: Self,
        *,
        retries: Zero | Positive[int] = 2,
        interval: Positive[float] = 1,
    ) -> Self:
        """
        Connect to the manager process for the worker pool.

        :param retries: Number of retry attempts if the connection fails.
        :param interval: Interval in seconds between retry attempts.
        :return: The session instance.
        :raises ConnectionError: If the connection fails after all retries.
        """
        if retries < 0:
            raise ValueError("Retries must be a positive integer")
        if not interval > 0:
            raise ValueError("Interval must be a positive float")
        if not self._manager:
            self._manager = Manager(
                address=self._address, authkey=self._authkey
            )
            attempts = threading.Semaphore(retries + 1)
            error = None
            i = 1
            while attempts.acquire(blocking=False):
                logging.debug(
                    f"Attempt {i} of {retries + 1} to connect to manager at "
                    f"{self._address}..."
                )
                try:
                    self._manager.connect()
                except (ConnectionRefusedError, ConnectionResetError) as e:
                    error = e
                    i += 1
                    time.sleep(interval)
                else:
                    break
            else:
                if error:
                    self._manager = None
                    raise error
            logging.debug(
                f"Successfully connected to manager at {self._address}"
            )
        else:
            logging.warning(f"Already connected to manager at {self._address}")
        return self

    def stop(self, wait: bool = True) -> None:
        """
        Shut down the worker pool and close the connection to the manager.

        :param wait: Whether to wait for in-flight tasks to complete before
            shutting down.
        :raises AssertionError: If the manager is not connected.
        """
        assert self._manager
        try:
            if wait and not (waiting := self._manager.waiting()).is_set():
                waiting.set()
            if not (stopped := self._manager.stopping()).is_set():
                stopped.set()
        finally:
            self._manager = None


Self = TypeVar("Self", bound=BaseSession)


class WorkerSession(BaseSession):
    """
    A session for interacting with a worker pool.

    Provides methods to retrieve task futures and interact with the task queue.
    """

    _futures = None
    _queue = None

    @command
    def futures(self) -> WeakValueDictionary[UUID, Future]:
        """
        Retrieve the dictionary of task futures.

        :return: A dictionary of task futures.
        :raises AssertionError: If the manager is not connected.
        """
        assert self.manager
        if not self._futures:
            self._futures = self.manager.futures()
        return self._futures

    @command
    def get(self, *args, **kwargs) -> Task:
        """
        Retrieve a task from the task queue.

        :param args: Additional positional arguments for the queue's `get`
            method.
        :param kwargs: Additional keyword arguments for the queue's `get`
            method.
        :return: The next task in the queue.
        :raises AssertionError: If the manager is not connected.
        """
        assert self.manager
        if not self._queue:
            self._queue = self.manager.queue()
        return self._queue.get(*args, **kwargs)

    def __enter__(self):
        """
        Enter the context of the worker session.

        :return: The session instance.
        """
        if not self.connected:
            self.connect()
        return self

    def __exit__(self, *_):
        """
        Exit the context of the worker session.
        """
        pass


# PUBLIC
def session(
    host: str = "localhost",
    port: int = 48800,
    *,
    authkey: bytes | None = None,
) -> WorkerPoolSession:
    """
    Convenience function to declare a worker pool session context.

    :param host: The hostname of the worker pool.
    :param port: The port of the worker pool.
    :param authkey: Optional authentication key for the worker pool.
    :return: A decorator that wraps the function to execute within the session.

    Usage:

    .. code-block:: python

        import wool


        @wool.session(host="localhost", port=48800)
        async def foo(): ...

    ...is functionally equivalent to...

    .. code-block:: python

        import wool


        async def foo():
            with wool.session(host="localhost", port=48800):
                ...

    This can be used with the ``@wool.task`` decorator to declare a task that
    is tightly coupled with the specified session:

    .. code-block:: python

        import wool


        @wool.session(host="localhost", port=48800)
        @wool.task
        async def foo(): ...

    .. note::

        The order of decorators matters. In order for invocations of the
        declared task to be dispatched to the pool specified by
        ``@wool.session``, the ``@wool.task`` decorator must be applied after
        ``@wool.session``.
    """
    return WorkerPoolSession((host, port), authkey=authkey)


# PUBLIC
def current_session() -> WorkerPoolSession:
    """
    Get the current client session.

    :return: The current client session, or None if no session is active.
    """
    return wool.__wool_session__.get()


# PUBLIC
class WorkerPoolSession(BaseSession):
    """
    A session for managing a pool of workers.

    Provides methods to interact with the worker pool and manage tasks.
    """

    _token: Token | None = None

    def __init__(
        self,
        address: tuple[str, int],
        *,
        authkey: bytes | None = None,
    ):
        """
        Initialize the pool session with the specified address and
        authentication key.

        :param address: The address of the worker pool (host, port).
        :param authkey: Optional authentication key for the worker pool.
        """
        super().__init__(address, authkey=authkey)

    def __call__(self, fn: AsyncCallable) -> AsyncCallable:
        """
        Decorator to execute a function within the context of the pool session.

        :param fn: The function to wrap.
        :return: The wrapped function.
        """

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs) -> Coroutine:
            with self:
                return await fn(*args, **kwargs)

        return wrapper

    def __enter__(self):
        """
        Enter the context of the pool session.

        :return: The session instance.
        """
        if not self.connected:
            self.connect()
        self._token = self.session.set(self)
        return self

    def __exit__(self, *_):
        """
        Exit the context of the pool session.
        """
        assert self._token
        self.session.reset(self._token)

    @property
    def session(self) -> ContextVar[WorkerPoolSession]:
        """
        Get the context variable for the pool session.

        :return: The context variable for the pool session.
        """
        return wool.__wool_session__

    @command
    def put(self, /, wool_task: Task) -> Future:
        """
        Submit a task to the worker pool.

        :param wool_task: The task to submit.
        :return: A future representing the result of the task.
        :raises AssertionError: If the manager is not connected.
        """
        assert self._manager
        return self._manager.put(wool_task)


# PUBLIC
class LocalSession(WorkerPoolSession):
    """
    A session for managing local tasks without a worker pool.

    Provides methods to execute tasks locally and retrieve their results.
    """

    def __init__(self):
        """
        Initialize the local session.
        """
        pass

    @property
    def manager(self) -> Manager | None:
        """
        Get the manager instance for the local session.

        :return: None, as the local session does not use a manager.
        """
        return None

    @property
    def connected(self) -> bool:
        """
        Check if the local session is connected.

        :return: True, as the local session is always connected.
        """
        return True

    def connect(self, *args, **kwargs) -> LocalSession:
        """
        Connect to the local session.

        :return: The local session instance.
        """
        return self

    def put(self, /, wool_task: wool.Task) -> Future:
        """
        Execute a task locally and retrieve its result.

        :param wool_task: The task to execute.
        :return: A future representing the result of the task.
        """
        wool_future = Future()
        loop = asyncio.get_event_loop()
        future = asyncio.run_coroutine_threadsafe(wool_task.run(), loop)
        future.add_done_callback(fulfill(wool_future))
        asyncio.run_coroutine_threadsafe(poll(wool_future, future), loop)
        return wool_future
