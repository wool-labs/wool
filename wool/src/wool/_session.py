from __future__ import annotations

import asyncio
import functools
import logging
import threading
import time
from contextvars import ContextVar
from contextvars import Token
from typing import TYPE_CHECKING
from typing import Callable
from typing import Coroutine
from typing import TypeVar
from uuid import UUID
from weakref import WeakValueDictionary

import wool
from wool._future import WoolFuture
from wool._future import fulfill
from wool._future import poll
from wool._manager import Manager

if TYPE_CHECKING:
    from wool._task import AsyncCallable
    from wool._task import WoolTask
    from wool._typing import Positive
    from wool._typing import Zero


def command(fn):
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
    def __init__(
        self,
        address: tuple[str, int],
        *,
        authkey: bytes | None = None,
    ):
        self._address: tuple[str, int] = address
        self._authkey: bytes | None = authkey
        self._manager: Manager | None = None

    @property
    def manager(self) -> Manager | None:
        return self._manager

    @property
    def connected(self) -> bool:
        return self._manager is not None

    def connect(
        self: Self,
        *,
        retries: Zero | Positive[int] = 2,
        interval: Positive[float] = 1,
    ) -> Self:
        """Establish a connection to the manager at the specified address."""
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
        """
        assert self._manager
        try:
            if wait and not (waiting := self._manager.waiting()).is_set():
                waiting.set()
            if not (stopped := self._manager.stopping()).is_set():
                stopped.set()
        except ConnectionRefusedError:
            logging.warning(
                f"Connection to manager at {self._address} refused."
            )
        finally:
            self._manager = None


Self = TypeVar("Self", bound=BaseSession)


class WorkerSession(BaseSession):
    _futures = None
    _queue = None

    @command
    def futures(self) -> WeakValueDictionary[UUID, WoolFuture]:
        assert self.manager
        if not self._futures:
            self._futures = self.manager.futures()
        return self._futures

    @command
    def get(self, *args, **kwargs) -> WoolTask:
        assert self.manager
        if not self._queue:
            self._queue = self.manager.queue()
        return self._queue.get(*args, **kwargs)

    def __enter__(self):
        if not self.connected:
            self.connect()
        return self

    def __exit__(self, *_):
        pass


# PUBLIC
def session(
    address: tuple[str, int],
    *,
    authkey: bytes | None = None,
) -> Callable[[AsyncCallable], AsyncCallable]:
    def _session(fn: AsyncCallable) -> AsyncCallable:
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs) -> Coroutine:
            with PoolSession(address, authkey=authkey):
                return await fn(*args, **kwargs)

        return wrapper

    return _session


# PUBLIC
def current_client() -> PoolSession | None:
    return wool.__wool_session__.get()


# PUBLIC
class PoolSession(BaseSession):
    _token: Token | None = None

    def __init__(
        self,
        address: tuple[str, int],
        *,
        authkey: bytes | None = None,
    ):
        super().__init__(address, authkey=authkey)

    def __enter__(self):
        if not self.connected:
            self.connect()
        self._token = self.session.set(self)
        return self

    def __exit__(self, *_):
        assert self._token
        self.session.reset(self._token)

    @property
    def session(self) -> ContextVar[PoolSession]:
        return wool.__wool_session__

    @command
    def put(self, /, wool_task: WoolTask) -> WoolFuture:
        assert self._manager
        return self._manager.put(wool_task)


# PUBLIC
class LocalSession(PoolSession):
    def __init__(self):
        pass

    @property
    def manager(self) -> Manager | None:
        return None

    @property
    def connected(self) -> bool:
        return True

    def connect(self, *args, **kwargs) -> LocalSession:
        return self

    def put(self, /, wool_task: wool.WoolTask) -> WoolFuture:
        wool_future = WoolFuture()
        loop = asyncio.get_event_loop()
        future = asyncio.run_coroutine_threadsafe(wool_task.run(), loop)
        future.add_done_callback(fulfill(wool_future))
        asyncio.run_coroutine_threadsafe(poll(wool_future, future), loop)
        return wool_future
