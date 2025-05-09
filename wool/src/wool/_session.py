from __future__ import annotations

import asyncio
import functools
import logging
import threading
import time
from contextvars import ContextVar
from typing import TYPE_CHECKING, Callable, Coroutine, TypeVar
from uuid import UUID
from weakref import WeakValueDictionary

import wool
from wool._future import WoolFuture, fulfill, poll
from wool._manager import Manager

if TYPE_CHECKING:
    from wool._queue import TaskQueue
    from wool._task import AsyncCallable, WoolTask
    from wool._typing import Positive, Zero


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
                f"Reconnected to manager at {self._address}. Retrying command..."
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
            self._manager = Manager(address=self._address, authkey=self._authkey)
            attempts = threading.Semaphore(retries + 1)
            error = None
            i = 1
            while attempts.acquire(blocking=False):
                logging.debug(
                    f"Attempt {i} of {retries + 1} to connect to manager at {self._address}..."
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
            logging.info(f"Successfully connected to manager at {self._address}")
        else:
            logging.warning(f"Already connected to manager at {self._address}")
        return self

    def stop(self, wait: bool = False) -> None:
        """Shut down the worker pool and close the connection to the manager."""
        assert self._manager
        try:
            self._manager.stop(wait=wait)
        except ConnectionRefusedError:
            logging.warning(f"Connection to manager at {self._address} refused.")
        finally:
            self._manager = None


Self = TypeVar("Self", bound=BaseSession)


class WorkerSession(BaseSession):
    @command
    def futures(self) -> WeakValueDictionary[UUID, WoolFuture]:
        assert self.manager
        return self.manager.futures()

    @command
    def queue(self) -> TaskQueue[WoolTask]:
        assert self.manager
        return self.manager.queue()

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
    def __init__(
        self,
        address: tuple[str, int],
        *,
        authkey: bytes | None = None,
    ):
        super().__init__(address, authkey=authkey)
        self._outer_client: PoolSession | None = None

    def __enter__(self):
        if not self.connected:
            self.connect()
        self._outer_client = self.session.get()
        self.session.set(self)

    def __exit__(self, *_):
        self.session.set(self._outer_client)
        self._outer_client = None

    @property
    def session(self) -> ContextVar[PoolSession]:
        return wool.__wool_session__

    @command
    def put(self, task: WoolTask) -> WoolFuture:
        assert self._manager
        return self._manager.put(task, worker=bool(wool.__wool_worker__))


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

    def put(self, wool_task: wool.WoolTask) -> WoolFuture:
        future = WoolFuture()
        loop = asyncio.get_event_loop()
        task = asyncio.run_coroutine_threadsafe(wool_task.run(), loop)
        task.add_done_callback(fulfill(future))
        asyncio.run_coroutine_threadsafe(poll(future, task), loop)
        return future
