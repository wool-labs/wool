from __future__ import annotations

import logging
import os
from contextvars import ContextVar
from contextvars import Token
from functools import partial
from functools import wraps
from multiprocessing import Pipe
from multiprocessing import Process
from multiprocessing import current_process
from multiprocessing.managers import Server
from signal import Signals
from signal import signal
from threading import Event
from threading import Semaphore
from threading import Thread
from typing import TYPE_CHECKING
from typing import Callable
from typing import Coroutine

import wool
from wool._manager import Manager
from wool._session import WorkerPoolSession
from wool._worker import Scheduler
from wool._worker import Worker

if TYPE_CHECKING:
    from wool._queue import TaskQueue
    from wool._task import AsyncCallable


def _stop(pool: WorkerPool, wait: bool, *_):
    pool.stop(wait=wait)


# PUBLIC
def pool(
    host: str = "localhost",
    port: int = 48800,
    *,
    authkey: bytes | None = None,
    breadth: int = 0,
    log_level: int = logging.INFO,
) -> Callable[[AsyncCallable], AsyncCallable]:
    """
    Convenience function to declare a worker pool context.

    :param host: The hostname of the worker pool. Defaults to "localhost".
    :param port: The port of the worker pool. Defaults to 48800.
    :param authkey: Optional authentication key for the worker pool.
    :param breadth: Number of worker processes in the pool. Defaults to 0
        (CPU count).
    :param log_level: Logging level for the worker pool. Defaults to
        logging.INFO.
    :return: A decorator that wraps the function to execute within the session.

    Usage:

    .. code-block:: python

        import wool


        @wool.pool(
            host="localhost",
            port=48800,
            authkey=b"deadbeef",
            breadth=4,
        )
        async def foo(): ...

    This is equivalent to:

    .. code-block:: python

        import wool


        async def foo():
            with wool.pool(
                host="localhost", port=48800, authkey=b"deadbeef", breadth=4
            ):
                ...

    This decorator can also be combined with the ``@wool.task`` decorator to
    declare a task that is tightly coupled with the specified pool:

    .. code-block:: python

        import wool


        @wool.pool(
            host="localhost",
            port=48800,
            authkey=b"deadbeef",
            breadth=4,
        )
        @wool.task
        async def foo(): ...

    .. note::

        The order of decorators matters. To ensure that invocations of the
        declared task are dispatched to the pool specified by ``@wool.pool``,
        the ``@wool.task`` decorator must be applied after ``@wool.pool``.
    """
    return WorkerPool(
        address=(host, port),
        authkey=authkey,
        breadth=breadth,
        log_level=log_level,
    )


# PUBLIC
class WorkerPool(Process):
    """
    A multiprocessing-based worker pool for executing asynchronous tasks. A
    pool consists of a single manager process and at least a single worker
    process. The manager process orchestrates its workers and serves client
    dispatch requests. The worker process(es) execute(s) dispatched tasks on a
    first-come, first-served basis.

    The worker pool class is implemented as a context manager and decorator,
    allowing users to easily spawn ephemeral pools that live for the duration
    of a client application's execution and tightly couple functions to a pool.

    :param address: The address of the worker pool (host, port).
    :param authkey: Optional authentication key for the pool. If not specified,
        the manager will inherit the authkey from the current process.
    :param breadth: Number of worker processes in the pool. Defaults to CPU
        count.
    :param log_level: Logging level for the pool.
    """

    _wait_event: Event | None = None
    _stop_event: Event | None = None

    def __init__(
        self,
        address: tuple[str, int] = ("localhost", 5050),
        *args,
        authkey: bytes | None = None,
        breadth: int = 0,
        log_level: int = logging.INFO,
        **kwargs,
    ) -> None:
        super().__init__(*args, name=self.__class__.__name__, **kwargs)
        if authkey is not None:
            self.authkey: bytes = authkey
        if not breadth:
            if not (breadth := (os.cpu_count() or 0)):
                raise ValueError("Unable to determine CPU count")
        if not breadth > 0:
            raise ValueError("Breadth must be a positive integer")
        self._breadth: int = breadth
        self._address: tuple[str, int] = address
        self._log_level: int = log_level
        self._token: Token | None = None
        self._session = self.session_type(
            address=self._address, authkey=self.authkey
        )
        self._get_ready, self._set_ready = Pipe(duplex=False)

    def __call__(self, fn: AsyncCallable) -> AsyncCallable:
        """
        Decorate a function to be executed within the pool.

        :param fn: The function to be executed.
        :return: The wrapped function.
        """

        @wraps(fn)
        async def wrapper(*args, **kwargs) -> Coroutine:
            with self:
                return await fn(*args, **kwargs)

        return wrapper

    def __enter__(self):
        """
        Enter the context of the pool, starting the pool and connecting the
        session.
        """
        self.start()
        self._session.connect()
        self._token = self.session_context.set(self._session)

    def __exit__(self, *_) -> None:
        """
        Exit the context of the pool, stopping the pool and disconnecting the
        session.
        """
        assert self._token
        self.session_context.reset(self._token)
        assert self.pid
        self.stop(wait=True)
        self.join()

    @property
    def session_type(self) -> type[WorkerPoolSession]:
        """
        Get the session type for the pool.

        :return: The session type.
        """
        return WorkerPoolSession

    @property
    def session_context(self) -> ContextVar[WorkerPoolSession]:
        """
        Get the session context variable for the pool.

        :return: The session context variable.
        """
        return wool.__wool_session__

    @property
    def scheduler_type(self) -> type[Scheduler]:
        """
        Get the scheduler type for the pool.

        :return: The scheduler type.
        """
        return Scheduler

    @property
    def log_level(self) -> int:
        """
        Get the logging level for the pool.

        :return: The logging level.
        """
        return self._log_level

    @log_level.setter
    def log_level(self, value: int) -> None:
        """
        Set the logging level for the pool.

        :param value: The new logging level.
        """
        if value < 0:
            raise ValueError("Log level must be non-negative")
        self._log_level = value

    @property
    def breadth(self) -> int:
        """
        Get the number of worker processes in the pool.

        :return: The number of worker processes.
        """
        return self._breadth

    @property
    def waiting(self) -> bool | None:
        """
        Check if the pool is in a waiting state.

        :return: True if waiting, False otherwise, or None if undefined.
        """
        return self._wait_event and self._wait_event.is_set()

    @property
    def stopping(self) -> bool | None:
        """
        Check if the pool is in a stopping state.

        :return: True if stopping, False otherwise, or None if undefined.
        """
        return self._stop_event and self._stop_event.is_set()

    def start(self) -> None:
        """
        Start the pool process and wait for it to be ready.
        """
        super().start()
        self._get_ready.recv()
        self._get_ready.close()

    def run(self) -> None:
        """
        Run the pool process, managing workers and the manager process.
        """
        if self.log_level:
            wool.__log_level__ = self.log_level
            logging.basicConfig(format=wool.__log_format__)
            logging.getLogger().setLevel(self.log_level)
            logging.info(f"Set log level to {self.log_level}")

        logging.debug("Thread started")

        signal(Signals.SIGINT, partial(_stop, self, False))
        signal(Signals.SIGTERM, partial(_stop, self, True))

        self.manager_sentinel = ManagerSentinel(
            address=self._address, authkey=self.authkey
        )
        self.manager_sentinel.start()

        self._wait_event = self.manager_sentinel.waiting
        self._stop_event = self.manager_sentinel.stopping

        worker_sentinels = []
        logging.info("Spawning workers...")
        try:
            for i in range(1, self.breadth + 1):
                if not self._stop_event.is_set():
                    worker_sentinel = WorkerSentinel(
                        address=self._address,
                        log_level=self.log_level,
                        id=i,
                        scheduler=self.scheduler_type,
                    )
                    worker_sentinel.start()
                    worker_sentinels.append(worker_sentinel)
            for worker_sentinel in worker_sentinels:
                worker_sentinel.ready.wait()
            self._set_ready.send(True)
            self._set_ready.close()
        except Exception:
            logging.exception("Error in worker pool")
            raise
        finally:
            while not self.idle and not self.stopping:
                self._stop_event.wait(1)
            else:
                self.stop(wait=bool(self.idle or self.waiting))

            logging.info("Stopping workers...")
            for worker_sentinel in worker_sentinels:
                if worker_sentinel.is_alive():
                    worker_sentinel.stop(wait=self.waiting)
            for worker_sentinel in worker_sentinels:
                worker_sentinel.join()

            logging.info("Stopping manager...")
            if self.manager_sentinel.is_alive():
                self.manager_sentinel.stop()
                self.manager_sentinel.join()

    @property
    def idle(self):
        """
        Check if the pool is idle.

        :return: True if idle, False otherwise.
        """
        assert self.manager_sentinel
        try:
            return self.manager_sentinel.idle
        except (ConnectionRefusedError, ConnectionResetError):
            return True

    def stop(self, *, wait: bool = True) -> None:
        """
        Stop the pool process.

        :param wait: Whether to wait for the pool to stop gracefully.
        """
        if self.pid == current_process().pid:
            if wait and self.waiting is False:
                assert self._wait_event
                self._wait_event.set()
            if self.stopping is False:
                assert self._stop_event
                self._stop_event.set()
        elif self.pid:
            if not self._session.connected:
                self._session.connect()
            self._session.stop(wait=wait)


class ManagerSentinel(Thread):
    _wait_event: Event | None = None
    _stop_event: Event | None = None
    _queue: TaskQueue | None = None

    def __init__(
        self, address: tuple[str, int], authkey: bytes, *args, **kwargs
    ) -> None:
        self._manager: Manager = Manager(address=address, authkey=authkey)
        self._server: Server = self._manager.get_server()
        super().__init__(*args, name=self.__class__.__name__, **kwargs)

    @property
    def waiting(self) -> Event:
        if not self._wait_event:
            self._manager.connect()
            self._wait_event = self._manager.waiting()
        return self._wait_event

    @property
    def stopping(self) -> Event:
        if not self._stop_event:
            self._manager.connect()
            self._stop_event = self._manager.stopping()
        return self._stop_event

    @property
    def idle(self) -> bool | None:
        if not self._queue:
            self._manager.connect()
            self._queue = self._manager.queue()
        return self._queue.idle()

    def run(self) -> None:
        self._server.serve_forever()

    def stop(self) -> None:
        stop_event = getattr(self._server, "stop_event")
        assert isinstance(stop_event, Event)
        logging.debug("Stopping manager...")
        stop_event.set()


class WorkerSentinel(Thread):
    _worker: Worker | None = None
    _semaphore: Semaphore = Semaphore(8)

    def __init__(
        self,
        address: tuple[str, int],
        *args,
        id: int,
        cooldown: float = 1,
        log_level: int = logging.INFO,
        scheduler: type[Scheduler] = Scheduler,
        **kwargs,
    ) -> None:
        self._address: tuple[str, int] = address
        self._id: int = id
        self._cooldown: float = cooldown
        self._log_level: int = log_level
        self._scheduler_type = scheduler
        self._stop_event: Event = Event()
        self._wait_event: Event = Event()
        self._ready: Event = Event()
        super().__init__(
            *args, name=f"{self.__class__.__name__}-{self.id}", **kwargs
        )

    @property
    def worker(self) -> Worker | None:
        return self._worker

    @property
    def id(self) -> int:
        return self._id

    @property
    def ready(self) -> Event:
        return self._ready

    @property
    def cooldown(self) -> float:
        return self._cooldown

    @cooldown.setter
    def cooldown(self, value: float) -> None:
        if value < 0:
            raise ValueError("Cooldown must be non-negative")
        self._cooldown = value

    @property
    def log_level(self) -> int:
        return self._log_level

    @property
    def waiting(self) -> bool:
        return self._wait_event.is_set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    @log_level.setter
    def log_level(self, value: int) -> None:
        if value < 0:
            raise ValueError("Log level must be non-negative")
        self._log_level = value

    def start(self) -> None:
        super().start()

    def run(self) -> None:
        logging.debug("Thread started")
        while not self._stop_event.is_set():
            worker = Worker(
                address=self._address,
                name=f"Worker-{self.id}",
                log_level=self.log_level,
                scheduler=self._scheduler_type,
            )
            with self._semaphore:
                worker.start()
            self._worker = worker
            logging.info(f"Spawned worker process {worker.pid}")
            self._ready.set()
            try:
                worker.join()
            except Exception as e:
                logging.error(e)
            finally:
                logging.info(f"Terminated worker process {worker.pid}")
                self._worker = None
                self._stop_event.wait(self.cooldown)
        logging.debug("Thread stopped")

    def stop(self, *, wait: bool = True) -> None:
        logging.info(f"Stopping thread {self.name}...")
        if wait and not self.waiting:
            self._wait_event.set()
        if not self.stopping:
            self._stop_event.set()
        if self._worker:
            self._worker.stop(wait=self._wait_event.is_set())
