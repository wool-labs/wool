from __future__ import annotations

import logging
import os
from copy import copy
from functools import partial, wraps
from multiprocessing import Process, current_process
from multiprocessing.managers import Server
from signal import Signals, signal
from threading import Event, Lock, Thread, current_thread
from time import sleep
from typing import Callable, Coroutine
from weakref import WeakSet

import wool
from wool._client import WoolClient
from wool._manager import Manager
from wool._task import AsyncCallable
from wool._worker import Worker


# PUBLIC
def pool(
    address: tuple[str, int],
    *,
    authkey: bytes | None = None,
    breadth: int = 0,
    log_level: int = logging.INFO,
) -> Callable[[AsyncCallable], AsyncCallable]:
    def _pool(fn: AsyncCallable) -> AsyncCallable:
        @wraps(fn)
        async def wrapper(*args, **kwargs) -> Coroutine:
            with WoolPool(
                breadth=breadth,
                address=address,
                authkey=authkey,
                log_level=log_level,
            ):
                return await fn(*args, **kwargs)

        return wrapper

    return _pool


# PUBLIC
class WoolPool(Process):
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
        self._outer_client: WoolClient | None = None
        self._client = WoolClient(address=self._address)

    def __enter__(self):
        self.start()
        self._outer_client = wool.__wool_client__.get()
        wool.__wool_client__.set(self._client)

    def __exit__(self, *_) -> None:
        assert self._outer_client
        wool.__wool_client__.set(self._outer_client)
        self._outer_client = None
        assert self.pid
        self.stop()
        self.join()

    @property
    def log_level(self) -> int:
        return self._log_level

    @log_level.setter
    def log_level(self, value: int) -> None:
        if value < 0:
            raise ValueError("Log level must be non-negative")
        self._log_level = value

    @property
    def breadth(self) -> int:
        return self._breadth

    def run(self) -> None:
        if self.log_level:
            wool.__log_level__ = self.log_level
            logging.basicConfig(format=wool.__log_format__)
            logging.getLogger().setLevel(self.log_level)
            logging.info(f"Set log level to {self.log_level}")

        logging.debug("Thread started")

        signal(Signals.SIGINT, partial(stop, self, True))
        signal(Signals.SIGTERM, partial(stop, self, False))

        self.lock = Lock()
        self._worker_sentinels = WeakSet()

        self._manager = Manager(address=self._address, authkey=self.authkey)

        server = self._manager.get_server()
        server_thread = Thread(
            target=server.serve_forever, name="ServerThread", daemon=True
        )
        server_thread.start()

        self._manager.connect()

        self._stop_event = Event()
        shutdown_sentinel = ShutdownSentinel(
            self._stop_event, server, self._worker_sentinels
        )
        shutdown_sentinel.start()

        with self.lock:
            for i in range(1, self.breadth + 1):
                if self._stop_event.is_set():
                    break
                logging.debug(f"Spawning worker {i}...")
                worker_sentinel = WorkerSentinel(
                    address=self._address,
                    log_level=self.log_level,
                    id=i,
                    lock=self.lock,
                )
                worker_sentinel.start()
                self._worker_sentinels.add(worker_sentinel)

        current_thread().name = "IdleSentinel"
        while not self.idle() and not self._stop_event.is_set():
            self._stop_event.wait(1)
        else:
            self.stop()

        server_thread.join()
        for worker_sentinel in self._worker_sentinels:
            if worker_sentinel.is_alive():
                worker_sentinel.join()
        if shutdown_sentinel.is_alive():
            shutdown_sentinel.join()

    def idle(self):
        assert self._manager
        try:
            return self._manager.queue().idle()
        except (ConnectionRefusedError, ConnectionResetError):
            return True

    def stop(self, *, wait: bool = True) -> None:
        if self.pid == current_process().pid:
            with self.lock:
                if self._stop_event and not self._stop_event.is_set():
                    self._stop_event.set()
                    for worker_sentinel in self._worker_sentinels:
                        if worker_sentinel.is_alive():
                            worker_sentinel.stop(wait=wait)
        elif self.pid:
            if not self._client.connected:
                self._client.connect()
            self._client.stop(wait=wait)


class WorkerSentinel(Thread):
    _worker: Worker | None = None

    def __init__(
        self,
        address: tuple[str, int],
        *args,
        id: int,
        lock: Lock,
        cooldown: float = 1,
        log_level: int = logging.INFO,
        **kwargs,
    ) -> None:
        self._address: tuple[str, int] = address
        self._id: int = id
        self._lock: Lock = lock
        self._cooldown: float = cooldown
        self._log_level: int = log_level
        self._stop_event: Event = Event()
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
    def lock(self) -> Lock:
        return self._lock

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

    @log_level.setter
    def log_level(self, value: int) -> None:
        if value < 0:
            raise ValueError("Log level must be non-negative")
        self._log_level = value

    def run(self) -> None:
        logging.debug("Thread started")
        while not self._stop_event.is_set():
            worker = Worker(
                address=self._address,
                name=f"Worker-{self.id}",
                log_level=self.log_level,
            )
            with self.lock:
                if self._stop_event.is_set():
                    logging.debug("Worker interrupted before starting")
                    break
                worker.start()
                self._worker = worker
                logging.info(f"Spawned worker process {worker.pid}")
                sleep(0.05)
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
        if not self._stop_event.is_set():
            self._stop_event.set()
        if self.worker:
            self.worker.stop(wait=wait)


class ShutdownSentinel(Thread):
    def __init__(
        self,
        stop: Event,
        server: Server,
        worker_sentinels: WeakSet[WorkerSentinel],
        *args,
        **kwargs,
    ) -> None:
        self._stop_event: Event = stop
        self._server: Server = server
        self._worker_sentinels: WeakSet[WorkerSentinel] = worker_sentinels
        super().__init__(*args, name="ShutdownSentinel", **kwargs)

    @property
    def stop(self) -> Event:
        return self._stop_event

    @property
    def server(self) -> Server:
        return self._server

    @property
    def worker_sentinels(self) -> WeakSet[WorkerSentinel]:
        return self._worker_sentinels

    def run(self) -> None:
        logging.debug("Thread started")
        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(1)
            else:
                logging.debug("Stopping workers...")
                for worker_sentinel in copy(self._worker_sentinels):
                    worker_sentinel.join()
                stop_event = getattr(self.server, "stop_event")
                assert isinstance(stop_event, Event)
                logging.debug("Stopping manager...")
                stop_event.set()
        finally:
            logging.debug("Thread stopped")


def stop(pool: WoolPool, wait: bool, *_):
    pool.stop(wait=wait)
