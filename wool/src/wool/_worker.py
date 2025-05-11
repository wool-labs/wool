from __future__ import annotations

import asyncio
import logging
import os
from contextvars import ContextVar
from functools import partial
from multiprocessing import Process, current_process
from queue import Empty
from signal import Signals, signal
from threading import Event, Lock, Thread
from time import sleep
from typing import TYPE_CHECKING

import wool
from wool._future import fulfill, poll
from wool._session import PoolSession, WorkerSession

if TYPE_CHECKING:
    from wool._task import WoolTask


def stop(worker: Worker, wait: bool, *_) -> None:
    worker.stop(wait=wait)


_lock: Lock = Lock()


class Scheduler(Thread):
    def __init__(
        self,
        address: tuple[str, int],
        loop: asyncio.AbstractEventLoop,
        stop_event: Event,
        timeout: float = 1,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, name="Scheduler", **kwargs)
        self._address: tuple[str, int] = address
        self._loop: asyncio.AbstractEventLoop = loop
        self._stop_event: Event = stop_event
        self._timeout: float = timeout

    @property
    def session_context(self) -> ContextVar[PoolSession]:
        return wool.__wool_session__

    def run(self) -> None:
        logging.debug("Thread started")
        with WorkerSession(address=self._address) as self.client:
            self.client.connect()
            self.session_context.set(
                PoolSession(address=self._address).connect()
            )
            queue = self.client.queue()
            while not self._stop_event.is_set():
                with _lock:
                    if self._stop_event.is_set():
                        break
                    try:
                        task: WoolTask = queue.get(timeout=self._timeout)
                    except Empty:
                        continue
                    else:
                        self._schedule_task(task, self._loop)
        logging.debug("Thread stopped")

    def _schedule_task(
        self, wool_task: WoolTask, loop: asyncio.AbstractEventLoop
    ) -> None:
        future = self.client.futures().setdefault(
            wool_task.id, wool.WoolFuture()
        )
        task = asyncio.run_coroutine_threadsafe(wool_task.run(), loop)
        task.add_done_callback(fulfill(future))
        asyncio.run_coroutine_threadsafe(poll(future, task), loop)


class Worker(Process):
    def __init__(
        self,
        address: tuple[str, int],
        *args,
        log_level: int = logging.INFO,
        scheduler: type[Scheduler] = Scheduler,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._address: tuple[str, int] = address
        self.log_level: int = log_level
        self._scheduler_type = scheduler

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_event_loop()

    def run(self) -> None:
        wool.__wool_worker__ = self
        self._stop_event = Event()
        self._wait_event = Event()
        signal(Signals.SIGINT, partial(stop, self, True))
        signal(Signals.SIGTERM, partial(stop, self, False))

        if self.log_level:
            wool.__log_level__ = self.log_level
            logging.basicConfig(format=wool.__log_format__)
            logging.getLogger().setLevel(self.log_level)
            logging.info(f"Set log level to {self.log_level}")

        logging.debug("Thread started")

        self.shutdown_sentinel = ShutdownSentinel(
            stop_event=self._stop_event,
            wait_event=self._wait_event,
            loop=self.loop,
        )
        self.shutdown_sentinel.start()

        logging.debug("Spawning scheduler thread...")
        self.scheduler = self._scheduler_type(
            address=self._address,
            loop=self.loop,
            stop_event=self._stop_event,
        )
        self.scheduler.start()

        loop = Thread(target=self.loop.run_forever, name="Loop")
        loop.start()
        loop.join()

    def stop(self, wait: bool = False) -> None:
        if self.pid == current_process().pid:
            with _lock:
                if wait and not self._wait_event.is_set():
                    self._wait_event.set()
                if not self._stop_event.is_set():
                    self._stop_event.set()
            self.scheduler.join()
            self.shutdown_sentinel.join()
            logging.warning("Thread stopped")
        elif self.pid:
            os.kill(self.pid, Signals.SIGINT if wait else Signals.SIGTERM)


class ShutdownSentinel(Thread):
    def __init__(
        self,
        stop_event: Event,
        wait_event: Event,
        loop: asyncio.AbstractEventLoop,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, name="ShutdownSentinel", **kwargs)
        self._stop_event: Event = stop_event
        self._wait_event: Event = wait_event
        self._loop: asyncio.AbstractEventLoop = loop

    def run(self) -> None:
        logging.debug("Thread started")
        self._stop_event.wait()
        logging.debug("Shutdown signal received")
        if not self._wait_event.is_set():
            logging.warning("Cancelling tasks...")
            self._loop.call_soon_threadsafe(self.cancel_tasks, self._loop)
        if tasks := asyncio.all_tasks(self._loop):
            logging.warning("Gathering tasks...")
            future = asyncio.run_coroutine_threadsafe(
                self.gather(*tasks), self._loop
            )
            while not future.done():
                sleep(0.1)
        self._loop.call_soon_threadsafe(self._loop.stop)
        logging.debug("Thread stopped")

    async def gather(self, *tasks: asyncio.Task) -> list:
        return await asyncio.gather(*tasks, return_exceptions=True)

    def cancel_tasks(self, loop):
        for task in asyncio.all_tasks(self._loop):
            if task.get_coro().__name__ != "poll":
                try:
                    task.cancel()
                except asyncio.InvalidStateError:
                    logging.warning(
                        f"Task {task} is already cancelled or finished"
                    )
