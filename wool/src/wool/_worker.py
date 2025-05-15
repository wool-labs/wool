from __future__ import annotations

import asyncio
import logging
from contextvars import ContextVar
from multiprocessing import Pipe
from multiprocessing import Process
from multiprocessing import current_process
from queue import Empty
from signal import Signals
from signal import signal
from threading import Event
from threading import Thread
from time import sleep
from typing import TYPE_CHECKING

import wool
from wool._event import TaskEvent
from wool._future import fulfill
from wool._future import poll
from wool._session import WorkerPoolSession
from wool._session import WorkerSession

if TYPE_CHECKING:
    from wool._task import Task


def _noop(*_):
    pass


class Scheduler(Thread):
    def __init__(
        self,
        address: tuple[str, int],
        loop: asyncio.AbstractEventLoop,
        stop_event: Event,
        ready: Event,
        timeout: float = 1,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, name="Scheduler", **kwargs)
        self._address: tuple[str, int] = address
        self._loop: asyncio.AbstractEventLoop = loop
        self._stop_event: Event = stop_event
        self._timeout: float = timeout
        self._worker_ready: Event = ready

    @property
    def session_context(self) -> ContextVar[WorkerPoolSession]:
        return wool.__wool_session__

    def run(self) -> None:
        logging.debug("Thread started")
        self._worker_ready.wait()
        sleep(0.1)
        with WorkerSession(address=self._address) as self.session:
            self.session_context.set(
                WorkerPoolSession(address=self._address).connect()
            )
            while not self._stop_event.is_set():
                try:
                    task: Task = self.session.get(timeout=self._timeout)
                except Empty:
                    continue
                else:
                    self._schedule_task(task, self._loop)
        logging.debug("Thread stopped")

    def _schedule_task(
        self, wool_task: Task, loop: asyncio.AbstractEventLoop
    ) -> None:
        future = self.session.futures().setdefault(wool_task.id, wool.Future())
        task = asyncio.run_coroutine_threadsafe(wool_task.run(), loop)
        task.add_done_callback(fulfill(future))
        asyncio.run_coroutine_threadsafe(poll(future, task), loop)
        TaskEvent("task-scheduled", task=wool_task).emit()


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
        self._get_stop, self._set_stop = Pipe(duplex=False)
        self._get_ready, self._set_ready = Pipe(duplex=False)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_event_loop()

    def start(self):
        super().start()
        self._get_ready.recv()
        self._get_ready.close()

    def run(self) -> None:
        signal(Signals.SIGINT, _noop)
        wool.__wool_worker__ = self
        self._set_stop.close()
        self._stop_event = Event()
        self._wait_event = Event()

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
            ready=(_ready_event := Event()),
        )
        self.scheduler.start()

        loop = Thread(target=self.loop.run_forever, name="EventLoop")
        loop.start()

        self._set_ready.send(True)
        self._set_ready.close()
        _ready_event.set()
        self.stop(self._get_stop.recv())
        self._get_stop.close()
        self.scheduler.join()
        loop.join()
        self.shutdown_sentinel.join()
        logging.info("Thread stopped")

    def stop(self, wait: bool = True) -> None:
        if self.pid == current_process().pid:
            if wait and not self._wait_event.is_set():
                self._wait_event.set()
            if not self._stop_event.is_set():
                self._stop_event.set()
        elif self.pid:
            self._set_stop.send(wait)


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
            asyncio.run_coroutine_threadsafe(self._cancel_tasks(), self._loop)
        if tasks := asyncio.all_tasks(self._loop):
            logging.info("Gathering tasks...")
            future = asyncio.run_coroutine_threadsafe(
                self._gather(*tasks), self._loop
            )
            while not future.done():
                sleep(0.1)
        self._loop.call_soon_threadsafe(self._loop.stop)
        logging.debug("Thread stopped")

    async def _gather(self, *tasks: asyncio.Task) -> list:
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _cancel_tasks(self):
        for task in asyncio.all_tasks(self._loop):
            if task == asyncio.current_task(self._loop):
                continue
            if task.get_coro():
                if task.cancel():
                    logging.debug(f"Cancelled task {task.get_coro()}")
        await asyncio.sleep(0)
