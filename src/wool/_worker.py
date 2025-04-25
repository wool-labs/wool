from __future__ import annotations

import asyncio
import logging
import os
from functools import partial
from multiprocessing import Process, current_process
from queue import Empty
from signal import Signals, signal
from threading import Event, Thread
from time import sleep
from typing import TYPE_CHECKING

import wool
from wool._client import WoolClient, WorkerClient
from wool._future import fulfill, poll

if TYPE_CHECKING:
    from wool._task import WoolTask


def stop(worker: Worker, wait: bool, *_) -> None:
    worker.stop(wait=wait)


class Worker(Process):
    def __init__(
        self,
        address: tuple[str, int],
        *args,
        log_level: int = logging.INFO,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._address: tuple[str, int] = address
        self.log_level: int = log_level

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return asyncio.get_event_loop()

    def run(self) -> None:
        wool.__wool_worker__ = self
        self._stop_event = Event()
        signal(Signals.SIGINT, partial(stop, self, True))
        signal(Signals.SIGTERM, partial(stop, self, False))

        if self.log_level:
            wool.__log_level__ = self.log_level
            logging.basicConfig(format=wool.__log_format__)
            logging.getLogger().setLevel(self.log_level)
            logging.info(f"Set log level to {self.log_level}")

        logging.debug("Thread started")

        shutdown_sentinel = ShutdownSentinel(
            stop_event=self._stop_event, loop=self.loop
        )
        shutdown_sentinel.start()

        logging.debug("Spawning scheduler thread...")
        scheduler = Scheduler(
            address=self._address,
            loop=self.loop,
            stop_event=self._stop_event,
        )
        scheduler.start()

        self.loop.run_forever()

        scheduler.join()
        shutdown_sentinel.join()
        logging.debug("Thread stopped")

    def stop(self, wait: bool = False) -> None:
        if self.pid == current_process().pid:
            if not self._stop_event.is_set():
                if not wait:
                    for task in asyncio.all_tasks(self.loop):
                        task.cancel()
            self._stop_event.set()
        elif self.pid:
            os.kill(self.pid, Signals.SIGINT if wait else Signals.SIGTERM)


class ShutdownSentinel(Thread):
    def __init__(
        self,
        stop_event: Event,
        loop: asyncio.AbstractEventLoop,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, name="ShutdownSentinel", **kwargs)
        self._stop_event: Event = stop_event
        self._loop: asyncio.AbstractEventLoop = loop

    def run(self) -> None:
        logging.debug("Thread started")
        self._stop_event.wait()
        if tasks := asyncio.all_tasks(self._loop):
            future = asyncio.run_coroutine_threadsafe(
                self.gather(*tasks), self._loop
            )
            while not future.done():
                sleep(0.1)
        self._loop.call_soon_threadsafe(self._loop.stop)
        logging.debug("Thread stopped")

    async def gather(self, *tasks: asyncio.Task) -> list:
        return await asyncio.gather(*tasks, return_exceptions=True)


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

    def run(self) -> None:
        logging.debug("Thread started")
        self.client = WorkerClient(address=self._address)
        self.client.connect()
        wool.__wool_client__.set(WoolClient(address=self._address))
        queue = self.client.queue()
        while not self._stop_event.is_set():
            try:
                task: WoolTask = queue.get(timeout=self._timeout)
            except Empty:
                continue
            else:
                self._schedule_task(task, self._loop)
        else:
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
