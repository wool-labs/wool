from __future__ import annotations

import logging
from time import perf_counter_ns
from typing import TYPE_CHECKING
from typing import Literal

from wool._typing import PassthroughDecorator

if TYPE_CHECKING:
    from wool._task import WoolTask
    from wool._task import WoolTaskEventCallback


# PUBLIC
class WoolTaskEvent:
    """
    Task events are emitted when a task is created, queued, started, stopped,
    and completed. Tasks can be started and stopped multiple times by the event
    loop. The cumulative time between start and stop events can be used to
    determine a task's CPU utilization.
    """

    type: WoolTaskEventType
    task: WoolTask

    _handlers: dict[str, list[WoolTaskEventCallback]] = {}

    def __init__(self, type: WoolTaskEventType, /, task: WoolTask) -> None:
        self.type = type
        self.task = task

    @classmethod
    def handler(
        cls, *event_types: WoolTaskEventType
    ) -> PassthroughDecorator[WoolTaskEventCallback]:
        def _handler(
            fn: WoolTaskEventCallback,
        ) -> WoolTaskEventCallback:
            for event_type in event_types:
                cls._handlers.setdefault(event_type, []).append(fn)
            return fn

        return _handler

    def emit(self):
        logging.debug(
            f"Emitting {self.type} event for "
            f"task {self.task.id} "
            f"({self.task.callable.__qualname__})"
        )
        if handlers := self._handlers.get(self.type):
            timestamp = perf_counter_ns()
            for handler in handlers:
                handler(self, timestamp)


# PUBLIC
WoolTaskEventType = Literal[
    "task-created",
    "task-queued",
    "task-scheduled",
    "task-started",
    "task-stopped",
    "task-completed",
]
