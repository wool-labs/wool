from __future__ import annotations

import logging
from time import perf_counter_ns
from typing import TYPE_CHECKING
from typing import Literal

from wool._typing import PassthroughDecorator

if TYPE_CHECKING:
    from wool._task import Task
    from wool._task import TaskEventCallback


# PUBLIC
class TaskEvent:
    """
    Represents an event related to a Wool task, such as creation, queuing,
    scheduling, starting, stopping, or completion.

    Task events are emitted during the lifecycle of a task. These events can
    be used to track task execution and measure performance, such as CPU
    utilization.

    :param type: The type of the task event.
    :param task: The task associated with the event.
    """

    type: TaskEventType
    task: Task

    _handlers: dict[str, list[TaskEventCallback]] = {}

    def __init__(self, type: TaskEventType, /, task: Task) -> None:
        """
        Initialize a WoolTaskEvent instance.

        :param type: The type of the task event.
        :param task: The task associated with the event.
        """
        self.type = type
        self.task = task

    @classmethod
    def handler(
        cls, *event_types: TaskEventType
    ) -> PassthroughDecorator[TaskEventCallback]:
        """
        Register a handler function for specific task event types.

        :param event_types: The event types to handle.
        :return: A decorator to register the handler function.
        """

        def _handler(
            fn: TaskEventCallback,
        ) -> TaskEventCallback:
            for event_type in event_types:
                cls._handlers.setdefault(event_type, []).append(fn)
            return fn

        return _handler

    def emit(self):
        """
        Emit the task event, invoking all registered handlers for the event
        type.

        Handlers are called with the event instance and a timestamp.

        :raises Exception: If any handler raises an exception.
        """
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
TaskEventType = Literal[
    "task-created",
    "task-queued",
    "task-scheduled",
    "task-started",
    "task-stopped",
    "task-completed",
]
"""
Defines the types of events that can occur during the lifecycle of a Wool 
task.

- "task-created": 
    Emitted when a task is created.
- "task-queued": 
    Emitted when a task is added to the queue.
- "task-scheduled": 
    Emitted when a task is scheduled for execution in a worker's event loop.
- "task-started": 
    Emitted when a task starts execution.
- "task-stopped": 
    Emitted when a task stops execution.
- "task-completed": 
    Emitted when a task completes execution.
"""
