"""Core work distribution functionality.

This module provides task decoration, execution context, and task classes.
"""

from wool.runtime.work.task import WorkTask
from wool.runtime.work.task import WorkTaskEvent
from wool.runtime.work.task import WorkTaskEventHandler
from wool.runtime.work.task import WorkTaskEventType
from wool.runtime.work.task import WorkTaskException
from wool.runtime.work.task import current_task
from wool.runtime.work.wrapper import routine
from wool.runtime.work.wrapper import work

__all__ = [
    "WorkTask",
    "WorkTaskEvent",
    "WorkTaskEventHandler",
    "WorkTaskEventType",
    "WorkTaskException",
    "current_task",
    "routine",
    "work",
]
