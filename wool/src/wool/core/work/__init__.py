"""Core work distribution functionality.

This module provides task decoration, execution context, and task classes.
"""

from wool.core.work.task import WorkTask
from wool.core.work.task import WorkTaskEvent
from wool.core.work.task import WorkTaskEventCallback
from wool.core.work.task import WorkTaskEventType
from wool.core.work.task import WorkTaskException
from wool.core.work.task import current_task
from wool.core.work.wrapper import routine
from wool.core.work.wrapper import work

__all__ = [
    "WorkTask",
    "WorkTaskEvent",
    "WorkTaskEventCallback",
    "WorkTaskEventType",
    "WorkTaskException",
    "current_task",
    "routine",
    "work",
]
