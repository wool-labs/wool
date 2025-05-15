import logging
from contextvars import ContextVar
from importlib.metadata import entry_points
from typing import Final

from tblib import pickling_support

from wool._cli import WorkerPoolCommand
from wool._cli import cli
from wool._future import Future
from wool._logging import __log_format__
from wool._pool import WorkerPool
from wool._pool import pool
from wool._session import LocalSession
from wool._session import WorkerPoolSession
from wool._session import session
from wool._task import Task
from wool._task import TaskEvent
from wool._task import TaskEventCallback
from wool._task import TaskException
from wool._task import current_task
from wool._task import task
from wool._worker import Scheduler
from wool._worker import Worker

pickling_support.install()

# PUBLIC
__log_format__: str = __log_format__

# PUBLIC
__log_level__: int = logging.INFO

# PUBLIC
__wool_session__: Final[ContextVar[WorkerPoolSession]] = ContextVar(
    "__wool_session__", default=LocalSession()
)

__wool_worker__: Worker | None = None

__all__ = [
    "TaskException",
    "Future",
    "Task",
    "TaskEvent",
    "TaskEventCallback",
    "WorkerPool",
    "WorkerPoolSession",
    "WorkerPoolCommand",
    "Scheduler",
    "__log_format__",
    "__log_level__",
    "__wool_session__",
    "cli",
    "current_task",
    "pool",
    "session",
    "task",
]

for symbol in __all__:
    attribute = globals().get(symbol)
    try:
        if attribute and "wool" in attribute.__module__.split("."):
            # Set the module to reflect imports of the symbol
            attribute.__module__ = __name__
    except AttributeError:
        continue

for plugin in entry_points(group="wool_cli_plugins"):
    try:
        plugin.load()
        logging.info(f"Loaded CLI plugin {plugin.name}")
    except Exception as e:
        logging.error(f"Failed to load CLI plugin {plugin.name}: {e}")
        raise
