import logging
from contextvars import ContextVar
from importlib.metadata import entry_points
from typing import Final

from tblib import pickling_support

from wool._cli import PoolCommand
from wool._cli import cli
from wool._future import WoolFuture
from wool._logging import __log_format__
from wool._pool import Pool
from wool._session import LocalSession
from wool._session import PoolSession
from wool._task import WoolTask
from wool._task import WoolTaskEvent
from wool._task import WoolTaskEventCallback
from wool._task import WoolTaskException
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
__wool_session__: Final[ContextVar[PoolSession]] = ContextVar(
    "__wool_session__", default=LocalSession()
)

__wool_worker__: Worker | None = None

__all__ = [
    "WoolTaskException",
    "WoolFuture",
    "WoolTask",
    "WoolTaskEvent",
    "WoolTaskEventCallback",
    "Pool",
    "PoolSession",
    "PoolCommand",
    "Scheduler",
    "__log_format__",
    "__log_level__",
    "__wool_session__",
    "cli",
    "current_task",
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
