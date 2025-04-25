import logging
from contextvars import ContextVar
from typing import Final, Literal

from wool._client import NullClient, WoolClient
from wool._future import WoolFuture
from wool._logging import __log_format__
from wool._pool import WoolPool
from wool._task import (
    WoolTask,
    WoolTaskEvent,
    WoolTaskEventCallback,
    WoolTaskException,
    current_task,
    task,
)
from wool._worker import Worker

# PUBLIC
__log_format__: str = __log_format__

# PUBLIC
__log_level__: int = logging.INFO

# PUBLIC
__wool_client__: Final[ContextVar[WoolClient]] = ContextVar(
    "__wool_client__", default=NullClient()
)

__wool_worker__: Worker | Literal[True] | None = None

__all__ = [
    "WoolTaskException",
    "WoolFuture",
    "WoolTask",
    "WoolTaskEvent",
    "WoolTaskEventCallback",
    "WoolPool",
    "WoolClient",
    "current_task",
    "__log_format__",
    "__log_level__",
    "task",
]

for symbol in __all__:
    attribute = globals().get(symbol)
    try:
        if attribute and "wool" in attribute.__module__.split("."):
            # Set the module to reflect imports of the symbol
            attribute.__module__ = __name__
    except AttributeError:
        pass
