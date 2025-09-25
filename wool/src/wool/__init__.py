from contextvars import ContextVar
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from typing import Final
from typing import Generic
from typing import TypeVar
from typing import cast

from tblib import pickling_support

from wool._resource_pool import ResourcePool
from wool._work import WoolTask
from wool._work import WoolTaskEvent
from wool._work import WoolTaskEventCallback
from wool._work import WoolTaskEventType
from wool._work import WoolTaskException
from wool._work import current_task as wool_current_task
from wool._work import routine
from wool._work import work
from wool._worker import Worker
from wool._worker import WorkerService
from wool._worker_discovery import DiscoveryService
from wool._worker_discovery import LanDiscoveryService
from wool._worker_discovery import LanRegistryService
from wool._worker_discovery import RegistryService
from wool._worker_pool import WorkerPool
from wool._worker_proxy import WorkerProxy

pickling_support.install()


SENTINEL = object()

T = TypeVar("T")


class GlobalVar(Generic[T]):
    def __init__(self, default: T | None = None) -> None:
        self._default = default
        self._value = SENTINEL

    def get(self) -> T | None:
        if self._value is SENTINEL:
            return self._default
        else:
            return cast(T, self._value)

    def set(self, value: T):
        self._value = value


try:
    __version__ = version("wool")
except PackageNotFoundError:
    __version__ = "unknown"

__proxy__: Final[ContextVar[WorkerProxy | None]] = ContextVar("__proxy__", default=None)

__proxy_pool__: Final[ContextVar[ResourcePool[WorkerProxy] | None]] = ContextVar(
    "__proxy_pool__", default=None
)

__all__ = [
    "LanDiscoveryService",
    "LanRegistryService",
    "Worker",
    "DiscoveryService",
    "WorkerPool",
    "WorkerProxy",
    "RegistryService",
    "WorkerService",
    "WoolTask",
    "WoolTaskEvent",
    "WoolTaskEventCallback",
    "WoolTaskEventType",
    "WoolTaskException",
    "routine",
    "work",
    "wool_current_task",
]

for symbol in __all__:
    attribute = globals().get(symbol)
    try:
        if attribute and "wool" in attribute.__module__.split("."):
            # Set the module to reflect imports of the symbol
            attribute.__module__ = __name__
    except AttributeError:
        continue

# for plugin in entry_points(group="wool_cli_plugins"):
#     try:
#         plugin.load()
#         logging.info(f"Loaded CLI plugin {plugin.name}")
#     except Exception as e:
#         logging.error(f"Failed to load CLI plugin {plugin.name}: {e}")
#         raise
