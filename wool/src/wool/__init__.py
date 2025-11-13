from contextvars import ContextVar
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from typing import Final

from tblib import pickling_support

from wool.runtime.context import RuntimeContext
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.work import WorkTask
from wool.runtime.work import WorkTaskEvent
from wool.runtime.work import WorkTaskEventHandler
from wool.runtime.work import WorkTaskEventType
from wool.runtime.work import WorkTaskException
from wool.runtime.work import current_task
from wool.runtime.work import work

# Backward compatibility aliases (deprecated)
WoolTask = WorkTask
WoolTaskEvent = WorkTaskEvent
WoolTaskEventHandler = WorkTaskEventHandler
WoolTaskEventType = WorkTaskEventType
WoolTaskException = WorkTaskException
from wool.runtime.discovery.base import Discovery
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoveryEventType
from wool.runtime.discovery.base import DiscoveryLike
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import PredicateFunction
from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.discovery.lan import LanDiscovery
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import ConnectionResourceFactory
from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.typing import Factory
from wool.runtime.worker.base import Worker
from wool.runtime.worker.base import WorkerFactory
from wool.runtime.worker.base import WorkerLike
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import UnexpectedResponse
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.pool import WorkerPool
from wool.runtime.worker.proxy import WorkerProxy
from wool.runtime.worker.service import WorkerService

pickling_support.install()

# Alias for backwards compatibility
routine = work

try:
    __version__ = version("wool")
except PackageNotFoundError:
    __version__ = "unknown"

__proxy__: Final[ContextVar[WorkerProxy | None]] = ContextVar("__proxy__", default=None)

__proxy_pool__: Final[ContextVar[ResourcePool[WorkerProxy] | None]] = ContextVar(
    "__proxy_pool__", default=None
)

__all__ = [
    # Connection
    "RpcError",
    "TransientRpcError",
    "UnexpectedResponse",
    "WorkerConnection",
    # Context
    "RuntimeContext",
    # Load balancing
    "ConnectionResourceFactory",
    "LoadBalancerContext",
    "LoadBalancerLike",
    "NoWorkersAvailable",
    "RoundRobinLoadBalancer",
    # Work - New names (preferred)
    "WorkTask",
    "WorkTaskEvent",
    "WorkTaskEventHandler",
    "WorkTaskEventType",
    "WorkTaskException",
    "current_task",
    "routine",
    "work",
    # Work - Backward compatibility (deprecated)
    "WoolTask",
    "WoolTaskEvent",
    "WoolTaskEventHandler",
    "WoolTaskEventType",
    "WoolTaskException",
    # Workers
    "LocalWorker",
    "Worker",
    "WorkerFactory",
    "WorkerLike",
    "WorkerPool",
    "WorkerProxy",
    "WorkerService",
    # Discovery
    "Discovery",
    "DiscoveryEvent",
    "DiscoveryEventType",
    "DiscoveryLike",
    "DiscoveryPublisherLike",
    "DiscoverySubscriberLike",
    "LanDiscovery",
    "LocalDiscovery",
    "PredicateFunction",
    "WorkerMetadata",
    # Typing
    "Factory",
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
