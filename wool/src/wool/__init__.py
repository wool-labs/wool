from contextvars import ContextVar
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from typing import Final

from tblib import pickling_support

from wool._connection import Connection
from wool._connection import RpcError
from wool._connection import TransientRpcError
from wool._connection import UnexpectedResponse
from wool._context import AppContext
from wool._resource_pool import ResourcePool
from wool._work import WoolTask
from wool._work import WoolTaskEvent
from wool._work import WoolTaskEventCallback
from wool._work import WoolTaskEventType
from wool._work import WoolTaskException
from wool._work import current_task as wool_current_task
from wool._work import work
from wool._worker_pool import WorkerPool
from wool._worker_proxy import WorkerProxy
from wool._worker_service import WorkerService
from wool.core.discovery.base import Discovery
from wool.core.discovery.base import DiscoveryEvent
from wool.core.discovery.base import DiscoveryEventType
from wool.core.discovery.base import DiscoveryLike
from wool.core.discovery.base import DiscoveryPublisherLike
from wool.core.discovery.base import DiscoverySubscriberLike
from wool.core.discovery.base import PredicateFunction
from wool.core.discovery.base import WorkerInfo
from wool.core.discovery.lan import LanDiscovery
from wool.core.discovery.local import LocalDiscovery
from wool.core.loadbalancer.base import ConnectionResourceFactory
from wool.core.loadbalancer.base import LoadBalancerContext
from wool.core.loadbalancer.base import LoadBalancerLike
from wool.core.loadbalancer.base import NoWorkersAvailable
from wool.core.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.core.typing import Factory
from wool.core.worker.base import Worker
from wool.core.worker.base import WorkerFactory
from wool.core.worker.base import WorkerLike
from wool.core.worker.local import LocalWorker

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
    "Connection",
    "RpcError",
    "TransientRpcError",
    "UnexpectedResponse",
    # Context
    "AppContext",
    # Load balancing
    "ConnectionResourceFactory",
    "LoadBalancerContext",
    "LoadBalancerLike",
    "NoWorkersAvailable",
    "RoundRobinLoadBalancer",
    # Work
    "WoolTask",
    "WoolTaskEvent",
    "WoolTaskEventCallback",
    "WoolTaskEventType",
    "WoolTaskException",
    "routine",
    "work",
    "wool_current_task",
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
    "WorkerInfo",
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
