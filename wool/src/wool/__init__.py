import contextvars
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from typing import Final

from tblib import pickling_support

from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import ContextVarCollision
from wool.runtime.context import Token
from wool.runtime.context import current_context
from wool.runtime.discovery.base import Discovery
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoveryEventType
from wool.runtime.discovery.base import DiscoveryLike
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import PredicateFunction
from wool.runtime.discovery.lan import LanDiscovery
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import LoadBalancerContextLike
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.routine.task import Serializer
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import TaskException
from wool.runtime.routine.task import current_task
from wool.runtime.routine.wrapper import routine
from wool.runtime.typing import Factory
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.base import Worker
from wool.runtime.worker.base import WorkerFactory
from wool.runtime.worker.base import WorkerLike
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import UnexpectedResponse
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.metadata import WorkerMetadata
from wool.runtime.worker.pool import WorkerPool
from wool.runtime.worker.proxy import WorkerProxy
from wool.runtime.worker.service import BackpressureContext
from wool.runtime.worker.service import BackpressureLike
from wool.runtime.worker.service import WorkerService

pickling_support.install()

try:
    __version__ = version("wool")
except PackageNotFoundError:
    __version__ = "unknown"

__proxy__: Final[contextvars.ContextVar[WorkerProxy | None]] = contextvars.ContextVar(
    "__proxy__", default=None
)

__proxy_pool__: Final[contextvars.ContextVar[ResourcePool[WorkerProxy] | None]] = (
    contextvars.ContextVar("__proxy_pool__", default=None)
)

__worker_metadata__: WorkerMetadata | None = None

__worker_uds_address__: str | None = None

__worker_service__: Final[contextvars.ContextVar[WorkerService | None]] = (
    contextvars.ContextVar("__worker_service__", default=None)
)

__all__ = [
    # Connection
    "RpcError",
    "TransientRpcError",
    "UnexpectedResponse",
    "WorkerConnection",
    # Context
    "Context",
    "ContextVar",
    "ContextVarCollision",
    "Token",
    "current_context",
    # Load balancing
    "LoadBalancerContextLike",
    "LoadBalancerLike",
    "NoWorkersAvailable",
    "RoundRobinLoadBalancer",
    # Routines
    "Serializer",
    "Task",
    "TaskException",
    "current_task",
    "routine",
    # Backpressure
    "BackpressureContext",
    "BackpressureLike",
    # Workers
    "LocalWorker",
    "Worker",
    "WorkerCredentials",
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
