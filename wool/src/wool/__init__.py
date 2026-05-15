import contextvars
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from typing import Final

from tblib import pickling_support

from wool.runtime.context import Context
from wool.runtime.context import ContextAlreadyBound
from wool.runtime.context import ContextDecodeWarning
from wool.runtime.context import ContextVar
from wool.runtime.context import ContextVarCollision
from wool.runtime.context import RuntimeContext
from wool.runtime.context import Token
from wool.runtime.context import copy_context
from wool.runtime.context import create_task
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
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import TaskException
from wool.runtime.routine.task import current_task
from wool.runtime.routine.wrapper import routine
from wool.runtime.serializer import CloudpickleSerializer
from wool.runtime.serializer import Serializer
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

__serializer__: Final[Serializer] = CloudpickleSerializer()

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
    "BackpressureContext",
    "BackpressureLike",
    "Context",
    "ContextAlreadyBound",
    "ContextDecodeWarning",
    "ContextVar",
    "ContextVarCollision",
    "Discovery",
    "DiscoveryEvent",
    "DiscoveryEventType",
    "DiscoveryLike",
    "DiscoveryPublisherLike",
    "DiscoverySubscriberLike",
    "Factory",
    "LanDiscovery",
    "LoadBalancerContextLike",
    "LoadBalancerLike",
    "LocalDiscovery",
    "LocalWorker",
    "NoWorkersAvailable",
    "PredicateFunction",
    "RoundRobinLoadBalancer",
    "RpcError",
    "RuntimeContext",
    "Serializer",
    "Task",
    "TaskException",
    "Token",
    "TransientRpcError",
    "UnexpectedResponse",
    "Worker",
    "WorkerConnection",
    "WorkerCredentials",
    "WorkerFactory",
    "WorkerLike",
    "WorkerMetadata",
    "WorkerPool",
    "WorkerProxy",
    "WorkerService",
    "copy_context",
    "create_task",
    "current_context",
    "current_task",
    "routine",
]

for symbol in __all__:
    attribute = globals().get(symbol)
    try:
        if attribute and "wool" in attribute.__module__.split("."):
            # Set the module to reflect imports of the symbol
            attribute.__module__ = __name__
    except AttributeError:
        continue
