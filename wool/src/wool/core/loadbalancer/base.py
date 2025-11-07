from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import AsyncIterator
from typing import Callable
from typing import Final
from typing import Protocol
from typing import TypeAlias
from typing import runtime_checkable

from wool._connection import Connection
from wool._resource_pool import Resource
from wool.core.discovery.base import WorkerInfo

if TYPE_CHECKING:
    from wool._work import WoolTask


# public
ConnectionResourceFactory: TypeAlias = Callable[[], Resource[Connection]]


# public
class NoWorkersAvailable(Exception):
    """Raised when no workers are available for task dispatch.

    This exception indicates that either no workers exist in the worker pool
    or all available workers have been tried and failed.
    """


# public
@runtime_checkable
class LoadBalancerLike(Protocol):
    """Protocol for load balancers that dispatch tasks to workers.

    Load balancers implementing this protocol operate on a
    :class:`LoadBalancerContext` to access workers and their connection
    factories. The context provides isolation, allowing a single load balancer
    instance to service multiple worker pools with independent state.

    The dispatch method accepts a :class:`WoolTask` and returns an async
    iterator that yields task results from the worker.
    """

    async def dispatch(
        self,
        task: WoolTask,
        *,
        context: LoadBalancerContext,
        timeout: float | None = None,
    ) -> AsyncIterator: ...


# public
class LoadBalancerContext:
    """Isolated load balancing context for a single worker pool.

    Manages workers and their connection resource factories for a specific
    worker pool, enabling load balancer instances to service multiple pools
    with independent state and worker lists.
    """

    _workers: Final[dict[WorkerInfo, ConnectionResourceFactory]]

    def __init__(self):
        self._workers = {}

    @property
    def workers(self) -> MappingProxyType[WorkerInfo, ConnectionResourceFactory]:
        """Read-only view of workers in this context.

        :returns:
            Immutable mapping of worker information to connection resource
            factories. Changes to the underlying context are reflected in
            the returned proxy.
        """
        return MappingProxyType(self._workers)

    def add_worker(
        self,
        worker_info: WorkerInfo,
        connection_resource_factory: ConnectionResourceFactory,
    ):
        """Add a worker to this context.

        :param worker_info:
            Information about the worker to add.
        :param connection_resource_factory:
            Factory function that creates connection resources for this worker.
        """
        self._workers[worker_info] = connection_resource_factory

    def update_worker(
        self,
        worker_info: WorkerInfo,
        connection_resource_factory: ConnectionResourceFactory,
        *,
        upsert: bool = False,
    ):
        """Update an existing worker's connection resource factory.

        :param worker_info:
            Information about the worker to update. If the worker is not
            present in the context, this method does nothing.
        :param connection_resource_factory:
            New factory function that creates connection resources for this
            worker.
        :param upsert:
            Flag indicating whether or not to add the worker if it's not
            already in the context.
        """
        if upsert or worker_info in self._workers:
            self._workers[worker_info] = connection_resource_factory

    def remove_worker(self, worker_info: WorkerInfo):
        """Remove a worker from this context.

        :param worker_info:
            Information about the worker to remove. If the worker is not
            present in the context, this method does nothing.
        """
        if worker_info in self._workers:
            del self._workers[worker_info]
