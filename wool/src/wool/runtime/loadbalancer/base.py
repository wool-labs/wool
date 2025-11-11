from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import AsyncIterator
from typing import Callable
from typing import Final
from typing import Protocol
from typing import TypeAlias
from typing import runtime_checkable

from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.resourcepool import Resource
from wool.runtime.worker.connection import WorkerConnection

if TYPE_CHECKING:
    from wool.runtime.work import WorkTask


# public
ConnectionResourceFactory: TypeAlias = Callable[[], Resource[WorkerConnection]]


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

    The dispatch method accepts a :class:`WorkTask` and returns an async
    iterator that yields task results from the worker.
    """

    async def dispatch(
        self,
        task: WorkTask,
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

    _workers: Final[dict[WorkerMetadata, ConnectionResourceFactory]]

    def __init__(self):
        self._workers = {}

    @property
    def workers(self) -> MappingProxyType[WorkerMetadata, ConnectionResourceFactory]:
        """Read-only view of workers in this context.

        :returns:
            Immutable mapping of worker metadata to connection resource
            factories. Changes to the underlying context are reflected in
            the returned proxy.
        """
        return MappingProxyType(self._workers)

    def add_worker(
        self,
        metadata: WorkerMetadata,
        connection_resource_factory: ConnectionResourceFactory,
    ):
        """Add a worker to this context.

        :param metadata:
            Information about the worker to add.
        :param connection_resource_factory:
            Factory function that creates connection resources for this worker.
        """
        self._workers[metadata] = connection_resource_factory

    def update_worker(
        self,
        metadata: WorkerMetadata,
        connection_resource_factory: ConnectionResourceFactory,
        *,
        upsert: bool = False,
    ):
        """Update an existing worker's connection resource factory.

        :param metadata:
            Information about the worker to update. If the worker is not
            present in the context, this method does nothing.
        :param connection_resource_factory:
            New factory function that creates connection resources for this
            worker.
        :param upsert:
            Flag indicating whether or not to add the worker if it's not
            already in the context.
        """
        if upsert or metadata in self._workers:
            self._workers[metadata] = connection_resource_factory

    def remove_worker(self, metadata: WorkerMetadata):
        """Remove a worker from this context.

        :param metadata:
            Information about the worker to remove. If the worker is not
            present in the context, this method does nothing.
        """
        if metadata in self._workers:
            del self._workers[metadata]
