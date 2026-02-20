from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import Final
from typing import Protocol
from typing import runtime_checkable

from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.worker.connection import WorkerConnection

if TYPE_CHECKING:
    from wool.runtime.routine.task import Task


# public
class NoWorkersAvailable(Exception):
    """Raised when no workers are available for task dispatch.

    This exception indicates that either no workers exist in the worker pool
    or all available workers have been tried and failed.
    """


# public
@runtime_checkable
class LoadBalancerContextLike(Protocol):
    """Protocol defining the interface for load balancer contexts.

    Load balancer contexts manage workers and their connections
    for a specific worker pool, enabling load balancer instances to
    service multiple pools with independent state and worker lists.
    """

    @property
    def workers(
        self,
    ) -> MappingProxyType[WorkerMetadata, WorkerConnection]:
        """Read-only view of workers in this context.

        :returns:
            Immutable mapping of worker metadata to connections.
        """
        ...

    def add_worker(
        self,
        metadata: WorkerMetadata,
        connection: WorkerConnection,
    ) -> None:
        """Add a worker to this context.

        :param metadata:
            Information about the worker to add.
        :param connection:
            The :py:class:`WorkerConnection` for this worker.
        """
        ...

    def update_worker(
        self,
        metadata: WorkerMetadata,
        connection: WorkerConnection,
        *,
        upsert: bool = False,
    ) -> None:
        """Update an existing worker's connection.

        :param metadata:
            Information about the worker to update.
        :param connection:
            New :py:class:`WorkerConnection` for this worker.
        :param upsert:
            Flag indicating whether or not to add the worker if
            it's not already in the context.
        """
        ...

    def remove_worker(self, metadata: WorkerMetadata) -> None:
        """Remove a worker from this context.

        :param metadata:
            Information about the worker to remove.
        """
        ...


# public
@runtime_checkable
class LoadBalancerLike(Protocol):
    """Protocol for load balancers that dispatch tasks to workers.

    Load balancers implementing this protocol operate on a
    :py:class:`LoadBalancerContextLike` to access workers and their
    connections. The context provides isolation, allowing a single load
    balancer instance to service multiple worker pools with independent
    state.

    The dispatch method accepts a :py:class:`Task` and returns an async
    iterator that yields task results from the worker.
    """

    async def dispatch(
        self,
        task: Task,
        *,
        context: LoadBalancerContextLike,
        timeout: float | None = None,
    ) -> AsyncGenerator: ...


class LoadBalancerContext:
    """Isolated load balancing context for a single worker pool.

    Manages workers and their connections for a specific worker pool,
    enabling load balancer instances to service multiple pools with
    independent state and worker lists.
    """

    _workers: Final[dict[WorkerMetadata, WorkerConnection]]

    def __init__(self):
        self._workers = {}

    @property
    def workers(self) -> MappingProxyType[WorkerMetadata, WorkerConnection]:
        """Read-only view of workers in this context.

        :returns:
            Immutable mapping of worker metadata to connections.
            Changes to the underlying context are reflected in
            the returned proxy.
        """
        return MappingProxyType(self._workers)

    def add_worker(
        self,
        metadata: WorkerMetadata,
        connection: WorkerConnection,
    ):
        """Add a worker to this context.

        :param metadata:
            Information about the worker to add.
        :param connection:
            The :py:class:`WorkerConnection` for this worker.
        """
        self._workers[metadata] = connection

    def update_worker(
        self,
        metadata: WorkerMetadata,
        connection: WorkerConnection,
        *,
        upsert: bool = False,
    ):
        """Update an existing worker's connection.

        :param metadata:
            Information about the worker to update. If the worker is not
            present in the context, this method does nothing.
        :param connection:
            New :py:class:`WorkerConnection` for this worker.
        :param upsert:
            Flag indicating whether or not to add the worker if it's not
            already in the context.
        """
        if upsert or metadata in self._workers:
            self._workers[metadata] = connection

    def remove_worker(self, metadata: WorkerMetadata):
        """Remove a worker from this context.

        :param metadata:
            Information about the worker to remove. If the worker is not
            present in the context, this method does nothing.
        """
        if metadata in self._workers:
            del self._workers[metadata]
