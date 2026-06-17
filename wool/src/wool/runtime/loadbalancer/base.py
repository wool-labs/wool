from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import Final
from typing import Mapping
from typing import Protocol
from typing import runtime_checkable
from uuid import UUID

from wool.runtime.worker.connection import HandshakeError
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata

if TYPE_CHECKING:
    from wool.runtime.routine.task import Task


# public
class NoWorkersAvailable(Exception):
    """Raised when no workers are available for task dispatch.

    This exception indicates that either no workers exist in the worker pool
    or all available workers have been tried and failed.
    """


# public
class AllWorkersUnauthenticated(NoWorkersAvailable):
    """Raised when every discovered worker failed the secure handshake.

    Distinguishes "workers are present, but every one of them refused, or
    was refused by, my credentials" from the bare "no workers are present"
    condition.  It is a subclass of :class:`NoWorkersAvailable` so existing
    callers that catch ``NoWorkersAvailable`` keep working unchanged, while
    callers that want the distinction can catch this type and inspect the
    per-worker :attr:`rejections`.

    :param message:
        Human-readable description.
    :param rejections:
        Mapping of worker uid to the :class:`HandshakeError` that evicted
        it during the failed dispatch.
    """

    def __init__(
        self,
        message: str,
        *,
        rejections: Mapping[UUID, HandshakeError],
    ):
        super().__init__(message)
        self.rejections = rejections


# public
@dataclass(frozen=True)
class HandshakeRejection:
    """A per-worker record of secure-handshake rejections.

    Accumulated on the load-balancer context and surfaced through the pool
    so an operator can see how many discovered workers refused the
    client's credentials, and why.

    :param uid:
        The rejecting worker's unique identifier.
    :param address:
        The address at which the worker was reached.
    :param count:
        How many times this worker has failed the handshake.
    :param error:
        The most recent :class:`HandshakeError`.
    """

    uid: UUID
    address: str
    count: int
    error: HandshakeError

    @property
    def reason(self) -> HandshakeError.Reason:
        """The most recent handshake-failure reason."""
        return self.error.reason


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
    ) -> AsyncGenerator:
        """Dispatch *task* through *context*'s workers and yield
        per-step results.

        Implementations rotate through ``context.workers`` per their
        policy, invoking each worker's
        :py:meth:`WorkerConnection.dispatch` and yielding the
        streamed responses until the routine completes or the pool
        is exhausted.

        :param task:
            The :py:class:`Task` to dispatch.
        :param context:
            The :py:class:`LoadBalancerContextLike` whose
            ``workers`` map names eligible candidates and through
            which ``remove_worker`` evicts unhealthy peers.
        :param timeout:
            Per-attempt timeout in seconds against a single worker.
            ``None`` (default) means no per-attempt bound.
        :returns:
            An async iterator yielding per-step worker responses.
            The iterator is awaitable (``await balancer.dispatch(...)``)
            then iterable (``async for response in iterator``); the
            iterator's lifecycle spans the full routine, not a single
            worker attempt.
        :raises NoWorkersAvailable:
            When the pool is exhausted — either empty on entry or
            fully evicted across one rotation cycle without a
            successful dispatch.

        **Worker-health contract.** Implementations MUST honor the
        classification documented on
        :py:class:`wool.runtime.worker.connection.RpcError`:

        - :py:class:`TransientRpcError` MUST cause the
          implementation to try a different worker for this call
          while leaving the failing worker in the pool — "skip
          without eviction".
        - Non-transient :py:class:`RpcError` MUST trigger
          ``context.remove_worker(metadata)`` so the failing worker
          is no longer offered to future dispatches — "evict".
        - Any other exception class (including
          :py:class:`BaseException` subclasses such as
          :py:class:`asyncio.CancelledError`) MUST propagate to the
          caller without mutating the pool. Catching
          :py:class:`Exception` *or* :py:class:`BaseException`
          indiscriminately will silently evict workers on every
          caller-side bug or mid-dispatch cancellation.
        """
        ...


class LoadBalancerContext:
    """Isolated load balancing context for a single worker pool.

    Manages workers and their connections for a specific worker pool,
    enabling load balancer instances to service multiple pools with
    independent state and worker lists.

    Beyond the :class:`LoadBalancerContextLike` protocol, the built-in
    context also keeps a :attr:`rejections` ledger via
    :meth:`record_rejection`, which the round-robin balancer populates and
    the pool surfaces for observability.  Custom contexts need not provide
    these; the balancer treats them as optional.
    """

    _workers: Final[dict[WorkerMetadata, WorkerConnection]]
    _rejections: Final[dict[UUID, HandshakeRejection]]

    def __init__(self):
        self._workers = {}
        self._rejections = {}

    @property
    def workers(self) -> MappingProxyType[WorkerMetadata, WorkerConnection]:
        """Read-only view of workers in this context.

        :returns:
            Immutable mapping of worker metadata to connections.
            Changes to the underlying context are reflected in
            the returned proxy.
        """
        return MappingProxyType(self._workers)

    @property
    def rejections(self) -> MappingProxyType[UUID, HandshakeRejection]:
        """Read-only view of the secure-handshake rejection ledger.

        :returns:
            Immutable mapping of worker uid to its
            :class:`HandshakeRejection` record.  Changes to the
            underlying ledger are reflected in the returned proxy.
        """
        return MappingProxyType(self._rejections)

    def record_rejection(self, metadata: WorkerMetadata, error: HandshakeError) -> None:
        """Record that a worker failed the secure handshake.

        Increments the worker's rejection count and stores the most
        recent error, keyed by worker uid so the record survives the
        worker's eviction and re-admission.

        :param metadata:
            Information about the rejecting worker.
        :param error:
            The :class:`HandshakeError` that evicted the worker.
        """
        previous = self._rejections.get(metadata.uid)
        count = previous.count + 1 if previous is not None else 1
        self._rejections[metadata.uid] = HandshakeRejection(
            uid=metadata.uid,
            address=metadata.address,
            count=count,
            error=error,
        )

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
