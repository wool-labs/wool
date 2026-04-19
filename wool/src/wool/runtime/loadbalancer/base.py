from __future__ import annotations

import sys
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import Final
from typing import Protocol
from typing import TypeAlias
from typing import runtime_checkable

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
@runtime_checkable
class LoadBalancerContextView(Protocol):
    """Read-only view of a load balancer context.

    This is the surface delegating load balancers see — only a
    read-only ``workers`` mapping. Eviction and pool-membership
    mutation are the :py:class:`WorkerProxy`'s responsibility;
    load balancers observe the pool but cannot mutate it.
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


# public
@runtime_checkable
class LoadBalancerContextLike(LoadBalancerContextView, Protocol):
    """Protocol defining the interface for load balancer contexts.

    Load balancer contexts manage workers and their connections
    for a specific worker pool, enabling load balancer instances to
    service multiple pools with independent state and worker lists.

    This is the mutable superset of :py:class:`LoadBalancerContextView`
    used by the legacy :py:class:`DispatchingLoadBalancerLike` protocol
    and by the :py:class:`WorkerProxy` itself for its internal pool
    bookkeeping.
    """

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
class DispatchingLoadBalancerLike(Protocol):
    """Deprecated legacy protocol for load balancers that own dispatch.

    Implementations couple worker selection with task dispatch: they
    accept a :py:class:`Task`, call :py:meth:`WorkerConnection.dispatch`
    internally, classify errors, retry on transient failures, and evict
    workers on non-transient errors.

    This coupling makes custom implementations brittle and forces every
    balancer to reimplement the same retry/eviction boilerplate. The
    replacement is :py:class:`DelegatingLoadBalancerLike`, which limits
    the balancer's responsibility to worker selection and routing
    policy.
    """

    async def dispatch(
        self,
        task: Task,
        *,
        context: LoadBalancerContextLike,
        timeout: float | None = None,
    ) -> AsyncGenerator: ...


# On Python 3.13+, apply @warnings.deprecated so type checkers and
# IDEs surface the deprecation. On earlier versions, the decorator's
# __deprecated__ attribute is included in _get_protocol_attrs and
# breaks @runtime_checkable isinstance checks — the runtime
# DeprecationWarning from WorkerProxy.start() covers those versions.
if sys.version_info >= (3, 13):
    import warnings

    DispatchingLoadBalancerLike = warnings.deprecated(
        "DispatchingLoadBalancerLike is deprecated; implement "
        "DelegatingLoadBalancerLike instead. Support for the "
        "dispatch-based protocol will be removed in the next "
        "major release. See issue #155."
    )(DispatchingLoadBalancerLike)


# public
@runtime_checkable
class DelegatingLoadBalancerLike(Protocol):
    """Protocol for load balancers that only select workers.

    Delegating load balancers are responsible solely for **routing
    policy**: given a read-only view of the worker pool, yield worker
    candidates in the order the balancer prefers. The
    :py:class:`WorkerProxy` owns the dispatch loop — it calls
    :py:meth:`WorkerConnection.dispatch` directly, evicts workers on
    non-transient errors, and reports each outcome back to the
    balancer's generator.

    The generator supports three signals:

    - ``anext()`` — request the next worker candidate.
    - ``athrow(exc)`` — report that the previous candidate's dispatch
      failed. On transient errors the proxy leaves the worker in the
      pool; on non-transient errors the proxy evicts the worker from
      the context *before* throwing into the generator, so the
      balancer observes the capacity change before choosing the next
      candidate.
    - ``asend(metadata)`` — report that the previous candidate's
      dispatch succeeded. **The generator MUST terminate after
      receiving a success signal** — either by ``return`` or by
      letting control fall off the end. Yielding another candidate
      after ``asend`` is a protocol violation; the proxy raises
      :py:class:`RuntimeError` at the call site.

    When the generator ends (via ``return`` or :py:exc:`StopIteration`
    in response to an ``athrow`` or the initial ``anext``), the proxy
    raises :py:exc:`NoWorkersAvailable`.
    """

    def delegate(
        self,
        *,
        context: LoadBalancerContextView,
    ) -> AsyncGenerator[
        tuple[WorkerMetadata, WorkerConnection],
        WorkerMetadata | None,
    ]: ...


# public
LoadBalancerLike: TypeAlias = DispatchingLoadBalancerLike | DelegatingLoadBalancerLike
"""Accepted load balancer protocol — transitional union.

During the deprecation window, :py:class:`WorkerProxy` and
:py:class:`WorkerPool` accept either a :py:class:`DelegatingLoadBalancerLike`
(preferred) or the deprecated :py:class:`DispatchingLoadBalancerLike`.
In the next major release this alias will narrow to just
:py:class:`DelegatingLoadBalancerLike`.
"""


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
