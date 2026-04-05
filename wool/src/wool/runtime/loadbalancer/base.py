from __future__ import annotations

import sys
import warnings
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import Final
from typing import Protocol
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

    def __init__(
        self, message: str = "No healthy workers available for dispatch"
    ) -> None:
        super().__init__(message)


# public
@runtime_checkable
class LoadBalancerContextView(Protocol):
    """Read-only view of a load balancer context.

    This is the surface delegating load balancers see, i.e., only a
    read-only ``workers`` mapping. Eviction and pool-membership
    mutation are the `WorkerProxy`'s responsibility; load balancers
    observe the pool but cannot mutate it.

    By convention across this package, a ``*View`` suffix marks a
    read-only protocol view of an otherwise mutable object.
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


# LoadBalancerContextLike exists only to serve the deprecated
# DispatchingLoadBalancerLike protocol; retire both together when the
# dispatch-based protocol is removed.
# public
@runtime_checkable
class LoadBalancerContextLike(LoadBalancerContextView, Protocol):
    """Protocol defining the interface for load balancer contexts.

    Load balancer contexts manage workers and their connections
    for a specific worker pool, enabling load balancer instances to
    service multiple pools with independent state and worker lists.

    This is the mutable superset of `LoadBalancerContextView` used by
    the legacy `DispatchingLoadBalancerLike` protocol and by the
    `WorkerProxy` itself for its internal pool bookkeeping.
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


# Retire DispatchingLoadBalancerLike together with LoadBalancerContextLike
# when the dispatch-based protocol is removed.
# public
@runtime_checkable
class DispatchingLoadBalancerLike(Protocol):
    """Deprecated legacy protocol for load balancers that own dispatch.

    Implementations couple worker selection with task dispatch: they
    accept a `Task`, call `WorkerConnection.dispatch` internally,
    classify errors, retry on transient failures, and evict workers on
    non-transient errors.

    This coupling makes custom implementations brittle and forces every
    balancer to reimplement the same retry/eviction boilerplate. The
    replacement is `LoadBalancerLike`, which limits the balancer's
    responsibility to worker selection and routing policy.
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
        `WorkerConnection.dispatch` and yielding the
        streamed responses until the routine completes or the pool
        is exhausted.

        :param task:
            The `Task` to dispatch.
        :param context:
            The `LoadBalancerContextLike` whose
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
        `wool.runtime.worker.connection.RpcError`:

        - `TransientRpcError` MUST cause the
          implementation to try a different worker for this call
          while leaving the failing worker in the pool — "skip
          without eviction".
        - Non-transient `RpcError` MUST trigger
          ``context.remove_worker(metadata)`` so the failing worker
          is no longer offered to future dispatches — "evict".
        - Any other exception class (including
          `BaseException` subclasses such as
          `asyncio.CancelledError`) MUST propagate to the
          caller without mutating the pool. Catching
          `Exception` *or* `BaseException`
          indiscriminately will silently evict workers on every
          caller-side bug or mid-dispatch cancellation.
        """
        ...


# Single source for the dispatch-protocol deprecation message, shared by
# the @warnings.deprecated decorator below and WorkerProxy's runtime
# DeprecationWarning so the two never drift.
DISPATCHING_LOADBALANCER_DEPRECATION_MESSAGE = (
    "DispatchingLoadBalancerLike is deprecated; implement "
    "LoadBalancerLike instead. Support for the dispatch-based protocol "
    "will be removed in the next major release."
)


# On Python 3.13+, apply @warnings.deprecated so type checkers and
# IDEs surface the deprecation. On earlier versions, the decorator's
# __deprecated__ attribute is included in _get_protocol_attrs and
# breaks @runtime_checkable isinstance checks — the runtime
# DeprecationWarning from WorkerProxy.start() covers those versions.
if sys.version_info >= (3, 13):
    DispatchingLoadBalancerLike = warnings.deprecated(
        DISPATCHING_LOADBALANCER_DEPRECATION_MESSAGE
    )(DispatchingLoadBalancerLike)


# public
@runtime_checkable
class LoadBalancerLike(Protocol):
    """Protocol for load balancers that select workers for dispatch.

    A load balancer is responsible solely for **routing policy**:
    given a task and a read-only view of the worker pool, yield worker
    candidates in the order the balancer prefers. Custom balancers
    implement this protocol; `RoundRobinLoadBalancer` is the built-in
    implementation.

    The `WorkerProxy` delegates candidate selection to the balancer
    through this generator: the balancer selects and orders candidates,
    and the proxy dispatches to them. The proxy owns the dispatch loop —
    it calls `WorkerConnection.dispatch` directly, evicts workers on
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
      dispatch succeeded. The value sent back is an opaque success
      echo — the proxy sends back the metadata it just yielded, and
      only its not-None-ness is meaningful. **The generator MUST
      terminate after receiving a success signal** — either by
      ``return`` or by letting control fall off the end. Yielding
      another candidate after ``asend`` is a protocol violation; the
      proxy raises `RuntimeError` at the call site.

    When the generator ends (via ``return`` or `StopAsyncIteration`
    in response to an ``athrow`` or the initial ``anext``), the proxy
    raises `NoWorkersAvailable`.
    """

    def delegate(
        self,
        task: Task,
        *,
        context: LoadBalancerContextView,
    ) -> AsyncGenerator[
        tuple[WorkerMetadata, WorkerConnection],
        WorkerMetadata | None,
    ]:
        """Yield worker candidates for the proxy to dispatch to.

        The `WorkerProxy` delegates candidate selection to this
        generator: the balancer selects and orders candidates, and the
        proxy drives the generator and dispatches to each yielded
        worker.

        :param task:
            The `Task` being routed. Read-only — the balancer MAY key
            its routing decisions on it (for example, hashing
            ``task.tag`` to a worker) or ignore it entirely.
        :param context:
            Read-only view of the worker pool. Only its ``workers``
            mapping is exposed; eviction and pool mutation are the
            proxy's responsibility.
        :yields:
            ``(metadata, connection)`` candidates in the balancer's
            preferred order. See the class docstring for the
            ``anext``/``athrow``/``asend`` protocol that drives this
            generator.
        """
        ...


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
