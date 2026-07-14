from __future__ import annotations

import sys
import uuid
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

    A worker is identified by its `WorkerMetadata.uid`, which is
    stable for the life of the worker process while the rest of its
    record (e.g., address, tags, transport options) is not: discovery
    republishes a changed record under the same uid, and the pool
    refreshes the entry in place. A uid observed at any time therefore
    still names the same worker, and always resolves to that worker's
    latest record.

    By convention across this package, a ``*View`` suffix marks a
    read-only protocol view of an otherwise mutable object.
    """

    @property
    def workers(
        self,
    ) -> MappingProxyType[uuid.UUID, tuple[WorkerMetadata, WorkerConnection]]:
        """Read-only view of workers in this context.

        :returns:
            A live view of pool membership. The proxy's mutations are
            visible through it without re-reading the property, and the
            entry under a uid is always that worker's latest record.
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
    `WorkerProxy` itself for its internal pool bookkeeping. Worker
    identity — and with it the semantics every mutator below follows —
    is defined on `LoadBalancerContextView`.

    Every mutator addresses a worker by ``metadata.uid``, so a record
    whose other fields have changed still names the worker it was read
    from. Writes are last-writer-wins: the stored entry is replaced
    with the given record wholesale, so a caller writing a record it
    captured earlier regresses a fresher one for the same uid.
    A displaced `WorkerConnection` is *not* closed — see
    `wool.runtime.worker.connection.WorkerConnection` for why channel
    lifetime belongs to the pooling layer rather than to the holder of
    a connection handle.
    """

    def add_worker(
        self,
        metadata: WorkerMetadata,
        connection: WorkerConnection,
    ) -> None:
        """Admit a worker to this context, replacing any entry it has.

        :param metadata:
            Information about the worker to admit.
        :param connection:
            The `WorkerConnection` for this worker.
        """
        ...

    def update_worker(
        self,
        metadata: WorkerMetadata,
        connection: WorkerConnection,
    ) -> None:
        """Refresh an admitted worker's metadata record and connection.

        Does nothing when the worker is not in the context; use
        `add_worker` to admit one.

        :param metadata:
            Information about the worker to refresh.
        :param connection:
            New `WorkerConnection` for this worker.
        """
        ...

    def remove_worker(self, metadata: WorkerMetadata) -> None:
        """Evict a worker from this context.

        Does nothing when the worker is not in the context.

        :param metadata:
            Information about the worker to evict.
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

    A balancer cannot route a task to a superseded address, however
    long it holds a candidate or however stale the view it chose from.
    Balancers that need a worker's record or connection to decide
    (e.g., to match tags, pin a version, or weigh a connection) read
    them from ``context.workers``, but do not carry what they read back
    across this boundary.

    Selecting a worker that has left the pool is not an error. The
    proxy finds no entry for the uid, asks for the next candidate, and
    reports nothing back — a departed worker has not failed a dispatch.

    The generator supports three signals:

    - ``anext()`` — request the next worker candidate.
    - ``athrow(exc)`` — report that the previous candidate's dispatch
      failed. On transient errors the proxy leaves the worker in the
      pool; on non-transient errors the proxy evicts the worker from
      the context *before* throwing into the generator, so the
      balancer observes the capacity change before choosing the next
      candidate.
    - ``asend(uid)`` — report that the previous candidate's dispatch
      succeeded. The value sent back is an opaque success echo — the
      proxy sends back the uid it just yielded, and only its
      not-None-ness is meaningful. **The generator MUST terminate
      after receiving a success signal** — either by ``return`` or by
      letting control fall off the end. Yielding another candidate
      after ``asend`` is a protocol violation; the proxy raises
      `RuntimeError` at the call site.

    When the generator ends (via ``return`` or `StopAsyncIteration`
    in response to an ``athrow`` or the initial ``anext``), the proxy
    raises `NoWorkersAvailable`.

    .. rubric:: Implementation notes

    The staleness guarantee is structural rather than enforced: a
    candidate names a worker, and the proxy resolves that name against
    the pool it owns at the moment it dispatches, so the record a
    balancer read is never the record dispatched through.
    """

    def delegate(
        self,
        task: Task,
        *,
        context: LoadBalancerContextView,
    ) -> AsyncGenerator[uuid.UUID, uuid.UUID | None]:
        """Yield worker candidates for the proxy to dispatch to.

        :param task:
            The `Task` being routed. Read-only — the balancer MAY key
            its routing decisions on it (for example, hashing
            ``task.tag`` to a worker) or ignore it entirely.
        :param context:
            Read-only view of the worker pool; see
            `LoadBalancerContextView` for the surface it exposes.
            Eviction and pool mutation are the proxy's responsibility.
        :yields:
            Worker uids in the balancer's preferred order. See the
            class docstring for the ``anext``/``athrow``/``asend``
            protocol that drives this generator.
        """
        ...


class LoadBalancerContext:
    """Isolated load balancing context for a single worker pool.

    Manages workers and their connections for a specific worker pool,
    enabling load balancer instances to service multiple pools with
    independent state and worker lists. The pool's identity model and
    mutation semantics are the ones `LoadBalancerContextView` and
    `LoadBalancerContextLike` define.
    """

    _workers: Final[dict[uuid.UUID, tuple[WorkerMetadata, WorkerConnection]]]

    def __init__(self):
        self._workers = {}

    @property
    def workers(
        self,
    ) -> MappingProxyType[uuid.UUID, tuple[WorkerMetadata, WorkerConnection]]:
        """Read-only view of workers in this context."""
        return MappingProxyType(self._workers)

    def add_worker(
        self,
        metadata: WorkerMetadata,
        connection: WorkerConnection,
    ):
        """Admit a worker to this context, replacing any entry it has."""
        self._workers[metadata.uid] = (metadata, connection)

    def update_worker(
        self,
        metadata: WorkerMetadata,
        connection: WorkerConnection,
    ):
        """Refresh an admitted worker's metadata record and connection."""
        if metadata.uid in self._workers:
            self._workers[metadata.uid] = (metadata, connection)

    def remove_worker(self, metadata: WorkerMetadata):
        """Evict a worker from this context."""
        self._workers.pop(metadata.uid, None)
