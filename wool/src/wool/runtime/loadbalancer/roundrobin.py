from __future__ import annotations

from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import Final
from weakref import WeakKeyDictionary

from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata

from .base import LoadBalancerContextView

if TYPE_CHECKING:
    from wool.runtime.routine.task import Task


# public
class RoundRobinLoadBalancer:
    """Round-robin load balancer for distributing tasks across workers.

    Cycles through workers in the given `LoadBalancerContextView`,
    advancing the index after each yielded candidate. The `WorkerProxy`
    owns the dispatch, eviction, and error-classification loop — the
    balancer's only responsibility is ordering.

    After one full cycle without a successful dispatch the generator
    terminates, causing the proxy to raise `NoWorkersAvailable`.

    .. rubric:: Implementation notes

    The cycle boundary is identified by the UID of the first yielded
    candidate. If that first candidate is evicted mid-cycle its UID can
    never recur, so the boundary is reseeded from the next surviving
    candidate.

    Per-context rotation state lives in a `WeakKeyDictionary` keyed on
    the context, so a retired pool's cursor — and the connections it
    references — are reclaimed with the context instead of pinned for
    the balancer's lifetime.
    """

    _index: Final[WeakKeyDictionary[LoadBalancerContextView, int]]

    def __init__(self):
        self._index = WeakKeyDictionary()

    def __reduce__(self):
        return (self.__class__, ())

    async def delegate(
        self,
        task: Task,
        *,
        context: LoadBalancerContextView,
    ) -> AsyncGenerator[
        tuple[WorkerMetadata, WorkerConnection],
        WorkerMetadata | None,
    ]:
        """Yield worker candidates in round-robin order.

        :param task:
            The task being routed. Accepted for protocol conformance
            and ignored: round-robin is task-agnostic. Task-aware
            balancers may key on it (e.g., hash-by-tag affinity);
            round-robin does not.
        :param context:
            Read-only view of the worker pool. Eviction is the
            proxy's responsibility; the balancer only reads
            ``context.workers`` to pick the next candidate.
        :yields:
            ``(metadata, connection)`` pairs. The generator is driven
            by the proxy via ``anext``/``athrow``/``asend``.
        """
        checkpoint: WorkerMetadata | None = None
        snapshot: list[tuple[WorkerMetadata, WorkerConnection]] = []
        cursor = 0
        while True:
            workers = context.workers
            if not workers:
                return
            # Select the candidate and advance the shared per-context index
            # in one synchronous step. With no ``await`` between the read and
            # the write, concurrent dispatches on the same context cannot
            # interleave here, so each observes a distinct starting worker
            # (anti-thundering-herd) — the single-threaded event loop provides
            # the mutual exclusion a lock would, for free.
            index = self._index.get(context, 0)
            if cursor >= len(snapshot):
                # Re-observe membership once per wrap and index the ordered
                # workers in O(1). Rebuilding the snapshot is the only O(n)
                # step and amortizes to O(1) per candidate over a cycle.
                snapshot = list(workers.items())
                cursor = index % len(snapshot)
            metadata, connection = snapshot[cursor]
            cursor += 1
            self._index[context] = index + 1

            if checkpoint is not None and checkpoint not in workers:
                # The cycle boundary was evicted; its UID can never recur,
                # so drop it and let the next survivor reseed the boundary.
                checkpoint = None
            if metadata not in workers:
                # Evicted since the snapshot was taken — skip it.
                continue
            if checkpoint is None:
                checkpoint = metadata
            elif metadata.uid == checkpoint.uid:
                # One full cycle completed without a successful
                # dispatch — signal exhaustion.
                return

            try:
                yield (metadata, connection)
            except Exception:
                # Proxy reports failure (transient or non-transient).
                # Advance to the next worker; eviction, if any, has
                # already happened in the proxy. GeneratorExit and
                # CancelledError propagate out of the generator so
                # aclose() and task cancellation work correctly.
                continue
            else:
                # Resumed without an exception: the proxy signaled
                # success. Round-robin has no per-dispatch state to
                # record, so terminate. See LoadBalancerLike for the
                # driver contract.
                return
