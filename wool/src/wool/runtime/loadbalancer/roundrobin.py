from __future__ import annotations

import uuid
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import Final
from weakref import WeakKeyDictionary

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

    The rotation snapshots the pool's uids once per wrap and indexes
    them in O(1). A refresh mid-cycle needs no handling here —
    candidates are uids (see `LoadBalancerLike`). The cycle boundary is
    the uid of the first yielded candidate and survives such refreshes:
    the cycle terminates when that uid recurs. Only when the boundary
    uid leaves the pool is the boundary reseeded from the next
    surviving candidate, since a uid that has left can never recur and
    would otherwise never close the cycle.

    Per-context rotation state lives in a `WeakKeyDictionary` keyed on
    the context, so a retired pool's cursor is reclaimed with the
    context instead of pinned for the balancer's lifetime.
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
    ) -> AsyncGenerator[uuid.UUID, uuid.UUID | None]:
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
            Worker uids. The generator is driven by the proxy via
            ``anext``/``athrow``/``asend``.
        """
        checkpoint: uuid.UUID | None = None
        snapshot: list[uuid.UUID] = []
        cursor = 0
        # Hoisted: ``workers`` is a live view (see
        # LoadBalancerContextView), so re-reading it per candidate would
        # buy nothing.
        workers = context.workers
        while True:
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
                snapshot = list(workers)
                cursor = index % len(snapshot)
            uid = snapshot[cursor]
            cursor += 1
            self._index[context] = index + 1

            if checkpoint is not None and checkpoint not in workers:
                # See the rubric: a boundary uid that has left the pool
                # can never recur, so reseed from the next survivor.
                checkpoint = None
            if uid not in workers:
                # Evicted since the snapshot was taken. The proxy would
                # skip it anyway; not yielding it keeps the cycle
                # boundary anchored to a worker that still exists.
                continue
            if checkpoint is None:
                checkpoint = uid
            elif uid == checkpoint:
                # One full cycle completed without a successful
                # dispatch — signal exhaustion.
                return

            try:
                yield uid
            except Exception:
                # Proxy reports failure (transient or non-transient).
                # Advance to the next worker; eviction, if any, has
                # already happened in the proxy. GeneratorExit and
                # CancelledError propagate out of the generator so
                # aclose() and task cancellation work correctly.
                continue
            else:
                # A non-exception resume means success *here* only
                # because the ``uid not in workers`` guard above never
                # offers a candidate the proxy would skip, so the
                # ``None`` resume `LoadBalancerLike` documents cannot
                # reach this branch. A balancer without that guard must
                # test the value the yield returns instead. Round-robin
                # has no per-dispatch state to record, so terminate.
                return
