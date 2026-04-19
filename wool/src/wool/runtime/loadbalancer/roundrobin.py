from __future__ import annotations

import itertools
import logging
from asyncio import Lock
from typing import AsyncGenerator
from typing import Final

from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata

from .base import LoadBalancerContextView

logger = logging.getLogger(__name__)


# public
class RoundRobinLoadBalancer:
    """Round-robin load balancer for distributing tasks across workers.

    Cycles through workers in the given
    :py:class:`LoadBalancerContextView`, advancing the index after each
    yielded candidate. The :py:class:`WorkerProxy` owns the dispatch,
    eviction, and error-classification loop — the balancer's only
    responsibility is ordering.

    After one full cycle (identified by the UID of the first yielded
    candidate) the generator terminates, causing the proxy to raise
    :py:exc:`NoWorkersAvailable`.
    """

    _index: Final[dict[LoadBalancerContextView, int]]

    def __init__(self):
        self._index = {}
        self._lock = Lock()

    def __reduce__(self):
        return (self.__class__, ())

    async def delegate(
        self,
        *,
        context: LoadBalancerContextView,
    ) -> AsyncGenerator[
        tuple[WorkerMetadata, WorkerConnection],
        WorkerMetadata | None,
    ]:
        """Yield worker candidates in round-robin order.

        :param context:
            Read-only view of the worker pool. Eviction is the
            proxy's responsibility; the balancer only reads
            ``context.workers`` to pick the next candidate.
        :yields:
            ``(metadata, connection)`` pairs. The generator is driven
            by the proxy via ``anext``/``athrow``/``asend``.
        """
        if context not in self._index:
            self._index[context] = 0

        checkpoint: WorkerMetadata | None = None
        while context.workers:
            async with self._lock:
                if self._index[context] >= len(context.workers):
                    self._index[context] = 0
                metadata, connection = next(
                    itertools.islice(
                        context.workers.items(),
                        self._index[context],
                        self._index[context] + 1,
                    )
                )
                if checkpoint is None:
                    checkpoint = metadata
                elif metadata.uid == checkpoint.uid:
                    # One full cycle completed without a successful
                    # dispatch — signal exhaustion.
                    return
                self._index[context] = self._index[context] + 1

            try:
                sent = yield (metadata, connection)
            except Exception:
                # Proxy reports failure (transient or non-transient).
                # Advance to the next worker; eviction, if any, has
                # already happened in the proxy. GeneratorExit and
                # CancelledError propagate out of the generator so
                # aclose() and task cancellation work correctly.
                continue

            if sent is not None:
                # Proxy reports success via asend(metadata). Contract:
                # the generator MUST terminate here — no further yields.
                # Round-robin has no per-dispatch bookkeeping to record,
                # so we just return.
                return
            # sent is None — caller resumed via anext() without signaling
            # success or failure. Continue cycling to the next worker.
