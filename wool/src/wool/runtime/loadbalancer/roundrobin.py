from __future__ import annotations

import itertools
import logging
from asyncio import Lock
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import Final

from wool.runtime.worker.connection import HandshakeError
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError

from .base import AllWorkersUnauthenticated
from .base import LoadBalancerContextLike
from .base import LoadBalancerLike
from .base import NoWorkersAvailable

if TYPE_CHECKING:
    from wool.runtime.routine.task import Task

logger = logging.getLogger(__name__)


# public
class RoundRobinLoadBalancer(LoadBalancerLike):
    """Round-robin load balancer for distributing tasks across workers.

    Cycles through workers in the given
    :py:class:`LoadBalancerContextLike`, advancing the index after each
    successful dispatch. When a dispatch attempt fails, transient
    errors skip to the next worker while non-transient errors remove
    the worker from the context. One full cycle is attempted per
    dispatch call.

    **Worker-health exception contract:** the load balancer treats
    :class:`~wool.runtime.worker.connection.RpcError` (and its
    transient subclass
    :class:`~wool.runtime.worker.connection.TransientRpcError`) as
    worker-health concerns. Other exceptions raised by
    :meth:`WorkerConnection.dispatch` — e.g. a strict-mode
    :class:`BaseExceptionGroup` of
    :class:`wool.ContextDecodeWarning` peers from a caller-side
    encode failure, or a programming-error
    :class:`ValueError` — propagate to the caller unwrapped, so a
    fault that has nothing to do with worker health does not evict
    workers from the pool.
    """

    _index: Final[dict[LoadBalancerContextLike, int]]

    def __init__(self):
        self._index = {}
        self._lock = Lock()

    def __reduce__(self):
        return (self.__class__, ())

    async def dispatch(
        self,
        task: Task,
        *,
        context: LoadBalancerContextLike,
        timeout: float | None = None,
    ) -> AsyncGenerator:
        """Dispatch a task to the next available worker.

        :param task:
            The :py:class:`Task` to dispatch.
        :param context:
            The :py:class:`LoadBalancerContextLike` to select workers
            from.
        :param timeout:
            Timeout in seconds for each dispatch attempt. If ``None``,
            no timeout is applied.
        :returns:
            An async iterator that yields worker responses.
        :raises NoWorkersAvailable:
            If no healthy workers are available for dispatch.
        """
        checkpoint = None
        rejections: dict = {}
        had_non_handshake_failure = False

        if context not in self._index:
            self._index[context] = 0

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
                    checkpoint = metadata.uid
                elif metadata.uid == checkpoint:
                    break

                try:
                    stream = await connection.dispatch(task, timeout=timeout)
                except TransientRpcError as exc:
                    logger.debug(
                        "Skipping worker %s on transient error: %s",
                        metadata.uid,
                        exc,
                    )
                    had_non_handshake_failure = True
                    self._index[context] = self._index[context] + 1
                    continue
                except HandshakeError as exc:
                    # A failed secure handshake is recoverable: the worker may
                    # adopt rotated credentials out of band. Skip it without
                    # eviction (like a transient error) so a later dispatch
                    # retries with freshly-resolved credentials, and record the
                    # rejection so the terminal raise can tell "every worker
                    # refused my credentials" apart from "the pool is empty".
                    # The per-rejection warning is the observability surface;
                    # aggregation over time is left to metrics/tracing.
                    logger.warning(
                        "Skipping worker %s at %s after handshake failure (%s): %s",
                        metadata.uid,
                        metadata.address,
                        exc.reason,
                        exc,
                    )
                    rejections[metadata.uid] = exc
                    self._index[context] = self._index[context] + 1
                    continue
                except RpcError as exc:
                    logger.warning(
                        "Evicting worker %s after non-transient RPC error: %s",
                        metadata.uid,
                        exc,
                    )
                    had_non_handshake_failure = True
                    context.remove_worker(metadata)
                    if metadata.uid == checkpoint:
                        checkpoint = None
                    continue
                else:
                    self._index[context] = self._index[context] + 1
                    return stream

        # Promote to AllWorkersUnauthenticated only when the cycle drained
        # purely on handshake rejections — a surviving transient worker or a
        # non-handshake eviction means the pool was not uniformly refusing the
        # client's credentials, so the plain empty-pool signal is correct.
        if rejections and not had_non_handshake_failure:
            raise AllWorkersUnauthenticated(
                "Every worker tried refused the client's credentials",
                rejections=rejections,
            )
        raise NoWorkersAvailable("No healthy workers available for dispatch")
