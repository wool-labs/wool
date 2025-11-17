from __future__ import annotations

import itertools
import logging
from asyncio import Lock
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import Final

from wool.runtime.worker.connection import TransientRpcError

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
    """

    _index: Final[dict[LoadBalancerContextLike, int]]

    def __init__(self):
        self._index = {}
        self._lock = Lock()

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

        if context not in self._index:
            self._index[context] = 0

        while context.workers:
            async with self._lock:
                if self._index[context] >= len(context.workers):
                    self._index[context] = 0

                metadata, connection_resource_factory = next(
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

                async with connection_resource_factory() as connection:
                    try:
                        stream = await connection.dispatch(task, timeout=timeout)
                    except TransientRpcError:
                        self._index[context] = self._index[context] + 1
                        continue
                    except Exception:
                        context.remove_worker(metadata)
                        if metadata.uid == checkpoint:
                            checkpoint = None
                        continue
                    else:
                        self._index[context] = self._index[context] + 1
                        return stream

        raise NoWorkersAvailable("No healthy workers available for dispatch")
