from __future__ import annotations

import itertools
from typing import TYPE_CHECKING
from typing import AsyncIterator
from typing import Final

from wool.runtime.worker.connection import TransientRpcError

from .base import LoadBalancerContext
from .base import LoadBalancerLike
from .base import NoWorkersAvailable

if TYPE_CHECKING:
    from wool.runtime.work import WorkTask


# public
class RoundRobinLoadBalancer(LoadBalancerLike):
    """Round-robin load balancer for distributing tasks across workers.

    Distributes tasks evenly across available workers using a simple round-robin
    algorithm. Workers are managed through :class:`LoadBalancerContext` instances
    passed to the dispatch method, enabling a single load balancer to service
    multiple worker pools with independent state.

    Automatically handles worker failures by trying the next worker in the
    round-robin cycle. Workers that encounter transient errors remain in the
    context, while workers that fail with non-transient errors are removed from
    the context's worker list.
    """

    _index: Final[dict[LoadBalancerContext, int]]

    def __init__(self):
        self._index = {}

    async def dispatch(
        self,
        task: WorkTask,
        *,
        context: LoadBalancerContext,
        timeout: float | None = None,
    ) -> AsyncIterator:
        """Dispatch a task to the next available worker using round-robin.

        Tries workers in one round-robin cycle until dispatch succeeds.
        Workers that fail to schedule the task with a non-transient error are
        removed from the context's worker list.

        :param task:
            The :class:`WorkTask` instance to dispatch to the worker.
        :param context:
            The :class:`LoadBalancerContext` containing workers to dispatch to.
        :param timeout:
            Timeout in seconds for each dispatch attempt. If ``None``, no
            timeout is applied.
        :returns:
            A streaming dispatch result that yields worker responses.
        :raises NoWorkersAvailable:
            If no healthy workers are available to schedule the task.
        """
        checkpoint = None

        # Initialize index for this context if not present
        if context not in self._index:
            self._index[context] = 0

        while context.workers:
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
                    result = await connection.dispatch(task, timeout=timeout)
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
                    return result

        raise NoWorkersAvailable("No healthy workers available for dispatch")
