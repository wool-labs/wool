from __future__ import annotations

from typing import AsyncIterator
from typing import Final

from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import PredicateFunction
from wool.runtime.discovery.base import WorkerMetadata


class afilter:
    """Async filter for discovery event streams with transition
    tracking.

    Wraps a :class:`~wool.runtime.discovery.base.DiscoverySubscriberLike`
    and yields only events whose worker metadata satisfies the
    predicate.  Maintains per-iteration state so that filter
    transitions produce correct synthetic events:

    - Worker enters filter → ``worker-added``
    - Worker exits filter → ``worker-dropped`` (with prior metadata)
    - ``worker-dropped`` for tracked worker → pass through
    - ``worker-dropped`` for untracked worker → suppressed

    Each ``__aiter__`` call creates a new independent filtered
    iterator, so the object supports multiple concurrent iterations.

    :param predicate:
        Function that accepts :class:`WorkerMetadata` and returns
        ``True`` for workers that should pass the filter.
    :param inner:
        The upstream subscriber to filter.
    """

    _predicate: Final[PredicateFunction[WorkerMetadata]]
    _inner: Final[DiscoverySubscriberLike]

    def __init__(
        self,
        predicate: PredicateFunction[WorkerMetadata],
        inner: DiscoverySubscriberLike,
    ) -> None:
        self._predicate = predicate
        self._inner = inner

    def __reduce__(self) -> tuple:
        return (afilter, (self._predicate, self._inner))

    def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
        return self._filter()

    async def _filter(self) -> AsyncIterator[DiscoveryEvent]:
        """Iterate the inner subscriber and apply the predicate."""
        tracked: dict[str, WorkerMetadata] = {}
        async for event in self._inner:
            worker = event.metadata
            uid = str(worker.uid)
            passes = self._predicate(worker)
            was_tracked = uid in tracked

            if event.type == "worker-dropped":
                if was_tracked:
                    del tracked[uid]
                    yield DiscoveryEvent("worker-dropped", metadata=worker)
            elif passes and not was_tracked:
                tracked[uid] = worker
                yield DiscoveryEvent("worker-added", metadata=worker)
            elif passes and was_tracked:
                tracked[uid] = worker
                yield DiscoveryEvent("worker-updated", metadata=worker)
            elif not passes and was_tracked:
                old = tracked.pop(uid)
                yield DiscoveryEvent("worker-dropped", metadata=old)
