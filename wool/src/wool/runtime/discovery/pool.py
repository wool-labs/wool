from __future__ import annotations

import weakref
from typing import Any
from typing import AsyncIterator
from typing import Callable
from typing import ClassVar

from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.worker.metadata import WorkerMetadata
from wool.utilities.fanout import Fanout

_subscriber_factories: dict[Any, Callable[[Any], Any]] = {}
"""Per-key factory registry populated by :class:`SubscriberMeta`."""


def _pool_factory(key: Any) -> Any:
    """Dispatch to the registered factory for *key*."""
    return _subscriber_factories[key](key)


def _reconstruct(cls: type, args: tuple, kwargs: dict) -> Any:
    """Pickle helper — calls ``cls(*args, **kwargs)``."""
    return cls(*args, **kwargs)


class _SharedSubscription:
    """Adapter bridging a discovery subscriber and a :class:`Fanout`.

    Each ``__aiter__`` call returns an independent async generator
    that enters a :class:`~wool.runtime.resourcepool.Resource` from
    the pool, wraps the raw subscriber in a shared
    :class:`~wool.utilities.fanout.Fanout`, and iterates a
    :class:`~wool.utilities.fanout.FanoutConsumer`.  The ``async
    with`` context naturally releases the pool reference on
    exhaustion or ``aclose()``.

    Late-joining consumers receive a replay of ``worker-added``
    events for all workers the shared source has already discovered,
    so they start with a consistent view of the current state.

    :param key:
        Cache key for the :class:`ResourcePool`.
    :param reduce_info:
        ``(cls, args, kwargs)`` tuple used by :meth:`__reduce__` for
        pickle support.
    """

    _fanouts: ClassVar[weakref.WeakKeyDictionary[Any, Fanout[DiscoveryEvent]]] = (
        weakref.WeakKeyDictionary()
    )
    _workers: ClassVar[weakref.WeakKeyDictionary[Any, dict[str, WorkerMetadata]]] = (
        weakref.WeakKeyDictionary()
    )

    def __init__(
        self,
        key: Any,
        reduce_info: tuple[type, tuple, dict[str, Any]],
    ) -> None:
        self._key = key
        self._reduce_info = reduce_info

    def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[DiscoveryEvent]:
        """Enter the pool resource, create a consumer, and iterate."""
        pool = __subscriber_pool__.get()
        if pool is None:
            raise RuntimeError("subscriber pool not initialised")
        async with pool.get(self._key) as subscriber:
            fanout = self._fanouts.get(subscriber)
            if fanout is None:
                fanout = Fanout(subscriber)
                self._fanouts[subscriber] = fanout
            consumer = fanout.consumer()
            # Replay current workers so late joiners see existing
            # state.
            workers = self._workers.get(subscriber)
            if workers:
                for metadata in workers.values():
                    consumer.enqueue(DiscoveryEvent("worker-added", metadata=metadata))
            async for event in consumer:
                # Track latest worker state for late-joiner replay.
                uid = str(event.metadata.uid)
                ws = self._workers.setdefault(subscriber, {})
                if event.type in ("worker-added", "worker-updated"):
                    ws[uid] = event.metadata
                elif event.type == "worker-dropped":
                    ws.pop(uid, None)
                yield event

    def __reduce__(self) -> tuple:
        cls, args, kwargs = self._reduce_info
        return _reconstruct, (cls, args, kwargs)


class SubscriberMeta(type):
    """Metaclass that caches discovery subscriber singletons.

    Intercepts class creation via ``__new__`` and injects a custom
    ``__new__`` onto the subscriber class.  The injected method
    registers a factory that creates raw subscribers, then returns a
    :class:`_SharedSubscription` whose pool
    :class:`~wool.runtime.resourcepool.Resource` is entered lazily
    on first iteration.

    Subscriber classes using this metaclass must define a
    ``_cache_key`` classmethod that returns a hashable key from the
    constructor arguments.
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ) -> SubscriberMeta:
        cls = super().__new__(mcs, name, bases, namespace)
        original_init = cls.__init__  # type: ignore[misc]

        def _subscriber_new(cls_arg: type, *args: Any, **kwargs: Any) -> Any:
            key = cls_arg._cache_key(*args, **kwargs)  # type: ignore[attr-defined]
            pool = __subscriber_pool__.get()
            if pool is None:
                pool = ResourcePool(
                    factory=_pool_factory,
                    finalizer=_pool_finalizer,
                    ttl=0,
                )
                __subscriber_pool__.set(pool)

            def factory(_: Any) -> Any:
                instance = object.__new__(cls_arg)
                original_init(instance, *args, **kwargs)
                return instance

            _subscriber_factories.setdefault(key, factory)
            return _SharedSubscription(
                key=key,
                reduce_info=(cls_arg, args, kwargs),
            )

        cls.__new__ = _subscriber_new  # type: ignore[assignment]
        return cls  # type: ignore[return-value]


async def _pool_finalizer(subscriber: Any) -> None:
    """Resource pool finalizer — clean up fanout and subscriber."""
    fanout = _SharedSubscription._fanouts.pop(subscriber, None)
    if fanout is not None:
        await fanout.cleanup()
    if hasattr(subscriber, "_shutdown"):
        await subscriber._shutdown()
