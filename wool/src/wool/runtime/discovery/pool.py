from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import AsyncIterator
from typing import Callable

from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.worker.metadata import WorkerMetadata
from wool.utilities.fanout import Fanout


@dataclass(frozen=True)
class _SubscriberKey:
    """Pair a subscriber's cache identity with how to build one.

    Only ``identity`` is compared and hashed, so constructions the key
    callable calls the same share one pooled subscriber, while each
    carries a factory able to build it.

    :param identity:
        The key callable's result, which alone decides sharing.
    :param factory:
        Builds the subscriber cached under ``identity``, whose events
        are what every consumer of that entry receives. Excluded from
        equality and hashing.

    .. rubric:: Implementation notes

    The factory travels with the key rather than living in a registry
    keyed alongside it. A subscription outlives the subscriber it
    built — the pool has no TTL, so the last release retires it — and
    rebuilds one by iterating again, so the factory must stay
    reachable from every subscription that could use it and from the
    entry it built, and is released with the last of them.
    """

    identity: Any
    factory: Callable[[], DiscoverySubscriberLike] = field(compare=False)

    def __repr__(self) -> str:
        return repr(self.identity)


@dataclass
class _PooledSubscriber:
    """Hold a subscriber with the state whose life is the pool entry's.

    The fanout over a subscriber and the worker map replayed to a late
    joiner are per-subscriber state reachable only from the entry that
    owns the subscriber, so whatever drops the entry drops them —
    including the two paths `ResourcePool` documents as evicting
    without running finalizers.

    :param subscriber:
        The subscriber the key's factory built.
    :param fanout:
        The shared fanout every consumer of this subscriber pulls
        through.
    :param workers:
        The workers the subscription currently holds, replayed to an
        iteration that joins it late.

    .. rubric:: Implementation notes

    Holding the state here rather than in a mapping keyed on the
    subscriber is what makes its reclamation structural: the entry is
    the only referrer. That bounds retention, not teardown. On the two
    finalizer-less eviction paths there is no live loop, or no
    permitted ``await``, so the source iterator is still not closed and
    the fanout's pull task is still not cancelled.

    What survives is specific to the backend. A
    `~wool.LocalDiscovery.Subscriber` iteration holds its teardown in
    an exit stack that never unwinds, so a watcher thread outlives the
    subscription and the descriptors it pinned stay open. The
    uncancelled pull reports itself as a pending task destroyed, naming
    neither the fanout nor the subscriber it belonged to.
    `~wool.LocalDiscovery` carries an `atexit` fallback against the
    same hazard, where a fanout has no second route of any kind.
    """

    subscriber: Any
    fanout: Fanout[DiscoveryEvent]
    workers: dict[str, WorkerMetadata] = field(default_factory=dict)


def _pool_factory(key: Any) -> _PooledSubscriber:
    """Build the subscriber ``key`` caches, with its per-entry state.

    :param key:
        The key the acquisition was made under.
    :returns:
        The subscriber its factory builds, paired with the fanout and
        replay map that live and die with the entry holding it.
    :raises TypeError:
        If ``key`` carries no factory.

    .. rubric:: Implementation notes

    `ResourcePool` types its key `typing.Any` and hands the factory
    whatever a caller passed, so the `_SubscriberKey` requirement is
    reachable through the pool's own surface even though every
    acquisition wool makes supplies a key through `SubscriberMeta`. The
    annotation cannot state the requirement, so it is checked here.
    """
    if not isinstance(key, _SubscriberKey):
        raise TypeError(f"Subscriber pool key carries no factory: {key!r}")
    subscriber = key.factory()
    return _PooledSubscriber(subscriber=subscriber, fanout=Fanout(subscriber))


async def _pool_finalizer(pooled: _PooledSubscriber) -> None:
    """Close the entry's fanout and retire its subscriber.

    Each step runs even if the one before it raised, so a subscriber is
    never left un-retired because closing its fanout failed. The first
    failure is the one that reaches the pool; a second is discarded, so
    retiring the subscriber cannot hide why closing its fanout did not
    work.

    :param pooled:
        The entry being retired, carrying the subscriber and the fanout
        over it.
    """
    failure: BaseException | None = None
    try:
        await pooled.fanout.cleanup()
    except BaseException as error:
        failure = error
    subscriber = pooled.subscriber
    # `DiscoverySubscriberLike` declares no shutdown hook, so a backend
    # that needs teardown opts in by exposing one.
    if hasattr(subscriber, "_shutdown"):
        try:
            await subscriber._shutdown()
        except BaseException as error:
            if failure is None:
                failure = error
    if failure is not None:
        raise failure


def install_subscriber_pool() -> ResourcePool[Any]:
    """Return the context's discovery subscriber pool, installing one if needed.

    The pool is the one `__subscriber_pool__` holds, created and set on
    the first call in a context that has none. Every task in the
    process inherits that context, so a task other than the one that
    first needed the pool can still clear it.

    :returns:
        The subscriber pool for the calling context.

    .. rubric:: Implementation notes

    A `contextvars.ContextVar` first set inside a dispatch task is
    invisible to the servicer task that later has to clear it, and a
    subscriber pool a worker's teardown cannot see is a partition
    dropped unfinalized when its loop closes.
    """
    pool = __subscriber_pool__.get()
    if pool is None:
        pool = ResourcePool(factory=_pool_factory, finalizer=_pool_finalizer, ttl=0)
        __subscriber_pool__.set(pool)
    return pool


def _reconstruct(cls: type, args: tuple, kwargs: dict) -> Any:
    """Call ``cls(*args, **kwargs)`` to rebuild a pickled subscription."""
    return cls(*args, **kwargs)


class _Subscription:
    """Bridge a discovery subscriber to a shared `Fanout`.

    One of these exists per construction, and each ``__aiter__`` call
    on it is an independent consumer of the shared subscriber. The pool
    reference is released on exhaustion or ``aclose()``.

    An iteration that joins a live subscription first receives
    ``worker-added`` for every worker the shared source has already
    discovered, so no ``worker-dropped`` reaches it ahead of the
    ``worker-added`` it drops.

    An iteration that stops being pulled while a peer keeps pulling
    accumulates events without bound; see
    `~wool.utilities.fanout.FanoutConsumer`. A dropped
    ``worker-dropped`` cannot be recovered by a later scan, because the
    subscriber's own diff forgets a worker as soon as it reports the
    drop, so a consumer that discards one keeps routing to a worker
    that is gone rather than merely lagging.

    :param key:
        The `_SubscriberKey` the `ResourcePool` caches under, carrying
        the factory that builds the subscriber and rebuilds one after a
        retirement.
    :param reduce_info:
        ``(cls, args, kwargs)`` tuple used by `__reduce__` for
        pickle support.

    .. rubric:: Implementation notes

    Each iteration enters a `~wool.runtime.resourcepool.Resource` for
    ``key``, which builds the underlying subscriber on first use, and
    consumes it through the `~wool.utilities.fanout.Fanout` the entry
    carries. The ``async with`` block is what ties the pool reference
    to the generator's lifetime.
    """

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
        """Yield the shared subscriber's events for this iteration.

        :raises RuntimeError:
            If the calling context has no subscriber pool installed.
        """
        pool = __subscriber_pool__.get()
        if pool is None:
            raise RuntimeError("subscriber pool not initialised")
        async with pool.get(self._key) as resource:
            # Annotated here rather than on the `as` target, where it
            # would parse as the block's body; the pool is typed `Any`.
            pooled: _PooledSubscriber = resource
            # Primed with the current workers — see the replay
            # guarantee in the class docstring.
            consumer = pooled.fanout.consumer(
                DiscoveryEvent("worker-added", metadata=metadata)
                for metadata in list(pooled.workers.values())
            )
            async for event in consumer:
                uid = str(event.metadata.uid)
                if event.type in ("worker-added", "worker-updated"):
                    pooled.workers[uid] = event.metadata
                elif event.type == "worker-dropped":
                    pooled.workers.pop(uid, None)
                yield event

    def __reduce__(self) -> tuple:
        """Reduce to a rebuild of the construction this wraps.

        .. rubric:: Implementation notes

        A two-element reduction is what keeps ``_key`` off the wire:
        pickle emits no ``BUILD`` and never consults ``__dict__``, so
        the factory the key carries is never reached — it is a
        closure, and unpicklable. The receiving side rebuilds both.
        """
        cls, args, kwargs = self._reduce_info
        return _reconstruct, (cls, args, kwargs)


class SubscriberMeta(type):
    """Share one underlying subscriber among constructions with equal keys.

    A subscriber class passes a ``key`` callable at class definition. The
    callable receives the constructor's ``(cls, *args, **kwargs)`` and
    returns a hashable key, which must determine every constructor
    argument: a shared subscriber is built with the arguments of
    whichever construction the pool builds it for, so a key that leaves
    an argument out has no stable answer for what that argument was. The
    class itself needs no such care — it is scoped in alongside the
    returned key, so the callable covers only the arguments.

    The key must also compare by value rather than by identity. One
    whose ``__eq__`` is inherited from `object` hashes and so passes
    every check here, and then never equals the key of a later
    construction, which costs sharing silently rather than loudly: one
    subscription per construction instead of one per identity.

    Constructing a subscriber class yields a
    `~wool.DiscoverySubscriberLike` that provides iteration and a
    reduction replaying the construction arguments. Attributes declared
    on the subscriber class are unreachable through it. Constructions
    with equal keys in one `contextvars.Context` share one underlying
    subscriber. Each iteration is an independent consumer of it, and an
    iteration that joins a live subscriber first receives
    ``worker-added`` for each worker the subscription currently holds.

    A type checker cannot see the construction this metaclass injects, so
    each subscriber class declares a checker-only ``__new__`` under
    ``TYPE_CHECKING`` with its constructor's parameters and the protocol
    return type.

    .. code-block:: python

        class Sub(metaclass=SubscriberMeta, key=lambda cls, ns: ns):
            if TYPE_CHECKING:

                def __new__(cls, ns: str) -> DiscoverySubscriberLike: ...

            def __init__(self, ns: str) -> None: ...

            def __aiter__(self) -> AsyncIterator[DiscoveryEvent]: ...

    .. rubric:: Implementation notes

    Class creation injects a ``__new__`` onto the subscriber class. Each
    construction installs the context's subscriber pool and returns a
    `_Subscription` whose key carries a factory of its own. Each
    iteration of that subscription enters the pool's resource for the
    key, which builds the underlying subscriber through that factory on
    first use, and consumes it through a shared
    `~wool.utilities.fanout.Fanout`.

    The class joins the identity here rather than in the caller's
    callable because it is not a constructor argument: a callable
    returning every argument would satisfy the contract while leaving
    two conforming classes to share one subscriber.
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> SubscriberMeta:
        key_fn: Callable[..., Any] | None = kwargs.pop("key", None)
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if key_fn is not None:
            cls._cache_key_fn = key_fn  # type: ignore[attr-defined]
        original_init = cls.__init__  # type: ignore[misc]

        def _subscriber_new(cls_arg: type, *args: Any, **kwargs: Any) -> Any:
            identity = (
                cls_arg,
                cls_arg._cache_key_fn(cls_arg, *args, **kwargs),  # type: ignore[attr-defined]
            )
            # Hashed here so an unhashable key fails at the construction
            # that supplied it, not from inside a later first iteration.
            hash(identity)
            install_subscriber_pool()

            # Closes over the pre-injection ``__init__``; calling
            # ``cls_arg(*args, **kwargs)`` would re-enter this function.
            def factory() -> Any:
                instance = object.__new__(cls_arg)
                original_init(instance, *args, **kwargs)
                return instance

            return _Subscription(
                key=_SubscriberKey(identity, factory),
                reduce_info=(cls_arg, args, kwargs),
            )

        cls.__new__ = _subscriber_new  # type: ignore[assignment]
        return cls  # type: ignore[return-value]
