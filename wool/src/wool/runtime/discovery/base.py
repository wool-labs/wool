from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import AsyncIterator
from typing import Callable
from typing import Final
from typing import Literal
from typing import Protocol
from typing import TypeAlias
from typing import TypeVar
from typing import runtime_checkable

from wool.runtime.worker.metadata import WorkerMetadata

T: Final = TypeVar("T")

# public
PredicateFunction: TypeAlias = Callable[[T], bool]
"""Predicate over discovered workers; returning True admits the worker."""


# public
DiscoveryEventType: TypeAlias = Literal[
    "worker-added", "worker-dropped", "worker-updated"
]
"""The worker lifecycle event kinds a discovery service emits."""


# public
class DiscoveryEvent:
    """Event representing a change in worker availability.

    Emitted by discovery services as workers are added, updated, or
    dropped. Contains both the event type and the affected worker's
    metadata.

    :param type:
        The type of discovery event.
    :param metadata:
        Information about the worker that triggered the event.
    """

    type: DiscoveryEventType
    metadata: WorkerMetadata

    def __init__(self, type: DiscoveryEventType, /, metadata: WorkerMetadata) -> None:
        self.type = type
        self.metadata = metadata


# public
@runtime_checkable
class DiscoveryPublisherLike(Protocol):
    """Protocol for publishing worker discovery events.

    Implementations must provide a publish method that broadcasts
    worker lifecycle events (added, updated, dropped) to subscribers,
    and declare a ``bind_host`` stating where advertised workers
    should listen.

    Implementations may additionally be sync or async context
    managers: the runtime enters a publisher before its first publish
    and exits it after the last, so transport setup and teardown
    belong in the context-manager hooks rather than ``__init__``.
    """

    #: Host that workers advertised through this publisher should
    #: bind, so the advertised address is properly served. A publisher
    #: that advertises off-host-reachable addresses prescribes the
    #: wildcard; a same-host publisher prescribes loopback. Threaded
    #: by `WorkerPool` into bind-host-aware worker factories,
    #: including the default factory (see `~wool.WorkerFactory`).
    bind_host: str

    async def publish(self, type: DiscoveryEventType, metadata: WorkerMetadata) -> None:
        """Publish a worker discovery event.

        Publishers cannot assume they saw every prior event for a
        worker, so lifecycles may arrive out of order. An
        implementation may tolerate this — e.g. treat an update for an
        unknown worker as a registration and a drop for an unknown
        worker as a no-op — or it may reject the event (e.g. raise) if
        the application requires strict lifecycle ordering; wool is
        unopinionated about which.

        Exceptions raised here propagate to the publishing pool: a
        failure while announcing a spawned worker aborts the pool's
        startup, so transient transport errors worth surviving must
        be retried or suppressed inside the implementation.

        :param type:
            The type of discovery event.
        :param metadata:
            Information about the worker.
        """
        ...


# public
@runtime_checkable
class DiscoverySubscriberLike(Protocol):
    """Protocol for receiving worker discovery events.

    Implementations must provide an async iterator that yields
    discovery events as workers join, leave, or update their status.

    Subscriber instances ride `WorkerProxy`'s reduction when routines
    dispatch nested routines, so they must be picklable; see the
    caution on `WorkerProxy` about pre-called context managers.
    """

    def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
        """Return an async iterator of discovery events.

        Implementations must meet the following requirements:

        - **Late subscribers must learn current state.** Iteration
          may begin long after workers were announced — a lazy proxy
          subscribes at its first dispatch — so each iteration must
          first surface every currently-known worker as a
          ``worker-added`` event before streaming deltas. A
          deltas-only stream starves late consumers of pre-existing
          workers.
        - **Iterations must be independent and cancellation-safe.**
          Each call should produce an isolated stream, since multiple
          consumers may iterate one subscriber concurrently, and the
          runtime cancels iteration to unsubscribe. Release resources
          on cancellation (e.g., in an async generator's ``finally``).

        :returns:
            An async iterator yielding discovery events.
        """
        ...


# public
@runtime_checkable
class DiscoveryLike(Protocol):
    """Structural type for custom discovery backends.

    A discovery backend pairs a publisher, i.e., how workers announce
    themselves, with subscribers, i.e., how peers learn about workers.
    The forms in which a discovery protocol may be supplied are documented
    by `WorkerPool`'s ``discovery`` parameter; the resolved object must
    satisfy this protocol.

    See `Discovery` for a convenience abstract base class.
    """

    @property
    def publisher(self) -> DiscoveryPublisherLike:
        """The publisher component.

        The runtime reads this once per publishing pool lifetime;
        returning a fresh publisher on each access is permitted.

        :returns:
            A publisher instance for broadcasting events.
        """
        ...

    @property
    def subscriber(self) -> DiscoverySubscriberLike:
        """The default subscriber component.

        Equivalent to an unfiltered `DiscoveryLike.subscribe`.

        :returns:
            A subscriber instance for receiving events.
        """
        ...

    def subscribe(
        self, filter: PredicateFunction | None = None
    ) -> DiscoverySubscriberLike:
        """Create a filtered subscriber.

        Filtering is the discovery's responsibility: consumers apply
        no worker filtering of their own, so a worker rejected by the
        predicate must never surface in the returned stream.

        :param filter:
            Optional predicate to filter discovered workers; ``None``
            subscribes to every worker.
        :returns:
            A subscriber instance for receiving filtered events.
        """
        ...


# public
class Discovery(ABC):
    """Convenience base class for discovery implementations.

    Conforming to `DiscoveryLike` via structural subtyping is
    sufficient; inheriting from this class is optional.
    """

    @property
    @abstractmethod
    def publisher(self) -> DiscoveryPublisherLike:
        """The publisher component; see `DiscoveryLike.publisher`."""
        ...

    @property
    @abstractmethod
    def subscriber(self) -> DiscoverySubscriberLike:
        """The default subscriber component; see `DiscoveryLike.subscriber`."""
        ...

    @abstractmethod
    def subscribe(
        self, filter: PredicateFunction | None = None
    ) -> DiscoverySubscriberLike:
        """Create a filtered subscriber; see `DiscoveryLike.subscribe`."""
        ...
