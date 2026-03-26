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


# public
DiscoveryEventType: TypeAlias = Literal[
    "worker-added", "worker-dropped", "worker-updated"
]


# public
class DiscoveryEvent:
    """Event representing a change in worker availability.

    Used by discovery services when workers are added, updated, or
    dropped from the pool. Contains both the event type and the
    affected worker's metadata.

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
    worker lifecycle events (added, updated, dropped) to subscribers.
    """

    async def publish(self, type: DiscoveryEventType, metadata: WorkerMetadata) -> None:
        """Publish a worker discovery event.

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
    """

    def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
        """Return an async iterator of discovery events.

        :returns:
            An async iterator yielding discovery events.
        """
        ...


# public
@runtime_checkable
class DiscoveryLike(Protocol):
    """Structural type for custom discovery backends.

    See :py:class:`Discovery` for a convenience abstract base class.
    """

    @property
    def publisher(self) -> DiscoveryPublisherLike:
        """Get the publisher component.

        :returns:
            A publisher instance for broadcasting events.
        """
        ...

    @property
    def subscriber(self) -> DiscoverySubscriberLike:
        """Get the default subscriber component.

        :returns:
            A subscriber instance for receiving events.
        """
        ...

    def subscribe(
        self, filter: PredicateFunction | None = None
    ) -> DiscoverySubscriberLike:
        """Create a filtered subscriber.

        :param filter:
            Optional predicate to filter discovered workers.
        :returns:
            A subscriber instance for receiving filtered events.
        """
        ...


# public
class Discovery(ABC):
    """Convenience base class for discovery implementations.

    Conforming to :py:class:`DiscoveryLike` via structural subtyping
    is sufficient; inheriting from this class is optional.
    """

    @property
    @abstractmethod
    def publisher(self) -> DiscoveryPublisherLike: ...

    @property
    @abstractmethod
    def subscriber(self) -> DiscoverySubscriberLike: ...

    @abstractmethod
    def subscribe(
        self, filter: PredicateFunction | None = None
    ) -> DiscoverySubscriberLike: ...
