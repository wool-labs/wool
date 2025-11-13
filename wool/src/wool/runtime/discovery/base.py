from __future__ import annotations

import uuid
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from types import MappingProxyType
from typing import AsyncIterator
from typing import Callable
from typing import Final
from typing import Literal
from typing import Protocol
from typing import TypeAlias
from typing import TypeVar
from typing import runtime_checkable

from wool.runtime.event import Event
from wool.runtime.protobuf.worker import WorkerMetadata as WorkerMetadataProtobuf

T: Final = TypeVar("T")

# public
PredicateFunction: TypeAlias = Callable[[T], bool]


# public
@dataclass(frozen=True)
class WorkerMetadata:
    """Properties and metadata for a worker instance.

    Contains identifying information and capabilities of a worker that
    can be used for discovery, filtering, and routing decisions.

    :param uid:
        Unique identifier for the worker instance (UUID).
    :param host:
        Network host address where the worker is accessible.
    :param port:
        Network port number where the worker is listening.
    :param pid:
        Process ID of the worker.
    :param version:
        Version string of the worker software.
    :param tags:
        Frozenset of capability tags for worker filtering and selection.
    :param extra:
        Additional arbitrary metadata as immutable key-value pairs.
    """

    uid: uuid.UUID
    host: str = field(hash=False)
    port: int | None = field(hash=False)
    pid: int = field(hash=False)
    version: str = field(hash=False)
    tags: frozenset[str] = field(default_factory=frozenset, hash=False)
    extra: MappingProxyType = field(
        default_factory=lambda: MappingProxyType({}), hash=False
    )

    @classmethod
    def from_protobuf(cls, protobuf: WorkerMetadataProtobuf) -> WorkerMetadata:
        """Create a WorkerMetadata instance from a protobuf message.

        :param protobuf:
            The protobuf WorkerMetadata message to deserialize.
        :returns:
            A new WorkerMetadata instance with data from the protobuf message.
        :raises ValueError:
            If the UID in the protobuf message is not a valid UUID.
        """

        return cls(
            uid=uuid.UUID(protobuf.uid),
            host=protobuf.host,
            port=protobuf.port if protobuf.port != 0 else None,
            pid=protobuf.pid,
            version=protobuf.version,
            tags=frozenset(protobuf.tags),
            extra=MappingProxyType(dict(protobuf.extra)),
        )

    def to_protobuf(self) -> WorkerMetadataProtobuf:
        """Convert this WorkerMetadata instance to a protobuf message.

        :returns:
            A protobuf WorkerMetadata message containing this instance's data.
        """

        return WorkerMetadataProtobuf(
            uid=str(self.uid),
            host=self.host,
            port=self.port if self.port is not None else 0,
            pid=self.pid,
            version=self.version,
            tags=list(self.tags),
            extra=dict(self.extra),
        )


# public
DiscoveryEventType: TypeAlias = Literal[
    "worker-added", "worker-dropped", "worker-updated"
]


# public
class DiscoveryEvent(Event):
    """Event representing a change in worker availability.

    Emitted by discovery services when workers are added, updated, or
    removed from the pool. Contains both the event type and the
    affected worker's metadata.

    :param type:
        The type of discovery event.
    :param metadata:
        Information about the worker that triggered the event.
    """

    metadata: WorkerMetadata

    def __init__(self, type: DiscoveryEventType, /, metadata: WorkerMetadata) -> None:
        super().__init__(type)
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
    """Protocol for complete discovery service implementations.

    Defines the interface for discovery services that provide both
    publishing and subscribing capabilities. Implementations must
    expose publisher and subscriber components and support filtered
    subscriptions.
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
    """Abstract base class for worker discovery implementations.

    Provides the foundation for pluggable discovery mechanisms that
    enable workers to find and communicate with each other. Concrete
    implementations handle the specifics of service registration and
    discovery based on the deployment environment (LAN, local, cloud).

    Subclasses must implement the publisher and subscriber properties,
    as well as the subscribe method for filtered subscriptions.
    """

    @property
    @abstractmethod
    def publisher(self) -> DiscoveryPublisherLike:
        """Get the publisher component.

        :returns:
            A publisher instance for broadcasting events.
        """
        ...

    @property
    @abstractmethod
    def subscriber(self) -> DiscoverySubscriberLike:
        """Get the default subscriber component.

        :returns:
            A subscriber instance for receiving events.
        """
        ...

    @abstractmethod
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
