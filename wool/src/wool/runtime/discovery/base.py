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

from wool.protocol.worker import WorkerMetadata as WorkerMetadataProtobuf
from wool.runtime.event import Event

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
    :param address:
        gRPC target address (e.g. ``"host:port"``,
        ``"unix:path"``).
    :param pid:
        Process ID of the worker.
    :param version:
        Version string of the worker software.
    :param tags:
        Frozenset of capability tags for worker filtering and
        selection.
    :param extra:
        Additional arbitrary metadata as immutable key-value pairs.
    :param secure:
        Whether the worker requires secure TLS/mTLS connections.
    """

    uid: uuid.UUID
    address: str = field(hash=False)
    pid: int = field(hash=False)
    version: str = field(hash=False)
    tags: frozenset[str] = field(default_factory=frozenset, hash=False)
    extra: MappingProxyType = field(
        default_factory=lambda: MappingProxyType({}), hash=False
    )
    secure: bool = field(default=False, hash=False)

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
            address=protobuf.address,
            pid=protobuf.pid,
            version=protobuf.version,
            tags=frozenset(protobuf.tags),
            extra=MappingProxyType(dict(protobuf.extra)),
            secure=protobuf.secure,
        )

    def to_protobuf(self) -> WorkerMetadataProtobuf:
        """Convert this WorkerMetadata instance to a protobuf message.

        :returns:
            A protobuf WorkerMetadata message containing this instance's data.
        """

        return WorkerMetadataProtobuf(
            uid=str(self.uid),
            address=self.address,
            pid=self.pid,
            version=self.version,
            tags=list(self.tags),
            extra=dict(self.extra),
            secure=self.secure,
        )


# public
DiscoveryEventType: TypeAlias = Literal[
    "worker-added", "worker-dropped", "worker-updated"
]


# public
class DiscoveryEvent(Event):
    """Event representing a change in worker availability.

    Emitted by discovery services when workers are added, updated, or
    dropped from the pool. Contains both the event type and the
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
