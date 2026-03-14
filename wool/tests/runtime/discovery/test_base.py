import uuid
from types import MappingProxyType
from typing import AsyncIterator

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool.protocol import WorkerMetadata as WorkerMetadataProtobuf
from wool.runtime.discovery.base import Discovery
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoveryEventType
from wool.runtime.discovery.base import DiscoveryLike
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import WorkerMetadata


@pytest.fixture
def metadata():
    """Provides sample WorkerMetadata for testing.

    Creates a WorkerMetadata instance with typical field values for use in
    tests that need a well-formed worker instance.
    """
    return WorkerMetadata(
        uid=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        address="localhost:50051",
        pid=12345,
        version="1.0.0",
        tags=frozenset(["test", "worker"]),
        extra=MappingProxyType({"key": "value"}),
    )


@pytest.fixture
def metadata_message():
    """Provides sample protobuf WorkerMetadata for testing.

    Creates a protobuf WorkerMetadata message with typical field values for use
    in tests that need to deserialize protobuf messages.
    """
    return WorkerMetadataProtobuf(
        uid="12345678-1234-5678-1234-567812345678",
        address="localhost:50051",
        pid=12345,
        version="1.0.0",
        tags=["test", "worker"],
        extra={"key": "value"},
    )


class TestWorkerMetadata:
    """Tests for WorkerMetadata dataclass.

    Fully qualified name: wool.runtime.discovery.base.WorkerMetadata
    """

    def test_immutability(self, metadata):
        """Test WorkerMetadata instances are immutable.

        Given:
            A WorkerMetadata instance
        When:
            Attempting to modify a field
        Then:
            It should raise AttributeError (frozen dataclass)
        """
        # Act & assert
        with pytest.raises(AttributeError):
            metadata.address = "newhost:50051"

    def test_init(self):
        """Test WorkerMetadata default field values.

        Given:
            Required field values for WorkerMetadata
        When:
            Creating WorkerMetadata without optional fields
        Then:
            It should use empty frozenset for tags and empty MappingProxyType
            for extra
        """
        # Arrange
        uid = uuid.uuid4()

        # Act
        worker = WorkerMetadata(
            uid=uid, address="localhost:50051", pid=123, version="1.0.0"
        )

        # Assert
        assert worker.tags == frozenset()
        assert worker.extra == MappingProxyType({})

    def test_hash(self):
        """Test WorkerMetadata hash is based on uid only.

        Given:
            Two WorkerMetadata instances with same uid but different other fields
        When:
            Computing hash of both instances
        Then:
            It should return the same hash value
        """
        # Arrange
        uid = uuid.uuid4()
        worker1 = WorkerMetadata(uid=uid, address="host1:5001", pid=123, version="1.0.0")
        worker2 = WorkerMetadata(uid=uid, address="host2:5002", pid=456, version="2.0.0")

        # Act & assert
        assert hash(worker1) == hash(worker2)

    def test_eq(self):
        """Test WorkerMetadata equality is based on all fields.

        Given:
            Two WorkerMetadata instances with same uid but different other fields
        When:
            Comparing the instances for equality
        Then:
            It should return False (equality checks all fields)
        """
        # Arrange
        uid = uuid.uuid4()
        worker1 = WorkerMetadata(uid=uid, address="host1:5001", pid=123, version="1.0.0")
        worker2 = WorkerMetadata(uid=uid, address="host2:5002", pid=456, version="2.0.0")

        # Act & assert
        assert worker1 != worker2

        # Workers with all same fields should be equal
        worker3 = WorkerMetadata(uid=uid, address="host1:5001", pid=123, version="1.0.0")
        assert worker1 == worker3

    def test_from_protobuf(self, metadata_message):
        """Test from_protobuf() with valid protobuf message.

        Given:
            A valid protobuf WorkerMetadata message
        When:
            Converting to WorkerMetadata
        Then:
            It should create WorkerMetadata with matching field values
        """
        # Act
        worker = WorkerMetadata.from_protobuf(metadata_message)

        # Assert
        assert worker.uid == uuid.UUID("12345678-1234-5678-1234-567812345678")
        assert worker.address == "localhost:50051"
        assert worker.pid == 12345
        assert worker.version == "1.0.0"
        assert worker.tags == frozenset(["test", "worker"])
        assert worker.extra == MappingProxyType({"key": "value"})

    def test_from_protobuf_invalid_uuid(self):
        """Test from_protobuf() with invalid UUID string.

        Given:
            A protobuf WorkerMetadata with invalid UUID string
        When:
            Converting to WorkerMetadata
        Then:
            It should raise ValueError
        """
        # Arrange
        protobuf = WorkerMetadataProtobuf(
            uid="invalid-uuid",
            address="localhost:50051",
            pid=12345,
            version="1.0.0",
        )

        # Act & assert
        with pytest.raises(ValueError):
            WorkerMetadata.from_protobuf(protobuf)

    def test_to_protobuf(self, metadata):
        """Test to_protobuf() with valid WorkerMetadata.

        Given:
            A valid WorkerMetadata instance
        When:
            Converting to protobuf
        Then:
            It should create protobuf message with matching field values
        """
        # Act
        protobuf = metadata.to_protobuf()

        # Assert
        assert protobuf.uid == "12345678-1234-5678-1234-567812345678"
        assert protobuf.address == "localhost:50051"
        assert protobuf.pid == 12345
        assert protobuf.version == "1.0.0"
        assert set(protobuf.tags) == {"test", "worker"}
        assert dict(protobuf.extra) == {"key": "value"}

    def test_to_protobuf_with_secure_flag_roundtrip(self):
        """Test secure=True roundtrip through protobuf serialization.

        Given:
            A WorkerMetadata with secure=True
        When:
            to_protobuf() and from_protobuf() roundtrip
        Then:
            It should preserve secure field as True.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=12345,
            version="1.0.0",
            secure=True,
        )

        # Act
        protobuf = worker.to_protobuf()
        restored = WorkerMetadata.from_protobuf(protobuf)

        # Assert
        assert restored.secure is True

    @given(
        address=st.from_regex(r"^[a-zA-Z0-9._-]+:[0-9]+$", fullmatch=True),
        pid=st.integers(min_value=1, max_value=2147483647),
        version=st.text(min_size=1),
    )
    def test_roundtrip_conversion(self, address, pid, version):
        """Test round-trip conversion preserves WorkerMetadata data.

        Given:
            A WorkerMetadata instance with arbitrary field values
        When:
            Converting to protobuf and back to WorkerMetadata
        Then:
            It should preserve all field values
        """
        # Arrange
        uid = uuid.uuid4()
        original = WorkerMetadata(
            uid=uid,
            address=address,
            pid=pid,
            version=version,
        )

        # Act
        serialized = original.to_protobuf()
        deserialized = WorkerMetadata.from_protobuf(serialized)

        # Assert
        assert deserialized.uid == original.uid
        assert deserialized.address == original.address
        assert deserialized.pid == original.pid
        assert deserialized.version == original.version
        assert deserialized.tags == original.tags
        assert deserialized.extra == original.extra


class TestDiscoveryEvent:
    """Tests for DiscoveryEvent dataclass.

    Fully qualified name: wool.runtime.discovery.base.DiscoveryEvent
    """

    @pytest.mark.parametrize(
        "event_type",
        ["worker-added", "worker-dropped", "worker-updated"],
    )
    def test_event(self, metadata, event_type: DiscoveryEventType):
        """Test creating DiscoveryEvent with valid event types.

        Given:
            A WorkerMetadata instance and valid event type
        When:
            Creating a DiscoveryEvent
        Then:
            It should create event with specified type and metadata
        """
        # Act
        event = DiscoveryEvent(event_type, metadata=metadata)

        # Assert
        assert event.type is event_type
        assert event.metadata is metadata

    def test_dataclass_properties(self, metadata):
        """Test DiscoveryEvent dataclass properties.

        Given:
            A DiscoveryEvent instance
        When:
            Accessing fields and modifying mutable instance
        Then:
            It should allow field access and modification (not frozen)
        """
        # Arrange
        event = DiscoveryEvent("worker-added", metadata=metadata)
        new_worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="newhost:9999",
            pid=99999,
            version="2.0.0",
        )

        # Act
        event.metadata = new_worker

        # Assert
        assert event.metadata is new_worker


class TestDiscoveryPublisherLike:
    """Tests for DiscoveryPublisherLike protocol.

    Fully qualified name: wool.runtime.discovery.base.DiscoveryPublisherLike
    """

    def test_conforming_protocol(self):
        """Test runtime_checkable protocol with conforming class.

        Given:
            A class with async publish(event) method
        When:
            Checking protocol compliance with isinstance
        Then:
            It should return True
        """

        # Arrange
        class ConformingPublisher:
            async def publish(
                self, type: DiscoveryEventType, metadata: WorkerMetadata
            ): ...

        publisher = ConformingPublisher()

        # Act & assert
        assert isinstance(publisher, DiscoveryPublisherLike)

    def test_nonconforming_protocol(self):
        """Test runtime_checkable protocol with non-conforming class.

        Given:
            A class without publish method
        When:
            Checking protocol compliance with isinstance
        Then:
            It should return False
        """

        # Arrange
        class NonConformingPublisher:
            async def something_else(self): ...

        publisher = NonConformingPublisher()

        # Act & assert
        assert not isinstance(publisher, DiscoveryPublisherLike)

    def test_runtime_checkable(self):
        """Test protocol is runtime_checkable.

        Given:
            The DiscoveryPublisherLike protocol
        When:
            Checking if it's runtime_checkable
        Then:
            It should allow isinstance checks at runtime
        """

        # Arrange
        class Publisher:
            async def publish(
                self, type: DiscoveryEventType, metadata: WorkerMetadata
            ) -> None: ...

        publisher = Publisher()

        # Act & assert
        isinstance(publisher, DiscoveryPublisherLike)


class TestDiscoverySubscriberLike:
    """Tests for DiscoverySubscriberLike protocol.

    Fully qualified name: wool.runtime.discovery.base.DiscoverySubscriberLike
    """

    def test_conforming_protocol(self):
        """Test runtime_checkable protocol with conforming class.

        Given:
            A class with __aiter__ method
        When:
            Checking protocol compliance with isinstance
        Then:
            It should return True
        """

        # Arrange
        class ConformingSubscriber:
            def __aiter__(self): ...

            async def __anext__(self):
                raise StopAsyncIteration

        subscriber = ConformingSubscriber()

        # Act & assert
        assert isinstance(subscriber, DiscoverySubscriberLike)

    def test_nonconforming_protocol(self):
        """Test runtime_checkable protocol with non-conforming class.

        Given:
            A class without __aiter__ method
        When:
            Checking protocol compliance with isinstance
        Then:
            It should return False
        """

        # Arrange
        class NonConformingSubscriber:
            async def something_else(self): ...

        subscriber = NonConformingSubscriber()

        # Act & assert
        assert not isinstance(subscriber, DiscoverySubscriberLike)

    def test_runtime_checkable(self):
        """Test protocol is runtime_checkable.

        Given:
            The DiscoverySubscriberLike protocol
        When:
            Checking if it's runtime_checkable
        Then:
            It should allow isinstance checks at runtime
        """

        # Arrange
        class Subscriber:
            def __aiter__(self) -> AsyncIterator[DiscoveryEvent]: ...

        subscriber = Subscriber()

        # Act & assert
        isinstance(subscriber, DiscoverySubscriberLike)


class TestDiscoveryLike:
    """Tests for DiscoveryLike protocol.

    Fully qualified name: wool.runtime.discovery.base.DiscoveryLike
    """

    def test_conforming_protocol(self):
        """Test runtime_checkable protocol with conforming class.

        Given:
            A class with publisher, subscriber properties and
            subscribe() method
        When:
            isinstance() check against DiscoveryLike
        Then:
            It should return True.
        """

        # Arrange
        class ConformingDiscovery:
            @property
            def publisher(self) -> DiscoveryPublisherLike: ...

            @property
            def subscriber(self) -> DiscoverySubscriberLike: ...

            def subscribe(self, filter=None) -> DiscoverySubscriberLike: ...

        instance = ConformingDiscovery()

        # Act & assert
        assert isinstance(instance, DiscoveryLike)

    def test_nonconforming_missing_subscribe(self):
        """Test runtime_checkable protocol rejects missing subscribe.

        Given:
            A class missing subscribe() method
        When:
            isinstance() check against DiscoveryLike
        Then:
            It should return False.
        """

        # Arrange
        class MissingSubscribe:
            @property
            def publisher(self) -> DiscoveryPublisherLike: ...

            @property
            def subscriber(self) -> DiscoverySubscriberLike: ...

        instance = MissingSubscribe()

        # Act & assert
        assert not isinstance(instance, DiscoveryLike)

    def test_nonconforming_missing_publisher(self):
        """Test runtime_checkable protocol rejects missing publisher.

        Given:
            A class missing publisher property
        When:
            isinstance() check against DiscoveryLike
        Then:
            It should return False.
        """

        # Arrange
        class MissingPublisher:
            @property
            def subscriber(self) -> DiscoverySubscriberLike: ...

            def subscribe(self, filter=None) -> DiscoverySubscriberLike: ...

        instance = MissingPublisher()

        # Act & assert
        assert not isinstance(instance, DiscoveryLike)


class TestDiscovery:
    """Tests for Discovery ABC.

    Fully qualified name: wool.runtime.discovery.base.Discovery
    """

    def test___init___with_concrete_subclass(self):
        """Test concrete subclass instantiation.

        Given:
            A concrete subclass implementing all abstract methods
        When:
            Subclass is instantiated
        Then:
            It should create an instance successfully.
        """

        # Arrange
        class ConcreteDiscovery(Discovery):
            @property
            def publisher(self) -> DiscoveryPublisherLike:
                return None  # type: ignore

            @property
            def subscriber(self) -> DiscoverySubscriberLike:
                return None  # type: ignore

            def subscribe(self, filter=None) -> DiscoverySubscriberLike:
                return None  # type: ignore

        # Act
        instance = ConcreteDiscovery()

        # Assert
        assert isinstance(instance, Discovery)

    def test___init___with_missing_abstract_method(self):
        """Test missing abstract method raises TypeError.

        Given:
            A subclass missing publisher implementation
        When:
            Subclass is instantiated
        Then:
            It should raise TypeError.
        """

        # Arrange
        class IncompleteDiscovery(Discovery):
            @property
            def subscriber(self) -> DiscoverySubscriberLike:
                return None  # type: ignore

            def subscribe(self, filter=None) -> DiscoverySubscriberLike:
                return None  # type: ignore

        # Act & assert
        with pytest.raises(TypeError):
            IncompleteDiscovery()  # type: ignore

    def test___init___structural_subtyping_conformance(self):
        """Test concrete Discovery conforms to DiscoveryLike.

        Given:
            A concrete Discovery subclass instance
        When:
            isinstance() check against DiscoveryLike
        Then:
            It should return True.
        """

        # Arrange
        class ConcreteDiscovery(Discovery):
            @property
            def publisher(self) -> DiscoveryPublisherLike:
                return None  # type: ignore

            @property
            def subscriber(self) -> DiscoverySubscriberLike:
                return None  # type: ignore

            def subscribe(self, filter=None) -> DiscoverySubscriberLike:
                return None  # type: ignore

        instance = ConcreteDiscovery()

        # Act & assert
        assert isinstance(instance, DiscoveryLike)
