import uuid
from types import MappingProxyType
from typing import AsyncIterator

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoveryEventType
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import WorkerInfo
from wool.runtime.protobuf.worker import WorkerInfo as WorkerInfoProtobuf


@pytest.fixture
def worker_info():
    """Provides sample WorkerInfo for testing.

    Creates a WorkerInfo instance with typical field values for use in
    tests that need a well-formed worker instance.
    """
    return WorkerInfo(
        uid=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        host="localhost",
        port=50051,
        pid=12345,
        version="1.0.0",
        tags=frozenset(["test", "worker"]),
        extra=MappingProxyType({"key": "value"}),
    )


@pytest.fixture
def worker_info_message():
    """Provides sample protobuf WorkerInfo for testing.

    Creates a protobuf WorkerInfo message with typical field values for use
    in tests that need to deserialize protobuf messages.
    """
    return WorkerInfoProtobuf(
        uid="12345678-1234-5678-1234-567812345678",
        host="localhost",
        port=50051,
        pid=12345,
        version="1.0.0",
        tags=["test", "worker"],
        extra={"key": "value"},
    )


class TestWorkerInfo:
    """Tests for WorkerInfo dataclass.

    Fully qualified name: wool.runtime.discovery.base.WorkerInfo
    """

    def test_immutability(self, worker_info):
        """Test WorkerInfo instances are immutable.

        Given:
            A WorkerInfo instance
        When:
            Attempting to modify a field
        Then:
            It should raise AttributeError (frozen dataclass)
        """
        # Act & Assert
        with pytest.raises(AttributeError):
            worker_info.host = "newhost"

    def test_init(self):
        """Test WorkerInfo default field values.

        Given:
            Required field values for WorkerInfo
        When:
            Creating WorkerInfo without optional fields
        Then:
            It should use empty frozenset for tags and empty MappingProxyType
            for extra
        """
        # Arrange
        uid = uuid.uuid4()

        # Act
        worker = WorkerInfo(
            uid=uid, host="localhost", port=50051, pid=123, version="1.0.0"
        )

        # Assert
        assert worker.tags == frozenset()
        assert worker.extra == MappingProxyType({})

    def test_hash(self):
        """Test WorkerInfo hash is based on uid only.

        Given:
            Two WorkerInfo instances with same uid but different other fields
        When:
            Computing hash of both instances
        Then:
            It should return the same hash value
        """
        # Arrange
        uid = uuid.uuid4()
        worker1 = WorkerInfo(uid=uid, host="host1", port=5001, pid=123, version="1.0.0")
        worker2 = WorkerInfo(uid=uid, host="host2", port=5002, pid=456, version="2.0.0")

        # Act & Assert
        assert hash(worker1) == hash(worker2)

    def test_eq(self):
        """Test WorkerInfo equality is based on all fields.

        Given:
            Two WorkerInfo instances with same uid but different other fields
        When:
            Comparing the instances for equality
        Then:
            It should return False (equality checks all fields)
        """
        # Arrange
        uid = uuid.uuid4()
        worker1 = WorkerInfo(uid=uid, host="host1", port=5001, pid=123, version="1.0.0")
        worker2 = WorkerInfo(uid=uid, host="host2", port=5002, pid=456, version="2.0.0")

        # Act & Assert
        assert worker1 != worker2

        # Workers with all same fields should be equal
        worker3 = WorkerInfo(uid=uid, host="host1", port=5001, pid=123, version="1.0.0")
        assert worker1 == worker3

    def test_from_protobuf(self, worker_info_message):
        """Test from_protobuf() with valid protobuf message.

        Given:
            A valid protobuf WorkerInfo message
        When:
            Converting to WorkerInfo
        Then:
            It should create WorkerInfo with matching field values
        """
        # Act
        worker = WorkerInfo.from_protobuf(worker_info_message)

        # Assert
        assert worker.uid == uuid.UUID("12345678-1234-5678-1234-567812345678")
        assert worker.host == "localhost"
        assert worker.port == 50051
        assert worker.pid == 12345
        assert worker.version == "1.0.0"
        assert worker.tags == frozenset(["test", "worker"])
        assert worker.extra == MappingProxyType({"key": "value"})

    def test_from_protobuf_invalid_uuid(self):
        """Test from_protobuf() with invalid UUID string.

        Given:
            A protobuf WorkerInfo with invalid UUID string
        When:
            Converting to WorkerInfo
        Then:
            It should raise ValueError
        """
        # Arrange
        protobuf = WorkerInfoProtobuf(
            uid="invalid-uuid",
            host="localhost",
            port=50051,
            pid=12345,
            version="1.0.0",
        )

        # Act & Assert
        with pytest.raises(ValueError):
            WorkerInfo.from_protobuf(protobuf)

    def test_from_protobuf_port_zero(self):
        """Test from_protobuf() converts port 0 to None.

        Given:
            A protobuf WorkerInfo with port=0
        When:
            Converting to WorkerInfo
        Then:
            It should set port to None
        """
        # Arrange
        protobuf = WorkerInfoProtobuf(
            uid="12345678-1234-5678-1234-567812345678",
            host="localhost",
            port=0,
            pid=12345,
            version="1.0.0",
        )

        # Act
        worker = WorkerInfo.from_protobuf(protobuf)

        # Assert
        assert worker.port is None

    def test_to_protobuf(self, worker_info):
        """Test to_protobuf() with valid WorkerInfo.

        Given:
            A valid WorkerInfo instance
        When:
            Converting to protobuf
        Then:
            It should create protobuf message with matching field values
        """
        # Act
        protobuf = worker_info.to_protobuf()

        # Assert
        assert protobuf.uid == "12345678-1234-5678-1234-567812345678"
        assert protobuf.host == "localhost"
        assert protobuf.port == 50051
        assert protobuf.pid == 12345
        assert protobuf.version == "1.0.0"
        assert set(protobuf.tags) == {"test", "worker"}
        assert dict(protobuf.extra) == {"key": "value"}

    def test_to_protobuf_port_none(self):
        """Test to_protobuf() converts None port to 0.

        Given:
            A WorkerInfo instance with port=None
        When:
            Converting to protobuf
        Then:
            It should set port to 0 in protobuf message
        """
        # Arrange
        worker = WorkerInfo(
            uid=uuid.UUID("12345678-1234-5678-1234-567812345678"),
            host="localhost",
            port=None,
            pid=12345,
            version="1.0.0",
        )

        # Act
        protobuf = worker.to_protobuf()

        # Assert
        assert protobuf.port == 0

    @given(
        host=st.text(min_size=1),
        port=st.one_of(st.integers(min_value=1, max_value=65535), st.none()),
        pid=st.integers(min_value=1, max_value=2147483647),  # int32 max
        version=st.text(min_size=1),
    )
    def test_roundtrip_conversion(self, host, port, pid, version):
        """Test round-trip conversion preserves WorkerInfo data.

        Given:
            A WorkerInfo instance with arbitrary field values
        When:
            Converting to protobuf and back to WorkerInfo
        Then:
            It should preserve all field values
        """
        # Arrange
        uid = uuid.uuid4()
        original = WorkerInfo(
            uid=uid,
            host=host,
            port=port,
            pid=pid,
            version=version,
        )

        # Act
        serialized = original.to_protobuf()
        deserialized = WorkerInfo.from_protobuf(serialized)

        # Assert
        assert deserialized.uid == original.uid
        assert deserialized.host == original.host
        assert deserialized.port == original.port
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
    def test_event(self, worker_info, event_type: DiscoveryEventType):
        """Test creating DiscoveryEvent with valid event types.

        Given:
            A WorkerInfo instance and valid event type
        When:
            Creating a DiscoveryEvent
        Then:
            It should create event with specified type and worker_info
        """
        # Act
        event = DiscoveryEvent(type=event_type, worker_info=worker_info)

        # Assert
        assert event.type is event_type
        assert event.worker_info is worker_info

    def test_dataclass_properties(self, worker_info):
        """Test DiscoveryEvent dataclass properties.

        Given:
            A DiscoveryEvent instance
        When:
            Accessing fields and modifying mutable instance
        Then:
            It should allow field access and modification (not frozen)
        """
        # Arrange
        event = DiscoveryEvent(type="worker-added", worker_info=worker_info)
        new_worker = WorkerInfo(
            uid=uuid.uuid4(),
            host="newhost",
            port=9999,
            pid=99999,
            version="2.0.0",
        )

        # Act
        event.worker_info = new_worker

        # Assert
        assert event.worker_info is new_worker


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
                self, type: DiscoveryEventType, worker_info: WorkerInfo
            ): ...

        publisher = ConformingPublisher()

        # Act & Assert
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

        # Act & Assert
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
                self, type: DiscoveryEventType, worker_info: WorkerInfo
            ) -> None: ...

        publisher = Publisher()

        # Act & Assert
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

        # Act & Assert
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

        # Act & Assert
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

        # Act & Assert
        isinstance(subscriber, DiscoverySubscriberLike)
