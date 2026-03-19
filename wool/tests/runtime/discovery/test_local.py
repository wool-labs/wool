import asyncio
import struct
import uuid
from types import MappingProxyType

import cloudpickle
import portalocker
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.discovery.local import LocalDiscovery


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
    )


@pytest.fixture
def namespace():
    """Provides unique namespace for test isolation.

    Creates a unique namespace string for each test to ensure shared
    memory regions don't interfere with each other.
    """
    return f"test-namespace-{uuid.uuid4()}"


class TestLocalDiscovery:
    """Tests for LocalDiscovery class.

    Fully qualified name: wool.runtime.discovery.local.LocalDiscovery
    """

    def test___init___without_namespace(self):
        """Test LocalDiscovery default namespace generation.

        Given:
            No arguments
        When:
            LocalDiscovery is instantiated
        Then:
            It should auto-generate a namespace starting with
            "workerpool-".
        """
        # Act
        discovery = LocalDiscovery()

        # Assert
        assert discovery.namespace.startswith("workerpool-")

    def test___init___with_custom_namespace(self):
        """Test LocalDiscovery custom namespace.

        Given:
            A custom namespace string
        When:
            LocalDiscovery is instantiated
        Then:
            It should return the provided namespace.
        """
        # Act
        discovery = LocalDiscovery("my-namespace")

        # Assert
        assert discovery.namespace == "my-namespace"

    def test_publisher_with_default_instance(self, namespace):
        """Test publisher property returns Publisher with matching namespace.

        Given:
            A LocalDiscovery instance
        When:
            publisher property is accessed
        Then:
            It should return a Publisher with matching namespace.
        """
        # Arrange
        discovery = LocalDiscovery(namespace)

        # Act
        publisher = discovery.publisher

        # Assert
        assert isinstance(publisher, LocalDiscovery.Publisher)
        assert publisher.namespace == namespace

    def test_subscriber_with_default_instance(self, namespace):
        """Test subscriber property returns Subscriber instance.

        Given:
            A LocalDiscovery instance
        When:
            subscriber property is accessed
        Then:
            It should return a Subscriber.
        """
        # Arrange
        discovery = LocalDiscovery(namespace)

        # Act
        subscriber = discovery.subscriber

        # Assert
        assert isinstance(subscriber, LocalDiscovery.Subscriber)
        assert subscriber.namespace == namespace

    @pytest.mark.asyncio
    async def test_subscribe_with_default_filter(self, namespace):
        """Test subscribe() propagates the constructor's default filter.

        Given:
            A LocalDiscovery with a default filter
        When:
            subscribe() is called without a filter
        Then:
            It should use the default filter for the event stream.
        """

        # Arrange
        def predicate(w):
            return w.address == "localhost:50051"

        worker_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )
        worker_no_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="otherhost:9999",
            pid=456,
            version="1.0",
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()

        with LocalDiscovery(namespace, filter=predicate) as discovery:
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = discovery.subscribe(poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))
                await asyncio.sleep(0.05)
                await publisher.publish("worker-added", worker_match)
                await publisher.publish("worker-added", worker_no_match)

                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                await asyncio.sleep(0.1)

                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        assert all(e.metadata.address == "localhost:50051" for e in events)

    @pytest.mark.asyncio
    async def test_subscribe_with_explicit_filter(self, namespace):
        """Test subscribe(filter=predicate) overrides the default filter.

        Given:
            A LocalDiscovery instance
        When:
            subscribe(filter=predicate) is called
        Then:
            It should use the provided filter for the event stream.
        """

        # Arrange
        def predicate(w):
            return w.address == "localhost:50051"

        worker_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )
        worker_no_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="otherhost:9999",
            pid=456,
            version="1.0",
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()

        with LocalDiscovery(namespace) as discovery:
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = discovery.subscribe(filter=predicate, poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))
                await asyncio.sleep(0.05)
                await publisher.publish("worker-added", worker_match)
                await publisher.publish("worker-added", worker_no_match)

                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                await asyncio.sleep(0.1)

                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        assert all(e.metadata.address == "localhost:50051" for e in events)

    @pytest.mark.asyncio
    async def test_subscribe_with_custom_poll_interval(self, namespace):
        """Test subscribe(poll_interval=...) uses the specified interval.

        Given:
            A LocalDiscovery instance
        When:
            subscribe(poll_interval=1.0) is called
        Then:
            It should discover workers via polling within the interval.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace) as discovery:
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = discovery.subscribe(poll_interval=0.1)

            async with publisher:
                await publisher.publish("worker-added", worker)

                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) >= 1
        assert events[0].type == "worker-added"

    def test___enter___and___exit___lifecycle(self):
        """Test LocalDiscovery context manager lifecycle.

        Given:
            A LocalDiscovery instance
        When:
            Used as a context manager via with statement
        Then:
            It should create shared memory on entry and clean up on
            exit.
        """
        # Arrange
        discovery = LocalDiscovery(f"test-cm-{uuid.uuid4()}")

        # Act & assert
        with discovery as ctx:
            assert ctx is discovery


class TestLocalDiscoveryPublisher:
    """Tests for LocalDiscovery.Publisher class.

    Fully qualified name:
    wool.runtime.discovery.local.LocalDiscovery.Publisher
    """

    def test_namespace_with_provided_value(self, namespace):
        """Test Publisher.namespace property returns provided value.

        Given:
            A namespace string
        When:
            Publisher is instantiated
        Then:
            It should return the provided namespace.
        """
        # Act
        publisher = LocalDiscovery.Publisher(namespace)

        # Assert
        assert publisher.namespace == namespace

    def test___init___with_negative_block_size(self, namespace):
        """Test Publisher rejects negative block sizes.

        Given:
            A negative block_size
        When:
            Publisher is instantiated
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="Block size must be positive"):
            LocalDiscovery.Publisher(namespace, block_size=-1)

    @pytest.mark.asyncio
    async def test___aenter___and___aexit___lifecycle(self, namespace):
        """Test Publisher async context manager lifecycle.

        Given:
            A Publisher instance
        When:
            Used as an async context manager via async with
        Then:
            It should be available inside the block and cleaned up
            after.
        """
        # Arrange
        publisher = LocalDiscovery.Publisher(namespace)

        # Act & assert
        async with publisher as ctx:
            assert ctx is publisher

    @pytest.mark.asyncio
    async def test_publish_worker_added(self, namespace, metadata):
        """Test publish("worker-added") makes worker discoverable.

        Given:
            A LocalDiscovery context and an initialized Publisher
        When:
            publish("worker-added", metadata) is called
        Then:
            It should store the worker so subscribers can discover it.
        """
        # Arrange
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            # Act
            async with publisher:
                await publisher.publish("worker-added", metadata)

                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test_publish_worker_dropped(self, namespace, metadata):
        """Test publish("worker-dropped") removes worker from discovery.

        Given:
            A published worker
        When:
            publish("worker-dropped", metadata) is called
        Then:
            It should remove the worker from shared memory.
        """
        # Arrange
        events = []
        worker_added = asyncio.Event()
        worker_dropped = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-added" and event.metadata.uid == metadata.uid:
                    worker_added.set()
                elif (
                    event.type == "worker-dropped" and event.metadata.uid == metadata.uid
                ):
                    worker_dropped.set()
                    break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))

                await publisher.publish("worker-added", metadata)
                await asyncio.wait_for(worker_added.wait(), timeout=2.0)

                # Act
                await publisher.publish("worker-dropped", metadata)

                try:
                    await asyncio.wait_for(worker_dropped.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker drop not detected within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        dropped = [e for e in events if e.type == "worker-dropped"]
        assert len(dropped) >= 1
        assert dropped[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test_publish_worker_updated(self, namespace, metadata):
        """Test publish("worker-updated") updates worker metadata.

        Given:
            A published worker
        When:
            publish("worker-updated", updated_metadata) is called
        Then:
            It should update the worker metadata in shared memory.
        """
        # Arrange
        updated_worker = WorkerMetadata(
            uid=metadata.uid,
            address="newhost:9999",
            pid=99999,
            version="2.0.0",
        )
        events = []
        worker_added = asyncio.Event()
        worker_updated = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-added" and event.metadata.uid == metadata.uid:
                    worker_added.set()
                elif (
                    event.type == "worker-updated"
                    and event.metadata.uid == metadata.uid
                    and event.metadata.version == "2.0.0"
                ):
                    worker_updated.set()
                    break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))

                await publisher.publish("worker-added", metadata)
                await asyncio.wait_for(worker_added.wait(), timeout=2.0)

                # Act
                await publisher.publish("worker-updated", updated_worker)

                try:
                    await asyncio.wait_for(worker_updated.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker update not detected within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        updated = [
            e
            for e in events
            if e.type == "worker-updated" and e.metadata.version == "2.0.0"
        ]
        assert len(updated) >= 1
        assert updated[0].metadata.version == "2.0.0"

    @pytest.mark.asyncio
    async def test_publish_with_invalid_event_type(self, namespace, metadata):
        """Test publish() raises error for invalid event types.

        Given:
            An initialized Publisher
        When:
            publish("invalid-type", metadata) is called
        Then:
            It should raise RuntimeError.
        """
        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)

            # Act & assert
            async with publisher:
                with pytest.raises(
                    RuntimeError,
                    match="Unexpected discovery event type",
                ):
                    await publisher.publish(
                        "invalid-type",
                        metadata,  # type: ignore
                    )

    @given(
        address=st.from_regex(r"^[a-zA-Z0-9._-]+:[0-9]+$", fullmatch=True),
        pid=st.integers(min_value=1, max_value=2147483647),
        version=st.text(
            min_size=1,
            max_size=20,
            alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-_",
        ),
    )
    @settings(
        max_examples=10,
        deadline=5000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_publish_roundtrip_with_arbitrary_metadata(
        self, namespace, address, pid, version
    ):
        """Test publish-discover roundtrip with arbitrary metadata.

        Given:
            Arbitrary valid WorkerMetadata field values
        When:
            Worker is published then discovered via a subscriber
        Then:
            All metadata fields should match the published values.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address=address,
            pid=pid,
            version=version,
        )

        events = []
        discovered = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                discovered.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            # Act
            async with publisher:
                await publisher.publish("worker-added", worker)

                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(discovered.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) == 1
        assert events[0].metadata.uid == worker.uid
        assert events[0].metadata.address == worker.address
        assert events[0].metadata.pid == worker.pid
        assert events[0].metadata.version == worker.version
        assert events[0].metadata.tags == worker.tags
        assert events[0].metadata.extra == worker.extra

    @pytest.mark.asyncio
    async def test_publish_worker_updated_non_existent_raises_key_error(
        self, namespace, metadata
    ):
        """Test update non-existent worker raises KeyError.

        Given:
            An initialized Publisher with no published workers
        When:
            publish("worker-updated", metadata) is called for a
            worker that was never added
        Then:
            It should raise KeyError.
        """
        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)

            # Act & assert
            async with publisher:
                with pytest.raises(KeyError, match=str(metadata.uid)):
                    await publisher.publish("worker-updated", metadata)

    @pytest.mark.asyncio
    async def test_publish_to_full_address_space_raises_runtime_error(self, namespace):
        """Test publish to full address space raises RuntimeError.

        Given:
            A LocalDiscovery whose address space is completely
            filled with non-null references
        When:
            A new worker is published
        Then:
            It should raise RuntimeError with "No available slots".
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )

        with LocalDiscovery(namespace, capacity=4) as discovery:
            publisher = LocalDiscovery.Publisher(namespace)

            # Fill all slots with non-null references
            buf = discovery._address_space.buf
            for i in range(0, len(buf), 16):
                struct.pack_into("16s", buf, i, b"\xff" * 16)

            # Act & assert
            async with publisher:
                with pytest.raises(RuntimeError, match="No available slots"):
                    await publisher.publish("worker-added", worker)

    @pytest.mark.asyncio
    async def test_publish_update_overflow_preserves_prior_state(self, namespace):
        """Test update with oversized metadata preserves prior state.

        Given:
            A published worker with small metadata in a Publisher
            with a small block size
        When:
            The worker is updated with metadata too large for the
            block
        Then:
            It should raise struct.error and preserve the original
            metadata.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )
        oversized_worker = WorkerMetadata(
            uid=worker.uid,
            address="localhost:50051",
            pid=123,
            version="1.0",
            extra=MappingProxyType({"data": "x" * 20000}),
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace, block_size=100)

            async with publisher:
                await publisher.publish("worker-added", worker)

                # Act
                with pytest.raises(struct.error):
                    await publisher.publish("worker-updated", oversized_worker)

                # Assert — original metadata preserved via subscriber
                subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discoverable after rollback")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        assert len(events) == 1
        assert events[0].metadata.uid == worker.uid
        assert events[0].metadata.version == "1.0"

    @pytest.mark.asyncio
    async def test_publish_with_concurrent_lock_contention(
        self, namespace, metadata, mocker
    ):
        """Test publish retries on concurrent lock contention.

        Given:
            A file lock on the publisher's namespace that is
            temporarily held by another process
        When:
            publish("worker-added") is called
        Then:
            It should retry lock acquisition and succeed once
            the lock is released.
        """
        # Arrange
        original_lock = portalocker.lock
        attempt = 0

        def contending_lock(fh, flags):
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                raise portalocker.LockException("Lock held")
            original_lock(fh, flags)

        mocker.patch(
            "wool.runtime.discovery.local.portalocker.lock",
            side_effect=contending_lock,
        )

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)

            # Act
            async with publisher:
                await publisher.publish("worker-added", metadata)

        # Assert
        assert attempt == 2


class TestLocalDiscoverySubscriber:
    """Tests for LocalDiscovery.Subscriber class.

    Fully qualified name:
    wool.runtime.discovery.local.LocalDiscovery.Subscriber
    """

    def test_namespace_with_provided_value(self, namespace):
        """Test Subscriber.namespace property returns provided value.

        Given:
            A namespace string
        When:
            Subscriber is instantiated
        Then:
            It should return the provided namespace.
        """
        # Act
        subscriber = LocalDiscovery.Subscriber(namespace)

        # Assert
        assert subscriber.namespace == namespace

    def test___init___with_negative_poll_interval(self, namespace):
        """Test Subscriber rejects negative poll_interval.

        Given:
            A negative poll_interval
        When:
            Subscriber is instantiated
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="Expected positive poll interval"):
            LocalDiscovery.Subscriber(namespace, poll_interval=-1.0)

    @pytest.mark.asyncio
    async def test___aiter___discovers_added_worker(self, namespace, metadata):
        """Test async for yields worker-added event.

        Given:
            A published worker
        When:
            Subscriber is iterated via async for
        Then:
            It should yield a worker-added event with matching
            metadata.
        """
        # Arrange
        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            async with publisher:
                await publisher.publish("worker-added", metadata)

                # Act
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) == 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test___aiter___detects_dropped_worker(self, namespace, metadata):
        """Test async for yields worker-dropped event.

        Given:
            A published then dropped worker
        When:
            Subscriber is iterated via async for
        Then:
            It should yield a worker-dropped event.
        """
        # Arrange
        events = []
        worker_added = asyncio.Event()
        worker_dropped = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-added" and event.metadata.uid == metadata.uid:
                    worker_added.set()
                elif (
                    event.type == "worker-dropped" and event.metadata.uid == metadata.uid
                ):
                    worker_dropped.set()
                    break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))

                await publisher.publish("worker-added", metadata)
                await asyncio.wait_for(worker_added.wait(), timeout=2.0)

                # Act
                await publisher.publish("worker-dropped", metadata)

                try:
                    await asyncio.wait_for(worker_dropped.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker drop not detected within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        dropped = [e for e in events if e.type == "worker-dropped"]
        assert len(dropped) >= 1
        assert dropped[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test___aiter___detects_updated_worker(self, namespace, metadata):
        """Test async for yields worker-updated event.

        Given:
            A published then updated worker
        When:
            Subscriber is iterated via async for
        Then:
            It should yield a worker-updated event with new metadata.
        """
        # Arrange
        updated_worker = WorkerMetadata(
            uid=metadata.uid,
            address="newhost:9999",
            pid=99999,
            version="2.0.0",
        )
        events = []
        worker_added = asyncio.Event()
        worker_updated = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-added" and event.metadata.uid == metadata.uid:
                    worker_added.set()
                elif (
                    event.type == "worker-updated"
                    and event.metadata.uid == metadata.uid
                    and event.metadata.version == "2.0.0"
                ):
                    worker_updated.set()
                    break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

            async with publisher:
                task = asyncio.create_task(collect(subscriber))

                await publisher.publish("worker-added", metadata)
                await asyncio.wait_for(worker_added.wait(), timeout=2.0)

                # Act
                await publisher.publish("worker-updated", updated_worker)

                try:
                    await asyncio.wait_for(worker_updated.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker update not detected within timeout")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        updated = [
            e
            for e in events
            if e.type == "worker-updated" and e.metadata.version == "2.0.0"
        ]
        assert len(updated) >= 1
        assert updated[0].metadata.version == "2.0.0"

    @pytest.mark.asyncio
    async def test___aiter___with_filter_predicate(self, namespace):
        """Test async for with filter predicate.

        Given:
            A subscriber with a filter predicate
        When:
            Workers matching and not matching the filter are published
        Then:
            It should yield only matching workers in the event stream.
        """
        # Arrange
        worker_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="host1:50051",
            pid=111,
            version="1.0",
        )
        worker_no_match = WorkerMetadata(
            uid=uuid.uuid4(),
            address="host2:9999",
            pid=222,
            version="1.0",
        )

        def filter_fn(w):
            return w.address == "host1:50051"

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(
                namespace, filter=filter_fn, poll_interval=0.05
            )

            async with publisher:
                task = asyncio.create_task(collect(subscriber))
                await asyncio.sleep(0.05)

                await publisher.publish("worker-added", worker_match)
                await publisher.publish("worker-added", worker_no_match)

                try:
                    await asyncio.wait_for(event_received.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                await asyncio.sleep(0.1)

                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        assert all(e.metadata.address == "host1:50051" for e in events)

    @pytest.mark.asyncio
    async def test___aiter___with_poll_interval(self, namespace):
        """Test subscriber discovers workers within poll window.

        Given:
            A subscriber with poll_interval set
        When:
            A worker is published
        Then:
            It should yield the event within the poll window.
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )

        events = []
        event_received = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                events.append(event)
                event_received.set()
                break

        with LocalDiscovery(namespace):
            publisher = LocalDiscovery.Publisher(namespace)
            subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.1)

            async with publisher:
                await publisher.publish("worker-added", worker)

                # Act
                task = asyncio.create_task(collect(subscriber))
                try:
                    await asyncio.wait_for(event_received.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pytest.fail("Worker not discovered within poll window")
                finally:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

        # Assert
        assert len(events) >= 1
        assert events[0].type == "worker-added"

    def test___reduce___pickle_roundtrip(self, namespace):
        """Test Subscriber pickle roundtrip via cloudpickle.

        Given:
            A Subscriber instance
        When:
            Subscriber is pickled and unpickled via cloudpickle
        Then:
            It should preserve the namespace.
        """
        # Arrange
        subscriber = LocalDiscovery.Subscriber(namespace, poll_interval=0.05)

        # Act
        pickled = cloudpickle.dumps(subscriber)
        unpickled = cloudpickle.loads(pickled)

        # Assert
        assert unpickled.namespace == namespace

    @pytest.mark.asyncio
    async def test___aiter___with_concurrent_namespaces(self):
        """Test concurrent subscribers on different namespaces do not collide.

        Given:
            Two LocalDiscovery instances with different namespaces
        When:
            Both subscribers async-iterate simultaneously
        Then:
            It should deliver events to both without RuntimeError or
            BlockingIOError.
        """
        # Arrange
        ns_a = f"test-concurrent-a-{uuid.uuid4()}"
        ns_b = f"test-concurrent-b-{uuid.uuid4()}"
        worker_a = WorkerMetadata(
            uid=uuid.uuid4(),
            address="host-a:50051",
            pid=111,
            version="1.0",
        )
        worker_b = WorkerMetadata(
            uid=uuid.uuid4(),
            address="host-b:50052",
            pid=222,
            version="1.0",
        )

        events_a: list = []
        events_b: list = []
        received_a = asyncio.Event()
        received_b = asyncio.Event()
        started_a = asyncio.Event()
        started_b = asyncio.Event()

        async def collect(subscriber, events, received, started):
            started.set()
            async for event in subscriber:
                events.append(event)
                received.set()
                break

        with LocalDiscovery(ns_a) as discovery_a, LocalDiscovery(ns_b) as discovery_b:
            publisher_a = discovery_a.publisher
            publisher_b = discovery_b.publisher
            subscriber_a = discovery_a.subscribe(poll_interval=0.05)
            subscriber_b = discovery_b.subscribe(poll_interval=0.05)

            async with publisher_a, publisher_b:
                task_a = asyncio.create_task(collect(subscriber_a, events_a, received_a, started_a))
                task_b = asyncio.create_task(collect(subscriber_b, events_b, received_b, started_b))
                await asyncio.gather(started_a.wait(), started_b.wait())

                # Act
                await publisher_a.publish("worker-added", worker_a)
                await publisher_b.publish("worker-added", worker_b)

                try:
                    await asyncio.wait_for(
                        asyncio.gather(received_a.wait(), received_b.wait()),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    pytest.fail("Concurrent subscribers did not both receive events")
                finally:
                    for t in (task_a, task_b):
                        t.cancel()
                        try:
                            await t
                        except asyncio.CancelledError:
                            pass

        # Assert
        assert len(events_a) == 1
        assert events_a[0].metadata.uid == worker_a.uid
        assert len(events_b) == 1
        assert events_b[0].metadata.uid == worker_b.uid

    @pytest.mark.asyncio
    async def test___aiter___with_multiple_subscribers_same_namespace(
        self, namespace, metadata
    ):
        """Test two subscribers on the same namespace receive events independently.

        Given:
            Two Subscribers on the same namespace
        When:
            A worker is published
        Then:
            It should deliver the worker-added event to both subscribers
            independently.
        """
        # Arrange
        events_1: list = []
        events_2: list = []
        received_1 = asyncio.Event()
        received_2 = asyncio.Event()
        started_1 = asyncio.Event()
        started_2 = asyncio.Event()

        async def collect(subscriber, events, received, started):
            started.set()
            async for event in subscriber:
                events.append(event)
                received.set()
                break

        with LocalDiscovery(namespace) as discovery:
            publisher = discovery.publisher
            subscriber_1 = discovery.subscribe(poll_interval=0.05)
            subscriber_2 = discovery.subscribe(poll_interval=0.05)

            async with publisher:
                task_1 = asyncio.create_task(collect(subscriber_1, events_1, received_1, started_1))
                task_2 = asyncio.create_task(collect(subscriber_2, events_2, received_2, started_2))
                await asyncio.gather(started_1.wait(), started_2.wait())

                # Act
                await publisher.publish("worker-added", metadata)

                try:
                    await asyncio.wait_for(
                        asyncio.gather(received_1.wait(), received_2.wait()),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    pytest.fail("Both subscribers did not receive the event")
                finally:
                    for t in (task_1, task_2):
                        t.cancel()
                        try:
                            await t
                        except asyncio.CancelledError:
                            pass

        # Assert
        assert len(events_1) == 1
        assert events_1[0].type == "worker-added"
        assert events_1[0].metadata.uid == metadata.uid
        assert len(events_2) == 1
        assert events_2[0].type == "worker-added"
        assert events_2[0].metadata.uid == metadata.uid
