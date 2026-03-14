import asyncio
import os
import socket
import uuid
from types import MappingProxyType

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from zeroconf import ServiceInfo

from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.discovery.lan import LanDiscovery


@pytest.fixture
def metadata():
    """Provides sample WorkerMetadata for testing.

    Creates a WorkerMetadata instance with typical field values for use in
    tests that need a well-formed worker instance.
    """
    return WorkerMetadata(
        uid=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        address="127.0.0.1:50051",
        pid=12345,
        version="1.0.0",
        tags=frozenset(["test", "worker"]),
        extra=MappingProxyType({"key": "value"}),
    )


@pytest.fixture
def worker_factory():
    """Factory for creating multiple unique workers.

    Provides a factory function that creates WorkerMetadata instances with
    unique UIDs and customizable address/tags for tests requiring multiple
    distinct workers.
    """

    def _create_worker(
        address: str, tags: frozenset[str] | None = None
    ) -> WorkerMetadata:
        return WorkerMetadata(
            uid=uuid.uuid4(),
            address=address,
            pid=os.getpid(),
            version="1.0.0",
            tags=tags or frozenset(),
            extra=MappingProxyType({}),
        )

    return _create_worker


class TestLanDiscovery:
    """Tests for LanDiscovery class.

    Fully qualified name: wool.runtime.discovery.lan.LanDiscovery
    """

    def test___init___with_default_service_type(self):
        """Test LanDiscovery default service type.

        Given:
            No arguments
        When:
            LanDiscovery is instantiated
        Then:
            It should have service_type equal to
            "_wool._tcp.local.".
        """
        # Act
        discovery = LanDiscovery()

        # Assert
        assert discovery.service_type == "_wool._tcp.local."

    def test_publisher_with_default_instance(self):
        """Test publisher property returns Publisher instance.

        Given:
            A LanDiscovery instance
        When:
            Accessing publisher property
        Then:
            It should return a new Publisher instance.
        """
        # Arrange
        discovery = LanDiscovery()

        # Act
        publisher = discovery.publisher

        # Assert
        assert isinstance(publisher, LanDiscovery.Publisher)

    def test_subscriber_with_default_instance(self):
        """Test subscriber property returns Subscriber instance.

        Given:
            A LanDiscovery instance
        When:
            Accessing subscriber property
        Then:
            It should return a new Subscriber instance.
        """
        # Arrange
        discovery = LanDiscovery()

        # Act
        subscriber = discovery.subscriber

        # Assert
        assert isinstance(subscriber, LanDiscovery.Subscriber)

    @pytest.mark.asyncio
    async def test_subscribe_with_default_filter(self, worker_factory):
        """Test subscribe() propagates the constructor's default filter.

        Given:
            A LanDiscovery with a default filter
        When:
            subscribe() is called without a filter
        Then:
            It should use the default filter for the event stream.
        """

        # Arrange
        def predicate(w):
            return w.address.endswith(":50051")

        discovery = LanDiscovery(filter=predicate)
        publisher = LanDiscovery.Publisher()
        subscriber = discovery.subscribe()

        worker_match = worker_factory(
            address="127.0.0.1:50051", tags=frozenset(["match"])
        )
        worker_no_match = worker_factory(
            address="127.0.0.1:9999", tags=frozenset(["no-match"])
        )

        events = []
        event_received = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                event_received.set()
                await asyncio.sleep(0.3)
                break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", worker_match)
            await publisher.publish("worker-added", worker_no_match)

            try:
                await asyncio.wait_for(event_received.wait(), timeout=2.0)
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
        assert all(e.metadata.address.endswith(":50051") for e in events)

    @pytest.mark.asyncio
    async def test_subscribe_with_explicit_filter(self, worker_factory):
        """Test subscribe(filter=predicate) uses the provided filter.

        Given:
            A LanDiscovery instance
        When:
            subscribe(filter=predicate) is called
        Then:
            It should use the provided filter for the event stream.
        """

        # Arrange
        def predicate(w):
            return w.address.endswith(":50051")

        discovery = LanDiscovery()
        publisher = LanDiscovery.Publisher()
        subscriber = discovery.subscribe(filter=predicate)

        worker_match = worker_factory(
            address="127.0.0.1:50051", tags=frozenset(["match"])
        )
        worker_no_match = worker_factory(
            address="127.0.0.1:9999", tags=frozenset(["no-match"])
        )

        events = []
        event_received = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                event_received.set()
                await asyncio.sleep(0.3)
                break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", worker_match)
            await publisher.publish("worker-added", worker_no_match)

            try:
                await asyncio.wait_for(event_received.wait(), timeout=2.0)
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
        assert all(e.metadata.address.endswith(":50051") for e in events)


class TestLanDiscoveryPublisher:
    """Tests for LanDiscovery.Publisher class.

    Fully qualified name:
    wool.runtime.discovery.lan.LanDiscovery.Publisher
    """

    def test_aiozc_with_uninitialized_publisher(self):
        """Test aiozc is None before entering async context.

        Given:
            A Publisher instance
        When:
            aiozc attribute is accessed before entering context
        Then:
            It should be None.
        """
        # Act
        publisher = LanDiscovery.Publisher()

        # Assert
        assert publisher.aiozc is None

    @pytest.mark.asyncio
    async def test___aenter___and___aexit___lifecycle(self):
        """Test Publisher async context manager lifecycle.

        Given:
            A Publisher instance
        When:
            Used as an async context manager via async with
        Then:
            It should initialize aiozc on entry and set to None on
            exit.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act & assert
        async with publisher:
            assert publisher.aiozc is not None

        assert publisher.aiozc is None

    @pytest.mark.asyncio
    async def test_publish_worker_added(self, metadata):
        """Test publish("worker-added") registers service.

        Given:
            An initialized Publisher
        When:
            publish("worker-added", metadata) is called
        Then:
            It should register the worker as a DNS-SD service.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)

            # Assert
            service_name = f"{metadata.uid}._wool._tcp.local."
            assert publisher.aiozc is not None
            service_info = await publisher.aiozc.async_get_service_info(
                "_wool._tcp.local.",
                service_name,
            )
            assert service_info is not None
            assert service_info.port == int(metadata.address.split(":")[1])

    @pytest.mark.asyncio
    async def test_publish_worker_dropped(self, metadata):
        """Test publish("worker-dropped") unregisters service.

        Given:
            A published worker
        When:
            publish("worker-dropped", metadata) is called
        Then:
            It should unregister the worker service.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)
            assert str(metadata.uid) in publisher.services

            await publisher.publish("worker-dropped", metadata)

            # Assert
            assert str(metadata.uid) not in publisher.services

    @pytest.mark.asyncio
    async def test_publish_worker_updated(self, metadata):
        """Test publish("worker-updated") updates service properties.

        Given:
            A published worker
        When:
            publish("worker-updated", updated_metadata) is called
        Then:
            It should update the worker service properties.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        updated_worker = WorkerMetadata(
            uid=metadata.uid,
            address=metadata.address,
            pid=metadata.pid,
            version="2.0.0",
            tags=frozenset(["updated"]),
            extra=MappingProxyType({"new": "data"}),
        )

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)
            await publisher.publish("worker-updated", updated_worker)

            # Assert
            service_info = publisher.services[str(metadata.uid)]
            properties = service_info.decoded_properties
            assert properties["version"] == "2.0.0"

    @pytest.mark.asyncio
    async def test_publish_with_uninitialized_publisher(self, metadata):
        """Test publish() when publisher not initialized.

        Given:
            A Publisher not inside an async context
        When:
            publish("worker-added", metadata) is called
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act & assert
        with pytest.raises(RuntimeError, match="not properly initialized"):
            await publisher.publish("worker-added", metadata)

    @pytest.mark.asyncio
    async def test_publish_with_invalid_event_type(self, metadata):
        """Test publish() with unexpected event type.

        Given:
            An initialized Publisher
        When:
            publish("invalid-type", metadata) is called
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

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

    @pytest.mark.asyncio
    async def test_services_with_published_worker(self, metadata):
        """Test services attribute tracks published workers.

        Given:
            A Publisher with a published worker
        When:
            services attribute is accessed
        Then:
            It should contain the published worker's ServiceInfo
            keyed by UID string.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)

            # Assert
            assert str(metadata.uid) in publisher.services
            service_info = publisher.services[str(metadata.uid)]
            assert service_info is not None
            assert b"pid" in service_info.properties


class TestLanDiscoverySubscriber:
    """Tests for LanDiscovery.Subscriber class.

    Fully qualified name:
    wool.runtime.discovery.lan.LanDiscovery.Subscriber
    """

    def test_service_type_with_default_value(self):
        """Test Subscriber service_type constant.

        Given:
            A Subscriber instance
        When:
            service_type is accessed
        Then:
            It should be "_wool._tcp.local.".
        """
        # Act
        subscriber = LanDiscovery.Subscriber()

        # Assert
        assert subscriber.service_type == "_wool._tcp.local."

    @pytest.mark.asyncio
    async def test___aiter___discovers_added_worker(self, metadata):
        """Test async for yields worker-added event.

        Given:
            A Publisher that has registered a worker
        When:
            Subscriber is iterated via async for
        Then:
            It should yield a worker-added event with matching
            metadata.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        events = []
        worker_discovered = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == metadata.uid:
                    worker_discovered.set()
                    break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", metadata)

            try:
                await asyncio.wait_for(worker_discovered.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Worker not discovered within timeout")
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test___aiter___detects_dropped_worker(self, metadata):
        """Test async for yields worker-dropped event.

        Given:
            A published then dropped worker
        When:
            Subscriber is iterated via async for
        Then:
            It should yield a worker-dropped event.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        events = []
        worker_dropped = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-dropped" and event.metadata.uid == metadata.uid:
                    worker_dropped.set()
                    break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", metadata)
            await asyncio.sleep(0.2)

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
    async def test___aiter___detects_updated_worker(self, metadata):
        """Test async for yields worker-updated event.

        Given:
            A published then updated worker
        When:
            Subscriber is iterated via async for
        Then:
            It should yield a worker-updated event with new metadata.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        updated_worker = WorkerMetadata(
            uid=metadata.uid,
            address=metadata.address,
            pid=metadata.pid,
            version="2.0.0",
            tags=frozenset(["updated"]),
        )

        events = []
        worker_updated = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-updated" and event.metadata.uid == metadata.uid:
                    worker_updated.set()
                    break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", metadata)
            await asyncio.sleep(0.2)

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
        updated = [e for e in events if e.type == "worker-updated"]
        assert len(updated) >= 1
        assert updated[0].metadata.version == "2.0.0"

    @pytest.mark.asyncio
    async def test___aiter___with_filter_predicate(self, worker_factory):
        """Test subscriber filtering with predicate.

        Given:
            A subscriber with a filter predicate
        When:
            Workers matching and not matching the filter are published
        Then:
            It should yield only matching workers.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        def filter_fn(w):
            return w.address.endswith(":50051")

        subscriber = LanDiscovery.Subscriber(filter=filter_fn)

        worker_match = worker_factory(
            address="127.0.0.1:50051", tags=frozenset(["match"])
        )
        worker_no_match = worker_factory(
            address="127.0.0.1:9999", tags=frozenset(["no-match"])
        )

        events = []
        event_received = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                event_received.set()
                await asyncio.sleep(0.3)
                break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", worker_match)
            await publisher.publish("worker-added", worker_no_match)

            try:
                await asyncio.wait_for(event_received.wait(), timeout=2.0)
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
        assert all(e.metadata.address.endswith(":50051") for e in events)
        assert any(e.metadata.uid == worker_match.uid for e in events)

    @pytest.mark.asyncio
    async def test___aiter___with_independent_iterators(self, metadata):
        """Test two subscriber iterators have isolated state.

        Given:
            Two independent subscriber iterators from the same
            instance
        When:
            Workers are published
        Then:
            Each iterator should receive events independently.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber1 = LanDiscovery.Subscriber()
        subscriber2 = LanDiscovery.Subscriber()

        events1 = []
        events2 = []
        both_discovered = asyncio.Event()

        async def collect1():
            async for event in subscriber1:
                events1.append(event)
                if len(events1) >= 1 and len(events2) >= 1:
                    both_discovered.set()
                    break

        async def collect2():
            async for event in subscriber2:
                events2.append(event)
                if len(events1) >= 1 and len(events2) >= 1:
                    both_discovered.set()
                    break

        # Act
        async with publisher:
            task1 = asyncio.create_task(collect1())
            task2 = asyncio.create_task(collect2())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", metadata)

            try:
                await asyncio.wait_for(both_discovered.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            finally:
                task1.cancel()
                task2.cancel()
                try:
                    await asyncio.gather(task1, task2, return_exceptions=True)
                except Exception:
                    pass

        # Assert
        assert len(events1) >= 1
        assert len(events2) >= 1

    @pytest.mark.asyncio
    async def test___aiter___with_dynamic_filter_transition(self, worker_factory):
        """Test worker transitions from matching to non-matching filter.

        Given:
            A subscriber tracking a worker that updates to fail the
            filter
        When:
            Worker properties change so the filter rejects it
        Then:
            It should yield a worker-dropped event for the
            now-filtered worker.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        def filter_fn(w):
            return "gpu" in w.tags

        subscriber = LanDiscovery.Subscriber(filter=filter_fn)

        worker = worker_factory(address="127.0.0.1:50051", tags=frozenset(["gpu"]))

        events = []
        dropped_event = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-dropped":
                    dropped_event.set()
                    break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", worker)
            await asyncio.sleep(0.2)

            updated_worker = WorkerMetadata(
                uid=worker.uid,
                address=worker.address,
                pid=worker.pid,
                version=worker.version,
                tags=frozenset(["cpu"]),
                extra=worker.extra,
            )
            await publisher.publish("worker-updated", updated_worker)

            try:
                await asyncio.wait_for(dropped_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        dropped = [e for e in events if e.type == "worker-dropped"]
        assert len(dropped) >= 1

    @pytest.mark.asyncio
    async def test___aiter___with_untracked_worker_promotion(self, worker_factory):
        """Test untracked worker promotion via update event.

        Given:
            A subscriber with a filter that rejects the initial
            worker
        When:
            The worker is updated to pass the filter
        Then:
            It should yield a worker-added event for the promoted
            worker.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        def filter_fn(w):
            return "gpu" in w.tags

        subscriber = LanDiscovery.Subscriber(filter=filter_fn)

        worker = worker_factory(address="127.0.0.1:50051", tags=frozenset(["cpu"]))

        events = []
        event_received = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == worker.uid:
                    event_received.set()
                    break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            # Add worker that fails filter (no "gpu" tag)
            await publisher.publish("worker-added", worker)
            await asyncio.sleep(0.2)

            # Update worker to pass filter (add "gpu" tag)
            updated_worker = WorkerMetadata(
                uid=worker.uid,
                address=worker.address,
                pid=worker.pid,
                version=worker.version,
                tags=frozenset(["gpu"]),
                extra=worker.extra,
            )
            await publisher.publish("worker-updated", updated_worker)

            try:
                await asyncio.wait_for(event_received.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Untracked worker promotion not detected within timeout")
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        assert events[0].type == "worker-added"
        assert "gpu" in events[0].metadata.tags

    @pytest.mark.asyncio
    async def test___aiter___end_to_end_publish_discover(self, metadata):
        """Test end-to-end publish-discover flow.

        Given:
            A Publisher and Subscriber on localhost
        When:
            Worker is published via Publisher
        Then:
            Subscriber should discover the worker and yield an event
            with matching metadata.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        events = []
        worker_discovered = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == metadata.uid:
                    worker_discovered.set()
                    break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", metadata)

            try:
                await asyncio.wait_for(worker_discovered.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Worker not discovered within timeout")
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        discovered = events[0]
        assert discovered.type == "worker-added"
        assert discovered.metadata.uid == metadata.uid
        assert discovered.metadata.address == metadata.address
        assert discovered.metadata.pid == metadata.pid

    @given(
        address=st.one_of(
            st.builds(
                lambda p: f"127.0.0.1:{p}",
                st.integers(min_value=1, max_value=65535),
            ),
            st.builds(
                lambda p: f"localhost:{p}",
                st.integers(min_value=1, max_value=65535),
            ),
        ),
        pid=st.integers(min_value=1, max_value=2147483647),
        version=st.text(
            min_size=1,
            max_size=20,
            alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-_",
        ),
        tags=st.frozensets(
            st.text(
                min_size=1,
                max_size=20,
                alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_",
            ),
            max_size=5,
        ),
    )
    @settings(max_examples=10, deadline=10000)
    @pytest.mark.asyncio
    async def test_publish_roundtrip_with_arbitrary_metadata(
        self, address, pid, version, tags
    ):
        """Test publish-discover roundtrip with arbitrary metadata.

        Given:
            Arbitrary valid WorkerMetadata with DNS-SD-safe field
            sizes
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
            tags=tags,
            extra=MappingProxyType({}),
        )
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        events = []
        discovered = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == worker.uid:
                    discovered.set()
                    break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", worker)

            try:
                await asyncio.wait_for(discovered.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pytest.fail("Worker not discovered within timeout")
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        event = events[0]
        assert event.metadata.uid == worker.uid
        assert event.metadata.pid == worker.pid
        assert event.metadata.version == worker.version
        assert event.metadata.tags == worker.tags
        expected_port = int(address.split(":")[1])
        assert event.metadata.address == f"127.0.0.1:{expected_port}"

    @pytest.mark.asyncio
    async def test___aiter___ignores_malformed_zeroconf_service(self, metadata):
        """Test subscriber ignores malformed Zeroconf services.

        Given:
            A malformed Zeroconf service with missing required
            properties and a valid worker published normally
        When:
            Subscriber iterates events
        Then:
            It should only yield the valid worker's event.
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        malformed_uid = uuid.uuid4()
        malformed_name = f"{malformed_uid}._wool._tcp.local."
        malformed_service = ServiceInfo(
            "_wool._tcp.local.",
            malformed_name,
            addresses=[socket.inet_aton("127.0.0.1")],
            port=9999,
            properties={"some_key": "some_value"},
        )

        events = []
        worker_discovered = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == metadata.uid:
                    worker_discovered.set()
                    break

        # Act
        async with publisher:
            assert publisher.aiozc is not None
            await publisher.aiozc.async_register_service(malformed_service)

            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", metadata)

            try:
                await asyncio.wait_for(worker_discovered.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Valid worker not discovered within timeout")
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            await publisher.aiozc.async_unregister_service(malformed_service)

        # Assert
        assert len(events) >= 1
        assert all(e.metadata.uid == metadata.uid for e in events)
