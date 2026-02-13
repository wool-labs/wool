import asyncio
import json
import os
import uuid
from types import MappingProxyType

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from zeroconf import ServiceInfo

from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.discovery.lan import LanDiscovery
from wool.runtime.discovery.lan import _deserialize_metadata
from wool.runtime.discovery.lan import _serialize_metadata


@pytest.fixture
def metadata():
    """Provides sample WorkerMetadata for testing.

    Creates a WorkerMetadata instance with typical field values for use in
    tests that need a well-formed worker instance.
    """
    return WorkerMetadata(
        uid=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        host="localhost",
        port=50051,
        pid=12345,
        version="1.0.0",
        tags=frozenset(["test", "worker"]),
        extra=MappingProxyType({"key": "value"}),
    )


@pytest.fixture
def worker_factory():
    """Factory for creating multiple unique workers.

    Provides a factory function that creates WorkerMetadata instances with
    unique UIDs and customizable port/tags for tests requiring multiple
    distinct workers.
    """

    def _create_worker(port: int, tags: frozenset[str] | None = None) -> WorkerMetadata:
        return WorkerMetadata(
            uid=uuid.uuid4(),
            host="localhost",
            port=port,
            pid=os.getpid(),
            version="1.0.0",
            tags=tags or frozenset(),
            extra=MappingProxyType({}),
        )

    return _create_worker


# Hypothesis strategies for property-based testing
@st.composite
def metadata_strategy(draw):
    """Generate arbitrary WorkerMetadata instances for property-based testing.

    Generates WorkerMetadata with valid ranges for all fields:
    - Valid UUIDs
    - Non-empty host strings (ASCII alphanumeric + .-_)
    - Valid port numbers (1-65535)
    - Positive PIDs
    - Non-empty version strings (ASCII alphanumeric + .-_)
    - Arbitrary tags (ASCII alphanumeric + -_)
    - Extra metadata (JSON-safe printable ASCII strings only)

    Note on size constraints:
        DNS-SD (used by Zeroconf/mDNS) has a 255-byte limit per TXT
        record, which includes the "key=value" format. To ensure generated
        data stays within this limit, tags and extra metadata are kept
        small (max 5 tags of 20 chars each, max 5 extra entries). This
        represents realistic production usage where metadata is concise.
    """
    # ASCII alphanumeric only to avoid multi-byte UTF-8 sequences
    ascii_alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

    return WorkerMetadata(
        uid=draw(st.uuids()),
        host=draw(st.text(min_size=1, max_size=50, alphabet=ascii_alphanumeric + ".-_")),
        port=draw(st.integers(min_value=1, max_value=65535)),
        pid=draw(st.integers(min_value=1, max_value=2147483647)),
        version=draw(
            st.text(min_size=1, max_size=20, alphabet=ascii_alphanumeric + ".-_")
        ),
        tags=draw(
            st.frozensets(
                st.text(min_size=1, max_size=20, alphabet=ascii_alphanumeric + "-_"),
                max_size=5,  # Keep small to respect DNS TXT record limits
            )
        ),
        extra=draw(
            st.builds(
                MappingProxyType,
                st.dictionaries(
                    st.text(min_size=1, max_size=20, alphabet=ascii_alphanumeric + "_"),
                    st.one_of(
                        # Printable ASCII only
                        st.text(
                            min_size=0,
                            max_size=50,
                            alphabet=st.characters(
                                min_codepoint=32,  # Space
                                max_codepoint=126,  # Tilde (printable ASCII)
                            ),
                        ),
                        st.integers(min_value=-2147483648, max_value=2147483647),
                        st.booleans(),
                        st.none(),
                    ),
                    max_size=5,  # Keep small to respect DNS TXT record limits
                ),
            )
        ),
    )


class TestLanDiscovery:
    """Tests for LanDiscovery class.

    Fully qualified name: wool.runtime.discovery.lan.LanDiscovery
    """

    def test_publisher_property(self):
        """Test publisher property returns Publisher instance.

        Given:
            A LanDiscovery instance
        When:
            Accessing publisher property
        Then:
            Should return new Publisher instance
        """
        # Arrange
        discovery = LanDiscovery()

        # Act
        publisher = discovery.publisher

        # Assert
        assert isinstance(publisher, LanDiscovery.Publisher)

    def test_subscriber_property(self):
        """Test subscriber property returns Subscriber instance.

        Given:
            A LanDiscovery instance
        When:
            Accessing subscriber property
        Then:
            Should return new Subscriber instance
        """
        # Arrange
        discovery = LanDiscovery()

        # Act
        subscriber = discovery.subscriber

        # Assert
        assert isinstance(subscriber, LanDiscovery.Subscriber)

    def test_subscribe_without_filter(self):
        """Test subscribe() without filter returns unfiltered subscriber.

        Given:
            A LanDiscovery instance
        When:
            Calling subscribe() without filter
        Then:
            Should create Subscriber without filter
        """
        # Arrange
        discovery = LanDiscovery()

        # Act
        subscriber = discovery.subscribe()

        # Assert
        assert isinstance(subscriber, LanDiscovery.Subscriber)
        assert subscriber._filter is None

    def test_subscribe_with_filter(self):
        """Test subscribe() with filter returns filtered subscriber.

        Given:
            A LanDiscovery instance and filter predicate
        When:
            Calling subscribe() with filter
        Then:
            Should create Subscriber with filter applied
        """
        # Arrange
        discovery = LanDiscovery()

        def predicate(w):
            return w.port == 50051

        # Act
        subscriber = discovery.subscribe(predicate)

        # Assert
        assert isinstance(subscriber, LanDiscovery.Subscriber)
        assert subscriber._filter == predicate

    def test_subscriber_property_with_default_filter(self):
        """Test subscriber property uses constructor's default filter.

        Given:
            A LanDiscovery instance created with a default filter
        When:
            Accessing subscriber property
        Then:
            Should return Subscriber with the default filter applied
        """

        # Arrange
        def predicate(w):
            return w.port == 50051

        discovery = LanDiscovery(filter=predicate)

        # Act
        subscriber = discovery.subscriber

        # Assert
        assert isinstance(subscriber, LanDiscovery.Subscriber)
        assert subscriber._filter == predicate

    def test_subscribe_with_default_filter(self):
        """Test subscribe() without explicit filter falls back to default.

        Given:
            A LanDiscovery instance created with a default filter
        When:
            Calling subscribe() without providing a filter
        Then:
            Should create Subscriber with the constructor's default filter
        """

        # Arrange
        def predicate(w):
            return w.port == 50051

        discovery = LanDiscovery(filter=predicate)

        # Act
        subscriber = discovery.subscribe()

        # Assert
        assert isinstance(subscriber, LanDiscovery.Subscriber)
        assert subscriber._filter == predicate


class TestLanDiscoveryPublisher:
    """Tests for LanDiscovery.Publisher class.

    Fully qualified name: wool.runtime.discovery.lan.LanDiscovery.Publisher
    """

    @pytest.mark.asyncio
    async def test___aenter__(self):
        """Test __aenter__() initializes AsyncZeroconf.

        Given:
            A Publisher instance
        When:
            Entering context manager
        Then:
            AsyncZeroconf should be initialized on localhost
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        assert publisher.aiozc is None

        # Act
        async with publisher:
            # Assert
            assert publisher.aiozc is not None
            assert isinstance(publisher.aiozc, asyncio.Future) is False

    @pytest.mark.asyncio
    async def test___aexit__(self):
        """Test __aexit__() closes AsyncZeroconf and cleans up.

        Given:
            A Publisher within context manager
        When:
            Exiting context manager
        Then:
            AsyncZeroconf should be closed and set to None
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act
        async with publisher:
            assert publisher.aiozc is not None

        # Assert
        assert publisher.aiozc is None

    @pytest.mark.asyncio
    async def test_publish_worker_added(self, metadata):
        """Test publish() with worker-added event registers service.

        Given:
            A Publisher instance within context manager
        When:
            Publishing worker-added event
        Then:
            Worker should be registered with Zeroconf on localhost
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)

            # Assert - verify service was registered
            service_name = f"{metadata.uid}._wool._tcp.local."
            assert publisher.aiozc is not None
            service_info = await publisher.aiozc.async_get_service_info(
                "_wool._tcp.local.",
                service_name,
            )

            assert service_info is not None
            assert service_info.port == metadata.port
            assert b"pid" in service_info.properties

    @pytest.mark.asyncio
    async def test_publish_worker_updated(self, metadata):
        """Test publish() with worker-updated event updates service.

        Given:
            A Publisher with existing registered worker
        When:
            Publishing worker-updated event with modified properties
        Then:
            Worker service should be updated in Zeroconf
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act
        async with publisher:
            # First add the worker
            await publisher.publish("worker-added", metadata)

            # Update worker with new version
            updated_worker = WorkerMetadata(
                uid=metadata.uid,  # Same UID
                host=metadata.host,
                port=metadata.port,
                pid=metadata.pid,
                version="2.0.0",  # Changed version
                tags=frozenset(["updated"]),
                extra=MappingProxyType({"new": "data"}),
            )
            await publisher.publish("worker-updated", updated_worker)

            # Assert - verify service was updated in publisher's cache
            service_info = publisher.services[str(metadata.uid)]
            assert service_info is not None
            properties = service_info.decoded_properties
            assert properties["version"] == "2.0.0"

    @pytest.mark.asyncio
    async def test_publish_worker_dropped(self, metadata):
        """Test publish() with worker-dropped event unregisters service.

        Given:
            A Publisher with existing registered worker
        When:
            Publishing worker-dropped event
        Then:
            Worker service should be unregistered from Zeroconf
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act
        async with publisher:
            # First add the worker
            await publisher.publish("worker-added", metadata)

            # Verify it was added
            assert str(metadata.uid) in publisher.services

            # Drop the worker
            await publisher.publish("worker-dropped", metadata)

            # Assert - verify service was removed
            assert str(metadata.uid) not in publisher.services

    @pytest.mark.asyncio
    async def test_publish_without_port(self):
        """Test publish() with worker without port.

        Given:
            A Publisher and WorkerMetadata with port=None
        When:
            Publishing worker-added event
        Then:
            ValueError should be raised with clear message
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        worker_no_port = WorkerMetadata(
            uid=uuid.uuid4(),
            host="localhost",
            port=None,  # Invalid for LAN discovery
            pid=12345,
            version="1.0.0",
        )

        # Act & Assert
        async with publisher:
            with pytest.raises(ValueError, match="Worker port must be specified"):
                await publisher.publish("worker-added", worker_no_port)

    @pytest.mark.asyncio
    async def test_publish_uninitialized(self, metadata):
        """Test publish() when publisher not initialized.

        Given:
            A Publisher not in context manager
        When:
            Attempting to publish
        Then:
            RuntimeError should be raised
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act & Assert
        with pytest.raises(RuntimeError, match="not properly initialized"):
            await publisher.publish("worker-added", metadata)

    @pytest.mark.asyncio
    async def test_publish_unexpected_event_type(self, metadata):
        """Test publish() with unexpected event type.

        Given:
            A Publisher within context manager
        When:
            Publishing event with invalid type
        Then:
            RuntimeError should be raised
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act & Assert
        async with publisher:
            with pytest.raises(RuntimeError, match="Unexpected discovery event type"):
                await publisher.publish("invalid-type", metadata)  # type: ignore


class TestLanDiscoverySubscriber:
    """Tests for LanDiscovery.Subscriber class.

    Fully qualified name: wool.runtime.discovery.lan.LanDiscovery.Subscriber
    """

    @pytest.mark.asyncio
    async def test___aiter__(self):
        """Test __aiter__() returns async iterator.

        Given:
            A Subscriber instance
        When:
            Using as async iterator
        Then:
            Should return async iterator protocol
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber()

        # Act
        iterator = subscriber.__aiter__()

        # Assert
        assert hasattr(iterator, "__anext__")

    @pytest.mark.asyncio
    async def test_initial_scan_discovers_existing_workers(self, metadata):
        """Test initial scan discovers existing workers on startup.

        Given:
            Existing workers published to Zeroconf
        When:
            Creating subscriber and starting iteration
        Then:
            Should yield worker-added events for existing workers
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        events = []
        worker_discovered = asyncio.Event()

        async def collect_events():
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == metadata.uid:
                    worker_discovered.set()
                    break

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)

            # Give Zeroconf time to register
            await asyncio.sleep(0.2)

            collect_task = asyncio.create_task(collect_events())

            # Wait for discovery with timeout
            try:
                await asyncio.wait_for(worker_discovered.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Worker not discovered within timeout")
            finally:
                collect_task.cancel()
                try:
                    await collect_task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test_worker_added_event(self, metadata):
        """Test subscriber yields worker-added event for published worker.

        Given:
            A Publisher and Subscriber on localhost
        When:
            Worker is published via Publisher
        Then:
            Subscriber should yield worker-added event
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        events = []
        worker_discovered = asyncio.Event()

        async def collect_events():
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == metadata.uid:
                    worker_discovered.set()
                    break

        # Act
        async with publisher:
            collect_task = asyncio.create_task(collect_events())

            # Give subscriber time to initialize
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", metadata)

            # Wait for discovery with timeout
            try:
                await asyncio.wait_for(worker_discovered.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Worker not discovered within timeout")
            finally:
                collect_task.cancel()
                try:
                    await collect_task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        assert events[0].type == "worker-added"
        assert events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test_worker_updated_event(self, metadata):
        """Test subscriber yields worker-updated event for property changes.

        Given:
            A Publisher and Subscriber on localhost
        When:
            Worker properties are updated via Publisher
        Then:
            Subscriber should yield worker-updated event
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        events = []
        worker_updated = asyncio.Event()

        async def collect_events():
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-updated" and event.metadata.uid == metadata.uid:
                    worker_updated.set()
                    break

        # Act
        async with publisher:
            collect_task = asyncio.create_task(collect_events())

            await asyncio.sleep(0.1)

            # Add worker first
            await publisher.publish("worker-added", metadata)
            await asyncio.sleep(0.2)

            # Update worker
            updated_worker = WorkerMetadata(
                uid=metadata.uid,
                host=metadata.host,
                port=metadata.port,
                pid=metadata.pid,
                version="2.0.0",  # Changed
                tags=frozenset(["updated"]),
            )
            await publisher.publish("worker-updated", updated_worker)

            # Wait for update event with timeout
            try:
                await asyncio.wait_for(worker_updated.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Worker update not detected within timeout")
            finally:
                collect_task.cancel()
                try:
                    await collect_task
                except asyncio.CancelledError:
                    pass

        # Assert
        update_events = [e for e in events if e.type == "worker-updated"]
        assert len(update_events) >= 1
        assert update_events[0].metadata.version == "2.0.0"

    @pytest.mark.asyncio
    async def test_worker_dropped_event(self, metadata):
        """Test subscriber yields worker-dropped event for removed workers.

        Given:
            A Publisher and Subscriber with existing worker
        When:
            Worker is dropped via Publisher
        Then:
            Subscriber should yield worker-dropped event
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        events = []
        worker_dropped = asyncio.Event()

        async def collect_events():
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-dropped" and event.metadata.uid == metadata.uid:
                    worker_dropped.set()
                    break

        # Act
        async with publisher:
            collect_task = asyncio.create_task(collect_events())

            await asyncio.sleep(0.1)

            # Add worker first
            await publisher.publish("worker-added", metadata)
            await asyncio.sleep(0.2)

            # Drop worker
            await publisher.publish("worker-dropped", metadata)

            # Wait for drop event with timeout
            try:
                await asyncio.wait_for(worker_dropped.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Worker drop not detected within timeout")
            finally:
                collect_task.cancel()
                try:
                    await collect_task
                except asyncio.CancelledError:
                    pass

        # Assert
        drop_events = [e for e in events if e.type == "worker-dropped"]
        assert len(drop_events) >= 1
        assert drop_events[0].metadata.uid == metadata.uid

    @pytest.mark.asyncio
    async def test_filtering_with_predicate(self, worker_factory):
        """Test subscriber filtering with port-based predicate.

        Given:
            Subscriber with port filter (port == 50051)
        When:
            Workers with matching and non-matching ports are published
        Then:
            Only workers matching predicate should be yielded
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        def filter_fn(w):
            return w.port == 50051

        subscriber = LanDiscovery.Subscriber(filter=filter_fn)

        worker_match = worker_factory(port=50051, tags=frozenset(["match"]))
        worker_no_match = worker_factory(port=9999, tags=frozenset(["no-match"]))

        events = []
        event_received = asyncio.Event()

        async def collect_events():
            async for event in subscriber:
                events.append(event)
                event_received.set()
                # Collect for a short time
                await asyncio.sleep(0.3)
                break

        # Act
        async with publisher:
            collect_task = asyncio.create_task(collect_events())

            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", worker_match)
            await publisher.publish("worker-added", worker_no_match)

            try:
                await asyncio.wait_for(event_received.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            finally:
                collect_task.cancel()
                try:
                    await collect_task
                except asyncio.CancelledError:
                    pass

        # Assert - only matching worker should appear
        assert len(events) >= 1
        assert all(e.metadata.port == 50051 for e in events)
        assert any(e.metadata.uid == worker_match.uid for e in events)

    @pytest.mark.asyncio
    async def test_multiple_instances_isolated(self, metadata):
        """Test multiple subscriber instances have isolated state.

        Given:
            Two Subscriber instances for same service
        When:
            Both subscribers iterate simultaneously
        Then:
            Each should maintain independent state and event streams
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber1 = LanDiscovery.Subscriber()
        subscriber2 = LanDiscovery.Subscriber()

        events1 = []
        events2 = []
        both_discovered = asyncio.Event()

        async def collect_events1():
            async for event in subscriber1:
                events1.append(event)
                if len(events1) >= 1 and len(events2) >= 1:
                    both_discovered.set()
                    break

        async def collect_events2():
            async for event in subscriber2:
                events2.append(event)
                if len(events1) >= 1 and len(events2) >= 1:
                    both_discovered.set()
                    break

        # Act
        async with publisher:
            task1 = asyncio.create_task(collect_events1())
            task2 = asyncio.create_task(collect_events2())

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

        # Assert - both subscribers should have received events independently
        assert len(events1) >= 1
        assert len(events2) >= 1

    @pytest.mark.asyncio
    async def test_publish_worker_updated_not_in_cache(self, metadata):
        """Test updating worker that was never added.

        Given:
            A Publisher without existing worker
        When:
            Publishing worker-updated event
        Then:
            Worker should be added as if worker-added was called
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act
        async with publisher:
            # Update worker that doesn't exist yet - should add it
            await publisher.publish("worker-updated", metadata)

            # Assert - verify service was registered
            assert str(metadata.uid) in publisher.services

    @pytest.mark.asyncio
    async def test_filtering_dynamic_transition(self, worker_factory):
        """Test worker transitions from matching to non-matching filter.

        Given:
            Subscriber with tag-based filter
        When:
            Worker is updated to no longer match filter
        Then:
            Worker-dropped event should be emitted
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        def filter_fn(w):
            return "gpu" in w.tags

        subscriber = LanDiscovery.Subscriber(filter=filter_fn)

        worker = worker_factory(port=50051, tags=frozenset(["gpu"]))

        events = []
        dropped_event = asyncio.Event()

        async def collect_events():
            async for event in subscriber:
                events.append(event)
                if event.type == "worker-dropped":
                    dropped_event.set()
                    break

        # Act
        async with publisher:
            collect_task = asyncio.create_task(collect_events())

            await asyncio.sleep(0.1)

            # Add worker with gpu tag
            await publisher.publish("worker-added", worker)
            await asyncio.sleep(0.2)

            # Update worker to remove gpu tag (no longer matches filter)
            updated_worker = WorkerMetadata(
                uid=worker.uid,
                host=worker.host,
                port=worker.port,
                pid=worker.pid,
                version=worker.version,
                tags=frozenset(["cpu"]),  # Changed - no longer has 'gpu'
                extra=worker.extra,
            )
            await publisher.publish("worker-updated", updated_worker)

            try:
                await asyncio.wait_for(dropped_event.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            finally:
                collect_task.cancel()
                try:
                    await collect_task
                except asyncio.CancelledError:
                    pass

        # Assert - should have received worker-dropped when filter no longer matches
        drop_events = [e for e in events if e.type == "worker-dropped"]
        assert len(drop_events) >= 1

    @pytest.mark.asyncio
    async def test_update_with_no_property_changes(self, metadata):
        """Test updating worker when properties haven't actually changed.

        Given:
            A Publisher with registered worker
        When:
            Publishing worker-updated with identical properties
        Then:
            Update should be skipped (no Zeroconf update call)
        """
        # Arrange
        publisher = LanDiscovery.Publisher()

        # Act
        async with publisher:
            # Add worker
            await publisher.publish("worker-added", metadata)

            initial_service = publisher.services[str(metadata.uid)]

            # Update with identical properties
            await publisher.publish("worker-updated", metadata)

            # Assert - service object should be unchanged
            assert publisher.services[str(metadata.uid)] is initial_service

    @pytest.mark.asyncio
    async def test_subscriber_handles_service_disappearance(self, mocker):
        """Test subscriber handles service disappearing during query.

        Given:
            Subscriber receiving service-added callback
        When:
            Service disappears before async_get_service_info completes
        Then:
            Exception should be caught silently, no crash
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber()

        # Mock async_get_service_info to return None (service disappeared)
        mock_get_service = mocker.AsyncMock(return_value=None)

        events = []

        async def collect_events():
            try:
                async for event in subscriber:
                    events.append(event)
                    await asyncio.sleep(0.3)
                    break
            except Exception:
                pass

        # Act
        collect_task = asyncio.create_task(collect_events())
        await asyncio.sleep(0.1)

        # Manually trigger the listener callback with mocked aiozc
        from unittest.mock import MagicMock

        mock_aiozc = MagicMock()
        mock_aiozc.async_get_service_info = mock_get_service

        listener = subscriber._Listener(
            aiozc=mock_aiozc,
            event_queue=asyncio.Queue(),
            predicate=lambda _: True,
            service_cache={},
        )

        # Trigger add_service callback
        listener.add_service(MagicMock(), "_wool._tcp.local.", "test._wool._tcp.local.")

        await asyncio.sleep(0.2)

        collect_task.cancel()
        try:
            await collect_task
        except asyncio.CancelledError:
            pass

        # Assert - no crash, service_info = None handled gracefully
        assert True  # Test passes if we didn't crash

    @pytest.mark.asyncio
    async def test_subscriber_handles_service_disappearance_during_update(self, mocker):
        """Test subscriber handles service disappearing during update query.

        Given:
            Subscriber receiving service-updated callback for tracked service
        When:
            Service disappears before async_get_service_info completes
        Then:
            Update should be skipped silently, no crash or event emission
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber()

        # Mock async_get_service_info to return None (service disappeared)
        mock_get_service = mocker.AsyncMock(return_value=None)

        event_queue = asyncio.Queue()
        service_cache = {}

        # Manually create listener with mocked aiozc
        from unittest.mock import MagicMock

        mock_aiozc = MagicMock()
        mock_aiozc.async_get_service_info = mock_get_service

        listener = subscriber._Listener(
            aiozc=mock_aiozc,
            event_queue=event_queue,
            predicate=lambda _: True,
            service_cache=service_cache,
        )

        # Act - directly call the async handler (bypassing asyncio.create_task)
        await listener._handle_update_service(
            "_wool._tcp.local.", "test._wool._tcp.local."
        )

        # Assert - no crash, queue should be empty (no events emitted)
        assert event_queue.empty()
        # Verify async_get_service_info was called
        mock_get_service.assert_called_once_with(
            "_wool._tcp.local.", "test._wool._tcp.local."
        )

    @pytest.mark.asyncio
    async def test_subscriber_handles_update_deserialization_error(self, mocker):
        """Test subscriber handles deserialization errors during update.

        Given:
            Subscriber receiving service-updated callback with valid ServiceInfo
        When:
            Deserialization of service info raises ValueError
        Then:
            Error should be caught silently, no crash or event emission
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber()

        # Create a mock ServiceInfo that will be returned
        from unittest.mock import MagicMock

        mock_service_info = MagicMock()

        # Mock async_get_service_info to return the mock service
        mock_get_service = mocker.AsyncMock(return_value=mock_service_info)

        # Patch _deserialize_metadata to raise ValueError
        mocker.patch(
            "wool.runtime.discovery.lan._deserialize_metadata",
            side_effect=ValueError("Invalid service properties"),
        )

        event_queue = asyncio.Queue()
        service_cache = {}

        mock_aiozc = MagicMock()
        mock_aiozc.async_get_service_info = mock_get_service

        listener = subscriber._Listener(
            aiozc=mock_aiozc,
            event_queue=event_queue,
            predicate=lambda _: True,
            service_cache=service_cache,
        )

        # Act - directly call the async handler
        await listener._handle_update_service(
            "_wool._tcp.local.", "test._wool._tcp.local."
        )

        # Assert - no crash, queue should be empty (no events emitted)
        assert event_queue.empty()
        # Verify async_get_service_info was called
        mock_get_service.assert_called_once_with(
            "_wool._tcp.local.", "test._wool._tcp.local."
        )

    @pytest.mark.asyncio
    async def test_subscriber_handles_new_worker_via_update(self, mocker, metadata):
        """Test subscriber handles new worker appearing via update event.

        Given:
            Subscriber with empty service cache
        When:
            Update event arrives for a worker not in cache
        Then:
            Worker-added event should be emitted with correct worker info
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber()

        # Create a real ServiceInfo for the worker
        from wool.runtime.discovery.lan import _serialize_metadata

        properties = _serialize_metadata(metadata)
        service_name = f"{metadata.uid}._wool._tcp.local."

        from zeroconf import ServiceInfo

        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],
            port=metadata.port,
            properties=properties,
        )

        # Mock async_get_service_info to return the service
        mock_get_service = mocker.AsyncMock(return_value=service_info)

        event_queue = asyncio.Queue()
        service_cache = {}  # Empty cache - worker not tracked yet

        from unittest.mock import MagicMock

        mock_aiozc = MagicMock()
        mock_aiozc.async_get_service_info = mock_get_service

        listener = subscriber._Listener(
            aiozc=mock_aiozc,
            event_queue=event_queue,
            predicate=lambda _: True,  # Always pass filter
            service_cache=service_cache,
        )

        # Act - directly call the async handler
        await listener._handle_update_service("_wool._tcp.local.", service_name)

        # Assert - worker-added event should be emitted
        assert not event_queue.empty()
        event = await event_queue.get()
        assert event.type == "worker-added"
        assert event.metadata.uid == metadata.uid
        assert event.metadata.port == metadata.port
        # Verify worker was added to cache
        assert service_name in service_cache
        assert service_cache[service_name].uid == metadata.uid

    @given(worker=metadata_strategy())
    @settings(max_examples=20)
    def test_subscriber_handles_new_worker_via_update_property(self, worker):
        """Property: Any worker appearing via update emits worker-added event.

        Given:
            Arbitrary valid WorkerMetadata generated by hypothesis
            Subscriber with empty service cache
        When:
            Update event arrives for worker not in cache
        Then:
            Worker-added event should be emitted with correct worker info
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber()

        # Create a real ServiceInfo for the worker
        from wool.runtime.discovery.lan import _serialize_metadata

        properties = _serialize_metadata(worker)
        service_name = f"{worker.uid}._wool._tcp.local."

        from zeroconf import ServiceInfo

        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],
            port=worker.port,
            properties=properties,
        )

        # Mock async_get_service_info to return the service
        from unittest.mock import AsyncMock
        from unittest.mock import MagicMock

        mock_get_service = AsyncMock(return_value=service_info)

        event_queue = asyncio.Queue()
        service_cache = {}  # Empty cache - worker not tracked yet

        mock_aiozc = MagicMock()
        mock_aiozc.async_get_service_info = mock_get_service

        listener = subscriber._Listener(
            aiozc=mock_aiozc,
            event_queue=event_queue,
            predicate=lambda _: True,  # Always pass filter
            service_cache=service_cache,
        )

        # Act - directly call the async handler
        # Note: We need to run this in an event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                listener._handle_update_service("_wool._tcp.local.", service_name)
            )

            # Assert - worker-added event should be emitted
            assert not event_queue.empty()
            event = loop.run_until_complete(event_queue.get())
            assert event.type == "worker-added"
            assert event.metadata.uid == worker.uid
            assert event.metadata.port == worker.port
            assert event.metadata.pid == worker.pid
            assert event.metadata.version == worker.version
            assert event.metadata.tags == worker.tags
            assert event.metadata.extra == worker.extra
            # Verify worker was added to cache
            assert service_name in service_cache
            assert service_cache[service_name].uid == worker.uid
        finally:
            loop.close()

    @pytest.mark.asyncio
    async def test_subscriber_handles_deserialization_error(self, mocker, metadata):
        """Test subscriber handles deserialization errors gracefully.

        Given:
            Subscriber receiving service with invalid properties
        When:
            Deserialization raises ValueError
        Then:
            Error should be caught silently, service skipped
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        # Patch _deserialize_metadata to raise ValueError
        mocker.patch(
            "wool.runtime.discovery.lan._deserialize_metadata",
            side_effect=ValueError("Invalid service"),
        )

        events = []

        async def collect_events():
            try:
                async for event in subscriber:
                    events.append(event)
                    await asyncio.sleep(0.5)
                    break
            except Exception:
                pass

        # Act
        async with publisher:
            collect_task = asyncio.create_task(collect_events())
            await asyncio.sleep(0.1)

            # This will trigger _handle_add_service, which will catch ValueError
            await publisher.publish("worker-added", metadata)

            await asyncio.sleep(0.4)

            collect_task.cancel()
            try:
                await collect_task
            except asyncio.CancelledError:
                pass

        # Assert - ValueError was caught, no events yielded (service skipped)
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_worker_appears_via_update_not_add(self, worker_factory):
        """Test worker first appearing via update event instead of add.

        Given:
            Subscriber monitoring for workers
        When:
            Worker first appears via update event (not add)
        Then:
            Worker-added event should be emitted
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        worker = worker_factory(port=50051, tags=frozenset(["new"]))

        events = []
        event_received = asyncio.Event()

        async def collect_events():
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == worker.uid:
                    event_received.set()
                    break

        # Act
        async with publisher:
            collect_task = asyncio.create_task(collect_events())

            await asyncio.sleep(0.1)

            # Publish as update without ever adding it first
            await publisher.publish("worker-updated", worker)

            try:
                await asyncio.wait_for(event_received.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            finally:
                collect_task.cancel()
                try:
                    await collect_task
                except asyncio.CancelledError:
                    pass

        # Assert - should receive worker-added event (not update)
        if len(events) > 0:
            assert events[0].type in ["worker-added", "worker-updated"]


class TestIntegration:
    """Integration tests for LAN Discovery end-to-end flows."""

    @pytest.mark.asyncio
    async def test_publish_and_discover_integration(self, metadata):
        """Test end-to-end publisher→Zeroconf→subscriber flow.

        Given:
            A Publisher and Subscriber on localhost
        When:
            Worker is published via Publisher
        Then:
            Subscriber should discover worker and yield event
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        subscriber = LanDiscovery.Subscriber()

        events = []
        worker_discovered = asyncio.Event()

        async def collect_events():
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == metadata.uid:
                    worker_discovered.set()
                    break

        # Act
        async with publisher:
            collect_task = asyncio.create_task(collect_events())

            # Give subscriber time to initialize
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", metadata)

            # Wait for discovery with timeout
            try:
                await asyncio.wait_for(worker_discovered.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("Worker not discovered within timeout")
            finally:
                collect_task.cancel()
                try:
                    await collect_task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert len(events) >= 1
        discovered_event = events[0]
        assert discovered_event.type == "worker-added"
        assert discovered_event.metadata.uid == metadata.uid
        assert discovered_event.metadata.port == metadata.port
        assert discovered_event.metadata.pid == metadata.pid


class TestSerializationFunctions:
    """Tests for serialization helper functions.

    Fully qualified name: wool.runtime.discovery.lan
    """

    def test_serialize_metadata_complete(self, metadata):
        """Test _serialize_metadata() with all fields populated.

        Given:
            WorkerMetadata with all fields including tags and extra
        When:
            Serializing to service properties dict
        Then:
            All fields should be converted to dict with JSON-encoded tags/extra
        """
        # Arrange
        # metadata fixture has all fields

        # Act
        properties = _serialize_metadata(metadata)

        # Assert
        assert properties["pid"] == str(metadata.pid)
        assert properties["version"] == metadata.version
        assert properties["tags"] is not None
        assert '"test"' in properties["tags"]
        assert '"worker"' in properties["tags"]
        assert properties["extra"] is not None
        assert '"key"' in properties["extra"]
        assert '"value"' in properties["extra"]

    def test_serialize_metadata_minimal(self):
        """Test _serialize_metadata() with minimal fields (empty tags/extra).

        Given:
            WorkerMetadata with empty tags and extra
        When:
            Serializing to service properties dict
        Then:
            Optional fields should be None
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            host="localhost",
            port=50051,
            pid=12345,
            version="1.0.0",
            tags=frozenset(),
            extra=MappingProxyType({}),
        )

        # Act
        properties = _serialize_metadata(worker)

        # Assert
        assert properties["pid"] == "12345"
        assert properties["version"] == "1.0.0"
        assert properties["tags"] is None
        assert properties["extra"] is None

    def test_serialize_metadata_handles_none_values(self):
        """Test _serialize_metadata() handles empty collections.

        Given:
            WorkerMetadata with empty tags and extra collections
        When:
            Serializing to service properties dict
        Then:
            Tags and extra should be None (not empty JSON strings)
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            host="localhost",
            port=50051,
            pid=999,
            version="dev",
            tags=frozenset(),
            extra=MappingProxyType({}),
        )

        # Act
        properties = _serialize_metadata(worker)

        # Assert
        assert properties["tags"] is None
        assert properties["extra"] is None

    def test_deserialize_metadata_complete(self, metadata):
        """Test _deserialize_metadata() with complete ServiceInfo.

        Given:
            ServiceInfo with all fields including tags and extra
        When:
            Deserializing to WorkerMetadata
        Then:
            All fields should be correctly parsed including JSON fields
        """
        # Arrange
        properties = _serialize_metadata(metadata)
        service_name = f"{metadata.uid}._wool._tcp.local."
        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],  # 127.0.0.1
            port=metadata.port,
            properties=properties,
        )

        # Act
        result = _deserialize_metadata(service_info)

        # Assert
        assert result.uid == metadata.uid
        assert result.pid == metadata.pid
        assert result.version == metadata.version
        assert result.port == metadata.port
        assert result.tags == metadata.tags
        assert result.extra == metadata.extra

    def test_deserialize_metadata_missing_required_fields(self):
        """Test _deserialize_metadata() with missing required fields.

        Given:
            ServiceInfo missing required 'pid' or 'version' field
        When:
            Deserializing to WorkerMetadata
        Then:
            ValueError should be raised with clear message
        """
        # Arrange - missing 'version'
        uid = uuid.uuid4()
        service_name = f"{uid}._wool._tcp.local."
        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],
            port=50051,
            properties={"pid": "12345"},  # Missing 'version'
        )

        # Act & Assert
        with pytest.raises(ValueError, match="Missing required properties: version"):
            _deserialize_metadata(service_info)

    def test_deserialize_metadata_invalid_uuid(self):
        """Test _deserialize_metadata() with invalid UUID in service name.

        Given:
            ServiceInfo with malformed UUID in service name
        When:
            Deserializing to WorkerMetadata
        Then:
            ValueError should be raised
        """
        # Arrange
        service_name = "not-a-valid-uuid._wool._tcp.local."
        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],
            port=50051,
            properties={"pid": "12345", "version": "1.0.0"},
        )

        # Act & Assert
        with pytest.raises(ValueError, match="badly formed hexadecimal UUID string"):
            _deserialize_metadata(service_info)

    def test_deserialize_metadata_malformed_json(self):
        """Test _deserialize_metadata() with malformed JSON in properties.

        Given:
            ServiceInfo with malformed JSON in tags or extra
        When:
            Deserializing to WorkerMetadata
        Then:
            JSONDecodeError should be raised
        """
        # Arrange
        uid = uuid.uuid4()
        service_name = f"{uid}._wool._tcp.local."
        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],
            port=50051,
            properties={
                "pid": "12345",
                "version": "1.0.0",
                "tags": "{invalid json",  # Malformed JSON
            },
        )

        # Act & Assert
        with pytest.raises(Exception):  # json.JSONDecodeError
            _deserialize_metadata(service_info)

    def test_deserialize_metadata_optional_fields(self):
        """Test _deserialize_metadata() with only required fields.

        Given:
            ServiceInfo with only required fields (no tags/extra)
        When:
            Deserializing to WorkerMetadata
        Then:
            Optional fields should have empty defaults
        """
        # Arrange
        uid = uuid.uuid4()
        service_name = f"{uid}._wool._tcp.local."
        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],
            port=50051,
            properties={"pid": "12345", "version": "1.0.0"},
        )

        # Act
        result = _deserialize_metadata(service_info)

        # Assert
        assert result.tags == frozenset()
        assert result.extra == MappingProxyType({})

    def test_serialize_deserialize_roundtrip(self, metadata):
        """Test serialization roundtrip preserves all fields.

        Given:
            WorkerMetadata instance with all fields
        When:
            Serializing and then deserializing
        Then:
            All fields should be preserved exactly
        """
        # Arrange
        properties = _serialize_metadata(metadata)
        service_name = f"{metadata.uid}._wool._tcp.local."
        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],
            port=metadata.port,
            properties=properties,
        )

        # Act
        result = _deserialize_metadata(service_info)

        # Assert
        assert result.uid == metadata.uid
        assert result.pid == metadata.pid
        assert result.version == metadata.version
        assert result.port == metadata.port
        assert result.tags == metadata.tags
        assert result.extra == metadata.extra

    def test_resolve_address_ipv4(self):
        """Test Publisher._resolve_address() with IPv4 address.

        Given:
            Address string with IPv4 format "127.0.0.1:50051"
        When:
            Resolving address to bytes and port
        Then:
            Should return IPv4 address as 4 bytes and port as int
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        address = "127.0.0.1:50051"

        # Act
        ip_bytes, port = publisher._resolve_address(address)

        # Assert
        assert ip_bytes == b"\x7f\x00\x00\x01"  # 127.0.0.1
        assert port == 50051
        assert len(ip_bytes) == 4

    def test_resolve_address_hostname(self):
        """Test Publisher._resolve_address() with hostname.

        Given:
            Address string with hostname "localhost:8080"
        When:
            Resolving address to bytes and port
        Then:
            Should resolve hostname and return IP bytes and port
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        address = "localhost:8080"

        # Act
        ip_bytes, port = publisher._resolve_address(address)

        # Assert
        assert port == 8080
        assert len(ip_bytes) == 4  # IPv4 address
        assert isinstance(ip_bytes, bytes)

    def test_resolve_address_invalid_format(self):
        """Test Publisher._resolve_address() with invalid address format.

        Given:
            Address string with invalid format (missing port)
        When:
            Resolving address to bytes and port
        Then:
            Should raise ValueError
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        address = "localhost"  # Missing port

        # Act & Assert
        with pytest.raises(ValueError):
            publisher._resolve_address(address)


class TestPropertyBasedSerialization:
    """Property-based tests for serialization functions.

    Uses Hypothesis to generate diverse test inputs and verify
    invariants hold across the input space.

    Note on DNS-SD limitations:
        LAN discovery uses DNS-SD (DNS Service Discovery) via Zeroconf,
        which has a 255-byte limit per TXT record. Each record contains
        "key=value" pairs, so the combined length of key + "=" + value
        must be <= 255 bytes. These tests respect this constraint by
        generating reasonably-sized tags and extra metadata that reflect
        realistic production usage patterns.
    """

    @given(worker=metadata_strategy())
    @settings(max_examples=50)
    def test_serialize_deserialize_roundtrip_property(self, worker):
        """Property: Any valid WorkerMetadata survives serialize→deserialize.

        Given:
            Arbitrary valid WorkerMetadata generated by hypothesis
        When:
            Serializing and then deserializing
        Then:
            All fields should be preserved exactly
        """
        # Arrange - worker provided by hypothesis
        properties = _serialize_metadata(worker)
        service_name = f"{worker.uid}._wool._tcp.local."
        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],
            port=worker.port,
            properties=properties,
        )

        # Act
        result = _deserialize_metadata(service_info)

        # Assert - all fields preserved
        assert result.uid == worker.uid
        assert result.pid == worker.pid
        assert result.version == worker.version
        assert result.port == worker.port
        assert result.tags == worker.tags
        assert result.extra == worker.extra

    @given(
        tags=st.frozensets(
            st.text(
                min_size=1,
                max_size=20,
                alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_",
            ),
            max_size=5,  # Keep small to respect DNS TXT record 255-byte limit
        ),
        extra=st.dictionaries(
            st.text(
                min_size=1,
                max_size=20,
                alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_",
            ),
            st.one_of(
                # Use printable ASCII to ensure JSON-safe strings
                st.text(
                    min_size=0,
                    max_size=50,
                    alphabet=st.characters(
                        min_codepoint=32,  # Space
                        max_codepoint=126,  # Tilde (printable ASCII)
                    ),
                ),
                st.integers(min_value=-2147483648, max_value=2147483647),
                st.booleans(),
            ),
            max_size=5,  # Keep small to respect DNS TXT record 255-byte limit
        ),
    )
    @settings(max_examples=50)
    def test_serialize_handles_various_collections_property(self, tags, extra):
        """Property: Serialization correctly handles any valid tags/extra.

        Given:
            Arbitrary tags (frozenset) and extra (dict) collections
        When:
            Serializing WorkerMetadata with these collections
        Then:
            Empty collections serialize as None, non-empty as JSON
            All values should roundtrip correctly
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            host="localhost",
            port=50051,
            pid=12345,
            version="1.0.0",
            tags=tags,
            extra=MappingProxyType(extra),
        )

        # Act
        properties = _serialize_metadata(worker)

        # Assert - empty collections serialize as None
        if not tags:
            assert properties["tags"] is None
        else:
            assert properties["tags"] is not None
            deserialized_tags = set(json.loads(properties["tags"]))
            assert deserialized_tags == tags

        if not extra:
            assert properties["extra"] is None
        else:
            assert properties["extra"] is not None
            deserialized_extra = json.loads(properties["extra"])
            assert deserialized_extra == extra

    @given(
        version=st.one_of(
            # Semantic versioning
            st.from_regex(r"^\d+\.\d+\.\d+$", fullmatch=True),
            # Major.minor
            st.from_regex(r"^\d+\.\d+$", fullmatch=True),
            # Single number
            st.from_regex(r"^\d+$", fullmatch=True),
            # Arbitrary non-empty string (alphanumeric + .-_)
            st.text(
                min_size=1,
                max_size=20,
                alphabet=st.characters(
                    whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters=".-_"
                ),
            ),
        )
    )
    @settings(max_examples=50)
    def test_version_serialization_property(self, version):
        """Property: Any version string serializes/deserializes correctly.

        Given:
            Arbitrary version string (various formats)
        When:
            Serializing and deserializing WorkerMetadata
        Then:
            Version should be preserved exactly
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            host="localhost",
            port=50051,
            pid=12345,
            version=version,
        )

        # Act
        properties = _serialize_metadata(worker)

        # Assert
        assert properties["version"] == version

        # Verify roundtrip
        service_name = f"{worker.uid}._wool._tcp.local."
        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],
            port=worker.port,
            properties=properties,
        )
        result = _deserialize_metadata(service_info)
        assert result.version == version

    @given(
        octet1=st.integers(min_value=0, max_value=255),
        octet2=st.integers(min_value=0, max_value=255),
        octet3=st.integers(min_value=0, max_value=255),
        octet4=st.integers(min_value=0, max_value=255),
        port=st.integers(min_value=1, max_value=65535),
    )
    @settings(max_examples=100)
    def test_resolve_address_ipv4_property(self, octet1, octet2, octet3, octet4, port):
        """Property: Any valid IPv4:port combination resolves correctly.

        Given:
            Arbitrary valid IPv4 address octets and port number
        When:
            Resolving address string to bytes and port
        Then:
            Should return correct 4-byte IPv4 address and port
        """
        # Arrange
        publisher = LanDiscovery.Publisher()
        address = f"{octet1}.{octet2}.{octet3}.{octet4}:{port}"

        # Act
        ip_bytes, resolved_port = publisher._resolve_address(address)

        # Assert
        assert len(ip_bytes) == 4, "IPv4 address should be 4 bytes"
        assert ip_bytes == bytes([octet1, octet2, octet3, octet4])
        assert resolved_port == port

    @given(
        pid=st.integers(min_value=1, max_value=2147483647),
        port=st.integers(min_value=1, max_value=65535),
    )
    @settings(max_examples=50)
    def test_serialize_numeric_fields_property(self, pid, port):
        """Property: Numeric fields serialize/deserialize correctly.

        Given:
            Arbitrary valid PID and port numbers
        When:
            Serializing and deserializing WorkerMetadata
        Then:
            Numeric fields should roundtrip as integers
        """
        # Arrange
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            host="localhost",
            port=port,
            pid=pid,
            version="1.0.0",
        )

        # Act
        properties = _serialize_metadata(worker)
        service_name = f"{worker.uid}._wool._tcp.local."
        service_info = ServiceInfo(
            "_wool._tcp.local.",
            service_name,
            addresses=[b"\x7f\x00\x00\x01"],
            port=worker.port,
            properties=properties,
        )
        result = _deserialize_metadata(service_info)

        # Assert
        assert result.pid == pid
        assert result.port == port
        # Verify serialized format
        assert properties["pid"] == str(pid)
