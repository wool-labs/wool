"""End-to-end LAN discovery round-trips over real mDNS multicast.

What a `LanDiscovery.Publisher` puts on the wire and a
`LanDiscovery.Subscriber` reads back off it — live service
registration, real multicast, and the wall-clock waits both entail.
The subscriber's own translation of records into discovery events is
covered without a network in ``tests/runtime/discovery/test_lan.py``;
what these prove is that the two halves agree across the wire, which
no stand-in for the network can establish.
"""

import asyncio
import uuid
from types import MappingProxyType

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.discovery.lan import LanDiscovery
from wool.runtime.worker.metadata import WorkerMetadata

# Unique per run so concurrent suites on one network segment do not see
# each other's mDNS records.
_TEST_SERVICE_TYPE = f"_wool-{uuid.uuid4().hex[:6]}._tcp.local."


@pytest.fixture
def metadata():
    """A well-formed worker record for publication."""
    return WorkerMetadata(
        uid=uuid.uuid4(),
        address="127.0.0.1:50051",
        pid=12345,
        version="1.0.0",
        tags=frozenset(["test"]),
        extra=MappingProxyType({}),
    )


@pytest.mark.integration
class TestLanDiscoveryRoundTrip:
    """Publisher-to-subscriber round-trips over real multicast.

    Fully qualified name: wool.runtime.discovery.lan.LanDiscovery
    """

    @pytest.mark.asyncio
    async def test___aiter___end_to_end_publish_discover(self, metadata):
        """Test end-to-end publish-discover flow.

        Given:
            A Publisher and Subscriber on the same host
        When:
            Worker is published via Publisher
        Then:
            Subscriber should discover the worker and yield an event
            with matching metadata.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)
        subscriber = LanDiscovery.Subscriber(_TEST_SERVICE_TYPE)

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
        advertise_host=st.one_of(st.none(), st.just("127.0.0.1")),
    )
    @settings(max_examples=10, deadline=10000)
    @pytest.mark.asyncio
    async def test_publish_roundtrip_with_arbitrary_metadata(
        self, address, pid, version, tags, advertise_host
    ):
        """Test publish-discover roundtrip with arbitrary metadata.

        Given:
            Arbitrary valid WorkerMetadata with DNS-SD-safe field
            sizes and an optionally overridden loopback publish host
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
        publisher = LanDiscovery.Publisher(
            _TEST_SERVICE_TYPE, advertise_host=advertise_host
        )
        subscriber = LanDiscovery.Subscriber(_TEST_SERVICE_TYPE)

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

        # Assert — events[-1] is the matched event (collect breaks
        # after finding worker.uid); earlier entries may be stale
        # because the shared Zeroconf browser persists across
        # hypothesis examples.
        assert len(events) >= 1
        event = events[-1]
        assert event.metadata.uid == worker.uid
        assert event.metadata.pid == worker.pid
        assert event.metadata.version == worker.version
        assert event.metadata.tags == worker.tags
        expected_port = int(address.split(":")[1])
        assert event.metadata.address == f"127.0.0.1:{expected_port}"
