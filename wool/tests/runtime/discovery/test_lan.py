import asyncio
import os
import socket
import uuid
import warnings
from types import MappingProxyType

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture
from zeroconf import ServiceInfo

import wool.runtime.discovery.lan as lan
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.lan import AdvertiseHostError
from wool.runtime.discovery.lan import LanDiscovery
from wool.runtime.discovery.lan import LoopbackAdvertisementWarning
from wool.runtime.worker.metadata import WorkerMetadata
from wool.utilities.afilter import afilter

# Unique per test run so concurrent suites on the same network segment
# do not see each other's mDNS records now that zeroconf binds all
# interfaces instead of loopback only.
_TEST_NAMESPACE = f"test-{uuid.uuid4().hex}"
_TEST_SERVICE_TYPE = f"_wool-{uuid.uuid4().hex[:6]}._tcp.local."


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


@pytest.fixture
def mock_zeroconf(mocker: MockerFixture):
    """Patches AsyncZeroconf in the lan module with an async mock.

    Isolates publisher tests from the network so registered
    ServiceInfo records can be inspected without real mDNS traffic.
    """
    instance = mocker.MagicMock()
    instance.async_register_service = mocker.AsyncMock()
    instance.async_unregister_service = mocker.AsyncMock()
    instance.async_update_service = mocker.AsyncMock()
    instance.async_close = mocker.AsyncMock()
    mocker.patch.object(lan, "AsyncZeroconf", return_value=instance)
    return instance


class _SubscriberDriver:
    """Feeds a `LanDiscovery.Subscriber` service records directly.

    Stands in for the mDNS network so the subscriber's own translation
    of records into `DiscoveryEvent` values can be tested without
    multicast, wall-clock waits, or a network the CI host may not have.
    What crosses the wire is proved end to end in
    ``tests/integration/test_lan_discovery.py``.
    """

    def __init__(self, service_type: str) -> None:
        self._service_type = service_type
        self._records: dict[str, ServiceInfo] = {}
        self._listeners: list[object] = []
        self.browsing = asyncio.Event()

    def resolve(self, name: str) -> ServiceInfo | None:
        """Answer what the subscriber's service-info lookup would."""
        return self._records.get(name)

    def attach(self, listener) -> None:
        """Record the listener the subscriber installed on its browser."""
        self._listeners.append(listener)
        self.browsing.set()

    async def _announce(self, callback: str, info: ServiceInfo) -> None:
        """Invoke a listener callback and await the work it schedules.

        The listener's callbacks are synchronous and schedule the real
        handling with `asyncio.create_task`, so returning before those
        tasks finish would let the next announcement overwrite a record
        the previous handler has not read yet. Awaiting exactly the
        tasks this call spawned keeps announcements ordered without a
        sleep to tune.
        """
        await self.browsing.wait()
        before = asyncio.all_tasks()
        for listener in self._listeners:
            getattr(listener, callback)(None, self._service_type, info.name)
        if spawned := asyncio.all_tasks() - before:
            await asyncio.wait(spawned)

    async def added(self, info: ServiceInfo) -> None:
        """Announce ``info`` as a newly discovered service."""
        self._records[info.name] = info
        await self._announce("add_service", info)

    async def updated(self, info: ServiceInfo) -> None:
        """Re-announce ``info`` with changed properties."""
        self._records[info.name] = info
        await self._announce("update_service", info)

    async def removed(self, info: ServiceInfo) -> None:
        """Withdraw a previously announced service."""
        await self._announce("remove_service", info)
        self._records.pop(info.name, None)


@pytest.fixture
def subscriber_driver(mocker: MockerFixture):
    """Patch the subscriber's two zeroconf seams and drive them by hand.

    `LanDiscovery.Subscriber` opens exactly two: it constructs an
    ``AsyncZeroconf`` to resolve service info, and an
    ``AsyncServiceBrowser`` that calls back as records appear. Both are
    network boundaries, which is the only place the test guide permits
    mocking, and replacing them leaves every line of wool's own
    listener under test.
    """
    driver = _SubscriberDriver(_TEST_SERVICE_TYPE)

    aiozc = mocker.MagicMock()
    aiozc.async_close = mocker.AsyncMock()
    aiozc.async_get_service_info = mocker.AsyncMock(
        side_effect=lambda type_, name, *args, **kwargs: driver.resolve(name)
    )
    mocker.patch.object(lan, "AsyncZeroconf", return_value=aiozc)

    def browse(zc, service_type, listener, *args, **kwargs):
        driver.attach(listener)
        browser = mocker.MagicMock()
        browser.async_cancel = mocker.AsyncMock()
        return browser

    mocker.patch.object(lan, "AsyncServiceBrowser", side_effect=browse)
    return driver


def _record(uid=None, *, port=9999, address="127.0.0.1", **properties) -> ServiceInfo:
    """Build a raw service record advertising ``properties``."""
    uid = uid if uid is not None else uuid.uuid4()
    return ServiceInfo(
        _TEST_SERVICE_TYPE,
        f"{uid}.{_TEST_SERVICE_TYPE}",
        addresses=[socket.inet_aton(address)],
        port=port,
        properties={"pid": "1001", "version": "1.0.0", **properties},
    )


async def _collect(subscriber, count, *, timeout=2.0):
    """Collect ``count`` events, failing rather than hanging."""
    events = []

    async def drain():
        async for event in subscriber:
            events.append(event)
            if len(events) >= count:
                return

    await asyncio.wait_for(drain(), timeout=timeout)
    return events


@pytest.fixture
def mock_route_probe(mocker: MockerFixture):
    """Patches the UDP probe socket with a deterministic routable IP.

    Returns the patched ``socket.socket`` mock so tests can assert
    how many times the default-route probe was opened.
    """
    probe = mocker.MagicMock()
    probe.getsockname.return_value = ("192.0.2.7", 0)
    return mocker.patch.object(socket, "socket", return_value=probe)


class TestLanDiscovery:
    """Tests for LanDiscovery class.

    Fully qualified name: wool.runtime.discovery.lan.LanDiscovery
    """

    def test___init___with_auto_generated_namespace(self):
        """Test LanDiscovery auto-generates a namespace.

        Given:
            No namespace argument
        When:
            LanDiscovery is instantiated
        Then:
            It should have a namespace starting with "pool-".
        """
        # Act
        discovery = LanDiscovery()

        # Assert
        assert discovery.namespace.startswith("pool-")

    def test___init___with_explicit_namespace(self):
        """Test LanDiscovery stores an explicit namespace.

        Given:
            An explicit namespace string
        When:
            LanDiscovery is instantiated with that namespace
        Then:
            It should return the provided namespace.
        """
        # Act
        discovery = LanDiscovery("my-pool")

        # Assert
        assert discovery.namespace == "my-pool"

    def test___init___with_unique_auto_namespaces(self):
        """Test auto-generated namespaces are unique.

        Given:
            Two LanDiscovery instances with no namespace argument
        When:
            Both are instantiated
        Then:
            Their namespace values should differ.
        """
        # Act
        d1 = LanDiscovery()
        d2 = LanDiscovery()

        # Assert
        assert d1.namespace != d2.namespace

    def test___hash___with_same_namespace(self):
        """Test hash equality for same namespace.

        Given:
            Two LanDiscovery instances with the same namespace.
        When:
            Their hashes are compared.
        Then:
            It should produce equal hashes.
        """
        # Arrange
        a = LanDiscovery("shared-ns")
        b = LanDiscovery("shared-ns")

        # Act & assert
        assert hash(a) == hash(b)

    def test___hash___with_different_namespace(self):
        """Test hash inequality for different namespaces.

        Given:
            Two LanDiscovery instances with different namespaces.
        When:
            Their hashes are compared.
        Then:
            It should produce different hashes.
        """
        # Arrange
        a = LanDiscovery("ns-a")
        b = LanDiscovery("ns-b")

        # Act & assert
        assert hash(a) != hash(b)

    def test___eq___with_same_namespace(self):
        """Test equality for same namespace.

        Given:
            Two LanDiscovery instances with the same namespace.
        When:
            They are compared with ==.
        Then:
            It should return True.
        """
        # Arrange
        a = LanDiscovery("shared-ns")
        b = LanDiscovery("shared-ns")

        # Act & assert
        assert a == b

    def test___eq___with_different_namespace(self):
        """Test inequality for different namespaces.

        Given:
            Two LanDiscovery instances with different namespaces.
        When:
            They are compared with ==.
        Then:
            It should return False.
        """
        # Arrange
        a = LanDiscovery("ns-a")
        b = LanDiscovery("ns-b")

        # Act & assert
        assert a != b

    def test___eq___with_non_lan_discovery(self):
        """Test equality with a non-LanDiscovery object.

        Given:
            A LanDiscovery instance and a non-LanDiscovery object.
        When:
            They are compared with ==.
        Then:
            It should not be equal.
        """
        # Act & assert
        assert LanDiscovery("ns") != "not-a-discovery"

    @pytest.mark.parametrize("advertise_host", ["0.0.0.0", "::", "::0", ""])
    def test___init___with_wildcard_advertise_host(self, advertise_host):
        """Test LanDiscovery rejects a wildcard advertise_host eagerly.

        Given:
            A wildcard or empty advertise_host string
        When:
            LanDiscovery is instantiated
        Then:
            It should raise ValueError at construction, surfacing the
            misconfiguration where it was written instead of at first
            publish.
        """
        # Act & assert
        with pytest.raises(ValueError, match="wildcard or empty"):
            LanDiscovery("ns", advertise_host=advertise_host)

    def test___eq___with_different_advertise_host(self):
        """Test equality is unaffected by advertise_host.

        Given:
            Two LanDiscovery instances with the same namespace but
            different advertise_host values.
        When:
            They are compared with == and their hashes are compared.
        Then:
            It should compare equal with equal hashes — identity is
            namespace-only.
        """
        # Arrange
        a = LanDiscovery("shared-ns", advertise_host="10.0.0.1")
        b = LanDiscovery("shared-ns")

        # Act & assert
        assert a == b
        assert hash(a) == hash(b)

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

    @pytest.mark.asyncio
    async def test_publisher_with_advertise_host(self, mock_zeroconf, metadata):
        """Test publisher property threads advertise_host into Publisher.

        Given:
            A LanDiscovery constructed with an explicit advertise_host
        When:
            A worker is published through the publisher property
        Then:
            It should advertise the advertise_host instead of the
            worker's bind host.
        """
        # Arrange
        discovery = LanDiscovery("ns", advertise_host="10.4.5.6")
        publisher = discovery.publisher
        assert isinstance(publisher, LanDiscovery.Publisher)

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)

            # Assert
            service_info = publisher.services[str(metadata.uid)]
            assert service_info.addresses == [socket.inet_aton("10.4.5.6")]

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
        assert isinstance(subscriber, DiscoverySubscriberLike)

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

        discovery = LanDiscovery(_TEST_NAMESPACE, filter=predicate)
        publisher = discovery.publisher
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

        discovery = LanDiscovery(_TEST_NAMESPACE)
        publisher = discovery.publisher
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

    @pytest.mark.asyncio
    async def test_subscribe_with_namespace_isolation(self, metadata):
        """Test subscribers on different namespaces are isolated.

        Given:
            Two LanDiscovery instances with different namespaces, a
            publisher on namespace A, and a subscriber on namespace B
        When:
            The publisher publishes a worker
        Then:
            The subscriber on namespace B should not receive the event.
        """
        # Arrange
        discovery_a = LanDiscovery(f"ns-alpha-{uuid.uuid4().hex}")
        discovery_b = LanDiscovery(f"ns-beta-{uuid.uuid4().hex}")
        publisher = discovery_a.publisher
        subscriber = discovery_b.subscriber

        events = []

        async def collect():
            async for event in subscriber:
                events.append(event)

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)

            await publisher.publish("worker-added", metadata)
            await asyncio.sleep(1.0)

            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Assert
        assert events == []


class TestLanDiscoveryPublisher:
    """Tests for LanDiscovery.Publisher class.

    Fully qualified name:
    wool.runtime.discovery.lan.LanDiscovery.Publisher
    """

    def test_bind_host_with_default_value(self):
        """Test bind_host prescribes the wildcard address.

        Given:
            A LanDiscovery Publisher
        When:
            The bind_host attribute is accessed
        Then:
            It should be "0.0.0.0" so advertised workers accept
            off-host connections.
        """
        # Act
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)

        # Assert
        assert publisher.bind_host == "0.0.0.0"

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
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)

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
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)

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
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)

            # Assert
            service_name = f"{metadata.uid}.{_TEST_SERVICE_TYPE}"
            assert publisher.aiozc is not None
            service_info = await publisher.aiozc.async_get_service_info(
                _TEST_SERVICE_TYPE,
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
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)

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
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)
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
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)

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
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)

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
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)

            # Assert
            assert str(metadata.uid) in publisher.services
            service_info = publisher.services[str(metadata.uid)]
            assert service_info is not None
            assert b"pid" in service_info.properties

    @pytest.mark.asyncio
    async def test_publish_worker_added_with_explicit_advertise_host(
        self, mock_zeroconf, metadata
    ):
        """Test publish substitutes an explicit advertise_host.

        Given:
            A Publisher with advertise_host set to a concrete address
            and a worker bound to a different host
        When:
            publish("worker-added", metadata) is called
        Then:
            It should advertise the advertise_host verbatim while
            keeping the worker's port.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE, advertise_host="10.1.2.3")

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)

            # Assert
            service_info = publisher.services[str(metadata.uid)]
            assert service_info.addresses == [socket.inet_aton("10.1.2.3")]
            assert service_info.port == 50051

    @pytest.mark.asyncio
    async def test_publish_worker_added_with_callable_advertise_host(
        self, mock_zeroconf, worker_factory
    ):
        """Test publish resolves a callable advertise_host lazily once.

        Given:
            A Publisher with a callable advertise_host and two workers
        When:
            Both workers are published
        Then:
            It should invoke the callable only at first publish,
            exactly once, and advertise its result for both workers.
        """
        # Arrange
        calls = []

        def resolve() -> str:
            calls.append(1)
            return "10.9.8.7"

        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE, advertise_host=resolve)
        first = worker_factory("127.0.0.1:5001")
        second = worker_factory("127.0.0.1:5002")

        # Act
        async with publisher:
            await publisher.publish("worker-added", first)
            await publisher.publish("worker-added", second)

            # Assert
            assert len(calls) == 1
            for worker in (first, second):
                service_info = publisher.services[str(worker.uid)]
                assert service_info.addresses == [socket.inet_aton("10.9.8.7")]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bind_address",
        [
            "0.0.0.0:50051",
            "[::]:50051",
            "[0:0:0:0:0:0:0:0]:50051",
            ":::50051",
            ":50051",
        ],
    )
    async def test_publish_worker_added_with_wildcard_bind_host(
        self, mock_zeroconf, mock_route_probe, worker_factory, bind_address
    ):
        """Test publish auto-resolves every wildcard bind form.

        Given:
            A Publisher with no advertise_host and a worker bound to a
            wildcard address — IPv4, bracketed IPv6, bare IPv6, or an
            empty host
        When:
            publish("worker-added", metadata) is called
        Then:
            It should advertise the routable address resolved from
            the default-route interface instead of the wildcard.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)
        worker = worker_factory(bind_address)

        # Act
        async with publisher:
            await publisher.publish("worker-added", worker)

            # Assert
            service_info = publisher.services[str(worker.uid)]
            assert service_info.addresses == [socket.inet_aton("192.0.2.7")]
            assert service_info.port == 50051

    @pytest.mark.parametrize("advertise_host", ["0.0.0.0", "::", "::0", ""])
    def test___init___with_wildcard_advertise_host(self, advertise_host):
        """Test the Publisher rejects a wildcard advertise_host eagerly.

        Given:
            A wildcard or empty advertise_host string
        When:
            The Publisher is instantiated
        Then:
            It should raise ValueError at construction because a
            wildcard is never a connectable advertised address.
        """
        # Act & assert
        with pytest.raises(ValueError, match="wildcard or empty"):
            LanDiscovery.Publisher(_TEST_SERVICE_TYPE, advertise_host=advertise_host)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("resolved_host", ["0.0.0.0", "::", "::0", ""])
    async def test_publish_worker_added_with_callable_returning_wildcard_host(
        self, mock_zeroconf, metadata, resolved_host
    ):
        """Test publish rejects a callable yielding a wildcard host.

        Given:
            A Publisher with a callable advertise_host returning a
            wildcard or empty string
        When:
            publish("worker-added", metadata) is called
        Then:
            It should raise ValueError because a wildcard is never a
            connectable advertised address.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(
            _TEST_SERVICE_TYPE, advertise_host=lambda: resolved_host
        )

        # Act & assert
        async with publisher:
            with pytest.raises(ValueError, match="wildcard or empty"):
                await publisher.publish("worker-added", metadata)

    @pytest.mark.asyncio
    async def test_publish_with_wildcard_bind_across_repeated_events(
        self, mock_zeroconf, mock_route_probe, worker_factory
    ):
        """Test publish caches the auto-resolved host across events.

        Given:
            A Publisher with no advertise_host and wildcard-bound
            workers
        When:
            A worker is added, updated, and a second worker is added
        Then:
            It should open the default-route probe exactly once and
            reuse the cached address for every subsequent event.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)
        first = worker_factory("0.0.0.0:5001")
        updated_first = WorkerMetadata(
            uid=first.uid,
            address=first.address,
            pid=first.pid,
            version="2.0.0",
            tags=frozenset(["updated"]),
            extra=MappingProxyType({}),
        )
        second = worker_factory("0.0.0.0:5002")
        mock_route_probe.assert_not_called()

        # Act
        async with publisher:
            await publisher.publish("worker-added", first)
            await publisher.publish("worker-updated", updated_first)
            await publisher.publish("worker-added", second)

            # Assert
            assert mock_route_probe.call_count == 1
            for worker in (first, second):
                service_info = publisher.services[str(worker.uid)]
                assert service_info.addresses == [socket.inet_aton("192.0.2.7")]

    @pytest.mark.asyncio
    async def test_publish_worker_added_with_concrete_bind_host(
        self, mock_zeroconf, mock_route_probe, worker_factory
    ):
        """Test publish advertises a concrete bind host verbatim.

        Given:
            A Publisher with no advertise_host and a worker bound to a
            concrete non-loopback address
        When:
            publish("worker-added", metadata) is called
        Then:
            It should advertise the bind host verbatim and never open
            the default-route probe.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)
        worker = worker_factory("192.0.2.50:50051")

        # Act
        async with publisher:
            await publisher.publish("worker-added", worker)

            # Assert
            service_info = publisher.services[str(worker.uid)]
            assert service_info.addresses == [socket.inet_aton("192.0.2.50")]
            mock_route_probe.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_worker_added_with_loopback_bind_host(
        self, mock_zeroconf, worker_factory
    ):
        """Test publish with a loopback bind and no advertise_host.

        Given:
            A Publisher with no advertise_host and a worker bound to
            127.0.0.1
        When:
            publish("worker-added", metadata) is called
        Then:
            It should emit LoopbackAdvertisementWarning, since
            off-host subscribers cannot reach the advertised address.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)
        worker = worker_factory("127.0.0.1:5001")

        # Act & assert
        async with publisher:
            with pytest.warns(LoopbackAdvertisementWarning, match="loopback"):
                await publisher.publish("worker-added", worker)

    @pytest.mark.asyncio
    async def test_publish_worker_added_with_explicit_loopback_advertise_host(
        self, mock_zeroconf, worker_factory
    ):
        """Test publish with an explicitly loopback advertise_host.

        Given:
            A Publisher with advertise_host="127.0.0.1" set explicitly
        When:
            publish("worker-added", metadata) is called
        Then:
            It should not emit LoopbackAdvertisementWarning — an
            explicit advertise_host is a deliberate choice.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(
            _TEST_SERVICE_TYPE, advertise_host="127.0.0.1"
        )
        worker = worker_factory("127.0.0.1:5001")

        # Act & assert
        async with publisher:
            with warnings.catch_warnings():
                warnings.simplefilter("error", LoopbackAdvertisementWarning)
                await publisher.publish("worker-added", worker)

    @pytest.mark.asyncio
    async def test_publish_worker_added_with_advertise_host_and_wildcard_bind(
        self, mock_zeroconf, mock_route_probe, worker_factory
    ):
        """Test publish prefers an explicit host over auto-resolution.

        Given:
            A Publisher with an explicit advertise_host and a worker
            bound to the wildcard address
        When:
            publish("worker-added", metadata) is called
        Then:
            It should advertise the advertise_host and never open the
            default-route probe.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE, advertise_host="10.0.0.9")
        worker = worker_factory("0.0.0.0:5001")

        # Act
        async with publisher:
            await publisher.publish("worker-added", worker)

            # Assert
            service_info = publisher.services[str(worker.uid)]
            assert service_info.addresses == [socket.inet_aton("10.0.0.9")]
            mock_route_probe.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_worker_updated_with_unknown_worker_and_advertise_host(
        self, mock_zeroconf, worker_factory
    ):
        """Test update of an unknown worker registers substituted host.

        Given:
            A Publisher with an explicit advertise_host and a worker
            that was never added
        When:
            publish("worker-updated", metadata) is called
        Then:
            It should fall through to registration with the
            advertise_host substituted into the advertised record.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE, advertise_host="10.1.2.3")
        worker = worker_factory("127.0.0.1:5001")

        # Act
        async with publisher:
            await publisher.publish("worker-updated", worker)

            # Assert
            service_info = publisher.services[str(worker.uid)]
            assert service_info.addresses == [socket.inet_aton("10.1.2.3")]

    @pytest.mark.asyncio
    async def test_publish_worker_updated_with_known_worker_and_advertise_host(
        self, mock_zeroconf, worker_factory
    ):
        """Test update of a known worker keeps the substituted host.

        Given:
            A Publisher with an explicit advertise_host and a worker
            that was already added
        When:
            publish("worker-updated", metadata) is called with changed
            properties
        Then:
            It should keep the substituted address on the updated
            record while the properties reflect the new metadata.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE, advertise_host="10.1.2.3")
        worker = worker_factory("127.0.0.1:5001")
        updated = WorkerMetadata(
            uid=worker.uid,
            address=worker.address,
            pid=worker.pid,
            version="2.0.0",
            tags=frozenset(["updated"]),
            extra=MappingProxyType({}),
        )

        # Act
        async with publisher:
            await publisher.publish("worker-added", worker)
            await publisher.publish("worker-updated", updated)

            # Assert
            service_info = publisher.services[str(worker.uid)]
            assert service_info.addresses == [socket.inet_aton("10.1.2.3")]
            assert service_info.decoded_properties["version"] == "2.0.0"

    @pytest.mark.asyncio
    async def test_publish_worker_dropped_with_unknown_worker(
        self, mock_zeroconf, metadata
    ):
        """Test drop of a never-added worker.

        Given:
            An initialized Publisher with no published workers
        When:
            publish("worker-dropped", metadata) is called
        Then:
            It should return without error and never unregister a
            service.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)

        # Act
        async with publisher:
            await publisher.publish("worker-dropped", metadata)

            # Assert
            assert publisher.services == {}
            mock_zeroconf.async_unregister_service.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_worker_added_with_malformed_address(
        self, mock_zeroconf, worker_factory
    ):
        """Test publish with an address missing the port separator.

        Given:
            An initialized Publisher and a worker whose address has no
            port separator
        When:
            publish("worker-added", metadata) is called
        Then:
            It should raise ValueError rather than misread the address
            as a wildcard bind.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)
        worker = worker_factory("50051")

        # Act & assert
        async with publisher:
            with pytest.raises(ValueError, match="port separator"):
                await publisher.publish("worker-added", worker)

    @pytest.mark.asyncio
    async def test_publish_worker_added_with_raising_callable_advertise_host(
        self, mock_zeroconf, metadata
    ):
        """Test publish with a callable advertise_host that raises.

        Given:
            A Publisher whose callable advertise_host raises an
            exception
        When:
            publish("worker-added", metadata) is called
        Then:
            It should propagate the exception and register nothing.
        """

        # Arrange
        def resolve() -> str:
            raise OSError("resolution backend unavailable")

        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE, advertise_host=resolve)

        # Act & assert
        async with publisher:
            with pytest.raises(OSError, match="resolution backend unavailable"):
                await publisher.publish("worker-added", metadata)

            assert publisher.services == {}
            mock_zeroconf.async_register_service.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_worker_added_with_failing_route_probe(
        self, mocker: MockerFixture, mock_zeroconf, worker_factory
    ):
        """Test publish when the default-route probe fails.

        Given:
            A Publisher with no advertise_host, a wildcard-bound worker,
            and a host without a default route (the probe socket's
            connect raises OSError)
        When:
            publish("worker-added", metadata) is called
        Then:
            It should raise AdvertiseHostError naming the
            advertise_host remedy and register nothing.
        """
        # Arrange
        probe = mocker.MagicMock()
        probe.connect.side_effect = OSError("Network is unreachable")
        mocker.patch.object(socket, "socket", return_value=probe)
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)
        worker = worker_factory("0.0.0.0:5001")

        # Act & assert
        async with publisher:
            with pytest.raises(AdvertiseHostError, match="advertise_host"):
                await publisher.publish("worker-added", worker)

            assert publisher.services == {}
            mock_zeroconf.async_register_service.assert_not_called()

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "IPv6-only advertisements are dropped by the V4Only "
            "deserializer and never reach subscribers; see #239"
        ),
    )
    @pytest.mark.asyncio
    async def test_publish_worker_added_with_bracketed_ipv6_bind_host(
        self, worker_factory
    ):
        """Test an IPv6-bound worker is discoverable by a subscriber.

        Given:
            A Publisher and Subscriber on the same host and a worker
            bound to a bracketed non-wildcard IPv6 literal
        When:
            The worker is published and the subscriber is iterated
        Then:
            It should yield a worker-added event carrying the worker's
            bracketed IPv6 address. Expected to fail until the V4Only
            deserializer surfaces IPv6-only records (see #239).
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)
        subscriber = LanDiscovery.Subscriber(_TEST_SERVICE_TYPE)
        worker = worker_factory("[fd00::1]:50051")

        events = []
        worker_discovered = asyncio.Event()

        async def collect():
            async for event in subscriber:
                events.append(event)
                if event.metadata.uid == worker.uid:
                    worker_discovered.set()
                    break

        # Act
        async with publisher:
            task = asyncio.create_task(collect())
            await asyncio.sleep(0.1)
            await publisher.publish("worker-added", worker)

            try:
                await asyncio.wait_for(worker_discovered.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pytest.fail("IPv6 worker not discovered within timeout")
            finally:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Assert
        assert events[0].type == "worker-added"
        assert events[0].metadata.address == worker.address

    @pytest.mark.asyncio
    async def test_publish_worker_added_with_hostname_advertise_host(
        self, mocker: MockerFixture, mock_zeroconf, metadata
    ):
        """Test publish resolves a hostname advertise_host.

        Given:
            A Publisher whose advertise_host is a hostname and a DNS
            resolver returning a concrete address
        When:
            publish("worker-added", metadata) is called
        Then:
            It should advertise the resolved address.
        """
        # Arrange
        mocker.patch.object(socket, "gethostbyname", return_value="203.0.113.5")
        publisher = LanDiscovery.Publisher(
            _TEST_SERVICE_TYPE, advertise_host="node1.example.com"
        )

        # Act
        async with publisher:
            await publisher.publish("worker-added", metadata)

            # Assert
            service_info = publisher.services[str(metadata.uid)]
            assert service_info.addresses == [socket.inet_aton("203.0.113.5")]

    @given(
        advertise_host=st.ip_addresses(v=4).map(str).filter(lambda h: h != "0.0.0.0"),
        bind_host=st.one_of(
            st.ip_addresses(v=4).map(str),
            st.sampled_from(["0.0.0.0", "[::]", ""]),
        ),
        port=st.integers(min_value=1, max_value=65535),
    )
    @settings(
        max_examples=25,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_publish_with_arbitrary_advertise_host_and_bind(
        self,
        mock_zeroconf,
        mock_route_probe,
        worker_factory,
        advertise_host,
        bind_host,
        port,
    ):
        """Test advertise_host substitution over the IPv4 domain.

        Given:
            Any non-wildcard IPv4 advertise_host, any bind host
            (concrete or wildcard), and any port
        When:
            A worker bound to that address is published with that
            advertise_host
        Then:
            It should advertise exactly the advertise_host with the
            worker's port preserved.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(
            _TEST_SERVICE_TYPE, advertise_host=advertise_host
        )
        worker = worker_factory(f"{bind_host}:{port}")

        # Act
        async with publisher:
            await publisher.publish("worker-added", worker)

            # Assert
            service_info = publisher.services[str(worker.uid)]
            assert service_info.addresses == [socket.inet_aton(advertise_host)]
            assert service_info.port == port

    @given(
        bind_host=st.ip_addresses(v=4)
        .map(str)
        .filter(lambda h: h not in ("0.0.0.0", "192.0.2.7")),
        port=st.integers(min_value=1, max_value=65535),
    )
    @settings(
        max_examples=25,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @pytest.mark.asyncio
    async def test_publish_with_arbitrary_concrete_bind_host(
        self, mock_zeroconf, mock_route_probe, worker_factory, bind_host, port
    ):
        """Test verbatim advertisement over the concrete IPv4 domain.

        Given:
            Any concrete non-wildcard IPv4 bind host (excluding the
            probe sentinel) and any port, with no advertise_host
        When:
            A worker bound to that address is published
        Then:
            It should advertise the bind host verbatim — never the
            probe's sentinel address — with the port preserved.
        """
        # Arrange
        publisher = LanDiscovery.Publisher(_TEST_SERVICE_TYPE)
        worker = worker_factory(f"{bind_host}:{port}")

        # Act
        async with publisher:
            await publisher.publish("worker-added", worker)

            # Assert
            service_info = publisher.services[str(worker.uid)]
            assert service_info.addresses == [socket.inet_aton(bind_host)]
            assert service_info.port == port


class TestLanDiscoverySubscriber:
    """Tests for LanDiscovery.Subscriber class.

    These drive the subscriber's own translation of service records
    into `DiscoveryEvent` values through a stand-in for the mDNS
    network — see the ``subscriber_driver`` fixture. What actually
    crosses the wire between a publisher and a subscriber is proved in
    ``tests/integration/test_lan_discovery.py``.

    Fully qualified name:
    wool.runtime.discovery.lan.LanDiscovery.Subscriber
    """

    @pytest.mark.asyncio
    async def test___aiter___should_yield_worker_added_for_a_new_record(
        self, subscriber_driver
    ):
        """Test a newly announced service becomes a worker-added event.

        Given:
            A subscriber being iterated, and a service record for a
            worker it has not seen.
        When:
            The record is announced.
        Then:
            It should yield a worker-added event carrying that worker's
            deserialized metadata.
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber(_TEST_SERVICE_TYPE)
        uid = uuid.uuid4()
        record = _record(uid, port=50051)

        # Act
        collected = asyncio.create_task(_collect(subscriber, 1))
        await subscriber_driver.added(record)
        (event,) = await collected

        # Assert
        assert event.type == "worker-added"
        assert event.metadata.uid == uid
        assert event.metadata.address == "127.0.0.1:50051"

    @pytest.mark.asyncio
    async def test___aiter___should_yield_worker_dropped_when_a_record_withdraws(
        self, subscriber_driver
    ):
        """Test withdrawing a tracked service becomes a worker-dropped event.

        Given:
            A subscriber that has already yielded a worker-added event
            for a record.
        When:
            That record is withdrawn.
        Then:
            It should yield a worker-dropped event for the same worker,
            since the subscriber tracks what it has admitted.
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber(_TEST_SERVICE_TYPE)
        uid = uuid.uuid4()
        record = _record(uid)

        # Act
        collected = asyncio.create_task(_collect(subscriber, 2))
        await subscriber_driver.added(record)
        await subscriber_driver.removed(record)
        added, dropped = await collected

        # Assert
        assert added.type == "worker-added"
        assert dropped.type == "worker-dropped"
        assert dropped.metadata.uid == uid

    @pytest.mark.asyncio
    async def test___aiter___should_yield_worker_updated_for_a_tracked_record(
        self, subscriber_driver
    ):
        """Test re-announcing a tracked service becomes a worker-updated event.

        Given:
            A subscriber that has already yielded a worker-added event
            for a record.
        When:
            The same record is re-announced with changed properties.
        Then:
            It should yield a worker-updated event carrying the new
            values, rather than a second worker-added.
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber(_TEST_SERVICE_TYPE)
        uid = uuid.uuid4()

        # Act
        collected = asyncio.create_task(_collect(subscriber, 2))
        await subscriber_driver.added(_record(uid, version="1.0.0"))
        await subscriber_driver.updated(_record(uid, version="2.0.0"))
        added, updated = await collected

        # Assert
        assert added.type == "worker-added"
        assert updated.type == "worker-updated"
        assert updated.metadata.version == "2.0.0"

    @pytest.mark.asyncio
    async def test___aiter___should_promote_an_update_for_an_untracked_record(
        self, subscriber_driver
    ):
        """Test an update for a worker never added arrives as an addition.

        Given:
            A subscriber that has seen no record for a worker.
        When:
            An update is announced for it, as happens when the
            subscriber starts after the worker did.
        Then:
            It should yield worker-added rather than worker-updated, so
            a late subscriber still learns the worker exists.
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber(_TEST_SERVICE_TYPE)
        uid = uuid.uuid4()

        # Act
        collected = asyncio.create_task(_collect(subscriber, 1))
        await subscriber_driver.updated(_record(uid))
        (event,) = await collected

        # Assert
        assert event.type == "worker-added"
        assert event.metadata.uid == uid

    @pytest.mark.asyncio
    async def test___aiter___should_skip_a_malformed_record(self, subscriber_driver):
        """Test an unparsable announcement does not break the stream.

        Given:
            A subscriber being iterated, and a record missing the
            properties every worker must advertise.
        When:
            The malformed record is announced, followed by a valid one.
        Then:
            It should yield only the valid worker, so one bad
            announcement cannot stall discovery for the rest.
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber(_TEST_SERVICE_TYPE)
        malformed = ServiceInfo(
            _TEST_SERVICE_TYPE,
            f"{uuid.uuid4()}.{_TEST_SERVICE_TYPE}",
            addresses=[socket.inet_aton("127.0.0.1")],
            port=9998,
            properties={"garbage": "yes"},
        )
        uid = uuid.uuid4()

        # Act
        collected = asyncio.create_task(_collect(subscriber, 1))
        await subscriber_driver.added(malformed)
        await subscriber_driver.added(_record(uid))
        (event,) = await collected

        # Assert
        assert event.metadata.uid == uid

    @pytest.mark.asyncio
    async def test___aiter___should_yield_to_every_concurrent_iteration(
        self, subscriber_driver
    ):
        """Test concurrent iterations each receive the full stream.

        Given:
            Two concurrent iterations of the same subscriber.
        When:
            A record is announced once.
        Then:
            Both should yield it, since the browser fans out rather
            than handing each event to whichever consumer asks first.
        """
        # Arrange
        subscriber = LanDiscovery.Subscriber(_TEST_SERVICE_TYPE)
        uid = uuid.uuid4()

        # Act
        first = asyncio.create_task(_collect(subscriber, 1))
        second = asyncio.create_task(_collect(subscriber, 1))
        await subscriber_driver.added(_record(uid))
        (one,), (two,) = await asyncio.gather(first, second)

        # Assert
        assert one.metadata.uid == uid
        assert two.metadata.uid == uid

    @pytest.mark.asyncio
    async def test___aiter___should_yield_only_workers_a_filter_admits(
        self, subscriber_driver
    ):
        """Test a filter predicate excludes non-matching workers.

        Given:
            A subscriber wrapped in a predicate admitting one port.
        When:
            A matching and a non-matching worker are both announced.
        Then:
            It should yield only the matching one, so a caller filters
            without seeing what it filtered out.
        """

        # Arrange
        def matches(worker):
            return worker.address.endswith(":50051")

        subscriber = afilter(matches, LanDiscovery.Subscriber(_TEST_SERVICE_TYPE))
        wanted = uuid.uuid4()

        # Act
        collected = asyncio.create_task(_collect(subscriber, 1))
        await subscriber_driver.added(_record(uuid.uuid4(), port=9999))
        await subscriber_driver.added(_record(wanted, port=50051))
        (event,) = await collected

        # Assert
        assert event.metadata.uid == wanted

    @pytest.mark.asyncio
    async def test___aiter___should_drop_a_worker_that_stops_matching(
        self, subscriber_driver
    ):
        """Test a worker whose properties leave a filter is reported dropped.

        Given:
            A subscriber filtering on a tag, and a worker that
            initially carries it.
        When:
            The worker re-announces itself without that tag.
        Then:
            It should yield a synthetic worker-dropped carrying the
            metadata the worker had while it still matched, so a
            consumer tracking the filtered set learns it left rather
            than silently keeping a worker the predicate now rejects.
        """

        # Arrange
        def matches(worker):
            return "gpu" in worker.tags

        subscriber = afilter(matches, LanDiscovery.Subscriber(_TEST_SERVICE_TYPE))
        uid = uuid.uuid4()

        # Act
        collected = asyncio.create_task(_collect(subscriber, 2))
        await subscriber_driver.added(_record(uid, tags='["gpu"]'))
        await subscriber_driver.updated(_record(uid, tags='["cpu"]'))
        added, dropped = await collected

        # Assert
        assert added.type == "worker-added"
        assert dropped.type == "worker-dropped"
        assert dropped.metadata.uid == uid
        assert dropped.metadata.tags == frozenset({"gpu"})
