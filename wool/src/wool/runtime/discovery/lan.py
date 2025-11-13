from __future__ import annotations

import asyncio
import json
import socket
from asyncio import Queue
from types import MappingProxyType
from typing import AsyncIterator
from typing import Dict
from typing import Final
from typing import Literal
from typing import Tuple
from uuid import UUID

from zeroconf import IPVersion
from zeroconf import ServiceInfo
from zeroconf import ServiceListener
from zeroconf import Zeroconf
from zeroconf.asyncio import AsyncServiceBrowser
from zeroconf.asyncio import AsyncZeroconf

from wool.runtime.discovery.base import Discovery
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoveryEventType
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import PredicateFunction
from wool.runtime.discovery.base import WorkerMetadata


# public
class LanDiscovery(Discovery):
    """Worker discovery on the local network using Zeroconf/Bonjour.

    Provides network-wide worker discovery using DNS Service Discovery
    (DNS-SD) via the Zeroconf protocol. Workers are automatically
    discovered as they join or leave the network without requiring
    central coordination.

    The service type "_wool._tcp.local." is used for all Wool worker
    services on the LAN. Publishers advertise workers by registering
    DNS-SD service records, and subscribers browse for these services.

    Example usage:

    Publish workers
    .. code-block:: python
        publisher = LanDiscovery.Publisher()
        async with publisher:
            await publisher.publish("worker-added", metadata)

    Subscribe to workers
    .. code-block:: python
        discovery = LanDiscovery()
        async for event in discovery.subscriber:
            print(f"Discovered worker: {event.metadata}")
    """

    service_type: Literal["_wool._tcp.local."] = "_wool._tcp.local."

    @property
    def publisher(self) -> DiscoveryPublisherLike:
        """A new publisher instance for this discovery service.

        :returns:
            A publisher instance for broadcasting worker events.
        """
        return self.Publisher()

    @property
    def subscriber(self) -> DiscoverySubscriberLike:
        """The default subscriber that receives all worker events.

        :returns:
            A subscriber instance that receives all worker discovery
            events.
        """
        return self.subscribe()

    def subscribe(
        self, filter: PredicateFunction | None = None
    ) -> DiscoverySubscriberLike:
        """Create a new subscriber with optional filtering.

        :param filter:
            Optional predicate function to filter workers. Only workers
            for which the predicate returns True will be included in
            events.
        :returns:
            A subscriber instance that receives filtered worker
            discovery events.
        """
        return self.Subscriber(filter)

    class Publisher:
        """Publisher for broadcasting worker discovery events.

        Publishes worker :class:`discovery events <~wool.DiscoveryEvent>`
        by registering and managing DNS-SD service records on the local
        network. Multiple publishers can safely operate on the same
        network, each advertising their own set of workers.

        Uses AsyncZeroconf for non-blocking service registration and
        management. Services are advertised on localhost (127.0.0.1) to
        avoid network warnings during development.
        """

        aiozc: AsyncZeroconf | None
        services: Dict[str, ServiceInfo]
        service_type: Literal["_wool._tcp.local."] = "_wool._tcp.local."

        def __init__(self):
            self.aiozc = None
            self.services = {}

        async def __aenter__(self):
            """Initialize and start the Zeroconf instance.

            Configures AsyncZeroconf to use localhost only to avoid
            network warnings during development.

            :returns:
                Self, for context manager usage.
            """
            # Configure zeroconf to use localhost only
            self.aiozc = AsyncZeroconf(interfaces=["127.0.0.1"])
            return self

        async def __aexit__(self, *_args):
            """Stop Zeroconf and clean up registered services.

            Closes the AsyncZeroconf instance and releases all registered
            service records.
            """
            if self.aiozc:
                await self.aiozc.async_close()
                self.aiozc = None

        async def publish(self, type: DiscoveryEventType, metadata: WorkerMetadata):
            """Publish a worker discovery event.

            Manages Zeroconf service records based on the event type:

            - worker-added: Registers a new service record
            - worker-dropped: Unregisters an existing service record
            - worker-updated: Updates an existing service record

            :param type:
                The type of discovery event.
            :param metadata:
                Worker metadata to publish.
            :raises RuntimeError:
                If the publisher is not properly initialized or if an
                unexpected event type is provided.
            """
            if self.aiozc is None:
                raise RuntimeError("Publisher not properly initialized")

            match type:
                case "worker-added":
                    await self._add(metadata)
                case "worker-dropped":
                    await self._drop(metadata)
                case "worker-updated":
                    await self._update(metadata)
                case _:
                    raise RuntimeError(f"Unexpected discovery event type: {type}")

        async def _add(self, metadata: WorkerMetadata) -> None:
            """Register a worker by publishing its service info.

            :param metadata:
                The worker details to publish.
            :raises RuntimeError:
                If the publisher is not properly initialized.
            :raises ValueError:
                If worker port is not specified.
            """
            assert self.aiozc

            if metadata.port is None:
                raise ValueError("Worker port must be specified for LAN discovery")

            address = f"{metadata.host}:{metadata.port}"
            ip_address, port = self._resolve_address(address)
            service_name = f"{metadata.uid}.{self.service_type}"
            service_info = ServiceInfo(
                self.service_type,
                service_name,
                addresses=[ip_address],
                port=port,
                properties=_serialize_metadata(metadata),
            )
            self.services[str(metadata.uid)] = service_info
            await self.aiozc.async_register_service(service_info)

        async def _drop(self, metadata: WorkerMetadata) -> None:
            """Unregister a worker by removing its service record.

            :param metadata:
                The worker to unregister.
            :raises RuntimeError:
                If the publisher is not properly initialized.
            """
            assert self.aiozc

            uid_str = str(metadata.uid)
            if uid_str in self.services:
                service = self.services[uid_str]
                await self.aiozc.async_unregister_service(service)
                del self.services[uid_str]

        async def _update(self, metadata: WorkerMetadata) -> None:
            """Update a worker's properties if they have changed.

            Updates both the Zeroconf service and local cache
            atomically. If the Zeroconf update fails, the local cache
            remains unchanged to maintain consistency.

            :param metadata:
                The updated worker metadata.
            :raises RuntimeError:
                If the publisher is not properly initialized.
            :raises Exception:
                If the Zeroconf service update fails.
            """
            assert self.aiozc

            uid_str = str(metadata.uid)
            if uid_str not in self.services:
                # Worker not found, treat as registration
                await self._add(metadata)
                return

            service = self.services[uid_str]
            new_properties = _serialize_metadata(metadata)

            if service.decoded_properties != new_properties:
                updated_service = ServiceInfo(
                    service.type,
                    service.name,
                    addresses=service.addresses,
                    port=service.port,
                    properties=new_properties,
                    server=service.server,
                )
                await self.aiozc.async_update_service(updated_service)
                self.services[uid_str] = updated_service

        def _resolve_address(self, address: str) -> Tuple[bytes, int]:
            """Resolve an address string to bytes and validate port.

            :param address:
                Address in format "host:port".
            :returns:
                Tuple of (IPv4/IPv6 address as bytes, port as int).
            :raises ValueError:
                If address format is invalid or port is out of range.
            :raises OSError:
                If hostname cannot be resolved.
            """
            host, port_str = address.split(":")
            port = int(port_str)

            try:
                return socket.inet_pton(socket.AF_INET, host), port
            except OSError:
                pass

            try:
                return socket.inet_pton(socket.AF_INET6, host), port
            except OSError:
                pass

            return socket.inet_aton(socket.gethostbyname(host)), port

    class Subscriber:
        """Subscriber for receiving worker discovery events.

        Subscribes to worker :class:`discovery events
        <~wool.DiscoveryEvent>` by browsing for DNS-SD services on the
        local network. As workers register and unregister their
        services, the subscriber yields corresponding events.

        Each call to ``__aiter__`` creates an isolated iterator with its
        own state. Multiple concurrent iterations from the same
        subscriber instance are fully independent.

        Uses AsyncZeroconf's service browser to monitor for service
        changes and converts Zeroconf events into Wool discovery
        events.

        :param filter:
            Optional predicate function to filter workers. Only workers
            for which the predicate returns True will be included in
            events.
        """

        _filter: Final[PredicateFunction[WorkerMetadata] | None]
        service_type: Literal["_wool._tcp.local."] = "_wool._tcp.local."

        def __init__(
            self,
            filter: PredicateFunction[WorkerMetadata] | None = None,
        ) -> None:
            self._filter = filter

        def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
            return self._event_stream()

        async def _event_stream(self) -> AsyncIterator[DiscoveryEvent]:
            """Stream discovery events from the network.

            Creates isolated state for this iteration including its own
            Zeroconf instance, service browser, event queue, and service
            cache. Automatically cleans up all resources when iteration
            completes or is interrupted.

            :yields:
                Discovery events as workers are added, updated, or removed.
            """
            # Create isolated state for this iterator
            event_queue: Queue[DiscoveryEvent] = Queue()
            service_cache: Dict[str, WorkerMetadata] = {}

            # Configure zeroconf to use localhost only to avoid network warnings
            aiozc = AsyncZeroconf(interfaces=["127.0.0.1"])

            try:
                browser = AsyncServiceBrowser(
                    aiozc.zeroconf,
                    self.service_type,
                    listener=self._Listener(
                        aiozc=aiozc,
                        event_queue=event_queue,
                        service_cache=service_cache,
                        predicate=self._filter or (lambda _: True),
                    ),
                )

                try:
                    while True:
                        event = await event_queue.get()
                        yield event
                finally:
                    await browser.async_cancel()
            finally:
                await aiozc.async_close()

        class _Listener(ServiceListener):
            """Zeroconf listener that delivers worker service events.

            :param aiozc:
                The AsyncZeroconf instance to use for async service
                info retrieval.
            :param event_queue:
                Queue to deliver discovery events to.
            :param service_cache:
                Cache to track service properties for pre/post event
                states.
            :param predicate:
                Function to filter which workers to track.
            """

            aiozc: AsyncZeroconf
            _event_queue: Queue[DiscoveryEvent]
            _service_addresses: Dict[str, str]
            _service_cache: Dict[str, WorkerMetadata]

            def __init__(
                self,
                aiozc: AsyncZeroconf,
                event_queue: Queue[DiscoveryEvent],
                predicate: PredicateFunction[WorkerMetadata],
                service_cache: Dict[str, WorkerMetadata],
            ) -> None:
                self.aiozc = aiozc
                self._event_queue = event_queue
                self._predicate = predicate
                self._service_addresses = {}
                self._service_cache = service_cache

            def add_service(self, zc: Zeroconf, type_: str, name: str):  # noqa: ARG002
                """Called by Zeroconf when a service is added."""
                if type_ == LanDiscovery.service_type:
                    asyncio.create_task(self._handle_add_service(type_, name))

            def remove_service(self, zc: Zeroconf, type_: str, name: str):  # noqa: ARG002
                """Called by Zeroconf when a service is removed."""
                if type_ == LanDiscovery.service_type:
                    if worker := self._service_cache.pop(name, None):
                        asyncio.create_task(
                            self._event_queue.put(
                                DiscoveryEvent("worker-dropped", metadata=worker)
                            )
                        )

            def update_service(self, zc: Zeroconf, type_, name):  # noqa: ARG002
                """Called by Zeroconf when a service is updated."""
                if type_ == LanDiscovery.service_type:
                    asyncio.create_task(self._handle_update_service(type_, name))

            async def _handle_add_service(self, type_: str, name: str):
                """Async handler for service addition."""
                try:
                    if not (
                        service_info := await self.aiozc.async_get_service_info(
                            type_, name
                        )
                    ):
                        return

                    try:
                        metadata = _deserialize_metadata(service_info)
                    except ValueError:
                        return

                    if self._predicate(metadata):
                        self._service_cache[name] = metadata
                        event = DiscoveryEvent("worker-added", metadata=metadata)
                        await self._event_queue.put(event)
                except Exception:  # pragma: no cover
                    pass

            async def _handle_update_service(self, type_: str, name: str):
                """Async handler for service update."""
                try:
                    if not (
                        service_info := await self.aiozc.async_get_service_info(
                            type_, name
                        )
                    ):
                        return

                    try:
                        metadata = _deserialize_metadata(service_info)
                    except ValueError:
                        return

                    if name not in self._service_cache:
                        # New worker that wasn't tracked before
                        if self._predicate(metadata):
                            self._service_cache[name] = metadata
                            event = DiscoveryEvent("worker-added", metadata=metadata)
                            await self._event_queue.put(event)
                    else:
                        # Existing tracked worker
                        old_worker = self._service_cache[name]
                        if self._predicate(metadata):
                            # Still satisfies filter, update cache and emit update
                            self._service_cache[name] = metadata
                            event = DiscoveryEvent("worker-updated", metadata=metadata)
                            await self._event_queue.put(event)
                        else:
                            # No longer satisfies filter, remove and emit removal
                            del self._service_cache[name]
                            removal_event = DiscoveryEvent(
                                "worker-dropped", metadata=old_worker
                            )
                            await self._event_queue.put(removal_event)

                except Exception:  # pragma: no cover
                    pass


def _serialize_metadata(
    info: WorkerMetadata,
) -> dict[str, str | None]:
    """Serialize WorkerMetadata to a flat dict for service properties.

    :param info:
        WorkerMetadata instance to serialize.
    :returns:
        Flat dict with pid, version, tags (JSON), extra (JSON).
    """
    properties = {
        "pid": str(info.pid),
        "version": info.version,
        "tags": (json.dumps(list(info.tags)) if info.tags else None),
        "extra": (json.dumps(dict(info.extra)) if info.extra else None),
    }
    return properties


def _deserialize_metadata(info: ServiceInfo) -> WorkerMetadata:
    """Deserialize ServiceInfo.decoded_properties to WorkerMetadata.

    :param info:
        ServiceInfo with decoded properties dict (str keys/values).
    :returns:
        WorkerMetadata instance.
    :raises ValueError:
        If required fields are missing or invalid JSON.
    """
    properties = info.decoded_properties
    if missing := {"pid", "version"} - set(k for k, v in properties.items() if v):
        missing_fields = ", ".join(missing)
        raise ValueError(f"Missing required properties: {missing_fields}")

    assert "pid" in properties and properties["pid"]
    assert "version" in properties and properties["version"]

    pid = int(properties["pid"])
    version = properties["version"]

    if "tags" in properties and properties["tags"]:
        tags = frozenset(json.loads(properties["tags"]))
    else:
        tags = frozenset()

    if "extra" in properties and properties["extra"]:
        extra = json.loads(properties["extra"])
    else:
        extra = {}

    # Extract UID from service name (format: "<uuid>._wool._tcp.local.")
    service_name = info.name
    uid_str = service_name.split(".")[0]

    return WorkerMetadata(
        uid=UUID(uid_str),
        pid=pid,
        host=str(info.ip_addresses_by_version(IPVersion.V4Only)[0]),
        port=info.port,
        version=version,
        tags=tags,
        extra=MappingProxyType(extra),
    )
