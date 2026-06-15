from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import socket
import warnings
from asyncio import Queue
from types import MappingProxyType
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Callable
from typing import Dict
from typing import Final
from typing import Tuple
from uuid import UUID
from uuid import uuid4

from zeroconf import IPVersion
from zeroconf import ServiceInfo
from zeroconf import ServiceListener
from zeroconf import Zeroconf
from zeroconf.asyncio import AsyncServiceBrowser
from zeroconf.asyncio import AsyncZeroconf

from wool.exception import WoolError
from wool.exception import WoolWarning
from wool.runtime.discovery.base import Discovery
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoveryEventType
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import PredicateFunction
from wool.runtime.discovery.pool import SubscriberMeta
from wool.runtime.worker.metadata import WorkerMetadata
from wool.utilities.afilter import afilter


# public
class AdvertiseHostError(WoolError):
    """Raised when a routable advertised address cannot be resolved.

    A worker bound to a wildcard address requires automatic resolution
    of a routable advertised host from the default-route interface; on
    a host with no default route (offline or isolated networking)
    there is nothing to advertise. Set ``advertise_host`` explicitly,
    e.g., ``"127.0.0.1"`` for same-host use, or bind the worker to a
    concrete host.
    """


# public
class LoopbackAdvertisementWarning(WoolWarning):
    """Emitted when a LAN publisher advertises a loopback address.

    Off-host subscribers cannot reach a loopback address. The warning
    fires when ``advertise_host`` is unset and a worker's bind host is
    loopback, typically a worker left on `LocalWorker`'s default bind
    while published over LAN discovery. Bind the worker to ``0.0.0.0``
    or set ``advertise_host``. Deliberate same-host use can silence
    the category via `warnings.filterwarnings`.
    """


# public
class LanDiscovery(Discovery):
    """Zeroconf DNS-SD discovery for network-wide worker pools.

    Workers are advertised as DNS-SD service records on the local
    network. Subscribers browse for these services and receive events
    as workers come and go. No central coordinator required.

    Each instance is scoped to a namespace that determines the DNS-SD
    service type used for registration and browsing. Only publishers
    and subscribers sharing the same namespace see each other's
    announcements. When no namespace is provided, a UUID-based
    namespace is auto-generated so that each instance is isolated by
    default.

    :param namespace:
        Namespace identifier for discovery isolation. Publishers
        and subscribers using the same namespace will see each
        other's worker announcements. Defaults to a UUID-based
        auto-generated namespace.
    :param advertise_host:
        Controls the host advertised in worker announcements.

        - ``None`` (default): Advertise the worker's bind host
          verbatim, except when it's a wildcard (``0.0.0.0``,
          ``::``, or empty), in which case a routable address is
          resolved automatically from the default-route interface.
        - ``str``: Advertise this host verbatim (a container bridge
          IP, a specific NIC, a NAT/proxy front address, etc.).
        - ``callable``: Invoked lazily on the first publish event to
          produce the host; the result is cached for the publisher's
          lifetime.

        The advertised host must never be a wildcard or empty string.
        Callables and hostname values are resolved synchronously on
        the event loop on the first publish event. Keep callables fast
        and prefer IP literals.
    :param filter:
        Optional default predicate function to filter workers.
        Used by `LanDiscovery.subscriber` and as the default for
        `LanDiscovery.subscribe` when no explicit filter is provided.

    Example — publish workers:

    .. code-block:: python

        async with LanDiscovery("my-pool").publisher as publisher:
            await publisher.publish("worker-added", metadata)

    Example — subscribe to workers:

    .. code-block:: python

        discovery = LanDiscovery("my-pool")
        async for event in discovery.subscriber:
            print(f"Discovered worker: {event.metadata}")

    Example — advertise a NAT/proxy front address:

    .. code-block:: python

        discovery = LanDiscovery("my-pool", advertise_host="10.0.0.5")
    """

    _namespace: Final[str]
    _advertise_host: Final[str | Callable[[], str] | None]
    _filter: Final[PredicateFunction | None]
    _service_type: Final[str]

    def __init__(
        self,
        namespace: str | None = None,
        *,
        advertise_host: str | Callable[[], str] | None = None,
        filter: PredicateFunction | None = None,
    ):
        if isinstance(advertise_host, str) and _is_wildcard(advertise_host):
            raise ValueError(
                f"Advertised host must not be a wildcard or empty: {advertise_host!r}"
            )
        self._namespace = namespace or f"pool-{uuid4().hex}"
        self._advertise_host = advertise_host
        self._filter = filter
        self._service_type = _namespaced_service_type(self._namespace)

    def __hash__(self) -> int:
        return hash((type(self), self._namespace))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LanDiscovery):
            return self._namespace == other._namespace
        return NotImplemented

    @property
    def namespace(self) -> str:
        """The namespace identifier for this discovery service.

        :returns:
            The namespace string.
        """
        return self._namespace

    @property
    def publisher(self) -> DiscoveryPublisherLike:
        """A new publisher instance for this discovery service.

        :returns:
            A publisher instance for broadcasting worker events.
        """
        return self.Publisher(self._service_type, advertise_host=self._advertise_host)

    @property
    def subscriber(self) -> DiscoverySubscriberLike:
        """A subscriber using the constructor's default filter.

        :returns:
            A subscriber instance for receiving worker discovery
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
            events. Falls back to the constructor's filter if not
            provided.
        :returns:
            A subscriber instance that receives filtered worker
            discovery events.
        """
        effective = filter if filter is not None else self._filter
        subscriber = self.Subscriber(self._service_type)
        if effective is not None:
            return afilter(effective, subscriber)
        return subscriber

    class Publisher:
        """Publisher for broadcasting worker discovery events.

        Publishes worker discovery events (see `~wool.DiscoveryEvent`)
        by registering and managing DNS-SD service records on the local
        network. Multiple publishers can safely operate on the same
        network, each advertising their own set of workers.

        Uses `~zeroconf.asyncio.AsyncZeroconf` for non-blocking service
        registration and management. Services are advertised on all
        network interfaces so announcements reach off-host subscribers.

        :param service_type:
            The DNS-SD service type string for this namespace.
        :param advertise_host:
            Optional advertised-host override. See `LanDiscovery` for
            the accepted forms and their semantics. Resolution is lazy,
            i.e., deferred to the first publish event, and cached for
            the publisher's lifetime.
        """

        service_type: str
        aiozc: AsyncZeroconf | None
        services: Dict[str, ServiceInfo]

        _advertise_host: str | Callable[[], str] | None
        _resolved_advertise_host: str | None

        #: LAN-advertised workers must accept off-host connections, so
        #: this publisher prescribes the wildcard bind. See
        #: `~wool.DiscoveryPublisherLike.bind_host` for the contract.
        bind_host: str = "0.0.0.0"

        def __init__(
            self,
            service_type: str,
            *,
            advertise_host: str | Callable[[], str] | None = None,
        ):
            if isinstance(advertise_host, str) and _is_wildcard(advertise_host):
                raise ValueError(
                    f"Advertised host must not be a wildcard or empty: "
                    f"{advertise_host!r}"
                )
            self.service_type = service_type
            self.aiozc = None
            self.services = {}
            self._advertise_host = advertise_host
            self._resolved_advertise_host = None

        async def __aenter__(self):
            """Initialize and start the Zeroconf instance.

            :returns:
                Self, for context manager usage.
            """
            self.aiozc = AsyncZeroconf()
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

            - ``worker-added``: Registers a new service record
            - ``worker-dropped``: Unregisters an existing service record
            - ``worker-updated``: Updates an existing service record

            Emits `LoopbackAdvertisementWarning` when registering a
            worker whose advertised address is loopback with no
            ``advertise_host`` override.

            :param type:
                The type of discovery event.
            :param metadata:
                Worker metadata to publish.
            :raises RuntimeError:
                If the publisher is not properly initialized or if an
                unexpected event type is provided.
            :raises ValueError:
                If the advertised host resolves to a wildcard or empty
                value, or the worker's address is malformed.
            :raises AdvertiseHostError:
                If a wildcard bind requires auto-resolution and no
                routable address is available.
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
            """
            assert self.aiozc

            host, port = self._advertise_host_port(metadata.address)
            ip_address = self._resolve_host(host)
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

            Unknown workers are ignored.

            :param metadata:
                The worker to unregister.
            """
            assert self.aiozc

            uid_str = str(metadata.uid)
            if uid_str in self.services:
                service = self.services[uid_str]
                await self.aiozc.async_unregister_service(service)
                del self.services[uid_str]

        async def _update(self, metadata: WorkerMetadata) -> None:
            """Update a worker's properties if they have changed.

            Unknown workers are treated as registrations. For known
            workers the cached service addresses are reused, so an
            update never re-resolves the advertised host. If the
            Zeroconf update fails, the local cache remains unchanged
            to maintain consistency.

            :param metadata:
                The updated worker metadata.
            """
            assert self.aiozc

            uid_str = str(metadata.uid)
            if uid_str not in self.services:
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

        def _advertise_host_port(self, address: str) -> Tuple[str, int]:
            """Determine the advertised host and port for a bind address.

            Applies the ``advertise_host`` policy documented on
            `LanDiscovery`, caching lazily-resolved hosts for the
            publisher's lifetime.

            :param address:
                Bind address in ``host:port`` or ``[host]:port`` format.
            :returns:
                Tuple of (advertised host, port as int).
            :raises ValueError:
                If the advertised host resolves to a wildcard or
                empty value.
            :raises AdvertiseHostError:
                If a wildcard bind requires auto-resolution and no
                routable address is available.
            """
            host, port = _split_host_port(address)

            if callable(self._advertise_host):
                if self._resolved_advertise_host is None:
                    self._resolved_advertise_host = self._advertise_host()
                host = self._resolved_advertise_host
            elif self._advertise_host is not None:
                host = self._advertise_host
            elif _is_wildcard(host):
                if self._resolved_advertise_host is None:
                    try:
                        self._resolved_advertise_host = _default_route_ip()
                    except OSError as error:
                        raise AdvertiseHostError(
                            "Cannot auto-resolve a routable address to "
                            f"advertise for wildcard bind {host!r}: {error}. "
                            "Set advertise_host explicitly ('127.0.0.1' for "
                            "same-host use) or bind the worker to a concrete "
                            "host."
                        ) from error
                host = self._resolved_advertise_host

            if _is_wildcard(host):
                raise ValueError(
                    f"Advertised host must not be a wildcard or empty: {host!r}"
                )
            if self._advertise_host is None and _is_loopback(host):
                warnings.warn(
                    f"Advertising loopback address {host!r} over LAN "
                    "discovery; off-host subscribers cannot reach it. Bind "
                    "the worker to '0.0.0.0' or set advertise_host.",
                    LoopbackAdvertisementWarning,
                    stacklevel=2,
                )
            return host, port

        def _resolve_host(self, host: str) -> bytes:
            """Resolve a host string to packed address bytes.

            :param host:
                IPv4/IPv6 literal or hostname.
            :returns:
                IPv4/IPv6 address as bytes.
            :raises OSError:
                If hostname cannot be resolved.
            """
            try:
                return socket.inet_pton(socket.AF_INET, host)
            except OSError:
                pass

            try:
                return socket.inet_pton(socket.AF_INET6, host)
            except OSError:
                pass

            return socket.inet_aton(socket.gethostbyname(host))

    class Subscriber(
        metaclass=SubscriberMeta,
        key=lambda cls, service_type: (cls, service_type),
    ):
        """Subscriber for receiving worker discovery events.

        Subscribes to worker discovery events (see `~wool.DiscoveryEvent`)
        by browsing for DNS-SD services on the local network. As workers
        register and unregister their services, the subscriber yields
        corresponding events.

        Instances are cached as singletons — two calls with the same
        ``service_type`` return the same object.

        Each call to ``__aiter__`` creates an isolated consumer fed from a
        Zeroconf browser shared across consumers of the same service type.
        The shared browser fans out, i.e., every concurrent iteration
        receives the full event stream, and the iterations are otherwise
        independent.

        :param service_type:
            The DNS-SD service type string for this namespace.
        """

        service_type: str

        def __init__(self, service_type: str) -> None:
            self.service_type = service_type

        def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
            return self._event_stream()

        async def _shutdown(self) -> None:
            """Release subscriber-level resources at cache eviction.

            LAN subscribers hold none — each iteration's event stream
            owns and releases its own Zeroconf resources.
            """

        async def _event_stream(self) -> AsyncGenerator[DiscoveryEvent, None]:
            """Stream discovery events from the network.

            Creates isolated state for this iteration including its own
            Zeroconf instance, service browser, event queue, and service
            cache. Automatically cleans up all resources when iteration
            completes or is interrupted.

            :yields:
                Discovery events as workers are added, updated, or
                removed.
            """
            event_queue: Queue[DiscoveryEvent] = Queue()
            service_cache: Dict[str, WorkerMetadata] = {}

            aiozc = AsyncZeroconf()

            try:
                browser = AsyncServiceBrowser(
                    aiozc.zeroconf,
                    self.service_type,
                    listener=self._Listener(
                        service_type=self.service_type,
                        aiozc=aiozc,
                        event_queue=event_queue,
                        service_cache=service_cache,
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

            :param service_type:
                The DNS-SD service type string for this namespace.
            :param aiozc:
                The AsyncZeroconf instance to use for async service
                info retrieval.
            :param event_queue:
                Queue to deliver discovery events to.
            :param service_cache:
                Cache to track service properties for pre/post event
                states.
            """

            aiozc: AsyncZeroconf
            _event_queue: Queue[DiscoveryEvent]
            _service_cache: Dict[str, WorkerMetadata]

            def __init__(
                self,
                service_type: str,
                aiozc: AsyncZeroconf,
                event_queue: Queue[DiscoveryEvent],
                service_cache: Dict[str, WorkerMetadata],
            ) -> None:
                self._service_type = service_type
                self.aiozc = aiozc
                self._event_queue = event_queue
                self._service_cache = service_cache

            def add_service(self, zc: Zeroconf, type_: str, name: str):  # noqa: ARG002
                """Schedule async handling of a matching service addition.

                Invoked by Zeroconf via the `~zeroconf.ServiceListener` protocol.
                """
                if type_ == self._service_type:
                    asyncio.create_task(self._handle_add_service(type_, name))

            def remove_service(self, zc: Zeroconf, type_: str, name: str):  # noqa: ARG002
                """Emit a worker-dropped event for a tracked service.

                Invoked by Zeroconf via the `~zeroconf.ServiceListener` protocol.
                Untracked services are ignored.
                """
                if type_ == self._service_type:
                    if worker := self._service_cache.pop(name, None):
                        asyncio.create_task(
                            self._event_queue.put(
                                DiscoveryEvent("worker-dropped", metadata=worker)
                            )
                        )

            def update_service(self, zc: Zeroconf, type_, name):  # noqa: ARG002
                """Schedule async handling of a matching service update.

                Invoked by Zeroconf via the `~zeroconf.ServiceListener` protocol.
                """
                if type_ == self._service_type:
                    asyncio.create_task(self._handle_update_service(type_, name))

            async def _handle_add_service(self, type_: str, name: str):
                """Fetch a service's info and emit a worker-added event.

                Unresolvable or malformed records are skipped silently
                so a bad announcement never breaks the event stream.
                """
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

                    self._service_cache[name] = metadata
                    await self._event_queue.put(
                        DiscoveryEvent("worker-added", metadata=metadata)
                    )
                except Exception:  # pragma: no cover
                    pass

            async def _handle_update_service(self, type_: str, name: str):
                """Fetch a service's info and emit an update event.

                Updates for untracked services promote to worker-added
                events. Unresolvable or malformed records are skipped
                silently so a bad announcement never breaks the event
                stream.
                """
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
                        self._service_cache[name] = metadata
                        await self._event_queue.put(
                            DiscoveryEvent("worker-added", metadata=metadata)
                        )
                    else:
                        self._service_cache[name] = metadata
                        await self._event_queue.put(
                            DiscoveryEvent("worker-updated", metadata=metadata)
                        )
                except Exception:  # pragma: no cover
                    pass


def _split_host_port(address: str) -> Tuple[str, int]:
    """Split an address string into host and port.

    Handles both ``host:port`` and the bracketed IPv6 form
    ``[host]:port``.

    :param address:
        Address in ``host:port`` or ``[host]:port`` format.
    :returns:
        Tuple of (host, port as int).
    :raises ValueError:
        If the address has no port separator or the port is not an
        integer.
    """
    host, separator, port_str = address.rpartition(":")
    if not separator:
        raise ValueError(f"Address has no port separator: {address!r}")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    return host, int(port_str)


def _is_wildcard(host: str) -> bool:
    """Whether a host is a wildcard or empty bind address.

    Detects every spelling of the unspecified address (``0.0.0.0``,
    ``::``, ``::0``, ...), not just the canonical forms.

    :param host:
        Host string to check.
    :returns:
        True for an unspecified IP address or an empty host.
    """
    if host == "":
        return True
    try:
        return ipaddress.ip_address(host).is_unspecified
    except ValueError:
        return False


def _is_loopback(host: str) -> bool:
    """Whether a host is a loopback address or name.

    :param host:
        Host string to check.
    :returns:
        True for ``localhost``, ``127.0.0.0/8`` literals, or ``::1``.
    """
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _default_route_ip() -> str:
    """Resolve a routable IP from the default-route interface.

    Opens a UDP socket toward a public address — no packets are sent —
    so the OS selects the egress interface for the default route. This
    avoids ``socket.gethostbyname(socket.gethostname())``, which
    returns ``127.0.1.1`` on stock Debian hosts.

    :returns:
        The IPv4 address of the default-route interface.
    """
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("8.8.8.8", 80))
        return probe.getsockname()[0]
    finally:
        probe.close()


def _namespaced_service_type(namespace: str) -> str:
    """Derive a DNS-SD service type from a namespace string.

    Produces a deterministic service type by appending a short hash of
    the namespace to the base ``_wool`` label. The resulting label is
    at most 13 characters, within the DNS-SD 15-character limit.

    :param namespace:
        The namespace identifier.
    :returns:
        A service type string like ``_wool-a1b2c3._tcp.local.``.
    """
    short = hashlib.md5(namespace.encode(), usedforsecurity=False).hexdigest()[:6]
    return f"_wool-{short}._tcp.local."


def _serialize_metadata(
    info: WorkerMetadata,
) -> dict[str, str | None]:
    """Serialize WorkerMetadata to a flat dict for service properties.

    :param info:
        WorkerMetadata instance to serialize.
    :returns:
        Flat dict with pid, version, tags (JSON), extra (JSON), secure.
    """
    properties = {
        "pid": str(info.pid),
        "version": info.version,
        "tags": (json.dumps(list(info.tags)) if info.tags else None),
        "extra": (json.dumps(dict(info.extra)) if info.extra else None),
        "secure": "true" if info.secure else "false",
    }
    return properties


def _deserialize_metadata(info: ServiceInfo) -> WorkerMetadata:
    """Deserialize ServiceInfo.decoded_properties to WorkerMetadata.

    The worker UID is recovered from the service name, which follows
    the ``<uuid>.<service_type>`` format established by the publisher.

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

    # Absent on records from versions predating the security flag;
    # default to insecure for backward compatibility.
    secure = False
    if "secure" in properties and properties["secure"]:
        secure = properties["secure"].lower() == "true"

    service_name = info.name
    uid_str = service_name.split(".")[0]

    ip = info.ip_addresses_by_version(IPVersion.V4Only)[0]
    return WorkerMetadata(
        uid=UUID(uid_str),
        address=f"{ip}:{info.port}",
        pid=pid,
        version=version,
        tags=tags,
        extra=MappingProxyType(extra),
        secure=secure,
    )
