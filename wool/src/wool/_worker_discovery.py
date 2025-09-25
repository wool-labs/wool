from __future__ import annotations

import asyncio
import hashlib
import json
import multiprocessing.shared_memory
import socket
import struct
from abc import ABC
from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import AsyncContextManager
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import ContextManager
from typing import Deque
from typing import Dict
from typing import Generic
from typing import Literal
from typing import Protocol
from typing import Tuple
from typing import TypeAlias
from typing import TypeVar
from typing import final
from typing import runtime_checkable

if TYPE_CHECKING:
    pass

from zeroconf import IPVersion
from zeroconf import ServiceInfo
from zeroconf import ServiceListener
from zeroconf import Zeroconf
from zeroconf.asyncio import AsyncServiceBrowser
from zeroconf.asyncio import AsyncZeroconf

if TYPE_CHECKING:
    pass


# public
@dataclass
class WorkerInfo:
    """Properties and metadata for a worker instance.

    Contains identifying information and capabilities of a worker that
    can be used for discovery, filtering, and routing decisions.

    :param uid:
        Unique identifier for the worker instance.
    :param host:
        Network host address where the worker is accessible.
    :param port:
        Network port number where the worker is listening.
    :param pid:
        Process ID of the worker.
    :param version:
        Version string of the worker software.
    :param tags:
        Set of capability tags for worker filtering and selection.
    :param extra:
        Additional arbitrary metadata as key-value pairs.
    """

    uid: str
    host: str
    port: int | None
    pid: int
    version: str
    tags: set[str] = field(default_factory=set)
    extra: dict[str, Any] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash(self.uid)


# public
DiscoveryEventType: TypeAlias = Literal[
    "worker_added", "worker_removed", "worker_updated"
]

_T = TypeVar("_T")
PredicateFunction: TypeAlias = Callable[[_T], bool]


class PredicatedQueue(Generic[_T]):
    """An asyncio queue that supports predicated gets.

    Items can be retrieved only if they match a predicate function.
    Non-matching items remain in the queue for future gets. This allows
    selective consumption from a shared queue based on item properties.

    :param maxsize:
        Maximum number of items in the queue (0 for unlimited).
    """

    _maxsize: int
    _queue: Deque[_T]
    _getters: Deque[Tuple[asyncio.Future[_T], PredicateFunction[_T] | None]]
    _putters: Deque[Tuple[asyncio.Future[None], _T]]
    _unfinished_tasks: int
    _finished: asyncio.Event

    def __init__(self, maxsize: int = 0):
        self._maxsize = maxsize
        self._queue: Deque[_T] = deque()
        self._getters: Deque[Tuple[asyncio.Future[_T], PredicateFunction[_T] | None]] = (
            deque()
        )
        self._putters: Deque[Tuple[asyncio.Future[None], _T]] = deque()
        self._unfinished_tasks = 0
        self._finished = asyncio.Event()
        self._finished.set()

    def qsize(self) -> int:
        """Number of items in the queue."""
        return len(self._queue)

    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise."""
        return not self._queue

    def full(self) -> bool:
        """Return True if there are maxsize items in the queue."""
        if self._maxsize <= 0:
            return False
        return self.qsize() >= self._maxsize

    async def put(self, item: _T) -> None:
        """Put an item into the queue.

        If the queue is full, wait until a free slot is available.
        """
        while self.full():
            putter_future: asyncio.Future[None] = asyncio.Future()
            self._putters.append((putter_future, item))
            try:
                await putter_future
                return
            except asyncio.CancelledError:
                putter_future.cancel()
                try:
                    self._putters.remove((putter_future, item))
                except ValueError:
                    pass
                raise

        self._put_nowait(item)

    def put_nowait(self, item: _T) -> None:
        """Put an item into the queue without blocking.

        :raises QueueFull:
            If no free slot is immediately available.
        """
        if self.full():
            raise asyncio.QueueFull
        self._put_nowait(item)

    def _put_nowait(self, item: _T) -> None:
        """Internal method to put item without capacity checks."""
        self._queue.append(item)
        self._unfinished_tasks += 1
        self._finished.clear()
        self._wakeup_next_getter(item)

    async def get(self, predicate: PredicateFunction[_T] | None = None) -> _T:
        """Remove and return an item from the queue that matches the predicate.

        If predicate is None, return the first available item. If predicate is
        provided, return the first item that makes predicate(item) True. Items
        that don't match the predicate remain in the queue.

        If no matching item is available, wait until one becomes available.

        :param predicate:
            Optional function to filter items.
        :returns:
            Item that matches the predicate.
        """
        while True:
            # Try to find a matching item in the current queue
            item = self._get_matching_item(predicate)
            if item is not None:
                self._wakeup_next_putter()
                return item

            # No matching item found, wait for new items
            getter_future: asyncio.Future[_T] = asyncio.Future()
            self._getters.append((getter_future, predicate))
            try:
                return await getter_future
            except asyncio.CancelledError:
                getter_future.cancel()
                try:
                    self._getters.remove((getter_future, predicate))
                except ValueError:
                    pass
                raise

    def get_nowait(self, predicate: PredicateFunction[_T] | None = None) -> _T:
        """Remove and return an item immediately that matches the predicate.

        :param predicate:
            Optional function to filter items.
        :returns:
            Item that matches the predicate.
        :raises QueueEmpty:
            If no matching item is immediately available.
        """
        item = self._get_matching_item(predicate)
        if item is None:
            raise asyncio.QueueEmpty
        self._wakeup_next_putter()
        return item

    def _get_matching_item(self, predicate: PredicateFunction[_T] | None) -> _T | None:
        """Find and remove the first item that matches the predicate."""
        if not self._queue:
            return None

        if predicate is None:
            # No predicate, return first available item
            return self._queue.popleft()

        # Search for matching item
        for i, item in enumerate(self._queue):
            if predicate(item):
                # Found matching item, remove it from queue
                del self._queue[i]
                return item

        return None

    def _wakeup_next_getter(self, item: _T) -> None:
        """Try to satisfy waiting getters with the new item."""
        remaining_getters = deque()
        item_consumed = False

        while self._getters and not item_consumed:
            getter_future, getter_predicate = self._getters.popleft()

            if getter_future.done():
                continue

            # Check if this getter's predicate matches the item
            if getter_predicate is None or getter_predicate(item):
                # This getter can take the item
                # Try to remove the item from queue and satisfy the getter
                try:
                    self._queue.remove(item)
                    getter_future.set_result(item)
                    item_consumed = True
                    # Item was successfully given to a getter, we're done
                    break
                except ValueError:
                    # Item was already taken by another operation
                    # Continue to next getter, but don't try to give them this item
                    remaining_getters.append((getter_future, getter_predicate))
            else:
                # This getter's predicate doesn't match, keep waiting
                remaining_getters.append((getter_future, getter_predicate))

        # Restore getters that couldn't be satisfied
        self._getters.extendleft(reversed(remaining_getters))

    def _wakeup_next_putter(self) -> None:
        """Wake up the next putter if there's space."""
        while self._putters and not self.full():
            putter_future, item = self._putters.popleft()
            if not putter_future.done():
                self._put_nowait(item)
                putter_future.set_result(None)
                break

    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete."""
        if self._unfinished_tasks <= 0:
            raise ValueError("task_done() called too many times")
        self._unfinished_tasks -= 1
        if self._unfinished_tasks == 0:
            self._finished.set()

    async def join(self) -> None:
        """Wait until all items in the queue have been gotten and completed."""
        await self._finished.wait()


# public
@dataclass
class DiscoveryEvent:
    """Represents a worker service discovery event.

    Contains information about worker service lifecycle events (added,
    updated, removed) including both pre- and post-event property states to
    enable comprehensive event handling.

    :param type:
        Type of discovery event (added, updated, or removed).
    :param worker:
        The :class:`~wool._worker_discovery.WorkerInfo` instance associated with this
        event.
    """

    type: DiscoveryEventType
    worker_info: WorkerInfo


_T_co = TypeVar("_T_co", covariant=True)


# public
class Reducible(Protocol):
    """Protocol for objects that support pickling via __reduce__."""

    def __reduce__(self) -> tuple: ...


# public
class ReducibleAsyncIteratorLike(Reducible, Protocol, Generic[_T_co]):
    """Protocol for async iterators that yield discovery events.

    Implementations must be pickleable via __reduce__ to support
    task-specific session contexts in distributed environments.
    """

    def __aiter__(self) -> ReducibleAsyncIteratorLike[_T_co]: ...

    def __anext__(self) -> Awaitable[_T_co]: ...


# public
@runtime_checkable
class Factory(Protocol, Generic[_T_co]):
    def __call__(
        self,
    ) -> (
        _T_co | Awaitable[_T_co] | AsyncContextManager[_T_co] | ContextManager[_T_co]
    ): ...


# public
class DiscoveryService(ABC):
    """Abstract base class for discovering worker services.

    When started, implementations should discover all existing services that
    satisfy the specified filter and deliver worker-added events for each.
    Subsequently, they should monitor for newly added, updated, or removed
    workers and deliver appropriate events via the :meth:`events` method.

    Service tracking behavior:
        - Only workers satisfying the filter should be tracked
        - Workers updated to satisfy the filter should trigger worker-added events
        - Workers updated to no longer satisfy the filter should trigger
          worker-removed events
        - Tracked workers removed from the registry entirely should always
          trigger worker-removed

    :param filter:
        Optional filter function to select which discovery events to yield.
        Only events matching the filter will be delivered.

    .. warning::
        The discovery procedure should not block continuously, as it will be
        executed in the current event loop.

    .. note::
        Implementations must be pickleable and provide an unstarted copy
        when unpickled to support task-specific session contexts.
    """

    _started: bool
    _service_cache: Dict[str, WorkerInfo]

    def __init__(
        self,
        filter: PredicateFunction[WorkerInfo] | None = None,
    ):
        self._filter = filter
        self._started = False
        self._service_cache = {}

    def __reduce__(self) -> tuple:
        """Return constructor args for unpickling an unstarted service copy."""
        return (self.__class__, (self._filter,))

    def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
        """Returns self as an async iterator."""
        return self.events()

    async def __anext__(self) -> DiscoveryEvent:
        """Delegate to the events async iterator."""
        return await anext(self.events())

    @final
    @property
    def started(self) -> bool:
        return self._started

    @final
    async def start(self) -> None:
        """Starts the worker discovery procedure.

        This method should initiate the discovery process, which may involve
        network operations or other asynchronous tasks.

        :raises RuntimeError:
            If the service has already been started.
        """
        if self._started:
            raise RuntimeError("Discovery service already started")
        await self._start()
        self._started = True

    @final
    async def stop(self) -> None:
        """Stops the worker discovery procedure.

        This method should clean up any resources used for discovery, such as
        network connections or event listeners.

        :raises RuntimeError:
            If the service has not been started.
        """
        if not self._started:
            raise RuntimeError("Discovery service not started")
        await self._stop()

    @abstractmethod
    def events(self) -> AsyncIterator[DiscoveryEvent]:
        """Yields discovery events as they occur.

        Returns an asynchronous iterator that yields discovery events for
        workers being added, updated, or removed from the registry. Events
        are filtered according to the filter function provided during
        initialization.

        :yields:
            Instances of :class:`DiscoveryEvent` representing worker
            additions, updates, and removals.
        """
        ...

    @abstractmethod
    async def _start(self) -> None:
        """Starts the worker discovery procedure."""
        ...

    @abstractmethod
    async def _stop(self) -> None:
        """Stops the worker discovery procedure."""
        ...


_T_DiscoveryServiceLike = TypeVar("_T_DiscoveryServiceLike", bound=DiscoveryService)


# public
class RegistryServiceLike(Protocol):
    """Abstract base class for a service where workers can register themselves.

    Provides the interface for worker registration, unregistration, and
    property updates within a distributed worker pool system.
    """

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def register(self, worker_info: WorkerInfo) -> None: ...

    async def unregister(self, worker_info: WorkerInfo) -> None: ...

    async def update(self, worker_info: WorkerInfo) -> None: ...


# public
class RegistryService(Generic[_T_DiscoveryServiceLike], ABC):
    """Abstract base class for a service where workers can register themselves.

    Provides the interface for worker registration, unregistration, and
    property updates within a distributed worker pool system.
    """

    _started: bool
    _stopped: bool

    def __init__(self):
        self._started = False
        self._stopped = False

    async def start(self) -> None:
        """Starts the registry service, making it ready to accept registrations.

        :raises RuntimeError:
            If the service has already been started.
        """
        if self._started:
            raise RuntimeError("Registry service already started")
        await asyncio.wait_for(self._start(), timeout=60)
        self._started = True

    async def stop(self) -> None:
        """Stops the registry service and cleans up any resources.

        :raises RuntimeError:
            If the service has not been started.
        """
        if self._stopped:
            return
        if not self._started:
            raise RuntimeError("Registry service not started")
        await self._stop()
        self._stopped = True

    @abstractmethod
    async def _start(self) -> None:
        """Starts the registry service, making it ready to accept registrations."""
        ...

    @abstractmethod
    async def _stop(self) -> None:
        """Stops the registry service and cleans up any resources."""
        ...

    async def register(
        self,
        worker_info: WorkerInfo,
    ) -> None:
        """Registers a worker by publishing its service information.

        :param worker_info:
            The :class:`~wool._worker_discovery.WorkerInfo` instance containing all
            worker details.
        :raises RuntimeError:
            If the registry service is not running.
        """
        if not self._started:
            raise RuntimeError("Registry service not started - call start() first")
        if self._stopped:
            raise RuntimeError("Registry service already stopped")
        await self._register(worker_info)

    @abstractmethod
    async def _register(
        self,
        worker_info: WorkerInfo,
    ) -> None:
        """Implementation-specific worker registration.

        :param worker_info:
            The :class:`~wool._worker_discovery.WorkerInfo` instance containing all
            worker details.
        """
        ...

    async def unregister(self, worker_info: WorkerInfo) -> None:
        """Unregisters a worker by removing its service record.

        :param worker_info:
            The :class:`~wool._worker_discovery.WorkerInfo` instance of the worker to
            unregister.
        :raises RuntimeError:
            If the registry service is not running.
        """
        if not self._started:
            raise RuntimeError("Registry service not started - call start() first")
        if self._stopped:
            raise RuntimeError("Registry service already stopped")
        await self._unregister(worker_info)

    @abstractmethod
    async def _unregister(self, worker_info: WorkerInfo) -> None:
        """Implementation-specific worker unregistration.

        :param worker_info:
            The :class:`~wool._worker_discovery.WorkerInfo` instance of the worker to
            unregister.
        """
        ...

    async def update(self, worker_info: WorkerInfo) -> None:
        """Updates a worker's properties if they have changed.

        :param worker_info:
            The updated :class:`~wool._worker_discovery.WorkerInfo` instance.
        :raises RuntimeError:
            If the registry service is not running.
        """
        if not self._started:
            raise RuntimeError("Registry service not started - call start() first")
        if self._stopped:
            raise RuntimeError("Registry service already stopped")
        await self._update(worker_info)

    @abstractmethod
    async def _update(self, worker_info: WorkerInfo) -> None:
        """Implementation-specific worker property updates.

        :param worker_info:
            The updated :class:`~wool._worker_discovery.WorkerInfo` instance.
        """
        ...


# public
class LanDiscoveryService(DiscoveryService):
    """Implements worker discovery on the local network using Zeroconf.

    This service browses the local network for DNS-SD services and delivers
    all worker service events to clients. Uses Zeroconf/Bonjour protocol
    for automatic service discovery without requiring central coordination.

    :param filter:
        Optional predicate function to filter discovered workers.
    """

    aiozc: AsyncZeroconf
    browser: AsyncServiceBrowser
    service_type: Literal["_wool._tcp.local."] = "_wool._tcp.local."
    _event_queue: PredicatedQueue[DiscoveryEvent]

    def __init__(
        self,
        filter: PredicateFunction[WorkerInfo] | None = None,
    ) -> None:
        super().__init__(filter)  # type: ignore[arg-type]
        self._event_queue = PredicatedQueue()

    async def events(self) -> AsyncIterator[DiscoveryEvent]:
        """Returns an async iterator over discovery events."""
        await self.start()
        try:
            while True:
                yield await self._event_queue.get()
        finally:
            await self.stop()

    async def _start(self) -> None:
        """Starts the Zeroconf service browser.

        :raises RuntimeError:
            If the service has already been started.
        """
        # Configure zeroconf to use localhost only to avoid network warnings
        self.aiozc = AsyncZeroconf(interfaces=["127.0.0.1"])
        self.browser = AsyncServiceBrowser(
            self.aiozc.zeroconf,
            self.service_type,
            listener=self._Listener(
                aiozc=self.aiozc,
                event_queue=self._event_queue,
                service_cache=self._service_cache,
                predicate=self._filter or (lambda _: True),
            ),
        )

    async def _stop(self) -> None:
        """Stops the Zeroconf service browser and closes the connection.

        :raises RuntimeError:
            If the service has not been started.
        """
        if self.browser:
            await self.browser.async_cancel()
        if self.aiozc:
            await self.aiozc.async_close()

    class _Listener(ServiceListener):
        """A Zeroconf listener that delivers all worker service events.

        :param aiozc:
            The :class:`~zeroconf.asyncio.AsyncZeroconf` instance to use
            for async service info retrieval.
        :param event_queue:
            Queue to deliver discovery events to.
        :param service_cache:
            Cache to track service properties for pre/post event states.
        :param predicate:
            Function to filter which workers to track.
        """

        aiozc: AsyncZeroconf
        _event_queue: PredicatedQueue[DiscoveryEvent]
        _service_addresses: Dict[str, str]
        _service_cache: Dict[str, WorkerInfo]

        def __init__(
            self,
            aiozc: AsyncZeroconf,
            event_queue: PredicatedQueue[DiscoveryEvent],
            predicate: PredicateFunction[WorkerInfo],
            service_cache: Dict[str, WorkerInfo],
        ) -> None:
            self.aiozc = aiozc
            self._event_queue = event_queue
            self._predicate = predicate
            self._service_addresses = {}
            self._service_cache = service_cache

        def add_service(self, zc: Zeroconf, type_: str, name: str):
            """Called by Zeroconf when a service is added."""
            if type_ == LanRegistryService.service_type:
                asyncio.create_task(self._handle_add_service(type_, name))

        def remove_service(self, zc: Zeroconf, type_: str, name: str):
            """Called by Zeroconf when a service is removed."""
            if type_ == LanRegistryService.service_type:
                if worker := self._service_cache.pop(name, None):
                    asyncio.create_task(
                        self._event_queue.put(
                            DiscoveryEvent(type="worker_removed", worker_info=worker)
                        )
                    )

        def update_service(self, zc: Zeroconf, type_, name):
            """Called by Zeroconf when a service is updated."""
            if type_ == LanRegistryService.service_type:
                asyncio.create_task(self._handle_update_service(type_, name))

        async def _handle_add_service(self, type_: str, name: str):
            """Async handler for service addition."""
            try:
                if not (
                    service_info := await self.aiozc.async_get_service_info(type_, name)
                ):
                    return

                try:
                    worker_info = _deserialize_worker_info(service_info)
                except ValueError:
                    return

                if self._predicate(worker_info):
                    self._service_cache[name] = worker_info
                    event = DiscoveryEvent(type="worker_added", worker_info=worker_info)
                    await self._event_queue.put(event)
            except Exception:
                pass  # Service may have disappeared before we could query it

        async def _handle_update_service(self, type_: str, name: str):
            """Async handler for service update."""
            try:
                if not (
                    service_info := await self.aiozc.async_get_service_info(type_, name)
                ):
                    return

                try:
                    worker_info = _deserialize_worker_info(service_info)
                except ValueError:
                    return

                if name not in self._service_cache:
                    # New worker that wasn't tracked before
                    if self._predicate(worker_info):
                        self._service_cache[name] = worker_info
                        event = DiscoveryEvent(
                            type="worker_added", worker_info=worker_info
                        )
                        await self._event_queue.put(event)
                else:
                    # Existing tracked worker
                    old_worker = self._service_cache[name]
                    if self._predicate(worker_info):
                        # Still satisfies filter, update cache and emit update
                        self._service_cache[name] = worker_info
                        event = DiscoveryEvent(
                            type="worker_updated", worker_info=worker_info
                        )
                        await self._event_queue.put(event)
                    else:
                        # No longer satisfies filter, remove and emit removal
                        del self._service_cache[name]
                        removal_event = DiscoveryEvent(
                            type="worker_removed", worker_info=old_worker
                        )
                        await self._event_queue.put(removal_event)

            except Exception:
                pass


# public
class LanRegistryService(RegistryService[LanDiscoveryService]):
    """Implements a worker registry using Zeroconf to advertise on the LAN.

    This service registers workers by publishing a DNS-SD service record on
    the local network, allowing :class:`LanDiscoveryService` to find them.
    """

    aiozc: AsyncZeroconf | None
    services: Dict[str, ServiceInfo]
    service_type: Literal["_wool._tcp.local."] = "_wool._tcp.local."

    def __init__(self):
        super().__init__()
        self.aiozc = None
        self.services = {}

    async def _start(self) -> None:
        """Initializes and starts the Zeroconf instance for advertising."""
        # Configure zeroconf to use localhost only to avoid network warnings
        self.aiozc = AsyncZeroconf(interfaces=["127.0.0.1"])

    async def _stop(self) -> None:
        """Stops the Zeroconf instance and cleans up all registered services."""
        if self.aiozc:
            await self.aiozc.async_close()
            self.aiozc = None

    async def _register(
        self,
        worker_info: WorkerInfo,
    ) -> None:
        """Registers a worker by publishing its service information via Zeroconf.

        :param worker_info:
            The :class:`~wool._worker_discovery.WorkerInfo` instance containing all
            worker details.
        :raises RuntimeError:
            If the registry service is not properly initialized.
        """
        if self.aiozc is None:
            raise RuntimeError("Registry service not properly initialized")
        address = f"{worker_info.host}:{worker_info.port}"
        ip_address, port = self._resolve_address(address)
        service_name = f"{worker_info.uid}.{self.service_type}"
        service_info = ServiceInfo(
            self.service_type,
            service_name,
            addresses=[ip_address],
            port=port,
            properties=_serialize_worker_info(worker_info),
        )
        self.services[worker_info.uid] = service_info
        await self.aiozc.async_register_service(service_info)

    async def _unregister(self, worker_info: WorkerInfo) -> None:
        """Unregisters a worker by removing its Zeroconf service record.

        :param worker_info:
            The :class:`~wool._worker_discovery.WorkerInfo` instance of the worker to
            unregister.
        :raises RuntimeError:
            If the registry service is not properly initialized.
        """
        if self.aiozc is None:
            raise RuntimeError("Registry service not properly initialized")
        service = self.services[worker_info.uid]
        await self.aiozc.async_unregister_service(service)
        del self.services[worker_info.uid]

    async def _update(self, worker_info: WorkerInfo) -> None:
        """Updates a worker's properties if they have changed.

        Updates both the Zeroconf service and local cache atomically.
        If the Zeroconf update fails, the local cache remains unchanged
        to maintain consistency.

        :param worker_info:
            The updated :class:`~wool._worker_discovery.WorkerInfo` instance.
        :raises RuntimeError:
            If the registry service is not properly initialized.
        :raises Exception:
            If the Zeroconf service update fails.
        """
        if self.aiozc is None:
            raise RuntimeError("Registry service not properly initialized")

        service = self.services[worker_info.uid]
        new_properties = _serialize_worker_info(worker_info)

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
            self.services[worker_info.uid] = updated_service

    def _resolve_address(self, address: str) -> Tuple[bytes, int]:
        """Resolve an address string to bytes and validate port.

        :param address:
            Address in format "host:port".
        :returns:
            Tuple of (IPv4/IPv6 address as bytes, port as int).
        :raises ValueError:
            If address format is invalid or port is out of range.
        """
        host, port = address.split(":")
        port = int(port)

        try:
            return socket.inet_pton(socket.AF_INET, host), port
        except OSError:
            pass

        try:
            return socket.inet_pton(socket.AF_INET6, host), port
        except OSError:
            pass

        return socket.inet_aton(socket.gethostbyname(host)), port


def _serialize_worker_info(
    info: WorkerInfo,
) -> dict[str, str | None]:
    """Serialize WorkerInfo to a flat dict for ServiceInfo.properties.

    :param info:
        :class:`~wool._worker_discovery.WorkerInfo` instance to serialize.
    :returns:
        Flat dict with pid, version, tags (JSON), extra (JSON).
    """
    properties = {
        "pid": str(info.pid),
        "version": info.version,
        "tags": (json.dumps(list(info.tags)) if info.tags else None),
        "extra": (json.dumps(info.extra) if info.extra else None),
    }
    return properties


def _deserialize_worker_info(info: ServiceInfo) -> WorkerInfo:
    """Deserialize ServiceInfo.decoded_properties to WorkerProperties.

    :param info:
        ServiceInfo with decoded properties dict (str keys/values).
    :returns:
        :class:`~wool._worker_discovery.WorkerInfo` instance.
    :raises ValueError:
        If required fields are missing or invalid JSON.
    """
    properties = info.decoded_properties
    if missing := {"pid", "version"} - set(k for k, v in properties.items() if v):
        missing = ", ".join(missing)
        raise ValueError(f"Missing required properties: {missing}")
    assert "pid" in properties and properties["pid"]
    assert "version" in properties and properties["version"]
    pid = int(properties["pid"])
    version = properties["version"]
    if "tags" in properties and properties["tags"]:
        tags = set(json.loads(properties["tags"]))
    else:
        tags = set()
    if "extra" in properties and properties["extra"]:
        extra = json.loads(properties["extra"])
    else:
        extra = {}
    return WorkerInfo(
        uid=info.name,
        pid=pid,
        host=str(info.ip_addresses_by_version(IPVersion.V4Only)[0]),
        port=info.port,
        version=version,
        tags=tags,
        extra=extra,
    )


# public
class LocalRegistryService(RegistryService):
    """Implements a worker registry using shared memory for local pools.

    This service registers workers by writing their information to a shared memory
    block, allowing LocalDiscoveryService instances to find them efficiently.
    The registry stores worker ports as integers in a simple array format,
    providing fast local discovery without network overhead.

    :param uri:
        Unique identifier for the shared memory segment.
    """

    _shared_memory: multiprocessing.shared_memory.SharedMemory | None = None
    _uri: str
    _created_shared_memory: bool = False

    def __init__(self, uri: str):
        super().__init__()
        self._uri = uri
        self._created_shared_memory = False

    async def _start(self) -> None:
        """Initialize shared memory for worker registration."""
        if self._shared_memory is None:
            # Try to connect to existing shared memory first, create if it doesn't exist
            shared_memory_name = hashlib.sha256(self._uri.encode()).hexdigest()[:12]
            try:
                self._shared_memory = multiprocessing.shared_memory.SharedMemory(
                    name=shared_memory_name
                )
            except FileNotFoundError:
                # Create new shared memory if it doesn't exist
                self._shared_memory = multiprocessing.shared_memory.SharedMemory(
                    name=shared_memory_name,
                    create=True,
                    size=1024,  # 1024 bytes = 256 worker slots (4 bytes per port)
                )
                self._created_shared_memory = True
                # Initialize all slots to 0 (empty)
                for i in range(len(self._shared_memory.buf)):
                    self._shared_memory.buf[i] = 0

    async def _stop(self) -> None:
        """Clean up shared memory resources."""
        if self._shared_memory:
            try:
                self._shared_memory.close()
                # Unlink the shared memory if this registry created it
                if self._created_shared_memory:
                    self._shared_memory.unlink()
            except Exception:
                pass
            self._shared_memory = None
            self._created_shared_memory = False

    async def _register(self, worker_info: WorkerInfo) -> None:
        """Register a worker by writing its port to shared memory.

        :param worker_info:
            The :class:`~wool._worker_discovery.WorkerInfo` instance containing all
            worker details. Only the port is stored in shared memory.
        :raises RuntimeError:
            If the registry service is not properly initialized.
        """
        if self._shared_memory is None:
            raise RuntimeError("Registry service not properly initialized")

        if worker_info.port is None:
            raise ValueError("Worker port must be specified")

        # Find first available slot and write port
        for i in range(0, len(self._shared_memory.buf), 4):
            existing_port = struct.unpack("I", self._shared_memory.buf[i : i + 4])[0]
            if existing_port == 0:  # Empty slot
                struct.pack_into("I", self._shared_memory.buf, i, worker_info.port)
                break
        else:
            raise RuntimeError("No available slots in shared memory registry")

    async def _unregister(self, worker_info: WorkerInfo) -> None:
        """Unregister a worker by removing its port from shared memory.

        :param worker_info:
            The :class:`~wool._worker_discovery.WorkerInfo` instance of the worker to
            unregister.
        :raises RuntimeError:
            If the registry service is not properly initialized.
        """
        if self._shared_memory is None:
            raise RuntimeError("Registry service not properly initialized")

        if worker_info.port is None:
            return

        # Find and clear the port
        for i in range(0, len(self._shared_memory.buf), 4):
            existing_port = struct.unpack("I", self._shared_memory.buf[i : i + 4])[0]
            if existing_port == worker_info.port:
                struct.pack_into("I", self._shared_memory.buf, i, 0)  # Clear slot
                break

    async def _update(self, worker_info: WorkerInfo) -> None:
        """Update a worker's properties in shared memory.

        For the simple port-based registry, update is the same as register.

        :param worker_info:
            The updated :class:`~wool._worker_discovery.WorkerInfo` instance.
        """
        await self._register(worker_info)


# public
class LocalDiscoveryService(DiscoveryService):
    """Implements worker discovery using shared memory for local pools.

    This service reads worker ports from a shared memory block and
    constructs WorkerInfo instances with localhost as the implied host.
    Provides efficient local discovery for single-machine worker pools.

    :param uri:
        Unique identifier for the shared memory segment.
    :param filter:
        Optional predicate function to filter discovered workers.
    """

    _shared_memory: multiprocessing.shared_memory.SharedMemory | None = None
    _uri: str
    _event_queue: asyncio.Queue[DiscoveryEvent]
    _monitor_task: asyncio.Task | None
    _stop_event: asyncio.Event

    def __init__(
        self,
        uri: str,
        filter: PredicateFunction[WorkerInfo] | None = None,
    ) -> None:
        super().__init__(filter)  # type: ignore[arg-type]
        self._uri = uri
        self._event_queue = asyncio.Queue()
        self._monitor_task = None
        self._stop_event = asyncio.Event()

    def __reduce__(self) -> tuple:
        """Return constructor args for unpickling an unstarted service copy."""
        func, args = super().__reduce__()
        return (func, (self._uri, *args))

    async def events(self) -> AsyncIterator[DiscoveryEvent]:
        """Returns an async iterator over discovery events."""
        await self.start()
        try:
            while True:
                event = await self._event_queue.get()
                yield event
        finally:
            await self.stop()

    async def _start(self) -> None:
        """Starts monitoring shared memory for worker registrations."""
        if self._shared_memory is None:
            # Try to connect to existing shared memory first
            self._shared_memory = multiprocessing.shared_memory.SharedMemory(
                name=hashlib.sha256(self._uri.encode()).hexdigest()[:12]
            )

        # Start monitoring task
        self._monitor_task = asyncio.create_task(self._monitor_shared_memory())

    async def _stop(self) -> None:
        """Stops monitoring shared memory."""
        self._stop_event.set()
        if self._monitor_task:
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        if self._shared_memory:
            try:
                self._shared_memory.close()
            except Exception:
                pass
            self._shared_memory = None

    async def _monitor_shared_memory(self) -> None:
        """Monitor shared memory for changes and emit events."""
        poll_interval = 0.1

        while not self._stop_event.is_set():
            try:
                current_workers = {}

                # Read current state from shared memory
                if self._shared_memory:
                    for i in range(0, len(self._shared_memory.buf), 4):
                        port = struct.unpack("I", self._shared_memory.buf[i : i + 4])[0]
                        if port > 0:  # Active worker
                            worker_info = WorkerInfo(
                                uid=f"worker-{port}",
                                host="localhost",
                                port=port,
                                pid=0,  # Not available in simple registry
                                version="unknown",  # Not available in simple registry
                                tags=set(),
                                extra={},
                            )
                            if self._filter is None or self._filter(worker_info):
                                current_workers[worker_info.uid] = worker_info

                # Detect changes
                await self._detect_changes(current_workers)

                # Wait before next poll
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=poll_interval
                    )
                    break  # Stop event was set
                except asyncio.TimeoutError:
                    continue  # Continue polling

            except Exception:
                continue

    async def _detect_changes(self, current_workers: Dict[str, WorkerInfo]) -> None:
        """Detect and emit events for worker changes."""
        # Find added workers
        for uid, worker_info in current_workers.items():
            if uid not in self._service_cache:
                self._service_cache[uid] = worker_info
                event = DiscoveryEvent(type="worker_added", worker_info=worker_info)
                await self._event_queue.put(event)

        # Find removed workers
        for uid in list(self._service_cache.keys()):
            if uid not in current_workers:
                worker_info = self._service_cache.pop(uid)
                event = DiscoveryEvent(type="worker_removed", worker_info=worker_info)
                await self._event_queue.put(event)

        # Find updated workers (minimal for port-only registry)
        for uid, worker_info in current_workers.items():
            if uid in self._service_cache:
                old_worker = self._service_cache[uid]
                if worker_info.port != old_worker.port:
                    self._service_cache[uid] = worker_info
                    event = DiscoveryEvent(
                        type="worker_updated", worker_info=worker_info
                    )
                    await self._event_queue.put(event)
