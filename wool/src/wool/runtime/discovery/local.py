from __future__ import annotations

import asyncio
import atexit
import hashlib
import struct
import tempfile
import warnings
from contextlib import asynccontextmanager
from contextlib import contextmanager
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Callable
from typing import Final
from typing import Iterator
from typing import Self
from uuid import UUID
from uuid import uuid4

import portalocker
from watchdog.events import FileSystemEvent
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from wool import protocol as wire
from wool.runtime.discovery.base import Discovery
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoveryEventType
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import PredicateFunction
from wool.runtime.discovery.exceptions import DiscoveryCapacityExhausted
from wool.runtime.discovery.pool import SubscriberMeta
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.worker.metadata import WorkerMetadata
from wool.utilities.afilter import afilter
from wool.utilities.noreentry import noreentry

REF_WIDTH: Final = 16
NULL_REF: Final = b"\x00" * REF_WIDTH
DEFAULT_LOCK_TIMEOUT: Final[float] = 30.0
_HEADER_MAGIC: Final = b"WLD1"
_HEADER_SIZE: Final = REF_WIDTH


class _Watchdog(FileSystemEventHandler):
    """Filesystem event handler for worker discovery notifications.

    Monitors the notification file for modifications and sets an asyncio
    Event to wake subscribers when publishers modify the shared memory.
    Thread-safe for use with watchdog's observer thread.

    Acquires the scan lock before setting the notification event to ensure
    that notifications are properly synchronized with ongoing scans. This
    prevents race conditions where a notification arrives while a scan is
    in progress.

    :param notification:
        asyncio.Event to set when the notification file is modified.
    :param watchdog:
        Path to the notification file to monitor.
    :param lock:
        asyncio.Lock to acquire before setting the notification event.
    :param loop:
        Event loop where the notification lives.
    """

    def __init__(
        self,
        notification: asyncio.Event,
        watchdog: Path,
        lock: asyncio.Lock,
        loop: asyncio.AbstractEventLoop,
    ):
        self._notification = notification
        self._watchdog = watchdog
        self._lock = lock
        self._loop = loop

    def on_modified(self, event: FileSystemEvent):
        """Handle file modification events.

        :param event:
            The filesystem event containing the modified file path.
        """
        event_path = Path(str(event.src_path))
        if event_path == self._watchdog:
            # Schedule the event.set() in the event loop with lock acquired
            # (thread-safe)
            self._loop.call_soon_threadsafe(self._set_event_with_lock)

    def _set_event_with_lock(self):
        """Set the notification event after acquiring the scan lock.

        This ensures that the event is only set when the lock is available,
        preventing the notification from being lost if a scan is in progress.
        Must be called from the event loop thread.
        """
        asyncio.create_task(self._async_set_event())

    async def _async_set_event(self):
        """Async helper to acquire lock and set event."""
        async with self._lock:
            self._notification.set()


class _WorkerReference:
    """Reference to a worker using its UUID.

    Provides both byte and unicode string representations of a worker's UUID.

    :param uid:
        The worker's UID to reference.
    """

    __slots__ = ("_uuid",)

    def __init__(self, uid: UUID):
        self._uuid = uid

    def __str__(self) -> str:
        """String representation (32-char hex) for :class:`SharedMemory` names.

        :returns:
            The UUID as a hex string without dashes.
        """
        return _short_hash(self._uuid.hex)

    def __hash__(self) -> int:
        return hash(self._uuid)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _WorkerReference):
            return self._uuid == other._uuid
        return NotImplemented  # pragma: no cover

    def __repr__(self) -> str:
        return f"_WorkerReference({self._uuid})"  # pragma: no cover

    @classmethod
    def from_bytes(cls, data: bytes) -> _WorkerReference:
        """Create a reference from its bytes representation.

        :param data:
            The 16-byte UUID representation.
        :returns:
            A new reference instance.
        :raises ValueError:
            If data is not 16 bytes or is NULL.
        """
        ref = object.__new__(cls)
        ref._uuid = UUID(bytes=data)
        return ref

    @property
    def bytes(self) -> bytes:
        """The 16-byte representation for address space storage.

        :returns:
            The UUID as 16 bytes.
        """
        return self._uuid.bytes


# public
class LocalDiscovery(Discovery):
    """Shared-memory discovery for single-machine worker pools.

    The default when a `~wool.runtime.worker.pool.WorkerPool` is created
    without an explicit discovery protocol. Workers and subscribers
    communicate through a shared-memory segment identified by a namespace
    string, so unrelated processes on the same host discover each other by
    agreeing on a namespace alone. File-based locking keeps the segment
    consistent across those processes.

    **Ownership.** Entering a context creates the namespace's segment, or
    attaches to it when it already exists. The first entrant across all
    processes wins the create race and *owns* the segment; every later
    entrant on that namespace merely attaches. Ownership falls out of that
    race — it is not something the caller selects.

    **Lifecycle.** An instance is single-use: it may be entered once, and a
    second entry raises `RuntimeError` whether or not the first has exited.
    Use a fresh instance per ``with`` block. The guard binds to the instance,
    so distinct instances sharing a namespace — which compare equal — are
    unaffected by each other.

    The owner's exit unlinks the segment out from under every still-attached
    peer, while a non-owner's exit only closes its own mapping; a non-owner
    can therefore outlive the segment it is mapped to. An owner that never
    exits at all — an abandoned context, an interpreter killed mid-block —
    still reclaims the segment at interpreter shutdown rather than leaking
    the namespace, via a fallback armed on owner entry and disarmed on owner
    exit.

    Teardown never raises: unlinking goes through `_unlink_quietly`, which
    owns the failure semantics. So `__exit__` cannot replace an exception a
    caller is already unwinding, and a genuine leak stays observable.

    :param namespace:
        Unique identifier for the shared-memory segment. Publishers
        and subscribers using the same namespace will see each
        other's workers.
    :param filter:
        Optional default predicate function to filter workers.
        Used by `subscriber` and as the default for `subscribe` when no
        explicit filter is provided.
    :param capacity:
        Maximum number of workers registrable — and discoverable —
        simultaneously. The owner stamps it into the segment on entry, so
        every publisher and subscriber enforces the same bound without
        re-declaring it. Publishing a worker once ``capacity`` are
        registered raises `RuntimeError`. Defaults to 128.
    :param block_size:
        Size in bytes for each worker's serialized data block.
        Defaults to 1024.
    :param lock_timeout:
        Maximum seconds a publisher waits to acquire the cross-process
        file lock; see `LocalDiscovery.Publisher` for the acquisition
        contract. Plumbed through to each `Publisher`. Defaults to
        `DEFAULT_LOCK_TIMEOUT`.

    Example — publish workers:

    .. code-block:: python

        with LocalDiscovery("my-worker-pool") as discovery:
            async with discovery.publisher as publisher:
                await publisher.publish("worker-added", metadata)

    Example — subscribe to workers:

    .. code-block:: python

        with LocalDiscovery("my-worker-pool") as discovery:
            async for event in discovery.subscriber:
                print(f"Discovered worker: {event.metadata}")

    .. rubric:: Implementation notes

    The shutdown fallback is an `atexit` handler registered on owner entry
    and unregistered on owner exit, before the unlink runs — a failed unlink
    must not leave the handler armed to fire a second time at interpreter
    shutdown.
    """

    _filter: Final[PredicateFunction | None]
    _namespace: Final[str]

    def __init__(
        self,
        namespace: str | None = None,
        *,
        filter: PredicateFunction | None = None,
        capacity: int = 128,
        block_size: int = 1024,
        lock_timeout: float | None = DEFAULT_LOCK_TIMEOUT,
    ):
        if capacity < 1:
            raise ValueError(f"Expected capacity of at least 1, got {capacity}")
        self._namespace = namespace or f"workerpool-{uuid4()}"
        self._filter = filter
        self._capacity = capacity
        self._block_size = block_size
        self._lock_timeout = lock_timeout

    @noreentry
    def __enter__(self) -> Self:
        """Create or attach to the namespace's shared-memory segment.

        See `LocalDiscovery` for the ownership and teardown contract.

        :returns:
            This instance.
        :raises RuntimeError:
            If this instance has already been entered.
        """
        size = _HEADER_SIZE + self._capacity * REF_WIDTH
        try:
            self._address_space = SharedMemory(
                name=_short_hash(self._namespace),
                create=True,
                size=size,
            )
            self._owner = True
        except FileExistsError:
            self._address_space = SharedMemory(
                name=_short_hash(self._namespace),
                create=False,
            )
            self._owner = False

        assert self._address_space.buf
        if self._owner:

            def cleanup():  # pragma: no cover
                _unlink_quietly(self._address_space)

            self._cleanup = atexit.register(cleanup)
            for i in range(size):
                self._address_space.buf[i] = 0
            struct.pack_into(
                "<4sI", self._address_space.buf, 0, _HEADER_MAGIC, self._capacity
            )
        return self

    def __exit__(self, *_):
        """Release this instance's hold on the namespace's segment.

        See `LocalDiscovery` for the ownership and teardown contract.
        """
        if self._owner:
            atexit.unregister(self._cleanup)
            self._address_space.close()
            _unlink_quietly(self._address_space)
        else:
            self._address_space.close()

    def __hash__(self) -> int:
        return hash((type(self), self._namespace))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, LocalDiscovery):
            return self._namespace == other._namespace
        return NotImplemented

    @property
    def namespace(self):
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
        return self.Publisher(
            self._namespace,
            block_size=self._block_size,
            lock_timeout=self._lock_timeout,
        )

    @property
    def subscriber(self) -> DiscoverySubscriberLike:
        """A subscriber using the constructor's default filter.

        :returns:
            A subscriber instance for receiving worker discovery
            events.
        """
        return self.subscribe()

    def subscribe(
        self,
        filter: PredicateFunction | None = None,
        *,
        poll_interval: float | None = None,
    ) -> DiscoverySubscriberLike:
        """Create a new subscriber with optional filtering.

        :param filter:
            Optional predicate function to filter workers. Only workers
            for which the predicate returns True will be included in
            events. Falls back to the constructor's filter if not
            provided.
        :param poll_interval:
            Optional interval in seconds between shared memory polls.
            If not specified, uses filesystem notifications for
            efficient updates.
        :returns:
            A subscriber instance that receives filtered worker
            discovery events.
        """
        effective = filter if filter is not None else self._filter
        subscriber = self.Subscriber(
            self._namespace,
            poll_interval=poll_interval,
        )
        if effective is not None:
            return afilter(effective, subscriber)
        return subscriber

    class Publisher:
        """Publisher for broadcasting worker discovery events.

        Publishes worker discovery events (see `~wool.DiscoveryEvent`) to
        a shared memory region where subscribers can discover them.
        Multiple publishers in different processes can safely write to the
        same namespace using cross-platform file locking for
        synchronization. The capacity bound is read from the segment the
        owning `LocalDiscovery` stamped, so a publisher never re-declares
        it.

        :param namespace:
            The namespace identifier for the shared memory region.
        :param block_size:
            Size in bytes for worker metadata storage blocks. Defaults
            to 512 bytes, which accommodates typical worker
            metadata including tags and extra metadata.
        :param lock_timeout:
            Maximum seconds to wait for the cross-process file lock before
            raising `TimeoutError`. ``None`` waits forever. Defaults to
            `DEFAULT_LOCK_TIMEOUT`.
        :raises ValueError:
            If ``block_size`` is negative, or ``lock_timeout`` is negative.
        """

        _block_size: int
        _cleanups: dict[str, Callable]
        _lock_timeout: float | None
        _namespace: Final[str]
        _shared_memory_pool: ResourcePool[SharedMemory]

        #: Shared-memory announcements are only discoverable on a
        #: common host, so this publisher prescribes the loopback bind.
        #: See `~wool.DiscoveryPublisherLike.bind_host` for the contract.
        bind_host: str = "127.0.0.1"

        def __init__(
            self,
            namespace: str,
            *,
            block_size: int = 1024,
            lock_timeout: float | None = DEFAULT_LOCK_TIMEOUT,
        ):
            if block_size < 0:
                raise ValueError("Block size must be positive")
            if lock_timeout is not None and lock_timeout < 0:
                raise ValueError("Lock timeout must be non-negative")
            self._namespace = namespace
            self._block_size = block_size
            self._lock_timeout = lock_timeout
            self._cleanups = {}
            self._shared_memory_pool = ResourcePool(
                factory=self._shared_memory_factory,
                finalizer=self._shared_memory_finalizer,
                ttl=0,
            )

        async def __aenter__(self) -> Self:
            await self._shared_memory_pool.__aenter__()
            return self

        async def __aexit__(self, *args):
            await self._shared_memory_pool.__aexit__(*args)

        @property
        def namespace(self):
            """The namespace identifier for this publisher.

            :returns:
                The namespace string.
            """
            return self._namespace

        async def publish(self, type: DiscoveryEventType, metadata: WorkerMetadata):
            """Publish a worker discovery event.

            Writes the event to shared memory where subscribers can
            discover it. The operation is synchronized across processes
            using file locking to ensure consistency. After publishing,
            touches a notification file to wake subscribers via
            filesystem events.

            :param type:
                The type of discovery event.
            :param metadata:
                Worker metadata to publish.
            :raises RuntimeError:
                If an unexpected event type is provided, or the segment is
                not yet initialized (a peer attached before the owner
                stamped the header); the pool's startup aborts and retries
                in that case.
            :raises DiscoveryCapacityExhausted:
                For ``worker-added``, if the segment is already at capacity.
            :raises TimeoutError:
                If the cross-process file lock is not acquired within this
                publisher's ``lock_timeout``.
            """
            async with _lock(self._namespace, timeout=self._lock_timeout):
                with _shared_memory(_short_hash(self._namespace)) as address_space:
                    if (
                        address_space.buf is None
                        or _read_capacity(address_space.buf) is None
                    ):  # pragma: no cover
                        raise RuntimeError("Registrar service not properly initialized")
                    match type:
                        case "worker-added":
                            await self._add(metadata, address_space)
                        case "worker-dropped":
                            await self._drop(metadata, address_space)
                        case "worker-updated":
                            await self._update(metadata, address_space)
                        case _:
                            raise RuntimeError(
                                f"Unexpected discovery event type: {type}"
                            )

                # Notify subscribers by touching the notification file
                _watchdog_path(self._namespace).touch()

        async def _add(self, metadata: WorkerMetadata, address_space: SharedMemory):
            """Register a worker by adding it to shared memory.

            :param metadata:
                The worker to publish to the namespace's shared memory.
            :raises DiscoveryCapacityExhausted:
                If no slots are available.
            :raises ValueError:
                If the worker UID is not specified.
            """
            assert address_space.buf is not None

            ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()
            size = len(serialized)

            for i, slot in _iter_slots(address_space.buf):
                if slot == NULL_REF:
                    try:
                        memory_block = await self._shared_memory_pool.acquire(str(ref))
                        assert memory_block.buf is not None
                        struct.pack_into(
                            f"I{size}s", memory_block.buf, 0, size, serialized
                        )
                        struct.pack_into("16s", address_space.buf, i, ref.bytes)
                    except Exception:
                        # Release what this method acquired rather than
                        # delegating to `_drop`, whose slot scan cannot find a
                        # ref that only lands on the last line of this block.
                        await self._shared_memory_pool.release(str(ref))
                        raise
                    break
            else:
                raise DiscoveryCapacityExhausted(_read_capacity(address_space.buf))

        async def _drop(self, metadata: WorkerMetadata, address_space: SharedMemory):
            """Unregister a worker by removing it from shared memory.

            :param metadata:
                The worker to unpublish from the namespace's shared memory.
            """
            assert address_space.buf is not None

            target_ref = _WorkerReference(metadata.uid)

            for i, slot in _iter_slots(address_space.buf):
                if slot != NULL_REF:
                    ref = _WorkerReference.from_bytes(slot)
                    if ref == target_ref:
                        struct.pack_into("16s", address_space.buf, i, NULL_REF)
                        await self._shared_memory_pool.release(str(ref))
                        break

        async def _update(self, metadata: WorkerMetadata, address_space: SharedMemory):
            """Update a worker's properties in shared memory.

            :param metadata:
                The updated worker to publish to the namespace's shared memory.
            :raises KeyError:
                If the worker is not found in the address space.
            """
            assert address_space.buf is not None

            target_ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()
            size = len(serialized)

            for _, slot in _iter_slots(address_space.buf):
                if slot != NULL_REF:
                    ref = _WorkerReference.from_bytes(slot)
                    if ref == target_ref:
                        memory_block = await self._shared_memory_pool.acquire(str(ref))
                        assert memory_block.buf is not None
                        # Save prior state before updating
                        prior_size = struct.unpack_from("I", memory_block.buf, 0)[0]
                        prior_serialized = struct.unpack_from(
                            f"{prior_size}s", memory_block.buf, 4
                        )[0]
                        try:
                            struct.pack_into(
                                f"I{size}s", memory_block.buf, 0, size, serialized
                            )
                        except Exception:
                            # Restore prior state on failure
                            struct.pack_into(
                                f"I{prior_size}s",
                                memory_block.buf,
                                0,
                                prior_size,
                                prior_serialized,
                            )
                            raise
                        return

            # Worker not found in address space
            raise KeyError(f"Worker {metadata.uid} not found in address space")

        def _shared_memory_factory(self, name: str):
            """Create a new shared memory block for worker metadata storage.

            Creates a shared memory region with the specified name and
            registers an atexit handler to ensure cleanup on process
            termination. Used by the resource pool to allocate memory blocks
            for individual worker metadata.

            :param name:
                The name for the shared memory block (typically a worker UUID
                hex string).
            :returns:
                A new SharedMemory instance.
            """
            shared_memory = SharedMemory(
                name=name,
                create=True,
                size=self._block_size,
            )

            def cleanup():  # pragma: no cover
                _unlink_quietly(shared_memory)

            self._cleanups[name] = atexit.register(cleanup)
            return shared_memory

        def _shared_memory_finalizer(self, shared_memory: SharedMemory):
            """Clean up a shared memory block when released from the pool.

            Unregisters the atexit handler before unlinking the block, so a
            failed unlink cannot leave the handler armed to fire again at
            interpreter shutdown. The unlink goes through `_unlink_quietly`;
            see it for the failure semantics.

            :param shared_memory:
                The SharedMemory instance to finalize.
            """
            atexit.unregister(self._cleanups.pop(shared_memory.name))
            _unlink_quietly(shared_memory)

    class Subscriber(
        metaclass=SubscriberMeta,
        key=lambda cls, namespace, *, poll_interval=None: (
            cls,
            namespace,
            poll_interval,
        ),
    ):
        """Subscriber for receiving worker discovery events.

        Subscribes to worker discovery events (see `~wool.DiscoveryEvent`)
        from a shared memory region, monitoring for changes via filesystem
        notifications and yielding events as workers are added, updated, or
        dropped. Multiple subscribers in different processes can read from
        the same namespace independently.

        Uses watchdog to monitor a notification file that publishers touch
        when modifying the shared memory, providing near-instant notification
        of changes. Falls back to periodic polling if notifications are
        delayed or missed.

        Instances are cached as singletons — two calls with the same
        ``namespace`` and ``poll_interval`` return the same object.

        Each call to ``__aiter__`` creates an isolated consumer fed from a
        shared-memory watch shared across consumers of the same namespace.
        The shared watch fans out, i.e., every concurrent iteration
        receives the full event stream, and the iterations are otherwise
        independent.

        :param namespace:
            The namespace identifier for the shared memory region.
        :param poll_interval:
            Maximum polling interval in seconds for when filesystem
            notifications are delayed or missed.
        """

        _namespace: Final[str]
        _poll_interval: Final[float | None]

        def __init__(
            self,
            namespace: str,
            *,
            poll_interval: float | None = None,
        ):
            self._namespace = namespace
            if poll_interval is not None and poll_interval < 0:
                raise ValueError(f"Expected positive poll interval, got {poll_interval}")
            self._poll_interval = poll_interval

        async def _shutdown(self) -> None:
            """Clean up shared subscription state for this subscriber."""

        def __reduce__(self):
            return type(self), (self._namespace,)

        def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
            return self._event_stream()

        @property
        def namespace(self):
            """The namespace identifier for this subscriber."""
            return self._namespace

        async def _event_stream(self) -> AsyncGenerator[DiscoveryEvent, None]:
            """Monitor shared memory for worker changes via filesystem
            notifications.

            Sets up a watchdog filesystem observer to monitor the
            notification file for modifications. When publishers touch
            the file (after updating shared memory), the observer
            triggers scanning of the shared memory address space. Falls
            back to periodic polling in case notifications are delayed
            or missed.

            :yields:
                Discovery events as changes are detected in shared
                memory.
            """
            cached_workers: dict[str, WorkerMetadata] = {}
            notification = asyncio.Event()
            lock = asyncio.Lock()
            loop = asyncio.get_running_loop()
            if not (watchdog := _watchdog_path(self._namespace)).exists():
                watchdog.touch()
            handler = _Watchdog(notification, watchdog, lock, loop)
            observer = Observer()
            observer.schedule(handler, path=str(watchdog.parent), recursive=False)
            observer.start()

            try:
                with _shared_memory(_short_hash(self._namespace)) as address_space:
                    assert address_space.buf is not None

                    while True:
                        async with lock:
                            notification.clear()
                            discovered_workers: dict[str, WorkerMetadata] = {}
                            for _, slot in _iter_slots(address_space.buf):
                                if slot != NULL_REF:
                                    ref = _WorkerReference.from_bytes(slot)
                                    metadata = self._deserialize_metadata(str(ref))
                                    discovered_workers[str(metadata.uid)] = metadata

                            for event in self._diff(cached_workers, discovered_workers):
                                yield event
                        try:
                            await asyncio.wait_for(
                                notification.wait(),
                                timeout=self._poll_interval,
                            )
                        except asyncio.TimeoutError:
                            pass
            finally:
                observer.stop()
                observer.join()

        def _deserialize_metadata(self, ref: str):
            """Load and deserialize worker metadata from shared memory.

            Opens the shared memory block identified by the reference string
            (worker UUID hex), reads the size header and serialized protobuf
            data, and reconstructs the WorkerMetadata instance.

            :param ref:
                The worker reference string (UUID hex) identifying the shared
                memory block containing the worker's metadata.
            :returns:
                The deserialized WorkerMetadata instance.
            """
            with _shared_memory(ref) as memory_block:
                assert memory_block.buf is not None
                size = struct.unpack_from("I", memory_block.buf, 0)[0]
                serialized = struct.unpack_from(f"{size}s", memory_block.buf, 4)[0]
                protobuf = wire.WorkerMetadata.FromString(serialized)
                return WorkerMetadata.from_protobuf(protobuf)

        def _diff(
            self,
            cached_workers: dict[str, WorkerMetadata],
            discovered_workers: dict[str, WorkerMetadata],
        ):
            """Detect and emit events for worker changes.

            Performs a three-way comparison between the cached worker state and
            the newly discovered workers, identifying which workers have been
            added, dropped, or updated. Updates the cache in-place and yields
            appropriate discovery events for each change.

            :param cached_workers:
                Dictionary of previously discovered workers (UID string ->
                WorkerMetadata). Modified in-place to reflect current state.
            :param discovered_workers:
                Dictionary of workers found in the current scan (UID string ->
                WorkerMetadata).
            :yields:
                Discovery events for each detected change (worker-added,
                worker-dropped, worker-updated).
            """

            # Identify added workers
            for uid in set(discovered_workers) - set(cached_workers):
                cached_workers[uid] = discovered_workers[uid]
                event = DiscoveryEvent("worker-added", metadata=discovered_workers[uid])
                yield event

            # Identify removed workers
            for uid in set(cached_workers) - set(discovered_workers):
                discovered_worker = cached_workers.pop(uid)
                event = DiscoveryEvent("worker-dropped", metadata=discovered_worker)
                yield event

            # Identify updated workers
            for uid in set(cached_workers) & set(discovered_workers):
                cached_workers[uid] = discovered_workers[uid]
                event = DiscoveryEvent(
                    "worker-updated", metadata=discovered_workers[uid]
                )
                yield event


def _read_capacity(buf: memoryview) -> int | None:
    """Return the owner-stamped capacity, or ``None`` when unstamped.

    The owner writes `_HEADER_MAGIC` and the capacity into the segment
    header on entry. An attacher that raced that write — or any segment
    not created by this module — reads a mismatched magic and is reported
    as not-yet-ready (`None`) rather than trusting a zero-filled header. A
    subscriber re-reads on its next scan; a publisher instead raises, and
    the pool's startup aborts and retries.

    :param buf:
        The mapped address-space buffer.
    :returns:
        The stamped capacity, or ``None`` when the header magic is absent.
    """
    magic, capacity = struct.unpack_from("<4sI", buf, 0)
    if magic != _HEADER_MAGIC:  # pragma: no cover
        return None
    return capacity


def _iter_slots(buf: memoryview) -> Iterator[tuple[int, bytes]]:
    """Yield each ``(offset, ref_bytes)`` slot bounded by the stamped capacity.

    Reads the owner-stamped capacity from the header and walks exactly
    that many slots, so ``capacity`` — not the page-rounded mapping — is
    the enforced ceiling. Yields nothing for a segment whose header is not
    yet stamped, so a subscriber simply re-reads on its next scan. The
    capacity is re-read on every call, so a scan always reflects the
    current header.

    :param buf:
        The mapped address-space buffer to scan.
    :yields:
        ``(offset, ref_bytes)`` for each 16-byte slot, in order.
    """
    capacity = _read_capacity(buf)
    if capacity is None:  # pragma: no cover
        return
    limit = _HEADER_SIZE + capacity * REF_WIDTH
    for offset in range(_HEADER_SIZE, limit, REF_WIDTH):
        yield offset, struct.unpack_from("16s", buf, offset)[0]


def _unlink_quietly(shared_memory: SharedMemory) -> None:
    """Unlink a segment without raising, warning if it fails unexpectedly.

    Every teardown path in this module unlinks through here, so no teardown
    can raise: an exception escaping a ``__exit__`` or an `atexit` handler
    would replace the exception the caller was already unwinding, or crash
    the interpreter at shutdown.

    A segment that is already gone is the expected case and passes silently —
    any process that attached to it may have unlinked it first (bpo-38119).
    Any other failure leaves the segment allocated, which is a leak the caller
    cannot act on but an operator can, so it surfaces as a `ResourceWarning`
    rather than being swallowed.

    :param shared_memory:
        The segment to unlink.
    """
    try:
        shared_memory.unlink()
    except FileNotFoundError:
        pass
    except OSError as error:
        warnings.warn(
            f"failed to unlink shared memory {shared_memory.name!r}: {error}",
            ResourceWarning,
            stacklevel=2,
        )


def _short_hash(s: str, n: int = 30) -> str:
    """Create a shortened hash of a string for use as a system identifier.

    Generates a SHA-256 hash of the input string and returns the first n
    characters of a URL-safe base64 encoding. This encoding provides 50% more
    entropy than hexadecimal in the same space (180 bits vs 120 bits for 30
    chars). Used to create platform-safe names for shared memory regions and
    lock files that fit within system limits (31 chars on macOS, 255 on Linux).

    :param s:
        The string to abbreviate (typically a namespace identifier).
    :param n:
        Number of base64 characters to return. Defaults to 30 for macOS
        compatibility.
    :returns:
        The first n characters of the URL-safe base64-encoded SHA-256 hash.
        Uses character set: A-Za-z0-9-_
    """
    import base64

    hash_bytes = hashlib.sha256(s.encode()).digest()
    # URL-safe base64 encoding (replaces + with -, / with _)
    b64_str = base64.urlsafe_b64encode(hash_bytes).decode("utf-8")
    # Remove padding characters and truncate to n chars
    return b64_str.rstrip("=")[:n]


@contextmanager
def _shared_memory(name):
    """Open an existing shared memory region by name.

    Context manager that opens a shared memory region for reading or writing
    and ensures it is properly closed on exit. Does not create new memory
    regions (use SharedMemory with create=True for that).

    :param name:
        The name of the shared memory region to open.
    :yields:
        An open SharedMemory instance.

    .. note::
        Close errors are silently ignored to handle cases where the memory
        region has been unlinked by another process.
    """
    shared_memory = SharedMemory(name=name)
    try:
        yield shared_memory
    finally:
        try:
            shared_memory.close()
        except Exception:
            pass  # pragma: no cover


@asynccontextmanager
async def _lock(namespace: str, *, timeout: float | None = DEFAULT_LOCK_TIMEOUT):
    """Acquire an exclusive lock for the address space identified by namespace.

    Uses cross-platform file locking (via portalocker) to synchronize access
    across unrelated processes that may be publishing to the same shared
    memory region. Works on Windows, Linux, and macOS, and does not block the
    event loop while waiting for acquisition.

    ``timeout`` bounds **acquisition only, never the held section**. Once the
    lock is held the ``with`` body runs to completion regardless of how long
    it takes.

    :param namespace:
        The namespace identifying the shared memory region to lock.
    :param timeout:
        Maximum seconds to wait for acquisition. ``None`` waits forever.
        Defaults to `DEFAULT_LOCK_TIMEOUT`.
    :raises TimeoutError:
        If the lock is not acquired within ``timeout`` seconds.

    .. rubric:: Implementation notes

    Acquisition uses non-blocking ``portalocker`` attempts, retrying every
    1ms (``await asyncio.sleep(0.001)``) until the lock is acquired or
    ``timeout`` elapses. The lock holder is by definition another process, so
    a hot spin here cannot make it release sooner — it would only burn CPU
    competing with the process being waited on — hence the 1ms poll rather
    than a zero-second yield.

    The held section runs to completion because interrupting a holder
    mid-write would corrupt the shared segment for every attached process.
    """
    lock_name = _short_hash(namespace)
    lock_path = Path(tempfile.gettempdir()).resolve() / f"wool-lock-{lock_name}"

    with open(lock_path, "w") as lock_file:
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            try:
                portalocker.lock(lock_file, portalocker.LOCK_EX | portalocker.LOCK_NB)
                break
            except portalocker.LockException:
                if deadline is not None and loop.time() >= deadline:
                    raise TimeoutError(
                        f"Timed out after {timeout}s waiting to acquire the "
                        f"discovery lock for namespace {namespace!r}"
                    )
                await asyncio.sleep(0.001)

        try:
            yield
        finally:
            portalocker.unlock(lock_file)


def _watchdog_path(namespace: str) -> Path:
    """Get the path to the notification file for a namespace.

    Returns the path to a temporary file that publishers touch when modifying
    the shared memory region, signaling subscribers to scan for changes.

    :param namespace:
        The namespace identifying the shared memory region.
    :returns:
        Path to the notification file for this namespace.
    """
    directory = Path(tempfile.gettempdir()).resolve() / f"wool-{namespace}"
    directory.mkdir(exist_ok=True)
    return directory / "notify"
