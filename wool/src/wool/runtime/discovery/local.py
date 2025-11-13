from __future__ import annotations

import asyncio
import atexit
import hashlib
import struct
import tempfile
from contextlib import asynccontextmanager
from contextlib import contextmanager
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
from typing import AsyncIterator
from typing import Callable
from typing import Final
from typing import Self
from uuid import UUID
from uuid import uuid4

import portalocker
from watchdog.events import FileSystemEvent
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from wool.runtime.discovery.base import Discovery
from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.base import DiscoveryEventType
from wool.runtime.discovery.base import DiscoveryPublisherLike
from wool.runtime.discovery.base import DiscoverySubscriberLike
from wool.runtime.discovery.base import PredicateFunction
from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.protobuf.worker import WorkerMetadata as WorkerMetadataProtobuf
from wool.runtime.resourcepool import ResourcePool

REF_WIDTH: Final = 16
NULL_REF: Final = b"\x00" * REF_WIDTH


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

    def __bytes__(self) -> bytes:
        """Bytes representation for address space storage.

        :returns:
            The UUID as 16 bytes.
        """
        return self._uuid.bytes

    def __hash__(self) -> int:
        return hash(self._uuid)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _WorkerReference):
            return self._uuid == other._uuid
        return NotImplemented

    def __repr__(self) -> str:
        return f"_WorkerReference({self._uuid})"

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
        if len(data) != REF_WIDTH:
            raise ValueError(f"Expected 16 bytes, got {len(data)}")
        if data == NULL_REF:
            raise ValueError("Cannot create _WorkerReference from NULL bytes")
        ref = object.__new__(cls)
        ref._uuid = UUID(bytes=data)
        return ref

    @property
    def uuid(self) -> UUID:
        """The UUID this reference points to.

        :returns:
            The UUID instance.
        """
        return self._uuid

    @property
    def bytes(self) -> bytes:
        """The 16-byte representation for address space storage.

        :returns:
            The UUID as 16 bytes.
        """
        return self._uuid.bytes


# public
class LocalDiscovery(Discovery):
    """Local discovery service using shared memory.

    Provides worker discovery within a single machine using shared
    memory for communication between publishers and subscribers.
    Multiple unrelated processes can share the same discovery
    namespace by using the same namespace identifier, enabling
    automatic worker discovery across process boundaries.

    The namespace identifies a shared memory region where worker
    metadata is stored. Publishers write worker metadata to this
    region, and subscribers read from it to discover available
    workers. All access is synchronized using file-based locking to
    ensure consistency.

    :param namespace:
        Unique identifier for the shared memory region. Publishers
        and subscribers using the same namespace will see each
        other's workers.
    :param capacity:
        Maximum number of workers that can be registered
        simultaneously. Defaults to 128.
    :param block_size:
        Size in bytes for each worker's serialized data block.
        Defaults to 1024.

    .. note::
        Multiple unrelated processes can create publishers and
        subscribers with the same namespace. They will automatically
        discover each other's workers through the shared memory
        region.

    Example usage:

    Publish workers

    .. code-block:: python

        publisher = LocalDiscovery.Publisher("my-worker-pool")
        async with publisher:
            await publisher.publish("worker-added", metadata)

    Subscribe to workers

    .. code-block:: python

        subscriber = LocalDiscovery.Subscriber("my-worker-pool")
        async for event in subscriber:
            print(f"Discovered worker: {event.metadata}")
    """

    _namespace: Final[str]

    def __init__(
        self,
        namespace: str | None = None,
        *,
        capacity: int = 128,
        block_size: int = 1024,
    ):
        self._namespace = namespace or f"workerpool-{uuid4()}"
        self._capacity = capacity
        self._block_size = block_size

    def __enter__(self) -> Self:
        size = self._capacity * 4
        self._address_space = SharedMemory(
            name=_short_hash(self._namespace),
            create=True,
            size=size,
        )
        assert self._address_space.buf
        self._cleanup = atexit.register(lambda: self._address_space.unlink())
        for i in range(size):
            self._address_space.buf[i] = 0
        return self

    def __exit__(self, *_):
        self._address_space.unlink()
        atexit.unregister(self._cleanup)

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
        return self.Publisher(self._namespace, block_size=self._block_size)

    @property
    def subscriber(self) -> DiscoverySubscriberLike:
        """The default subscriber for all worker events.

        :returns:
            A subscriber instance that receives all worker discovery
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
            events.
        :param poll_interval:
            Optional interval in seconds between shared memory polls.
            If not specified, uses filesystem notifications for
            efficient updates.
        :returns:
            A subscriber instance that receives filtered worker
            discovery events.
        """
        return self.Subscriber(self._namespace, filter, poll_interval=poll_interval)

    class Publisher:
        """Publisher for broadcasting worker discovery events.

        Publishes worker :class:`discovery events
        <~wool.DiscoveryEvent>` to a shared memory region where
        subscribers can discover them. Multiple publishers in different
        processes can safely write to the same namespace using
        cross-platform file locking for synchronization.

        :param namespace:
            The namespace identifier for the shared memory region.
        :param block_size:
            Size in bytes for worker metadata storage blocks. Defaults
            to 512 bytes, which accommodates typical worker
            metadata including tags and extra metadata.
        :raises ValueError:
            If block_size is negative.
        """

        _block_size: int
        _cleanups: dict[str, Callable]
        _namespace: Final[str]
        _shared_memory_pool: ResourcePool[SharedMemory]

        def __init__(self, namespace: str, *, block_size: int = 512):
            if block_size < 0:
                raise ValueError("Block size must be positive")
            self._namespace = namespace
            self._block_size = block_size
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
                If an unexpected event type is provided or if the
                shared memory is not properly initialized.
            """
            async with _lock(self._namespace):
                with _shared_memory(_short_hash(self._namespace)) as address_space:
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
            :raises RuntimeError:
                If the shared memory region is not properly initialized or no
                slots are available.
            :raises ValueError:
                If the worker UID is not specified.
            """
            if address_space.buf is None:
                raise RuntimeError("Registrar service not properly initialized")

            ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()
            size = len(serialized)

            for i in range(0, len(address_space.buf), REF_WIDTH):
                slot = struct.unpack_from("16s", address_space.buf, i)[0]
                if slot == NULL_REF:
                    try:
                        memory_block = await self._shared_memory_pool.acquire(str(ref))
                        assert memory_block.buf is not None
                        struct.pack_into(
                            f"I{size}s", memory_block.buf, 0, size, serialized
                        )
                        struct.pack_into("16s", address_space.buf, i, ref.bytes)
                    except Exception:
                        await self._drop(metadata, address_space)
                        raise
                    break
            else:
                raise RuntimeError("No available slots in shared memory registrar")

        async def _drop(self, metadata: WorkerMetadata, address_space: SharedMemory):
            """Unregister a worker by removing it from shared memory.

            :param metadata:
                The worker to unpublish from the namespace's shared memory.
            :raises RuntimeError:
                If the registrar service is not properly initialized.
            """
            if address_space.buf is None:
                raise RuntimeError("Registrar service not properly initialized")

            target_ref = _WorkerReference(metadata.uid)

            for i in range(0, len(address_space.buf), REF_WIDTH):
                slot = struct.unpack_from("16s", address_space.buf, i)[0]
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
            :raises RuntimeError:
                If the registrar service is not properly initialized.
            :raises KeyError:
                If the worker is not found in the address space.
            """
            if address_space.buf is None:
                raise RuntimeError("Registrar service not properly initialized")

            target_ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()
            size = len(serialized)

            for i in range(0, len(address_space.buf), REF_WIDTH):
                slot = struct.unpack_from("16s", address_space.buf, i)[0]
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

            def cleanup():
                try:
                    shared_memory.unlink()
                except OSError:
                    pass

            self._cleanups[name] = atexit.register(cleanup)
            return shared_memory

        def _shared_memory_finalizer(self, shared_memory: SharedMemory):
            """Clean up a shared memory block when released from the pool.

            Unlinks the shared memory region and unregisters the atexit
            handler. Errors during unlink are silently ignored to handle
            Windows platforms where unlink may fail if other processes still
            have the memory file open.

            :param shared_memory:
                The SharedMemory instance to finalize.
            """
            try:
                shared_memory.unlink()
            except OSError:
                pass
            atexit.unregister(self._cleanups.pop(shared_memory.name))

    class Subscriber:
        """Subscriber for receiving worker discovery events.

        Subscribes to worker :class:`discovery events <~wool.DiscoveryEvent>`
        from a shared memory region, monitoring for changes via filesystem
        notifications and yielding events as workers are added, updated, or
        dropped. Multiple subscribers in different processes can read from
        the same namespace independently.

        Uses watchdog to monitor a notification file that publishers touch
        when modifying the shared memory, providing near-instant notification
        of changes. Falls back to periodic polling if notifications are
        delayed or missed.

        :param namespace:
            The namespace identifier for the shared memory region.
        :param filter:
            Optional predicate function to filter workers. Only workers for
            which the predicate returns True will be included in events.
        :param poll_interval:
            Maximum polling interval in seconds for when filesystem
            notifications are delayed or missed.
        """

        _filter: Final[PredicateFunction | None]
        _namespace: Final[str]
        _poll_interval: Final[float | None]

        def __init__(
            self,
            namespace: str,
            filter: PredicateFunction | None = None,
            *,
            poll_interval: float | None = None,
        ):
            self._namespace = namespace
            self._filter = filter
            if poll_interval is not None and poll_interval < 0:
                raise ValueError(f"Expected positive poll interval, got {poll_interval}")
            self._poll_interval = poll_interval

        def __reduce__(self):
            return type(self), (self._namespace, self._filter)

        def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
            return self._event_stream(self._filter)

        @property
        def namespace(self):
            """The namespace identifier for this subscriber."""
            return self._namespace

        async def _event_stream(
            self, filter: PredicateFunction | None = None
        ) -> AsyncIterator[DiscoveryEvent]:
            """Monitor shared memory for worker changes via filesystem notifications.

            Sets up a watchdog filesystem observer to monitor the notification
            file for modifications. When publishers touch the file (after
            updating shared memory), the observer triggers scanning of the
            shared memory address space. Falls back to periodic polling in
            case notifications are delayed or missed.

            :param filter:
                Optional predicate function to filter workers. Only workers for
                which the predicate returns True will be included in events.
            :yields:
                Discovery events as changes are detected in shared memory.
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
                            for i in range(0, len(address_space.buf), REF_WIDTH):
                                slot = struct.unpack_from("16s", address_space.buf, i)[0]
                                if slot != NULL_REF:
                                    ref = _WorkerReference.from_bytes(slot)
                                    metadata = self._deserialize_metadata(str(ref))
                                    if filter is None or filter(metadata):
                                        discovered_workers[str(metadata.uid)] = metadata

                            for event in self._diff(cached_workers, discovered_workers):
                                yield event
                        try:
                            await asyncio.wait_for(
                                notification.wait(), timeout=self._poll_interval
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
                protobuf = WorkerMetadataProtobuf.FromString(serialized)
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
            pass


@asynccontextmanager
async def _lock(namespace: str):
    """Acquire an exclusive lock for the address space identified by namespace.

    Uses cross-platform file locking (via portalocker) to synchronize access
    across unrelated processes that may be publishing to the same shared
    memory region. Works on Windows, Linux, and macOS.

    Uses non-blocking lock attempts with async sleep to avoid blocking the
    event loop while waiting for lock acquisition. Retries every 1ms until
    the lock is acquired.

    :param namespace:
        The namespace identifying the shared memory region to lock.
    """
    lock_name = _short_hash(namespace)
    lock_path = Path(tempfile.gettempdir()) / f"wool-lock-{lock_name}"

    with open(lock_path, "w") as lock_file:
        while True:
            try:
                portalocker.lock(lock_file, portalocker.LOCK_EX | portalocker.LOCK_NB)
                break
            except portalocker.LockException:
                await asyncio.sleep(0)

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
    return Path(tempfile.gettempdir()) / f"wool-notify-{namespace}"
