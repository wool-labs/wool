from __future__ import annotations

import asyncio
import atexit
import hashlib
import os
import struct
import sys
import tempfile
import threading
import warnings
from contextlib import asynccontextmanager
from contextlib import contextmanager
from multiprocessing import resource_tracker
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Callable
from typing import Final
from typing import Generator
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
from wool.runtime.discovery.exceptions import DiscoveryBlockExhausted
from wool.runtime.discovery.exceptions import DiscoveryCapacityExhausted
from wool.runtime.discovery.exceptions import DiscoveryWorkerNotFound
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
# Serialises the resource-tracker rebind window; see `_suppressing` for the
# scoping constraint that makes it safe. Taken by every construction this
# module performs, create and attach alike, and on every Python version —
# `_attach` skips it only where `track` makes the window unnecessary.
_tracker_lock: threading.Lock = threading.Lock()

# The shims an open window has installed, keyed by hook name, each paired
# with the hook it displaced. Written only under `_tracker_lock`, so a
# forked child can restore precisely what this module changed and nothing
# else.
_shims: dict[str, tuple[Callable, Callable]] = {}


def _reinit_tracker_state() -> None:  # pragma: no cover — fork-only path
    """Reset the suppression state a forked child inherited mid-window.

    A ``fork`` inside the rebind window copies the lock held, wedging every
    later attach or create in the child. Wool starts its own workers with
    ``spawn``, but `LocalDiscovery` is public and runs inside host processes
    that may fork — the default start method on Linux below 3.14.

    The shims are unwound too: a fork inside the window leaves the child
    holding them, since the frame that would have put them back died with
    the parent's thread. Each is replaced by the hook it displaced rather
    than by a snapshot taken at import, so a child forked with no window
    open changes nothing, and instrumentation another library installed
    after this module was imported survives.
    """
    global _tracker_lock
    _tracker_lock = threading.Lock()
    for hook, (shim, forward) in _shims.items():
        if getattr(resource_tracker, hook) is shim:
            setattr(resource_tracker, hook, forward)
    _shims.clear()


if hasattr(os, "register_at_fork"):  # pragma: no branch — POSIX-only guard
    os.register_at_fork(after_in_child=_reinit_tracker_state)


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
        """Return the `SharedMemory` name identifying this worker's block.

        :returns:
            The UUID abbreviated by `_short_hash` — 30 characters of
            URL-safe base64, short enough for the platform name limit.
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
        re-declaring it. Publishing a new worker once ``capacity`` are
        registered raises `DiscoveryCapacityExhausted`; see
        `LocalDiscovery.Publisher.publish` for the full contract.
        Defaults to 128.
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
            self._address_space = _attach(_short_hash(self._namespace))
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

            Publishing ``worker-added`` for a worker that is already
            registered refreshes the registration in place — last write
            wins — consuming no additional slot, so a single
            ``worker-dropped`` always fully unregisters the worker. Live
            subscribers observe a refresh as a ``worker-updated`` event.
            A refresh writes into the block created at the worker's first
            registration, so ``block_size`` governs only blocks this
            publisher creates. If that block has vanished — e.g., its
            publisher exited without dropping the worker — the re-add
            reclaims the stale registration and registers the worker
            fresh.

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
                For ``worker-added``, if the segment is already at capacity
                and the worker is not already registered.
            :raises DiscoveryWorkerNotFound:
                For ``worker-updated``, if the worker is not registered.
            :raises DiscoveryBlockExhausted:
                For ``worker-added`` and ``worker-updated``, if the
                serialized metadata exceeds the worker's block; the prior
                registration is restored before the error propagates.
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
            """Register a worker, or refresh one already registered.

            See `publish` for the re-add contract. The refresh attaches to
            the existing block by name rather than acquiring it from the
            pool, so it holds no pool reference and reaches blocks created
            by another publisher's pool.

            :param metadata:
                The worker to publish to the namespace's shared memory.
            :raises DiscoveryCapacityExhausted:
                If no slots are available and the worker is not already
                registered.
            """
            assert address_space.buf is not None

            ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()

            free_offset = None
            match_offset = None
            for offset, slot in _iter_slots(address_space.buf):
                if slot == ref.bytes:
                    match_offset = offset
                    break
                if free_offset is None and slot == NULL_REF:
                    free_offset = offset

            if match_offset is not None:
                try:
                    with _shared_memory(str(ref)) as memory_block:
                        assert memory_block.buf is not None
                        _rewrite_block(memory_block.buf, serialized)
                    return
                except FileNotFoundError:
                    # The block vanished out from under its slot — a dead
                    # publisher's teardown unlinks blocks without nulling
                    # slots — so reclaim the stale slot and register fresh.
                    struct.pack_into("16s", address_space.buf, match_offset, NULL_REF)
                    if free_offset is None:
                        free_offset = match_offset

            if free_offset is None:
                raise DiscoveryCapacityExhausted(_read_capacity(address_space.buf))

            try:
                memory_block = await self._shared_memory_pool.acquire(str(ref))
                assert memory_block.buf is not None
                size = len(serialized)
                try:
                    struct.pack_into(f"I{size}s", memory_block.buf, 0, size, serialized)
                except struct.error as error:
                    raise DiscoveryBlockExhausted(size) from error
                struct.pack_into("16s", address_space.buf, free_offset, ref.bytes)
            except Exception:
                # Release what this method acquired rather than delegating
                # to `_drop`, whose slot scan cannot find a ref that only
                # lands on the last line of this block.
                await self._shared_memory_pool.release(str(ref))
                raise

        async def _drop(self, metadata: WorkerMetadata, address_space: SharedMemory):
            """Unregister a worker by removing it from shared memory.

            :param metadata:
                The worker to unpublish from the namespace's shared memory.
            """
            assert address_space.buf is not None

            target_ref = _WorkerReference(metadata.uid)

            for offset, slot in _iter_slots(address_space.buf):
                if slot == target_ref.bytes:
                    struct.pack_into("16s", address_space.buf, offset, NULL_REF)
                    await self._shared_memory_pool.release(str(target_ref))
                    break

        async def _update(self, metadata: WorkerMetadata, address_space: SharedMemory):
            """Update a registered worker's metadata in shared memory.

            Attaches to the worker's block by name — holding no pool
            reference, and reaching blocks created by another publisher's
            pool — and rewrites it via `_rewrite_block`.

            :param metadata:
                The updated worker to publish to the namespace's shared memory.
            :raises DiscoveryWorkerNotFound:
                If the worker is not found in the address space.
            """
            assert address_space.buf is not None

            target_ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()

            for _, slot in _iter_slots(address_space.buf):
                if slot == target_ref.bytes:
                    with _shared_memory(str(target_ref)) as memory_block:
                        assert memory_block.buf is not None
                        _rewrite_block(memory_block.buf, serialized)
                    return

            raise DiscoveryWorkerNotFound(metadata.uid)

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


def _rewrite_block(buf: memoryview, serialized: bytes) -> None:
    """Rewrite a metadata block in place, restoring it on failure.

    Writes the size-prefixed payload over the block's current contents.
    If the write fails — e.g., the payload exceeds the block — the prior
    contents are restored before the error propagates, so a failed
    rewrite never corrupts or regresses the block's registration.

    :param buf:
        The mapped buffer of the block to rewrite.
    :param serialized:
        The serialized metadata to write.
    :raises DiscoveryBlockExhausted:
        If the payload does not fit the block.
    """
    size = len(serialized)
    prior_size = struct.unpack_from("I", buf, 0)[0]
    prior_serialized = struct.unpack_from(f"{prior_size}s", buf, 4)[0]
    try:
        struct.pack_into(f"I{size}s", buf, 0, size, serialized)
    except Exception as error:
        struct.pack_into(f"I{prior_size}s", buf, 0, prior_size, prior_serialized)
        if isinstance(error, struct.error):
            raise DiscoveryBlockExhausted(size) from error
        raise


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
def _suppressing(
    name: str, *, register: bool, unregister: bool = True
) -> Generator[None, None, None]:
    """Silence the tracker calls one `SharedMemory` constructor would make.

    Rebinds the named `resource_tracker` hooks for the duration of the
    block, so the calls `SharedMemory.__init__` makes on this module's
    behalf never reach the tracker process. Suppressing ``unregister`` is
    always right: the constructor's own failure handler issues one whether
    or not the name was ever registered. Suppressing ``register`` too is
    right only where the mapping must not be tracked at all — a
    construction that creates the segment leaves it set, so the entry its
    own later `SharedMemory.unlink` needs is made.

    .. important::
        The block MUST contain exactly one `SharedMemory` construction and
        nothing that constructs shared memory transitively. `_tracker_lock`
        is not reentrant, so widening the window — across a
        ``try``/``except`` that falls back to another construction, or
        across a `_shared_memory` body whose nested attach `_add`
        performs — deadlocks the process with nothing to diagnose from.

    :param name:
        The segment name whose tracker calls are suppressed. Callers pass
        it bare; `SharedMemory` registers names slash-prefixed on POSIX,
        so both sides are stripped before matching.
    :param register:
        Whether to suppress `resource_tracker.register`.
    :param unregister:
        Whether to suppress `resource_tracker.unregister`.
    :yields:
        Nothing. The block runs with the hooks installed, and they are
        restored whether it returns or raises.
    :raises TimeoutError:
        If the lock cannot be acquired within `DEFAULT_LOCK_TIMEOUT`.

    .. rubric:: Implementation notes

    Rebinding works because `SharedMemory` resolves ``register`` and
    ``unregister`` as attributes of the `resource_tracker` *module* at call
    time, on every supported version. A future CPython that imported them
    by value instead would leave these shims installed and consulted by
    nobody — silently, in production. What catches that is
    ``tests/integration/test_local_discovery_tracker.py``: its controls
    assert the unguarded fault still reproduces, so a CPython that fixed or
    restructured this turns that suite red rather than turning the guard
    into a no-op.

    The tracker keeps a per-resource-type `set`, so removing a name it does
    not hold raises `KeyError` in its main loop. That traceback is printed
    by the tracker process to the stderr it inherited, after the
    interpreter that caused it has already exited 0 — nothing in this
    process can intercept it, which is why suppression at the source is
    the only remedy available.

    The suppression matches on the calling thread, the resource type, and
    this segment's name, so the only calls it can swallow are the ones made
    on the wrapped construction's behalf. A construction running
    concurrently on another thread stays tracked even for the same name.

    The lock serialises the rebind. Without it two overlapping windows
    would each capture the other's shim: the second window's call would
    reach a shim whose thread does not match, be forwarded to the real
    tracker, and take effect after all — reinstating for an attach the
    registration bpo-38119 makes fatal, and for a create the stray
    unregister. That a shim would also be left permanently installed is the
    lesser effect. Acquisition is bounded rather than indefinite, so a
    holder that wedges surfaces as an attributable error instead of a
    process-wide stall.
    """
    lock = _tracker_lock
    if not lock.acquire(timeout=DEFAULT_LOCK_TIMEOUT):
        raise TimeoutError(
            f"Timed out acquiring the resource-tracker suppression lock "
            f"after {DEFAULT_LOCK_TIMEOUT}s"
        )
    constructing = threading.get_ident()
    target = name.lstrip("/")
    installed: list[str] = []
    active = True

    def _matches(name: str, rtype: str) -> bool:
        return (
            active
            and threading.get_ident() == constructing
            and rtype == "shared_memory"
            and name.lstrip("/") == target
        )

    def _install(hook: str) -> None:
        forward = getattr(resource_tracker, hook)

        def _suppressed(name: str, rtype: str) -> None:
            if not _matches(name, rtype):
                forward(name, rtype)

        setattr(resource_tracker, hook, _suppressed)
        _shims[hook] = (_suppressed, forward)
        installed.append(hook)

    try:
        if register:
            _install("register")
        if unregister:
            _install("unregister")
        yield
    finally:
        # Neutralise before unwinding. A shim a third party wrapped stays
        # in their delegation chain for the life of the process, where it
        # would otherwise keep matching this segment's name against a
        # thread ident the interpreter is free to reuse once this thread
        # dies — swallowing a later, legitimate call for the same name.
        active = False
        for hook in installed:
            shim, forward = _shims.pop(hook)
            # Only restore what is still ours: a third party that rebound
            # this hook inside the window keeps its wrapper.
            if getattr(resource_tracker, hook) is shim:
                setattr(resource_tracker, hook, forward)
        lock.release()


def _attach(name: str) -> SharedMemory:
    """Map an existing shared memory segment without tracking it.

    Only the process that created a segment may own its lifetime. An
    attach-only mapping registered with this process's resource tracker
    would be unlinked when this process exits, out from under its owner
    (bpo-38119), so this maps the segment without registering it.

    A segment mapped through here MUST NOT be unlinked. Where `track` is
    unavailable `SharedMemory.unlink` unregisters unconditionally, which
    would discard the creator's entry after all; teardown in this module
    unlinks only segments it created, through `_unlink_quietly`.

    :param name:
        The name of the shared memory segment to map.
    :returns:
        An untracked `SharedMemory` mapped to the named segment.
    :raises FileNotFoundError:
        If no segment of that name exists. The tracker hooks are restored
        whether the mapping succeeds or raises.

    .. rubric:: Implementation notes

    Python 3.13 says this directly with ``track=False``. Below it there is
    no such parameter, so the registration is suppressed at its source for
    as long as the constructor runs. Undoing it afterwards, i.e. registering
    and then unregistering, is *not* equivalent: the tracker's cache is a
    set shared by every process in the tree, so the first attach's
    unregister discards the entry the creator made, and the creator's own
    unlink later finds nothing to remove — which raises `KeyError` inside
    the tracker process and prints a traceback to stderr that nothing in the
    attaching process can intercept.

    Both hooks are rebound, not just `resource_tracker.register`: the
    constructor wraps its own ``mmap`` in ``except OSError: self.unlink()``,
    and that unlink issues the very unregister this function exists to
    avoid. It also calls ``shm_unlink``, which is CPython's to own and
    cannot be intercepted from here — a mid-construction `OSError` therefore
    still destroys the segment.

    See `_suppressing` for how the window is scoped and serialised, and
    for the tracker behaviour both callers are working around.
    """
    if sys.version_info >= (3, 13):
        # Unreachable below 3.13, where the parameter does not exist, so
        # the 3.11 and 3.12 legs would otherwise report it missing.
        return SharedMemory(name=name, track=False)  # pragma: no cover

    with _suppressing(name, register=True):
        return SharedMemory(name=name)


@contextmanager
def _shared_memory(name):
    """Open an existing shared memory region by name.

    Context manager that opens a shared memory region for reading or writing
    and ensures it is properly closed on exit. Does not create new memory
    regions (use `SharedMemory` with ``create=True`` for that). The mapping
    is untracked — see `_attach` for why, and for what that forbids.

    :param name:
        The name of the shared memory region to open.
    :yields:
        An open SharedMemory instance.

    .. note::
        Close errors are silently ignored to handle cases where the memory
        region has been unlinked by another process.
    """
    shared_memory = _attach(name)
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
