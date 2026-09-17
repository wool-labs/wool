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
from contextlib import AsyncExitStack
from contextlib import asynccontextmanager
from contextlib import contextmanager
from multiprocessing import resource_tracker
from multiprocessing.shared_memory import SharedMemory
from pathlib import Path
from typing import TYPE_CHECKING
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
from wool.runtime.discovery.exceptions import DiscoveryBlockExhausted
from wool.runtime.discovery.exceptions import DiscoveryCapacityExhausted
from wool.runtime.discovery.exceptions import DiscoveryNamespaceInUse
from wool.runtime.discovery.exceptions import DiscoveryNamespaceNotFound
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
# Serializes the resource-tracker rebind; see `_attach`. Never hold it across
# a registry mapping's body: slot reads and writes attach worker blocks there,
# and the nested attach would deadlock.
_attach_lock: threading.Lock = threading.Lock()

# The tracker's own hooks, captured before anything can rebind them, so a
# forked child can be put back to a known state.
_tracker_register: Final = resource_tracker.register
_tracker_unregister: Final = resource_tracker.unregister


def _reinit_attach_lock() -> None:  # pragma: no cover — fork-only path
    """Reset the attach state a forked child inherited mid-window.

    A ``fork`` inside the rebind window copies the lock held, wedging every
    later attach in the child. Wool starts its own workers with ``spawn``,
    but `LocalDiscovery` is public and runs inside host processes that may
    fork — the default start method on Linux below 3.14.

    The hooks are restored too: a fork inside the window leaves the child
    holding this module's shims, since the frame that would have put them
    back died with the parent's thread.
    """
    global _attach_lock
    _attach_lock = threading.Lock()
    resource_tracker.register = _tracker_register
    resource_tracker.unregister = _tracker_unregister


if hasattr(os, "register_at_fork"):  # pragma: no branch — POSIX-only guard
    os.register_at_fork(after_in_child=_reinit_attach_lock)


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
        """The 16-byte representation stored in a registry slot.

        :returns:
            The UUID as 16 bytes.
        """
        return self._uuid.bytes


# public
class LocalDiscovery(Discovery):
    """Shared-memory discovery for single-machine worker pools.

    The default discovery protocol of a
    `~wool.runtime.worker.pool.WorkerPool` created without one. Processes
    on one host share a registry of workers identified by a namespace
    string, and a cross-process file lock serializes writes to it.

    **Ownership.** A namespace's registry has exactly one owner: the
    instance whose entry created it. Entering creates the registry, and
    exiting reclaims it. Entering a namespace whose registry already
    exists raises `DiscoveryNamespaceInUse`. An owner that never exits,
    e.g., one abandoned when the interpreter shuts down, reclaims the
    registry at shutdown. A killed owner's registry persists until the
    owner's `multiprocessing` resource tracker exits; for a child
    process, that is when its parent's tracker exits. Until then every
    entry on the namespace raises `DiscoveryNamespaceInUse` naming the
    segment to remove.

    **Borrowing.** `LocalDiscovery.Publisher` and
    `LocalDiscovery.Subscriber`, including those `publisher`,
    `subscriber` and `subscribe` return, borrow a namespace's registry
    and never create one. A borrower *binds* when it opens the registry:
    a publisher on entry and on each publish, a subscriber when its
    subscription starts. A bind where the namespace has no registry
    raises `DiscoveryNamespaceNotFound`.

    **Orphaning.** A borrower that outlives its owner is orphaned. A
    mapping it already holds stays readable. Its next bind raises
    `DiscoveryNamespaceNotFound`, unless a successor owner has entered
    the namespace, in which case the bind reaches the successor's
    registry. Orphaning is defined behavior: a borrower holds no claim on
    the registry, so it has no registry to reclaim. See
    `LocalDiscovery.Subscriber` for which subscriber iterations bind.

    **Lifecycle.** An instance is single-use: any entry attempt spends
    it, so a second entry raises `RuntimeError`. Retrying a rejected
    claim requires a new instance. Exiting never raises; a failed unlink
    surfaces as a `ResourceWarning`.

    :param namespace:
        Identifier of the registry. Defaults to a unique
        ``workerpool-<uuid>`` name.
    :param filter:
        Optional default predicate function to filter workers.
        Used by `subscriber` and as the default for `subscribe` when no
        explicit filter is provided.
    :param capacity:
        Maximum number of workers registered at once. The owner stamps
        it on entry and a borrower binds at the owner's capacity, so this
        value applies only when this instance is entered. See
        `LocalDiscovery.Publisher.publish` for exhaustion. Defaults to
        128.
    :param block_size:
        Size in bytes for each worker's serialized data block. Each
        block spends 4 bytes on a length prefix, leaving
        ``block_size - 4`` for the serialized metadata. Defaults to
        1024.
    :param lock_timeout:
        Maximum seconds each publisher waits for the cross-process file
        lock; see `LocalDiscovery.Publisher`. Defaults to
        `DEFAULT_LOCK_TIMEOUT`.
    :raises ValueError:
        If ``capacity`` or ``block_size`` is less than 1, or
        ``lock_timeout`` is negative.

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

    Example — borrow a namespace another process owns:

    .. code-block:: python

        async for event in LocalDiscovery.Subscriber("my-worker-pool"):
            print(f"Discovered worker: {event.metadata}")

    .. rubric:: Implementation notes

    Every teardown path unlinks through `_unlink_quietly`, so `__exit__`
    cannot replace an exception its caller is already unwinding. The
    shutdown fallback is an `atexit` handler registered on entry and
    unregistered on exit before the unlink runs, so a failed unlink
    leaves no handler armed to fire again at interpreter shutdown.

    The `multiprocessing` resource tracker reclaims a killed owner's
    registry: creating the registry registers its segment with the
    tracker, which unlinks the segment when it exits. One tracker serves
    a whole process tree, which is why a killed child's registry outlives
    it until the parent's tracker exits.

    No entry adopts a stranded registry. A liveness check and the
    adoption that follows it cannot be made atomic against an owner that
    is slow to respond, so adoption would hand one namespace to two
    owners. The caller, who knows which processes should exist, removes
    the segment.
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
        _validate_block_size(block_size)
        if lock_timeout is not None and lock_timeout < 0:
            raise ValueError("Lock timeout must be non-negative")
        self._namespace = namespace or f"workerpool-{uuid4()}"
        self._filter = filter
        self._capacity = capacity
        self._block_size = block_size
        self._lock_timeout = lock_timeout

    @noreentry
    def __enter__(self) -> Self:
        """Create the namespace's registry and claim its ownership.

        See `LocalDiscovery` for the ownership and teardown contract.

        :returns:
            This instance.
        :raises RuntimeError:
            If entry has already been attempted on this instance.
        :raises DiscoveryNamespaceInUse:
            If the namespace's registry already exists; see
            `LocalDiscovery`.
        """
        size = _HEADER_SIZE + self._capacity * REF_WIDTH
        segment = _short_hash(self._namespace)
        try:
            self._address_space = SharedMemory(
                name=segment,
                create=True,
                size=size,
            )
        except FileExistsError as error:
            raise DiscoveryNamespaceInUse(self._namespace, segment=segment) from error

        assert self._address_space.buf

        def cleanup():  # pragma: no cover
            _unlink_quietly(self._address_space)

        self._cleanup = atexit.register(cleanup)
        # A segment created exclusively starts zeroed, so every slot already
        # reads as `NULL_REF`.
        struct.pack_into(
            "<4sI", self._address_space.buf, 0, _HEADER_MAGIC, self._capacity
        )
        return self

    def __exit__(self, *_):
        """Reclaim the namespace's registry.

        See `LocalDiscovery` for the ownership and teardown contract.
        """
        atexit.unregister(self._cleanup)
        self._address_space.close()
        _unlink_quietly(self._address_space)

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
        """Return a subscriber to this namespace with optional filtering.

        :param filter:
            Optional predicate function to filter workers. Only workers
            for which the predicate returns True will be included in
            events. Falls back to the constructor's filter if not
            provided.
        :param poll_interval:
            See `LocalDiscovery.Subscriber`.
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
        a namespace's registry, where subscribers discover them.
        Publishers in different processes write to one namespace under a
        cross-process file lock. A publisher borrows the registry; see
        `LocalDiscovery` for the borrowing and orphaning contract.

        :param namespace:
            The namespace identifier for the registry to borrow.
        :param block_size:
            Size in bytes for worker metadata storage blocks. Each
            block spends 4 bytes on a length prefix, leaving
            ``block_size - 4`` for the serialized metadata. Defaults
            to 1024 bytes, which accommodates typical worker
            metadata including tags and extra metadata.
        :param lock_timeout:
            Maximum seconds to wait for the cross-process file lock before
            raising `TimeoutError`. ``None`` waits forever. Defaults to
            `DEFAULT_LOCK_TIMEOUT`.
        :raises ValueError:
            If ``block_size`` is less than 1, or ``lock_timeout`` is
            negative.
        """

        _block_size: int
        _blocks: dict[str, AsyncExitStack]
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
            _validate_block_size(block_size)
            if lock_timeout is not None and lock_timeout < 0:
                raise ValueError("Lock timeout must be non-negative")
            self._namespace = namespace
            self._block_size = block_size
            self._lock_timeout = lock_timeout
            self._cleanups = {}
            # The block each published worker holds, keyed by its ref;
            # a drop exits the handle and the exit closes the rest.
            self._blocks = {}
            self._shared_memory_pool = ResourcePool(
                factory=self._shared_memory_factory,
                finalizer=self._shared_memory_finalizer,
                ttl=0,
            )

        async def __aenter__(self) -> Self:
            """Verify the namespace's registry exists, then enter the block pool.

            See `LocalDiscovery` for the borrowing contract.

            :returns:
                This instance.
            :raises DiscoveryNamespaceNotFound:
                If the namespace has no registry.
            """
            # Bind first, so a bind that raises leaves no pool entered.
            with _registry(self._namespace):
                pass
            await self._shared_memory_pool.__aenter__()
            return self

        async def __aexit__(self, *args):
            """Close every block this publisher holds, then exit the pool.

            The first close that fails is the one that propagates; a
            later block's failure is discarded, so one bad handle cannot
            hide the others or stop them being released. The
            shared-memory pool is exited regardless, and its own failure
            supersedes.

            :param args:
                The exception info the block is exiting with, forwarded
                to the pool.
            """
            failure: BaseException | None = None
            try:
                for ref in list(self._blocks):
                    try:
                        await self._blocks.pop(ref).aclose()
                    except BaseException as error:
                        if failure is None:
                            failure = error
                if failure is not None:
                    raise failure
            finally:
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
                If an unexpected event type is provided, or the registry's
                header is not yet stamped, i.e., this publisher bound
                between the owner's creation of the registry and its stamp.
            :raises DiscoveryCapacityExhausted:
                For ``worker-added``, if the registry is already at
                capacity and the worker is not already registered.
            :raises DiscoveryWorkerNotFound:
                For ``worker-updated``, if the worker is not registered.
            :raises DiscoveryBlockExhausted:
                For ``worker-added`` and ``worker-updated``, if the
                serialized metadata exceeds the worker's block; the prior
                registration is restored before the error propagates.
            :raises TimeoutError:
                If the cross-process file lock is not acquired within this
                publisher's ``lock_timeout``.
            :raises DiscoveryNamespaceNotFound:
                If the namespace has no registry; see `LocalDiscovery`.
            """
            async with _lock(self._namespace, timeout=self._lock_timeout):
                with _registry(self._namespace) as address_space:
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

            The ledger holds at most one handle per ref, and a
            registration that displaces one closes it first: overwriting
            it would strand a pool reference nothing can drop until the
            publisher exits.

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
                    with _mapped(_attach(str(ref))) as memory_block:
                        assert memory_block.buf is not None
                        _rewrite_block(memory_block.buf, serialized)
                    return
                except FileNotFoundError:
                    # Stale slot; see the re-add contract in `publish`.
                    struct.pack_into("16s", address_space.buf, match_offset, NULL_REF)
                    if free_offset is None:
                        free_offset = match_offset

            if free_offset is None:
                raise DiscoveryCapacityExhausted(_read_capacity(address_space.buf))

            block = AsyncExitStack()
            try:
                memory_block = await block.enter_async_context(
                    self._shared_memory_pool.get(str(ref))
                )
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
                await block.aclose()
                raise
            # One handle per ref — see the docstring.
            if (stale := self._blocks.pop(str(ref), None)) is not None:
                await stale.aclose()
            self._blocks[str(ref)] = block

        async def _drop(self, metadata: WorkerMetadata, address_space: SharedMemory):
            """Unregister a worker by removing it from shared memory.

            The handle this publisher holds is released whether or not
            the slot scan matched, since a peer that dropped the same
            worker first leaves nothing to match and the block is this
            publisher's to release either way.

            :param metadata:
                The worker to unpublish from the namespace's shared memory.
            """
            assert address_space.buf is not None

            target_ref = _WorkerReference(metadata.uid)

            for offset, slot in _iter_slots(address_space.buf):
                if slot == target_ref.bytes:
                    struct.pack_into("16s", address_space.buf, offset, NULL_REF)
                    break
            # Released outside the scan — see the docstring.
            block = self._blocks.pop(str(target_ref), None)
            if block is not None:
                await block.aclose()

        async def _update(self, metadata: WorkerMetadata, address_space: SharedMemory):
            """Update a registered worker's metadata in shared memory.

            Attaches to the worker's block by name — holding no pool
            reference, and reaching blocks created by another publisher's
            pool — and rewrites it via `_rewrite_block`.

            :param metadata:
                The updated worker to publish to the namespace's shared memory.
            :raises DiscoveryWorkerNotFound:
                If the worker is not registered.
            """
            assert address_space.buf is not None

            target_ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()

            for _, slot in _iter_slots(address_space.buf):
                if slot == target_ref.bytes:
                    with _mapped(_attach(str(target_ref))) as memory_block:
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

        Yields worker discovery events (see `~wool.DiscoveryEvent`) from a
        namespace's registry as workers are added, updated, or dropped,
        rescanning the registry whenever a publisher writes to it. A
        subscriber borrows the registry; see `LocalDiscovery` for the
        borrowing and orphaning contract.

        Constructions sharing a ``namespace`` and ``poll_interval``
        within one `contextvars.Context` are served from one
        subscription; see `SubscriberMeta`. Only the iteration that
        starts a subscription binds, so only that iteration raises
        `DiscoveryNamespaceNotFound`. An iteration that joins a live
        subscription over an orphaned mapping keeps reading it and never
        observes a successor owner, and an iteration that joins a failing
        bind ends without events.

        :param namespace:
            The namespace identifier for the registry to borrow.
        :param poll_interval:
            Seconds between rescans in addition to the rescans publisher
            writes trigger. ``None`` rescans only when a publisher writes.
            Part of the subscription key.
        :raises ValueError:
            If ``poll_interval`` is negative, from the iteration that
            starts the subscription.
        """

        _namespace: Final[str]
        _poll_interval: Final[float | None]

        if TYPE_CHECKING:

            def __new__(
                cls, namespace: str, *, poll_interval: float | None = None
            ) -> DiscoverySubscriberLike: ...

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

        def __aiter__(self) -> AsyncIterator[DiscoveryEvent]:
            return self._event_stream()

        async def _event_stream(self) -> AsyncGenerator[DiscoveryEvent, None]:
            """Bind the registry and yield events from each rescan of it.

            A watchdog observer watches the notification file publishers
            touch after each write, and each notification triggers a
            rescan of the registry. A ``poll_interval`` adds a rescan
            whenever that many seconds pass without a notification.

            :yields:
                Discovery events as changes are detected in shared
                memory.
            """
            cached_workers: dict[str, WorkerMetadata] = {}
            notification = asyncio.Event()
            lock = asyncio.Lock()
            loop = asyncio.get_running_loop()

            # Bind first, so a bind that raises creates no notify file and
            # starts no observer.
            with _registry(self._namespace) as address_space:
                assert address_space.buf is not None

                if not (watchdog := _watchdog_path(self._namespace)).exists():
                    watchdog.touch()
                handler = _Watchdog(notification, watchdog, lock, loop)
                observer = Observer()
                observer.schedule(handler, path=str(watchdog.parent), recursive=False)
                observer.start()
                try:
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

        async def _shutdown(self) -> None:
            """Clean up shared subscription state for this subscriber."""

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
            with _mapped(_attach(ref)) as memory_block:
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


def _validate_block_size(block_size: int) -> None:
    """Reject a block size below one.

    :raises ValueError:
        If ``block_size`` is less than 1.
    """
    if block_size < 1:
        raise ValueError(f"Expected block size of at least 1, got {block_size}")


def _read_capacity(buf: memoryview) -> int | None:
    """Return the owner-stamped capacity, or ``None`` when unstamped.

    The owner writes `_HEADER_MAGIC` and the capacity into the registry's
    header on entry. A borrower that bound before that write, or any
    segment this module did not create, reads a mismatched magic and is
    reported as unstamped (``None``), so a zero-filled header is never
    trusted. A subscriber re-reads on its next scan; a publisher raises
    `RuntimeError` (see `LocalDiscovery.Publisher.publish`).

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
    yet stamped, so a subscriber re-reads on its next scan. The
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
    as long as the constructor runs. Undoing it afterwards, i.e., registering
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

    The suppression matches on the calling thread, the resource type, and
    this segment's name, and lasts only one constructor call, so the sole
    call it can swallow is the one made on this function's behalf. A
    ``create=True`` running concurrently on another thread stays tracked
    even for the same name. `SharedMemory` registers names slash-prefixed
    on POSIX while callers pass them bare, so both sides are stripped
    before matching.

    The lock serializes the rebind. Without it two overlapping attaches
    would each capture the other's shim: the second attach's registration
    would reach a shim whose thread does not match, be forwarded to the
    real tracker, and be tracked after all — reinstating bpo-38119. That a
    shim would also be left permanently installed is the lesser effect.
    `os.register_at_fork` replaces the lock in a child, since a fork inside
    the window would otherwise copy it held and wedge every later attach.
    """
    if sys.version_info >= (3, 13):
        # Unreachable below 3.13, where the parameter does not exist, so
        # the 3.11 and 3.12 legs would otherwise report it missing.
        return SharedMemory(name=name, track=False)  # pragma: no cover

    with _attach_lock:
        register = resource_tracker.register
        unregister = resource_tracker.unregister
        attaching = threading.get_ident()
        target = name.lstrip("/")

        def _suppressed(name: str, rtype: str) -> bool:
            return (
                threading.get_ident() == attaching
                and rtype == "shared_memory"
                and name.lstrip("/") == target
            )

        def _register(name: str, rtype: str) -> None:
            if not _suppressed(name, rtype):
                register(name, rtype)

        def _unregister(name: str, rtype: str) -> None:
            if not _suppressed(name, rtype):
                unregister(name, rtype)

        try:
            resource_tracker.register = _register
            resource_tracker.unregister = _unregister
            return SharedMemory(name=name)
        finally:
            # Only restore what is still ours: a third party that rebound
            # either hook inside the window keeps its wrapper.
            if resource_tracker.register is _register:
                resource_tracker.register = register
            if resource_tracker.unregister is _unregister:
                resource_tracker.unregister = unregister


@contextmanager
def _mapped(shared_memory: SharedMemory) -> Iterator[SharedMemory]:
    """Yield an open mapping and close it on exit.

    :param shared_memory:
        The mapping to close once the body completes.
    :yields:
        The mapping, unchanged.

    .. note::
        A close that fails is suppressed, so it cannot replace an
        exception already unwinding through the body. The realistic
        failure is `BufferError`, raised where a memoryview over the
        mapping is still exported when the close runs.
    """
    try:
        yield shared_memory
    finally:
        try:
            shared_memory.close()
        except Exception:
            pass  # pragma: no cover


@contextmanager
def _registry(namespace: str) -> Iterator[SharedMemory]:
    """Open the namespace's registry for a borrower's bind.

    Every borrower binds through here. It never creates a registry, and
    reports a missing one as `DiscoveryNamespaceNotFound` naming the
    namespace the caller asked for; the hashed segment name stays
    internal.

    Only the mapping is translated. A `FileNotFoundError` raised inside
    the body (e.g., a worker's own metadata block vanishing mid-scan) is
    a different failure and propagates untouched.

    :param namespace:
        The namespace whose registry to open.
    :yields:
        The open registry mapping.
    :raises DiscoveryNamespaceNotFound:
        If the namespace has no registry.
    """
    try:
        address_space = _attach(_short_hash(namespace))
    except FileNotFoundError as error:
        raise DiscoveryNamespaceNotFound(namespace) from error
    with _mapped(address_space):
        yield address_space


@asynccontextmanager
async def _lock(namespace: str, *, timeout: float | None = DEFAULT_LOCK_TIMEOUT):
    """Acquire an exclusive lock on the registry identified by namespace.

    Uses cross-platform file locking (via portalocker) to synchronize access
    across unrelated processes that may be publishing to the same registry.
    Works on Windows, Linux, and macOS, and does not block the event loop
    while waiting for acquisition.

    ``timeout`` bounds **acquisition only, never the held section**. Once the
    lock is held the ``with`` body runs to completion regardless of how long
    it takes.

    :param namespace:
        The namespace identifying the registry to lock.
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
    the registry, signaling subscribers to scan for changes.

    :param namespace:
        The namespace identifying the registry.
    :returns:
        Path to the notification file for this namespace.
    """
    directory = Path(tempfile.gettempdir()).resolve() / f"wool-{namespace}"
    directory.mkdir(exist_ok=True)
    return directory / "notify"
