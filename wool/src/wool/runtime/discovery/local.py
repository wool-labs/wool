from __future__ import annotations

import asyncio
import atexit
import errno
import fcntl
import os
import struct
import sys
import tempfile
import warnings
from contextlib import AsyncExitStack
from contextlib import asynccontextmanager
from contextlib import closing
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import BinaryIO
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

DEFAULT_LOCK_TIMEOUT: Final[float] = 30.0
_REF_WIDTH: Final = 16
_NULL_REF: Final = b"\x00" * _REF_WIDTH
_HEADER_MAGIC: Final = b"WLD1"
_HEADER_SIZE: Final = _REF_WIDTH
_REGISTRY: Final = "registry"
_STAGING: Final = "registry.tmp"
_NOTIFY: Final = "notify"


class _Watchdog(FileSystemEventHandler):
    """Filesystem event handler for worker discovery notifications.

    Monitors the notification file for modifications and sets an asyncio
    Event to wake subscribers when publishers write to the registry.
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
        """Return the file name of this worker's metadata block.

        :returns:
            The UUID as 32 hexadecimal digits.
        """
        return self._uuid.hex

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


class _File:
    """An open file in a namespace's directory, read and written at offsets.

    Reads and writes address the file directly, so a write is visible to
    every process holding the same file, and a handle stays usable after
    the file it names is removed or replaced.

    :param path:
        The path the file was opened by.
    :param file:
        The file, open for reading and writing. The handle takes
        ownership of it and closes it on `close`.
    """

    __slots__ = ("file", "path")

    file: BinaryIO
    path: Path

    def __init__(self, path: Path, file: BinaryIO):
        self.path = path
        self.file = file

    @classmethod
    def create(cls, path: Path, size: int) -> _File:
        """Create a zero-filled file of ``size`` bytes and open it.

        :param path:
            The file to create.
        :param size:
            The file's size in bytes.
        :returns:
            A handle on the new file.
        :raises FileExistsError:
            If ``path`` already exists.
        """
        file = os.fdopen(os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600), "r+b")
        try:
            os.ftruncate(file.fileno(), size)
        except BaseException:  # pragma: no cover
            file.close()
            _unlink_quietly(path)
            raise
        return cls(path, file)

    @classmethod
    def open(cls, path: Path) -> _File:
        """Open an existing file.

        :param path:
            The file to open.
        :returns:
            A handle on the file.
        :raises FileNotFoundError:
            If ``path`` does not exist.
        """
        return cls(path, path.open("r+b"))

    @property
    def size(self) -> int:
        """The file's size in bytes.

        :returns:
            The size of the open file, whatever its path now names.
        """
        return os.fstat(self.file.fileno()).st_size

    def current(self) -> bool:
        """Report whether this handle's path still names this file.

        A handle whose path was removed, or replaced by a successor
        owner's file, is stale: the file it holds is orphaned.

        :returns:
            True while the path resolves to the open file.
        """
        try:
            return os.path.samestat(os.fstat(self.file.fileno()), os.stat(self.path))
        except FileNotFoundError:
            return False

    def read(self, size: int, offset: int) -> bytes:
        """Return ``size`` bytes read from ``offset``.

        :param size:
            The number of bytes to read.
        :param offset:
            The offset to read from.
        :returns:
            The bytes read, fewer than ``size`` at end of file.
        """
        return os.pread(self.file.fileno(), size, offset)

    def write(self, data: bytes, offset: int) -> None:
        """Write ``data`` at ``offset``.

        :param data:
            The bytes to write.
        :param offset:
            The offset to write at.
        """
        os.pwrite(self.file.fileno(), data, offset)

    def close(self) -> None:
        """Close the file, leaving it in place."""
        self.file.close()


# public
class LocalDiscovery(Discovery):
    """File-backed discovery for single-machine worker pools.

    The default discovery protocol of a
    `~wool.runtime.worker.pool.WorkerPool` created without one. Processes
    on one host share a registry of workers identified by a namespace
    string, and a cross-process file lock serializes writes to it.

    **Ownership.** A namespace's registry has exactly one owner: the
    entered instance holding the namespace's claim. Entering claims the
    namespace and creates its registry, and exiting reclaims both.
    Entering a namespace that a live owner holds raises
    `DiscoveryNamespaceInUse`. An owner that never exits, e.g., one
    abandoned when the interpreter shuts down, reclaims the registry at
    shutdown. A killed owner's claim ends with its process, so the next
    entry on the namespace succeeds and replaces any registry the killed
    owner left behind.

    **Forks.** A process forked from an entered owner inherits both the
    claim and the owner's teardown, so the fork's own exit reclaims the
    namespace's files while the claim itself lives until every process
    holding it has exited. Wool starts its own workers with ``spawn``; a
    host that forks should enter a namespace in the process that owns
    it.

    **Borrowing.** `LocalDiscovery.Publisher` and
    `LocalDiscovery.Subscriber`, including those `publisher`,
    `subscriber` and `subscribe` return, borrow a namespace's registry
    and never create one. A borrower *binds* when it opens the registry:
    a publisher on entry and on each publish, a subscriber when its
    subscription starts. A bind where the namespace has no registry
    raises `DiscoveryNamespaceNotFound`.

    **Orphaning.** A borrower that outlives its owner is orphaned. A
    registry it already holds stays readable. Its next bind raises
    `DiscoveryNamespaceNotFound`, unless a successor owner has entered
    the namespace, in which case the bind reaches the successor's
    registry. Orphaning is defined behavior: a borrower holds no claim on
    the registry, so it has no registry to reclaim. See
    `LocalDiscovery.Subscriber` for which subscriber iterations bind.

    **Lifecycle.** An instance is single-use: any entry attempt spends
    it, so a second entry raises `RuntimeError`. Retrying a rejected
    claim requires a new instance. Exiting never raises; a failed
    removal surfaces as a `ResourceWarning`. Once the owner and every
    publisher have exited, the namespace leaves nothing on the
    filesystem.

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

    A namespace lives in one directory, ``wool-<namespace>``, under
    ``/dev/shm`` where Linux provides it and the temporary directory
    otherwise. The directory holds the registry, i.e., a header and
    fixed-width slots of worker references, the ``notify`` file
    subscribers watch, and one metadata block file per published worker,
    named by the worker's UUID. Every process reads and writes those
    files at fixed offsets, through the page cache, so a write is visible
    to every other process holding the same file.

    The files are read and written rather than memory-mapped. CPython
    forces a device-level flush (``F_FULLFSYNC``) whenever it maps a file
    descriptor on Apple platforms, which costs milliseconds per mapping,
    varies with unrelated disk activity, and imposes that flush on the
    whole host. ``pread`` and ``pwrite`` over a held descriptor carry the
    same cross-process visibility without it.

    The claim is an exclusive ``flock`` on the directory, held on a
    descriptor the owner keeps open. The kernel releases it only when
    every descriptor sharing it closes — hence the fork behavior above —
    so a claim cannot be taken from an owner that is merely slow, and a
    successor that takes it knows any registry it finds is stranded. The
    lock is taken on a descriptor opened before locking, so the claimant
    re-checks that the path still names that directory: a directory
    removed in between would otherwise hand a claim on an unlinked inode
    to one owner while another claims its replacement. A claim that finds
    the directory replaced under it retries, and each retry follows
    another process's completed reclaim rather than contending with one,
    so a claim resolves rather than livelocking.

    The owner writes the registry, header included, under a staging name
    and renames it into place, so no borrower ever binds a registry with
    an unstamped header.

    Publishers lock the registry file itself rather than a separate lock
    file. The lock and the data it guards therefore always share an
    inode: an orphaned publisher still locking a reclaimed registry can
    only write to that registry, never to a successor's.

    Borrowers never create the directory or anything in it but their
    own blocks. Exiting removes the registry, the notification file and
    the directory; the directory stays while an orphaned publisher still
    holds a block in it, and that publisher removes the directory with
    its last block.

    Every teardown path removes files through `_unlink_quietly`, so
    `__exit__` cannot replace an exception its caller is already
    unwinding. The shutdown fallback is an `atexit` handler registered on
    entry and unregistered on exit before the removal runs, so a failed
    removal leaves no handler armed to fire again at interpreter
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
        """Claim the namespace and create its registry.

        See `LocalDiscovery` for the ownership and teardown contract.

        :returns:
            This instance.
        :raises RuntimeError:
            If entry has already been attempted on this instance.
        :raises DiscoveryNamespaceInUse:
            If a live owner holds the namespace; see `LocalDiscovery`.
        """
        directory = _directory(self._namespace)
        while True:
            directory.mkdir(exist_ok=True)
            try:
                self._claim = os.open(directory, os.O_RDONLY)
            except FileNotFoundError:
                continue
            try:
                fcntl.flock(self._claim, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # See the directory re-check in the implementation notes.
                if os.path.samestat(os.fstat(self._claim), os.stat(directory)):
                    self._stage(directory)
                    break
            except BlockingIOError as error:
                os.close(self._claim)
                raise DiscoveryNamespaceInUse(self._namespace) from error
            except FileNotFoundError:
                # The directory was removed after it was opened.
                pass
            except BaseException:
                self._release()
                raise
            os.close(self._claim)
        self._cleanup = atexit.register(self._release)
        return self

    def __exit__(self, *_):
        """Reclaim the namespace's registry and release the claim.

        See `LocalDiscovery` for the ownership and teardown contract.
        """
        atexit.unregister(self._cleanup)
        self._release()

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

    def _stage(self, directory: Path) -> None:
        """Create the claimed namespace's registry and notification file.

        Replaces any registry a killed owner left behind. See
        `LocalDiscovery`'s implementation notes for why the registry is
        staged.

        :param directory:
            The claimed namespace's directory.
        :raises FileNotFoundError:
            If the directory was removed after it was claimed.
        """
        staging = directory / _STAGING
        descriptor = os.open(staging, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            # Truncation zero-fills, so every slot already reads as `_NULL_REF`.
            os.ftruncate(descriptor, _HEADER_SIZE + self._capacity * _REF_WIDTH)
            os.pwrite(descriptor, struct.pack("<4sI", _HEADER_MAGIC, self._capacity), 0)
        finally:
            os.close(descriptor)
        os.replace(staging, directory / _REGISTRY)
        (directory / _NOTIFY).touch()

    def _release(self) -> None:
        """Remove the namespace's files and directory, then release the claim.

        The directory is removed while the claim is still held; see the
        directory re-check in `LocalDiscovery`'s implementation notes.
        """
        directory = _directory(self._namespace)
        _unlink_quietly(directory / _STAGING)
        _unlink_quietly(directory / _REGISTRY)
        _unlink_quietly(directory / _NOTIFY)
        _rmdir_quietly(directory)
        os.close(self._claim)

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

        _block_pool: ResourcePool[_File]
        _block_size: int
        _blocks: dict[str, AsyncExitStack]
        _cleanups: dict[str, Callable]
        _lock_timeout: float | None
        _namespace: Final[str]
        _registry: _File | None

        #: Registry announcements are only discoverable on a common
        #: host, so this publisher prescribes the loopback bind.
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
            self._registry = None
            # The block each published worker holds, keyed by its ref;
            # a drop exits the handle and the exit closes the rest.
            self._blocks = {}
            self._block_pool = ResourcePool(
                factory=self._block_factory,
                finalizer=self._block_finalizer,
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
            self._bind()
            await self._block_pool.__aenter__()
            return self

        async def __aexit__(self, *args):
            """Close every block this publisher holds, then exit the pool.

            The first close that fails is the one that propagates; a
            later block's failure is discarded, so one bad handle cannot
            hide the others or stop them being released. The block pool
            is exited regardless, and its own failure supersedes. The
            registry this publisher bound is released either way.

            :param args:
                The exception info the block is exiting with, forwarded
                to the pool.
            """
            failure: BaseException | None = None
            try:
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
                    await self._block_pool.__aexit__(*args)
            finally:
                if self._registry is not None:
                    self._registry.close()
                    self._registry = None

        @property
        def namespace(self):
            """The namespace identifier for this publisher.

            :returns:
                The namespace string.
            """
            return self._namespace

        async def publish(self, type: DiscoveryEventType, metadata: WorkerMetadata):
            """Publish a worker discovery event.

            Writes the event to the namespace's registry where subscribers
            can discover it. The operation is synchronized across
            processes using file locking to ensure consistency. After
            publishing, touches a notification file to wake subscribers
            via filesystem events.

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
                header is not stamped.
            :raises DiscoveryCapacityExhausted:
                For ``worker-added``, if the registry is already at
                capacity and the worker is not already registered.
            :raises DiscoveryWorkerNotFound:
                For ``worker-updated``, if the worker is not registered.
            :raises DiscoveryBlockExhausted:
                For ``worker-added`` and ``worker-updated``, if the
                serialized metadata exceeds the worker's block, which is
                rejected before anything is written, so the prior
                registration is left intact.
            :raises TimeoutError:
                If the cross-process file lock is not acquired within this
                publisher's ``lock_timeout``.
            :raises DiscoveryNamespaceNotFound:
                If the namespace has no registry; see `LocalDiscovery`.
            """
            registry = self._bind()
            async with _lock(
                registry, namespace=self._namespace, timeout=self._lock_timeout
            ):
                if _read_capacity(registry) is None:  # pragma: no cover
                    raise RuntimeError("Registrar service not properly initialized")
                match type:
                    case "worker-added":
                        await self._add(metadata, registry)
                    case "worker-dropped":
                        await self._drop(metadata, registry)
                    case "worker-updated":
                        await self._update(metadata, registry)
                    case _:
                        raise RuntimeError(f"Unexpected discovery event type: {type}")

                _notify(self._namespace)

        async def _add(self, metadata: WorkerMetadata, registry: _File):
            """Register a worker, or refresh one already registered.

            See `publish` for the re-add contract. The refresh opens the
            existing block by name rather than acquiring it from the pool,
            so it holds no pool reference and reaches blocks created by
            another publisher's pool.

            The ledger holds at most one handle per ref, and a
            registration that displaces one closes it first: overwriting
            it would strand a pool reference nothing can drop until the
            publisher exits.

            :param metadata:
                The worker to publish to the namespace's registry.
            :raises DiscoveryCapacityExhausted:
                If no slots are available and the worker is not already
                registered.
            """
            ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()

            free_offset = None
            match_offset = None
            for offset, slot in _iter_slots(registry):
                if slot == ref.bytes:
                    match_offset = offset
                    break
                if free_offset is None and slot == _NULL_REF:
                    free_offset = offset

            if match_offset is not None:
                try:
                    with closing(_block(self._namespace, ref)) as block_file:
                        _write_block(block_file, serialized)
                    return
                except FileNotFoundError:
                    # Stale slot; see the re-add contract in `publish`.
                    registry.write(_NULL_REF, match_offset)
                    if free_offset is None:
                        free_offset = match_offset

            if free_offset is None:
                raise DiscoveryCapacityExhausted(_read_capacity(registry))

            block = AsyncExitStack()
            try:
                block_file = await block.enter_async_context(
                    self._block_pool.get(str(ref))
                )
                _write_block(block_file, serialized)
                registry.write(ref.bytes, free_offset)
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

        async def _drop(self, metadata: WorkerMetadata, registry: _File):
            """Unregister a worker by removing it from the registry.

            The handle this publisher holds is released whether or not
            the slot scan matched, since a peer that dropped the same
            worker first leaves nothing to match and the block is this
            publisher's to release either way.

            :param metadata:
                The worker to unpublish from the namespace's registry.
            """
            target_ref = _WorkerReference(metadata.uid)

            for offset, slot in _iter_slots(registry):
                if slot == target_ref.bytes:
                    registry.write(_NULL_REF, offset)
                    break
            # Released outside the scan — see the docstring.
            block = self._blocks.pop(str(target_ref), None)
            if block is not None:
                await block.aclose()

        async def _update(self, metadata: WorkerMetadata, registry: _File):
            """Update a registered worker's metadata block.

            Opens the worker's block by name — holding no pool reference,
            and reaching blocks created by another publisher's pool — and
            rewrites it via `_write_block`.

            :param metadata:
                The updated worker to publish to the namespace's registry.
            :raises DiscoveryWorkerNotFound:
                If the worker is not registered.
            """
            target_ref = _WorkerReference(metadata.uid)
            serialized = metadata.to_protobuf().SerializeToString()

            for _, slot in _iter_slots(registry):
                if slot == target_ref.bytes:
                    with closing(_block(self._namespace, target_ref)) as block_file:
                        _write_block(block_file, serialized)
                    return

            raise DiscoveryWorkerNotFound(metadata.uid)

        def _bind(self) -> _File:
            """Return this publisher's registry, rebinding it when stale.

            A publisher holds the registry it bound, so a publish costs
            no reopen, and re-checks it on each bind: a registry its owner
            has reclaimed is released here, and a successor owner's
            registry is bound in its place. See `LocalDiscovery` for the
            borrowing and orphaning contract.

            :returns:
                The open registry.
            :raises DiscoveryNamespaceNotFound:
                If the namespace has no registry.
            """
            if self._registry is not None:
                if self._registry.current():
                    return self._registry
                self._registry.close()
                self._registry = None
            self._registry = _open_registry(self._namespace)
            return self._registry

        def _block_factory(self, name: str) -> _File:
            """Create a worker's metadata block in the namespace's directory.

            Registers an atexit handler that removes the block, so a
            publisher that never exits leaves no block behind.

            :param name:
                The block's file name, i.e., the worker reference.
            :returns:
                The new block's handle.
            """
            path = _directory(self._namespace) / name
            block = _File.create(path, self._block_size)

            def cleanup():  # pragma: no cover
                _unlink_quietly(path)
                _rmdir_quietly(path.parent)

            self._cleanups[name] = atexit.register(cleanup)
            return block

        def _block_finalizer(self, block: _File):
            """Remove a metadata block released from the pool.

            Unregisters the atexit handler before removing the block, so a
            failed removal cannot leave the handler armed to fire again at
            interpreter shutdown. The removal goes through
            `_unlink_quietly`; see it for the failure semantics. The
            namespace's directory is then removed if nothing else is left
            in it, which reclaims it after an owner that exited first.

            :param block:
                The block to finalize.
            """
            atexit.unregister(self._cleanups.pop(block.path.name))
            _unlink_quietly(block.path)
            _rmdir_quietly(block.path.parent)
            block.close()

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
                Discovery events as changes are detected in the registry.
            """
            cached_workers: dict[str, WorkerMetadata] = {}
            notification = asyncio.Event()
            lock = asyncio.Lock()
            loop = asyncio.get_running_loop()

            # Bind first, so a bind that raises starts no observer.
            with closing(_open_registry(self._namespace)) as registry:
                watchdog = _directory(self._namespace) / _NOTIFY
                handler = _Watchdog(notification, watchdog, lock, loop)
                observer = Observer()
                observer.schedule(handler, path=str(watchdog.parent), recursive=False)
                try:
                    observer.start()
                except FileNotFoundError as error:  # pragma: no cover
                    # The owner exited between the bind and the watch.
                    raise DiscoveryNamespaceNotFound(self._namespace) from error
                try:
                    while True:
                        async with lock:
                            notification.clear()
                            discovered_workers: dict[str, WorkerMetadata] = {}
                            for _, slot in _iter_slots(registry):
                                if slot != _NULL_REF:
                                    ref = _WorkerReference.from_bytes(slot)
                                    metadata = self._deserialize_metadata(ref)
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

        def _deserialize_metadata(self, ref: _WorkerReference):
            """Load and deserialize a worker's metadata from its block.

            :param ref:
                The reference identifying the worker's metadata block.
            :returns:
                The deserialized WorkerMetadata instance.
            :raises FileNotFoundError:
                If the block does not exist.
            """
            with closing(_block(self._namespace, ref)) as block_file:
                protobuf = wire.WorkerMetadata.FromString(_read_block(block_file))
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


def _read_capacity(registry: _File) -> int | None:
    """Return the owner-stamped capacity, or ``None`` when unstamped.

    The owner writes `_HEADER_MAGIC` and the capacity into the registry's
    header before the registry becomes visible. A file this module did not
    create reads a mismatched magic and is reported as unstamped
    (``None``), so a zero-filled header is never trusted. A subscriber
    yields no workers from it; a publisher raises `RuntimeError` (see
    `LocalDiscovery.Publisher.publish`).

    :param registry:
        The open registry.
    :returns:
        The stamped capacity, or ``None`` when the header magic is absent.
    """
    header = registry.read(struct.calcsize("<4sI"), 0)
    if len(header) < struct.calcsize("<4sI"):  # pragma: no cover
        return None
    magic, capacity = struct.unpack("<4sI", header)
    if magic != _HEADER_MAGIC:  # pragma: no cover
        return None
    return capacity


def _iter_slots(registry: _File) -> Iterator[tuple[int, bytes]]:
    """Yield each ``(offset, ref_bytes)`` slot bounded by the stamped capacity.

    Reads the owner-stamped capacity from the header and walks exactly
    that many slots, so ``capacity`` — not the file's size — is the
    enforced ceiling. Yields nothing for a registry whose header is not
    stamped. The capacity is re-read on every call, so a scan always
    reflects the current header.

    The slots are read in one pass, so an iteration walks the registry as
    it stood when the iteration began rather than picking up writes made
    part way through it.

    :param registry:
        The open registry to scan.
    :yields:
        ``(offset, ref_bytes)`` for each 16-byte slot, in order.
    """
    capacity = _read_capacity(registry)
    if capacity is None:  # pragma: no cover
        return
    slots = registry.read(capacity * _REF_WIDTH, _HEADER_SIZE)
    for index in range(capacity):
        start = index * _REF_WIDTH
        yield _HEADER_SIZE + start, slots[start : start + _REF_WIDTH]


def _write_block(block: _File, serialized: bytes) -> None:
    """Write size-prefixed metadata over a block's current contents.

    A payload that does not fit the block is rejected before anything is
    written, so a rejected write leaves the block's prior registration
    intact.

    :param block:
        The open block to write.
    :param serialized:
        The serialized metadata to write.
    :raises DiscoveryBlockExhausted:
        If the payload does not fit the block.
    """
    size = len(serialized)
    if struct.calcsize("I") + size > block.size:
        raise DiscoveryBlockExhausted(size)
    block.write(struct.pack(f"I{size}s", size, serialized), 0)


def _read_block(block: _File) -> bytes:
    """Return the serialized metadata a block holds.

    :param block:
        The open block to read.
    :returns:
        The serialized metadata, as written by `_write_block`.
    """
    prefix = struct.calcsize("I")
    size = struct.unpack("I", block.read(prefix, 0))[0]
    return block.read(size, prefix)


def _unlink_quietly(path: Path) -> None:
    """Remove a file without raising, warning if it fails unexpectedly.

    Every teardown path in this module removes files through here, so no
    teardown can raise: an exception escaping a ``__exit__`` or an
    `atexit` handler would replace the exception the caller was already
    unwinding, or crash the interpreter at shutdown.

    A file that is already gone is the expected case and passes silently.
    Any other failure leaves the file in place, which is a leak the caller
    cannot act on but an operator can, so it surfaces as a
    `ResourceWarning` rather than being swallowed.

    :param path:
        The file to remove.
    """
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError as error:
        warnings.warn(
            f"failed to remove {str(path)!r}: {error}",
            ResourceWarning,
            stacklevel=2,
        )


def _rmdir_quietly(directory: Path) -> None:
    """Remove a directory if it is empty, without raising.

    A directory that is already gone, or that still holds files, passes
    silently. Any other failure surfaces as a `ResourceWarning`; see
    `_unlink_quietly`.

    :param directory:
        The directory to remove.
    """
    try:
        os.rmdir(directory)
    except OSError as error:
        if error.errno not in (errno.ENOENT, errno.ENOTEMPTY, errno.EEXIST):
            warnings.warn(
                f"failed to remove {str(directory)!r}: {error}",
                ResourceWarning,
                stacklevel=2,
            )


@cache
def _root() -> Path:
    """Return the directory that holds every namespace's directory.

    :returns:
        ``/dev/shm`` on Linux when it is a writable directory, i.e.,
        memory-backed, otherwise the temporary directory. Symlinks are
        resolved, so watchdog event paths compare equal to paths built
        from it.
    """
    shm = Path("/dev/shm")
    if sys.platform.startswith("linux") and os.access(shm, os.W_OK | os.X_OK):
        return shm.resolve()  # pragma: no cover — Linux only
    return Path(tempfile.gettempdir()).resolve()  # pragma: no cover — not Linux


def _directory(namespace: str) -> Path:
    """Return the path of a namespace's directory, without creating it.

    :param namespace:
        The namespace identifying the directory.
    :returns:
        The path of the namespace's directory.
    """
    return _root() / f"wool-{namespace}"


def _block(namespace: str, ref: _WorkerReference) -> _File:
    """Open an existing worker metadata block.

    :param namespace:
        The namespace whose directory holds the block.
    :param ref:
        The reference identifying the worker's block.
    :returns:
        A handle on the block.
    :raises FileNotFoundError:
        If the block does not exist.
    """
    return _File.open(_directory(namespace) / str(ref))


def _open_registry(namespace: str) -> _File:
    """Open the namespace's registry for a borrower's bind.

    Every borrower binds through here. It never creates a registry or
    the namespace's directory, and reports a missing registry as
    `DiscoveryNamespaceNotFound`.

    :param namespace:
        The namespace whose registry to open.
    :returns:
        The open registry.
    :raises DiscoveryNamespaceNotFound:
        If the namespace has no registry.
    """
    try:
        return _File.open(_directory(namespace) / _REGISTRY)
    except FileNotFoundError as error:
        raise DiscoveryNamespaceNotFound(namespace) from error


@asynccontextmanager
async def _lock(
    registry: _File,
    *,
    namespace: str,
    timeout: float | None = DEFAULT_LOCK_TIMEOUT,
):
    """Acquire an exclusive lock on an open registry.

    Uses file locking (via portalocker) on the registry file itself to
    synchronize writes across unrelated processes publishing to the same
    registry, without blocking the event loop while waiting for
    acquisition. The lock and the data it guards therefore share an
    inode; see `LocalDiscovery`'s implementation notes.

    ``timeout`` bounds **acquisition only, never the held section**. Once the
    lock is held the ``with`` body runs to completion regardless of how long
    it takes.

    :param registry:
        The open registry to lock.
    :param namespace:
        The namespace the registry belongs to, named in a timeout.
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
    mid-write would corrupt the registry for every process mapping it.
    """
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    while True:
        try:
            portalocker.lock(registry.file, portalocker.LOCK_EX | portalocker.LOCK_NB)
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
        portalocker.unlock(registry.file)


def _notify(namespace: str) -> None:
    """Wake the namespace's subscribers by touching its notification file.

    A namespace whose owner has exited has no notification file, and no
    subscriber can start watching it, so a missing file passes silently
    rather than being recreated.

    :param namespace:
        The namespace whose subscribers to wake.
    """
    try:
        os.utime(_directory(namespace) / _NOTIFY)
    except FileNotFoundError:
        pass
