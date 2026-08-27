from __future__ import annotations

import uuid
import warnings
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import Final
from typing import Protocol
from typing import TypeAlias
from typing import final
from typing import runtime_checkable

import grpc

if TYPE_CHECKING:
    from wool.runtime.worker.auth import WorkerCredentials
    from wool.runtime.worker.auth import WorkerCredentialsProvider
    from wool.runtime.worker.metadata import WorkerMetadata


# public
@dataclass(frozen=True)
class ChannelOptions:
    """Options for gRPC channel configuration.

    Controls the maximum message sizes and keepalive behaviour for
    gRPC channels.  Workers advertise these options via `WorkerMetadata`
    so that clients connect with compatible settings automatically.

    :param max_receive_message_length:
        Maximum inbound message size in bytes.
    :param max_send_message_length:
        Maximum outbound message size in bytes.
    :param keepalive_time_ms:
        Interval in milliseconds between HTTP/2 keepalive pings.
    :param keepalive_timeout_ms:
        Time in milliseconds to wait for a keepalive ping response
        before considering the connection dead.
    :param keepalive_permit_without_calls:
        If ``True``, send keepalive pings even when there are no
        active RPCs.
    :param max_pings_without_data:
        Maximum keepalive pings allowed when no data or header
        frames have been sent.
    :param max_concurrent_streams:
        Maximum concurrent HTTP/2 streams per connection.  Also
        used by the client to size its per-channel concurrency
        semaphore.
    :param compression:
        Default compression algorithm for messages.
    """

    max_receive_message_length: int = 100 * 1024 * 1024
    max_send_message_length: int = 100 * 1024 * 1024
    keepalive_time_ms: int = 30_000
    keepalive_timeout_ms: int = 30_000
    keepalive_permit_without_calls: bool = True
    max_pings_without_data: int = 2
    max_concurrent_streams: int = 100
    compression: grpc.Compression = grpc.Compression.NoCompression


# public
@dataclass(frozen=True)
class WorkerOptions:
    """Options for gRPC worker server configuration.

    Composes `ChannelOptions` (advertised to clients) with
    server-side settings that are not communicated over the wire.

    :param channel:
        Channel options advertised to connecting clients.
    :param http2_min_recv_ping_interval_without_data_ms:
        Server-side minimum allowed interval in milliseconds
        between client keepalive pings when there is no data
        being sent.
    :param max_ping_strikes:
        Maximum keepalive ping violations before the server
        sends GOAWAY.
    :param max_connection_idle_ms:
        Server idle timeout in milliseconds before closing the
        connection.  ``None`` uses gRPC's default (infinite).
    :param max_connection_age_ms:
        Maximum connection lifespan in milliseconds before the
        server forces a reconnect.  ``None`` uses gRPC's default
        (infinite).
    :param max_connection_age_grace_ms:
        Grace period in milliseconds for in-flight RPCs after
        max connection age is reached.  ``None`` uses gRPC's
        default (infinite).
    """

    channel: ChannelOptions = field(default_factory=ChannelOptions)
    http2_min_recv_ping_interval_without_data_ms: int = 30_000
    max_ping_strikes: int = 2
    max_connection_idle_ms: int | None = None
    max_connection_age_ms: int | None = None
    max_connection_age_grace_ms: int | None = None

    def __post_init__(self):
        """Validate keepalive option compatibility.

        :raises ValueError:
            If ``channel.keepalive_time_ms`` is less than
            ``http2_min_recv_ping_interval_without_data_ms``.
        """
        if (
            self.channel.keepalive_time_ms
            < self.http2_min_recv_ping_interval_without_data_ms
        ):
            raise ValueError(
                "keepalive_time_ms must be >= "
                "http2_min_recv_ping_interval_without_data_ms"
            )


# public
class WorkerFactory(Protocol):
    """Protocol for fully pool-driven worker factory callables.

    Worker factories create `WorkerLike` instances with specific tags
    and configuration; `WorkerPool` uses them to spawn workers. This
    protocol receives both keywords the pool can prescribe: the bind
    host from the pool's discovery publisher, so factory-customized
    workers stay reachable wherever the publisher advertises them, and
    the pool's ``identity``. `LocalWorker` satisfies it as-is.
    """

    def __call__(
        self,
        *tags: str,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
        host: str,
        identity: str | None,
    ) -> WorkerLike:
        """Create a new worker instance bound to the given host.

        :param tags:
            Capability tags for worker discovery and filtering.
        :param credentials:
            Credentials for the worker.
        :param host:
            Host the worker should bind, prescribed by the pool's
            discovery publisher (see `~wool.DiscoveryPublisherLike.bind_host`).
        :param identity:
            The peer name the pool intends this worker to advertise, or
            ``None`` when the pool has no identity to give. Declaring
            it is what asks for it — see `WorkerFactoryLike` for when the
            pool passes a value and when it withholds the keyword.
        :returns:
            Configured worker instance.
        """
        ...


# public
class BoundWorkerFactory(Protocol):
    """Protocol for worker factory callables that own their binding.

    Identical to `WorkerFactory` except the factory is never passed a
    bind host: the pool never overrides the binding it produces.
    """

    def __call__(
        self,
        *tags: str,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
        identity: str | None,
    ) -> WorkerLike:
        """Create a new worker instance.

        :param tags:
            Capability tags for worker discovery and filtering.
        :param credentials:
            Credentials for the worker.
        :param identity:
            The peer name the pool intends this worker to advertise, or
            ``None`` when the pool has no identity to give. Declaring
            it is what asks for it — see `WorkerFactoryLike` for when the
            pool passes a value and when it withholds the keyword.
        :returns:
            Configured `WorkerLike` instance.
        """
        ...


# public
class IdentifiedWorkerFactory(Protocol):
    """Protocol for factories owning the name their workers advertise.

    `WorkerFactory` minus the pool's ``identity``: the factory decides
    what its workers advertise, and the pool never passes a name.

    One of the four shapes `WorkerFactoryLike` admits — see that alias
    for the axes that separate them.
    """

    def __call__(
        self,
        *tags: str,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
        host: str,
    ) -> WorkerLike:
        """Create a new worker instance bound to the given host.

        :param tags:
            Capability tags for worker discovery and filtering.
        :param credentials:
            Credentials for the worker.
        :param host:
            Host the worker should bind, prescribed by the pool's
            discovery publisher (see `~wool.DiscoveryPublisherLike.bind_host`).
        :returns:
            Configured worker instance.
        """
        ...


# public
class IdentifiedBoundWorkerFactory(Protocol):
    """Protocol for factories owning both their binding and their name.

    `BoundWorkerFactory` minus the pool's ``identity``: the pool passes
    only tags and credentials.

    One of the four shapes `WorkerFactoryLike` admits — see that alias
    for the axes that separate them.
    """

    def __call__(
        self,
        *tags: str,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
    ) -> WorkerLike:
        """Create a new worker instance.

        :param tags:
            Capability tags for worker discovery and filtering.
        :param credentials:
            Credentials for the worker.
        :returns:
            Configured `WorkerLike` instance.
        """
        ...


# TODO: Replace this enumeration of factory protocols with a type
# intersection if Python ever implements the capability.
# public
WorkerFactoryLike: TypeAlias = (
    WorkerFactory
    | IdentifiedWorkerFactory
    | BoundWorkerFactory
    | IdentifiedBoundWorkerFactory
)
"""Any callable `WorkerPool` accepts for its ``worker`` parameter.

The four shapes vary along two orthogonal axes, each being a keyword
the pool passes only to a factory that can receive it:

==============================  ==========  ============
Shape                           ``host``    ``identity``
==============================  ==========  ============
`WorkerFactory`                 yes         yes
`IdentifiedWorkerFactory`       yes         no
`BoundWorkerFactory`            no          yes
`IdentifiedBoundWorkerFactory`  no          no
==============================  ==========  ============

Whichever keyword a factory does not take, it **owns**: ``Bound``
owns its binding, ``Identified`` owns the name its workers advertise.
The pool inspects the factory's signature to determine which keywords
to pass it, with one exception: a `functools.partial` that pre-supplies
the keyword keeps its own value; the pool defers to it rather than
overriding what the partial function pins.

.. rubric:: Implementation notes

Classification binds the intended call — see
`~wool.utilities.signature.accepts_kwarg` — because for a callable
protocol ``isinstance`` tests only that the object is callable.

The enumeration is a workaround. Expressing "accepts host **and**
identity" as a composition of two constraints needs a type
intersection, which Python does not have: protocol inheritance
resolves ``__call__`` by MRO and silently keeps only the first base.
This alias is the sole name callers need, so the shapes behind it can
be replaced when the language grows one.
"""


# public
@runtime_checkable
class WorkerLike(Protocol):
    """Protocol defining the worker interface.

    All worker implementations must satisfy this protocol. Prefer
    `WorkerLike` over `Worker` for type annotations to
    support structural subtyping.

    Workers execute distributed tasks within their own process and event
    loop, exposing a gRPC server for task dispatch.
    """

    @property
    def uid(self) -> uuid.UUID:
        """The worker's unique identifier.

        :returns:
            Unique UUID assigned to this worker instance.
        """
        ...

    @property
    def metadata(self) -> WorkerMetadata | None:
        """Worker metadata including network address and metadata.

        :returns:
            The worker's complete metadata or None if not started.
        """
        ...

    @property
    def tags(self) -> set[str]:
        """Capability tags for this worker.

        :returns:
            Set of capability tags associated with this worker.
        """
        ...

    @property
    def extra(self) -> dict[str, Any]:
        """Additional arbitrary metadata for this worker.

        :returns:
            Dictionary of arbitrary key-value metadata.
        """
        ...

    @property
    def address(self) -> str | None:
        """Network address where the worker is listening.

        :returns:
            The worker's network address or None if not started.
        """
        ...

    async def start(self, *, timeout: float | None = None):
        """Start the worker and register it with the pool.

        :param timeout:
            Maximum time in seconds to wait for worker startup.
        :raises TimeoutError:
            If startup takes longer than the specified timeout.
        :raises RuntimeError:
            If the worker has already been started.
        :raises ValueError:
            If the timeout is not positive.
        """
        ...

    async def stop(self, *, grace: float | None = None, timeout: float | None = None):
        """Stop the worker and unregister it from the pool.

        :param grace:
            The worker's shutdown grace period in seconds — how long to
            wait for in-flight tasks to drain before cancelling them.
            ``None`` (the default) applies no grace: in-flight tasks
            are cancelled immediately. A negative value waits
            indefinitely for the drain.
        :param timeout:
            Deprecated alias for ``grace``, retained for backwards
            compatibility; passing it emits a ``DeprecationWarning``.
        :raises RuntimeError:
            If the worker has not been started.
        """
        ...


# public
class Worker(ABC):
    """Abstract base class for worker implementations.

    Workers execute distributed tasks in dedicated processes, each running
    a gRPC server for task dispatch. Subclasses implement the actual worker
    process lifecycle in `_start` and `_stop`.

    **Implementing a custom worker:**

    .. code-block:: python

        from wool.runtime.worker.base import Worker
        from wool.runtime.worker.metadata import WorkerMetadata


        class CustomWorker(Worker):
            async def _start(self, timeout):
                # Start your worker process
                self._info = WorkerMetadata(...)

            async def _stop(self, grace):
                # Clean shutdown
                ...

            @property
            def address(self):
                return self._address

    :param tags:
        Capability tags for filtering and selection.
    :param extra:
        Additional metadata as key-value pairs.
    """

    _info: WorkerMetadata | None = None
    _started: bool = False
    _uid: Final[uuid.UUID]
    _tags: Final[set[str]]
    _extra: Final[dict[str, Any]]

    def __init__(self, *tags: str, **extra: Any):
        self._uid = uuid.uuid4()
        self._tags = set(tags)
        self._extra = extra

    @property
    def uid(self) -> uuid.UUID:
        """The worker's unique identifier."""
        return self._uid

    @property
    def metadata(self) -> WorkerMetadata | None:
        """Worker metadata including network address and metadata.

        :returns:
            The worker's complete metadata or None if not started.
        """
        return self._info

    @property
    def tags(self) -> set[str]:
        """Capability tags for this worker."""
        return self._tags

    @property
    def extra(self) -> dict[str, Any]:
        """Additional arbitrary metadata for this worker."""
        return self._extra

    @property
    @abstractmethod
    def address(self) -> str | None: ...

    @final
    async def start(self, *, timeout: float | None = None):
        """Start the worker and register it with the pool.

        This method is a final implementation that calls the abstract
        `_start` method to initialize the worker process and register
        it with the registrar service.

        :param timeout:
            Maximum time in seconds to wait for worker startup.
        :raises TimeoutError:
            If startup takes longer than the specified timeout.
        :raises RuntimeError:
            If the worker has already been started.
        :raises ValueError:
            If the timeout is not positive.
        """
        if timeout is not None and timeout <= 0:
            raise ValueError("Timeout must be positive")
        if self._started:
            raise RuntimeError("Worker has already been started")

        await self._start(timeout=timeout)
        self._started = True
        assert self._info

    @final
    async def stop(self, *, grace: float | None = None, timeout: float | None = None):
        """Stop the worker and unregister it from the pool.

        This method is a final implementation that calls the abstract
        `_stop` method to gracefully shut down the worker process and
        unregister it from the registrar service.

        :param grace:
            The worker's shutdown grace period in seconds — how long to
            wait for in-flight tasks to drain before cancelling them.
            ``None`` (the default) applies no grace: in-flight tasks
            are cancelled immediately. A negative value waits
            indefinitely for the drain.
        :param timeout:
            Deprecated alias for ``grace``, retained for backwards
            compatibility; passing it emits a ``DeprecationWarning``.
        """
        if timeout is not None:
            warnings.warn(
                "The 'timeout' parameter of Worker.stop is deprecated; "
                "use 'grace' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if grace is None:
                grace = timeout
        if not self._started:
            raise RuntimeError("Worker has not been started")
        try:
            await self._stop(grace)
        finally:
            self._started = False

    @abstractmethod
    async def _start(self, timeout: float | None):
        """Implementation-specific worker startup logic.

        Subclasses must implement this method to handle the actual
        startup of their worker process and gRPC server.

        :param timeout:
            Maximum time in seconds to wait for worker startup.
        """
        ...

    @abstractmethod
    async def _stop(self, grace: float | None):
        """Implementation-specific worker shutdown logic.

        Subclasses must implement this method to handle the graceful
        shutdown of their worker process and cleanup of resources.

        :param grace:
            The shutdown grace period forwarded by `stop` — see `stop`
            for the ``None``/positive/negative domain, which
            implementations must honor.
        """
        ...
