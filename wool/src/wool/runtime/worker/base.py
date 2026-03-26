from __future__ import annotations

import uuid
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import Final
from typing import Protocol
from typing import final
from typing import runtime_checkable

import grpc

if TYPE_CHECKING:
    from wool.runtime.worker.auth import WorkerCredentials
    from wool.runtime.worker.metadata import WorkerMetadata


# public
@dataclass(frozen=True)
class ChannelOptions:
    """Options for gRPC channel configuration.

    Controls the maximum message sizes and keepalive behaviour for
    gRPC channels.  Workers advertise these options via
    :class:`~wool.runtime.discovery.base.WorkerMetadata` so that
    clients connect with compatible settings automatically.

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

    Composes :class:`ChannelOptions` (advertised to clients) with
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
@runtime_checkable
class WorkerFactory(Protocol):
    """Protocol for worker factory callables.

    Worker factories create :class:`WorkerLike` instances with specific
    tags and configuration. Used by :class:`WorkerPool` to spawn workers.
    """

    def __call__(
        self,
        *tags: str,
        credentials: WorkerCredentials | None = None,
    ) -> WorkerLike:
        """Create a new worker instance.

        :param tags:
            Capability tags for worker discovery and filtering.
        :param credentials:
            Credentials for the worker.
        :returns:
            Configured :class:`WorkerLike` instance.
        """
        ...


# public
@runtime_checkable
class WorkerLike(Protocol):
    """Protocol defining the worker interface.

    All worker implementations must satisfy this protocol. Prefer
    :class:`WorkerLike` over :class:`Worker` for type annotations to
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

    async def stop(self, *, timeout: float | None = None):
        """Stop the worker and unregister it from the pool.

        :param timeout:
            Maximum time in seconds to wait for worker shutdown.
        :raises RuntimeError:
            If the worker has not been started.
        """
        ...


class Worker(ABC):
    """Abstract base class for worker implementations.

    Workers execute distributed tasks in dedicated processes, each running
    a gRPC server for task dispatch. Subclasses implement the actual worker
    process lifecycle in :meth:`_start` and :meth:`_stop`.

    **Implementing a custom worker:**

    .. code-block:: python

        from wool.runtime.worker.base import Worker
        from wool.runtime.worker.metadata import WorkerMetadata


        class CustomWorker(Worker):
            async def _start(self, timeout):
                # Start your worker process
                self._info = WorkerMetadata(...)

            async def _stop(self, timeout):
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
    async def stop(self, *, timeout: float | None = None):
        """Stop the worker and unregister it from the pool.

        This method is a final implementation that calls the abstract
        `_stop` method to gracefully shut down the worker process and
        unregister it from the registrar service.
        """
        if not self._started:
            raise RuntimeError("Worker has not been started")
        try:
            await self._stop(timeout)
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
    async def _stop(self, timeout: float | None):
        """Implementation-specific worker shutdown logic.

        Subclasses must implement this method to handle the graceful
        shutdown of their worker process and cleanup of resources.
        """
        ...
