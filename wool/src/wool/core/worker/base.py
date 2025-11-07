from __future__ import annotations

import uuid
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Final
from typing import Protocol
from typing import final
from typing import runtime_checkable

from wool.core.discovery.base import WorkerInfo


# public
@runtime_checkable
class WorkerFactory(Protocol):
    """Protocol for creating worker instances with registrar integration.

    Defines the callable interface for worker factory implementations
    that can create :class:`WorkerLike` instances configured with specific
    capability tags and metadata.

    Worker factories are used by :class:`WorkerPool` to spawn multiple
    worker processes with consistent configuration.
    """

    def __call__(self, *tags: str, **_) -> WorkerLike:
        """Create a new worker instance.

        :param tags:
            Additional tags to associate with this worker for discovery
            and filtering purposes.
        :returns:
            A new :class:`WorkerLike` instance configured with the
            specified tags and metadata.
        """
        ...


# public
@runtime_checkable
class WorkerLike(Protocol):
    """Protocol defining the public interface for worker implementations.

    This protocol represents the structural interface that all workers
    must implement, regardless of their concrete implementation. It is
    the preferred type for type hints and function signatures that accept
    worker instances.

    .. note::
       Prefer using :class:`WorkerLike` for type annotations instead of
       :class:`Worker` to allow for structural subtyping and greater
       flexibility in worker implementations.
    """

    @property
    def uid(self) -> uuid.UUID:
        """The worker's unique identifier.

        :returns:
            Unique UUID assigned to this worker instance.
        """
        ...

    @property
    def info(self) -> WorkerInfo | None:
        """Worker information including network address and metadata.

        :returns:
            The worker's complete information or None if not started.
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

    @property
    def host(self) -> str | None:
        """Hostname or IP address of the worker.

        :returns:
            The worker's host or None if not started.
        """
        ...

    @property
    def port(self) -> int | None:
        """Port number the worker is listening on.

        :returns:
            The worker's port number or None if not started.
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
    """Abstract base class for worker implementations in the wool framework.

    Workers are individual processes that execute distributed tasks within
    a worker pool. Each worker runs a gRPC server and registers itself with
    a discovery service to be found by client sessions.

    This class defines the core interface that all worker implementations
    must provide, including lifecycle management and registrar service
    integration for peer-to-peer discovery.

    :param tags:
        Capability tags associated with this worker for filtering and
        selection by client sessions.
    :param registrar:
        Service instance or factory for worker registration and discovery
        within the distributed pool. Can be provided as:

        - **Instance**: Direct registrar service object
        - **Factory function**: Function returning a registrar service instance
        - **Context manager factory**: Function returning a context manager
            that yields a registrar service
    :param extra:
        Additional arbitrary metadata as key-value pairs.
    """

    _info: WorkerInfo | None = None
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
    def info(self) -> WorkerInfo | None:
        """Worker information including network address and metadata.

        :returns:
            The worker's complete information or None if not started.
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

    @property
    @abstractmethod
    def host(self) -> str | None: ...

    @property
    @abstractmethod
    def port(self) -> int | None: ...

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
