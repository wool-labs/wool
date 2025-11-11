from __future__ import annotations

import uuid
from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Final
from typing import Protocol
from typing import final
from typing import runtime_checkable

from wool.runtime.discovery.base import WorkerMetadata


# public
@runtime_checkable
class WorkerFactory(Protocol):
    """Protocol for worker factory callables.

    Worker factories create :class:`WorkerLike` instances with specific
    tags and configuration. Used by :class:`WorkerPool` to spawn workers.

    **Example factory:**

    .. code-block:: python

        from functools import partial


        def custom_factory(*tags, custom_param=None):
            return LocalWorker(*tags, host="0.0.0.0", port=8080)


        # Or using partial
        factory = partial(LocalWorker, host="0.0.0.0")

    """

    def __call__(self, *tags: str, **_) -> WorkerLike:
        """Create a new worker instance.

        :param tags:
            Capability tags for worker discovery and filtering.
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
    """Abstract base class for worker implementations.

    Workers execute distributed tasks in dedicated processes, each running
    a gRPC server for task dispatch. Subclasses implement the actual worker
    process lifecycle in :meth:`_start` and :meth:`_stop`.

    **Implementing a custom worker:**

    .. code-block:: python

        from wool.runtime.worker.base import Worker
        from wool.runtime.discovery.base import WorkerMetadata


        class CustomWorker(Worker):
            async def _start(self, timeout):
                # Start your worker process
                self._info = WorkerMetadata(...)

            async def _stop(self, timeout):
                # Clean shutdown
                ...

            @property
            def address(self):
                return f"{self.host}:{self.port}"

            @property
            def host(self):
                return self._host

            @property
            def port(self):
                return self._port

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
