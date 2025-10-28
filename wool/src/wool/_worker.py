from __future__ import annotations

import asyncio
import signal
import uuid
from abc import ABC
from abc import abstractmethod
from contextlib import contextmanager
from multiprocessing import Pipe
from multiprocessing import Process
from multiprocessing.connection import Connection
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import Any
from typing import AsyncContextManager
from typing import Awaitable
from typing import ContextManager
from typing import Final
from typing import Protocol
from typing import final

import grpc.aio

import wool
from wool import _protobuf as pb
from wool._resource_pool import ResourcePool
from wool._worker_discovery import Factory
from wool._worker_discovery import RegistrarLike
from wool._worker_discovery import WorkerInfo
from wool._worker_service import WorkerService

if TYPE_CHECKING:
    from wool._worker_proxy import WorkerProxy


@contextmanager
def _signal_handlers(service: WorkerService):
    """Context manager for setting up signal handlers for graceful shutdown.

    Installs SIGTERM and SIGINT handlers that gracefully shut down the worker
    service when the process receives termination signals.

    :param service:
        The :class:`WorkerService` instance to shut down on signal receipt.
    :yields:
        Control to the calling context with signal handlers installed.
    """
    loop = asyncio.get_running_loop()

    def sigterm_handler(signum, frame):
        if loop.is_running():
            loop.call_soon_threadsafe(
                lambda: asyncio.create_task(
                    service.stop(pb.worker.StopRequest(timeout=0), None)
                )
            )

    def sigint_handler(signum, frame):
        if loop.is_running():
            loop.call_soon_threadsafe(
                lambda: asyncio.create_task(
                    service.stop(pb.worker.StopRequest(timeout=None), None)
                )
            )

    old_sigterm = signal.signal(signal.SIGTERM, sigterm_handler)
    old_sigint = signal.signal(signal.SIGINT, sigint_handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, old_sigterm)
        signal.signal(signal.SIGINT, old_sigint)


# public
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
    _registrar: RegistrarLike | Factory[RegistrarLike]
    _registrar_service: RegistrarLike | None = None
    _registrar_context: Any | None = None
    _uid: Final[str]
    _tags: Final[set[str]]
    _extra: Final[dict[str, Any]]

    def __init__(
        self, *tags: str, registrar: RegistrarLike | Factory[RegistrarLike], **extra: Any
    ):
        self._uid = f"worker-{uuid.uuid4().hex}"
        self._tags = set(tags)
        self._extra = extra
        self._registrar = registrar

    @property
    def uid(self) -> str:
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

        self._registrar_service, self._registrar_context = await self._enter_context(
            self._registrar
        )
        if not isinstance(self._registrar_service, RegistrarLike):
            raise ValueError("Registrar factory must return a RegistrarLike instance")

        await self._start(timeout=timeout)
        self._started = True
        assert self._info
        await self._registrar_service.register(self._info)

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
            if not self._info:
                raise RuntimeError("Cannot unregister - worker has no info")
            assert self._registrar_service is not None
            await self._registrar_service.unregister(self._info)
        finally:
            try:
                await self._stop(timeout)
            finally:
                await self._exit_context(self._registrar_context)
                self._registrar_service = None
                self._registrar_context = None
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

    async def _enter_context(self, factory):
        """Enter context for factory objects, handling different factory types."""
        ctx = None
        if isinstance(factory, ContextManager):
            ctx = factory
            obj = ctx.__enter__()
        elif isinstance(factory, AsyncContextManager):
            ctx = factory
            obj = await ctx.__aenter__()
        elif callable(factory):
            return await self._enter_context(factory())
        elif isinstance(factory, Awaitable):
            obj = await factory
        else:
            obj = factory
        return obj, ctx

    async def _exit_context(
        self, ctx: AsyncContextManager | ContextManager | None, *args
    ):
        """Exit context for context managers."""
        if not args:
            args = (None, None, None)
        if isinstance(ctx, AsyncContextManager):
            await ctx.__aexit__(*args)
        elif isinstance(ctx, ContextManager):
            ctx.__exit__(*args)


# public
class WorkerFactory(Protocol):
    """Protocol for creating worker instances with registrar integration.

    Defines the callable interface for worker factory implementations
    that can create :class:`Worker` instances configured with specific
    capability tags and metadata.

    Worker factories are used by :class:`WorkerPool` to spawn multiple
    worker processes with consistent configuration.
    """

    def __call__(self, *tags: str, **_) -> Worker:
        """Create a new worker instance.

        :param tags:
            Additional tags to associate with this worker for discovery
            and filtering purposes.
        :returns:
            A new :class:`Worker` instance configured with the
            specified tags and metadata.
        """
        ...


# public
class LocalWorker(Worker):
    """Local worker implementation that runs tasks in a separate process.

    :class:`LocalWorker` creates and manages a dedicated worker process
    that hosts a gRPC server for executing distributed wool tasks. Each
    worker automatically registers itself with the provided registrar service
    for discovery by client sessions.

    The worker process runs independently and can handle multiple concurrent
    tasks within its own asyncio event loop, providing process-level
    isolation for task execution.

    :param tags:
        Capability tags to associate with this worker for filtering
        and selection by client sessions.
    :param host:
        Host address where the worker will listen.
    :param port:
        Port number where the worker will listen. If 0, a random
        available port will be selected.
    :param registrar:
        Service instance or factory for worker registration and discovery.
    :param shutdown_grace_period:
        Graceful shutdown timeout for the gRPC server in seconds.
    :param proxy_pool_ttl:
        Time-to-live for the proxy resource pool in seconds.
    :param extra:
        Additional arbitrary metadata as key-value pairs.
    """

    _worker_process: WorkerProcess

    def __init__(
        self,
        *tags: str,
        host: str = "127.0.0.1",
        port: int = 0,
        registrar: RegistrarLike | Factory[RegistrarLike],
        shutdown_grace_period: float = 60.0,
        proxy_pool_ttl: float = 60.0,
        **extra: Any,
    ):
        super().__init__(*tags, registrar=registrar, **extra)
        self._worker_process = WorkerProcess(
            host=host,
            port=port,
            shutdown_grace_period=shutdown_grace_period,
            proxy_pool_ttl=proxy_pool_ttl,
        )

    @property
    def address(self) -> str | None:
        """The network address where the worker is listening.

        :returns:
            The address in "host:port" format, or None if not started.
        """
        return self._worker_process.address

    @property
    def host(self) -> str | None:
        """The host where the worker is listening.

        :returns:
            The host address, or None if not started.
        """
        return self._info.host if self._info else None

    @property
    def port(self) -> int | None:
        """The port where the worker is listening.

        :returns:
            The port number, or None if not started.
        """
        return self._info.port if self._info else None

    async def _start(self, timeout: float | None):
        """Start the worker process and register it with the pool.

        Initializes the registrar service, starts the worker process
        with its gRPC server, and registers the worker's network
        address with the registrar for discovery by client sessions.

        :param timeout:
            Maximum time in seconds to wait for worker process startup.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda t: self._worker_process.start(timeout=t), timeout
        )
        if not self._worker_process.address:
            raise RuntimeError("Worker process failed to start - no address")
        if not self._worker_process.pid:
            raise RuntimeError("Worker process failed to start - no PID")

        host, port_str = self._worker_process.address.split(":")
        port = int(port_str)

        self._info = WorkerInfo(
            uid=self._uid,
            host=host,
            port=port,
            pid=self._worker_process.pid,
            version=wool.__version__,
            tags=frozenset(self._tags),
            extra=MappingProxyType(self._extra),
        )

    async def _stop(self, timeout: float | None):
        """Stop the worker process and unregister it from the pool.

        Unregisters the worker from the registrar service, gracefully
        shuts down the worker process using SIGINT, and cleans up
        the registrar service. If graceful shutdown fails, the process
        is forcefully terminated.
        """
        if self._worker_process.is_alive():
            assert self.address
            channel = grpc.aio.insecure_channel(self.address)
            stub = pb.worker.WorkerStub(channel)
            await stub.stop(pb.worker.StopRequest(timeout=timeout))


class WorkerProcess(Process):
    """A :class:`multiprocessing.Process` that runs a gRPC worker
    server.

    :class:`WorkerProcess` creates an isolated Python process that hosts a
    gRPC server for executing distributed tasks. Each process maintains
    its own event loop and serves as an independent worker node in the
    wool distributed runtime.

    :param host:
        Host address where the gRPC server will listen.
    :param port:
        Port number where the gRPC server will listen. If 0, a random
        available port will be selected.
    :param shutdown_grace_period:
        Graceful shutdown timeout for the gRPC server in seconds.
    :param proxy_pool_ttl:
        Time-to-live for the proxy resource pool in seconds.
    :param args:
        Additional positional arguments passed to the parent
        :class:`multiprocessing.Process` class.
    :param kwargs:
        Additional keyword arguments passed to the parent
        :class:`multiprocessing.Process` class.

    .. attribute:: address
        The network address where the gRPC server is listening.
    """

    _port: int | None
    _get_port: Connection
    _set_port: Connection
    _shutdown_grace_period: float
    _proxy_pool_ttl: float

    def __init__(
        self,
        *args,
        host: str = "127.0.0.1",
        port: int = 0,
        shutdown_grace_period: float = 60.0,
        proxy_pool_ttl: float = 60.0,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not host:
            raise ValueError("Host must be a non-blank string")
        self._host = host
        if port < 0:
            raise ValueError("Port must be a positive integer")
        self._port = port
        if shutdown_grace_period <= 0:
            raise ValueError("Shutdown grace period must be positive")
        self._shutdown_grace_period = shutdown_grace_period
        if proxy_pool_ttl <= 0:
            raise ValueError("Proxy pool TTL must be positive")
        self._proxy_pool_ttl = proxy_pool_ttl
        self._get_port, self._set_port = Pipe(duplex=False)

    @property
    def address(self) -> str | None:
        """The network address where the gRPC server is listening.

        :returns:
            The address in "host:port" format, or None if not started.
        """
        return self._address(self._host, self._port) if self._port else None

    @property
    def host(self) -> str | None:
        """The host where the gRPC server is listening.

        :returns:
            The host address, or None if not started.
        """
        return self._host

    @property
    def port(self) -> int | None:
        """The port where the gRPC server is listening.

        :returns:
            The port number, or None if not started.
        """
        return self._port or None

    def start(self, *, timeout: float | None = None):
        """Start the worker process.

        Launches the worker process and waits until it has started
        listening on a port. After starting, the :attr:`address`
        property will contain the actual network address.

        :param timeout:
            Maximum time in seconds to wait for worker process startup.
        :raises RuntimeError:
            If the worker process fails to start within the timeout.
        :raises ValueError:
            If the timeout is not positive.
        """
        if timeout is not None and timeout <= 0:
            raise ValueError("Timeout must be positive")
        super().start()
        if self._get_port.poll(timeout=timeout):
            self._port = self._get_port.recv()
        else:
            self.terminate()
            self.join()
            raise RuntimeError(
                f"Worker process failed to start within {timeout} seconds"
            )
        self._get_port.close()

    def run(self) -> None:
        """Run the worker process.

        Sets the event loop for this process and starts the gRPC server,
        blocking until the server is stopped.
        """

        async def proxy_factory(proxy: WorkerProxy):
            """Factory function for WorkerProxy instances in ResourcePool.

            Starts the proxy if not already started and returns it.
            The proxy object itself is used as the cache key.

            :param proxy:
                The WorkerProxy instance to start (passed as key from ResourcePool).
            :returns:
                The started WorkerProxy instance.
            """
            if not proxy.started:
                await proxy.start()
            return proxy

        async def proxy_finalizer(proxy: WorkerProxy):
            """Finalizer function for WorkerProxy instances in ResourcePool.

            Stops the proxy when it's being cleaned up from the resource pool.
            Based on the cleanup logic from WorkerProxyCache._delayed_cleanup.

            :param proxy:
                The WorkerProxy instance to clean up.
            """
            try:
                await proxy.stop()
            except Exception:
                pass

        wool.__proxy_pool__.set(
            ResourcePool(
                factory=proxy_factory,
                finalizer=proxy_finalizer,
                ttl=self._proxy_pool_ttl,
            )
        )
        asyncio.run(self._serve())

    async def _serve(self):
        """Start the gRPC server in this worker process.

        This method is called by the event loop to start serving
        requests. It creates a gRPC server, adds the worker service, and
        starts listening for incoming connections.
        """
        server = grpc.aio.server()
        port = server.add_insecure_port(self._address(self._host, self._port))
        service = WorkerService()
        pb.add_to_server[pb.worker.WorkerServicer](service, server)

        with _signal_handlers(service):
            try:
                await server.start()
                try:
                    self._set_port.send(port)
                finally:
                    self._set_port.close()
                await service.stopped.wait()
            finally:
                await server.stop(grace=self._shutdown_grace_period)

    def _address(self, host, port) -> str:
        """Format network address for the given port.

        :param port:
            Port number to include in the address.
        :returns:
            Address string in "host:port" format.
        """
        return f"{host}:{port}"
