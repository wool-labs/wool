from __future__ import annotations

import asyncio
import signal
from contextlib import contextmanager
from functools import partial
from multiprocessing import Pipe
from multiprocessing import Process
from multiprocessing.connection import Connection
from typing import TYPE_CHECKING

import grpc.aio

import wool
from wool.runtime import protobuf as pb
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.worker.service import WorkerService

if TYPE_CHECKING:
    from wool.runtime.worker.proxy import WorkerProxy


class WorkerProcess(Process):
    """Subprocess hosting a gRPC worker server.

    Isolated Python process running a gRPC server for task execution.
    Maintains its own event loop and serves as an independent worker node.

    Communicates the bound port back to the parent process via pipe after
    startup. Handles SIGTERM and SIGINT for graceful shutdown.

    :param host:
        Host address to bind.
    :param port:
        Port to bind. 0 for random available port.
    :param shutdown_grace_period:
        Graceful shutdown timeout in seconds.
    :param proxy_pool_ttl:
        Proxy pool TTL in seconds.
    :param args:
        Additional args for :class:`multiprocessing.Process`.
    :param kwargs:
        Additional kwargs for :class:`multiprocessing.Process`.
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
        if port < 0 or port > 65535:
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
        wool.__proxy_pool__.set(
            ResourcePool(
                factory=_proxy_factory,
                finalizer=_proxy_finalizer,
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

    old_sigterm = signal.signal(signal.SIGTERM, partial(_sigterm_handler, loop, service))
    old_sigint = signal.signal(signal.SIGINT, partial(_sigint_handler, loop, service))
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, old_sigterm)
        signal.signal(signal.SIGINT, old_sigint)


def _sigterm_handler(loop, service, signum, frame):
    if loop.is_running():
        loop.call_soon_threadsafe(
            lambda: asyncio.create_task(
                service.stop(pb.worker.StopRequest(timeout=0), None)
            )
        )


def _sigint_handler(loop, service, signum, frame):
    if loop.is_running():
        loop.call_soon_threadsafe(
            lambda: asyncio.create_task(
                service.stop(pb.worker.StopRequest(timeout=None), None)
            )
        )


async def _proxy_factory(proxy: WorkerProxy):
    """Factory function for WorkerProxy instances in ResourcePool.

    Starts the proxy if not already started and returns it.
    The proxy object itself is used as the cache key.

    :param proxy:
        The WorkerProxy instance to start (passed as key from
        ResourcePool).
    :returns:
        The started WorkerProxy instance.
    """
    if not proxy.started:
        await proxy.start()
    return proxy


async def _proxy_finalizer(proxy: WorkerProxy):
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
