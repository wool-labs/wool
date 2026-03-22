from __future__ import annotations

import asyncio
import contextlib
import logging
import multiprocessing as _mp
import os
import signal
import socket
import sys
import tempfile
import uuid
from contextlib import contextmanager
from functools import partial
from multiprocessing.connection import Connection
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import Any
from typing import Final

import grpc.aio

import wool
from wool import protocol
from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.worker.auth import CredentialContext
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.base import WorkerOptions
from wool.runtime.worker.interceptor import VersionInterceptor
from wool.runtime.worker.service import WorkerService

if TYPE_CHECKING:
    from wool.runtime.worker.proxy import WorkerProxy

_ctx = _mp.get_context("spawn")
Pipe = _ctx.Pipe
Process = _ctx.Process

logger = logging.getLogger(__name__)

_HAS_UDS: Final[bool] = hasattr(socket, "AF_UNIX")


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
    :param credentials:
        Optional worker credentials for TLS/mTLS.
    :param options:
        gRPC message size options. Defaults to
        :class:`WorkerOptions` with 100 MB limits.
    :param args:
        Additional args for :class:`multiprocessing.Process`.
    :param kwargs:
        Additional kwargs for :class:`multiprocessing.Process`.
    """

    _port: int | None
    _get_metadata: Connection
    _set_metadata: Connection
    _metadata: WorkerMetadata | None
    _shutdown_grace_period: float
    _proxy_pool_ttl: float
    _credentials: WorkerCredentials | None
    _options: WorkerOptions

    def __init__(
        self,
        *args,
        uid: uuid.UUID | None = None,
        host: str = "127.0.0.1",
        port: int = 0,
        shutdown_grace_period: float = 60.0,
        proxy_pool_ttl: float = 60.0,
        credentials: WorkerCredentials | None = None,
        options: WorkerOptions | None = None,
        tags: frozenset[str] = frozenset(),
        extra: dict[str, Any] | None = None,
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
        self._credentials = credentials
        self._options = options or WorkerOptions()
        self._uid = uid if uid is not None else uuid.uuid4()
        self._tags = tags
        self._extra = extra if extra is not None else {}
        self._metadata = None
        self._get_metadata, self._set_metadata = Pipe(duplex=False)

    @property
    def address(self) -> str | None:
        """The network address where the gRPC server is listening.

        After :meth:`start`, the address comes from the
        :class:`WorkerMetadata` returned by the child process.
        Before start, returns ``host:port`` when a fixed port was
        given, or ``None`` when port is 0 (random).

        :returns:
            The address in "host:port" format, or None if not started
            and port is 0.
        """
        if self._metadata is not None:
            return self._metadata.address
        return None

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

    @property
    def metadata(self) -> WorkerMetadata | None:
        """The worker metadata received from the child process.

        :returns:
            :class:`WorkerMetadata` once started, or ``None``.
        """
        return self._metadata

    def start(self, *, timeout: float | None = None):
        """Start the worker process.

        Launches the worker process and waits until it has reported
        its :class:`WorkerMetadata` back via pipe. After starting,
        the :attr:`metadata` and :attr:`address` properties are
        populated.

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
        if self._get_metadata.poll(timeout=timeout):
            self._metadata = WorkerMetadata.from_protobuf(
                protocol.WorkerMetadata.FromString(self._get_metadata.recv())
            )
            assert self._metadata is not None
            self._port = int(self._metadata.address.rsplit(":", 1)[1])
        else:
            self.terminate()
            self.join()
            raise RuntimeError(
                f"Worker process failed to start within {timeout} seconds"
            )
        self._get_metadata.close()

    def run(self) -> None:
        """Run the worker process.

        Sets the event loop for this process and starts the gRPC server,
        blocking until the server is stopped.
        """
        # Configure logging for this subprocess
        logging.basicConfig(
            level=logging.INFO,
            format=f"%(asctime)s - WORKER[{self.pid}] - %(name)s - %(levelname)s - %(message)s",
            stream=sys.stderr,
        )
        logger.info(f"Worker process starting on {self._host}:{self._port}")

        wool.__proxy_pool__.set(
            ResourcePool(
                factory=_proxy_factory,
                finalizer=_proxy_finalizer,
                ttl=self._proxy_pool_ttl,
            )
        )
        try:
            asyncio.run(self._serve())
        except Exception as e:
            logger.exception(f"Worker process crashed: {type(e).__name__}: {e}")
            raise

    async def _serve(self):
        """Start the gRPC server in this worker process.

        This method is called by the event loop to start serving
        requests. It creates a gRPC server, adds the worker service, and
        starts listening for incoming connections.
        """
        creds_ctx = (
            CredentialContext(self._credentials)
            if self._credentials is not None
            else contextlib.nullcontext()
        )
        with creds_ctx:
            grpc_options = [
                (
                    "grpc.max_receive_message_length",
                    self._options.max_receive_message_length,
                ),
                ("grpc.max_send_message_length", self._options.max_send_message_length),
            ]
            server = grpc.aio.server(
                interceptors=[VersionInterceptor()], options=grpc_options
            )
            credentials = (
                self._credentials.server_credentials()
                if self._credentials is not None
                else None
            )
            address = self._address(self._host, self._port)

            if credentials is not None:
                port = server.add_secure_port(address, credentials)
            else:
                port = server.add_insecure_port(address)

            uds_address = None
            if _HAS_UDS:
                uds_path = os.path.join(tempfile.gettempdir(), f"wool-{self._uid}.sock")
                uds_target = f"unix:{uds_path}"
                with contextlib.suppress(OSError):
                    os.unlink(uds_path)
                server.add_insecure_port(uds_target)
                uds_address = uds_target

            service = WorkerService()
            protocol.add_to_server[protocol.WorkerServicer](service, server)

            with _signal_handlers(service):
                try:
                    await server.start()
                    logger.info(f"Worker gRPC server started on port {port}")

                    metadata = WorkerMetadata(
                        uid=self._uid,
                        address=self._address(self._host, port),
                        pid=os.getpid(),
                        version=protocol.__version__,
                        tags=self._tags,
                        extra=MappingProxyType(self._extra),
                        secure=self._credentials is not None,
                    )
                    wool.__worker_metadata__ = metadata
                    wool.__worker_uds_address__ = uds_address
                    wool.__worker_service__.set(service)

                    try:
                        self._set_metadata.send(
                            metadata.to_protobuf().SerializeToString()
                        )
                    finally:
                        self._set_metadata.close()
                    await service.stopped.wait()
                    logger.info("Worker service stopped, shutting down server")
                except Exception as e:
                    logger.exception(f"Worker server error: {type(e).__name__}: {e}")
                    raise
                finally:
                    logger.info("Worker server stopping with grace period")
                    await server.stop(grace=self._shutdown_grace_period)
                    if uds_address is not None:
                        uds_path = uds_address.removeprefix("unix:")
                        with contextlib.suppress(OSError):
                            os.unlink(uds_path)

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
                service.stop(protocol.StopRequest(timeout=0), None)
            )
        )


def _sigint_handler(loop, service, signum, frame):
    if loop.is_running():
        loop.call_soon_threadsafe(
            lambda: asyncio.create_task(
                service.stop(protocol.StopRequest(timeout=-1), None)
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
