from __future__ import annotations

import asyncio
from types import MappingProxyType
from typing import Any

import grpc.aio

import wool
from wool.runtime import protobuf as pb
from wool.runtime.discovery.base import WorkerMetadata
from wool.runtime.worker.base import Worker
from wool.runtime.worker.process import WorkerProcess


# public
class LocalWorker(Worker):
    """Worker running in a local subprocess.

    Spawns a dedicated process hosting a gRPC server for task execution.
    Handles multiple concurrent tasks in an isolated asyncio event loop.

    **Basic usage:**

    .. code-block:: python

        worker = LocalWorker("gpu-capable")
        await worker.start()
        # Worker is now accepting tasks
        await worker.stop()

    **Custom configuration:**

    .. code-block:: python

        worker = LocalWorker(
            "production",
            "high-memory",
            host="0.0.0.0",  # Listen on all interfaces
            port=50051,  # Fixed port
            shutdown_grace_period=30.0,
        )

    :param tags:
        Capability tags for filtering and selection.
    :param host:
        Host address to bind. Defaults to localhost.
    :param port:
        Port to bind. 0 for random available port.
    :param shutdown_grace_period:
        Graceful shutdown timeout in seconds.
    :param proxy_pool_ttl:
        Proxy pool TTL in seconds.
    :param extra:
        Additional metadata as key-value pairs.
    """

    _worker_process: WorkerProcess

    def __init__(
        self,
        *tags: str,
        host: str = "127.0.0.1",
        port: int = 0,
        shutdown_grace_period: float = 60.0,
        proxy_pool_ttl: float = 60.0,
        **extra: Any,
    ):
        super().__init__(*tags, **extra)
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

        self._info = WorkerMetadata(
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
