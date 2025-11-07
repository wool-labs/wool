from __future__ import annotations

import asyncio
from types import MappingProxyType
from typing import Any

import grpc.aio

import wool
from wool.core import protobuf as pb
from wool.core.discovery.base import WorkerInfo
from wool.core.worker.base import Worker
from wool.core.worker.process import WorkerProcess


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
