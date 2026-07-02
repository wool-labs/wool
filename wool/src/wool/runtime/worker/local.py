from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from typing import Any
from typing import Final

from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.base import Worker
from wool.runtime.worker.base import WorkerOptions
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.process import WorkerProcess

if TYPE_CHECKING:
    from wool.runtime.worker.service import BackpressureLike

_STOP_RPC_MARGIN: Final[float] = 5.0
"""Seconds added to a finite stop timeout to form the graceful stop
RPC's deadline. The worker drains in-flight tasks for up to the full
stop timeout before responding, so the deadline must exceed it; the
margin covers transport and response overhead. Without a deadline, a
wedged worker would hang ``stop()`` forever and the force-terminate
fallback would never be reached."""


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
    :param credentials:
        Optional credentials for TLS/mTLS authentication:

        - :class:`WorkerCredentials`: Provides both server and client
          credentials for mutual TLS. Enables secure worker-to-worker
          communication.
        - ``None``: Worker uses insecure connections.
    :param options:
        gRPC message size options. Defaults to
        :class:`WorkerOptions` with 100 MB limits.
    :param backpressure:
        Optional admission control hook. A callable receiving a
        :class:`~wool.runtime.worker.service.BackpressureContext`
        and returning ``True`` to **reject** the task or ``False``
        to **accept** it. Both sync and async callables are
        supported. When a task is rejected the worker responds with
        gRPC ``RESOURCE_EXHAUSTED``, causing the load balancer to
        skip to the next worker. ``None`` (default) accepts all
        tasks unconditionally.
    :param daemon:
        Whether the worker subprocess is daemonic. Forwarded to
        `WorkerProcess`, which owns the default and the per-value
        semantics; omit to accept that default.
    :param extra:
        Additional metadata as key-value pairs.
    """

    _worker_process: WorkerProcess
    _credentials: WorkerCredentials | None

    def __init__(
        self,
        *tags: str,
        host: str = "127.0.0.1",
        port: int = 0,
        shutdown_grace_period: float = 60.0,
        proxy_pool_ttl: float = 60.0,
        credentials: WorkerCredentials | None = None,
        options: WorkerOptions | None = None,
        backpressure: BackpressureLike | None = None,
        daemon: bool | None | UndefinedType = Undefined,
        **extra: Any,
    ):
        super().__init__(*tags, **extra)
        self._credentials = credentials
        self._worker_process = WorkerProcess(
            uid=self._uid,
            host=host,
            port=port,
            shutdown_grace_period=shutdown_grace_period,
            proxy_pool_ttl=proxy_pool_ttl,
            credentials=credentials,
            options=options,
            tags=frozenset(self._tags),
            extra=self._extra,
            backpressure=backpressure,
            **({} if daemon is Undefined else {"daemon": daemon}),
        )

    @property
    def address(self) -> str | None:
        """The network address where the worker is listening.

        :returns:
            The address in "host:port" format, or None if not started.
        """
        return self._worker_process.address

    async def _start(self, timeout: float | None):
        """Start the worker subprocess and adopt its metadata.

        Runs the blocking `WorkerProcess.start` in an executor thread
        and, once the subprocess reports back over its pipe, adopts
        the reported metadata as this worker's own.

        :param timeout:
            Maximum time in seconds to wait for worker process startup.
        :raises RuntimeError:
            If the worker process starts without reporting metadata.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda t: self._worker_process.start(timeout=t), timeout
        )
        self._info = self._worker_process.metadata
        if self._info is None:
            raise RuntimeError("Worker process failed to start - no metadata")

    async def _stop(self, grace: float | None):
        """Stop the worker process gracefully, then reap it.

        Sends the worker a stop RPC bounded by a deadline derived from
        ``grace`` (see `_STOP_RPC_MARGIN`), then — however the RPC
        fared — reaps the subprocess; see `WorkerProcess.reap` for the
        escalation. The reap runs in an executor thread so it
        completes even when this coroutine is cancelled; only a second
        cancellation landing while the executor job is still queued
        can skip it, in which case the worker-side parent watchdog
        remains the backstop against orphans.

        Credential and secure-channel handling for the stop RPC lives
        on `WorkerConnection.stop`.

        :param grace:
            Bound on the worker's graceful drain, forwarded in the
            stop request; see `WorkerConnection.stop` for its
            semantics.
        """
        try:
            if self._worker_process.is_alive():
                assert self.address

                # Route the stop RPC through WorkerConnection; ``close``
                # releases the pooled channel this stop acquired.
                credentials = (
                    self._credentials.client_credentials()
                    if self._credentials is not None
                    else None
                )
                connection = WorkerConnection(self.address, credentials=credentials)
                try:
                    # ``grace=None`` keeps the RPC unbounded; see
                    # `_STOP_RPC_MARGIN` for the finite deadline.
                    deadline = grace + _STOP_RPC_MARGIN if grace is not None else None
                    await connection.stop(grace=grace, timeout=deadline)
                finally:
                    await connection.close()
        finally:
            # `reap` blocks on `join`, so it must run off-loop; see
            # the docstring for the cancellation contract.
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._worker_process.reap, grace)
