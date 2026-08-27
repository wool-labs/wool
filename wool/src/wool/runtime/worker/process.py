from __future__ import annotations

import asyncio
import contextlib
import logging
import multiprocessing as _mp
import os
import shutil
import signal
import socket
import sys
import threading
import time
import uuid
import warnings
from contextlib import contextmanager
from functools import partial
from multiprocessing.connection import Connection
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import Any
from typing import Final

import cloudpickle
import grpc.aio

import wool
from wool import protocol
from wool.runtime.context.factory import install_task_factory
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.worker.auth import WorkerCredentials
from wool.runtime.worker.auth import WorkerCredentialsProvider
from wool.runtime.worker.auth import credentials_scope
from wool.runtime.worker.base import WorkerOptions
from wool.runtime.worker.exceptions import SlowCredentialResolutionWarning
from wool.runtime.worker.interceptor import VersionInterceptor
from wool.runtime.worker.metadata import WorkerMetadata
from wool.runtime.worker.service import WorkerService

if TYPE_CHECKING:
    from wool.runtime.worker.proxy import WorkerProxy
    from wool.runtime.worker.service import BackpressureLike

_ctx = _mp.get_context("spawn")
Pipe = _ctx.Pipe
Process = _ctx.Process

logger = logging.getLogger(__name__)

# A worker's own credentials are resolved once, before it serves. The
# resolution is awaited so it cannot stall the child's event loop, but a
# slow one still delays the worker becoming available and, from outside,
# is indistinguishable from a hung start. Warn once this much of it has
# elapsed, while the resolution is still running — a warning that waited
# for the resolution to finish would arrive exactly when it stopped being
# useful.
_SLOW_STARTUP_RESOLVE_S: Final = 5.0

_HAS_UDS: Final[bool] = hasattr(socket, "AF_UNIX")

#: Seconds to wait for a terminated worker process to exit
#: before escalating to SIGKILL in `WorkerProcess.reap`.
_REAP_GRACE: Final[float] = 5.0

# Factor by which the server's HTTP/2 ``MAX_CONCURRENT_STREAMS``
# ceiling exceeds the advertised client concurrency gate.
_SERVER_STREAM_CEILING_MULTIPLIER: Final[int] = 2


class WorkerProcess(Process):
    """Subprocess hosting a gRPC worker server.

    Isolated Python process running a gRPC server for task execution.
    Maintains its own event loop and serves as an independent worker node.

    Communicates the bound port back to the parent process via pipe after
    startup. Handles SIGTERM and SIGINT for graceful shutdown.

    Spawned daemonic by default, so a worker still alive at interpreter
    exit never blocks it and never outlives its parent. One consequence:
    daemonic processes cannot spawn `multiprocessing` children (including
    `concurrent.futures.ProcessPoolExecutor`), so a routine that must
    create them requires a non-daemonic worker; `subprocess` and
    `asyncio` subprocesses are unaffected.

    :param host:
        Host address to bind.
    :param port:
        Port to bind. 0 for random available port.
    :param shutdown_grace_period:
        Graceful shutdown timeout in seconds.
    :param proxy_pool_ttl:
        Proxy pool TTL in seconds.
    :param credentials:
        Optional worker credentials for TLS/mTLS — either a
        `WorkerCredentials` or a `WorkerCredentialsProvider`. With a
        reloadable provider, rotated material is adopted without
        restarting the worker — see `_server_credentials` for the
        per-handshake mechanics.
    :param options:
        gRPC message size options. Defaults to
        `WorkerOptions` with 100 MB limits.
    :param uid:
        Unique identifier for this worker. Auto-generated if not
        provided.
    :param tags:
        Capability tags for filtering and selection.
    :param extra:
        Additional metadata as key-value pairs.
    :param backpressure:
        Optional admission control hook. See
        `~wool.runtime.worker.service.BackpressureLike`.
        Serialized via `wool.__serializer__` for transfer to
        the subprocess.
    :param daemon:
        Whether the worker process is daemonic. Defaults to ``True``.
        ``False`` opts out, which a routine that must create
        `multiprocessing` children requires. ``None`` inherits from the
        creating process, per `multiprocessing.Process`.
    :param args:
        Additional args for `multiprocessing.Process`.
    :param kwargs:
        Additional kwargs for `multiprocessing.Process`.

    .. rubric:: Implementation notes

    The daemonic default and the parent-death watchdog cover opposite
    directions of one invariant: neither process outlives the other.
    `multiprocessing`'s atexit handler terminates daemonic children
    before joining them, and the SIGTERM handler turns that termination
    into a graceful stop. Were the child non-daemonic, the same handler
    would join it while still live — and the child's watchdog cannot
    fire until the parent is already gone, so the join wedges
    interpreter exit. The watchdog covers the reverse case, where the
    parent dies first.
    """

    _port: int | None
    _get_metadata: Connection
    _set_metadata: Connection
    _metadata: WorkerMetadata | None
    _shutdown_grace_period: float
    _proxy_pool_ttl: float
    _provider: WorkerCredentialsProvider | None
    _options: WorkerOptions

    def __init__(
        self,
        *args,
        uid: uuid.UUID | None = None,
        host: str = "127.0.0.1",
        port: int = 0,
        shutdown_grace_period: float = 60.0,
        proxy_pool_ttl: float = 60.0,
        credentials: WorkerCredentials | WorkerCredentialsProvider | None = None,
        options: WorkerOptions | None = None,
        tags: frozenset[str] = frozenset(),
        extra: dict[str, Any] | None = None,
        backpressure: BackpressureLike | None = None,
        daemon: bool | None = True,
        **kwargs,
    ):
        super().__init__(*args, daemon=daemon, **kwargs)
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
        self._provider = WorkerCredentialsProvider.coerce(credentials)
        self._options = options or WorkerOptions()
        self._uid = uid if uid is not None else uuid.uuid4()
        self._tags = tags
        self._extra = extra if extra is not None else {}
        self._metadata = None
        self._backpressure = (
            wool.__serializer__.dumps(backpressure) if backpressure is not None else None
        )
        self._get_metadata, self._set_metadata = Pipe(duplex=False)

    @property
    def address(self) -> str | None:
        """The network address where the gRPC server is listening.

        After `start`, the address comes from the `WorkerMetadata`
        returned by the child process.
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
            `WorkerMetadata` once started, or ``None``.
        """
        return self._metadata

    def start(self, *, timeout: float | None = None):
        """Start the worker process.

        Launches the worker process and waits until it has reported
        its `WorkerMetadata` back via pipe. After starting,
        the `metadata` and `address` properties are
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
            self.reap(timeout=0)
            raise RuntimeError(
                f"Worker process failed to start within {timeout} seconds"
            )
        self._get_metadata.close()

    def reap(self, timeout: float | None = None) -> None:
        """Ensure the worker process has fully terminated.

        Joins the process, escalating to `terminate` (SIGTERM)
        and then `kill` (SIGKILL) if it does not exit within
        the given bound, so a worker can never outlive its manager
        regardless of how graceful shutdown fared. Blocks the calling
        thread; a no-op if the process was never started.

        :param timeout:
            Maximum time in seconds to wait for the process to exit
            on its own before escalating. Defaults to the worker's
            shutdown grace period, the upper bound on legitimate
            post-stop work in the subprocess.
        """
        if self.pid is None:
            return
        self.join(timeout if timeout is not None else self._shutdown_grace_period)
        if self.is_alive():
            logger.warning(
                f"Worker process {self.pid} did not exit gracefully; terminating"
            )
            self.terminate()
            self.join(_REAP_GRACE)
            if self.is_alive():
                logger.warning(
                    f"Worker process {self.pid} survived termination; killing"
                )
                self.kill()
                self.join()

    def run(self) -> None:
        """Run the worker process.

        Sets the event loop for this process and starts the gRPC server,
        blocking until the server is stopped.
        """
        # Configure logging for this subprocess
        logging.basicConfig(
            level=logging.INFO,
            format=(
                f"%(asctime)s - WORKER[{self.pid}] - "
                f"%(name)s - %(levelname)s - %(message)s"
            ),
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

    async def _server_credentials(self) -> grpc.ServerCredentials | None:
        """Build the gRPC server credentials for this worker.

        Returns ``None`` for an insecure worker.  A reloadable provider
        yields `grpc.dynamic_ssl_server_credentials` whose fetcher
        re-resolves the provider on each new connection, so rotated
        certificate, key, or CA material is adopted without restarting the
        worker; the fetcher rides the provider's caching (see
        `WorkerCredentialsProvider.credentials`), and established
        connections continue on their existing material.  A static
        provider takes the unchanged `WorkerCredentials.server_credentials`
        path so the static-mTLS posture is byte-for-byte preserved. The
        mutual-TLS mode is fixed from the initial material, i.e., rotation
        replaces the bytes, not the handshake mode.

        :returns:
            Server credentials, or ``None`` for an insecure worker.
        """
        provider = self._provider
        if provider is None:
            return None
        initial = await self._resolve_startup_credentials(provider)
        if provider.reloadable:
            return grpc.dynamic_ssl_server_credentials(
                initial.server_certificate_configuration(),
                # Synchronous by necessity: the gRPC core calls this per
                # handshake, from its own thread, with no loop to await on.
                lambda: provider.credentials.get().server_certificate_configuration(),
                require_client_authentication=initial.mutual,
            )
        return initial.server_credentials()

    async def _resolve_startup_credentials(
        self, provider: WorkerCredentialsProvider
    ) -> WorkerCredentials:
        """Resolve this worker's own credentials before it begins serving.

        Awaited rather than read inline so a ``factory`` that blocks cannot
        stall the child's event loop while the server is still being built.
        Nothing is being served yet, so the delay is invisible to callers;
        it is not invisible to whoever is waiting for the worker to come
        up, so a timer warns them the moment it turns pathological rather
        than once it is over. Keeping the loop free is what lets that timer
        fire at all, which is the second reason this is awaited.
        """
        handle = asyncio.get_running_loop().call_later(
            _SLOW_STARTUP_RESOLVE_S,
            lambda: warnings.warn(
                f"Worker credential resolution has exceeded "
                f"{_SLOW_STARTUP_RESOLVE_S:g}s and is still running; a slow "
                f"credential factory delays the worker becoming available.",
                SlowCredentialResolutionWarning,
                stacklevel=2,
            ),
        )
        try:
            return await provider.credentials
        finally:
            # A no-op once it has fired, so this needs no guard.
            handle.cancel()

    async def _serve(self):
        """Run the worker's gRPC server for the lifetime of the process.

        Creates the gRPC server with the configured channel options,
        registers the worker service, ties the process's lifetime to
        its parent via the parent-death watchdog, installs credential
        and signal-handler context managers, and blocks until a
        shutdown signal fires.  Where the platform supports ``AF_UNIX``,
        also binds the loopback self-dispatch socket — an insecure
        Unix-domain port whose reachability is confined to the worker's
        own uid (see the worker README's "Local self-dispatch socket"
        for the trust boundary).

        .. rubric:: Implementation notes

        Self-dispatch socket placement.  The socket serves the full,
        unauthenticated dispatch service, so it is bound inside a
        per-worker ``0700`` directory.  That directory lives under a
        short base — ``$XDG_RUNTIME_DIR`` (the per-user runtime dir on
        Linux, already a 0700 tmpfs) where set and present, else
        ``/tmp`` — rather than the system temp dir, because an
        ``AF_UNIX`` path is capped near 104 bytes (108 on Linux, as
        little as 92 on some platforms) and macOS's per-user
        ``$TMPDIR`` (``/var/folders/.../T``) is deep enough to
        overflow it.  The directory name is derived deterministically
        from the worker uid (``wool-{uid}``) so a respawned worker
        reclaims whatever an unclean exit (SIGKILL/OOM skips the
        graceful removal in the ``finally`` block) left behind: uids
        are unique per worker instance — each defaults to a fresh
        ``uuid4`` at construction — so any pre-existing entry at the
        path can only be a dead predecessor's, and it is removed
        before the directory is recreated.  Creation uses a bare
        ``mkdir(0o700)``, not ``makedirs(exist_ok=True)``, so a
        concurrent recreation or a symlink planted between removal
        and creation raises instead of silently binding through it.
        """
        creds_ctx = (
            credentials_scope(self._provider)
            if self._provider is not None
            else contextlib.nullcontext()
        )
        with creds_ctx:
            channel = self._options.channel
            grpc_options = [
                (
                    "grpc.max_receive_message_length",
                    channel.max_receive_message_length,
                ),
                ("grpc.max_send_message_length", channel.max_send_message_length),
                ("grpc.keepalive_time_ms", channel.keepalive_time_ms),
                ("grpc.keepalive_timeout_ms", channel.keepalive_timeout_ms),
                (
                    "grpc.keepalive_permit_without_calls",
                    int(channel.keepalive_permit_without_calls),
                ),
                ("grpc.http2.max_pings_without_data", channel.max_pings_without_data),
                (
                    "grpc.max_concurrent_streams",
                    channel.max_concurrent_streams * _SERVER_STREAM_CEILING_MULTIPLIER,
                ),
                (
                    "grpc.default_compression_algorithm",
                    channel.compression.value,
                ),
                (
                    "grpc.http2.min_recv_ping_interval_without_data_ms",
                    self._options.http2_min_recv_ping_interval_without_data_ms,
                ),
                ("grpc.http2.max_ping_strikes", self._options.max_ping_strikes),
            ]
            if self._options.max_connection_idle_ms is not None:
                grpc_options.append(
                    ("grpc.max_connection_idle_ms", self._options.max_connection_idle_ms)
                )
            if self._options.max_connection_age_ms is not None:
                grpc_options.append(
                    ("grpc.max_connection_age_ms", self._options.max_connection_age_ms)
                )
            if self._options.max_connection_age_grace_ms is not None:
                grpc_options.append(
                    (
                        "grpc.max_connection_age_grace_ms",
                        self._options.max_connection_age_grace_ms,
                    )
                )
            server = grpc.aio.server(
                interceptors=[VersionInterceptor()], options=grpc_options
            )
            credentials = await self._server_credentials()
            address = self._address(self._host, self._port)

            if credentials is not None:
                port = server.add_secure_port(address, credentials)
            else:
                port = server.add_insecure_port(address)

            uds_address = None
            uds_dir = None
            if _HAS_UDS:
                # Uid-confined, self-reclaiming socket dir — see this
                # method's implementation notes.
                uds_base = os.environ.get("XDG_RUNTIME_DIR") or "/tmp"
                if not os.path.isdir(uds_base):
                    uds_base = "/tmp"
                uds_dir = os.path.join(uds_base, f"wool-{self._uid}")
                if os.path.lexists(uds_dir):
                    if os.path.isdir(uds_dir) and not os.path.islink(uds_dir):
                        shutil.rmtree(uds_dir)
                    else:
                        os.unlink(uds_dir)
                os.mkdir(uds_dir, 0o700)
                uds_path = os.path.join(uds_dir, "dispatch.sock")
                server.add_insecure_port(f"unix:{uds_path}")
                uds_address = f"unix:{uds_path}"

            backpressure = (
                cloudpickle.loads(self._backpressure)
                if self._backpressure is not None
                else None
            )
            service = WorkerService(backpressure=backpressure)
            protocol.add_to_server[protocol.WorkerServicer](service, server)

            install_task_factory()

            _parent_watchdog(
                asyncio.get_running_loop(), service, self._shutdown_grace_period
            )

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
                        secure=self._provider is not None,
                        options=self._options.channel,
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
                        if uds_dir is not None:
                            with contextlib.suppress(OSError):
                                os.rmdir(uds_dir)

    def _address(self, host, port) -> str:
        """Format network address for the given host and port.

        :param host:
            Host address to include in the address.
        :param port:
            Port number to include in the address.
        :returns:
            Address string in "host:port" format.
        """
        return f"{host}:{port}"


def _parent_watchdog(
    loop: asyncio.AbstractEventLoop,
    service: WorkerService,
    grace: float,
) -> threading.Thread | None:
    """Tie the worker process's lifetime to its parent process.

    Starts a daemon thread that blocks until the parent process
    exits — including abrupt deaths such as SIGKILL, which bypass
    every parent-side teardown path — then initiates the same
    graceful shutdown as SIGTERM and, if the process is still alive
    once the grace window elapses, hard-exits via `os._exit`.
    Without this, a worker whose parent never completes the stop RPC
    reparents to init and accumulates as an orphan across runs.

    :param loop:
        The worker's running event loop.
    :param service:
        The `WorkerService` to stop when the parent dies.
    :param grace:
        Seconds to allow graceful shutdown after parent death before
        hard-exiting.
    :returns:
        The started watchdog thread, or ``None`` when the process
        was not spawned by `multiprocessing` (e.g., in-process
        test invocations of `WorkerProcess.run`).
    """
    parent = _mp.parent_process()
    if parent is None:
        return None

    def watch():
        parent.join()
        logger.warning("Parent process exited; shutting down worker")
        _schedule_stop(loop, service, timeout=0)
        time.sleep(grace)
        os._exit(1)

    thread = threading.Thread(target=watch, name="wool-parent-watchdog", daemon=True)
    thread.start()
    return thread


@contextmanager
def _signal_handlers(service: WorkerService):
    """Context manager for setting up signal handlers for graceful shutdown.

    Installs SIGTERM and SIGINT handlers that gracefully shut down the worker
    service when the process receives termination signals.

    :param service:
        The `WorkerService` instance to shut down on signal receipt.
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
    """Stop the service immediately, cancelling in-flight tasks."""
    _schedule_stop(loop, service, timeout=0)


def _sigint_handler(loop, service, signum, frame):
    """Stop the service, draining in-flight tasks indefinitely."""
    _schedule_stop(loop, service, timeout=-1)


def _schedule_stop(
    loop: asyncio.AbstractEventLoop,
    service: WorkerService,
    timeout: float,
) -> None:
    """Dispatch a graceful service stop onto the worker's event loop.

    Safe to call from any thread, including signal handlers and the
    parent-death watchdog. A ``timeout`` of ``0`` cancels in-flight
    tasks immediately; a negative value drains them indefinitely (see
    `WorkerService.stop`).

    :param loop:
        The worker's event loop.
    :param service:
        The `WorkerService` to stop.
    :param timeout:
        Drain bound forwarded in the ``StopRequest``.
    """
    if loop.is_running():
        # The loop can close between `is_running` and the dispatch; a
        # closed loop has nothing left to stop gracefully, so callers'
        # fallbacks (e.g., the watchdog's hard exit) must survive the
        # race.
        try:
            loop.call_soon_threadsafe(
                lambda: asyncio.create_task(
                    service.stop(protocol.StopRequest(timeout=timeout), None)
                )
            )
        except RuntimeError:
            pass


async def _proxy_factory(
    proxy: WorkerProxy,
):  # pragma: no cover — runs in worker subprocess; integration-tested
    """Factory function for WorkerProxy instances in ResourcePool.

    Calls ``enter()`` on the proxy.  Lazy proxies defer actual
    startup until first dispatch; non-lazy proxies start eagerly.
    The proxy object itself is used as the cache key.

    :param proxy:
        The WorkerProxy instance (passed as key from ResourcePool).
    :returns:
        The entered WorkerProxy instance.
    """
    await proxy.enter()
    return proxy


async def _proxy_finalizer(
    proxy: WorkerProxy,
):  # pragma: no cover — runs in worker subprocess; integration-tested
    """Finalizer function for WorkerProxy instances in ResourcePool.

    Exits the proxy context when it's being cleaned up from the
    resource pool.  Lazy proxies that were never started are handled
    gracefully by the proxy's own exit method.

    :param proxy:
        The WorkerProxy instance to clean up.
    """
    try:
        await proxy.exit()
    except Exception:
        pass
