import asyncio
import contextlib
import uuid
from contextlib import asynccontextmanager

import grpc
import grpc.aio
import pytest

from wool import protocol
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.connection import IdleUnavailable
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.pool import WorkerPool

from . import routines
from .conftest import CredentialType
from .conftest import _DirectDiscovery

pytestmark = pytest.mark.integration


def _client_credentials(creds):
    """Resolve WorkerCredentials to the client-side channel credentials.

    Mirrors ``LocalWorker._stop``: secure workers use their client
    credentials, insecure workers use ``None``.
    """
    return creds.client_credentials() if creds is not None else None


async def _poll(predicate, *, tries=600, interval=0.05):
    """Await until the sync *predicate* returns truthy, or fail."""
    for _ in range(tries):
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


async def _poll_coro(predicate, *, tries=200, interval=0.05):
    """Await until the async *predicate* returns truthy, or fail."""
    for _ in range(tries):
        if await predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


@asynccontextmanager
async def _bare_worker(creds):
    """Start a real LocalWorker and yield it plus a direct-connection
    factory. No pool — for tests that only poll idle/stop by address.
    """
    conns = []
    worker = LocalWorker(credentials=creds)
    await worker.start()

    def connect():
        conn = WorkerConnection(worker.address, credentials=_client_credentials(creds))
        conns.append(conn)
        return conn

    try:
        yield worker, connect
    finally:
        for conn in conns:
            with contextlib.suppress(Exception):
                await conn.close()
        with contextlib.suppress(Exception):
            await worker.stop()


@asynccontextmanager
async def _worker_with_pool(creds):
    """Start a real LocalWorker behind a DURABLE pool (so routines can
    be dispatched to it) and yield the worker, the pool, and a
    direct-connection factory. Teardown is defensive.
    """
    conns = []
    namespace = f"idle-{uuid.uuid4().hex[:12]}"
    with LocalDiscovery(namespace) as discovery:
        worker = LocalWorker(credentials=creds)
        await worker.start()

        def connect():
            conn = WorkerConnection(
                worker.address, credentials=_client_credentials(creds)
            )
            conns.append(conn)
            return conn

        try:
            publisher = discovery.publisher
            async with publisher:
                await publisher.publish("worker-added", worker.metadata)
                try:
                    pool = WorkerPool(
                        discovery=_DirectDiscovery(discovery), credentials=creds
                    )
                    async with pool:
                        yield worker, pool, connect
                finally:
                    with contextlib.suppress(Exception):
                        await publisher.publish("worker-dropped", worker.metadata)
        finally:
            for conn in conns:
                with contextlib.suppress(Exception):
                    await conn.close()
            with contextlib.suppress(Exception):
                await worker.stop()


class TestWorkerIdleReporting:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "cred",
        [CredentialType.INSECURE, CredentialType.MTLS, CredentialType.ONE_WAY],
    )
    async def test_idle_time_should_accrue_from_startup_over_the_real_wire(
        self, credentials_map, retry_grpc_internal, cred
    ):
        """Test idle accrues from startup, over each transport.

        Given:
            A freshly started real worker that has never been dispatched
            a task, reached over an insecure, mTLS, or one-way-TLS
            channel
        When:
            A direct WorkerConnection polls idle twice with a wait
            between
        Then:
            Both readings should be positive and non-decreasing — idle
            counts from startup and inherits the connection's
            credential handling.
        """

        async def body():
            # Arrange
            async with _bare_worker(credentials_map[cred]) as (worker, connect):
                conn = connect()

                # Act
                first = await conn.idle_time()
                await asyncio.sleep(0.1)
                second = await conn.idle_time()

                # Assert
                assert first >= 0.0
                assert second >= first
                assert second > 0.0

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_idle_time_should_report_zero_while_a_task_is_in_flight(
        self, credentials_map, retry_grpc_internal, tmp_path
    ):
        """Test idle is zero while a dispatched routine runs on the worker.

        Given:
            A real worker with one in-flight routine (confirmed via its
            "started" sentinel)
        When:
            idle is polled through a direct WorkerConnection
        Then:
            It should report exactly zero — the docket is non-empty.
        """

        async def body():
            async with _worker_with_pool(credentials_map[CredentialType.INSECURE]) as (
                worker,
                pool,
                connect,
            ):
                conn = connect()
                sentinel = tmp_path / "in-flight.txt"
                dispatch = asyncio.create_task(
                    routines.cancellable_sleep(str(sentinel), 30.0)
                )
                try:
                    # Arrange
                    await _poll(
                        lambda: sentinel.exists() and sentinel.read_text() == "started"
                    )

                    # Act & assert
                    assert await conn.idle_time() == 0.0
                finally:
                    dispatch.cancel()
                    with contextlib.suppress(BaseException):
                        await dispatch

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_idle_time_should_reset_after_the_in_flight_set_drains(
        self, credentials_map, retry_grpc_internal, tmp_path
    ):
        """Test idle resets once the worker's in-flight set drains.

        Given:
            A real worker that has accrued idle, then runs one routine
            to completion
        When:
            idle is polled before dispatch and after the routine drains
        Then:
            It should report accrued idle before, and a smaller value
            after — proving the count reset at the drain rather than
            continuing to accrue from startup.
        """

        async def body():
            async with _worker_with_pool(credentials_map[CredentialType.INSECURE]) as (
                worker,
                pool,
                connect,
            ):
                conn = connect()
                await asyncio.sleep(0.2)
                before = await conn.idle_time()

                # Act
                sentinel = tmp_path / "drain.txt"
                await routines.cancellable_sleep(str(sentinel), 0.3)

                # Wait for the docket-drain to be reflected (idle > 0),
                # then confirm it counts from the drain, not from startup.
                async def _reset():
                    return await conn.idle_time() > 0.0

                await _poll_coro(_reset)
                after = await conn.idle_time()

                # Assert
                assert before > 0.0
                assert after < before

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_idle_time_should_raise_idle_unavailable_for_a_legacy_worker(
        self, retry_grpc_internal
    ):
        """Test idle surfaces IdleUnavailable against a legacy worker.

        Given:
            A real gRPC server whose Worker servicer does not implement
            idle (a stand-in for a worker predating the RPC), so the
            framework answers UNIMPLEMENTED
        When:
            A real WorkerConnection polls idle against it
        Then:
            It should raise IdleUnavailable — and not an RpcError — over
            the real channel.
        """

        async def body():
            # Arrange
            server = grpc.aio.server()
            protocol.add_WorkerServicer_to_server(protocol.WorkerServicer(), server)
            port = server.add_insecure_port("127.0.0.1:0")
            await server.start()
            conn = WorkerConnection(f"127.0.0.1:{port}")
            try:
                # Act & assert
                with pytest.raises(IdleUnavailable) as exc_info:
                    await conn.idle_time()
                assert not isinstance(exc_info.value, RpcError)
            finally:
                await conn.close()
                await server.stop(None)

        await retry_grpc_internal(body)


class TestWorkerControlSurface:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("cred", [CredentialType.INSECURE, CredentialType.MTLS])
    async def test_poll_then_retire_should_terminate_the_worker(
        self, credentials_map, retry_grpc_internal, cred
    ):
        """Test the poll-then-retire flow stops the worker.

        Given:
            An idle real worker and a direct WorkerConnection to it
        When:
            The caller polls idle, then calls stop, then polls idle again
        Then:
            stop returns and the worker terminates — a subsequent idle
            eventually raises TransientRpcError (the server is gone).
        """

        async def body():
            # Arrange
            async with _bare_worker(credentials_map[cred]) as (worker, connect):
                conn = connect()
                assert await conn.idle_time() >= 0.0

                # Act
                await conn.stop()

                # Assert
                async def _unreachable():
                    try:
                        await conn.idle_time()
                        return False
                    except TransientRpcError:
                        return True

                await _poll_coro(_unreachable, tries=200, interval=0.1)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "cred",
        [CredentialType.INSECURE, CredentialType.MTLS, CredentialType.ONE_WAY],
    )
    async def test_local_worker_stop_should_succeed_over_each_transport(
        self, credentials_map, retry_grpc_internal, cred
    ):
        """Test LocalWorker.stop succeeds through the refactored path.

        Given:
            A started real LocalWorker under each credential type
        When:
            worker.stop is awaited (now routed through
            WorkerConnection.stop and close)
        Then:
            It should complete without error, exercising the refactored
            _stop's credential and secure-channel handling end-to-end.
        """

        async def body():
            # Arrange
            worker = LocalWorker(credentials=credentials_map[cred])
            await worker.start()

            # Act & assert (completes without raising)
            await worker.stop()

        await retry_grpc_internal(body)
