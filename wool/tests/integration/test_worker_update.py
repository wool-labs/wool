"""End-to-end tests for live worker-metadata updates (#292).

These are targeted standalone tests rather than pairwise scenarios: the
scenario matrix's dimensions are static composition axes resolved once
at build time, and worker metadata never changes mid-scenario there —
the update flow is a temporal lifecycle behavior against one live pool.
The full pipeline is proved with real components: a ``LocalDiscovery``
publish flows through the shared-memory rescan, the subscriber diff,
the proxy's worker sentinel, and the uid-keyed load-balancer context
into real gRPC dispatch. LAN discovery is deliberately not exercised:
its updates never re-resolve the advertised address, so a connection
refresh to a new address cannot occur on that transport.
"""

import asyncio
from dataclasses import replace

import pytest

from . import routines
from .conftest import poll_dispatch_until_pid
from .conftest import poll_dispatch_until_unavailable

_TIMEOUT = 30
_PROPAGATION_TIMEOUT = 15.0


async def _poll_until(predicate, message):
    """Dispatch get_pid until the predicate holds, or fail.

    Discovery propagation is asynchronous, so the pool observes an
    event some bounded time after it is published; dispatching is the
    only public window onto which worker is pooled.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + _PROPAGATION_TIMEOUT
    while not predicate(pid := await routines.get_pid()):
        assert loop.time() < deadline, f"dispatch still reaches pid {pid}; {message}"
        await asyncio.sleep(0.1)


@pytest.mark.integration
class TestWorkerUpdatePropagation:
    @pytest.mark.asyncio
    async def test_dispatch_should_follow_update_when_metadata_changes(
        self, worker_update_pool, retry_grpc_internal
    ):
        """Test a worker-updated event redirects dispatch end to end.

        Given:
            A pool over a real LocalDiscovery with one announced worker
            and a second live but unannounced worker
        When:
            The publisher publishes worker-updated carrying the
            announced worker's uid with the unannounced worker's
            address and pid
        Then:
            It should redirect dispatch to the second worker within a
            bounded deadline
        """
        _, publisher, worker_a, worker_b = worker_update_pool

        async def body():
            async with asyncio.timeout(_TIMEOUT):
                # Arrange
                assert await routines.get_pid() == worker_a.metadata.pid
                updated = replace(worker_b.metadata, uid=worker_a.metadata.uid)

                # Act
                await publisher.publish("worker-updated", updated)

                # Assert
                await _poll_until(
                    lambda pid: pid == worker_b.metadata.pid,
                    "the worker-updated event never propagated",
                )

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_keep_routing_when_stale_worker_stops(
        self, worker_update_pool, retry_grpc_internal
    ):
        """Test the redirected pool holds no reference to the old worker.

        Given:
            A pool whose sole worker's metadata was refreshed onto a
            second live worker's address, with dispatch already
            observed to follow the refresh
        When:
            The original worker's process is stopped
        Then:
            It should keep dispatching to the second worker — the
            refresh replaced the pooled connection rather than adding
            to it
        """
        _, publisher, worker_a, worker_b = worker_update_pool

        async def body():
            async with asyncio.timeout(_TIMEOUT):
                # Arrange
                updated = replace(worker_b.metadata, uid=worker_a.metadata.uid)
                await publisher.publish("worker-updated", updated)
                await _poll_until(
                    lambda pid: pid == worker_b.metadata.pid,
                    "the worker-updated event never propagated",
                )

                # Act
                await worker_a.stop()

                # Assert
                assert await routines.get_pid() == worker_b.metadata.pid

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_raise_when_worker_dropped_after_update(
        self, worker_update_pool, retry_grpc_internal
    ):
        """Test a drop following an applied update evicts the worker.

        Given:
            A pool over a real LocalDiscovery whose sole worker's
            metadata was refreshed by an applied worker-updated event,
            with both worker processes alive throughout
        When:
            The publisher publishes worker-dropped for that uid
        Then:
            It should evict the worker — dispatch raises
            NoWorkersAvailable within a bounded deadline instead of a
            ghost entry serving dispatches forever
        """
        _, publisher, worker_a, worker_b = worker_update_pool

        async def body():
            async with asyncio.timeout(_TIMEOUT):
                # Arrange
                updated = replace(worker_b.metadata, uid=worker_a.metadata.uid)
                await publisher.publish("worker-updated", updated)
                await _poll_until(
                    lambda pid: pid == worker_b.metadata.pid,
                    "the worker-updated event never propagated",
                )

                # Act
                await publisher.publish("worker-dropped", updated)

                # Assert
                await poll_dispatch_until_unavailable(
                    "worker-dropped never evicted the refreshed entry"
                )

        await retry_grpc_internal(body)

    @pytest.mark.parametrize("worker_update_pool", [1], indirect=True)
    @pytest.mark.asyncio
    async def test_dispatch_should_admit_capacity_freed_worker_on_refresh(
        self, worker_update_pool, retry_grpc_internal
    ):
        """Test a worker refused for capacity is admitted once a slot frees.

        Given:
            A pool over a real LocalDiscovery with lease=1, holding its
            one announced worker, and a second announced worker the cap
            rejected
        When:
            The admitted worker is dropped, freeing the lease slot, so
            discovery's next scan re-publishes the rejected worker as a
            worker-updated event
        Then:
            It should admit that worker into the freed slot — dispatch
            reaches it within a bounded deadline, because the sentinel
            reconciles membership on every event rather than admitting
            only on worker-added
        """
        _, publisher, worker_a, worker_b = worker_update_pool

        async def body():
            async with asyncio.timeout(_TIMEOUT):
                # Arrange — dispatch once so the lazy pool starts and
                # admits worker_a against the cap, then announce worker_b
                # so the cap rejects it.
                assert await routines.get_pid() == worker_a.metadata.pid
                await publisher.publish("worker-added", worker_b.metadata)

                # Act — retire worker_a, freeing its lease slot; the same
                # scan that drops it re-publishes worker_b as an update.
                await publisher.publish("worker-dropped", worker_a.metadata)

                # Assert
                await poll_dispatch_until_pid(
                    worker_b.metadata.pid,
                    "the capacity-freed worker was never admitted on refresh",
                )

        await retry_grpc_internal(body)
