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

from wool.runtime.loadbalancer.base import NoWorkersAvailable

from . import routines

_TIMEOUT = 30
_PROPAGATION_TIMEOUT = 15.0
# Long enough for the publish to traverse the rescan and the subscriber
# diff, so a sentinel that wrongly admitted the worker would have done
# so before the assertion reads the pool.
_SETTLE = 2.0


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


async def _poll_until_unavailable(message):
    """Dispatch get_pid until the pool has no workers left, or fail.

    The eviction counterpart of :func:`_poll_until`: an evicted worker
    is observable only as a dispatch that can no longer be routed.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + _PROPAGATION_TIMEOUT
    while True:
        try:
            await routines.get_pid()
        except NoWorkersAvailable:
            return
        assert loop.time() < deadline, message
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
                await _poll_until_unavailable(
                    "worker-dropped never evicted the refreshed entry"
                )

        await retry_grpc_internal(body)

    @pytest.mark.parametrize("worker_update_pool", [1], indirect=True)
    @pytest.mark.asyncio
    async def test_dispatch_should_raise_when_update_precedes_add(
        self, worker_update_pool, retry_grpc_internal
    ):
        """Test a worker-updated event never admits an unadmitted worker.

        Given:
            A pool over a real LocalDiscovery with lease=1, holding its
            one announced worker, and a second announced worker the cap
            rejected, after which the admitted worker is dropped and the
            pool is empty
        When:
            The publisher publishes worker-updated for the rejected
            worker's uid
        Then:
            It should leave the pool empty — dispatch keeps raising
            NoWorkersAvailable, because a refresh never admits a worker
        """
        _, publisher, worker_a, worker_b = worker_update_pool

        async def body():
            async with asyncio.timeout(_TIMEOUT):
                # Arrange — discovery's registrar refuses to publish
                # worker-updated for a uid it has never registered, so
                # the only worker a refresh can name that the pool does
                # not hold is one the pool never admitted: one the lease
                # cap rejected. Dispatch once so the lazy pool starts and
                # admits worker_a against the cap, announce worker_b so
                # the cap rejects it, then retire worker_a so only an
                # admission could repopulate the pool.
                assert await routines.get_pid() == worker_a.metadata.pid
                await publisher.publish("worker-added", worker_b.metadata)
                await publisher.publish("worker-dropped", worker_a.metadata)
                await _poll_until_unavailable(
                    "the cap-rejected worker was admitted, or worker_a was never evicted"
                )

                # Act
                await publisher.publish("worker-updated", worker_b.metadata)
                await asyncio.sleep(_SETTLE)

                # Assert
                with pytest.raises(NoWorkersAvailable):
                    await routines.get_pid()

        await retry_grpc_internal(body)
