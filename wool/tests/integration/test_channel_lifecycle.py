"""Channel-pool lifecycle integration tests.

These pin the symptom the channel-pool hold exists to remove: a channel
left unclosed when the loop that opened it stops. They are targeted
standalone tests rather than pairwise scenarios: the pairwise array's
oracle is "the dispatch returned the expected value", and every claim
here is instead about what the channel pool holds *after* a dispatch has
already succeeded, e.g., an entry count, a rebind warning, or a channel
closed on one loop and not another. Those oracles are invisible to the
array, and the arrangements they need (a pool run to completion on a
loop that then stops, a worker whose proxy pool retires mid-test) are
not dimensions any pairwise row can carry without breaking the single
dispatch-success oracle the array is built around.

"""

import asyncio
import gc
import logging
import uuid

import pytest

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.connection import channel_pool_stats
from wool.runtime.worker.connection import clear_channel_pool
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.pool import WorkerPool
from wool.runtime.worker.proxy import WorkerProxy

from . import routines
from .conftest import PoolMode
from .conftest import RoutineShape
from .conftest import _DirectDiscovery
from .conftest import build_pool_from_scenario
from .conftest import default_scenario
from .conftest import invoke_routine
from .conftest import poll_until
from .conftest import poll_until_channel_pool_settles
from .conftest import run_on_foreign_loop

#: The logger a `~wool.runtime.resourcepool.ResourcePool` reports a
#: dropped entry on. A stranded-entry claim is only meaningful against
#: records from this logger, so every assertion here filters by it.
_RESOURCEPOOL_LOGGER = "wool.runtime.resourcepool"

#: Seconds a spawned worker keeps a nested-dispatch proxy cached, for
#: tests that observe the proxy's retirement. Short enough that the
#: retirement is observable inside one test, long enough that the warm
#: probe still finds the proxy alive.
_PROXY_POOL_TTL = 1.0


def _resourcepool_records(caplog):
    """Return the records logged on the resource pool's logger."""
    return [record for record in caplog.records if record.name == _RESOURCEPOOL_LOGGER]


@pytest.mark.integration
class TestChannelPoolLifecycle:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pool_mode",
        [PoolMode.DEFAULT, PoolMode.EPHEMERAL],
        ids=lambda mode: mode.name,
    )
    async def test___aexit___should_close_pooled_channels_when_last_pool_exits(
        self, pool_mode, credentials_map, retry_grpc_internal
    ):
        """Test a pool's exit closes the channels its dispatches opened.

        Given:
            A one-worker or two-worker pool that has dispatched a
            coroutine routine, leaving a channel cached on the caller's
            loop.
        When:
            The pool's context is exited.
        Then:
            It should leave the channel pool caching nothing. A pool
            that spawns its own workers closes their channels through
            the stop RPC either way, so this pins the symptom end to end
            rather than the hold alone.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE, pool_mode=pool_mode
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                await invoke_routine(scenario)
                # Guards the assertion below against passing vacuously
                # on a pool that never opened a channel at all.
                assert channel_pool_stats().total_entries >= 1

            # Assert
            await poll_until_channel_pool_settles()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test___aexit___should_close_pooled_channels_when_worker_outlives_pool(
        self, started_worker, retry_grpc_internal
    ):
        """Test a pool closes its channels even when it stops no worker.

        Given:
            A worker started outside the pool and published to a private
            discovery namespace, and a pool that discovers it, dispatches
            a coroutine, and leaves it running on exit.
        When:
            The pool's context is exited.
        Then:
            It should leave the channel pool caching nothing, closing the
            dispatch channel through the proxy's own hold rather than
            through the stop RPC a pool-owned worker's shutdown would
            have sent over the same pool key.
        """
        # Arrange
        worker = await started_worker(LocalWorker())
        namespace = f"channel-lifecycle-{uuid.uuid4().hex[:12]}"

        async def body():
            # Arrange
            with LocalDiscovery(namespace) as discovery:
                async with discovery.publisher as publisher:
                    await publisher.publish("worker-added", worker.metadata)

                    # Act
                    async with WorkerPool(
                        discovery=_DirectDiscovery(discovery),
                        loadbalancer=RoundRobinLoadBalancer,
                    ):
                        assert await routines.add(1, 2) == 3
                        assert channel_pool_stats().total_entries >= 1

                    # Assert
                    await poll_until_channel_pool_settles()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test___aexit___should_strand_no_channel_when_loop_stops_after_exit(
        self, credentials_map, caplog
    ):
        """Test a pool that closed its channels strands nothing at loop stop.

        Given:
            A full pool lifecycle — build, dispatch, exit — run to
            completion under ``asyncio.run`` on another thread, whose
            loop then stops.
        When:
            This test's loop makes its first channel-pool operation,
            which is what rebinds the pool and drops what the stopped
            loop left.
        Then:
            It should drop nothing, reporting no record at all on the
            resource pool's logger.
        """

        async def body():
            # Arrange
            caplog.clear()
            scenario = default_scenario(pool_mode=PoolMode.DEFAULT)

            async def lifecycle():
                async with build_pool_from_scenario(scenario, credentials_map):
                    await invoke_routine(scenario)
                settled = await poll_until_channel_pool_settles()
                return settled.total_entries

            # Act
            with caplog.at_level(logging.WARNING, logger=_RESOURCEPOOL_LOGGER):
                stranded = await run_on_foreign_loop(lifecycle)
                # Deliberately this loop's first channel-pool operation —
                # that is when the rebind runs (see `ResourcePool`).
                await clear_channel_pool()

            # Assert
            assert stranded == 0
            assert channel_pool_stats().total_entries == 0
            assert _resourcepool_records(caplog) == []

        await body()

    @pytest.mark.asyncio
    async def test_clear_channel_pool_should_warn_when_loop_stops_without_a_proxy(
        self, started_worker, caplog
    ):
        """Test a channel nobody closed is reported when its loop stops.

        Given:
            A running worker polled for its idle duration by a
            `wool.runtime.worker.connection.WorkerConnection` on another
            thread's loop that is never entered and never closed before
            that loop stops.
        When:
            This test's loop makes its first channel-pool operation,
            rebinding the pool off the stopped loop.
        Then:
            It should report exactly one warning naming the one idle
            entry it dropped without finalizing.
        """
        # Arrange
        worker = await started_worker(LocalWorker())

        async def strand():
            connection = WorkerConnection(worker.address)
            # Deliberately never closed: this is the control case the
            # proxy-scoped tests are measured against.
            await connection.idle()

        async def body():
            # Arrange
            caplog.clear()

            # Act
            with caplog.at_level(logging.WARNING, logger=_RESOURCEPOOL_LOGGER):
                await run_on_foreign_loop(strand)
                await clear_channel_pool()

            # Assert
            records = _resourcepool_records(caplog)
            assert len(records) == 1
            assert records[0].levelno == logging.WARNING
            assert "0 referenced and 1 idle" in records[0].getMessage()

        await body()

    @pytest.mark.asyncio
    async def test___aexit___should_strand_no_channel_when_connection_used_as_context(
        self, started_worker, caplog
    ):
        """Test a connection used as a context manager cleans up after itself.

        Given:
            A running worker polled for its idle duration by a
            `wool.runtime.worker.connection.WorkerConnection` entered as
            an async context manager on another thread's loop, which
            stops once the block exits.
        When:
            This test's loop makes its first channel-pool operation,
            rebinding the pool off the stopped loop.
        Then:
            It should report nothing on the resource pool's logger,
            since exiting the connection retired the channel it opened.
        """
        # Arrange
        worker = await started_worker(LocalWorker())

        async def use_and_exit():
            async with WorkerConnection(worker.address) as connection:
                await connection.idle()

        async def body():
            # Arrange
            caplog.clear()

            # Act
            with caplog.at_level(logging.WARNING, logger=_RESOURCEPOOL_LOGGER):
                await run_on_foreign_loop(use_and_exit)
                await clear_channel_pool()

            # Assert
            assert _resourcepool_records(caplog) == []

        await body()

    @pytest.mark.asyncio
    async def test___aexit___should_close_channels_when_proxy_loop_stops_at_once(
        self, started_worker, caplog
    ):
        """Test a proxy over an external worker strands no channel.

        Given:
            A running worker dispatched one coroutine through a static
            `wool.WorkerProxy` entered on another thread's loop, which
            stops the instant the proxy's context exits.
        When:
            This test's loop makes its first channel-pool operation,
            rebinding the pool off the stopped loop.
        Then:
            It should drop nothing, and the stats read inside the proxy
            should show the completed dispatch's channel idle rather
            than still referenced.
        """
        # Arrange
        worker = await started_worker(LocalWorker())

        async def dispatch_under_proxy():
            async with WorkerProxy(workers=[worker.metadata]):
                assert await routines.add(1, 2) == 3
                stats = channel_pool_stats()
            return stats.total_entries, stats.referenced_entries

        async def body():
            # Arrange
            caplog.clear()

            # Act
            with caplog.at_level(logging.WARNING, logger=_RESOURCEPOOL_LOGGER):
                total, referenced = await run_on_foreign_loop(dispatch_under_proxy)
                # Deliberately this loop's first channel-pool operation —
                # that is when the rebind runs (see `ResourcePool`).
                await clear_channel_pool()

            # Assert
            assert total == 1
            assert referenced == 0
            assert _resourcepool_records(caplog) == []

        await body()

    @pytest.mark.asyncio
    async def test___aexit___should_keep_outer_pool_channels_when_inner_pool_exits(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an inner pool's exit retires only its own channels.

        Given:
            An outer pool that has dispatched once, leaving one idle
            channel, with a second pool entered and dispatched through
            inside it on the same loop.
        When:
            The inner pool's context exits while the outer pool's stays
            open.
        Then:
            It should drop only the inner pool's entry, leave the outer
            pool's cached with its idle timer, keep the outer pool
            dispatchable, and settle to nothing once the outer pool
            exits too.
        """

        async def body():
            # Arrange
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                assert await routines.add(1, 2) == 3
                outer_only = channel_pool_stats()

                # Act
                async with WorkerPool(spawn=1):
                    assert await routines.add(3, 4) == 7
                    nested = channel_pool_stats()
                after_inner = channel_pool_stats()
                # The dispatchability probe has to run while the outer
                # pool is still open, so it stays inside the block.
                assert await routines.add(5, 6) == 11

            # Assert
            assert outer_only.total_entries == 1
            assert nested.total_entries == outer_only.total_entries + 1
            assert after_inner.total_entries == outer_only.total_entries
            assert after_inner.referenced_entries == 0
            assert after_inner.pending_cleanup == 1
            await poll_until_channel_pool_settles()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_stop_should_close_worker_channels_when_pooled_proxy_retires(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker closes its nested-dispatch channel with its proxy.

        Given:
            A single-worker pool whose worker caches nested-dispatch
            proxies for one second, warmed by a nested coroutine
            dispatch that opens a channel on the worker's task loop.
        When:
            The worker's channel pool counters are read straight after
            the nested dispatch and again once the proxy's idle TTL has
            elapsed several times over.
        Then:
            It should report one idle channel awaiting cleanup while the
            proxy is warm and nothing at all once the proxy has retired.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.NESTED_COROUTINE, pool_mode=PoolMode.DEFAULT
            )

            # Act
            async with build_pool_from_scenario(
                scenario, credentials_map, proxy_pool_ttl=_PROXY_POOL_TTL
            ):
                assert await routines.nested_add(1, 2) == 3
                warm = await routines.worker_channel_pool_stats()
                # Every probe is itself a task on the worker, which
                # re-acquires the cached proxy and restarts its idle
                # TTL, so the interval must outlast the TTL or the
                # proxy never idles long enough to retire.
                retired = await poll_until(
                    routines.worker_channel_pool_stats,
                    lambda stats: stats.total_entries == 0,
                    describe="worker channel pool never retired its proxy's channel",
                    timeout=_PROXY_POOL_TTL * 8,
                    interval=_PROXY_POOL_TTL * 2,
                )

            # Assert
            assert warm.total_entries == 1
            assert warm.referenced_entries == 0
            assert warm.pending_cleanup == 1
            assert retired.total_entries == 0
            assert retired.referenced_entries == 0
            assert retired.pending_cleanup == 0

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test___aexit___should_settle_channel_pool_when_release_lands_late(
        self, credentials_map, retry_grpc_internal, caplog, recwarn, tmp_path
    ):
        """Test a release landing during pool exit finalizes cleanly.

        Given:
            An async-generator dispatch whose consumer task is cancelled
            and left unawaited, so its teardown release of the pooled
            channel lands while the pool's exit is rebuilding a channel
            for the same key to send each worker its stop RPC.
        When:
            The pool exits and the cancelled task is awaited afterwards.
        Then:
            It should raise `asyncio.CancelledError`, run the
            worker-side ``finally``, settle the channel pool to nothing
            without a dropped-entry record, and finalize inline rather
            than leaving an unawaited-coroutine warning behind.
        """

        async def body():
            # Arrange
            caplog.clear()
            recwarn.clear()
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ACLOSE,
                pool_mode=PoolMode.EPHEMERAL,
            )
            sentinel = tmp_path / "cleanup_reason.txt"
            collected = []
            started = asyncio.Event()

            async def consume():
                gen = routines.cancellable_gen(str(sentinel))
                collected.append(await gen.__anext__())
                started.set()
                # Park awaiting the next value; the cancellation lands
                # here, and its teardown release chases the pool's exit.
                collected.append(await gen.__anext__())

            # Act
            with caplog.at_level(logging.WARNING, logger=_RESOURCEPOOL_LOGGER):
                async with build_pool_from_scenario(scenario, credentials_map):
                    task = asyncio.create_task(consume())
                    await asyncio.wait_for(started.wait(), timeout=15)
                    task.cancel()
                    # Deliberately not awaited before the exit: that is
                    # what makes the release land mid-teardown.

                # A ValueError here would be the entry the stop RPC
                # rebuilt being corrupted by the late release.
                with pytest.raises(asyncio.CancelledError):
                    await task

                # Poll for the worker's ``finally`` to write the
                # sentinel — it runs after the gRPC stream tears down
                # and tolerates CI load.
                await poll_until(
                    lambda: sentinel.exists() and sentinel.read_text() == "cleaned_up",
                    bool,
                    describe="sentinel never written",
                    timeout=15.0,
                    interval=0.1,
                )

                settled = await poll_until_channel_pool_settles()

            # Assert
            assert collected == ["alive"]
            assert sentinel.read_text() == "cleaned_up"
            assert settled.total_entries == 0
            assert _resourcepool_records(caplog) == []
            # An orphaned coroutine only warns when it is destroyed, so
            # collect before reading the warnings.
            gc.collect()
            assert [w for w in recwarn if issubclass(w.category, RuntimeWarning)] == []

        await retry_grpc_internal(body)
