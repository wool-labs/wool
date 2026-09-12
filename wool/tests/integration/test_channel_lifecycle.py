"""Channel-pool lifecycle integration tests.

These pin the symptom the channel-pool hold exists to remove: a channel
left unclosed when the loop that opened it stops. Their second subject
is partition isolation: the pool serves every running loop at once,
each through a partition only that loop can reach, so a lifecycle run
on one loop must leave every other loop's channels — a worker's own
included — exactly as it found them. They stand apart from the pairwise
array because the oracle here is what the pool holds after a successful
dispatch, which the array's dispatch-success oracle cannot express.

`TestChannelPoolLifecycle` is named for the behavior it pins rather
than for a class under test, under the test guide's exception for a
behavior-pinning suite: the subject is a process-wide pool reached
through module functions, not one class.
"""

import asyncio
import inspect
import logging
import threading
import uuid
from contextlib import AsyncExitStack

import pytest

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.connection import channel_pool_hold
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

#: Seconds to wait for a barrier the other thread may never reach, so a
#: failure in one concurrent loop surfaces as a broken barrier rather
#: than a hung test.
_BARRIER_TIMEOUT = 60.0


#: The name a `~wool.runtime.resourcepool.ResourcePool` reports the
#: channel pool under: its factory's. The hold pool shares the logger, so
#: a stranded-channel claim filters on this name as well.
_CHANNEL_POOL = "_channel_factory"

#: The name the holds behind `channel_pool_hold` are reported under.
#: Holds live in their own pool with their own registry, so what a
#: stopped loop stranded there is a separate record from its channels'.
_HOLD_POOL = "_channel_pool_hold_factory"


def _resourcepool_records(caplog, *, pool=_CHANNEL_POOL):
    """Return the records the resource pool's logger holds for ``pool``."""
    return [
        record
        for record in caplog.records
        if record.name == _RESOURCEPOOL_LOGGER
        and record.getMessage().startswith(f"ResourcePool({pool})")
    ]


async def _records_after_stranding(
    caplog, factory, *, trigger=clear_channel_pool, sweep_holds=False
):
    """Run ``factory()`` on a foreign loop and report what its sweep drops.

    Returns the coroutine's result and the channel pool's records from
    this loop's next mutating channel-pool operation afterwards, which
    is what sweeps the closed loop's partition and drops whatever it
    left (see `wool.runtime.resourcepool.ResourcePool`). ``trigger`` is
    that operation, `clear_channel_pool` by default; a caller holding
    channels of its own passes a dispatch or an ``idle`` call instead,
    since a read sweeps nothing and a clear would close what the caller
    is measuring. A sync or async trigger is equally accepted.

    The holds keep their own pool and their own registry, so a hold a
    stopped loop stranded is reported only once a live loop enters
    `channel_pool_hold`: ``sweep_holds`` takes and releases one before
    the trigger for a caller that means to observe it. That release also
    retires this loop's own channels, so a caller with channels to keep
    leaves it alone. Only the channel pool's records are returned; a
    caller reading the holds pool's asks `_resourcepool_records` for
    them.
    """
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=_RESOURCEPOOL_LOGGER):
        result = await run_on_foreign_loop(factory)
        if sweep_holds:
            async with channel_pool_hold():
                pass
        # Deliberately this loop's next mutating channel-pool operation
        # — that is when the sweep runs.
        swept = trigger()
        if inspect.isawaitable(swept):
            await swept
    return result, _resourcepool_records(caplog)


def _concurrent_lifecycle_probe(scenario, credentials_map, barrier):
    """Build a coroutine factory that runs one pool lifecycle and reports.

    The returned factory is meant for `run_on_foreign_loop`. Its
    coroutine dispatches through a pool, waits on *barrier* so both
    loops hold their channels at the same instant, reads the channel
    pool's total while still inside the pool, exits, and reports
    ``(live_total, settled_total)``.
    """

    async def probe():
        async with build_pool_from_scenario(scenario, credentials_map):
            assert await routines.add(1, 2) == 3
            # Block on the barrier off-loop: both lifecycles must be
            # holding their channels when the live total is read, or
            # the isolation claim is untested.
            await asyncio.to_thread(barrier.wait, _BARRIER_TIMEOUT)
            live = channel_pool_stats().total_entries
        settled = await poll_until_channel_pool_settles()
        return live, settled.total_entries

    return probe


def _concurrent_proxy_probe(metadata, barrier):
    """Build a coroutine factory that runs one proxy lifecycle and reports.

    Shaped like `_concurrent_lifecycle_probe`, but over a static
    `wool.WorkerProxy` on a worker started outside it, so every loop
    dials the same worker under the same pool key. The coroutine
    dispatches, waits on *barrier* so both loops hold their channel at
    the same instant, reads the total while still inside the proxy,
    exits, and reports ``(pid, live_total, settled_total)``.
    """

    async def probe():
        async with WorkerProxy(workers=[metadata]):
            pid = await routines.get_pid()
            # Block on the barrier off-loop, for the reason
            # `_concurrent_lifecycle_probe` gives.
            await asyncio.to_thread(barrier.wait, _BARRIER_TIMEOUT)
            live = channel_pool_stats().total_entries
        settled = await poll_until_channel_pool_settles()
        return pid, live, settled.total_entries

    return probe


async def _gather_probes(barrier, *factories):
    """Run each factory on its own foreign loop and report their results.

    Gathers with ``return_exceptions=True`` so a probe that fails does
    not leave its peer running detached into the next test, aborts
    *barrier* in a ``finally`` so no thread is left waiting on it once
    the gather returns, and re-raises the exception a probe reported so
    the failure is the test's. A failing probe aborts the barrier on its
    way out too, releasing a peer parked on a rendezvous it will now
    never reach rather than leaving it there for the barrier's own
    timeout; the failure re-raised is the probe's own, not the broken
    barrier its peer reports as a consequence, so a transient gRPC
    error in either probe is still the one the retry fixture sees.
    """

    async def probe(factory):
        try:
            return await run_on_foreign_loop(factory)
        except BaseException:
            barrier.abort()
            raise

    try:
        results = await asyncio.gather(
            *(probe(factory) for factory in factories), return_exceptions=True
        )
    finally:
        barrier.abort()
    errors = [result for result in results if isinstance(result, BaseException)]
    if errors:
        raise next(
            (e for e in errors if not isinstance(e, threading.BrokenBarrierError)),
            errors[0],
        )
    return results


@pytest.mark.integration
class TestChannelPoolLifecycle:
    @pytest.mark.asyncio
    async def test_poll_until_channel_pool_settles_should_fail_when_channel_cached(
        self, started_worker
    ):
        """Test the settle helper fails on a channel nobody closed.

        Given:
            A running worker polled for its idle duration by a
            `wool.runtime.worker.connection.WorkerConnection` entered
            nowhere and never closed, so this loop keeps one channel
            cached.
        When:
            The channel pool is polled for a settle within half a
            second.
        Then:
            It should fail, naming the settle it never saw, and settle
            once the connection is closed — every claim in this file
            rests on that helper, and a gate that cannot fail is not a
            gate.
        """
        # Arrange
        worker = await started_worker(LocalWorker())
        connection = WorkerConnection(worker.address)
        await connection.idle()

        try:
            # Act & assert
            with pytest.raises(AssertionError, match="never settled"):
                await poll_until_channel_pool_settles(timeout=0.5)
        finally:
            await connection.close()

        # Assert: the same poll passes once the channel is closed, so
        # the failure above was the cached channel and nothing else.
        await poll_until_channel_pool_settles()

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
                        # Guards the assertion below against passing
                        # vacuously on a pool that never opened a channel.
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
            This test's loop makes its next channel-pool operation —
            `clear_channel_pool`, as it happens, since this loop holds
            nothing of its own to keep — which is what sweeps the
            stopped loop's partition.
        Then:
            It should drop nothing, reporting no record at all on the
            resource pool's logger.
        """

        # Arrange
        scenario = default_scenario(pool_mode=PoolMode.DEFAULT)

        async def lifecycle():
            async with build_pool_from_scenario(scenario, credentials_map):
                await invoke_routine(scenario)
                # Guards the assertions below against passing vacuously
                # on a pool that never opened a channel at all.
                assert channel_pool_stats().total_entries >= 1
            settled = await poll_until_channel_pool_settles()
            return settled.total_entries

        # Act
        stranded, records = await _records_after_stranding(caplog, lifecycle)

        # Assert
        assert stranded == 0
        assert records == []

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
            This test's loop makes its next channel-pool operation,
            sweeping the stopped loop's partition.
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

        # Act
        _, records = await _records_after_stranding(caplog, strand)

        # Assert
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert "0 referenced and 1 idle" in records[0].getMessage()

    @pytest.mark.asyncio
    async def test_dispatch_should_keep_this_loops_entry_when_it_sweeps(
        self, started_worker, retry_grpc_internal, caplog
    ):
        """Test a sweep drops the stopped loop's entries and only those.

        Given:
            This loop already owning a partition, from a dispatch under
            a live `wool.WorkerProxy`, and a bare
            `wool.runtime.worker.connection.WorkerConnection` abandoned
            on another thread's loop that then stops.
        When:
            This loop dispatches again, a mutating pool access that
            sweeps without closing anything of its own.
        Then:
            It should report exactly one warning for the entry the
            stopped loop stranded, and leave this loop's own entry
            cached and dispatchable.
        """
        # Arrange
        worker = await started_worker(LocalWorker())

        async def strand():
            connection = WorkerConnection(worker.address)
            # Deliberately never closed: this is the entry the sweep
            # has to find.
            await connection.idle()

        async def body():
            # Arrange
            async with WorkerProxy(workers=[worker.metadata]):
                assert await routines.add(1, 2) == 3
                # Guards the isolation claim below against passing
                # vacuously on a loop that cached nothing itself.
                assert channel_pool_stats().total_entries == 1

                # Act: the trigger is a dispatch, not a clear — a clear
                # would close the very entry this test is measuring.
                _, records = await _records_after_stranding(
                    caplog, strand, trigger=lambda: routines.add(3, 4)
                )
                mine = channel_pool_stats()
                # The dispatchability probe has to run while the proxy
                # is still entered, so it stays inside the block.
                assert await routines.add(3, 4) == 7

            # Assert
            assert len(records) == 1
            assert records[0].levelno == logging.WARNING
            assert "0 referenced and 1 idle" in records[0].getMessage()
            assert mine.total_entries == 1
            await poll_until_channel_pool_settles()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_clear_channel_pool_should_keep_the_channels_another_loop_cached(
        self, started_worker, retry_grpc_internal, caplog
    ):
        """Test a clear on one loop leaves another loop's channels alone.

        Given:
            An idle channel cached on this loop under a still-entered
            `wool.WorkerProxy`, and another thread's loop whose only
            channel-pool operation is `clear_channel_pool` before it
            stops.
        When:
            That loop clears and stops.
        Then:
            It should leave this loop's entry cached and dispatchable,
            and report nothing — a clear reaches the calling loop's
            partition only, and an empty partition strands nothing.
        """
        # Arrange
        worker = await started_worker(LocalWorker())

        async def clear_only():
            # A loop that touches the pool only to clear it: its
            # partition is created empty and stays that way.
            await clear_channel_pool()

        async def body():
            # Arrange
            async with WorkerProxy(workers=[worker.metadata]):
                assert await routines.add(1, 2) == 3
                before = channel_pool_stats()

                # Act
                _, records = await _records_after_stranding(
                    caplog, clear_only, trigger=lambda: routines.add(3, 4)
                )
                after = channel_pool_stats()
                # The dispatchability probe has to run while the proxy
                # is still entered, so it stays inside the block.
                assert await routines.add(3, 4) == 7

            # Assert
            assert before.total_entries == 1
            assert after.total_entries == 1
            assert records == []
            await poll_until_channel_pool_settles()

        await retry_grpc_internal(body)

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
            This test's loop makes its next channel-pool operation,
            sweeping the stopped loop's partition.
        Then:
            It should report nothing on the resource pool's logger,
            since exiting the connection retired the channel it opened.
        """
        # Arrange
        worker = await started_worker(LocalWorker())

        async def use_and_exit():
            async with WorkerConnection(worker.address) as connection:
                await connection.idle()

        # Act
        _, records = await _records_after_stranding(caplog, use_and_exit)

        # Assert
        assert records == []

    @pytest.mark.asyncio
    async def test_channel_pool_hold_should_keep_another_loops_channel_on_release(
        self, started_worker, retry_grpc_internal, caplog
    ):
        """Test the last hold's release retires the releasing loop only.

        Given:
            A `wool.runtime.worker.connection.WorkerConnection` entered
            on this loop, and a whole `wool.WorkerProxy` lifecycle on
            another thread's loop, whose exit releases that loop's last
            hold.
        When:
            The foreign hold releases and its loop stops.
        Then:
            It should retire that loop's channels alone, leaving this
            loop's cached and still able to poll the worker's idle
            duration, and report nothing.
        """
        # Arrange
        worker = await started_worker(LocalWorker())

        async def proxy_lifecycle():
            async with WorkerProxy(workers=[worker.metadata]):
                assert await routines.add(1, 2) == 3
            # The last release retires that loop's channels drain-first;
            # the settle proves it finished before the loop stopped.
            settled = await poll_until_channel_pool_settles()
            return settled.total_entries

        async def body():
            # Arrange
            async with WorkerConnection(worker.address) as connection:
                assert await connection.idle() >= 0
                before = channel_pool_stats()

                # Act
                stranded, records = await _records_after_stranding(
                    caplog, proxy_lifecycle, trigger=connection.idle
                )
                after = channel_pool_stats()
                # The usability probe has to run while the connection
                # is still entered, so it stays inside the block.
                assert await connection.idle() >= 0

            # Assert
            assert stranded == 0
            assert before.total_entries == 1
            assert after.total_entries == 1
            assert records == []
            await poll_until_channel_pool_settles()

        await retry_grpc_internal(body)

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
            This test's loop makes its next channel-pool operation,
            sweeping the stopped loop's partition.
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

        # Act
        (total, referenced), records = await _records_after_stranding(
            caplog, dispatch_under_proxy
        )

        # Assert
        assert total == 1
        assert referenced == 0
        assert records == []

    @pytest.mark.asyncio
    async def test_channel_pool_hold_should_report_a_stranded_hold_when_loop_stops(
        self, started_worker, caplog
    ):
        """Test a hold a stopped loop never released is reported too.

        Given:
            A `wool.WorkerProxy` entered on an exit stack that is never
            closed, dispatched through, on another thread's loop that
            then stops — so the loop strands both the proxy's hold and
            the dispatch's idle channel.
        When:
            This loop takes and releases a hold of its own, then clears.
        Then:
            It should report one record against the channel pool for
            the idle channel and one against the holds pool for the
            hold: separate pools with separate registries, each swept
            only by an operation that reaches it.
        """
        # Arrange
        worker = await started_worker(LocalWorker())

        async def strand():
            stack = AsyncExitStack()
            await stack.enter_async_context(WorkerProxy(workers=[worker.metadata]))
            assert await routines.add(1, 2) == 3
            # Deliberately never closed: the stack goes out of scope
            # with the proxy still entered and its hold still held.

        # Act
        _, channel_records = await _records_after_stranding(
            caplog, strand, sweep_holds=True
        )
        hold_records = _resourcepool_records(caplog, pool=_HOLD_POOL)

        # Assert
        assert len(channel_records) == 1
        assert "0 referenced and 1 idle" in channel_records[0].getMessage()
        assert len(hold_records) == 1
        assert hold_records[0].levelno == logging.WARNING
        assert "1 referenced and 0 idle" in hold_records[0].getMessage()

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
    async def test_channel_pool_stats_should_report_no_reference_when_nested_retired(
        self, credentials_map, retry_grpc_internal, caplog
    ):
        """Test an outer pool still dispatches once a nested pool retired.

        Given:
            An outer pool entered on a loop where a second pool has
            already been entered, dispatched through, and left again,
            so its channels were retired before the body starts.
        When:
            The outer pool dispatches, with the pool's counters read on
            entry and after the dispatch.
        Then:
            It should carry no reference over from the retired pool,
            return the dispatch, settle its own partition on exit, and
            report nothing on the resource pool's logger.
        """

        async def body():
            # Arrange
            caplog.clear()
            scenario = default_scenario(pool_mode=PoolMode.NESTED_RETIRED_IN_EPHEMERAL)

            # Act
            with caplog.at_level(logging.WARNING, logger=_RESOURCEPOOL_LOGGER):
                async with build_pool_from_scenario(scenario, credentials_map):
                    at_entry = channel_pool_stats()
                    assert await routines.add(1, 2) == 3
                    dispatched = channel_pool_stats()
                settled = await poll_until_channel_pool_settles()

            # Assert
            assert at_entry.referenced_entries == 0
            assert dispatched.total_entries >= 1
            assert settled.total_entries == 0
            assert _resourcepool_records(caplog) == []

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_channel_pool_stats_should_report_nothing_when_proxy_retires(
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
                retired = await poll_until_channel_pool_settles(
                    get=routines.worker_channel_pool_stats,
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
    async def test_worker_channel_pool_stats_should_count_only_the_worker_loop(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker's channels and its client's are counted apart.

        Given:
            A single-worker pool that has dispatched a nested coroutine,
            so the client loop holds the channel to the worker and the
            worker's own loop holds the channel its nested dispatch
            opened.
        When:
            The client's counters and the worker's are read back to
            back.
        Then:
            It should report one entry on each, neither counting the
            other's: a partition belongs to a loop, and the two loops
            are in different processes.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.NESTED_COROUTINE, pool_mode=PoolMode.DEFAULT
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                assert await routines.nested_add(1, 2) == 3
                client = channel_pool_stats()
                served = await routines.worker_channel_pool_stats()

            # Assert
            assert client.total_entries == 1
            assert served.total_entries == 1
            await poll_until_channel_pool_settles()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_channel_pool_stats_should_report_one_channel_per_fanout(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker's nested dispatches share one pooled channel.

        Given:
            A single-worker pool dispatched a routine that fans out
            three nested dispatches of its own.
        When:
            The worker's channel pool counters are read afterwards.
        Then:
            It should report one channel rather than three: the
            partition belongs to the worker's loop, not to the task
            that opened the channel.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.NESTED_COROUTINE, pool_mode=PoolMode.DEFAULT
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                _, inner_pids = await routines.nested_pid_fanout(3)
                served = await routines.worker_channel_pool_stats()

            # Assert
            assert len(inner_pids) == 3
            assert served.total_entries == 1
            await poll_until_channel_pool_settles()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test___aexit___should_settle_channel_pool_when_release_lands_late(
        self, credentials_map, retry_grpc_internal, caplog, tmp_path
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
            worker-side ``finally``, and settle the channel pool to
            nothing without a dropped-entry record.
        """

        async def body():
            # Arrange
            caplog.clear()
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

                # Act & assert: a ValueError here would be the entry the
                # stop RPC rebuilt being corrupted by the late release.
                with pytest.raises(asyncio.CancelledError):
                    await task

                # Poll for the worker's ``finally`` to write the
                # sentinel — it runs after the gRPC stream tears down
                # and tolerates CI load.
                await poll_until(
                    lambda: sentinel.exists() and sentinel.read_text() == "cleaned_up",
                    bool,
                    description="sentinel never written",
                    timeout=15.0,
                    interval=0.1,
                )

                settled = await poll_until_channel_pool_settles()

            # Assert
            assert collected == ["alive"]
            assert sentinel.read_text() == "cleaned_up"
            assert settled.total_entries == 0
            assert _resourcepool_records(caplog) == []

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_channel_pool_stats_should_keep_a_parked_stream_referenced(
        self, started_worker, retry_grpc_internal, caplog
    ):
        """Test a stream parked mid-dispatch survives another loop's sweep.

        Given:
            An async-generator dispatch parked after one value on this
            loop under a live `wool.WorkerProxy`, and a whole channel
            lifecycle — open, use, close — run to completion against the
            same worker on another thread's loop, which then stops.
        When:
            That lifecycle completes and this loop reads the pool,
            sweeping the stopped loop's partition.
        Then:
            It should leave this loop's channel referenced by the
            parked stream, let the stream drain to exhaustion, and
            report nothing.
        """
        # Arrange
        worker = await started_worker(LocalWorker())

        async def lifecycle():
            # A pool or a proxy cannot be entered here: the chain this
            # loop arms to hold the parked stream travels with the
            # context `run_on_foreign_loop` copies onto its thread, and
            # arming it a second time off-thread is contention by
            # design. A connection opened and closed is the whole
            # channel lifecycle this file is about either way.
            async with WorkerConnection(worker.address) as connection:
                assert await connection.idle() >= 0
            settled = await poll_until_channel_pool_settles()
            return settled.total_entries

        async def body():
            # Arrange
            async with WorkerProxy(workers=[worker.metadata]):
                stream = routines.gen_range(3)
                collected = [await stream.__anext__()]

                # Act: the trigger is a read — a clear would force-close
                # the channel the parked stream is still using.
                stranded, records = await _records_after_stranding(
                    caplog, lifecycle, trigger=channel_pool_stats
                )
                parked = channel_pool_stats()
                collected.extend([value async for value in stream])

            # Assert
            assert stranded == 0
            assert parked.referenced_entries == 1
            assert collected == [0, 1, 2]
            assert records == []
            await poll_until_channel_pool_settles()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_channel_pool_stats_should_isolate_loops_when_two_run_concurrently(
        self, credentials_map, retry_grpc_internal
    ):
        """Test concurrent loops each see only their own pooled channels.

        Given:
            Two one-worker pools dispatching on two loops running
            concurrently on separate threads, synchronized so both hold
            their channel at the same moment.
        When:
            Each loop reads the channel pool's totals while both are
            live and again after its own pool exits.
        Then:
            It should report one entry each — never the other loop's —
            and each loop should empty its own partition.
        """

        async def body():
            # Arrange
            scenario = default_scenario(pool_mode=PoolMode.DEFAULT)
            barrier = threading.Barrier(2)

            # Act
            first, second = await _gather_probes(
                barrier,
                _concurrent_lifecycle_probe(scenario, credentials_map, barrier),
                _concurrent_lifecycle_probe(scenario, credentials_map, barrier),
            )

            # Assert
            assert first == (1, 0)
            assert second == (1, 0)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_channel_pool_stats_should_isolate_loops_when_both_dial_one_worker(
        self, started_worker, retry_grpc_internal
    ):
        """Test two loops dialing one worker each cache their own channel.

        Given:
            One worker started outside any pool, and two loops running
            concurrently on separate threads, each entering a
            `wool.WorkerProxy` over that worker's metadata and
            synchronized so both hold their channel at the same moment.
        When:
            Each loop reads the totals and dispatches `get_pid`, and the
            worker's own counters are read afterwards.
        Then:
            It should report one entry per loop — the same key on two
            partitions, not one shared entry — the same pid from both,
            and an empty partition on the worker, which dialed nobody.
        """
        # Arrange
        worker = await started_worker(LocalWorker())

        async def body():
            # Arrange
            barrier = threading.Barrier(2)

            # Act
            first, second = await _gather_probes(
                barrier,
                _concurrent_proxy_probe(worker.metadata, barrier),
                _concurrent_proxy_probe(worker.metadata, barrier),
            )
            async with WorkerProxy(workers=[worker.metadata]):
                served = await routines.worker_channel_pool_stats()

            # Assert
            assert first == (worker.metadata.pid, 1, 0)
            assert second == (worker.metadata.pid, 1, 0)
            assert served.total_entries == 0
            await poll_until_channel_pool_settles()

        await retry_grpc_internal(body)
