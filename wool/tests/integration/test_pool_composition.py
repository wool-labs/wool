"""Tests for pool composition via build_pool_from_scenario."""

import asyncio
import uuid
from functools import partial

import pytest

import wool
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.worker.base import ChannelOptions
from wool.runtime.worker.base import WorkerOptions
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.pool import WorkerPool

from . import routines
from .conftest import BackpressureMode
from .conftest import ContextVarPattern
from .conftest import CredentialType
from .conftest import DiscoveryFactory
from .conftest import LazyMode
from .conftest import LbFactory
from .conftest import PoolMode
from .conftest import QuorumMode
from .conftest import RoutineBinding
from .conftest import RoutineShape
from .conftest import Scenario
from .conftest import TimeoutKind
from .conftest import WorkerOptionsKind
from .conftest import _DirectDiscovery
from .conftest import build_pool_from_scenario
from .conftest import invoke_routine


@pytest.mark.integration
class TestPoolComposition:
    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_return_result_when_default_mode(
        self, credentials_map, retry_grpc_internal
    ):
        """Test building a pool with DEFAULT mode.

        Given:
            A complete scenario using DEFAULT pool mode with insecure
            credentials.
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result (add(1, 2) == 3).
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
                discovery=DiscoveryFactory.NONE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.DEFAULT,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.NONE,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)

            # Assert
            assert result == 3

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_return_result_when_ephemeral_mode(
        self, credentials_map, retry_grpc_internal
    ):
        """Test building a pool with EPHEMERAL mode and size=2.

        Given:
            A complete scenario using EPHEMERAL pool mode with 2 workers
            and eager proxy start.
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result.
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.EPHEMERAL,
                discovery=DiscoveryFactory.NONE,
                lb=LbFactory.INSTANCE,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.DEFAULT,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.EAGER,
                backpressure=BackpressureMode.NONE,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)

            # Assert
            assert result == 3

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_return_result_when_durable_mode(
        self, credentials_map, retry_grpc_internal
    ):
        """Test building a pool with DURABLE mode.

        Given:
            A complete scenario using DURABLE pool mode with internally
            managed LocalDiscovery and externally started workers.
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result.
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DURABLE,
                discovery=DiscoveryFactory.NONE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.DEFAULT,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.NONE,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)

            # Assert
            assert result == 3

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_return_result_when_hybrid_mode(
        self, credentials_map, retry_grpc_internal
    ):
        """Test building a pool with HYBRID mode and LOCAL_CALLABLE discovery.

        Given:
            A complete scenario using HYBRID pool mode with local
            callable discovery factory.
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result.
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.HYBRID,
                discovery=DiscoveryFactory.LOCAL_CALLABLE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.DEFAULT,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.NONE,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)

            # Assert
            assert result == 3

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_return_result_when_durable_joined(
        self, credentials_map, retry_grpc_internal
    ):
        """Test building a pool with DURABLE_JOINED mode and LOCAL_CALLABLE.

        Given:
            A complete scenario using DURABLE_JOINED pool mode with
            LOCAL_CALLABLE discovery, where a non-owner joiner
            discovers workers through an existing namespace.
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result via the non-owner
            LocalDiscovery.
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DURABLE_JOINED,
                discovery=DiscoveryFactory.LOCAL_CALLABLE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.DEFAULT,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.NONE,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)

            # Assert
            assert result == 3

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_return_result_when_restrictive_opts(
        self, credentials_map, retry_grpc_internal
    ):
        """Test building a pool with restrictive message size options.

        Given:
            A complete scenario using RESTRICTIVE worker options (64KB
            message limits).
        When:
            A pool is built and a small-payload coroutine is dispatched.
        Then:
            It should return the correct result within the size limits.
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
                discovery=DiscoveryFactory.NONE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.RESTRICTIVE,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.NONE,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)

            # Assert
            assert result == 3

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_return_result_when_keepalive_opts(
        self, credentials_map, retry_grpc_internal
    ):
        """Test building a pool with keepalive worker options.

        Given:
            A complete scenario using KEEPALIVE worker options (10s ping
            interval, 5s timeout).
        When:
            A pool is built and a small-payload coroutine is dispatched.
        Then:
            It should return the correct result with keepalive options
            active on both server and client.
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
                discovery=DiscoveryFactory.NONE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.KEEPALIVE,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.NONE,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)

            # Assert
            assert result == 3

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_propagate_dispatch_timeout_to_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the VIA_DISPATCH_TIMEOUT_VAR dimension propagates to the worker.

        Given:
            A complete scenario using VIA_DISPATCH_TIMEOUT_VAR timeout,
            which makes ``build_pool_from_scenario`` set the ambient
            ``dispatch_timeout`` var before the dispatch.
        When:
            A pool is built and a coroutine is dispatched, then a
            routine that returns the worker-side ``dispatch_timeout``
            value is dispatched.
        Then:
            The first dispatch should return its result and the
            second should report the builder's ``dispatch_timeout``
            value — proving the dimension's ambient var rides the wire
            and is restored on the worker.
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
                discovery=DiscoveryFactory.NONE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.DEFAULT,
                timeout=TimeoutKind.VIA_DISPATCH_TIMEOUT_VAR,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.NONE,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)
                worker_timeout = await routines.read_dispatch_timeout()

            # Assert
            assert result == 3
            # ``build_pool_from_scenario`` sets dispatch_timeout=30.0
            # for the VIA_DISPATCH_TIMEOUT_VAR dimension; the worker
            # must observe that value through the wire-shipped
            # RuntimeContext.
            assert worker_timeout == 30.0

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_runtime_context_manager_should_propagate_dispatch_timeout(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a caller-side wool.RuntimeContext rides the dispatch wire.

        Given:
            A default-timeout scenario pool and a caller that wraps a
            dispatch in ``with wool.RuntimeContext(dispatch_timeout=X)``.
        When:
            A routine reading the worker-side ``dispatch_timeout`` is
            dispatched inside the block.
        Then:
            The worker should observe ``X`` — ``RuntimeContext.__enter__``
            sets the ambient timeout, which rides the wire — and the
            ambient ``dispatch_timeout`` should be restored to its prior
            value once the block exits (``__exit__``).
        """
        from wool.runtime.context.runtime import dispatch_timeout

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
                discovery=DiscoveryFactory.NONE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.DEFAULT,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.NONE,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )
            before = dispatch_timeout.get()

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                with wool.RuntimeContext(dispatch_timeout=7.5):
                    worker_timeout = await routines.read_dispatch_timeout()
                restored = dispatch_timeout.get()

            # Assert
            assert worker_timeout == 7.5
            assert restored == before

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_return_result_when_shared_discovery(
        self, credentials_map, retry_grpc_internal
    ):
        """Test two pools sharing the same discovery subscriber.

        Given:
            Two WorkerPool instances sharing the same LocalDiscovery
            with one externally started worker, exercising
            SubscriberMeta caching and _SharedSubscription fan-out.
        When:
            Both pools are entered and a coroutine is dispatched
            through the primary pool.
        Then:
            It should discover the worker and return the correct
            result.
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DURABLE_SHARED,
                discovery=DiscoveryFactory.NONE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.DEFAULT,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.NONE,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)

            # Assert
            assert result == 3

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_return_result_when_sync_backpressure(
        self, credentials_map, retry_grpc_internal
    ):
        """Test building a pool with a sync backpressure accept hook.

        Given:
            A complete scenario using a SYNC backpressure hook that
            accepts all tasks (survives cloudpickle serialization to
            the subprocess).
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result.
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
                discovery=DiscoveryFactory.NONE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.DEFAULT,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.SYNC,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)

            # Assert
            assert result == 3

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_should_return_result_when_async_backpressure(
        self, credentials_map, retry_grpc_internal
    ):
        """Test building a pool with an async backpressure accept hook.

        Given:
            A complete scenario using an ASYNC backpressure hook that
            accepts all tasks (async hook survives cloudpickle
            serialization to the subprocess).
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result.
        """

        async def body():
            # Arrange
            scenario = Scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
                discovery=DiscoveryFactory.NONE,
                lb=LbFactory.CLASS_REF,
                credential=CredentialType.INSECURE,
                options=WorkerOptionsKind.DEFAULT,
                timeout=TimeoutKind.NONE,
                binding=RoutineBinding.MODULE_FUNCTION,
                lazy=LazyMode.LAZY,
                backpressure=BackpressureMode.ASYNC,
                ctx_var_1=ContextVarPattern.NONE,
                ctx_var_2=ContextVarPattern.NONE,
                ctx_var_3=ContextVarPattern.NONE,
                quorum=QuorumMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await invoke_routine(scenario)

            # Assert
            assert result == 3

        await retry_grpc_internal(body)


def _sync_reject_hook(ctx):
    """Sync backpressure hook that rejects all tasks."""
    return True


async def _async_reject_hook(ctx):
    """Async backpressure hook that rejects all tasks."""
    return True


@pytest.mark.integration
class TestBackpressureRejection:
    @pytest.mark.asyncio
    async def test_sync_backpressure_should_raise_no_workers_available(
        self, retry_grpc_internal
    ):
        """Test sync backpressure hook rejects task end-to-end.

        Given:
            A single-worker pool with a sync backpressure hook that
            rejects all tasks.
        When:
            A coroutine routine is dispatched.
        Then:
            It should raise NoWorkersAvailable because the only worker
            rejects with RESOURCE_EXHAUSTED.
        """

        async def body():
            # Arrange
            pool = WorkerPool(
                size=1,
                loadbalancer=RoundRobinLoadBalancer,
                worker=partial(LocalWorker, backpressure=_sync_reject_hook),
            )

            # Act & assert
            async with pool:
                with pytest.raises(NoWorkersAvailable):
                    await routines.add(1, 2)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_backpressure_should_raise_no_workers_available(
        self, retry_grpc_internal
    ):
        """Test async backpressure hook rejects task end-to-end.

        Given:
            A single-worker pool with an async backpressure hook that
            rejects all tasks.
        When:
            A coroutine routine is dispatched.
        Then:
            It should raise NoWorkersAvailable because the only worker
            rejects with RESOURCE_EXHAUSTED.
        """

        async def body():
            # Arrange
            pool = WorkerPool(
                size=1,
                loadbalancer=RoundRobinLoadBalancer,
                worker=partial(LocalWorker, backpressure=_async_reject_hook),
            )

            # Act & assert
            async with pool:
                with pytest.raises(NoWorkersAvailable):
                    await routines.add(1, 2)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_backpressure_should_fall_through_to_accepting_worker(
        self, retry_grpc_internal
    ):
        """Test load balancer falls through to an accepting worker.

        Given:
            A durable pool with two workers: one rejecting all tasks
            via backpressure and one accepting all tasks.
        When:
            A coroutine routine is dispatched.
        Then:
            It should succeed by falling through to the accepting
            worker after the rejecting worker returns
            RESOURCE_EXHAUSTED.
        """

        async def body():
            # Arrange
            namespace = f"bp-fallback-{uuid.uuid4().hex[:12]}"
            rejecting_worker = LocalWorker(backpressure=_sync_reject_hook)
            accepting_worker = LocalWorker()

            await rejecting_worker.start()
            await accepting_worker.start()
            try:
                with LocalDiscovery(namespace) as discovery:
                    publisher = discovery.publisher
                    async with publisher:
                        await publisher.publish(
                            "worker-added", rejecting_worker.metadata
                        )
                        await publisher.publish(
                            "worker-added", accepting_worker.metadata
                        )
                        pool = WorkerPool(
                            discovery=_DirectDiscovery(discovery),
                            loadbalancer=RoundRobinLoadBalancer,
                        )

                        # Act
                        async with pool:
                            result = await routines.add(1, 2)

                        # Assert
                        assert result == 3

                        await publisher.publish(
                            "worker-dropped", rejecting_worker.metadata
                        )
                        await publisher.publish(
                            "worker-dropped", accepting_worker.metadata
                        )
            finally:
                await accepting_worker.stop()
                await rejecting_worker.stop()

        await retry_grpc_internal(body)


# Saturating fan-out regression parameters (issue #290). The client gate is
# small so the test is fast; each burst dispatches well over the gate to force
# the semaphore permit-turnover overshoot that, before the fix, drove the
# server's stream ceiling past its limit and faulted the connection with
# INTERNAL / ExecuteBatchError. Empirically the fault fired within a couple of
# bursts when the ceiling equalled the gate; these values reproduce it while
# keeping the run well under a second on the fixed build.
_FANOUT_GATE = 32
_FANOUT_SIZE = 128
_FANOUT_BURSTS = 10


@pytest.mark.integration
class TestSaturatingFanout:
    @pytest.mark.asyncio
    async def test_dispatch_should_complete_saturating_fanout_without_spurious_eviction(
        self,
    ):
        """Test sustained saturating fan-out to a one-worker pool.

        Given:
            A one-worker pool whose worker advertises a small
            max_concurrent_streams gate, driven by repeated bursts of
            concurrent dispatches that far exceed the gate.
        When:
            Every burst is awaited to completion in turn.
        Then:
            It should complete every dispatch without raising
            NoWorkersAvailable, leaving the healthy worker in the pool —
            the transient stream-starvation fault neither surfaces nor
            evicts the live worker.
        """
        # Arrange
        worker = partial(
            LocalWorker,
            options=WorkerOptions(
                channel=ChannelOptions(max_concurrent_streams=_FANOUT_GATE)
            ),
        )

        # Act & assert
        async with WorkerPool(spawn=1, worker=worker):
            for _ in range(_FANOUT_BURSTS):
                results = await asyncio.gather(
                    *(routines.add(1, 2) for _ in range(_FANOUT_SIZE))
                )
                assert results == [3] * _FANOUT_SIZE
            # The worker must still serve after the sustained fan-out —
            # a spurious eviction would raise NoWorkersAvailable here.
            assert await routines.add(1, 2) == 3
