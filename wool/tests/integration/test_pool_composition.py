"""Tests for pool composition via build_pool_from_scenario."""

import uuid
from functools import partial

import pytest

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.pool import WorkerPool

from . import routines
from .conftest import BackpressureMode
from .conftest import CredentialType
from .conftest import DiscoveryFactory
from .conftest import LazyMode
from .conftest import LbFactory
from .conftest import PoolMode
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
    async def test_build_pool_from_scenario_with_default_mode(self, credentials_map):
        """Test building a pool with DEFAULT mode.

        Given:
            A complete scenario using DEFAULT pool mode with insecure
            credentials.
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result (add(1, 2) == 3).
        """
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_ephemeral_mode(self, credentials_map):
        """Test building a pool with EPHEMERAL mode and size=2.

        Given:
            A complete scenario using EPHEMERAL pool mode with 2 workers
            and eager proxy start.
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result.
        """
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_durable_mode(self, credentials_map):
        """Test building a pool with DURABLE mode.

        Given:
            A complete scenario using DURABLE pool mode with internally
            managed LocalDiscovery and externally started workers.
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result.
        """
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_hybrid_mode(self, credentials_map):
        """Test building a pool with HYBRID mode and LOCAL_CALLABLE discovery.

        Given:
            A complete scenario using HYBRID pool mode with local
            callable discovery factory.
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result.
        """
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_durable_joined_local(
        self, credentials_map
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_restrictive_opts(self, credentials_map):
        """Test building a pool with restrictive message size options.

        Given:
            A complete scenario using RESTRICTIVE worker options (64KB
            message limits).
        When:
            A pool is built and a small-payload coroutine is dispatched.
        Then:
            It should return the correct result within the size limits.
        """
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_keepalive_opts(self, credentials_map):
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_dispatch_timeout(self, credentials_map):
        """Test building a pool with dispatch_timeout via RuntimeContext.

        Given:
            A complete scenario using VIA_RUNTIME_CONTEXT timeout.
        When:
            A pool is built within a RuntimeContext and a coroutine is
            dispatched.
        Then:
            It should return the correct result with the timeout active.
        """
        # Arrange
        scenario = Scenario(
            shape=RoutineShape.COROUTINE,
            pool_mode=PoolMode.DEFAULT,
            discovery=DiscoveryFactory.NONE,
            lb=LbFactory.CLASS_REF,
            credential=CredentialType.INSECURE,
            options=WorkerOptionsKind.DEFAULT,
            timeout=TimeoutKind.VIA_RUNTIME_CONTEXT,
            binding=RoutineBinding.MODULE_FUNCTION,
            lazy=LazyMode.LAZY,
            backpressure=BackpressureMode.NONE,
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_shared_discovery(self, credentials_map):
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_sync_backpressure(
        self, credentials_map
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_async_backpressure(
        self, credentials_map
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3


def _sync_reject_hook(ctx):
    """Sync backpressure hook that rejects all tasks."""
    return True


async def _async_reject_hook(ctx):
    """Async backpressure hook that rejects all tasks."""
    return True


@pytest.mark.integration
class TestBackpressureRejection:
    @pytest.mark.asyncio
    async def test_sync_backpressure_rejection(self):
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

    @pytest.mark.asyncio
    async def test_async_backpressure_rejection(self):
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

    @pytest.mark.asyncio
    async def test_backpressure_fallback_to_accepting_worker(self):
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
                    await publisher.publish("worker-added", rejecting_worker.metadata)
                    await publisher.publish("worker-added", accepting_worker.metadata)
                    pool = WorkerPool(
                        discovery=_DirectDiscovery(discovery),
                        loadbalancer=RoundRobinLoadBalancer,
                    )

                    # Act
                    async with pool:
                        result = await routines.add(1, 2)

                    # Assert
                    assert result == 3

                    await publisher.publish("worker-dropped", rejecting_worker.metadata)
                    await publisher.publish("worker-dropped", accepting_worker.metadata)
        finally:
            await accepting_worker.stop()
            await rejecting_worker.stop()
