"""Tests for pool composition via build_pool_from_scenario."""

import pytest

from .conftest import CredentialType
from .conftest import DiscoveryFactory
from .conftest import LbFactory
from .conftest import PoolMode
from .conftest import RoutineBinding
from .conftest import RoutineShape
from .conftest import Scenario
from .conftest import TimeoutKind
from .conftest import WorkerOptionsKind
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
            A complete scenario using EPHEMERAL pool mode with 2 workers.
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3

    @pytest.mark.asyncio
    async def test_build_pool_from_scenario_with_durable_joined_lan(
        self, credentials_map
    ):
        """Test building a pool with DURABLE_JOINED mode and LAN_CALLABLE.

        Given:
            A complete scenario using DURABLE_JOINED pool mode with
            LAN_CALLABLE discovery, where a separate LanDiscovery
            instance discovers externally published workers.
        When:
            A pool is built and a coroutine routine is dispatched.
        Then:
            It should return the correct result via the separate
            LanDiscovery.
        """
        # Arrange
        scenario = Scenario(
            shape=RoutineShape.COROUTINE,
            pool_mode=PoolMode.DURABLE_JOINED,
            discovery=DiscoveryFactory.LAN_CALLABLE,
            lb=LbFactory.CLASS_REF,
            credential=CredentialType.INSECURE,
            options=WorkerOptionsKind.DEFAULT,
            timeout=TimeoutKind.NONE,
            binding=RoutineBinding.MODULE_FUNCTION,
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
        )

        # Act
        async with build_pool_from_scenario(scenario, credentials_map):
            result = await invoke_routine(scenario)

        # Assert
        assert result == 3
