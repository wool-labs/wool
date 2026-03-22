"""Full integration tests — pairwise parametrized and Hypothesis exploration."""

import asyncio

import pytest
from hypothesis import HealthCheck
from hypothesis import example
from hypothesis import given
from hypothesis import settings

from .conftest import PAIRWISE_SCENARIOS
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
from .conftest import scenarios_strategy

_INTEGRATION_TIMEOUT = 30

_NESTED_SHAPES = {RoutineShape.NESTED_COROUTINE, RoutineShape.NESTED_ASYNC_GEN}


def _xfail_known_bugs(scenario):
    if scenario.shape in _NESTED_SHAPES:
        pytest.xfail(
            "grpcio 1.78 PollerCompletionQueue thundering herd race "
            "(https://github.com/grpc/grpc/pull/41483)"
        )


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", PAIRWISE_SCENARIOS, ids=str)
async def test_dispatch_pairwise(scenario, credentials_map):
    """Test routine dispatch across pairwise scenario combinations.

    Given:
        A complete scenario from the pairwise covering array.
    When:
        A pool is built and the appropriate routine is dispatched.
    Then:
        It should return the expected result for the given routine shape.
    """
    _xfail_known_bugs(scenario)

    # Arrange, act, & assert
    async with asyncio.timeout(_INTEGRATION_TIMEOUT):
        async with build_pool_from_scenario(scenario, credentials_map):
            await invoke_routine(scenario)


@pytest.mark.integration
@pytest.mark.asyncio
@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[
        HealthCheck.function_scoped_fixture,
        HealthCheck.too_slow,
    ],
)
@example(
    scenario=Scenario(
        RoutineShape.COROUTINE,
        PoolMode.DEFAULT,
        DiscoveryFactory.NONE,
        LbFactory.CLASS_REF,
        CredentialType.INSECURE,
        WorkerOptionsKind.DEFAULT,
        TimeoutKind.NONE,
        RoutineBinding.MODULE_FUNCTION,
    )
)
@example(
    scenario=Scenario(
        RoutineShape.ASYNC_GEN_ANEXT,
        PoolMode.EPHEMERAL,
        DiscoveryFactory.NONE,
        LbFactory.INSTANCE,
        CredentialType.INSECURE,
        WorkerOptionsKind.DEFAULT,
        TimeoutKind.NONE,
        RoutineBinding.MODULE_FUNCTION,
    )
)
@given(scenario=scenarios_strategy())
async def test_dispatch_hypothesis(scenario, credentials_map):
    """Test routine dispatch with Hypothesis-generated scenarios.

    Given:
        A randomly generated valid scenario from the Hypothesis strategy.
    When:
        A pool is built and the appropriate routine is dispatched.
    Then:
        It should return the expected result for the given routine shape.
    """
    _xfail_known_bugs(scenario)

    # Arrange, act, & assert
    async with asyncio.timeout(_INTEGRATION_TIMEOUT):
        async with build_pool_from_scenario(scenario, credentials_map):
            await invoke_routine(scenario)
