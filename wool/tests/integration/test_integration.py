"""Full integration tests — pairwise parametrized and Hypothesis exploration."""

import asyncio
import contextvars

import pytest
from hypothesis import HealthCheck
from hypothesis import example
from hypothesis import given
from hypothesis import settings

from .conftest import PAIRWISE_SCENARIOS
from .conftest import BackpressureMode
from .conftest import ContextVarPattern
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
from .conftest import build_pool_from_scenario
from .conftest import invoke_routine
from .conftest import scenarios_strategy

_INTEGRATION_TIMEOUT = 30


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", PAIRWISE_SCENARIOS, ids=str)
async def test_dispatch_pairwise(scenario, credentials_map, retry_grpc_internal):
    """Test routine dispatch across pairwise scenario combinations.

    Given:
        A complete scenario from the pairwise covering array.
    When:
        A pool is built and the appropriate routine is dispatched.
    Then:
        It should return the expected result for the given routine shape.
    """

    # Arrange, act, & assert
    async def body():
        async with asyncio.timeout(_INTEGRATION_TIMEOUT):
            async with build_pool_from_scenario(scenario, credentials_map):
                # Each invocation runs in an isolated stdlib Context
                # copy. pytest-asyncio re-uses a single ``context=``
                # across every test invocation, so mutations made
                # inside ``invoke_routine`` otherwise leak into the
                # shared context and cross-contaminate other
                # parametrized cases. See pytest-asyncio #136 /
                # AnyIO testing docs for the known interaction
                # between pytest runners, ``asyncio.Task(context=)``
                # and ContextVar mutation visibility.
                isolated = contextvars.copy_context()
                await asyncio.create_task(invoke_routine(scenario), context=isolated)

    await retry_grpc_internal(body)


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
        LazyMode.LAZY,
        BackpressureMode.NONE,
        ContextVarPattern.NONE,
        ContextVarPattern.NONE,
        ContextVarPattern.NONE,
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
        LazyMode.LAZY,
        BackpressureMode.SYNC,
        ContextVarPattern.NONE,
        ContextVarPattern.NONE,
        ContextVarPattern.NONE,
    )
)
@example(
    scenario=Scenario(
        RoutineShape.COROUTINE,
        PoolMode.DEFAULT,
        DiscoveryFactory.NONE,
        LbFactory.CLASS_REF,
        CredentialType.INSECURE,
        WorkerOptionsKind.KEEPALIVE,
        TimeoutKind.NONE,
        RoutineBinding.MODULE_FUNCTION,
        LazyMode.LAZY,
        BackpressureMode.ASYNC,
        ContextVarPattern.NONE,
        ContextVarPattern.NONE,
        ContextVarPattern.NONE,
    )
)
@given(scenario=scenarios_strategy())
async def test_dispatch_hypothesis(scenario, credentials_map, retry_grpc_internal):
    """Test routine dispatch with Hypothesis-generated scenarios.

    Given:
        A randomly generated valid scenario from the Hypothesis strategy.
    When:
        A pool is built and the appropriate routine is dispatched.
    Then:
        It should return the expected result for the given routine shape.
    """

    # Arrange, act, & assert
    async def body():
        async with asyncio.timeout(_INTEGRATION_TIMEOUT):
            async with build_pool_from_scenario(scenario, credentials_map):
                # See ``test_dispatch_pairwise`` above for the
                # rationale behind isolating each Hypothesis example
                # in its own stdlib Context copy.
                isolated = contextvars.copy_context()
                await asyncio.create_task(invoke_routine(scenario), context=isolated)

    await retry_grpc_internal(body)
