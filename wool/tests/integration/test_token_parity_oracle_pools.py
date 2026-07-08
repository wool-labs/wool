"""Differential token-parity oracle across varied pool topologies.

Runs the op-script differential oracle from ``_token_ops`` — the same
caller/worker set/reset/get scripts checked against a local stdlib
``contextvars.ContextVar`` ground truth — over pools whose topology
varies: a two-worker round-robin EPHEMERAL pool, an eagerly started
quorum-2 variant, a nested DEFAULT-in-EPHEMERAL arrangement, and an
mTLS-secured DEFAULT pool. The point under test is that pool topology
(worker count, startup laziness, nesting, transport credentials) is
invisible to ``wool.Token`` semantics: a dispatch chain is a logical
continuation of one context, so the consumed-token ledger must follow
the chain wherever the load balancer sends it. The single-worker
DEFAULT/insecure baseline is covered by the companion oracle suite.

Each parametrized topology enters ONE pool shared by every Hypothesis
example (pool startup dominates runtime; the shared pool is the point
of the ``function_scoped_fixture`` suppression), while every example
executes against a fresh uniquely named ``wool.ContextVar`` inside its
own stdlib context copy, keeping examples independent.
"""

from __future__ import annotations

import asyncio
import contextvars
import dataclasses
import uuid
from typing import Any

import pytest
import pytest_asyncio
from hypothesis import HealthCheck
from hypothesis import example
from hypothesis import given
from hypothesis import settings

import wool

from . import routines
from ._token_ops import CALLER
from ._token_ops import UNSET
from ._token_ops import WORKER
from ._token_ops import Observation
from ._token_ops import Op
from ._token_ops import ResetOp
from ._token_ops import SetOp
from ._token_ops import describe_mismatch
from ._token_ops import observations_match
from ._token_ops import run_stdlib_oracle
from ._token_ops import run_wool_script
from ._token_ops import scripts
from .conftest import CredentialType
from .conftest import LazyMode
from .conftest import PoolMode
from .conftest import QuorumMode
from .conftest import build_pool_from_scenario
from .conftest import default_scenario

_SCRIPT_TIMEOUT = 30

#: Two workers, both deterministically up before the first dispatch:
#: EAGER starts them at pool entry and quorum 2 blocks entry until both
#: are serving, so round-robin alternation across workers is guaranteed
#: from the first op. Shared by the parametrized property test and the
#: deterministic worker-hopping example test.
_EAGER_TWO_WORKER_SCENARIO = default_scenario(
    pool_mode=PoolMode.EPHEMERAL,
    lazy=LazyMode.EAGER,
    quorum=QuorumMode.ABOVE_DEFAULT,
)

#: Pool topologies the oracle must be indifferent to. The DEFAULT
#: insecure single-worker baseline is deliberately absent (companion
#: suite); DURABLE modes are excluded for external-worker startup cost.
POOL_TOPOLOGIES = [
    # Two workers, sequential remote ops round-robin across BOTH.
    default_scenario(pool_mode=PoolMode.EPHEMERAL),
    # Both workers eagerly up from the start (quorum 2).
    _EAGER_TWO_WORKER_SCENARIO,
    # A second (nested) pool context entered inside the outer pool.
    default_scenario(pool_mode=PoolMode.NESTED_DEFAULT_IN_EPHEMERAL),
    # Canonical secure transport: mutual TLS on the default pool shape.
    dataclasses.replace(default_scenario(), credential=CredentialType.MTLS),
]

#: The just-fixed cross-process double-reset regression: a worker
#: consumes the caller's first token, and the caller's later reset of
#: that same token must observe stdlib's single-use RuntimeError.
_DOUBLE_RESET_REGRESSION: list[Op] = [
    SetOp(CALLER, 1),
    ResetOp(WORKER, 0),
    SetOp(CALLER, 2),
    ResetOp(CALLER, 0),
]


@pytest_asyncio.fixture
async def topology_pool(request, credentials_map):
    """Enter one pool for the requested scenario and hold it for the test.

    Parametrized indirectly with a complete :class:`Scenario`. The pool
    is entered once per test invocation and shared by every Hypothesis
    example — ``wool.__proxy__`` set here propagates into the test body
    (pytest-asyncio runs async fixtures and tests in one context).
    """
    async with build_pool_from_scenario(request.param, credentials_map):
        yield request.param


async def _wool_trace(script, retry_grpc_internal):
    """Execute *script* over the entered pool and return its trace.

    Each attempt runs against a fresh uniquely named ``wool.ContextVar``
    inside its own stdlib context copy: retries never see a partially
    mutated variable, and caller-side mutations cannot leak between
    Hypothesis examples (which otherwise share one ambient context).
    """

    async def body():
        async with asyncio.timeout(_SCRIPT_TIMEOUT):
            var: wool.ContextVar[Any] = wool.ContextVar(f"a5_{uuid.uuid4().hex}")
            isolated = contextvars.copy_context()
            return await asyncio.create_task(
                run_wool_script(script, var), context=isolated
            )

    return await retry_grpc_internal(body)


@pytest.mark.integration
class TestTokenParityAcrossPoolTopologies:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("topology_pool", POOL_TOPOLOGIES, ids=str, indirect=True)
    @settings(
        max_examples=10,
        deadline=None,
        suppress_health_check=[
            HealthCheck.function_scoped_fixture,
            HealthCheck.too_slow,
        ],
    )
    @example(script=_DOUBLE_RESET_REGRESSION)
    @given(script=scripts())
    async def test_run_wool_script_should_match_stdlib_oracle_when_topology_varies(
        self, script, topology_pool, retry_grpc_internal
    ):
        """Test generated op scripts keep stdlib parity on every pool topology.

        Given:
            Any script of up to twelve set/reset/get ops spread across
            the caller and worker actors, with int/str values and token
            indexes drawn with replacement, plus one live pool per
            parametrized topology (two-worker round-robin, eager
            quorum-2, nested, mTLS) shared across all examples — and a
            pinned example replaying the cross-process double-reset
            regression whose final caller reset must observe
            RuntimeError.
        When:
            The script runs through run_wool_script against a fresh
            wool.ContextVar and, separately, through run_stdlib_oracle.
        Then:
            It should produce identical observation traces — pool
            topology must be invisible to token semantics.
        """
        # Act
        wool_observations = await _wool_trace(script, retry_grpc_internal)
        stdlib_observations = run_stdlib_oracle(script)

        # Assert
        assert observations_match(wool_observations, stdlib_observations), (
            describe_mismatch(wool_observations, stdlib_observations)
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "topology_pool", [_EAGER_TWO_WORKER_SCENARIO], ids=str, indirect=True
    )
    async def test_remote_resets_should_match_stdlib_oracle_when_they_hop_workers(
        self, topology_pool, retry_grpc_internal
    ):
        """Test remote resets that alternate workers keep parity across the hop.

        Given:
            An EPHEMERAL pool with two eagerly started workers behind a
            round-robin load balancer (quorum 2, so both serve from the
            first op) and a fixed script of two worker sets, two worker
            resets, and a final caller reset of the first token — driven
            through the attributed oracle routines so each remote reset
            reports the worker pid that performed it.
        When:
            The script runs op by op against a fresh wool.ContextVar
            and, separately, through run_stdlib_oracle.
        Then:
            It should produce identical observation traces, the final
            caller reset should observe RuntimeError because token 0 was
            already consumed remotely on the other worker, and the two
            remote resets should be attributed to at least two distinct
            worker pids — proving the resets actually hopped workers.
        """
        # Arrange — the worker-heavy script whose two remote resets must
        # land on alternating workers.
        script = [
            SetOp(WORKER, 1),
            SetOp(WORKER, 2),
            ResetOp(WORKER, 1),
            ResetOp(WORKER, 0),
            ResetOp(CALLER, 0),
        ]

        async def body() -> tuple[list[Observation], list[int]]:
            async with asyncio.timeout(_SCRIPT_TIMEOUT):

                async def drive() -> tuple[list[Observation], list[int]]:
                    var: wool.ContextVar[Any] = wool.ContextVar(f"a5_{uuid.uuid4().hex}")
                    tokens: list[Any] = []
                    trace: list[Observation] = []
                    reset_pids: list[int] = []
                    for op in script:
                        if isinstance(op, SetOp):
                            _pid, token = await routines.oracle_set_report(
                                var.namespace, var.name, op.value
                            )
                            tokens.append(token)
                            trace.append(Observation("set", var.get(UNSET), None))
                        elif op.actor == WORKER:
                            token = tokens[op.token_index % len(tokens)]
                            pid, kind, _msg, value = await routines.oracle_reset_report(
                                var.namespace, var.name, token
                            )
                            reset_pids.append(pid)
                            error = None if kind == "ok" else kind
                            trace.append(Observation("reset", value, error))
                        else:  # caller-side reset
                            token = tokens[op.token_index % len(tokens)]
                            error = None
                            try:
                                var.reset(token)
                            except (RuntimeError, ValueError, TypeError) as exc:
                                error = type(exc).__name__
                            trace.append(Observation("reset", var.get(UNSET), error))
                    return trace, reset_pids

                isolated = contextvars.copy_context()
                return await asyncio.create_task(drive(), context=isolated)

        # Act
        wool_observations, reset_pids = await retry_grpc_internal(body)
        stdlib_observations = run_stdlib_oracle(script)

        # Assert
        assert observations_match(wool_observations, stdlib_observations), (
            describe_mismatch(wool_observations, stdlib_observations)
        )
        assert wool_observations[-1].error == "RuntimeError"
        assert len(set(reset_pids)) >= 2, (
            "the two remote resets never landed on different workers, so the "
            "cross-worker ledger relay the round-robin pool advertises was "
            "not exercised"
        )
