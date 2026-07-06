"""Multi-variable wool.Token parity across the dispatch wire (lane A8).

Chain state carries EVERY registered wool.ContextVar's bookkeeping on
every dispatch frame, so one variable's token ledger travels the wire
alongside every other variable's — a real interleaving surface. These
tests pin per-variable independence of that bookkeeping: a remote
consumption touches only the consumed variable's ledger, a fresh set
prunes only its own variable's spent key (the just-fixed regression),
wrong-variable rejections keep stdlib's error precedence across
processes (used beats wrong-var), and any interleaved multi-variable
script decomposes into independent per-variable stdlib traces — each
variable behaves exactly as if the others did not exist (the per-var
projection property).
"""

from __future__ import annotations

import asyncio
import contextvars
import uuid
from collections.abc import Sequence
from typing import Any

import pytest
import pytest_asyncio
from hypothesis import HealthCheck
from hypothesis import example
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool

from . import routines
from ._token_ops import CALLER
from ._token_ops import UNSET
from ._token_ops import WORKER
from ._token_ops import GetOp
from ._token_ops import Observation
from ._token_ops import Op
from ._token_ops import ResetOp
from ._token_ops import SetOp
from ._token_ops import describe_mismatch
from ._token_ops import observations_match
from ._token_ops import run_stdlib_oracle
from ._token_ops import scripts
from .conftest import build_pool_from_scenario
from .conftest import default_scenario

_SCRIPT_TIMEOUT = 30

#: A tagged multi-var replay of the just-fixed ledger-prune regression:
#: var A's first token is consumed remotely, a fresh set on A prunes
#: A's spent key, and the caller's retry of the consumed token must
#: still observe RuntimeError — while var B's token, minted before the
#: whole A cycle, must stay cleanly spendable throughout.
_LEDGER_PRUNE_REGRESSION: list[tuple[int, Op]] = [
    (0, SetOp(CALLER, 1)),
    (1, SetOp(CALLER, 10)),
    (0, ResetOp(WORKER, 0)),
    (0, SetOp(CALLER, 2)),
    (0, ResetOp(CALLER, 0)),
    (1, ResetOp(CALLER, 0)),
]


def _fresh_var() -> wool.ContextVar[Any]:
    """Mint a unique default-less wool.ContextVar for one test or example."""
    return wool.ContextVar(
        f"a8_{uuid.uuid4().hex}", namespace=f"a8_ns_{uuid.uuid4().hex}"
    )


def _project(tagged_script: Sequence[tuple[int, Op]], var_index: int) -> list[Op]:
    """Return the ops tagged for *var_index* in interleaving order.

    The projection preserves per-var op order and per-var token-list
    indexing: because the multi-var executor keeps one token list per
    variable, a var's projected script replayed through the single-var
    stdlib oracle selects exactly the tokens the interleaved run did.
    """
    return [op for tag, op in tagged_script if tag == var_index]


def _tagged_scripts(*, max_ops: int = 8) -> st.SearchStrategy[list[tuple[int, Op]]]:
    """Draw an interleaved two-var script: ops plus per-op var tags.

    Each op drawn by ``_token_ops.scripts`` is assigned to var A (0) or
    var B (1) via a boolean list of matching length. Reset token
    indexes are interpreted against the tagged var's OWN token list at
    execution time, so every tagging yields a valid per-var projection.
    """

    def tag(script: list[Op]) -> st.SearchStrategy[list[tuple[int, Op]]]:
        return st.lists(st.booleans(), min_size=len(script), max_size=len(script)).map(
            lambda tags: [(int(t), op) for t, op in zip(tags, script)]
        )

    return scripts(max_ops=max_ops).flatmap(tag)


async def _run_wool_multivar_script(
    tagged_script: Sequence[tuple[int, Op]],
    variables: Sequence[wool.ContextVar[Any]],
) -> list[list[Observation]]:
    """Execute an interleaved multi-var script; return per-var traces.

    Mirrors ``_token_ops.run_wool_script`` op mechanics exactly — caller
    ops run locally, worker ops dispatch the key-addressed ``oracle_*``
    routines — except each op is tagged with the index of the variable
    it targets and every variable keeps its OWN ordered token list.
    ``ResetOp.token_index`` is normalized modulo the tagged variable's
    minted-token count, so each returned per-var observation trace is
    directly comparable to ``run_stdlib_oracle`` on that variable's
    projected script. Must run inside an entered pool context.
    """
    tokens: list[list[Any]] = [[] for _ in variables]
    projections: list[list[Observation]] = [[] for _ in variables]
    for var_index, op in tagged_script:
        var = variables[var_index]
        var_tokens = tokens[var_index]
        namespace = var.namespace
        name = var.name
        error: str | None = None
        if isinstance(op, SetOp):
            kind = "set"
            if op.actor == CALLER:
                var_tokens.append(var.set(op.value))
            else:
                var_tokens.append(await routines.oracle_set(namespace, name, op.value))
            value = var.get(UNSET)
        elif isinstance(op, ResetOp):
            kind = "reset"
            if not var_tokens:
                value = var.get(UNSET)
            else:
                token = var_tokens[op.token_index % len(var_tokens)]
                if op.actor == CALLER:
                    try:
                        var.reset(token)
                    except (RuntimeError, ValueError, TypeError) as exc:
                        error = type(exc).__name__
                    value = var.get(UNSET)
                else:
                    outcome_kind, _message, value = await routines.oracle_reset(
                        namespace, name, token
                    )
                    if outcome_kind != "ok":
                        error = outcome_kind
        elif isinstance(op, GetOp):
            kind = "get"
            if op.actor == CALLER:
                value = var.get(UNSET)
            else:
                value = await routines.oracle_get(namespace, name, UNSET)
        else:
            raise TypeError(f"unknown op type: {op!r}")
        projections[var_index].append(Observation(kind, value, error))
    return projections


@pytest_asyncio.fixture
async def default_pool(credentials_map):
    """Enter one DEFAULT pool and hold it for the whole test.

    Shared by every Hypothesis example of the projection property —
    ``wool.__proxy__`` set here propagates into the test body
    (pytest-asyncio runs async fixtures and tests in one context).
    """
    async with build_pool_from_scenario(default_scenario(), credentials_map) as pool:
        yield pool


@pytest.mark.integration
class TestMultiVarLedgerIndependence:
    """A consumption or fresh set on one var never disturbs another's ledger.

    These example tests pin the independence invariant on fixed two-var
    cycles. Its generalizing property home is ``TestPerVarProjection``:
    ``test_interleaved_scripts_should_decompose_into_stdlib_projections``
    sweeps drawn two-var interleavings and asserts each var decomposes
    into its own single-var stdlib projection as if the other did not
    exist.
    """

    @pytest.mark.asyncio
    async def test_worker_consumption_should_leave_other_vars_token_usable(
        self, credentials_map, retry_grpc_internal
    ):
        """Test consuming var A's token remotely leaves var B's token spendable.

        Given:
            Two fresh default-less wool.ContextVars A and B, each set
            once on the caller, and A's token dispatched to the oracle
            reset routine on a worker.
        When:
            The worker consumes A's token and the caller then resets B's
            token locally.
        Then:
            It should unbind A on both sides, leave B's value untouched
            by A's consumption, and spend B's token cleanly — B's ledger
            must be independent of A's remote consumption.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var_a = _fresh_var()
                var_b = _fresh_var()
                token_a = var_a.set("a-initial")
                token_b = var_b.set("b-initial")
                kind, message, value_after = await routines.oracle_reset(
                    var_a.namespace, var_a.name, token_a
                )
                assert (kind, message, value_after) == ("ok", "", UNSET)
                assert var_a.get(UNSET) == UNSET
                assert var_b.get(UNSET) == "b-initial"
                var_b.reset(token_b)
                assert var_b.get(UNSET) == UNSET

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_consumed_token_should_stay_spent_when_its_var_freshly_set(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a fresh set on A prunes only A's key without reviving A's token.

        Given:
            Fresh vars A and B each set once on the caller, A's token
            consumed on a worker, then a fresh caller-side set on A —
            the op that previously erased A's consumed-token ledger.
        When:
            The caller retries the consumed token locally and via the
            worker, then spends B's token and A's fresh token.
        Then:
            It should raise the single-use RuntimeError on both retries
            with A's value left intact, spend B's untouched token
            cleanly, and spend A's fresh token cleanly — the prune stays
            per-var and never resurrects a consumed token.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var_a = _fresh_var()
                var_b = _fresh_var()
                token_a = var_a.set("a1")
                token_b = var_b.set("b1")
                kind, message, value_after = await routines.oracle_reset(
                    var_a.namespace, var_a.name, token_a
                )
                assert (kind, message, value_after) == ("ok", "", UNSET)
                token_a2 = var_a.set("a2")
                with pytest.raises(RuntimeError, match="already been used once"):
                    var_a.reset(token_a)
                kind, message, value_after = await routines.oracle_reset(
                    var_a.namespace, var_a.name, token_a
                )
                assert kind == "RuntimeError"
                assert "already been used once" in message
                assert value_after == "a2"
                assert var_a.get(UNSET) == "a2"
                var_b.reset(token_b)
                assert var_b.get(UNSET) == UNSET
                var_a.reset(token_a2)
                assert var_a.get(UNSET) == UNSET

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_consumed_token_should_stay_spent_when_roles_swapped(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the symmetric cycle: consuming and re-setting B leaves A intact.

        Given:
            Fresh vars A and B each set once on the caller, B's token
            consumed on a worker, then a fresh caller-side set on B.
        When:
            The caller retries B's consumed token locally and via the
            worker, then spends A's token and B's fresh token.
        Then:
            It should raise the single-use RuntimeError on both retries
            of B's token, leave A's value and token family completely
            unaffected, and spend A's token and B's fresh token cleanly.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var_a = _fresh_var()
                var_b = _fresh_var()
                token_a = var_a.set("a1")
                token_b = var_b.set("b1")
                kind, message, value_after = await routines.oracle_reset(
                    var_b.namespace, var_b.name, token_b
                )
                assert (kind, message, value_after) == ("ok", "", UNSET)
                token_b2 = var_b.set("b2")
                with pytest.raises(RuntimeError, match="already been used once"):
                    var_b.reset(token_b)
                kind, message, value_after = await routines.oracle_reset(
                    var_b.namespace, var_b.name, token_b
                )
                assert kind == "RuntimeError"
                assert "already been used once" in message
                assert value_after == "b2"
                assert var_a.get(UNSET) == "a1"
                var_a.reset(token_a)
                assert var_a.get(UNSET) == UNSET
                var_b.reset(token_b2)
                assert var_b.get(UNSET) == UNSET

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestCrossVarTokenRejection:
    """A token presented to the wrong var keeps stdlib's error precedence."""

    @pytest.mark.asyncio
    async def test_reset_should_raise_value_error_when_token_from_other_var(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an unconsumed token presented to the wrong var raises ValueError.

        Given:
            Fresh vars A and B each set once on the caller, and A's
            live token presented to B's reset both locally and via the
            worker-side oracle addressed at B — mirrored against a
            stdlib ContextVar pair for parity.
        When:
            B rejects the foreign token at both sites.
        Then:
            It should raise the different-ContextVar ValueError in both
            places with both values untouched, leave A's token still
            spendable afterward, and match the stdlib pair's error.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var_a = _fresh_var()
                var_b = _fresh_var()
                token_a = var_a.set("a1")
                var_b.set("b1")
                stdlib_a: contextvars.ContextVar[str] = contextvars.ContextVar(
                    f"a8_stdlib_{uuid.uuid4().hex}"
                )
                stdlib_b: contextvars.ContextVar[str] = contextvars.ContextVar(
                    f"a8_stdlib_{uuid.uuid4().hex}"
                )
                stdlib_token_a = stdlib_a.set("a1")
                stdlib_b.set("b1")
                with pytest.raises(ValueError, match="different ContextVar"):
                    stdlib_b.reset(stdlib_token_a)
                with pytest.raises(ValueError, match="different ContextVar"):
                    var_b.reset(token_a)
                kind, message, value_after = await routines.oracle_reset(
                    var_b.namespace, var_b.name, token_a
                )
                assert kind == "ValueError"
                assert "different ContextVar" in message
                assert value_after == "b1"
                assert var_a.get(UNSET) == "a1"
                assert var_b.get(UNSET) == "b1"
                var_a.reset(token_a)
                assert var_a.get(UNSET) == UNSET

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_reset_should_raise_runtime_error_when_foreign_token_consumed(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a consumed token presented to the wrong var raises RuntimeError.

        Given:
            Fresh vars A and B each set once on the caller, A's token
            consumed on a worker, and the consumed token then presented
            to B's reset both locally and via the worker-side oracle
            addressed at B — mirrored against a stdlib ContextVar pair.
        When:
            B rejects the consumed foreign token at both sites.
        Then:
            It should raise the single-use RuntimeError in both places
            — the used gate beats the wrong-var gate, exactly as stdlib
            orders the checks — with B's value untouched.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var_a = _fresh_var()
                var_b = _fresh_var()
                token_a = var_a.set("a1")
                var_b.set("b1")
                stdlib_a: contextvars.ContextVar[str] = contextvars.ContextVar(
                    f"a8_stdlib_{uuid.uuid4().hex}"
                )
                stdlib_b: contextvars.ContextVar[str] = contextvars.ContextVar(
                    f"a8_stdlib_{uuid.uuid4().hex}"
                )
                stdlib_token_a = stdlib_a.set("a1")
                stdlib_b.set("b1")
                stdlib_a.reset(stdlib_token_a)
                with pytest.raises(RuntimeError, match="already been used once"):
                    stdlib_b.reset(stdlib_token_a)
                kind, message, value_after = await routines.oracle_reset(
                    var_a.namespace, var_a.name, token_a
                )
                assert (kind, message, value_after) == ("ok", "", UNSET)
                with pytest.raises(RuntimeError, match="already been used once"):
                    var_b.reset(token_a)
                kind, message, value_after = await routines.oracle_reset(
                    var_b.namespace, var_b.name, token_a
                )
                assert kind == "RuntimeError"
                assert "already been used once" in message
                assert value_after == "b1"
                assert var_b.get(UNSET) == "b1"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestPerVarProjection:
    """Multi-var interleavings decompose into independent per-var traces."""

    @pytest.mark.asyncio
    async def test_interleaved_cycles_should_match_per_var_stdlib_projections(
        self, default_pool, retry_grpc_internal
    ):
        """Test alternating two-var cross-process cycles decompose per var.

        Given:
            Two fresh wool.ContextVars A and B and a fixed script that
            alternates set/reset/get ops between the vars and between
            the caller and a worker, including a double reset on A that
            must fail while B's ops all succeed.
        When:
            The interleaved script executes against a live pool and each
            var's projected trace is compared with the stdlib oracle run
            on that var's projected ops alone.
        Then:
            It should match both projections exactly — one RuntimeError
            on A's doubly reset token, no errors on B — as if each var
            ran its subsequence with the other var absent.
        """
        # Arrange
        tagged_script: list[tuple[int, Op]] = [
            (0, SetOp(CALLER, "a1")),
            (1, SetOp(WORKER, "b1")),
            (0, SetOp(WORKER, "a2")),
            (1, GetOp(CALLER)),
            (0, ResetOp(WORKER, 1)),
            (1, SetOp(CALLER, "b2")),
            (0, ResetOp(CALLER, 1)),
            (1, ResetOp(CALLER, 1)),
            (0, GetOp(WORKER)),
            (1, GetOp(WORKER)),
            (0, ResetOp(CALLER, 0)),
            (1, ResetOp(WORKER, 0)),
            (0, GetOp(CALLER)),
            (1, GetOp(CALLER)),
        ]

        # Act
        async def body():
            async with asyncio.timeout(_SCRIPT_TIMEOUT):

                async def run() -> list[list[Observation]]:
                    variables = (_fresh_var(), _fresh_var())
                    return await _run_wool_multivar_script(tagged_script, variables)

                isolated = contextvars.copy_context()
                return await asyncio.create_task(run(), context=isolated)

        projections = await retry_grpc_internal(body)

        # Assert
        for var_index in (0, 1):
            expected = run_stdlib_oracle(_project(tagged_script, var_index))
            assert observations_match(projections[var_index], expected), (
                f"var {var_index} projection diverged:\n"
                + describe_mismatch(projections[var_index], expected)
            )
        errors_a = [entry.error for entry in projections[0] if entry.error]
        assert errors_a == ["RuntimeError"]
        assert all(entry.error is None for entry in projections[1])

    @pytest.mark.asyncio
    async def test_three_var_pipeline_should_consume_each_token_independently(
        self, default_pool, retry_grpc_internal
    ):
        """Test three vars' tokens consumed in scrambled order stay independent.

        Given:
            Three fresh wool.ContextVars A, B, and C set once each on
            the caller (C first), with C's token additionally round-
            tripped through a worker as unreset luggage — the nearest
            relay to a nested hop reachable with key-addressed routines.
        When:
            The tokens are consumed in an order scrambled against mint
            order — B locally, A on a worker, then C's returned wire
            copy on a worker — with the other vars observed after each
            consumption, and every original token retried locally.
        Then:
            It should leave the not-yet-consumed vars untouched at each
            step on both caller and worker, raise the single-use
            RuntimeError on every retry — including C's original after
            its wire copy was consumed — and end with each var matching
            its own single-var stdlib projection: unbound.
        """

        # Arrange, act, & assert
        async def body():
            async with asyncio.timeout(_SCRIPT_TIMEOUT):
                var_a = _fresh_var()
                var_b = _fresh_var()
                var_c = _fresh_var()
                token_c = var_c.set("c0")
                token_a = var_a.set("a0")
                token_b = var_b.set("b0")
                relayed, var_name = await routines.describe_tenant_id_token(token_c)
                assert var_name == var_c.name

                var_b.reset(token_b)
                assert var_b.get(UNSET) == UNSET
                assert var_a.get(UNSET) == "a0"
                worker_a = await routines.oracle_get(var_a.namespace, var_a.name, UNSET)
                assert worker_a == "a0"
                assert var_c.get(UNSET) == "c0"

                kind, message, value_after = await routines.oracle_reset(
                    var_a.namespace, var_a.name, token_a
                )
                assert (kind, message, value_after) == ("ok", "", UNSET)
                assert var_a.get(UNSET) == UNSET
                assert var_c.get(UNSET) == "c0"
                worker_c = await routines.oracle_get(var_c.namespace, var_c.name, UNSET)
                assert worker_c == "c0"

                kind, message, value_after = await routines.oracle_reset(
                    var_c.namespace, var_c.name, relayed
                )
                assert (kind, message, value_after) == ("ok", "", UNSET)
                assert var_c.get(UNSET) == UNSET

                with pytest.raises(RuntimeError, match="already been used once"):
                    var_a.reset(token_a)
                with pytest.raises(RuntimeError, match="already been used once"):
                    var_b.reset(token_b)
                with pytest.raises(RuntimeError, match="already been used once"):
                    var_c.reset(token_c)

                final_projections = [
                    (var_a, [SetOp(CALLER, "a0"), ResetOp(WORKER, 0)]),
                    (var_b, [SetOp(CALLER, "b0"), ResetOp(CALLER, 0)]),
                    (var_c, [SetOp(CALLER, "c0"), ResetOp(WORKER, 0)]),
                ]
                for var, script in final_projections:
                    assert var.get(UNSET) == run_stdlib_oracle(script)[-1].value

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    @settings(
        max_examples=15,
        deadline=None,
        suppress_health_check=[
            HealthCheck.function_scoped_fixture,
            HealthCheck.too_slow,
        ],
    )
    @example(tagged_script=_LEDGER_PRUNE_REGRESSION)
    @given(tagged_script=_tagged_scripts())
    async def test_interleaved_scripts_should_decompose_into_stdlib_projections(
        self, tagged_script, default_pool, retry_grpc_internal
    ):
        """Test drawn two-var interleavings decompose into per-var stdlib runs.

        Given:
            Any script of up to eight set/reset/get ops across both
            actors, each op tagged to one of two fresh wool.ContextVars
            by a drawn boolean list, one DEFAULT pool shared across all
            examples, and a pinned example replaying the ledger-prune
            regression on var A beside an untouched var B.
        When:
            The merged interleaving executes against the pool and each
            var's projected trace is compared with the stdlib oracle run
            on that var's projected subsequence alone.
        Then:
            It should match both projections exactly — multi-var
            interleaving must decompose so each var behaves as if the
            other did not exist.
        """

        # Arrange & act
        async def body():
            async with asyncio.timeout(_SCRIPT_TIMEOUT):

                async def run() -> list[list[Observation]]:
                    variables = (_fresh_var(), _fresh_var())
                    return await _run_wool_multivar_script(tagged_script, variables)

                isolated = contextvars.copy_context()
                return await asyncio.create_task(run(), context=isolated)

        projections = await retry_grpc_internal(body)

        # Assert
        for var_index in (0, 1):
            expected = run_stdlib_oracle(_project(tagged_script, var_index))
            assert observations_match(projections[var_index], expected), (
                f"var {var_index} projection diverged:\n"
                + describe_mismatch(projections[var_index], expected)
            )
