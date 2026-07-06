"""Wire-lifted differential oracle for cross-process wool.Token parity.

Hypothesis draws strictly sequential op scripts over both actors
(caller and worker) and executes each script twice: through
:func:`run_wool_script` against a fresh :class:`wool.ContextVar` on a
live DEFAULT pool of real worker subprocesses, and through
:func:`run_stdlib_oracle` on a stdlib ``contextvars.ContextVar`` in a
single local context. An awaited dispatch is a continuation of the
caller's context, so the two observation traces must be identical for
every script. Known-regression scripts are pinned twice: as
``@example`` scripts on the property and as longhand deterministic
tests whose failures read without decoding oracle diffs.

The property shares ONE pool across all examples — pool startup is far
too expensive to repeat per example — by running the Hypothesis engine
off-loop in a worker thread (``asyncio.to_thread`` propagates the
pool-entered context) and submitting each example's script back onto
the pool's loop with ``run_coroutine_threadsafe``, the same
copied-context dispatch pattern ``test_dispatch_pairwise`` uses.
Examples are isolated by per-example variable keys: every script runs
against a freshly named wool.ContextVar, so chain state from one
example is invisible to the next.
"""

import asyncio
import uuid
from typing import Any

import pytest
from hypothesis import HealthCheck
from hypothesis import example
from hypothesis import given
from hypothesis import settings

import wool

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
from ._token_ops import run_wool_script
from ._token_ops import scripts
from .conftest import build_pool_from_scenario
from .conftest import default_scenario

#: Per-attempt budget for one script's dispatches (excludes pool build).
_EXAMPLE_TIMEOUT = 30

#: Thread-side wait on a submitted example — covers every retry attempt
#: of ``retry_grpc_internal`` (4 x 30 s plus backoff) with headroom.
_RESULT_TIMEOUT = 150


def _fresh_var() -> wool.ContextVar[Any]:
    """Mint a uniquely keyed wool.ContextVar for one script execution."""
    return wool.ContextVar(f"a4_{uuid.uuid4().hex}")


@pytest.mark.integration
class TestTokenParityOracle:
    @pytest.mark.asyncio
    async def test_caller_reset_should_raise_when_worker_already_consumed_token(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a caller retry of a worker-consumed token is rejected.

        Given:
            A DEFAULT pool and a script where the caller sets the
            variable, a worker resets the caller-minted token, and the
            caller sets a new value before retrying the same token.
        When:
            The script executes through run_wool_script against a
            fresh wool.ContextVar.
        Then:
            It should observe the worker reset unbinding the variable,
            and the caller's retry of the consumed token should
            observe RuntimeError with the newest value left in place —
            matching stdlib single-use token semantics.
        """

        # Arrange, act, & assert
        async def body():
            script = [
                SetOp(CALLER, 1),
                ResetOp(WORKER, 0),
                SetOp(CALLER, 2),
                ResetOp(CALLER, 0),
            ]
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                observations = await run_wool_script(script, _fresh_var())
            assert len(observations) == 4
            assert observations[0] == Observation("set", 1, None)
            assert observations[1] == Observation("reset", UNSET, None)
            assert observations[2] == Observation("set", 2, None)
            assert observations[3] == Observation("reset", 2, "RuntimeError")

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_resets_should_restore_captured_values_when_applied_out_of_order(
        self, credentials_map, retry_grpc_internal
    ):
        """Test out-of-order resets restore each token's captured value.

        Given:
            A DEFAULT pool and a script that interleaves caller and
            worker sets with resets applied out of mint order — the
            worker unwinds the second token, the caller sets a third
            value, then the caller resets the first and third tokens.
        When:
            The script executes through run_wool_script against a
            fresh wool.ContextVar.
        Then:
            It should restore each reset token's captured old value in
            turn — second token back to the first set, first token
            back to unset, third token back to the first set's value —
            exactly as stdlib contextvars does locally.
        """

        # Arrange, act, & assert
        async def body():
            script = [
                SetOp(CALLER, 1),
                SetOp(WORKER, 2),
                ResetOp(WORKER, 1),
                SetOp(CALLER, 3),
                ResetOp(CALLER, 0),
                ResetOp(CALLER, 2),
            ]
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                observations = await run_wool_script(script, _fresh_var())
            assert len(observations) == 6
            assert observations[0] == Observation("set", 1, None)
            assert observations[1] == Observation("set", 2, None)
            assert observations[2] == Observation("reset", 1, None)
            assert observations[3] == Observation("set", 3, None)
            assert observations[4] == Observation("reset", UNSET, None)
            assert observations[5] == Observation("reset", 1, None)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_minted_tokens_should_unwind_when_reset_from_both_actors(
        self, credentials_map, retry_grpc_internal
    ):
        """Test worker-minted tokens unwind from either side of the wire.

        Given:
            A DEFAULT pool and a worker-heavy script — both sets and
            their tokens minted on workers, the second token reset by
            the caller, the first reset by a worker, then a caller
            read.
        When:
            The script executes through run_wool_script against a
            fresh wool.ContextVar.
        Then:
            It should unwind LIFO to unset — the caller reset restores
            the first set's value, the worker reset restores unbound —
            and the final caller read should observe the unset
            sentinel with no errors anywhere.
        """

        # Arrange, act, & assert
        async def body():
            script = [
                SetOp(WORKER, 1),
                SetOp(WORKER, 2),
                ResetOp(CALLER, 1),
                ResetOp(WORKER, 0),
                GetOp(CALLER),
            ]
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                observations = await run_wool_script(script, _fresh_var())
            assert len(observations) == 5
            assert observations[0] == Observation("set", 1, None)
            assert observations[1] == Observation("set", 2, None)
            assert observations[2] == Observation("reset", 1, None)
            assert observations[3] == Observation("reset", UNSET, None)
            assert observations[4] == Observation("get", UNSET, None)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_second_worker_reset_should_report_runtime_error_when_token_spent(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a double remote reset of one token is rejected remotely.

        Given:
            A DEFAULT pool and a script where the caller sets the
            variable once and a worker resets the caller-minted token
            twice in a row.
        When:
            The script executes through run_wool_script against a
            fresh wool.ContextVar.
        Then:
            It should observe the first worker reset unbinding the
            variable and the second worker reset reporting
            RuntimeError with the variable still unbound — the
            consumed state survives the hop back to the caller and out
            to the next dispatch.
        """

        # Arrange, act, & assert
        async def body():
            script = [
                SetOp(CALLER, 1),
                ResetOp(WORKER, 0),
                ResetOp(WORKER, 0),
            ]
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                observations = await run_wool_script(script, _fresh_var())
            assert len(observations) == 3
            assert observations[0] == Observation("set", 1, None)
            assert observations[1] == Observation("reset", UNSET, None)
            assert observations[2] == Observation("reset", UNSET, "RuntimeError")

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_run_wool_script_should_match_stdlib_oracle_when_scripts_are_drawn(
        self, credentials_map, retry_grpc_internal
    ):
        """Test drawn op scripts agree wool-vs-stdlib across a live pool.

        Given:
            Scripts of up to twelve strictly sequential set/reset/get
            ops, each tagged caller or worker, with int/str values and
            token indexes drawn with replacement, plus four pinned
            regression scripts, all executed against one shared
            DEFAULT pool of real worker subprocesses.
        When:
            Each script runs through run_wool_script against a fresh
            wool.ContextVar and, separately, through
            run_stdlib_oracle.
        Then:
            It should produce identical observation traces for every
            script — location transparency holds over the whole drawn
            op-script space.
        """
        # Arrange — build ONE pool for every Hypothesis example; the
        # engine runs off-loop so each example can round-trip through
        # the pool entered here.
        scenario = default_scenario()
        async with build_pool_from_scenario(scenario, credentials_map):
            loop = asyncio.get_running_loop()

            async def execute_on_pool(script: list[Op]) -> list[Observation]:
                async def attempt() -> list[Observation]:
                    async with asyncio.timeout(_EXAMPLE_TIMEOUT):
                        return await run_wool_script(script, _fresh_var())

                return await retry_grpc_internal(attempt)

            @settings(
                max_examples=30,
                deadline=None,
                suppress_health_check=[HealthCheck.too_slow],
            )
            @example(
                script=[
                    SetOp(CALLER, 1),
                    ResetOp(WORKER, 0),
                    SetOp(CALLER, 2),
                    ResetOp(CALLER, 0),
                ]
            )
            @example(
                script=[
                    SetOp(CALLER, 1),
                    SetOp(WORKER, 2),
                    ResetOp(WORKER, 1),
                    SetOp(CALLER, 3),
                    ResetOp(CALLER, 0),
                    ResetOp(CALLER, 2),
                ]
            )
            @example(
                script=[
                    SetOp(WORKER, 1),
                    SetOp(WORKER, 2),
                    ResetOp(CALLER, 1),
                    ResetOp(WORKER, 0),
                    GetOp(CALLER),
                ]
            )
            @example(
                script=[
                    SetOp(CALLER, 1),
                    ResetOp(WORKER, 0),
                    ResetOp(WORKER, 0),
                ]
            )
            @given(script=scripts())
            def check_parity(script: list[Op]) -> None:
                future = asyncio.run_coroutine_threadsafe(execute_on_pool(script), loop)
                wool_observations = future.result(timeout=_RESULT_TIMEOUT)
                stdlib_observations = run_stdlib_oracle(script)
                assert observations_match(wool_observations, stdlib_observations), (
                    describe_mismatch(wool_observations, stdlib_observations)
                )

            # Act & assert
            await asyncio.to_thread(check_parity)
