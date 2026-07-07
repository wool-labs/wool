"""Streaming (async-generator dispatch) wool.Token parity integration tests.

Matrix dimension D5: token semantics across per-yield re-mounts of a
streaming dispatch. These tests extend the precedents in
``test_context_var_propagation.py`` (``TestTokenStreaming``,
``TestSelfDispatchStreamingVarMutation``,
``TestAsyncGenSetAndResetAcrossYield``) without duplicating them:

* A caller-side local reset mid-stream forward-propagates to later
  yield frames (the MID_STREAM_FORWARD direction, caller → worker per
  ``__anext__`` request frame).
* A token consumed inside a stream stays consumed after exhaustion —
  including after a fresh caller-side set (ledger retention) and on a
  worker-side retry (cross-process single-use).
* A worker-minted token that reaches the caller mid-stream is
  consumable exactly once; every later attempt at any subsequent yield
  step or after completion raises — per-step re-mounts must not
  re-mint anchors.
* After a stream of worker-side mutation cycles the caller's local
  set/reset behavior matches stdlib contextvars exactly.
"""

import asyncio
import contextvars
import uuid

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
from ._token_ops import WORKER
from ._token_ops import ResetOp
from ._token_ops import SetOp
from ._token_ops import run_stdlib_oracle
from .conftest import RoutineShape
from .conftest import build_pool_from_scenario
from .conftest import default_scenario

_SCRIPT_TIMEOUT = 30


@st.composite
def _stream_shapes(draw) -> tuple[int, int]:
    """Draw an ``(n_yields, reset_step)`` streaming shape.

    ``n_yields`` is the number of value yields (1..6); ``reset_step`` is
    the yield index at which the caller resets the worker-minted token,
    from 0 (before the first value yield) through ``n_yields`` (at the
    exact final yield, after the last value read).
    """
    n_yields = draw(st.integers(min_value=1, max_value=6))
    reset_step = draw(st.integers(min_value=0, max_value=n_yields))
    return n_yields, reset_step


@pytest_asyncio.fixture
async def streaming_pool(credentials_map):
    """Enter one async-generator pool shared by every property example.

    ``wool.__proxy__`` set here propagates into the property body
    (pytest-asyncio runs async fixtures and tests in one context), so
    every drawn example round-trips through the pool entered once.
    """
    scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
    async with build_pool_from_scenario(scenario, credentials_map) as pool:
        yield pool


@pytest.mark.integration
class TestStreamingTokenParity:
    @pytest.mark.asyncio
    async def test_streaming_should_keep_worker_and_caller_ledgers_bounded(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a streaming set/reset routine bounds both worker and caller ledgers.

        Given:
            A worker pool driving a streaming routine that performs many
            set-then-reset cycles on a fresh per-test wool.ContextVar, one per
            yield, reporting the worker-side spent-token ledger size after each
            cycle and back-propagating each consumed id to the caller's chain.
        When:
            The caller drives the async-generator dispatch to exhaustion, then
            reads its own chain's spent-token ledger for that var's key.
        Then:
            Every reported worker-side size should stay at most one — the single
            in-flight consumed id — and the caller's own ledger should stay a
            small constant, not growing with the cycle count, so a long-lived
            streaming chain reaps each collected token on both sides instead of
            accumulating one entry per cycle for the chain's lifetime.
        """

        async def body():
            # Arrange
            var = wool.ContextVar(f"a5_{uuid.uuid4().hex}")
            key = (var.namespace, var.name)
            n = 128
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                # Act
                sizes = [
                    size async for size in routines.stream_spent_ledger_size_for(*key, n)
                ]
                caller_spent = wool.__chain__.get().spent_tokens.get(key, frozenset())

                # Assert
                assert len(sizes) == n
                assert max(sizes) <= 1, f"worker ledger grew unbounded: {sizes}"
                assert len(caller_spent) <= 2, (
                    f"caller ledger grew across {n} cycles: {len(caller_spent)}"
                )

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_interleaved_streams_should_each_keep_the_ledger_bounded(
        self, credentials_map, retry_grpc_internal
    ):
        """Test two interleaved streams on one chain each keep the ledger bounded.

        Given:
            Two async-generator dispatches open concurrently on one armed caller
            chain, each performing per-yield set-then-reset-then-drop cycles on
            its own distinct variable and reporting its per-key spent size.
        When:
            The caller advances both generators in strict alternation to
            exhaustion.
        Then:
            Each stream's reported per-key size should stay at most one, and the
            caller's own ledger for each key stays bounded — the reap keeps each
            key O(1) even as two runners fold consumed ids into one shared chain
            across alternating re-mounts.
        """

        # Arrange, act, & assert
        async def body():
            n = 40
            var_a = wool.ContextVar(f"a5_{uuid.uuid4().hex}")
            var_b = wool.ContextVar(f"a5_{uuid.uuid4().hex}")
            key_a = (var_a.namespace, var_a.name)
            key_b = (var_b.namespace, var_b.name)
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                gen_a = routines.stream_spent_ledger_size_for(*key_a, n)
                gen_b = routines.stream_spent_ledger_size_for(*key_b, n)
                sizes_a: list[int] = []
                sizes_b: list[int] = []
                for _ in range(n):
                    sizes_a.append(await anext(gen_a))
                    sizes_b.append(await anext(gen_b))
                assert len(sizes_a) == n and len(sizes_b) == n
                assert max(sizes_a) <= 1, f"stream A grew unbounded: {sizes_a}"
                assert max(sizes_b) <= 1, f"stream B grew unbounded: {sizes_b}"
                caller = wool.__chain__.get()
                assert len(caller.spent_tokens.get(key_a, frozenset())) <= 2
                assert len(caller.spent_tokens.get(key_b, frozenset())) <= 2

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_reset_mid_stream_should_forward_propagate_to_later_yields(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a caller-side local reset reaches later yield frames.

        Given:
            A caller that minted a TENANT_ID token before opening a
            streaming dispatch of an async generator that yields the
            var's current value on every iteration.
        When:
            The caller resets the token locally between the second and
            third yields.
        Then:
            The yields before the reset should observe the caller's
            set value, every yield after it should observe the
            restored default — the reset forward-propagates on the
            next ``__anext__`` request frame — and a caller-side
            re-reset should raise the single-use RuntimeError.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("pre-stream")
                gen = routines.stream_tenant_id_echo(4)
                try:
                    assert await anext(gen) == "pre-stream"
                    assert await anext(gen) == "pre-stream"
                    routines.TENANT_ID.reset(token)
                    assert routines.TENANT_ID.get() == "unknown"
                    assert await anext(gen) == "unknown"
                    assert await anext(gen) == "unknown"
                    with pytest.raises(StopAsyncIteration):
                        await anext(gen)
                finally:
                    await gen.aclose()
                assert routines.TENANT_ID.get() == "unknown"
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_stream_consumed_token_should_stay_rejected_when_caller_sets_fresh(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a stream-consumed token stays rejected past a fresh set.

        Given:
            A caller-minted TENANT_ID token consumed by an async
            generator that resets it between its two yields, with the
            stream driven to exhaustion.
        When:
            The caller retries the reset, sets a fresh value, and
            retries the old token again.
        Then:
            It should raise the single-use RuntimeError on both
            retries — the fresh set must not evict the consumed-token
            ledger entry — while the fresh token itself resets
            normally, restoring the default.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("stream-take")
                gen = routines.stream_reset_tenant_id_token(token)
                collected: list[str] = []
                try:
                    async for value in gen:
                        collected.append(value)
                finally:
                    await gen.aclose()
                assert collected == ["stream-take", "unknown"]
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)
                fresh = routines.TENANT_ID.set("fresh-after-stream")
                assert routines.TENANT_ID.get() == "fresh-after-stream"
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)
                routines.TENANT_ID.reset(fresh)
                assert routines.TENANT_ID.get() == "unknown"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_stream_consumed_token_should_be_rejected_on_a_worker_after_stream(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a stream-consumed token is rejected by a later dispatch.

        Given:
            A caller-minted TENANT_ID token consumed mid-stream by an
            async generator that resets it between yields, with the
            stream driven to exhaustion.
        When:
            The caller dispatches the same token to a coroutine that
            retries the reset on a worker, then retries locally.
        Then:
            The worker retry should report the single-use RuntimeError
            and the local retry should raise it — the consumed state
            recorded mid-stream binds every later attempt on either
            side of the wire.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("stream-take-worker")
                gen = routines.stream_reset_tenant_id_token(token)
                try:
                    async for _ in gen:
                        pass
                finally:
                    await gen.aclose()
                _, kind, message = await routines.reset_tenant_id_token_report(token)
                assert kind == "RuntimeError"
                assert "already been used once" in message
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_minted_token_should_reset_exactly_once_when_used_mid_stream(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker-minted token consumed mid-stream never revives.

        Given:
            An async generator that sets TENANT_ID on the worker,
            yields the minted token first, and yields the current
            value three more times across suspensions.
        When:
            The caller resets the yielded token mid-stream — after the
            first value yield — and then retries the reset at every
            remaining yield step and after exhaustion.
        Then:
            The mid-stream reset should succeed, restoring the default
            for the caller and for every later worker-side read, and
            every retry should raise the single-use RuntimeError —
            per-step re-mounts must not re-mint the token's anchor.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.set_tenant_id_and_yield_token("stream-mint", 3)
                try:
                    token = await anext(gen)
                    assert await anext(gen) == "stream-mint"
                    routines.TENANT_ID.reset(token)
                    assert routines.TENANT_ID.get() == "unknown"
                    later_values: list[str] = []
                    async for value in gen:
                        later_values.append(value)
                        with pytest.raises(RuntimeError, match="already been used once"):
                            routines.TENANT_ID.reset(token)
                finally:
                    await gen.aclose()
                assert later_values == ["unknown", "unknown"]
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_unary_mint_interleaved_with_stream_should_stay_single_use(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token minted by a unary dispatch mid-stream stays single-use.

        Given:
            A fresh default-less wool.ContextVar, an open streaming
            dispatch, and a unary ``oracle_set`` dispatch issued
            between two yield steps that mints a token on the worker.
        When:
            The caller resets the returned token after driving the
            stream to exhaustion, then retries locally and via a
            worker-side ``oracle_reset``.
        Then:
            The worker's set should back-propagate to the caller
            mid-stream, the first reset should succeed unbinding the
            var, and both retries should surface the single-use
            RuntimeError — the interleaved stream frames must not
            re-mint the unary token's anchor.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                var: wool.ContextVar[str] = wool.ContextVar(
                    f"a6_stream_{uuid.uuid4().hex}",
                    namespace=f"a6_ns_{uuid.uuid4().hex}",
                )
                unset = routines.ORACLE_UNSET
                gen = routines.stream_tenant_id_echo(3)
                try:
                    await anext(gen)
                    token = await routines.oracle_set(
                        var.namespace, var.name, "minted-mid-stream"
                    )
                    assert var.get(unset) == "minted-mid-stream"
                    await anext(gen)
                    await anext(gen)
                    with pytest.raises(StopAsyncIteration):
                        await anext(gen)
                finally:
                    await gen.aclose()
                var.reset(token)
                assert var.get(unset) == unset
                with pytest.raises(RuntimeError, match="already been used once"):
                    var.reset(token)
                kind, message, value_after = await routines.oracle_reset(
                    var.namespace, var.name, token
                )
                assert kind == "RuntimeError"
                assert "already been used once" in message
                assert value_after == unset

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_post_stream_local_ledger_should_match_stdlib_after_worker_cycles(
        self, credentials_map, retry_grpc_internal
    ):
        """Test post-stream local set/reset behavior matches stdlib.

        Given:
            A caller that consumed one local TENANT_ID token before the
            stream and an async generator that sets the var on the
            worker on every one of its three yields, back-propagating
            each mutation.
        When:
            The caller performs a fresh set/reset cycle after the
            stream and then retries the pre-stream token.
        Then:
            The fresh token should reset normally — restoring the
            final back-propagated worker value as its old value, per
            stdlib non-LIFO reset semantics — and the pre-stream token
            should raise the single-use RuntimeError, proving the
            consumed ledger survived N per-yield back-propagations.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                pre_stream = routines.TENANT_ID.set("pre-stream-cycle")
                routines.TENANT_ID.reset(pre_stream)
                assert routines.TENANT_ID.get() == "unknown"
                collected: list[str] = []
                async for value in routines.mutate_on_each_yield(3):
                    collected.append(value)
                assert collected == ["step-0", "step-1", "step-2"]
                assert routines.TENANT_ID.get() == "step-2"
                fresh = routines.TENANT_ID.set("fresh-post-stream")
                assert routines.TENANT_ID.get() == "fresh-post-stream"
                routines.TENANT_ID.reset(fresh)
                assert routines.TENANT_ID.get() == "step-2"
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(pre_stream)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    @settings(
        max_examples=20,
        deadline=None,
        suppress_health_check=[
            HealthCheck.function_scoped_fixture,
            HealthCheck.too_slow,
        ],
    )
    @example(shape=(1, 0))  # single-yield stream, reset before the first yield
    @example(shape=(1, 1))  # single-yield stream, reset at the exact final yield
    @example(shape=(6, 0))  # longest stream, reset before the first yield
    @example(shape=(6, 6))  # longest stream, reset at the exact final yield
    @given(shape=_stream_shapes())
    async def test_worker_minted_token_should_be_single_use_over_stream_shapes(
        self, shape, streaming_pool, retry_grpc_internal
    ):
        """Test a mid-stream reset stays single-use across every stream shape.

        Given:
            A worker-minted TENANT_ID token yielded first by an N-yield
            streaming dispatch (1 <= N <= 6) and a caller-side reset
            applied at a drawn step in [0, N] — before the first value
            yield, at any interior step, or at the exact final yield —
            over one async-generator pool shared across all examples.
        When:
            The caller drives the stream to exhaustion, resetting the
            token at the drawn step, then retries the reset.
        Then:
            Every value from the reset step onward should observe the
            restored default that a run_stdlib_oracle projection of the
            equivalent worker-set-then-reset script predicts, and the
            retry should raise the single-use RuntimeError — per-yield
            re-mounts must not re-mint the token's anchor.
        """
        # Arrange
        n_yields, reset_step = shape
        minted = "minted"
        default = "unknown"  # routines.TENANT_ID's constructor default
        oracle = run_stdlib_oracle([SetOp(WORKER, minted), ResetOp(CALLER, 0)])
        restored = oracle[-1].value
        expected_restored = default if restored == routines.ORACLE_UNSET else restored

        async def run() -> tuple[list[str], str]:
            gen = routines.set_tenant_id_and_yield_token(minted, n_yields)
            observed: list[str] = []
            try:
                token = await anext(gen)
                for step in range(n_yields):
                    if step == reset_step:
                        routines.TENANT_ID.reset(token)
                    observed.append(await anext(gen))
                if reset_step == n_yields:
                    routines.TENANT_ID.reset(token)
                with pytest.raises(StopAsyncIteration):
                    await anext(gen)
            finally:
                await gen.aclose()
            restored_value = routines.TENANT_ID.get()
            with pytest.raises(RuntimeError, match="already been used once"):
                routines.TENANT_ID.reset(token)
            return observed, restored_value

        async def body() -> tuple[list[str], str]:
            async with asyncio.timeout(_SCRIPT_TIMEOUT):
                isolated = contextvars.copy_context()
                return await asyncio.create_task(run(), context=isolated)

        # Act
        observed, restored_value = await retry_grpc_internal(body)

        # Assert
        expected = [
            minted if step < reset_step else expected_restored
            for step in range(n_yields)
        ]
        assert observed == expected
        assert restored_value == expected_restored
