"""Cross-process wool.Token parity — rows T5-T8: a worker mints the token.

Matrix rows covered here, complementing the caller-mints rows owned by
sibling lane files:

- T5: a worker sets a var and the minted token rides home; the awaiting
  caller observes the back-propagated value, resets the token (both
  previously-unset and non-default priors), and any further use of the
  consumed token is rejected everywhere — including across a subsequent
  dispatch and after a fresh caller-side set.
- T6: the token returns to the SAME worker (DEFAULT pool, exactly one
  worker process) on a later dispatch and is consumed there; every
  subsequent reset attempt — third dispatch or caller-side — raises the
  single-use RuntimeError.
- T7: the token is minted on one worker of a two-worker EPHEMERAL pool
  and consumed exactly once as reset attempts round-robin across BOTH
  workers; the consumed-token ledger follows the token across both
  processes and home.
- T8: nested-dispatch mints — an outer worker mints and the inner
  worker consumes (``outer_mint_inner_reset``), a worker-minted token
  returned home is consumed through a two-hop nested relay
  (``relay_reset_tenant_id_token``), and an INNER worker mints via a
  nested dispatch with the token riding two hops home for the caller
  to reset (``nested_oracle_set_report``).
"""

import gc
import os
import uuid

import pytest

import wool

from . import routines
from .conftest import LazyMode
from .conftest import PoolMode
from .conftest import QuorumMode
from .conftest import build_pool_from_scenario
from .conftest import default_scenario

#: Caller-side fallback mirroring the worker oracle's unbound sentinel,
#: so caller and worker observations of "unset" compare equal.
UNSET = routines.ORACLE_UNSET


def _fresh_var() -> wool.ContextVar[str]:
    """Mint a unique, default-less wool.ContextVar for a single test body."""
    return wool.ContextVar(f"a3_{uuid.uuid4().hex}")


@pytest.mark.integration
class TestWorkerMintedTokenParity:
    """T5/T6 — a worker-minted token honored at home and on the same worker."""

    @pytest.mark.asyncio
    async def test_worker_minted_token_should_restore_unset_state_when_prior_was_unset(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker-minted token restores the previously-unset state.

        Given:
            A default-less wool.ContextVar that is unbound on the caller
            and a DEFAULT pool whose worker sets it via the oracle set
            routine, returning the minted token.
        When:
            The caller resets the returned token after observing the
            back-propagated value.
        Then:
            It should restore the exact pre-set state — get() raises
            LookupError again, mirroring stdlib old-value semantics for
            a previously-unset variable — and a second caller reset
            should raise the single-use RuntimeError.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                with pytest.raises(LookupError):
                    var.get()

                # Act
                token = await routines.oracle_set(var.namespace, var.name, "t5-value")

                # Act & assert
                assert var.get(UNSET) == "t5-value"
                var.reset(token)
                with pytest.raises(LookupError):
                    var.get()
                assert var.get(UNSET) == UNSET
                with pytest.raises(RuntimeError, match="has already been used once"):
                    var.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_minted_token_should_restore_prior_value_when_caller_preset_it(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker-minted token restores a caller-set non-default prior.

        Given:
            A wool.ContextVar the caller set to a prior value before
            dispatch and a DEFAULT pool whose worker overwrites it via
            the oracle set routine, returning the minted token.
        When:
            The caller resets the worker's token and then resets its own
            earlier caller-minted token.
        Then:
            It should restore the caller's prior value exactly, reject a
            second use of the worker token with RuntimeError while
            leaving the value intact, and still honor the caller's own
            unconsumed token afterwards — per-token single-use, exactly
            as stdlib tracks each token independently.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                prior_token = var.set("t5-prior")

                # Act
                token = await routines.oracle_set(var.namespace, var.name, "t5-override")

                # Act & assert
                assert var.get(UNSET) == "t5-override"
                var.reset(token)
                assert var.get() == "t5-prior"
                with pytest.raises(RuntimeError, match="has already been used once"):
                    var.reset(token)
                assert var.get() == "t5-prior"
                var.reset(prior_token)
                with pytest.raises(LookupError):
                    var.get()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_consumed_worker_token_should_stay_rejected_after_a_fresh_set(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a consumed worker-minted token stays dead across a fresh set.

        Given:
            A worker-minted token already consumed by a caller-side
            reset, then a fresh caller-side set minting an independent
            token on the same variable.
        When:
            The caller attempts to reset the consumed worker token
            again after the fresh set.
        Then:
            It should raise the single-use RuntimeError without
            mutating the variable — the fresh set must not resurrect
            the spent token — and the fresh token should remain
            resettable, matching stdlib set-reset-set-reset behavior.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = await routines.oracle_set(var.namespace, var.name, "t5-remote")
                var.reset(token)
                fresh_token = var.set("t5-fresh")

                # Act & assert
                with pytest.raises(RuntimeError, match="has already been used once"):
                    var.reset(token)
                assert var.get() == "t5-fresh"
                var.reset(fresh_token)
                assert var.get(UNSET) == UNSET

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_consumed_worker_token_should_report_used_when_redispatched(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a caller-consumed worker token is refused by a later dispatch.

        Given:
            A worker-minted token consumed by a caller-side reset after
            it rode home on the response frame.
        When:
            The caller dispatches the oracle reset routine with the
            consumed token as an argument.
        Then:
            It should report a RuntimeError outcome carrying the
            single-use message and leave the worker-side variable
            unbound — the consumed state travels forward on the next
            dispatch frame, enforcing single-use across processes.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = await routines.oracle_set(var.namespace, var.name, "t5-once")
                var.reset(token)

                # Act
                kind, message, value_after = await routines.oracle_reset(
                    var.namespace, var.name, token
                )

                # Assert
                assert kind == "RuntimeError"
                assert "has already been used once" in message
                assert value_after == UNSET
                assert var.get(UNSET) == UNSET

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_minted_token_should_reset_on_the_same_worker_when_sent_back(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the minting worker consumes its own token on a later dispatch.

        Given:
            A DEFAULT pool with exactly one worker process, a caller-set
            prior value, and a token minted on that worker by an earlier
            attributed oracle set dispatch.
        When:
            The caller dispatches the attributed oracle reset routine
            with the token as an argument.
        Then:
            It should report an ok outcome from the same pid that minted
            the token, with the value restored to the caller's prior on
            the worker and back-propagated home on the response merge.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var.set("t6-prior")
                mint_pid, token = await routines.oracle_set_report(
                    var.namespace, var.name, "t6-mint"
                )
                assert var.get(UNSET) == "t6-mint"

                # Act
                (
                    reset_pid,
                    kind,
                    message,
                    value_after,
                ) = await routines.oracle_reset_report(var.namespace, var.name, token)

                # Assert
                assert reset_pid == mint_pid
                assert (kind, message, value_after) == ("ok", "", "t6-prior")
                assert var.get(UNSET) == "t6-prior"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_consumed_token_should_be_rejected_by_worker_and_caller(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token consumed on its minting worker is dead everywhere after.

        Given:
            A DEFAULT pool with exactly one worker process where a
            second dispatch already consumed the worker-minted token
            via the oracle reset routine.
        When:
            A third dispatch re-resets the same token and the caller
            then resets its own returned copy.
        Then:
            It should report a RuntimeError outcome with the single-use
            message from the same worker, and the caller-side reset
            should raise the same RuntimeError — the spent ledger, not
            a local used flag, rejects the caller's never-locally-used
            copy.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                mint_pid, token = await routines.oracle_set_report(
                    var.namespace, var.name, "t6-remote"
                )
                (
                    first_pid,
                    first_kind,
                    _,
                    first_value,
                ) = await routines.oracle_reset_report(var.namespace, var.name, token)
                assert first_pid == mint_pid
                assert first_kind == "ok"
                assert first_value == UNSET

                # Act
                (
                    third_pid,
                    kind,
                    message,
                    value_after,
                ) = await routines.oracle_reset_report(var.namespace, var.name, token)

                # Assert
                assert third_pid == mint_pid
                assert kind == "RuntimeError"
                assert "has already been used once" in message
                assert value_after == UNSET
                with pytest.raises(RuntimeError, match="has already been used once"):
                    var.reset(token)
                assert var.get(UNSET) == UNSET

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_remotely_consumed_worker_token_should_stay_rejected_after_fresh_set(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a remotely consumed token stays dead after a fresh caller set.

        Given:
            A worker-minted token consumed on the worker by a later
            dispatch — so the caller's copy was never used locally and
            its rejection rides solely on the propagated spent ledger —
            followed by a fresh caller-side set on the same variable.
        When:
            The caller attempts to reset the remotely consumed token
            after the fresh set.
        Then:
            It should raise the single-use RuntimeError without
            mutating the variable — the fresh set's ledger housekeeping
            must not forget a remote consumption — and the fresh token
            should remain resettable afterwards.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = await routines.oracle_set(var.namespace, var.name, "t6-stale")
                kind, message, _ = await routines.oracle_reset(
                    var.namespace, var.name, token
                )
                assert (kind, message) == ("ok", "")
                fresh_token = var.set("t6-fresh")

                # Act & assert
                with pytest.raises(RuntimeError, match="has already been used once"):
                    var.reset(token)
                assert var.get() == "t6-fresh"
                var.reset(fresh_token)
                assert var.get(UNSET) == UNSET

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestWorkerMintedTokenLedgerRelay:
    """T7/T8 — the consumed-token ledger follows the token across workers
    and nested dispatch chains."""

    @pytest.mark.asyncio
    async def test_worker_minted_token_should_be_single_use_across_two_workers_and_home(
        self, credentials_map, retry_grpc_internal
    ):
        """Test one consumption is honored across both pool workers and home.

        Given:
            An EPHEMERAL pool with two eagerly started workers (quorum
            2, both guaranteed up) and a token minted on one worker by
            the attributed oracle set routine.
        When:
            The caller dispatches attributed oracle reset attempts
            repeatedly so the round-robin balancer reaches a pid other
            than the minting worker's, then retries once more and
            finally resets at home.
        Then:
            It should consume the token exactly once — the first
            attempt reports ok wherever it lands, including on the
            non-minting worker — and every subsequent attempt on either
            worker or the caller should raise or report the single-use
            RuntimeError, proving the ledger follows the token across
            both processes and home.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario(
                pool_mode=PoolMode.EPHEMERAL,
                lazy=LazyMode.EAGER,
                quorum=QuorumMode.ABOVE_DEFAULT,
            )
            async with build_pool_from_scenario(scenario, credentials_map):
                mint_pid, token = await routines.oracle_set_report(
                    var.namespace, var.name, "t7-value"
                )
                assert var.get(UNSET) == "t7-value"

                # Act — round-robin reaches the other worker within a
                # few dispatches; keep dispatching until a different
                # pid reports the reset outcome.
                attempts = []
                for _ in range(8):
                    report = await routines.oracle_reset_report(
                        var.namespace, var.name, token
                    )
                    attempts.append(report)
                    if report[0] != mint_pid:
                        break

                # Assert
                assert any(pid != mint_pid for pid, _, _, _ in attempts), (
                    "no reset attempt ever landed on the second worker, so "
                    "the cross-worker ledger relay was not exercised"
                )
                first_pid, first_kind, _, _ = attempts[0]
                assert first_kind == "ok"
                if first_pid != mint_pid:
                    # The other worker consumed the token fresh.
                    assert len(attempts) == 1
                for _, kind, message, _ in attempts[1:]:
                    assert kind == "RuntimeError"
                    assert "has already been used once" in message
                # One more worker attempt after consumption — on a
                # two-worker round-robin this lands on the pid that did
                # NOT perform the consuming reset.
                _, extra_kind, extra_message, _ = await routines.oracle_reset_report(
                    var.namespace, var.name, token
                )
                assert extra_kind == "RuntimeError"
                assert "has already been used once" in extra_message
                with pytest.raises(RuntimeError, match="has already been used once"):
                    var.reset(token)
                assert var.get(UNSET) == UNSET

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_consumed_token_forwarded_onward_should_be_rejected_downstream(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a consumed token forwarded onward is rejected by the ledger.

        Given:
            An EPHEMERAL two-worker pool and a worker-minted token returned
            home, dispatched to a routine that consumes it via a nested reset
            and then forwards the still-unused token onward through a further
            reset dispatch.
        When:
            The caller drives that relay, retrying with a fresh token until the
            consuming reset and the onward forward provably land on different
            worker processes.
        Then:
            On every attempt the nested reset should report ok and the onward
            reset should report the single-use RuntimeError; and at least one
            attempt should forward to a process distinct from the one that
            consumed the token — proving the consumed-token signal rode the
            request frame across the process boundary rather than being served
            by an intra-process ledger, so the reap never dropped the entry
            while a downstream dispatch still needed it.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario(
                pool_mode=PoolMode.EPHEMERAL,
                lazy=LazyMode.EAGER,
                quorum=QuorumMode.ABOVE_DEFAULT,
            )
            async with build_pool_from_scenario(scenario, credentials_map):
                # Act — a fresh token each attempt; single-use must hold every
                # time, and round-robin reaches a distinct onward process within
                # a few attempts.
                crossed = False
                for i in range(8):
                    _, token = await routines.oracle_set_report(
                        var.namespace, var.name, f"onward-{i}"
                    )
                    inner, downstream = await routines.relay_consume_then_forward_report(
                        var.namespace, var.name, token
                    )

                    # Assert — the nested reset consumes; the onward reset is
                    # rejected by the consumed-token ledger on the request frame.
                    assert inner[1] == "ok"
                    assert downstream[1] == "RuntimeError"
                    assert "has already been used once" in downstream[2]
                    if inner[0] != downstream[0]:
                        crossed = True
                        break

                # Assert — the onward hop reached a distinct process at least
                # once, so the consumed signal provably rode the wire.
                assert crossed, (
                    "the onward hop never reached a process distinct from the "
                    "consuming reset, so cross-process forwarding was not "
                    "exercised"
                )

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_ledger_should_stay_bounded_across_a_worker_mint_reset_loop(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the caller's ledger stays bounded across many worker-mint resets.

        Given:
            A DEFAULT pool and a single caller context in which the caller
            repeatedly has a worker mint a token, resets it, and drops it.
        When:
            The loop runs many times, each minting a fresh worker token,
            resetting it, then dropping the instance and collecting.
        Then:
            The caller's own spent-token ledger for the variable's key should
            stay bounded — a small constant, not growing with the loop count —
            since the caller reaps each consumed token it no longer holds an
            instance of rather than accumulating one entry per iteration.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            key = (var.namespace, var.name)
            k = 64
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                # Act
                for i in range(k):
                    _, token = await routines.oracle_set_report(
                        var.namespace, var.name, f"loop-{i}"
                    )
                    var.reset(token)
                    del token
                    gc.collect()

                # Assert
                caller_spent = wool.__chain__.get().spent_tokens.get(key, frozenset())
                assert len(caller_spent) <= 2, (
                    f"caller ledger grew across {k} iterations: {len(caller_spent)}"
                )

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_outer_minted_token_should_be_single_use_when_inner_worker_consumes(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an inner worker consumes an outer-minted token exactly once.

        Given:
            A DEFAULT pool and the nested routine in which the outer
            worker sets TENANT_ID, sends the minted token to an inner
            dispatch that resets it, then retries the reset locally.
        When:
            The caller awaits the nested routine with TENANT_ID at its
            default.
        Then:
            It should report the inner reset restoring the propagated
            default, the outer retry failing the single-use gate with
            RuntimeError — the consumed state rode the inner response
            frame back one hop — and the caller's own value unchanged
            after the whole set-and-reset chain nets out.
        """

        async def body():
            # Arrange
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                assert routines.TENANT_ID.get() == "unknown"

                # Act
                inner_value, kind, message = await routines.outer_mint_inner_reset()

                # Assert
                assert inner_value == "unknown"
                assert kind == "RuntimeError"
                assert "has already been used once" in message
                assert routines.TENANT_ID.get() == "unknown"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_inner_reset_of_outer_minted_token_should_restore_the_callers_prior(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the inner worker's reset restores the caller's preset prior.

        Given:
            A DEFAULT pool, TENANT_ID set to a non-default prior on the
            caller, and the nested routine in which the outer worker
            mints a token that the inner dispatch consumes.
        When:
            The caller awaits the nested routine and afterwards resets
            its own pre-dispatch token.
        Then:
            It should restore the caller's propagated prior on the
            inner reset, back-propagate that prior home unchanged, and
            leave the caller's own unconsumed token valid — resetting
            it still restores the default, since worker-side
            consumptions are per-token.
        """

        async def body():
            # Arrange
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                prior_token = routines.TENANT_ID.set("t8-prior")

                # Act
                inner_value, kind, message = await routines.outer_mint_inner_reset()

                # Assert
                assert inner_value == "t8-prior"
                assert kind == "RuntimeError"
                assert "has already been used once" in message
                assert routines.TENANT_ID.get() == "t8-prior"
                routines.TENANT_ID.reset(prior_token)
                assert routines.TENANT_ID.get() == "unknown"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_minted_token_should_be_consumed_through_a_nested_relay_chain(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker-minted token returned home dies via a nested relay.

        Given:
            A DEFAULT pool, a TENANT_ID token minted on a worker and
            returned to the awaiting caller, and the relay routine that
            forwards a token through a nested dispatch whose innermost
            frame resets it.
        When:
            The caller dispatches the relay with the worker-minted
            token, sending it caller-to-outer-to-inner before the
            consumption, then resets its own copy at home.
        Then:
            It should restore the pre-mint default at the innermost
            frame, back-propagate the restore across both hops home,
            and reject the caller's copy with the single-use
            RuntimeError — the consumed-token ledger followed the token
            through the entire nested chain and back.
        """

        async def body():
            # Arrange
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                assert routines.TENANT_ID.get() == "unknown"
                token = await routines.set_tenant_id_and_return_token("t8-relay")
                assert routines.TENANT_ID.get() == "t8-relay"

                # Act
                restored = await routines.relay_reset_tenant_id_token(token)

                # Assert
                assert restored == "unknown"
                assert routines.TENANT_ID.get() == "unknown"
                with pytest.raises(RuntimeError, match="has already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_inner_minted_token_should_reset_at_home_when_ridden_two_hops(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a nested-dispatch-minted token is caller-resettable after two hops.

        Given:
            A fresh default-less var and a two-worker EPHEMERAL pool
            with both workers eagerly started.
        When:
            The caller dispatches nested_oracle_set_report — the outer
            worker dispatches oracle_set back into the pool, so an
            inner worker mints the token — and the token rides home
            through both response frames for the caller to reset.
        Then:
            It should attribute the mint to worker processes distinct
            from the caller, back-propagate the inner set home, reset
            exactly once at home restoring the unset state, and reject
            every further use — both the caller retry and a worker
            retry.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario(
                pool_mode=PoolMode.EPHEMERAL,
                lazy=LazyMode.EAGER,
                quorum=QuorumMode.ABOVE_DEFAULT,
            )
            async with build_pool_from_scenario(scenario, credentials_map):
                # Act
                outer_pid, inner_pid, token = await routines.nested_oracle_set_report(
                    var.namespace, var.name, "t8-inner"
                )

                # Assert
                caller_pid = os.getpid()
                assert outer_pid != caller_pid
                assert inner_pid != caller_pid
                assert token.var is var
                assert token.old_value is wool.Token.MISSING
                assert var.get(UNSET) == "t8-inner"
                var.reset(token)
                assert var.get(UNSET) == UNSET
                with pytest.raises(RuntimeError, match="has already been used once"):
                    var.reset(token)
                kind, _message, value_after = await routines.oracle_reset(
                    var.namespace, var.name, token
                )
                assert kind == "RuntimeError"
                assert value_after == UNSET

        await retry_grpc_internal(body)
