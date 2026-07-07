"""Integration tests for caller-minted wool.Token stdlib parity (rows T1-T4).

Each test mints a :class:`wool.Token` on the caller and drives one row of
the cross-process token-parity matrix: T1 (an unrelated dispatch merges its
response before a purely local reset), T2 (the token rides to a worker and
back unreset, then either copy is consumed locally), T3 (a worker consumes
the token and the consumption back-propagates), and T4 (the token relays
through one worker to a second, which consumes it). An awaited dispatch is
a logical continuation of the caller's context, so every sequence must
match the same ops applied to a stdlib ContextVar in a single context:
exact old-value restoration (non-default prior and previously-unset) and
single-use enforcement via RuntimeError on any second reset.
"""

import uuid
from dataclasses import replace

import pytest

import wool

from . import routines
from .conftest import CredentialType
from .conftest import LazyMode
from .conftest import PoolMode
from .conftest import QuorumMode
from .conftest import build_pool_from_scenario
from .conftest import default_scenario

# Curated pool configurations for the T3 remote-consumption row. The
# other rows run on the default scenario only. MTLS is the canonical
# secure credential variant (`replace` because ``default_scenario``
# pins INSECURE and ``Scenario.__or__`` rejects conflicting fields).
T3_POOL_SCENARIOS = [
    default_scenario(),
    default_scenario(pool_mode=PoolMode.EPHEMERAL),
    default_scenario(
        pool_mode=PoolMode.EPHEMERAL,
        lazy=LazyMode.EAGER,
        quorum=QuorumMode.ABOVE_DEFAULT,
    ),
    default_scenario(pool_mode=PoolMode.NESTED_DEFAULT_IN_EPHEMERAL),
    replace(default_scenario(), credential=CredentialType.MTLS),
]


@pytest.mark.integration
class TestCallerMintedTokenLocalReset:
    """Rows T1 and T2 — the caller itself consumes the token it minted."""

    @pytest.mark.asyncio
    async def test_caller_reset_should_restore_prior_value_when_dispatch_merged(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token survives an unrelated dispatch merge and resets locally.

        Given:
            TENANT_ID set to "a" then "b" on the caller, and an unrelated
            routine dispatched and merged between the set and the reset.
        When:
            The caller resets the "b" token locally, then resets it again.
        Then:
            It should restore "a" exactly as stdlib would — the merge must
            not invalidate the token — and the second reset should raise
            the single-use RuntimeError.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token_a = routines.TENANT_ID.set("a")
                try:
                    token_b = routines.TENANT_ID.set("b")
                    assert token_b.old_value == "a"
                    assert await routines.add(1, 2) == 3
                    routines.TENANT_ID.reset(token_b)
                    assert routines.TENANT_ID.get() == "a"
                    with pytest.raises(RuntimeError, match="already been used once"):
                        routines.TENANT_ID.reset(token_b)
                finally:
                    routines.TENANT_ID.reset(token_a)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_reset_should_unbind_the_var_when_previously_unset(
        self, credentials_map, retry_grpc_internal
    ):
        """Test resetting a first-set token after a merge restores unbound state.

        Given:
            A fresh default-less wool.ContextVar first set on the caller
            (its token's old_value is wool.Token.MISSING) and an unrelated
            routine dispatched and merged before the reset.
        When:
            The caller resets the token locally, then resets it again.
        Then:
            It should return the variable to the unbound state — get()
            raises LookupError, mirroring stdlib — and the second reset
            should raise the single-use RuntimeError.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var: wool.ContextVar[str] = wool.ContextVar(f"a2_t1_{uuid.uuid4().hex}")
                token = var.set("only")
                assert token.old_value is wool.Token.MISSING
                assert await routines.add(1, 2) == 3
                var.reset(token)
                with pytest.raises(LookupError):
                    var.get()
                with pytest.raises(RuntimeError, match="already been used once"):
                    var.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_original_token_should_reset_when_clone_rode_the_wire_unreset(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the original resets fine after its clone rides the wire as luggage.

        Given:
            TENANT_ID set to "a" then "b" on the caller, with the "b"
            token carried to a worker and returned unreset alongside its
            var name.
        When:
            The caller resets the ORIGINAL token, then resets the
            returned clone.
        Then:
            It should restore "a" via the original — the round trip must
            not consume or damage it — and the clone's reset should raise
            the single-use RuntimeError: same id, one ledger.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                prior = routines.TENANT_ID.set("a")
                try:
                    token = routines.TENANT_ID.set("b")
                    returned, var_name = await routines.describe_tenant_id_token(token)
                    assert var_name == routines.TENANT_ID.name
                    assert returned.var.name == routines.TENANT_ID.name
                    assert returned.old_value == "a"
                    routines.TENANT_ID.reset(token)
                    assert routines.TENANT_ID.get() == "a"
                    with pytest.raises(RuntimeError, match="already been used once"):
                        routines.TENANT_ID.reset(returned)
                finally:
                    routines.TENANT_ID.reset(prior)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_returned_clone_should_reset_and_consume_the_original(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the returned clone resets like stdlib and consumes the original.

        Given:
            TENANT_ID set to "a" then "b" on the caller, with the "b"
            token carried to a worker and returned unreset.
        When:
            The caller resets the RETURNED clone, then resets the
            original token.
        Then:
            It should restore "a" — stdlib has one token object, so the
            clone must behave as that one token — and the original's
            reset should raise the single-use RuntimeError: same id, one
            ledger.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                prior = routines.TENANT_ID.set("a")
                try:
                    token = routines.TENANT_ID.set("b")
                    returned, _ = await routines.describe_tenant_id_token(token)
                    routines.TENANT_ID.reset(returned)
                    assert routines.TENANT_ID.get() == "a"
                    with pytest.raises(RuntimeError, match="already been used once"):
                        routines.TENANT_ID.reset(token)
                finally:
                    routines.TENANT_ID.reset(prior)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_returned_clone_should_equal_the_original(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token carried to a worker and back equals the original.

        Given:
            TENANT_ID set on the caller, with the token carried to a worker
            and returned unreset while the original is still held.
        When:
            The returned token is compared with the original.
        Then:
            It should compare equal to and hash alike the original — the two
            handles denote one token, so a round-tripped token is equal to the
            one the caller still holds, as stdlib's single token object would
            be.
        """

        async def body():
            # Arrange
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                prior = routines.TENANT_ID.set("a")
                try:
                    token = routines.TENANT_ID.set("b")

                    # Act
                    returned, _ = await routines.describe_tenant_id_token(token)

                    # Assert
                    assert returned == token
                    assert hash(returned) == hash(token)
                    routines.TENANT_ID.reset(token)
                finally:
                    routines.TENANT_ID.reset(prior)

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestCallerMintedTokenRemoteReset:
    """Rows T3 and T4 — a worker consumes the caller-minted token."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("scenario", T3_POOL_SCENARIOS, ids=str)
    async def test_worker_reset_should_consume_the_callers_token(
        self, scenario, credentials_map, retry_grpc_internal
    ):
        """Test a worker-side reset back-propagates and consumes the token.

        Given:
            TENANT_ID set to "a" then "b" on the caller and the "b" token
            dispatched to a routine that resets it on the worker, across
            the curated pool configurations.
        When:
            The dispatch returns and the caller retries the reset.
        Then:
            It should restore "a" on both worker and caller and raise the
            single-use RuntimeError on the caller's retry of the consumed
            token, matching one stdlib context exactly.
        """

        # Arrange, act, & assert
        async def body():
            async with build_pool_from_scenario(scenario, credentials_map):
                prior = routines.TENANT_ID.set("a")
                try:
                    token1 = routines.TENANT_ID.set("b")
                    assert token1.old_value == "a"
                    observed = await routines.reset_tenant_id_token(token1)
                    assert observed == "a"
                    assert routines.TENANT_ID.get() == "a"
                    with pytest.raises(RuntimeError, match="already been used once"):
                        routines.TENANT_ID.reset(token1)
                finally:
                    routines.TENANT_ID.reset(prior)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_consumed_token_should_stay_used_when_var_freshly_set(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a fresh caller set never resurrects a worker-consumed token.

        Given:
            TENANT_ID set to "a" then "b" on the caller with the "b"
            token consumed by a worker-side reset that back-propagated
            home, then a fresh caller-side set minting a new token.
        When:
            The caller retries the consumed token after the fresh set.
        Then:
            It should raise the single-use RuntimeError — the fresh set
            must not prune the consumed token's ledger entry — while the
            fresh token still resets cleanly, restoring "a".
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                prior = routines.TENANT_ID.set("a")
                try:
                    token1 = routines.TENANT_ID.set("b")
                    await routines.reset_tenant_id_token(token1)
                    assert routines.TENANT_ID.get() == "a"
                    token2 = routines.TENANT_ID.set("c")

                    with pytest.raises(RuntimeError, match="already been used once"):
                        routines.TENANT_ID.reset(token1)
                    routines.TENANT_ID.reset(token2)
                    assert routines.TENANT_ID.get() == "a"
                finally:
                    routines.TENANT_ID.reset(prior)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_reset_should_unbind_the_var_when_previously_unset(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker-side reset of a first-set token restores unbound state.

        Given:
            A fresh default-less wool.ContextVar first set on the caller
            (its token's old_value is wool.Token.MISSING) and the token
            dispatched to the oracle reset routine on a worker.
        When:
            The worker resets the token, the caller retries the reset,
            mints a fresh token via set, retries the old token again, and
            finally resets the fresh token.
        Then:
            It should report an unbound variable on the worker, leave the
            caller's variable unbound (get() raises LookupError), raise
            the single-use RuntimeError on both retries of the consumed
            token, and return to unbound via the fresh token.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var: wool.ContextVar[str] = wool.ContextVar(f"a2_t3_{uuid.uuid4().hex}")
                token1 = var.set("only")
                assert token1.old_value is wool.Token.MISSING
                kind, message, value_after = await routines.oracle_reset(
                    var.namespace, var.name, token1
                )
                assert (kind, message) == ("ok", "")
                assert value_after == routines.ORACLE_UNSET
                with pytest.raises(LookupError):
                    var.get()
                with pytest.raises(RuntimeError, match="already been used once"):
                    var.reset(token1)
                # Regression: a fresh caller-side set() must not prune
                # the consumed token's ledger entry.
                token2 = var.set("again")
                with pytest.raises(RuntimeError, match="already been used once"):
                    var.reset(token1)
                var.reset(token2)
                with pytest.raises(LookupError):
                    var.get()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_two_hop_relay_reset_should_consume_the_callers_token(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token consumed two hops away restores "a" and reads as used.

        Given:
            TENANT_ID set to "a" then "b" on the caller and the "b" token
            dispatched through a relay routine on worker A that
            nested-dispatches the actual reset to worker B in an
            EPHEMERAL two-worker pool.
        When:
            The relay returns and the caller reads the var and retries
            the reset locally.
        Then:
            It should observe "a" restored on both the inner worker and
            the caller, and the local retry should raise the single-use
            RuntimeError — the consumed state crossed both hops back.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                prior = routines.TENANT_ID.set("a")
                try:
                    token = routines.TENANT_ID.set("b")
                    assert token.old_value == "a"
                    observed = await routines.relay_reset_tenant_id_token(token)
                    assert observed == "a"
                    assert routines.TENANT_ID.get() == "a"
                    with pytest.raises(RuntimeError, match="already been used once"):
                        routines.TENANT_ID.reset(token)
                finally:
                    routines.TENANT_ID.reset(prior)

        await retry_grpc_internal(body)
