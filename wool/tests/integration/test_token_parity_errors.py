"""Cross-process error-shape parity for wool.Token reset rejections.

Matrix dimension D4 of the token-parity suite: every reset rejection —
used token, wrong variable, different Context, non-token argument —
must surface with stdlib-identical exception classes, check order, and
canonical message phrases wherever the reset is attempted: locally
after a remote consumption, remotely via the oracle routines, and on a
second worker that never saw the original reset. Complements the
single-use and ownership-topology coverage in
tests/integration/test_context_var_propagation.py by exercising the
oracle-addressed routes, the used-before-wrong-var gate order across
the wire, and message-text parity against live stdlib contextvars
twins.
"""

import asyncio
import contextvars
from typing import Any
from uuid import uuid4

import pytest

import wool

from . import routines
from .conftest import PoolMode
from .conftest import build_pool_from_scenario
from .conftest import default_scenario


def _fresh_var() -> wool.ContextVar[str]:
    """Declare a unique default-less wool.ContextVar for one oracle route."""
    return wool.ContextVar(f"a7_{uuid4().hex}")


def _fresh_twin() -> contextvars.ContextVar[str]:
    """Declare a unique stdlib contextvars twin for message-parity baselines."""
    return contextvars.ContextVar(f"a7_twin_{uuid4().hex}")


def _trailing_phrase(message: str) -> str:
    """Return the canonical phrase following the token repr in *message*."""
    return message.rsplit("> ", 1)[-1]


@pytest.mark.integration
class TestUsedTokenGateAcrossProcesses:
    @pytest.mark.asyncio
    async def test_local_retry_after_fresh_set_should_still_report_used_token(
        self, credentials_map, retry_grpc_internal
    ):
        """Test remote consumption survives an intervening fresh local set.

        Given:
            A DEFAULT pool, a fresh wool.ContextVar whose token was
            consumed by a successful oracle reset on the worker, and a
            subsequent fresh local set binding the same variable again.
        When:
            The caller retries the consumed token locally after the
            fresh set.
        Then:
            It should raise RuntimeError with the canonical
            "has already been used once" message and leave the fresh
            binding untouched — the cross-process consumption is still
            authoritative after the variable is re-armed locally.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var = _fresh_var()
                token = var.set("first")
                kind, message, value_after = await routines.oracle_reset(
                    var.namespace, var.name, token
                )
                assert (kind, message) == ("ok", "")
                assert value_after == routines.ORACLE_UNSET
                var.set("fresh")
                with pytest.raises(RuntimeError) as exc_info:
                    var.reset(token)
                assert "has already been used once" in str(exc_info.value)
                # Ledger-discovered consumption stamps the used flag, so
                # the repr carries stdlib's marker on this route too.
                assert "<Token used var=" in str(exc_info.value)
                assert var.get() == "fresh"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_locally_consumed_token_dispatched_should_report_used_remotely(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a locally consumed token is refused by a remote oracle reset.

        Given:
            A fresh wool.ContextVar bound to "base", a second set
            minting a token, and a successful LOCAL reset consuming
            that token before any dispatch.
        When:
            The caller dispatches the consumed token to the oracle
            reset routine on a DEFAULT pool.
        Then:
            It should report RuntimeError with the canonical
            "has already been used once" message and a worker-side
            value still equal to "base" — the rejected remote reset
            mutated nothing on either side of the wire.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var = _fresh_var()
                var.set("base")
                token = var.set("second")
                var.reset(token)
                assert var.get() == "base"
                kind, message, value_after = await routines.oracle_reset(
                    var.namespace, var.name, token
                )
                assert kind == "RuntimeError"
                assert "has already been used once" in message
                assert value_after == "base"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_token_consumed_on_one_worker_should_read_used_on_another(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a consumption on one worker is enforced by a different worker.

        Given:
            An EPHEMERAL pool with two workers and a fresh
            wool.ContextVar token consumed by an attributed oracle
            reset that reported one worker pid.
        When:
            The caller re-dispatches the attributed oracle reset
            repeatedly (up to eight times) so at least one attempt
            lands on the other worker process.
        Then:
            Every post-consumption attempt should report RuntimeError
            with the canonical "has already been used once" message,
            including at least one attempt attributed to a different
            pid than the consuming worker's.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                var = _fresh_var()
                token = var.set("relay-mint")
                first_pid, first_kind, _, _ = await routines.oracle_reset_report(
                    var.namespace, var.name, token
                )
                assert first_kind == "ok"
                # Round-robin over the two-worker pool reaches the other
                # worker within a few dispatches; keep dispatching until a
                # different pid confirms the second worker enforced it.
                observed_other_pid = False
                for _ in range(8):
                    pid, kind, message, _ = await routines.oracle_reset_report(
                        var.namespace, var.name, token
                    )
                    assert kind == "RuntimeError"
                    assert "has already been used once" in message
                    if pid != first_pid:
                        observed_other_pid = True
                        break
                assert observed_other_pid, (
                    "the reset attempt never landed on a second worker, so "
                    "cross-worker used-token enforcement was not exercised"
                )

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestWrongVariableAcrossTheWire:
    @pytest.mark.asyncio
    async def test_remote_reset_addressed_to_wrong_var_should_report_value_error(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token dispatched to the wrong variable fails remotely.

        Given:
            Two fresh wool.ContextVars — B bound to "b-value" and a
            live token minted by A — on a DEFAULT pool.
        When:
            The caller dispatches an oracle reset addressed to B with
            A's token, then retries A's token locally.
        Then:
            It should report ValueError with the canonical
            "was created by a different ContextVar" message, leave B's
            worker-side value untouched, and the local retry on A
            should still succeed — the rejection never consumed the
            token.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var_a = _fresh_var()
                var_b = _fresh_var()
                var_b.set("b-value")
                token = var_a.set("a-value")
                kind, message, value_after = await routines.oracle_reset(
                    var_b.namespace, var_b.name, token
                )
                assert kind == "ValueError"
                assert "was created by a different ContextVar" in message
                assert value_after == "b-value"
                var_a.reset(token)
                assert var_a.get("<unset>") == "<unset>"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_round_tripped_token_should_refuse_local_wrong_var_reset(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token that travelled as luggage still binds to its own var.

        Given:
            A live token minted by a fresh wool.ContextVar A, carried
            to a worker and back as luggage by the describe routine,
            and a second fresh wool.ContextVar B.
        When:
            The caller resets B with the round-tripped copy, then
            resets A with the original token.
        Then:
            The wrong-var reset should raise ValueError with the
            canonical "was created by a different ContextVar" message,
            and the original token should still reset A successfully —
            the round trip neither consumed nor rebound the token.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var_a = _fresh_var()
                var_b = _fresh_var()
                token = var_a.set("a-value")
                returned, var_name = await routines.describe_tenant_id_token(token)
                assert var_name == var_a.name
                assert returned.var.name == var_a.name
                with pytest.raises(
                    ValueError, match="was created by a different ContextVar"
                ):
                    var_b.reset(returned)
                var_a.reset(token)
                assert var_a.get("<unset>") == "<unset>"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestResetGateOrderAcrossProcesses:
    @pytest.mark.asyncio
    async def test_consumed_token_addressed_to_wrong_var_should_report_used_first(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the used gate outranks the wrong-var gate on the worker.

        Given:
            Two fresh wool.ContextVars — B bound to "b-armed" and a
            token minted by A already consumed by a successful oracle
            reset addressed to A on a DEFAULT pool.
        When:
            The caller dispatches a second oracle reset addressed to B
            with the consumed token — a token that is BOTH used AND
            for the wrong variable.
        Then:
            It should report RuntimeError with the canonical
            "has already been used once" message — not the wrong-var
            ValueError — proving the used gate fires first on the
            receiving side, matching stdlib's check order, and B's
            worker-side value should be untouched.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var_a = _fresh_var()
                var_b = _fresh_var()
                var_b.set("b-armed")
                token = var_a.set("a-value")
                kind, _, _ = await routines.oracle_reset(
                    var_a.namespace, var_a.name, token
                )
                assert kind == "ok"
                kind, message, value_after = await routines.oracle_reset(
                    var_b.namespace, var_b.name, token
                )
                assert kind == "RuntimeError"
                assert kind != "ValueError"
                assert "has already been used once" in message
                assert value_after == "b-armed"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestOrphanTokenErrorsAcrossProcesses:
    @pytest.mark.asyncio
    async def test_child_task_minted_token_should_report_different_context(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token minted inside a create_task child is an orphan everywhere.

        Given:
            A fresh wool.ContextVar and a token minted INSIDE an
            asyncio child task created via ``create_task`` — the
            child's context fork drops the mint from the parent's
            continuation.
        When:
            The parent round-trips the token as luggage, dispatches an
            oracle reset with it, and finally resets it locally.
        Then:
            Transport should never raise, the remote reset should
            report ValueError with the canonical "was created in a
            different Context" message and an unbound worker-side
            value, and the parent's local reset should raise the same
            ValueError — the orphan is rejected identically on both
            sides of the wire.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var = _fresh_var()

                async def _mint():
                    return var.set("child-mint")

                orphan = await asyncio.create_task(_mint())
                returned, var_name = await routines.describe_tenant_id_token(orphan)
                assert var_name == var.name
                assert returned.var.name == var.name
                kind, message, value_after = await routines.oracle_reset(
                    var.namespace, var.name, orphan
                )
                assert kind == "ValueError"
                assert "was created in a different Context" in message
                assert value_after == routines.ORACLE_UNSET
                with pytest.raises(
                    ValueError, match="was created in a different Context"
                ):
                    var.reset(orphan)

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestResetTypeErrorParity:
    @pytest.mark.asyncio
    async def test_local_reset_with_non_token_should_match_stdlib_type_error(
        self, credentials_map, retry_grpc_internal
    ):
        """Test local non-token rejections match stdlib's TypeError messages.

        Given:
            A fresh wool.ContextVar armed and already dispatched
            through a DEFAULT pool, and a stdlib contextvars twin
            rejecting the same non-token arguments.
        When:
            The caller resets the wool variable with a plain string
            and with None.
        Then:
            Both should raise TypeError with messages equal to the
            stdlib twin's — "expected an instance of Token, got ..." —
            and the variable's live token should still reset
            successfully afterward.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var = _fresh_var()
                token = var.set("armed")
                observed = await routines.oracle_get(
                    var.namespace, var.name, routines.ORACLE_UNSET
                )
                assert observed == "armed"
                twin = _fresh_twin()
                not_a_token: Any = "not-a-token"
                none_token: Any = None
                with pytest.raises(TypeError) as stdlib_string:
                    twin.reset(not_a_token)
                with pytest.raises(TypeError) as stdlib_none:
                    twin.reset(none_token)
                with pytest.raises(TypeError) as wool_string:
                    var.reset(not_a_token)
                with pytest.raises(TypeError) as wool_none:
                    var.reset(none_token)
                assert "expected an instance of Token" in str(wool_string.value)
                assert str(wool_string.value) == str(stdlib_string.value)
                assert str(wool_none.value) == str(stdlib_none.value)
                var.reset(token)
                assert var.get("<unset>") == "<unset>"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_remote_reset_with_non_token_should_report_stdlib_type_error(
        self, credentials_map, retry_grpc_internal
    ):
        """Test remote non-token rejections match stdlib's TypeError messages.

        Given:
            A fresh wool.ContextVar bound to "armed" and a stdlib
            contextvars twin rejecting the same non-token arguments
            locally.
        When:
            The caller dispatches oracle resets carrying a plain
            string and None instead of a token on a DEFAULT pool.
        Then:
            Both should report TypeError with messages equal to the
            stdlib twin's, and the worker-side binding should remain
            "armed" — the rejections mutated nothing.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var = _fresh_var()
                var.set("armed")
                twin = _fresh_twin()
                not_a_token: Any = "not-a-token"
                none_token: Any = None
                with pytest.raises(TypeError) as stdlib_string:
                    twin.reset(not_a_token)
                with pytest.raises(TypeError) as stdlib_none:
                    twin.reset(none_token)
                kind, message, value_after = await routines.oracle_reset(
                    var.namespace, var.name, "not-a-token"
                )
                assert kind == "TypeError"
                assert message == str(stdlib_string.value)
                assert value_after == "armed"
                kind, message, value_after = await routines.oracle_reset(
                    var.namespace, var.name, None
                )
                assert kind == "TypeError"
                assert message == str(stdlib_none.value)
                assert value_after == "armed"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestStdlibMessageShapeParity:
    @pytest.mark.asyncio
    async def test_remote_used_error_should_match_stdlib_message_shape(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the remote used-token message mirrors stdlib's anatomy.

        Given:
            A stdlib contextvars twin whose double reset produced the
            baseline RuntimeError message, and a fresh wool.ContextVar
            token consumed locally then dispatched to the oracle reset
            routine on a DEFAULT pool.
        When:
            The worker's rejection message is compared to the stdlib
            baseline.
        Then:
            Both should lead with the used-token repr pattern
            "<Token used var=" and both should end with exactly the
            same canonical phrase "has already been used once".
        """

        # Arrange, act, & assert
        async def body():
            twin = _fresh_twin()
            twin_token = twin.set("twin-value")
            twin.reset(twin_token)
            with pytest.raises(RuntimeError) as stdlib_exc:
                twin.reset(twin_token)
            stdlib_message = str(stdlib_exc.value)
            assert stdlib_message.startswith("<Token used var=")

            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var = _fresh_var()
                token = var.set("wool-value")
                var.reset(token)
                kind, message, _ = await routines.oracle_reset(
                    var.namespace, var.name, token
                )
                assert kind == "RuntimeError"
                assert message.startswith("<Token used var=")
                assert _trailing_phrase(message) == _trailing_phrase(stdlib_message)
                assert _trailing_phrase(message) == "has already been used once"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_remote_wrong_var_error_should_match_stdlib_message_shape(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the remote wrong-var message mirrors stdlib's anatomy.

        Given:
            Two stdlib contextvars twins whose cross-var reset
            produced the baseline ValueError message, and a live token
            minted by fresh wool.ContextVar A dispatched to an oracle
            reset addressed to fresh wool.ContextVar B on a DEFAULT
            pool.
        When:
            The worker's rejection message is compared to the stdlib
            baseline.
        Then:
            Both should lead with the live-token repr pattern
            "<Token var=" and both should end with exactly the same
            canonical phrase "was created by a different ContextVar".
        """

        # Arrange, act, & assert
        async def body():
            twin_a = _fresh_twin()
            twin_b = _fresh_twin()
            twin_token = twin_a.set("twin-value")
            with pytest.raises(ValueError) as stdlib_exc:
                twin_b.reset(twin_token)
            stdlib_message = str(stdlib_exc.value)
            assert stdlib_message.startswith("<Token var=")

            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var_a = _fresh_var()
                var_b = _fresh_var()
                token = var_a.set("a-value")
                kind, message, _ = await routines.oracle_reset(
                    var_b.namespace, var_b.name, token
                )
                assert kind == "ValueError"
                assert message.startswith("<Token var=")
                assert _trailing_phrase(message) == _trailing_phrase(stdlib_message)
                assert (
                    _trailing_phrase(message) == "was created by a different ContextVar"
                )

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_remote_orphan_error_should_match_stdlib_message_shape(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the remote different-Context message mirrors stdlib's anatomy.

        Given:
            A stdlib contextvars twin whose copy_context-sibling token
            produced the baseline ValueError message, and a fresh
            wool.ContextVar token minted in a stdlib copy_context
            sibling dispatched to the oracle reset routine on a
            DEFAULT pool.
        When:
            The worker's rejection message is compared to the stdlib
            baseline.
        Then:
            Both should lead with the live-token repr pattern
            "<Token var=" and both should end with exactly the same
            canonical phrase "was created in a different Context".
        """

        # Arrange, act, & assert
        async def body():
            twin = _fresh_twin()
            twin_orphan = contextvars.copy_context().run(twin.set, "twin-sibling")
            with pytest.raises(ValueError) as stdlib_exc:
                twin.reset(twin_orphan)
            stdlib_message = str(stdlib_exc.value)
            assert stdlib_message.startswith("<Token var=")

            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                var = _fresh_var()
                orphan = contextvars.copy_context().run(var.set, "wool-sibling")
                kind, message, _ = await routines.oracle_reset(
                    var.namespace, var.name, orphan
                )
                assert kind == "ValueError"
                assert message.startswith("<Token var=")
                assert _trailing_phrase(message) == _trailing_phrase(stdlib_message)
                assert _trailing_phrase(message) == "was created in a different Context"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestTokenCarriedInException:
    @pytest.mark.asyncio
    async def test_token_carried_in_exception_should_transport_and_stay_single_use(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token raised inside an exception transports and stays single-use.

        Given:
            A DEFAULT pool whose worker sets a fresh wool.ContextVar, captures
            the minted token, and raises a TokenCarrier exception carrying it, so
            the token rides home on the exception frame rather than a return
            value.
        When:
            The caller catches the exception, extracts the token, resets it, then
            attempts to reset it a second time.
        Then:
            The extracted token should be a live wool.Token whose first reset is
            honored — anchored home exactly as a returned token would be — and a
            second reset should raise the single-use RuntimeError, proving token
            transport and single-use hold uniformly across the exception channel.
        """

        async def body():
            # Arrange
            var = _fresh_var()
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                # Act
                with pytest.raises(routines.TokenCarrier) as exc_info:
                    await routines.set_var_and_raise_with_token(
                        var.namespace, var.name, "exc-value"
                    )
                token = exc_info.value.token

                # Assert
                assert isinstance(token, wool.Token)
                var.reset(token)
                with pytest.raises(RuntimeError, match="has already been used once"):
                    var.reset(token)

        await retry_grpc_internal(body)
