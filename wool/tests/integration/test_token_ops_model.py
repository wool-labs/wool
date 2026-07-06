"""Unit tests for the token-parity op-script model (``_token_ops``).

Fast, pool-free tests that validate the script model and the stdlib
oracle executor before the wire-facing suites build on them, plus one
end-to-end smoke test proving ``run_wool_script`` matches
``run_stdlib_oracle`` across a real worker pool.
"""

import dataclasses

import pytest
from hypothesis import given
from hypothesis import settings

import wool
from tests.helpers import _unique

from ._token_ops import CALLER
from ._token_ops import UNSET
from ._token_ops import WORKER
from ._token_ops import GetOp
from ._token_ops import Observation
from ._token_ops import ResetOp
from ._token_ops import SetOp
from ._token_ops import describe_mismatch
from ._token_ops import observations_match
from ._token_ops import run_stdlib_oracle
from ._token_ops import run_wool_script
from ._token_ops import scripts
from .conftest import build_pool_from_scenario
from .conftest import default_scenario


class TestOpModel:
    def test___init___should_expose_actor_and_payload_fields(self):
        """Test the op dataclasses expose their actor and payload fields.

        Given:
            One op of each kind constructed with an actor and its
            payload.
        When:
            The fields are read back.
        Then:
            It should expose actor/value/token_index as constructed.
        """
        # Arrange & act
        set_op = SetOp(CALLER, "v")
        reset_op = ResetOp(WORKER, 3)
        get_op = GetOp(CALLER)

        # Assert
        assert (set_op.actor, set_op.value) == (CALLER, "v")
        assert (reset_op.actor, reset_op.token_index) == (WORKER, 3)
        assert get_op.actor == CALLER

    def test___post_init___should_raise_value_error_when_actor_unknown(self):
        """Test op construction rejects actors outside the caller/worker pair.

        Given:
            An actor string that is neither "caller" nor "worker".
        When:
            A SetOp is constructed with it.
        Then:
            It should raise ValueError naming the accepted actors.
        """
        # Act & assert
        with pytest.raises(ValueError, match="actor must be one of"):
            SetOp("elsewhere", 1)

    def test___eq___should_compare_equal_when_fields_match(self):
        """Test the op dataclasses compare by field value.

        Given:
            Ops of the same kind, one built with matching fields and one
            with a differing field.
        When:
            They are compared for equality and inequality.
        Then:
            It should compare equal on identical field values and
            unequal once a field differs.
        """
        # Arrange & act
        set_op = SetOp(CALLER, "v")
        reset_op = ResetOp(WORKER, 3)

        # Assert
        assert set_op == SetOp(CALLER, "v")
        assert reset_op != ResetOp(CALLER, 3)

    def test___setattr___should_reject_field_mutation_when_frozen(self):
        """Test the op dataclasses are immutable.

        Given:
            A constructed SetOp.
        When:
            Its value field is assigned.
        Then:
            It should raise FrozenInstanceError.
        """
        # Arrange
        op = SetOp(CALLER, 1)

        # Act & assert
        with pytest.raises(dataclasses.FrozenInstanceError):
            op.value = 2  # type: ignore[misc]


def test_run_stdlib_oracle_should_restore_prior_values_when_resets_are_lifo():
    """Test the stdlib oracle unwinds LIFO resets while ignoring actor tags.

    Given:
        A script that sets twice (once tagged caller, once worker),
        resets the tokens in LIFO order, then reads.
    When:
        run_stdlib_oracle executes the script.
    Then:
        It should restore each token's captured old value in turn and
        end unset — actor tags changing nothing since every op is
        local.
    """
    # Arrange
    script = [
        SetOp(CALLER, "a"),
        SetOp(WORKER, "b"),
        ResetOp(WORKER, 1),
        ResetOp(CALLER, 0),
        GetOp(CALLER),
    ]

    # Act
    observations = run_stdlib_oracle(script)

    # Assert
    assert observations == [
        Observation("set", "a", None),
        Observation("set", "b", None),
        Observation("reset", "a", None),
        Observation("reset", UNSET, None),
        Observation("get", UNSET, None),
    ]


def test_run_stdlib_oracle_should_apply_captured_old_values_when_resets_out_of_order():
    """Test out-of-order resets restore each token's own captured old value.

    Given:
        A script that sets "a" then "b", resets the FIRST token, reads,
        then resets the second token.
    When:
        run_stdlib_oracle executes the script.
    Then:
        It should unset the variable on the first reset (the first
        token captured no prior value) and restore "a" on the second —
        stdlib tokens restore their mint-time snapshot, not a stack.
    """
    # Arrange
    script = [
        SetOp(CALLER, "a"),
        SetOp(CALLER, "b"),
        ResetOp(CALLER, 0),
        GetOp(CALLER),
        ResetOp(CALLER, 1),
    ]

    # Act
    observations = run_stdlib_oracle(script)

    # Assert
    assert observations == [
        Observation("set", "a", None),
        Observation("set", "b", None),
        Observation("reset", UNSET, None),
        Observation("get", UNSET, None),
        Observation("reset", "a", None),
    ]


def test_run_stdlib_oracle_should_report_runtime_error_when_token_reused():
    """Test a double reset records the single-use RuntimeError.

    Given:
        A script that sets once, then resets the same token twice.
    When:
        run_stdlib_oracle executes the script.
    Then:
        It should record a clean first reset and a "RuntimeError"
        observation for the second, with the value left unchanged.
    """
    # Arrange
    script = [SetOp(CALLER, 1), ResetOp(CALLER, 0), ResetOp(CALLER, 0)]

    # Act
    observations = run_stdlib_oracle(script)

    # Assert
    assert observations == [
        Observation("set", 1, None),
        Observation("reset", UNSET, None),
        Observation("reset", UNSET, "RuntimeError"),
    ]


def test_run_stdlib_oracle_should_track_new_sets_between_resets():
    """Test a set interleaved between resets mints an independently valid token.

    Given:
        A script that sets 1 and 2, resets the second token, sets 3,
        resets the first token, then reads.
    When:
        run_stdlib_oracle executes the script.
    Then:
        It should restore 1 from the second token, apply the new set of
        3, then unset the variable via the first token's empty
        snapshot.
    """
    # Arrange
    script = [
        SetOp(CALLER, 1),
        SetOp(CALLER, 2),
        ResetOp(CALLER, 1),
        SetOp(CALLER, 3),
        ResetOp(CALLER, 0),
        GetOp(CALLER),
    ]

    # Act
    observations = run_stdlib_oracle(script)

    # Assert
    assert observations == [
        Observation("set", 1, None),
        Observation("set", 2, None),
        Observation("reset", 1, None),
        Observation("set", 3, None),
        Observation("reset", UNSET, None),
        Observation("get", UNSET, None),
    ]


def test_run_stdlib_oracle_should_noop_when_reset_precedes_any_set():
    """Test a reset drawn before any set observes without acting.

    Given:
        A script whose first op is a reset (no token minted yet)
        followed by a read.
    When:
        run_stdlib_oracle executes the script.
    Then:
        It should record the reset as an error-free no-op observation
        and leave the variable unset.
    """
    # Arrange
    script = [ResetOp(CALLER, 4), GetOp(CALLER)]

    # Act
    observations = run_stdlib_oracle(script)

    # Assert
    assert observations == [
        Observation("reset", UNSET, None),
        Observation("get", UNSET, None),
    ]


def test_run_stdlib_oracle_should_return_unset_sentinel_when_get_on_unbound_var():
    """Test a get on a never-set variable records the UNSET sentinel.

    Given:
        A script containing only a worker-tagged get.
    When:
        run_stdlib_oracle executes the script.
    Then:
        It should record a single error-free observation whose value is
        the UNSET sentinel.
    """
    # Act
    observations = run_stdlib_oracle([GetOp(WORKER)])

    # Assert
    assert observations == [Observation("get", UNSET, None)]


def test_run_stdlib_oracle_should_normalize_token_index_when_out_of_range():
    """Test an out-of-range token index wraps modulo the minted-token count.

    Given:
        A script with two sets and a reset whose index exceeds the
        token list length.
    When:
        run_stdlib_oracle executes the script.
    Then:
        It should reset the token at index 7 % 2 == 1, restoring "a".
    """
    # Arrange
    script = [SetOp(CALLER, "a"), SetOp(CALLER, "b"), ResetOp(CALLER, 7)]

    # Act
    observations = run_stdlib_oracle(script)

    # Assert
    assert observations[-1] == Observation("reset", "a", None)


def test_run_stdlib_oracle_should_match_hand_computed_trace_when_ops_mixed():
    """Test the stdlib oracle reproduces a fully hand-computed nontrivial trace.

    Given:
        A seven-op script mixing sets, an out-of-order reset, a
        double-reset rejection, a set after an unset, a late reset of a
        stale token, and a final read.
    When:
        run_stdlib_oracle executes the script.
    Then:
        It should equal the observation trace computed by hand from
        stdlib contextvars semantics, op by op.
    """
    # Arrange
    script = [
        SetOp(CALLER, "a"),
        SetOp(WORKER, "b"),
        ResetOp(WORKER, 0),
        ResetOp(CALLER, 0),
        SetOp(CALLER, "c"),
        ResetOp(WORKER, 1),
        GetOp(CALLER),
    ]

    # Act
    observations = run_stdlib_oracle(script)

    # Assert
    assert observations == [
        Observation("set", "a", None),
        Observation("set", "b", None),
        Observation("reset", UNSET, None),
        Observation("reset", UNSET, "RuntimeError"),
        Observation("set", "c", None),
        Observation("reset", "a", None),
        Observation("get", "a", None),
    ]


@given(script=scripts())
@settings(max_examples=50, deadline=None)
def test_run_stdlib_oracle_should_return_one_observation_per_op(script):
    """Test the stdlib oracle is total over the drawn script space.

    Given:
        Any script of up to twelve set/reset/get ops over both actors,
        with int/str values and arbitrary token indexes.
    When:
        run_stdlib_oracle executes the script.
    Then:
        It should never raise and return exactly one Observation per
        op, with errors reported only on reset observations.
    """
    # Act
    observations = run_stdlib_oracle(script)

    # Assert
    assert len(observations) == len(script)
    assert all(isinstance(entry, Observation) for entry in observations)
    assert all(entry.error is None for entry in observations if entry.kind != "reset")


def test_observations_match_should_return_true_when_traces_identical():
    """Test the comparator accepts equal traces from either executor shape.

    Given:
        Two independently built but field-equal observation lists.
    When:
        observations_match compares them.
    Then:
        It should return True.
    """
    # Arrange
    left = [Observation("set", 1, None), Observation("reset", UNSET, None)]
    right = [Observation("set", 1, None), Observation("reset", UNSET, None)]

    # Act & assert
    assert observations_match(left, right)


def test_observations_match_should_return_false_when_traces_diverge():
    """Test the comparator rejects traces that differ in any entry.

    Given:
        Two observation lists equal except for one divergent entry.
    When:
        observations_match compares them.
    Then:
        It should return False.
    """
    # Arrange
    left = [Observation("set", 1, None), Observation("reset", UNSET, None)]
    diverged = [Observation("set", 1, None), Observation("reset", 1, None)]

    # Act & assert
    assert not observations_match(left, diverged)


def test_describe_mismatch_should_flag_divergent_rows():
    """Test the diff helper marks exactly the rows that diverge.

    Given:
        Two traces equal at index 0 and divergent at index 1.
    When:
        describe_mismatch renders them.
    Then:
        It should flag the divergent row with "!!" and leave the equal
        row unflagged.
    """
    # Arrange
    left = [Observation("set", 1, None), Observation("get", 1, None)]
    right = [Observation("set", 1, None), Observation("get", 2, None)]

    # Act
    rendered = describe_mismatch(left, right)

    # Assert
    lines = rendered.splitlines()
    assert lines[1].startswith("  ")
    assert lines[2].startswith("!! [1]")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_run_wool_script_should_match_stdlib_oracle_when_ops_span_the_wire(
    credentials_map, retry_grpc_internal
):
    """Test the wire executor reproduces the stdlib oracle trace end-to-end.

    Given:
        A DEFAULT pool of real worker subprocesses and a fixed script
        mixing caller-side and worker-side set/reset/get ops against a
        fresh wool.ContextVar.
    When:
        The script runs through run_wool_script and, separately,
        through run_stdlib_oracle.
    Then:
        It should produce identical observation traces — location
        transparency holds across the dispatch boundary.
    """

    # Arrange, act, & assert
    async def body():
        script = [
            SetOp(CALLER, "caller-value"),
            SetOp(WORKER, "worker-value"),
            ResetOp(WORKER, 1),
            ResetOp(CALLER, 0),
            GetOp(CALLER),
        ]
        scenario = default_scenario()
        async with build_pool_from_scenario(scenario, credentials_map):
            var: wool.ContextVar[str] = wool.ContextVar(
                _unique("token_ops_smoke"), namespace=_unique("token_ops_ns")
            )
            wool_observations = await run_wool_script(script, var)
        stdlib_observations = run_stdlib_oracle(script)
        assert observations_match(wool_observations, stdlib_observations), (
            describe_mismatch(wool_observations, stdlib_observations)
        )

    await retry_grpc_internal(body)
