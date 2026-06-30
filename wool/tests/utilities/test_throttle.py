import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

import wool.utilities.throttle as wt
from wool.utilities.throttle import Throttle


@pytest.fixture
def clock(mocker: MockerFixture):
    """Freeze the throttle's clock, yielding a one-element list to advance it."""
    now = [1000.0]
    mocker.patch.object(wt.time, "monotonic", side_effect=lambda: now[0])
    return now


class TestThrottle:
    """Test suite for the Throttle log rate limiter."""

    def test_due_should_emit_when_key_is_new(self, clock):
        """Test the first occurrence of a key is never suppressed.

        Given:
            A throttle that has seen nothing.
        When:
            An occurrence is recorded.
        Then:
            It should emit, reporting nothing suppressed.
        """
        # Arrange
        throttle = Throttle(60.0)

        # Act
        emit, suppressed = throttle.due("worker-1", "boom")

        # Assert
        assert emit is True
        assert suppressed == 0

    def test_due_should_suppress_when_detail_unchanged_within_interval(self, clock):
        """Test an unchanged detail is suppressed until the interval passes.

        Given:
            A throttle that has just emitted for a key.
        When:
            The same detail recurs before the interval elapses.
        Then:
            It should not emit.
        """
        # Arrange
        throttle = Throttle(60.0)
        throttle.due("worker-1", "boom")

        # Act
        clock[0] += 59.0
        emit, _ = throttle.due("worker-1", "boom")

        # Assert
        assert emit is False

    def test_due_should_emit_when_interval_elapses(self, clock):
        """Test an unchanged detail emits again after the interval.

        Given:
            A throttle that suppressed two occurrences since emitting.
        When:
            The same detail recurs past the interval.
        Then:
            It should emit, reporting the two it suppressed.
        """
        # Arrange
        throttle = Throttle(60.0)
        throttle.due("worker-1", "boom")
        clock[0] += 1.0
        throttle.due("worker-1", "boom")
        throttle.due("worker-1", "boom")

        # Act
        clock[0] += 60.0
        emit, suppressed = throttle.due("worker-1", "boom")

        # Assert
        assert emit is True
        assert suppressed == 2

    def test_due_should_emit_when_detail_changes(self, clock):
        """Test a changed detail bypasses the interval.

        Given:
            A throttle that has just emitted for a key.
        When:
            The same key recurs within the interval with a new detail.
        Then:
            It should emit, because a changed detail is new information.
        """
        # Arrange
        throttle = Throttle(60.0)
        throttle.due("worker-1", "boom")

        # Act
        emit, _ = throttle.due("worker-1", "different")

        # Assert
        assert emit is True

    def test_due_should_emit_when_a_different_key_recurs(self, clock):
        """Test one key's cadence does not suppress another's.

        Given:
            A throttle that has just emitted for one key.
        When:
            A different key records the same detail within the interval.
        Then:
            It should emit, since it is that key's first occurrence.
        """
        # Arrange
        throttle = Throttle(60.0)
        throttle.due("worker-1", "boom")

        # Act
        emit, suppressed = throttle.due("worker-2", "boom")

        # Assert
        assert emit is True
        assert suppressed == 0

    def test_due_should_share_one_slot_when_key_omitted(self, clock):
        """Test the keyless form tracks one global condition.

        Given:
            A caller that omits the key, tracking a single condition.
        When:
            Two occurrences are recorded within the interval.
        Then:
            The second should be suppressed, sharing the first's slot.
        """
        # Arrange
        throttle = Throttle(60.0)
        throttle.due()

        # Act
        emit, _ = throttle.due()

        # Assert
        assert emit is False

    def test_due_should_report_every_occurrence_suppressed_since_emitting(self, clock):
        """Test the suppressed count spans every suppressed occurrence.

        Given:
            A throttle suppressing a steady stream of identical details.
        When:
            The interval finally elapses.
        Then:
            The emission should report every occurrence suppressed since
            the last one, not merely the most recent.
        """
        # Arrange
        throttle = Throttle(60.0)
        throttle.due("worker-1", "boom")
        for _ in range(5):
            clock[0] += 1.0
            throttle.due("worker-1", "boom")

        # Act
        clock[0] += 60.0
        emit, suppressed = throttle.due("worker-1", "boom")

        # Assert
        assert emit is True
        assert suppressed == 5

    def test_due_should_restart_the_interval_when_emitting(self, clock):
        """Test the interval is measured from the last emission.

        Given:
            A throttle whose key emitted a second time on a changed
            detail, part-way through the first interval.
        When:
            That detail recurs less than an interval after the second
            emission but more than one after the first.
        Then:
            It should be suppressed, since the clock restarts on emit.
        """
        # Arrange
        throttle = Throttle(60.0)
        throttle.due("worker-1", "boom")
        clock[0] += 30.0
        throttle.due("worker-1", "different")

        # Act
        clock[0] += 40.0
        emit, _ = throttle.due("worker-1", "different")

        # Assert
        assert emit is False

    @given(
        steps=st.lists(
            st.tuples(st.floats(min_value=0.0, max_value=40.0), st.booleans()),
            min_size=1,
            max_size=40,
        )
    )
    @settings(
        max_examples=200,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def test_due_should_account_for_every_occurrence_exactly_once(self, clock, steps):
        """Test suppressed counts never lose or double-count an occurrence.

        Given:
            Any sequence of occurrences of one key, each separated by an
            arbitrary clock advance and carrying one of two details.
        When:
            The sequence is replayed against a throttle.
        Then:
            Once a final occurrence flushes whatever is still pending,
            the counts reported by every emission should sum to exactly
            the number of occurrences that did not emit — so a
            suppressed occurrence is neither lost nor counted twice.
        """
        # Arrange
        throttle = Throttle(30.0)
        emitted = 0
        reported = 0

        # Act
        for advance, flip in steps:
            clock[0] += advance
            emit, suppressed = throttle.due("key", "b" if flip else "a")
            if emit:
                emitted += 1
                reported += suppressed
        # A suppressed occurrence is reported by the *next* emission, so
        # drain the pending count past the interval before accounting.
        clock[0] += 31.0
        emit, suppressed = throttle.due("key", "b" if steps[-1][1] else "a")

        # Assert
        assert emit is True
        assert reported + suppressed == len(steps) + 1 - (emitted + 1)

    def test_discard_should_emit_fresh_when_the_key_recurs(self, clock):
        """Test a discarded key emits fresh rather than resuming.

        Given:
            A throttle that has emitted for a key and suppressed one.
        When:
            The key is discarded and the same detail recurs.
        Then:
            It should emit with no suppressed count, treating the
            recurrence as a new incident rather than a continuation.
        """
        # Arrange
        throttle = Throttle(60.0)
        throttle.due("worker-1", "boom")
        throttle.due("worker-1", "boom")

        # Act
        throttle.discard("worker-1")
        emit, suppressed = throttle.due("worker-1", "boom")

        # Assert
        assert emit is True
        assert suppressed == 0

    def test_discard_should_succeed_when_key_unknown(self, clock):
        """Test discarding a key that was never recorded is harmless.

        Given:
            A throttle that has seen nothing.
        When:
            An unknown key is discarded.
        Then:
            It should not raise.
        """
        # Arrange
        throttle = Throttle(60.0)

        # Act & assert
        throttle.discard("never-seen")
