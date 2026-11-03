import pytest

from wool.utilities.failure import FailureReport


def _raise() -> None:
    raise RuntimeError("origin")


def _record() -> RuntimeError:
    """Catch a failure one frame above its raise, as a container does."""
    try:
        _raise()
    except RuntimeError as error:
        return error
    raise AssertionError("the origin did not raise")


def _record_under_context() -> RuntimeError:
    """Catch a failure raised while an unrelated exception was handled."""
    try:
        raise ValueError("unrelated")
    except ValueError:
        return _record()


def _frames(error: BaseException) -> list[str]:
    """Name every frame the exception's traceback currently reaches."""
    names, traceback = [], error.__traceback__
    while traceback is not None:
        names.append(traceback.tb_frame.f_code.co_name)
        traceback = traceback.tb_next
    return names


class TestFailureReport:
    def test___init___should_hold_the_failure_it_was_given(self):
        """Test the report exposes the instance it was constructed from.

        Given:
            A recorded failure
        When:
            A FailureReport is constructed from it
        Then:
            It should expose that same instance, since callers match on
            identity rather than on a rebuilt equivalent.
        """
        # Arrange
        failure = _record()

        # Act
        report = FailureReport(failure)

        # Assert
        assert report.failure is failure

    def test_raise_failure_should_raise_the_held_instance(self):
        """Test the held failure reaches the caller unwrapped.

        Given:
            A report over a recorded failure
        When:
            The report raises it
        Then:
            It should raise that same instance, so an except clause
            written for the original type still matches.
        """
        # Arrange
        failure = _record()
        report = FailureReport(failure)

        # Act & assert
        with pytest.raises(RuntimeError) as excinfo:
            report.raise_failure()
        assert excinfo.value is failure

    def test_raise_failure_should_restore_the_context_from_the_first_report(self):
        """Test chaining captured at construction survives every report.

        Given:
            A report over a failure recorded while an unrelated
            exception was being handled, so it carries that exception as
            its context
        When:
            The report raises it more than once from a frame handling
            nothing
        Then:
            It should present the original context every time, rather
            than losing it to the first propagation.
        """
        # Arrange
        report = FailureReport(_record_under_context())

        # Act
        contexts = []
        for _ in range(3):
            with pytest.raises(RuntimeError) as excinfo:
                report.raise_failure()
            contexts.append(excinfo.value.__context__)

        # Assert
        assert [type(context) for context in contexts] == [ValueError] * 3

    def test_raise_failure_should_hand_the_caller_the_origin_traceback(self):
        """Test every report carries the frames the failure came from.

        Given:
            A failure caught one frame above its raise, as a container
            recording a failure it will owe to later callers does
        When:
            The report raises it
        Then:
            It should hand the caller the origin's frames alongside
            their own, so the report is diagnosable from wherever it
            surfaces rather than only from where it was recorded.
        """
        # Arrange
        report = FailureReport(_record())

        # Act
        with pytest.raises(RuntimeError) as excinfo:
            report.raise_failure()

        # Assert -- the origin is reported in full. Holding it is also
        # what the report retains for its life; see `FailureReport`.
        frames = _frames(excinfo.value)
        assert "_record" in frames
        assert "_raise" in frames

    def test_raise_failure_should_not_deepen_the_traceback_across_reports(self):
        """Test repeated reports hand out a traceback of one depth.

        Given:
            A report over a recorded failure
        When:
            It is raised repeatedly
        Then:
            It should hand every caller the same depth, rather than one
            that grows by the frames of every report before it.
        """
        # Arrange
        report = FailureReport(_record())

        # Act
        depths = []
        for _ in range(5):
            with pytest.raises(RuntimeError) as excinfo:
                report.raise_failure()
            depths.append(len(_frames(excinfo.value)))

        # Assert
        assert len(set(depths)) == 1
