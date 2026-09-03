import pytest

pytestmark = pytest.mark.cicd


@pytest.mark.parametrize(
    ("base_ref", "head_ref", "segment", "channel"),
    [
        ("master", "123-some-fix", "patch", "production"),
        ("master", "release", "minor", "candidate"),
        ("release", "master", "patch", "candidate"),
        ("release", "123-some-fix", "patch", "candidate"),
    ],
)
def test_version_segment_should_report_the_segment_and_channel_of_the_branch_pair(
    script, base_ref, head_ref, segment, channel
):
    """Test the release contract each merge is resolved through.

    Given:
        The base and head branches of a merged pull request.
    When:
        The version segment is determined.
    Then:
        It should report the segment to move and the channel to move it from.
    """
    # Act
    result = script("version-segment.sh", base_ref, head_ref)

    # Assert
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [f"segment={segment}", f"channel={channel}"]


def test_version_segment_should_exit_nonzero_when_the_base_branch_is_unsupported(
    script,
):
    """Test the base branch's validation.

    Given:
        A base branch that is not a release line.
    When:
        The version segment is determined.
    Then:
        It should exit non-zero and name the unsupported branch.
    """
    # Act
    result = script("version-segment.sh", "main", "123-some-fix")

    # Assert
    assert result.returncode != 0
    assert "Unsupported base branch main" in result.stderr


def test_version_segment_should_exit_nonzero_when_a_branch_is_missing(script):
    """Test the argument count's validation.

    Given:
        Only a base branch.
    When:
        The version segment is determined.
    Then:
        It should exit non-zero and print the usage line.
    """
    # Act
    result = script("version-segment.sh", "master")

    # Assert
    assert result.returncode != 0
    assert "Usage:" in result.stderr
