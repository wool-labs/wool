import pytest


@pytest.mark.parametrize(
    ("base_ref", "head_ref", "segment", "channel"),
    [
        ("master", "400-release-version-lookup", "patch", "production"),
        ("master", "release", "minor", "candidate"),
        ("release", "master", "patch", "candidate"),
        ("release", "401-fix", "patch", "candidate"),
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
    result = script("version-segment.sh", "main", "401-fix")

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


def test_a_fix_merged_into_master_should_patch_the_last_production_release(
    repository, script
):
    """Test the release the issue exists to make possible.

    Given:
        A master reaching a production tag with a stray candidate on its head.
    When:
        A fix branch is merged and the resulting version is derived.
    Then:
        It should patch the production tag rather than advance the candidate.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.branch("fix")
    repository.commit()
    repository.checkout("master")
    # The stray candidate PR #395's merge left on master's head.
    repository.commit()
    repository.tag("v0.15.0-rc1")
    merge = repository.merge("fix")

    # Act
    version = _derive(script, "master", "fix", merge)

    # Assert
    assert version == "v0.14.1"


def test_a_release_merged_into_master_should_promote_the_reachable_candidate(
    repository, script
):
    """Test the version a finalized release is published as.

    Given:
        A release branch carrying the pending candidate.
    When:
        It is merged into master and the resulting version is derived.
    Then:
        It should promote the candidate to its production version.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.1")
    repository.branch("release")
    repository.commit()
    repository.tag("v0.15.0-rc2")
    repository.checkout("master")
    merge = repository.merge("release")

    # Act
    version = _derive(script, "master", "release", merge)

    # Assert
    # The production tag the merge also reaches must not leak in.
    assert version == "v0.15.0"


def test_a_sync_merged_into_release_should_advance_the_reachable_candidate(
    repository, script
):
    """Test the version a sync into the release branch is published as.

    Given:
        A release branch carrying a candidate, synced from master.
    When:
        The sync is merged and the resulting version is derived.
    Then:
        It should advance the candidate.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.1")
    repository.branch("release")
    repository.commit()
    repository.tag("v0.15.0-rc2")
    repository.checkout("master")
    repository.commit()
    repository.checkout("release")
    merge = repository.merge("master")

    # Act
    version = _derive(script, "release", "master", merge)

    # Assert
    assert version == "v0.15.0-rc3"


def _derive(script, base_ref: str, head_ref: str, ref: str) -> str:
    """Resolve the version a merge publishes, as the workflow composes it."""
    segment, channel = (
        line.split("=", 1)[1]
        for line in script("version-segment.sh", base_ref, head_ref).stdout.split()
    )
    old_version = script("latest-version.sh", channel, ref).stdout.strip()
    return script("bump-version.sh", segment, old_version).stdout.strip()
