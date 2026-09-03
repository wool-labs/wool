"""The version each merge into a release line publishes.

Composes the three scripts the publish workflow composes, so the release
contract is exercised end to end rather than a script at a time.
"""

import pytest

pytestmark = pytest.mark.cicd


def test_release_version_should_patch_the_production_tag_when_a_fix_merges(
    repository, script
):
    """Test the version a fix merged into master is published as.

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
    # A candidate tag stranded on master's head by an earlier release.
    repository.commit()
    repository.tag("v0.15.0-rc1")
    merge = repository.merge("fix")

    # Act
    version = _derive(script, "master", "fix", merge)

    # Assert
    assert version == "v0.14.1"


def test_release_version_should_promote_the_candidate_when_release_merges(
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


def test_release_version_should_advance_the_candidate_when_a_sync_merges(
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
    """Resolve the version a merge publishes.

    Chains the three scripts as ``.github/workflows/publish-release.yaml``
    chains them, failing on any step the workflow would have failed on.
    """

    def run(*arguments: str) -> str:
        result = script(*arguments)
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    segment, channel = (
        line.split("=", 1)[1]
        for line in run("version-segment.sh", base_ref, head_ref).split()
    )
    return run("bump-version.sh", segment, run("latest-version.sh", channel, ref))
