import pytest


def test_latest_version_should_return_production_tag_when_candidate_is_nearer(
    repository, script
):
    """Test the production lookup against a pending release candidate.

    Given:
        A production tag with a nearer candidate tag on the same branch.
    When:
        The production channel is queried.
    Then:
        It should return the production tag.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.commit()
    repository.tag("v0.15.0-rc1")

    # Act
    result = script("latest-version.sh", "production")

    # Assert
    assert result.stdout.strip() == "v0.14.0"


def test_latest_version_should_return_candidate_tag_when_production_is_nearer(
    repository, script
):
    """Test the candidate lookup against a nearer production tag.

    Given:
        A candidate tag with a nearer production tag on the same branch.
    When:
        The candidate channel is queried.
    Then:
        It should return the candidate tag.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.15.0-rc2")
    repository.commit()
    repository.tag("v0.14.1")

    # Act
    result = script("latest-version.sh", "candidate")

    # Assert
    assert result.stdout.strip() == "v0.15.0-rc2"


def test_latest_version_should_ignore_tags_off_the_ref(repository, script):
    """Test that the lookup is scoped to the ref's own lineage.

    Given:
        A tag on a branch the queried ref does not reach.
    When:
        The production channel is queried for that ref.
    Then:
        It should return the tag reachable from the ref.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.branch("main")
    repository.commit()
    repository.tag("v0.16.0")
    repository.checkout("master")

    # Act
    result = script("latest-version.sh", "production", "master")

    # Assert
    assert result.stdout.strip() == "v0.14.0"


def test_latest_version_should_resolve_by_pattern_when_merge_reaches_both_channels(
    repository, script
):
    """Test the lookup on a merge commit reaching both release channels.

    Given:
        A merge commit reaching both a production tag and a candidate tag.
    When:
        Each channel is queried for that merge commit.
    Then:
        It should return the tag matching the queried channel.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.branch("release")
    repository.commit()
    repository.tag("v0.15.0-rc2")
    repository.checkout("master")
    repository.commit()
    repository.tag("v0.14.1")
    merge = repository.merge("release")

    # Act
    production = script("latest-version.sh", "production", merge)
    candidate = script("latest-version.sh", "candidate", merge)

    # Assert
    assert production.stdout.strip() == "v0.14.1"
    assert candidate.stdout.strip() == "v0.15.0-rc2"


def test_latest_version_should_exclude_alpha_and_beta_from_production(
    repository, script
):
    """Test the production lookup against alpha and beta cycle tags.

    Given:
        A production tag followed by alpha and beta cycle tags.
    When:
        The production channel is queried.
    Then:
        It should return the production tag.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.commit()
    repository.tag("v0.15.0-a1")
    repository.commit()
    repository.tag("v0.15.0-b1")

    # Act
    result = script("latest-version.sh", "production")

    # Assert
    assert result.stdout.strip() == "v0.14.0"


def test_latest_version_should_return_nearest_tag_when_channel_is_any(
    repository, script
):
    """Test the unscoped lookup.

    Given:
        A production tag with a nearer candidate tag.
    When:
        The any channel is queried.
    Then:
        It should return the nearest tag of either channel.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.commit()
    repository.tag("v0.15.0-rc1")

    # Act
    result = script("latest-version.sh", "any")

    # Assert
    assert result.stdout.strip() == "v0.15.0-rc1"


def test_latest_version_should_return_zero_version_when_no_tag_matches(
    repository, script
):
    """Test the lookup against a lineage with no tag of the channel.

    Given:
        A repository whose only tag is a candidate.
    When:
        The production channel is queried.
    Then:
        It should return the zero version.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.15.0-rc1")

    # Act
    result = script("latest-version.sh", "production")

    # Assert
    assert result.returncode == 0
    assert result.stdout.strip() == "v0.0.0"


def test_latest_version_should_return_zero_version_when_repository_has_no_tags(
    repository, script
):
    """Test the lookup against an untagged repository.

    Given:
        A repository with a commit and no tags.
    When:
        Any channel is queried.
    Then:
        It should return the zero version.
    """
    # Arrange
    repository.commit()

    # Act
    result = script("latest-version.sh", "any")

    # Assert
    assert result.returncode == 0
    assert result.stdout.strip() == "v0.0.0"


def test_latest_version_should_default_to_head_when_ref_omitted(repository, script):
    """Test the default ref.

    Given:
        A checked-out branch carrying a tag its sibling does not reach.
    When:
        The production channel is queried without a ref.
    Then:
        It should return the tag reachable from the checked-out branch.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.branch("main")
    repository.commit()
    repository.tag("v0.16.0")

    # Act
    result = script("latest-version.sh", "production")

    # Assert
    assert result.stdout.strip() == "v0.16.0"


def test_latest_version_should_exit_nonzero_when_channel_invalid(repository, script):
    """Test the channel argument's validation.

    Given:
        A repository with a tagged commit.
    When:
        An unrecognized channel is queried.
    Then:
        It should exit non-zero and report the invalid channel.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")

    # Act
    result = script("latest-version.sh", "stable")

    # Assert
    assert result.returncode != 0
    assert "Invalid release channel: stable" in result.stderr


def test_latest_version_should_return_the_highest_reachable_tag(repository, script):
    """Test the lookup against a lower tag on a shorter path.

    Given:
        A merge whose other parent forks before the highest production tag.
    When:
        The production channel is queried.
    Then:
        It should return the highest reachable production tag.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.13.1")
    fork = repository.git("rev-parse", "HEAD")
    repository.commit()
    repository.tag("v0.14.0")
    for _ in range(8):
        repository.commit()
    repository.checkout("-b", "feature", fork)
    repository.commit()
    repository.checkout("master")
    repository.merge("feature")

    # Assert
    # Commit distance ranks v0.13.1 nearer here, and bumping it would
    # publish a version below one already released.
    assert script("latest-version.sh", "production").stdout.strip() == "v0.14.0"


def test_latest_version_should_return_the_highest_candidate_when_cycles_are_two_digit(
    repository, script
):
    """Test the candidate lookup across a two-digit release cycle.

    Given:
        Release candidates whose cycle numbers span one and two digits.
    When:
        The candidate channel is queried.
    Then:
        It should return the numerically highest candidate.
    """
    # Arrange
    for tag in ("v0.15.0-rc2", "v0.15.0-rc9", "v0.15.0-rc10"):
        repository.commit()
        repository.tag(tag)

    # Assert
    assert script("latest-version.sh", "candidate").stdout.strip() == "v0.15.0-rc10"


def test_latest_version_should_rank_a_release_above_its_candidate(repository, script):
    """Test the unscoped lookup across a consumed release cycle.

    Given:
        A release and the candidate it was promoted from.
    When:
        The any channel is queried.
    Then:
        It should return the release rather than its candidate.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.15.0-rc2")
    repository.commit()
    repository.tag("v0.15.0")

    # Assert
    assert script("latest-version.sh", "any").stdout.strip() == "v0.15.0"


def test_latest_version_should_resolve_both_channels_when_one_commit_carries_both(
    repository, script
):
    """Test the lookup on a commit tagged in both release channels.

    Given:
        A single commit carrying a production tag and a candidate tag.
    When:
        Each channel is queried.
    Then:
        It should return the tag matching the queried channel.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.16.0")
    repository.tag("v0.16.0-rc9")

    # Assert
    assert script("latest-version.sh", "production").stdout.strip() == "v0.16.0"
    assert script("latest-version.sh", "candidate").stdout.strip() == "v0.16.0-rc9"


@pytest.mark.parametrize("channel", ["production", "candidate", "any"])
def test_latest_version_should_ignore_tags_that_are_not_versions(
    repository, script, channel
):
    """Test the lookup against tags outside the versioning scheme.

    Given:
        Non-version tags nearer than the version tags.
    When:
        Any channel is queried.
    Then:
        It should never return a non-version tag.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.commit()
    repository.tag("v0.15.0-rc2")
    repository.commit()
    repository.tag("nightly-2026-01-01")
    repository.tag("docs-rc-cleanup")
    repository.tag("release-2026")

    # Act
    result = script("latest-version.sh", channel)

    # Assert
    assert result.stdout.strip() in {"v0.14.0", "v0.15.0-rc2"}


def test_latest_version_should_exit_nonzero_when_the_ref_cannot_be_resolved(
    repository, script
):
    """Test the lookup against a ref that does not exist.

    Given:
        A repository with a tagged commit.
    When:
        A ref that does not resolve is queried.
    Then:
        It should exit non-zero rather than report the zero version.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")

    # Act
    result = script("latest-version.sh", "production", "origin/master")

    # Assert
    # Reporting v0.0.0 here would bump to v0.0.1 and publish it.
    assert result.returncode != 0
    assert "origin/master" in result.stderr


def test_latest_version_should_exit_nonzero_when_run_outside_a_repository(
    tmp_path, script
):
    """Test the lookup outside a git repository.

    Given:
        A working directory that is not a git repository.
    When:
        Any channel is queried.
    Then:
        It should exit non-zero rather than report the zero version.
    """
    # Act
    result = script("latest-version.sh", "any", cwd=tmp_path)

    # Assert
    assert result.returncode != 0


def test_latest_version_should_exit_nonzero_when_given_extra_arguments(
    repository, script
):
    """Test the argument count's validation.

    Given:
        A repository with a tagged commit.
    When:
        The lookup is invoked with a third argument.
    Then:
        It should exit non-zero and print the usage line intact.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    # A bracket expression in the usage line globs against the working
    # directory unless it is quoted.
    (repository.path / "R").write_text("")
    (repository.path / "H").write_text("")

    # Act
    result = script("latest-version.sh", "production", "HEAD", "extra")

    # Assert
    assert result.returncode != 0
    assert "[REF=HEAD]" in result.stderr
