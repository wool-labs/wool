import pytest

pytestmark = pytest.mark.cicd


def test_parse_git_should_return_the_tag_when_head_is_tagged(repository, version):
    """Test the version of a commit that carries a tag.

    Given:
        A repository whose head commit is tagged below a tag on an ancestor.
    When:
        The version is parsed from git.
    Then:
        It should be the tag, with no local identifier.
    """
    # Arrange
    repository.commit()
    # A higher tag on an ancestor: the version is a property of the commit,
    # not the highest version in the repository.
    repository.tag("v0.15.0-rc2")
    repository.commit()
    repository.tag("v0.14.1")

    # Act
    result = version()

    # Assert
    assert str(result) == "0.14.1"
    assert result.build is None


def test_parse_git_should_append_commit_hash_when_head_is_not_tagged(
    repository, version
):
    """Test the version of a commit that carries no tag.

    Given:
        A repository whose head commit follows a tagged commit.
    When:
        The version is parsed from git.
    Then:
        It should be the nearest tag qualified by the commit hash.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.branch("release")
    repository.commit()
    repository.tag("v0.15.0-rc2")
    repository.checkout("master")
    repository.commit()
    commit = repository.git("rev-parse", "--short", "HEAD")

    # Act
    result = version()

    # Assert
    assert str(result) == f"0.14.0+{commit}"
    # The commit belongs in the local identifier: a describe suffix parsed
    # as a pre-release would sort the build below the tag it followed.
    assert result.pre_release is None


def test_parse_git_should_ignore_tags_off_the_head_lineage(repository, version):
    """Test the version of a commit a higher tag does not reach.

    Given:
        A higher tag on a branch that head does not reach.
    When:
        The version is parsed from git.
    Then:
        It should be the tag reachable from head.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.branch("release")
    repository.commit()
    repository.tag("v0.15.0-rc2")
    repository.checkout("master")

    # Act
    result = version()

    # Assert
    assert str(result) == "0.14.0"


def test_parse_git_should_append_dirty_when_a_change_is_staged(repository, version):
    """Test the version of a worktree with uncommitted changes.

    Given:
        A repository whose head commit is tagged and whose index carries a
        staged modification.
    When:
        The version is parsed from git.
    Then:
        It should be the tag qualified as dirty.
    """
    # Arrange
    tracked = repository.path / "tracked.txt"
    tracked.write_text("committed")
    repository.git("add", "tracked.txt")
    repository.commit()
    repository.tag("v0.14.0")
    tracked.write_text("modified")
    repository.git("add", "tracked.txt")

    # Act
    result = version()

    # Assert
    assert str(result) == "0.14.0+dirty"


def test_parse_git_should_append_dirty_when_the_worktree_has_untracked_files(
    repository, version
):
    """Test the version of a worktree with untracked files.

    Given:
        A repository whose head commit is tagged and whose worktree has an
        untracked file.
    When:
        The version is parsed from git.
    Then:
        It should be the tag qualified as dirty.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    (repository.path / "untracked.txt").write_text("untracked")

    # Act
    result = version()

    # Assert
    assert str(result) == "0.14.0+dirty"


def test_parse_git_should_return_zero_version_when_repository_has_no_tags(
    repository, version
):
    """Test the version of a repository with no tags.

    Given:
        A repository with a commit and no tags.
    When:
        The version is parsed from git.
    Then:
        It should be the zero version qualified by the commit hash.
    """
    # Arrange
    repository.commit()
    commit = repository.git("rev-parse", "--short", "HEAD")

    # Act
    result = version()

    # Assert
    assert str(result) == f"0.0.0+{commit}"


def test_parse_git_should_return_the_candidate_tag_when_head_is_a_candidate(
    repository, version
):
    """Test the version of a commit tagged as a release candidate.

    Given:
        A repository whose head commit carries a candidate tag.
    When:
        The version is parsed from git.
    Then:
        It should be the candidate, with no local identifier.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.15.0-rc2")

    # Act
    result = version()

    # Assert
    assert str(result) == "0.15.0-rc2"
    assert result.build is None


def test_parse_git_should_return_the_highest_tag_when_a_merge_reaches_both(
    repository, version
):
    """Test the version of a merge commit reaching both release channels.

    Given:
        A merge commit reaching a production tag and a lower candidate tag.
    When:
        The version is parsed from git.
    Then:
        It should be the higher tag qualified by the commit hash.
    """
    # Arrange
    repository.commit()
    repository.branch("release")
    repository.commit()
    repository.tag("v0.15.0-rc2")
    repository.checkout("master")
    repository.commit()
    repository.tag("v0.15.0")
    repository.merge("release")
    commit = repository.git("rev-parse", "--short", "HEAD")

    # Act
    result = version()

    # Assert
    # A promoted release outranks the candidate it was promoted from, so a
    # build of it is never labelled with the candidate.
    assert str(result) == f"0.15.0+{commit}"


def test_parse_git_should_append_both_identifiers_when_untagged_and_dirty(
    repository, version
):
    """Test the version of an untagged commit in a dirty worktree.

    Given:
        A repository past a tagged commit with an untracked file.
    When:
        The version is parsed from git.
    Then:
        It should carry the commit hash and the dirty marker, in that order.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.commit()
    commit = repository.git("rev-parse", "--short", "HEAD")
    (repository.path / "untracked.txt").write_text("untracked")

    # Act
    result = version()

    # Assert
    assert str(result) == f"0.14.0+{commit}-dirty"


def test_parse_git_should_return_the_tag_when_head_is_detached(repository, version):
    """Test the version of a detached head at a tag.

    Given:
        A repository with a later commit, checked out at an earlier tag.
    When:
        The version is parsed from git.
    Then:
        It should be the tag.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.commit()
    repository.tag("v0.14.1")
    # build-release checks the release tag out by ref, detaching HEAD.
    repository.checkout("v0.14.0")

    # Act
    result = version()

    # Assert
    assert str(result) == "0.14.0"


def test_parse_git_should_return_the_tag_when_invoked_from_a_subdirectory(
    repository, version, monkeypatch
):
    """Test the version resolved from below the repository root.

    Given:
        A tagged repository and a working directory inside it.
    When:
        The version is parsed from git.
    Then:
        It should be the tag.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    # Hatch invokes the hook from the package directory, never the root.
    (package := repository.path / "wool").mkdir()
    monkeypatch.chdir(package)

    # Act
    result = version()

    # Assert
    assert str(result) == "0.14.0"


def test_parse_git_should_ignore_tags_that_are_not_versions(repository, version):
    """Test the version of a commit carrying tags outside the scheme.

    Given:
        A tagged repository with non-version tags on a later commit.
    When:
        The version is parsed from git.
    Then:
        It should be the version tag rather than fail to parse one.
    """
    # Arrange
    repository.commit()
    repository.tag("v0.14.0")
    repository.commit()
    repository.tag("nightly-2026-01-01")
    repository.tag("docs-rc-cleanup")
    commit = repository.git("rev-parse", "--short", "HEAD")

    # Act
    result = version()

    # Assert
    assert str(result) == f"0.14.0+{commit}"


def test_parse_git_should_return_the_highest_tag_when_head_carries_two(
    repository, version
):
    """Test the version of a commit carrying more than one version tag.

    Given:
        A repository whose head commit carries two version tags.
    When:
        The version is parsed from git.
    Then:
        It should be the higher of them.
    """
    # Arrange
    repository.commit()
    repository.tag("v1.2.3")
    repository.tag("v1.3.0")

    # Act
    result = version()

    # Assert
    assert str(result) == "1.3.0"


@pytest.mark.parametrize("state", ["bare", "empty"])
def test_parse_git_should_raise_when_the_repository_cannot_be_versioned(
    repository, version, state
):
    """Test the repositories the hook refuses.

    Given:
        A repository that is bare, or that carries no commits.
    When:
        The version is parsed from git.
    Then:
        It should raise.
    """
    # Arrange
    if state == "bare":
        repository.git("config", "core.bare", "true")

    # Act & assert
    with pytest.raises(RuntimeError):
        version()
