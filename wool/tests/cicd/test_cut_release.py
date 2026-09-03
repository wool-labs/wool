import pytest

pytestmark = pytest.mark.cicd


@pytest.fixture
def published(repository, tmp_path):
    """A repository on ``main`` with an ``origin`` it can push to."""
    origin = tmp_path / "origin.git"
    repository.git("init", "--bare", "--initial-branch", "master", str(origin))
    repository.git("remote", "add", "origin", str(origin))
    repository.commit()
    repository.branch("main")
    repository.git("push", "--quiet", "--set-upstream", "origin", "main")
    return repository


def test_cut_release_should_return_the_next_candidate(published, script):
    """Test the candidate a minor release is cut as.

    Given:
        A main branch reaching a production tag.
    When:
        A minor release is cut.
    Then:
        It should return the next minor's first candidate and push the branch.
    """
    # Arrange
    published.tag("v0.14.0")

    # Act
    result = script("cut-release.sh", "minor")

    # Assert
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "v0.15.0-rc0"
    assert "release" in published.git("branch", "--list", "release")
    assert "release" in published.git("ls-remote", "--heads", "origin", "release")


def test_cut_release_should_return_the_first_candidate_when_there_are_no_tags(
    published, script
):
    """Test the candidate cut from an untagged history.

    Given:
        A main branch with no tags.
    When:
        A minor release is cut.
    Then:
        It should return the first candidate of the zero version's next minor.
    """
    # Act
    result = script("cut-release.sh", "minor")

    # Assert
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "v0.1.0-rc0"


def test_cut_release_should_exit_nonzero_when_a_candidate_is_pending(published, script):
    """Test the guard against cutting over a pending candidate.

    Given:
        A main branch reaching a candidate tag.
    When:
        A minor release is cut.
    Then:
        It should exit non-zero and name the pending candidate.
    """
    # Arrange
    published.tag("v0.14.0")
    published.commit()
    published.tag("v0.15.0-rc0")

    # Act
    result = script("cut-release.sh", "minor")

    # Assert
    assert result.returncode != 0
    assert "An active release candidate already exists: v0.15.0-rc0" in result.stderr


def test_cut_release_should_cut_again_once_the_candidate_is_promoted(published, script):
    """Test the cut following a finalized release.

    Given:
        A main branch reaching a candidate and the release it was promoted to.
    When:
        A minor release is cut.
    Then:
        It should return the next minor's first candidate.
    """
    # Arrange
    published.tag("v0.15.0-rc0")
    published.commit()
    published.tag("v0.15.0")

    # Act
    result = script("cut-release.sh", "minor")

    # Assert
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "v0.16.0-rc0"


def test_cut_release_should_exit_nonzero_when_the_branch_already_exists(
    published, script
):
    """Test the guard against cutting over an existing release branch.

    Given:
        A repository that already has a release branch.
    When:
        A minor release is cut.
    Then:
        It should exit non-zero and report the existing branch.
    """
    # Arrange
    published.tag("v0.14.0")
    published.branch("release")
    published.checkout("main")

    # Act
    result = script("cut-release.sh", "minor")

    # Assert
    assert result.returncode != 0
    assert "already exists" in result.stderr


def test_cut_release_should_return_the_next_major_candidate(published, script):
    """Test the candidate a major release is cut as.

    Given:
        A main branch reaching a production tag.
    When:
        A major release is cut.
    Then:
        It should return the next major's first candidate.
    """
    # Arrange
    published.tag("v0.14.0")

    # Act
    result = script("cut-release.sh", "major")

    # Assert
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "v1.0.0-rc0"


def test_cut_release_should_cut_the_named_branch_when_one_is_given(published, script):
    """Test the branch a release is cut onto.

    Given:
        A main branch reaching a production tag.
    When:
        A release is cut onto a named branch.
    Then:
        It should create that branch rather than the default one.
    """
    # Arrange
    published.tag("v0.14.0")

    # Act
    result = script("cut-release.sh", "minor", "hotfix")

    # Assert
    assert result.returncode == 0, result.stderr
    assert "hotfix" in published.git("branch", "--list", "hotfix")


@pytest.mark.parametrize("arguments", [("nightly",), ("minor", "release", "extra")])
def test_cut_release_should_exit_nonzero_when_the_arguments_are_invalid(
    published, script, arguments
):
    """Test the argument validation.

    Given:
        An unrecognized release type, or too many arguments.
    When:
        A release is cut.
    Then:
        It should exit non-zero and print the usage line intact on stderr.
    """
    # Arrange
    # A bracket expression in the usage line globs against the working
    # directory unless it is quoted.
    for name in ("B", "e", "l", "s"):
        (published.path / name).write_text("")

    # Act
    result = script("cut-release.sh", *arguments)

    # Assert
    assert result.returncode != 0
    assert "[BRANCH=release]" in result.stderr
    assert result.stdout == ""


def test_cut_release_should_exit_nonzero_when_the_branch_exists_on_the_remote(
    published, script
):
    """Test the guard against cutting over a release branch on the remote.

    Given:
        A release branch that exists only on the remote.
    When:
        A minor release is cut.
    Then:
        It should exit non-zero and leave the remote branch where it was.
    """
    # Arrange
    published.tag("v0.14.0")
    published.branch("release")
    published.git("push", "--quiet", "origin", "release")
    published.checkout("main")
    published.git("branch", "--delete", "--force", "release")
    before = published.git("ls-remote", "--heads", "origin", "release")

    # Act
    result = script("cut-release.sh", "minor")

    # Assert
    # A CI checkout carries only the branch it cloned, so an existing
    # release branch is visible there as a remote ref and nowhere else.
    assert result.returncode != 0
    assert "already exists" in result.stderr
    assert published.git("ls-remote", "--heads", "origin", "release") == before
