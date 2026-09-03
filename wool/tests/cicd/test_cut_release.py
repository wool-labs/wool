import pytest


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
