import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

#: Release cycles in ascending order, production last.
_CYCLES = [None, "a", "b", "rc"]


@pytest.mark.parametrize(
    ("segment", "old_version", "new_version"),
    [
        ("patch", "v0.14.0", "v0.14.1"),
        ("patch", "v0.15.0-rc2", "v0.15.0-rc3"),
        ("patch", "v0.0.0", "v0.0.1"),
        ("minor", "v0.14.0", "v0.15.0"),
        ("minor", "v0.15.0-rc2", "v0.15.0"),
        ("minor", "v0.15.0-a1", "v0.15.0-b0"),
        ("minor", "v0.15.0-b1", "v0.15.0-rc0"),
        ("major", "v0.14.0", "v1.0.0"),
    ],
)
def test_bump_version_should_return_the_bumped_version(
    script, segment, old_version, new_version
):
    """Test the version transitions the release workflows depend on.

    Given:
        A version and the segment to move.
    When:
        The version is bumped.
    Then:
        It should return the next version of that segment.
    """
    # Act
    result = script("bump-version.sh", segment, old_version)

    # Assert
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == new_version


def test_bump_version_should_exit_nonzero_when_bumping_a_prerelease_major(script):
    """Test the major bump's rejection of a pre-release version.

    Given:
        A release candidate version.
    When:
        Its major segment is bumped.
    Then:
        It should exit non-zero and report the rejected bump.
    """
    # Act
    result = script("bump-version.sh", "major", "v0.15.0-rc2")

    # Assert
    assert result.returncode != 0
    assert "Cannot bump major version segment" in result.stderr


@pytest.mark.parametrize(
    "version", ["v1.0.0-rc.1", "v1.0.0-alpha.1", "v1.0.0+build.5", "nightly", ""]
)
def test_bump_version_should_exit_nonzero_when_the_version_is_malformed(script, version):
    """Test the version argument's validation.

    Given:
        A string that is not a version this tooling produces.
    When:
        Its patch segment is bumped.
    Then:
        It should exit non-zero rather than return the string unchanged.
    """
    # Act
    result = script("bump-version.sh", "patch", version)

    # Assert
    assert result.returncode != 0
    assert "Invalid version" in result.stderr


# The repository the script fixture runs in is arrangement Hypothesis has no
# reason to redraw between examples -- bump-version.sh reads its arguments,
# not the repository -- so re-using it across examples is safe. The deadline
# is lifted because each example spawns two shells, which a loaded CI runner
# can take longer over than Hypothesis's default allows.
@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(
    segment=st.sampled_from(["patch", "minor"]),
    major=st.integers(min_value=0, max_value=99),
    minor=st.integers(min_value=0, max_value=99),
    patch=st.integers(min_value=0, max_value=99),
    cycle=st.sampled_from(_CYCLES),
)
def test_bump_version_should_return_a_greater_version(
    script, segment, major, minor, patch, cycle
):
    """Test the ordering invariant every release bump rests on.

    Given:
        Any production or pre-release version and a patch or minor segment.
    When:
        The version is bumped.
    Then:
        It should return a version that ranks above the original.
    """
    # Arrange
    if cycle:
        old_version = f"v{major}.{minor}.0-{cycle}{patch}"
    else:
        old_version = f"v{major}.{minor}.{patch}"

    # Act
    result = script("bump-version.sh", segment, old_version)

    # Assert
    assert result.returncode == 0, result.stderr
    bumped = result.stdout.strip()
    # Ranked on the segments themselves rather than through SemanticVersion,
    # whose pre-release ordering compares "rc10" against "rc9" as text.
    assert _rank(bumped) > _rank(old_version)


def _rank(version: str) -> tuple:
    """Order a version by its numeric segments and its release cycle."""
    core, _, pre = version.lstrip("v").partition("-")
    major, minor, patch = (int(segment) for segment in core.split("."))
    if pre:
        cycle = pre.rstrip("0123456789")
        return (major, minor, patch, _CYCLES.index(cycle), int(pre[len(cycle) :]))
    return (major, minor, patch, len(_CYCLES), 0)


@pytest.mark.xfail(
    strict=True,
    reason="SemanticVersion compares pre-release identifiers as text, so "
    "rc10 ranks below rc9. Unreachable from the release path, which no "
    "longer orders versions, but the model still reports it.",
)
def test_bump_version_should_return_a_version_semantic_version_ranks_above(
    script, semantic_version
):
    """Test the version model's ordering of a two-digit release candidate.

    Given:
        A release candidate whose next patch carries a two-digit cycle.
    When:
        The candidate's patch segment is bumped.
    Then:
        It should return a version the version model ranks above it.
    """
    # Arrange
    old_version = "v0.15.0-rc9"

    # Act
    result = script("bump-version.sh", "patch", old_version)

    # Assert
    assert result.stdout.strip() == "v0.15.0-rc10"
    assert semantic_version.parse(result.stdout.strip()) > semantic_version.parse(
        old_version
    )
