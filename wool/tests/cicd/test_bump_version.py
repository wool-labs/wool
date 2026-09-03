import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from packaging.version import Version

pytestmark = pytest.mark.cicd

#: The pre-release cycles bump-version.sh moves through, and no cycle.
_CYCLES = (None, "a", "b", "rc")


@pytest.mark.parametrize(
    ("segment", "old_version", "new_version"),
    [
        ("patch", "v0.14.0", "v0.14.1"),
        ("patch", "v0.15.0-rc2", "v0.15.0-rc3"),
        ("patch", "v0.15.0-rc9", "v0.15.0-rc10"),
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
    number=st.integers(min_value=0, max_value=99),
)
def test_bump_version_should_return_a_greater_version(
    script, segment, major, minor, patch, cycle, number
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
    # A pre-release always carries a zero patch segment: that is the only
    # shape bump-version.sh emits, and the only one it can bump.
    if cycle:
        old_version = f"v{major}.{minor}.0-{cycle}{number}"
    else:
        old_version = f"v{major}.{minor}.{patch}"

    # Act
    result = script("bump-version.sh", segment, old_version)

    # Assert
    assert result.returncode == 0, result.stderr
    # Ordered by PEP 440 rather than through SemanticVersion; see the
    # expected failure below for why the model is not the oracle here.
    assert Version(result.stdout.strip()) > Version(old_version)


@pytest.mark.xfail(
    strict=True,
    reason="SemanticVersion compares pre-release identifiers as text, so "
    "rc10 ranks below rc9. The release path does not order versions, so "
    "nothing depends on it, but the model still reports it.",
)
def test_parse_should_rank_a_two_digit_cycle_above_a_single_digit_one(
    semantic_version,
):
    """Test the version model's ordering of a two-digit release candidate.

    Given:
        Two release candidates of one version, with one and two digit cycles.
    When:
        They are compared.
    Then:
        It should rank the two-digit candidate above the single-digit one.
    """
    # Act & assert
    assert semantic_version.parse("v0.15.0-rc10") > semantic_version.parse("v0.15.0-rc9")


@pytest.mark.parametrize("arguments", [("nightly", "v0.14.0"), ("patch",), ()])
def test_bump_version_should_exit_nonzero_when_the_arguments_are_invalid(
    script, arguments
):
    """Test the argument validation.

    Given:
        An unrecognized segment, or the wrong number of arguments.
    When:
        The version is bumped.
    Then:
        It should exit non-zero and print the usage line on stderr.
    """
    # Act
    result = script("bump-version.sh", *arguments)

    # Assert
    assert result.returncode != 0
    assert "Usage:" in result.stderr
    assert result.stdout == ""
