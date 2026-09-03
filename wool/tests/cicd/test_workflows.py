"""Pins on the release workflow definitions themselves.

The release scripts are exercised directly elsewhere in this package; what
is pinned here is the wiring that decides whether they are called at all,
and called with the history and the arguments they need.
"""

import os
import re

import pytest
import yaml

from tests.cicd.conftest import ACTIONS
from tests.cicd.conftest import SCRIPTS
from tests.cicd.conftest import WORKFLOWS

pytestmark = pytest.mark.cicd

#: The scripts that read a version out of the repository's tags. A job that
#: calls one of these needs the history git walks to find them.
VERSION_READERS = re.compile(r"\.github/scripts/(latest-version|cut-release)\.sh")

#: Definitions are sorted so parametrization order does not follow the
#: filesystem's.
DEFINITIONS = sorted(WORKFLOWS.glob("*.yaml")) + sorted(ACTIONS.glob("*/action.yaml"))

#: Every `.github/scripts/<name>.sh` the workflows or the scripts execute.
#: Scripts nothing invokes are excluded -- their mode bit carries no risk.
EXECUTED = sorted(
    {
        script
        for source in (*DEFINITIONS, *SCRIPTS.glob("*.sh"))
        for reference in re.findall(
            r"(?:\.github/scripts/|BASH_SOURCE\[0\]\}\"\)/)([\w-]+\.sh)",
            source.read_text(),
        )
        if (script := SCRIPTS / reference).exists()
    }
)


def _jobs(definition: dict) -> list[dict]:
    """Return every job of a workflow, or the single job of an action."""
    if runs := definition.get("runs"):
        return [runs]
    return list(definition.get("jobs", {}).values())


def _checkouts(job: dict) -> list[dict]:
    """Return every ``actions/checkout`` step of a job."""
    return [
        step
        for step in job.get("steps", [])
        if "actions/checkout" in step.get("uses", "")
    ]


def _run(job: dict) -> str:
    """Return every shell body of a job, concatenated."""
    return "\n".join(step.get("run", "") for step in job.get("steps", []))


@pytest.mark.parametrize("path", DEFINITIONS, ids=lambda path: path.name)
def test_every_job_that_reads_a_version_should_check_out_the_full_history(path):
    """Test the history the release version is derived from.

    Given:
        A workflow or action whose job reads a version from the repository.
    When:
        That job's checkout step is read.
    Then:
        It should fetch the full history.
    """
    # Arrange
    definition = yaml.safe_load(path.read_text())

    # Act
    readers = [job for job in _jobs(definition) if VERSION_READERS.search(_run(job))]

    # Assert
    # A shallow checkout makes the lookup report the zero version and exit
    # zero, so the release publishes v0.0.1 rather than failing. Only the
    # checkout the job starts from matters; a later one re-checks out a ref
    # the version has already been read from.
    for job in readers:
        assert _checkouts(job)[0].get("with", {}).get("fetch-depth") == 0, path.name


def test_build_release_should_check_out_the_tag_with_its_history():
    """Test the checkout the release build derives its version from.

    Given:
        The build-release action.
    When:
        Its checkout step is read.
    Then:
        It should fetch the tags and the history they are reachable through.
    """
    # Arrange
    definition = yaml.safe_load((ACTIONS / "build-release" / "action.yaml").read_text())

    # Act
    steps = _checkouts(_jobs(definition)[0])

    # Assert
    # The metadata hook resolves any ref it is given, not only a tagged one;
    # see the checkout step's own comment for what that buys.
    assert [step["with"]["fetch-depth"] for step in steps] == [0]
    assert [step["with"]["fetch-tags"] for step in steps] == [True]


@pytest.mark.parametrize(
    ("workflow", "channel"),
    [
        ("publish-release.yaml", '"$VERSION_CHANNEL"'),
        ("manual-release.yaml", "production"),
    ],
)
def test_each_release_workflow_should_read_its_channel_through_the_lookup(
    workflow, channel
):
    """Test the wiring between the release workflows and the lookup.

    Given:
        A workflow that publishes a release.
    When:
        Its bump step is read.
    Then:
        It should read the old version from the channel lookup.
    """
    # Arrange
    definition = yaml.safe_load((WORKFLOWS / workflow).read_text())

    # Act
    body = _run(definition["jobs"]["bump-version"])

    # Assert
    assert f".github/scripts/latest-version.sh {channel}" in body


def test_publish_release_should_resolve_the_segment_through_the_contract():
    """Test the wiring between the publish workflow and the branch contract.

    Given:
        The publish-release workflow.
    When:
        Its segment step is read.
    Then:
        It should resolve the segment and channel through version-segment.
    """
    # Arrange
    definition = yaml.safe_load((WORKFLOWS / "publish-release.yaml").read_text())

    # Act
    body = _run(definition["jobs"]["bump-version"])

    # Assert
    assert ".github/scripts/version-segment.sh" in body
    assert "$GITHUB_OUTPUT" in body


def test_publish_release_should_trigger_only_on_the_package_source():
    """Test the paths a merge publishes a release from.

    Given:
        The publish-release workflow.
    When:
        Its pull_request_target paths filter is read.
    Then:
        It should name only the package's source and its project file.
    """
    # Arrange
    definition = yaml.safe_load((WORKFLOWS / "publish-release.yaml").read_text())

    # Act
    # ``on`` is parsed as the boolean True by the YAML 1.1 loader.
    paths = definition[True]["pull_request_target"]["paths"]

    # Assert
    # Widening this filter to the test tree would make every change to this
    # package publish a release.
    assert paths == ["wool/src/**", "wool/pyproject.toml"]


@pytest.mark.parametrize("script", EXECUTED, ids=lambda path: path.name)
def test_every_release_script_should_be_executable(script):
    """Test the mode bits of the scripts the release runs.

    Given:
        A script under .github/scripts.
    When:
        Its mode is read.
    Then:
        It should be executable.
    """
    # Act & assert
    # The workflows and the scripts exec each other directly rather than
    # through bash, so a lost mode bit breaks the release at runtime and
    # nowhere earlier.
    assert os.access(script, os.X_OK)
