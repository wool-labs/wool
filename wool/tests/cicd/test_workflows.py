import os
import pathlib
import re

import pytest
import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

WORKFLOWS = REPO_ROOT / ".github" / "workflows"

ACTIONS = REPO_ROOT / ".github" / "actions"

#: Every `.github/scripts/<name>.sh` reference in a workflow or action body.
SCRIPT_REFERENCE = re.compile(r"\.github/scripts/[\w-]+\.sh")


def checkouts(definition: dict) -> list:
    """Every ``actions/checkout`` step in a workflow or composite action."""
    jobs = definition.get("jobs", {}).values()
    steps = definition.get("runs", {}).get("steps", [])
    for job in jobs:
        steps = [*steps, *job.get("steps", [])]
    return [step for step in steps if "actions/checkout" in step.get("uses", "")]


def test_build_release_should_check_out_the_full_history():
    """Test the checkout the release build derives its version from.

    Given:
        The build-release action.
    When:
        Its checkout step is read.
    Then:
        It should fetch the full history and the tags.
    """
    # Arrange
    definition = yaml.safe_load((ACTIONS / "build-release" / "action.yaml").read_text())

    # Act
    step = checkouts(definition)[0]

    # Assert
    # Without the full history the metadata hook cannot describe the tag it
    # was checked out at, and the wheel is labelled 0.0.0 instead.
    assert step["with"]["fetch-depth"] == 0
    assert step["with"]["fetch-tags"] is True


def test_publish_release_should_check_out_the_full_history_before_bumping():
    """Test the checkout the published version is derived from.

    Given:
        The publish-release workflow.
    When:
        The bump-version job's checkout step is read.
    Then:
        It should fetch the full history.
    """
    # Arrange
    definition = yaml.safe_load((WORKFLOWS / "publish-release.yaml").read_text())

    # Act
    job = definition["jobs"]["bump-version"]

    # Assert
    assert checkouts({"jobs": {"bump-version": job}})[0]["with"]["fetch-depth"] == 0


def test_publish_release_should_not_be_triggered_by_this_suite():
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
    assert paths == ["wool/src/**", "wool/pyproject.toml"]


@pytest.mark.parametrize(
    "definition",
    [
        *WORKFLOWS.glob("*.yaml"),
        *ACTIONS.glob("*/action.yaml"),
    ],
    ids=lambda path: path.parent.name + "/" + path.name,
)
def test_every_referenced_script_should_be_executable(definition):
    """Test the release scripts the workflows invoke directly.

    Given:
        A workflow or action naming scripts under .github/scripts.
    When:
        Each referenced script is resolved.
    Then:
        It should exist and be executable.
    """
    # Act
    referenced = set(SCRIPT_REFERENCE.findall(definition.read_text()))

    # Assert
    for reference in referenced:
        script = REPO_ROOT / reference
        assert script.exists(), reference
        # The workflows exec these directly rather than through bash, so a
        # lost mode bit breaks the release at runtime and nowhere earlier.
        assert os.access(script, os.X_OK), reference
