"""Fixtures for the release-tooling tests.

The release scripts and the build metadata hook both answer questions about
a git repository, so the units under test are exercised against throwaway
repositories with synthetic histories rather than against this checkout.
"""

import importlib.util
import os
import pathlib
import subprocess
import sys
from collections.abc import Callable
from collections.abc import Iterator
from types import ModuleType

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

SCRIPTS = REPO_ROOT / ".github" / "scripts"

WORKFLOWS = REPO_ROOT / ".github" / "workflows"

ACTIONS = REPO_ROOT / ".github" / "actions"

BUILD_HOOKS = REPO_ROOT / "build-hooks"


class Repository:
    """A throwaway git repository under construction.

    Wraps the handful of plumbing commands the histories in this package are
    built from, so a test reads as the topology it describes.

    :param path: The directory the repository is created in. The fixture
        creates it before ``git init`` runs.
    """

    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        self._commits = 0

    def git(self, *arguments: str) -> str:
        """Run a git command in the repository and return its output."""
        return subprocess.run(
            ("git", *arguments),
            capture_output=True,
            check=True,
            cwd=self.path,
            env=environment(self.path),
            text=True,
        ).stdout.strip()

    def commit(self, message: str = "") -> str:
        """Commit the index, allowing an empty commit, and return its hash.

        Each default message is distinct, so sibling branches built from one
        parent never collapse onto a single commit: two commits sharing a
        parent, a tree, a message, an identity and a one-second timestamp are
        the same git object, and a merge between them would be a no-op.
        """
        self._commits += 1
        self.git("commit", "--allow-empty", "--message", message or f"c{self._commits}")
        return self.git("rev-parse", "HEAD")

    def tag(self, name: str) -> None:
        """Tag the current commit."""
        self.git("tag", name)

    def branch(self, name: str) -> None:
        """Create a branch at the current commit and check it out."""
        self.git("checkout", "-b", name)

    def checkout(self, *arguments: str) -> None:
        """Run ``git checkout`` with the given arguments."""
        self.git("checkout", *arguments)

    def merge(self, name: str, message: str = "merge") -> str:
        """Merge a branch with an explicit merge commit and return its hash.

        Fails when the merge produced no merge commit, which ``git merge``
        otherwise reports as success.
        """
        self.git("merge", "--no-ff", name, "--message", message)
        head = self.git("rev-parse", "HEAD")
        parents = self.git("rev-list", "--parents", "--max-count", "1", "HEAD")
        assert len(parents.split()) == 3, f"expected a merge commit, got {parents}"
        return head


def environment(root: pathlib.Path) -> dict[str, str]:
    """Return a git environment isolated from the one running the tests.

    Ambient ``GIT_DIR`` or ``GIT_WORK_TREE`` would redirect every command at
    the repository the tests run from -- which they push to and tag -- and
    ambient configuration would decide results that the fixture means to set
    itself.
    """
    return {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    } | {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CEILING_DIRECTORIES": str(root.parent),
    }


@pytest.fixture
def repository(tmp_path: pathlib.Path) -> Repository:
    """Return an initialized git repository with no commits, on ``master``."""
    repository = Repository(tmp_path / "repository")
    repository.path.mkdir()
    repository.git("init", "--initial-branch", "master")
    repository.git("config", "user.email", "tests@wool.io")
    repository.git("config", "user.name", "Tests")
    return repository


@pytest.fixture
def script(repository: Repository) -> Callable[..., subprocess.CompletedProcess]:
    """Return a runner for the release scripts, in the repository's directory.

    The repository is never this checkout, so a script that resolves a
    sibling script relative to the working directory fails these tests.
    """

    def run(
        name: str, *arguments: str, cwd: pathlib.Path | None = None
    ) -> subprocess.CompletedProcess:
        return subprocess.run(
            (str(SCRIPTS / name), *arguments),
            capture_output=True,
            cwd=cwd or repository.path,
            env=environment(repository.path),
            text=True,
        )

    return run


@pytest.fixture
def semantic_version() -> Iterator[type]:
    """Return the version model the release tooling's output is parsed against."""
    yield _load("_version").SemanticVersion
    _unload()


@pytest.fixture
def version(repository: Repository, monkeypatch) -> Iterator[Callable]:
    """Return a renderer for the build hook's version of the repository.

    The working directory is moved into the repository for the duration of
    the test, which is what the hook reads.
    """
    monkeypatch.chdir(repository.path)
    version = _load("_version")
    # Importing the hook registers it with the version parser registry,
    # which is what makes it reachable as ``parse.git``.
    _load("_git")
    yield lambda: version.SemanticVersion.parse.git()
    _unload()


def _load(name: str) -> ModuleType:
    """Import a build hook module from its file, bypassing ``sys.path``.

    ``build-hooks`` holds modules whose names would shadow common top-level
    imports, so the directory is never put on the path; the hooks import each
    other by bare name, so each is registered under its own.
    """
    if (module := sys.modules.get(name)) is not None:
        return module
    specification = importlib.util.spec_from_file_location(
        name, BUILD_HOOKS / f"{name}.py"
    )
    assert specification and specification.loader
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


def _unload() -> None:
    """Drop the build hook modules a test loaded."""
    for name in ("_git", "_version"):
        sys.modules.pop(name, None)
