"""Fixtures for the release-tooling tests.

The release scripts and the build metadata hook both answer questions about
a git repository, so the units under test are exercised against throwaway
repositories with synthetic histories rather than against this checkout.
"""

import importlib.util
import pathlib
import subprocess
import sys
from collections.abc import Callable
from types import ModuleType

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

SCRIPTS = REPO_ROOT / ".github" / "scripts"

BUILD_HOOKS = REPO_ROOT / "build-hooks"


class Repository:
    """A throwaway git repository under construction.

    Wraps the handful of plumbing commands the histories in this package are
    built from, so a test reads as the topology it describes.
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
            text=True,
        ).stdout.strip()

    def commit(self, message: str = "") -> str:
        """Add an empty commit and return its full hash.

        The default message is numbered because two empty commits sharing a
        parent, a tree, a message and a one-second timestamp are the same git
        object: sibling branches would silently collapse onto one commit and
        a merge between them would be a no-op.
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
        """Check out a branch, a tag, or a commit."""
        self.git("checkout", *arguments)

    def merge(self, name: str, message: str = "merge") -> str:
        """Merge a branch with an explicit merge commit and return its hash.

        The parent count is asserted because ``git merge`` reports success
        when it has nothing to do, which would leave a test that means to
        exercise a merge commit asserting against a linear history.
        """
        self.git("merge", "--no-ff", name, "--message", message)
        head = self.git("rev-parse", "HEAD")
        parents = self.git("rev-list", "--parents", "--max-count", "1", "HEAD")
        assert len(parents.split()) == 3, f"expected a merge commit, got {parents}"
        return head


@pytest.fixture
def repository(tmp_path: pathlib.Path) -> Repository:
    """An initialized git repository with no commits, on ``master``."""
    repository = Repository(tmp_path / "repository")
    repository.path.mkdir()
    repository.git("init", "--initial-branch", "master")
    repository.git("config", "user.email", "tests@wool.io")
    repository.git("config", "user.name", "Tests")
    repository.git("config", "commit.gpgsign", "false")
    repository.git("config", "tag.gpgsign", "false")
    return repository


@pytest.fixture
def script(repository: Repository) -> Callable[..., subprocess.CompletedProcess]:
    """Run one of the release scripts with the repository as its directory.

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
            text=True,
        )

    return run


def _load(name: str) -> ModuleType:
    """Import a build hook module by path, under a name of this suite's own.

    ``build-hooks`` holds modules named ``build`` and ``metadata``; putting
    it on ``sys.path`` would shadow those names for every test that runs
    afterward.
    """
    if (module := sys.modules.get(name)) is not None:
        return module
    specification = importlib.util.spec_from_file_location(
        name, BUILD_HOOKS / f"{name.rpartition('.')[2]}.py"
    )
    assert specification and specification.loader
    module = importlib.util.module_from_spec(specification)
    # Registered before execution so the hook's own ``from _version import``
    # resolves to this module rather than re-executing it under that name.
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def semantic_version() -> type:
    """The version model the release tooling's output is parsed against."""
    return _load("_version").SemanticVersion


@pytest.fixture
def version(semantic_version: type, repository: Repository, monkeypatch) -> Callable:
    """Render the build metadata hook's version for the repository."""
    monkeypatch.chdir(repository.path)
    # Importing the hook registers it with the version parser registry,
    # which is what makes it reachable as ``parse.git``.
    _load("_git")
    return lambda: semantic_version.parse.git()
