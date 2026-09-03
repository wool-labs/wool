import re

import git
from _version import parser
from packaging.version import Version

#: The tag shape this project's release tooling produces. Kept in step with
#: the channel patterns in .github/scripts/latest-version.sh, which selects
#: the version to bump from the same set.
VERSION_TAG = re.compile(r"^v[0-9]+\.[0-9]+\.(?:[0-9]+|0-(?:a|b|rc)[0-9]+)$")


@parser("git")
def parse() -> str:
    """
    Derive the package version from the current git repository.

    A commit that carries a version tag is versioned as the highest of them,
    so an artifact built from a checked-out release tag is labelled with that
    release. Any other commit is versioned as the highest version tag
    reachable from it, qualified by a local identifier naming the commit,
    which marks the artifact as a build between releases rather than as one.
    Either version is further qualified as ``dirty`` when the worktree
    carries uncommitted or untracked changes.

    Only tags of the shape the release tooling produces are considered, so a
    tag outside the scheme neither labels a build nor breaks one. A commit
    with no version tag in its lineage is versioned as ``v0.0.0``, with the
    same local identifier.

    Tags are ordered by version and not by commit distance, which a merge
    from an older fork point inverts — labelling a build with a version below
    one already released.

    Returns:
        The tag, with a local identifier appended when the commit is not the
        release the tag names.

    Raises:
        RuntimeError: If the repository is bare or carries no commits.
    """
    repo = git.Repo(search_parent_directories=True)
    if repo.bare:
        raise RuntimeError(f"The repo at '{repo.working_dir}' cannot be bare!")
    if not repo.head.is_valid():
        raise RuntimeError(f"The repo at '{repo.working_dir}' has no commits!")
    if tags := _versions(repo, "--points-at", "HEAD"):
        exact, tag_name = True, tags[0]
    else:
        exact, tag_name = (
            False,
            next(iter(_versions(repo, "--merged", "HEAD")), "v0.0.0"),
        )
    local = []
    if not exact:
        local.append(repo.git.rev_parse("HEAD", short=True))
    if repo.is_dirty(untracked_files=True):
        local.append("dirty")
    return f"{tag_name}+{'-'.join(local)}" if local else tag_name


def _versions(repo: git.Repo, *selector: str) -> list[str]:
    """
    Return the repository's version tags for a selector, highest first.

    The selector is a ``git tag`` filter, e.g. ``--points-at HEAD``.
    """
    return sorted(
        (tag for tag in repo.git.tag(*selector).splitlines() if VERSION_TAG.match(tag)),
        key=Version,
        reverse=True,
    )
