import git
from _version import parser


@parser("git")
def parse() -> str:
    """
    Parses the current git repository to generate a version string.

    A commit that carries a tag is versioned as that tag, so a release
    artifact is labelled with the version it was cut as. Any other commit is
    versioned as the nearest tag reachable from it -- tags on branches the
    commit does not reach are invisible, so a pending release candidate never
    labels a build of the production line -- qualified by a local identifier
    naming the commit, and further qualified as ``dirty`` when the worktree
    carries uncommitted changes. A commit with no tag in its lineage is
    versioned as ``0.0.0``.

    Returns:
        A version string based on the tag reachable at HEAD, the commit hash,
        and uncommitted changes.

    Raises:
        RuntimeError: If the repository is bare.
    """
    repo = git.Repo(search_parent_directories=True)
    if repo.bare:
        raise RuntimeError(f"The repo at '{repo.working_dir}' cannot be empty!")
    try:
        tag_name = repo.git.describe("--tags", "--exact-match")
    except git.GitCommandError:
        exact = False
        try:
            tag_name = repo.git.describe("--tags", "--abbrev=0")
        except git.GitCommandError:
            tag_name = "0.0.0"
    else:
        exact = True
    public, *local = tag_name.split("+")
    if not exact:
        local.append(repo.git.rev_parse(repo.head.commit.hexsha, short=True))
    if repo.is_dirty(untracked_files=True):
        local.append("dirty")
    local = "-".join(local)
    return f"{public}+{local}" if local else public
