import git
from _version import PythonicVersion, parser


@parser("git")
def parse() -> str:
    """
    Parses the current git repository to generate a version string.

    Returns:
        A version string based on the latest tag, commit hash, and uncommitted
        changes.

    Raises:
        RuntimeError: If the repository is empty.
    """
    repo = git.Repo(search_parent_directories=True)
    if repo.bare:
        raise RuntimeError(f"The repo at '{repo.working_dir}' cannot be empty!")
    head_commit = repo.head.commit
    try:
        tag = max(repo.tags, key=lambda t: PythonicVersion.parse(str(t)))
    except ValueError:
        tag_name = "0"
        tag_commit = None
    else:
        tag_name = tag.name
        tag_commit = tag.commit
    public, *local = tag_name.split("+")
    if head_commit != tag_commit:
        commit_label = repo.git.rev_parse(head_commit.hexsha, short=True)
        local.append(commit_label)
    dirty = repo.index.diff(None) or repo.untracked_files
    if dirty:
        local.append("dirty")
    local = ".".join(local)
    return f"{public}+{local}" if local else public
