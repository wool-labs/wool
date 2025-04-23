#!/usr/bin/env python3
"""Rewrite git history to follow the release workflow branching model."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass


@dataclass
class Commit:
    sha: str
    message: str


@dataclass
class Interval:
    tag: str
    branch_name: str
    commits: list[Commit]
    expected_sha: str
    merge_message_release: str = ""
    merge_message_main: str = ""


INTERVALS: list[Interval] = [
    Interval(
        tag="v0.1rc1",
        branch_name="wool_locking_subpackage",
        commits=[
            Commit("a49c595", "ci: Fix glob patterns in label-pr, publish-release, and validate-pr workflows"),
            Commit("3bbf219", "ci: Fix glob patterns in label-pr, publish-release, and validate-pr workflows"),
            Commit("f2c160c", "feat: Implement wool-locking subpackage"),
            Commit("5c6421b", "docs: Update wool-locking package description"),
        ],
        expected_sha="5c6421b",
    ),
    Interval(
        tag="v0.1rc2",
        branch_name="namespace_build_fixes",
        commits=[
            Commit("b36d9aa", "build: Create symlink to .git in namespace packages prior to building"),
            Commit("d7bccee", "fix: Update wool.__init__.py to update modules on public symbols"),
        ],
        expected_sha="d7bccee",
    ),
    Interval(
        tag="v0.1rc3",
        branch_name="scm_root_config",
        commits=[Commit("202cd5d", "build: Set SCM root to .git")],
        expected_sha="202cd5d",
    ),
    Interval(
        tag="v0.1rc4",
        branch_name="release_title_and_classifiers",
        commits=[
            Commit("604afc1", "ci: Fix GitHub release title generation and remove tag verification"),
            Commit("910eb5f", "build: Add package classifiers"),
        ],
        expected_sha="910eb5f",
    ),
    Interval(
        tag="v0.1rc5",
        branch_name="release_workflow_and_formatting",
        commits=[
            Commit("2e91774", "ci: Update workflows to create GitHub release once and push all namespace packages"),
            Commit("5dfe7e1", "style: Fix formatting"),
        ],
        expected_sha="5dfe7e1",
    ),
    Interval(
        tag="v0.1rc6",
        branch_name="remove_invalid_classifiers",
        commits=[Commit("f58238a", "build: Remove invalid classifiers")],
        expected_sha="f58238a",
    ),
    Interval(
        tag="v0.1rc7",
        branch_name="hatchling_migration_and_mempool",
        commits=[
            Commit("c68a29c", "ci: Update sync-branches workflow to approve generated sync PRs as wool-labs"),
            Commit("2131792", "build: Migrate to hatchling build backend and implement custom build hooks"),
            Commit("7102423", "ci: Fix Build release action"),
            Commit("6bb3ea2", "feat: Implement shared memory pool"),
        ],
        expected_sha="6bb3ea2",
    ),
    Interval(
        tag="v0.1rc8",
        branch_name="grpc_mempool_service",
        commits=[
            Commit("57d27ad", "feat: Add gRPC-based MemoryPool service implementation"),
            Commit("4706a7d", "refactor: Improve MemoryPool caching and resource management"),
            Commit("c180154", "refactor: Clean up mempool.proto protobuf definitions and update type hints"),
            Commit("9740ffb", "build: Add grpcio to wool project dependencies"),
            Commit("740ae2c", "test: Update tests to use latest version of pytest-grpc-aio"),
            Commit("0088e10", "fix: Add fallback for typing.Self import to support Python 3.10"),
            Commit("8804fec", "refactor: Implement PR feedback"),
        ],
        expected_sha="b770f22",
    ),
    Interval(
        tag="v0.1rc9",
        branch_name="mempool_session_client",
        commits=[Commit("700ba4f", "feat: Implement memory pool session client")],
        expected_sha="700ba4f",
    ),
    Interval(
        tag="v0.1rc10",
        branch_name="distributed_worker_pool",
        commits=[Commit("a74140d", "feat: Implement gRPC-based distributed worker pool with service discovery and load balancing")],
        expected_sha="a74140d",
    ),
    Interval(
        tag="v0.1rc11",
        branch_name="fix_worker_factory_default",
        commits=[Commit("51369be", "fix: Fix bug in WorkerPool default worker factory")],
        expected_sha="51369be",
    ),
    Interval(
        tag="v0.1rc12",
        branch_name="refactor_factory_protocols",
        commits=[Commit("2e55f25", "refactor: Refactor Worker and WorkerProxy to accept RegistrarLike, DiscoveryLike, and LoadBalancerLike factories")],
        expected_sha="2e55f25",
    ),
    Interval(
        tag="v0.1rc13",
        branch_name="fix_version_parser",
        commits=[Commit("a073523", "fix: Fix latent bug in git version parser")],
        expected_sha="a073523",
    ),
    Interval(
        tag="v0.1rc14",
        branch_name="readme_updates_and_mempool_optimization",
        commits=[
            Commit("e7607c6", "docs: Update wool package README.md"),
            Commit("7300625", "docs: Fix URL in root README.md"),
            Commit("3756b1d", "perf: Optimize shared memory usage and limit creation rights to WorkerPool"),
        ],
        expected_sha="3756b1d",
    ),
    Interval(
        tag="v0.1rc15",
        branch_name="worker_config_and_cicd_fixes",
        commits=[
            Commit("14ee560", "ci: Make publish-release workflow manually runnable"),
            Commit("52cdaa7", "ci: Fix bug causing tag-version job or publish-release workflow to be skipped"),
            Commit("42d474f", "docs: Update README"),
            Commit("139eebc", "feat: Expose various worker timeouts and concurrency limits as function arguments"),
        ],
        expected_sha="139eebc",
    ),
    Interval(
        tag="v0.1rc16",
        branch_name="refactor_discovery_subpackage",
        commits=[
            Commit("d6cedd8", "docs: Replace README.md"),
            Commit("33c2ca5", "refactor: Refactor discovery logic into a multi-layer subpackage"),
        ],
        expected_sha="33c2ca5",
    ),
    Interval(
        tag="v0.1rc17",
        branch_name="refactor_loadbalancer_subpackage",
        commits=[Commit("a7cdc89", "refactor: Refactor load balancing logic into a multi-layer subpackage")],
        expected_sha="a7cdc89",
    ),
    Interval(
        tag="v0.1rc18",
        branch_name="refactor_worker_subpackage",
        commits=[Commit("6581a31", "refactor: Refactor worker logic into a multi-layer subpackage")],
        expected_sha="6581a31",
    ),
    Interval(
        tag="v0.1rc19",
        branch_name="move_connection_and_service",
        commits=[Commit("e890d5d", "refactor: Move _connection.py and _worker_service.py into worker subpackage")],
        expected_sha="e890d5d",
    ),
    Interval(
        tag="v0.1rc20",
        branch_name="move_worker_pool_and_proxy",
        commits=[Commit("aa3b5fc", "refactor: Move _worker_pool.py and _worker_proxy.py into worker subpackage")],
        expected_sha="aa3b5fc",
    ),
    Interval(
        tag="v0.1rc21",
        branch_name="move_resource_pool_and_typing",
        commits=[Commit("cc4d9cf", "refactor: Move _resource_pool.py, _typing.py, and _context.py into worker subpackage")],
        expected_sha="cc4d9cf",
    ),
    Interval(
        tag="v0.1rc22",
        branch_name="refactor_work_subpackage",
        commits=[Commit("58521eb", "refactor: Refactor work logic into a multi-layer subpackage")],
        expected_sha="58521eb",
    ),
    Interval(
        tag="v0.1rc23",
        branch_name="rename_core_to_runtime",
        commits=[Commit("8de60b6", "refactor: Rename core to runtime")],
        expected_sha="8de60b6",
    ),
    Interval(
        tag="v0.1rc24",
        branch_name="refactor_event_system",
        commits=[Commit("35cfb94", "refactor: Refactor event system into a generic base class")],
        expected_sha="35cfb94",
    ),
    Interval(
        tag="v0.1rc25",
        branch_name="distributed_async_generators",
        commits=[Commit("b339b3c", "feat: Add support for distributed async generator work tasks")],
        expected_sha="b339b3c",
    ),
    Interval(
        tag="v0.1rc26",
        branch_name="tls_worker_auth",
        commits=[Commit("8f2180b", "feat: Implement TLS-based worker authentication")],
        expected_sha="8f2180b",
    ),
    Interval(
        tag="v0.1rc27",
        branch_name="thread_pool_and_cicd_fixes",
        commits=[
            Commit("d5c3d25", "ci: Fix several issues with CI/CD pipeline"),
            Commit("a739c77", "feat: Offload task execution to thread pool and fix concurrent dispatch bug"),
            Commit("207cc40", "ci: Fix token bug in publish-release workflow"),
            Commit("97c4b95", "ci: Pass new version downstream in publish-release workflow"),
        ],
        expected_sha="97c4b95",
    ),
    Interval(
        tag="v0.1rc28",
        branch_name="fix_lifecycle_events",
        commits=[Commit("90dd3fc", "fix: Fix bug in task-completed lifecycle event emission; Make wool.routine the canonical API")],
        expected_sha="90dd3fc",
    ),
    Interval(
        tag="v0.1rc29",
        branch_name="event_handler_thread",
        commits=[Commit("9ecf2ce", "feat: Implement dedicated event handler thread")],
        expected_sha="9ecf2ce",
    ),
    Interval(
        tag="v0.1rc30",
        branch_name="version_tag_and_cancellation_fix",
        commits=[
            Commit("bcecf67", "ci: Do not push new version tag in bump-version job"),
            Commit("eb885d5", "fix: Fix race condition in task cancellation during worker shutdown"),
        ],
        expected_sha="eb885d5",
    ),
    Interval(
        tag="v0.1rc31",
        branch_name="discovery_filter_and_docs",
        commits=[Commit("e909835", "feat: Add optional filter argument to LanDiscovery and LocalDiscovery constructors; Update discovery docstrings")],
        expected_sha="e909835",
    ),
    Interval(
        tag="v0.1rc32",
        branch_name="loadbalancer_protocol",
        commits=[Commit("bc1e512", "refactor: Create LoadBalancerContextLike protocol to replace LoadBalancerContext in public API; Update load balancer docstrings")],
        expected_sha="bc1e512",
    ),
    Interval(
        tag="v0.1rc33",
        branch_name="rename_work_to_routine",
        commits=[Commit("28cbd0c", "refactor: Rename work subpackage to routine; Update READMEs for discovery, loadbalancer, and routine subpackages")],
        expected_sha="28cbd0c",
    ),
    Interval(
        tag="v0.1rc34",
        branch_name="worker_metadata_address",
        commits=[Commit("72c0d00", "refactor: Replace WorkerMetadata host and port with address; Add worker subpackage README")],
        expected_sha="72c0d00",
    ),
    Interval(
        tag="v0.1rc35",
        branch_name="channel_pooling_and_docs",
        commits=[
            Commit("d99e862", "ci: Add issue templates for bug reports, feature requests, etc."),
            Commit("269510f", "docs: Add LLM workflow guides and skill definitions"),
            Commit("dc1493a", "docs: State explicitly that Markdown text should not be hard-wrapped in relevant LLM skills"),
            Commit("f971915", "ci: Add synchronize trigger to label-pr workflow"),
            Commit("56ec440", "fix: Handle closed event loop in ResourcePool cross-loop cleanup"),
            Commit("ab93cf0", "fix: Pool gRPC channels in WorkerConnection to outlive dispatch scope"),
            Commit("f2a485d", "test: Update tests for channel-pooled WorkerConnection"),
            Commit("ed0a55b", "docs: Update READMEs for channel-pooled WorkerConnection"),
            Commit("de59ce4", "refactor: Give _Channel its own close() method"),
            Commit("5624022", "test: Stop importing _channel_pool internals in tests"),
            Commit("d9730f8", "style: Fix test docstring format and remove duplicate decorator"),
        ],
        expected_sha="d9730f8",
    ),
    Interval(
        tag="v0.1rc36",
        branch_name="worker_factory_credentials",
        commits=[
            Commit("e338524", "test: Add regression tests for WorkerFactory credential passing"),
            Commit("cd0f114", "fix: Pass full WorkerCredentials to worker factory"),
            Commit("25153df", "docs: Update WorkerFactory protocol snippet in README"),
        ],
        expected_sha="25153df",
    ),
]

PRE_RC0_COMMITS: list[Commit] = [
    Commit("385b9b1", "Initial commit"),
    Commit("25ab44f", "ci: Add CI/CD workflows"),
    Commit("44a293b", "feat: Add project setup, core functionality, and CLI commands"),
]

POST_RC36_COMMIT = Commit("269a6b8", "fix: Fix root README symlink")
FINAL_EXPECTED_SHA = "651198b"


class GitOps:
    def __init__(self, *, dry_run: bool = False, verbose: bool = False):
        self.dry_run = dry_run
        self.verbose = verbose

    def _exec(self, *args: str, env: dict[str, str] | None = None, check: bool = True) -> str:
        cmd = ["git", *args]
        full_env = {**os.environ, **(env or {})}
        result = subprocess.run(cmd, capture_output=True, text=True, env=full_env)
        if check and result.returncode != 0:
            raise RuntimeError(f"Command failed: {' '.join(cmd)}\nstdout: {result.stdout}\nstderr: {result.stderr}")
        return result.stdout.strip()

    def run(self, *args: str, env: dict[str, str] | None = None, check: bool = True) -> str:
        cmd = ["git", *args]
        if self.verbose or self.dry_run:
            env_prefix = " ".join(f"{k}={v}" for k, v in (env or {}).items())
            prefix = f"{env_prefix} " if env_prefix else ""
            print(f"  $ {prefix}{' '.join(cmd)}", flush=True)
        if self.dry_run:
            return ""
        return self._exec(*args, env=env, check=check)

    def get_author_info(self, sha: str) -> tuple[str, str, str]:
        fmt = self._exec("log", "--format=%an%n%ae%n%aI", "-1", sha)
        lines = fmt.split("\n")
        return lines[0], lines[1], lines[2]

    def get_tree(self, sha: str) -> str:
        return self._exec("rev-parse", f"{sha}^{{tree}}")

    def cherry_pick_no_commit(self, sha: str) -> bool:
        result = subprocess.run(["git", "cherry-pick", "--no-commit", sha], capture_output=True, text=True)
        if result.returncode != 0:
            subprocess.run(["git", "cherry-pick", "--abort"], capture_output=True)
            return False
        return True

    def read_tree_reset(self, target_sha: str) -> None:
        self.run("read-tree", "-u", "--reset", f"{target_sha}^{{tree}}")

    def commit(self, message: str, author_name: str, author_email: str, author_date: str) -> str:
        env = {
            "GIT_AUTHOR_NAME": author_name,
            "GIT_AUTHOR_EMAIL": author_email,
            "GIT_AUTHOR_DATE": author_date,
            "GIT_COMMITTER_DATE": author_date,
        }
        self.run("commit", "--allow-empty", "-m", message, env=env)
        return self._exec("rev-parse", "HEAD") if not self.dry_run else ""

    def commit_tree(
        self, tree_sha: str, parents: list[str], message: str,
        author_name: str, author_email: str, author_date: str,
        committer_date: str | None = None,
    ) -> str:
        cmd = ["git", "commit-tree", tree_sha]
        for p in parents:
            cmd += ["-p", p]
        cmd += ["-m", message]
        env = {
            **os.environ,
            "GIT_AUTHOR_NAME": author_name,
            "GIT_AUTHOR_EMAIL": author_email,
            "GIT_AUTHOR_DATE": author_date,
            "GIT_COMMITTER_DATE": committer_date or author_date,
        }
        if self.verbose or self.dry_run:
            print(f"  $ {' '.join(cmd)}", flush=True)
        if self.dry_run:
            return "dry-run-sha"
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            raise RuntimeError(f"commit-tree failed: {result.stderr}")
        return result.stdout.strip()

    def no_ff_merge(self, source_branch: str, message: str) -> str:
        self.run("merge", "--no-ff", source_branch, "-m", message)
        return self._exec("rev-parse", "HEAD") if not self.dry_run else ""

    def create_branch(self, name: str, start_point: str | None = None) -> None:
        if start_point:
            self.run("branch", name, start_point)
        else:
            self.run("branch", name)

    def checkout(self, ref: str) -> None:
        self.run("checkout", ref)

    def checkout_new(self, name: str) -> None:
        self.run("checkout", "-b", name)

    def checkout_orphan(self, name: str) -> None:
        self.run("checkout", "--orphan", name)
        self.run("rm", "-rf", ".", check=False)

    def tag(self, name: str) -> None:
        self.run("tag", "-f", name)

    def delete_branch(self, name: str) -> None:
        self.run("branch", "-D", name)

    def verify_tree(self, expected_sha: str) -> bool:
        diff = self._exec("diff-tree", "--no-commit-id", "-r", "HEAD", expected_sha)
        return diff == ""

    def current_sha(self) -> str:
        return self._exec("rev-parse", "HEAD")

    def update_ref(self, ref: str, sha: str) -> None:
        self.run("update-ref", ref, sha)

    def reset_hard(self, ref: str) -> None:
        self.run("reset", "--hard", ref)


def cherry_pick_commit(git: GitOps, commit: Commit) -> str:
    name, email, date = git.get_author_info(commit.sha)
    if not git.dry_run:
        success = git.cherry_pick_no_commit(commit.sha)
        if not success:
            print(f"    Cherry-pick conflict for {commit.sha}, using read-tree fallback")
            git.read_tree_reset(commit.sha)
    else:
        print(f"  $ git cherry-pick --no-commit {commit.sha}")
    return git.commit(commit.message, name, email, date)


def process_interval(git: GitOps, interval: Interval, interval_idx: int) -> None:
    total = len(INTERVALS)
    print(f"\n[{interval_idx + 1}/{total}] Processing {interval.tag} ({interval.branch_name})")

    git.checkout("release")
    git.checkout_new(interval.branch_name)

    for i, commit in enumerate(interval.commits):
        print(f"  Commit {i + 1}/{len(interval.commits)}: {commit.sha[:7]} {commit.message[:60]}")
        cherry_pick_commit(git, commit)

    if not git.dry_run:
        if not git.verify_tree(interval.expected_sha):
            print(f"  Tree mismatch after cherry-picks, forcing tree to match {interval.expected_sha}")
            git.read_tree_reset(interval.expected_sha)
            git.run("add", "-A")
            name, email, date = git.get_author_info(interval.expected_sha)
            subprocess.run(
                ["git", "commit", "--amend", "--no-edit", "--allow-empty"],
                env={**os.environ, "GIT_AUTHOR_NAME": name, "GIT_AUTHOR_EMAIL": email,
                     "GIT_AUTHOR_DATE": date, "GIT_COMMITTER_DATE": date},
                capture_output=True,
            )

    merge_msg_release = interval.merge_message_release or f"Merge branch '{interval.branch_name}' into release"
    git.checkout("release")

    if not git.dry_run:
        feature_sha = git._exec("rev-parse", interval.branch_name)
        release_sha = git.current_sha()
        target_tree = git.get_tree(interval.expected_sha)
        name, email, date = git.get_author_info(interval.expected_sha)
        merge_sha = git.commit_tree(
            target_tree, [release_sha, feature_sha], merge_msg_release,
            name, email, date, committer_date=date,
        )
        git.update_ref("refs/heads/release", merge_sha)
        git.reset_hard("release")
    else:
        git.no_ff_merge(interval.branch_name, merge_msg_release)

    git.tag(interval.tag)

    if not git.dry_run:
        if not git.verify_tree(interval.expected_sha):
            raise RuntimeError(f"FATAL: Tree mismatch on release after merge for {interval.tag}")
        print(f"  Verified: release tree matches {interval.expected_sha}")

    merge_msg_main = interval.merge_message_main or "Merge branch 'release' into main"
    git.checkout("new-main")

    if not git.dry_run:
        main_sha = git.current_sha()
        release_sha = git._exec("rev-parse", "release")
        target_tree = git.get_tree(interval.expected_sha)
        name, email, date = git.get_author_info(interval.expected_sha)
        merge_sha = git.commit_tree(
            target_tree, [main_sha, release_sha], merge_msg_main,
            name, email, date, committer_date=date,
        )
        git.update_ref("refs/heads/new-main", merge_sha)
        git.reset_hard("new-main")
    else:
        git.no_ff_merge("release", merge_msg_main)

    git.delete_branch(interval.branch_name)


def process_post_rc36(git: GitOps) -> None:
    print("\nProcessing post-rc36: fix_readme_symlink")
    branch_name = "fix_readme_symlink"
    commit = POST_RC36_COMMIT

    git.checkout("release")
    git.checkout_new(branch_name)

    name, email, date = git.get_author_info(commit.sha)
    print(f"  Commit: {commit.sha[:7]} {commit.message}")

    if not git.dry_run:
        success = git.cherry_pick_no_commit(commit.sha)
        if not success:
            print("    Cherry-pick conflict, using read-tree fallback")
            git.read_tree_reset(FINAL_EXPECTED_SHA)
        git.commit(commit.message, name, email, date)

        if not git.verify_tree(FINAL_EXPECTED_SHA):
            print(f"  Forcing tree to match {FINAL_EXPECTED_SHA}")
            git.read_tree_reset(FINAL_EXPECTED_SHA)
            git.run("add", "-A")
            subprocess.run(
                ["git", "commit", "--amend", "--no-edit", "--allow-empty"],
                env={**os.environ, "GIT_AUTHOR_NAME": name, "GIT_AUTHOR_EMAIL": email,
                     "GIT_AUTHOR_DATE": date, "GIT_COMMITTER_DATE": date},
                capture_output=True,
            )
    else:
        print(f"  $ git cherry-pick --no-commit {commit.sha}")
        git.commit(commit.message, name, email, date)

    merge_msg_release = f"Merge branch '{branch_name}' into release"
    git.checkout("release")

    if not git.dry_run:
        feature_sha = git._exec("rev-parse", branch_name)
        release_sha = git.current_sha()
        target_tree = git.get_tree(FINAL_EXPECTED_SHA)
        merge_sha = git.commit_tree(
            target_tree, [release_sha, feature_sha], merge_msg_release,
            name, email, date, committer_date=date,
        )
        git.update_ref("refs/heads/release", merge_sha)
        git.reset_hard("release")
    else:
        git.no_ff_merge(branch_name, merge_msg_release)

    merge_msg_main = "Merge branch 'release' into main"
    git.checkout("new-main")

    if not git.dry_run:
        main_sha = git.current_sha()
        release_sha = git._exec("rev-parse", "release")
        target_tree = git.get_tree(FINAL_EXPECTED_SHA)
        merge_sha = git.commit_tree(
            target_tree, [main_sha, release_sha], merge_msg_main,
            name, email, date, committer_date=date,
        )
        git.update_ref("refs/heads/new-main", merge_sha)
        git.reset_hard("new-main")
    else:
        git.no_ff_merge("release", merge_msg_main)

    git.delete_branch(branch_name)


def validate_all_tags(git: GitOps) -> bool:
    print("\nValidating all tags...")
    tag_map: dict[str, str] = {"v0.1rc0": "44a293b"}
    for interval in INTERVALS:
        tag_map[interval.tag] = interval.expected_sha

    all_ok = True
    for tag_name, expected_sha in sorted(tag_map.items()):
        if git.dry_run:
            print(f"  [DRY RUN] Would verify {tag_name} against {expected_sha}")
            continue
        tag_tree = git.get_tree(tag_name)
        expected_tree = git.get_tree(expected_sha)
        if tag_tree == expected_tree:
            print(f"  OK: {tag_name}")
        else:
            print(f"  MISMATCH: {tag_name}")
            all_ok = False
    return all_ok


def validate_structure(git: GitOps) -> bool:
    if git.dry_run:
        return True
    print("\nValidating branch structure (first-parent lineage)...")
    all_ok = True
    for branch in ["new-main", "release"]:
        rc0_sha = git._exec("rev-parse", "v0.1rc0")
        log = git._exec("log", "--first-parent", "--format=%H %P", f"{rc0_sha}..{branch}")
        if not log:
            continue
        for line in log.split("\n"):
            parts = line.split()
            if len(parts[1:]) < 2:
                print(f"  WARNING: Non-merge commit {parts[0][:10]} on {branch}")
                all_ok = False
    if all_ok:
        print("  OK: All post-rc0 first-parent commits on main/release are merge commits")
    return all_ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Rewrite git history to follow release workflow")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--resume-from", metavar="TAG")
    args = parser.parse_args()

    git = GitOps(dry_run=args.dry_run, verbose=args.verbose)

    resume_idx = 0
    if args.resume_from:
        for i, interval in enumerate(INTERVALS):
            if interval.tag == args.resume_from:
                resume_idx = i
                break
        else:
            print(f"Error: Unknown tag {args.resume_from}", file=sys.stderr)
            sys.exit(1)

    if resume_idx == 0:
        print("Step 1: Creating backup branch")
        git.create_branch("backup/main-before-rewrite", "main")

        print("\nStep 2: Creating orphan new-main with pre-rc0 commits")
        git.checkout_orphan("new-main")
        for i, commit in enumerate(PRE_RC0_COMMITS):
            print(f"  Commit {i + 1}/{len(PRE_RC0_COMMITS)}: {commit.sha[:7]} {commit.message[:60]}")
            name, email, date = git.get_author_info(commit.sha)
            if not git.dry_run:
                git.read_tree_reset(commit.sha)
                git.run("add", "-A")
            git.commit(commit.message, name, email, date)

        print("\nStep 3: Tagging v0.1rc0 and creating release branch")
        git.tag("v0.1rc0")
        git.create_branch("release")
        if not git.dry_run:
            if git.verify_tree("44a293b"):
                print("  Verified: v0.1rc0 tree matches 44a293b")
            else:
                raise RuntimeError("v0.1rc0 tree does not match 44a293b!")
    else:
        print(f"Resuming from {args.resume_from} (interval index {resume_idx})")
        git.checkout("new-main")

    print(f"\nStep 4: Processing {len(INTERVALS)} intervals")
    for i, interval in enumerate(INTERVALS):
        if i < resume_idx:
            continue
        process_interval(git, interval, i)

    print("\nStep 5: Post-rc36 README symlink fix")
    process_post_rc36(git)

    if not git.dry_run:
        if git.verify_tree(FINAL_EXPECTED_SHA):
            print(f"  Verified: final HEAD tree matches {FINAL_EXPECTED_SHA}")
        else:
            raise RuntimeError(f"Final HEAD tree does not match {FINAL_EXPECTED_SHA}!")

    print("\nStep 6: Validation")
    tags_ok = validate_all_tags(git)
    structure_ok = validate_structure(git)

    if not tags_ok or not structure_ok:
        print("\nWARNING: Some validations failed.")
        if not git.dry_run:
            sys.exit(1)

    print("\nStep 7: Replacing main with new-main")
    git.checkout("new-main")
    git.run("branch", "-M", "new-main", "main")

    print("\nDone! New history has been written.")
    print("\nTo inspect:")
    print("  git log --oneline --graph --all --decorate | head -80")
    print("\nTo push (force required):")
    print("  git push --force --tags origin main release")


if __name__ == "__main__":
    main()
