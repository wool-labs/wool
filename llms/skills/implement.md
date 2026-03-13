---
name: implement
description: >
  Implement a planned PR or issue. Use this skill whenever the user says
  "/implement <number>", "implement #N", "start implementing #N", or similar.
  Accepts a PR or issue number, resolves it to a draft PR (creating one via
  the /pr skill if needed), checks out the branch, and enters plan mode to
  design a concrete execution plan before writing code.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT, REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be interpreted as described in RFC 2119.

# Implement Skill

Resolve a PR or issue number, check out the branch, read the PR description, and enter plan mode to design a concrete execution plan before writing any code.

## Pipeline Context

This skill is part of the development workflow pipeline: `/issue` → `/pr` → `/implement` → `/commit` → `/pr` (update). This skill is the **third** stage.

## Arguments

A PR or issue number MUST be provided as the sole argument (e.g., `/implement 103`).

## Workflow

### TL;DR

1. Resolve target repository
2. Resolve the input to a PR
3. Assign the PR and issue
4. Check out the PR branch
5. Read the PR description
6. Gather context
7. Enter plan mode
8. Execute after approval
9. Prompt the user to move onto the commit step

### 1. Resolve target repository

```bash
gh repo view --json isFork,parent
```

If `isFork` is `true`, extract `parent.owner.login` and `parent.name` to form the upstream repo identifier (`<owner>/<name>`). This upstream identifier becomes the **target repo** for all subsequent `gh` commands that reference issues or pull requests. If the repo is not a fork, the target repo is the current repo and no `--repo` flag is needed.

**User override:** If the user explicitly asks to target the fork — by saying "fork", "on the fork", "fork #N", or similar — the target repo MUST be set to the current (fork) repo instead of upstream. The user's explicit intent always takes precedence.

All `gh` commands in subsequent steps that reference issues or PRs MUST include `--repo <target>` when the target repo differs from the current repo.

### 2. Resolve the input to a PR

Run `gh pr view <number> --repo <target>` to check if the number is a PR.

- **If it is a PR** — use it directly.
- **If it is not a PR** (the command fails) — run `gh issue view <number> --repo <target>` to confirm it is an issue.
  - **If it is an issue** — query for linked PRs via the Development sidebar field:
    ```bash
    gh issue view <number> --repo <target> --json closedByPullRequestsReferences \
      --jq '.closedByPullRequestsReferences[].number'
    ```
    - If a linked PR exists, use it.
    - If no linked PR exists, invoke the `/pr` skill to create one. Stop after the PR is created — the user MUST re-run `/implement` once the PR draft is approved.
- **If the number is neither a PR nor an issue** — inform the user and stop.

The `--repo <target>` flag ensures commands operate against the upstream repo when working from a fork (as resolved in step 1). If the target repo is the current repo, the flag MAY be omitted.

### 3. Assign the PR and issue

Assign both the PR and its linked issue to the current user so that ownership is visible on the board:

```bash
gh pr edit <number> --repo <target> --add-assignee @me
```

The linked issue number is already known from the PR's summary (the `Closes #<issue>` reference parsed in step 2). Assign it as well:

```bash
gh issue edit <issue-number> --repo <target> --add-assignee @me
```

The `--repo <target>` flag MUST be included when the target repo differs from the current repo.

### 4. Check out the PR branch

Extract the branch name from the PR:

```bash
gh pr view <number> --repo <target> --json headRefName --jq .headRefName
```

Fetch and check out the branch:

```bash
git fetch origin <branch>
git checkout <branch>
```

If the local branch is behind the remote, pull to bring it up to date.

### 5. Read the PR description

Fetch the full PR body:

```bash
gh pr view <number> --repo <target> --json body,title
```

Parse the four sections: **Summary**, **Proposed changes**, **Test cases**, and **Implementation plan**.

The implementation plan's checkbox list is the primary input for planning. If all items are already checked off, inform the user that the implementation plan appears complete and stop. If the plan is partially checked off, only plan the remaining unchecked items.

### 6. Gather context

Before entering plan mode, read enough of the codebase to plan confidently:

- Read every source file referenced in the PR's "Proposed changes" section.
- Read existing tests for the affected modules.
- Read the project test guide (`@llm/guides/testguide-python.md`) to internalize testing conventions.
- Read project-level instructions (`CLAUDE.md`) for build tooling, documentation style, and architecture context.

### 7. Enter plan mode

Call `EnterPlanMode` to begin the planning phase. The execution plan:

- MUST map each unchecked item from the PR's implementation plan to concrete code changes: exact files, functions, classes, and the nature of each modification.
- SHOULD prefer test-first ordering when the PR's plan supports it.
- MUST follow the testing conventions in the project test guide (`@llm/guides/testguide-python.md`). Test case IDs (e.g., WC-001, VP-001) MUST NOT be assigned — the docstring provides sufficient traceability without the maintenance burden of cross-PR ID schemes.
- MUST include a verification section with the exact command(s) to run the test suite (see the project test guide for the runner command).

### 8. Execute after approval

Once the user approves the plan, implement each step sequentially.

### 9. Prompt the user to move onto the commit step

The user MUST be prompted with the next pipeline step: "Ready to commit? Run `/commit` to stage and commit the changes." DO NOT proceed on your own.

## Edge cases

- **All items already checked off:** Inform the user that the implementation plan appears complete and stop.
- **Partially checked off:** Only plan the remaining unchecked items.
- **PR is not a draft:** Proceed normally — the PR may have been marked ready for review before implementation was complete.
- **Merge conflicts:** Stop and explain the situation rather than trying to resolve automatically.
