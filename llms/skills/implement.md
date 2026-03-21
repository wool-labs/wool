---
name: implement
description: >
  Implement a GitHub issue. Use this skill whenever the user says
  "/implement <number>", "implement #N", "start implementing #N", or similar.
  Accepts an issue number, fetches the issue, creates a branch, gathers
  context, and enters plan mode to design a concrete execution plan before
  writing code. On re-invocation after a PR exists, addresses unresolved
  review feedback.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT, REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be interpreted as described in RFC 2119.

# Implement Skill

Fetch a GitHub issue, create a branch, gather codebase context, and enter plan mode to design a concrete execution plan before writing any code. On re-invocation when a PR already exists, address unresolved review feedback or verify implementation completeness.

## Pipeline Context

This skill is part of the development workflow pipeline: `/issue` → `/implement` → `/test` → `/commit` → `/pr`. This skill is the **second** stage. The implement, test, and commit steps are iterative — they can be invoked multiple times for a given issue to address PR feedback or refine the implementation.

## Arguments

An issue number MUST be provided as the sole argument (e.g., `/implement 103`).

## Workflow

### TL;DR

1. Resolve target repository
2. Fetch the issue
3. Check for existing PR (re-invocation path)
4. Generate branch name and create branch
5. Assign the issue
6. Gather context
7. Enter plan mode
8. Execute after approval
9. Prompt the user to move onto the test or commit step

### 1. Resolve target repository

```bash
gh repo view --json isFork,parent
```

If `isFork` is `true`, extract `parent.owner.login` and `parent.name` to form the upstream repo identifier (`<owner>/<name>`). This upstream identifier becomes the **target repo** for all subsequent `gh` commands that reference issues or pull requests. If the repo is not a fork, the target repo is the current repo and no `--repo` flag is needed.

**User override:** If the user explicitly asks to target the fork — by saying "fork", "on the fork", "fork #N", or similar — the target repo MUST be set to the current (fork) repo instead of upstream. The user's explicit intent always takes precedence.

All `gh` commands in subsequent steps that reference issues or PRs MUST include `--repo <target>` when the target repo differs from the current repo.

### 2. Fetch the issue

```bash
gh issue view <number> --repo <target>
```

Read the issue title, body, and labels. If the issue does not exist or is closed, inform the user and stop. The `--repo <target>` flag ensures the issue is fetched from the upstream repo when working from a fork (as resolved in step 1). If the target repo is the current repo, the flag MAY be omitted.

### 3. Check for existing PR (re-invocation path)

Query for a linked PR:

```bash
gh pr list --repo <target> --search "Closes #<number>" --json number,headRefName,url --jq '.[0]'
```

- **If a PR exists** — check out the branch (`git fetch origin <branch> && git checkout <branch>`). Then check for unresolved review comments:

  ```bash
  gh api graphql -f query='
    query($owner: String!, $repo: String!, $pr: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $pr) {
          reviewThreads(first: 100) {
            nodes { isResolved comments(first: 1) { nodes { body path line } } }
          }
        }
      }
    }
  ' -f owner='<owner>' -f repo='<repo>' -F pr=<number>
  ```

  **Definition of "unresolved review comment":** A review comment is unresolved when its thread is literally marked as unresolved in GitHub's review UI (i.e., the thread has not been clicked "Resolve conversation"). This is a binary GitHub state, not a judgment call. Use the `isResolved` field on review comment threads to determine this. An unresolved thread means the reviewer intentionally left it open — the skill MUST read each unresolved comment, understand what the reviewer is asking for, and plan changes to address it. Do not dismiss unresolved comments as already handled without verifying the reviewer's intent.

  - **If unresolved comments exist** — read each comment, understand the feedback, and proceed to step 6 (gather context) then step 7 (plan changes to address the feedback).
  - **If no unresolved comments** — verify that the issue is fully implemented: review the issue body against the current branch state and tie off any loose ends. If everything is complete, inform the user and stop. Otherwise, plan remaining work.

- **If no PR exists** — this is a fresh implementation. Continue to step 4.

### 4. Generate branch name and create branch

Derive a short, descriptive branch name from the issue number and title:

```
<number>-<kebab-case-summary>
```

Examples:
- `96-fix-worker-factory-credentials`
- `102-add-retry-logic-to-discovery`

The branch name MUST be under 50 characters. Filler words SHOULD be stripped.

```bash
git checkout -b <branch-name> main
```

If the branch already exists, the user MUST be asked whether to switch to it or recreate it.

### 5. Assign the issue

Assign the issue to the current user so that ownership is visible on the board:

```bash
gh issue edit <number> --repo <target> --add-assignee @me
```

The `--repo <target>` flag MUST be included when the target repo differs from the current repo.

### 6. Gather context

Before entering plan mode, read enough of the codebase to plan confidently:

- Read source files referenced in the issue's description.
- Read existing tests for the affected modules.
- Read the project test guide (`@llm/guides/testguide-python.md`) to internalize testing conventions.
- Read project-level instructions (`CLAUDE.md`) for build tooling, documentation style, and architecture context.

### 7. Enter plan mode

Call `EnterPlanMode` to begin the planning phase. The execution plan:

- MUST map the issue's requirements (or unresolved review comments, on re-invocation) to concrete code changes: exact files, functions, classes, and the nature of each modification.
- SHOULD prefer test-first ordering when applicable.
- MUST follow the testing conventions in the project test guide (`@llm/guides/testguide-python.md`). Test case IDs (e.g., WC-001, VP-001) MUST NOT be assigned — the docstring provides sufficient traceability without the maintenance burden of cross-PR ID schemes.
- MUST include a verification section with the exact command(s) to run the test suite (see the project test guide for the runner command).

### 8. Execute after approval

Once the user approves the plan, implement each step sequentially.

### 9. Prompt the user to move onto the test or commit step

The user MUST be prompted with the next pipeline step: "Ready to generate tests? Run `/test <number>` to analyze coverage and write tests. Or ready to commit? Run `/commit` to stage and commit the changes." DO NOT proceed on your own.

## Edge cases

- **Branch already exists:** Ask the user whether to switch to it or recreate it.
- **Issue is closed:** Inform the user and stop.
- **Merge conflicts:** Stop and explain the situation rather than trying to resolve automatically.
- **Re-invocation with PR but no unresolved comments:** Verify the issue is fully implemented. If complete, inform the user and stop.
