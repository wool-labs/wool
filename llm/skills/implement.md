---
name: implement
description: >
  Implement a planned PR or issue. Use this skill whenever the user says
  "/implement <number>", "implement #N", "start implementing #N", or similar.
  Accepts a PR or issue number, resolves it to a draft PR (creating one via
  the /pr skill if needed), checks out the branch, and enters plan mode to
  design a concrete execution plan before writing code.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT,
REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be
interpreted as described in RFC 2119.

# Implement Skill

Resolve a PR or issue number, check out the branch, read the PR description,
and enter plan mode to design a concrete execution plan before writing any
code.

## Pipeline Context

This skill is part of the development workflow pipeline:
`/issue` → `/pr` → `/implement` → `/commit` → `/pr` (update).
This skill is the **third** stage.

## Arguments

A PR or issue number MUST be provided as the sole argument (e.g.,
`/implement 103`).

## Workflow

### 1. Resolve the input to a PR

Run `gh pr view <number>` to check if the number is a PR.

- **If it is a PR** — use it directly.
- **If it is not a PR** (the command fails) — run `gh issue view <number>`
  to confirm it is an issue.
  - **If it is an issue** — query for linked PRs via the Development
    sidebar field:
    ```bash
    gh issue view <number> --json closedByPullRequestsReferences \
      --jq '.closedByPullRequestsReferences[].number'
    ```
    - If a linked PR exists, use it.
    - If no linked PR exists, invoke the `/pr` skill to create one. Stop
      after the PR is created — the user MUST re-run `/implement` once the
      PR draft is approved.
- **If the number is neither a PR nor an issue** — inform the user and
  stop.

### 2. Check out the PR branch

Extract the branch name from the PR:

```bash
gh pr view <number> --json headRefName --jq .headRefName
```

Fetch and check out the branch:

```bash
git fetch origin <branch>
git checkout <branch>
```

If the local branch is behind the remote, pull to bring it up to date.

### 3. Read the PR description

Fetch the full PR body:

```bash
gh pr view <number> --json body,title
```

Parse the four sections: **Summary**, **Proposed changes**, **Test cases**,
and **Implementation plan**.

The implementation plan's checkbox list is the primary input for planning.
If all items are already checked off, inform the user that the
implementation plan appears complete and stop. If the plan is partially
checked off, only plan the remaining unchecked items.

### 4. Gather context

Before entering plan mode, read enough of the codebase to plan
confidently:

- Read every source file referenced in the PR's "Proposed changes"
  section.
- Read existing tests for the affected modules.
- Read `TESTGUIDE.md` (co-located in this skill directory) to internalize
  the project's testing conventions.
- Note wool-specific conventions: `uv` for builds, `pytest` +
  `pytest-asyncio` for tests, reST docstrings, the
  worker/proxy/discovery architecture.

### 5. Enter plan mode

Call `EnterPlanMode` to begin the planning phase. The execution plan:

- MUST map each unchecked item from the PR's implementation plan to
  concrete code changes: exact files, functions, classes, and the nature
  of each modification.
- SHOULD prefer test-first ordering when the PR's plan supports it.
- MUST follow the testing conventions in `TESTGUIDE.md` (AAA pattern,
  Given-When-Then docstrings, behavioral focus, property-based testing
  where appropriate). Test case IDs (e.g., WC-001, VP-001) MUST NOT be
  assigned — the Given-When-Then docstring provides sufficient
  traceability without the maintenance burden of cross-PR ID schemes.
- MUST include a verification section with the exact `uv run pytest`
  command(s) to validate the implementation.

### 6. Execute after approval

Once the user approves the plan, implement each step sequentially.

After all steps are complete:

1. Invoke the `/commit` skill to stage and commit the changes.
2. Invoke the `/pr` skill to update the PR description: check off
   completed implementation steps, refresh the test cases table, and
   update the summary to reflect what was actually implemented.
3. Inform the user that the branch is ready for review and the PR can
   be marked ready via `gh pr ready <number>`.

## Edge cases

- **All items already checked off:** Inform the user that the
  implementation plan appears complete and stop.
- **Partially checked off:** Only plan the remaining unchecked items.
- **PR is not a draft:** Proceed normally — the PR may have been marked
  ready for review before implementation was complete.
- **Merge conflicts:** Stop and explain the situation rather than trying
  to resolve automatically.
