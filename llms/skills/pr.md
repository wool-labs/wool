---
name: pr
description: >
  Create a branch and draft PR from a GitHub issue. Use this skill whenever the
  user says "/pr <number>", "create a PR for issue #N", "start working on #N",
  or similar. Takes an issue number as argument, fetches the issue, creates a
  branch, and opens a draft PR with an implementation plan. Also use this skill
  to update an existing draft PR when the implementation plan changes or code
  is committed.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT, REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be interpreted as described in RFC 2119.

# PR Skill

Create a branch and draft pull request from an existing GitHub issue. The PR serves as the implementation plan — no code is written yet.

## Pipeline Context

This skill is part of the development workflow pipeline: `/issue` → `/pr` → `/implement` → `/commit` → `/pr` (update). This skill is the **second** stage, and is also invoked at the end to update the PR after implementation.

## Arguments

An issue number MUST be provided as the sole argument (e.g., `/pr 96`).

## Workflow

### TL;DR

1. Resolve target repository
2. Fetch the issue
3. Generate a branch name
4. Create and checkout the branch
5. Draft the PR description
6. Show draft for approval
7. Push and create the draft PR
8. Return the PR URL
9. Prompt the user to move onto the implementation step

### 1. Resolve target repository

```bash
gh repo view --json isFork,parent
```

If `isFork` is `true`, extract `parent.owner.login` and `parent.name` to form the upstream repo identifier (`<owner>/<name>`). This upstream identifier becomes the **target repo** for all subsequent `gh` commands that reference issues or pull requests. The default PR base becomes the upstream repo's default branch and the PR will be opened against the upstream repo. If the repo is not a fork, the target repo is the current repo and no `--repo` flag is needed.

**User override:** If the user explicitly asks to target the fork — by saying "fork", "on the fork", "fork #N", or similar — the target repo MUST be set to the current (fork) repo instead of upstream. The user's explicit intent always takes precedence.

All `gh` commands in subsequent steps that reference issues or PRs MUST include `--repo <target>` when the target repo differs from the current repo.

### 2. Fetch the issue

```bash
gh issue view <number> --repo <target>
```

Read the issue title, body, and labels. If the issue does not exist or is closed, inform the user and stop. The `--repo <target>` flag ensures the issue is fetched from the upstream repo when working from a fork (as resolved in step 1). If the target repo is the current repo, the flag MAY be omitted.

### 3. Generate a branch name

Derive a short, descriptive branch name from the issue number and title:

```
<number>-<kebab-case-summary>
```

Examples:
- `96-fix-worker-factory-credentials`
- `102-add-retry-logic-to-discovery`

The branch name MUST be under 50 characters. Filler words SHOULD be stripped.

### 4. Create and checkout the branch

```bash
git checkout -b <branch-name> main
```

If the branch already exists, the user MUST be asked whether to switch to it or recreate it.

### 5. Draft the PR

The PR title should match its associated issue exactly with `— Closes #<number>` appended to the end.

All PR description prose MUST be written in the imperative mood — "Add retry logic" not "Adds retry logic" or "Added retry logic". This applies to the title, summary, proposed changes, and implementation plan steps. The imperative mood MUST be maintained even when updating the description after work has been completed. Descriptions of the current state of the system are exempt and SHOULD use present tense — "The registry stores entries in memory" not "Store entries in memory".

Prose in PR descriptions MUST NOT be hard-wrapped at a fixed column width. Write each sentence or logical phrase as a single unwrapped line. Markdown renderers handle line wrapping automatically — manual line breaks inside paragraphs create unnecessary diffs and awkward rendering.

The PR description MUST contain exactly four sections:

**Summary** — A quick recap of the issue, the high-level approach, and any trade-offs worth noting. The summary MUST end with `Closes #<number>` to link the issue.

**Proposed changes** — Subsections for each logical change. Design rationale and before/after code snippets SHOULD be included where useful.

**Test cases** — The `/test` skill MUST be used to generate a Given-When-Then test case table for the affected modules. The table MUST be included in the PR description so reviewers can see expected behavior at a glance. The table format:

| Test Suite | Test ID | Given | When | Then | Coverage Target |
|------------|---------|-------|------|------|-----------------|
| `TestParser` | PA-001 | A parser with default config | Empty input is processed | Returns an empty result | Default behavior |

The first word of every plain-language table entry MUST be capitalized. Table entries MUST NOT end with punctuation (no trailing periods, commas, etc.). Code spans in entries (e.g., `` `foo.bar()` is called ``) are exempt from the capitalization rule.

**Implementation plan** — Sequenced steps for the implementation, formatted as an ordered checkbox list. Test-first ordering SHOULD be preferred when applicable. Each step MUST describe a concrete output (writing code, tests, documentation, schema changes, etc.). Steps MUST NOT include running tests, linting, or other verification tasks — these are handled automatically by CI/CD. Example format:

```markdown
1. - [ ] Add `version` field to `Ack` in `worker.proto`; regenerate bindings
2. - [ ] Write discovery-time version filter tests (VP-001 through VP-004)
3. - [ ] Implement major-version filter in `proxy.py`
```

When code is committed and the PR description is updated to reflect the implemented state, completed steps MUST be checked off:

```markdown
1. - [x] Add `version` field to `Ack` in `worker.proto`; regenerate bindings
2. - [x] Write discovery-time version filter tests (VP-001 through VP-004)
3. - [ ] Implement major-version filter in `proxy.py`
```

### 6. Show draft for approval

The full PR (title, body, branch name) MUST be presented to the user. The PR MUST NOT be created until the user explicitly approves.

### 7. Push and create the draft PR

GitHub requires at least one commit of difference between the base and head branches to create a PR. Since the branch has no code yet, an empty commit MUST be created as a placeholder (it can be rebased away when real work starts):

```bash
git commit --allow-empty -m "chore: Open draft PR for #<number>"
git push -u origin <branch-name>
gh pr create --draft --title "<title>" --body "$(cat <<'EOF'
<body>
EOF
)"
```

When the repo is a fork (detected in step 2), add `--repo <upstream-owner>/<upstream-name>` so the PR is opened against the upstream repo:

```bash
gh pr create --draft --repo <upstream-owner>/<upstream-name> --title "<title>" --body "$(cat <<'EOF'
<body>
EOF
)"
```

The PR MUST be created as a **draft** since no code has been written yet.

After the PR is created, explicitly link the issue to the PR so the Development sidebar is populated immediately. The `Closes #<number>` keyword in the body is only processed on merge and does not always create the sidebar link:

```bash
gh issue develop <issue-number> --repo <target> --branch <branch-name>
```

If the command is not available in the installed `gh` version, the `Closes #<number>` reference in the PR body serves as the fallback.

### 8. Return the PR URL

The PR URL returned by `gh pr create` MUST be printed so the user can access it directly.

The user SHOULD be prompted with the next pipeline step: "Ready to implement? Run `/implement <number>` to start coding against this plan."

## Keeping the PR consistent

The PR description is a living document. It MUST be re-evaluated and updated when:

- **The user requests changes to the proposed implementation plan.** The test cases table MUST be regenerated to reflect the revised plan before showing the updated draft for approval.
- **Code is committed to the branch.** This skill MUST be re-run against the actual code being pushed: update the summary, proposed changes, and test cases to match what was implemented rather than what was planned. Use `gh pr edit` to update the existing PR body:

  ```bash
  gh pr edit <number> --repo <target> --body "$(cat <<'EOF'
  <updated body>
  EOF
  )"
  ```

  The `--repo <target>` flag MUST be included when the target repo differs from the current repo (as resolved in step 1).

The PR description MUST always accurately reflect the current state — planned or implemented — and MUST NOT drift from reality.

### 9. Prompt the user to move onto the implementation step

The user MUST be prompted with the next pipeline step: "Ready to implement? Run `/implement <number>` to start coding against this plan." DO NOT proceed on your own.
