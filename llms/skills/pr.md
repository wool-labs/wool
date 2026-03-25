---
name: pr
description: >
  Review committed changes on a branch and create a draft pull request. Use
  this skill whenever the user says "/pr <number>", "create a PR for #N",
  "open a PR", or similar. Accepts an issue number, reviews the branch diff,
  and drafts a PR description based on what was actually implemented.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT, REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be interpreted as described in RFC 2119.

# PR Skill

Review committed source and test changes on a branch and create a draft pull request with a description based on what was actually implemented.

## Pipeline Context

This skill is part of the development workflow pipeline: `/issue` → `/implement` → `/test` → `/commit` → `/pr`. This skill is the **fifth** (final) stage. After review feedback is received, the user re-enters the loop at the implement skill to address comments, then optionally re-runs the test and commit skills before re-running the PR skill to update the description.

## Arguments

An issue number MUST be provided as the sole argument (e.g., `/pr 103`). The issue number is used to identify the branch (`<number>-<kebab-case-summary>`) and to link the PR via `Closes #<number>`.

## Workflow

### TL;DR

1. Resolve target repository
2. Identify the issue and branch
3. Review the branch diff
4. Draft the PR description
5. Show draft for approval
6. Push and create the draft PR
7. Return the PR URL

### 1. Resolve target repository

```bash
gh repo view --json isFork,parent
```

If `isFork` is `true`, extract `parent.owner.login` and `parent.name` to form the upstream repo identifier (`<owner>/<name>`). This upstream identifier becomes the **target repo** for all subsequent `gh` commands that reference issues or pull requests. The default PR base becomes the upstream repo's default branch and the PR will be opened against the upstream repo. If the repo is not a fork, the target repo is the current repo and no `--repo` flag is needed.

**User override:** If the user explicitly asks to target the fork — by saying "fork", "on the fork", "fork #N", or similar — the target repo MUST be set to the current (fork) repo instead of upstream. The user's explicit intent always takes precedence.

All `gh` commands in subsequent steps that reference issues or PRs MUST include `--repo <target>` when the target repo differs from the current repo.

### 2. Identify the issue and branch

Fetch the issue:

```bash
gh issue view <number> --repo <target>
```

Read the issue title, body, and labels. If the issue does not exist or is closed, inform the user and stop.

Check out the branch. The branch name follows the convention `<number>-<kebab-case-summary>`:

```bash
git fetch origin
git checkout <branch>
```

If the branch does not exist, inform the user that the implement skill must be run first and stop.

Check for an existing PR:

```bash
gh pr list --repo <target> --search "Closes #<number>" --json number --jq '.[0].number'
```

If a PR already exists, this is an update — the description will be edited rather than a new PR created.

### 3. Review the branch diff

Determine the base and diff against it:

```bash
git merge-base HEAD $(git rev-parse --abbrev-ref @{upstream} 2>/dev/null || echo main)
git log <merge-base>..HEAD --oneline
git diff <merge-base>..HEAD
git diff <merge-base>..HEAD --stat
```

Analyze all committed source and test changes. Understand what was implemented, what was refactored, what tests were added or modified.

### 4. Draft the PR description

#### 4a. Detect a PR template

Before drafting, check whether the repository provides a PR template:

```bash
cat "$(git rev-parse --show-toplevel)/.github/pull_request_template.md" 2>/dev/null
```

- **If the file exists** — read its section headings (lines starting with `##`) and use them as the scaffold for the PR body. Populate each section contextually based on the diff analysis from step 3. Any guidance inside HTML comments (`<!-- ... -->`) informs what content belongs in that section but MUST NOT appear in the final PR body.
- **If the file does not exist** — fall back to the built-in three-section format defined below.

The remaining rules in this step apply regardless of whether a template is used.

#### 4b. Title

The PR title MUST match the associated issue title exactly with ` — Closes #<number>` appended to the end.

#### 4c. Body prose rules

All PR description prose MUST be written in the imperative mood — "Add retry logic" not "Adds retry logic" or "Added retry logic". This applies to the title, summary, and proposed changes. Descriptions of the current state of the system are exempt and SHOULD use present tense — "The registry stores entries in memory" not "Store entries in memory".

Prose in PR descriptions MUST NOT be hard-wrapped at a fixed column width. Write each sentence or logical phrase as a single unwrapped line. Markdown renderers handle line wrapping automatically — manual line breaks inside paragraphs create unnecessary diffs and awkward rendering.

#### 4d. Built-in section format (fallback)

When no PR template file is detected, the PR description MUST contain a **Summary** and **Proposed changes** section. A **Test cases** section MUST be included when the PR contains code changes; it MAY be omitted for PRs that only modify documentation, configuration, or other non-code files.

**Summary** — A recap of what was implemented, the high-level approach, and any trade-offs worth noting. The summary is based on the actual diff, not the issue body. The summary MUST end with `Closes #<number>` to link the issue.

**Proposed changes** — Subsections for each logical change, derived from the committed diff. Design rationale SHOULD be included where useful.

**Test cases** (code changes only) — An enumerated Given-When-Then table summarizing the test coverage from committed test files. Read the test files and generate a table in this format:

| # | Test Suite | Given | When | Then | Coverage Target |
|---|------------|-------|------|------|-----------------|
| 1 | `TestParser` | A parser with default config | Empty input is processed | Returns an empty result | Default behavior |

The first word of every plain-language table entry MUST be capitalized. Table entries MUST NOT end with punctuation (no trailing periods, commas, etc.). Code spans in entries (e.g., `` `foo.bar()` is called ``) are exempt from the capitalization rule.

### 5. Show draft for approval

The full PR (title, body, branch name) MUST be presented to the user. The PR MUST NOT be created until the user explicitly approves.

### 6. Push and create the draft PR

Push the branch if not already pushed:

```bash
git push -u origin <branch-name>
```

**Creating a new PR:**

A heredoc MUST be used for the body to avoid shell escaping issues:

```bash
gh pr create --draft --title "<title>" --body "$(cat <<'EOF'
<body>
EOF
)"
```

When the repo is a fork (detected in step 1), add `--repo <upstream-owner>/<upstream-name>` so the PR is opened against the upstream repo.

The PR MUST always be created as a **draft**. The user marks it ready for review when satisfied.

After the PR is created, link the issue to the PR in the Development sidebar:

```bash
gh issue develop <issue-number> --repo <target> --branch <branch-name>
```

If the command is not available in the installed `gh` version, the `Closes #<number>` reference in the PR body serves as the fallback.

**Updating an existing PR:**

If a PR already exists (detected in step 2), update the description instead of creating a new PR:

```bash
gh pr edit <pr-number> --repo <target> --body "$(cat <<'EOF'
<updated body>
EOF
)"
```

The `--repo <target>` flag MUST be included when the target repo differs from the current repo.

### 7. Return the PR URL

The PR URL MUST be printed so the user can access it directly.

The user MUST be informed that the draft PR is created and can be marked ready for review via `gh pr ready <number>`. Note that if review feedback is received, they can re-enter the implementation loop by running the implement skill again.
