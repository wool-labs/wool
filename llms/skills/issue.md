---
name: issue
description: >
  Draft and push a GitHub issue. Use this skill whenever the user says "/issue",
  "file an issue", "create an issue", "open a bug report", or similar. If
  `.issue.md` exists in the repo root, it is used as the source. Otherwise the
  issue is drafted interactively from the conversation.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT, REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be interpreted as described in RFC 2119.

# Issue Skill

Create a well-structured GitHub issue, either from a prepared `.issue.md` file or interactively from conversation context.

## Pipeline Context

This skill is part of the development workflow pipeline: `/issue` → `/implement` → `/test` → `/commit` → `/pr`. This skill is the **first** stage. The implement, test, and commit steps are iterative — they can be invoked multiple times for a given issue.

## Invariants

- MUST NOT push the issue to GitHub until the user explicitly approves the draft.
- MUST use a heredoc for the `gh issue create` body to avoid shell escaping issues.
- MUST NOT add the `--assignee` flag -- issues are not auto-assigned at creation.
- MUST use the knowledge graph (`llms/skills/understand-chat.md`) for context gathering when `.understand-anything/knowledge-graph.json` exists.

## Workflow

### Checklist

1. Determine the source
2. Resolve target repository
3. Gather knowledge graph context
4. Draft interactively
5. Suggest labels
6. Show draft for approval
7. Push the issue
8. Clean up `.issue.md` (if applicable)
9. Return the issue URL
10. Prompt the user to move onto the implement step

### 1. Determine the source

Check whether `.issue.md` exists in the repository root:

```bash
test -f .issue.md && echo "exists" || echo "missing"
```

- **If `.issue.md` exists** — read it. The first `# heading` MUST be parsed as the issue title and the remaining content as the body. Proceed to step 3.
- **If `.issue.md` does not exist** — draft the issue interactively (step 2).

### 2. Resolve target repository

```bash
gh repo view --json isFork,parent
```

If `isFork` is `true`, extract `parent.owner.login` and `parent.name` to form the upstream repo identifier (`<owner>/<name>`). This upstream identifier becomes the **target repo** for all subsequent `gh` commands that reference issues or pull requests. If the repo is not a fork, the target repo is the current repo and no `--repo` flag is needed.

**User override:** If the user explicitly asks to target the fork — by saying "fork", "on the fork", "fork #N", or similar — the target repo MUST be set to the current (fork) repo instead of upstream. The user's explicit intent always takes precedence.

All `gh` commands in subsequent steps that reference issues or PRs MUST include `--repo <target>` when the target repo differs from the current repo.

### 3. Gather knowledge graph context

Check whether a knowledge graph exists:

```bash
test -f .understand-anything/knowledge-graph.json && echo "exists" || echo "missing"
```

If the graph exists, read and follow `llms/skills/understand-chat.md` with a query synthesized from the conversation context (the user's description of the problem or feature) to gather architectural context — component summaries, relationships, and layer assignments — that helps scope the issue by revealing which components and layers are relevant. If the graph does not exist, skip this step and continue.

### 4. Draft interactively

Based on conversation context, determine the issue type and draft using the matching template. All templates share the same structure — only the field names and auto-label differ.

Issue titles MUST be written in the imperative mood. Issue body prose SHOULD use the imperative mood for proposed actions and present tense for descriptions of current system state.

Prose in issue bodies MUST NOT be hard-wrapped at a fixed column width. Write each sentence or logical phrase as a single unwrapped line. Markdown renderers handle line wrapping automatically — manual line breaks inside paragraphs create unnecessary diffs and awkward rendering.

**Bug** (label: `bug`):

```markdown
# <title>

## Description

<What is the bug? Steps to reproduce if applicable.>

## Expected behavior

<What did you expect to happen?>

## Root cause

<What is causing the bug? Include relevant code snippets.>
```

**Feature** (label: `feature`):

```markdown
# <title>

## Description

<What is the feature? Describe the desired behavior.>

## Motivation

<Why is this feature needed? What problem does it solve?>

## Expected outcome

<What should be true when this is done?>
```

**Refactor** (label: `refactor`):

```markdown
# <title>

## Description

<What should be restructured and what does the end state look like?>

## Motivation

<Why is this restructuring needed?>

## Expected outcome

<What should be true when this is done?>
```

**Test** (label: `test`):

```markdown
# <title>

## Description

<What needs to be tested or what test infrastructure is needed?>

## Motivation

<Why is this test work needed? What gap does it fill?>

## Expected outcome

<What should be true when this is done?>
```

**CI/CD** (label: `cicd`):

```markdown
# <title>

## Description

<What CI/CD change is needed?>

## Motivation

<Why is this change needed? What does it improve?>

## Expected outcome

<What should be true when this is done?>
```

**Build** (label: `build`):

```markdown
# <title>

## Description

<What build system or dependency change is needed?>

## Motivation

<Why is this change needed?>

## Expected outcome

<What should be true when this is done?>
```

For issue types backed by a GitHub form template (Bug, Feature), the **Expected outcome** / **Expected behavior** section is required by the form and MUST be included. For fallback-only templates (Refactor, Test, CI/CD, Build), the section MAY be omitted if it is not relevant.

When the project defines a GitHub issue form template in `.github/ISSUE_TEMPLATE/` that matches the issue type (e.g., `bug.yaml` for bugs, `feature.yaml` for features), the skill MUST mirror the form template's section structure instead of using the built-in Markdown template above. The built-in Markdown templates serve as a fallback for issue types that do not have a corresponding GitHub form template.

The draft MUST be shown to the user and iterated until they approve.

### 5. Suggest labels

The issue content MUST be analyzed and one or more labels suggested from the repository's label set:

| Label | Use when |
|-------|----------|
| `bug` | Something isn't working |
| `feature` | New feature or capability |
| `refactor` | Code restructuring without behavior change |
| `test` | The core focus is test coverage or test infrastructure |
| `cicd` | CI/CD pipeline changes |
| `build` | Build system or dependency changes |

Each label SHOULD only be applied when the issue's **primary focus** matches that category. For example, a bug fix will naturally include tests as part of its implementation — that MUST NOT warrant the `test` label. The `test` label is reserved for issues whose core purpose is adding, fixing, or improving tests or test infrastructure.

The suggested labels MUST be shown alongside the draft for user approval.

### 6. Show draft for approval

The full issue (title, body, labels) MUST be presented to the user. The issue MUST NOT be pushed until the user explicitly approves.

### 7. Push the issue

A heredoc MUST be used for the body to avoid shell escaping issues:

```bash
gh issue create --repo <target> --title "<title>" --label "<label1>,<label2>" --body "$(cat <<'EOF'
<body>
EOF
)"
```

The `--repo <target>` flag MUST be included when the target repo differs from the current repo (as resolved in step 2). The `--assignee` flag MUST NOT be used — issues are not auto-assigned.

### 8. Clean up `.issue.md`

If the issue was created from `.issue.md`, the user MUST be asked whether to delete the file now that the issue has been pushed. If they agree:

```bash
rm .issue.md
```

### 9. Return the issue URL

The issue URL returned by `gh issue create` MUST be printed so the user can access it directly.

The user SHOULD be prompted with the next pipeline step: "Ready to implement? Run `/implement <number>` to create a branch and start planning."

### 10. Prompt the user to move onto the implement step

The user MUST be prompted with the next pipeline step: "Ready to implement? Run `/implement <number>` to create a branch and start planning." When working from a fork, note that `<number>` refers to the issue on the upstream repo. DO NOT proceed on your own.
