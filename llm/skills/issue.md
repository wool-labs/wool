---
name: issue
description: >
  Draft and push a GitHub issue. Use this skill whenever the user says "/issue",
  "file an issue", "create an issue", "open a bug report", or similar. If
  `.issue.md` exists in the repo root, it is used as the source. Otherwise the
  issue is drafted interactively from the conversation.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT,
REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be
interpreted as described in RFC 2119.

# Issue Skill

Create a well-structured GitHub issue, either from a prepared `.issue.md`
file or interactively from conversation context.

## Pipeline Context

This skill is part of the development workflow pipeline:
`/issue` → `/pr` → `/implement` → `/commit` → `/pr` (update).
This skill is the **first** stage.

## Workflow

### 1. Determine the source

Check whether `.issue.md` exists in the repository root:

```bash
test -f .issue.md && echo "exists" || echo "missing"
```

- **If `.issue.md` exists** — read it. The first `# heading` MUST be parsed
  as the issue title and the remaining content as the body. Proceed to
  step 3.
- **If `.issue.md` does not exist** — draft the issue interactively
  (step 2).

### 2. Draft interactively

Based on conversation context, determine the issue type and draft using the
matching template. All templates share the same structure — only the field
names and auto-label differ.

Issue titles MUST be written in the imperative mood. Issue body prose
SHOULD use the imperative mood for proposed actions and present tense
for descriptions of current system state.

**Bug** (label: `bug`):

```markdown
# <title>

## Summary

<What is the bug? Steps to reproduce if applicable.>

## Root cause

<What is causing the bug? Include relevant code snippets.>

## Affected code

<Which files, modules, or components are affected?>
```

**Feature** (label: `feature`):

```markdown
# <title>

## Summary

<What is the feature? Describe the desired behavior.>

## Motivation

<Why is this feature needed? What problem does it solve?>

## Affected code

<Which files, modules, or components would be affected?>
```

**Refactor** (label: `refactor`):

```markdown
# <title>

## Summary

<What should be restructured and what does the end state look like?>

## Motivation

<Why is this restructuring needed?>

## Affected code

<Which files, modules, or components are affected?>
```

**Test** (label: `test`):

```markdown
# <title>

## Summary

<What needs to be tested or what test infrastructure is needed?>

## Motivation

<Why is this test work needed? What gap does it fill?>

## Affected code

<Which files, modules, or components are affected?>
```

**CI/CD** (label: `cicd`):

```markdown
# <title>

## Summary

<What CI/CD change is needed?>

## Motivation

<Why is this change needed? What does it improve?>

## Affected code

<Which workflows, pipelines, or config files are affected?>
```

**Build** (label: `build`):

```markdown
# <title>

## Summary

<What build system or dependency change is needed?>

## Motivation

<Why is this change needed?>

## Affected code

<Which build files, configs, or dependencies are affected?>
```

The **Affected code** section MAY be omitted if it is not relevant.

The draft MUST be shown to the user and iterated until they approve.

### 3. Suggest labels

The issue content MUST be analyzed and one or more labels suggested from
the repository's label set:

| Label | Use when |
|-------|----------|
| `bug` | Something isn't working |
| `feature` | New feature or capability |
| `refactor` | Code restructuring without behavior change |
| `test` | The core focus is test coverage or test infrastructure |
| `cicd` | CI/CD pipeline changes |
| `build` | Build system or dependency changes |

Each label SHOULD only be applied when the issue's **primary focus** matches
that category. For example, a bug fix will naturally include tests as part
of its implementation — that MUST NOT warrant the `test` label. The `test`
label is reserved for issues whose core purpose is adding, fixing, or
improving tests or test infrastructure.

The suggested labels MUST be shown alongside the draft for user approval.

### 4. Show draft for approval

The full issue (title, body, labels) MUST be presented to the user. The
issue MUST NOT be pushed until the user explicitly approves.

### 5. Push the issue

A heredoc MUST be used for the body to avoid shell escaping issues:

```bash
gh issue create --title "<title>" --label "<label1>,<label2>" --body "$(cat <<'EOF'
<body>
EOF
)"
```

The `--assignee` flag MUST NOT be used — issues are not auto-assigned.

### 6. Clean up `.issue.md`

If the issue was created from `.issue.md`, the user MUST be asked whether to
delete the file now that the issue has been pushed. If they agree:

```bash
rm .issue.md
```

### 7. Return the issue URL

The issue URL returned by `gh issue create` MUST be printed so the user can
access it directly.

The user SHOULD be prompted with the next pipeline step: "Ready to plan
this? Run `/pr <number>` to create a branch and draft PR."
