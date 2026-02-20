---
name: commit
description: >
  Stages and commits uncommitted changes in a git repository using a disciplined,
  atomic commit workflow. Use this skill whenever the user asks to "commit my changes",
  "clean up my git state", "stage and commit", "make commits from my changes", or
  anything similar. Also trigger when the user has described a body of work and says
  something like "can you commit this properly" or "let's checkpoint this". The skill
  creates a staging branch, analyzes the full diff, groups changes by logical kind,
  and commits them sequentially with well-formed messages — without ever mixing
  unrelated changes into a single commit.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT,
REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be
interpreted as described in RFC 2119.

# Commit Skill

This skill transforms a messy working tree into a clean sequence of atomic,
well-described commits on a staging branch. It MUST NOT touch the original
branch until the user is satisfied.

## Pipeline Context

This skill is part of the development workflow pipeline:
`/issue` → `/pr` → `/implement` → `/commit` → `/pr` (update).
This skill is the **fourth** stage.

## Workflow

### 1. Verify git state

```bash
git status
git diff HEAD
```

If the working tree is clean (nothing to commit), tell the user and stop.

Untracked files SHOULD be checked for new source files, config files, etc.
The user MUST be asked before including untracked files unless their
inclusion is obvious. Build artifacts, cache directories, and anything in
.gitignore MUST be ignored.

### 2. Create a staging branch

Get the current branch name:

```bash
git branch --show-current
```

Create and switch to a staging branch:

```bash
git checkout -b {current-branch-name}-staging
```

If a staging branch already exists from a previous run, the user MUST be
asked whether to delete and recreate it or continue from where it left off.

### 3. Analyze the full diff

```bash
git diff HEAD
git diff HEAD --name-only
```

The diff MUST be read carefully. For each changed file, note:
- What kind of change is it (new functionality, bug fix, docs, style/formatting,
  refactoring, performance, tests, build/tooling, CI, reversion)?
- Which other changed files is it logically related to?
- Does it mix concerns that should be separated?

A commit plan MUST be built before touching anything. The goal is a sequence
of commits where each one represents exactly one logical change.

### 4. Plan the commits

Group files and hunks into commits. The categories to use as a guide:

- **New functionality** — new features or capabilities, standalone
- **Bug fixes** — correcting incorrect behavior, isolate so they're cherry-pickable
- **Documentation** — docs, docstrings, comments; no behavior change
- **Style and formatting** — whitespace, formatting; MUST NOT be mixed with logic changes
- **Refactoring** — restructuring without behavior change
- **Performance** — speed/resource improvements, no other behavioral change
- **Tests** — adding or correcting tests
- **Build and tooling** — pyproject.toml, Makefile, dependencies, packaging
- **CI** — workflow files, pipeline config
- **Reversions** — reverting prior work, with clear reference to what and why

Mixed changes in a single file are common and MUST be handled with partial
staging (see below). Splitting a file's changes across commits is exactly
the right thing to do when the changes are of different kinds.

Logical ordering MUST be considered: if commit B depends on commit A, A
comes first. Generally: refactoring before feature work, build changes
before code that uses them, tests after (or alongside) the code they test.

### 5. Stage and commit sequentially

For each planned commit, stage exactly the right changes and commit.

**Staging whole files:**
```bash
git add path/to/file.py
```

**Staging specific hunks from a file** (when a file mixes concerns):

Use `git add -p path/to/file.py` in interactive mode to select only the relevant
hunks. Since this skill runs non-interactively, use the `-e` (edit) hunk approach
or use patch files instead:

```bash
# Generate a patch for just the relevant hunks, then apply selectively
git diff HEAD -- path/to/file.py > /tmp/full.patch
# Edit /tmp/full.patch to keep only desired hunks, then:
git apply --cached /tmp/full.patch
```

Alternatively, for clean hunk boundaries, use:
```bash
git diff HEAD -U0 -- path/to/file.py
```
...to see exact line numbers, then stage by line range using a patch file.

After staging, the staged diff MUST be verified before committing:
```bash
git diff --cached
```

Then commit. The message MUST be written to a temp file to avoid shell
escaping issues:

```bash
cat > /tmp/commit_msg.txt << 'EOF'
Subject line here

Body here if needed.

Footer here if needed.
EOF
git commit -F /tmp/commit_msg.txt
```

Repeat for each planned commit.

### 6. Review and present

After all commits:

```bash
git log {original-branch}..HEAD --oneline
```

The resulting commit list MUST be shown to the user for approval. If they
want changes — different grouping, reworded messages, splitting or
squashing — use `git rebase -i HEAD~N` on the staging branch to make
adjustments.

When the user is satisfied, remind them they can merge back:

```bash
git checkout {original-branch}
git merge --ff-only {original-branch}-staging
git branch -d {original-branch}-staging
```

Or if they prefer to review as a PR first, they can push the staging branch
and open a pull request against the original branch.

If the current branch is associated with a PR, the user SHOULD be
prompted with the next pipeline step: "Ready to update the PR? Run
`/pr <number>` to sync the PR description with the committed changes."

---

## Commit Message Style Guide

Every commit message MUST follow these rules.

### Structure

```
<subject>

<body>

<footer>
```

The subject is REQUIRED. Body and footer are OPTIONAL but RECOMMENDED for
anything non-trivial.

### Subject line

- MUST be 72 characters maximum; 50 is RECOMMENDED
- MUST begin with a type prefix: `feat:`, `fix:`, `docs:`, `style:`, `refactor:`, `perf:`, `test:`, `build:`, `ci:`, or `revert:`
- `!` SHOULD be used after the type for breaking changes: `feat!: Remove legacy auth endpoint`
- MUST use imperative mood after the prefix: "feat: Add feature" not "feat: Added feature"
- The first word after the prefix MUST be capitalized
- MUST NOT have a trailing period
- MUST be specific: "fix: Handle null pointer when user has no profile photo" not "fix: Fix bug"
- MUST be plain text only — no backticks, asterisks, or markup of any kind
- Symbol names MUST be written bare: "fix: Handle FooABC raising ValueError on None input"

### Body

- MUST be separated from subject with a blank line
- SHOULD explain what and why, not how — the diff shows how
- MUST wrap at 72 characters
- MUST be plain text only

### Footer

- Issue references: Fixes #42, Closes #38, References #100
- Breaking changes: BREAKING CHANGE: description of the break
- Signed-off-by if the project uses DCO: Signed-off-by: Name <email>
- Co-author lines MUST NOT be added

### What to avoid

- Vague subjects: "fix", "WIP", "update", "misc", "oops", "changes"
- Describing what the diff already shows: "change foo to return early"
- Committing commented-out code
- Mixing whitespace changes with logic changes
- Repeating the subject verbatim in the body
- Any markup (backticks, asterisks, bold, etc.)

### Examples

Good:
```
fix: Handle session refresh crash when token has expired

The refresh worker was catching all exceptions as network errors and
retrying indefinitely. TokenExpiredError needs distinct handling since
retrying will never succeed — the user must re-authenticate instead.

Fixes #214
```

Good:
```
feat: Add support for Ed25519 signing keys
```

Bad:
```
fix stuff
```

Bad (mixes concerns, uses markup, past tense, no prefix, too long):
```
Updated `parse_config()` to handle missing keys and also reformatted
the whole file and added a test
```

---

## Edge Cases

**Untracked files:** The user MUST be asked before staging new files that
are not obviously part of the work. Files that MAY be local scratch work
MUST NOT be automatically added.

**Binary files:** Binary files MUST be noted explicitly and the user MUST be
asked whether to include them.

**Large diffs:** For very large changesets, the commit plan MUST be narrated
to the user before executing and confirmation MUST be obtained.

**Merge conflicts or unusual repo state:** Execution MUST stop and the
situation MUST be explained rather than worked around. A confused repo state
needs human judgment.

**Already-staged changes:** If the user has already staged some changes with
git add, those MUST be incorporated into the plan. Already-staged changes
MUST NOT be blindly unstaged. `git diff --cached` MUST be run first to see
what is already staged.
