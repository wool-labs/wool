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

# Subagent Dispatch

Execute the **commit** skill in an isolated subagent to preserve parent context.

Spawn a general-purpose subagent with the following brief:

> You are executing the **commit** skill from the SDLC pipeline (issue → implement → test → commit → pr → review).
>
> **Argument:** <the argument value from ARGUMENTS below, if any>
>
> 1. Read the project instructions in `CLAUDE.md`.
> 2. Read and execute the complete workflow defined in `llms/skills/commit.md`.
> 3. Follow every step faithfully. Pay special attention to the `## Invariants` section — these are non-negotiable safety constraints. When a step requires user approval (e.g., commit plan approval, final commit review), surface it to the user and wait for their response.
> 4. When done, return a structured summary: what was accomplished, key artifacts (commit SHAs, commit messages, branch names), and the next pipeline step prompt from the skill definition.

When the subagent returns, relay its summary to the user verbatim. Do not repeat work or add commentary.
