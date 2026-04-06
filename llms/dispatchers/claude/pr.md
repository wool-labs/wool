---
name: pr
description: >
  Review committed changes on a branch and create a draft pull request. Use
  this skill whenever the user says "/pr <number>", "create a PR for #N",
  "open a PR", or similar. Accepts an issue number, reviews the branch diff,
  and drafts a PR description based on what was actually implemented.
---

# Subagent Dispatch

Execute the **pr** skill in an isolated subagent to preserve parent context.

Spawn a general-purpose subagent with the following brief:

> You are executing the **pr** skill from the SDLC pipeline (issue → implement → test → commit → pr → review).
>
> **Argument:** <the argument value from ARGUMENTS below>
>
> 1. Read the project instructions in `CLAUDE.md`.
> 2. Read and execute the complete workflow defined in `llms/skills/pr.md`.
> 3. Follow every step faithfully. When a step requires user approval (e.g., PR description approval), surface it to the user and wait for their response.
> 4. When done, return a structured summary: what was accomplished, key artifacts (PR URL, PR number, branch name), and the next pipeline step prompt from the skill definition.

When the subagent returns, relay its summary to the user verbatim. Do not repeat work or add commentary.
