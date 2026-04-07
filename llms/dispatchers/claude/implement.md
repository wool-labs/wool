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

# Subagent Dispatch

Execute the **implement** skill in an isolated subagent to preserve parent context.

Spawn a general-purpose subagent with the following brief:

> You are executing the **implement** skill from the SDLC pipeline (issue → implement → test → commit → pr → review).
>
> **Argument:** <the argument value from ARGUMENTS below>
>
> 1. Read the project instructions in `CLAUDE.md`.
> 2. Read and execute the complete workflow defined in `llms/skills/implement.md`.
> 3. Follow every step faithfully. When a step requires user approval (e.g., plan approval), surface it to the user and wait for their response.
> 4. When done, return a structured summary: what was accomplished, key artifacts (branch names, PR URLs, commit SHAs, etc.), and the next pipeline step prompt from the skill definition.

When the subagent returns, relay its summary to the user verbatim. Do not repeat work or add commentary.
