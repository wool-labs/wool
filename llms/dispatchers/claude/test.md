---
name: test
description: >
  Analyze code changes for an issue, evaluate existing test coverage, plan
  and implement tests. Use this skill whenever the user says "/test <number>",
  "write tests for #N", "test #N", or similar. Accepts an issue number,
  resolves the branch, diffs against the base, evaluates existing tests, and
  writes new or updated tests to achieve comprehensive coverage.
---

# Subagent Dispatch

Execute the **test** skill in an isolated subagent to preserve parent context.

Spawn a general-purpose subagent with the following brief:

> You are executing the **test** skill from the SDLC pipeline (issue → implement → test → commit → pr → review).
>
> **Argument:** <the argument value from ARGUMENTS below>
>
> 1. Read the project instructions in `CLAUDE.md`.
> 2. Read and execute the complete workflow defined in `llms/skills/test.md`.
> 3. Follow every step faithfully. Pay special attention to the `## Invariants` section — these are non-negotiable safety constraints. When a step requires user approval (e.g., test plan approval), surface it to the user and wait for their response.
> 4. When done, return a structured summary: what was accomplished, key artifacts (test files created or modified, test results), and the next pipeline step prompt from the skill definition.

When the subagent returns, relay its summary to the user verbatim. Do not repeat work or add commentary.
