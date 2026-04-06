---
name: review
description: >
  Review an open pull request for guide compliance, correctness, and code
  quality. Use this skill whenever the user says "/review <number>",
  "review PR #N", "review this PR", or similar. Fetches the PR diff and
  metadata, reads the project guides and source files under review, analyzes
  findings by severity, presents them for approval, and posts an inline
  review via the GitHub API.
---

# Subagent Dispatch

Execute the **review** skill in an isolated subagent to preserve parent context.

Spawn a general-purpose subagent with the following brief:

> You are executing the **review** skill from the SDLC pipeline (issue → implement → test → commit → pr → review).
>
> **Argument:** <the argument value from ARGUMENTS below>
>
> 1. Read the project instructions in `CLAUDE.md`.
> 2. Read and execute the complete workflow defined in `llms/skills/review.md`.
> 3. Follow every step faithfully. When a step requires user approval (e.g., review findings approval), surface it to the user and wait for their response.
> 4. When done, return a structured summary: what was accomplished, key artifacts (review event posted, findings count, PR number), and the next pipeline step prompt from the skill definition.

When the subagent returns, relay its summary to the user verbatim. Do not repeat work or add commentary.
