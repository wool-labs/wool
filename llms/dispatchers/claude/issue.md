---
name: issue
description: >
  Draft and push a GitHub issue. Use this skill whenever the user says "/issue",
  "file an issue", "create an issue", "open a bug report", or similar. If
  `.issue.md` exists in the repo root, it is used as the source. Otherwise the
  issue is drafted interactively from the conversation.
---

# Subagent Dispatch

Execute the **issue** skill in an isolated subagent to preserve parent context.

Spawn a general-purpose subagent with the following brief:

> You are executing the **issue** skill from the SDLC pipeline (issue → implement → test → commit → pr → review).
>
> **Argument:** <the argument value from ARGUMENTS below, if any>
>
> 1. Read the project instructions in `CLAUDE.md`.
> 2. Read and execute the complete workflow defined in `llms/skills/issue.md`.
> 3. Follow every step faithfully. When a step requires user approval (e.g., draft approval, label approval), surface it to the user and wait for their response.
> 4. When done, return a structured summary: what was accomplished, key artifacts (issue URL, issue number, labels applied), and the next pipeline step prompt from the skill definition.

When the subagent returns, relay its summary to the user verbatim. Do not repeat work or add commentary.
