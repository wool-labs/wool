---
name: audit
description: >
  Post-skill compliance checker. Use this skill whenever the user says
  "/audit <skill-name>", "audit the last skill run", "check compliance",
  or similar. Spawns a fresh subagent with no shared context to evaluate
  whether a skill's MUST/SHALL requirements were actually met, using binary
  checklist decomposition for unbiased assessment.
---

# Subagent Dispatch

Execute the **audit** skill in an isolated subagent to preserve parent context.

Spawn a general-purpose subagent with the following brief:

> You are executing the **audit** skill from the SDLC pipeline. This is a cross-cutting quality gate invoked after any pipeline stage.
>
> **Argument:** <the argument value from ARGUMENTS below>
>
> 1. Read the project instructions in `CLAUDE.md`.
> 2. Read and execute the complete workflow defined in `llms/skills/audit.md`.
> 3. Follow every step faithfully. Pay special attention to the `## Invariants` section — these are non-negotiable safety constraints. When a step requires user approval (e.g., remediation decision), surface it to the user and wait for their response.
> 4. The checklist MUST be exhaustive — extract every MUST, MUST NOT, SHALL, SHALL NOT, REQUIRED, and RECOMMENDED statement from the skill definition, including those in the `## Invariants` section. A partial checklist that omits normative requirements is itself a compliance failure.
> 5. When done, return the full verification report including the pass/fail verdicts and overall compliance status.

When the subagent returns, relay its summary to the user verbatim. Do not repeat work or add commentary.
