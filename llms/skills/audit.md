---
name: audit
description: >
  Post-skill compliance checker. Use this skill whenever the user says
  "/audit <skill-name>", "audit the last skill run", "check compliance",
  or similar. Spawns a fresh subagent with no shared context to evaluate
  whether a skill's MUST/SHALL requirements were actually met, using binary
  checklist decomposition for unbiased assessment.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT, REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be interpreted as described in RFC 2119.

# Audit Skill

Spawn a fresh subagent to evaluate whether a skill's requirements were met during its most recent invocation. The subagent receives the skill definition and a structured action summary, extracts binary checklist items from every normative requirement, and reports a pass/fail verdict per item.

## Pipeline Context

This skill is a **cross-cutting quality gate** invoked at the end of every SDLC pipeline stage: `/issue` → audit → `/implement` → audit → `/test` → audit → `/commit` → audit → `/pr` → audit → `/review` → audit.

## Arguments

```
/audit <skill-name> [summary]
```

- `<skill-name>` — REQUIRED. The name of the skill to audit (e.g., `issue`, `pr`, `implement`, `commit`, `test`).
- `[summary]` — OPTIONAL. An inline action summary. If omitted, the primary agent MUST compile one from the current conversation context before spawning the subagent.

## Workflow

### TL;DR

1. Resolve the skill definition
2. Compile the action summary
3. Spawn the verification subagent
4. Present the report

### 1. Resolve the skill definition

Map `<skill-name>` to `llm/skills/<skill-name>.md`. If the file does not exist, inform the user that no skill definition was found for `<skill-name>` and stop.

### 2. Compile the action summary

If an inline summary was provided, use it. Otherwise, compile a structured action summary from the current conversation. The summary MUST include:

- **Steps taken** — ordered list of actions performed during the skill invocation.
- **Commands run** — shell commands executed, with their outcomes.
- **Artifacts produced** — files created or modified, PRs opened or updated, issues created, commits made, etc.
- **Key decisions** — any choices made during execution, especially where multiple valid paths existed.

### 3. Spawn the verification subagent

Use the `Agent` tool to launch a general-purpose subagent. The prompt MUST include:

1. The full text of the skill definition (read from the resolved path).
2. The compiled action summary.
3. The evaluation procedure described below.

The subagent prompt MUST be structured as follows:

````
You are a compliance auditor. You have been given a skill definition
and a summary of actions taken during that skill's invocation. Your
job is to determine whether the skill's requirements were met.

## Skill Definition

<full text of llm/skills/<skill-name>.md>

## Action Summary

<compiled action summary>

## Evaluation Procedure

1. Extract every normative requirement from the skill definition.
   A normative requirement is any statement using MUST, MUST NOT,
   SHALL, SHALL NOT, REQUIRED, or RECOMMENDED (as defined by
   RFC 2119). Each requirement becomes one checklist item.

2. For each checklist item, determine whether it was satisfied based
   on the action summary. Assign one of:
   - **PASS** — the requirement was clearly met, with cited evidence.
   - **FAIL** — the requirement was clearly not met, with explanation.
   - **SKIP** — the requirement was not applicable in this invocation
     (e.g., an edge case that did not arise) or the user explicitly requested to ignore the requirement.

3. Produce a summary report in the following format:

### Verification Report: <skill-name>

| # | Requirement | Verdict | Evidence |
|---|-------------|---------|----------|
| 1 | <requirement text> | PASS/FAIL/SKIP | <evidence or explanation> |

### Summary

- **Total**: N requirements
- **Passed**: N
- **Failed**: N
- **Skipped**: N
- **Verdict**: COMPLIANT / NON-COMPLIANT

If any requirement has a FAIL verdict, the overall verdict MUST be
NON-COMPLIANT. Otherwise it is COMPLIANT.

For each FAIL, include an actionable description of what was missed
and what would need to happen to remediate.
````

### 4. Present the report

Display the subagent's verification report to the user. If the verdict is NON-COMPLIANT:

- List the failed requirements with their remediation descriptions.
- Ask the user whether to remediate the failures or proceed as-is.

If the verdict is COMPLIANT, confirm that all requirements were met and continue.
