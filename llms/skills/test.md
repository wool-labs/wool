---
name: test
description: >
  Analyze code changes for an issue, evaluate existing test coverage, plan
  and implement tests. Use this skill whenever the user says "/test <number>",
  "write tests for #N", "test #N", or similar. Accepts an issue number,
  resolves the branch, diffs against the base, evaluates existing tests, and
  writes new or updated tests to achieve comprehensive coverage.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT, REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be interpreted as described in RFC 2119.

# Test Skill

Analyze code changes associated with an issue, evaluate existing test coverage, and plan and implement tests to achieve comprehensive coverage of public APIs.

## Pipeline Context

This skill is part of the development workflow pipeline: `/issue` → `/implement` → `/test` → `/commit` → `/pr`. This skill is the **third** stage. The implement, test, and commit steps are iterative — they can be invoked multiple times for a given issue.

## Arguments

An issue number MUST be provided as the sole argument (e.g., `/test 103`).

> **Test guide:** Determine the language of the changed source files from their file extensions, then read the corresponding test guide before generating test specifications:
>
> | Language | Guide |
> |----------|-------|
> | Python (`.py`) | `@llm/guides/testguide-python.md` |
>
> If no guide exists for the detected language, inform the user and stop.

## Workflow

### TL;DR

1. Resolve issue, PR, and branch
2. Analyze code changes
3. Evaluate existing tests
4. Generate test plan
5. Enter plan mode
6. Implement tests after approval
7. Prompt the user to move onto the commit step

### 1. Resolve issue, PR, and branch

Perform the same resolution logic as the implement skill:

```bash
gh repo view --json isFork,parent
gh issue view <number> --repo <target>
```

Read the issue title, body, and labels. If the issue does not exist or is closed, inform the user and stop.

Check for an existing PR:

```bash
gh pr list --repo <target> --search "Closes #<number>" --json number,headRefName --jq '.[0]'
```

If a PR exists, note its number for context. Check out the branch:

```bash
git fetch origin <branch>
git checkout <branch>
```

If the branch does not exist, inform the user that the implement skill must be run first and stop. The test skill MUST NOT create branches.

### 2. Analyze code changes

Determine the base branch and diff against it:

```bash
git merge-base HEAD $(git rev-parse --abbrev-ref @{upstream} 2>/dev/null || echo main)
git diff <merge-base>..HEAD --name-only
git diff <merge-base>..HEAD
```

Read every changed source file to understand what was implemented. Note which modules have new or modified public APIs.

### 3. Evaluate existing tests

Read the current test files for all affected modules. For each existing test:

- Determine which behaviors are already covered and whether the coverage is satisfactory.
- Identify tests that can be **modified** to cover new behavior rather than duplicating coverage.
- Identify tests that **no longer apply** due to removed or significantly changed functionality — flag these for removal or rewriting.
- Note gaps that require **new** test cases.

### 4. Generate test plan

Generate a Given-When-Then test plan internally. This plan is an agent planning artifact — do NOT write spec files to disk. The plan MUST:

- Account for existing coverage: mark behaviors already tested, propose modifications to existing tests where appropriate, flag stale tests for removal or rewriting, and only add new test cases for genuinely uncovered behavior.
- Achieve 100% coverage of new or changed public APIs.
- Follow the structure in [Table Format](#table-format) below.
- Follow the project test guide conventions.

### 5. Enter plan mode

Call `EnterPlanMode` to present the test plan to the user for approval. The plan MUST clearly distinguish between:

- **Existing tests to keep** — already cover the behavior correctly
- **Existing tests to modify** — need updates to cover changed behavior
- **Existing tests to remove** — no longer apply due to removed or changed functionality
- **New tests to add** — cover genuinely uncovered behavior

### 6. Implement tests after approval

Once the user approves the plan, implement the test files:

- Modify existing test files where the plan calls for updates or removals.
- Add new test cases where the plan identifies coverage gaps.
- Follow the project test guide for file naming, class organization, and test conventions.

### 7. Prompt the user to move onto the commit step

The user MUST be prompted with the next pipeline step: "Ready to commit? Run `/commit` to stage and commit the changes." DO NOT proceed on your own.

## Table Format

Each test table MUST have these columns:

| Test Suite | Test ID | Given | When | Then | Coverage Target |
|------------|---------|-------|------|------|-----------------|

**Column descriptions**:

- **Test Suite**: Test class name (e.g., `TestParser`). Property-based tests use the SAME test class, not a separate properties class.
- **Test ID**: Sequential ID (e.g., PA-001, PA-002, PBT-001 for property-based).
- **Given**: Setup conditions (public state only).
- **When**: Action taken (public API only, using language idioms).
- **Then**: Observable outcome (public behavior only).
- **Coverage Target**: What this test covers.

## Testing Requirements

### Coverage and Scope

- MUST achieve 100% test coverage of all new or changed public APIs.
- MUST test ONLY public functions, methods, and classes (no underscore prefix).
- MUST NOT manipulate or validate any private/internal state.
- MUST NOT reference private functions/methods/variables in test specifications.
- Private functions SHOULD achieve coverage naturally through public API usage.

### Test Design Principles

- Tests MUST focus on observable behavior, not implementation details.
- Tests SHOULD remain valid even if internal implementation is completely refactored.
- MUST use "Given-When-Then" format for all test cases.
- Similar behaviors with many scenarios SHOULD use property-based tests.
- SHOULD avoid excessive test case overlap in assertions.

### Idiomatic Testing

MUST test language idioms using their natural syntax, not by calling special methods directly. Consult the project test guide for language-specific rules.

### Test Organization

- MUST group tests by module.
- MUST organize within test file by public API (class/function).
- MUST group property-based tests with their target object IN THE SAME test class.
  - BAD: Separate properties class (e.g., `TestParserProperties`).
  - GOOD: Property-based tests in the same class with PBT-xxx test IDs.
- MUST group edge cases with their target function/class.
- SHOULD follow progression: basic cases, variations, edge cases, property-based tests.

## Integration Test Specifications

When the user requests integration specs or targets an `integration/` directory, generate cross-boundary scenario specifications instead of per-module API coverage. Integration specs validate that assembled subsystems work together — they are orthogonal to unit specs, not a replacement.

### Discover existing dimensions

Before proposing any changes, MUST read the existing integration test infrastructure to discover:

- The current dimension definitions (enumerations or equivalent) and their members
- Cross-dimension constraints (which combinations are structurally invalid)
- Permanent exclusions (documented limitations that will not be fixed) vs temporary expected failures (bugs with open issues)
- The scenario model, builder, and invocation dispatcher
- Any expected-failure conditions in the test files and the issues they reference

### Extending vs adding dimensions

New work SHOULD extend existing dimensions with new members rather than adding new dimensions. A new dimension MUST only be added when the feature or subsystem being tested is genuinely orthogonal to all existing dimensions — i.e., it cannot be expressed as a new member of any existing dimension. When in doubt, prefer a new member over a new dimension; the combinatorial explosion of pairwise coverage grows with each new dimension.

When adding a new member to an existing dimension:

1. Add the member to the dimension definition
2. Update the filter function if the new member has cross-dimension constraints
3. Update the builder to resolve the new member to its concrete runtime value
4. Update the invocation dispatcher if the new member changes how results are produced or asserted
5. The pairwise covering array and exploration strategy automatically pick up the new member

### Spec generation

- **Scope**: Integration specs focus on interactions between components, not exhaustive coverage of a single module's API surface
- **Dimensions column**: Integration spec tables include a "Dimensions" column listing which scenario axes are exercised (e.g., "D1=COROUTINE, D2=DEFAULT, D3=NONE")
- **Pairwise coverage**: Scenarios are generated combinatorially via pairwise covering arrays, not enumerated manually — the spec describes the dimensions and constraints, not individual test cases
- **Stochastic exploration**: A random strategy draws from per-dimension value sets with conditional filtering, providing coverage beyond the deterministic pairwise array
- **Table format**: Use the same table columns as unit specs, but replace "Coverage Target" with "Dimensions" to indicate which scenario axes each test exercises

Consult the language-specific test guide for implementation details (libraries, data structures, parametrization mechanisms).

## Good and Bad Examples

See the project test guide for language-specific good/bad examples.

### Good Examples

| Test Suite | Test ID | Given | When | Then | Coverage Target |
|------------|---------|-------|------|------|-----------------|
| TestParser | PA-001 | Valid parameters for a Parser | Parser is instantiated | A "parser-created" event is emitted | Instantiation and event emission |
| TestParser | PA-004 | A Parser is used as a context manager | Context is entered using the language's context manager syntax | Context manager provides access to parser execution | Context manager entry |
| TestParser | PBT-001 | Any Parser with valid serializable data | Serialize then deserialize | Original object equals deserialized object in all public attributes | Serialization round-trip property |

### Bad Examples

| Test Suite | Test ID | Given | When | Then | Coverage Target |
|------------|---------|-------|------|------|-----------------|
| TestParser | BAD-001 | _internal_state is set to None | Constructor hook is called directly | Field is None | Private state |
| TestParser | BAD-002 | _dispatch is called with args | _resolve strips self parameter | Args are modified | Private function |
| TestParser | BAD-003 | A Parser | Context manager entry method is called directly | Returns run method | Calling special method directly |
| TestParserProperties | PBT-001 | Any Parser with valid data | Serialize then deserialize | Original equals deserialized | Separate property test class |

## Validation Checklist

A test plan is valid if and only if:

- Describes only publicly observable behavior.
- Uses natural language idioms (context managers, instantiation, etc.).
- Groups related tests together (by module, then by API).
- Includes edge cases with normal cases.
- Has clear Given-When-Then structure.
- Would remain valid even if all private code is rewritten.
- Focuses on "what" not "how".
- Accounts for existing test coverage (no unnecessary duplication).
- Flags stale tests for removal.

## Key Rules

- **Evaluate existing tests**: MUST read existing test files and account for current coverage before planning new tests.
- **Public APIs only**: No underscore-prefixed symbols in test specs.
- **Behavior not implementation**: Tests validate what, not how.
- **Idiomatic testing**: Use language idioms, not direct special method calls.
- **Comprehensive coverage**: 100% of new or changed public APIs must be covered.
- **Clear organization**: Group by module, class/function, test progression.
- **No spec files**: The test plan is an internal planning artifact, not a physical document.
