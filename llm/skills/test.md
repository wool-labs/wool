---
name: test
description: >
  Generate comprehensive test specifications for Python modules with 100%
  coverage of public APIs. Use this skill whenever the user says "/test",
  "generate test specs", "create test plan", "write test specifications", or
  similar. Analyzes module structure and produces Given-When-Then test case
  tables organized by public class and function.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT,
REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be
interpreted as described in RFC 2119.

# Test Skill

Generate comprehensive test specifications for Python modules with 100%
coverage of public APIs.

## Pipeline Context

This skill is part of the development workflow pipeline:
`/issue` → `/pr` → `/implement` → `/commit` → `/pr` (update).
This skill is a **supporting tool** invoked by `/pr` to generate test
case tables.

## Arguments

A list of target modules or file references MUST be provided as
arguments (e.g., `/test src/wool/worker/service.py`). If no arguments
are provided, the user MUST be asked which modules to analyze.

## Workflow

### 1. Identify target modules

Analyze referenced files or use the provided arguments to determine
which Python modules need test specifications.

### 2. Analyze module structure

For each module:

- **CRITICAL**: MUST NOT consider any existing tests when creating the
  test plan. Analyze only the source module itself.
- Identify all public classes (no underscore prefix).
- Identify all public functions (no underscore prefix).
- Identify all public methods in each class.
- Note dunder methods that need idiomatic testing.

### 3. Generate test specifications

For each Python module, create `specs/test_<module_name>.md` with this
structure:

- Module header: `## Module: <module_name>.py`
- For each public class:
  - Section header: `### Public Class: <ClassName>`
  - Test table (see Table Format below)
  - Include: basic cases, variations, edge cases, property-based tests
- For each public function:
  - Section header: `### Public Function: <function_name>()`
  - Test table (see Table Format below)
- Test Implementation Notes section
- Test Organization section (showing test file structure)
- Summary Statistics section

**Naming convention**: If source module is `foo_bar.py`, spec file is
`specs/test_foo_bar.md`.

Create `specs/QUICK_REFERENCE.md` with an at-a-glance summary:

- Test spec file locations (links to each `test_*.md` file)
- Test suite class names per module
- Coverage statistics per module
- Quick navigation to each module/class/function

### 4. Validate test specifications

- All tests focus on public APIs only (no underscore-prefixed symbols).
- All tests use Given-When-Then format.
- Dunder methods tested idiomatically (not called directly).
- Edge cases grouped with normal cases.
- Property-based tests grouped with their target object.
- 100% coverage of public APIs achieved.

### 5. Report completion

Provide paths to generated files and summary statistics.

## Table Format

Each test table MUST have these columns:

| Test Suite | Test ID | Given | When | Then | Coverage Target |
|------------|---------|-------|------|------|-----------------|

**Column descriptions**:

- **Test Suite**: Test class name (e.g., `TestWorkTask`). Property-based
  tests use the SAME test class, not a separate
  `TestWorkTaskProperties` class.
- **Test ID**: Sequential ID (e.g., WT-001, WT-002, PBT-001 for
  property-based).
- **Given**: Setup conditions (public state only).
- **When**: Action taken (public API only, using Python idioms).
- **Then**: Observable outcome (public behavior only).
- **Coverage Target**: What this test covers.

## Testing Requirements

### Coverage and Scope

- MUST achieve 100% test coverage of all public APIs.
- MUST test ONLY public functions, methods, and classes (no underscore
  prefix).
- MUST NOT manipulate or validate any private/internal state.
- MUST NOT reference private functions/methods/variables in test
  specifications.
- Private functions SHOULD achieve coverage naturally through public API
  usage.

### Test Design Principles

- Tests MUST focus on observable behavior, not implementation details.
- Tests SHOULD remain valid even if internal implementation is
  completely refactored.
- MUST use "Given-When-Then" format for all test cases.
- Similar behaviors with many scenarios SHOULD use property-based tests
  (Hypothesis).
- SHOULD avoid excessive test case overlap in assertions.

### Idiomatic Testing

MUST test dunder methods idiomatically, MUST NOT call them directly:

- `__init__`/`__post_init__`: Test via class instantiation.
- `__enter__`/`__exit__`: Test via `with` statements.
- `__call__`: Test by calling the object as a function.
- Other dunder methods: Use their natural Python idiom.

### Test Organization

- MUST group tests by module (e.g., `test_<module_name>.py`).
- MUST organize within test file by public API (class/function).
- MUST group property-based tests with their target object IN THE SAME
  test class.
  - BAD: Separate `TestWorkTaskProperties` class.
  - GOOD: Property-based tests in `TestWorkTask` with PBT-xxx test IDs.
- MUST group edge cases with their target function/class.
- SHOULD follow progression: basic cases, variations, edge cases,
  property-based tests.

## Good and Bad Examples

### Good Examples

| Test Suite | Test ID | Given | When | Then | Coverage Target |
|------------|---------|-------|------|------|-----------------|
| TestWorkTask | WT-001 | Valid parameters for a WorkTask | WorkTask is instantiated | A "task-created" event is emitted | Task instantiation and event emission |
| TestWorkTask | WT-004 | A WorkTask is used as a context manager | Context is entered using `with` statement | Context manager provides access to task execution | Context manager entry |
| TestWorkTask | PBT-001 | Any WorkTask with valid picklable data | Serialize with to_protobuf then deserialize with from_protobuf | Original task equals deserialized task in all public attributes | Serialization round-trip property |

### Bad Examples

| Test Suite | Test ID | Given | When | Then | Coverage Target |
|------------|---------|-------|------|------|-----------------|
| TestWorkTask | BAD-001 | _current_task is set to None | `__post_init__` is called | caller field is None | Private state |
| TestWorkTask | BAD-002 | _dispatch is called with args | _resolve strips self parameter | Args are modified | Private function |
| TestWorkTask | BAD-003 | A WorkTask | __enter__() is called directly | Returns run method | Calling dunder directly |
| TestWorkTaskProperties | PBT-001 | Any WorkTask with valid data | Serialize then deserialize | Original equals deserialized | Separate property test class |

## Validation Checklist

A test specification is valid if and only if:

- Describes only publicly observable behavior.
- Uses natural Python idioms (with statements, instantiation, etc.).
- Groups related tests together (by module, then by API).
- Includes edge cases with normal cases.
- Has clear Given-When-Then structure.
- Would remain valid even if all private code is rewritten.
- Focuses on "what" not "how".

## Key Rules

- **Ignore existing tests**: MUST NOT consider any existing test files
  when creating test specifications.
- **Public APIs only**: No underscore-prefixed symbols in test specs.
- **Behavior not implementation**: Tests validate what, not how.
- **Idiomatic testing**: Use Python idioms, not direct dunder calls.
- **Comprehensive coverage**: 100% of public APIs must be covered.
- **Clear organization**: Group by module, class/function, test
  progression.
- **Separate files**: One spec file per Python module in `specs/`
  directory.
