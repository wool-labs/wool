---
name: test
description: >
  Generate comprehensive test specifications for source modules with 100%
  coverage of public APIs. Use this skill whenever the user says "/test",
  "generate test specs", "create test plan", "write test specifications", or
  similar. Analyzes module structure and produces Given-When-Then test case
  tables organized by public class and function.
---

The key words MUST, MUST NOT, SHALL, SHALL NOT, SHOULD, SHOULD NOT, REQUIRED, RECOMMENDED, MAY, and OPTIONAL in this document are to be interpreted as described in RFC 2119.

# Test Skill

Generate comprehensive test specifications for source modules with 100% coverage of public APIs.

## Pipeline Context

This skill is part of the development workflow pipeline: `/issue` → `/pr` → `/implement` → `/commit` → `/pr` (update). This skill is a **supporting tool** invoked by `/pr` to generate test case tables.

## Arguments

A list of target modules or file references MUST be provided as arguments (e.g., `/test src/wool/worker/service.py`). If no arguments are provided, the user MUST be asked which modules to analyze.

> **Test guide:** Determine the language of the target modules from their file extensions, then read the corresponding test guide before generating test specifications:
>
> | Language | Guide |
> |----------|-------|
> | Python (`.py`) | `@llm/guides/testguide-python.md` |
>
> If no guide exists for the detected language, inform the user and stop.

## Workflow

### 1. Identify target modules

Analyze referenced files or use the provided arguments to determine which source modules need test specifications.

### 2. Analyze module structure

For each module:

- **CRITICAL**: MUST NOT consider any existing tests when creating the test plan. Analyze only the source module itself.
- Identify all public classes (no underscore prefix).
- Identify all public functions (no underscore prefix).
- Identify all public methods in each class.
- Note language-specific idioms that need idiomatic testing (per the project test guide).

### 3. Generate test specifications

For each source module, create a test specification file using the naming convention from the project test guide:

- Module header: `## Module: <module_name>`
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

Create `specs/QUICK_REFERENCE.md` with an at-a-glance summary:

- Test spec file locations (links to each spec file)
- Test suite class names per module
- Coverage statistics per module
- Quick navigation to each module/class/function

### 4. Validate test specifications

- All tests focus on public APIs only (no underscore-prefixed symbols).
- All tests use Given-When-Then format.
- Language idioms tested idiomatically (not by calling special methods directly).
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

- **Test Suite**: Test class name (e.g., `TestParser`). Property-based tests use the SAME test class, not a separate properties class.
- **Test ID**: Sequential ID (e.g., PA-001, PA-002, PBT-001 for property-based).
- **Given**: Setup conditions (public state only).
- **When**: Action taken (public API only, using language idioms).
- **Then**: Observable outcome (public behavior only).
- **Coverage Target**: What this test covers.

## Testing Requirements

### Coverage and Scope

- MUST achieve 100% test coverage of all public APIs.
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

A test specification is valid if and only if:

- Describes only publicly observable behavior.
- Uses natural language idioms (context managers, instantiation, etc.).
- Groups related tests together (by module, then by API).
- Includes edge cases with normal cases.
- Has clear Given-When-Then structure.
- Would remain valid even if all private code is rewritten.
- Focuses on "what" not "how".

## Key Rules

- **Ignore existing tests**: MUST NOT consider any existing test files when creating test specifications.
- **Public APIs only**: No underscore-prefixed symbols in test specs.
- **Behavior not implementation**: Tests validate what, not how.
- **Idiomatic testing**: Use language idioms, not direct special method calls.
- **Comprehensive coverage**: 100% of public APIs must be covered.
- **Clear organization**: Group by module, class/function, test progression.
- **Separate files**: One spec file per source module in `specs/` directory.