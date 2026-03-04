# Test Rules

Key words MUST, MUST NOT, SHOULD, and SHOULD NOT are interpreted as
described in RFC 2119. Examples use Python, but the rules apply
language-agnostically.

## 1. Core Philosophy

Test behavior, not implementation.

## 2. File and Class Organization

- Test files: `test_<module_name>.py` — mirrors the module under test.
- Test classes mirror production classes: `class Test<ClassName>:` where
  `<ClassName>` is the exact `__name__` of the class under test.
- Tests for class methods live in the corresponding test class.
- Tests for module-level functions are module-level test functions.
- Tests MUST be grouped by the behavior they exercise, not by
  implementation detail. For example, property-based tests for a method
  live alongside example-based tests for the same method in the same
  class — do NOT create separate classes by test technique.
- Within a test class or module, order tests from foundational to
  derived: test `__init__` before methods that require a constructed
  instance, and test simple methods before methods that compose them.
  This ensures `pytest -x` stops at the most fundamental failure first.

## 3. Test Naming

Test methods mirror the qualified name of their subject:

```
test_<method_name>_<brief_scenario>
```

The scenario portion is derived from the Given and When — it describes
the condition and action, not the expected outcome:

```
test_dispatch_with_stopping_service
test_to_protobuf_with_unpicklable_callable
test___init___outside_task_context
```

Rules:
- `<method_name>` MUST match the method's `__name__` exactly, including
  dunder prefixes (e.g., `test___init___...`).
- `<brief_scenario>` SHOULD be 2-5 words in snake_case.
- Do NOT encode the expected outcome in the name — that belongs in the
  `Then` section of the docstring.

## 4. Docstrings (Given-When-Then)

Scope: test functions and methods only — NOT fixtures or helpers.

**Standard test:**

```python
"""Test <summary of what is being tested>.

Given:
    <preconditions>
When:
    <action under test>
Then:
    It should <expected outcome>
"""
```

**Property-based test:**

Same format. The `Given` describes the generated input space, not a
specific value:

```python
"""Test <property being verified>.

Given:
    <description of generated input domain>
When:
    <action under test>
Then:
    <invariant that must hold>
"""
```

Rules:
- First line MUST start with `Test` and describe what is being tested.
- Blank line after the first line.
- `Given:`, `When:`, `Then:` each on their own line, followed by a colon.
- Content under each section indented 4 spaces.
- Each section's content starts with a capital letter.
- `Then` content typically starts with "It should...".
- One `Given`/`When`/`Then` block per docstring (no bullet lists within).

## 5. Test Body (AAA)

Every test MUST use Arrange-Act-Assert phase comments. One behavior per
test.

**Full 3-phase (default):**

```python
def test_<method>_<scenario>(self, ...):
    """..."""
    # Arrange
    <set up preconditions>

    # Act
    <call the method under test>

    # Assert
    <verify the outcome>
```

**Combined phases:**

When phases are inseparable, combine the comments:

*No arrangement needed:*

```python
# Act
result = SomeClass()

# Assert
assert result.field == expected
```

*Action and assertion are the same expression:*

```python
# Arrange
<set up preconditions>

# Act & Assert
with pytest.raises(SomeError, match="expected message"):
    unit.method()
```

*Trivial one-liner:*

```python
# Arrange & Act & Assert
async with SomePool(size=3) as pool:
    assert pool is not None
```

The combined forms are acceptable but the full 3-phase form is preferred.
When in doubt, use the full form.

## 6. Mocking

- MUST use the `mocker` fixture (pytest-mock), not `unittest.mock`
  directly.
- Mock at system boundaries only: external services, I/O, network.
- MUST NOT mock internal/private methods.
- MUST NOT mock the unit under test.

## 7. Async Testing

- Use `pytest-asyncio` with `@pytest.mark.asyncio`.
- Async mocks via `mocker.AsyncMock()`.

```python
@pytest.mark.asyncio
async def test_fetch_data_with_valid_endpoint(self, mocker):
    """..."""
    # Arrange
    mock_response = mocker.AsyncMock(return_value={"key": "value"})
    mocker.patch("module.http_client.get", mock_response)

    # Act
    result = await unit.fetch_data("/endpoint")

    # Assert
    assert result == {"key": "value"}
```

## 8. Fixtures

- Use fixtures for shared setup that multiple tests need.
- Fixtures MUST NOT have Given-When-Then docstrings. Plain reST
  docstrings are fine.
- Prefer narrowest scope: function > class > module > session.

## 9. Property-Based Testing (Hypothesis)

Property-based testing is a peer of example-based testing, not an
afterthought. Property tests live in the same class as example tests,
grouped by the method they exercise.

MUST be used for:
- Invariants.
- Roundtrip/serialization.
- Edge-case discovery.
- Any function with a wide input domain.

Rules:
- Use `@given` with appropriate strategies.
- Use `@settings` to control `max_examples` and suppress health checks
  when justified.
- Property tests get Given-When-Then docstrings (the `Given` describes
  the generated domain, not a specific value).

```python
@given(st.binary())
def test_roundtrip_with_arbitrary_payload(self, payload):
    """Test serialization roundtrip with arbitrary payloads.

    Given:
        Any binary payload.
    When:
        The payload is serialized and deserialized.
    Then:
        It should equal the original payload.
    """
    # Act
    result = deserialize(serialize(payload))

    # Assert
    assert result == payload
```

## 10. Test Independence

- Tests MUST be independent. Each test sets up its own preconditions.
- When a foundation behavior breaks, many tests fail. The blast radius
  IS the information — it tells you which behaviors depend on the broken
  one.
- Use `pytest -x` (stop on first failure), `--maxfail=N`, or `-lf`
  (last failed) to manage noise.
- Dependency markers (`@pytest.mark.dependency`) MUST NOT be used in
  new tests.

## 11. Verification Commands

```
uv run pytest <path> -x
uv run pytest <path> --maxfail=3
uv run pytest <path> -lf
```
