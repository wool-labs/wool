import asyncio

import pytest

from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import current_context


def test_current_context_with_set_vars():
    """Test current_context() returns the live Context for the scope.

    Given:
        A ContextVar with an explicit value set
    When:
        current_context() is called twice from the same scope
    Then:
        The returned Context contains the var with its value and
        both calls yield the same Context instance (idempotent
        within a scope)
    """
    # Arrange
    var = ContextVar("cur_ctx", default=0)
    var.set(1)

    # Act
    ctx = current_context()

    # Assert
    assert ctx[var] == 1
    assert current_context() is ctx


@pytest.mark.asyncio
async def test_context_registry_get_when_scope_has_no_bound_ctx():
    """Test :meth:`_ContextRegistry.get` returns ``None`` when the
    current scope has no bound :class:`Context`.

    Given:
        A scope (an asyncio task) running with the asyncio default
        task factory — no wool task factory is installed, so no
        :class:`Context` is auto-bound to the task identity.
    When:
        :meth:`context_registry.get` is called twice from inside
        that scope.
    Then:
        Both reads observe ``None`` — the non-creating accessor
        does not materialize a Context, while
        :func:`current_context` does (covered separately by
        :func:`test_current_context_with_set_vars`).
    """
    # Arrange
    from wool.runtime.context.registry import context_registry

    loop = asyncio.get_running_loop()
    # Ensure no wool factory is installed on this loop so the child
    # task does not auto-bind a Context to its identity.
    loop.set_task_factory(None)

    observed: list[Context | None] = []

    async def body():
        observed.append(context_registry.get())
        observed.append(context_registry.get())

    # Act — run under the asyncio default task factory so no wool
    # binding leaks in.
    await loop.create_task(body())

    # Assert
    assert observed == [None, None]
