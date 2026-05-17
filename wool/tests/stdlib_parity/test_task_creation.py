"""Stdlib parity pins for ``wool.ContextVar`` propagation into child tasks.

These tests pin BOTH stdlib ``contextvars`` behavior and the Wool
parity: a :class:`wool.ContextVar` value set in a parent context must
be visible inside child tasks created through every asyncio task-
spawning edge — :func:`asyncio.create_task`,
:meth:`loop.create_task`, :func:`asyncio.ensure_future`,
:func:`asyncio.gather`, and :class:`asyncio.TaskGroup`. Unlike a plain
:class:`contextvars.ContextVar` (which a child task observes under the
*same* :class:`contextvars.Context` copy), a Wool child task runs on a
freshly-minted ``chain_id`` — copy-on-fork — while still inheriting the
parent's variable values.

The value-propagation tests take the ``make_var`` fixture and run once
per variable type, so a single assertion proves the two propagate
identically. The ``wool``-only tests additionally pin copy-on-fork:
each child, and each of two siblings, forks onto a distinct chain.

Test classes group by the asyncio task-spawning entrypoint under test
rather than by a single production class — this is a cross-cutting
parity suite with no one class under test. Every test additionally
runs under both the default ``asyncio`` loop and uvloop, via the
``event_loop_policy`` fixture in ``conftest.py``.

A future change to CPython's task-context copy semantics, or to Wool's
task factory, fails here first.
"""

import asyncio
import contextvars
import uuid

import pytest

from wool.runtime.context import ContextVar
from wool.runtime.context import current_snapshot
from wool.runtime.context import install_task_factory

pytestmark = pytest.mark.stdlib_parity


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class TestCreateTaskParity:
    @pytest.mark.asyncio
    async def test_create_task_propagates_a_scoped_value(self, make_var):
        """Test a context variable value is visible in a create_task child.

        Given:
            A context variable set in the parent.
        When:
            A child created with asyncio.create_task reads it.
        Then:
            It should observe the parent's value, identically for a
            stdlib and a wool variable.
        """
        # Arrange
        var = make_var("ct")
        var.set("parent")

        async def child() -> str:
            return var.get()

        # Act
        observed = await asyncio.create_task(child())

        # Assert
        assert observed == "parent"

    @pytest.mark.asyncio
    async def test_create_task_forks_a_fresh_chain(self):
        """Test a create_task child forks onto a fresh chain.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            A child created with asyncio.create_task reads the value
            and current_snapshot().chain_id.
        Then:
            It should observe the parent's value on a chain id distinct
            from the parent's, and leave the parent's own chain
            unchanged — copy-on-fork.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("wool_ct"))
        var.set("parent")
        parent = current_snapshot()
        assert parent is not None

        async def child() -> tuple[str, uuid.UUID]:
            snapshot = current_snapshot()
            assert snapshot is not None
            return var.get(), snapshot.chain_id

        # Act
        observed, child_chain = await asyncio.create_task(child())

        # Assert
        assert observed == "parent"
        assert child_chain != parent.chain_id
        after = current_snapshot()
        assert after is not None
        assert after.chain_id == parent.chain_id

    @pytest.mark.asyncio
    async def test_create_task_forks_distinct_sibling_chains(self):
        """Test sibling create_task children each fork onto a distinct chain.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            Two sibling child tasks are created with asyncio.create_task.
        Then:
            It should give each sibling a chain id distinct from the
            parent's and from the other sibling's — copy-on-fork mints a
            fresh chain per task, never one chain shared across siblings.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("ct_siblings"))
        var.set("parent")
        parent = current_snapshot()
        assert parent is not None

        async def child() -> uuid.UUID:
            snapshot = current_snapshot()
            assert snapshot is not None
            return snapshot.chain_id

        # Act
        first, second = await asyncio.gather(
            asyncio.create_task(child()),
            asyncio.create_task(child()),
        )

        # Assert
        assert first != parent.chain_id
        assert second != parent.chain_id
        assert first != second

    @pytest.mark.asyncio
    async def test_create_task_with_unarmed_context(self):
        """Test the task factory is dormant when the context is unarmed.

        Given:
            The Wool task factory installed but no wool.ContextVar set
            (unarmed context).
        When:
            A child task is created with asyncio.create_task.
        Then:
            It should observe no snapshot inside the child — the factory
            is dormant when unarmed.
        """
        # Arrange
        install_task_factory()

        async def child() -> bool:
            return current_snapshot() is None

        # Act
        snapshot_is_none = await asyncio.create_task(child())

        # Assert
        assert snapshot_is_none


class TestLoopCreateTaskParity:
    @pytest.mark.asyncio
    async def test_create_task_propagates_a_scoped_value(self, make_var):
        """Test a context variable value is visible in a loop.create_task child.

        Given:
            A context variable set in the parent.
        When:
            A child created with loop.create_task reads it.
        Then:
            It should observe the parent's value, identically for a
            stdlib and a wool variable.
        """
        # Arrange
        var = make_var("lct")
        var.set("parent")
        loop = asyncio.get_running_loop()

        async def child() -> str:
            return var.get()

        # Act
        observed = await loop.create_task(child())

        # Assert
        assert observed == "parent"

    @pytest.mark.asyncio
    async def test_create_task_forks_a_fresh_chain(self):
        """Test a loop.create_task child forks onto a fresh chain.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            A child created with loop.create_task reads the value and
            current_snapshot().chain_id.
        Then:
            It should observe the parent's value on a chain id distinct
            from the parent's — loop.create_task routes through the same
            task factory as asyncio.create_task.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("wool_lct"))
        var.set("parent")
        parent = current_snapshot()
        assert parent is not None
        loop = asyncio.get_running_loop()

        async def child() -> tuple[str, uuid.UUID]:
            snapshot = current_snapshot()
            assert snapshot is not None
            return var.get(), snapshot.chain_id

        # Act
        observed, child_chain = await loop.create_task(child())

        # Assert
        assert observed == "parent"
        assert child_chain != parent.chain_id


class TestEnsureFutureParity:
    @pytest.mark.asyncio
    async def test_ensure_future_propagates_a_scoped_value(self, make_var):
        """Test a context variable value is visible in an ensure_future child.

        Given:
            A context variable set in the parent.
        When:
            A child wrapped with asyncio.ensure_future reads it.
        Then:
            It should observe the parent's value, identically for a
            stdlib and a wool variable.
        """
        # Arrange
        var = make_var("ef")
        var.set("parent")

        async def child() -> str:
            return var.get()

        # Act
        observed = await asyncio.ensure_future(child())

        # Assert
        assert observed == "parent"

    @pytest.mark.asyncio
    async def test_ensure_future_forks_a_fresh_chain(self):
        """Test an ensure_future child forks onto a fresh chain.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            A child wrapped with asyncio.ensure_future reads the value
            and current_snapshot().chain_id.
        Then:
            It should observe the parent's value on a chain id distinct
            from the parent's — copy-on-fork.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("wool_ef"))
        var.set("parent")
        parent = current_snapshot()
        assert parent is not None

        async def child() -> tuple[str, uuid.UUID]:
            snapshot = current_snapshot()
            assert snapshot is not None
            return var.get(), snapshot.chain_id

        # Act
        observed, child_chain = await asyncio.ensure_future(child())

        # Assert
        assert observed == "parent"
        assert child_chain != parent.chain_id

    @pytest.mark.asyncio
    async def test_ensure_future_with_unarmed_context(self):
        """Test the task factory is dormant for ensure_future when unarmed.

        Given:
            The Wool task factory installed but no wool.ContextVar set
            (unarmed context).
        When:
            A child is created with asyncio.ensure_future.
        Then:
            It should observe no snapshot inside the child — the factory
            is dormant when unarmed.
        """
        # Arrange
        install_task_factory()

        async def child() -> bool:
            return current_snapshot() is None

        # Act
        snapshot_is_none = await asyncio.ensure_future(child())

        # Assert
        assert snapshot_is_none


class TestGatherParity:
    @pytest.mark.asyncio
    async def test_gather_propagates_a_scoped_value(self, make_var):
        """Test a context variable value is visible in gather children.

        Given:
            A context variable set in the parent.
        When:
            Two children wrapped with asyncio.gather read it.
        Then:
            It should let both children observe the parent's value,
            identically for a stdlib and a wool variable.
        """
        # Arrange
        var = make_var("gt")
        var.set("parent")

        async def child() -> str:
            return var.get()

        # Act
        observed = await asyncio.gather(child(), child())

        # Assert
        assert observed == ["parent", "parent"]

    @pytest.mark.asyncio
    async def test_gather_forks_distinct_sibling_chains(self):
        """Test gather wraps each sibling coroutine onto a distinct chain.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            Two sibling coroutines are run with asyncio.gather.
        Then:
            It should give each sibling a chain id distinct from the
            parent's and from the other sibling's — gather wraps each
            coroutine through the task factory, which forks per task.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("gt_siblings"))
        var.set("parent")
        parent = current_snapshot()
        assert parent is not None

        async def child() -> uuid.UUID:
            snapshot = current_snapshot()
            assert snapshot is not None
            return snapshot.chain_id

        # Act
        first, second = await asyncio.gather(child(), child())

        # Assert
        assert first != parent.chain_id
        assert second != parent.chain_id
        assert first != second


class TestTaskGroupParity:
    @pytest.mark.asyncio
    async def test_task_group_propagates_a_scoped_value(self, make_var):
        """Test a context variable value is visible in a TaskGroup child.

        Given:
            A context variable set in the parent.
        When:
            A child created via asyncio.TaskGroup.create_task reads it.
        Then:
            It should observe the parent's value, identically for a
            stdlib and a wool variable.
        """
        # Arrange
        var = make_var("tg")
        var.set("parent")
        observed: list[str] = []

        async def child() -> None:
            observed.append(var.get())

        # Act
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child())

        # Assert
        assert observed == ["parent"]

    @pytest.mark.asyncio
    async def test_task_group_forks_a_fresh_chain(self):
        """Test a TaskGroup child forks onto a fresh chain.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            A child created via asyncio.TaskGroup.create_task reads the
            value and current_snapshot().chain_id.
        Then:
            It should observe the parent's value on a chain id distinct
            from the parent's — copy-on-fork.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("wool_tg"))
        var.set("parent")
        parent = current_snapshot()
        assert parent is not None
        observed: list[tuple[str, uuid.UUID]] = []

        async def child() -> None:
            snapshot = current_snapshot()
            assert snapshot is not None
            observed.append((var.get(), snapshot.chain_id))

        # Act
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child())

        # Assert
        assert observed[0][0] == "parent"
        assert observed[0][1] != parent.chain_id

    @pytest.mark.asyncio
    async def test_task_group_forks_distinct_sibling_chains(self):
        """Test sibling TaskGroup children each fork onto a distinct chain.

        Given:
            A wool.ContextVar set in an armed parent with the task
            factory installed.
        When:
            Two sibling children are created via
            asyncio.TaskGroup.create_task.
        Then:
            It should give each sibling a chain id distinct from the
            parent's and from the other sibling's — copy-on-fork mints a
            fresh chain per task, never one chain shared across siblings.
        """
        # Arrange
        install_task_factory()
        var = ContextVar(_unique("tg_siblings"))
        var.set("parent")
        parent = current_snapshot()
        assert parent is not None
        chains: list[uuid.UUID] = []

        async def child() -> None:
            snapshot = current_snapshot()
            assert snapshot is not None
            chains.append(snapshot.chain_id)

        # Act
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child())
            tg.create_task(child())

        # Assert
        assert len(set(chains)) == 2
        assert all(chain != parent.chain_id for chain in chains)

    @pytest.mark.asyncio
    async def test_task_group_with_unarmed_context(self):
        """Test the task factory is dormant for TaskGroup when unarmed.

        Given:
            The Wool task factory installed but no wool.ContextVar set
            (unarmed context).
        When:
            A child is created via asyncio.TaskGroup.create_task.
        Then:
            It should observe no snapshot inside the child — the factory
            is dormant when unarmed.
        """
        # Arrange
        install_task_factory()
        observed: list[bool] = []

        async def child() -> None:
            observed.append(current_snapshot() is None)

        # Act
        async with asyncio.TaskGroup() as tg:
            tg.create_task(child())

        # Assert
        assert observed == [True]


class TestUnarmedFactory:
    @pytest.mark.asyncio
    async def test_unarmed_factory_does_not_break_plain_contextvars(self):
        """Test the installed task factory leaves plain contextvars intact.

        Given:
            The Wool task factory installed on the loop, no
            wool.ContextVar ever set (an unarmed context), and a plain
            contextvars.ContextVar set in the parent.
        When:
            A child task created with asyncio.create_task reads the
            plain variable and current_snapshot.
        Then:
            It should observe the parent's plain value and no Wool
            snapshot — installing the factory costs an unarmed context
            nothing, behaving as a plain contextvars.Context.
        """
        # Arrange
        install_task_factory()
        var: contextvars.ContextVar[str] = contextvars.ContextVar(_unique("unarmed"))
        var.set("parent")

        async def child() -> tuple[str, bool]:
            return var.get(), current_snapshot() is None

        # Act
        observed_value, snapshot_is_none = await asyncio.create_task(child())

        # Assert
        assert observed_value == "parent"
        assert snapshot_is_none
