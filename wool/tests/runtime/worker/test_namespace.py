import asyncio
import uuid

import pytest

import wool
from wool.runtime.worker import namespace as _namespace


class TestActivate:
    @pytest.mark.asyncio
    async def test_activate_binds_context_id_visible_via_current_context(self):
        """Test activate() makes the context id visible through wool.current_context.

        Given:
            A fresh context id
        When:
            activate() is entered inside an asyncio task and
            wool.current_context() is called
        Then:
            The returned Context's id should equal the activated context id
        """
        # Arrange
        context_id = uuid.uuid4()

        # Act
        with _namespace.activate(context_id):
            observed = wool.current_context().id

        # Assert
        assert observed == context_id

    @pytest.mark.asyncio
    async def test_activate_restores_context_id_on_exit(self):
        """Test activate() restores the prior context id once the block exits.

        Given:
            An asyncio task entering activate() with some context id
        When:
            The context block exits and wool.current_context() is read
        Then:
            The returned Context's id should not equal the just-activated
            context id
        """
        # Arrange
        context_id = uuid.uuid4()

        # Act
        with _namespace.activate(context_id):
            pass
        observed = wool.current_context().id

        # Assert
        assert observed != context_id

    @pytest.mark.asyncio
    async def test_activate_propagates_context_id_to_descendant_task(self):
        """Test activate() plants the context id so a child task adopts it.

        Given:
            An asyncio task entering activate() with a context UUID
        When:
            A child asyncio.create_task reads wool.current_context()
        Then:
            The child should observe the activated context id
        """

        # Arrange
        context_id = uuid.uuid4()

        async def child():
            return wool.current_context().id

        # Act
        with _namespace.activate(context_id):
            observed = await asyncio.create_task(child())

        # Assert
        assert observed == context_id

    def test_activate_without_running_task_binds_context_id_for_sync_caller(self):
        """Test activate() binds the context id for sync callers with no task.

        Given:
            A sync caller with no running asyncio task
        When:
            activate(context_id) is entered and wool.current_context() is
            read inside the ``with`` block
        Then:
            The returned Context.id equals the activated context_id, and
            on exit current_context().id no longer equals it — the
            handler-task-is-None branch of activate correctly plants
            and clears _intended_context_id.
        """
        # Arrange
        context_id = uuid.uuid4()

        # Act
        with _namespace.activate(context_id):
            inside = wool.current_context().id
        outside = wool.current_context().id

        # Assert
        assert inside == context_id
        assert outside != context_id

    @pytest.mark.asyncio
    async def test_activate_restores_context_id_on_exception(self):
        """Test activate() restores the prior context id when the body raises.

        Given:
            An asyncio task with a known pre-activate context id
        When:
            activate() is entered and the body raises an exception
        Then:
            The context id after catching the exception should not equal
            the activated id
        """
        # Arrange
        context_id = uuid.uuid4()
        before_id = wool.current_context().id

        # Act
        with pytest.raises(RuntimeError):
            with _namespace.activate(context_id):
                raise RuntimeError("boom")
        after_id = wool.current_context().id

        # Assert
        assert after_id != context_id
        assert after_id == before_id

    @pytest.mark.asyncio
    async def test_activate_nested_restores_outer_context_id(self):
        """Test nested activate() restores the outer context id on exit.

        Given:
            Two distinct context ids (outer and inner)
        When:
            activate() is nested with the inner id inside the outer id
        Then:
            Inside the inner block the inner id should be visible,
            after the inner exits the outer id should be restored,
            and after both exit the original id should be restored
        """
        # Arrange
        outer_id = uuid.uuid4()
        inner_id = uuid.uuid4()
        original_id = wool.current_context().id

        # Act
        with _namespace.activate(outer_id):
            observed_outer = wool.current_context().id
            with _namespace.activate(inner_id):
                observed_inner = wool.current_context().id
            observed_after_inner = wool.current_context().id
        observed_after_outer = wool.current_context().id

        # Assert
        assert observed_outer == outer_id
        assert observed_inner == inner_id
        assert observed_after_inner == outer_id
        assert observed_after_outer == original_id

    @pytest.mark.asyncio
    async def test_activate_creates_empty_context_for_block(self):
        """Test activate() starts with an empty context that has no vars.

        Given:
            A ContextVar with a value set before the activate block
        When:
            activate() is entered and the var is read inside the block
        Then:
            The var should not have a value set in the new context
            (get with a default should return the default)
        """
        # Arrange
        var = wool.ContextVar("ns_test_isolation", namespace="test_ns")
        var.set("before_activate")
        context_id = uuid.uuid4()

        # Act
        with _namespace.activate(context_id):
            inside_value = var.get("fallback")

        # Assert
        assert inside_value == "fallback"

    @pytest.mark.asyncio
    async def test_activate_vars_set_inside_do_not_leak_outside(self):
        """Test vars set inside activate() do not leak to the outer context.

        Given:
            A ContextVar with no value in the outer context
        When:
            activate() is entered and the var is set inside the block
        Then:
            After exiting the block the var should not have the value
            set inside
        """
        # Arrange
        var = wool.ContextVar("ns_test_leak", namespace="test_ns_leak")
        context_id = uuid.uuid4()

        # Act
        with _namespace.activate(context_id):
            var.set("inside_only")
        has_value = True
        try:
            var.get()
        except LookupError:
            has_value = False

        # Assert
        assert not has_value

    @pytest.mark.asyncio
    async def test_activate_var_set_inside_visible_to_child_task(self):
        """Test a ContextVar set inside activate propagates to a child task.

        Given:
            An activate block with a ContextVar set inside it
        When:
            A child task reads the var
        Then:
            The child task should see the value set in the activate block
        """
        # Arrange
        var = wool.ContextVar("ns_test_child_prop", namespace="test_ns_child")
        context_id = uuid.uuid4()

        async def child():
            return var.get("missing")

        # Act
        with _namespace.activate(context_id):
            var.set("propagated")
            observed = await asyncio.create_task(child())

        # Assert
        assert observed == "propagated"


class TestAdoptContext:
    def test_run_adopts_outer_context_id_for_sync_caller_without_task(self):
        """Test Context.run propagates id via adopt_context for sync callers.

        Given:
            A sync caller with no running asyncio task and an outer
            wool.Context whose ``run`` invokes a function that reads
            wool.current_context()
        When:
            Context.run is invoked with a sync fn that observes the
            current context id
        Then:
            The observed id equals the outer Context's id — the
            adopt_context no-asyncio-task branch leaves the seeded
            ``_intended_context_id`` in place, and current_context()
            reads it back.
        """
        # Arrange
        outer = wool.Context()
        observed: list = []

        def fn():
            observed.append(wool.current_context().id)

        # Act
        outer.run(fn)

        # Assert
        assert observed == [outer.id]

    @pytest.mark.asyncio
    async def test_run_binds_context_id_via_adopt_when_task_is_running(self):
        """Test Context.run binds the id via adopt_context when a task is present.

        Given:
            An async caller with a running asyncio task invoking
            wool.Context.run(fn), where fn reads current_context().id
        When:
            The Context id inside fn is observed
        Then:
            It equals the outer Context's id — adopt_context's
            task-is-not-None branch binds the wool.Context id onto
            the current asyncio task so subsequent current_context()
            reads return it.
        """
        # Arrange
        outer = wool.Context()
        observed: list = []

        def fn():
            observed.append(wool.current_context().id)

        # Act
        outer.run(fn)

        # Assert
        assert observed == [outer.id]

    @pytest.mark.asyncio
    async def test_run_restores_previous_context_on_exit(self):
        """Test Context.run restores the previous context after fn returns.

        Given:
            An asyncio task with a known current context id and a
            separate wool.Context
        When:
            Context.run is invoked and returns
        Then:
            The current context id after run exits should equal the
            original id, not the run context's id
        """
        # Arrange
        original_id = wool.current_context().id
        other = wool.Context()

        def fn():
            pass

        # Act
        other.run(fn)
        after_id = wool.current_context().id

        # Assert
        assert after_id == original_id
        assert after_id != other.id

    @pytest.mark.asyncio
    async def test_run_async_binds_context_id_for_coroutine(self):
        """Test Context.run_async binds the context id for an async coroutine.

        Given:
            A wool.Context and a coroutine that reads current_context().id
        When:
            Context.run_async is invoked with the coroutine
        Then:
            The coroutine should observe the Context's id
        """
        # Arrange
        ctx = wool.Context()

        async def coro():
            return wool.current_context().id

        # Act
        observed = await ctx.run_async(coro())

        # Assert
        assert observed == ctx.id
