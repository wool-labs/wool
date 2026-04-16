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
