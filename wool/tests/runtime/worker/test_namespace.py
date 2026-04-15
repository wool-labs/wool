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
