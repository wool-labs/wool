import asyncio
import uuid

import pytest

import wool
from wool.runtime.worker import namespace as _namespace


class TestActivate:
    @pytest.mark.asyncio
    async def test_activate_binds_lineage_visible_via_current_context(self):
        """Test activate() makes the lineage visible through wool.current_context.

        Given:
            A fresh lineage UUID
        When:
            activate() is entered inside an asyncio task and
            wool.current_context() is called
        Then:
            The returned Context's id should equal the activated UUID
        """
        # Arrange
        lineage = uuid.uuid4()

        # Act
        with _namespace.activate(lineage):
            observed = wool.current_context().id

        # Assert
        assert observed == lineage

    @pytest.mark.asyncio
    async def test_activate_restores_lineage_on_exit(self):
        """Test activate() restores the prior lineage once the block exits.

        Given:
            An asyncio task entering activate() with some lineage
        When:
            The context block exits and wool.current_context() is read
        Then:
            The returned Context's id should not equal the just-activated
            lineage
        """
        # Arrange
        lineage = uuid.uuid4()

        # Act
        with _namespace.activate(lineage):
            pass
        observed = wool.current_context().id

        # Assert
        assert observed != lineage

    @pytest.mark.asyncio
    async def test_activate_propagates_lineage_to_descendant_task(self):
        """Test activate() plants the lineage so a child task adopts it.

        Given:
            An asyncio task entering activate() with a lineage UUID
        When:
            A child asyncio.create_task reads wool.current_context()
        Then:
            The child should observe the activated lineage
        """

        # Arrange
        lineage = uuid.uuid4()

        async def child():
            return wool.current_context().id

        # Act
        with _namespace.activate(lineage):
            observed = await asyncio.create_task(child())

        # Assert
        assert observed == lineage
