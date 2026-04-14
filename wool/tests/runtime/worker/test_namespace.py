import asyncio
import uuid

import pytest

from wool.runtime.worker import namespace as _namespace


class TestActivate:
    @pytest.mark.asyncio
    async def test_activate_binds_lineage_for_current_lineage(self):
        """Test activate() binds the lineage UUID so _current_lineage returns it.

        Given:
            A fresh lineage UUID
        When:
            activate() is entered inside an asyncio task and
            _current_lineage() is called
        Then:
            It should return the activated UUID
        """
        # Arrange
        lineage = uuid.uuid4()

        # Act
        with _namespace.activate(lineage):
            observed = _namespace._current_lineage()

        # Assert
        assert observed == lineage

    @pytest.mark.asyncio
    async def test_activate_restores_on_exit(self):
        """Test activate() resets _intended_lineage + sentinel on exit.

        Given:
            An asyncio task entering activate() with some lineage
        When:
            The context block exits
        Then:
            _intended_lineage should be cleared and the sentinel
            restored to its pre-activate state
        """
        # Arrange
        lineage = uuid.uuid4()
        before_intended = _namespace._intended_lineage.get(None)
        before_sentinel = _namespace._wool_sentinel.get(None)

        # Act
        with _namespace.activate(lineage):
            pass

        # Assert
        assert _namespace._intended_lineage.get(None) is before_intended
        assert _namespace._wool_sentinel.get(None) is before_sentinel

    @pytest.mark.asyncio
    async def test_activate_propagates_lineage_to_descendant_task(self):
        """Test activate() plants intended lineage so a child task adopts it.

        Given:
            An asyncio task entering activate() with a lineage UUID
        When:
            A child asyncio.create_task reads _current_lineage()
        Then:
            The child should observe the activated lineage
        """
        # Arrange
        lineage = uuid.uuid4()

        # Act
        async def child():
            return _namespace._current_lineage()

        with _namespace.activate(lineage):
            observed = await asyncio.create_task(child())

        # Assert
        assert observed == lineage


class TestInstallUninstall:
    def test_install_is_idempotent(self):
        """Test install() can be called multiple times without error.

        Given:
            The namespace module
        When:
            install() is called twice in succession
        Then:
            No exception should be raised
        """
        # Act
        _namespace.install()
        _namespace.install()

    def test_uninstall_is_idempotent(self):
        """Test uninstall() can be called multiple times without error.

        Given:
            The namespace module
        When:
            uninstall() is called twice in succession
        Then:
            No exception should be raised
        """
        # Act
        _namespace.uninstall()
        _namespace.uninstall()
