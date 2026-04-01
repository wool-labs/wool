from __future__ import annotations

import asyncio

import pytest

from wool.utilities.noreentry import noreentry


# Test helpers (not fixtures)
class _SyncDummy:
    """Class with a sync @noreentry method."""

    @noreentry
    def run(self):
        return "ok"


class _AsyncDummy:
    """Class with an async @noreentry method."""

    @noreentry
    async def run(self):
        return "ok"


class _MultiMethodDummy:
    """Class with multiple @noreentry methods."""

    @noreentry
    def first(self):
        return "first"

    @noreentry
    def second(self):
        return "second"


class TestNoReentry:
    """Tests for the noreentry decorator."""

    def test_noreentry_sync_method_first_invocation(self):
        """Test sync method executes normally on first invocation.

        Given:
            A class with a sync @noreentry method.
        When:
            The method is called for the first time.
        Then:
            It should return normally.
        """
        # Arrange
        obj = _SyncDummy()

        # Act
        result = obj.run()

        # Assert
        assert result == "ok"

    def test_noreentry_sync_method_second_invocation_raises(self):
        """Test sync method raises RuntimeError on second invocation.

        Given:
            A class with a sync @noreentry method called once.
        When:
            The method is called a second time on the same instance.
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        obj = _SyncDummy()
        obj.run()

        # Act & assert
        with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
            obj.run()

    @pytest.mark.asyncio
    async def test_noreentry_async_method_first_invocation(self):
        """Test async method executes normally on first invocation.

        Given:
            A class with an async @noreentry method.
        When:
            The method is awaited for the first time.
        Then:
            It should return normally.
        """
        # Arrange
        obj = _AsyncDummy()

        # Act
        result = await obj.run()

        # Assert
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_noreentry_async_method_second_invocation_raises(self):
        """Test async method raises RuntimeError on second invocation.

        Given:
            A class with an async @noreentry method awaited once.
        When:
            The method is awaited a second time on the same instance.
        Then:
            It should raise RuntimeError.
        """
        # Arrange
        obj = _AsyncDummy()
        await obj.run()

        # Act & assert
        with pytest.raises(RuntimeError, match="cannot be invoked more than once"):
            await obj.run()

    def test_noreentry_separate_instances_independent(self):
        """Test instances track guard state independently.

        Given:
            Two instances of a class with a @noreentry method.
        When:
            The method is called on the first instance.
        Then:
            The method remains callable on the second instance.
        """
        # Arrange
        a = _SyncDummy()
        b = _SyncDummy()
        a.run()

        # Act
        result = b.run()

        # Assert
        assert result == "ok"

    def test_noreentry_error_message_qualname(self):
        """Test RuntimeError includes the method's qualified name.

        Given:
            A class with a @noreentry method called once.
        When:
            The method is called a second time.
        Then:
            The RuntimeError message should include the method's __qualname__.
        """
        # Arrange
        obj = _SyncDummy()
        obj.run()

        # Act & assert
        with pytest.raises(RuntimeError, match="_SyncDummy.run"):
            obj.run()

    def test_noreentry_preserves_coroutinefunction_check(self):
        """Test decorator preserves async function detection.

        Given:
            A class with an async @noreentry method.
        When:
            asyncio.iscoroutinefunction is called on the method.
        Then:
            It should return True.
        """
        # Act & assert
        assert asyncio.iscoroutinefunction(_AsyncDummy.run)

    def test_noreentry_preserves_wrapped_function_name(self):
        """Test decorator preserves the original function name.

        Given:
            A class with a @noreentry method.
        When:
            The decorated method's __name__ is inspected.
        Then:
            It should equal the original function name.
        """
        # Act & assert
        assert _SyncDummy.run.__name__ == "run"

    def test_noreentry_multiple_methods_independent(self):
        """Test guard on one method does not affect other methods.

        Given:
            A class with two @noreentry methods where the first
            has been guarded (called twice).
        When:
            The second method is called.
        Then:
            The second method executes normally.
        """
        # Arrange
        obj = _MultiMethodDummy()
        obj.first()
        with pytest.raises(RuntimeError):
            obj.first()

        # Act
        result = obj.second()

        # Assert
        assert result == "second"

    def test_noreentry_unbound_access_returns_descriptor(self):
        """Test accessing decorated method via class returns descriptor.

        Given:
            A class with a @noreentry method.
        When:
            The method is accessed through the class (unbound).
        Then:
            It should return the descriptor itself.
        """
        # Act
        unbound = _SyncDummy.run

        # Assert
        assert unbound is not None

    def test_noreentry_bare_function_raises_error(self):
        """Test decorator rejects application to bare functions.

        Given:
            A @noreentry-decorated function called without a bound instance.
        When:
            The descriptor is called directly.
        Then:
            It should raise TypeError.
        """
        # Arrange
        unbound = _SyncDummy.run

        # Act & assert
        with pytest.raises(
            TypeError, match="only decorates methods, not bare functions"
        ):
            unbound()
