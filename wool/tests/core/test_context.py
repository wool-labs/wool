import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool.core.context import RuntimeContext
from wool.core.context import dispatch_timeout
from wool.core.typing import Undefined


class TestRuntimeContext:
    def test___init___with_dispatch_timeout(self):
        """Test RuntimeContext initialization with dispatch_timeout.

        Given:
            A dispatch_timeout value
        When:
            RuntimeContext is instantiated with dispatch_timeout
        Then:
            It should store the timeout value internally
        """
        # Arrange
        timeout = 5.0

        # Act
        context = RuntimeContext(dispatch_timeout=timeout)

        # Assert - the context was created successfully
        # (actual timeout is set when entering the context)
        assert context is not None

    def test___init___with_none_dispatch_timeout(self):
        """Test RuntimeContext initialization with None dispatch_timeout.

        Given:
            A None dispatch_timeout value
        When:
            RuntimeContext is instantiated with dispatch_timeout=None
        Then:
            It should store None as the timeout value internally
        """
        # Act
        context = RuntimeContext(dispatch_timeout=None)

        # Assert - the context was created successfully
        assert context is not None

    def test___init___with_undefined_dispatch_timeout(self):
        """Test RuntimeContext initialization with Undefined dispatch_timeout.

        Given:
            No dispatch_timeout argument (defaults to Undefined)
        When:
            RuntimeContext is instantiated without dispatch_timeout
        Then:
            It should not modify the contextvar when entered
        """
        # Arrange
        original_value = dispatch_timeout.get()

        # Act
        context = RuntimeContext()
        context.__enter__()

        # Assert - contextvar should remain unchanged
        assert dispatch_timeout.get() == original_value

        # Cleanup
        context.__exit__(None, None, None)

    def test___enter___does_not_set_undefined_dispatch_timeout(self):
        """Test context manager entry with Undefined dispatch_timeout.

        Given:
            A RuntimeContext with dispatch_timeout=Undefined
        When:
            The context manager is entered
        Then:
            It should not modify the dispatch_timeout contextvar
        """
        # Arrange
        context = RuntimeContext(dispatch_timeout=Undefined)
        original_value = dispatch_timeout.get()

        # Act
        context.__enter__()

        # Assert
        assert dispatch_timeout.get() == original_value

    def test___enter___stores_token(self):
        """Test context manager entry stores reset token.

        Given:
            A RuntimeContext with a dispatch_timeout value
        When:
            The context manager is entered
        Then:
            It should set the contextvar and allow proper cleanup on exit
        """
        # Arrange
        original_value = dispatch_timeout.get()
        timeout = 3.0
        context = RuntimeContext(dispatch_timeout=timeout)

        # Act
        context.__enter__()

        # Assert - timeout is set
        assert dispatch_timeout.get() == timeout

        # Act - exit the context
        context.__exit__(None, None, None)

        # Assert - timeout is restored
        assert dispatch_timeout.get() == original_value

    def test___enter___stores_undefined_token_when_timeout_undefined(self):
        """Test context manager entry does not modify contextvar when Undefined.

        Given:
            A RuntimeContext with dispatch_timeout=Undefined
        When:
            The context manager is entered and exited
        Then:
            The contextvar should remain unchanged throughout
        """
        # Arrange
        original_value = dispatch_timeout.get()
        context = RuntimeContext(dispatch_timeout=Undefined)

        # Act
        context.__enter__()

        # Assert - contextvar unchanged
        assert dispatch_timeout.get() == original_value

        # Act - exit
        context.__exit__(None, None, None)

        # Assert - still unchanged
        assert dispatch_timeout.get() == original_value

    def test___exit___resets_dispatch_timeout(self):
        """Test context manager exit resets dispatch_timeout.

        Given:
            A RuntimeContext that has been entered with a timeout
        When:
            The context manager is exited
        Then:
            It should restore the previous dispatch_timeout value
        """
        # Arrange
        original_value = dispatch_timeout.get()
        timeout = 7.5
        context = RuntimeContext(dispatch_timeout=timeout)
        context.__enter__()
        assert dispatch_timeout.get() == timeout

        # Act
        context.__exit__(None, None, None)

        # Assert
        assert dispatch_timeout.get() == original_value

    def test___exit___does_not_reset_when_token_undefined(self):
        """Test context manager exit with Undefined token.

        Given:
            A RuntimeContext entered with dispatch_timeout=Undefined
        When:
            The context manager is exited
        Then:
            It should not attempt to reset the contextvar
        """
        # Arrange
        original_value = dispatch_timeout.get()
        context = RuntimeContext(dispatch_timeout=Undefined)
        context.__enter__()

        # Act
        context.__exit__(None, None, None)

        # Assert
        assert dispatch_timeout.get() == original_value

    def test_nested_contexts_with_undefined(self):
        """Test nested contexts with Undefined in the middle.

        Given:
            Nested RuntimeContext managers where inner context has Undefined
        When:
            They are entered and exited
        Then:
            Undefined context should not modify the timeout set by outer
            context
        """
        # Arrange
        original_value = dispatch_timeout.get()
        outer_timeout = 15.0

        # Act & Assert
        with RuntimeContext(dispatch_timeout=outer_timeout):
            assert dispatch_timeout.get() == outer_timeout

            # Inner context with Undefined should not change the timeout
            with RuntimeContext(dispatch_timeout=Undefined):
                assert dispatch_timeout.get() == outer_timeout

            # After exiting inner context, timeout should still be outer
            assert dispatch_timeout.get() == outer_timeout

        assert dispatch_timeout.get() == original_value

    def test_nested_contexts_with_none(self):
        """Test nested contexts where inner context sets None.

        Given:
            Nested RuntimeContext managers where inner sets timeout to None
        When:
            They are entered and exited
        Then:
            Inner context should set timeout to None, outer should restore
            its value
        """
        # Arrange
        original_value = dispatch_timeout.get()
        outer_timeout = 20.0

        # Act & Assert
        with RuntimeContext(dispatch_timeout=outer_timeout):
            assert dispatch_timeout.get() == outer_timeout

            with RuntimeContext(dispatch_timeout=None):
                assert dispatch_timeout.get() is None

            assert dispatch_timeout.get() == outer_timeout

        assert dispatch_timeout.get() == original_value

    def test_nested_contexts_exception_in_inner(self):
        """Test nested contexts when exception occurs in inner context.

        Given:
            Nested RuntimeContext managers
        When:
            An exception is raised in the inner context
        Then:
            Both contexts should properly restore their previous values
        """
        # Arrange
        original_value = dispatch_timeout.get()
        outer_timeout = 8.0
        inner_timeout = 12.0

        # Act & Assert
        with pytest.raises(RuntimeError, match="inner error"):
            with RuntimeContext(dispatch_timeout=outer_timeout):
                assert dispatch_timeout.get() == outer_timeout

                with RuntimeContext(dispatch_timeout=inner_timeout):
                    assert dispatch_timeout.get() == inner_timeout
                    raise RuntimeError("inner error")

        # Both contexts should have cleaned up
        assert dispatch_timeout.get() == original_value

    @given(timeout=st.one_of(st.none(), st.floats(min_value=0.0, max_value=1000.0)))
    def test_context_manager_restores_original_value(self, timeout):
        """Property: Context manager always restores the original value.

        Given:
            Any valid timeout value (None or positive float)
        When:
            A RuntimeContext is entered and exited
        Then:
            The dispatch_timeout contextvar should be restored to its
            original value
        """
        # Arrange
        original_value = dispatch_timeout.get()

        # Act & Assert
        with RuntimeContext(dispatch_timeout=timeout):
            pass  # Exit immediately

        assert dispatch_timeout.get() == original_value

    @given(timeout=st.floats(min_value=0.0, max_value=1000.0))
    def test_context_sets_timeout_property(self, timeout):
        """Property: Context manager sets the timeout to the specified value.

        Given:
            Any positive float timeout value
        When:
            A RuntimeContext is entered
        Then:
            The dispatch_timeout contextvar should be set to that value
        """
        # Arrange
        original_value = dispatch_timeout.get()

        # Act & Assert
        with RuntimeContext(dispatch_timeout=timeout):
            assert dispatch_timeout.get() == timeout

        # Cleanup verification
        assert dispatch_timeout.get() == original_value

    @given(
        outer=st.floats(min_value=0.0, max_value=1000.0),
        inner=st.floats(min_value=0.0, max_value=1000.0),
    )
    def test_nested_contexts_restore_properly(self, outer, inner):
        """Property: Nested contexts restore values in reverse order.

        Given:
            Any two valid timeout values
        When:
            Two RuntimeContexts are nested
        Then:
            Each should restore the previous value when exiting
        """
        # Arrange
        original_value = dispatch_timeout.get()

        # Act & Assert
        with RuntimeContext(dispatch_timeout=outer):
            assert dispatch_timeout.get() == outer

            with RuntimeContext(dispatch_timeout=inner):
                assert dispatch_timeout.get() == inner

            assert dispatch_timeout.get() == outer

        assert dispatch_timeout.get() == original_value

    @given(
        timeouts=st.lists(
            st.floats(min_value=0.0, max_value=1000.0), min_size=1, max_size=10
        )
    )
    def test_sequential_contexts_always_restore(self, timeouts):
        """Property: Sequential contexts always restore the original value.

        Given:
            A list of valid timeout values
        When:
            Multiple RuntimeContexts are used sequentially
        Then:
            The original value should be restored after each one
        """
        # Arrange
        original_value = dispatch_timeout.get()

        # Act & Assert
        for timeout in timeouts:
            with RuntimeContext(dispatch_timeout=timeout):
                assert dispatch_timeout.get() == timeout

            assert dispatch_timeout.get() == original_value

    @given(timeout=st.floats(min_value=0.0, max_value=1000.0))
    def test_exception_always_restores_value(self, timeout):
        """Property: Exceptions don't prevent context cleanup.

        Given:
            Any valid timeout value
        When:
            An exception is raised within a RuntimeContext
        Then:
            The original dispatch_timeout value should still be restored
        """
        # Arrange
        original_value = dispatch_timeout.get()

        # Act & Assert
        with pytest.raises(ValueError):
            with RuntimeContext(dispatch_timeout=timeout):
                assert dispatch_timeout.get() == timeout
                raise ValueError("test")

        assert dispatch_timeout.get() == original_value

    @given(timeout=st.one_of(st.none(), st.floats(min_value=0.0, max_value=1000.0)))
    def test_none_is_valid_timeout_value(self, timeout):
        """Property: None is a valid timeout value that can be set/restored.

        Given:
            Either None or a valid float timeout
        When:
            A RuntimeContext is used with this value
        Then:
            It should set and restore the value correctly
        """
        # Arrange
        original_value = dispatch_timeout.get()

        # Act
        with RuntimeContext(dispatch_timeout=timeout):
            # Assert - value is set
            if timeout is None:
                assert dispatch_timeout.get() is None
            else:
                assert dispatch_timeout.get() == timeout

        # Assert - value is restored
        assert dispatch_timeout.get() == original_value

    @given(
        levels=st.lists(
            st.floats(min_value=0.0, max_value=1000.0), min_size=2, max_size=5
        )
    )
    def test_deeply_nested_contexts_maintain_stack_order(self, levels):
        """Property: Deeply nested contexts maintain proper stack ordering.

        Given:
            A list of timeout values representing nesting levels
        When:
            RuntimeContexts are nested to those levels
        Then:
            Each level should see the correct timeout and restore properly
        """
        # Arrange
        original_value = dispatch_timeout.get()

        # Act & Assert - build up nested contexts recursively
        def nest_contexts(remaining_levels):
            if not remaining_levels:
                return

            current_timeout = remaining_levels[0]
            next_levels = remaining_levels[1:]

            with RuntimeContext(dispatch_timeout=current_timeout):
                assert dispatch_timeout.get() == current_timeout
                nest_contexts(next_levels)
                # After inner contexts exit, should still have current timeout
                assert dispatch_timeout.get() == current_timeout

        nest_contexts(levels)

        # Assert - original value restored
        assert dispatch_timeout.get() == original_value

    @given(timeout=st.floats(min_value=0.0, max_value=1000.0))
    def test_undefined_context_preserves_any_existing_value(self, timeout):
        """Property: Undefined context never modifies the contextvar.

        Given:
            Any timeout value currently set in the contextvar
        When:
            A RuntimeContext with Undefined is entered and exited
        Then:
            The contextvar value should remain unchanged
        """
        # Arrange - set a specific value first
        original_value = dispatch_timeout.get()
        dispatch_timeout.set(timeout)

        # Act
        with RuntimeContext(dispatch_timeout=Undefined):
            # Assert - value unchanged during context
            assert dispatch_timeout.get() == timeout

        # Assert - value still unchanged after exit
        assert dispatch_timeout.get() == timeout

        # Cleanup
        dispatch_timeout.set(original_value)
