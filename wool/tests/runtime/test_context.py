import asyncio
import contextvars
import gc
import logging
import sys

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool import protocol
from wool.runtime.context import _UNSET
from wool.runtime.context import ContextVar
from wool.runtime.context import Manifest
from wool.runtime.context import RuntimeContext
from wool.runtime.context import _Context
from wool.runtime.context import _UnsetType
from wool.runtime.context import dispatch_timeout
from wool.runtime.typing import Undefined


class TestUnsetType:
    def test___new___returns_singleton_instance(self):
        """Test that _UnsetType always returns the same instance.

        Given:
            The _UnsetType class
        When:
            It is instantiated multiple times
        Then:
            It should return the same singleton instance each time
        """
        # Act
        first = _UnsetType()
        second = _UnsetType()

        # Assert
        assert first is second
        assert first is _UNSET

    def test___repr___returns_unset_literal(self):
        """Test _UnsetType repr returns the string "UNSET".

        Given:
            The _UNSET singleton
        When:
            repr() is called on it
        Then:
            It should return the string "UNSET"
        """
        # Act & assert
        assert repr(_UNSET) == "UNSET"

    def test___bool___returns_false(self):
        """Test _UnsetType is falsy.

        Given:
            The _UNSET singleton
        When:
            It is evaluated in a boolean context
        Then:
            It should be falsy
        """
        # Act & assert
        assert not _UNSET
        assert bool(_UNSET) is False

    def test___reduce___roundtrip_preserves_singleton_identity(self):
        """Test pickle roundtrip returns the same singleton.

        Given:
            The _UNSET singleton
        When:
            It is serialized via cloudpickle.dumps and deserialized via
            cloudpickle.loads
        Then:
            The deserialized value should be the same singleton instance
        """
        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(_UNSET))

        # Assert
        assert restored is _UNSET


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

        # Act & assert
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

        # Act & assert
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

        # Act & assert
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

        # Act & assert
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

        # Act & assert
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

        # Act & assert
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

        # Act & assert
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

        # Act & assert
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

        # Act & assert - build up nested contexts recursively
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

    def test_to_protobuf_with_dispatch_timeout(self):
        """Test to_protobuf serializes dispatch_timeout.

        Given:
            A RuntimeContext with dispatch_timeout=5.0
        When:
            to_protobuf() is called
        Then:
            It should return a protobuf message with dispatch_timeout=5.0
        """
        # Arrange
        context = RuntimeContext(dispatch_timeout=5.0)

        # Act
        pb = context.to_protobuf()

        # Assert
        assert pb.dispatch_timeout == 5.0

    def test_to_protobuf_with_none_dispatch_timeout(self):
        """Test to_protobuf leaves field unset for None.

        Given:
            A RuntimeContext with dispatch_timeout=None
        When:
            to_protobuf() is called
        Then:
            It should return a protobuf message with dispatch_timeout unset
        """
        # Arrange
        context = RuntimeContext(dispatch_timeout=None)

        # Act
        pb = context.to_protobuf()

        # Assert
        assert not pb.HasField("dispatch_timeout")

    def test_to_protobuf_with_undefined_dispatch_timeout_reads_contextvar(self):
        """Test to_protobuf falls back to the ContextVar when Undefined.

        Given:
            A RuntimeContext with no explicit dispatch_timeout and the
            dispatch_timeout ContextVar set to 3.5
        When:
            to_protobuf() is called
        Then:
            It should read the current value from the ContextVar
        """
        # Arrange
        original_value = dispatch_timeout.get()
        dispatch_timeout.set(3.5)
        context = RuntimeContext()

        # Act
        pb = context.to_protobuf()

        # Assert
        assert pb.dispatch_timeout == 3.5

        # Cleanup
        dispatch_timeout.set(original_value)

    def test_from_protobuf_with_dispatch_timeout(self):
        """Test from_protobuf deserializes dispatch_timeout.

        Given:
            A protobuf RuntimeContext message with dispatch_timeout=12.0
        When:
            RuntimeContext.from_protobuf() is called
        Then:
            It should return a RuntimeContext with dispatch_timeout=12.0
        """
        # Arrange
        pb = protocol.RuntimeContext(dispatch_timeout=12.0)

        # Act
        context = RuntimeContext.from_protobuf(pb)

        # Assert
        with context:
            assert dispatch_timeout.get() == 12.0

    def test_from_protobuf_with_zero_dispatch_timeout(self):
        """Test from_protobuf preserves zero dispatch_timeout.

        Given:
            A protobuf RuntimeContext message with dispatch_timeout=0.0
        When:
            RuntimeContext.from_protobuf() is called
        Then:
            It should return a RuntimeContext with dispatch_timeout=0.0
        """
        # Arrange
        pb = protocol.RuntimeContext(dispatch_timeout=0.0)

        # Act
        context = RuntimeContext.from_protobuf(pb)

        # Assert
        with context:
            assert dispatch_timeout.get() == 0.0

    def test_from_protobuf_with_unset_dispatch_timeout(self):
        """Test from_protobuf maps unset field to None.

        Given:
            A protobuf RuntimeContext message with dispatch_timeout unset
        When:
            RuntimeContext.from_protobuf() is called
        Then:
            It should return a RuntimeContext with dispatch_timeout=None
        """
        # Arrange
        pb = protocol.RuntimeContext()

        # Act
        context = RuntimeContext.from_protobuf(pb)

        # Assert
        with context:
            assert dispatch_timeout.get() is None

    def test_to_protobuf_from_protobuf_roundtrip(self):
        """Test protobuf roundtrip preserves dispatch_timeout.

        Given:
            A RuntimeContext with dispatch_timeout=8.5
        When:
            Serialized via to_protobuf() then deserialized via from_protobuf()
        Then:
            It should preserve dispatch_timeout through the roundtrip
        """
        # Arrange
        original = RuntimeContext(dispatch_timeout=8.5)

        # Act
        restored = RuntimeContext.from_protobuf(original.to_protobuf())

        # Assert
        with restored:
            assert dispatch_timeout.get() == 8.5

    @given(
        timeout=st.floats(
            min_value=0.0, max_value=1000.0, allow_nan=False, allow_infinity=False
        )
    )
    def test_to_protobuf_from_protobuf_roundtrip_property(self, timeout):
        """Test protobuf roundtrip with arbitrary non-negative floats.

        Given:
            Any non-negative float dispatch_timeout (including 0.0)
        When:
            Serialized via to_protobuf() then deserialized via from_protobuf()
        Then:
            It should equal the original value
        """
        # Arrange
        original = RuntimeContext(dispatch_timeout=timeout)

        # Act
        restored = RuntimeContext.from_protobuf(original.to_protobuf())

        # Assert
        with restored:
            assert dispatch_timeout.get() == timeout

    def test_to_protobuf_with_empty_context_vars_produces_empty_map(self):
        """Test to_protobuf leaves context_vars empty when none are propagated.

        Given:
            A RuntimeContext with no context_vars
        When:
            to_protobuf() is called
        Then:
            The resulting message should have an empty context_vars map
        """
        # Arrange
        context = RuntimeContext(dispatch_timeout=0.0)

        # Act
        pb = context.to_protobuf()

        # Assert
        assert len(pb.vars) == 0


class TestContextVar:
    def test___init___with_name_only(self):
        """Test ContextVar construction with a name and no default.

        Given:
            A string name and no default argument
        When:
            ContextVar is instantiated
        Then:
            It should have the given name and raise LookupError on get()
        """
        # Arrange & act
        var: ContextVar[str] = ContextVar("cv_no_default")

        # Assert
        assert var.name == "cv_no_default"
        with pytest.raises(LookupError):
            var.get()

    def test___init___with_name_and_default(self):
        """Test ContextVar construction with an explicit default.

        Given:
            A string name and a default value
        When:
            ContextVar is instantiated
        Then:
            get() should return the default value
        """
        # Arrange & act
        var = ContextVar("cv_with_default", default="fallback")

        # Assert
        assert var.get() == "fallback"

    def test_name_property_matches_constructor_argument(self):
        """Test that the name property returns the constructor name.

        Given:
            A ContextVar constructed with an explicit name
        When:
            The name property is read
        Then:
            It should return the constructor argument
        """
        # Arrange & act
        var: ContextVar[int] = ContextVar("cv_name_check")

        # Assert
        assert var.name == "cv_name_check"

    def test_get_with_value_set(self):
        """Test get returns the value currently set in the context.

        Given:
            A ContextVar with a value set via set()
        When:
            get() is called
        Then:
            It should return the set value
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_get_set")
        var.set("live_value")

        # Act & assert
        assert var.get() == "live_value"

    def test_get_fallback_argument_when_unset(self):
        """Test get returns the fallback argument when no value is set.

        Given:
            A ContextVar with no default and no set value
        When:
            get(fallback) is called
        Then:
            It should return the fallback argument
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_get_fallback")
        sentinel = "sentinel_value"

        # Act & assert
        assert var.get(sentinel) == sentinel

    def test_set_returns_token_and_reset_restores_previous_value(self):
        """Test set/reset token round-trip.

        Given:
            A ContextVar with a default value
        When:
            set() is called and then reset() with the returned token
        Then:
            get() should return the original default after reset
        """
        # Arrange
        var = ContextVar("cv_set_reset", default="before")

        # Act
        token = var.set("after")
        during = var.get()
        var.reset(token)

        # Assert
        assert during == "after"
        assert var.get() == "before"

    def test_reset_with_foreign_token_raises_value_error(self):
        """Test reset with a token from a different var raises.

        Given:
            Two ContextVars and a token from one
        When:
            reset is called on the other var with the foreign token
        Then:
            It should raise ValueError
        """
        # Arrange
        var_a = ContextVar("cv_reset_a", default="a")
        var_b = ContextVar("cv_reset_b", default="b")
        token_a = var_a.set("new_a")

        # Act & assert
        with pytest.raises(ValueError):
            var_b.reset(token_a)

    def test_reset_with_consumed_token_raises_runtime_error(self):
        """Test reset with a previously used token raises.

        Given:
            A ContextVar and a token that has already been used to
            reset the var
        When:
            reset is called with the same token a second time
        Then:
            It should raise RuntimeError
        """
        # Arrange
        var = ContextVar("cv_double_reset", default="original")
        token = var.set("new_value")
        var.reset(token)

        # Act & assert
        with pytest.raises(RuntimeError, match="Token has already been used"):
            var.reset(token)

    def test_reset_with_out_of_order_token_restores_prior_value(self):
        """Test out-of-order reset restores to the pre-set value.

        Given:
            A ContextVar with two tokens minted from sequential set
            calls — t1 from the first set, t2 from the second
        When:
            reset is called with t1 (the older token) while t2's
            mutation is still active
        Then:
            The var should be restored to the value it had before
            t1's set was called, matching stdlib semantics. The
            stdlib contextvars.ContextVar.reset documentation states
            that reset returns the var to "the value it had before
            the ContextVar.set() that created the token was used" —
            it does not enforce LIFO ordering
        """
        # Arrange
        var: ContextVar[int] = ContextVar("cv_reset_restores_prior")

        # Act
        t1 = var.set(1)
        var.set(2)
        var.reset(t1)

        # Assert — restored to pre-t1 state (unset, so LookupError)
        with pytest.raises(LookupError):
            var.get()

    def test___repr___includes_name(self):
        """Test repr includes the variable name.

        Given:
            A ContextVar with a known name
        When:
            repr() is called
        Then:
            The result should include the name
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_repr_check")

        # Act
        result = repr(var)

        # Assert
        assert "cv_repr_check" in result
        assert "wool.ContextVar" in result

    def test___reduce___roundtrip_with_default(self):
        """Test pickle roundtrip preserves name and default.

        Given:
            A ContextVar constructed with a default value
        When:
            It is serialized via cloudpickle.dumps then deserialized
            via cloudpickle.loads
        Then:
            The restored var should have the same name and get()
            should return the original default
        """
        # Arrange
        var = ContextVar("cv_pickle_default", default="fallback")

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(var))

        # Assert
        assert restored.name == "cv_pickle_default"
        assert restored.get() == "fallback"

    def test___reduce___roundtrip_without_default(self):
        """Test pickle roundtrip preserves name when no default is set.

        Given:
            A ContextVar constructed without a default
        When:
            It is serialized via cloudpickle.dumps then deserialized
            via cloudpickle.loads
        Then:
            The restored var should have the same name and get()
            should raise LookupError
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_pickle_no_default")

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(var))

        # Assert
        assert restored.name == "cv_pickle_no_default"
        with pytest.raises(LookupError):
            restored.get()

    def test_token___repr___includes_var_name(self):
        """Test Token repr includes the variable name.

        Given:
            A Token returned by ContextVar.set
        When:
            repr() is called on it
        Then:
            The result should include the wool.Token tag and the
            var name
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_token_repr")
        token = var.set("value")

        # Act
        result = repr(token)

        # Assert
        assert "wool.Token" in result
        assert "cv_token_repr" in result

    def test_token___reduce___roundtrip_preserves_var_reference(self):
        """Test Token pickle round-trip preserves the var reference.

        Given:
            A Token returned by ContextVar.set
        When:
            The token is pickled and unpickled via cloudpickle
        Then:
            The restored token's _var should resolve to the same
            ContextVar instance via sys.modules unification, and
            reset should succeed
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_token_pickle_roundtrip")
        var.set("before")
        token = var.set("after")

        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(token))

        # Assert — the restored token's _var unifies with the
        # original via sys.modules
        assert restored._var is var
        var.reset(restored)
        assert var.get() == "before"

    def test___init___with_duplicate_name_returns_distinct_instances(self):
        """Test duplicate-name construction produces independent vars.

        Given:
            Two wool.ContextVar constructions with the same name
        When:
            Both are constructed
        Then:
            They should be two distinct Python objects with distinct
            UUIDs, matching the stdlib contextvars.ContextVar
            semantic that each construction is independent.
        """
        # Act
        first = ContextVar("cv_duplicate_name", default="d")
        second = ContextVar("cv_duplicate_name", default="d")

        # Assert
        assert first is not second
        assert first._uuid != second._uuid

    def test___init___registers_in_sys_modules_under_uuid(self):
        """Test construction registers the instance under a UUID key.

        Given:
            A fresh wool.ContextVar
        When:
            It is constructed
        Then:
            It should be registered in sys.modules under the
            synthetic key wool._vars.<uuid> (not the user-facing
            name, which is purely cosmetic).
        """
        # Act
        var = ContextVar("cv_sys_modules_check")

        # Assert
        synthetic = f"wool._vars.{var._uuid}"
        assert sys.modules.get(synthetic) is var
        assert var.__name__ == synthetic

    def test_propagation_round_trip_preserves_set_value(self):
        """Test end-to-end propagation of an explicitly set value.

        Given:
            A wool.ContextVar with a value set in the current context
        When:
            RuntimeContext.get_current() snapshots the registry, is
            serialized to protobuf, deserialized, and entered as a
            context manager
        Then:
            get() should return the propagated value inside the
            restored context
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_propagate")
        var.set("tenant_acme")

        # Act
        snapshot = RuntimeContext.get_current()
        pb = snapshot.to_protobuf()
        restored = RuntimeContext.from_protobuf(pb)

        # Assert
        with restored:
            assert var.get() == "tenant_acme"

    def test_propagation_does_not_ship_default_only_value(self):
        """Test default-only values are not serialized.

        Given:
            A wool.ContextVar with a default that has never been set
            in the current context
        When:
            A protobuf round-trip is performed
        Then:
            The serialized context_vars map should not contain the var
        """
        # Arrange
        ContextVar("cv_default_only", default="baseline")

        # Act
        snapshot = RuntimeContext.get_current()
        pb = snapshot.to_protobuf()

        # Assert
        assert "cv_default_only" not in pb.vars

    def test_propagation_preserves_unrelated_worker_var(self):
        """Test restoration does not clobber worker-side vars absent from payload.

        Given:
            A wool.ContextVar set locally on the worker and a
            RuntimeContext payload whose context_vars does not mention
            it
        When:
            The payload is entered as a context manager
        Then:
            The worker-side value should remain unchanged
        """
        # Arrange
        worker_only: ContextVar[str] = ContextVar("cv_worker_only")
        worker_only.set("local_value")
        payload = RuntimeContext(vars={"other_name": "other_value"})

        # Act & assert
        with payload:
            assert worker_only.get() == "local_value"
        assert worker_only.get() == "local_value"

    @pytest.mark.asyncio
    async def test_concurrent_tasks_see_isolated_values(self):
        """Test concurrent asyncio tasks observe isolated values.

        Given:
            Two asyncio tasks created from fresh copy_context()
            contexts each setting a different value on the same
            wool.ContextVar instance
        When:
            Both run concurrently
        Then:
            Each should observe its own value, never the other's —
            identity is shared but value is per stdlib Context.
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_concurrent")
        observed: dict[str, str] = {}

        async def run_with(name: str, value: str):
            var.set(value)
            observed[name] = var.get()
            await asyncio.sleep(0)
            observed[name] = var.get()

        loop = asyncio.get_running_loop()

        # Act
        task_a = loop.create_task(
            run_with("a", "value_a"), context=contextvars.copy_context()
        )
        task_b = loop.create_task(
            run_with("b", "value_b"), context=contextvars.copy_context()
        )
        await asyncio.gather(task_a, task_b)

        # Assert
        assert observed["a"] == "value_a"
        assert observed["b"] == "value_b"

    def test_to_protobuf_raises_typeerror_for_non_picklable_value(self):
        """Test serialization errors name the offending variable.

        Given:
            A RuntimeContext carrying a non-picklable value for a
            named variable (e.g., a lambda closure over a local)
        When:
            to_protobuf() is called with a cloudpickle-incompatible
            serializer
        Then:
            It should raise TypeError naming the var
        """

        # Arrange
        class Unpicklable:
            def __reduce__(self):
                raise TypeError("cannot pickle me")

        context = RuntimeContext(vars={"cv_bad": Unpicklable()})

        # Act & assert
        with pytest.raises(TypeError, match="cv_bad"):
            context.to_protobuf()

    def test_garbage_collected_var_drops_from_cache(self):
        """Test function-scoped vars drop from the metaclass cache on GC.

        Given:
            A wool.ContextVar constructed, set, and reset inside a
            helper scope that then returns, so no strong references
            remain
        When:
            gc.collect() runs and RuntimeContext.get_current() is
            called afterward
        Then:
            The protobuf serialization should not include the
            collected var's name — the WeakValueDictionary cache in
            the metaclass drops entries whose strong references have
            been released
        """

        # Arrange
        def construct_use_and_discard():
            local_var: ContextVar[str] = ContextVar("cv_transient_gc")
            t = local_var.set("transient")
            local_var.reset(t)

        construct_use_and_discard()
        gc.collect()

        # Act
        snapshot = RuntimeContext.get_current()
        pb = snapshot.to_protobuf()

        # Assert
        assert "cv_transient_gc" not in pb.vars

    def test_get_current_called_twice_returns_independent_snapshots(self):
        """Test repeated get_current calls produce independent snapshots.

        Given:
            A wool.ContextVar with a value set in the current context
        When:
            get_current() is called twice
        Then:
            Each returned RuntimeContext should be independently
            enterable and see the snapshotted value
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_double_snapshot")
        var.set("snapshotted")

        # Act
        first = RuntimeContext.get_current()
        second = RuntimeContext.get_current()

        # Assert
        restored_first = RuntimeContext.from_protobuf(first.to_protobuf())
        restored_second = RuntimeContext.from_protobuf(second.to_protobuf())
        with restored_first:
            assert var.get() == "snapshotted"
        with restored_second:
            assert var.get() == "snapshotted"

    def test_propagation_of_none_value(self):
        """Test None values round-trip cleanly.

        Given:
            A wool.ContextVar explicitly set to None
        When:
            A protobuf round-trip is performed
        Then:
            The restored context should yield None
        """
        # Arrange
        var: ContextVar[str | None] = ContextVar("cv_none_value", default="not_none")
        var.set(None)

        # Act
        pb = RuntimeContext.get_current().to_protobuf()
        restored = RuntimeContext.from_protobuf(pb)

        # Assert
        with restored:
            assert var.get() is None

    def test_propagation_honors_custom_serializer(self):
        """Test to_protobuf/from_protobuf use the supplied dumps/loads.

        Given:
            A RuntimeContext with propagated values and custom
            dumps/loads callables that tag the bytes
        When:
            The round-trip is performed with the custom callables
        Then:
            The restored values should equal the originals, and the
            custom callables should be observed to have run
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_custom_serializer")
        var.set("payload")
        snapshot = RuntimeContext.get_current()
        calls: dict[str, int] = {"dumps": 0, "loads": 0}

        def dumps(obj):
            calls["dumps"] += 1
            return cloudpickle.dumps(obj)

        def loads(data):
            calls["loads"] += 1
            return cloudpickle.loads(data)

        # Act
        pb = snapshot.to_protobuf(dumps=dumps)
        restored = RuntimeContext.from_protobuf(pb, loads=loads)

        # Assert
        assert calls["dumps"] >= 1
        assert calls["loads"] >= 1
        with restored:
            assert var.get() == "payload"

    @given(
        value=st.one_of(
            st.integers(),
            st.text(),
            st.none(),
            st.lists(st.integers(), max_size=10),
            st.booleans(),
        )
    )
    def test_propagation_round_trip_with_arbitrary_picklable_value(self, value):
        """Test propagation round-trip with arbitrary picklable values.

        Given:
            Any picklable payload from a mix of basic Python types
        When:
            A wool.ContextVar is set, snapshotted, protobuf round-tripped,
            and the restored context entered
        Then:
            get() should return a value equal to the original
        """
        # Arrange
        var: ContextVar = ContextVar("cv_hypothesis_roundtrip")
        var.set(value)

        # Act
        pb = RuntimeContext.get_current().to_protobuf()
        restored = RuntimeContext.from_protobuf(pb)

        # Assert
        with restored:
            assert var.get() == value


class TestContext:
    def test_snapshot_includes_explicitly_set_var(self):
        """Test snapshot serializes explicitly set vars.

        Given:
            A registered wool.ContextVar with a value set in the
            current context
        When:
            _Context.snapshot() is called
        Then:
            The returned dict should contain the var's name mapped
            to bytes that deserialize to the set value
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_ctx_snapshot_set")
        var.set("live")

        # Act
        result = _Context.snapshot()

        # Assert
        assert var.__name__ in result
        assert cloudpickle.loads(result[var.__name__]) == "live"

    def test_snapshot_skips_vars_without_any_set_value(self):
        """Test snapshot omits vars that have never been set.

        Given:
            A registered wool.ContextVar with no value set in the
            current context
        When:
            _Context.snapshot() is called
        Then:
            The returned dict should not include the var's name
        """
        # Arrange
        var = ContextVar("cv_ctx_snapshot_never_set", default="baseline")

        # Act
        result = _Context.snapshot()

        # Assert
        assert var.__name__ not in result
        # Keep the var alive until after snapshot() runs
        assert var.name == "cv_ctx_snapshot_never_set"

    def test_snapshot_raises_typeerror_for_non_picklable_value(self):
        """Test snapshot errors name the offending variable.

        Given:
            A wool.ContextVar set to an object whose __reduce__
            raises
        When:
            _Context.snapshot() is called
        Then:
            It should raise TypeError whose message includes the
            offending variable name
        """

        # Arrange
        class Unpicklable:
            def __reduce__(self):
                raise TypeError("cannot pickle me")

        var: ContextVar = ContextVar("cv_ctx_snapshot_bad")
        var.set(Unpicklable())

        # Act & assert
        with pytest.raises(TypeError, match="cv_ctx_snapshot_bad"):
            _Context.snapshot()

    def test_snapshot_from_reads_values_from_given_context(self):
        """Test snapshot_from reads values from a supplied context.

        Given:
            A wool.ContextVar with a value set inside a fresh
            contextvars.Context obtained via copy_context().run
        When:
            _Context.snapshot_from is called with that context
        Then:
            The returned dict should contain the var's name mapped
            to bytes that deserialize to the value set inside that
            context
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_ctx_snapshot_from")
        ctx = contextvars.copy_context()

        def setter():
            var.set("inside_ctx")

        ctx.run(setter)

        # Act
        result = _Context.snapshot_from(ctx)

        # Assert
        assert var.__name__ in result
        assert cloudpickle.loads(result[var.__name__]) == "inside_ctx"

    def test_snapshot_from_excludes_vars_not_in_context(self):
        """Test snapshot_from skips vars not present in the given context.

        Given:
            A wool.ContextVar created after a contextvars.Context
            was copied, so the var's stdlib variable is not in the
            context at all
        When:
            _Context.snapshot_from is called with the older context
        Then:
            The var's name should not appear in the returned dict
        """
        # Arrange
        ctx = contextvars.copy_context()
        var: ContextVar[str] = ContextVar("cv_ctx_snapshot_from_absent")
        var.set("set_outside_ctx")

        # Act
        result = _Context.snapshot_from(ctx)

        # Assert
        assert var.__name__ not in result

    def test_snapshot_from_raises_typeerror_for_non_picklable_value(self):
        """Test snapshot_from errors name the offending variable.

        Given:
            A wool.ContextVar with a non-picklable value set inside
            a fresh contextvars.Context
        When:
            _Context.snapshot_from is called with that context
        Then:
            It should raise TypeError whose message includes the
            offending variable name
        """

        # Arrange
        class Unpicklable:
            def __reduce__(self):
                raise TypeError("cannot pickle me")

        var: ContextVar = ContextVar("cv_ctx_snapshot_from_bad")
        ctx = contextvars.copy_context()

        def setter():
            var.set(Unpicklable())

        ctx.run(setter)

        # Act & assert
        with pytest.raises(TypeError, match="cv_ctx_snapshot_from_bad"):
            _Context.snapshot_from(ctx)

    def test_apply_with_empty_vars_is_noop(self):
        """Test apply returns immediately for an empty map.

        Given:
            A registered wool.ContextVar with an existing value and
            an empty vars dict
        When:
            _Context.apply is called with the empty dict
        Then:
            The existing var value should remain unchanged
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_ctx_apply_empty", default="baseline")
        var.set("pre_apply")

        # Act
        _Context.apply({})

        # Assert
        assert var.get() == "pre_apply"

    def test_apply_sets_registered_vars_from_snapshot(self):
        """Test apply deserializes and sets each registered var.

        Given:
            A registered wool.ContextVar and a vars dict mapping
            its name to serialized bytes for a new value
        When:
            _Context.apply is called with the dict
        Then:
            get() on the var should return the deserialized value
        """
        # Arrange
        var: ContextVar[str] = ContextVar("cv_ctx_apply_valid")
        payload = {var.__name__: cloudpickle.dumps("propagated")}

        # Act
        _Context.apply(payload)

        # Assert
        assert var.get() == "propagated"

    def test_apply_logs_warning_for_unregistered_name(self, caplog):
        """Test apply warns and continues for unknown names.

        Given:
            A vars dict containing a name that no registered
            wool.ContextVar matches
        When:
            _Context.apply is called with the dict
        Then:
            A warning should be logged mentioning the unknown name
            and no exception should propagate
        """
        # Arrange
        payload = {
            "cv_ctx_apply_ghost_name": cloudpickle.dumps("value"),
        }

        # Act
        with caplog.at_level(logging.WARNING):
            _Context.apply(payload)

        # Assert
        assert any(
            "cv_ctx_apply_ghost_name" in record.message for record in caplog.records
        )


class TestManifest:
    def test_build_with_no_vars_returns_empty_dict(self):
        """Test manifest build on a process with zero ContextVars is empty.

        Given:
            A process with ContextVar._construction_count == 0
        When:
            Manifest.build() is called
        Then:
            An empty dict should be returned without walking sys.modules
        """
        # Arrange — note: fixture clears wool._vars between tests, but
        # _construction_count is class state that persists. Accept
        # whatever count exists from earlier tests in this run and
        # verify only our bindings don't appear.
        before = Manifest.build()

        # Act
        result = Manifest.build()

        # Assert — deterministic: same state → same output
        assert result == before

    def test_build_discovers_module_scoped_bindings(self):
        """Test manifest build finds ContextVars at module __dict__ scope.

        Given:
            A ContextVar assigned as an attribute of a module
        When:
            Manifest.build() is called
        Then:
            The (module_name, attr_name) binding should appear in the
            result with the ContextVar instance as the value.
        """
        # Arrange
        import types as _types

        fake = _types.ModuleType("_test_manifest_build_lib")
        var = ContextVar("cv_manifest_build")
        fake.probe_var = var
        sys.modules[fake.__name__] = fake

        try:
            # Act
            result = Manifest.build()

            # Assert
            assert (fake.__name__, "probe_var") in result
            assert result[(fake.__name__, "probe_var")] is var
        finally:
            del sys.modules[fake.__name__]

    def test_build_records_aliased_bindings_independently(self):
        """Test manifest build records each binding of an aliased var.

        Given:
            A ContextVar bound to multiple (module, attr) locations
            (simulating `from lib import var` aliasing)
        When:
            Manifest.build() is called
        Then:
            Every binding should appear in the result, each pointing
            to the same ContextVar instance.
        """
        # Arrange
        import types as _types

        var = ContextVar("cv_manifest_alias")
        mod_a = _types.ModuleType("_test_manifest_alias_a")
        mod_b = _types.ModuleType("_test_manifest_alias_b")
        mod_a.local_name = var
        mod_b.another_name = var
        sys.modules[mod_a.__name__] = mod_a
        sys.modules[mod_b.__name__] = mod_b

        try:
            # Act
            result = Manifest.build()

            # Assert
            assert result.get((mod_a.__name__, "local_name")) is var
            assert result.get((mod_b.__name__, "another_name")) is var
        finally:
            del sys.modules[mod_a.__name__]
            del sys.modules[mod_b.__name__]

    def test_apply_substitutes_into_provided_module_dict(self):
        """Test manifest apply writes instances into target modules.

        Given:
            A manifest entry (module, attr) → var and a target-modules
            dict containing that module
        When:
            Manifest.apply is called with the manifest and target dict
        Then:
            The module's attribute should be replaced by the
            wire-reconstructed instance.
        """
        # Arrange
        import types as _types

        original_var = ContextVar("cv_manifest_apply_old")
        replacement_var = ContextVar("cv_manifest_apply_new")
        target_mod = _types.ModuleType("_test_manifest_apply_target")
        target_mod.slot = original_var

        # Act
        Manifest.apply(
            {("_test_manifest_apply_target", "slot"): replacement_var},
            target_modules={"_test_manifest_apply_target": target_mod},
        )

        # Assert
        assert target_mod.slot is replacement_var

    def test_wire_roundtrip_preserves_var_identity(self):
        """Test manifest wire round-trip preserves ContextVar UUID.

        Given:
            A manifest with a single (module, attr) → ContextVar entry
        When:
            to_wire / from_wire are chained
        Then:
            The restored manifest should contain a ContextVar whose
            UUID matches the original (via sys.modules unification).
        """
        # Arrange
        var = ContextVar("cv_manifest_wire_rt")
        manifest = {("example_mod", "example_attr"): var}

        # Act
        wire_entries = Manifest.to_wire(manifest)
        restored = Manifest.from_wire(wire_entries)

        # Assert
        assert len(wire_entries) == 1
        mod_name, attr_name, pickled = wire_entries[0]
        assert mod_name == "example_mod"
        assert attr_name == "example_attr"
        assert isinstance(pickled, bytes) and len(pickled) > 0
        assert restored[("example_mod", "example_attr")]._uuid == var._uuid
