"""Unit tests for RuntimeContext — block-scoped runtime option overrides."""

import pytest

from wool import protocol
from wool.runtime.context.runtime import RuntimeContext
from wool.runtime.context.runtime import dispatch_timeout


class TestRuntimeContext:
    def test___enter___should_install_dispatch_timeout(self):
        """Test RuntimeContext.__enter__ installs an explicit dispatch timeout.

        Given:
            A RuntimeContext constructed with an explicit
            dispatch_timeout.
        When:
            It is entered as a context manager.
        Then:
            The ambient dispatch_timeout variable should report the
            supplied value for the duration of the block.
        """
        # Arrange
        context = RuntimeContext(dispatch_timeout=7.5)

        # Act & assert
        with context:
            assert dispatch_timeout.get() == 7.5

    def test___exit___should_restore_prior_dispatch_timeout(self):
        """Test RuntimeContext.__exit__ restores the prior dispatch timeout.

        Given:
            A RuntimeContext entered over an explicit dispatch_timeout.
        When:
            The context-manager block exits.
        Then:
            The ambient dispatch_timeout variable should revert to the
            value it held before the block.
        """
        # Arrange
        token = dispatch_timeout.set(2.0)

        # Act
        with RuntimeContext(dispatch_timeout=9.0):
            pass

        # Assert
        assert dispatch_timeout.get() == 2.0
        dispatch_timeout.reset(token)

    def test___enter___should_leave_dispatch_timeout_when_no_override(self):
        """Test a bare RuntimeContext does not touch the dispatch timeout.

        Given:
            A bare RuntimeContext() constructed without an explicit
            dispatch_timeout — the default sentinel.
        When:
            It is entered and exited as a context manager over a scope
            with a pre-set dispatch_timeout.
        Then:
            The ambient dispatch_timeout should be unchanged throughout
            — the no-override path skips setting the variable.
        """
        # Arrange
        token = dispatch_timeout.set(4.0)

        # Act & assert
        with RuntimeContext():
            assert dispatch_timeout.get() == 4.0
        assert dispatch_timeout.get() == 4.0
        dispatch_timeout.reset(token)

    def test_get_current_should_capture_live_dispatch_timeout(self):
        """Test RuntimeContext.get_current snapshots the live dispatch timeout.

        Given:
            A scope with the ambient dispatch_timeout set to a value.
        When:
            RuntimeContext.get_current is called.
        Then:
            The captured RuntimeContext should re-install that value
            when entered.
        """
        # Arrange
        token = dispatch_timeout.set(3.25)

        # Act
        captured = RuntimeContext.get_current()

        # Assert
        dispatch_timeout.reset(token)
        with captured:
            assert dispatch_timeout.get() == 3.25

    def test_from_protobuf_should_reconstruct_dispatch_timeout(self):
        """Test RuntimeContext.from_protobuf reconstructs an explicit timeout.

        Given:
            A protocol.RuntimeContext message carrying a
            dispatch_timeout field.
        When:
            RuntimeContext.from_protobuf is called on it.
        Then:
            Entering the reconstructed RuntimeContext should install
            the message's timeout value.
        """
        # Arrange
        message = protocol.RuntimeContext(dispatch_timeout=6.0)

        # Act
        context = RuntimeContext.from_protobuf(message)

        # Assert
        with context:
            assert dispatch_timeout.get() == 6.0

    def test_to_protobuf_should_omit_field_when_explicit_none(self):
        """Test RuntimeContext.to_protobuf omits the field for an explicit None.

        Given:
            A RuntimeContext constructed with an explicit
            dispatch_timeout of None.
        When:
            to_protobuf is called.
        Then:
            The message should not carry the dispatch_timeout field —
            an explicit None lets the receiver inherit its own scope's
            default.
        """
        # Arrange
        context = RuntimeContext(dispatch_timeout=None)

        # Act
        message = context.to_protobuf()

        # Assert
        assert message.HasField("dispatch_timeout") is False

    def test_to_protobuf_should_capture_live_value_when_default_sentinel(self):
        """Test RuntimeContext.to_protobuf captures the live timeout when unset.

        Given:
            A bare RuntimeContext() — the default sentinel — encoded
            inside a scope whose ambient dispatch_timeout is set.
        When:
            to_protobuf is called.
        Then:
            The message should carry the scope's live dispatch_timeout
            value — a bare RuntimeContext propagates the encoder's
            effective timeout to the receiver.
        """
        # Arrange
        token = dispatch_timeout.set(11.5)

        # Act
        message = RuntimeContext().to_protobuf()

        # Assert
        dispatch_timeout.reset(token)
        assert message.HasField("dispatch_timeout") is True
        assert message.dispatch_timeout == 11.5

    def test_to_protobuf_should_round_trip_explicit_timeout(self):
        """Test a RuntimeContext round-trips an explicit timeout through protobuf.

        Given:
            A RuntimeContext with an explicit dispatch_timeout.
        When:
            It is encoded with to_protobuf and decoded with
            from_protobuf.
        Then:
            Entering the decoded RuntimeContext should install the
            original timeout value.
        """
        # Arrange
        original = RuntimeContext(dispatch_timeout=8.0)

        # Act
        restored = RuntimeContext.from_protobuf(original.to_protobuf())

        # Assert
        with restored:
            assert dispatch_timeout.get() == 8.0

    def test_double_enter_should_raise(self):
        """Test re-entering an already-active RuntimeContext raises.

        Given:
            A RuntimeContext currently inside an active ``with``
            block.
        When:
            The same instance is entered as a context manager a
            second time.
        Then:
            It should raise RuntimeError — RuntimeContext instances
            are block-scoped and single-use.
        """
        # Arrange
        rc = RuntimeContext(dispatch_timeout=2.0)

        # Act & assert
        with rc:
            with pytest.raises(RuntimeError, match="already active"):
                rc.__enter__()
