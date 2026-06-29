import uuid
from contextvars import Token

import cloudpickle
import pytest

import wool
from tests.helpers import scoped_context
from wool.runtime.context.var import ContextVar

# ``dumps`` is the wool serializer — ContextVar serialises only through
# it. ``loads`` is plain ``cloudpickle.loads``: wool-encoded bytes are
# cloudpickle-loadable, so a separate wool deserializer is not needed
# here.
dumps = wool.__serializer__.dumps
loads = cloudpickle.loads


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class TestToken:
    def test___repr___should_reference_backing_contextvar(self):
        """Test Token repr references the backing ContextVar.

        Given:
            A Token produced by set() on a wool.ContextVar.
        When:
            repr() is called on it.
        Then:
            It should include the backing stdlib ContextVar's
            qualified name — wool encodes ``(namespace, name)`` into
            the backing's name so the repr names the originating
            wool variable in a stdlib-shaped form.
        """
        # Arrange
        var = ContextVar(_unique("repr_token_var"))

        # Act
        with scoped_context():
            token = var.set("x")
            text = repr(token)

        # Assert
        assert var.namespace in text
        assert var.name in text

    def test_old_value_should_report_missing_when_var_previously_unset(self):
        """Test Token.old_value is MISSING when the var was previously unset.

        Given:
            A previously-unset ContextVar.
        When:
            set() mints a Token and its old_value is read.
        Then:
            It should be Token.MISSING.
        """
        # Arrange
        var = ContextVar(_unique("old_value_missing"))

        # Act
        with scoped_context():
            token = var.set("x")

        # Assert
        assert token.old_value is Token.MISSING

    def test_old_value_should_report_prior_value_when_nested_set(self):
        """Test Token.old_value reports the prior value for a nested set.

        Given:
            A ContextVar already set to a value.
        When:
            A second set mints a Token whose old_value is read.
        Then:
            It should equal the first value.
        """
        # Arrange
        var = ContextVar(_unique("old_value_prior"))

        # Act
        with scoped_context():
            var.set("first")
            token = var.set("second")

        # Assert
        assert token.old_value == "first"

    def test_token_should_reject_pickle_cloudpickle_and_copy(self):
        """Test wool.Token rejects pickle, cloudpickle, copy.copy, and deepcopy.

        Given:
            A live wool.Token (stdlib contextvars.Token under the alias).
        When:
            pickle.dumps, cloudpickle.dumps, copy.copy, and
            copy.deepcopy are each invoked on it.
        Then:
            All four raise TypeError — stdlib :class:`contextvars.Token`
            is not picklable, and a chain-bound Token has no meaningful
            copy semantics. Cross-process Token transport is deferred;
            see issue #231.
        """
        # Arrange
        import copy as _copy
        import pickle

        var = ContextVar(_unique("token_reject_channels"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            for channel in (
                pickle.dumps,
                cloudpickle.dumps,
                _copy.copy,
                _copy.deepcopy,
            ):
                with pytest.raises(TypeError, match="cannot pickle"):
                    channel(token)


def test_token_should_pin_context_var_in_weak_registry():
    """Test a live Token keeps its backing variable reachable.

    Given:
        A Token minted by ContextVar.set, with no other strong
        reference to the wool variable.
    When:
        A garbage collection runs while the token is held.
    Then:
        The token's ``var`` attribute — the backing stdlib
        :class:`contextvars.ContextVar` whose qualified name encodes
        the wool ``(namespace, name)`` identity — should still
        resolve.
    """
    import gc

    # Arrange
    namespace = uuid.uuid4().hex
    with scoped_context():
        token = ContextVar("pinned_var", namespace=namespace).set("x")

    # Act
    gc.collect()

    # Assert — the backing carries the qualified key in its name.
    assert namespace in token.var.name
    assert "pinned_var" in token.var.name
