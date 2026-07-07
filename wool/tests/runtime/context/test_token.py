import copy
import pickle
import uuid

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from tests.helpers import scoped_context
from wool.runtime.context.token import Token
from wool.runtime.context.token import token_sink
from wool.runtime.context.var import ContextVar
from wool.runtime.typing import Undefined

# ``dumps`` is the wool serializer — a Token serialises only through it.
# ``loads`` is plain ``cloudpickle.loads``: wool-encoded bytes are
# cloudpickle-loadable, so a separate wool deserializer is not needed here.
dumps = wool.__serializer__.dumps
loads = cloudpickle.loads


# Sentinel distinguishing "the variable was never set" from a drawn prior
# value of None in the roundtrip property below.
_UNSET = object()


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class TestToken:
    def test___init___should_reject_direct_construction(self):
        """Test wool.Token cannot be constructed directly.

        Given:
            The public wool.Token type.
        When:
            It is instantiated directly rather than minted by set().
        Then:
            It should raise TypeError — mirroring contextvars.Token, which
            is likewise not user-constructible, so a forged token cannot
            bypass the single-use ledger.
        """
        # Arrange, act, & assert
        with pytest.raises(TypeError, match="can only be created"):
            wool.Token()

    def test___repr___should_name_the_wool_context_var(self):
        """Test Token repr names the wool ContextVar.

        Given:
            A Token produced by set() on a wool.ContextVar.
        When:
            repr() is called on it.
        Then:
            It should include the variable's name and namespace in a
            stdlib-shaped ``<Token var=<...> at 0x...>`` form.
        """
        # Arrange
        var = ContextVar(_unique("repr_token_var"))

        # Act
        with scoped_context():
            token = var.set("x")
            text = repr(token)

        # Assert
        assert "Token" in text
        assert var.namespace in text
        assert var.name in text

    def test___repr___should_flip_used_marker_when_consumed(self):
        """Test Token repr gains the used marker once the token is consumed.

        Given:
            A Token minted by set() on a wool.ContextVar.
        When:
            repr() is read before and after the token is consumed by
            reset().
        Then:
            It should show no used marker while the token is live and
            the stdlib-shaped ``<Token used var=`` marker after
            consumption.
        """
        # Arrange
        var = ContextVar(_unique("repr_flip"))

        # Act
        with scoped_context():
            token = var.set("x")
            before = repr(token)
            var.reset(token)
            after = repr(token)

        # Assert
        assert "used " not in before
        assert "<Token used var=" in after

    def test_var_should_be_the_wool_context_var(self):
        """Test Token.var is the wool ContextVar that minted it.

        Given:
            A ContextVar in an armed context.
        When:
            set() mints a Token and its ``var`` is read.
        Then:
            It should be the wool ContextVar itself, so ``var.set(x).var is
            var`` holds as it does in stdlib.
        """
        # Arrange
        var = ContextVar(_unique("token_var"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            assert token.var is var

    def test_old_value_should_report_undefined_when_var_previously_unset(self):
        """Test Token.old_value is Undefined when the var was previously unset.

        Given:
            A previously-unset ContextVar.
        When:
            set() mints a Token and its old_value is read.
        Then:
            It should be Wool's Undefined sentinel (not the stdlib
            contextvars.Token.MISSING).
        """
        # Arrange
        var = ContextVar(_unique("old_value_unset"))

        # Act
        with scoped_context():
            token = var.set("x")

        # Assert
        assert token.old_value is Undefined

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

    def test_old_value_should_be_undefined_when_set_follows_reset_to_unset(self):
        """Test old_value reports Undefined for a set after a reset to unset.

        Given:
            A ContextVar set once and then reset via its token,
            returning the variable to the unset state.
        When:
            The variable is set again, minting a new Token.
        Then:
            The new token's old_value should be the Undefined sentinel
            by identity — a reset-to-unset variable has no prior value.
        """
        # Arrange
        var = ContextVar(_unique("old_value_reset_unset"))

        # Act
        with scoped_context():
            first = var.set("x")
            var.reset(first)
            token = var.set("y")

        # Assert
        assert token.old_value is Undefined

    @pytest.mark.parametrize(
        "state",
        ["live", "used", "orphan"],
    )
    @pytest.mark.parametrize(
        "channel",
        [pickle.dumps, cloudpickle.dumps, copy.copy, copy.deepcopy],
        ids=["pickle", "cloudpickle", "copy", "deepcopy"],
    )
    def test_token_should_reject_vanilla_serialization(self, channel, state):
        """Test a Token rejects the vanilla channels in every lifecycle state.

        Given:
            A wool.Token that is live, already consumed by reset, or an
            orphan reconstituted from a wool-serializer roundtrip, and
            one of the vanilla serialization or copy channels — pickle,
            cloudpickle, copy.copy, or copy.deepcopy.
        When:
            The channel is invoked on the token outside a Wool dispatch.
        Then:
            It should raise TypeError pointing at Wool's dispatch path —
            arbitrary transport is disallowed in every token state even
            though dispatch transport is supported.
        """
        # Arrange
        var = ContextVar(_unique("token_reject_channels"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            if state == "used":
                var.reset(token)
            elif state == "orphan":
                token = loads(dumps(token))
            with pytest.raises(TypeError, match="Wool dispatch"):
                channel(token)

    def test_token_should_roundtrip_through_the_wool_serializer(self):
        """Test a Token survives serialization through the Wool serializer.

        Given:
            A live wool.Token.
        When:
            It is serialized with the Wool serializer and deserialized.
        Then:
            It should reconstruct as a Token whose var is the original
            ContextVar and whose old_value equals the original's — the
            dispatch-transport path the vanilla guard does not block.
        """
        # Arrange
        var = ContextVar(_unique("token_roundtrip"))

        # Act
        with scoped_context():
            token = var.set("x")
            restored = loads(dumps(token))

        # Assert
        assert isinstance(restored, Token)
        assert restored.var is var
        assert restored.old_value == token.old_value

    def test_old_value_should_be_undefined_identity_after_wire_roundtrip(self):
        """Test a first-set token's old_value survives the wire as the sentinel.

        Given:
            A Token minted by the first set on a previously-unset
            ContextVar, so its old_value is the Undefined sentinel.
        When:
            The token is serialized with the Wool serializer and
            deserialized.
        Then:
            The restored token's old_value should be the
            UndefinedType.Undefined singleton by identity, not a copy.
        """
        # Arrange
        var = ContextVar(_unique("old_value_wire_unset"))

        # Act
        with scoped_context():
            token = var.set("x")
            restored = loads(dumps(token))

        # Assert
        assert restored.old_value is Undefined

    @given(
        prior=st.just(_UNSET)
        | st.none()
        | st.text()
        | st.integers()
        | st.lists(st.integers())
    )
    @settings(max_examples=50, deadline=None)
    def test_token_should_roundtrip_with_arbitrary_prior_value(self, prior):
        """Test the wool-serializer roundtrip preserves old_value for any prior.

        Given:
            A ContextVar either left unset or set to any drawn None,
            text, integer, or list-of-integers prior value, then set
            again to mint a Token.
        When:
            The token is serialized with the Wool serializer and
            deserialized in-process.
        Then:
            The restored token's var should be the original ContextVar
            and its old_value should equal the original token's — with
            the Undefined sentinel preserved by identity when the var
            was previously unset.
        """
        # Arrange
        var = ContextVar(_unique("token_roundtrip_prior"))

        # Act
        with scoped_context():
            if prior is not _UNSET:
                var.set(prior)
            token = var.set("current")
            restored = loads(dumps(token))

        # Assert
        assert restored.var is var
        if prior is _UNSET:
            assert token.old_value is Undefined
            assert restored.old_value is Undefined
        else:
            assert restored.old_value == token.old_value

    def test_token_should_survive_repeated_wool_serialization(self):
        """Test a Token survives two consecutive wool-serializer hops.

        Given:
            A live wool.Token roundtripped through the Wool serializer
            once, yielding an orphan clone that is serialized again.
        When:
            The double-hop clone is inspected and reset.
        Then:
            It should still be a usable Token naming the original var
            with the original old_value, and its reset should raise
            ValueError — it was created in a different Context.
        """
        # Arrange
        var = ContextVar(_unique("token_double_hop"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            clone = loads(dumps(loads(dumps(token))))
            assert isinstance(clone, Token)
            assert clone.var is var
            assert clone.old_value == token.old_value
            with pytest.raises(ValueError, match="was created in a different Context"):
                var.reset(clone)


def test_token_should_keep_its_wool_var_reachable():
    """Test a live Token keeps its wool variable reachable.

    Given:
        A Token minted by ContextVar.set, with no other strong reference to
        the wool variable.
    When:
        A garbage collection runs while the token is held.
    Then:
        The token's ``var`` should still resolve to the wool ContextVar with
        the original namespace and name.
    """
    import gc

    # Arrange
    namespace = uuid.uuid4().hex
    with scoped_context():
        token = ContextVar("pinned_var", namespace=namespace).set("x")

    # Act
    gc.collect()

    # Assert
    assert token.var.namespace == namespace
    assert token.var.name == "pinned_var"


def test_token_sink_should_scope_wire_token_capture_to_the_decode():
    """Test the decode collector captures live wire tokens only within its scope.

    Given:
        A live token serialized through the Wool serializer.
    When:
        It is reconstituted inside a token_sink() scope and, separately,
        outside any scope.
    Then:
        The in-scope reconstitution should be captured for the frame's mount to
        anchor, while an out-of-scope reconstitution is captured by nothing — a
        token whose frame never mounts is reclaimed with the frame's list rather
        than accumulating in a process-global registry.
    """
    # Arrange
    var = ContextVar(_unique("collect_scope"))
    with scoped_context():
        payload = dumps(var.set("x"))

    # Act
    with token_sink() as collected:
        clone = loads(payload)
    outside = loads(payload)

    # Assert
    assert collected == [clone]
    assert isinstance(outside, Token)
    assert outside not in collected
