import uuid

import cloudpickle
import pytest

import wool
from tests.helpers import scoped_context
from wool.runtime.context import ContextVar
from wool.runtime.context import Token
from wool.runtime.context import TokenLike

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
    def test___repr___includes_var_key(self):
        """Test Token repr includes the owning var's key.

        Given:
            A Token produced by set() on a ContextVar.
        When:
            repr() is called on it.
        Then:
            It should include the var's full key.
        """
        # Arrange
        var = ContextVar(_unique("repr_token_var"))

        # Act
        with scoped_context():
            token = var.set("x")
            text = repr(token)

        # Assert
        assert repr((var.namespace, var.name)) in text

    def test_id_across_separate_sets(self):
        """Test each Token carries a distinct id.

        Given:
            Two Tokens minted by separate set calls.
        When:
            Their ids are compared.
        Then:
            They should differ.
        """
        # Arrange
        var = ContextVar(_unique("token_id"))

        # Act
        with scoped_context():
            first = var.set("a")
            second = var.set("b")

        # Assert
        assert first.id != second.id

    def test_var_resolves_owning_contextvar(self):
        """Test Token.var resolves the owning ContextVar from its key.

        Given:
            A Token minted by ContextVar.set.
        When:
            Token.var is accessed.
        Then:
            It should return the originating ContextVar instance.
        """
        # Arrange
        var = ContextVar(_unique("token_var"))

        # Act
        with scoped_context():
            token = var.set("x")

        # Assert
        assert token.var is var

    def test_used_across_a_reset(self):
        """Test Token.used flips from False to True when the owning var is reset.

        Given:
            A ContextVar and a Token produced by var.set().
        When:
            var.reset(token) is called.
        Then:
            Token.used should be False before the reset and True after.
        """
        # Arrange
        var = ContextVar(_unique("used_flag"), default="d")

        # Act & assert
        with scoped_context():
            token = var.set("x")
            assert token.used is False
            var.reset(token)
            assert token.used is True

    def test_used_reflects_external_used_log(self):
        """Test Token.used reports True from the context external-used log.

        Given:
            A Token whose id has been recorded in the active context's
            external_used log — the wire-propagated used signal.
        When:
            Token.used is read inside that armed context.
        Then:
            It should be True even though the token was never reset in
            this process — used-state also rests on the wire-propagated
            external_used log.
        """
        # Arrange
        from wool.runtime.context import current_context

        var = ContextVar(_unique("used_external"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            assert token.used is False
            armed = current_context()
            assert armed is not None
            armed.evolve(external_used={token.id: (var.namespace, var.name)}).mount()
            assert token.used is True

    def test_old_value_reports_missing_for_first_set(self):
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

    def test_old_value_reports_prior_value_for_nested_set(self):
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

    def test_missing_singleton_pickle_identity(self):
        """Test pickling Token.MISSING returns the same singleton instance.

        Given:
            Token.MISSING — a singleton sentinel.
        When:
            Token.MISSING is dumped and loaded back.
        Then:
            The reloaded value should be identical to the original.
        """
        # Arrange
        original = Token.MISSING

        # Act
        loaded = loads(dumps(original))

        # Assert
        assert loaded is original
        assert loaded is Token.MISSING

    def test___reduce_ex___rejects_vanilla_pickle_cloudpickle_and_copy(self):
        """Test wool.Token rejects pickle, cloudpickle, copy.copy, and deepcopy.

        Given:
            A live wool.Token.
        When:
            pickle.dumps, cloudpickle.dumps, copy.copy, and
            copy.deepcopy are each invoked on it.
        Then:
            All four raise TypeError — Token reset semantics are scoped
            to a live chain and Token is currently not transportable
            across processes; a chain-bound Token has no meaningful
            copy semantics either.
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
                with pytest.raises(TypeError, match="wool.Token cannot be pickled"):
                    channel(token)


class TestTokenLike:
    def test_concrete_token_satisfies_tokenlike_surface(self):
        """Test the concrete Token structurally matches the TokenLike Protocol.

        Given:
            A live, concrete wool.Token cast to the TokenLike Protocol.
        When:
            Each member of the Protocol's narrow surface is read off
            the cast view — ``.var``, ``.old_value``, ``MISSING``.
        Then:
            They should resolve to the same values as on the concrete
            instance — TokenLike is an additive type-only narrow
            surface, not a runtime adapter.
        """
        # Arrange
        from typing import cast

        var = ContextVar(_unique("tokenlike_surface"))

        # Act
        with scoped_context():
            token = var.set("x")
            narrow = cast(TokenLike[str], token)

            # Assert — every TokenLike surface member resolves through
            # to the concrete Token.
            assert narrow.var is var
            assert narrow.old_value is token.old_value
            assert narrow.MISSING is Token.MISSING

    def test_tokenlike_is_not_runtime_checkable(self):
        """Test TokenLike rejects isinstance checks at runtime.

        Given:
            The wool.TokenLike Protocol — declared without
            @runtime_checkable.
        When:
            isinstance is invoked against it with any object.
        Then:
            It should raise TypeError — a stdlib contextvars.Token
            must not satisfy isinstance(t, wool.Token) because the
            two have different reset semantics, so a runtime
            structural check is the wrong tool.
        """
        # Arrange
        import contextvars as _cv

        sentinel = _cv.ContextVar[str]("__tokenlike_test_var__")

        # Act & assert
        with pytest.raises(TypeError, match="runtime_checkable"):
            isinstance(sentinel, TokenLike)
