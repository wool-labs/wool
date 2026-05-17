import gc
import uuid
import weakref

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from tests.helpers import scoped_context
from wool.runtime.context import ContextVar
from wool.runtime.context import Token

# ``dumps`` is the wool serializer — Token/ContextVar serialize only through
# it. ``loads`` is plain ``cloudpickle.loads``: wool-encoded bytes are
# cloudpickle-loadable, so a separate wool deserializer is not needed here.
dumps = wool.__serializer__.dumps
loads = cloudpickle.loads


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class TestToken:
    def test___wool_reduce___with_var_reference(self):
        """Test Token pickle roundtrip carries its owning ContextVar by key.

        Given:
            A ContextVar and a Token produced by set().
        When:
            The Token is dumped and loaded via the wool serializer.
        Then:
            It should reference the same ContextVar instance and carry
            MISSING as the old value.
        """
        # Arrange
        var = ContextVar(_unique("tokened"))

        # Act
        with scoped_context():
            token = var.set("x")
            restored = loads(dumps(token))

        # Assert
        assert restored.var is var
        assert restored.old_value is Token.MISSING

    def test___wool_reduce___same_process_identity(self):
        """Test same-process pickle of a Token returns the same instance.

        Given:
            A live Token minted via ContextVar.set, strongly referenced
            so its token registry entry stays alive.
        When:
            The Token is dumped and loaded in the same process.
        Then:
            It should be the same Python object as the original.
        """
        # Arrange
        var = ContextVar(_unique("pickle_identity"))

        # Act
        with scoped_context():
            token = var.set("x")
            restored = loads(dumps(token))

        # Assert
        assert restored is token

    def test___wool_reduce___with_used_flag(self):
        """Test Token serialization carries the used flag across a registry miss.

        Given:
            A Token minted and then consumed via ContextVar.reset,
            dumped after consumption and released so the registry
            entry drops.
        When:
            The dumped bytes are loaded with the original collected.
        Then:
            It should report used=True.
        """
        # Arrange
        var = ContextVar(_unique("pickle_used_flag"))
        with scoped_context():
            token = var.set("x")
            var.reset(token)
            pickled = dumps(token)
        original_ref = weakref.ref(token)
        del token
        gc.collect()
        assert original_ref() is None, (
            "Original Token must be collected before the load for the "
            "registry-miss path to fire"
        )

        # Act
        restored = loads(pickled)

        # Assert
        assert restored.used is True

    def test___wool_reduce___old_value_survives_a_registry_miss(self):
        """Test a non-MISSING old_value survives reconstitution on a registry miss.

        Given:
            A Token capturing a prior value as its old_value, dumped
            and then released so its token-registry entry is collected.
        When:
            The dumped bytes are loaded with the original collected,
            forcing the registry-miss reconstitution path.
        Then:
            It should rebuild a Token carrying an equal old_value — the
            pickled old_value is not dropped when no live instance is
            found.
        """
        # Arrange
        var = ContextVar(_unique("old_value_registry_miss"))
        with scoped_context():
            var.set("prior")
            token = var.set("new")
            pickled = dumps(token)
        original_ref = weakref.ref(token)
        del token
        gc.collect()
        assert original_ref() is None, (
            "Original Token must be collected before the load for the "
            "registry-miss path to fire"
        )

        # Act
        restored = loads(pickled)

        # Assert
        assert restored.old_value == "prior"

    @given(
        old_value=st.one_of(st.text(), st.integers(), st.binary(), st.none()),
    )
    @settings(max_examples=50)
    def test___wool_reduce___roundtrip_with_arbitrary_old_value(self, old_value):
        """Test Token wool-pickler roundtrip preserves identity and old_value.

        Given:
            Any old_value drawn from text, integer, binary, or None,
            seeded as a variable's prior value.
        When:
            A Token capturing that old_value is dumped via the wool
            serializer and loaded back in the same process.
        Then:
            It should be the same Python object and carry an equal
            old_value.
        """
        # Arrange
        var = ContextVar(_unique("hyp_token_rt"))

        with scoped_context():
            # Seed a prior value so the second set captures it as old_value.
            var.set(old_value)
            token = var.set("new")

            # Act
            restored = loads(dumps(token))

        # Assert
        assert restored is token
        assert restored.old_value == old_value

    def test_set_reset_loop_token_lifecycle(self):
        """Test a tight set/reset loop does not accumulate per-iteration state.

        Given:
            A ContextVar in a fresh context and a weakref to a
            sampled iteration's Token.
        When:
            set(value) followed by reset(token) runs many times with
            no lingering strong reference to any iteration's Token,
            followed by gc.collect().
        Then:
            The sampled Token's weakref should resolve to None.
        """
        # Arrange
        var = ContextVar(_unique("loop_no_leak"))
        sampled_ref: weakref.ref[Token] | None = None

        # Act
        with scoped_context():
            for i in range(200):
                token = var.set("x")
                if i == 100:
                    sampled_ref = weakref.ref(token)
                var.reset(token)
                del token
        gc.collect()

        # Assert
        assert sampled_ref is not None
        assert sampled_ref() is None

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

    def test___wool_reduce___missing_singleton_identity(self):
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

    def test___wool_reduce___out_of_order_syncs_used(self):
        """Test loading an older Token snapshot before a newer one syncs used state.

        Given:
            A Token dumped before reset and again after reset, with
            the original dropped so the registry entry is collected.
        When:
            The two dumped snapshots are loaded in chronological order.
        Then:
            Both reloaded references should converge on one registry
            instance reporting used=True.
        """
        # Arrange
        var = ContextVar(_unique("sync_used"), default="d")
        with scoped_context():
            token = var.set("x")
            data_pre = dumps(token)
            var.reset(token)
            data_post = dumps(token)
        token_ref = weakref.ref(token)
        del token
        gc.collect()
        assert token_ref() is None, (
            "Original Token must be collected before loading for the "
            "registry-miss path to fire"
        )

        # Act
        loaded_pre = loads(data_pre)
        loaded_post = loads(data_post)

        # Assert
        assert loaded_post is loaded_pre
        assert loaded_pre.used is True

    def test___reduce_ex___rejects_vanilla_pickle(self):
        """Test wool.Token rejects serialization via vanilla pickle.

        Given:
            A live wool.Token.
        When:
            pickle.dumps is called on it.
        Then:
            It should raise TypeError — wool.__serializer__ is the only
            valid serialization channel.
        """
        # Arrange
        import pickle

        var = ContextVar(_unique("token_reject_pickle"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            with pytest.raises(TypeError, match="wool.Token cannot be pickled"):
                pickle.dumps(token)

    def test___reduce_ex___rejects_cloudpickle(self):
        """Test wool.Token rejects serialization via cloudpickle.

        Given:
            A live wool.Token.
        When:
            cloudpickle.dumps is called on it.
        Then:
            It should raise TypeError — wool.__serializer__ is the only
            valid serialization channel.
        """
        # Arrange
        var = ContextVar(_unique("token_reject_cloudpickle"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            with pytest.raises(TypeError, match="wool.Token cannot be pickled"):
                cloudpickle.dumps(token)

    def test___reduce_ex___rejects_copy(self):
        """Test wool.Token rejects copy.copy.

        Given:
            A live wool.Token.
        When:
            copy.copy is called on it.
        Then:
            It should raise TypeError — a registry-bound Token has no
            meaningful copy semantics.
        """
        # Arrange
        import copy as _copy

        var = ContextVar(_unique("token_reject_copy"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            with pytest.raises(TypeError, match="wool.Token cannot be pickled"):
                _copy.copy(token)

    def test___reduce_ex___rejects_deepcopy(self):
        """Test wool.Token rejects copy.deepcopy.

        Given:
            A live wool.Token.
        When:
            copy.deepcopy is called on it.
        Then:
            It should raise TypeError — a registry-bound Token has no
            meaningful copy semantics.
        """
        # Arrange
        import copy as _copy

        var = ContextVar(_unique("token_reject_deepcopy"))

        # Act & assert
        with scoped_context():
            token = var.set("x")
            with pytest.raises(TypeError, match="wool.Token cannot be pickled"):
                _copy.deepcopy(token)

    def test___wool_reduce___encodes_via_wool_serializer(self):
        """Test wool.__serializer__.dumps encodes a Token and it round-trips back.

        Given:
            A live wool.Token whose __reduce_ex__ guard rejects vanilla
            cloudpickle.
        When:
            The Token is fed to wool.__serializer__.dumps and the bytes
            are loaded back.
        Then:
            It should round-trip to the same Token instance.
        """
        # Arrange
        var = ContextVar(_unique("wool_serializer_token"))

        # Act
        with scoped_context():
            token = var.set("x")
            encoded = wool.__serializer__.dumps(token)
            restored = cloudpickle.loads(encoded)

        # Assert
        assert restored is token
