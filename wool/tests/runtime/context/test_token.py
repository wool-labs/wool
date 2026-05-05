import gc
import uuid
import weakref

import cloudpickle
import pytest

import wool
from tests.helpers import scoped_context
from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import Token
from wool.runtime.context import attached

dumps = wool.__serializer__.dumps
loads = cloudpickle.loads


class TestToken:
    def test_pickle_roundtrip_with_var_reference(self):
        """Test Token pickle roundtrip carries its owning ContextVar by key.

        Given:
            A ContextVar and a Token produced by set()
        When:
            The Token is pickled and unpickled
        Then:
            The restored token should reference the same ContextVar instance
        """
        # Arrange
        var = ContextVar("tokened")
        token = var.set("x")

        # Act
        restored = loads(dumps(token))

        # Assert
        assert restored.var is var
        assert restored.old_value is Token.MISSING

    def test_pickle_roundtrip_in_same_process(self):
        """Test same-process pickle of a Token returns the same instance.

        Given:
            A live Token minted via ContextVar.set — strongly
            referenced so its entry in the process-wide token
            registry stays alive
        When:
            The Token is pickled and unpickled in the same process
        Then:
            The restored token should be the same Python object as
            the original — the registry lookup in Token._reconstitute
            resolves the id back to the live instance so mutations
            to ``_used`` stay visible across all references
        """
        # Arrange
        var = ContextVar("pickle_identity")
        token = var.set("x")

        # Act
        restored = loads(dumps(token))

        # Assert
        assert restored is token

    def test_pickle_roundtrip_with_used_flag(self):
        """Test Token.__wool_reduce__ serializes the _used flag.

        Given:
            A Token minted and then consumed via ContextVar.reset,
            pickled after consumption
        When:
            The pickled bytes are loaded in a context where the
            original Token is no longer reachable (registry miss
            forces _reconstitute to build a fresh stub)
        Then:
            The restored stub should have used=True — the flag
            rides the pickle tuple so a cross-process copy cannot
            silently attempt reset
        """
        # Arrange
        var = ContextVar("pickle_used_flag")
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

    def test_pickle_roundtrip_in_active_receiver_context(self):
        """Test pickling a Token does not mutate the receiver's
        :class:`Context` via an embedded var value.

        Given:
            A wool.ContextVar bound to value ``"A"`` in one Context,
            with a Token pickled while that binding is active. The
            receiver later enters a different Context where the same
            var has been bound to ``"B"``.
        When:
            The pickled Token bytes are loaded under the receiver's
            Context
        Then:
            The receiver's Context should still report ``"B"`` for
            the var — a Token is a reset receipt, not a value-
            bearing wire payload, so its pickle round-trip must not
            transitively propagate the originating Context's binding
            through the owning ContextVar's reduce path
        """
        # Arrange
        var = ContextVar(f"token_no_value_leak_{uuid.uuid4().hex}")
        token = var.set("A")
        pickled = dumps(token)
        var.reset(token)
        observed: list[str] = []

        # Act
        with scoped_context():
            var.set("B")
            loads(pickled)
            observed.append(var.get())

        # Assert
        assert observed == ["B"]

    def test_set_reset_loop_token_lifecycle(self):
        """Test a tight set/reset loop does not accumulate per-iteration state.

        Given:
            A ContextVar in a fresh Context scope and a weakref to a
            sampled iteration's Token
        When:
            set(value) followed by reset(token) runs many times
            with no lingering strong reference to any iteration's
            Token, followed by gc.collect()
        Then:
            The sampled Token's weakref should resolve to None —
            each iteration's Token becomes unreachable once the
            loop's local binding is overwritten, so no lingering
            per-iteration state keeps it alive
        """
        # Arrange
        var = ContextVar("loop_no_leak")
        sampled_ref: weakref.ref[Token] | None = None

        # Act
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

    def test_repr_includes_var_key(self):
        """Test Token repr includes the owning var's key.

        Given:
            A Token produced by set() on a ContextVar
        When:
            repr() is called on it
        Then:
            The repr should include the var's full key
        """
        # Arrange
        var = ContextVar("repr_token_var")
        token = var.set("x")

        # Act
        text = repr(token)

        # Assert
        assert repr((var.namespace, var.name)) in text

    def test_missing_pickle_roundtrip_singleton_identity(self):
        """Test pickling and unpickling Token.MISSING returns the same
        singleton instance.

        Given:
            Token.MISSING — a singleton-by-construction sentinel
            whose ``__new__`` caches the lone instance and whose
            ``__reduce__`` rebuilds via the same constructor
        When:
            Token.MISSING is pickled with cloudpickle and the bytes
            are loaded back
        Then:
            The reloaded value is identical to the original —
            ``loaded is Token.MISSING`` — so callers comparing
            ``token.old_value is Token.MISSING`` after a wire round-
            trip still hit the identity check.
        """
        # Arrange
        original = Token.MISSING

        # Act
        pickled = dumps(original)
        loaded = loads(pickled)

        # Assert
        assert loaded is original
        assert loaded is Token.MISSING

    def test_used_is_false_before_reset_and_true_after(self):
        """Test Token.used flips from False to True when the owning var is reset.

        Given:
            A ContextVar and a Token produced by var.set()
        When:
            var.reset(token) is called
        Then:
            Token.used should be False before the reset and True
            after — single-process sanity for the lifecycle flag
        """
        # Arrange
        var = ContextVar("used_flag", default="d")
        token = var.set("x")

        # Act & assert
        assert token.used is False
        var.reset(token)
        assert token.used is True

    def test_out_of_order_pickle_loads_sync_used_state(self):
        """Test loading an older Token snapshot before a newer one
        leaves the registered instance synced to the most-progressed
        ``_used`` value witnessed across the round-trips.

        Given:
            A user pickles a Token before reset and again after
            reset, drops their reference to the original so the
            weak registry entry is collected, and then loads the
            two pickled snapshots in chronological order
            (``data_pre`` first, ``data_post`` second)
        When:
            The second load lands a registry hit on the stub
            registered by the first load and observes the wire
            payload's ``used=True``
        Then:
            Both reloaded references report ``used=True`` — the
            registry instance is monotonically advanced via
            :meth:`Token._sync_state` so a subsequent
            :meth:`ContextVar.reset` sees the consumed state and
            cannot silently double-reset against an older snapshot
        """
        # Arrange
        var = ContextVar(f"sync_used_{uuid.uuid4().hex}", default="d")
        token = var.set("x")
        data_pre = dumps(token)
        var.reset(token)
        data_post = dumps(token)

        # Drop the original strong reference so the WeakValueDictionary
        # entry can be collected; force a GC to ensure the entry is
        # gone before the first load fires.
        del token
        gc.collect()

        # Act
        loaded_pre = loads(data_pre)
        loaded_post = loads(data_post)

        # Assert
        assert loaded_post is loaded_pre, (
            "Same-id pickle round-trips should converge on a single registry instance"
        )
        assert loaded_pre.used is True, (
            "Older snapshot followed by newer snapshot must leave the "
            "registry instance reflecting the consumed state, not the "
            "pre-reset snapshot"
        )

    def test_vanilla_pickle_copy_and_deepcopy_are_rejected(self):
        """Test wool.Token rejects pickle, cloudpickle, copy.copy,
        and copy.deepcopy.

        Given:
            A live wool.Token
        When:
            pickle.dumps, cloudpickle.dumps, copy.copy, and
            copy.deepcopy are each invoked on it
        Then:
            All four raise TypeError. Tokens are bound to the
            process-wide registry and a live wool.Context, neither of
            which is reconstructible outside Wool's dispatch path —
            wool.__serializer__ remains the only valid serialization
            channel.
        """
        # Arrange
        import copy as _copy
        import pickle

        match = "wool.Token cannot be pickled"
        var = ContextVar("token_pickle_rejection")
        token = var.set("x")

        # Act & assert
        with pytest.raises(TypeError, match=match):
            pickle.dumps(token)
        with pytest.raises(TypeError, match=match):
            cloudpickle.dumps(token)
        with pytest.raises(TypeError, match=match):
            _copy.copy(token)
        with pytest.raises(TypeError, match=match):
            _copy.deepcopy(token)

    def test_wool_serializer_encodes_token_that_vanilla_cloudpickle_rejects(self):
        """Test wool.__serializer__.dumps encodes a wool.Token where
        bare cloudpickle.dumps raises.

        Given:
            A live wool.Token whose __reduce_ex__ guard rejects
            vanilla pickle / cloudpickle.
        When:
            The same Token is fed to wool.__serializer__.dumps and to
            cloudpickle.dumps.
        Then:
            wool.__serializer__.dumps should produce bytes that
            round-trip through cloudpickle.loads back to the same
            Token, while cloudpickle.dumps raises TypeError —
            documenting the worker dispatch handler's contract that
            non-passthrough fallback MUST go through
            wool.__serializer__ (whose internal _WoolPickler honors
            __wool_reduce__) and NOT bare cloudpickle (which trips
            the __reduce_ex__ guard).
        """
        # Arrange
        var = ContextVar("wool_serializer_token_round_trip")
        token = var.set("x")

        # Act
        encoded = wool.__serializer__.dumps(token)
        restored = cloudpickle.loads(encoded)

        # Assert
        assert restored is token, "Same-process round-trip preserves identity"
        with pytest.raises(TypeError, match="wool.Token cannot be pickled"):
            cloudpickle.dumps(token)


def test_token_reconstitute_with_uuid_in_active_external_set():
    """Test cross-process pickle reload of a Token whose UUID arrived
    earlier on the wire as a consumed-token id converges to a
    consumed Token instance.

    Given:
        A :class:`Token` minted under a sender's :class:`Context`,
        cloudpickled, the original released so the process-wide
        registry no longer carries it, and a wire
        :class:`protocol.Context` whose ``consumed_tokens`` lists
        that token's id — reconstituted to a Context that is then
        attached as the active scope.
    When:
        The pickle is loaded under that active Context.
    Then:
        ``restored.used`` is True — the reconstitute path observes
        the matching id in the active Context's external set and
        adopts the consumed state, so a subsequent
        :meth:`ContextVar.reset` raises rather than silently
        succeeding.
    """
    # Arrange
    from wool import protocol

    var = ContextVar(f"reconstitute_promote_{uuid.uuid4().hex}")
    token = var.set("x")
    token_id = token.id
    pickled = dumps(token)
    del token
    gc.collect()

    pb = protocol.Context(id=uuid.uuid4().hex)
    pb.vars.add(
        namespace=var.namespace,
        name=var.name,
        consumed_tokens=[token_id.hex],
    )
    secondary = Context.from_protobuf(pb)

    # Act
    with attached(secondary, guarded=False):
        restored = loads(pickled)

    # Assert
    assert restored.used is True


def test_token_does_not_pin_originating_context():
    """Test a live Token does not keep its originating wool.Context
    alive once the user drops their own references to the Context.

    Given:
        A wool.Context held via a weakref and a Token minted under
        that Context (via ContextVar.set inside Context.run) retained
        in a strong-reffed local — modeling a user who stashes a
        Token on a long-lived service object.
    When:
        The original strong reference to the Context is dropped and
        ``gc.collect()`` runs.
    Then:
        The weakref to the Context resolves to ``None`` — the Token
        does not pin the Context, so other vars and values bound in
        that Context are eligible for collection. A long-lived Token
        no longer leaks its originating Context's state.
    """
    # Arrange
    var = ContextVar(f"no_pin_{uuid.uuid4().hex}")
    ctx = Context()
    ctx_ref = weakref.ref(ctx)

    def mint() -> Token:
        return var.set("x")

    token = ctx.run(mint)

    # Act
    del ctx
    gc.collect()

    # Assert
    assert ctx_ref() is None, (
        "Originating Context should be collectible once the user "
        "drops their reference, even with the Token still alive"
    )
    # Sanity: the token is still alive and reports unused.
    assert token.used is False
