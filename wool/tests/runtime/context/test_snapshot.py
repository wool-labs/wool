import threading
import uuid
import warnings
from uuid import UUID
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from tests.helpers import scoped_context
from wool import protocol
from wool.runtime.context import ContextVar
from wool.runtime.context import Snapshot
from wool.runtime.context import current_snapshot
from wool.runtime.context import decode_snapshot
from wool.runtime.context import encode_snapshot
from wool.runtime.context import install_snapshot
from wool.runtime.context import merge_snapshot
from wool.runtime.context import snapshot_has_state
from wool.runtime.context import with_snapshot
from wool.runtime.context.snapshot import fork_snapshot


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class TestSnapshot:
    def test___init___with_required_fields(self):
        """Test Snapshot construction with only the required fields.

        Given:
            A fresh chain id and an owner thread id.
        When:
            A Snapshot is constructed with only chain_id and owner.
        Then:
            It should expose empty data, consumed, and stub_pins
            collections by default.
        """
        # Arrange
        chain_id = uuid4()
        owner = threading.get_ident()

        # Act
        snapshot = Snapshot(chain_id=chain_id, owner=owner)

        # Assert
        assert snapshot.chain_id == chain_id
        assert snapshot.owner == owner
        assert dict(snapshot.data) == {}
        assert dict(snapshot.consumed) == {}
        assert snapshot.stub_pins == frozenset()

    def test___init___with_all_fields(self):
        """Test Snapshot construction with every field supplied.

        Given:
            Explicit data, consumed, and stub_pins collections.
        When:
            A Snapshot is constructed with all fields.
        Then:
            It should expose each supplied collection verbatim.
        """
        # Arrange
        var = ContextVar(_unique("snap_init"))
        token_id = uuid4()
        data = {var: "value"}
        consumed = {token_id: (var.namespace, var.name)}

        # Act
        snapshot = Snapshot(
            chain_id=uuid4(),
            owner=threading.get_ident(),
            data=data,
            consumed=consumed,
            stub_pins=frozenset({var}),
        )

        # Assert
        assert dict(snapshot.data) == data
        assert dict(snapshot.consumed) == consumed
        assert snapshot.stub_pins == frozenset({var})

    def test_evolve_replaces_named_field(self):
        """Test evolve returns a copy with the named change applied.

        Given:
            A snapshot with an empty data map.
        When:
            evolve is called with a new data map.
        Then:
            It should return a new snapshot carrying the new data
            while leaving the original untouched.
        """
        # Arrange
        var = ContextVar(_unique("snap_evolve"))
        original = Snapshot(chain_id=uuid4(), owner=threading.get_ident())

        # Act
        evolved = original.evolve(data={var: "x"})

        # Assert
        assert dict(evolved.data) == {var: "x"}
        assert dict(original.data) == {}
        assert evolved.chain_id == original.chain_id

    def test_evolve_preserves_unchanged_fields(self):
        """Test evolve preserves fields that are not named.

        Given:
            A snapshot with a known chain id and owner.
        When:
            evolve is called changing only the consumed map.
        Then:
            It should preserve the original chain id and owner.
        """
        # Arrange
        chain_id = uuid4()
        owner = threading.get_ident()
        original = Snapshot(chain_id=chain_id, owner=owner)
        token_id = uuid4()

        # Act
        evolved = original.evolve(consumed={token_id: ("ns", "name")})

        # Assert
        assert evolved.chain_id == chain_id
        assert evolved.owner == owner
        assert dict(evolved.consumed) == {token_id: ("ns", "name")}

    def test_equality_is_identity_based(self):
        """Test Snapshot equality is identity-based.

        Given:
            Two snapshots constructed with identical field values.
        When:
            They are compared for equality.
        Then:
            They should be unequal — Snapshot is declared eq=False so
            distinct instances never compare equal.
        """
        # Arrange
        chain_id = uuid4()
        owner = threading.get_ident()

        # Act
        first = Snapshot(chain_id=chain_id, owner=owner)
        second = Snapshot(chain_id=chain_id, owner=owner)

        # Assert
        assert first != second
        assert first == first


def test_current_snapshot_when_unarmed():
    """Test current_snapshot returns None for an unarmed context.

    Given:
        A fresh, unarmed Wool context where no ContextVar was set.
    When:
        current_snapshot is called.
    Then:
        It should return None.
    """
    # Arrange, act, & assert
    with scoped_context():
        assert current_snapshot() is None


def test_current_snapshot_when_armed():
    """Test current_snapshot returns the installed snapshot.

    Given:
        A context with a snapshot explicitly installed.
    When:
        current_snapshot is called.
    Then:
        It should return that same snapshot instance.
    """
    # Arrange
    snapshot = Snapshot(chain_id=uuid4(), owner=threading.get_ident())

    with scoped_context():
        # Act
        install_snapshot(snapshot)
        observed = current_snapshot()

        # Assert
        assert observed is snapshot


def test_install_snapshot_makes_snapshot_current():
    """Test install_snapshot makes the supplied snapshot current.

    Given:
        A snapshot and an unarmed context.
    When:
        install_snapshot is called with the snapshot.
    Then:
        current_snapshot should subsequently return that snapshot.
    """
    # Arrange
    snapshot = Snapshot(chain_id=uuid4(), owner=threading.get_ident())

    with scoped_context():
        # Act
        install_snapshot(snapshot)

        # Assert
        assert current_snapshot() is snapshot


def test_with_snapshot_scopes_install_to_block():
    """Test with_snapshot installs only for the duration of the block.

    Given:
        An unarmed context and a snapshot to scope.
    When:
        with_snapshot is entered and then exited.
    Then:
        current_snapshot should return the snapshot inside the block
        and revert to the prior value on exit.
    """
    # Arrange
    snapshot = Snapshot(chain_id=uuid4(), owner=threading.get_ident())

    with scoped_context():
        # Act
        with with_snapshot(snapshot):
            # Assert — snapshot is active inside the block
            assert current_snapshot() is snapshot

        # Assert — snapshot is no longer active after exit
        assert current_snapshot() is None


def test_with_snapshot_restores_on_exception():
    """Test with_snapshot restores the prior snapshot when the block raises.

    Given:
        An unarmed context and a snapshot to scope.
    When:
        with_snapshot wraps a block that raises an exception.
    Then:
        current_snapshot should revert to None after the exception
        unwinds.
    """
    # Arrange
    snapshot = Snapshot(chain_id=uuid4(), owner=threading.get_ident())

    # Act
    with scoped_context():
        with pytest.raises(RuntimeError):
            with with_snapshot(snapshot):
                raise RuntimeError("boom")

        # Assert
        assert current_snapshot() is None


def test_snapshot_has_state_when_none():
    """Test snapshot_has_state returns False for None.

    Given:
        A None snapshot (an unarmed context).
    When:
        snapshot_has_state is called.
    Then:
        It should return False.
    """
    # Arrange, act, & assert
    assert snapshot_has_state(None) is False


def test_snapshot_has_state_when_empty():
    """Test snapshot_has_state returns False for an empty snapshot.

    Given:
        A snapshot with no data and no consumed tokens.
    When:
        snapshot_has_state is called.
    Then:
        It should return False.
    """
    # Arrange
    snapshot = Snapshot(chain_id=uuid4(), owner=threading.get_ident())

    # Act & assert
    assert snapshot_has_state(snapshot) is False


def test_snapshot_has_state_with_data():
    """Test snapshot_has_state returns True when data is present.

    Given:
        A snapshot carrying one variable binding.
    When:
        snapshot_has_state is called.
    Then:
        It should return True.
    """
    # Arrange
    var = ContextVar(_unique("has_state_data"))
    snapshot = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={var: "x"},
    )

    # Act & assert
    assert snapshot_has_state(snapshot) is True


def test_snapshot_has_state_with_consumed_only():
    """Test snapshot_has_state returns True when only consumed tokens exist.

    Given:
        A snapshot with no data but a non-empty consumed log.
    When:
        snapshot_has_state is called.
    Then:
        It should return True — a propagated reset still carries state.
    """
    # Arrange
    snapshot = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        consumed={uuid4(): ("ns", "name")},
    )

    # Act & assert
    assert snapshot_has_state(snapshot) is True


def test_encode_snapshot_with_none():
    """Test encode_snapshot returns an empty wire context for None.

    Given:
        A None snapshot (an unarmed context).
    When:
        encode_snapshot is called.
    Then:
        It should return an empty protocol.Context with no vars.
    """
    # Act
    wire = encode_snapshot(None)

    # Assert
    assert isinstance(wire, protocol.Context)
    assert len(wire.vars) == 0
    assert wire.id == ""


def test_encode_snapshot_carries_chain_id():
    """Test encode_snapshot writes the chain id to the wire context.

    Given:
        A snapshot with a known chain id.
    When:
        encode_snapshot is called.
    Then:
        The wire context id should equal the chain id's hex form.
    """
    # Arrange
    chain_id = uuid4()
    snapshot = Snapshot(chain_id=chain_id, owner=threading.get_ident())

    # Act
    wire = encode_snapshot(snapshot)

    # Assert
    assert wire.id == chain_id.hex


def test_encode_snapshot_emits_one_entry_per_variable():
    """Test encode_snapshot emits one wire entry per bound variable.

    Given:
        A snapshot with two distinct variable bindings.
    When:
        encode_snapshot is called.
    Then:
        The wire context should carry one entry per variable, each
        with a populated value field.
    """
    # Arrange
    var_a = ContextVar(_unique("encode_a"))
    var_b = ContextVar(_unique("encode_b"))
    snapshot = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={var_a: 1, var_b: 2},
    )

    # Act
    wire = encode_snapshot(snapshot)

    # Assert
    keys = {(entry.namespace, entry.name) for entry in wire.vars}
    assert keys == {(var_a.namespace, var_a.name), (var_b.namespace, var_b.name)}
    assert all(entry.HasField("value") for entry in wire.vars)


def test_encode_snapshot_with_empty_data():
    """Test encode_snapshot produces an empty wire context when data is empty.

    Given:
        A snapshot with no variable bindings in its data map.
    When:
        encode_snapshot is called.
    Then:
        The wire context should carry no entries.
    """
    # Arrange
    snapshot = Snapshot(chain_id=uuid4(), owner=threading.get_ident())

    # Act
    wire = encode_snapshot(snapshot)

    # Assert
    assert len(wire.vars) == 0


def test_encode_snapshot_emits_consumed_tokens():
    """Test encode_snapshot propagates consumed-token ids on the wire.

    Given:
        A snapshot carrying a variable binding and a consumed-token
        log entry for that variable's key.
    When:
        encode_snapshot is called.
    Then:
        The wire entry for that variable should list the consumed
        token's hex id.
    """
    # Arrange
    var = ContextVar(_unique("encode_consumed"))
    token_id = uuid4()
    snapshot = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={var: "x"},
        consumed={token_id: (var.namespace, var.name)},
    )

    # Act
    wire = encode_snapshot(snapshot)

    # Assert
    entry = next(
        e for e in wire.vars if (e.namespace, e.name) == (var.namespace, var.name)
    )
    assert list(entry.consumed_tokens) == [token_id.hex]


def test_encode_snapshot_emits_reset_without_rebind():
    """Test encode_snapshot emits a consumed entry for a variable reset but not re-set.

    Given:
        A snapshot with a consumed-token log entry for a variable that
        has no current binding in data (reset and not subsequently re-set).
    When:
        encode_snapshot is called and the result is decoded.
    Then:
        The decoded snapshot should carry the consumed token entry for
        that variable.
    """
    # Arrange
    var = ContextVar(_unique("encode_reset_no_rebind"))
    token_id = uuid4()
    snapshot = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={},
        consumed={token_id: (var.namespace, var.name)},
    )

    # Act
    wire = encode_snapshot(snapshot)
    decoded = decode_snapshot(wire)

    # Assert
    assert token_id in decoded.consumed
    assert decoded.consumed[token_id] == (var.namespace, var.name)


def test_encode_snapshot_suppresses_consumed_tokens_for_unserializable_value():
    """Test encode_snapshot omits consumed tokens when the value fails to serialize.

    Given:
        A snapshot with an unserializable value and a consumed-token
        log entry for the same variable.
    When:
        encode_snapshot is called.
    Then:
        The wire context should carry no entry for that variable —
        neither the value nor the consumed tokens are emitted, to
        prevent a phantom reset on the receiver.
    """
    # Arrange
    var = ContextVar(_unique("encode_phantom_suppress"))
    token_id = uuid4()
    snapshot = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={var: threading.Lock()},
        consumed={token_id: (var.namespace, var.name)},
    )

    # Act
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        wire = encode_snapshot(snapshot)

    # Assert
    keys = {(entry.namespace, entry.name) for entry in wire.vars}
    assert (var.namespace, var.name) not in keys


def test_encode_snapshot_skips_unserializable_value_with_warning():
    """Test encode_snapshot warns and skips a variable it cannot serialize.

    Given:
        A snapshot carrying an unpicklable value alongside a normal
        binding.
    When:
        encode_snapshot is called.
    Then:
        It should emit a ContextDecodeWarning and emit only the
        serializable variable on the wire.
    """
    # Arrange
    good = ContextVar(_unique("encode_good"))
    bad = ContextVar(_unique("encode_bad"))
    snapshot = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={good: "ok", bad: threading.Lock()},
    )

    # Act
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        wire = encode_snapshot(snapshot)

    # Assert
    keys = {(entry.namespace, entry.name) for entry in wire.vars}
    assert keys == {(good.namespace, good.name)}
    assert any(issubclass(w.category, wool.ContextDecodeWarning) for w in caught)


def test_encode_snapshot_strict_mode_aggregates_failures():
    """Test encode_snapshot raises an exception group under strict mode.

    Given:
        A snapshot with two unserializable values and strict warning
        filtering for ContextDecodeWarning.
    When:
        encode_snapshot is called.
    Then:
        It should raise a BaseExceptionGroup aggregating both
        per-variable failures.
    """
    # Arrange
    bad_a = ContextVar(_unique("encode_strict_a"))
    bad_b = ContextVar(_unique("encode_strict_b"))
    snapshot = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={bad_a: threading.Lock(), bad_b: threading.Lock()},
    )

    # Act & assert
    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=wool.ContextDecodeWarning)
        with pytest.raises(BaseExceptionGroup) as exc_info:
            encode_snapshot(snapshot)
    assert len(exc_info.value.exceptions) == 2
    assert all(
        isinstance(e, wool.ContextDecodeWarning) for e in exc_info.value.exceptions
    )


def test_decode_snapshot_round_trips_a_value():
    """Test decode_snapshot recovers an encoded variable binding.

    Given:
        A wire context produced by encoding a snapshot with one
        variable binding.
    When:
        decode_snapshot is called on that wire context.
    Then:
        The decoded snapshot should carry the same variable bound to
        the same value.
    """
    # Arrange
    var = ContextVar(_unique("decode_value"))
    snapshot = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={var: "hello"},
    )
    wire = encode_snapshot(snapshot)

    # Act
    decoded = decode_snapshot(wire)

    # Assert
    assert decoded.data[var] == "hello"
    assert decoded.chain_id == snapshot.chain_id


def test_decode_snapshot_round_trips_consumed_token():
    """Test decode_snapshot preserves the consumed-token log through encode/decode.

    Given:
        A snapshot carrying a variable binding and a consumed token for
        that variable.
    When:
        The snapshot is encoded to the wire and decoded back.
    Then:
        The decoded snapshot's consumed log should contain the original
        token id mapped to the variable's key.
    """
    # Arrange
    var = ContextVar(_unique("decode_consumed_rt"))
    token_id = uuid4()
    snapshot = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={var: "v"},
        consumed={token_id: (var.namespace, var.name)},
    )
    wire = encode_snapshot(snapshot)

    # Act
    decoded = decode_snapshot(wire)

    # Assert
    assert token_id in decoded.consumed
    assert decoded.consumed[token_id] == (var.namespace, var.name)


def test_decode_snapshot_adopts_calling_thread_as_owner():
    """Test decode_snapshot sets the owner to the calling thread.

    Given:
        A wire context with a variable binding.
    When:
        decode_snapshot is called.
    Then:
        The decoded snapshot's owner should be the calling thread id.
    """
    # Arrange
    var = ContextVar(_unique("decode_owner"))
    wire = encode_snapshot(
        Snapshot(
            chain_id=uuid4(),
            owner=threading.get_ident(),
            data={var: 1},
        )
    )

    # Act
    decoded = decode_snapshot(wire)

    # Assert
    assert decoded.owner == threading.get_ident()


def test_decode_snapshot_with_malformed_chain_id():
    """Test decode_snapshot falls back to a fresh chain id when the wire id is bad.

    Given:
        A wire context whose id is not a valid UUID hex string.
    When:
        decode_snapshot is called.
    Then:
        It should emit a ContextDecodeWarning and produce a snapshot
        with a freshly minted chain id.
    """
    # Arrange
    wire = protocol.Context(id="not-a-uuid")

    # Act
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        decoded = decode_snapshot(wire)

    # Assert
    assert isinstance(decoded.chain_id, UUID)
    assert any(issubclass(w.category, wool.ContextDecodeWarning) for w in caught)


def test_decode_snapshot_registers_stub_for_undeclared_variable():
    """Test decode_snapshot pins a stub for an undeclared wire variable.

    Given:
        A wire context referencing a variable key that was never
        declared as a ContextVar in this process.
    When:
        decode_snapshot is called.
    Then:
        The decoded snapshot should pin a stub variable for that key.
    """
    # Arrange
    key = ("undeclared_ns", _unique("decode_stub"))
    wire = protocol.Context(id=uuid4().hex)
    wire.vars.add(namespace=key[0], name=key[1])

    # Act
    decoded = decode_snapshot(wire)

    # Assert
    pinned_keys = {(var.namespace, var.name) for var in decoded.stub_pins}
    assert key in pinned_keys


def test_decode_snapshot_skips_unserializable_value_with_warning():
    """Test decode_snapshot warns and skips a value it cannot deserialize.

    Given:
        A wire context entry whose value bytes are not valid pickle
        data.
    When:
        decode_snapshot is called.
    Then:
        It should emit a ContextDecodeWarning and omit that variable
        from the decoded data.
    """
    # Arrange
    var = ContextVar(_unique("decode_bad_value"))
    wire = protocol.Context(id=uuid4().hex)
    wire.vars.add(namespace=var.namespace, name=var.name, value=b"not-pickle")

    # Act
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        decoded = decode_snapshot(wire)

    # Assert
    assert var not in decoded.data
    assert any(issubclass(w.category, wool.ContextDecodeWarning) for w in caught)


def test_decode_snapshot_with_malformed_consumed_token_id():
    """Test decode_snapshot warns and skips a malformed consumed-token id.

    Given:
        A wire entry whose consumed_tokens list holds a non-UUID
        string.
    When:
        decode_snapshot is called.
    Then:
        It should emit a ContextDecodeWarning and omit that id from
        the decoded consumed log.
    """
    # Arrange
    var = ContextVar(_unique("decode_bad_token"))
    wire = protocol.Context(id=uuid4().hex)
    wire.vars.add(
        namespace=var.namespace,
        name=var.name,
        consumed_tokens=["not-a-uuid"],
    )

    # Act
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        decoded = decode_snapshot(wire)

    # Assert
    assert dict(decoded.consumed) == {}
    assert any(issubclass(w.category, wool.ContextDecodeWarning) for w in caught)


def test_decode_snapshot_strict_mode_aggregates_failures():
    """Test decode_snapshot raises an exception group under strict mode.

    Given:
        A wire context with a malformed chain id and an unparseable
        value, under strict ContextDecodeWarning filtering.
    When:
        decode_snapshot is called.
    Then:
        It should raise a BaseExceptionGroup aggregating both
        failures.
    """
    # Arrange
    var = ContextVar(_unique("decode_strict"))
    wire = protocol.Context(id="bad-id")
    wire.vars.add(namespace=var.namespace, name=var.name, value=b"bad")

    # Act & assert
    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=wool.ContextDecodeWarning)
        with pytest.raises(BaseExceptionGroup) as exc_info:
            decode_snapshot(wire)
    assert len(exc_info.value.exceptions) == 2
    assert all(
        isinstance(e, wool.ContextDecodeWarning) for e in exc_info.value.exceptions
    )


def test_decode_snapshot_flips_live_token_to_used():
    """Test decode_snapshot marks a live token as used when its id appears in consumed.

    Given:
        A live token minted by var.set() in a scoped context, and a
        wire context carrying that token's id in consumed_tokens.
    When:
        decode_snapshot is called on that wire context.
    Then:
        The live token's used property should be True.
    """
    # Arrange
    var = ContextVar(_unique("decode_flip_token"))
    with scoped_context():
        token = var.set("initial")
        token_id = token.id
        wire = protocol.Context(id=uuid4().hex)
        wire.vars.add(
            namespace=var.namespace,
            name=var.name,
            consumed_tokens=[token_id.hex],
        )

        # Act
        decode_snapshot(wire)

        # Assert
        assert token.used is True


# encode_snapshot / decode_snapshot are module-level functions; their
# property tests live at module scope alongside the example tests.
@given(
    values=st.lists(
        st.text() | st.integers() | st.lists(st.integers()),
        min_size=0,
        max_size=5,
    ),
    reset_count=st.integers(min_value=0, max_value=3),
)
@settings(max_examples=50)
def test_encode_decode_round_trip_with_an_arbitrary_snapshot(values, reset_count):
    """Test encode/decode round-trips multi-variable snapshots and reset signals.

    Given:
        A snapshot binding zero or more variables to arbitrary
        serializable values, plus a consumed-token log of zero or more
        reset signals keyed to those variables.
    When:
        The snapshot is encoded to the wire and decoded back.
    Then:
        The decoded snapshot should carry every variable binding, every
        consumed-token entry, and the original chain id.
    """
    # Arrange
    vars_ = [ContextVar(_unique("rt_snapshot")) for _ in values]
    data = {var: value for var, value in zip(vars_, values)}
    consumed = {
        uuid4(): (vars_[i].namespace, vars_[i].name)
        for i in range(min(reset_count, len(vars_)))
    }
    chain_id = uuid4()
    snapshot = Snapshot(
        chain_id=chain_id,
        owner=threading.get_ident(),
        data=data,
        consumed=consumed,
    )

    # Act
    decoded = decode_snapshot(encode_snapshot(snapshot))

    # Assert
    assert {var: decoded.data[var] for var in vars_} == data
    assert dict(decoded.consumed) == consumed
    assert decoded.chain_id == chain_id


def test_merge_snapshot_into_armed_context():
    """Test merge_snapshot folds incoming data into the active snapshot.

    Given:
        An armed context carrying one variable and an incoming
        snapshot carrying a different variable.
    When:
        merge_snapshot is called with the incoming snapshot.
    Then:
        The active snapshot should carry both variables and keep its
        original chain id.
    """
    # Arrange
    existing = ContextVar(_unique("merge_existing"))
    incoming_var = ContextVar(_unique("merge_incoming"))

    with scoped_context():
        existing.set("a")
        armed = current_snapshot()
        assert armed is not None
        original_chain = armed.chain_id
        incoming = Snapshot(
            chain_id=uuid4(),
            owner=threading.get_ident(),
            data={incoming_var: "b"},
        )

        # Act
        merge_snapshot(incoming)
        merged = current_snapshot()

        # Assert
        assert merged is not None
        assert merged.data[existing] == "a"
        assert merged.data[incoming_var] == "b"
        assert merged.chain_id == original_chain


def test_merge_snapshot_incoming_wins_on_overlap():
    """Test merge_snapshot lets the incoming snapshot win overlapping keys.

    Given:
        An armed context and an incoming snapshot that both bind the
        same variable to different values.
    When:
        merge_snapshot is called.
    Then:
        The merged snapshot should carry the incoming value.
    """
    # Arrange
    var = ContextVar(_unique("merge_overlap"))

    # Act
    with scoped_context():
        var.set("local")
        incoming = Snapshot(
            chain_id=uuid4(),
            owner=threading.get_ident(),
            data={var: "remote"},
        )
        merge_snapshot(incoming)

        # Assert
        assert var.get() == "remote"


def test_merge_snapshot_arms_unarmed_context():
    """Test merge_snapshot arms an unarmed context.

    Given:
        An unarmed context and an incoming snapshot with state.
    When:
        merge_snapshot is called.
    Then:
        The context should become armed and observe the merged value.
    """
    # Arrange
    var = ContextVar(_unique("merge_arm"))
    incoming = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={var: "armed"},
    )

    # Act
    with scoped_context():
        assert current_snapshot() is None
        merge_snapshot(incoming)

        # Assert
        assert current_snapshot() is not None
        assert var.get() == "armed"


def test_merge_snapshot_propagates_reset_signal():
    """Test merge_snapshot removes a variable that the incoming snapshot reset.

    Given:
        An armed context binding a variable, and an incoming snapshot
        that records a consumed token for that variable but does not
        re-bind it.
    When:
        merge_snapshot is called.
    Then:
        The variable should be removed from the active snapshot.
    """
    # Arrange
    var = ContextVar(_unique("merge_reset"))

    # Act
    with scoped_context():
        var.set("present")
        incoming = Snapshot(
            chain_id=uuid4(),
            owner=threading.get_ident(),
            consumed={uuid4(): (var.namespace, var.name)},
        )
        merge_snapshot(incoming)

        # Assert
        with pytest.raises(LookupError):
            var.get()


def test_reset_signal_propagates_through_encode_decode_merge():
    """Test a reset signal survives encode, decode, and merge end to end.

    Given:
        A receiver context binding a variable, and a wire context
        encoding a reset for that variable — a consumed token with no
        re-binding — decoded into a snapshot.
    When:
        The decoded snapshot is merged into the receiver context.
    Then:
        The receiver should observe LookupError on the variable — the
        reset signal survives the full encode -> decode -> merge wire
        path, not just an in-memory merge.
    """
    # Arrange
    var = ContextVar(_unique("e2e_reset"))
    token_id = uuid4()
    sender = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        consumed={token_id: (var.namespace, var.name)},
    )
    decoded = decode_snapshot(encode_snapshot(sender))

    # Act
    with scoped_context():
        var.set("present")
        merge_snapshot(decoded)

        # Assert
        with pytest.raises(LookupError):
            var.get()


def test_merge_snapshot_flips_live_token_to_used():
    """Test merge_snapshot marks a live token as used when its id appears in consumed.

    Given:
        A live token minted by var.set() in a scoped context, and an
        incoming snapshot carrying that token's id in its consumed log.
    When:
        merge_snapshot is called with the incoming snapshot.
    Then:
        The live token's used property should be True.
    """
    # Arrange
    var = ContextVar(_unique("merge_flip_token"))

    with scoped_context():
        token = var.set("initial")
        incoming = Snapshot(
            chain_id=uuid4(),
            owner=threading.get_ident(),
            consumed={token.id: (var.namespace, var.name)},
        )

        # Act
        merge_snapshot(incoming)

        # Assert
        assert token.used is True


def test_merge_snapshot_preserves_re_set_after_reset():
    """Test merge_snapshot does not remove a variable that was reset then re-set.

    Given:
        An incoming snapshot that carries both a consumed token for a
        variable and a new binding for that same variable in data
        (reset then immediately re-set).
    When:
        merge_snapshot is called.
    Then:
        The variable should remain in the active snapshot with the
        incoming value.
    """
    # Arrange
    var = ContextVar(_unique("merge_reset_rebind"))

    with scoped_context():
        var.set("old")
        token_id = uuid4()
        incoming = Snapshot(
            chain_id=uuid4(),
            owner=threading.get_ident(),
            data={var: "new"},
            consumed={token_id: (var.namespace, var.name)},
        )

        # Act
        merge_snapshot(incoming)

        # Assert
        assert var.get() == "new"


def test_fork_snapshot_mints_fresh_chain_id():
    """Test fork_snapshot mints a new chain id while copying data.

    Given:
        A snapshot carrying a variable binding.
    When:
        fork_snapshot is called.
    Then:
        The fork should carry a different chain id but the same
        variable bindings, and an empty consumed log.
    """
    # Arrange
    var = ContextVar(_unique("fork_chain"))
    original = Snapshot(
        chain_id=uuid4(),
        owner=threading.get_ident(),
        data={var: "x"},
        consumed={uuid4(): (var.namespace, var.name)},
    )

    # Act
    forked = fork_snapshot(original)

    # Assert
    assert forked.chain_id != original.chain_id
    assert dict(forked.data) == {var: "x"}
    assert dict(forked.consumed) == {}
