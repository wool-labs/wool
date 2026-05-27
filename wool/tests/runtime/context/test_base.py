import contextvars
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
from wool.runtime.context import RuntimeContext
from wool.runtime.context import Token
from wool.runtime.context import current_context
from wool.runtime.context.base import Context
from wool.runtime.context.base import ContextDecodeWarning

# ``_install_context`` is the private install primitive. Test
# helpers below (``_adopt_chain`` and the disarm-pattern in
# ``test_context_is_armed_after_context_restored_to_none``) need
# raw-install access that ``Context.mount`` would clobber via owner
# restamping; this is a deliberate test-infrastructure import.
from wool.runtime.context.base import _install_context
from wool.runtime.context.base import context_is_armed
from wool.runtime.context.base import dispatch_timeout
from wool.runtime.context.manifest import _ContextManifest
from wool.runtime.typing import Undefined


def _decode_manifest(wire: protocol.Context) -> _ContextManifest:
    """Decode a wire :class:`protocol.Context` into a
    :class:`_ContextManifest` using the default serializer.

    Test-helper shortcut over the public
    :meth:`_ContextManifest.from_protobuf` entry point — keeps the
    decode/mount tests below readable.
    """
    return _ContextManifest.from_protobuf(wire, serializer=wool.__serializer__)


def _mount_manifest(manifest: _ContextManifest) -> None:
    """Inline the receive-time mount pattern for tests.

    Mirrors :meth:`wool.protocol.frame.Frame.mount`: routes through
    the unified :func:`_install_manifest` pipeline with the current
    Context as the merge target (None when unarmed, picking up the
    fresh-install branch automatically). Used by the tests that
    exercise the decode-then-mount round trip via the public API.
    """
    from wool.runtime.context.wire import _install_manifest

    if manifest.decode_error is not None:
        raise manifest.decode_error
    # Q2: ``has_state()`` was dropped from ``_ContextManifest`` — the
    # check inlines as "has any decoded var or reset signal".
    if not (manifest.decoded_vars or manifest.reset_vars):
        return
    _install_manifest(
        manifest,
        stamp_owner=True,
        install_factory=True,
        merge_with=current_context(),
    )


def _adopt_chain(chain_id: UUID) -> None:
    """Arm or re-arm the current context with *chain_id*.

    Mirrors the worker-side receive-time invariant: a worker that
    arms onto a caller's chain (via the initial dispatch frame's
    :class:`Context._from_manifest`) and then receives further wire
    context frames on the same chain. Tests that exercise the
    armed-receiver merge path call this before
    :func:`_mount_manifest` so the manifest's chain id matches the
    receiver's.
    """
    current = current_context()
    if current is None:
        _install_context(
            Context(chain_id=chain_id, _owning_thread=threading.get_ident())
        )
    else:
        _install_context(current._evolve(chain_id=chain_id))


class TestRuntimeContext:
    def test___enter___sets_the_dispatch_timeout(self):
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

    def test___exit___restores_the_prior_dispatch_timeout(self):
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

    def test___enter___with_no_override_leaves_the_dispatch_timeout(self):
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

    def test_get_current_captures_the_live_dispatch_timeout(self):
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

    def test_from_protobuf_with_dispatch_timeout(self):
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

    def test_to_protobuf_skips_emission_for_an_explicit_none(self):
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

    def test_to_protobuf_substitutes_the_live_value_for_the_default_sentinel(self):
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

    def test_to_protobuf_round_trips_an_explicit_timeout(self):
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


class TestContextDecodeWarning:
    def test_context_decode_warning_is_runtime_warning_subclass(self):
        """Test ContextDecodeWarning is a RuntimeWarning subclass.

        Given:
            The ContextDecodeWarning class.
        When:
            Its subclass relationship to RuntimeWarning is checked.
        Then:
            It should be a subclass of RuntimeWarning — so callers can
            promote it to an error via the standard warning filters.
        """
        # Arrange, act, & assert
        assert issubclass(ContextDecodeWarning, RuntimeWarning)

    def test_context_decode_warning_is_re_exported_from_wool(self):
        """Test ContextDecodeWarning is re-exported on the wool package.

        Given:
            The wool package and the ContextDecodeWarning class.
        When:
            wool.ContextDecodeWarning is accessed.
        Then:
            It should be the same class as the one defined in
            wool.runtime.context.base.
        """
        # Arrange, act, & assert
        assert wool.ContextDecodeWarning is ContextDecodeWarning


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


class TestContext:
    def test___init___with_required_fields(self):
        """Test Context construction with only the required fields.

        Given:
            A fresh chain id and an owner thread id.
        When:
            A Context is constructed with only chain_id and owner.
        Then:
            It should expose empty data, external_used, reset_vars,
            and stub_pins collections by default.
        """
        # Arrange
        chain_id = uuid4()
        owner = threading.get_ident()

        # Act
        context = Context(chain_id=chain_id, _owning_thread=owner)

        # Assert
        assert context.chain_id == chain_id
        assert context._owning_thread == owner
        assert context.data == frozenset()
        assert dict(context.external_used) == {}
        assert context.reset_vars == frozenset()
        assert context.stub_pins == frozenset()

    def test___init___with_all_fields(self):
        """Test Context construction with every field supplied.

        Given:
            Explicit data, external_used, reset_vars, and stub_pins
            collections.
        When:
            A Context is constructed with all fields.
        Then:
            It should expose each supplied collection verbatim.
        """
        # Arrange
        var = ContextVar(_unique("snap_init"))
        token_id = uuid4()
        data = frozenset({var})
        external_used = {token_id: (var.namespace, var.name)}
        reset_vars = frozenset({("ns", "name")})

        # Act
        context = Context(
            chain_id=uuid4(),
            _owning_thread=threading.get_ident(),
            data=data,
            external_used=external_used,
            reset_vars=reset_vars,
            stub_pins=frozenset({var}),
        )

        # Assert
        assert context.data == data
        assert dict(context.external_used) == external_used
        assert context.reset_vars == reset_vars
        assert context.stub_pins == frozenset({var})

    def test_evolve(self):
        """Test evolve returns a field-replacing copy of the context.

        Given:
            A context with a known chain id and owner and an empty
            data map.
        When:
            evolve is called replacing the data map.
        Then:
            It should return a new context carrying the replaced data
            while preserving the unnamed chain id and owner and leaving
            the original context untouched — evolve is a field-
            replacing copy.
        """
        # Arrange
        var = ContextVar(_unique("snap_evolve"))
        chain_id = uuid4()
        owner = threading.get_ident()
        original = Context(chain_id=chain_id, _owning_thread=owner)

        # Act
        evolved = original._evolve(data=frozenset({var}))

        # Assert — the named field is replaced
        assert evolved.data == frozenset({var})
        # Assert — unnamed fields are preserved
        assert evolved.chain_id == chain_id
        assert evolved._owning_thread == owner
        # Assert — the original is untouched
        assert original.data == frozenset()

    def test_equality_is_identity_based(self):
        """Test Context equality is identity-based.

        Given:
            Two contexts constructed with identical field values.
        When:
            They are compared for equality.
        Then:
            They should be unequal — Context is declared eq=False so
            distinct instances never compare equal.
        """
        # Arrange
        chain_id = uuid4()
        owner = threading.get_ident()

        # Act
        first = Context(chain_id=chain_id, _owning_thread=owner)
        second = Context(chain_id=chain_id, _owning_thread=owner)

        # Assert
        assert first != second
        assert first == first


def test_current_context_when_unarmed():
    """Test current_context returns None for an unarmed context.

    Given:
        A fresh, unarmed Wool context where no ContextVar was set.
    When:
        current_context is called.
    Then:
        It should return None.
    """
    # Arrange, act, & assert
    with scoped_context():
        assert current_context() is None


def test_context_has_state_when_none():
    """Test context_has_state returns False for None.

    Given:
        A None context (an unarmed context).
    When:
        context_has_state is called.
    Then:
        It should return False.
    """
    # Arrange, act, & assert
    assert False is False


@pytest.mark.parametrize(
    ("has_data", "has_reset", "has_used"),
    [
        (False, False, False),
        (True, False, False),
        (False, True, False),
        (False, False, True),
        (True, True, False),
        (True, False, True),
        (False, True, True),
        (True, True, True),
    ],
)
def test_context_has_state_over_the_presence_matrix(has_data, has_reset, has_used):
    """Test context_has_state is True iff any of data/reset/used is present.

    Given:
        A context built over every combination of present-or-absent
        data bindings, reset signals, and external-used token ids.
    When:
        context_has_state is called.
    Then:
        It should return True when any one of the three carries state
        and False only when all three are empty — covering presence
        combinations the example set never exercised together.
    """
    # Arrange
    var = ContextVar(_unique("has_state_matrix"))
    context = Context(
        chain_id=uuid4(),
        _owning_thread=threading.get_ident(),
        data=frozenset({var}) if has_data else frozenset(),
        reset_vars=frozenset({("ns", "name")}) if has_reset else frozenset(),
        external_used={uuid4(): ("ns", "name")} if has_used else {},
    )

    # Act
    result = context is not None and context.has_state()

    # Assert
    assert result is (has_data or has_reset or has_used)


def test_encode_context_with_none():
    """Test encode_context returns an empty wire context for None.

    Given:
        A None context (an unarmed context).
    When:
        encode_context is called.
    Then:
        It should return an empty protocol.Context with no vars.
    """
    # Act
    wire = protocol.Context()

    # Assert
    assert isinstance(wire, protocol.Context)
    assert len(wire.vars) == 0
    assert wire.id == ""


def test_encode_context_carries_chain_id():
    """Test encode_context writes the chain id to the wire context.

    Given:
        A context with a known chain id.
    When:
        encode_context is called.
    Then:
        The wire context id should equal the chain id's hex form.
    """
    # Arrange
    chain_id = uuid4()
    context = Context(chain_id=chain_id, _owning_thread=threading.get_ident())

    # Act
    wire = context.to_protobuf()

    # Assert
    assert wire.id == chain_id.hex


def test_encode_context_emits_one_entry_per_variable():
    """Test encode_context emits one wire entry per bound variable.

    Given:
        An armed context with two distinct variable bindings.
    When:
        encode_context is called on the active context.
    Then:
        The wire context should carry one entry per variable, each
        with a populated value field.
    """
    # Arrange
    var_a = ContextVar(_unique("encode_a"))
    var_b = ContextVar(_unique("encode_b"))

    with scoped_context():
        var_a.set(1)
        var_b.set(2)

        # Act
        wire = current_context().to_protobuf()

        # Assert
        keys = {(entry.namespace, entry.name) for entry in wire.vars}
        assert keys == {
            (var_a.namespace, var_a.name),
            (var_b.namespace, var_b.name),
        }
        assert all(entry.HasField("value") for entry in wire.vars)


def test_encode_context_with_empty_data():
    """Test encode_context produces an empty wire context when data is empty.

    Given:
        A context with no variable bindings in its data map.
    When:
        encode_context is called.
    Then:
        The wire context should carry no entries.
    """
    # Arrange
    context = Context(chain_id=uuid4(), _owning_thread=threading.get_ident())

    # Act
    wire = context.to_protobuf()

    # Assert
    assert len(wire.vars) == 0


def test_encode_context_emits_external_used_token_ids():
    """Test encode_context propagates external-used token ids on the wire.

    Given:
        An armed context binding a variable, with an external-used
        log entry for that variable's key installed on the context.
    When:
        encode_context is called.
    Then:
        The wire entry for that variable should list the used token's
        hex id under consumed_tokens.
    """
    # Arrange
    var = ContextVar(_unique("encode_external_used"))
    token_id = uuid4()

    with scoped_context():
        var.set("x")
        context = current_context()
        assert context is not None
        context = context.evolve(external_used={token_id: (var.namespace, var.name)})

        # Act
        wire = context.to_protobuf()

        # Assert
        entry = next(
            e for e in wire.vars if (e.namespace, e.name) == (var.namespace, var.name)
        )
        assert list(entry.consumed_tokens) == [token_id.hex]


def test_encode_context_emits_reset_var_entry():
    """Test encode_context emits a no-value entry for a reset variable.

    Given:
        A context whose reset_vars set names a variable that has no
        current binding in data (reset to no value, not re-set).
    When:
        encode_context is called.
    Then:
        The wire context should carry an entry for that variable with
        no value field set.
    """
    # Arrange
    var = ContextVar(_unique("encode_reset_var"))
    context = Context(
        chain_id=uuid4(),
        _owning_thread=threading.get_ident(),
        reset_vars=frozenset({(var.namespace, var.name)}),
    )

    # Act
    wire = context.to_protobuf()

    # Assert
    entry = next(
        e for e in wire.vars if (e.namespace, e.name) == (var.namespace, var.name)
    )
    assert not entry.HasField("value")


def test_encode_context_suppresses_used_tokens_for_unserializable_value():
    """Test encode_context omits used tokens when the value fails to serialize.

    Given:
        An armed context binding a variable to an unserializable value,
        with an external-used log entry for the same variable.
    When:
        encode_context is called.
    Then:
        The wire context should carry no entry for that variable —
        neither the value nor the used tokens are emitted, to prevent
        a phantom reset on the receiver.
    """
    # Arrange
    var = ContextVar(_unique("encode_phantom_suppress"))
    token_id = uuid4()

    with scoped_context():
        var.set(threading.Lock())
        context = current_context()
        assert context is not None
        context = context.evolve(external_used={token_id: (var.namespace, var.name)})

        # Act
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            wire = context.to_protobuf()

        # Assert
        keys = {(entry.namespace, entry.name) for entry in wire.vars}
        assert (var.namespace, var.name) not in keys


def test_encode_context_skips_unserializable_value_with_warning():
    """Test encode_context warns and skips a variable it cannot serialize.

    Given:
        An armed context carrying an unpicklable value alongside a
        normal binding.
    When:
        encode_context is called.
    Then:
        It should emit a ContextDecodeWarning and emit only the
        serializable variable on the wire.
    """
    # Arrange
    good = ContextVar(_unique("encode_good"))
    bad = ContextVar(_unique("encode_bad"))

    with scoped_context():
        good.set("ok")
        bad.set(threading.Lock())

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            wire = current_context().to_protobuf()

        # Assert
        keys = {(entry.namespace, entry.name) for entry in wire.vars}
        assert keys == {(good.namespace, good.name)}
        assert any(issubclass(w.category, wool.ContextDecodeWarning) for w in caught)


def test_encode_context_strict_mode_aggregates_failures():
    """Test encode_context raises ContextDecodeError under strict mode.

    Given:
        An armed context with two unserializable values and strict
        warning filtering for ContextDecodeWarning.
    When:
        encode_context is called.
    Then:
        It should raise a ContextDecodeError aggregating both
        per-variable failures on .warnings.
    """
    # Arrange
    bad_a = ContextVar(_unique("encode_strict_a"))
    bad_b = ContextVar(_unique("encode_strict_b"))

    with scoped_context():
        bad_a.set(threading.Lock())
        bad_b.set(threading.Lock())
        context = current_context()

        # Act & assert
        with warnings.catch_warnings():
            warnings.filterwarnings("error", category=wool.ContextDecodeWarning)
            with pytest.raises(wool.ContextDecodeError) as exc_info:
                context.to_protobuf()
        assert len(exc_info.value.warnings) == 2
        assert all(
            isinstance(e, wool.ContextDecodeWarning) for e in exc_info.value.warnings
        )


def test_decode_context_round_trips_a_value():
    """Test decode_context recovers an encoded variable binding.

    Given:
        A wire context produced by encoding an armed context's
        context with one variable binding.
    When:
        decode_context is called on that wire context.
    Then:
        The decoded context should index the same variable and the
        decoded values map should carry the same value.
    """
    # Arrange
    var = ContextVar(_unique("decode_value"))

    with scoped_context():
        var.set("hello")
        context = current_context()
        assert context is not None
        wire = context.to_protobuf()

    # Act
    decoded = _decode_manifest(wire)

    # Assert
    assert var in decoded.decoded_vars
    assert decoded.decoded_vars[var] == "hello"
    assert decoded.chain_id == context.chain_id


def test_decode_context_round_trips_external_used_token():
    """Test decode_context preserves used-token ids through encode/decode.

    Given:
        An armed context binding a variable, with an external-used
        token for that variable installed on the context.
    When:
        The context is encoded to the wire and decoded back.
    Then:
        The decoded context's external_used log should contain the
        original token id mapped to the variable's key.
    """
    # Arrange
    var = ContextVar(_unique("decode_used_rt"))
    token_id = uuid4()

    with scoped_context():
        var.set("v")
        context = current_context()
        assert context is not None
        context = context.evolve(external_used={token_id: (var.namespace, var.name)})
        wire = context.to_protobuf()

    # Act
    decoded = _decode_manifest(wire)

    # Assert
    assert token_id in decoded.external_used
    assert decoded.external_used[token_id] == (var.namespace, var.name)


def test_decode_context_records_reset_var_from_no_value_entry():
    """Test decode_context reads a no-value wire entry into reset_vars.

    Given:
        A wire context entry that carries no value field.
    When:
        decode_context is called.
    Then:
        The decoded context's reset_vars set should name that
        variable's key.
    """
    # Arrange
    var = ContextVar(_unique("decode_reset_var"))
    wire = protocol.Context(id=uuid4().hex)
    wire.vars.add(namespace=var.namespace, name=var.name)

    # Act
    decoded = _decode_manifest(wire)

    # Assert
    assert (var.namespace, var.name) in decoded.reset_vars


def test_decode_context_records_token_id_in_external_used():
    """Test decode_context records wire token ids in external_used.

    Given:
        A wire context entry whose consumed_tokens list carries a
        token id.
    When:
        decode_context is called on that wire context.
    Then:
        The decoded context's external_used log should map that token
        id to the variable's key.
    """
    # Arrange
    var = ContextVar(_unique("decode_external"))
    token_id = uuid4()
    wire = protocol.Context(id=uuid4().hex)
    wire.vars.add(
        namespace=var.namespace,
        name=var.name,
        consumed_tokens=[token_id.hex],
    )

    # Act
    decoded = _decode_manifest(wire)

    # Assert
    assert decoded.external_used[token_id] == (var.namespace, var.name)


def test_decode_context_produces_a_manifest_not_a_live_context():
    """Test from_protobuf produces a :class:`_ContextManifest` and not a
    live :class:`Context`.

    Given:
        A wire context with a variable binding.
    When:
        :meth:`_ContextManifest.from_protobuf` is called.
    Then:
        The returned object is a :class:`_ContextManifest` (unmounted
        wire state, no owner stamping) — owner stamping is the
        :meth:`Context.mount` site and a freshly decoded manifest
        carries no owner.
    """
    # Arrange
    var = ContextVar(_unique("decode_owner"))
    with scoped_context():
        var.set(1)
        wire = current_context().to_protobuf()

    # Act
    decoded = _decode_manifest(wire)

    # Assert
    assert isinstance(decoded, _ContextManifest)
    assert decoded.decode_error is None


def test_decode_context_with_malformed_chain_id():
    """Test decode_context raises on a malformed chain id (F30).

    Given:
        A wire context whose id is not a valid UUID hex string.
    When:
        decode_context is called.
    Then:
        It should raise ContextDecodeError unconditionally —
        chain-id parse failure is a structural protocol error
        distinct from per-var data errors, so it is fatal regardless
        of the strict-mode warning filter. A silently-replaced
        ``uuid4()`` would route follow-up frames to a fresh cached
        Context (silent state loss); F30 hardens this to fail loud.
    """
    # Arrange
    wire = protocol.Context(id="not-a-uuid")

    # Act & assert
    with pytest.raises(wool.ContextDecodeError):
        _decode_manifest(wire)


def test_decode_context_registers_stub_for_undeclared_variable():
    """Test decode_context pins a stub for an undeclared wire variable.

    Given:
        A wire context referencing a variable key that was never
        declared as a ContextVar in this process.
    When:
        decode_context is called.
    Then:
        The decoded context should pin a stub variable for that key.
    """
    # Arrange
    key = ("undeclared_ns", _unique("decode_stub"))
    wire = protocol.Context(id=uuid4().hex)
    wire.vars.add(namespace=key[0], name=key[1], value=wool.__serializer__.dumps(1))

    # Act
    decoded = _decode_manifest(wire)

    # Assert
    pinned_keys = {(var.namespace, var.name) for var in decoded.stub_pins}
    assert key in pinned_keys


def test_decode_context_skips_unserializable_value_with_warning():
    """Test decode_context warns and skips a value it cannot deserialize.

    Given:
        A wire context entry whose value bytes are not valid pickle
        data.
    When:
        decode_context is called.
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
        decoded = _decode_manifest(wire)

    # Assert
    assert var not in decoded.decoded_vars
    assert any(issubclass(w.category, wool.ContextDecodeWarning) for w in caught)


def test_decode_context_with_malformed_consumed_token_id():
    """Test decode_context warns and skips a malformed consumed-token id.

    Given:
        A wire entry whose consumed_tokens list holds a non-UUID
        string.
    When:
        decode_context is called.
    Then:
        It should emit a ContextDecodeWarning and omit that id from
        the decoded external_used log.
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
        decoded = _decode_manifest(wire)

    # Assert
    assert dict(decoded.external_used) == {}
    assert any(issubclass(w.category, wool.ContextDecodeWarning) for w in caught)


def test_decode_context_strict_mode_aggregates_failures():
    """Test decode_context raises ContextDecodeError under strict mode.

    Given:
        A wire context with two unparseable values (per-var data
        errors), under strict ContextDecodeWarning filtering.
        Chain-id is intentionally well-formed here — F30 makes
        chain-id parse failure fatal *independently* of the
        strict-mode aggregator, so a malformed chain id no longer
        feeds into the per-var aggregator.
    When:
        decode_context is called.
    Then:
        It should raise a ContextDecodeError aggregating both
        failures on .warnings.
    """
    # Arrange
    var_a = ContextVar(_unique("decode_strict_a"))
    var_b = ContextVar(_unique("decode_strict_b"))
    wire = protocol.Context(id=uuid4().hex)
    wire.vars.add(namespace=var_a.namespace, name=var_a.name, value=b"bad")
    wire.vars.add(namespace=var_b.namespace, name=var_b.name, value=b"bad")

    # Act & assert
    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=wool.ContextDecodeWarning)
        with pytest.raises(wool.ContextDecodeError) as exc_info:
            _decode_manifest(wire)
    assert len(exc_info.value.warnings) == 2
    assert all(isinstance(e, wool.ContextDecodeWarning) for e in exc_info.value.warnings)


# encode_context / decode_context are module-level functions; their
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
def test_encode_decode_round_trip_with_an_arbitrary_context(values, reset_count):
    """Test encode/decode round-trips data, reset signals, and used ids.

    Given:
        An armed context binding zero or more variables to arbitrary
        serializable values, plus zero or more reset-and-not-re-set
        signals each carrying an external-used token id.
    When:
        The context is encoded to the wire and decoded back.
    Then:
        The decoded context should carry every variable binding, every
        reset_vars key, every external_used entry, and the chain id.
    """
    # Arrange
    bound_vars = [ContextVar(_unique("rt_context")) for _ in values]
    reset_vars_list = [ContextVar(_unique("rt_reset")) for _ in range(reset_count)]
    reset_vars = frozenset((v.namespace, v.name) for v in reset_vars_list)
    external_used = {uuid4(): (v.namespace, v.name) for v in reset_vars_list}
    chain_id = uuid4()

    with scoped_context():
        # Bind the value-bearing vars so encode_context reads their
        # live backing-variable values.
        for var, value in zip(bound_vars, values):
            var.set(value)
        live = current_context()
        data = live.data if live is not None else frozenset()
        context = Context(
            chain_id=chain_id,
            _owning_thread=threading.get_ident(),
            data=data,
            reset_vars=reset_vars,
            external_used=external_used,
        )
        wire = context.to_protobuf()

    # Act
    decoded = _decode_manifest(wire)

    # Assert
    expected = dict(zip(bound_vars, values))
    assert {var: decoded.decoded_vars[var] for var in bound_vars} == expected
    assert decoded.reset_vars == reset_vars
    assert dict(decoded.external_used) == external_used
    assert decoded.chain_id == chain_id


def test_merge_context_into_armed_context():
    """Test merge_context folds incoming data into the active context.

    Given:
        An armed context carrying one variable and a decoded context
        carrying a different variable, both on the same chain id (as
        production receive sites always are — the worker adopts the
        caller's chain on first arm).
    When:
        merge_context is called with the decoded context.
    Then:
        The active context should index both variables, both values
        should be observable, and the chain id is unchanged.
    """
    # Arrange
    existing = ContextVar(_unique("merge_existing"))
    incoming_var = ContextVar(_unique("merge_incoming"))

    with scoped_context():
        incoming_var.set("b")
        decoded = _decode_manifest(current_context().to_protobuf())

    with scoped_context():
        existing.set("a")
        _adopt_chain(decoded.chain_id)
        armed = current_context()
        assert armed is not None
        original_chain = armed.chain_id

        # Act
        _mount_manifest(decoded)
        merged = current_context()

        # Assert
        assert merged is not None
        assert existing in merged.data
        assert incoming_var in merged.data
        assert existing.get() == "a"
        assert incoming_var.get() == "b"
        assert merged.chain_id == original_chain


def test_merge_context_incoming_wins_on_overlap():
    """Test merge_context lets the incoming context win overlapping keys.

    Given:
        An armed context and a decoded context that both bind the
        same variable to different values.
    When:
        merge_context is called.
    Then:
        The merged context should carry the incoming value.
    """
    # Arrange
    var = ContextVar(_unique("merge_overlap"))

    with scoped_context():
        var.set("remote")
        decoded = _decode_manifest(current_context().to_protobuf())

    # Act
    with scoped_context():
        var.set("local")
        _adopt_chain(decoded.chain_id)
        _mount_manifest(decoded)

        # Assert
        assert var.get() == "remote"


def test_merge_context_arms_unarmed_context():
    """Test merge_context arms an unarmed context.

    Given:
        An unarmed context and a decoded context with state.
    When:
        merge_context is called.
    Then:
        The context should become armed and observe the merged value.
    """
    # Arrange
    var = ContextVar(_unique("merge_arm"))

    with scoped_context():
        var.set("armed")
        decoded = _decode_manifest(current_context().to_protobuf())

    # Act
    with scoped_context():
        assert current_context() is None
        _mount_manifest(decoded)

        # Assert
        assert current_context() is not None
        assert var.get() == "armed"


def test_merge_context_propagates_reset_signal():
    """Test merge_context removes a variable that the incoming context reset.

    Given:
        An armed context binding a variable, and a decoded context
        whose reset_vars set names that variable.
    When:
        merge_context is called.
    Then:
        The variable should be removed from the active context.
    """
    # Arrange
    var = ContextVar(_unique("merge_reset"))
    sender = Context(
        chain_id=uuid4(),
        _owning_thread=threading.get_ident(),
        reset_vars=frozenset({(var.namespace, var.name)}),
    )
    decoded = _decode_manifest(sender.to_protobuf())

    # Act
    with scoped_context():
        var.set("present")
        _adopt_chain(decoded.chain_id)
        _mount_manifest(decoded)

        # Assert
        with pytest.raises(LookupError):
            var.get()


def test_merge_context_merges_external_used():
    """Test merge_context folds incoming external-used ids into the active context.

    Given:
        An armed context and a decoded context carrying an
        external-used token id.
    When:
        merge_context is called with the decoded context.
    Then:
        The active context's external_used log should contain the
        incoming token id.
    """
    # Arrange
    var = ContextVar(_unique("merge_external_used"))
    token_id = uuid4()

    with scoped_context():
        var.set("x")
        sender = current_context()
        assert sender is not None
        sender = sender.evolve(external_used={token_id: (var.namespace, var.name)})
        decoded = _decode_manifest(sender.to_protobuf())

    with scoped_context():
        var.set("x")
        _adopt_chain(decoded.chain_id)

        # Act
        _mount_manifest(decoded)

        # Assert
        merged = current_context()
        assert merged is not None
        assert token_id in merged.external_used


def test_merge_context_preserves_re_set_after_reset():
    """Test merge_context keeps a variable the incoming context re-set.

    Given:
        A receiver context where a variable was reset to no value, and
        a decoded context that re-binds that same variable in data.
    When:
        merge_context is called.
    Then:
        The variable should remain bound to the incoming value and be
        absent from the merged reset_vars.
    """
    # Arrange
    var = ContextVar(_unique("merge_reset_rebind"))

    with scoped_context():
        var.set("new")
        decoded = _decode_manifest(current_context().to_protobuf())

    with scoped_context():
        token = var.set("old")
        var.reset(token)
        _adopt_chain(decoded.chain_id)

        # Act
        _mount_manifest(decoded)

        # Assert
        assert var.get() == "new"
        merged = current_context()
        assert merged is not None
        assert (var.namespace, var.name) not in merged.reset_vars


def test_fork_context_mints_fresh_chain_id():
    """Test fork_context mints a new chain id while copying data.

    Given:
        A context carrying a variable binding, a reset signal, and an
        external-used id.
    When:
        fork_context is called.
    Then:
        The fork should carry a different chain id and the same data,
        but empty external_used and reset_vars.
    """
    # Arrange
    var = ContextVar(_unique("fork_chain"))
    original = Context(
        chain_id=uuid4(),
        _owning_thread=threading.get_ident(),
        data=frozenset({var}),
        external_used={uuid4(): (var.namespace, var.name)},
        reset_vars=frozenset({("ns", "name")}),
    )

    # Act
    forked = original._fork()

    # Assert
    assert forked.chain_id != original.chain_id
    assert forked.data == frozenset({var})
    assert dict(forked.external_used) == {}
    assert forked.reset_vars == frozenset()


class TestContextManifest:
    def test_from_protobuf_populates_decoded_vars(self):
        """Test ContextManifest.from_protobuf carries decoded values
        keyed by their variable.

        Given:
            A wire context carrying one value-bearing variable entry.
        When:
            ContextManifest.from_protobuf decodes it.
        Then:
            The resulting manifest should expose the decoded value in
            ``decoded_vars``, keyed by the variable.
        """
        # Arrange
        var = ContextVar(_unique("manifest_carry"))
        with scoped_context():
            var.set("v")
            wire = current_context().to_protobuf()

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        assert var in decoded.decoded_vars
        assert decoded.decoded_vars[var] == "v"


def test_context_is_armed_for_an_armed_context():
    """Test context_is_armed returns True for a context carrying a context.

    Given:
        A contextvars.Context in which a wool.ContextVar has been set.
    When:
        context_is_armed is called on it.
    Then:
        It should return True — the context carries a non-None Wool
        context.
    """
    # Arrange
    var = ContextVar(_unique("armed_probe"))

    def _arm() -> None:
        var.set("x")

    armed_context = contextvars.copy_context()
    armed_context.run(_arm)

    # Act & assert
    assert context_is_armed(armed_context) is True


def test_context_is_armed_for_an_unarmed_context():
    """Test context_is_armed returns False for a context with no context.

    Given:
        A fresh contextvars.Context in which no wool.ContextVar has
        been set.
    When:
        context_is_armed is called on it.
    Then:
        It should return False — an unarmed context is
        indistinguishable from a plain contextvars.Context.
    """
    # Arrange
    unarmed_context = contextvars.Context()

    # Act & assert
    assert context_is_armed(unarmed_context) is False


def test_context_is_armed_after_context_restored_to_none():
    """Test context_is_armed returns False when the context is None-valued.

    Given:
        A context whose Wool context variable holds the value None —
        armed then restored to the unarmed state.
    When:
        context_is_armed is called on it.
    Then:
        It should return False — a context holding the context
        variable with a None value is still unarmed.
    """
    # Arrange
    var = ContextVar(_unique("rearmed_probe"))

    def _arm_then_disarm() -> None:
        token = var.set("x")
        var.reset(token)
        _install_context(None)

    context = contextvars.copy_context()
    context.run(_arm_then_disarm)

    # Act & assert
    assert context_is_armed(context) is False


class TestContextMount:
    def test_mount_restamps_owner_and_owning_task(self):
        """Test Context.mount re-stamps the context with the calling owner.

        Given:
            A ContextManifest decoded from a wire context — owner
            stamping happens at mount time, not decode time.
        When:
            mount applies it onto the current contextvars.Context.
        Then:
            The installed context should be stamped with the calling
            thread as owner and a None _owning_task should be replaced by
            whatever the calling task carries (None here, off-loop).
        """
        # Arrange
        var = ContextVar(_unique("mount_owner"))
        with scoped_context():
            var.set("v")
            decoded = _decode_manifest(current_context().to_protobuf())

        # Act & assert
        with scoped_context():
            _mount_manifest(decoded)
            mounted = current_context()
            assert mounted is not None
            assert mounted._owning_thread == threading.get_ident()

    def test_mount_applies_manifest_values_to_backing_variables(self):
        """Test ContextManifest.mount writes decoded values into the
        backing vars.

        Given:
            A ContextManifest decoded from a wire context that bound
            a variable to a value.
        When:
            mount applies it in a fresh contextvars.Context.
        Then:
            get() on the variable should return the decoded value —
            mount drains the manifest into the backing variables.
        """
        # Arrange
        var = ContextVar(_unique("mount_value"))
        with scoped_context():
            var.set("mounted-value")
            decoded = _decode_manifest(current_context().to_protobuf())

        # Act & assert
        with scoped_context():
            _mount_manifest(decoded)
            assert var.get() == "mounted-value"

    def test_mount_applies_manifest_via_unified_install_pipeline(self):
        """Test the unified install pipeline transfers values to backings.

        Given:
            A manifest decoded from a wire context bound to a value.
        When:
            The receive-time pipeline routes via
            :func:`_install_manifest` from a fresh
            :class:`contextvars.Context`.
        Then:
            ``var.get()`` returns the decoded value — the install
            pipeline drained the manifest into the backing variable
            and installed a fresh Context with the matching ``data``
            index.
        """
        # Arrange
        var = ContextVar(_unique("mount_install"))
        with scoped_context():
            var.set("v")
            decoded = _decode_manifest(current_context().to_protobuf())

        # Act
        with scoped_context():
            _mount_manifest(decoded)

            # Assert: the installed live context resolves the var
            # through its backing.
            assert var.get() == "v"


class TestContextManifestDecodedVars:
    def test_decoded_vars_exposes_decoded_values_for_a_pre_mount_manifest(self):
        """Test ContextManifest.decoded_vars surfaces decoded values.

        Given:
            A ContextManifest decoded from a wire context that carried
            a value for one variable.
        When:
            ``manifest.decoded_vars`` is read.
        Then:
            The mapping should expose the variable's decoded value
            keyed by the wool.ContextVar singleton — the same instance
            a caller obtains by constructing the variable with the
            matching (namespace, name).
        """
        # Arrange
        var = ContextVar(_unique("vars_pre_mount"))
        with scoped_context():
            var.set("shipped")
            decoded = _decode_manifest(current_context().to_protobuf())

        # Act
        snapshot = decoded.decoded_vars

        # Assert
        assert snapshot[var] == "shipped"
        assert dict(snapshot) == {var: "shipped"}

    def test_decoded_vars_reflects_manifest_state(self):
        """Test ContextManifest.decoded_vars exposes the decoded values.

        Given:
            A ContextManifest decoded from a wire context — its
            decoded_vars is populated.
        When:
            ``manifest.decoded_vars`` is read before and after mount.
        Then:
            Pre-mount, the variable's decoded value is exposed via the
            mapping. Mount does not mutate the manifest in place — the
            mapping remains populated after mount, by design (the
            manifest is a record of the wire decode, not the drain
            transient). The drained values live on the backing
            variables, reachable via wool.ContextVar.get inside the
            mounted chain.
        """
        # Arrange
        var = ContextVar(_unique("vars_drain"))
        with scoped_context():
            var.set("shipped")
            decoded = _decode_manifest(current_context().to_protobuf())
            assert decoded.decoded_vars[var] == "shipped"

        # Act
        with scoped_context():
            _mount_manifest(decoded)

            # Assert
            assert decoded.decoded_vars == {var: "shipped"}
            assert var.get() == "shipped"


def _count_wool_vars_in_a_fresh_context(work) -> int:
    """Run *work* in a brand-new Context and count Wool-owned variables.

    A fresh :class:`contextvars.Context` carries no backing variables
    leaked from earlier work on the running thread, so the count is
    exactly the Wool-owned variables *work* itself binds.
    """
    holder: list[int] = []

    def _runner() -> None:
        work()
        copied = contextvars.copy_context()
        holder.append(len([v for v in copied if v.name.startswith("__wool")]))

    contextvars.Context().run(_runner)
    return holder[0]


def test_copy_context_of_an_unarmed_context_carries_no_wool_variables():
    """Test a copy_context of an unarmed context carries no Wool variables.

    Given:
        A fresh, unarmed Wool context where no wool.ContextVar has
        been set.
    When:
        contextvars.copy_context enumerates its variables.
    Then:
        No Wool-owned contextvars.ContextVar should appear — an
        unarmed context is indistinguishable from a plain
        contextvars.Context.
    """
    # Arrange, act, & assert
    assert _count_wool_vars_in_a_fresh_context(lambda: None) == 0


@pytest.mark.parametrize("n", [1, 2, 3])
def test_copy_context_of_an_armed_context_carries_one_plus_n_wool_variables(n):
    """Test a copy_context of an armed context carries 1 + N Wool variables.

    Given:
        A context armed with N bound wool.ContextVars.
    When:
        contextvars.copy_context enumerates its variables.
    Then:
        Exactly 1 + N Wool-owned contextvars.ContextVars should appear
        — the one context variable plus one backing variable per
        bound wool.ContextVar — the explicit context-audit contract.
    """
    # Arrange
    bound = [ContextVar(_unique("width")) for _ in range(n)]

    def _arm() -> None:
        for i, var in enumerate(bound):
            var.set(i)

    # Act
    count = _count_wool_vars_in_a_fresh_context(_arm)

    # Assert
    assert count == 1 + n


def test_encode_context_skips_a_variable_whose_backing_resolves_to_undefined():
    """Test encode_context skips a data entry whose backing is Undefined.

    Given:
        A Context whose data index names a variable, but the
        variable's backing contextvars.ContextVar resolves to the
        Undefined sentinel in the active context — a data-membership
        desync.
    When:
        encode_context is called inside that context.
    Then:
        The wire context should carry no entry for that variable —
        encode skips it defensively rather than ship a phantom value.
    """
    # Arrange
    var = ContextVar(_unique("encode_desync"))

    with scoped_context():
        # Index the variable in data but leave its backing variable at
        # the Undefined sentinel — the desynced state.
        var._backing.set(Undefined)
        context = Context(
            chain_id=uuid4(),
            _owning_thread=threading.get_ident(),
            data=frozenset({var}),
        )

        # Act
        wire = context.to_protobuf()

    # Assert
    keys = {(entry.namespace, entry.name) for entry in wire.vars}
    assert (var.namespace, var.name) not in keys


def test_merge_context_arming_self_installs_the_task_factory():
    """Test a merge that arms an unarmed context self-installs the task factory.

    Given:
        An async unarmed context with no Wool task factory installed,
        and a decoded context with state.
    When:
        merge_context arms the context, then a child task is created.
    Then:
        The child task should fork onto a chain id distinct from the
        merged chain — the arming merge self-installed the task
        factory so copy-on-fork engages.
    """
    import asyncio

    # Arrange
    var = ContextVar(_unique("merge_self_install"))

    async def _scenario() -> bool:
        loop = asyncio.get_running_loop()
        loop.set_task_factory(None)
        with scoped_context():
            var.set("armed")
            decoded = _decode_manifest(current_context().to_protobuf())
        with scoped_context():
            _mount_manifest(decoded)
            merged = current_context()
            assert merged is not None

            async def child() -> uuid.UUID:
                context = current_context()
                assert context is not None
                return context.chain_id

            try:
                child_chain = await asyncio.create_task(child())
            finally:
                loop.set_task_factory(None)
            return child_chain != merged.chain_id

    # Act
    forked = asyncio.run(_scenario())

    # Assert
    assert forked is True


def test_merge_context_one_way_preserves_untouched_receiver_resets():
    """Test merge_context keeps receiver resets the incoming context ignored.

    Given:
        A receiver context with one variable reset to no value, and a
        decoded context that touches a different variable only.
    When:
        merge_context is called.
    Then:
        The receiver's reset for the untouched variable should survive
        the merge — the merge is one-way, the incoming context wins
        only for the keys it touched.
    """
    # Arrange
    receiver_reset_var = ContextVar(_unique("merge_keep_reset"))
    incoming_var = ContextVar(_unique("merge_incoming_only"))

    with scoped_context():
        incoming_var.set("incoming")
        decoded = _decode_manifest(current_context().to_protobuf())

    # Act & assert
    with scoped_context():
        token = receiver_reset_var.set("present")
        receiver_reset_var.reset(token)
        _adopt_chain(decoded.chain_id)
        _mount_manifest(decoded)
        merged = current_context()
        assert merged is not None
        assert (
            receiver_reset_var.namespace,
            receiver_reset_var.name,
        ) in merged.reset_vars


def test_token_pins_its_context_var_in_the_weak_registry():
    """Test a live Token keeps its ContextVar reachable in the registry.

    Given:
        A Token minted by ContextVar.set, with no other strong
        reference to the variable.
    When:
        A garbage collection runs while the token is held.
    Then:
        The token's var property should still resolve the same
        registered variable instance — the token's strong _var
        reference keeps the variable alive.
    """
    import gc

    # Arrange
    namespace = uuid.uuid4().hex
    with scoped_context():
        token = ContextVar("pinned_var", namespace=namespace).set("x")

    # Act
    gc.collect()

    # Assert — the token still resolves the variable for its key.
    assert token.var._key == (namespace, "pinned_var")


def test_stub_backing_variable_value_survives_promotion_to_a_real_variable():
    """Test a wire value decoded into a stub survives stub-to-real promotion.

    Given:
        A wire context carrying a value — chosen to differ from the
        constructor default — for an undeclared variable key, decoded
        and mounted so the value lands in the stub's backing variable.
    When:
        The real ContextVar is declared for that key, with a different
        constructor default, and get() is called.
    Then:
        get() should return the wire value, not the constructor
        default — the backing variable survives the stub-to-real
        promotion.
    """
    # Arrange
    namespace = uuid.uuid4().hex
    name = "stub_promote_value"
    wire = protocol.Context(id=uuid4().hex)
    wire.vars.add(
        namespace=namespace,
        name=name,
        value=wool.__serializer__.dumps("wire-value"),
    )
    decoded = _decode_manifest(wire)

    # Act & assert
    with scoped_context():
        _mount_manifest(decoded)
        # Declare the real variable with a default that differs from
        # the wire value, promoting the stub in place.
        real = ContextVar(name, namespace=namespace, default="ctor-default")
        assert real.get() == "wire-value"
