"""Unit tests for the wire-abstraction types — ChainManifest (the decoded
chain snapshot) and ContextVarManifest (the per-variable identity layer)."""

import asyncio
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
from tests.helpers import _unique
from tests.helpers import scoped_context
from wool import protocol
from wool.runtime.context.chain import Chain
from wool.runtime.context.exceptions import TaskFactoryDisplaced
from wool.runtime.context.manifest import ChainManifest
from wool.runtime.context.manifest import ContextVarManifest
from wool.runtime.context.manifest import resolve_stub
from wool.runtime.context.var import ContextVar
from wool.runtime.typing import Undefined


def _decode_manifest(wire: protocol.ChainManifest) -> ChainManifest:
    """Decode a wire `protocol.ChainManifest` into a
    `ChainManifest` using the default serializer.

    Test-helper shortcut over the public
    `ChainManifest.from_protobuf` entry point — keeps the
    decode/mount tests below readable. Raises ``ChainSerializationError``
    under strict mode, just as the production decode does.
    """
    return ChainManifest.from_protobuf(wire, serializer=wool.__serializer__)


def _mount_manifest(manifest: ChainManifest) -> None:
    """Inline the receive-time mount pattern for tests.

    Mirrors `wool.runtime.worker.frame.Frame.mount`: routes through
    `wool.runtime.context.chain.Chain.from_manifest` with the
    current Chain as the merge target (None when unarmed, picking up the
    fresh-install branch automatically). Used by the tests that exercise
    the decode-then-mount round trip via the public API.
    """
    if not (manifest.vars or manifest.resets):
        return
    Chain.from_manifest(
        manifest,
        owned=True,
        merge_with=wool.__chain__.get(None),
    )


def _adopt_chain(chain_id: UUID) -> None:
    """Arm or re-arm the current context with *chain_id*.

    Mirrors the worker-side receive-time invariant: a worker that arms
    onto a caller's chain and then receives further chain-manifest frames
    on the same chain. Tests that exercise the armed-receiver merge path
    call this before `_mount_manifest` so the manifest's chain id
    matches the receiver's.
    """
    current = wool.__chain__.get(None)
    if current is None:
        wool.__chain__.set(Chain(id=chain_id, thread=threading.get_ident()))
    else:
        wool.__chain__.set(current._evolve(id=chain_id))


class TestChainManifest:
    def test_from_protobuf_should_round_trip_value(self):
        """Test from_protobuf recovers an encoded variable binding.

        Given:
            A chain manifest produced by encoding an armed chain
            with one variable binding.
        When:
            from_protobuf is called on that chain manifest.
        Then:
            The decoded chain manifest should index the same variable and the
            decoded values map should carry the same value.
        """
        # Arrange
        var = ContextVar(_unique("decode_value"))

        with scoped_context():
            var.set("hello")
            context = wool.__chain__.get(None)
            assert context is not None
            wire = context.to_manifest().to_protobuf()

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        assert var in decoded.vars
        assert decoded.vars[var] == "hello"
        assert decoded.id == context.id

    def test_from_protobuf_should_record_reset_var_from_no_value_entry(self):
        """Test from_protobuf reads a no-value wire entry into resets.

        Given:
            A chain manifest entry that carries no value field.
        When:
            from_protobuf is called.
        Then:
            The decoded chain manifest's resets set should name that
            variable's key.
        """
        # Arrange
        var = ContextVar(_unique("decode_reset_var"))
        wire = protocol.ChainManifest(id=uuid4().hex)
        wire.vars.add(namespace=var.namespace, name=var.name)

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        assert (var.namespace, var.name) in decoded.resets

    def test_from_protobuf_should_produce_manifest_not_live_context(self):
        """Test from_protobuf produces a `ChainManifest` and not a
        live `Chain`.

        Given:
            A chain manifest with a variable binding.
        When:
            `ChainManifest.from_protobuf` is called.
        Then:
            The returned object is a `ChainManifest` (unmounted
            wire state, no owner stamping) — owner stamping is the
            `Chain.mount` site and a freshly decoded manifest
            carries no owner.
        """
        # Arrange
        var = ContextVar(_unique("decode_owner"))
        with scoped_context():
            var.set(1)
            wire = wool.__chain__.get().to_manifest().to_protobuf()

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        assert isinstance(decoded, ChainManifest)

    def test_from_protobuf_should_raise_when_malformed_chain_id(self):
        """Test from_protobuf raises on a malformed chain id.

        Given:
            A chain manifest whose id is not a valid UUID hex string.
        When:
            from_protobuf is called.
        Then:
            It should raise ChainSerializationError unconditionally —
            chain-id parse failure is a structural protocol error
            distinct from per-var data errors, so it is fatal regardless
            of the strict-mode warning filter. A silently-replaced
            ``uuid4()`` would route follow-up frames to a fresh cached
            Chain (silent state loss), so the decode fails loud instead.
            The aggregated warning carries the decode direction, no
            var_key (the failure is structural, not per-variable), and
            the underlying ValueError as cause — the same ValueError
            chained on the error's __cause__.
        """
        # Arrange
        wire = protocol.ChainManifest(id="not-a-uuid")

        # Act & assert
        with pytest.raises(wool.ChainSerializationError) as exc_info:
            _decode_manifest(wire)
        warning = exc_info.value.warnings[0]
        assert warning.direction == "decode"
        assert warning.var_key is None
        assert isinstance(warning.cause, ValueError)
        assert exc_info.value.__cause__ is warning.cause

    def test_from_protobuf_should_register_stub_when_undeclared_variable(self):
        """Test from_protobuf pins a stub for an undeclared wire variable.

        Given:
            A chain manifest referencing a variable key that was never
            declared as a ContextVar in this process.
        When:
            from_protobuf is called.
        Then:
            The decoded chain manifest should pin a stub variable for that key.
        """
        # Arrange
        key = ("undeclared_ns", _unique("decode_stub"))
        wire = protocol.ChainManifest(id=uuid4().hex)
        wire.vars.add(namespace=key[0], name=key[1], value=wool.__serializer__.dumps(1))

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        pinned_keys = {(var.namespace, var.name) for var in decoded.stubs}
        assert key in pinned_keys

    def test_from_protobuf_should_warn_and_skip_unserializable_value(self):
        """Test from_protobuf warns and skips a value it cannot deserialize.

        Given:
            A chain manifest entry whose value bytes are not valid pickle
            data.
        When:
            from_protobuf is called.
        Then:
            It should emit a SerializationWarning carrying the failed
            variable's key, the decode direction, and the underlying
            cause, and omit that variable from the decoded data.
        """
        # Arrange
        var = ContextVar(_unique("decode_bad_value"))
        wire = protocol.ChainManifest(id=uuid4().hex)
        wire.vars.add(namespace=var.namespace, name=var.name, value=b"not-pickle")

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            decoded = _decode_manifest(wire)

        # Assert
        assert var not in decoded.vars
        emitted = [
            w for w in caught if issubclass(w.category, wool.SerializationWarning)
        ]
        assert emitted
        warning = emitted[0].message
        assert isinstance(warning, wool.SerializationWarning)
        assert warning.var_key == (var.namespace, var.name)
        assert warning.direction == "decode"
        assert warning.cause is not None

    def test_from_protobuf_should_aggregate_failures_when_strict_mode(self):
        """Test from_protobuf raises ChainSerializationError under strict mode.

        Given:
            A chain manifest with two unparseable values (per-var data
            errors), under strict SerializationWarning filtering.
            Chain-id is intentionally well-formed here — a chain-id parse
            failure is fatal *independently* of the strict-mode
            aggregator, so a malformed chain id never feeds into the
            per-var aggregator.
        When:
            from_protobuf is called.
        Then:
            It should raise a ChainSerializationError aggregating both
            failures on .warnings, each carrying its variable's key, the
            decode direction, and the underlying cause.
        """
        # Arrange
        var_a = ContextVar(_unique("decode_strict_a"))
        var_b = ContextVar(_unique("decode_strict_b"))
        wire = protocol.ChainManifest(id=uuid4().hex)
        wire.vars.add(namespace=var_a.namespace, name=var_a.name, value=b"bad")
        wire.vars.add(namespace=var_b.namespace, name=var_b.name, value=b"bad")

        # Act & assert
        with warnings.catch_warnings():
            warnings.filterwarnings("error", category=wool.SerializationWarning)
            with pytest.raises(wool.ChainSerializationError) as exc_info:
                _decode_manifest(wire)
        assert len(exc_info.value.warnings) == 2
        assert all(
            isinstance(e, wool.SerializationWarning) for e in exc_info.value.warnings
        )
        assert {w.var_key for w in exc_info.value.warnings} == {
            (var_a.namespace, var_a.name),
            (var_b.namespace, var_b.name),
        }
        assert all(w.direction == "decode" for w in exc_info.value.warnings)
        assert all(w.cause is not None for w in exc_info.value.warnings)

    def test_from_protobuf_should_populate_vars(self):
        """Test ChainManifest.from_protobuf carries decoded values
        keyed by their variable.

        Given:
            A chain manifest carrying one value-bearing variable entry.
        When:
            ChainManifest.from_protobuf decodes it.
        Then:
            The resulting manifest should expose the decoded value in
            ``vars``, keyed by the variable.
        """
        # Arrange
        var = ContextVar(_unique("manifest_carry"))
        with scoped_context():
            var.set("v")
            wire = wool.__chain__.get().to_manifest().to_protobuf()

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        assert var in decoded.vars
        assert decoded.vars[var] == "v"

    def test_from_protobuf_should_warn_when_duplicate_var_keys(self):
        """Test ChainManifest.from_protobuf warns on duplicate var keys.

        Given:
            A chain manifest whose ``vars`` list carries two entries
            with the same ``(namespace, name)`` key — a malformed
            sender that duplicated an entry rather than overwriting.
        When:
            ChainManifest.from_protobuf decodes it.
        Then:
            A SerializationWarning is emitted naming the duplicate
            key — carrying the duplicated var_key, the decode
            direction, and no cause — and the first occurrence wins
            in the decoded manifest (the second is dropped). The
            decode otherwise succeeds — duplicate keys are
            recoverable; the decode does not raise outside strict
            mode.
        """
        # Arrange
        var = ContextVar(_unique("manifest_dup"))
        wire = protocol.ChainManifest(id=uuid4().hex)
        wire.vars.add(
            namespace=var.namespace,
            name=var.name,
            value=wool.__serializer__.dumps("first"),
        )
        wire.vars.add(
            namespace=var.namespace,
            name=var.name,
            value=wool.__serializer__.dumps("second-ignored"),
        )

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            decoded = _decode_manifest(wire)

        # Assert — duplicate-key warning was emitted naming the key,
        # and the first occurrence won in the decoded manifest.
        dup_warnings = [
            w for w in caught if issubclass(w.category, wool.SerializationWarning)
        ]
        assert dup_warnings, "expected a SerializationWarning on duplicate key"
        assert any("Duplicate" in str(w.message) for w in dup_warnings)
        duplicate = next(
            w.message for w in dup_warnings if "Duplicate" in str(w.message)
        )
        assert isinstance(duplicate, wool.SerializationWarning)
        assert duplicate.var_key == (var.namespace, var.name)
        assert duplicate.direction == "decode"
        assert duplicate.cause is None
        assert decoded.vars[var] == "first"

    @given(
        values=st.lists(
            st.text() | st.integers() | st.lists(st.integers()),
            min_size=0,
            max_size=5,
        ),
        reset_count=st.integers(min_value=0, max_value=3),
    )
    @settings(max_examples=50)
    def test_from_protobuf_should_round_trip_arbitrary_context(
        self, values, reset_count
    ):
        """Test to_protobuf/from_protobuf round-trips data and reset signals.

        Given:
            An armed chain binding zero or more variables to arbitrary
            serializable values, plus zero or more reset-and-not-re-set
            signals.
        When:
            The chain is encoded to the wire and decoded back.
        Then:
            The decoded chain manifest should carry every variable binding, every
            resets key, and the chain id.
        """
        # Arrange
        bound_vars = [ContextVar(_unique("rt_context")) for _ in values]
        reset_targets = [ContextVar(_unique("rt_reset")) for _ in range(reset_count)]
        resets = frozenset((v.namespace, v.name) for v in reset_targets)
        chain_id = uuid4()

        with scoped_context():
            # Bind the value-bearing vars so to_protobuf reads their
            # live backing-variable values.
            for var, value in zip(bound_vars, values):
                var.set(value)
            live = wool.__chain__.get(None)
            bound = live.vars if live is not None else frozenset()
            context = Chain(
                id=chain_id,
                thread=threading.get_ident(),
                vars=bound,
                resets=resets,
            )
            wire = context.to_manifest().to_protobuf()

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        expected = dict(zip(bound_vars, values))
        assert {var: decoded.vars[var] for var in bound_vars} == expected
        assert decoded.resets == resets
        assert decoded.id == chain_id


class TestChainManifestToProtobuf:
    def test_to_protobuf_should_return_empty_manifest_when_none(self):
        """Test to_protobuf returns an empty chain manifest for None.

        Given:
            A None chain (an unarmed chain).
        When:
            to_protobuf is called.
        Then:
            It should return an empty protocol.ChainManifest with no vars.
        """
        # Act
        wire = protocol.ChainManifest()

        # Assert
        assert isinstance(wire, protocol.ChainManifest)
        assert len(wire.vars) == 0
        assert wire.id == ""

    def test_to_protobuf_should_carry_chain_id(self):
        """Test to_protobuf writes the chain id to the chain manifest.

        Given:
            A chain with a known chain id.
        When:
            to_protobuf is called.
        Then:
            The chain manifest id should equal the chain id's hex form.
        """
        # Arrange
        chain_id = uuid4()
        context = Chain(id=chain_id, thread=threading.get_ident())

        # Act
        wire = context.to_manifest().to_protobuf()

        # Assert
        assert wire.id == chain_id.hex

    def test_to_protobuf_should_emit_one_entry_per_variable(self):
        """Test to_protobuf emits one wire entry per bound variable.

        Given:
            An armed chain with two distinct variable bindings.
        When:
            to_protobuf is called on the active chain.
        Then:
            The chain manifest should carry one entry per variable, each
            with a populated value field.
        """
        # Arrange
        var_a = ContextVar(_unique("encode_a"))
        var_b = ContextVar(_unique("encode_b"))

        with scoped_context():
            var_a.set(1)
            var_b.set(2)

            # Act
            wire = wool.__chain__.get().to_manifest().to_protobuf()

            # Assert
            keys = {(entry.namespace, entry.name) for entry in wire.vars}
            assert keys == {
                (var_a.namespace, var_a.name),
                (var_b.namespace, var_b.name),
            }
            assert all(entry.HasField("value") for entry in wire.vars)

    def test_to_protobuf_should_emit_no_entries_when_empty_data(self):
        """Test to_protobuf produces an empty chain manifest when data is empty.

        Given:
            A chain with no variable bindings in its data map.
        When:
            to_protobuf is called.
        Then:
            The chain manifest should carry no entries.
        """
        # Arrange
        context = Chain(id=uuid4(), thread=threading.get_ident())

        # Act
        wire = context.to_manifest().to_protobuf()

        # Assert
        assert len(wire.vars) == 0

    def test_to_protobuf_should_emit_reset_var_entry(self):
        """Test to_protobuf emits a no-value entry for a reset variable.

        Given:
            A chain whose resets set names a variable that has no
            current binding in data (reset to no value, not re-set).
        When:
            to_protobuf is called.
        Then:
            The chain manifest should carry an entry for that variable with
            no value field set.
        """
        # Arrange
        var = ContextVar(_unique("encode_reset_var"))
        context = Chain(
            id=uuid4(),
            thread=threading.get_ident(),
            resets=frozenset({(var.namespace, var.name)}),
        )

        # Act
        wire = context.to_manifest().to_protobuf()

        # Assert
        entry = next(
            e for e in wire.vars if (e.namespace, e.name) == (var.namespace, var.name)
        )
        assert not entry.HasField("value")

    def test_to_protobuf_should_warn_and_skip_unserializable_value(self):
        """Test to_protobuf warns and skips a variable it cannot serialize.

        Given:
            An armed chain carrying an unpicklable value alongside a
            normal binding.
        When:
            to_protobuf is called.
        Then:
            It should emit a SerializationWarning carrying the failed
            variable's key, the encode direction, and the underlying
            cause, and emit only the serializable variable on the wire.
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
                wire = wool.__chain__.get().to_manifest().to_protobuf()

            # Assert
            keys = {(entry.namespace, entry.name) for entry in wire.vars}
            assert keys == {(good.namespace, good.name)}
            emitted = [
                w for w in caught if issubclass(w.category, wool.SerializationWarning)
            ]
            assert emitted
            warning = emitted[0].message
            assert isinstance(warning, wool.SerializationWarning)
            assert warning.var_key == (bad.namespace, bad.name)
            assert warning.direction == "encode"
            assert warning.cause is not None

    def test_to_protobuf_should_aggregate_failures_when_strict_mode(self):
        """Test to_protobuf raises ChainSerializationError under strict mode.

        Given:
            An armed chain with two unserializable values and strict
            warning filtering for SerializationWarning.
        When:
            to_protobuf is called.
        Then:
            It should raise a ChainSerializationError aggregating both
            per-variable failures on .warnings, each carrying its
            variable's key, the encode direction, and the underlying
            cause.
        """
        # Arrange
        bad_a = ContextVar(_unique("encode_strict_a"))
        bad_b = ContextVar(_unique("encode_strict_b"))

        with scoped_context():
            bad_a.set(threading.Lock())
            bad_b.set(threading.Lock())
            context = wool.__chain__.get(None)
            assert context is not None

            # Act & assert
            with warnings.catch_warnings():
                warnings.filterwarnings("error", category=wool.SerializationWarning)
                with pytest.raises(wool.ChainSerializationError) as exc_info:
                    context.to_manifest().to_protobuf()
            assert len(exc_info.value.warnings) == 2
            assert all(
                isinstance(e, wool.SerializationWarning) for e in exc_info.value.warnings
            )
            assert {w.var_key for w in exc_info.value.warnings} == {
                (bad_a.namespace, bad_a.name),
                (bad_b.namespace, bad_b.name),
            }
            assert all(w.direction == "encode" for w in exc_info.value.warnings)
            assert all(w.cause is not None for w in exc_info.value.warnings)

    def test_to_protobuf_should_skip_variable_when_backing_undefined(self):
        """Test to_protobuf skips a data entry whose backing is Undefined.

        Given:
            A Chain whose data index names a variable, but the
            variable's backing contextvars.ContextVar resolves to the
            Undefined sentinel in the active context — a data-membership
            desync.
        When:
            to_protobuf is called inside that context.
        Then:
            The chain manifest should carry no entry for that variable —
            encode skips it defensively rather than ship a phantom value.
        """
        # Arrange
        var = ContextVar(_unique("encode_desync"))

        with scoped_context():
            # Drive the backing to the Undefined sentinel through the
            # public set/reset cycle — a first-set reset rewinds the
            # backing to its unset default — then index the variable in
            # data, producing the desynced state.
            token = var.set("v")
            var.reset(token)
            context = Chain(
                id=uuid4(),
                thread=threading.get_ident(),
                vars=frozenset({var}),
            )

            # Act
            wire = context.to_manifest().to_protobuf()

        # Assert
        keys = {(entry.namespace, entry.name) for entry in wire.vars}
        assert (var.namespace, var.name) not in keys


class TestChainMount:
    def test_mount_should_restamp_owner_and_owning_task(self):
        """Test Chain.mount re-stamps the context with the calling owner.

        Given:
            A ChainManifest decoded from a wire chain manifest — owner
            stamping happens at mount time, not decode time.
        When:
            mount applies it onto the current contextvars.Context.
        Then:
            The installed chain should be stamped with the calling
            thread as owner and a None _owning_task should be replaced by
            whatever the calling task carries (None here, off-loop).
        """
        # Arrange
        var = ContextVar(_unique("mount_owner"))
        with scoped_context():
            var.set("v")
            decoded = _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())

        # Act & assert
        with scoped_context():
            _mount_manifest(decoded)
            mounted = wool.__chain__.get(None)
            assert mounted is not None
            assert mounted.thread == threading.get_ident()

    def test_mount_should_apply_manifest_values_to_backing_variables(self):
        """Test Chain.from_manifest writes decoded values into the
        backing vars.

        Given:
            A ChainManifest decoded from a wire chain manifest that bound
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
            decoded = _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())

        # Act & assert
        with scoped_context():
            _mount_manifest(decoded)
            assert var.get() == "mounted-value"

    def test_mount_should_apply_manifest_via_unified_install_pipeline(self):
        """Test the unified install pipeline transfers values to backings.

        Given:
            A manifest decoded from a wire chain manifest bound to a value.
        When:
            The receive-time pipeline routes via
            `Chain.from_manifest` from a fresh `contextvars.Context`.
        Then:
            ``var.get()`` returns the decoded value — the install
            pipeline drained the manifest into the backing variable
            and installed a fresh Chain with the matching ``data``
            index.
        """
        # Arrange
        var = ContextVar(_unique("mount_install"))
        with scoped_context():
            var.set("v")
            decoded = _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())

        # Act
        with scoped_context():
            _mount_manifest(decoded)

            # Assert: the installed live chain resolves the var
            # through its backing.
            assert var.get() == "v"

    def test_mount_should_fold_data_into_active_context_when_armed(self):
        """Test mount folds incoming data into the active chain.

        Given:
            An armed chain carrying one variable and a decoded chain
            manifest carrying a different variable, both on the same chain
            id (as production receive sites always are — the worker adopts
            the caller's chain on first arm).
        When:
            mount is called with the decoded chain manifest.
        Then:
            The active chain should index both variables, both values
            should be observable, and the chain id is unchanged.
        """
        # Arrange
        existing = ContextVar(_unique("merge_existing"))
        incoming_var = ContextVar(_unique("merge_incoming"))

        with scoped_context():
            incoming_var.set("b")
            decoded = _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())

        with scoped_context():
            existing.set("a")
            _adopt_chain(decoded.id)
            armed = wool.__chain__.get(None)
            assert armed is not None
            original_chain = armed.id

            # Act
            _mount_manifest(decoded)
            merged = wool.__chain__.get(None)

            # Assert
            assert merged is not None
            assert existing in merged.vars
            assert incoming_var in merged.vars
            assert existing.get() == "a"
            assert incoming_var.get() == "b"
            assert merged.id == original_chain

    def test_mount_should_let_incoming_win_when_overlap(self):
        """Test mount lets the incoming chain manifest win overlapping keys.

        Given:
            An armed chain and a decoded chain manifest that both bind the
            same variable to different values.
        When:
            mount is called.
        Then:
            The merged chain should carry the incoming value.
        """
        # Arrange
        var = ContextVar(_unique("merge_overlap"))

        with scoped_context():
            var.set("remote")
            decoded = _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())

        # Act
        with scoped_context():
            var.set("local")
            _adopt_chain(decoded.id)
            _mount_manifest(decoded)

            # Assert
            assert var.get() == "remote"

    def test_mount_should_arm_unarmed_context(self):
        """Test mount arms an unarmed chain.

        Given:
            An unarmed chain and a decoded chain manifest with state.
        When:
            mount is called.
        Then:
            The chain should become armed and observe the merged value.
        """
        # Arrange
        var = ContextVar(_unique("merge_arm"))

        def _build_decoded():
            var.set("armed")
            return _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())

        decoded = contextvars.Context().run(_build_decoded)

        # Act
        assert wool.__chain__.get(None) is None
        _mount_manifest(decoded)

        # Assert
        assert wool.__chain__.get(None) is not None
        assert var.get() == "armed"

    def test_mount_should_propagate_reset_signal(self):
        """Test mount removes a variable that the incoming chain manifest reset.

        Given:
            An armed chain binding a variable, and a decoded chain manifest
            whose resets set names that variable.
        When:
            mount is called.
        Then:
            The variable should be removed from the active chain.
        """
        # Arrange
        var = ContextVar(_unique("merge_reset"))
        sender = Chain(
            id=uuid4(),
            thread=threading.get_ident(),
            resets=frozenset({(var.namespace, var.name)}),
        )
        decoded = _decode_manifest(sender.to_manifest().to_protobuf())

        # Act
        with scoped_context():
            var.set("present")
            _adopt_chain(decoded.id)
            _mount_manifest(decoded)

            # Assert
            with pytest.raises(LookupError):
                var.get()

    def test_mount_should_preserve_re_set_after_reset(self):
        """Test mount keeps a variable the incoming chain manifest re-set.

        Given:
            A receiver chain where a variable was reset to no value, and
            a decoded chain manifest that re-binds that same variable in data.
        When:
            mount is called.
        Then:
            The variable should remain bound to the incoming value and be
            absent from the merged resets.
        """
        # Arrange
        var = ContextVar(_unique("merge_reset_rebind"))

        with scoped_context():
            var.set("new")
            decoded = _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())

        with scoped_context():
            token = var.set("old")
            var.reset(token)
            _adopt_chain(decoded.id)

            # Act
            _mount_manifest(decoded)

            # Assert
            assert var.get() == "new"
            merged = wool.__chain__.get(None)
            assert merged is not None
            assert (var.namespace, var.name) not in merged.resets

    def test_mount_should_preserve_untouched_receiver_resets(self):
        """Test mount keeps receiver resets the incoming chain manifest ignored.

        Given:
            A receiver chain with one variable reset to no value, and a
            decoded chain manifest that touches a different variable only.
        When:
            mount is called.
        Then:
            The receiver's reset for the untouched variable should survive
            the merge — the merge is one-way, the incoming chain manifest wins
            only for the keys it touched.
        """
        # Arrange
        receiver_reset_var = ContextVar(_unique("merge_keep_reset"))
        incoming_var = ContextVar(_unique("merge_incoming_only"))

        with scoped_context():
            incoming_var.set("incoming")
            decoded = _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())

        # Act & assert
        with scoped_context():
            token = receiver_reset_var.set("present")
            receiver_reset_var.reset(token)
            _adopt_chain(decoded.id)
            _mount_manifest(decoded)
            merged = wool.__chain__.get(None)
            assert merged is not None
            assert (
                receiver_reset_var.namespace,
                receiver_reset_var.name,
            ) in merged.resets

    @pytest.mark.asyncio
    async def test_mount_should_self_install_task_factory_when_arming(self):
        """Test a mount that arms an unarmed chain self-installs the task factory.

        Given:
            An async unarmed chain with no Wool task factory installed,
            and a decoded chain manifest with state.
        When:
            mount arms the chain, then a child task is created.
        Then:
            The child task should fork onto a chain id distinct from the
            merged chain — the arming merge self-installed the task
            factory so copy-on-fork engages.
        """
        # Arrange
        var = ContextVar(_unique("merge_self_install"))
        loop = asyncio.get_running_loop()
        loop.set_task_factory(None)
        with scoped_context():
            var.set("armed")
            decoded = _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())

        # Act
        with scoped_context():
            _mount_manifest(decoded)
            merged = wool.__chain__.get(None)
            assert merged is not None

            async def child() -> uuid.UUID:
                context = wool.__chain__.get(None)
                assert context is not None
                return context.id

            try:
                child_chain = await asyncio.create_task(child())
            finally:
                loop.set_task_factory(None)

        # Assert
        assert child_chain != merged.id

    @pytest.mark.asyncio
    async def test_mount_should_surface_displacement_when_unowned(self):
        """Test an unowned mount surfaces a displaced task factory.

        Given:
            An armed chain on a loop whose Wool task factory was
            displaced by a third-party factory installed after it.
        When:
            The chain is mounted with owned=False (the worker-side
            per-step driver path).
        Then:
            It should raise TaskFactoryDisplaced — the unowned mount
            ensures the factory unconditionally, so displacement
            surfaces at the mount rather than only on the next
            wool.ContextVar set.
        """
        # Arrange
        var = ContextVar(_unique("unowned_displaced"))
        loop = asyncio.get_running_loop()
        loop.set_task_factory(None)
        with scoped_context():
            var.set("armed")
            chain = wool.__chain__.get()

            def third_party_factory(loop, coro, **kwargs):
                return asyncio.Task(coro, loop=loop, **kwargs)

            loop.set_task_factory(third_party_factory)

            # Act & assert
            try:
                with pytest.raises(TaskFactoryDisplaced, match="displaced"):
                    chain.mount(owned=False)
            finally:
                loop.set_task_factory(None)


class TestChainManifestVars:
    def test_vars_should_expose_decoded_values_when_pre_mount_manifest(self):
        """Test ChainManifest.vars surfaces decoded values.

        Given:
            A ChainManifest decoded from a wire chain manifest that carried
            a value for one variable.
        When:
            ``manifest.vars`` is read.
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
            decoded = _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())

        # Act
        snapshot = decoded.vars

        # Assert
        assert snapshot[var] == "shipped"
        assert dict(snapshot) == {var: "shipped"}

    def test_vars_should_reflect_manifest_state(self):
        """Test ChainManifest.vars exposes the decoded values.

        Given:
            A ChainManifest decoded from a wire chain manifest — its
            vars is populated.
        When:
            ``manifest.vars`` is read before and after mount.
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
            decoded = _decode_manifest(wool.__chain__.get().to_manifest().to_protobuf())
            assert decoded.vars[var] == "shipped"

        # Act
        with scoped_context():
            _mount_manifest(decoded)

            # Assert
            assert decoded.vars == {var: "shipped"}
            assert var.get() == "shipped"


def test_stub_backing_variable_value_should_survive_promotion_to_real_variable():
    """Test a wire value decoded into a stub survives stub-to-real promotion.

    Given:
        A wire chain manifest carrying a value — chosen to differ from the
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
    wire = protocol.ChainManifest(id=uuid4().hex)
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


class TestResolveStub:
    def test_resolve_stub_should_create_data_only_placeholder_when_key_unregistered(
        self,
    ):
        """Test resolving an unregistered key yields a bare data placeholder.

        Given:
            A (namespace, name) key no variable has been declared or
            resolved for.
        When:
            resolve_stub is called with that key.
        Then:
            It should return a placeholder flagged as a stub and carrying
            no constructor default — exposing the identity surface
            (namespace, name) but none of the live get/set/reset
            behavior of a declared variable.
        """
        # Arrange
        key = ("pkg", _unique("bare"))

        # Act
        manifest = resolve_stub(key)

        # Assert
        assert type(manifest) is ContextVarManifest
        assert not isinstance(manifest, ContextVar)
        assert manifest._stub is True
        assert manifest._default is Undefined
        assert (manifest.namespace, manifest.name) == key
        assert not hasattr(manifest, "set")
        assert not hasattr(manifest, "get")

    def test_resolve_stub_should_return_same_instance_when_key_already_resolved(self):
        """Test repeated resolution of one key converges on a single placeholder.

        Given:
            A key already resolved once into a registered placeholder.
        When:
            resolve_stub is called again with the same key.
        Then:
            It should return the very same instance — the registry holds
            one placeholder per key.
        """
        # Arrange
        key = ("pkg", _unique("singleton"))
        first = resolve_stub(key)

        # Act
        second = resolve_stub(key)

        # Assert
        assert first is second

    def test_resolve_stub_should_seed_default_into_a_default_less_placeholder(self):
        """Test a later resolution folds a default into a default-less placeholder.

        Given:
            A key first resolved with no default — a default-less
            placeholder.
        When:
            resolve_stub is called again for the key with a default.
        Then:
            The same placeholder should adopt the default rather than
            discard it — whichever ingress carries the default wins.
        """
        # Arrange
        key = ("pkg", _unique("fold"))
        manifest = resolve_stub(key)
        assert manifest._default is Undefined

        # Act
        again = resolve_stub(key, default="seeded")

        # Assert
        assert again is manifest
        assert manifest._default == "seeded"
