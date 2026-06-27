"""Unit tests for ChainManifest — the decoded-but-unmounted wire snapshot."""

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
from tests.helpers import scoped_context
from wool import protocol
from wool.runtime.context.chain import Chain
from wool.runtime.context.manifest import ChainManifest
from wool.runtime.context.var import ContextVar


def _decode_manifest(wire: protocol.ChainManifest) -> ChainManifest:
    """Decode a wire :class:`protocol.ChainManifest` into a
    :class:`ChainManifest` using the default serializer.

    Test-helper shortcut over the public
    :meth:`ChainManifest.from_protobuf` entry point — keeps the
    decode/mount tests below readable. Raises ``ChainSerializationError``
    under strict mode, just as the production decode does.
    """
    return ChainManifest.from_protobuf(wire, serializer=wool.__serializer__)


def _mount_manifest(manifest: ChainManifest) -> None:
    """Inline the receive-time mount pattern for tests.

    Mirrors :meth:`wool.runtime.worker.frame.Frame.mount`: routes through
    :meth:`wool.runtime.context.manifest.ChainManifest.mount` with the
    current Chain as the merge target (None when unarmed, picking up the
    fresh-install branch automatically). Used by the tests that exercise
    the decode-then-mount round trip via the public API.
    """
    if not (manifest.vars or manifest.resets):
        return
    manifest.mount(
        stamp_owner=True,
        install_factory=True,
        merge_with=wool.__chain__.get(None),
    )


def _adopt_chain(chain_id: UUID) -> None:
    """Arm or re-arm the current context with *chain_id*.

    Mirrors the worker-side receive-time invariant: a worker that arms
    onto a caller's chain and then receives further chain-manifest frames
    on the same chain. Tests that exercise the armed-receiver merge path
    call this before :func:`_mount_manifest` so the manifest's chain id
    matches the receiver's.
    """
    current = wool.__chain__.get(None)
    if current is None:
        wool.__chain__.set(Chain(id=chain_id, thread=threading.get_ident()))
    else:
        wool.__chain__.set(current._evolve(id=chain_id))


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


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
            wire = context.to_protobuf()

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        assert var in decoded.vars
        assert decoded.vars[var] == "hello"
        assert decoded.id == context.id

    @pytest.mark.skip(
        reason="external_used removed — cross-process token transport deferred; #231"
    )
    def test_from_protobuf_should_round_trip_external_used_token(self):
        """Test from_protobuf preserves used-token ids through encode/decode.

        Given:
            An armed chain binding a variable, with an external-used
            token for that variable installed on the chain.
        When:
            The chain is encoded to the wire and decoded back.
        Then:
            The decoded chain manifest's external_used log should contain the
            original token id mapped to the variable's key.
        """
        # Arrange
        var = ContextVar(_unique("decode_used_rt"))
        token_id = uuid4()

        with scoped_context():
            var.set("v")
            context = wool.__chain__.get(None)
            assert context is not None
            context = context.evolve(external_used={token_id: (var.namespace, var.name)})
            wire = context.to_protobuf()

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        assert token_id in decoded.external_used
        assert decoded.external_used[token_id] == (var.namespace, var.name)

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

    @pytest.mark.skip(
        reason="consumed_tokens wire field removed with the ContextVar comment "
        "block; cross-process token transport deferred; #231"
    )
    def test_from_protobuf_should_record_token_id_in_external_used(self):
        """Test from_protobuf records wire token ids in external_used.

        Given:
            A chain manifest entry whose consumed_tokens list carries a
            token id.
        When:
            from_protobuf is called on that chain manifest.
        Then:
            The decoded chain manifest's external_used log should map that token
            id to the variable's key.
        """
        # Arrange
        var = ContextVar(_unique("decode_external"))
        token_id = uuid4()
        wire = protocol.ChainManifest(id=uuid4().hex)
        wire.vars.add(
            namespace=var.namespace,
            name=var.name,
            consumed_tokens=[token_id.hex],
        )

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        assert decoded.external_used[token_id] == (var.namespace, var.name)

    def test_from_protobuf_should_produce_manifest_not_live_context(self):
        """Test from_protobuf produces a :class:`ChainManifest` and not a
        live :class:`Chain`.

        Given:
            A chain manifest with a variable binding.
        When:
            :meth:`ChainManifest.from_protobuf` is called.
        Then:
            The returned object is a :class:`ChainManifest` (unmounted
            wire state, no owner stamping) — owner stamping is the
            :meth:`Chain.mount` site and a freshly decoded manifest
            carries no owner.
        """
        # Arrange
        var = ContextVar(_unique("decode_owner"))
        with scoped_context():
            var.set(1)
            wire = wool.__chain__.get().to_protobuf()

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        assert isinstance(decoded, ChainManifest)

    def test_from_protobuf_should_raise_when_malformed_chain_id(self):
        """Test from_protobuf raises on a malformed chain id (F30).

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
            Chain (silent state loss); F30 hardens this to fail loud.
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

    @pytest.mark.skip(
        reason="consumed_tokens wire field removed; cross-process token transport "
        "deferred; #231"
    )
    def test_from_protobuf_should_warn_and_skip_when_malformed_consumed_token_id(self):
        """Test from_protobuf warns and skips a malformed consumed-token id.

        Given:
            A wire entry whose consumed_tokens list holds a non-UUID
            string.
        When:
            from_protobuf is called.
        Then:
            It should emit a SerializationWarning and omit that id from
            the decoded external_used log.
        """
        # Arrange
        var = ContextVar(_unique("decode_bad_token"))
        wire = protocol.ChainManifest(id=uuid4().hex)
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
        assert any(issubclass(w.category, wool.SerializationWarning) for w in caught)

    def test_from_protobuf_should_aggregate_failures_when_strict_mode(self):
        """Test from_protobuf raises ChainSerializationError under strict mode.

        Given:
            A chain manifest with two unparseable values (per-var data
            errors), under strict SerializationWarning filtering.
            Chain-id is intentionally well-formed here — F30 makes
            chain-id parse failure fatal *independently* of the
            strict-mode aggregator, so a malformed chain id no longer
            feeds into the per-var aggregator.
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
            wire = wool.__chain__.get().to_protobuf()

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
            resets key, and the chain id. (external_used round-trip
            coverage is deferred with the consumed_tokens wire field; #231.)
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
            wire = context.to_protobuf()

        # Act
        decoded = _decode_manifest(wire)

        # Assert
        expected = dict(zip(bound_vars, values))
        assert {var: decoded.vars[var] for var in bound_vars} == expected
        assert decoded.resets == resets
        assert decoded.id == chain_id


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
            decoded = _decode_manifest(wool.__chain__.get().to_protobuf())

        # Act & assert
        with scoped_context():
            _mount_manifest(decoded)
            mounted = wool.__chain__.get(None)
            assert mounted is not None
            assert mounted.thread == threading.get_ident()

    def test_mount_should_apply_manifest_values_to_backing_variables(self):
        """Test ChainManifest.mount writes decoded values into the
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
            decoded = _decode_manifest(wool.__chain__.get().to_protobuf())

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
            :func:`_install_manifest` from a fresh
            :class:`contextvars.Context`.
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
            decoded = _decode_manifest(wool.__chain__.get().to_protobuf())

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
            decoded = _decode_manifest(wool.__chain__.get().to_protobuf())

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
            decoded = _decode_manifest(wool.__chain__.get().to_protobuf())

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
            return _decode_manifest(wool.__chain__.get().to_protobuf())

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
        decoded = _decode_manifest(sender.to_protobuf())

        # Act
        with scoped_context():
            var.set("present")
            _adopt_chain(decoded.id)
            _mount_manifest(decoded)

            # Assert
            with pytest.raises(LookupError):
                var.get()

    @pytest.mark.skip(
        reason="external_used removed — cross-process token transport deferred; #231"
    )
    def test_mount_should_merge_external_used(self):
        """Test mount folds incoming external-used ids into the active chain.

        Given:
            An armed chain and a decoded chain manifest carrying an
            external-used token id.
        When:
            mount is called with the decoded chain manifest.
        Then:
            The active chain's external_used log should contain the
            incoming token id.
        """
        # Arrange
        var = ContextVar(_unique("merge_external_used"))
        token_id = uuid4()

        with scoped_context():
            var.set("x")
            sender = wool.__chain__.get(None)
            assert sender is not None
            sender = sender.evolve(external_used={token_id: (var.namespace, var.name)})
            decoded = _decode_manifest(sender.to_protobuf())

        with scoped_context():
            var.set("x")
            _adopt_chain(decoded.id)

            # Act
            _mount_manifest(decoded)

            # Assert
            merged = wool.__chain__.get(None)
            assert merged is not None
            assert token_id in merged.external_used

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
            decoded = _decode_manifest(wool.__chain__.get().to_protobuf())

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
            decoded = _decode_manifest(wool.__chain__.get().to_protobuf())

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
            decoded = _decode_manifest(wool.__chain__.get().to_protobuf())

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
            decoded = _decode_manifest(wool.__chain__.get().to_protobuf())

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
            decoded = _decode_manifest(wool.__chain__.get().to_protobuf())
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
