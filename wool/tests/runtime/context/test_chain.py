"""Unit tests for Chain — the live, immutable chain-state model."""

import asyncio
import contextvars
import gc
import threading
from uuid import uuid4

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from tests.helpers import _unique
from tests.helpers import scoped_context
from wool.runtime.context.chain import Chain
from wool.runtime.context.manifest import ChainManifest
from wool.runtime.context.token import Token
from wool.runtime.context.token import token_sink
from wool.runtime.context.var import ContextVar


def _count_wool_vars_in_a_fresh_context(work) -> int:
    """Run *work* in a brand-new Chain and count Wool-owned variables.

    A fresh `contextvars.Context` carries no backing variables
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


class TestChain:
    def test___init___should_default_collections_empty_when_required_fields_only(self):
        """Test Chain construction with only the required fields.

        Given:
            A fresh chain id and an owner thread id.
        When:
            A Chain is constructed with only chain_id and owner.
        Then:
            It should expose empty data, resets, and stubs collections
            and an empty unspent_tokens mapping by default.
        """
        # Arrange
        chain_id = uuid4()
        owner = threading.get_ident()

        # Act
        context = Chain(id=chain_id, thread=owner)

        # Assert
        assert context.id == chain_id
        assert context.thread == owner
        assert context.vars == frozenset()
        assert context.resets == frozenset()
        assert context.stubs == frozenset()
        assert context.unspent_tokens == {}

    def test___init___should_expose_supplied_collections_when_all_fields(self):
        """Test Chain construction with every field supplied.

        Given:
            Explicit data, resets, stubs, and unspent_tokens
            collections.
        When:
            A Chain is constructed with all fields.
        Then:
            It should expose each supplied collection verbatim.
        """
        # Arrange
        var = ContextVar(_unique("snap_init"))
        bound = frozenset({var})
        resets = frozenset({("ns", "name")})
        unspent = {(var.namespace, var.name): frozenset({"aa", "bb"})}

        # Act
        context = Chain(
            id=uuid4(),
            thread=threading.get_ident(),
            vars=bound,
            resets=resets,
            stubs=frozenset({var}),
            unspent_tokens=unspent,
        )

        # Assert
        assert context.vars == bound
        assert context.resets == resets
        assert context.stubs == frozenset({var})
        assert context.unspent_tokens == unspent

    def test_mount_should_install_field_replacing_copy(self):
        """Test mount installs a field-replacing copy of the chain.

        Given:
            A directly-constructed chain with a known chain id and one
            variable binding.
        When:
            mount is called on it.
        Then:
            It should install a new chain carrying the same chain id and
            bindings while leaving the original instance untouched — the
            mount is a field-replacing copy, not an in-place mutation.
        """
        # Arrange
        var = ContextVar(_unique("snap_evolve"))
        chain_id = uuid4()
        original = Chain(id=chain_id, vars=frozenset({var}))

        # Act
        with scoped_context():
            installed = original.mount()

        # Assert — the installed copy preserves the chain id and bindings
        assert installed.id == chain_id
        assert installed.vars == frozenset({var})
        # Assert — the mount is a copy, not an in-place mutation
        assert installed is not original

    def test___post_init___should_coerce_iterables_to_frozensets(self):
        """Test Chain coerces non-frozenset iterables on construction.

        Given:
            Plain sets, lists, and tuples supplied for the data,
            resets, and stubs fields, and a dict whose values are plain
            sets supplied for unspent_tokens — collections that satisfy
            the typing intent of the field but are not the frozen shapes
            at the call site.
        When:
            A Chain is constructed with those iterables.
        Then:
            The three set-shaped fields should expose frozenset views
            and unspent_tokens should expose a dict of frozensets after
            construction — the post-init coerces the containers so the
            dataclass invariant (hashable + immutable container shape)
            holds regardless of what the caller passed.
        """
        # Arrange
        var = ContextVar(_unique("post_init_coerce"))
        key = (var.namespace, var.name)

        # Act — supply a plain set, list, tuple, and dict-of-set respectively.
        context = Chain(
            id=uuid4(),
            thread=threading.get_ident(),
            vars={var},  # set, not frozenset
            resets=[("ns", "name")],  # list, not frozenset
            stubs=(var,),  # tuple, not frozenset
            unspent_tokens={key: {"aa", "bb"}},  # dict of set, not frozenset
        )

        # Assert
        assert isinstance(context.vars, frozenset)
        assert isinstance(context.resets, frozenset)
        assert isinstance(context.stubs, frozenset)
        assert isinstance(context.unspent_tokens, dict)
        assert isinstance(context.unspent_tokens[key], frozenset)
        assert context.vars == frozenset({var})
        assert context.resets == frozenset({("ns", "name")})
        assert context.stubs == frozenset({var})
        assert context.unspent_tokens == {key: frozenset({"aa", "bb"})}

    def test_equality_should_be_identity_based(self):
        """Test Chain equality is identity-based.

        Given:
            Two chains constructed with identical field values.
        When:
            They are compared for equality.
        Then:
            They should be unequal — Chain is declared eq=False so
            distinct instances never compare equal.
        """
        # Arrange
        chain_id = uuid4()
        owner = threading.get_ident()

        # Act
        first = Chain(id=chain_id, thread=owner)
        second = Chain(id=chain_id, thread=owner)

        # Assert
        assert first != second
        assert first == first

    @given(
        ops=st.lists(
            st.tuples(
                st.sampled_from(["set", "reset"]),
                st.integers(min_value=0, max_value=2),
                st.integers(),
            ),
            max_size=30,
        )
    )
    @settings(max_examples=50, deadline=None)
    def test_bookkeeping_should_match_oracle_when_arbitrary_set_reset_sequences(
        self, ops
    ):
        """Test chain bookkeeping tracks an oracle over set/reset sequences.

        Given:
            Three fresh wool.ContextVars in an unarmed scoped context,
            an arbitrary sequence of set/reset operations over them,
            and a naive per-var oracle replaying each operation (set
            binds and clears any pending reset; reset spends the most
            recent unspent token, restoring its prior state — a prior
            of unbound leaves the key reset-pending).
        When:
            Each operation is applied through the public set/reset API.
        Then:
            After every operation the live chain's vars index and
            resets keys should match the oracle, every var's get()
            should observe the oracle's value or default, and vars and
            resets should stay disjoint.
        """
        unset = object()

        def _check() -> None:
            targets = [ContextVar(_unique(f"oracle_{i}")) for i in range(3)]
            value_of: dict[int, object] = {i: unset for i in range(3)}
            pending: set[tuple[str, str]] = set()
            tokens: dict[int, list] = {i: [] for i in range(3)}

            for op, slot, value in ops:
                var = targets[slot]
                key = (var.namespace, var.name)
                if op == "set":
                    tokens[slot].append((var.set(value), value_of[slot]))
                    value_of[slot] = value
                    pending.discard(key)
                else:
                    if not tokens[slot]:
                        continue  # no unspent token — nothing to reset
                    token, prior = tokens[slot].pop()
                    var.reset(token)
                    value_of[slot] = prior
                    if prior is unset:
                        pending.add(key)
                    else:
                        pending.discard(key)

                chain = wool.__chain__.get(None)
                chain_vars = (
                    {(v.namespace, v.name) for v in chain.vars}
                    if chain is not None
                    else set()
                )
                chain_resets = set(chain.resets) if chain is not None else set()
                expected_bound = {
                    (targets[i].namespace, targets[i].name)
                    for i in range(3)
                    if value_of[i] is not unset
                }
                assert chain_vars == expected_bound
                assert chain_resets == pending
                assert not (chain_vars & chain_resets)
                for i, target in enumerate(targets):
                    expected = "<unbound>" if value_of[i] is unset else value_of[i]
                    assert target.get("<unbound>") == expected

        contextvars.Context().run(_check)

    @given(
        ops=st.lists(
            st.tuples(
                st.sampled_from(["set", "reset"]),
                st.integers(min_value=0, max_value=2),
                st.integers(),
            ),
            max_size=30,
        )
    )
    @settings(max_examples=50, deadline=None)
    def test_unspent_tokens_should_track_live_token_count_when_arbitrary_sequences(
        self, ops
    ):
        """Test the unspent-token snapshot tracks the live-token count.

        Given:
            Three fresh wool.ContextVars in a fresh context and an
            arbitrary sequence of set/reset operations over them, where
            each reset spends the most recent unspent token for its
            variable.
        When:
            Each operation is applied through the public set/reset API
            and the live chain is snapshotted via to_manifest after
            each one.
        Then:
            The total id count across the snapshot's per-var
            unspent_tokens should equal the number of unspent tokens —
            each set adds one id under its var's key, each successful
            reset removes one — and an id consumed by a reset should
            never reappear in a later snapshot.
        """

        def _live_ids() -> frozenset[str]:
            chain = wool.__chain__.get(None)
            if chain is None:
                return frozenset()
            unspent = chain.to_manifest().unspent_tokens
            return frozenset().union(*unspent.values()) if unspent else frozenset()

        def _check() -> None:
            targets = [ContextVar(_unique(f"live_oracle_{i}")) for i in range(3)]
            tokens: dict[int, list] = {i: [] for i in range(3)}
            unspent = 0
            consumed: set[str] = set()

            for op, slot, value in ops:
                var = targets[slot]
                if op == "set":
                    tokens[slot].append(var.set(value))
                    unspent += 1
                else:
                    if not tokens[slot]:
                        continue  # no unspent token — nothing to reset
                    before = _live_ids()
                    var.reset(tokens[slot].pop())
                    unspent -= 1
                    consumed |= before - _live_ids()

                live = _live_ids()
                assert len(live) == unspent
                assert not (consumed & live)

        # Arrange, act, & assert
        contextvars.Context().run(_check)

    def test_to_manifest_should_snapshot_bound_values_inline(self):
        """Test to_manifest captures each bound variable's live value.

        Given:
            An armed chain with two distinct variable bindings.
        When:
            to_manifest is called on the active chain.
        Then:
            The returned manifest should map each bound variable to the
            value read from its backing in the calling context.
        """
        # Arrange
        var_a = ContextVar(_unique("snap_a"))
        var_b = ContextVar(_unique("snap_b"))

        with scoped_context():
            var_a.set(1)
            var_b.set(2)

            # Act
            manifest = wool.__chain__.get().to_manifest()

        # Assert
        assert manifest.vars == {var_a: 1, var_b: 2}

    def test_to_manifest_should_skip_variable_when_backing_undefined(self):
        """Test to_manifest omits a variable whose backing is Undefined.

        Given:
            A chain whose vars index names a variable whose backing
            resolves to the Undefined sentinel in the active context.
        When:
            to_manifest is called inside that context.
        Then:
            The returned manifest should carry no entry for that variable.
        """
        # Arrange
        var = ContextVar(_unique("snap_desync"))

        with scoped_context():
            token = var.set("v")
            var.reset(token)
            context = Chain(
                id=uuid4(),
                thread=threading.get_ident(),
                vars=frozenset({var}),
            )

            # Act
            manifest = context.to_manifest()

        # Assert
        assert var not in manifest.vars

    def test_to_manifest_should_carry_id_resets_and_stubs(self):
        """Test to_manifest carries the chain id and reset signals through.

        Given:
            A chain with a known id and a reset-pending variable key.
        When:
            to_manifest is called.
        Then:
            The returned manifest should preserve the chain id and the
            reset signal verbatim, with no inline value for the reset key.
        """
        # Arrange
        var = ContextVar(_unique("snap_reset"))
        chain_id = uuid4()
        context = Chain(
            id=chain_id,
            thread=threading.get_ident(),
            resets=frozenset({(var.namespace, var.name)}),
        )

        # Act
        manifest = context.to_manifest()

        # Assert
        assert manifest.id == chain_id
        assert manifest.resets == frozenset({(var.namespace, var.name)})
        assert manifest.vars == {}

    def test_to_manifest_should_grow_unspent_tokens_when_var_set_repeatedly(self):
        """Test each set grows the var's unspent-token snapshot by one id.

        Given:
            An armed chain where one variable is set twice, with a
            snapshot taken after each set.
        When:
            to_manifest is called after each set.
        Then:
            The var's unspent_tokens entry should carry one token id
            after the first set and two after the second, the second
            snapshot a strict superset of the first — every set mints a
            fresh live token id under the var's key.
        """
        # Arrange
        var = ContextVar(_unique("live_grow"))
        key = (var.namespace, var.name)

        with scoped_context():
            # Act
            var.set("first")
            first = wool.__chain__.get().to_manifest().unspent_tokens[key]
            var.set("second")
            second = wool.__chain__.get().to_manifest().unspent_tokens[key]

        # Assert
        assert len(first) == 1
        assert len(second) == 2
        assert second > first

    def test_to_manifest_should_prune_unspent_tokens_when_token_reset(self):
        """Test reset prunes the consumed token's id from the snapshot.

        Given:
            An armed chain where one variable was set twice, with a
            snapshot taken after the first set.
        When:
            The second set's token is reset and to_manifest is called
            again.
        Then:
            The new snapshot's per-var unspent token ids should equal
            the first-set snapshot — the consumed token's id is pruned
            from the var's live set.
        """
        # Arrange
        var = ContextVar(_unique("live_prune"))
        key = (var.namespace, var.name)

        with scoped_context():
            var.set("first")
            baseline = wool.__chain__.get().to_manifest().unspent_tokens[key]
            second = var.set("second")

            # Act
            var.reset(second)

            # Assert
            assert wool.__chain__.get().to_manifest().unspent_tokens[key] == baseline

    def test_to_manifest_should_snapshot_spent_tokens_when_token_consumed(self):
        """Test to_manifest snapshots the spent-token ledger for its keys.

        Given:
            An armed chain where a variable was set and its token
            reset, recording the spent token id in the chain's ledger.
        When:
            to_manifest is called on the active chain.
        Then:
            It should carry a non-empty spent-id set under that
            variable's (namespace, name) key in spent_tokens.
        """
        # Arrange
        var = ContextVar(_unique("ledger_snapshot"))

        with scoped_context():
            token = var.set("v")
            var.reset(token)

            # Act
            manifest = wool.__chain__.get().to_manifest()

        # Assert
        consumed = manifest.spent_tokens[(var.namespace, var.name)]
        assert isinstance(consumed, frozenset)
        assert consumed

    def test_to_manifest_should_exclude_foreign_keys_from_spent_tokens(self):
        """Test to_manifest omits ledger entries for keys outside the chain.

        Given:
            A token for one variable consumed inside a sibling
            contextvars.Context — which arms its own chain — and a
            separate armed chain binding only a different variable.
        When:
            to_manifest is called on the chain binding the other
            variable.
        Then:
            It should carry no spent_tokens entry for the
            sibling-consumed variable's key — the ledger is chain-local,
            so a consumption in a sibling chain never reaches this one.
        """
        # Arrange
        local_var = ContextVar(_unique("ledger_local"))
        foreign_var = ContextVar(_unique("ledger_foreign"))

        def _consume_foreign() -> None:
            foreign_var.reset(foreign_var.set("x"))

        contextvars.Context().run(_consume_foreign)

        with scoped_context():
            local_var.set("a")

            # Act
            manifest = wool.__chain__.get().to_manifest()

        # Assert
        assert (foreign_var.namespace, foreign_var.name) not in manifest.spent_tokens

    def test_from_manifest_should_seed_unspent_tokens_when_receiver_unarmed(self):
        """Test from_manifest seeds the manifest's unspent ids on a fresh receiver.

        Given:
            A manifest snapshotted from a sender scope holding two
            live tokens, and a fresh unarmed receiver context.
        When:
            Chain.from_manifest installs the manifest with no merge
            target.
        Then:
            The receiver chain's snapshot should carry exactly the
            sender's per-var unspent token ids.
        """
        # Arrange
        var = ContextVar(_unique("seed_live"))

        def _sender() -> ChainManifest:
            var.set("first")
            var.set("second")
            return wool.__chain__.get().to_manifest()

        manifest = contextvars.Context().run(_sender)
        assert manifest.unspent_tokens

        def _receiver() -> dict[tuple[str, str], frozenset[str]]:
            assert wool.__chain__.get(None) is None
            Chain.from_manifest(manifest, owned=True)
            return wool.__chain__.get().to_manifest().unspent_tokens

        # Act
        received = contextvars.Context().run(_receiver)

        # Assert
        assert received == manifest.unspent_tokens

    def test_from_manifest_should_union_unspent_tokens_when_merging(self):
        """Test from_manifest unions sender and receiver unspent token ids per var.

        Given:
            A live receiver chain holding one live token and a sender
            manifest from a sibling scope holding a different live
            token, each under its own variable's key.
        When:
            Chain.from_manifest installs the manifest merged onto the
            receiver chain.
        Then:
            The installed chain should carry the per-var union of both
            unspent-token mappings (two ids across two keys) under the
            receiver's chain id.
        """
        # Arrange
        sender_var = ContextVar(_unique("union_sender"))
        receiver_var = ContextVar(_unique("union_receiver"))

        def _sender() -> ChainManifest:
            sender_var.set("s")
            return wool.__chain__.get().to_manifest()

        manifest = contextvars.Context().run(_sender)

        with scoped_context():
            receiver_var.set("r")
            receiver = wool.__chain__.get()

            # Act
            merged = Chain.from_manifest(manifest, owned=True, merge_with=receiver)

            # Assert
            assert merged.unspent_tokens == {
                **receiver.unspent_tokens,
                **manifest.unspent_tokens,
            }
            assert sum(len(ids) for ids in merged.unspent_tokens.values()) == 2
            assert merged.id == receiver.id

    def test_from_manifest_should_seed_spent_tokens_when_receiver_unarmed(self):
        """Test from_manifest seeds the manifest's spent ids on a fresh receiver.

        Given:
            A manifest snapshotted from a sender scope that set a variable and
            reset its token — recording a spent id whose token instance is kept
            alive — and a fresh unarmed receiver context.
        When:
            Chain.from_manifest installs the manifest with no merge target.
        Then:
            The receiver chain's snapshot should carry exactly the sender's
            per-var spent token ids — the seed branch mirrors the unspent one,
            and the live instance keeps the id from being reaped on ingress.
        """
        # Arrange
        var = ContextVar(_unique("seed_spent"))

        def _sender() -> tuple[ChainManifest, Token]:
            token = var.set("v")
            var.reset(token)
            return wool.__chain__.get().to_manifest(), token

        manifest, token = contextvars.Context().run(_sender)
        assert manifest.spent_tokens

        def _receiver() -> dict[tuple[str, str], frozenset[str]]:
            assert wool.__chain__.get(None) is None
            Chain.from_manifest(manifest, owned=True)
            return wool.__chain__.get().to_manifest().spent_tokens

        # Act
        received = contextvars.Context().run(_receiver)

        # Assert
        assert received == manifest.spent_tokens
        assert isinstance(token, Token)  # hold the instance so the seed is not reaped

    def test_from_manifest_should_block_local_reset_when_ledger_ingested(self):
        """Test an ingested consumed-token ledger blocks the local reset.

        Given:
            A live chain holding one live token, and a manifest
            carrying the same chain id with that token's id in
            spent_tokens — as if a remote hop had already spent the
            token.
        When:
            Chain.from_manifest merges the manifest onto the live
            chain and the original token is then reset locally.
        Then:
            The reset should raise RuntimeError reporting the token
            has already been used once — cross-process single-use
            holds even for the origin still holding the original
            token.
        """
        # Arrange
        var = ContextVar(_unique("ledger_ingest"))

        with scoped_context():
            token = var.set("v")
            live = wool.__chain__.get()
            key = (var.namespace, var.name)
            token_id = next(iter(live.to_manifest().unspent_tokens[key]))
            manifest = ChainManifest(
                id=live.id,
                vars={},
                resets=frozenset(),
                stubs=frozenset(),
                spent_tokens={key: frozenset({token_id})},
            )
            Chain.from_manifest(manifest, owned=True, merge_with=live)

            # Act & assert
            with pytest.raises(RuntimeError, match="has already been used once"):
                var.reset(token)

    def test_to_manifest_should_retain_spent_tokens_when_instance_held(self):
        """Test a re-set retains the spent entry while its instance is held.

        Given:
            A variable whose token has been consumed by reset, leaving its
            id in the chain snapshot's spent_tokens for that key, with the
            consumed token still referenced.
        When:
            The variable is set again, minting a fresh token.
        Then:
            The new snapshot's spent_tokens should still carry the consumed
            id under the key — a copy of the consumed token may live on any
            process the id reached with its used flag unset, so this ledger
            is the only witness rejecting a cross-process double reset, and
            the reap holds off while an instance is alive.
        """
        # Arrange
        var = ContextVar(_unique("retain_spent_tokens"))
        key = (var.namespace, var.name)

        with scoped_context():
            token = var.set("v")
            token_id = next(iter(wool.__chain__.get().to_manifest().unspent_tokens[key]))
            var.reset(token)

            # Act
            var.set("again")

            # Assert
            spent = wool.__chain__.get().to_manifest().spent_tokens
            assert token_id in spent[key]
            assert isinstance(token, Token)  # keep the instance alive to the assertion

    def test_mount_should_keep_spent_ledger_bounded_across_a_set_reset_loop(self):
        """Test the spent ledger stays bounded across many set/reset cycles.

        Given:
            A single long-lived chain driving many set-then-reset cycles,
            each discarding the consumed token — the streaming routine's
            growth pattern.
        When:
            The cycle repeats N times and the per-key spent-token count is
            sampled each iteration.
        Then:
            The count should stay pinned at the single in-flight consumed id
            rather than growing with N — each cycle's collected token is reaped
            instead of accumulating one entry per cycle for the chain's life.
        """
        # Arrange
        n = 64
        var = ContextVar(_unique("loop_bound"))
        key = (var.namespace, var.name)
        sizes: list[int] = []

        def _loop() -> None:
            for i in range(n):
                token = var.set(i)
                var.reset(token)
                del token
                gc.collect()
                chain = wool.__chain__.get()
                sizes.append(len(chain.spent_tokens.get(key, frozenset())))

        # Act
        contextvars.Context().run(_loop)

        # Assert — one in-flight consumed id per iteration, independent of N.
        assert max(sizes) <= 1

    @given(
        cycles=st.lists(
            st.tuples(st.integers(), st.booleans()),
            max_size=16,
        )
    )
    @settings(max_examples=30, deadline=None)
    def test_mount_should_keep_spent_ledger_bounded_when_arbitrary_set_reset_cycles(
        self, cycles
    ):
        """Test the spent ledger stays bounded across arbitrary set/reset cycles.

        Given:
            A single var in an isolated context and an arbitrary-length
            sequence of cycles, each carrying a value to set and a flag for
            whether to reset the token it mints, with the token
            deterministically dropped and collected after every cycle.
        When:
            Each cycle sets the var, optionally resets the minted token, then
            drops and collects it, sampling the per-key spent-token count.
        Then:
            The count should never exceed one however many cycles run — each
            cycle's collected consumed id is reaped on the next mount instead
            of accumulating one entry per cycle for the chain's life.
        """
        # Arrange
        var = ContextVar(_unique("loop_bound_prop"))
        key = (var.namespace, var.name)
        sizes: list[int] = []

        def _drive() -> None:
            for value, do_reset in cycles:
                token = var.set(value)
                if do_reset:
                    var.reset(token)
                del token
                gc.collect()
                chain = wool.__chain__.get()
                sizes.append(len(chain.spent_tokens.get(key, frozenset())))

        # Act
        contextvars.Context().run(_drive)

        # Assert — at most one in-flight consumed id, independent of length.
        assert all(size <= 1 for size in sizes)

    def test_mount_should_reap_a_reconstituted_token_when_dropped(self):
        """Test a wire-reconstituted token's consumed id is reaped once dropped.

        Given:
            A live token minted in an origin scope and shipped home, its origin
            instance then collected, and a receiver that reconstitutes it,
            anchors it, resets it — recording the id in spent_tokens — then
            drops it and re-arms the chain.
        When:
            The re-arm transits the reap.
        Then:
            The consumed id should be reaped once the receiver holds no
            instance of it — the ledger is bounded by token lifespan, not the
            chain's.
        """
        # Arrange
        var = ContextVar(_unique("reconstituted_reap"))
        key = (var.namespace, var.name)

        def _origin() -> tuple[ChainManifest, bytes]:
            token = var.set("v")
            return wool.__chain__.get().to_manifest(), wool.__serializer__.dumps(token)

        manifest, payload = contextvars.Context().run(_origin)
        tok_id = next(iter(manifest.unspent_tokens[key]))
        gc.collect()  # drop the origin-side instance

        def _receiver() -> frozenset[str]:
            with token_sink() as pending:
                wire = cloudpickle.loads(payload)
            Chain.from_manifest(manifest, owned=True, wire_tokens=pending)
            var.reset(wire)
            assert tok_id in wool.__chain__.get().spent_tokens.get(key, frozenset())
            # Drop every reference to the instance — the sink list still holds it.
            del wire
            del pending
            gc.collect()
            var.set("bump")
            return wool.__chain__.get().spent_tokens.get(key, frozenset())

        # Act
        spent_after = contextvars.Context().run(_receiver)

        # Assert
        assert tok_id not in spent_after

    def test_from_manifest_should_reap_a_spent_id_without_a_live_instance(self):
        """Test a wire-arrived spent id with no live instance is reaped on ingress.

        Given:
            A fresh receiver that ingests a manifest carrying a consumed id in
            spent_tokens for which it holds no token instance.
        When:
            Chain.from_manifest installs it, transiting the reap.
        Then:
            The id should be reaped — with no instance in this process nothing
            can reset or forward it, so the ledger need not retain it; it
            re-arrives with its own instance if the token is ever forwarded
            here, keeping single-use intact.
        """
        # Arrange
        var = ContextVar(_unique("external_reap"))
        key = (var.namespace, var.name)
        external_id = uuid4().hex

        def _receiver() -> frozenset[str]:
            manifest = ChainManifest(
                id=uuid4(),
                vars={},
                resets=frozenset(),
                stubs=frozenset(),
                spent_tokens={key: frozenset({external_id})},
            )
            Chain.from_manifest(manifest, owned=True)
            return wool.__chain__.get().spent_tokens.get(key, frozenset())

        # Act
        spent = contextvars.Context().run(_receiver)

        # Assert
        assert external_id not in spent

    def test_mount_should_not_reap_spent_token_while_a_round_trip_clone_lives(self):
        """Test the reap holds a consumed id while a returned clone still lives.

        Given:
            A round-trip where a variable is set and the same token rides home
            as a live, anchored clone, so two instances share one id in this
            process; the original is then reset and dropped, and the chain is
            re-armed by a fresh set.
        When:
            The still-live returned clone is reset.
        Then:
            It should raise RuntimeError reporting the token was already used —
            the reap must not drop the consumed id while any instance of it
            survives, so single-use holds and the freshly-set value stays
            intact.
        """
        # Arrange
        var = ContextVar(_unique("round_trip_reap"))

        with scoped_context():
            origin = var.set("v1")
            manifest = wool.__chain__.get().to_manifest()
            payload = wool.__serializer__.dumps(origin)
            # The token rides home on a frame — a live, anchored clone.
            with token_sink() as pending:
                clone = cloudpickle.loads(payload)
            Chain.from_manifest(
                manifest,
                owned=True,
                merge_with=wool.__chain__.get(),
                wire_tokens=pending,
            )
            # Consume via the original, then drop it and re-arm the chain.
            var.reset(origin)
            del origin
            gc.collect()
            var.set("v2")

            # Act & assert — the clone still lives, so the id must not be reaped.
            with pytest.raises(RuntimeError, match="has already been used once"):
                var.reset(clone)
            assert var.get() == "v2"

    def test_from_manifest_should_reap_dropped_but_keep_held_when_merging(self):
        """Test the merge-path reap discriminates by instance liveness.

        Given:
            An armed receiver with two consumed ids — one whose token instance
            has been dropped and collected, one whose instance is still held.
        When:
            A manifest is merged onto the receiver via Chain.from_manifest,
            transiting the reap on the wire-ingress path.
        Then:
            The dropped id should be reaped from spent_tokens while the held id
            is retained — the merge reap bounds only ids with no live instance.
        """
        # Arrange
        var_a = ContextVar(_unique("merge_dropped"))
        var_b = ContextVar(_unique("merge_held"))
        key_a = (var_a.namespace, var_a.name)
        key_b = (var_b.namespace, var_b.name)

        def _receiver() -> tuple[str, str, frozenset[str], frozenset[str], Token]:
            dropped = var_a.set("a")
            id_a = next(iter(wool.__chain__.get().to_manifest().unspent_tokens[key_a]))
            var_a.reset(dropped)
            del dropped
            gc.collect()
            held = var_b.set("b")
            id_b = next(iter(wool.__chain__.get().to_manifest().unspent_tokens[key_b]))
            var_b.reset(held)
            receiver = wool.__chain__.get()
            manifest = ChainManifest(
                id=receiver.id,
                vars={},
                resets=frozenset(),
                stubs=frozenset(),
            )
            Chain.from_manifest(manifest, owned=True, merge_with=receiver)
            chain = wool.__chain__.get()
            return (
                id_a,
                id_b,
                chain.spent_tokens.get(key_a, frozenset()),
                chain.spent_tokens.get(key_b, frozenset()),
                held,
            )

        # Act
        id_a, id_b, spent_a, spent_b, held = contextvars.Context().run(_receiver)

        # Assert
        assert id_a not in spent_a  # instance dropped → reaped on merge
        assert id_b in spent_b  # instance held → retained
        assert isinstance(held, Token)  # keep the held instance alive

    def test_mount_should_reap_a_consumed_token_when_dropped_on_worker_side(self):
        """Test a worker-side (owned=False) mount reaps a consumed id on drop.

        Given:
            A worker-side install (owned=False) of a reconstituted token that
            is reset, held live, then dropped and collected.
        When:
            A second worker-side mount transits the reap.
        Then:
            The consumed id should be retained while the instance is held and
            reaped once it is dropped — the reap runs on the owned=False
            per-step driver path too.
        """
        # Arrange
        var = ContextVar(_unique("worker_reap"))
        key = (var.namespace, var.name)

        def _origin() -> tuple[ChainManifest, bytes]:
            token = var.set("v")
            return wool.__chain__.get().to_manifest(), wool.__serializer__.dumps(token)

        manifest, payload = contextvars.Context().run(_origin)
        tok_id = next(iter(manifest.unspent_tokens[key]))
        gc.collect()

        def _worker() -> tuple[frozenset[str], frozenset[str]]:
            with token_sink() as pending:
                wire = cloudpickle.loads(payload)
            # Worker-side install: owned=False (driven by step-tasks, not a task).
            Chain.from_manifest(manifest, owned=False, wire_tokens=pending)
            var.reset(wire)
            held = wool.__chain__.get().spent_tokens.get(key, frozenset())
            # Drop the instance, then transit another owned=False mount.
            del wire
            del pending
            gc.collect()
            live = wool.__chain__.get()
            empty = ChainManifest(
                id=live.id, vars={}, resets=frozenset(), stubs=frozenset()
            )
            Chain.from_manifest(empty, owned=False, merge_with=live)
            reaped = wool.__chain__.get().spent_tokens.get(key, frozenset())
            return held, reaped

        # Act
        held, reaped = contextvars.Context().run(_worker)

        # Assert
        assert tok_id in held  # consumed, retained while held
        assert tok_id not in reaped  # reaped once dropped, on an owned=False mount

    def test_mount_should_never_prune_unspent_ids(self):
        """Test the reap never removes an id from the anchor (unspent) ledger.

        Given:
            A variable set once — its id live in unspent_tokens — and never
            reset, with its token then dropped and collected.
        When:
            A different variable is set, transiting the reap.
        Then:
            The first id should remain in unspent_tokens — the reap is
            restricted to spent ids, so it never prunes an unspent (in-flight)
            token by instance collection.
        """
        # Arrange
        var_a = ContextVar(_unique("unspent_survive_a"))
        var_b = ContextVar(_unique("unspent_survive_b"))
        key_a = (var_a.namespace, var_a.name)

        def _run() -> tuple[str, frozenset[str]]:
            token = var_a.set("a")  # id_a in unspent, never reset
            id_a = next(iter(wool.__chain__.get().to_manifest().unspent_tokens[key_a]))
            del token
            gc.collect()
            var_b.set("b")  # a mount transiting the reap
            return id_a, wool.__chain__.get().unspent_tokens.get(key_a, frozenset())

        # Act
        id_a, unspent_a = contextvars.Context().run(_run)

        # Assert
        assert id_a in unspent_a

    def test_mount_should_reap_only_the_dead_id_within_a_shared_key(self):
        """Test a partial reap drops the dead id and keeps the live one per key.

        Given:
            One variable reset twice under a single key, so spent_tokens[key]
            holds two consumed ids, with the first token dropped and collected
            (dead) and the second still held live.
        When:
            A fresh set transits mount's reap.
        Then:
            spent_tokens[key] should retain exactly the surviving id under the
            same key — the reap subtracts only the dead id from the key rather
            than dropping the whole key when any of its ids is dead.
        """
        # Arrange
        var = ContextVar(_unique("partial_reap"))
        key = (var.namespace, var.name)

        with scoped_context():
            first = var.set("a")
            id1 = next(iter(wool.__chain__.get().to_manifest().unspent_tokens[key]))
            var.reset(first)
            second = var.set("b")
            id2 = next(iter(wool.__chain__.get().to_manifest().unspent_tokens[key]))
            var.reset(second)
            spent = wool.__chain__.get().spent_tokens.get(key, frozenset())
            assert spent == frozenset({id1, id2})

            # Act — drop only the first instance, then transit a reaping mount.
            del first
            gc.collect()
            var.set("c")

            # Assert — only the dead id is reaped; the survivor stays under the key.
            reaped = wool.__chain__.get().spent_tokens.get(key, frozenset())
            assert reaped == frozenset({id2})
            assert isinstance(second, Token)  # keep the survivor alive

    def test_mount_should_drop_a_spent_key_entirely_when_all_ids_reaped(self):
        """Test a fully-reaped key is removed, not left as an empty frozenset.

        Given:
            A variable set and reset (its key holding one consumed id), then the
            token dropped and collected so the key's only id is dead.
        When:
            A re-arming mount transits the reap.
        Then:
            The key should be absent from spent_tokens entirely — a fully reaped
            key is dropped, not retained as an empty frozenset that would
            accumulate one dead key per reaped variable over a chain's life.
        """
        # Arrange
        var = ContextVar(_unique("key_drop"))
        key = (var.namespace, var.name)

        with scoped_context():
            token = var.set("v")
            token_id = next(iter(wool.__chain__.get().to_manifest().unspent_tokens[key]))
            var.reset(token)
            assert key in wool.__chain__.get().spent_tokens

            # Act
            del token
            gc.collect()
            var.set("again")

            # Assert — the whole key is gone (not an empty frozenset), so the id is too.
            chain = wool.__chain__.get()
            assert key not in chain.spent_tokens
            assert token_id not in chain.spent_tokens.get(key, frozenset())

    def test_mount_should_not_reap_while_token_only_transitively_reachable(self):
        """Test the reap holds while a token is reachable only transitively.

        Given:
            A wire-reconstituted token reset (its consumed id in spent_tokens)
            whose only surviving reference is the token_sink capture list, the
            direct name having been dropped and collected.
        When:
            A mount transits the reap while the sink list still holds the token,
            then again after the sink list is dropped.
        Then:
            The id should stay in spent_tokens while the sink list keeps the
            token reachable, and be reaped only once that last reference is
            dropped — liveness counts every reference, not just named ones.
        """
        # Arrange
        var = ContextVar(_unique("transitive_reach"))
        key = (var.namespace, var.name)

        def _origin() -> tuple[ChainManifest, bytes]:
            token = var.set("v")
            return wool.__chain__.get().to_manifest(), wool.__serializer__.dumps(token)

        manifest, payload = contextvars.Context().run(_origin)
        tok_id = next(iter(manifest.unspent_tokens[key]))
        gc.collect()  # drop the origin-side instance

        def _receiver() -> tuple[frozenset[str], frozenset[str]]:
            with token_sink() as pending:
                wire = cloudpickle.loads(payload)
            Chain.from_manifest(manifest, owned=True, wire_tokens=pending)
            var.reset(wire)
            assert tok_id in wool.__chain__.get().spent_tokens.get(key, frozenset())
            # Drop the direct name only; the sink list ``pending`` still holds it.
            del wire
            gc.collect()
            var.set("bump")  # a reaping mount while ``pending`` keeps it reachable
            held = wool.__chain__.get().spent_tokens.get(key, frozenset())
            # Now drop the last reference and transit another reaping mount.
            del pending
            gc.collect()
            var.set("bump2")
            reaped = wool.__chain__.get().spent_tokens.get(key, frozenset())
            return held, reaped

        # Act
        held, reaped = contextvars.Context().run(_receiver)

        # Assert
        assert held  # retained while the sink list kept the token reachable
        assert not reaped  # reaped once the last reference was dropped

    def test_mount_should_reap_a_token_dropped_on_another_thread(self):
        """Test a token dropped on another thread is still reaped.

        Given:
            An armed chain with a variable set and reset (its consumed id in
            spent_tokens), the token handed to a worker thread inside a
            container.
        When:
            The worker thread drops the token and runs a collection, then a
            mount on the main thread transits the reap.
        Then:
            The consumed id should be reaped — a finalizer firing on a different
            thread than the one that minted the token still releases its count,
            so the reap sees no live instance.
        """
        # Arrange
        var = ContextVar(_unique("cross_thread_reap"))
        key = (var.namespace, var.name)

        with scoped_context():
            token = var.set("v")
            var.reset(token)
            assert key in wool.__chain__.get().spent_tokens
            box = [token]
            del token

            # Act — drop the token's last reference on a different thread.
            def _drop() -> None:
                box.clear()
                gc.collect()

            worker = threading.Thread(target=_drop)
            worker.start()
            worker.join()
            gc.collect()
            var.set("again")  # a reaping mount on the main thread

            # Assert
            assert key not in wool.__chain__.get().spent_tokens

    def test_from_manifest_should_restore_prior_value_when_wire_token_anchored(self):
        """Test an anchored wire token restores a real prior value on reset.

        Given:
            A variable set to a prior value and then re-set, so its token's
            old_value is that prior value rather than unset, serialized with
            its manifest and reconstituted in a fresh receiver context.
        When:
            The manifest is installed via Chain.from_manifest and the
            reconstituted token is reset.
        Then:
            The variable should rewind to the prior value — the wire reset
            path applies the token's captured old value, not just the unset
            sentinel.
        """
        # Arrange
        var = ContextVar(_unique("wire_prior"))

        def _sender() -> tuple[ChainManifest, bytes]:
            var.set("prior")
            token = var.set("current")
            manifest = wool.__chain__.get().to_manifest()
            return manifest, wool.__serializer__.dumps(token)

        manifest, payload = contextvars.Context().run(_sender)

        def _receiver() -> object:
            with token_sink() as pending:
                clone = cloudpickle.loads(payload)
            Chain.from_manifest(manifest, owned=True, wire_tokens=pending)
            var.reset(clone)
            return var.get("<unset>")

        # Act
        restored = contextvars.Context().run(_receiver)

        # Assert
        assert restored == "prior"

    def test_from_manifest_should_poison_origin_when_response_carries_consumption(self):
        """Test a receiver's reset poisons the origin once the response carries it home.

        Given:
            A caller chain that sets a variable, snapshots its request
            manifest, and serialises the minted token; a receiver that
            reconstitutes and resets the clone against that request and
            snapshots the response.
        When:
            The response manifest — now carrying the spent id — is merged
            back onto the caller chain and the caller resets the original
            token.
        Then:
            The receiver reset should rewind the variable there, and the
            caller reset should raise RuntimeError — the consumption rode
            home in the response's spent ledger, not a process-global one.
        """
        # Arrange
        var = ContextVar(_unique("grant_reset"))
        sentinel = object()

        with scoped_context():
            original = var.set("granted")
            caller = wool.__chain__.get()
            request = caller.to_manifest()
            payload = wool.__serializer__.dumps(original)

            def _receiver() -> ChainManifest:
                with token_sink() as pending:
                    clone = cloudpickle.loads(payload)
                Chain.from_manifest(request, owned=True, wire_tokens=pending)
                var.reset(clone)
                assert var.get(sentinel) is sentinel
                return wool.__chain__.get().to_manifest()

            response = contextvars.Context().run(_receiver)

            # Act — the response merges the receiver's consumption home.
            Chain.from_manifest(response, owned=True, merge_with=caller)

            # Assert
            with pytest.raises(RuntimeError, match="has already been used once"):
                var.reset(original)

    def test_from_manifest_should_leave_origin_resettable_when_consumption_unpropagated(
        self,
    ):
        """Test from_manifest keeps the ledger local when a consumption is unpropagated.

        Given:
            A caller chain that sets a variable and dispatches the minted
            token to a receiver that resets the clone — but whose response,
            carrying the consumption, is never merged back.
        When:
            The caller resets the original token, in a fresh context that
            never ingested the spent id.
        Then:
            It should raise ValueError reporting a different Context, not
            RuntimeError — spent is chain-local, so a consumption that never
            rode a frame home cannot poison the origin; the context guard
            catches the stale token instead. This is the deliberate parity
            edge of moving the ledger off a process-global registry.
        """
        # Arrange
        var = ContextVar(_unique("no_response"))

        def _sender() -> tuple[wool.Token, ChainManifest, bytes]:
            token = var.set("granted")
            return (
                token,
                wool.__chain__.get().to_manifest(),
                wool.__serializer__.dumps(token),
            )

        original, request, payload = contextvars.Context().run(_sender)

        def _receiver() -> None:
            with token_sink() as pending:
                clone = cloudpickle.loads(payload)
            Chain.from_manifest(request, owned=True, wire_tokens=pending)
            var.reset(clone)

        contextvars.Context().run(_receiver)

        # Act & assert — no response merged, so the origin's context guard
        # rejects it rather than the (chain-local, never-propagated) ledger.
        with pytest.raises(ValueError, match="was created in a different Context"):
            var.reset(original)

    def test_from_manifest_should_orphan_sibling_token_when_absent_from_snapshot(
        self,
    ):
        """Test a sibling-context token stays an orphan on the receiver.

        Given:
            An armed outer scope binding one variable, a token for a
            second variable minted inside a copy_context sibling (same
            chain id, id absent from the outer snapshot), and a
            manifest taken in the outer scope.
        When:
            A fresh receiver reconstitutes the token, installs the
            outer manifest, and resets the clone.
        Then:
            The reset should raise ValueError reporting the token was
            created in a different Context — the mount gate anchors
            only ids present in the manifest's live-token snapshot.
        """
        # Arrange
        outer_var = ContextVar(_unique("sibling_outer"))
        sibling_var = ContextVar(_unique("sibling_inner"))

        with scoped_context():
            outer_var.set("outer")
            sibling_token = contextvars.copy_context().run(
                lambda: sibling_var.set("sibling")
            )
            payload = wool.__serializer__.dumps(sibling_token)
            manifest = wool.__chain__.get().to_manifest()
            assert sum(len(ids) for ids in manifest.unspent_tokens.values()) == 1

        def _receiver() -> None:
            clone = cloudpickle.loads(payload)
            Chain.from_manifest(manifest, owned=True)

            # Act & assert
            with pytest.raises(ValueError, match="was created in a different Context"):
                sibling_var.reset(clone)

        contextvars.Context().run(_receiver)

    def test_from_manifest_should_bound_unspent_tokens_across_dispatch_reset_stream(
        self,
    ):
        """Test the anchor ledger stays bounded across a dispatch-and-reset stream.

        Given:
            A single long-lived caller chain driving many iterations of
            the dispatch-and-reset streaming pattern: each iteration sets
            a variable to a fresh value — minting one live token — then
            merges back the response manifest a worker would ship after
            resetting that token, carrying the consumed id in
            spent_tokens and a reset signal for the variable.
        When:
            The set-then-merge round-trip repeats N times against the one
            chain, and after each set the caller's live-token ledger and
            the unspent_tokens list that would be encoded onto the next
            dispatch frame are measured.
        Then:
            Both the per-var unspent-token count on the chain and the
            encoded frame's unspent_tokens length should stay pinned at
            the single in-flight token rather than growing with N — each
            reset's consumed id is pruned from the anchor ledger instead
            of accumulating one dead id per frame.
        """
        # Arrange
        n = 64
        var = ContextVar(_unique("stream_bound"))
        key = (var.namespace, var.name)
        unspent_sizes: list[int] = []
        wire_sizes: list[int] = []

        def _stream() -> None:
            for i in range(n):
                live = wool.__chain__.get(None)
                before = (
                    live.to_manifest().unspent_tokens.get(key, frozenset())
                    if live is not None
                    else frozenset()
                )
                # Caller mints a fresh token for this dispatch frame.
                var.set(i)
                chain = wool.__chain__.get()
                manifest = chain.to_manifest()
                # The frame gains exactly the freshly minted id over the
                # prior snapshot — the id a worker would consume and ship
                # home spent.
                minted = manifest.unspent_tokens.get(key, frozenset()) - before
                unspent_sizes.append(len(chain.unspent_tokens.get(key, frozenset())))
                wire = manifest.to_protobuf()
                entry = next(e for e in wire.vars if (e.namespace, e.name) == key)
                wire_sizes.append(len(entry.unspent_tokens))
                # The response a worker ships after resetting the token:
                # the consumed id in the spent ledger and the variable
                # reset to no prior value.
                response = ChainManifest(
                    id=chain.id,
                    vars={},
                    resets=frozenset({key}),
                    stubs=frozenset(),
                    spent_tokens={key: frozenset(minted)},
                )
                Chain.from_manifest(response, owned=True, merge_with=chain)

        # Act
        contextvars.Context().run(_stream)

        # Assert — exactly one in-flight token per iteration, independent of N.
        assert max(unspent_sizes) == 1
        assert max(wire_sizes) == 1

    @pytest.mark.asyncio
    async def test_child_task_should_fork_chain_with_empty_unspent_tokens(self):
        """Test a child task's forked chain drops the parent's unspent token ids.

        Given:
            An armed parent chain holding one live token.
        When:
            A child task created under Wool's task factory snapshots
            its own chain.
        Then:
            The child's snapshot should carry no unspent token ids — the
            per-task fork drops them, so a parent-minted token is
            never anchorable from the fork.
        """
        # Arrange
        var = ContextVar(_unique("fork_live"))

        with scoped_context():
            var.set("bound")
            parent = wool.__chain__.get().to_manifest()
            assert parent.unspent_tokens

            async def child() -> dict[tuple[str, str], frozenset[str]]:
                return wool.__chain__.get().to_manifest().unspent_tokens

            # Act
            child_live = await asyncio.create_task(child())

        # Assert
        assert child_live == {}

    @pytest.mark.asyncio
    async def test_child_task_should_fork_chain_with_empty_spent_tokens(self):
        """Test a child task's forked chain drops the parent's spent token ids.

        Given:
            An armed parent chain that set a variable and reset its token,
            recording a spent id.
        When:
            A child task created under Wool's task factory snapshots its
            own chain.
        Then:
            The child's snapshot should carry no spent token ids — the
            per-task fork drops them like the unspent half, since a
            parent-spent token can never be re-reset from the fork anyway.
        """
        # Arrange
        var = ContextVar(_unique("fork_spent"))

        with scoped_context():
            var.reset(var.set("bound"))
            parent = wool.__chain__.get().to_manifest()
            assert parent.spent_tokens

            async def child() -> dict[tuple[str, str], frozenset[str]]:
                return wool.__chain__.get().to_manifest().spent_tokens

            # Act
            child_spent = await asyncio.create_task(child())

        # Assert
        assert child_spent == {}

    @pytest.mark.asyncio
    async def test_child_task_should_fork_fresh_chain_copying_bindings(self):
        """Test a child task forks a fresh chain that copies bindings and drops resets.

        Given:
            An armed chain carrying one variable binding and one
            reset-pending key (a variable set then reset to no value).
        When:
            A child task is created under Wool's task factory.
        Then:
            The child's chain should carry a different chain id, inherit
            the bound variable, and start with empty resets — copy-on-
            fork mints a fresh chain id, copies the bindings, and drops
            the parent's reset signals.
        """
        # Arrange
        bound_var = ContextVar(_unique("fork_bound"))
        reset_var = ContextVar(_unique("fork_reset"))

        with scoped_context():
            bound_var.set("bound")
            token = reset_var.set("transient")
            reset_var.reset(token)
            parent = wool.__chain__.get(None)
            assert parent is not None
            assert (reset_var.namespace, reset_var.name) in parent.resets

            async def child() -> Chain:
                forked = wool.__chain__.get(None)
                assert forked is not None
                return forked

            # Act
            forked = await asyncio.create_task(child())

        # Assert
        assert forked.id != parent.id
        assert bound_var in forked.vars
        assert forked.resets == frozenset()


def test_copy_context_should_carry_no_wool_variables_when_unarmed():
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
def test_copy_context_should_carry_one_plus_n_wool_variables_when_armed(n):
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


def test_copy_context_should_carry_one_plus_n_wool_variables_when_tokens_live():
    """Test repeated sets add no extra Wool-owned context variables.

    Given:
        A context armed with two bound wool.ContextVars, each set
        three times so several live tokens accumulate on the chain.
    When:
        contextvars.copy_context enumerates its variables.
    Then:
        Exactly 1 + N Wool-owned contextvars.ContextVars should appear
        — live-token bookkeeping rides on the chain itself, adding no
        per-token context variables.
    """
    # Arrange
    bound = [ContextVar(_unique("width_tokens")) for _ in range(2)]

    def _arm() -> None:
        for i, var in enumerate(bound):
            for step in range(3):
                var.set((i, step))

    # Act
    count = _count_wool_vars_in_a_fresh_context(_arm)

    # Assert
    assert count == 1 + 2
