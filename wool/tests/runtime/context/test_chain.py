"""Unit tests for Chain — the live, immutable chain-state model."""

import asyncio
import contextvars
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
            A manifest snapshotted from a sender scope that set a variable
            and reset its token, recording a spent id, and a fresh unarmed
            receiver context.
        When:
            Chain.from_manifest installs the manifest with no merge target.
        Then:
            The receiver chain's snapshot should carry exactly the sender's
            per-var spent token ids — the seed branch mirrors the unspent one.
        """
        # Arrange
        var = ContextVar(_unique("seed_spent"))

        def _sender() -> ChainManifest:
            var.reset(var.set("v"))
            return wool.__chain__.get().to_manifest()

        manifest = contextvars.Context().run(_sender)
        assert manifest.spent_tokens

        def _receiver() -> dict[tuple[str, str], frozenset[str]]:
            assert wool.__chain__.get(None) is None
            Chain.from_manifest(manifest, owned=True)
            return wool.__chain__.get().to_manifest().spent_tokens

        # Act
        received = contextvars.Context().run(_receiver)

        # Assert
        assert received == manifest.spent_tokens

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

    def test_to_manifest_should_retain_spent_tokens_when_var_re_set(self):
        """Test re-setting a variable retains its spent-token ledger entry.

        Given:
            A variable whose token has been consumed by reset, leaving its
            id in the chain snapshot's spent_tokens for that key.
        When:
            The variable is set again, minting a fresh token.
        Then:
            The new snapshot's spent_tokens should still carry the consumed
            id under the key — a copy of the consumed token may live on any
            process the id reached with its used flag unset, so this ledger
            is the only witness rejecting a cross-process double reset.
            (Bounding the ledger to token lifespan is issue #226.)
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
