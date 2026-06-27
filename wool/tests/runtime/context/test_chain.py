"""Unit tests for Chain — the live, immutable chain-state model."""

import asyncio
import contextvars
import threading
import uuid
import warnings
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

import wool
from tests.helpers import scoped_context
from wool import protocol
from wool.runtime.context.chain import Chain
from wool.runtime.context.var import ContextVar


def _unique(stem: str) -> str:
    """Return a process-unique variable name to avoid registry collisions."""
    return f"{stem}_{uuid.uuid4().hex}"


def _count_wool_vars_in_a_fresh_context(work) -> int:
    """Run *work* in a brand-new Chain and count Wool-owned variables.

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


class TestChain:
    def test___init___should_default_collections_empty_when_required_fields_only(self):
        """Test Chain construction with only the required fields.

        Given:
            A fresh chain id and an owner thread id.
        When:
            A Chain is constructed with only chain_id and owner.
        Then:
            It should expose empty data, resets, and stubs
            collections by default.
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

    def test___init___should_expose_supplied_collections_when_all_fields(self):
        """Test Chain construction with every field supplied.

        Given:
            Explicit data, resets, and stubs collections.
        When:
            A Chain is constructed with all fields.
        Then:
            It should expose each supplied collection verbatim.
        """
        # Arrange
        var = ContextVar(_unique("snap_init"))
        bound = frozenset({var})
        resets = frozenset({("ns", "name")})

        # Act
        context = Chain(
            id=uuid4(),
            thread=threading.get_ident(),
            vars=bound,
            resets=resets,
            stubs=frozenset({var}),
        )

        # Assert
        assert context.vars == bound
        assert context.resets == resets
        assert context.stubs == frozenset({var})

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
            resets, and stubs fields — collections that
            satisfy the typing intent of the field but are not
            frozensets at the call site.
        When:
            A Chain is constructed with those iterables.
        Then:
            All three fields should expose frozenset views after
            construction — the post-init coerces non-frozenset
            iterables so the dataclass invariant (hashable + immutable
            container shape) holds regardless of what the caller
            passed.
        """
        # Arrange
        var = ContextVar(_unique("post_init_coerce"))

        # Act — supply a plain set, list, and tuple respectively.
        context = Chain(
            id=uuid4(),
            thread=threading.get_ident(),
            vars={var},  # set, not frozenset
            resets=[("ns", "name")],  # list, not frozenset
            stubs=(var,),  # tuple, not frozenset
        )

        # Assert
        assert isinstance(context.vars, frozenset)
        assert isinstance(context.resets, frozenset)
        assert isinstance(context.stubs, frozenset)
        assert context.vars == frozenset({var})
        assert context.resets == frozenset({("ns", "name")})
        assert context.stubs == frozenset({var})

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
        wire = context.to_protobuf()

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
            wire = wool.__chain__.get().to_protobuf()

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
        wire = context.to_protobuf()

        # Assert
        assert len(wire.vars) == 0

    @pytest.mark.skip(
        reason="external_used / consumed_tokens removed — cross-process token "
        "transport deferred to a Wool Token wrapper ride; see issue #231"
    )
    def test_to_protobuf_should_emit_external_used_token_ids(self):
        """Test to_protobuf propagates external-used token ids on the wire.

        Given:
            An armed chain binding a variable, with an external-used
            log entry for that variable's key installed on the chain.
        When:
            to_protobuf is called.
        Then:
            The wire entry for that variable should list the used token's
            hex id under consumed_tokens.
        """
        # Arrange
        var = ContextVar(_unique("encode_external_used"))
        token_id = uuid4()

        with scoped_context():
            var.set("x")
            context = wool.__chain__.get(None)
            assert context is not None
            context = context.evolve(external_used={token_id: (var.namespace, var.name)})

            # Act
            wire = context.to_protobuf()

            # Assert
            entry = next(
                e
                for e in wire.vars
                if (e.namespace, e.name) == (var.namespace, var.name)
            )
            assert list(entry.consumed_tokens) == [token_id.hex]

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
        wire = context.to_protobuf()

        # Assert
        entry = next(
            e for e in wire.vars if (e.namespace, e.name) == (var.namespace, var.name)
        )
        assert not entry.HasField("value")

    @pytest.mark.skip(
        reason="external_used removed — cross-process token transport deferred; #231"
    )
    def test_to_protobuf_should_suppress_used_tokens_when_unserializable_value(self):
        """Test to_protobuf omits used tokens when the value fails to serialize.

        Given:
            An armed chain binding a variable to an unserializable value,
            with an external-used log entry for the same variable.
        When:
            to_protobuf is called.
        Then:
            The chain manifest should carry no entry for that variable —
            neither the value nor the used tokens are emitted, to prevent
            a phantom reset on the receiver.
        """
        # Arrange
        var = ContextVar(_unique("encode_phantom_suppress"))
        token_id = uuid4()

        with scoped_context():
            var.set(threading.Lock())
            context = wool.__chain__.get(None)
            assert context is not None
            context = context.evolve(external_used={token_id: (var.namespace, var.name)})

            # Act
            with warnings.catch_warnings(record=True):
                warnings.simplefilter("always")
                wire = context.to_protobuf()

            # Assert
            keys = {(entry.namespace, entry.name) for entry in wire.vars}
            assert (var.namespace, var.name) not in keys

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
                wire = wool.__chain__.get().to_protobuf()

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

            # Act & assert
            with warnings.catch_warnings():
                warnings.filterwarnings("error", category=wool.SerializationWarning)
                with pytest.raises(wool.ChainSerializationError) as exc_info:
                    context.to_protobuf()
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
            wire = context.to_protobuf()

        # Assert
        keys = {(entry.namespace, entry.name) for entry in wire.vars}
        assert (var.namespace, var.name) not in keys

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
