import asyncio
import gc
import uuid
from types import SimpleNamespace
from typing import Any
from typing import cast

import cloudpickle
import pytest

import wool
from tests.helpers import scoped_context
from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import RuntimeContext
from wool.runtime.context import Token
from wool.runtime.context import attached
from wool.runtime.context import copy_context
from wool.runtime.context import current_context
from wool.runtime.context import dispatch_timeout
from wool.runtime.serializer import Serializer

dumps = wool.__serializer__.dumps
loads = cloudpickle.loads


class TestRuntimeContext:
    def test___init___with_default_sentinel(self):
        """Test RuntimeContext entered with the default sentinel skips
        installing the stdlib dispatch_timeout value.

        Given:
            A RuntimeContext constructed with no dispatch_timeout
            argument and a stdlib dispatch_timeout value already
            installed in the current scope
        When:
            The RuntimeContext is entered as a context manager and the
            stdlib var is read inside the block
        Then:
            It should observe the prior value unchanged — the
            "no-override" usage leaves the live var alone
        """
        # Arrange
        prior_token = dispatch_timeout.set(7.5)
        try:
            # Act
            with RuntimeContext():
                observed = dispatch_timeout.get()

            # Assert
            assert observed == 7.5
        finally:
            dispatch_timeout.reset(prior_token)

    def test___enter___with_explicit_value(self):
        """Test RuntimeContext entered with an explicit value overrides
        and restores the stdlib dispatch_timeout.

        Given:
            A RuntimeContext constructed with ``dispatch_timeout=2.5``
            and a different prior value installed in the current scope
        When:
            The RuntimeContext is entered as a context manager, the
            stdlib var is read inside the block, then the block exits
            and the var is read again
        Then:
            The in-block read should return ``2.5`` and the post-exit
            read should return the prior value — __enter__/__exit__
            performs a scoped override
        """
        # Arrange
        prior_token = dispatch_timeout.set(1.0)
        try:
            inside: list[float | None] = []

            # Act
            with RuntimeContext(dispatch_timeout=2.5):
                inside.append(dispatch_timeout.get())
            after = dispatch_timeout.get()

            # Assert
            assert inside == [2.5]
            assert after == 1.0
        finally:
            dispatch_timeout.reset(prior_token)

    def test___enter___with_explicit_none(self):
        """Test RuntimeContext entered with explicit ``None`` overrides
        the stdlib dispatch_timeout to ``None`` (distinct from the
        Undefined skip path).

        Given:
            A RuntimeContext constructed with ``dispatch_timeout=None``
            and a non-None prior value installed in the current scope
        When:
            The RuntimeContext is entered as a context manager and the
            stdlib var is read inside the block
        Then:
            It should observe ``None`` inside — explicit None is a
            real override that __enter__ applies, separate from the
            default Undefined sentinel that skips
        """
        # Arrange
        prior_token = dispatch_timeout.set(4.0)
        try:
            inside: list[float | None] = []

            # Act
            with RuntimeContext(dispatch_timeout=None):
                inside.append(dispatch_timeout.get())

            # Assert
            assert inside == [None]
        finally:
            dispatch_timeout.reset(prior_token)

    def test_get_current_with_live_value(self):
        """Test RuntimeContext.get_current snapshots the live stdlib
        dispatch_timeout value.

        Given:
            A stdlib ``dispatch_timeout`` set to ``4.0`` in the current
            scope
        When:
            ``RuntimeContext.get_current`` is called
        Then:
            The returned RuntimeContext's ``to_protobuf`` should emit
            ``dispatch_timeout=4.0`` — get_current captures the live
            value rather than carrying the Undefined sentinel forward
        """
        # Arrange
        prior_token = dispatch_timeout.set(4.0)
        try:
            # Act
            captured = RuntimeContext.get_current()
            wire = captured.to_protobuf()

            # Assert
            assert wire.HasField("dispatch_timeout") is True
            assert wire.dispatch_timeout == 4.0
        finally:
            dispatch_timeout.reset(prior_token)

    def test_from_protobuf_with_dispatch_timeout_set(self):
        """Test RuntimeContext.from_protobuf decodes the dispatch_timeout
        wire value.

        Given:
            A ``protocol.RuntimeContext`` message with
            ``dispatch_timeout=12.5``
        When:
            ``RuntimeContext.from_protobuf`` decodes it and the result
            is entered as a context manager
        Then:
            The stdlib var inside the block should observe ``12.5``
        """
        # Arrange
        from wool import protocol

        wire = protocol.RuntimeContext(dispatch_timeout=12.5)

        # Act
        decoded = RuntimeContext.from_protobuf(wire)
        observed: list[float | None] = []
        with decoded:
            observed.append(dispatch_timeout.get())

        # Assert
        assert observed == [12.5]

    def test_from_protobuf_without_dispatch_timeout(self):
        """Test RuntimeContext.from_protobuf treats an absent wire
        dispatch_timeout as an explicit ``None`` override.

        Given:
            A ``protocol.RuntimeContext`` message with no
            ``dispatch_timeout`` field set
        When:
            ``RuntimeContext.from_protobuf`` decodes it and the result
            is re-emitted via ``to_protobuf``
        Then:
            The re-emitted wire message should have
            ``HasField('dispatch_timeout')`` False — the absent wire
            field decodes to explicit ``None``, which on re-emission
            skips the field rather than substituting the live scope
            value
        """
        # Arrange
        from wool import protocol

        wire = protocol.RuntimeContext()

        # Act
        decoded = RuntimeContext.from_protobuf(wire)
        re_emitted = decoded.to_protobuf()

        # Assert
        assert re_emitted.HasField("dispatch_timeout") is False

    def test_to_protobuf_with_explicit_value(self):
        """Test RuntimeContext.to_protobuf emits an explicitly-set
        dispatch_timeout value.

        Given:
            A RuntimeContext constructed with ``dispatch_timeout=3.0``
        When:
            ``to_protobuf`` is called
        Then:
            The returned message should have
            ``HasField('dispatch_timeout')`` True and
            ``dispatch_timeout == 3.0``
        """
        # Arrange
        ctx = RuntimeContext(dispatch_timeout=3.0)

        # Act
        wire = ctx.to_protobuf()

        # Assert
        assert wire.HasField("dispatch_timeout") is True
        assert wire.dispatch_timeout == 3.0

    def test_to_protobuf_with_explicit_none(self):
        """Test RuntimeContext.to_protobuf skips emission when the value
        is explicit ``None``.

        Given:
            A RuntimeContext constructed with ``dispatch_timeout=None``
        When:
            ``to_protobuf`` is called
        Then:
            The returned message should have
            ``HasField('dispatch_timeout')`` False — explicit None
            means "no timeout", which the wire shape encodes as
            absence so the receiver inherits its own scope's default
        """
        # Arrange
        ctx = RuntimeContext(dispatch_timeout=None)

        # Act
        wire = ctx.to_protobuf()

        # Assert
        assert wire.HasField("dispatch_timeout") is False

    def test_to_protobuf_with_default_sentinel_substitutes_live_value(self):
        """Test RuntimeContext.to_protobuf substitutes the live scope
        value when constructed with the default Undefined sentinel.

        Given:
            A RuntimeContext constructed with no dispatch_timeout
            argument (Undefined sentinel) and a stdlib
            ``dispatch_timeout`` value of ``9.5`` set in the current
            scope
        When:
            ``to_protobuf`` is called
        Then:
            The returned message should emit ``dispatch_timeout=9.5``
            — the encode-time live-value substitution lets a bare
            ``RuntimeContext()`` ride the wire with the encoder's
            effective timeout
        """
        # Arrange
        prior_token = dispatch_timeout.set(9.5)
        try:
            ctx = RuntimeContext()

            # Act
            wire = ctx.to_protobuf()

            # Assert
            assert wire.HasField("dispatch_timeout") is True
            assert wire.dispatch_timeout == 9.5
        finally:
            dispatch_timeout.reset(prior_token)

    def test_to_protobuf_with_default_sentinel_and_no_live_value(self):
        """Test RuntimeContext.to_protobuf skips emission when the
        sentinel resolves to a live ``None``.

        Given:
            A RuntimeContext constructed with no dispatch_timeout
            argument and the stdlib ``dispatch_timeout`` at its
            default (None) in the current scope
        When:
            ``to_protobuf`` is called
        Then:
            The returned message should have
            ``HasField('dispatch_timeout')`` False — Undefined
            substituted to None, which then skips emission, mirroring
            the explicit-None branch
        """
        # Arrange
        ctx = RuntimeContext()

        # Act
        wire = ctx.to_protobuf()

        # Assert
        assert wire.HasField("dispatch_timeout") is False

    def test___exit___when_called_after_unentered_context(self):
        """Test RuntimeContext.__exit__ is a no-op when no token was
        captured.

        Given:
            A RuntimeContext constructed with the default Undefined
            sentinel and never entered (so no token was captured)
        When:
            ``__exit__`` is invoked directly on the instance
        Then:
            It should return without error and without mutating the
            stdlib var — the no-token branch is reached
        """
        # Arrange
        prior_token = dispatch_timeout.set(5.0)
        try:
            ctx = RuntimeContext()

            # Act
            ctx.__exit__(None, None, None)

            # Assert
            assert dispatch_timeout.get() == 5.0
        finally:
            dispatch_timeout.reset(prior_token)


class TestContext:
    def test___new___with_direct_instantiation(self):
        """Test Context() constructs an empty Context with a fresh id.

        Given:
            The Context class
        When:
            It is instantiated directly
        Then:
            The result should have a fresh id and no captured vars
        """
        # Act
        ctx = Context()

        # Assert
        assert ctx.id is not None
        assert len(ctx) == 0

    def test___bool___is_false_for_fresh_context(self):
        """Test bool(Context()) is False when no state has been captured.

        Given:
            A freshly constructed empty Context
        When:
            bool() is invoked on it
        Then:
            The result should be False so callers can use
            ``if not ctx:`` as a fast-path gate
        """
        # Act
        ctx = Context()

        # Assert
        assert bool(ctx) is False

    def test___bool___when_var_is_set(self):
        """Test bool(ctx) is True once a ContextVar has been set in it.

        Given:
            A Context with a var bound via ContextVar.set
        When:
            bool() is invoked on it
        Then:
            The result should be True
        """
        # Arrange
        var = ContextVar("bool_var_set", default="initial")
        var.set("value")
        ctx = current_context()

        # Act
        result = bool(ctx)

        # Assert
        assert result is True

    def test_has_state_is_true_when_wire_carried_consumed_tokens(self):
        """Test ctx.has_state() is True when _data is empty but the
        wire Context carried a non-empty consumed_tokens list.

        Given:
            A wire protocol.Context with no var bindings but a
            non-empty consumed_tokens list (modeling a back-prop
            response that only carries used-token state)
        When:
            Context.from_protobuf reconstructs it and has_state() is
            invoked on the result
        Then:
            The result should be True — the ids must be applied on
            merge so the corresponding live tokens flip their _used
            flags. ``bool()`` follows ``__len__`` (var bindings only)
            so it remains False; ``has_state()`` is the wire-truthy
            predicate
        """
        # Arrange
        from wool import protocol

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars.add(
            namespace="",
            name="",
            consumed_tokens=[uuid.uuid4().hex],
        )

        # Act
        reconstructed = Context.from_protobuf(pb)

        # Assert
        assert len(reconstructed) == 0
        assert bool(reconstructed) is False
        assert reconstructed.has_state() is True

    def test_has_state_is_true_when_live_consumed_token_is_present(self):
        """Test ctx.has_state() is True when _data is empty but a
        live consumed Token populated by ``ContextVar.reset`` is
        still reachable.

        Given:
            A Context that ran a set+reset cycle on a ContextVar
            and a strong reference to the consumed Token kept alive
            outside the Context — the var binding cleared back to
            MISSING but the Token sits in ``_used_tokens`` for
            outbound wire emission
        When:
            has_state() is invoked on the Context
        Then:
            The result should be True — the Context still carries
            wire-shippable state via ``Context.to_protobuf``'s
            ``consumed_tokens`` field, so ``has_state()`` aligns
            with what the wire emission produces. ``bool()`` follows
            ``__len__`` (var bindings only) and remains False
        """
        # Arrange
        var = ContextVar(f"used_token_id_truthy_{uuid.uuid4().hex}")
        ctx = Context()
        captured: list[Token[Any]] = []

        def consume():
            t = var.set("x")
            var.reset(t)
            captured.append(t)

        ctx.run(consume)

        # Act
        result = ctx.has_state()

        # Assert
        assert len(ctx) == 0
        assert bool(ctx) is False
        assert result is True

    def test_consumed_tokens_are_bounded_by_live_token_count(self):
        """Test the consumed-token tracking set does not grow
        unboundedly across many set/reset cycles whose Tokens are
        not retained.

        Given:
            A Context that performs 1000 set/reset cycles on the
            same ContextVar, with no strong reference kept to any
            Token after its reset
        When:
            ``gc.collect`` runs and the Context's emitted
            ``consumed_tokens`` is inspected
        Then:
            The emitted list has length 0 — the auto-pruning
            ``weakref.WeakSet`` reclaimed all Tokens whose role
            (double-reset detection) has nothing left to bind to,
            so the chain forwards no stale UUIDs onward
        """
        # Arrange
        var = ContextVar(f"bounded_consumed_tokens_{uuid.uuid4().hex}")
        ctx = Context()

        def churn():
            for _ in range(1000):
                t = var.set("x")
                var.reset(t)

        ctx.run(churn)
        gc.collect()

        # Act
        emitted = ctx.to_protobuf()

        # Assert
        emitted_token_ids = [
            tid for entry in emitted.vars for tid in entry.consumed_tokens
        ]
        assert emitted_token_ids == []

    def test___bool___when_consumed_token_is_collected(self):
        """Test bool(ctx) is False after the only reference to a
        locally-consumed Token is dropped.

        Given:
            A Context that ran a set+reset cycle whose Token was
            never propagated outward and whose only strong reference
            (the function-local ``t``) has gone out of scope, so the
            ``weakref.WeakSet`` tracking entry has been pruned by GC
        When:
            bool() is invoked on the Context
        Then:
            The result should be False — there is no live Token
            anywhere whose double-reset detection could be triggered,
            so the consumed-state record has nothing left to forward;
            the new ownership-handoff design prefers reclamation over
            indefinite UUID retention
        """
        # Arrange
        var = ContextVar(f"used_token_id_collected_{uuid.uuid4().hex}")
        ctx = Context()

        def consume():
            t = var.set("x")
            var.reset(t)

        ctx.run(consume)
        gc.collect()

        # Act
        result = bool(ctx)

        # Assert
        assert len(ctx) == 0
        assert result is False

    def test_update_with_live_token_when_wire_carries_id(self):
        """Test Context.update flips a live unused Token's used flag
        when the incoming wire Context lists its id.

        Given:
            A live Token whose used flag is False, and a temp
            Context reconstructed from a wire message whose
            consumed_tokens list contains that Token's id
        When:
            current_context().update(temp) is called
        Then:
            Token.used on the live Token flips to True — the merge
            resolves the incoming id through the process-wide token
            registry and applies cross-process consumption to the
            local instance
        """
        # Arrange
        from wool import protocol

        var = ContextVar("update_flip_used", default="d")
        token = var.set("x")
        assert token.used is False

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars.add(
            namespace=var.namespace,
            name=var.name,
            consumed_tokens=[token.id.hex],
        )
        incoming = Context.from_protobuf(pb)

        # Act
        current_context().update(incoming)

        # Assert
        assert token.used is True

    def test_update_with_incoming_id_and_no_live_token(self):
        """Test Context.update ignores incoming ids with no live Token.

        Given:
            A current Context and a temp Context reconstructed from
            a wire message whose consumed_tokens list contains a
            UUID that matches no live Token in the process
        When:
            current_context().update(temp) is called
        Then:
            The merge completes without raising — unregistered
            incoming ids are silently dropped because no peer Token
            object exists to flip
        """
        # Arrange
        from wool import protocol

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars.add(
            namespace="",
            name="",
            consumed_tokens=[uuid.uuid4().hex],
        )
        incoming = Context.from_protobuf(pb)

        # Act & assert
        current_context().update(incoming)

    def test_run_seeds_vars_and_scopes_mutations(self):
        """Test Context.run seeds vars in a fresh stdlib Context and scopes mutations.

        Given:
            A Context captured with an initial var value
        When:
            Context.run() runs a function that mutates the var
        Then:
            Mutations should be visible inside the run and captured on exit
        """
        # Arrange
        var = ContextVar("run_seed", default="initial")
        var.set("seeded")
        ctx = current_context()

        def body():
            assert var.get() == "seeded"
            var.set("mutated")
            return var.get()

        # Act
        result = ctx.run(body)

        # Assert
        assert result == "mutated"
        assert ctx[var] == "mutated"

    def test_run_binds_context_for_sync_callers(self):
        """Test Context.run makes self.id the active Context id inside fn.

        Given:
            A Context constructed directly (sync caller, no asyncio task)
        When:
            Context.run invokes a function that reads current_context().id
        Then:
            The reported context id equals the Context's own id, not the
            process-default id
        """
        # Arrange
        ctx = Context()

        # Act
        observed = ctx.run(lambda: current_context().id)

        # Assert
        assert observed == ctx.id

    def test_run_snapshot_with_unset_or_reset_vars(self):
        """Test Context.run's snapshot excludes vars that look unset at exit.

        Given:
            One var never set inside the run, one var that is set and
            then reset inside the run
        When:
            Context.run() returns and captures the post-run snapshot
        Then:
            Neither var should appear in the Context's captured vars
        """
        # Arrange
        untouched = ContextVar("untouched", default="x")
        set_then_reset = ContextVar("set_then_reset")
        ctx = Context()

        def body():
            token = set_then_reset.set("temp")
            set_then_reset.reset(token)

        # Act
        ctx.run(body)

        # Assert
        assert untouched not in ctx
        assert set_then_reset not in ctx

    def test_run_with_re_entry_on_same_task(self):
        """Test Context.run rejects a nested run() call on the same task.

        Given:
            A Context already executing a synchronous run() body
        When:
            A second run() is attempted from inside the body, on
            the same thread/task
        Then:
            The single-task guard rejects the inner call —
            re-entry from the owning execution scope is treated the
            same as concurrent entry from another scope. Cross-thread
            concurrency is exercised separately by
            ``test_attached_with_concurrent_entry``.
        """
        # Arrange
        ctx = Context()

        def outer():
            with pytest.raises(RuntimeError):
                ctx.run(lambda: None)

        # Act & assert
        ctx.run(outer)

    @pytest.mark.asyncio
    async def test_attached_keeps_context_across_await(self):
        """Test attached(ctx) keeps the supplied Context installed
        across coroutine suspension points.

        Given:
            A Context populated with a ContextVar binding and an
            async function that suspends via ``await asyncio.sleep(0)``
            before reading the var
        When:
            The async function is awaited inside a ``with attached(ctx)``
            block
        Then:
            The post-suspension read should observe the Context's
            value — the attach scope spans the entire coroutine
            body, not just the synchronous frame that constructed
            the coroutine
        """
        # Arrange
        var = ContextVar("attached_keeps_context", default="default")
        ctx = Context()
        ctx.run(lambda: var.set("inside"))
        observed: list[str] = []

        async def read_after_suspend():
            await asyncio.sleep(0)
            observed.append(var.get())

        # Act
        with attached(ctx):
            await read_after_suspend()

        # Assert
        assert observed == ["inside"]

    @pytest.mark.asyncio
    async def test_attached_with_concurrent_entry(self):
        """Test attached(ctx) raises when another task is already
        running inside the same supplied Context.

        Given:
            A Context with one task suspended inside an
            ``attached(ctx)`` block (holding the single-task guard
            across an await)
        When:
            A concurrent task attempts ``attached(ctx)`` on the same
            Context
        Then:
            It should raise RuntimeError — the single-task
            invariant holds across the await window, not just the
            synchronous portion
        """
        # Arrange
        ctx = Context()
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        outcomes: list[str] = []

        async def first():
            with attached(ctx):
                first_entered.set()
                await release_first.wait()

        async def second():
            try:
                with attached(ctx):
                    pass
            except RuntimeError:
                outcomes.append("second-rejected")

        first_task = asyncio.create_task(first())
        await first_entered.wait()

        # Act
        await second()
        release_first.set()
        await first_task

        # Assert
        assert outcomes == ["second-rejected"]

    def test_attached_installs_and_restores_binding(self):
        """Test attached(ctx) makes ctx the current Context and restores
        the prior binding on exit.

        Given:
            A Context that is not the active scope's Context
        When:
            ``with attached(ctx):`` is entered, current_context()
            is read inside, and the block exits
        Then:
            current_context() inside the block is ctx; after exit,
            current_context() resolves to the prior binding
        """
        # Arrange
        prior = current_context()
        target = Context()

        # Act & assert
        assert current_context() is prior
        with attached(target):
            assert current_context() is target
        assert current_context() is prior

    def test_attached_unguarded_is_reentrant_against_running_context(self):
        """Test attached(ctx, guarded=False) does not acquire the
        single-task guard and so may be used while ctx is already
        running a routine.

        Given:
            A Context already executing a routine via ``Context.run``
            (holding the single-task guard)
        When:
            ``with attached(ctx, guarded=False):`` is entered against
            the same ctx from inside the running routine
        Then:
            It does not raise — guarded=False opts out of the
            single-task claim so deserialization scopes nested inside
            a running routine remain valid
        """
        # Arrange
        ctx = Context()
        observed: list[Context] = []

        def inside_run():
            with attached(ctx, guarded=False):
                observed.append(current_context())

        # Act
        ctx.run(inside_run)

        # Assert
        assert observed == [ctx]

    def test_copy_with_populated_source(self):
        """Test Context.copy returns a sibling Context with the same
        var bindings but a fresh logical-chain id.

        Given:
            A Context populated with one or more var bindings via
            ``Context.run``
        When:
            ``source.copy()`` is invoked on the populated Context,
            and a second ``source.copy()`` is invoked alongside
        Then:
            Each copy holds the same var bindings as the source,
            each copy's id differs from the source's id, and the
            two copies' ids differ from each other — mirrors
            ``contextvars.Context.copy`` semantics with wool's
            chain-id contract that copies are new chains in the
            tree, not aliases of the source
        """
        # Arrange
        var = ContextVar(f"copy_source_{uuid.uuid4().hex}")
        source = Context()
        source.run(lambda: var.set("seed-value"))

        # Act
        sibling_a = source.copy()
        sibling_b = source.copy()

        # Assert
        assert sibling_a.id != source.id
        assert sibling_b.id != source.id
        assert sibling_a.id != sibling_b.id
        assert sibling_a[var] == "seed-value"
        assert sibling_b[var] == "seed-value"

    def test_iter_yields_captured_vars(self):
        """Test Context iterates over captured ContextVar instances.

        Given:
            A Context with multiple captured vars
        When:
            It is iterated
        Then:
            The iterator should yield each captured var
        """
        # Arrange
        a = ContextVar("iter_a", default=0)
        b = ContextVar("iter_b", default=0)
        a.set(1)
        b.set(2)

        # Act
        ctx = current_context()

        # Assert
        assert set(iter(ctx)) == {a, b}

    def test_getitem_with_captured_value(self):
        """Test Context[var] returns the captured value.

        Given:
            A Context with a captured var
        When:
            The Context is indexed by the var
        Then:
            The captured value should be returned
        """
        # Arrange
        var = ContextVar("get_item", default=0)
        var.set(1)
        ctx = current_context()

        # Act & assert
        assert ctx[var] == 1

    def test_contains_reports_membership(self):
        """Test `var in ctx` reports whether the var was captured.

        Given:
            A Context with one captured var and one uncaptured var
        When:
            Membership is tested for each
        Then:
            The captured var should be present and the uncaptured absent
        """
        # Arrange
        in_ctx = ContextVar("present_var", default=0)
        out_ctx = ContextVar("absent_var", default=0)
        in_ctx.set(1)

        # Act
        ctx = current_context()

        # Assert
        assert in_ctx in ctx
        assert out_ctx not in ctx

    def test_len_with_captured_vars(self):
        """Test len(ctx) returns the number of captured vars.

        Given:
            A Context with two captured vars
        When:
            len() is called on the Context
        Then:
            It should return 2
        """
        # Arrange
        a = ContextVar("len_a", default=0)
        b = ContextVar("len_b", default=0)
        a.set(1)
        b.set(2)

        # Act
        ctx = current_context()

        # Assert
        assert len(ctx) == 2

    def test_keys_values_items_expose_captured_pairs(self):
        """Test Context keys/values/items expose the captured mapping.

        Given:
            A Context with two captured vars
        When:
            keys(), values(), items() are called
        Then:
            Each accessor should return the expected captured pairs
        """
        # Arrange
        a = ContextVar("kvitems_a", default="")
        b = ContextVar("kvitems_b", default="")
        a.set("x")
        b.set("y")

        # Act
        ctx = current_context()

        # Assert
        assert set(ctx.keys()) == {a, b}
        assert set(ctx.values()) == {"x", "y"}
        assert dict(ctx.items()) == {a: "x", b: "y"}

    def test_get_with_set_value_or_default(self):
        """Test Context.get returns the set value or the supplied default.

        Given:
            A Context with one var set and another var that was never
            set in this Context
        When:
            get(var) and get(var, default) are called
        Then:
            The set var returns its value; the unset var returns the
            supplied default (or None if no default is given)
        """
        # Arrange
        set_var = ContextVar("get_set", default="class-default")
        unset_var = ContextVar("get_unset", default="class-default")
        set_var.set("value")
        ctx = current_context()

        # Act & assert
        assert ctx.get(set_var) == "value"
        assert ctx.get(set_var, "fallback") == "value"
        assert ctx.get(unset_var) is None
        assert ctx.get(unset_var, "fallback") == "fallback"

    def test_repr_includes_id_and_var_count(self):
        """Test Context repr mentions id and number of vars.

        Given:
            A Context with one captured var
        When:
            repr() is called on it
        Then:
            The repr should contain "id=" and "vars=1"
        """
        # Arrange
        var = ContextVar("repr_var", default=0)
        var.set(1)
        ctx = current_context()

        # Act
        text = repr(ctx)

        # Assert
        assert "id=" in text
        assert "vars=1" in text

    def test___reduce_ex___under_pickle_copy_and_deepcopy(self):
        """Test wool.Context refuses pickle, copy.copy, and copy.deepcopy.

        Given:
            A live wool.Context
        When:
            pickle.dumps, cloudpickle.dumps, wool.__serializer__.dumps,
            copy.copy, and copy.deepcopy are each invoked on it
        Then:
            All five raise TypeError. Wool's own pickler rejects too —
            Context has no __wool_reduce__ — so a snapshot disconnected
            from live state cannot leak through any pickling path.
            Callers must use Context.copy() explicitly for in-process
            duplication; cross-process propagation rides
            Context.to_protobuf and Context.from_protobuf instead.
        """
        # Arrange
        import copy as _copy
        import pickle

        var = ContextVar("ctx_unpicklable", default="zero")
        var.set("one")
        ctx = current_context()

        # Act & assert
        with pytest.raises(TypeError, match="wool.Context"):
            pickle.dumps(ctx)
        with pytest.raises(TypeError, match="wool.Context"):
            cloudpickle.dumps(ctx)
        with pytest.raises(TypeError, match="wool.Context"):
            wool.__serializer__.dumps(ctx)
        with pytest.raises(TypeError, match="wool.Context"):
            _copy.copy(ctx)
        with pytest.raises(TypeError, match="wool.Context"):
            _copy.deepcopy(ctx)

    def test_to_protobuf_with_unpicklable_value(self):
        """Test Context.to_protobuf emits a ContextDecodeWarning for an
        unpicklable var and skips that entry.

        Given:
            A ContextVar set to an unpicklable value (a local generator
            function object) alongside a ContextVar set to a
            serializable value
        When:
            current_context().to_protobuf() is called to snapshot the
            current vars
        Then:
            A ContextDecodeWarning is emitted naming the offending var
            key, the returned wire context contains only the
            serializable var, and the snapshot does not preempt the
            primary signal — mirroring from_protobuf's per-entry
            decode resilience.
        """
        # Arrange
        from wool import ContextDecodeWarning

        good = ContextVar("ctx001_serializable", default="default")
        bad = ContextVar("ctx001_unpicklable")

        def _local_gen():
            yield 1

        good.set("kept")
        bad.set(_local_gen())

        # Act
        with pytest.warns(ContextDecodeWarning, match=bad.name):
            wire_ctx = current_context().to_protobuf()

        # Assert
        emitted_keys = {(e.namespace, e.name) for e in wire_ctx.vars}
        assert (good.namespace, good.name) in emitted_keys, (
            "Serializable var should ride the wire alongside the skipped entry"
        )
        assert (bad.namespace, bad.name) not in emitted_keys, (
            "Unpicklable entry should be skipped, not partially encoded"
        )

    def test_to_protobuf_with_multiple_unpicklable_values_strict_mode(self):
        """Test Context.to_protobuf aggregates strict-mode encode
        failures into a single BaseExceptionGroup naming every
        offending var.

        Given:
            Two ContextVars each set to a value whose ``__reduce__``
            raises, with a strict warnings filter promoting
            ContextDecodeWarning to an exception
        When:
            current_context().to_protobuf() is called to snapshot the
            current vars
        Then:
            A BaseExceptionGroup is raised whose peers are
            ContextDecodeWarning instances — one per offending var,
            each naming the corresponding key — so callers learn
            about every bad var on a single dispatch attempt.
        """
        import warnings as _warnings

        from wool import ContextDecodeWarning

        # Arrange
        first = ContextVar("ctx001_strict_a")
        second = ContextVar("ctx001_strict_b")

        class _Unpicklable:
            def __reduce__(self):
                raise TypeError("synthetic unpicklable")

        first.set(_Unpicklable())
        second.set(_Unpicklable())

        # Act & assert
        with _warnings.catch_warnings():
            _warnings.simplefilter("error", category=ContextDecodeWarning)
            with pytest.raises(BaseExceptionGroup) as exc_info:
                current_context().to_protobuf()

        peers = exc_info.value.exceptions
        assert all(isinstance(p, ContextDecodeWarning) for p in peers), (
            "Every peer should be a ContextDecodeWarning"
        )
        peer_messages = [str(p) for p in peers]
        assert any(first.name in msg for msg in peer_messages), (
            "First offending var should be named in a peer"
        )
        assert any(second.name in msg for msg in peer_messages), (
            "Second offending var should be named in a peer"
        )
        assert len(peers) == 2, "Group should hold one peer per offending var, no more"

    def test_to_protobuf_omits_entry_when_value_serialization_fails_with_consumed_tokens(
        self,
    ):
        """Test Context.to_protobuf omits the wire entry entirely when a
        var's value fails to serialize, even when the var carries
        consumed tokens.

        Given:
            A Context where a var was set, reset (producing a consumed
            token held by a strong reference), and re-set to an
            unserializable value — the var carries both a consumed
            token in :attr:`_used_tokens` and a current binding in
            :attr:`_data` whose value cannot be serialized.
        When:
            ``ctx.to_protobuf()`` is called.
        Then:
            A :class:`ContextDecodeWarning` is emitted, and no wire
            entry is produced for the offending var — neither the
            value nor the consumed-token list rides the wire. A
            half-encoded entry (consumed tokens but no value) would
            propagate a phantom reset to the receiver.
        """
        # Arrange
        from wool import ContextDecodeWarning

        var = ContextVar(f"halfencoded_{uuid.uuid4().hex}")
        ctx = Context()
        captured: list[Token[Any]] = []

        def _local_gen():
            yield 1

        def cycle() -> None:
            captured.append(var.set("serializable"))
            var.reset(captured[0])
            var.set(_local_gen())

        ctx.run(cycle)

        # Act
        with pytest.warns(ContextDecodeWarning, match=var.name):
            emitted = ctx.to_protobuf()

        # Assert
        emitted_keys = {(e.namespace, e.name) for e in emitted.vars}
        assert (var.namespace, var.name) not in emitted_keys, (
            "A var whose value failed to serialize must be suppressed "
            "entirely; emitting consumed tokens without a value would "
            "propagate a phantom reset to the receiver"
        )

    def test_update_does_not_spuriously_reset_on_sender_serialization_failure(self):
        """Test Context.update preserves the receiver's binding when
        the sender's wire frame omits a var due to a serialization
        failure on the sender.

        Given:
            A receiver Context populated from a clean initial frame
            binding ``var`` to ``"serializable"``; a sender Context
            that subsequently ran a reset+re-set cycle re-setting the
            var to an unserializable value, then produced a second
            wire frame.
        When:
            ``receiver.update(Context.from_protobuf(second_frame))``
            is called.
        Then:
            The receiver still observes ``var == "serializable"`` —
            the sender's serialization failure does not propagate a
            phantom reset that would clobber the receiver's prior
            binding.
        """
        # Arrange
        from wool import ContextDecodeWarning

        var = ContextVar(f"no_phantom_reset_{uuid.uuid4().hex}")
        sender = Context()
        captured: list[Token[Any]] = []

        def _local_gen():
            yield 1

        def setup() -> None:
            captured.append(var.set("serializable"))

        sender.run(setup)
        receiver = Context.from_protobuf(sender.to_protobuf())
        assert receiver[var] == "serializable", (
            "Sanity: initial frame should carry the set value"
        )

        def cycle() -> None:
            var.reset(captured[0])
            var.set(_local_gen())

        sender.run(cycle)

        with pytest.warns(ContextDecodeWarning, match=var.name):
            second_frame = sender.to_protobuf()

        # Act
        receiver.update(Context.from_protobuf(second_frame))

        # Assert
        assert var in receiver, (
            "A serialization failure on the sender must not propagate "
            "a phantom reset to the receiver"
        )
        assert receiver[var] == "serializable", (
            "The receiver's prior binding must survive the sender's failed re-set"
        )

    def test_update_with_empty_context_is_noop(self):
        """Test update applied with an empty peer leaves state unchanged.

        Given:
            A Context with a var set and an empty peer Context
        When:
            current.update(empty) is called
        Then:
            The current context is unchanged and no exception is
            raised.
        """
        # Arrange
        from wool import protocol

        var = ContextVar("ctx002_seed", default="default")
        var.set("before")
        before = current_context()
        empty = Context.from_protobuf(protocol.Context())

        # Act
        before.update(empty)

        # Assert
        after = current_context()
        assert after[var] == before[var]
        assert set(after.keys()) == set(before.keys())

    def test_update_propagates_caller_side_reset(self):
        """Test Context.update unsets a var on the receiver when the
        sender's wire frame consumed the corresponding token without
        re-setting the var.

        Given:
            A receiver Context with var bound to a value (modeling a
            worker that received the caller's initial frame) and a
            second wire frame from the caller showing the var was
            reset between dispatches — the sender's vars map no
            longer carries the var, but its consumed_tokens list
            does
        When:
            receiver.update(Context.from_protobuf(reset_frame)) is
            called
        Then:
            The receiver no longer sees the var — the reset signal
            propagates through the merge so mid-stream resets reach
            the worker, not just the initial-dispatch state
        """
        # Arrange
        var = ContextVar(f"reset_propagation_{uuid.uuid4().hex}")
        sender = Context()

        def set_var() -> Token[str]:
            return var.set("caller-value")

        token = sender.run(set_var)
        receiver = Context.from_protobuf(sender.to_protobuf())
        assert receiver[var] == "caller-value", (
            "Sanity: initial frame should carry the set value"
        )

        def reset_var() -> None:
            var.reset(token)

        sender.run(reset_var)
        reset_frame = sender.to_protobuf()

        # Act
        receiver.update(Context.from_protobuf(reset_frame))

        # Assert
        assert var not in receiver, (
            "Reset should propagate via update so the worker observes "
            "the caller's post-reset state"
        )

    def test_update_preserves_re_set_after_reset(self):
        """Test Context.update keeps a re-set value when the sender's
        wire frame carries both the consumed token and a fresh value
        for the same var.

        Given:
            A sender Context that ran var.set('A'), var.reset(token),
            then var.set('B') in sequence — its data carries the new
            'B' value and its used-token set carries the consumed
            token. A receiver Context populated from the sender's
            initial frame.
        When:
            receiver.update(Context.from_protobuf(sender.to_protobuf()))
            is called with the post-re-set frame.
        Then:
            ``receiver[var] == 'B'`` — the consumed token's var key
            matches an entry in the sender's vars map, so the merge
            keeps the re-set value rather than treating reset
            propagation as a blanket pop.
        """
        # Arrange
        var = ContextVar(f"reset_then_reset_{uuid.uuid4().hex}")
        sender = Context()

        def set_a() -> Token[str]:
            return var.set("A")

        token = sender.run(set_a)
        receiver = Context.from_protobuf(sender.to_protobuf())
        assert receiver[var] == "A"

        def reset_then_set_b() -> None:
            var.reset(token)
            var.set("B")

        sender.run(reset_then_set_b)

        # Act
        receiver.update(Context.from_protobuf(sender.to_protobuf()))

        # Assert
        assert receiver[var] == "B", (
            "Re-set after reset should win over the consumed-token's "
            "pop signal — the var key is in both the consumed list "
            "and the vars map, so the vars value is authoritative"
        )

    def test_update_propagates_external_token_reset_through_transit_hop(self):
        """Test Context.update unsets a var on the receiver when the
        wire frame's consumed token arrives without a live token
        instance — the transit-hop case.

        Given:
            A receiver Context with var bound to ``"caller-value"``;
            a wire ``protocol.Context`` whose ``vars`` list carries a
            single ``protocol.ContextVar`` entry under the var's
            ``(namespace, name)`` identity, with no current value but
            with a consumed-token id whose UUID was never registered
            in this process's token registry — modeling a frame
            relayed through a hop that never reconstituted the live
            Token instance.
        When:
            receiver.update(Context.from_protobuf(pb)) is called.
        Then:
            ``var not in receiver`` — the external-token entry's
            ``(namespace, name)`` identity threads through
            ``_external_used_tokens`` and propagates the reset signal
            to the receiver even when the live Token instance is
            absent.
        """
        # Arrange
        from wool import protocol

        var = ContextVar(f"transit_reset_{uuid.uuid4().hex}")
        with scoped_context() as receiver:
            var.set("caller-value")
            assert receiver[var] == "caller-value"

            pb = protocol.Context()
            pb.vars.add(
                namespace=var.namespace,
                name=var.name,
                consumed_tokens=[uuid.uuid4().hex],
            )

            # Act
            receiver.update(Context.from_protobuf(pb))

            # Assert
            assert var not in receiver, (
                "External-token consumed entry with var_key should "
                "propagate the reset to the receiver even without a "
                "live Token instance"
            )

    def test_to_protobuf_emits_consumed_token_under_var_entry(self):
        """Test Context.to_protobuf emits each consumed token inside
        the wire entry for the var that minted it.

        Given:
            A Context that ran a set+reset cycle on a ContextVar with
            a strong reference held to the resulting Token so the
            ``weakref.WeakSet`` does not prune it before emission.
        When:
            ``ctx.to_protobuf()`` is called.
        Then:
            The wire frame contains one :class:`protocol.ContextVar`
            entry whose ``namespace`` and ``name`` match the owning
            var and whose ``consumed_tokens`` list carries the
            token's id hex — the wire shape colocates the consumed
            token with its var so transit hops can propagate the
            reset signal.
        """
        # Arrange
        var = ContextVar(f"emit_var_key_{uuid.uuid4().hex}")
        ctx = Context()
        captured: list[Token[str]] = []

        def consume() -> None:
            t = var.set("x")
            var.reset(t)
            captured.append(t)

        ctx.run(consume)

        # Act
        emitted = ctx.to_protobuf()

        # Assert
        entries = list(emitted.vars)
        assert len(entries) == 1
        assert (entries[0].namespace, entries[0].name) == (var.namespace, var.name)
        assert list(entries[0].consumed_tokens) == [captured[0].id.hex]

    def test_to_protobuf_emits_value_and_consumed_tokens_in_one_entry(self):
        """Test Context.to_protobuf collates a current value and a
        consumed-token id under the same wire entry when both belong
        to the same var.

        Given:
            A Context that ran ``var.set("A")`` capturing the token,
            then ``var.reset(token)``, then ``var.set("B")`` — the var
            now carries a current value alongside a consumed-token
            for the same identity, with a strong reference held to
            the captured token.
        When:
            ``ctx.to_protobuf()`` is called.
        Then:
            Exactly one ``protocol.ContextVar`` entry is emitted; its
            ``(namespace, name)`` matches the owning var, its
            ``value`` field is set (``HasField('value') is True``),
            and its ``consumed_tokens`` list contains the captured
            token's id hex — confirming the merged wire shape
            colocates set state and consumed-token state for one
            var into a single entry.
        """
        # Arrange
        var = ContextVar(f"value_and_token_{uuid.uuid4().hex}")
        ctx = Context()
        captured: list[Token[str]] = []

        def churn() -> None:
            t = var.set("A")
            var.reset(t)
            captured.append(t)
            var.set("B")

        ctx.run(churn)

        # Act
        emitted = ctx.to_protobuf()

        # Assert
        entries = list(emitted.vars)
        assert len(entries) == 1
        entry = entries[0]
        assert (entry.namespace, entry.name) == (var.namespace, var.name)
        assert entry.HasField("value")
        assert cloudpickle.loads(entry.value) == "B"
        assert list(entry.consumed_tokens) == [captured[0].id.hex]

    def test_to_protobuf_folds_repeated_consumed_tokens_for_one_var(self):
        """Test Context.to_protobuf groups multiple consumed tokens
        for the same var into a single wire entry's
        ``consumed_tokens`` list.

        Given:
            A Context that ran two set+reset cycles against the same
            var — producing two distinct consumed Tokens under the
            same ``(namespace, name)`` — with strong references held
            to both Tokens so the ``weakref.WeakSet`` does not prune
            them before emission.
        When:
            ``ctx.to_protobuf()`` is called.
        Then:
            Exactly one ``protocol.ContextVar`` entry is emitted for
            the var, and its ``consumed_tokens`` list contains both
            token id hexes — the encode loop deduplicates by
            ``(namespace, name)`` rather than emitting one entry per
            consumed token.
        """
        # Arrange
        var = ContextVar(f"two_tokens_one_var_{uuid.uuid4().hex}")
        ctx = Context()
        captured: list[Token[str]] = []

        def churn() -> None:
            t1 = var.set("first")
            var.reset(t1)
            captured.append(t1)
            t2 = var.set("second")
            var.reset(t2)
            captured.append(t2)

        ctx.run(churn)

        # Act
        emitted = ctx.to_protobuf()

        # Assert
        entries = list(emitted.vars)
        assert len(entries) == 1
        entry = entries[0]
        assert (entry.namespace, entry.name) == (var.namespace, var.name)
        assert set(entry.consumed_tokens) == {captured[0].id.hex, captured[1].id.hex}

    def test_from_protobuf_decodes_value_and_consumed_tokens_in_one_entry(self):
        """Test Context.from_protobuf decodes a wire entry that
        carries both an optional value and a consumed-token id under
        the same var identity.

        Given:
            A wire ``protocol.Context`` whose single
            ``protocol.ContextVar`` entry carries a registered var's
            ``(namespace, name)``, a serialized value, and a
            consumed-token id hex.
        When:
            ``Context.from_protobuf(pb)`` is called and the result is
            re-emitted via ``to_protobuf``.
        Then:
            The reconstructed Context binds the var to the
            deserialized value, and the re-emitted wire frame carries
            the consumed-token id under the same merged entry —
            decode handles colocated value and consumed_tokens
            symmetrically with encode.
        """
        # Arrange
        from wool import protocol

        var = ContextVar(f"merged_decode_{uuid.uuid4().hex}", default="initial")
        token_id = uuid.uuid4()
        pb = protocol.Context(
            id=uuid.uuid4().hex,
            vars=[
                protocol.ContextVar(
                    namespace=var.namespace,
                    name=var.name,
                    value=cloudpickle.dumps("decoded"),
                    consumed_tokens=[token_id.hex],
                ),
            ],
        )

        # Act
        reconstructed = Context.from_protobuf(pb)
        re_emitted = reconstructed.to_protobuf()

        # Assert
        assert reconstructed[var] == "decoded"
        re_entries = list(re_emitted.vars)
        assert len(re_entries) == 1
        re_entry = re_entries[0]
        assert (re_entry.namespace, re_entry.name) == (var.namespace, var.name)
        assert re_entry.HasField("value")
        assert cloudpickle.loads(re_entry.value) == "decoded"
        assert list(re_entry.consumed_tokens) == [token_id.hex]

    def test_from_protobuf_with_unknown_keys_alongside_known_ones(self):
        """Test Context.from_protobuf stubs unknown keys and applies their
        values, while still deserializing known keys as normal.

        Given:
            A wire-form protocol.Context carrying a mix of keys — one
            registered on this process, one not
        When:
            Context.from_protobuf is invoked with the payload
        Then:
            The registered key is deserialized as before, and the
            unregistered key results in a stub entry so the receiver
            can observe the propagated value when the var is later
            declared (rolling-deploy / lazy-import scenario).
        """
        # Arrange
        from wool import protocol

        known_var = ContextVar("ctx003_known", default="initial")
        unknown_ns = f"ctx003_unknown_{uuid.uuid4().hex}"
        pb = protocol.Context(
            vars=[
                protocol.ContextVar(
                    namespace=unknown_ns,
                    name="missing",
                    value=dumps("propagated"),
                ),
                protocol.ContextVar(
                    namespace=known_var.namespace,
                    name=known_var.name,
                    value=dumps("applied"),
                ),
            ]
        )

        # Act
        reconstructed = Context.from_protobuf(pb)
        current_context().update(reconstructed)

        # Assert
        assert reconstructed[known_var] == "applied"
        late_declared: ContextVar[str] = ContextVar("missing", namespace=unknown_ns)
        assert late_declared.get() == "propagated"

    def test_from_protobuf_with_unregistered_key_then_later_var_declaration(
        self,
    ):
        """Test wire ingress of an unregistered var matches the pickle-path
        stub-promotion semantics.

        Given:
            A wire protocol.Context carrying a var key that is not yet
            registered on this process, and the corresponding
            wool.ContextVar declaration arrives later (lazy-import on
            the receiver)
        When:
            Context.from_protobuf reconstructs the payload,
            current_context().update merges it, and the user then
            declares the ContextVar under the same key
        Then:
            ContextVar.get should return the wire-propagated value —
            the wire-ingress path creates and pins a stub the same way
            the pickled-ContextVar-instance path does, so lazy-import
            receivers converge after one dispatch rather than needing
            a second one that carries a ContextVar instance in-args.
        """
        # Arrange
        from wool import protocol

        unique_ns = f"wire_stub_{uuid.uuid4().hex}"
        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars.add(
            namespace=unique_ns,
            name="tenant_id",
            value=dumps("acme-corp"),
        )

        incoming = Context.from_protobuf(pb)
        current_context().update(incoming)

        # Act
        var: ContextVar[str] = ContextVar("tenant_id", namespace=unique_ns)

        # Assert
        assert var.get() == "acme-corp"

    def test_from_protobuf_with_corrupt_value(self):
        """Test Context.from_protobuf emits a ContextDecodeWarning
        naming a corrupt key and skips that entry instead of
        aborting the whole decode.

        Given:
            A registered wool.ContextVar and a wire-form
            protocol.Context containing that key mapped to bytes that
            are not a valid pickle stream
        When:
            Context.from_protobuf is invoked with the payload
        Then:
            It emits a ContextDecodeWarning naming the offending
            key and returns a Context with the corrupt entry
            skipped — surviving entries decode normally
        """
        # Arrange
        from wool import ContextDecodeWarning
        from wool import protocol

        var = ContextVar("ctx003_corrupt", default="initial")
        pb = protocol.Context(
            vars=[
                protocol.ContextVar(
                    namespace=var.namespace,
                    name=var.name,
                    value=b"\x00not a valid pickle stream\x00",
                )
            ]
        )

        # Act
        with pytest.warns(ContextDecodeWarning, match=var.name):
            ctx = Context.from_protobuf(pb)

        # Assert
        assert var not in ctx, "Corrupt entry should be skipped, not partially decoded"

    def test_from_protobuf_with_malformed_id_emits_warning(self):
        """Test Context.from_protobuf emits a ContextDecodeWarning when
        the wire context's chain id cannot be parsed, falling back to
        a freshly-minted chain id.

        Given:
            A wire-form protocol.Context whose ``id`` field is a
            non-empty string that does not parse as a valid UUID
        When:
            Context.from_protobuf is invoked under default warning
            filters
        Then:
            A ContextDecodeWarning is emitted naming the malformed
            id, and the returned Context carries a freshly-minted
            chain id rather than propagating the bad value
        """
        # Arrange
        from wool import ContextDecodeWarning
        from wool import protocol

        pb = protocol.Context(id="not-a-uuid")

        # Act
        with pytest.warns(ContextDecodeWarning, match="not-a-uuid"):
            ctx = Context.from_protobuf(pb)

        # Assert
        assert isinstance(ctx.id, uuid.UUID), (
            "Fallback chain id should be a freshly-minted UUID"
        )
        assert ctx.id.hex != "not-a-uuid", "Malformed id should not be propagated"

    def test_from_protobuf_with_malformed_id_strict_mode(self):
        """Test Context.from_protobuf aggregates a strict-mode
        catastrophic id-parse failure into a BaseExceptionGroup.

        Given:
            A wire-form protocol.Context with a malformed ``id``
            field, with a strict warnings filter promoting
            ContextDecodeWarning to an exception
        When:
            Context.from_protobuf is invoked
        Then:
            A BaseExceptionGroup is raised with a single
            ContextDecodeWarning peer naming the malformed id —
            catastrophic decode failures ride the same group
            channel as per-var failures
        """
        import warnings as _warnings

        from wool import ContextDecodeWarning
        from wool import protocol

        # Arrange
        pb = protocol.Context(id="not-a-uuid")

        # Act & assert
        with _warnings.catch_warnings():
            _warnings.simplefilter("error", category=ContextDecodeWarning)
            with pytest.raises(BaseExceptionGroup) as exc_info:
                Context.from_protobuf(pb)

        peers = exc_info.value.exceptions
        assert len(peers) == 1, "Group should hold one peer for the id failure"
        assert isinstance(peers[0], ContextDecodeWarning)
        assert "not-a-uuid" in str(peers[0])

    def test_from_protobuf_aggregates_id_and_var_failures_strict_mode(self):
        """Test Context.from_protobuf folds catastrophic id-parse
        failures and per-var decode failures into a single
        BaseExceptionGroup under strict mode.

        Given:
            A wire-form protocol.Context with both a malformed
            ``id`` field and a registered var bound to bytes that
            are not a valid pickle stream, with strict mode active
        When:
            Context.from_protobuf is invoked
        Then:
            A BaseExceptionGroup is raised whose peers include one
            ContextDecodeWarning naming the malformed id and one
            naming the corrupt var key — confirming the unified
            decode-failure channel covers both axes simultaneously
        """
        import warnings as _warnings

        from wool import ContextDecodeWarning
        from wool import protocol

        # Arrange
        var = ContextVar("ctx003_strict_combined", default="initial")
        pb = protocol.Context(
            id="not-a-uuid",
            vars=[
                protocol.ContextVar(
                    namespace=var.namespace,
                    name=var.name,
                    value=b"\x00not a valid pickle stream\x00",
                )
            ],
        )

        # Act & assert
        with _warnings.catch_warnings():
            _warnings.simplefilter("error", category=ContextDecodeWarning)
            with pytest.raises(BaseExceptionGroup) as exc_info:
                Context.from_protobuf(pb)

        peers = exc_info.value.exceptions
        assert all(isinstance(p, ContextDecodeWarning) for p in peers), (
            "Every peer should be a ContextDecodeWarning"
        )
        peer_messages = [str(p) for p in peers]
        assert any("not-a-uuid" in msg for msg in peer_messages), (
            "Malformed id should be named in a peer"
        )
        assert any(var.name in msg for msg in peer_messages), (
            "Corrupt var should be named in a peer"
        )
        assert len(peers) == 2, "Group should hold one peer per failure axis (id + var)"

    def test_from_protobuf_with_multiple_corrupt_values_strict_mode(self):
        """Test Context.from_protobuf aggregates strict-mode decode
        failures into a single BaseExceptionGroup naming every
        offending var.

        Given:
            A wire-form protocol.Context carrying two registered keys
            mapped to bytes that are not a valid pickle stream, with a
            strict warnings filter promoting ContextDecodeWarning to
            an exception
        When:
            Context.from_protobuf is invoked with the payload
        Then:
            A BaseExceptionGroup is raised whose peers are
            ContextDecodeWarning instances — one per offending var,
            each naming the corresponding key — so callers learn
            about every bad var on a single decode attempt.
        """
        import warnings as _warnings

        from wool import ContextDecodeWarning
        from wool import protocol

        # Arrange
        first = ContextVar("ctx003_strict_a", default="a-default")
        second = ContextVar("ctx003_strict_b", default="b-default")
        pb = protocol.Context(
            vars=[
                protocol.ContextVar(
                    namespace=first.namespace,
                    name=first.name,
                    value=b"\x00not a valid pickle stream\x00",
                ),
                protocol.ContextVar(
                    namespace=second.namespace,
                    name=second.name,
                    value=b"\x01also not valid\x01",
                ),
            ]
        )

        # Act & assert
        with _warnings.catch_warnings():
            _warnings.simplefilter("error", category=ContextDecodeWarning)
            with pytest.raises(BaseExceptionGroup) as exc_info:
                Context.from_protobuf(pb)

        peers = exc_info.value.exceptions
        assert all(isinstance(p, ContextDecodeWarning) for p in peers), (
            "Every peer should be a ContextDecodeWarning"
        )
        peer_messages = [str(p) for p in peers]
        assert any(first.name in msg for msg in peer_messages), (
            "First offending var should be named in a peer"
        )
        assert any(second.name in msg for msg in peer_messages), (
            "Second offending var should be named in a peer"
        )
        assert len(peers) == 2, "Group should hold one peer per offending var, no more"

    def test_from_protobuf_with_malformed_consumed_token_ids(
        self,
    ):
        """Test Context.from_protobuf tolerates a single malformed
        consumed-token id without aborting the whole frame decode.

        Given:
            A wire-form protocol.Context carrying a valid var
            binding, a valid consumed-token hex id, and a malformed
            consumed-token hex id (not a UUID)
        When:
            Context.from_protobuf is invoked with the payload
        Then:
            The valid var binding is applied, the valid consumed-token
            id lands in the reconstructed Context's incoming buffer,
            the malformed id is skipped with a ContextDecodeWarning
            naming it, and no ValueError propagates — matching the
            per-var log-and-skip policy already in place for var
            values.
        """
        # Arrange
        from wool import ContextDecodeWarning
        from wool import protocol

        var = ContextVar("partial_decode_with_invalid_token_id", default="d")
        valid_id = uuid.uuid4()
        pb = protocol.Context(
            id=uuid.uuid4().hex,
            vars=[
                protocol.ContextVar(
                    namespace=var.namespace,
                    name=var.name,
                    value=dumps("applied"),
                    consumed_tokens=[valid_id.hex, "not-a-uuid"],
                ),
            ],
        )

        # Act
        with pytest.warns(ContextDecodeWarning, match="not-a-uuid"):
            reconstructed = Context.from_protobuf(pb)

        # Assert
        assert reconstructed[var] == "applied"
        current_context().update(reconstructed)
        emitted = current_context().to_protobuf()
        emitted_token_ids = {
            tid for entry in emitted.vars for tid in entry.consumed_tokens
        }
        assert valid_id.hex in emitted_token_ids

    def test_from_protobuf_strict_mode_aggregates_malformed_token_id(self):
        """Test Context.from_protobuf folds a malformed consumed-token
        id into the strict-mode BaseExceptionGroup with the owning
        var's identity in the warning message.

        Given:
            A wire ``protocol.Context`` whose single
            ``protocol.ContextVar`` entry carries a registered var's
            ``(namespace, name)``, a valid pickled value, and a
            malformed consumed-token hex ("not-a-uuid"); a strict
            warnings filter promotes ContextDecodeWarning to an
            exception.
        When:
            Context.from_protobuf is invoked with the payload.
        Then:
            A BaseExceptionGroup is raised whose peers include
            exactly one ContextDecodeWarning whose message names
            both the malformed hex and the var's namespace and name —
            confirming malformed-token-id failures route through the
            same strict-mode aggregation channel as value-decode and
            id-parse failures, and that the error message uses the
            var-key context the merged wire shape exposes.
        """
        import warnings as _warnings

        from wool import ContextDecodeWarning
        from wool import protocol

        # Arrange
        var = ContextVar(f"strict_token_id_{uuid.uuid4().hex}", default="d")
        pb = protocol.Context(
            id=uuid.uuid4().hex,
            vars=[
                protocol.ContextVar(
                    namespace=var.namespace,
                    name=var.name,
                    value=cloudpickle.dumps("applied"),
                    consumed_tokens=["not-a-uuid"],
                ),
            ],
        )

        # Act & assert
        with _warnings.catch_warnings():
            _warnings.simplefilter("error", category=ContextDecodeWarning)
            with pytest.raises(BaseExceptionGroup) as exc_info:
                Context.from_protobuf(pb)

        peers = exc_info.value.exceptions
        assert all(isinstance(p, ContextDecodeWarning) for p in peers)
        token_id_peers = [p for p in peers if "not-a-uuid" in str(p)]
        assert len(token_id_peers) == 1
        message = str(token_id_peers[0])
        assert var.namespace in message
        assert var.name in message

    def test_from_protobuf_with_stub_pinning(self):
        """Test Context.from_protobuf attaches resolved stubs to the
        Context it constructs and returns, not to the caller's
        currently-active Context.

        Given:
            A wire-form protocol.Context carrying an unregistered
            namespaced key, decoded from inside an outer
            scoped_context block. The returned Context is then
            dropped while the outer Context is still in scope, so
            only the pin anchor's keep-alive can preserve the stub
            once the outer block subsequently exits
        When:
            The returned Context is dropped, the outer scope exits,
            and gc.collect runs
        Then:
            The stub should NOT be discoverable in the process-wide
            var registry — the pin attribution lives on the
            returned Context (now gone), so the stub is reclaimed.
            If the pin had attached to the outer Context the stub
            would have outlived the returned Context (an attribution
            inversion).
        """
        # Arrange
        from wool import protocol
        from wool.runtime.context import var_registry

        key = ("pin_attribution", uuid.uuid4().hex)
        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars.add(
            namespace=key[0],
            name=key[1],
            value=cloudpickle.dumps("propagated"),
        )

        # Act
        with scoped_context():
            incoming = Context.from_protobuf(pb)
            del incoming
            gc.collect()
            # Window: returned Context dropped, outer scope still
            # active. With the pin on incoming this releases the
            # stub immediately; with the pin on outer the stub
            # would survive until outer also dies.
            in_registry_after_incoming_dies = key in var_registry

        # Assert
        assert in_registry_after_incoming_dies is False

    @pytest.mark.asyncio
    async def test_from_protobuf_in_caller_task(self):
        """Test Context.from_protobuf does not lazy-stamp a Context on
        the calling task's scope as a side effect of decoding a wire
        frame.

        Given:
            An asyncio task with no wool.Context bound to it —
            a fresh ``loop.create_task`` child created without
            wool's task factory installed, so no Context is
            auto-bound to the task identity.
        When:
            Context.from_protobuf is called from inside that task.
        Then:
            The wool registry slot for the task remains ``None`` —
            decoding a wire frame must not materialize a Context on
            the decoding task's scope.
        """
        # Arrange
        from wool import protocol
        from wool.runtime.context.registry import context_registry

        loop = asyncio.get_running_loop()
        # Bypass the wool task factory so no Context is auto-bound
        # to the child task's identity.
        loop.set_task_factory(None)

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars.add(
            namespace="no_side_effect",
            name=uuid.uuid4().hex,
            value=cloudpickle.dumps("v"),
        )
        observed: list[Context | None] = []

        async def body():
            Context.from_protobuf(pb)
            observed.append(context_registry.get())

        # Act
        await loop.create_task(body())

        # Assert
        assert observed == [None]

    def test_update_merges_consumed_tokens(self):
        """Test Context.update folds the source's consumed-token ids
        into the destination's outbound wire emission.

        Given:
            A secondary :class:`Context` reconstructed from a wire
            payload whose ``consumed_tokens`` lists a token id, and
            a primary :class:`Context` that has not yet observed
            that id
        When:
            ``primary.update(secondary)`` is called
        Then:
            ``primary.to_protobuf().consumed_tokens`` contains the
            merged id — observable via the wire shape rather than
            via a registry-aliased Token whose ``used`` flag is
            trivially True regardless of whether ``update`` did
            anything
        """
        # Arrange
        from wool import protocol

        token_id = uuid.uuid4()
        wire = protocol.Context(id=uuid.uuid4().hex)
        wire.vars.add(namespace="", name="", consumed_tokens=[token_id.hex])

        secondary = Context.from_protobuf(wire)
        primary = Context()

        # Act
        primary.update(secondary)

        # Assert
        emitted = primary.to_protobuf()
        emitted_token_ids = {
            tid for entry in emitted.vars for tid in entry.consumed_tokens
        }
        assert token_id.hex in emitted_token_ids

    def test_to_protobuf_with_locally_reset_token(self):
        """Test Context.to_protobuf emits consumed-token ids scoped to
        this Context's logical chain, excluding tokens reset under a
        different chain.

        Given:
            Two Contexts A and B with distinct ids, a Token minted
            and reset under A, and a Token minted and reset under B
        When:
            A.to_protobuf() is called
        Then:
            The resulting ``consumed_tokens`` list contains A's
            token id but not B's — the per-lineage scoping of
            wire emission holds regardless of whether it is derived
            from a global scan or from per-Context bookkeeping
        """
        # Arrange
        var_a = ContextVar("pin_scope_a", default="d")
        var_b = ContextVar("pin_scope_b", default="d")

        a_tokens: list[Token] = []
        b_tokens: list[Token] = []

        def consume_in_a() -> None:
            a_tokens.append(var_a.set("ax"))
            var_a.reset(a_tokens[-1])

        def consume_in_b() -> None:
            b_tokens.append(var_b.set("bx"))
            var_b.reset(b_tokens[-1])

        ctx_a = Context()
        ctx_b = Context()
        ctx_a.run(consume_in_a)
        ctx_b.run(consume_in_b)

        # Act
        a_pb = ctx_a.to_protobuf()

        # Assert
        a_hex = {tid for entry in a_pb.vars for tid in entry.consumed_tokens}
        assert a_tokens[0].id.hex in a_hex
        assert b_tokens[0].id.hex not in a_hex

    def test_update_with_flipped_token_ids_then_to_protobuf(
        self,
    ):
        """Test Context.update makes a merged used-token id visible in a
        subsequent Context.to_protobuf on the same Context.

        Given:
            A live Token minted under the current Context, and a
            wire protocol.Context whose ``consumed_tokens`` lists
            that Token's id (modeling a back-prop frame from a peer)
        When:
            The wire Context is merged via current_context().update
            and current_context().to_protobuf() is called
        Then:
            The resulting ``consumed_tokens`` list contains the
            merged token id — forwarding the used-state onward
            through this Context's wire emissions
        """
        # Arrange
        from wool import protocol

        var = ContextVar("pin_forward", default="d")
        token = var.set("x")

        pb = protocol.Context(id=uuid.uuid4().hex)
        pb.vars.add(
            namespace=var.namespace,
            name=var.name,
            consumed_tokens=[token.id.hex],
        )

        incoming = Context.from_protobuf(pb)
        current_context().update(incoming)

        # Act
        emitted = current_context().to_protobuf()

        # Assert
        emitted_token_ids = {
            tid for entry in emitted.vars for tid in entry.consumed_tokens
        }
        assert token.id.hex in emitted_token_ids

    def test_to_protobuf_roundtrips_consumed_tokens(self):
        """Test Context.to_protobuf followed by Context.from_protobuf
        preserves consumed-token state end-to-end.

        Given:
            A Context that has consumed a Token via ContextVar.reset
            and a strong reference to the consumed Token kept alive
            outside the Context so the ``weakref.WeakSet`` tracking
            entry survives until ``to_protobuf`` runs
        When:
            The Context is serialized via to_protobuf and a new
            Context is reconstructed via from_protobuf
        Then:
            The reconstructed Context emits the same consumed-token
            id when serialized again — the round-trip is observable
            via the wire shape rather than via a registry-aliased
            Token whose ``used`` flag is trivially True regardless
            of whether ``from_protobuf`` populated the destination
        """
        # Arrange
        var = ContextVar("proto_consumed", default="d")
        origin = Context()
        captured: list[Token[Any]] = []

        def consume():
            t = var.set("x")
            var.reset(t)
            captured.append(t)

        origin.run(consume)

        # Act
        roundtripped = Context.from_protobuf(origin.to_protobuf())

        # Assert
        emitted = roundtripped.to_protobuf()
        emitted_token_ids = {
            tid for entry in emitted.vars for tid in entry.consumed_tokens
        }
        assert captured[0].id.hex in emitted_token_ids

    def test_to_protobuf_forwards_wire_consumed_tokens_without_local_mutation(
        self,
    ):
        """Test from_protobuf → to_protobuf round-trip preserves
        wire-supplied consumed-token ids when no local mutation
        occurred between the two operations.

        Given:
            A Context built via Context.from_protobuf from a wire
            payload whose consumed_tokens field carries a token id,
            and no local ContextVar.reset or update call against the
            resulting Context after construction
        When:
            to_protobuf is called on the resulting Context
        Then:
            The emitted wire payload's consumed_tokens contains the
            original id — a participant in a dispatch chain must
            forward upstream-told consumed-token state, not silently
            drop it on the second hop
        """
        # Arrange
        from wool import protocol

        token_id = uuid.uuid4()
        wire_in = protocol.Context(id=uuid.uuid4().hex)
        wire_in.vars.add(namespace="", name="", consumed_tokens=[token_id.hex])

        # Act
        ctx = Context.from_protobuf(wire_in)
        wire_out = ctx.to_protobuf()

        # Assert
        emitted_token_ids = {
            tid for entry in wire_out.vars for tid in entry.consumed_tokens
        }
        assert token_id.hex in emitted_token_ids, (
            "Context built from a wire payload must forward "
            "upstream consumed-token ids on subsequent to_protobuf "
            "emission; otherwise nested dispatches lose chain state"
        )

    def test_to_protobuf_with_custom_dumps(self):
        """Test Context.to_protobuf returns a Context with custom-serialized vars.

        Given:
            A ContextVar with a value set and a custom dumps function
        When:
            current_context().to_protobuf(serializer=custom) is called
        Then:
            It should return a protocol.Context whose vars map was
            produced by the custom serializer and whose id is a 32-char
            UUID hex string.
        """
        # Arrange
        var = ContextVar("tpb_custom_dumps", namespace="tpb")
        var.set(42)

        calls: list[object] = []

        def custom_dumps(value: object) -> bytes:
            calls.append(value)
            return b"tpb:" + str(value).encode()

        # Act
        pb = current_context().to_protobuf(
            serializer=cast(Serializer, SimpleNamespace(dumps=custom_dumps))
        )

        # Assert
        emitted = {(e.namespace, e.name): e.value for e in pb.vars}
        assert (var.namespace, var.name) in emitted
        assert emitted[(var.namespace, var.name)] == b"tpb:42"
        assert isinstance(pb.id, str)
        assert len(pb.id) == 32
        assert calls == [42]

    def test_from_protobuf_with_custom_loads(self):
        """Test Context.from_protobuf deserializes values via a custom loads callable.

        Given:
            A ContextVar registered in the process and a wire-form
            protocol.Context with a custom-encoded value
        When:
            Context.from_protobuf is called with a custom loads function
        Then:
            It should deserialize each value through the custom callable.
        """
        # Arrange
        from wool import protocol

        var = ContextVar("fpb_custom_loads", namespace="fpb")
        pb = protocol.Context(
            vars=[
                protocol.ContextVar(
                    namespace=var.namespace,
                    name=var.name,
                    value=b"custom-payload",
                )
            ]
        )

        calls: list[bytes] = []

        def custom_loads(data: bytes) -> object:
            calls.append(data)
            return "decoded-" + data.decode()

        # Act
        reconstructed = Context.from_protobuf(
            pb, serializer=cast(Serializer, SimpleNamespace(loads=custom_loads))
        )

        # Assert
        assert reconstructed[var] == "decoded-custom-payload"
        assert calls == [b"custom-payload"]


def test_copy_context_with_set_vars():
    """Test copy_context() snapshots vars and assigns a fresh chain id.

    Given:
        A ContextVar with an explicit value set in the live Context
    When:
        copy_context() is called
    Then:
        The snapshot contains the var's value but its id differs
        from the live Context's id — the copy is an independent
        logical chain
    """
    # Arrange
    var = ContextVar("copy_ctx", default=0)
    var.set(1)
    live_id = current_context().id

    # Act
    snapshot = copy_context()

    # Assert
    assert snapshot[var] == 1
    assert snapshot.id != live_id


def test_copy_context_chain_id_uniqueness():
    """Test successive copy_context() calls each get a distinct id.

    Given:
        No mutation to the live Context between calls
    When:
        copy_context() is called twice
    Then:
        The two snapshots have distinct ids
    """
    # Act
    a = copy_context()
    b = copy_context()

    # Assert
    assert a.id != b.id


@pytest.mark.asyncio
async def test_create_task_with_explicit_wool_context_skips_fork():
    """Test passing a :class:`wool.Context` to :func:`wool.create_task`
    pre-binds the given Context and bypasses the copy-on-fork path
    the wool task factory would otherwise take.

    Given:
        An event loop with wool's task factory installed and a
        parent scope holding a ContextVar binding that the factory's
        fork path would normally propagate to child tasks.
    When:
        :func:`wool.create_task` is called with a fresh (empty)
        target wool.Context distinct from the parent's.
    Then:
        The child sees the target Context directly — same identity,
        and crucially without the parent's var binding — proving
        the fork path was skipped rather than taken. This is the
        canonical wool task-binding idiom: stdlib's ``context=``
        kwarg is intercepted by wool's task factory and routed
        through wool's per-task registry, with
        :func:`wool.create_task` providing the typing shim around
        the duck-typed wool.Context payload.
    """
    from wool.runtime.context import create_task
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    install_task_factory(loop)

    sentinel_var = ContextVar("bound_task_fork_sentinel", default="default")
    sentinel_var.set("parent-value")

    target = Context()
    observed_ctx: list[Context] = []
    observed_value: list[str] = []

    async def body():
        observed_ctx.append(current_context())
        observed_value.append(sentinel_var.get())

    # Act
    task = create_task(body(), context=target)
    await task

    # Assert
    assert observed_ctx == [target]
    # A forked child would have inherited "parent-value" via
    # parent.copy(); the bound child sees the fresh target's empty
    # state and falls through to the var's default.
    assert observed_value == ["default"]


@pytest.mark.asyncio
async def test_create_task_inside_parent_context_scope():
    """Test the child task spawned via asyncio.create_task inherits a fork
    of the parent's current_context (not an empty Context).

    Given:
        An event loop with wool's task factory installed, a parent
        task that holds a non-empty Context carrying a ContextVar
        binding
    When:
        The parent calls asyncio.create_task to spawn a child
    Then:
        The child's current_context should be a distinct Context
        (fresh chain id) but carrying the same var binding the
        parent set — locking in the fork-on-spawn contract that
        depends on ``asyncio.current_task()`` resolving to the
        parent inside the task-factory callback
    """
    from wool.runtime.context import install_task_factory

    loop = asyncio.get_running_loop()
    install_task_factory(loop)

    var = ContextVar("fork_invariant_probe", default="d")
    var.set("parent-value")
    parent_ctx = current_context()

    captured: list[tuple[uuid.UUID, str]] = []

    async def child() -> None:
        child_ctx = current_context()
        captured.append((child_ctx.id, var.get()))

    await asyncio.create_task(child())

    assert len(captured) == 1
    child_id, child_value = captured[0]
    assert child_id != parent_ctx.id  # fork mints a fresh chain id
    assert child_value == "parent-value"  # but carries parent's var state


@pytest.mark.asyncio
async def test_install_task_factory_idempotent():
    """Test install_task_factory is a no-op when already installed.

    Given:
        install_task_factory has been called on the running loop
    When:
        install_task_factory is called again
    Then:
        It should return without error (idempotent)
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    install_task_factory()

    # Act & assert — no error
    install_task_factory()


@pytest.mark.asyncio
async def test_install_task_factory_with_existing_factory():
    """Test install_task_factory wraps an existing factory.

    Given:
        A custom task factory already set on the loop
    When:
        install_task_factory is called
    Then:
        It should wrap the existing factory, creating tasks via the
        original while also seeding wool Context on the child
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    calls = []

    def custom_factory(loop, coro, **kwargs):
        calls.append("custom")
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(custom_factory)

    # Act
    install_task_factory()

    var = ContextVar("compose_test", namespace="test_compose")
    var.set("parent_value")

    async def child():
        return var.get("missing")

    result = await asyncio.create_task(child())

    # Assert
    assert len(calls) > 0  # custom factory was called
    assert result == "parent_value"  # wool context inherited

    # Cleanup
    loop.set_task_factory(None)


@pytest.mark.asyncio
async def test_install_task_factory_idempotent_over_composed():
    """Test install_task_factory is a no-op when a wool-composed factory is installed.

    Given:
        A user factory was set on the loop and install_task_factory
        composed around it
    When:
        install_task_factory is called again
    Then:
        The second call recognizes the _wool_wrapped marker and
        returns without replacing the composed factory
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()

    def custom_factory(loop, coro, **kwargs):
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(custom_factory)
    install_task_factory()
    composed = loop.get_task_factory()

    # Act
    install_task_factory()

    # Assert
    assert loop.get_task_factory() is composed

    # Cleanup
    loop.set_task_factory(None)


@pytest.mark.asyncio
async def test_install_task_factory_on_fresh_loop(caplog):
    """Test install_task_factory logs a fresh-install message on an empty loop.

    Given:
        A running event loop with no task factory set
    When:
        install_task_factory runs once
    Then:
        A debug record naming the "installed" path is emitted
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    loop.set_task_factory(None)

    # Act
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()

    # Assert
    assert any("wool task factory installed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_install_task_factory_when_recalled(caplog):
    """Test a second install on the same loop logs the already-installed path.

    Given:
        A running event loop with wool's factory already installed
    When:
        install_task_factory runs a second time
    Then:
        A debug record naming the "already installed" path is emitted
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    loop.set_task_factory(None)
    install_task_factory()

    # Act
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()

    # Assert
    assert any("already installed" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_install_task_factory_with_user_factory_present(caplog):
    """Test install over a non-wool user factory logs the compose path.

    Given:
        A running event loop with a non-wool user task factory in place
    When:
        install_task_factory runs
    Then:
        A debug record naming the "composed with existing factory"
        path is emitted
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    loop.set_task_factory(None)

    def custom_factory(loop, coro, **kwargs):
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(custom_factory)

    # Act
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()

    # Assert
    assert any("composed with existing factory" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_install_task_factory_when_recalled_over_composed(
    caplog,
):
    """Test a second install over a composed factory logs the already-composed path.

    Given:
        A running event loop with wool's factory already composed
        over a user factory
    When:
        install_task_factory runs a second time
    Then:
        A debug record naming the "composed task factory already
        installed" path is emitted
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    loop.set_task_factory(None)

    def custom_factory(loop, coro, **kwargs):
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(custom_factory)
    install_task_factory()

    # Act
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()

    # Assert
    assert any(
        "composed task factory already installed" in r.message for r in caplog.records
    )


def test_update_with_external_uuid_resolved_to_reloaded_token():
    """Test Context.update flips the used flag on a live Token
    re-registered via cloudpickle reload after the wire snapshot
    was decoded.

    Given:
        A wire :class:`protocol.Context` whose ``consumed_tokens``
        carries a UUID for which no live :class:`Token` exists at
        decode time — the originating Token was cloudpickled and
        then released. After ``Context.from_protobuf`` records the
        UUID, the pickle is reloaded so a fresh live Token is in
        the registry under the same UUID.
    When:
        ``current_context().update(secondary)`` runs against that
        wire-derived Context.
    Then:
        The reloaded Token's ``used`` flag flips to True — the
        merge resolves the incoming UUID through the live token
        registry rather than parking it as a bare id.
    """
    # Arrange
    from wool import protocol

    var = ContextVar(f"update_external_uuid_{uuid.uuid4().hex}")
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

    restored = loads(pickled)
    assert restored.used is False

    # Act
    current_context().update(secondary)

    # Assert
    assert restored.used is True


@pytest.mark.asyncio
async def test_create_task_with_context_already_bound_to_another_running_task():
    """Test :func:`wool.create_task` rejects a second concurrent task
    pinned to a :class:`wool.Context` already running another task.

    Given:
        A running event loop with wool's task factory installed and a
        :class:`wool.Context` bound to a first task whose coroutine is
        suspended (the bound-task slot points at a still-running task).
    When:
        :func:`wool.create_task` schedules a second coroutine targeting
        the same Context while the first task is still alive, and the
        second task is awaited.
    Then:
        It should raise :class:`RuntimeError` naming the "first-task-
        wins for the routine's lifetime" invariant.
    """
    # Arrange
    from wool.runtime.context import create_task as wool_create_task
    from wool.runtime.context import install_task_factory

    install_task_factory()
    ctx = Context()
    started = asyncio.Event()
    release = asyncio.Event()

    async def first():
        started.set()
        await release.wait()

    async def second():
        return "should-not-run"

    first_task = wool_create_task(first(), context=ctx)
    await started.wait()
    second_task = wool_create_task(second(), context=ctx)

    # Act & assert
    try:
        with pytest.raises(RuntimeError, match="bound to another live task"):
            await second_task
    finally:
        release.set()
        await first_task


@pytest.mark.asyncio
async def test_create_task_with_copy_context_inherits_and_forks():
    """Test passing a :func:`contextvars.copy_context` to
    ``create_task`` does not break wool's fork-on-task semantics —
    the child still inherits the parent's wool.Context state under
    a fresh chain id via the wool task factory's copy-at-creation
    path.

    Given:
        A running event loop with wool's task factory installed and
        a parent scope whose live wool.Context carries an explicit
        ContextVar binding.
    When:
        A child coroutine is scheduled with
        ``context=contextvars.copy_context()`` — exercising the
        stdlib ``context=`` forwarding path of the wool factory.
    Then:
        The child's :func:`current_context` carries the parent's
        ContextVar binding (inherited by wool's fork-on-task) under
        a fresh chain id, and child mutations do not leak back to
        the parent — the stdlib ``context=`` argument is forwarded
        verbatim to asyncio without disturbing wool's parallel
        registry.
    """
    # Arrange
    import contextvars

    from wool.runtime.context import install_task_factory

    install_task_factory()

    var = ContextVar("copy_context_inherit_probe", default="default")
    var.set("parent-value")
    parent_ctx = current_context()

    captured: list[tuple[uuid.UUID, str]] = []

    async def child() -> None:
        captured.append((current_context().id, var.get()))
        # Mutate to verify isolation back toward the parent.
        var.set("child-mutated")

    # Act
    await asyncio.get_running_loop().create_task(
        child(), context=contextvars.copy_context()
    )

    # Assert
    assert len(captured) == 1
    child_id, child_value = captured[0]
    assert child_value == "parent-value"
    assert child_id != parent_ctx.id
    # Parent observes its own original binding — the child's mutation
    # rode the forked wool.Context, independent of stdlib Context.
    assert var.get() == "parent-value"
    assert current_context() is parent_ctx


@pytest.mark.asyncio
async def test_current_context_self_installs_task_factory():
    """Test :func:`current_context` self-installs the wool task
    factory on the running loop the first time it is called, so
    user code that touches wool without an explicit
    :func:`install_task_factory` call still gets fork-on-task
    semantics for tasks created afterward.

    Given:
        A running event loop on which the wool task factory has not
        been installed (the factory slot is empty).
    When:
        :func:`current_context` is called inside a task on that loop.
    Then:
        The loop's task factory is set to wool's wrapped factory,
        and a child task subsequently created with
        ``context=contextvars.copy_context()`` observes the
        copy-on-fork contract — the child's wool.Context inherits
        the parent's ContextVar bindings but carries a fresh chain
        id, and child mutations do not leak back to the parent.
    """
    # Arrange
    import contextvars

    loop = asyncio.get_running_loop()
    # Reset to a clean slate: any factory the test harness installed
    # for prior tests is removed so the auto-install path is exercised.
    loop.set_task_factory(None)

    # Act — first wool API touch on this loop should install the factory.
    parent_ctx = current_context()

    # Assert — factory is now wool's wrapped factory.
    factory = loop.get_task_factory()
    assert factory is not None
    assert getattr(factory, "__wool_wrapped__", False) is True

    # And the wrapper is doing its job: a child task with
    # copy_context inherits state but forks the chain.
    var = ContextVar("auto_install_probe", default="default")
    var.set("parent-value")

    captured: list[tuple[uuid.UUID, str]] = []

    async def child() -> None:
        captured.append((current_context().id, var.get()))
        var.set("child-mutated")

    await loop.create_task(child(), context=contextvars.copy_context())

    assert len(captured) == 1
    child_id, child_value = captured[0]
    assert child_value == "parent-value"
    assert child_id != parent_ctx.id
    assert var.get() == "parent-value"
