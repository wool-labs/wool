import asyncio
import contextvars
from types import SimpleNamespace
from typing import cast

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import ContextVarCollision
from wool.runtime.context import Token
from wool.runtime.context import apply_vars
from wool.runtime.context import build_frame_payload
from wool.runtime.context import current_context
from wool.runtime.context import reconstruct_var
from wool.runtime.context import snapshot_vars
from wool.runtime.routine.task import Serializer

dumps = cloudpickle.dumps
loads = cloudpickle.loads


@pytest.fixture(autouse=True)
def isolate_registry():
    from wool.runtime.context import _thread_context
    from wool.runtime.context import resolve_context

    saved = dict(ContextVar._registry)
    ctx = resolve_context()
    saved_data = dict(ctx._data)
    yield
    ctx._data.clear()
    ctx._data.update(saved_data)
    ContextVar._registry.clear()
    for k, v in saved.items():
        ContextVar._registry[k] = v
    if hasattr(_thread_context, "ctx"):
        _thread_context.ctx._data.clear()


class TestContextVar:
    def test___init___with_name_only(self):
        """Test ContextVar initialization with a name and no default.

        Given:
            A name string
        When:
            ContextVar is instantiated with the name
        Then:
            It should expose the name and raise LookupError on get() with no value set
        """
        # Act
        var = ContextVar("init_nameonly")

        # Assert
        assert var.name == "init_nameonly"
        with pytest.raises(LookupError):
            var.get()

    def test___init___with_default(self):
        """Test ContextVar initialization with a default value.

        Given:
            A name string and a default value
        When:
            ContextVar is instantiated with both
        Then:
            get() should return the default when no value is set
        """
        # Arrange
        var = ContextVar("init_withdefault", default=42)

        # Act & assert
        assert var.get() == 42

    def test___init___infers_namespace_from_caller(self):
        """Test ContextVar infers namespace from the caller's top-level package.

        Given:
            A ContextVar constructed from this test module
        When:
            No explicit namespace is provided
        Then:
            The namespace should be the top-level package of ``__name__``
        """
        # Arrange
        expected_ns = __name__.partition(".")[0]

        # Act
        var = ContextVar("inferred")

        # Assert
        assert var.namespace == expected_ns
        assert var.key == f"{expected_ns}:inferred"

    def test___init___accepts_explicit_namespace(self):
        """Test ContextVar uses an explicit namespace when provided.

        Given:
            A ContextVar constructed with namespace='myapp'
        When:
            No implicit inference is needed
        Then:
            The key should combine the explicit namespace with the name
        """
        # Act
        var = ContextVar("explicit", namespace="myapp")

        # Assert
        assert var.namespace == "myapp"
        assert var.key == "myapp:explicit"

    @given(
        name=st.text(
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
            min_size=1,
            max_size=12,
        ),
        namespace=st.text(
            alphabet=st.characters(min_codepoint=ord("a"), max_codepoint=ord("z")),
            min_size=1,
            max_size=12,
        ),
        default_a=st.one_of(st.integers(), st.text(), st.booleans()),
        default_b=st.one_of(st.integers(), st.text(), st.booleans()),
    )
    def test___init___raises_on_duplicate_key(
        self, name, namespace, default_a, default_b
    ):
        """Test duplicate-key construction raises regardless of default match.

        Given:
            Any non-empty name, namespace, and pair of default values
        When:
            A ContextVar is constructed, then a second with the identical key
        Then:
            The second construction raises ContextVarCollision whether or
            not the two defaults are equal
        """
        # The registry holds vars weakly, so we hold ``first`` for the
        # duration of the second construction; without that pin the
        # weakref would drop before the second call and no collision
        # would fire. The try/finally isolates each Hypothesis example
        # from its siblings since Hypothesis reuses the test function.
        #
        # _registry.pop is unavoidable per-example cleanup (not an
        # assertion): Hypothesis runs multiple examples within a single
        # test-function call, so the isolate_registry fixture cannot
        # reset state between examples.
        key = f"{namespace}:{name}"
        ContextVar._registry.pop(key, None)
        try:
            # Arrange
            first = ContextVar(name, namespace=namespace, default=default_a)

            # Act & assert
            with pytest.raises(ContextVarCollision):
                ContextVar(name, namespace=namespace, default=default_b)

            # Hold ``first`` to the end so the collision actually had
            # something to collide with.
            assert first.key == key
        finally:
            ContextVar._registry.pop(key, None)

    def test___init___allows_same_name_in_different_namespace(self):
        """Test duplicate names across different namespaces are allowed.

        Given:
            A ContextVar 'shared' in namespace 'lib_a'
        When:
            A second ContextVar 'shared' is constructed in namespace 'lib_b'
        Then:
            Both should register without collision
        """
        # Act
        a = ContextVar("shared", namespace="lib_a")
        b = ContextVar("shared", namespace="lib_b")

        # Assert
        assert a is not b
        assert a.key == "lib_a:shared"
        assert b.key == "lib_b:shared"

    def test_get_with_explicit_default_fallback(self):
        """Test ContextVar.get returns the supplied fallback when unset.

        Given:
            A ContextVar with no class-level default and no value set
        When:
            get() is called with a fallback argument
        Then:
            It should return the fallback argument
        """
        # Arrange
        var = ContextVar("no_default")

        # Act
        value = var.get("fallback")

        # Assert
        assert value == "fallback"

    def test_set_returns_usable_token(self):
        """Test ContextVar.set returns a Token that can restore prior state.

        Given:
            A ContextVar with a value set
        When:
            A second set() is performed
        Then:
            The returned Token should reset to the prior value via reset()
        """
        # Arrange
        var = ContextVar("restorable", default="initial")
        first_token = var.set("second")

        # Act
        second_token = var.set("third")
        var.reset(second_token)

        # Assert
        assert var.get() == "second"
        var.reset(first_token)
        assert var.get() == "initial"

    def test_reset_rejects_used_token(self):
        """Test ContextVar.reset raises on a token already consumed.

        Given:
            A ContextVar and a Token that has already been used
        When:
            reset() is called with the same token again
        Then:
            It should raise RuntimeError
        """
        # Arrange
        var = ContextVar("once", default=0)
        token = var.set(1)
        var.reset(token)

        # Act & assert
        with pytest.raises(RuntimeError):
            var.reset(token)

    def test_reset_rejects_token_for_different_var(self):
        """Test ContextVar.reset rejects tokens minted by a different var.

        Given:
            Two distinct ContextVar instances, each with a set value
        When:
            reset() is called on one with the other's token
        Then:
            It should raise ValueError
        """
        # Arrange
        a = ContextVar("reset_a", default=0)
        b = ContextVar("reset_b", default=0)
        token = a.set(1)

        # Act & assert
        with pytest.raises(ValueError):
            b.reset(token)

    def test_reset_restores_old_value_in_different_context_scope(self):
        """Test reset restores old_value when invoked in a different Context scope.

        Given:
            A ContextVar set twice in the original Context, then a
            different wool.Context scope entered via Context.run
        When:
            reset(token) is invoked inside the scoped Context
        Then:
            The var should revert to the value captured in the token
        """
        # Arrange
        var = ContextVar("reset_fallback", default="initial")
        var.set("first")
        token = var.set("second")
        seeded = contextvars.copy_context()

        def body():
            var.set("outer-most")
            var.reset(token)
            return var.get()

        # Act
        result = seeded.run(body)

        # Assert
        assert result == "first"

    def test_reset_restores_unset_in_different_context_scope(self):
        """Test reset restores unset state when old_value was MISSING.

        Given:
            A previously-unset ContextVar whose set() Token captured
            MISSING as the old_value, then a different wool.Context
            scope entered via Context.run
        When:
            reset(token) is invoked in the scoped Context
        Then:
            The var should revert to unset; get(fallback) returns the
            supplied fallback
        """
        # Arrange
        var = ContextVar("reset_unset_fallback")
        token = var.set("briefly")
        seeded = contextvars.copy_context()

        def body():
            var.set("nested")
            var.reset(token)
            return var.get("<fallback>")

        # Act
        result = seeded.run(body)

        # Assert
        assert result == "<fallback>"

    @pytest.mark.asyncio
    async def test_reset_rejects_token_from_different_context(self):
        """Test reset raises ValueError when the token is from another Context.

        Given:
            A Token minted inside one Context (async run)
        When:
            reset() is called on the var inside a different Context
        Then:
            It should raise ValueError citing the Context mismatch
        """
        # Arrange
        var = ContextVar("context_check", default="start")
        ctx_a = Context()
        ctx_b = Context()
        captured: list[Token] = []

        async def capture():
            captured.append(var.set("a"))

        await ctx_a.run_async(capture())

        async def try_reset():
            with pytest.raises(ValueError, match="different wool.Context"):
                var.reset(captured[0])

        # Act & assert
        await ctx_b.run_async(try_reset())

    def test___init___promotes_reconstructed_stub_preserving_identity(self):
        """Test constructing a var whose key was reconstructed from the wire
        returns the same instance instead of raising.

        Given:
            A ContextVar previously reconstructed under key 'myapp:promo_a'
            with no prior module-scope construction
        When:
            A module-scope ContextVar is constructed with the same key
        Then:
            The constructor returns the same instance (identity preserved)
            without raising ContextVarCollision
        """
        # Arrange
        reconstructed = reconstruct_var("myapp:promo_a", True, "from_wire")

        # Act
        promoted = ContextVar("promo_a", namespace="myapp", default="from_code")

        # Assert
        assert promoted is reconstructed

    def test___init___promotion_adopts_authoritative_default(self):
        """Test promotion adopts the module-scope default over the wire default.

        Given:
            A reconstructed stub with default 'from_wire'
        When:
            A module-scope ContextVar with default 'from_code' is constructed
            and get() is called with no value set
        Then:
            get() returns 'from_code' --- the module-scope default wins
        """
        # Arrange
        stub = reconstruct_var("myapp:promo_b", True, "from_wire")

        # Act
        var = ContextVar("promo_b", namespace="myapp", default="from_code")

        # Assert
        assert var is stub
        assert var.get() == "from_code"

    def test___init___promotion_preserves_wire_applied_value(self):
        """Test a value applied to the stub survives promotion.

        Given:
            A reconstructed stub to which a wire value has been applied
            via apply_vars
        When:
            A module-scope ContextVar for the same key is constructed and
            get() is called
        Then:
            get() returns the wire-applied value, not the module default
        """
        # Arrange
        stub = reconstruct_var("myapp:promo_c", True, "from_wire")
        apply_vars({"myapp:promo_c": dumps("applied_value")})

        # Act
        var = ContextVar("promo_c", namespace="myapp", default="from_code")

        # Assert
        assert var is stub
        assert var.get() == "applied_value"

    def test___init___second_promotion_attempt_raises(self):
        """Test a real (promoted) var still collides on subsequent construction.

        Given:
            A reconstructed stub that has been promoted by a first
            module-scope construction
        When:
            A second module-scope construction with the same key runs
        Then:
            ContextVarCollision is raised --- promotion is one-shot
        """
        # Arrange
        stub = reconstruct_var("myapp:promo_d", True, "from_wire")
        promoted = ContextVar("promo_d", namespace="myapp", default="first")

        # Act & assert
        with pytest.raises(ContextVarCollision):
            ContextVar("promo_d", namespace="myapp", default="second")

        assert promoted is stub

    def test___init___promotion_works_with_inferred_namespace(self):
        """Test promotion still works when the namespace is inferred from the caller.

        Given:
            A reconstructed stub under '<caller-package>:promo_e'
        When:
            A ContextVar is constructed with only the name (namespace inferred)
        Then:
            The constructor returns the reconstructed stub, confirming
            namespace inference resolves to the stub's key
        """
        # Arrange
        expected_ns = __name__.partition(".")[0]
        reconstructed = reconstruct_var(f"{expected_ns}:promo_e", True, "from_wire")

        # Act
        promoted = ContextVar("promo_e", default="from_code")

        # Assert
        assert promoted is reconstructed

    def test_reconstructed_stub_survives_garbage_collection(self):
        """Test a reconstructed stub is not reaped before promotion.

        Given:
            A stub reconstructed from the wire with no user-visible owner
        When:
            The garbage collector runs a full cycle
        Then:
            The stub is still in the registry and its applied value
            remains readable --- the Context-scoped pin keeps it alive
        """
        import gc

        # Arrange
        reconstruct_var("elsewhere:gc_target", True, "fallback")
        apply_vars({"elsewhere:gc_target": dumps("applied_before_gc")})

        # Act
        gc.collect()
        gc.collect()

        # Assert — reconstruct returns the existing instance if alive
        survived = reconstruct_var("elsewhere:gc_target", True, "fallback")

        assert survived is not None
        assert survived.get() == "applied_before_gc"

    def test_stub_pin_released_on_promotion_allows_gc(self):
        """Test promotion drops the stub pin so a promoted var can be GC'd.

        Given:
            A reconstructed stub that is promoted by a module-scope
            ContextVar construction
        When:
            The promoted var's local reference is dropped and GC runs
        Then:
            The registry entry is also dropped --- the stub pin no longer
            holds it, so lifetime defers to user code's strong refs
        """
        import gc
        import weakref

        # Arrange
        reconstruct_var("myapp:release_target", False, None)
        promoted = ContextVar("release_target", namespace="myapp")
        ref = weakref.ref(promoted)

        # Act
        del promoted
        gc.collect()

        # Assert — direct evidence the promoted var was reclaimed
        assert ref() is None
        # And re-constructing with the same key succeeds (no collision)
        replacement = ContextVar("release_target", namespace="myapp")
        assert replacement is not None

    def test_reconstructed_var_supports_set_get_reset_before_promotion(self):
        """Test a reconstructed stub behaves like a real var before promotion.

        Given:
            A ContextVar reconstructed from the wire but never promoted
        When:
            set, get, and reset are exercised on it through the public API
        Then:
            It behaves identically to a module-constructed var --- the set
            value is visible, reset restores the prior default
        """
        # Arrange
        var = reconstruct_var("elsewhere:pre_promo", True, "initial")

        # Act
        token = var.set("updated")
        assert var.get() == "updated"

        var.reset(token)

        # Assert
        assert var.get() == "initial"

    def test_pickle_roundtrip_returns_same_instance(self):
        """Test cloudpickle roundtrips a ContextVar to the same registered instance.

        Given:
            A ContextVar registered in the process-wide registry
        When:
            It is pickled and unpickled via dumps / loads
        Then:
            The unpickled instance should be the same object as the original
        """
        # Arrange
        var = ContextVar("shipped")

        # Act
        restored = loads(dumps(var))

        # Assert
        assert restored is var

    def test_repr_includes_key(self):
        """Test ContextVar repr includes the full key.

        Given:
            A ContextVar with a name
        When:
            repr() is called on it
        Then:
            The repr should include 'namespace:name'
        """
        # Arrange
        var = ContextVar("repr_cv")

        # Act
        text = repr(var)

        # Assert
        assert f"'{var.key}'" in text


class TestToken:
    def test_pickle_roundtrip_preserves_var_reference(self):
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
        assert var.key in text


class TestContext:
    def test___new___allows_direct_instantiation(self):
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

    def test_run_snapshot_skips_vars_that_are_unset_or_set_then_reset(self):
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

    def test_run_concurrent_entry_raises(self):
        """Test Context.run raises on re-entry.

        Given:
            A Context with a task currently running inside it
        When:
            A second run() is attempted
        Then:
            It should raise RuntimeError
        """
        # Arrange
        ctx = Context()

        def outer():
            with pytest.raises(RuntimeError):
                ctx.run(lambda: None)

        # Act & assert
        ctx.run(outer)

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

    def test_getitem_returns_captured_value(self):
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

    def test_len_returns_captured_count(self):
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

    @pytest.mark.asyncio
    async def test_run_async_seeds_and_scopes(self):
        """Test Context.run_async seeds vars and scopes mutations asynchronously.

        Given:
            A Context with a seeded var value
        When:
            run_async runs a coroutine that mutates the var
        Then:
            The mutation should be visible inside and captured on exit
        """
        # Arrange
        var = ContextVar("runa_seed", default="initial")
        var.set("seeded")
        ctx = current_context()

        async def body():
            assert var.get() == "seeded"
            var.set("mutated")
            return var.get()

        # Act
        result = await ctx.run_async(body())

        # Assert
        assert result == "mutated"
        assert ctx[var] == "mutated"

    @pytest.mark.asyncio
    async def test_run_async_snapshot_skips_vars_unset_or_set_then_reset(self):
        """Test Context.run_async's snapshot excludes vars that look unset at exit.

        Given:
            One var never set inside the coroutine, one var set and then
            reset inside the coroutine
        When:
            Context.run_async returns and captures the post-run snapshot
        Then:
            Neither var should appear in the Context's captured vars
        """
        # Arrange
        untouched = ContextVar("async_untouched", default="x")
        set_then_reset = ContextVar("async_set_then_reset")
        ctx = Context()

        async def body():
            token = set_then_reset.set("temp")
            set_then_reset.reset(token)

        # Act
        await ctx.run_async(body())

        # Assert
        assert untouched not in ctx
        assert set_then_reset not in ctx

    @pytest.mark.asyncio
    async def test_run_async_snapshot_preserves_seeded_value(self):
        """Test Context.run_async captures seeded values after the coroutine runs.

        Given:
            A Context seeded with a var value and a coroutine that
            doesn't touch that var
        When:
            Context.run_async runs the coroutine
        Then:
            The captured snapshot still holds the seeded value — the
            var was set (in the seeded Context) and counts as modified
        """
        # Arrange
        var = ContextVar("runa_preserve_seed", default="initial")
        var.set("seeded")
        ctx = current_context()

        async def body():
            return var.get()

        # Act
        result = await ctx.run_async(body())

        # Assert
        assert result == "seeded"
        assert ctx[var] == "seeded"

    @pytest.mark.asyncio
    async def test_run_async_snapshot_reflects_worker_mutations(self):
        """Test Context.run_async captures worker-side mutations on top of seed values.

        Given:
            A Context seeded with two vars and a coro that modifies one
        When:
            Context.run_async runs the coroutine
        Then:
            The captured snapshot reflects the mutation for the touched
            var while the untouched var still shows the seed value
        """
        # Arrange
        untouched = ContextVar("runa_untouched_seed", default="initial")
        touched = ContextVar("runa_touched_seed", default="initial")
        untouched.set("seed_untouched")
        touched.set("seed_touched")
        ctx = current_context()

        async def body():
            touched.set("mutated")

        # Act
        await ctx.run_async(body())

        # Assert
        assert ctx[untouched] == "seed_untouched"
        assert ctx[touched] == "mutated"

    def test_pickle_roundtrip_preserves_id_and_vars(self):
        """Test Context pickles its id and var dict.

        Given:
            A Context captured after setting a var
        When:
            It is pickled and unpickled via dumps / loads
        Then:
            The restored Context should have the same id and contain the var
        """
        # Arrange
        var = ContextVar("ctx_ship", default="zero")
        var.set("one")
        ctx = current_context()

        # Act
        restored = loads(dumps(ctx))

        # Assert
        assert restored.id == ctx.id
        assert restored[var] == "one"

    def test_pickle_roundtrip_embeds_current_value(self):
        """Test pickling a ContextVar captures its current value for the receiver.

        Given:
            A ContextVar set to a specific value in the current context
        When:
            The var is pickled and unpickled inside a fresh wool.Context
        Then:
            The receiver's var.get() returns the value that was set at
            pickle time --- the reducer-override embedded it
        """
        # Arrange
        var = ContextVar("pickle_with_value", default="default_value")
        var.set("pickled_value")
        pickled = dumps(var)

        # Act
        observed: list[object] = []

        def in_fresh():
            restored = loads(pickled)
            observed.append(restored.get())

        Context().run(in_fresh)

        # Assert
        assert observed == ["pickled_value"]

    def test_pickle_roundtrip_carries_no_value_when_unset(self):
        """Test pickling a never-set ContextVar doesn't apply a value on receive.

        Given:
            A ContextVar that has never been set in the current context
        When:
            The var is pickled and unpickled inside a fresh wool.Context
        Then:
            The receiver's var.get() returns the class-level default
            and the receiver's Context does not contain the var
            (no value was embedded in the pickle)
        """
        # Arrange
        var = ContextVar("pickle_no_value", default="default_value")
        pickled = dumps(var)

        # Act
        observed_get: list[object] = []
        observed_ctx: list[Context] = []

        def in_fresh():
            restored = loads(pickled)
            observed_get.append(restored.get())
            observed_ctx.append(current_context())

        Context().run(in_fresh)

        # Assert
        assert observed_get == ["default_value"]
        assert var not in observed_ctx[0]

    @pytest.mark.asyncio
    async def test_run_async_concurrent_entry_raises(self):
        """Test Context.run_async raises on re-entry while a task is running.

        Given:
            A Context with a run_async task currently pending
        When:
            A concurrent run_async is attempted
        Then:
            The second call should raise RuntimeError
        """
        # Arrange
        ctx = Context()

        async def slow():
            await asyncio.sleep(0.01)

        task = asyncio.create_task(ctx.run_async(slow()))
        await asyncio.sleep(0)

        second = slow()

        # Act & assert
        try:
            with pytest.raises(RuntimeError):
                await ctx.run_async(second)
        finally:
            second.close()

        await task

    def test_build_frame_payload_raises_typeerror_for_unpicklable_value(self):
        """Test build_frame_payload raises TypeError naming the offending var.

        Given:
            A ContextVar set to an unpicklable value (a local generator
            function object)
        When:
            build_frame_payload() is called to snapshot the current vars
        Then:
            TypeError is raised with a message naming the offending var
            key, per the public serialization contract that
            non-serializable values surface a TypeError at dispatch
            time.
        """
        # Arrange
        var = ContextVar("ctx001_unpicklable")

        def _local_gen():
            yield 1

        var.set(_local_gen())

        # Act & assert
        with pytest.raises(TypeError, match=var.key):
            build_frame_payload()

    def test_apply_vars_with_empty_map_is_noop(self):
        """Test apply_vars returns without touching state on an empty map.

        Given:
            A fresh wool Context and an empty ``vars`` map
        When:
            apply_vars is invoked with the empty map
        Then:
            The current context is unchanged and no exception is raised,
            mirroring the dispatch handler's behavior for requests with
            empty vars (i.e., the empty-map short-circuit path).
        """
        # Arrange
        var = ContextVar("ctx002_seed", default="default")
        var.set("before")
        before = current_context()

        # Act
        apply_vars({})

        # Assert
        after = current_context()
        assert after[var] == before[var]
        assert set(after.keys()) == set(before.keys())

    def test_apply_vars_with_unknown_key_drops_silently(self):
        """Test apply_vars drops unknown keys without raising.

        Given:
            A wire-form ``vars`` payload containing a key that is not
            registered on this process
        When:
            apply_vars is invoked with the payload
        Then:
            It returns without raising; unknown keys are silently
            dropped (rolling-deploy scenario where the caller has a
            var not yet present on the worker).
        """
        # Arrange
        known_var = ContextVar("ctx003_known", default="initial")
        payload = {
            "ctx003_unknown:missing": dumps("ignored"),
            known_var.key: dumps("applied"),
        }

        # Act
        apply_vars(payload)

        # Assert
        assert known_var.get() == "applied"

    def test_pickle_roundtrip_applies_embedded_value_to_receiver_context(self):
        """Test reconstructing a var writes its embedded value into the
        receiver's Context.

        Given:
            A wool.ContextVar set to value ``v`` and pickled via
            dumps (which embeds the current value in the reduce
            tuple)
        When:
            The pickled bytes are unpickled inside a fresh
            wool.Context scope and current_context() is captured
        Then:
            The reconstructed var appears in the captured Context
            with the embedded value --- confirming reconstruct writes
            into the receiver's Context._data (observable via
            ``var in ctx``).
        """
        # Arrange --- pickle while the var holds "wire_value", then
        # reset so the receiver's Context no longer has the value.
        # Unpickling forces reconstruct to apply the embedded
        # current_value into the receiver's Context._data.
        var = ContextVar("ctx004_roundtrip", default="d")
        token = var.set("wire_value")
        pickled = dumps(var)
        var.reset(token)

        observed: list[Context] = []

        def in_fresh():
            restored = loads(pickled)
            observed.append(current_context())
            assert restored.get() == "wire_value"

        # Act
        contextvars.copy_context().run(in_fresh)

        # Assert
        assert len(observed) == 1
        assert var in observed[0]
        assert observed[0][var] == "wire_value"

    @pytest.mark.asyncio
    async def test_current_context_omits_var_after_reset_to_unset(self):
        """Test current_context omits vars removed by reset.

        Given:
            A ContextVar set then immediately reset inside
            Context.run_async, removing it from the Context's data
        When:
            current_context() is invoked mid-run after the reset
        Then:
            The returned Context does not contain the var — reset
            popped it from _data so the snapshot excludes it.
        """
        # Arrange
        var = ContextVar("ctx005_reset_then_probe")
        ctx = Context()
        observed: list[Context] = []

        async def body():
            token = var.set("temporary")
            var.reset(token)
            observed.append(current_context())

        # Act
        await ctx.run_async(body())

        # Assert
        assert len(observed) == 1
        assert var not in observed[0]


def test_current_context_captures_set_vars_with_id():
    """Test current_context() captures explicitly-set vars and active id.

    Given:
        A ContextVar with an explicit value set
    When:
        current_context() is called
    Then:
        The returned Context should contain the var with its value
        and a non-None id
    """
    # Arrange
    var = ContextVar("cur_ctx", default=0)
    var.set(1)

    # Act
    ctx = current_context()

    # Assert
    assert ctx[var] == 1
    assert ctx.id is not None


def test_snapshot_vars_with_custom_dumps():
    """Test snapshot_vars serializes values via a custom dumps callable.

    Given:
        A ContextVar with a value set and a custom dumps function
    When:
        snapshot_vars is called with the custom dumps
    Then:
        It should serialize each value through the custom callable
    """
    # Arrange
    var = ContextVar("snap_custom_dumps", namespace="snap")
    var.set("hello")

    calls: list[object] = []

    def custom_dumps(value: object) -> bytes:
        calls.append(value)
        return b"custom:" + str(value).encode()

    # Act
    result = snapshot_vars(
        serializer=cast(Serializer, SimpleNamespace(dumps=custom_dumps))
    )

    # Assert
    assert var.key in result
    assert result[var.key] == b"custom:hello"
    assert calls == ["hello"]


def test_build_frame_payload_with_custom_dumps():
    """Test build_frame_payload returns a Context with custom-serialized vars.

    Given:
        A ContextVar with a value set and a custom dumps function
    When:
        build_frame_payload(serializer=custom) is called
    Then:
        It should return a protocol.Context whose vars map was
        produced by the custom serializer and whose id is a 32-char
        UUID hex string.
    """
    # Arrange
    var = ContextVar("bfp_custom_dumps", namespace="bfp")
    var.set(42)

    def custom_dumps(value: object) -> bytes:
        return b"bfp:" + str(value).encode()

    # Act
    ctx_msg = build_frame_payload(
        serializer=cast(Serializer, SimpleNamespace(dumps=custom_dumps))
    )

    # Assert
    assert var.key in ctx_msg.vars
    assert ctx_msg.vars[var.key] == b"bfp:42"
    assert isinstance(ctx_msg.id, str)
    assert len(ctx_msg.id) == 32


def test_apply_vars_with_custom_loads():
    """Test apply_vars deserializes values via a custom loads callable.

    Given:
        A ContextVar registered in the process and a wire-form vars
        map with a custom-encoded value
    When:
        apply_vars is called with a custom loads function
    Then:
        It should deserialize each value through the custom callable
        and apply it to the current Context
    """
    # Arrange
    var = ContextVar("apply_custom_loads", namespace="apcl")
    wire_vars = {var.key: b"custom-payload"}

    calls: list[bytes] = []

    def custom_loads(data: bytes) -> object:
        calls.append(data)
        return "decoded-" + data.decode()

    # Act
    apply_vars(
        wire_vars, serializer=cast(Serializer, SimpleNamespace(loads=custom_loads))
    )

    # Assert
    assert var.get() == "decoded-custom-payload"
    assert calls == [b"custom-payload"]


@pytest.mark.asyncio
async def test_task_contexts_register_is_visible_to_resolve_context():
    """Test resolve_context sees a task pre-registered in task_contexts.

    Given:
        An asyncio.Task with a wool.Context pre-registered in the
        task_contexts store under task_contexts_lock.
    When:
        The task's coroutine calls resolve_context().
    Then:
        It receives the pre-registered Context.
    """
    from wool.runtime.context import resolve_context
    from wool.runtime.context import task_contexts
    from wool.runtime.context import task_contexts_lock

    seeded = Context()
    captured: list[Context] = []

    async def body():
        captured.append(resolve_context())

    loop = asyncio.get_running_loop()
    task = asyncio.Task(body(), loop=loop)
    with task_contexts_lock:
        task_contexts[task] = seeded
    await task

    assert captured == [seeded]


def test_swap_context_sync_fallback_creates_fresh_previous():
    """Test swap_context returns a fresh Context when no prior thread context.

    Given:
        A sync caller with no prior wool.Context in the thread-local
        fallback (first swap in this thread scope)
    When:
        swap_context is called with a new Context
    Then:
        It should return a fresh empty Context as the previous value
        and install the new Context as current
    """
    import threading

    from wool.runtime.context import swap_context

    # Run in a fresh thread where no thread-local context exists.
    result: list[Context] = []

    def run():
        prev = swap_context(Context())
        result.append(prev)

    t = threading.Thread(target=run)
    t.start()
    t.join()

    # Assert
    assert isinstance(result[0], Context)
    assert len(result[0]) == 0


def test_token_cross_process_reset_rejects_wrong_context_id():
    """Test cross-process token reset rejects mismatched context id.

    Given:
        A Token reconstructed via reconstruct_token with _context=None
        and a context_id that differs from the current Context
    When:
        reset is called
    Then:
        It should raise ValueError citing the Context mismatch
    """
    from wool.runtime.context import reconstruct_token

    # Arrange
    var = ContextVar("xproc_reject", namespace="test_xproc")
    var.set("current")
    from uuid import uuid4

    wrong_id = uuid4()
    token = reconstruct_token(var, "old_val", wrong_id)

    # Act & assert
    with pytest.raises(ValueError, match="different wool.Context"):
        var.reset(token)


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
async def test_install_task_factory_composes_with_existing():
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
async def test_install_task_factory_emits_debug_log_for_each_path(caplog):
    """Test install_task_factory emits a debug log for each install path.

    Given:
        A running event loop and the wool context module's logger at
        DEBUG level
    When:
        install_task_factory runs against no prior factory, an already
        installed wool factory, and a custom user factory
    Then:
        Each invocation emits a debug log naming the path taken
    """
    from wool.runtime.context import install_task_factory

    # Arrange
    loop = asyncio.get_running_loop()
    loop.set_task_factory(None)

    # Act & assert — fresh install path
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()
    assert any("wool task factory installed" in r.message for r in caplog.records)
    caplog.clear()

    # Act & assert — already-installed path
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()
    assert any("already installed" in r.message for r in caplog.records)
    caplog.clear()

    # Arrange — custom factory in place
    loop.set_task_factory(None)

    def custom_factory(loop, coro, **kwargs):
        return asyncio.Task(coro, loop=loop, **kwargs)

    loop.set_task_factory(custom_factory)

    # Act & assert — compose path
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()
    assert any("composed with existing factory" in r.message for r in caplog.records)
    caplog.clear()

    # Act & assert — already-composed path
    with caplog.at_level("DEBUG", logger="wool.runtime.context"):
        install_task_factory()
    assert any(
        "composed task factory already installed" in r.message for r in caplog.records
    )

    # Cleanup
    loop.set_task_factory(None)
