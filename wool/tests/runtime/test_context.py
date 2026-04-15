import asyncio
import contextvars

import cloudpickle
import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool.runtime.context import _UNSET
from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import ContextVarCollision
from wool.runtime.context import Token
from wool.runtime.context import _apply_vars
from wool.runtime.context import _dumps
from wool.runtime.context import _loads
from wool.runtime.context import _reconstruct
from wool.runtime.context import _snapshot_vars
from wool.runtime.context import _UnsetType
from wool.runtime.context import current_context
from wool.runtime.context import dispatch_timeout


@pytest.fixture(autouse=True)
def isolate_registry():
    saved = dict(ContextVar._registry)
    yield
    ContextVar._registry.clear()
    for k, v in saved.items():
        ContextVar._registry[k] = v


class TestUnsetType:
    def test___new___returns_singleton_instance(self):
        """Test _UnsetType returns the same instance on every construction.

        Given:
            The _UnsetType class
        When:
            It is instantiated multiple times
        Then:
            It should return the same singleton instance each time
        """
        first = _UnsetType()
        second = _UnsetType()

        assert first is second
        assert first is _UNSET

    def test___repr___matches_stdlib_token_missing_format(self):
        """Test _UnsetType repr matches stdlib Token.MISSING format.

        Given:
            The _UNSET singleton
        When:
            repr() is called on it
        Then:
            It should return the stdlib-parity string '<Token.MISSING>'
        """
        assert repr(_UNSET) == "<Token.MISSING>"

    def test___bool___returns_false(self):
        """Test _UnsetType is falsy.

        Given:
            The _UNSET singleton
        When:
            It is evaluated in a boolean context
        Then:
            It should be falsy
        """
        assert not _UNSET

    def test___reduce___roundtrip_preserves_singleton_identity(self):
        """Test cloudpickle roundtrip returns the same singleton.

        Given:
            The _UNSET singleton
        When:
            It is serialized via cloudpickle.dumps and deserialized via cloudpickle.loads
        Then:
            The deserialized value should be the same singleton instance
        """
        restored = cloudpickle.loads(cloudpickle.dumps(_UNSET))

        assert restored is _UNSET


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
        var = ContextVar("init_nameonly")

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
        var = ContextVar("init_withdefault", default=42)

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
        expected_ns = __name__.partition(".")[0]

        var = ContextVar("inferred")

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
        var = ContextVar("explicit", namespace="myapp")

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
        key = f"{namespace}:{name}"
        ContextVar._registry.pop(key, None)
        try:
            first = ContextVar(name, namespace=namespace, default=default_a)

            with pytest.raises(ContextVarCollision):
                ContextVar(name, namespace=namespace, default=default_b)

            assert ContextVar._registry[key] is first
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
        a = ContextVar("shared", namespace="lib_a")
        b = ContextVar("shared", namespace="lib_b")

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
        var = ContextVar("no_default")

        value = var.get("fallback")

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
        var = ContextVar("restorable", default="initial")
        first_token = var.set("second")

        second_token = var.set("third")
        var.reset(second_token)

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
        var = ContextVar("once", default=0)
        token = var.set(1)
        var.reset(token)

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
        a = ContextVar("reset_a", default=0)
        b = ContextVar("reset_b", default=0)
        token = a.set(1)

        with pytest.raises(ValueError):
            b.reset(token)

    def test_reset_falls_back_to_old_value_in_fresh_stdlib_context(self):
        """Test reset via _old_value fallback when stdlib token is cross-context.

        Given:
            A ContextVar set twice, then a fresh stdlib Context is entered
            in which the captured Token's stdlib-token is no longer valid
        When:
            reset(token) is invoked inside the fresh stdlib Context
        Then:
            It should catch the stdlib ValueError and restore via _old_value
        """
        var = ContextVar("reset_fallback", default="initial")
        var.set("first")
        token = var.set("second")
        seeded = contextvars.copy_context()

        def body():
            var.set("outer-most")
            var.reset(token)
            return var.get()

        result = seeded.run(body)

        assert result == "first"

    def test_reset_falls_back_to_unset_old_value_in_fresh_stdlib_context(self):
        """Test reset fallback restores _UNSET when that was the prior state.

        Given:
            A previously-unset ContextVar; a set() produces a Token whose
            _old_value is _UNSET; a fresh stdlib Context is entered
        When:
            reset(token) is invoked in the fresh Context
        Then:
            The var should revert to unset; get(fallback) returns the
            supplied fallback
        """
        var = ContextVar("reset_unset_fallback")
        token = var.set("briefly")
        seeded = contextvars.copy_context()

        def body():
            var.set("nested")
            var.reset(token)
            return var.get("<fallback>")

        result = seeded.run(body)

        assert result == "<fallback>"

    @pytest.mark.asyncio
    async def test_reset_rejects_token_from_different_lineage(self):
        """Test reset raises ValueError when the token is from another lineage.

        Given:
            A Token minted inside one Context's lineage (async run)
        When:
            reset() is called on the var inside a different Context's lineage
        Then:
            It should raise ValueError citing the lineage mismatch
        """
        var = ContextVar("lineage_check", default="start")
        ctx_a = Context()
        ctx_b = Context()
        captured: list[Token] = []

        async def capture():
            captured.append(var.set("a"))

        await ctx_a.run_async(capture())

        async def try_reset():
            with pytest.raises(ValueError, match="different wool.Context lineage"):
                var.reset(captured[0])

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
        reconstructed = _reconstruct("myapp:promo_a", True, "from_wire")

        promoted = ContextVar("promo_a", namespace="myapp", default="from_code")

        assert promoted is reconstructed

    def test___init___promotion_adopts_authoritative_default(self):
        """Test promotion adopts the module-scope default over the wire default.

        Given:
            A reconstructed stub with default 'from_wire'
        When:
            A module-scope ContextVar with default 'from_code' is constructed
            and get() is called with no value set
        Then:
            get() returns 'from_code' — the module-scope default wins
        """
        _stub = _reconstruct("myapp:promo_b", True, "from_wire")

        var = ContextVar("promo_b", namespace="myapp", default="from_code")

        assert var is _stub
        assert var.get() == "from_code"

    def test___init___promotion_preserves_wire_applied_value(self):
        """Test a value applied to the stub survives promotion.

        Given:
            A reconstructed stub to which a wire value has been applied
            via _apply_vars
        When:
            A module-scope ContextVar for the same key is constructed and
            get() is called
        Then:
            get() returns the wire-applied value, not the module default
        """
        _stub = _reconstruct("myapp:promo_c", True, "from_wire")
        _apply_vars({"myapp:promo_c": _dumps("applied_value")})

        var = ContextVar("promo_c", namespace="myapp", default="from_code")

        assert var is _stub
        assert var.get() == "applied_value"

    def test___init___second_promotion_attempt_raises(self):
        """Test a real (promoted) var still collides on subsequent construction.

        Given:
            A reconstructed stub that has been promoted by a first
            module-scope construction
        When:
            A second module-scope construction with the same key runs
        Then:
            ContextVarCollision is raised — promotion is one-shot
        """
        _stub = _reconstruct("myapp:promo_d", True, "from_wire")
        _promoted = ContextVar("promo_d", namespace="myapp", default="first")

        with pytest.raises(ContextVarCollision):
            ContextVar("promo_d", namespace="myapp", default="second")

        assert _promoted is _stub

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
        expected_ns = __name__.partition(".")[0]
        reconstructed = _reconstruct(f"{expected_ns}:promo_e", True, "from_wire")

        promoted = ContextVar("promo_e", default="from_code")

        assert promoted is reconstructed

    def test_reconstructed_stub_survives_garbage_collection(self):
        """Test a sallyport-registered stub is not reaped before promotion.

        Given:
            A stub reconstructed from the wire with no user-visible owner
        When:
            The garbage collector runs a full cycle
        Then:
            The stub is still in the registry and its applied value
            remains readable — the sallyport pin keeps it alive
        """
        import gc

        _reconstruct("elsewhere:gc_target", True, "fallback")
        _apply_vars({"elsewhere:gc_target": _dumps("applied_before_gc")})
        gc.collect()
        gc.collect()

        survived = ContextVar._registry.get("elsewhere:gc_target")

        assert survived is not None
        assert survived.get() == "applied_before_gc"

    def test_stub_pin_released_on_promotion_allows_gc(self):
        """Test promotion drops the sallyport pin so a promoted var can be GC'd.

        Given:
            A reconstructed stub that is promoted by a module-scope
            ContextVar construction
        When:
            The promoted var's local reference is dropped and GC runs
        Then:
            The registry entry is also dropped — the stub pin no longer
            holds it, so lifetime defers to user code's strong refs
        """
        import gc

        _reconstruct("myapp:release_target", False, None)
        key = "myapp:release_target"

        promoted = ContextVar("release_target", namespace="myapp")
        assert key not in ContextVar._stub_pins

        del promoted
        gc.collect()

        assert key not in ContextVar._registry

    def test_reconstructed_var_supports_set_get_reset_before_promotion(self):
        """Test a reconstructed stub behaves like a real var before promotion.

        Given:
            A ContextVar reconstructed from the wire but never promoted
        When:
            set, get, and reset are exercised on it through the public API
        Then:
            It behaves identically to a module-constructed var — the set
            value is visible, reset restores the prior default
        """
        var = _reconstruct("elsewhere:pre_promo", True, "initial")

        token = var.set("updated")
        assert var.get() == "updated"

        var.reset(token)

        assert var.get() == "initial"

    def test_pickle_roundtrip_returns_same_instance(self):
        """Test cloudpickle roundtrips a ContextVar to the same registered instance.

        Given:
            A ContextVar registered in the process-wide registry
        When:
            It is pickled and unpickled via _dumps / _loads
        Then:
            The unpickled instance should be the same object as the original
        """
        var = ContextVar("shipped")

        restored = _loads(_dumps(var))

        assert restored is var


class Test_reconstruct:
    def test_returns_existing_instance_when_registered(self):
        """Test _reconstruct returns the existing instance on registry hit.

        Given:
            A ContextVar already registered
        When:
            _reconstruct is invoked with the same key
        Then:
            It should return the existing instance
        """
        var = ContextVar("known")

        restored = _reconstruct(var.key, False, None)

        assert restored is var

    def test_creates_missing_instance_defensively(self):
        """Test _reconstruct creates and registers a var on registry miss.

        Given:
            An empty registry (no var registered under 'elsewhere:ghost')
        When:
            _reconstruct is invoked with that key
        Then:
            A new ContextVar should be created, registered, and returned
            with the namespace and name split from the key
        """
        restored = _reconstruct("elsewhere:ghost", True, "fallback")

        assert restored.namespace == "elsewhere"
        assert restored.name == "ghost"
        assert restored.get() == "fallback"
        assert ContextVar._registry["elsewhere:ghost"] is restored


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
        var = ContextVar("tokened")
        token = var.set("x")

        restored = _loads(_dumps(token))

        assert restored.var is var
        assert restored.old_value is _UNSET


class Test_snapshot_vars:
    def test_omits_vars_with_no_explicit_value(self):
        """Test _snapshot_vars omits vars with no explicit value.

        Given:
            A ContextVar registered but not set
        When:
            _snapshot_vars() is called
        Then:
            The result should not contain the var's key
        """
        var = ContextVar("never_set")

        result = _snapshot_vars()

        assert var.key not in result

    def test_emits_set_value_under_full_key(self):
        """Test _snapshot_vars emits 'namespace:name' keys with serialized values.

        Given:
            A ContextVar with an explicit value set
        When:
            _snapshot_vars() is called
        Then:
            The result should contain an entry under the var's full key
        """
        var = ContextVar("shipped")
        var.set("hello")

        result = _snapshot_vars()

        assert var.key in result
        assert _loads(result[var.key]) == "hello"

    def test_reads_from_explicit_stdlib_context(self):
        """Test _snapshot_vars(ctx) reads values from the supplied Context.

        Given:
            An explicit stdlib Context seeded with a var value
        When:
            _snapshot_vars(explicit_ctx) is called
        Then:
            The result should reflect the seeded value, not the outer context
        """
        var = ContextVar("ctx_read")
        var.set("outer")
        seeded = contextvars.copy_context()

        def body():
            var.set("inner")

        seeded.run(body)

        result = _snapshot_vars(seeded)

        assert _loads(result[var.key]) == "inner"

    def test_skips_vars_absent_from_explicit_context(self):
        """Test _snapshot_vars(ctx) skips vars not present in the ctx.

        Given:
            A ContextVar never set in the supplied stdlib Context
        When:
            _snapshot_vars(fresh_ctx) is called
        Then:
            The result should not contain the var's key
        """
        var = ContextVar("not_in_ctx")
        empty_ctx = contextvars.Context()

        result = _snapshot_vars(empty_ctx)

        assert var.key not in result

    def test_raises_typeerror_on_unpicklable_value(self):
        """Test _snapshot_vars raises TypeError naming the failing var.

        Given:
            A ContextVar holding a value that cloudpickle cannot serialize
        When:
            _snapshot_vars() attempts to serialize it
        Then:
            A TypeError should be raised mentioning the var's key
        """

        class Holder:
            def __reduce__(self):
                raise TypeError("nope")

        var = ContextVar("unpickle_me")
        var.set(Holder())

        with pytest.raises(TypeError) as exc_info:
            _snapshot_vars()

        assert "unpickle_me" in str(exc_info.value)


class Test_apply_vars:
    def test_restores_value_on_target_var(self):
        """Test _apply_vars restores a serialized value onto the local var.

        Given:
            A ContextVar registered locally and a snapshot dict
        When:
            _apply_vars is called with the dict
        Then:
            The var's value in the current context should match the snapshot
        """
        var = ContextVar("apply_restore")
        snapshot = {var.key: _dumps("hello")}

        _apply_vars(snapshot)

        assert var.get() == "hello"

    def test_skips_and_warns_on_unknown_key(self, caplog):
        """Test _apply_vars warns and skips values for unregistered keys.

        Given:
            A snapshot dict with a key that has no registered ContextVar
        When:
            _apply_vars is called with caplog capturing WARNINGs
        Then:
            A warning should be emitted and no var created
        """
        import logging

        caplog.set_level(logging.WARNING, logger="wool.runtime.context")
        snapshot = {"elsewhere:appeared": _dumps("late")}

        _apply_vars(snapshot)

        assert "elsewhere:appeared" not in ContextVar._registry
        assert any("elsewhere:appeared" in record.message for record in caplog.records)

    def test_empty_dict_returns_immediately(self):
        """Test _apply_vars is a no-op for an empty dict.

        Given:
            An empty dict
        When:
            _apply_vars({}) is called
        Then:
            No error should be raised
        """
        _apply_vars({})


class TestContext:
    def test___new___allows_direct_instantiation(self):
        """Test Context() constructs an empty Context with a fresh lineage UUID.

        Given:
            The Context class
        When:
            It is instantiated directly
        Then:
            The result should have a fresh lineage UUID and no captured vars
        """
        ctx = Context()

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
        var = ContextVar("run_seed", default="initial")
        var.set("seeded")
        ctx = current_context()

        def body():
            assert var.get() == "seeded"
            var.set("mutated")
            return var.get()

        result = ctx.run(body)

        assert result == "mutated"
        assert ctx[var] == "mutated"

    def test_run_binds_lineage_for_sync_callers(self):
        """Test Context.run makes self.id the active lineage inside fn.

        Given:
            A Context constructed directly (sync caller, no asyncio task)
        When:
            Context.run invokes a function that reads current_context().id
        Then:
            The reported lineage id equals the Context's own id, not the
            process-default lineage
        """
        ctx = Context()

        observed = ctx.run(lambda: current_context().id)

        assert observed == ctx.id

    def test_run_snapshot_skips_untouched_and_unset_vars(self):
        """Test Context.run's snapshot excludes registered vars that weren't set
        and vars explicitly holding the _UNSET sentinel.

        Given:
            Two registered vars — one never set in any context, one reset
            to _UNSET via the fallback path inside the run
        When:
            Context.run() returns and captures the post-run snapshot
        Then:
            Neither var should appear in the Context's captured vars
        """
        from wool.runtime.context import _UNSET

        untouched = ContextVar("untouched", default="x")  # never set
        reset_to_unset = ContextVar("reset_unset")  # no default
        ctx = Context()

        def body():
            reset_to_unset._stdlib.set(_UNSET)

        ctx.run(body)

        assert untouched not in ctx
        assert reset_to_unset not in ctx

    def test_run_concurrent_entry_raises(self):
        """Test Context.run raises on re-entry.

        Given:
            A Context with a task currently running inside it
        When:
            A second run() is attempted
        Then:
            It should raise RuntimeError
        """
        ctx = Context()

        def outer():
            with pytest.raises(RuntimeError):
                ctx.run(lambda: None)

        ctx.run(outer)

    def test_pickle_roundtrip_preserves_id_and_vars(self):
        """Test Context pickles its lineage id and var dict.

        Given:
            A Context captured after setting a var
        When:
            It is pickled and unpickled via _dumps / _loads
        Then:
            The restored Context should have the same id and contain the var
        """
        var = ContextVar("ctx_ship", default="zero")
        var.set("one")
        ctx = current_context()

        restored = _loads(_dumps(ctx))

        assert restored.id == ctx.id
        assert restored[var] == "one"

    def test_iter_yields_captured_vars(self):
        """Test Context iterates over captured ContextVar instances.

        Given:
            A Context with multiple captured vars
        When:
            It is iterated
        Then:
            The iterator should yield each captured var
        """
        a = ContextVar("iter_a", default=0)
        b = ContextVar("iter_b", default=0)
        a.set(1)
        b.set(2)

        ctx = current_context()

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
        var = ContextVar("get_item", default=0)
        var.set(1)
        ctx = current_context()

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
        in_ctx = ContextVar("present_var", default=0)
        out_ctx = ContextVar("absent_var", default=0)
        in_ctx.set(1)

        ctx = current_context()

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
        a = ContextVar("len_a", default=0)
        b = ContextVar("len_b", default=0)
        a.set(1)
        b.set(2)

        ctx = current_context()

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
        a = ContextVar("kvitems_a", default="")
        b = ContextVar("kvitems_b", default="")
        a.set("x")
        b.set("y")

        ctx = current_context()

        assert set(ctx.keys()) == {a, b}
        assert set(ctx.values()) == {"x", "y"}
        assert dict(ctx.items()) == {a: "x", b: "y"}

    def test_repr_includes_lineage_id_and_var_count(self):
        """Test Context repr mentions lineage id and number of vars.

        Given:
            A Context with one captured var
        When:
            repr() is called on it
        Then:
            The repr should contain 'lineage=' and 'vars=1'
        """
        var = ContextVar("repr_var", default=0)
        var.set(1)
        ctx = current_context()

        text = repr(ctx)

        assert "lineage=" in text
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
        var = ContextVar("runa_seed", default="initial")
        var.set("seeded")
        ctx = current_context()

        async def body():
            assert var.get() == "seeded"
            var.set("mutated")
            return var.get()

        result = await ctx.run_async(body())

        assert result == "mutated"
        assert ctx[var] == "mutated"

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
        ctx = Context()

        async def slow():
            await asyncio.sleep(0.01)

        task = asyncio.create_task(ctx.run_async(slow()))
        await asyncio.sleep(0)

        second = slow()
        try:
            with pytest.raises(RuntimeError):
                await ctx.run_async(second)
        finally:
            second.close()

        await task


class Test_current_context:
    def test_captures_set_vars_with_lineage(self):
        """Test current_context() captures explicitly-set vars and active lineage.

        Given:
            A ContextVar with an explicit value set
        When:
            current_context() is called
        Then:
            The returned Context should contain the var with its value
            and a non-None lineage id
        """
        var = ContextVar("cur_ctx", default=0)
        var.set(1)

        ctx = current_context()

        assert ctx[var] == 1
        assert ctx.id is not None


class Test_build_frame_payload:
    def test_returns_vars_and_lineage_hex(self):
        """Test build_frame_payload returns (vars_dict, lineage_hex).

        Given:
            A ContextVar with an explicit value set
        When:
            build_frame_payload() is called
        Then:
            It should return (dict with the var's key, hex-string lineage id)
        """
        from wool.runtime.context import build_frame_payload

        var = ContextVar("frame_var")
        var.set("v")

        vars_dict, lineage_hex = build_frame_payload()

        assert var.key in vars_dict
        assert isinstance(lineage_hex, str)
        assert len(lineage_hex) == 32  # UUID hex


class TestDispatchTimeoutContextVar:
    def test_default_is_none(self):
        """Test dispatch_timeout defaults to None.

        Given:
            The dispatch_timeout contextvar
        When:
            get() is called without setting
        Then:
            It should return None
        """
        assert dispatch_timeout.get() is None

    def test_set_and_reset_restores_default(self):
        """Test dispatch_timeout.set returns a token usable to restore default.

        Given:
            A call to dispatch_timeout.set(5.0)
        When:
            reset(token) is called
        Then:
            get() should return the default None
        """
        token = dispatch_timeout.set(5.0)
        assert dispatch_timeout.get() == 5.0

        dispatch_timeout.reset(token)

        assert dispatch_timeout.get() is None


class TestPublicTypeShape:
    def test_token_repr_includes_var_key(self):
        """Test Token repr includes the owning var's key.

        Given:
            A Token produced by set() on a ContextVar
        When:
            repr() is called on it
        Then:
            The repr should include the var's full key
        """
        var = ContextVar("repr_token_var")
        token = var.set("x")

        text = repr(token)

        assert var.key in text

    def test_contextvar_repr_includes_key(self):
        """Test ContextVar repr includes the full key.

        Given:
            A ContextVar with a name
        When:
            repr() is called on it
        Then:
            The repr should include 'namespace:name'
        """
        var = ContextVar("repr_cv")

        text = repr(var)

        assert f"'{var.key}'" in text
