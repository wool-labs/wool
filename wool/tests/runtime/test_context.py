import asyncio
import contextvars
import sys
import types

import cloudpickle
import pytest

from wool.runtime.context import _UNSET
from wool.runtime.context import Context
from wool.runtime.context import ContextVar
from wool.runtime.context import Token
from wool.runtime.context import _apply_vars
from wool.runtime.context import _dumps
from wool.runtime.context import _loads
from wool.runtime.context import _snapshot_vars
from wool.runtime.context import _UnsetType
from wool.runtime.context import current_context
from wool.runtime.context import dispatch_timeout


@pytest.fixture
def fresh_module():
    """Create a synthetic module registered in sys.modules and drop it on teardown."""
    name = "tests_fresh_contextvar_mod"
    module = types.ModuleType(name)
    sys.modules[name] = module
    yield module
    sys.modules.pop(name, None)


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
        # Act
        first = _UnsetType()
        second = _UnsetType()

        # Assert
        assert first is second
        assert first is _UNSET

    def test___repr___returns_unset_literal(self):
        """Test _UnsetType repr returns the string 'UNSET'.

        Given:
            The _UNSET singleton
        When:
            repr() is called on it
        Then:
            It should return the string 'UNSET'
        """
        # Act & assert
        assert repr(_UNSET) == "UNSET"

    def test___bool___returns_false(self):
        """Test _UnsetType is falsy.

        Given:
            The _UNSET singleton
        When:
            It is evaluated in a boolean context
        Then:
            It should be falsy
        """
        # Act & assert
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
        # Act
        restored = cloudpickle.loads(cloudpickle.dumps(_UNSET))

        # Assert
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
        # Arrange, act
        var = ContextVar("example")

        # Assert
        assert var.name == "example"
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
        # Arrange, act
        var = ContextVar("example", default=42)

        # Assert
        assert var.get() == 42

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
        a = ContextVar("a", default=0)
        b = ContextVar("b", default=0)
        token = a.set(1)

        # Act & assert
        with pytest.raises(ValueError):
            b.reset(token)

    def test_reset_falls_back_to_old_value_in_fresh_stdlib_context(self, fresh_module):
        """Test reset via _old_value fallback when stdlib token is cross-context.

        Given:
            A module-bound ContextVar that is set twice (so the second
            Token's captured old_value is a real previous value), then a
            fresh stdlib Context is entered in which the second Token's
            stdlib-token is not valid
        When:
            reset(token) is invoked inside the fresh stdlib Context
        Then:
            It should catch the stdlib ValueError and restore the var to
            its pre-second-set value via the Token's captured _old_value
        """
        # Arrange
        var = ContextVar("reset_fallback", default="initial")
        fresh_module.reset_fallback = var
        var.set("first")
        token = var.set("second")
        seeded = contextvars.copy_context()

        def body():
            var.set("outer-most")
            # Inside this stdlib Context the outer token is not valid.
            var.reset(token)
            return var.get()

        # Act
        result = seeded.run(body)

        # Assert — reset fell back to _old_value = "first"
        assert result == "first"

    def test_reset_falls_back_to_unset_old_value_in_fresh_stdlib_context(
        self, fresh_module
    ):
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
        # Arrange
        var = ContextVar("reset_unset_fallback")
        fresh_module.reset_unset_fallback = var
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

    def test_pickle_roundtrip_via_module_binding(self, fresh_module):
        """Test cloudpickle roundtrips a module-bound ContextVar to the same instance.

        Given:
            A ContextVar assigned to a module attribute
        When:
            It is pickled and unpickled via cloudpickle
        Then:
            The unpickled object should be the same module-attribute instance
        """
        # Arrange
        var = ContextVar("shipped")
        fresh_module.shipped = var

        # Act
        restored = _loads(_dumps(var))

        # Assert
        assert restored is var

    def test_aliased_bindings_both_see_mutations(self, fresh_module):
        """Test a ContextVar aliased under multiple attrs shares state.

        Given:
            A ContextVar bound to two module attributes on the same module
        When:
            One alias sets a value
        Then:
            The other alias should observe the same value via get()
        """
        # Arrange
        var = ContextVar("aliased")
        fresh_module.foo = var
        fresh_module.bar = var

        # Act
        fresh_module.foo.set("mutation")

        # Assert
        assert fresh_module.bar.get() == "mutation"

    def test_snapshot_emits_entries_for_every_alias(self, fresh_module):
        """Test _snapshot_vars includes all bindings of an aliased var.

        Given:
            A ContextVar aliased under multiple module attributes
        When:
            _snapshot_vars() is called after setting a value
        Then:
            The snapshot should contain one entry per alias, each
            carrying the same serialized value
        """
        # Arrange
        var = ContextVar("multi")
        fresh_module.primary = var
        fresh_module.secondary = var
        var.set("value")

        # Act
        snapshot = _snapshot_vars()

        # Assert
        primary_key = f"{fresh_module.__name__}:primary"
        secondary_key = f"{fresh_module.__name__}:secondary"
        assert primary_key in snapshot
        assert secondary_key in snapshot
        assert snapshot[primary_key] == snapshot[secondary_key]

    def test_reconstruct_falls_back_to_later_locations(self, fresh_module):
        """Test the pickle reconstructor tries every location in order.

        Given:
            A ContextVar whose cached locations list starts with a
            location that does not exist on the receiver
        When:
            The var is pickled and unpickled
        Then:
            The reconstructor should fall back to the next valid
            location and return the same instance
        """
        # Arrange
        var = ContextVar("fallback")
        fresh_module.fallback = var
        # Seed the cache with a bad first entry followed by the real one.
        var._locations = [
            ("nonexistent_module_for_fallback_test", "ghost"),
            (fresh_module.__name__, "fallback"),
        ]

        # Act
        restored = _loads(_dumps(var))

        # Assert
        assert restored is var

    def test_resolve_locations_cache_hit_avoids_rescan(self, fresh_module):
        """Test repeated pickling of a bound var reuses the cached locations.

        Given:
            A module-bound ContextVar that has been pickled once so the
            locations cache is populated
        When:
            The same var is pickled again without any intervening module
            changes
        Then:
            The produced byte strings should be byte-identical, reflecting
            a cache-hit path that doesn't rescan sys.modules
        """
        # Arrange
        var = ContextVar("cache_hit")
        fresh_module.cache_hit = var
        first = _dumps(var)

        # Act
        second = _dumps(var)

        # Assert
        assert first == second

    def test_pickling_unbound_var_raises_pickling_error(self):
        """Test pickling an unbound ContextVar raises PicklingError.

        Given:
            A ContextVar that is never assigned to any module attribute
        When:
            It is pickled via _dumps (inside a closure-carrying tuple)
        Then:
            It should raise pickle.PicklingError describing the unbound
            state
        """
        # Arrange
        import pickle as _pickle

        var = ContextVar("never_bound")
        # Deliberately do NOT assign to any module.

        # Act & assert
        with pytest.raises(_pickle.PicklingError, match="not bound to any"):
            _dumps(var)

    def test_resolve_locations_skips_none_module_entries(self, fresh_module):
        """Test _resolve_locations silently skips None entries in sys.modules.

        Given:
            A bound ContextVar, with sys.modules containing a None entry
            (sentinel values can appear in sys.modules during failed imports)
        When:
            The var is pickled (triggering _resolve_locations)
        Then:
            The pickle should succeed, producing the var's bound location;
            the None entry does not cause an exception
        """
        # Arrange
        var = ContextVar("skip_none")
        fresh_module.skip_none = var
        # Invalidate cache so sys.modules walk actually runs.
        var._locations = None
        sys.modules["__none_sentinel_probe__"] = None
        try:
            # Act
            restored = _loads(_dumps(var))
        finally:
            sys.modules.pop("__none_sentinel_probe__", None)

        # Assert
        assert restored is var

    def test_resolve_locations_skips_non_dict_modules(self, fresh_module):
        """Test _resolve_locations silently skips modules whose __dict__ raises.

        Given:
            A bound ContextVar, with sys.modules containing an object whose
            __dict__ access raises AttributeError
        When:
            The var is pickled (triggering _resolve_locations)
        Then:
            The pickle should succeed; the pathological entry is skipped
            rather than causing the walk to fail
        """

        # Arrange
        class _NoDictModule:
            def __getattribute__(self, item):
                if item == "__dict__":
                    raise AttributeError("no dict here")
                return super().__getattribute__(item)

        var = ContextVar("skip_no_dict")
        fresh_module.skip_no_dict = var
        var._locations = None
        sys.modules["__nodict_sentinel_probe__"] = _NoDictModule()
        try:
            # Act
            restored = _loads(_dumps(var))
        finally:
            sys.modules.pop("__nodict_sentinel_probe__", None)

        # Assert
        assert restored is var

    def test_reconstruct_falls_back_across_import_error(self, fresh_module):
        """Test the reconstructor falls past locations whose module can't import.

        Given:
            A module-bound ContextVar pickled while bound at two
            locations (a temporary module first, then a legitimate one);
            before unpickling, the temporary module is removed from
            sys.modules and is not importable
        When:
            The pickled form is loaded
        Then:
            The reconstructor should encounter ImportError on the first
            location, fall through, and return the var via the second
        """
        # Arrange — bind the var in both modules, forcing temp first
        temp_name = "__temp_import_fail_module__"
        temp_mod = types.ModuleType(temp_name)
        sys.modules[temp_name] = temp_mod
        var = ContextVar("import_fail_cv")
        temp_mod.exported = var
        fresh_module.import_fail_cv = var
        # Pin the order so the non-importable location is tried first.
        var._locations = [
            (temp_name, "exported"),
            (fresh_module.__name__, "import_fail_cv"),
        ]
        try:
            blob = _dumps(var)
            sys.modules.pop(temp_name, None)
            del temp_mod.exported

            # Act
            restored = _loads(blob)

            # Assert
            assert restored is fresh_module.import_fail_cv
        finally:
            sys.modules.pop(temp_name, None)

    def test_reconstruct_raises_when_no_location_resolves(self, fresh_module):
        """Test the reconstructor raises RuntimeError when every location fails.

        Given:
            A ContextVar whose locations list points exclusively to
            modules that are not in sys.modules and not importable
        When:
            The pickled form is loaded
        Then:
            It should raise RuntimeError whose message names the failed
            locations
        """
        # Arrange — two temp modules; both removed before unpickling
        name_a = "__temp_all_fail_a__"
        name_b = "__temp_all_fail_b__"
        mod_a = types.ModuleType(name_a)
        mod_b = types.ModuleType(name_b)
        sys.modules[name_a] = mod_a
        sys.modules[name_b] = mod_b
        var = ContextVar("all_fail_cv")
        mod_a.exported = var
        mod_b.exported = var
        var._locations = [(name_a, "exported"), (name_b, "exported")]
        try:
            blob = _dumps(var)
            sys.modules.pop(name_a, None)
            sys.modules.pop(name_b, None)
            del mod_a.exported
            del mod_b.exported

            # Act & assert
            with pytest.raises(RuntimeError, match="Cannot resolve"):
                _loads(blob)
        finally:
            sys.modules.pop(name_a, None)
            sys.modules.pop(name_b, None)

    def test_reconstruct_falls_back_when_attr_is_not_context_var(self, fresh_module):
        """Test the reconstructor skips locations whose attr is not a ContextVar.

        Given:
            A module-bound ContextVar pickled at two locations; before
            unpickling, one of those locations is rebound to a
            non-ContextVar attribute
        When:
            The pickled form is loaded
        Then:
            The reconstructor should record the not-a-ContextVar error
            at the first location, fall through, and return the var via
            the second
        """
        # Arrange — bind the var at a second attribute on fresh_module
        var = ContextVar("not_cv_cv")
        fresh_module.not_cv_primary = var
        fresh_module.not_cv_alias = var
        blob = _dumps(var)
        # Rebind primary to a non-ContextVar BEFORE unpickling
        fresh_module.not_cv_primary = 42

        # Act
        restored = _loads(blob)

        # Assert
        assert restored is var


class TestToken:
    def test_pickle_roundtrip_preserves_reset_semantics(self, fresh_module):
        """Test cloudpickle roundtrips a Token and it still restores prior value.

        Given:
            A module-bound ContextVar and a Token from set()
        When:
            The token is pickled, unpickled, and used with reset()
        Then:
            The ContextVar should return to its pre-set value
        """
        # Arrange
        var = ContextVar("tok", default="original")
        fresh_module.tok = var
        token = var.set("mutated")

        # Act
        restored_token = _loads(_dumps(token))
        assert var.get() == "mutated"
        var.reset(restored_token)

        # Assert
        assert var.get() == "original"


class Test_snapshot_vars:
    def test_omits_vars_with_no_explicit_value(self, fresh_module):
        """Test _snapshot_vars omits vars with no explicit value.

        Given:
            A module-bound ContextVar with no value set
        When:
            _snapshot_vars() is called
        Then:
            The resulting dict should not contain that var's key
        """
        # Arrange
        var = ContextVar("snapshot_unset")
        fresh_module.snapshot_unset = var

        # Act
        result = _snapshot_vars()

        # Assert
        key = f"{fresh_module.__name__}:snapshot_unset"
        assert key not in result

    def test_emits_set_value_under_module_attr_key(self, fresh_module):
        """Test _snapshot_vars emits mod:attr keys with serialized values.

        Given:
            A module-bound ContextVar with a value set
        When:
            _snapshot_vars() is called
        Then:
            The dict should contain 'mod:attr' → cloudpickled value
        """
        # Arrange
        var = ContextVar("snap_value")
        fresh_module.snap_value = var
        var.set({"x": 1})

        # Act
        result = _snapshot_vars()

        # Assert
        key = f"{fresh_module.__name__}:snap_value"
        assert key in result
        assert cloudpickle.loads(result[key]) == {"x": 1}

    def test_reads_from_explicit_stdlib_context(self, fresh_module):
        """Test _snapshot_vars(ctx) reads values from the supplied Context.

        Given:
            A module-bound ContextVar set to value X in an explicit
            stdlib Context, and a different value Y set in the current
            context
        When:
            _snapshot_vars(explicit_ctx) is called
        Then:
            The wire map should contain X (the value from the explicit
            Context), not Y
        """
        # Arrange
        var = ContextVar("ctx_snap")
        fresh_module.ctx_snap = var
        var.set("outer")
        seeded = contextvars.copy_context()
        seeded.run(lambda: var.set("inner"))

        # Act
        result = _snapshot_vars(seeded)

        # Assert
        key = f"{fresh_module.__name__}:ctx_snap"
        assert key in result
        assert cloudpickle.loads(result[key]) == "inner"

    def test_skips_vars_absent_from_explicit_context(self, fresh_module):
        """Test _snapshot_vars(ctx) skips vars not present in the ctx.

        Given:
            A registered ContextVar whose stdlib backing has never been
            set in a freshly-created stdlib Context
        When:
            _snapshot_vars(fresh_ctx) is called
        Then:
            The result should omit that var entirely
        """
        # Arrange
        var = ContextVar("ctx_absent")
        fresh_module.ctx_absent = var
        # A copy_context captured before any set includes no entry for var._stdlib
        empty_ctx = contextvars.copy_context()

        # Act
        result = _snapshot_vars(empty_ctx)

        # Assert
        key = f"{fresh_module.__name__}:ctx_absent"
        assert key not in result

    def test_warns_and_skips_unbound_var_in_current_context(self, caplog):
        """Test _snapshot_vars omits vars not bound to any module attr.

        Given:
            A ContextVar constructed but never assigned to any module
            attribute, with a value set in the current context
        When:
            _snapshot_vars() is called with caplog capturing WARNINGs
        Then:
            The result should not contain any entry for this var and a
            WARNING should be logged naming the var
        """
        # Arrange
        import logging

        var = ContextVar("unbound_snapshot")
        # Deliberately do NOT bind to a module.
        var.set("ephemeral")
        try:
            # Act
            with caplog.at_level(logging.WARNING, logger="wool.runtime.context"):
                result = _snapshot_vars()

            # Assert — no wire entry for the unbound var
            assert all("unbound_snapshot" not in k for k in result)
            assert any("unbound_snapshot" in rec.getMessage() for rec in caplog.records)
        finally:
            # Clean up registry-level side-effects from this var.
            var._stdlib.set(_UNSET)

    def test_raises_typeerror_on_unpicklable_value(self, fresh_module):
        """Test _snapshot_vars raises TypeError naming the failed var.

        Given:
            A module-bound ContextVar whose current value is unpicklable
            (a running generator cannot be cloudpickled)
        When:
            _snapshot_vars() attempts to serialize it
        Then:
            It should raise TypeError whose message includes the var's
            name
        """

        # Arrange
        def _gen():
            yield 1

        var = ContextVar("unpicklable_snapshot")
        fresh_module.unpicklable_snapshot = var
        running = _gen()
        next(running)  # Advance so it cannot be pickled.
        var.set(running)

        # Act & assert
        with pytest.raises(TypeError, match="unpicklable_snapshot"):
            _snapshot_vars()


class Test_apply_vars:
    def test_restores_value_on_target_var(self, fresh_module):
        """Test _apply_vars restores a serialized value onto the local var.

        Given:
            A module-bound ContextVar and a snapshot dict with its key
        When:
            _apply_vars is called with the dict
        Then:
            get() on the var should return the deserialized value
        """
        # Arrange
        var = ContextVar("apply_target")
        fresh_module.apply_target = var
        snapshot = {
            f"{fresh_module.__name__}:apply_target": cloudpickle.dumps("restored")
        }

        # Act
        _apply_vars(snapshot)

        # Assert
        assert var.get() == "restored"

    def test_skips_unknown_module(self, caplog):
        """Test _apply_vars warns and skips entries for unknown modules.

        Given:
            A snapshot dict keyed by an absent module
        When:
            _apply_vars is called
        Then:
            It should log a warning and not raise
        """
        # Arrange
        snapshot = {"no_such_module:nope": cloudpickle.dumps("x")}

        # Act
        _apply_vars(snapshot)

        # Assert — no exception; no assertion needed beyond completion

    def test_empty_dict_returns_immediately(self):
        """Test _apply_vars is a no-op for an empty dict.

        Given:
            An empty vars dict
        When:
            _apply_vars({}) is called
        Then:
            It should return without raising and perform no work
        """
        # Act
        _apply_vars({})

        # Assert — completion without exception

    def test_warns_on_malformed_key(self, caplog):
        """Test _apply_vars warns on keys missing the module:attr separator.

        Given:
            A vars dict with a key that lacks a colon
        When:
            _apply_vars is called with caplog capturing WARNINGs
        Then:
            It should log a warning naming the malformed key and skip
            the entry; no exception is raised
        """
        # Arrange
        import logging

        snapshot = {"no_colon_here": cloudpickle.dumps("x")}

        # Act
        with caplog.at_level(logging.WARNING, logger="wool.runtime.context"):
            _apply_vars(snapshot)

        # Assert
        assert any("Malformed" in rec.getMessage() for rec in caplog.records)

    def test_warns_when_attr_is_not_a_context_var(self, fresh_module, caplog):
        """Test _apply_vars warns when location resolves to a non-ContextVar.

        Given:
            A module attribute that is not a wool.ContextVar (a plain int)
            and a vars dict pointing at that location
        When:
            _apply_vars is called with caplog capturing WARNINGs
        Then:
            It should log a warning and skip the entry; no exception
            is raised
        """
        # Arrange
        import logging

        fresh_module.not_a_cv = 99
        snapshot = {f"{fresh_module.__name__}:not_a_cv": cloudpickle.dumps("ignored")}

        # Act
        with caplog.at_level(logging.WARNING, logger="wool.runtime.context"):
            _apply_vars(snapshot)

        # Assert
        assert any("not a ContextVar" in rec.getMessage() for rec in caplog.records)
        assert fresh_module.not_a_cv == 99  # unchanged


class TestContext:
    def test___new___direct_instantiation_rejected(self):
        """Test Context.__new__ forbids direct construction.

        Given:
            The Context class
        When:
            A caller attempts Context()
        Then:
            It should raise TypeError
        """
        # Act & assert
        with pytest.raises(TypeError):
            Context()

    def test_run_seeds_vars_and_scopes_mutations(self, fresh_module):
        """Test Context.run seeds vars and scopes inner mutations.

        Given:
            A Context captured with a ContextVar set to a known value
        When:
            Context.run mutates the var inside the callable
        Then:
            The mutation should be observable from within the callable,
            and the Context's own snapshot should reflect the mutation
        """
        # Arrange
        var = ContextVar("run_seed", default="before")
        fresh_module.run_seed = var
        token = var.set("outer")
        ctx = current_context()
        var.reset(token)

        # Act
        def body():
            assert var.get() == "outer"
            var.set("inner")
            return var.get()

        result = ctx.run(body)

        # Assert
        assert result == "inner"
        assert ctx[var] == "inner"

    def test_run_concurrent_entry_raises(self, fresh_module):
        """Test Context.run rejects re-entry while already active.

        Given:
            A Context whose run() body tries to invoke ctx.run again
        When:
            The inner ctx.run fires
        Then:
            It should raise RuntimeError
        """
        # Arrange
        var = ContextVar("reentry", default=0)
        fresh_module.reentry = var
        var.set(1)
        ctx = current_context()

        def inner():
            return "never"

        def outer():
            ctx.run(inner)

        # Act & assert
        with pytest.raises(RuntimeError):
            ctx.run(outer)

    @pytest.mark.asyncio
    async def test_run_async_seeds_vars(self, fresh_module):
        """Test Context.run_async seeds vars into the task's context.

        Given:
            A Context captured with a ContextVar set to a known value
        When:
            Context.run_async runs a coroutine that reads the var
        Then:
            The coroutine should observe the seeded value
        """
        # Arrange
        var = ContextVar("async_seed", default="before")
        fresh_module.async_seed = var
        token = var.set("outer")
        ctx = current_context()
        var.reset(token)

        async def coro():
            return var.get()

        # Act
        result = await ctx.run_async(coro())

        # Assert
        assert result == "outer"

    @pytest.mark.asyncio
    async def test_run_async_concurrent_entry_raises(self, fresh_module):
        """Test Context.run_async rejects concurrent entry.

        Given:
            A Context whose run_async body awaits a second run_async on itself
        When:
            The inner run_async fires
        Then:
            It should raise RuntimeError
        """
        # Arrange
        var = ContextVar("async_reentry", default=0)
        fresh_module.async_reentry = var
        var.set(1)
        ctx = current_context()

        async def inner():
            return "never"

        async def outer():
            await ctx.run_async(inner())

        # Act & assert
        with pytest.raises(RuntimeError):
            await ctx.run_async(outer())

    def test_pickle_roundtrip_preserves_id_and_vars(self, fresh_module):
        """Test Context pickles preserving lineage id and var values.

        Given:
            A Context captured with a module-bound ContextVar value
        When:
            It is pickled and unpickled via cloudpickle
        Then:
            The restored Context should equal by id and expose the same vars
        """
        # Arrange
        var = ContextVar("ctx_pickle")
        fresh_module.ctx_pickle = var
        var.set("captured")
        ctx = current_context()

        # Act
        restored = _loads(_dumps(ctx))

        # Assert
        assert restored.id == ctx.id
        assert restored[var] == "captured"

    def test_iter_yields_captured_vars(self, fresh_module):
        """Test iterating a Context yields the captured ContextVar keys.

        Given:
            A Context captured with one ContextVar set to a value
        When:
            iter(ctx) is consumed
        Then:
            The captured ContextVar should appear in the iteration
        """
        # Arrange
        var = ContextVar("iter_var")
        fresh_module.iter_var = var
        var.set("value")
        ctx = current_context()

        # Act
        captured = list(iter(ctx))

        # Assert
        assert var in captured

    def test_getitem_returns_captured_value(self, fresh_module):
        """Test ctx[var] returns the captured value for that var.

        Given:
            A Context captured with a ContextVar set to a known value
        When:
            ctx[var] is accessed
        Then:
            It should return the captured value
        """
        # Arrange
        var = ContextVar("item_var")
        fresh_module.item_var = var
        var.set(123)
        ctx = current_context()

        # Act & assert
        assert ctx[var] == 123

    def test_contains_reports_captured_membership(self, fresh_module):
        """Test `var in ctx` returns True only for captured vars.

        Given:
            A Context captured with one ContextVar set, and a separate
            ContextVar that was never set
        When:
            The `in` operator is evaluated for each
        Then:
            The captured var reports True; the unset var reports False
        """
        # Arrange
        captured_var = ContextVar("contains_captured")
        other_var = ContextVar("contains_other")
        fresh_module.contains_captured = captured_var
        fresh_module.contains_other = other_var
        captured_var.set("x")
        ctx = current_context()

        # Act & assert
        assert captured_var in ctx
        assert other_var not in ctx

    def test_len_returns_captured_count(self, fresh_module):
        """Test len(ctx) matches the number of captured vars.

        Given:
            A Context captured with two ContextVars each set to a value
        When:
            len(ctx) is called
        Then:
            It should report at least 2 (and more if other tests have
            left registered vars with set values)
        """
        # Arrange
        a = ContextVar("len_a")
        b = ContextVar("len_b")
        fresh_module.len_a = a
        fresh_module.len_b = b
        a.set(1)
        b.set(2)
        ctx = current_context()

        # Act & assert
        assert len(ctx) >= 2

    def test_keys_values_items_expose_captured_pairs(self, fresh_module):
        """Test keys()/values()/items() expose the captured var→value mapping.

        Given:
            A Context captured with a ContextVar set to a known value
        When:
            keys(), values(), items() are consumed
        Then:
            All three views should include the captured var/value
        """
        # Arrange
        var = ContextVar("views_var")
        fresh_module.views_var = var
        var.set("shared")
        ctx = current_context()

        # Act
        keys = list(ctx.keys())
        values = list(ctx.values())
        items = list(ctx.items())

        # Assert
        assert var in keys
        assert "shared" in values
        assert (var, "shared") in items

    def test_repr_includes_lineage_id_and_var_count(self, fresh_module):
        """Test repr(ctx) exposes the lineage UUID and var count.

        Given:
            A Context captured with at least one var set
        When:
            repr(ctx) is evaluated
        Then:
            The string should contain the lineage UUID and the var count
        """
        # Arrange
        var = ContextVar("repr_ctx_var")
        fresh_module.repr_ctx_var = var
        var.set("v")
        ctx = current_context()

        # Act
        result = repr(ctx)

        # Assert
        assert str(ctx.id) in result
        assert f"vars={len(ctx)}" in result

    def test_run_snapshot_omits_vars_left_unset(self, fresh_module):
        """Test post-run snapshot excludes vars whose final value is _UNSET.

        Given:
            A Context captured with two vars set; inside Context.run, one
            var is "unset" (its backing is overwritten with _UNSET via
            the stdlib contextvar)
        When:
            Context.run returns
        Then:
            The Context's _vars (observed via the public mapping
            protocol) should no longer contain the unset var, while the
            other var remains
        """
        # Arrange
        keep_var = ContextVar("keep_var")
        drop_var = ContextVar("drop_var")
        fresh_module.keep_var = keep_var
        fresh_module.drop_var = drop_var
        keep_var.set("kept")
        drop_var.set("will_drop")
        ctx = current_context()

        def body():
            # Simulate a var becoming unset within the run
            drop_var._stdlib.set(_UNSET)
            return None

        # Act
        ctx.run(body)

        # Assert — the post-run snapshot excludes drop_var
        assert keep_var in ctx
        assert drop_var not in ctx

    def test_run_snapshot_skips_registered_vars_not_touched_in_context(
        self, fresh_module
    ):
        """Test post-run snapshot skips registered vars absent from the run ctx.

        Given:
            Two registered ContextVars — one set before capturing the
            Context, the other never set at all
        When:
            Context.run completes and the post-run snapshot runs
        Then:
            The never-set var should not appear in the Context; the set
            var should
        """
        # Arrange
        set_var = ContextVar("absent_ctx_set")
        untouched_var = ContextVar("absent_ctx_untouched")
        fresh_module.absent_ctx_set = set_var
        fresh_module.absent_ctx_untouched = untouched_var
        set_var.set("present")
        ctx = current_context()

        # Act
        ctx.run(lambda: None)

        # Assert
        assert set_var in ctx
        assert untouched_var not in ctx


class Test_current_context:
    def test_captures_set_vars_with_lineage(self, fresh_module):
        """Test current_context returns a Context with the current lineage and vars.

        Given:
            A module-bound ContextVar with a value set in the current context
        When:
            current_context() is called
        Then:
            The returned Context should contain the var with its value
            and carry a non-nil lineage UUID
        """
        # Arrange
        var = ContextVar("cur")
        fresh_module.cur = var
        var.set("value")

        # Act
        ctx = current_context()

        # Assert
        assert ctx[var] == "value"
        assert ctx.id.int != 0


class TestDispatchTimeoutContextVar:
    def test_default_is_none(self):
        """Test dispatch_timeout has no default value assigned.

        Given:
            The dispatch_timeout contextvar
        When:
            Read via .get() in a fresh context
        Then:
            It should return None
        """
        # Act & assert
        assert dispatch_timeout.get() is None

    def test_set_and_reset_restores_default(self):
        """Test dispatch_timeout.set is reversible via reset.

        Given:
            dispatch_timeout set to a float
        When:
            The token is reset
        Then:
            .get() should return None again
        """
        # Arrange
        token = dispatch_timeout.set(5.0)

        # Act
        dispatch_timeout.reset(token)

        # Assert
        assert dispatch_timeout.get() is None


class TestPublicTypeShape:
    def test_token_repr_includes_var_name(self, fresh_module):
        """Test Token repr surfaces the underlying var name.

        Given:
            A Token obtained from a ContextVar.set call
        When:
            repr() is called on it
        Then:
            The string should include the var's name
        """
        # Arrange
        var = ContextVar("repr_var")
        fresh_module.repr_var = var
        token = var.set(1)

        # Act
        result = repr(token)

        # Assert
        assert "repr_var" in result
        assert isinstance(token, Token)

    def test_contextvar_repr_includes_name(self):
        """Test ContextVar repr includes its name.

        Given:
            A ContextVar instance
        When:
            repr() is called on it
        Then:
            The string should include the var's name
        """
        # Arrange, act
        var = ContextVar("repr_form")

        # Assert
        assert "repr_form" in repr(var)


@pytest.mark.asyncio
async def test_await_is_continuous_mutations_leak_to_caller(fresh_module):
    """Test plain await on a coroutine mutating a var leaks to the caller.

    Given:
        A module-bound ContextVar and a coroutine that mutates it
    When:
        The coroutine is awaited directly (no create_task)
    Then:
        The caller should observe the mutation after the await
    """
    # Arrange
    var = ContextVar("continuity", default="before")
    fresh_module.continuity = var

    async def coro():
        var.set("after")

    # Act
    await coro()

    # Assert
    assert var.get() == "after"


@pytest.mark.asyncio
async def test_create_task_forks_context_isolating_mutations(fresh_module):
    """Test asyncio.create_task forks the context, isolating mutations.

    Given:
        A module-bound ContextVar and a coroutine that mutates it
    When:
        The coroutine is wrapped in asyncio.create_task
    Then:
        The caller's context should not observe the mutation
    """
    # Arrange
    var = ContextVar("forked", default="before")
    fresh_module.forked = var

    async def coro():
        var.set("child_only")

    # Act
    await asyncio.create_task(coro())

    # Assert
    assert var.get() == "before"


@pytest.mark.asyncio
async def test_create_task_with_explicit_context_writes_to_context(fresh_module):
    """Test asyncio.create_task with explicit context lands mutations there.

    Given:
        A stdlib Context and a coroutine that mutates a module-bound var
    When:
        The coroutine is scheduled with context=<explicit>
    Then:
        The explicit context should contain the mutation post-run
    """
    # Arrange
    var = ContextVar("explicit", default="before")
    fresh_module.explicit = var
    ctx = contextvars.copy_context()

    async def coro():
        var.set("from_task")

    # Act
    await asyncio.create_task(coro(), context=ctx)

    # Assert — caller's current context is unchanged
    assert var.get() == "before"
    # and ctx holds the mutation
    assert ctx[var._stdlib] == "from_task"
