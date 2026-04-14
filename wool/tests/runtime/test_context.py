import asyncio
import contextvars
import sys
import types
from uuid import uuid4

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

    def test___repr___matches_stdlib_token_missing_format(self):
        """Test _UnsetType repr matches stdlib Token.MISSING format.

        Given:
            The _UNSET singleton
        When:
            repr() is called on it
        Then:
            It should return the stdlib-parity string '<Token.MISSING>'
        """
        # Act & assert
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

    def test_pickling_unbound_var_mints_uuid4_and_roundtrips(self):
        """Test pickling an unbound ContextVar succeeds via lazy UUID4.

        Given:
            A ContextVar never assigned to any module attribute
        When:
            It is pickled and unpickled via _dumps / _loads
        Then:
            The unpickled instance should be identical to the original
            (via the UUID4 registry)
        """
        # Arrange
        var = ContextVar("ephemeral")

        # Act
        restored = _loads(_dumps(var))

        # Assert — same instance, identity preserved via UUID4 registry
        assert restored is var

    def test_resolve_ids_is_deterministic_across_calls(self, fresh_module):
        """Test _resolve_ids returns a stable id list for a bound var.

        Given:
            A module-bound ContextVar
        When:
            _resolve_ids is called multiple times with no module changes
        Then:
            The returned lists should be equal in order and content
        """
        # Arrange
        var = ContextVar("stable")
        fresh_module.stable = var

        # Act
        first = var._resolve_ids()
        second = var._resolve_ids()

        # Assert
        assert first == second
        assert all(uid.version == 5 for uid in first)

    def test_class_attribute_contextvar_pickles_via_uuid5(self, fresh_module):
        """Test a ContextVar bound inside a class body pickles via UUID5.

        Given:
            A ContextVar bound as a class-level attribute
        When:
            The var is pickled and unpickled
        Then:
            The round-trip should return the same instance, resolved
            via UUID5 of the class-scoped location
        """

        # Arrange
        class Holder:
            pass

        var = ContextVar("class_bound")
        Holder.trace = var
        fresh_module.Holder = Holder

        # Act
        restored = _loads(_dumps(var))

        # Assert
        assert restored is var
        # Verify the id list includes a UUID5 for the class-scoped path
        ids = var._resolve_ids()
        assert any(uid.version == 5 for uid in ids)

    def test_divergence_raises_context_var_identity_error(self, fresh_module):
        """Test multi-instance resolution raises ContextVarIdentityError.

        Given:
            Two distinct ContextVar instances registered under two
            UUIDs that the reducer would ship together (simulating
            worker-side drift where caller's aliases resolve to
            different worker-side instances)
        When:
            The synthesized pickle is loaded
        Then:
            _reconstruct should raise ContextVarIdentityError
        """
        # Arrange — produce two distinct instances with known UUID4s
        from wool.runtime.context import ContextVarIdentityError

        a = ContextVar("divergent_a")
        b = ContextVar("divergent_b")
        # Force each to mint a UUID4 via _resolve_ids (no module binding)
        a_ids = a._resolve_ids()
        b_ids = b._resolve_ids()
        mixed_ids = [a_ids[0], b_ids[0]]

        # Act & assert
        from wool.runtime.context import _reconstruct

        with pytest.raises(ContextVarIdentityError):
            _reconstruct(mixed_ids, "mixed", False, None)

    def test_reconstruct_constructs_fresh_when_nothing_resolves(self):
        """Test _reconstruct creates a fresh instance for unknown UUIDs.

        Given:
            A list of UUIDs that are not in the registry and not
            resolvable via sys.modules walk (fresh UUID4s simulate a
            worker receiving a caller-minted unbound var)
        When:
            _reconstruct is invoked
        Then:
            It should construct and register a fresh ContextVar
            instance, returning it
        """
        # Arrange
        from wool.runtime.context import _reconstruct

        fresh_ids = [uuid4(), uuid4()]

        # Act
        restored = _reconstruct(fresh_ids, "fresh_var", True, "fallback")

        # Assert
        assert isinstance(restored, ContextVar)
        assert restored.name == "fresh_var"
        assert restored.get() == "fallback"
        # Registered under all ids
        for uid in fresh_ids:
            assert ContextVar._uuid_registry.get(uid) is restored

    def test_reconstruct_resolves_via_sys_modules_walk(self, fresh_module):
        """Test _reconstruct falls back to sys.modules UUID5 match.

        Given:
            A bound ContextVar whose UUID5 is NOT in the registry yet
            (simulating receiver first-touch)
        When:
            _reconstruct is called with the var's UUID5
        Then:
            It should walk sys.modules, compute UUID5 for each bound
            ContextVar, find the match, and return that instance
        """
        # Arrange
        from wool.runtime.context import _reconstruct

        var = ContextVar("walk_target")
        fresh_module.walk_target = var
        # Compute what the UUID5 would be, but DON'T call _resolve_ids
        # (which would populate the registry).
        from uuid import uuid5

        from wool.runtime.context import _NAMESPACE_UUID

        target_uuid = uuid5(_NAMESPACE_UUID, f"{fresh_module.__name__}:walk_target")
        # Ensure not in registry
        ContextVar._uuid_registry.pop(target_uuid, None)

        # Act
        restored = _reconstruct([target_uuid], "walk_target", False, None)

        # Assert
        assert restored is var

    def test_resolve_ids_walk_tolerates_none_and_non_dict_modules(self, fresh_module):
        """Test _resolve_ids silently skips bad sys.modules entries.

        Given:
            A bound ContextVar and sys.modules containing both a None
            entry and an entry whose __dict__ access raises AttributeError
        When:
            _resolve_ids walks sys.modules
        Then:
            The pathological entries are skipped; the legitimate
            binding is still discovered
        """

        class _NoDict:
            def __getattribute__(self, item):
                if item == "__dict__":
                    raise AttributeError("no dict")
                return super().__getattribute__(item)

        # Arrange
        var = ContextVar("walk_guards")
        fresh_module.walk_guards = var
        sys.modules["__probe_none__"] = None  # type: ignore[assignment]
        sys.modules["__probe_nodict__"] = _NoDict()  # type: ignore[assignment]
        try:
            # Act
            ids = var._resolve_ids()

            # Assert — a UUID5 for the legit binding was produced
            assert any(uid.version == 5 for uid in ids)
        finally:
            sys.modules.pop("__probe_none__", None)
            sys.modules.pop("__probe_nodict__", None)

    def test_find_var_by_uuid_returns_none_for_uuid4(self):
        """Test the UUID5 matcher skips UUID4 inputs.

        Given:
            A random UUID4 (no location can produce it)
        When:
            _find_var_by_uuid is called with the UUID4
        Then:
            It should return None without walking sys.modules
        """
        # Arrange
        from wool.runtime.context import _find_var_by_uuid

        # Act
        result = _find_var_by_uuid(uuid4())

        # Assert
        assert result is None

    def test_find_var_by_uuid_returns_none_for_unknown_uuid5(self):
        """Test the UUID5 matcher returns None for unresolvable UUIDs.

        Given:
            A UUID5 computed for a location that doesn't exist in
            sys.modules
        When:
            _find_var_by_uuid is called
        Then:
            It should walk sys.modules without finding a match and
            return None
        """
        # Arrange
        from uuid import uuid5

        from wool.runtime.context import _NAMESPACE_UUID
        from wool.runtime.context import _find_var_by_uuid

        ghost = uuid5(_NAMESPACE_UUID, "no.such.module:ghost")

        # Act
        result = _find_var_by_uuid(ghost)

        # Assert
        assert result is None

    def test_find_var_by_uuid_traverses_class_bodies(self, fresh_module):
        """Test the UUID5 matcher recurses into class bodies.

        Given:
            A ContextVar bound as a class attribute and a UUID5
            computed from its class-scoped location
        When:
            _find_var_by_uuid is invoked with that UUID5
        Then:
            It should recursively descend into the class body and
            return the matching instance
        """

        # Arrange
        class Outer:
            class Inner:
                pass

        var = ContextVar("class_walked")
        Outer.Inner.leaf = var
        fresh_module.Outer = Outer

        from uuid import uuid5

        from wool.runtime.context import _NAMESPACE_UUID
        from wool.runtime.context import _find_var_by_uuid

        target = uuid5(_NAMESPACE_UUID, f"{fresh_module.__name__}:Outer.Inner.leaf")
        # Ensure registry miss
        ContextVar._uuid_registry.pop(target, None)

        # Act
        result = _find_var_by_uuid(target)

        # Assert
        assert result is var

    def test_find_var_by_uuid_breaks_self_referential_class_cycles(self, fresh_module):
        """Test the receiver-side walker breaks cycles in class graphs.

        Given:
            A class that references itself as a class attribute
            (creating a cycle in the class walk)
        When:
            _find_var_by_uuid searches for an unresolvable UUID5
        Then:
            The walk should terminate without stack overflow via the
            visited-set guard
        """

        # Arrange
        class Cyclic:
            pass

        Cyclic.myself = Cyclic  # type: ignore[attr-defined]
        fresh_module.Cyclic = Cyclic

        from uuid import uuid5

        from wool.runtime.context import _NAMESPACE_UUID
        from wool.runtime.context import _find_var_by_uuid

        ghost = uuid5(_NAMESPACE_UUID, "no.match:anywhere")

        # Act
        result = _find_var_by_uuid(ghost)

        # Assert — walk completed without recursion blowup
        assert result is None

    def test_find_var_by_uuid_tolerates_none_and_non_dict_modules(self):
        """Test the UUID5 matcher tolerates bad sys.modules entries.

        Given:
            sys.modules containing a None entry and a non-dict-module
            entry, plus an unresolvable UUID5 target
        When:
            _find_var_by_uuid is called
        Then:
            It should complete the walk without raising and return None
        """

        class _NoDict:
            def __getattribute__(self, item):
                if item == "__dict__":
                    raise AttributeError
                return super().__getattribute__(item)

        # Arrange
        from uuid import uuid5

        from wool.runtime.context import _NAMESPACE_UUID
        from wool.runtime.context import _find_var_by_uuid

        ghost = uuid5(_NAMESPACE_UUID, "still.absent:ghost")
        sys.modules["__probe_walk_none__"] = None  # type: ignore[assignment]
        sys.modules["__probe_walk_nodict__"] = _NoDict()  # type: ignore[assignment]
        try:
            # Act
            result = _find_var_by_uuid(ghost)

            # Assert
            assert result is None
        finally:
            sys.modules.pop("__probe_walk_none__", None)
            sys.modules.pop("__probe_walk_nodict__", None)

    @pytest.mark.asyncio
    async def test_token_reset_rejects_cross_lineage_usage(self, fresh_module):
        """Test Token.reset raises when used outside its creating lineage.

        Given:
            A Token created in one wool.Context lineage
        When:
            reset() is attempted from a wool.Context.run_async with a
            different lineage (async path adopts lineage via stdlib
            contextvar sentinel in the new task)
        Then:
            It should raise ValueError naming the lineage mismatch
        """
        # Arrange
        var = ContextVar("cross_lineage")
        fresh_module.cross_lineage = var
        token = var.set("outer")
        other_ctx = Context()  # fresh lineage

        async def body():
            with pytest.raises(ValueError, match="different wool.Context lineage"):
                var.reset(token)

        # Act
        await other_ctx.run_async(body())

    def test_token_var_and_old_value_properties(self, fresh_module):
        """Test Token exposes var and old_value as read-only properties.

        Given:
            A ContextVar with a prior value, then mutated via set()
        When:
            The returned Token's var and old_value properties are read
        Then:
            var returns the ContextVar instance; old_value returns the
            previously-set value
        """
        # Arrange
        var = ContextVar("prop_check", default="default")
        fresh_module.prop_check = var
        var.set("first")
        token = var.set("second")
        try:
            # Act & assert
            assert token.var is var
            assert token.old_value == "first"
        finally:
            var._stdlib.set(_UNSET)

    def test_reconstruct_enriches_registry_with_unresolved_ids(self, fresh_module):
        """Test reconstruction registers unknown ids under the resolved instance.

        Given:
            A bound ContextVar known to the registry under one UUID
            and a wire id list that includes an additional unknown UUID
        When:
            _reconstruct is invoked with both ids
        Then:
            The resolved instance should be returned, and the unknown
            UUID should become registered under that instance
        """
        # Arrange
        from wool.runtime.context import _reconstruct

        var = ContextVar("enrich_target")
        fresh_module.enrich_target = var
        var._resolve_ids()  # populate registry
        ids = list(var._ids or [])
        ghost_id = uuid4()
        combined_ids = [*ids, ghost_id]

        # Act
        resolved = _reconstruct(combined_ids, "enrich_target", False, None)

        # Assert
        assert resolved is var
        assert ContextVar._uuid_registry.get(ghost_id) is var


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

    def test_emits_set_value_under_uuid_key(self, fresh_module):
        """Test _snapshot_vars emits UUID-hex keys with serialized values.

        Given:
            A module-bound ContextVar with a value set
        When:
            _snapshot_vars() is called
        Then:
            The dict should contain the var's canonical UUID hex key
            mapped to the cloudpickled value
        """
        # Arrange
        var = ContextVar("snap_value")
        fresh_module.snap_value = var
        var.set({"x": 1})

        # Act
        result = _snapshot_vars()

        # Assert
        ids = var._resolve_ids()
        key = ids[0].hex
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
        ids = var._resolve_ids()
        key = ids[0].hex
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
        ids = var._resolve_ids()
        key = ids[0].hex
        assert key not in result

    def test_unbound_var_emitted_under_uuid4(self):
        """Test _snapshot_vars emits unbound vars under their lazy UUID4.

        Given:
            A ContextVar never assigned to any module with a value set
        When:
            _snapshot_vars() is called
        Then:
            The result should include an entry under the var's UUID4
            (minted lazily during id resolution)
        """
        # Arrange
        var = ContextVar("unbound_emitted")
        var.set("ephemeral")
        try:
            # Act
            result = _snapshot_vars()

            # Assert
            ids = var._resolve_ids()
            assert any(uid.version == 4 for uid in ids)
            uuid4_key = next(uid.hex for uid in ids if uid.version == 4)
            assert uuid4_key in result
            assert cloudpickle.loads(result[uuid4_key]) == "ephemeral"
        finally:
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
            A module-bound ContextVar and a snapshot dict keyed by the
            var's canonical UUID hex
        When:
            _apply_vars is called with the dict
        Then:
            get() on the var should return the deserialized value
        """
        # Arrange
        var = ContextVar("apply_target")
        fresh_module.apply_target = var
        ids = var._resolve_ids()
        snapshot = {ids[0].hex: cloudpickle.dumps("restored")}

        # Act
        _apply_vars(snapshot)

        # Assert
        assert var.get() == "restored"

    def test_skips_unknown_uuid(self, caplog):
        """Test _apply_vars warns and skips entries for unknown UUIDs.

        Given:
            A snapshot dict keyed by a UUID not in the registry
        When:
            _apply_vars is called
        Then:
            It should log a warning and not raise
        """
        # Arrange
        import logging

        snapshot = {uuid4().hex: cloudpickle.dumps("x")}

        # Act
        with caplog.at_level(logging.WARNING, logger="wool.runtime.context"):
            _apply_vars(snapshot)

        # Assert
        assert any("not resolvable" in rec.getMessage() for rec in caplog.records)

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
        """Test _apply_vars warns on keys that aren't valid UUID hex.

        Given:
            A vars dict with a key that isn't a valid UUID
        When:
            _apply_vars is called with caplog capturing WARNINGs
        Then:
            It should log a warning naming the malformed key and skip
            the entry; no exception is raised
        """
        # Arrange
        import logging

        snapshot = {"not-a-valid-uuid": cloudpickle.dumps("x")}

        # Act
        with caplog.at_level(logging.WARNING, logger="wool.runtime.context"):
            _apply_vars(snapshot)

        # Assert
        assert any("Malformed" in rec.getMessage() for rec in caplog.records)


class TestContext:
    def test___new___allows_direct_instantiation(self):
        """Test Context() creates an empty context with a fresh lineage UUID.

        Given:
            The Context class
        When:
            Context() is called directly
        Then:
            It should return an empty Context with a freshly-minted
            lineage UUID; two direct constructions produce distinct
            lineages
        """
        # Act
        ctx_a = Context()
        ctx_b = Context()

        # Assert
        assert len(ctx_a) == 0
        assert ctx_a.id != ctx_b.id

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
