"""Unit tests for the namespace module's isolation primitives.

Covers the building blocks — _IsolatedGlobals, clone_module, and the
activate() context manager — that underpin per-task module
substitution for wool.ContextVar propagation.
"""

from __future__ import annotations

import sys
import types
import uuid

import pytest

from wool.runtime.context import ContextVar
from wool.runtime.worker import namespace
from wool.runtime.worker.namespace import _IsolatedGlobals
from wool.runtime.worker.namespace import _lineage_cache
from wool.runtime.worker.namespace import clone_module
from wool.runtime.worker.namespace import current_lineage  # noqa: F401


@pytest.fixture(autouse=True)
def _reset_lineage_state():
    """Clear lineage cache and contextvar state between tests."""
    yield
    _lineage_cache.clear()
    # Reset _active_lineage is context-scoped; outside any activate()
    # the default applies, so no explicit reset is necessary.


class TestIsolatedGlobals:
    def test_missing_falls_through_to_base(self):
        """Test read of absent key delegates to the base dict.

        Given:
            An _IsolatedGlobals wrapping a base dict that contains a key
        When:
            The key is read on the overlay (which doesn't have it
            locally)
        Then:
            The read should return the base dict's value
        """
        # Arrange
        base = {"outer_key": 42}
        overlay = _IsolatedGlobals(base)

        # Act & assert
        assert overlay["outer_key"] == 42

    def test_writes_land_locally(self):
        """Test write to overlay does not mutate the base dict.

        Given:
            An _IsolatedGlobals wrapping a base dict
        When:
            A key is written via overlay assignment
        Then:
            The base dict should be unchanged and the overlay should
            hold the new value locally.
        """
        # Arrange
        base = {"x": 1}
        overlay = _IsolatedGlobals(base)

        # Act
        overlay["x"] = 99

        # Assert
        assert overlay["x"] == 99
        assert base["x"] == 1

    def test_local_write_takes_precedence_over_base(self):
        """Test local entries take precedence over identical keys in base.

        Given:
            An _IsolatedGlobals with the same key set both locally and
            in the base
        When:
            The key is read
        Then:
            The overlay's local value should be returned (base is
            consulted only on __missing__).
        """
        # Arrange
        base = {"shared": "from_base"}
        overlay = _IsolatedGlobals(base)
        overlay["shared"] = "from_overlay"

        # Act & assert
        assert overlay["shared"] == "from_overlay"


class TestCloneModule:
    def test_clone_produces_distinct_module_instance(self):
        """Test clone_module returns a fresh ModuleType with same name.

        Given:
            A module with an attribute
        When:
            clone_module is called
        Then:
            The clone should be a distinct Python object with the same
            __name__ and the same top-level attribute values.
        """
        # Arrange
        original = types.ModuleType("cl_mod_distinct")
        original.CONST = 123

        # Act
        clone = clone_module(original)

        # Assert
        assert clone is not original
        assert clone.__name__ == "cl_mod_distinct"
        assert clone.CONST == 123

    def test_clone_rebinds_locally_defined_functions(self):
        """Test functions defined in the module get clone-scoped globals.

        Given:
            A module containing a function whose __globals__ points at
            the module's __dict__
        When:
            clone_module is called
        Then:
            The cloned module's function should be a distinct object
            whose __globals__ references the clone's __dict__, so that
            LOAD_GLOBAL inside the function resolves against the
            clone.
        """
        # Arrange
        original = types.ModuleType("cl_mod_rebind")
        original.TAG = "original_tag"

        def helper():
            return TAG  # noqa: F821 — resolved via LOAD_GLOBAL

        # Rebind helper's globals to the original module so the clone
        # rebinding guard (value.__globals__ is original.__dict__)
        # fires.
        helper = types.FunctionType(helper.__code__, original.__dict__, "helper")
        original.helper = helper

        # Act
        clone = clone_module(original)

        # Assert
        assert clone.helper is not helper
        assert clone.helper.__globals__ is clone.__dict__
        # Prove read-after-substitution works: swap TAG in clone, the
        # clone's helper sees the new value while the original's
        # helper still sees the old.
        clone.TAG = "clone_tag"
        assert clone.helper() == "clone_tag"
        assert original.helper() == "original_tag"

    def test_clone_preserves_re_exported_functions_verbatim(self):
        """Test functions re-exported from other modules are not rebound.

        Given:
            A module re-exporting a function from a different module
            (i.e., `value.__globals__ is not original.__dict__`)
        When:
            clone_module is called
        Then:
            The re-exported function should appear in the clone as the
            same Python object — the rebinding guard skips it so it
            continues to resolve its globals against its own defining
            module.
        """
        # Arrange
        source = types.ModuleType("cl_mod_source_of_fn")
        source.MARKER = "source_marker"

        def source_helper():
            return MARKER  # noqa: F821

        source_helper = types.FunctionType(
            source_helper.__code__, source.__dict__, "source_helper"
        )
        source.helper = source_helper

        consumer = types.ModuleType("cl_mod_consumer")
        consumer.helper = source_helper  # re-export by reference

        # Act
        clone = clone_module(consumer)

        # Assert
        assert clone.helper is source_helper
        assert clone.helper.__globals__ is source.__dict__


class TestActivate:
    def test_activate_binds_lineage_for_current_lineage_calls(self):
        """Test activate makes current_lineage return the adopted id.

        Given:
            A lineage UUID and an empty manifest
        When:
            activate() is entered
        Then:
            current_lineage() should return the adopted UUID inside
            the context and return a different (process-default or
            freshly-minted) UUID after exit.
        """
        # Arrange
        lineage_id = uuid.uuid4()

        # Act
        with namespace.activate(lineage_id, {}, drop_on_exit=True):
            inside = namespace.current_lineage()

        # Assert
        assert inside == lineage_id

    def test_activate_substitutes_manifest_entries_into_sys_modules(self):
        """Test manifest substitution targets live sys.modules entries.

        Given:
            A manifested (module_name, attr_name) → ContextVar binding
            where the module is registered in sys.modules
        When:
            activate() is entered with that manifest
        Then:
            The real sys.modules entry's attribute should point to the
            manifested instance for the duration of the context.
        """
        # Arrange
        mod = types.ModuleType("_test_activate_substitute")
        mod.slot = "placeholder"
        sys.modules[mod.__name__] = mod
        wire_var = ContextVar("cv_activate_substitute")
        manifest = {(mod.__name__, "slot"): wire_var}
        lineage_id = uuid.uuid4()

        try:
            # Act & assert
            with namespace.activate(lineage_id, manifest, drop_on_exit=True):
                assert mod.slot is wire_var
        finally:
            del sys.modules[mod.__name__]

    def test_activate_skips_missing_target_modules_without_raising(self):
        """Test manifest entries for absent modules are tolerated.

        Given:
            A manifest referencing a module name that is not in
            sys.modules
        When:
            activate() is entered
        Then:
            No exception should be raised; the entry is silently
            skipped.
        """
        # Arrange
        wire_var = ContextVar("cv_activate_missing_mod")
        manifest = {("_test_activate_ghost_module", "slot"): wire_var}
        lineage_id = uuid.uuid4()

        # Act — should not raise
        with namespace.activate(lineage_id, manifest, drop_on_exit=True):
            pass

    def test_activate_drop_on_exit_removes_lineage_cache_entry(self):
        """Test drop_on_exit evicts the lineage cache when the context exits.

        Given:
            activate() called with drop_on_exit=True and a fresh
            lineage ID
        When:
            The context manager exits
        Then:
            The lineage cache should no longer contain an entry for
            that lineage.
        """
        # Arrange
        lineage_id = uuid.uuid4()

        # Act
        with namespace.activate(lineage_id, {}, drop_on_exit=True):
            assert lineage_id in _lineage_cache

        # Assert
        assert lineage_id not in _lineage_cache
