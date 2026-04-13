"""Per-task module isolation for worker execution.

A task running on a worker needs its own view of any module that
contains a :class:`wool.ContextVar` so that wire-shipped context
values land in the task's view of the library, not the shared
worker-level module dict. This module provides the machinery.

Pieces:

- :class:`_IsolatedGlobals` — a ``dict`` subclass with read-through
  to a base globals dict. Used to wrap a routine's ``__globals__`` so
  the routine's own writes don't contaminate the worker-level module.
- :func:`clone_module` — shallow-copy a module's ``__dict__`` and
  rebind every function defined in the module to use the clone's
  dict as its ``__globals__``. Helpers in the clone then see the
  clone's dict on every ``LOAD_GLOBAL``.
- :class:`TaskLoader` + :class:`TaskMetaPathFinder` — an import-system
  integration that, when a task is active, routes imports through a
  lineage-scoped loader. The loader consults a lineage cache: first
  dispatch in a lineage populates it, subsequent dispatches reuse.
- :func:`activate` — context manager used on each dispatch to set
  the active lineage, apply the wire-shipped manifest, and tear down
  cleanly on exit.

Note: the underlying dict-subclass mechanism depends on CPython's
``STORE_GLOBAL`` and ``LOAD_GLOBAL`` honoring dict subclass behavior.
``STORE_GLOBAL`` uses ``PyDict_SetItem`` at the C level, which writes
to the dict's actual hash table (not via Python-level ``__setitem__``).
``LOAD_GLOBAL`` uses ``PyDict_GetItem``; if the key is absent from
the hash table, ``__missing__`` is called. This is why
:class:`_IsolatedGlobals` must subclass ``dict`` directly and rely on
``__missing__`` for the read-through path.
"""

from __future__ import annotations

import contextvars
import importlib.abc
import importlib.machinery
import importlib.util
import logging
import sys
import types
import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterator

if TYPE_CHECKING:
    from wool.runtime.context import ContextVar


_log = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Routine globals overlay
# ----------------------------------------------------------------------


class _IsolatedGlobals(dict):
    """Dict-subclass overlay with read-through to a base globals dict.

    Python's ``LOAD_GLOBAL`` bytecode looks a name up via
    ``PyDict_GetItem`` on the function's ``__globals__``. For a dict
    subclass, ``PyDict_GetItem`` checks the subclass's own hash table;
    on miss it calls ``__missing__``. ``STORE_GLOBAL`` uses
    ``PyDict_SetItem`` which writes to the subclass's hash table —
    these writes do not propagate to the base dict.

    Use as ``__globals__`` for a routine you want to isolate from its
    defining module: reads fall through to the module's real globals,
    writes land in the overlay.

    :param base:
        The base globals dict (typically ``function.__globals__``
        before wrapping).
    """

    __slots__ = ("_base",)

    def __init__(self, base: dict):
        super().__init__()
        self._base = base

    def __missing__(self, key: str) -> Any:
        return self._base[key]

    def __repr__(self) -> str:
        return f"<_IsolatedGlobals base_id={id(self._base)}>"


# ----------------------------------------------------------------------
# Module cloning
# ----------------------------------------------------------------------


def clone_module(original: types.ModuleType) -> types.ModuleType:
    """Shallow-clone a module with function ``__globals__`` rebinding.

    Produces a fresh :class:`types.ModuleType` whose ``__dict__`` is
    a shallow copy of *original*'s, with every function defined in
    *original* rebuilt as a new :class:`types.FunctionType` whose
    ``__globals__`` references the clone's dict instead of the
    original's. This ensures helpers inside the clone resolve
    ``LOAD_GLOBAL`` against the clone's dict, so wire-shipped
    ContextVar substitutions (applied to the clone) are visible to
    helpers.

    Re-exports are preserved verbatim: if ``original.x`` is a function
    whose ``__globals__`` belongs to *another* module (i.e., the
    function was imported, not defined locally), the rebinding guard
    ``value.__globals__ is original.__dict__`` skips it. Only truly
    local functions get new ``__globals__``.

    Non-function attributes — classes, constants, other modules,
    bound methods, etc. — are shared by reference. This is a
    deliberate shallow-clone: callers who mutate a class or dict
    attribute of the clone will mutate it on all tasks sharing the
    cached lineage clone.

    :param original:
        The module to clone.
    :returns:
        A fresh :class:`types.ModuleType` ready to install in a task's
        isolated ``sys.modules``.
    """
    clone = types.ModuleType(original.__name__)
    clone.__dict__.update(original.__dict__)

    for attr_name, value in list(clone.__dict__.items()):
        if (
            isinstance(value, types.FunctionType)
            and value.__globals__ is original.__dict__
        ):
            rebound = types.FunctionType(
                value.__code__,
                clone.__dict__,
                value.__name__,
                value.__defaults__,
                value.__closure__,
            )
            rebound.__kwdefaults__ = value.__kwdefaults__
            rebound.__qualname__ = value.__qualname__
            rebound.__dict__.update(value.__dict__)
            clone.__dict__[attr_name] = rebound

    return clone


# ----------------------------------------------------------------------
# Lineage cache and active-task tracking
# ----------------------------------------------------------------------


# contextvar holding the current lineage ID for a task. None outside
# of any active dispatch.
_active_lineage: contextvars.ContextVar[uuid.UUID | None] = contextvars.ContextVar(
    "wool.namespace.active_lineage", default=None
)


# In-process cache: lineage_id → {module_name → cloned_module}.
# Entries live for the lineage lifetime; eviction is expected to be
# driven by a ResourcePool-backed TTL wrapper (added in a follow-up).
_lineage_cache: dict[uuid.UUID, dict[str, types.ModuleType]] = {}


def _get_lineage_entry(lineage_id: uuid.UUID) -> dict[str, types.ModuleType]:
    """Get or create the module-clone dict for *lineage_id*."""
    entry = _lineage_cache.get(lineage_id)
    if entry is None:
        entry = {}
        _lineage_cache[lineage_id] = entry
    return entry


def _discard_lineage(lineage_id: uuid.UUID) -> None:
    """Drop the cache entry for *lineage_id*. Idempotent."""
    _lineage_cache.pop(lineage_id, None)


# ----------------------------------------------------------------------
# Task import machinery
# ----------------------------------------------------------------------


class TaskLoader(importlib.abc.Loader):
    """Loader that routes module execution through the lineage cache.

    Wraps a base loader obtained from the default meta-path finders.
    On a cold lineage for a given module, :meth:`exec_module` delegates
    to the base loader, then runs the result through
    :func:`clone_module` and caches the clone on the lineage. On a
    warm lineage, :meth:`create_module` returns the cached clone
    directly, bypassing execution entirely.

    :param base_spec:
        The spec the original finder would have produced. Holds the
        base loader used for cold-path execution.
    :param lineage_id:
        The lineage this loader is bound to.
    """

    def __init__(
        self,
        base_spec: importlib.machinery.ModuleSpec,
        lineage_id: uuid.UUID,
    ):
        self._base_spec = base_spec
        self._lineage_id = lineage_id

    def create_module(
        self, spec: importlib.machinery.ModuleSpec
    ) -> types.ModuleType | None:
        entry = _get_lineage_entry(self._lineage_id)
        cached = entry.get(spec.name)
        if cached is not None:
            return cached
        base_loader = self._base_spec.loader
        if base_loader is None:
            return None
        create = getattr(base_loader, "create_module", None)
        if create is None:
            return None
        return create(spec)

    def exec_module(self, module: types.ModuleType) -> None:
        entry = _get_lineage_entry(self._lineage_id)
        cached = entry.get(module.__name__)
        if cached is module:
            return
        base_loader = self._base_spec.loader
        if base_loader is None:
            return
        base_loader.exec_module(module)
        clone = clone_module(module)
        entry[module.__name__] = clone
        # Mutate the freshly-executed module to match the clone so the
        # returned object (which the import system installed in the
        # task's sys.modules view) has the rebound functions.
        module.__dict__.clear()
        module.__dict__.update(clone.__dict__)


class TaskMetaPathFinder(importlib.abc.MetaPathFinder):
    """Meta-path finder gated on an active lineage.

    When :data:`_active_lineage` is set, resolves the spec via the
    underlying meta-path (excluding itself to avoid recursion), then
    wraps it with :class:`TaskLoader`. When no lineage is active,
    returns ``None`` so the default finders handle the import
    normally.
    """

    def find_spec(
        self,
        fullname: str,
        path: Any = None,
        target: types.ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        lineage_id = _active_lineage.get()
        if lineage_id is None:
            return None

        # Resolve via the rest of the meta-path, skipping self.
        for finder in sys.meta_path:
            if finder is self:
                continue
            find = getattr(finder, "find_spec", None)
            if find is None:
                continue
            try:
                base_spec = find(fullname, path, target)
            except ImportError:
                continue
            if base_spec is not None:
                return importlib.machinery.ModuleSpec(
                    name=base_spec.name,
                    loader=TaskLoader(base_spec, lineage_id),
                    origin=base_spec.origin,
                    is_package=base_spec.submodule_search_locations is not None,
                )
        return None


_installed_finder: TaskMetaPathFinder | None = None


def install() -> None:
    """Install :class:`TaskMetaPathFinder` at ``sys.meta_path[0]``.

    Idempotent. Called at worker startup so that task imports are
    routed through the lineage machinery whenever a lineage is
    active.
    """
    global _installed_finder
    if _installed_finder is not None:
        return
    finder = TaskMetaPathFinder()
    sys.meta_path.insert(0, finder)
    _installed_finder = finder


def uninstall() -> None:
    """Remove the finder from ``sys.meta_path``. Idempotent."""
    global _installed_finder
    if _installed_finder is None:
        return
    try:
        sys.meta_path.remove(_installed_finder)
    except ValueError:
        pass
    _installed_finder = None


# ----------------------------------------------------------------------
# Activation
# ----------------------------------------------------------------------


@contextmanager
def activate(
    lineage_id: uuid.UUID,
    manifest: dict[tuple[str, str], ContextVar],
    *,
    drop_on_exit: bool = False,
) -> Iterator[dict[str, types.ModuleType]]:
    """Activate a task's namespace for the duration of the context.

    Sets :data:`_active_lineage` so the :class:`TaskMetaPathFinder`
    begins routing imports through the lineage cache. Ensures that
    every module named in *manifest* is cloned into the lineage cache
    (populating it lazily via :func:`clone_module` against the live
    module in worker-level ``sys.modules``). Applies the manifest
    entries — writes each ``(mod, attr) → var`` into the appropriate
    clone's ``__dict__``, overwriting any fresh worker-side instance.

    Yields the dict of lineage-scoped module clones (keyed by module
    name) for the caller to use when wrapping routine globals.

    :param lineage_id:
        The lineage UUID to activate. Shared across dispatches in the
        same logical flow; fresh per root-dispatch or per
        ``asyncio.create_task`` fork.
    :param manifest:
        ``(module_name, attr_name) → ContextVar`` mapping, typically
        the output of :func:`Manifest.from_wire` applied to the wire
        payload.
    :param drop_on_exit:
        If ``True``, evict the lineage cache entry on context exit.
        Normally ``False`` — the cache entry outlives a single
        dispatch so subsequent dispatches in the same lineage can
        reuse clones. Set ``True`` for root dispatches whose lineage
        will not be reused, or for tests.
    """
    entry = _get_lineage_entry(lineage_id)
    token = _active_lineage.set(lineage_id)

    try:
        # Ensure every manifested module has a clone in the lineage.
        for mod_name, _attr_name in manifest:
            if mod_name in entry:
                continue
            original = sys.modules.get(mod_name)
            if original is None:
                _log.debug(
                    "manifest references module %r not present in sys.modules; "
                    "skipping clone (will be constructed lazily via TaskLoader "
                    "if the task imports it)",
                    mod_name,
                )
                continue
            entry[mod_name] = clone_module(original)

        # Apply manifest substitutions into the clones.
        for (mod_name, attr_name), var in manifest.items():
            clone = entry.get(mod_name)
            if clone is None:
                continue
            setattr(clone, attr_name, var)

        yield entry
    finally:
        _active_lineage.reset(token)
        if drop_on_exit:
            _discard_lineage(lineage_id)
