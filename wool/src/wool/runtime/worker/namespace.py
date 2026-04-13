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

import asyncio
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


if sys.version_info < (3, 12):

    def _task_context_id(task: asyncio.Task[Any]) -> int:
        """Return ``id(ctx)`` for the task's bound :class:`contextvars.Context`.

        Python 3.11 fallback: :meth:`asyncio.Task.get_context` was
        introduced in 3.12, so reach for the non-public ``_context``
        attribute. Documented in CPython's asyncio internals and
        stable across 3.11 patch releases. Delete this ``if`` block
        when 3.11 support is dropped and the long-form implementation
        below becomes the only path.
        """
        return id(task._context)  # type: ignore[attr-defined]

else:

    def _task_context_id(task: asyncio.Task[Any]) -> int:
        """Return ``id(ctx)`` for the task's bound :class:`contextvars.Context`.

        Uses the public :meth:`asyncio.Task.get_context` (Python
        3.12+).
        """
        return id(task.get_context())


class _LineageSentinel:
    """Marker that binds a lineage UUID to the stdlib Context it was
    established in.

    Stored in the :data:`_wool_sentinel` stdlib contextvar so lineage
    is recoverable from the current context. When stdlib copies the
    contextvar across an ``asyncio.create_task`` boundary, the
    sentinel's value propagates verbatim — but ``ctx_id`` still
    identifies the PARENT task's :class:`contextvars.Context` via
    :func:`_task_context_id`. :func:`current_lineage` uses this to
    detect implicit forks: a mismatch between the sentinel's stored
    ``ctx_id`` and the current task's context id means we're inside
    a forked context (``asyncio.create_task`` or equivalent), which
    mints a fresh lineage.
    """

    __slots__ = ("lineage_id", "ctx_id")

    def __init__(self, lineage_id: uuid.UUID, ctx_id: int):
        self.lineage_id = lineage_id
        self.ctx_id = ctx_id


# Sentinel holding the active lineage UUID and the task it was
# bound to. Inside the same asyncio Task, lineage is constant and
# continues across sequential awaits. Crossing an asyncio.create_task
# boundary flips the current task, :func:`current_lineage` detects
# the mismatch, and mints a fresh lineage — stdlib parity with
# ``contextvars.Context`` fork semantics.
_wool_sentinel: contextvars.ContextVar[_LineageSentinel | None] = contextvars.ContextVar(
    "wool.namespace.wool_sentinel", default=None
)


# Worker-side adoption mechanism. When a dispatch handler enters
# :func:`activate`, it sets this contextvar to the caller's wire
# lineage. The first asyncio task descendant that calls
# :func:`current_lineage` consumes the intended value (by writing
# ``None`` to its own local context) and binds the sentinel to
# itself. Descendants of that task (via the user's own
# ``asyncio.create_task``) see ``None`` and mint fresh lineages —
# stdlib fork semantics.
_intended_lineage: contextvars.ContextVar[uuid.UUID | None] = contextvars.ContextVar(
    "wool.namespace.intended_lineage", default=None
)


# Fallback lineage for code that dispatches outside any asyncio task
# (e.g., synchronous tests building payloads manually). One per process.
_process_default_lineage: uuid.UUID = uuid.uuid4()


def _current_lineage() -> uuid.UUID:
    """Internal fork-detection primitive. Returns the UUID of the
    current execution context's lineage.

    Exposed publicly as :attr:`wool.Context.lineage_id` via
    :func:`wool.current_context`. Kept private because the primary
    user-facing abstraction is :class:`wool.Context` — lineage is an
    attribute of a Context, not a top-level concept.

    Resolution order:

    1. If we're in an asyncio task and the current stdlib ``Context``
       has a :class:`_LineageSentinel` whose ``ctx_id`` matches the
       current task's context id, return its lineage (continuation).
    2. Else if :data:`_intended_lineage` is set (typically by
       :func:`activate` on the worker), consume it by writing
       ``None`` to our own context so descendants don't re-adopt,
       bind the sentinel to the current context, and return the
       adopted UUID.
    3. Else in an asyncio task, mint a fresh lineage, bind the
       sentinel to the current context, and return it (implicit fork
       across an ``asyncio.create_task`` boundary).
    4. Outside any asyncio task and with no intended lineage, return
       the process-wide default lineage.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None

    ctx_id = _task_context_id(task) if task is not None else None

    if ctx_id is not None:
        sentinel = _wool_sentinel.get(None)
        if sentinel is not None and sentinel.ctx_id == ctx_id:
            return sentinel.lineage_id

    intended = _intended_lineage.get(None)
    if intended is not None:
        _intended_lineage.set(None)
        if ctx_id is not None:
            _wool_sentinel.set(_LineageSentinel(intended, ctx_id))
        return intended

    if ctx_id is None:
        return _process_default_lineage

    lineage = uuid.uuid4()
    _wool_sentinel.set(_LineageSentinel(lineage, ctx_id))
    return lineage


def adopt_lineage(lineage_id: uuid.UUID) -> None:
    """Bind *lineage_id* to the current asyncio task.

    Used on the worker side when a dispatched task adopts the
    caller's lineage instead of minting a fresh one. Must be called
    from within the asyncio task that will run user code.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    if task is None:
        return
    _wool_sentinel.set(_LineageSentinel(lineage_id, _task_context_id(task)))


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

    When a sentinel is bound (i.e., we're inside an adopted or
    freshly-minted lineage), resolves the spec via the underlying
    meta-path (excluding itself to avoid recursion) and wraps it
    with :class:`TaskLoader`. Otherwise returns ``None`` so the
    default finders handle the import normally.
    """

    def find_spec(
        self,
        fullname: str,
        path: Any = None,
        target: types.ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        try:
            task = asyncio.current_task()
        except RuntimeError:
            return None
        if task is None:
            return None
        sentinel = _wool_sentinel.get(None)
        if sentinel is None or sentinel.ctx_id != _task_context_id(task):
            return None
        lineage_id = sentinel.lineage_id

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
    # Set the intended lineage so the next asyncio task descendant
    # (typically the worker-side task created by the dispatch
    # handler to run user code) adopts it instead of minting a fresh
    # one. Direct sentinel adoption here would bind to the handler's
    # task — sub-tasks created under it would see the mismatch and
    # fork, which is the opposite of what we want for worker
    # adoption.
    intended_token = _intended_lineage.set(lineage_id)

    try:
        # Substitute manifest entries directly into the live
        # sys.modules. Identity mutation is safe across concurrent
        # tasks because wool.ContextVar identity is deterministic per
        # (module, attr): the wire always ships the same UUID for
        # the same caller-side binding, so concurrent substitutions
        # are idempotent no-ops after the first write.
        #
        # Value-level per-task isolation is provided by stdlib
        # contextvars: the ContextVar's backing ``contextvars.ContextVar``
        # observes each asyncio task's own Context, so values set on
        # the shared identity remain task-local.
        #
        # The lineage cache (``entry``) is reserved for a future
        # enhancement that swaps in per-lineage module clones when
        # full binding-level isolation is required (see
        # TaskLoader / TaskMetaPathFinder, not yet wired in).
        for (mod_name, attr_name), var in manifest.items():
            mod = sys.modules.get(mod_name)
            if mod is None:
                _log.debug(
                    "manifest references module %r not present on worker; "
                    "skipping substitution",
                    mod_name,
                )
                continue
            setattr(mod, attr_name, var)

        yield entry
    finally:
        _intended_lineage.reset(intended_token)
        if drop_on_exit:
            _discard_lineage(lineage_id)
