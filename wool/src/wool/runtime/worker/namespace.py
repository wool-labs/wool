"""Context-id bookkeeping for dispatched task execution.

A wool.Context id identifies a logical execution chain that spans
caller and worker. Inside a single :class:`asyncio.Task` the id is
stable across sequential awaits; :func:`asyncio.create_task` /
:func:`asyncio.gather` children fork a fresh id on their first wool
interaction, mirroring stdlib ``contextvars.Context`` fork semantics.

Pieces:

- :data:`_wool_sentinel` — stdlib contextvar carrying the active
  wool.Context id paired with the ``id`` of the stdlib Context it
  was bound in. The ctx_id comparison is how we detect implicit
  forks.
- :data:`_intended_context_id` — worker-side handoff: the dispatch
  handler plants the caller's id here; the first descendant task
  that calls :func:`_current_context_id` adopts it and clears the
  slot.
- :func:`_current_context_id` — the fork-detection primitive that
  backs :func:`wool.current_context`.
- :func:`adopt_context` — explicit adoption used by
  :meth:`wool.Context.run` when seeding a fresh stdlib Context in
  async code.
- :func:`activate` — context manager used by the dispatch handler
  to set the intended context id and bind the sentinel for its own
  task's duration.
"""

from __future__ import annotations

import asyncio
import contextvars
import sys
import uuid
from contextlib import contextmanager
from typing import Any
from typing import Iterator

# ----------------------------------------------------------------------
# Context-id fork detection
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


class _ContextSentinel:
    """Marker that binds a wool.Context id to the stdlib Context it was
    established in.

    Stored in the :data:`_wool_sentinel` stdlib contextvar so the id
    is recoverable from the current context. When stdlib copies the
    contextvar across an ``asyncio.create_task`` boundary, the
    sentinel's value propagates verbatim — but ``ctx_id`` still
    identifies the PARENT task's :class:`contextvars.Context` via
    :func:`_task_context_id`. :func:`_current_context_id` uses this
    to detect implicit forks: a mismatch between the sentinel's
    stored ``ctx_id`` and the current task's context id means we're
    inside a forked context (``asyncio.create_task`` or equivalent),
    which mints a fresh id.
    """

    __slots__ = ("context_id", "ctx_id")

    def __init__(self, context_id: uuid.UUID, ctx_id: int):
        self.context_id = context_id
        self.ctx_id = ctx_id


# Sentinel holding the active wool.Context id and the task it was
# bound to. Inside the same asyncio Task, the id is constant and
# continues across sequential awaits. Crossing an
# asyncio.create_task boundary flips the current task,
# :func:`_current_context_id` detects the mismatch, and mints a
# fresh id — stdlib parity with ``contextvars.Context`` fork
# semantics.
_wool_sentinel: contextvars.ContextVar[_ContextSentinel | None] = contextvars.ContextVar(
    "wool.namespace.wool_sentinel", default=None
)


# Worker-side adoption mechanism. When a dispatch handler enters
# :func:`activate`, it sets this contextvar to the caller's wire
# context id. The first asyncio task descendant that calls
# :func:`_current_context_id` consumes the intended value (by
# writing ``None`` to its own local context) and binds the sentinel
# to itself. Descendants of that task (via the user's own
# ``asyncio.create_task``) see ``None`` and mint fresh ids — stdlib
# fork semantics.
_intended_context_id: contextvars.ContextVar[uuid.UUID | None] = contextvars.ContextVar(
    "wool.namespace.intended_context_id", default=None
)


# Stable fallback context id for sync callers outside any asyncio
# task. Sharing a process-wide UUID across unrelated sync flows is
# the price for Tokens minted by ``var.set(...)`` in sync code to
# still match at ``var.reset(...)``. Per-call minting would make
# the Token context-id check reject every sync reset.
_process_default_context_id: uuid.UUID = uuid.uuid4()


def _current_context_id() -> uuid.UUID:
    """Internal fork-detection primitive. Returns the UUID of the
    current execution context.

    Exposed publicly as :attr:`wool.Context.id` via
    :func:`wool.current_context`. Kept private because the primary
    user-facing abstraction is :class:`wool.Context` — the id is an
    attribute of a Context, not a top-level concept.

    Resolution order:

    1. If we're in an asyncio task and the current stdlib ``Context``
       has a :class:`_ContextSentinel` whose ``ctx_id`` matches the
       current task's context id, return its context id (continuation).
    2. Else if :data:`_intended_context_id` is set (typically by
       :func:`activate` on the worker or by :meth:`wool.Context.run`
       / :meth:`run_async` when seeding): inside an asyncio task,
       consume it by writing ``None`` so descendants don't re-adopt
       and bind the sentinel to the current context; outside an
       asyncio task, leave :data:`_intended_context_id` set so
       subsequent reads in the same stdlib ``Context`` scope keep
       returning the same id (the stdlib ``Context`` is the only
       isolator available when there's no task to bind a sentinel
       to).
    3. Else in an asyncio task, mint a fresh id, bind the sentinel
       to the current context, and return it (implicit fork across
       an ``asyncio.create_task`` boundary).
    4. Outside any asyncio task and with no intended context id,
       return the process-wide default id. Shared across unrelated
       sync flows in the same process, but stable — Tokens captured
       at ``set()`` must compare equal on ``reset()`` for the common
       sync ``var.set(...); var.reset(token)`` case to work.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None

    ctx_id = task and _task_context_id(task)

    if ctx_id is not None:
        sentinel = _wool_sentinel.get(None)
        if sentinel is not None and sentinel.ctx_id == ctx_id:
            return sentinel.context_id

    intended = _intended_context_id.get(None)
    if intended is not None:
        if ctx_id is not None:
            _intended_context_id.set(None)
            _wool_sentinel.set(_ContextSentinel(intended, ctx_id))
        return intended

    if ctx_id is None:
        return _process_default_context_id

    context_id = uuid.uuid4()
    _wool_sentinel.set(_ContextSentinel(context_id, ctx_id))
    return context_id


def adopt_context(context_id: uuid.UUID) -> None:
    """Bind *context_id* to the current asyncio task.

    Used on the worker side when a dispatched task adopts the
    caller's context id instead of minting a fresh one. Must be
    called from within the asyncio task that will run user code.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    if task is None:
        return
    _wool_sentinel.set(_ContextSentinel(context_id, _task_context_id(task)))


# ----------------------------------------------------------------------
# Activation
# ----------------------------------------------------------------------


@contextmanager
def activate(context_id: uuid.UUID) -> Iterator[None]:
    """Activate a dispatched task's context id for the duration of the block.

    Plants :data:`_intended_context_id` so the worker-loop sub-task
    adopts the caller's id on its first
    :func:`_current_context_id` call, and binds the sentinel to the
    current (handler) task so the id is also recoverable from the
    handler's context.

    :param context_id:
        The wool.Context id to activate.
    """
    intended_token = _intended_context_id.set(context_id)

    try:
        handler_task = asyncio.current_task()
    except RuntimeError:
        handler_task = None
    handler_sentinel_token = None
    if handler_task is not None:
        handler_sentinel_token = _wool_sentinel.set(
            _ContextSentinel(context_id, _task_context_id(handler_task))
        )

    try:
        yield
    finally:
        if handler_sentinel_token is not None:
            _wool_sentinel.reset(handler_sentinel_token)
        _intended_context_id.reset(intended_token)
