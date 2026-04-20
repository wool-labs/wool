"""Context-id bookkeeping for dispatched task execution.

A wool.Context id identifies a logical execution chain that spans
caller and worker. The dispatch handler uses :func:`activate` to
install a Context with the caller's id for the duration of the
handler scope. Worker-loop tasks inherit a copy of that Context
so they share the caller's id and var state.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import Iterator

from wool.runtime.context import Context
from wool.runtime.context import swap_context


@contextmanager
def activate(context_id: uuid.UUID) -> Iterator[None]:
    """Install a fresh wool.Context with the given id for the block.

    Creates a new empty :class:`Context` whose ``_id`` is
    *context_id* and swaps it into the current scope. On exit,
    the previous Context is restored. Used by the dispatch handler
    so that ``apply_vars`` writes into the handler's Context and
    the worker-loop task can copy it.

    :param context_id:
        The wool.Context id to activate.
    """
    new_ctx = Context._create(context_id, {})
    prev = swap_context(new_ctx)
    try:
        yield
    finally:
        swap_context(prev)
