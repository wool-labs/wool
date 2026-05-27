from collections.abc import Generator
from contextlib import contextmanager

from wool.runtime.context import current_context

# ``_install_context`` is the private install primitive — the public
# install entry point is :meth:`Context.mount`, which restamps owner
# and drains pending values. The :func:`scoped_context` helper below
# needs the raw install (disarm to ``None`` and restore the captured
# Context verbatim), which has no public-API substitute. This is the
# sole legitimate test-infrastructure consumer of the private name.
from wool.runtime.context.base import _install_context


@contextmanager
def scoped_context() -> Generator[None]:
    """Test helper — run a block under a fresh, unarmed Wool context.

    Wool chain state rides in one wool-owned stdlib
    ``contextvars.ContextVar`` as an immutable
    :class:`~wool.runtime.context.base.Context`. This helper
    captures whatever context is active, installs ``None`` (the
    unarmed state — no chain, no guard, behaves as a plain
    :class:`contextvars.Context`), yields, then reinstalls the
    captured context on exit.

    Used by autouse isolation fixtures so a :meth:`wool.ContextVar.set`
    in one test arms a context that does not leak into the next.
    The process-wide ``var_registry``/``token_registry`` are not
    reset; tests SHOULD use unique key namespaces (e.g. via a uuid
    suffix) to avoid cross-test collisions on shared keys.
    """
    saved = current_context()
    _install_context(None)
    try:
        yield
    finally:
        _install_context(saved)


def context_is_unarmed() -> bool:
    """Test helper — return whether the current context carries no Wool state.

    A module-level, picklable function so it can be dispatched to a
    :class:`~concurrent.futures.ProcessPoolExecutor` worker, where it
    proves a bare ``run_in_executor`` offload carries no Wool chain
    into a worker process.
    """
    return current_context() is None
