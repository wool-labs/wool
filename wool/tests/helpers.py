from collections.abc import Generator
from contextlib import contextmanager

from wool.runtime.context import current_snapshot
from wool.runtime.context import install_snapshot


@contextmanager
def scoped_context() -> Generator[None]:
    """Test helper — run a block under a fresh, unarmed Wool context.

    Wool chain state rides in one wool-owned stdlib
    ``contextvars.ContextVar`` as an immutable
    :class:`~wool.runtime.context.snapshot.Snapshot`. This helper
    captures whatever snapshot is active, installs ``None`` (the
    unarmed state — no chain, no guard, behaves as a plain
    :class:`contextvars.Context`), yields, then reinstalls the
    captured snapshot on exit.

    Used by autouse isolation fixtures so a :meth:`wool.ContextVar.set`
    in one test arms a snapshot that does not leak into the next.
    The process-wide ``var_registry``/``token_registry`` are not
    reset; tests SHOULD use unique key namespaces (e.g. via a uuid
    suffix) to avoid cross-test collisions on shared keys.
    """
    saved = current_snapshot()
    install_snapshot(None)
    try:
        yield
    finally:
        install_snapshot(saved)


def context_is_unarmed() -> bool:
    """Test helper — return whether the current context carries no Wool snapshot.

    A module-level, picklable function so it can be dispatched to a
    :class:`~concurrent.futures.ProcessPoolExecutor` worker, where it
    proves a bare ``run_in_executor`` offload carries no Wool chain
    into a worker process.
    """
    return current_snapshot() is None
