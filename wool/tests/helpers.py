from collections.abc import Generator
from contextlib import contextmanager

import wool


@contextmanager
def scoped_context() -> Generator[None]:
    """Test helper — bracket a block of Wool chain mutations.

    Per-test isolation lives in the ``pytest_pyfunc_call`` hook in
    ``tests/conftest.py``, which runs each sync test in a fresh
    :func:`contextvars.copy_context` (async tests self-isolate via their
    task's context copy). With ``__chain__`` typed
    :class:`~wool.runtime.context.chain.Chain` there is no settable
    "unarmed" value to install in place, so this manager no longer
    disarms; it is retained as a no-op scope around chain mutations.
    """
    yield


def context_is_unarmed() -> bool:
    """Test helper — return whether the current context carries no Wool state.

    A module-level, picklable function so it can be dispatched to a
    :class:`~concurrent.futures.ProcessPoolExecutor` worker, where it
    proves a bare ``run_in_executor`` offload carries no Wool chain
    into a worker process.
    """
    return wool.__chain__.get(None) is None
