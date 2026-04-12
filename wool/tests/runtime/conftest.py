import gc
import sys

import pytest


@pytest.fixture(autouse=True)
def _collect_weakrefs():
    """Clean up test-scoped wool.ContextVar instances between tests.

    wool.ContextVar instances are types.ModuleType subclasses that
    register in sys.modules under synthetic keys (wool._vars.<name>).
    Without cleanup, test-constructed ContextVars persist across
    tests and cause ordering-dependent failures via __new__
    unification (a second test constructing the same name gets the
    first test's instance, which may have stale state).

    This fixture removes all wool._vars.* entries from sys.modules
    after each test and forces a GC pass so the WeakSet registry
    drops the orphaned instances.
    """
    yield
    to_remove = [key for key in sys.modules if key.startswith("wool._vars.")]
    for key in to_remove:
        del sys.modules[key]
    gc.collect()
