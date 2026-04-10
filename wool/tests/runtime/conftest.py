import gc

import pytest


@pytest.fixture(autouse=True)
def _collect_weakrefs():
    """Force a GC pass after each test so WeakSet-registered objects drop.

    wool.ContextVar uses a class-level weakref.WeakSet registry; forcing
    a collect in teardown keeps cross-test state deterministic when
    tests construct function-scoped context vars.
    """
    yield
    gc.collect()
