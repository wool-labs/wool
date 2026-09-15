from __future__ import annotations

import pytest
import pytest_asyncio

from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.discovery.pool import _subscriber_factories
from wool.runtime.discovery.pool import install_subscriber_pool


@pytest.fixture(autouse=True)
def subscriber_pool():
    """Install a subscriber pool for the test and yield it.

    Synchronous on purpose: a value set here is visible to the task
    pytest-asyncio runs the test in, so the subscribers a test builds
    cache into this object rather than into one they install privately.
    The pool is torn down by `_clear_subscriber_pool`, which holds this
    same object rather than reading the context var back.
    """
    token = __subscriber_pool__.set(None)
    try:
        yield install_subscriber_pool()
    finally:
        __subscriber_pool__.reset(token)
        _subscriber_factories.clear()


@pytest_asyncio.fixture(autouse=True)
async def _clear_subscriber_pool(subscriber_pool):
    """Finalize the subscriber pool on the loop that used it.

    The clear runs on the owning loop, the only place the pool's
    finalizers can run; anything a test left cached would otherwise be
    discarded and reported once this loop has closed. It clears the
    object `subscriber_pool` yielded rather than whatever
    ``__subscriber_pool__`` reads back here: an async fixture's teardown
    runs in a task with a context copy of its own, where the test's own
    assignments are not visible.
    """
    yield
    await subscriber_pool.clear()
