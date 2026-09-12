from __future__ import annotations

import pytest_asyncio

from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.discovery.pool import _subscriber_factories


@pytest_asyncio.fixture(autouse=True)
async def _clear_subscriber_pool():
    """Finalize the discovery subscriber pool on the loop that used it,
    then reset the pool and factory registry.

    The clear runs on the owning loop, the only place the pool's
    finalizers can run; anything a test left cached would otherwise be
    swept and reported once this loop has stopped.
    """
    yield
    if pool := __subscriber_pool__.get():
        await pool.clear()
    __subscriber_pool__.set(None)
    _subscriber_factories.clear()
