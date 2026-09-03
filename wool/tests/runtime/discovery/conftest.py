from __future__ import annotations

import pytest_asyncio

from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.discovery.pool import _subscriber_factories


@pytest_asyncio.fixture(autouse=True)
async def _clear_subscriber_pool():
    """Finalize the discovery subscriber pool on the loop that used it,
    then reset the pool and factory registry.

    The clear is for prompt finalization on the owning loop, not
    correctness: the pool would rebind and drop its entries on the next
    loop regardless.
    """
    yield
    if pool := __subscriber_pool__.get():
        await pool.clear()
    __subscriber_pool__.set(None)
    _subscriber_factories.clear()
