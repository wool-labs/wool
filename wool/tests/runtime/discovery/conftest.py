from __future__ import annotations

import pytest_asyncio

from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.discovery.pool import _subscriber_factories


@pytest_asyncio.fixture(autouse=True)
async def _clear_subscriber_pool():
    """Clear the discovery subscriber pool and factory registry
    between tests.
    """
    yield
    if pool := __subscriber_pool__.get():
        await pool.clear()
    __subscriber_pool__.set(None)
    _subscriber_factories.clear()
