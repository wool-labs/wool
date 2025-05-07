from __future__ import annotations

from contextvars import ContextVar

import wool
import wool.locking


class LockClient(wool.WoolClient):
    _outer_client: LockClient | None = None

    @property
    def client_context(self) -> ContextVar[LockClient]:
        return wool.locking.__lock_client__
