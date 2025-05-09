from __future__ import annotations

from contextvars import ContextVar

import wool
import wool.locking


class LockPoolSession(wool.PoolSession):
    _outer_client: LockPoolSession | None = None

    @property
    def session(self) -> ContextVar[LockPoolSession]:
        return wool.locking.__locking_session__
