from __future__ import annotations

from contextvars import ContextVar

import wool
import wool.locking


class LockPoolSession(wool.PoolSession):
    @property
    def session(self) -> ContextVar[wool.PoolSession]:
        return wool.locking.__locking_session__
