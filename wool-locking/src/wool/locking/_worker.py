from contextvars import ContextVar

import wool
import wool.locking


class LockScheduler(wool.Scheduler):
    @property
    def session_context(self) -> ContextVar[wool.PoolSession]:
        return wool.locking.__locking_session__
