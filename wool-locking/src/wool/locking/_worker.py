from contextvars import ContextVar

import wool
import wool.locking
from wool.locking._session import LockPoolSession


class LockScheduler(wool.Scheduler):
    @property
    def session_context(self) -> ContextVar[LockPoolSession]:
        return wool.locking.__locking_session__
