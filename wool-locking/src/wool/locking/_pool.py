from contextvars import ContextVar

import wool.locking
from wool.locking._session import LockPoolSession
from wool.locking._worker import LockScheduler


class LockPool(wool.Pool):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, breadth=1, **kwargs)

    @property
    def session_type(self) -> type[LockPoolSession]:
        return LockPoolSession

    @property
    def session_context(self) -> ContextVar[LockPoolSession]:
        return wool.locking.__locking_session__

    @property
    def scheduler_type(self) -> type[LockScheduler]:
        return LockScheduler
