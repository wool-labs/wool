from contextvars import ContextVar

import wool.locking
from wool.locking._client import LockClient


class LockPool(wool.WoolPool):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, breadth=1, **kwargs)

    @property
    def client_context(self) -> ContextVar[LockClient]:
        return wool.locking.__lock_client__
