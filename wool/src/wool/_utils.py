from __future__ import annotations

import threading
from typing import Callable
from typing import Final
from typing import Generic
from typing import TypeVar


class UndefinedType:
    _instance: UndefinedType | None = None

    def __repr__(self) -> str:
        return "Undefined"

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __bool__(self) -> bool:
        return False


Undefined: Final = UndefinedType()


class PredicatedEvent(threading.Event):
    def __init__(self, predicate: Callable[[], bool]):
        self._predicate = predicate
        super().__init__()

    def set(self, *args, **kwargs):
        return super().set(*args, **kwargs)

    def is_set(self):
        if not super().is_set() and self._predicate():
            self.set()
        return super().is_set()


T = TypeVar("T")


class Property(Generic[T]):
    _value: T | UndefinedType = Undefined
    _default: T | UndefinedType = Undefined

    def __init__(self, *, default: T = Undefined) -> None:
        self._default = default

    def get(self) -> T:
        if isinstance(self._value, UndefinedType):
            if isinstance(self._default, UndefinedType):
                raise ValueError("Property value is undefined")
            return self._default
        return self._value

    def set(self, value: T) -> None:
        self._value = value

    def reset(self) -> None:
        self._value = Undefined
