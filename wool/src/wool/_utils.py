from __future__ import annotations

import threading
from typing import Callable
from typing import Final
from typing import Generic
from typing import TypeVar


class UndefinedType:
    """
    A singleton class representing an undefined value.

    This class is used to differentiate between `None` and an undefined state.
    """

    _instance: UndefinedType | None = None

    def __repr__(self) -> str:
        """
        Return a string representation of the undefined value.

        :return: The string "Undefined".
        """
        return "Undefined"

    def __new__(cls, *args, **kwargs):
        """
        Create or return the singleton instance of UndefinedType.

        :return: The singleton instance of UndefinedType.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __bool__(self) -> bool:
        """
        Return the boolean value of the undefined state.

        :return: False, indicating the value is undefined.
        """
        return False


Undefined: Final = UndefinedType()
"""
A constant representing the undefined value.
"""


class PredicatedEvent(threading.Event):
    """
    An event that is automatically set when a predicate function evaluates to
    True.

    :param predicate: A callable that returns a boolean value.
    """

    def __init__(self, predicate: Callable[[], bool]):
        """
        Initialize the PredicatedEvent with a predicate function.

        :param predicate: A callable that returns a boolean value.
        """
        self._predicate = predicate
        super().__init__()

    def set(self, *args, **kwargs):
        """
        Set the event's internal flag.

        :param args: Additional positional arguments.
        :param kwargs: Additional keyword arguments.
        """
        return super().set(*args, **kwargs)

    def is_set(self):
        """
        Check whether the event is set, evaluating the predicate if necessary.

        If the predicate evaluates to True, the event is automatically set.

        :return: True if the event is set, False otherwise.
        """
        if not super().is_set() and self._predicate():
            self.set()
        return super().is_set()


T = TypeVar("T")


class Property(Generic[T]):
    """
    A generic property class with support for default values.

    :param T: The type of the property value.
    """

    _value: T | UndefinedType = Undefined
    _default: T | UndefinedType = Undefined

    def __init__(self, *, default: T = Undefined) -> None:
        """
        Initialize the Property with an optional default value.

        :param default: The default value for the property.
        """
        self._default = default

    def get(self) -> T:
        """
        Retrieve the current value of the property.

        If the value is undefined, the default value is returned.

        :return: The current value of the property.
        :raises ValueError: If the value and default are both undefined.
        """
        if isinstance(self._value, UndefinedType):
            if isinstance(self._default, UndefinedType):
                raise ValueError("Property value is undefined")
            return self._default
        return self._value

    def set(self, value: T) -> None:
        """
        Set the value of the property.

        :param value: The new value for the property.
        """
        self._value = value

    def reset(self) -> None:
        """
        Reset the property value to its default state.
        """
        self._value = Undefined
