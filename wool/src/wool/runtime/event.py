"""Core event infrastructure for the Wool runtime system.

This module provides the foundational event system used throughout Wool's
runtime. All runtime events (task lifecycle, worker discovery, etc.) inherit
from the Event base class and implement the EventLike protocol.
"""

from __future__ import annotations

from abc import ABC
from time import perf_counter_ns
from typing import Any
from typing import Callable
from typing import Protocol
from typing import runtime_checkable


# public
@runtime_checkable
class EventLike(Protocol):
    """Protocol defining the interface for all event types.

    All Wool events must conform to this protocol to ensure consistent
    behavior across different event types. The protocol is runtime
    checkable via isinstance().
    """

    type: str
    """Event type identifier (e.g., 'task-created', 'worker-added')."""

    def emit(self, context: Any | None = None) -> None:
        """Emit this event to all registered handlers.

        :param context:
            Optional metadata passed to handlers (e.g., emitter
            identification, debugging information).
        """
        ...


# public
@runtime_checkable
class EventHandler(Protocol):
    """Protocol for event handler functions.

    Handlers conforming to this protocol can be registered to receive
    event notifications via the Event.handler decorator.
    """

    def __call__(
        self,
        event: EventLike,
        timestamp: int,
        context: Any | None = None,
    ) -> None:
        """Handle an emitted event.

        :param event:
            The emitted event instance.
        :param timestamp:
            Nanosecond-precision timestamp from perf_counter_ns()
            captured at emission time.
        :param context:
            Optional metadata passed by emitter via emit(context=...).
        """
        ...


class Event(ABC):
    """Abstract base class for all Wool runtime events.

    Provides shared infrastructure for event emission and handler
    registration across all event types. Subclasses must set the `type`
    attribute to identify the event.

    **Example Usage**::

        from wool.runtime.event import Event


        class MyEvent(Event):
            def __init__(self, type: str, /, data: dict):
                super().__init__(type)
                self.data = data


        # Register handler
        @MyEvent.handler("my-event")
        def my_handler(event, timestamp, context=None):
            print(f"Event: {event.type}")


        # Emit event
        event = MyEvent("my-event", {"key": "value"})
        event.emit(context={"source": "app"})
    """

    type: str
    """Event type identifier set by subclass."""

    _handlers: dict[str, list[EventHandler]] = {}
    """Class-level handler registry shared across all event instances."""

    def __init__(self, type: str, /) -> None:
        """Initialize event with type.

        :param type:
            Event type identifier (positional-only).
        """
        self.type = type

    def emit(self, context: Any | None = None) -> None:
        """Emit this event to all registered handlers.

        Handlers are scheduled on the event loop via asyncio.call_soon()
        and invoked asynchronously in registration order. If no handlers
        are registered for this event type, this method returns
        immediately (no-op).

        Supports both legacy 2-argument handlers (event, timestamp) and
        modern 3-argument handlers (event, timestamp, context) for
        backward compatibility.

        Exceptions raised by handlers do not propagate to the caller;
        they are handled by the event loop's exception handler. This
        ensures that one failing handler does not prevent other handlers
        from executing.

        :param context:
            Optional metadata passed to handlers. May be any Python
            object. Should be JSON-serializable for debugging purposes.
        """
        if handlers := self._handlers.get(self.type):
            import asyncio

            timestamp = perf_counter_ns()
            loop = asyncio.get_event_loop()
            for handler in handlers:
                loop.call_soon(handler, self, timestamp, context)

    @classmethod
    def handler(cls, *event_types: str) -> Callable[[EventHandler], EventHandler]:
        """Decorator for registering event handlers.

        Handlers registered for specific event types will be invoked
        asynchronously when events of those types are emitted.

        :param event_types:
            One or more event type strings to handle.
        :returns:
            Decorator function that registers the handler.

        **Example**::

            @WorkTaskEvent.handler("task-created", "task-completed")
            def my_handler(event, timestamp, context=None):
                print(f"Task event: {event.type}")
        """

        def decorator(fn: EventHandler) -> EventHandler:
            for event_type in event_types:
                cls._handlers.setdefault(event_type, []).append(fn)
            return fn

        return decorator
