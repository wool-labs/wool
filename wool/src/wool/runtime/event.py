from __future__ import annotations

import asyncio
import logging
import threading
from abc import ABC
from inspect import iscoroutinefunction
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
    event notifications via the Event.handler decorator. Registered
    handlers are executed on a dedicated handler thread's event loop
    when triggered.
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


# public
@runtime_checkable
class AsyncEventHandler(Protocol):
    """Protocol for async event handler functions.

    Async handlers conforming to this protocol can be registered to
    receive event notifications via the Event.handler decorator.
    Registered handlers are executed on a dedicated handler thread's
    event loop when triggered.
    """

    async def __call__(
        self,
        event: EventLike,
        timestamp: int,
        context: Any | None = None,
    ) -> None:
        """Handle an emitted event asynchronously.

        :param event:
            The emitted event instance.
        :param timestamp:
            Nanosecond-precision timestamp from perf_counter_ns()
            captured at emission time.
        :param context:
            Optional metadata passed by emitter via emit(context=...).
        """
        ...


class _EventHandlerThread:
    """Dedicated daemon thread for executing event handlers.

    Provides a single event loop on a background thread that decouples
    handler execution from the emitter's thread/event loop. Supports
    both synchronous and asynchronous handlers. The thread is lazily
    initialized on first use via :py:meth:`_ensure_started`.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    def _ensure_started(self) -> asyncio.AbstractEventLoop:
        """Lazily start the handler thread and event loop.

        Uses double-checked locking to avoid acquiring the lock on
        the fast path when the thread is already running.

        :returns:
            The event loop running on the handler thread.
        """
        if (
            self._loop is not None
            and self._thread is not None
            and self._thread.is_alive()
        ):
            return self._loop
        with self._lock:
            if (
                self._loop is not None
                and self._thread is not None
                and self._thread.is_alive()
            ):
                return self._loop
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(
                target=self._loop.run_forever,
                daemon=True,
                name="wool-event-handler",
            )
            self._thread.start()
            return self._loop

    def schedule(
        self,
        handler: EventHandler | AsyncEventHandler,
        event: EventLike,
        timestamp: int,
        context: Any | None,
    ) -> None:
        """Schedule a handler for execution on the handler thread.

        Dispatches to the appropriate scheduling mechanism based on
        whether the handler is a coroutine function or not. Each
        invocation is wrapped in try/except so that exceptions are
        logged rather than crashing the event loop.

        :param handler:
            The event handler to invoke (sync or async).
        :param event:
            The emitted event instance.
        :param timestamp:
            Nanosecond-precision timestamp captured at emission time.
        :param context:
            Optional metadata passed by emitter.
        """
        loop = self._ensure_started()

        if iscoroutinefunction(handler):

            async def _safe_invoke_async():
                try:
                    await handler(event, timestamp, context)
                except Exception:
                    logging.exception(
                        "Exception in event handler %r for event type %r",
                        handler,
                        event.type,
                    )

            asyncio.run_coroutine_threadsafe(_safe_invoke_async(), loop)
        else:

            def _safe_invoke():
                try:
                    handler(event, timestamp, context)
                except Exception:
                    logging.exception(
                        "Exception in event handler %r for event type %r",
                        handler,
                        event.type,
                    )

            loop.call_soon_threadsafe(_safe_invoke)

    def flush(self, *, timeout: float = 5.0) -> None:
        """Block until all pending handlers on the thread complete.

        Submits a sentinel coroutine that gathers all pending tasks
        on the handler loop, then blocks until that sentinel finishes.

        :param timeout:
            Maximum time in seconds to wait for pending handlers.
        """
        loop = self._loop
        if loop is None or self._thread is None or not self._thread.is_alive():
            return

        async def _drain():
            current = asyncio.current_task()
            pending = [
                t for t in asyncio.all_tasks(loop) if t is not current and not t.done()
            ]
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        future = asyncio.run_coroutine_threadsafe(_drain(), loop)
        future.result(timeout=timeout)

    def stop(self) -> None:
        """Stop the handler event loop and join the thread.

        Cancels all pending tasks on the handler loop before stopping,
        following the pattern from
        :py:meth:`WorkerService._destroy_worker_loop`.
        """
        loop = self._loop
        thread = self._thread
        if loop is None or thread is None:
            return

        if thread.is_alive():

            async def _shutdown():
                current = asyncio.current_task()
                tasks = [
                    t
                    for t in asyncio.all_tasks(loop)
                    if t is not current and not t.done()
                ]
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                loop.stop()

            loop.call_soon_threadsafe(lambda: loop.create_task(_shutdown()))
            thread.join(timeout=5)

        if not loop.is_closed():
            loop.close()

        self._loop = None
        self._thread = None


_handler_thread = _EventHandlerThread()


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

        Handlers are scheduled on a dedicated handler thread's event
        loop and invoked asynchronously in registration order. Both
        synchronous and asynchronous handlers are supported. If no
        handlers are registered for this event type, this method
        returns immediately (no-op).

        Exceptions raised by handlers do not propagate to the caller;
        they are logged via :py:func:`logging.exception`. This ensures
        that one failing handler does not prevent other handlers from
        executing.

        :param context:
            Optional metadata passed to handlers. May be any Python
            object. Should be JSON-serializable for debugging purposes.
        """
        if handlers := self._handlers.get(self.type):
            timestamp = perf_counter_ns()
            for handler in handlers:
                _handler_thread.schedule(handler, self, timestamp, context)

    @classmethod
    def flush(cls, *, timeout: float = 5.0) -> None:
        """Block until all pending event handlers complete.

        Ensures all handlers scheduled by prior :py:meth:`emit` calls
        have finished executing before returning. Useful for graceful
        shutdown sequences where all side-effects must be observed
        before proceeding.

        :param timeout:
            Maximum time in seconds to wait for pending handlers.
        """
        _handler_thread.flush(timeout=timeout)

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
