import logging
from abc import ABC

import pytest
from hypothesis import given
from hypothesis import strategies as st

from wool.runtime.event import AsyncEventHandler
from wool.runtime.event import Event
from wool.runtime.event import EventHandler
from wool.runtime.event import EventLike


@pytest.fixture
def concrete_event_class():
    """Concrete Event subclass for testing.

    Returns:
        A concrete Event subclass that can be instantiated.
    """

    class TestEvent(Event):
        def __init__(self, type: str, /, data: dict | None = None):
            super().__init__(type)
            self.data = data or {}

    return TestEvent


@pytest.fixture
def event_spy():
    """Spy function that records 3-arg handler calls.

    Returns:
        A callable spy function with a `calls` attribute.
    """
    calls = []

    def spy(event, timestamp, context=None):
        calls.append((event, timestamp, context))

    spy.calls = calls
    return spy


@pytest.fixture(autouse=True)
def clear_handlers():
    """Clear handler registry before and after each test.

    Ensures test isolation by cleaning the class-level handler registry
    and stopping the handler thread.
    """
    Event._handlers.clear()
    yield
    Event._handlers.clear()


class TestEventLike:
    """Tests for EventLike protocol conformance."""

    def test_conforming_class_with_type_and_emit(self):
        """Test isinstance check for conforming class.

        Given:
            A class with `type` attribute and `emit()` method
        When:
            Check isinstance(obj, EventLike)
        Then:
            Returns True
        """

        # Arrange
        class ConformingEvent:
            type = "test-event"

            def emit(self, context=None):
                pass

        obj = ConformingEvent()

        # Act
        result = isinstance(obj, EventLike)

        # Assert
        assert result is True

    def test_nonconforming_class_missing_type(self):
        """Test isinstance check for class missing type attribute.

        Given:
            A class missing `type` attribute
        When:
            Check isinstance(obj, EventLike)
        Then:
            Returns False
        """

        # Arrange
        class NonConformingEvent:
            def emit(self, context=None):
                pass

        obj = NonConformingEvent()

        # Act
        result = isinstance(obj, EventLike)

        # Assert
        assert result is False

    def test_nonconforming_class_missing_emit(self):
        """Test isinstance check for class missing emit method.

        Given:
            A class with `type` but no `emit()` method
        When:
            Check isinstance(obj, EventLike)
        Then:
            Returns False
        """

        # Arrange
        class NonConformingEvent:
            type = "test-event"

        obj = NonConformingEvent()

        # Act
        result = isinstance(obj, EventLike)

        # Assert
        assert result is False

    def test_event_base_class_conforms(self, concrete_event_class):
        """Test Event base class conforms to protocol.

        Given:
            Event base class instance
        When:
            Check isinstance(event, EventLike)
        Then:
            Returns True
        """
        # Arrange
        event = concrete_event_class("test-event")

        # Act
        result = isinstance(event, EventLike)

        # Assert
        assert result is True

    def test_runtime_checkable_decorator(self):
        """Test EventLike protocol is runtime checkable.

        Given:
            EventLike protocol itself
        When:
            Verify @runtime_checkable decorator present
        Then:
            isinstance() checks work at runtime
        """
        # Arrange & Act
        # Protocol is already defined with @runtime_checkable

        # Assert - can do isinstance checks
        class TestClass:
            type = "test"

            def emit(self, context=None):
                pass

        assert isinstance(TestClass(), EventLike)


class TestEventHandler:
    """Tests for EventHandler protocol conformance.

    Note: Python's @runtime_checkable Protocol only checks for the existence
    of __call__, not its signature. Signature validation must be done at
    registration or invocation time, not via isinstance().
    """

    def test_conforming_callable(self):
        """Test isinstance check for callable objects.

        Given:
            A callable function
        When:
            Check isinstance(fn, EventHandler)
        Then:
            Returns True (Protocol checks for __call__ existence)
        """

        # Arrange
        def handler(event, timestamp, context=None):
            pass

        # Act
        result = isinstance(handler, EventHandler)

        # Assert
        assert result is True

    def test_conforming_lambda(self):
        """Test isinstance check for lambda.

        Given:
            A lambda function
        When:
            Check isinstance(lambda, EventHandler)
        Then:
            Returns True (Protocol checks for __call__ existence)
        """
        # Arrange
        handler = lambda event, timestamp, context=None: None

        # Act
        result = isinstance(handler, EventHandler)

        # Assert
        assert result is True

    def test_conforming_callable_object(self):
        """Test isinstance check for callable class instances.

        Given:
            An instance of a class with __call__ method
        When:
            Check isinstance(obj, EventHandler)
        Then:
            Returns True
        """

        # Arrange
        class CallableHandler:
            def __call__(self, event, timestamp, context=None):
                pass

        handler = CallableHandler()

        # Act
        result = isinstance(handler, EventHandler)

        # Assert
        assert result is True

    def test_nonconforming_non_callable(self):
        """Test isinstance check for non-callable object.

        Given:
            A non-callable object
        When:
            Check isinstance(obj, EventHandler)
        Then:
            Returns False
        """
        # Arrange
        obj = "not a callable"

        # Act
        result = isinstance(obj, EventHandler)

        # Assert
        assert result is False

    def test_protocol_is_runtime_checkable(self):
        """Test EventHandler protocol is runtime checkable.

        Given:
            EventHandler protocol
        When:
            Verify @runtime_checkable decorator present
        Then:
            isinstance() checks work at runtime
        """

        # Arrange
        def handler(event, timestamp):
            pass

        # Act & Assert - can use isinstance without error
        assert isinstance(handler, EventHandler)


class TestAsyncEventHandler:
    """Tests for AsyncEventHandler protocol and async handler execution."""

    def test_async_handler_protocol_conformance(self):
        """Test AsyncEventHandler protocol is runtime checkable.

        Given:
            An async callable function
        When:
            Check isinstance(fn, AsyncEventHandler)
        Then:
            Returns True (Protocol checks for __call__ existence)
        """

        # Arrange
        async def handler(event, timestamp, context=None):
            pass

        # Act & Assert
        assert isinstance(handler, AsyncEventHandler)

    def test_sync_callable_is_also_async_handler(self):
        """Test sync callable also satisfies AsyncEventHandler protocol.

        Given:
            A synchronous callable function
        When:
            Check isinstance(fn, AsyncEventHandler)
        Then:
            Returns True (runtime_checkable only checks __call__)
        """

        # Arrange
        def handler(event, timestamp, context=None):
            pass

        # Act & Assert
        # runtime_checkable Protocol only checks for __call__ existence
        assert isinstance(handler, AsyncEventHandler)

    def test_async_handler_invocation(self, concrete_event_class):
        """Test async handler is invoked via the handler thread.

        Given:
            An async handler registered for an event type
        When:
            Event is emitted and flushed
        Then:
            The async handler is called with correct arguments
        """
        # Arrange
        calls = []

        async def async_handler(event, timestamp, context=None):
            calls.append((event, timestamp, context))

        Event._handlers["test-event"] = [async_handler]
        event = concrete_event_class("test-event")

        # Act
        event.emit()
        Event.flush()

        # Assert
        assert len(calls) == 1
        emitted_event, timestamp, context = calls[0]
        assert emitted_event is event
        assert isinstance(timestamp, int)
        assert context is None

    def test_async_handler_with_context(self, concrete_event_class):
        """Test async handler receives context argument.

        Given:
            An async handler registered for an event type
        When:
            Event is emitted with context and flushed
        Then:
            The async handler receives the context
        """
        # Arrange
        calls = []

        async def async_handler(event, timestamp, context=None):
            calls.append((event, timestamp, context))

        Event._handlers["test-event"] = [async_handler]
        event = concrete_event_class("test-event")
        context_data = {"key": "value"}

        # Act
        event.emit(context=context_data)
        Event.flush()

        # Assert
        assert len(calls) == 1
        _, _, context = calls[0]
        assert context is context_data

    def test_mixed_sync_and_async_handlers(self, concrete_event_class):
        """Test mixed sync and async handlers are both invoked.

        Given:
            Both sync and async handlers registered for same type
        When:
            Event is emitted and flushed
        Then:
            All handlers are called in registration order
        """
        # Arrange
        call_order = []

        def sync_handler(event, timestamp, context=None):
            call_order.append("sync")

        async def async_handler(event, timestamp, context=None):
            call_order.append("async")

        Event._handlers["test-event"] = [sync_handler, async_handler]
        event = concrete_event_class("test-event")

        # Act
        event.emit()
        Event.flush()

        # Assert
        assert len(call_order) == 2
        assert "sync" in call_order
        assert "async" in call_order

    def test_async_handler_exception_isolation(
        self,
        concrete_event_class,
        caplog,
    ):
        """Test async handler exception is logged, not propagated.

        Given:
            An async handler that raises an exception
        When:
            Event is emitted and flushed
        Then:
            Exception is logged and does not propagate
        """

        # Arrange
        async def failing_handler(event, timestamp, context=None):
            raise ValueError("Async handler failed")

        Event._handlers["test-event"] = [failing_handler]
        event = concrete_event_class("test-event")

        # Act
        with caplog.at_level(logging.ERROR):
            event.emit()
            Event.flush()

        # Assert
        assert any(
            "Exception in event handler" in record.message for record in caplog.records
        )


class TestEvent:
    """Tests for Event base class - all functionality."""

    # Basic structure and instantiation (EV-001 to EV-005)

    def test_event_is_abstract_base_class(self):
        """Test Event is declared as ABC.

        Given:
            Event is abstract base class
        When:
            Check inheritance and ABC marker
        Then:
            Event inherits from ABC
        """
        # Act & Assert
        assert issubclass(Event, ABC)
        # Note: Event can be instantiated because it has no abstract methods,
        # but it's not meant to be used directly - subclasses should define type

    def test_subclass_instantiation(self, concrete_event_class):
        """Test concrete Event subclass instantiation.

        Given:
            Concrete Event subclass with type attribute
        When:
            Instantiate the subclass
        Then:
            Instance created successfully
        """
        # Act
        event = concrete_event_class("test-event")

        # Assert
        assert event is not None
        assert isinstance(event, Event)

    def test_type_attribute_accessible(self, concrete_event_class):
        """Test type attribute is public and accessible.

        Given:
            Concrete Event subclass instance
        When:
            Access instance.type attribute
        Then:
            Returns the set event type string
        """
        # Arrange
        event = concrete_event_class("test-event")

        # Act
        result = event.type

        # Assert
        assert result == "test-event"

    def test_inherits_from_event_base_class(self, concrete_event_class):
        """Test subclass inherits from Event base class.

        Given:
            Concrete Event subclass instance
        When:
            Check isinstance(event, Event)
        Then:
            Returns True
        """
        # Arrange
        event = concrete_event_class("test-event")

        # Act
        result = isinstance(event, Event)

        # Assert
        assert result is True

    def test_implements_eventlike_protocol(self, concrete_event_class):
        """Test Event implements EventLike protocol.

        Given:
            Concrete Event subclass instance
        When:
            Check isinstance(event, EventLike)
        Then:
            Returns True
        """
        # Arrange
        event = concrete_event_class("test-event")

        # Act
        result = isinstance(event, EventLike)

        # Assert
        assert result is True

    # Handler registration (EV-006 to EV-012)

    def test_handler_decorator_registration(self, concrete_event_class):
        """Test handler decorator basic registration.

        Given:
            A concrete Event subclass
        When:
            Decorate a function with @Event.handler("event-type")
        Then:
            Handler is registered and function remains callable
        """

        # Arrange & Act
        @Event.handler("test-event")
        def test_handler(event, timestamp, context=None):
            pass

        # Assert
        assert "test-event" in Event._handlers
        assert test_handler in Event._handlers["test-event"]
        assert callable(test_handler)

    def test_handler_multi_type_registration(self):
        """Test handler registration for multiple event types.

        Given:
            A concrete Event subclass
        When:
            Decorate function with @Event.handler("type1", "type2")
        Then:
            Handler registered for multiple event types
        """

        # Arrange & Act
        @Event.handler("type1", "type2")
        def test_handler(event, timestamp, context=None):
            pass

        # Assert
        assert "type1" in Event._handlers
        assert "type2" in Event._handlers
        assert test_handler in Event._handlers["type1"]
        assert test_handler in Event._handlers["type2"]

    def test_multiple_handlers_per_type(self):
        """Test multiple handlers for same event type.

        Given:
            Handler registered for "test-event"
        When:
            Decorate another function for same event type
        Then:
            Both handlers registered for that type
        """

        # Arrange
        @Event.handler("test-event")
        def handler1(event, timestamp, context=None):
            pass

        # Act
        @Event.handler("test-event")
        def handler2(event, timestamp, context=None):
            pass

        # Assert
        assert len(Event._handlers["test-event"]) == 2
        assert handler1 in Event._handlers["test-event"]
        assert handler2 in Event._handlers["test-event"]

    def test_handler_isolation_between_subclasses(self):
        """Test handler isolation between Event subclasses.

        Given:
            Two different Event subclasses
        When:
            Register handlers on each subclass independently
        Then:
            Each subclass has separate handler registry
        """

        # Arrange
        class EventA(Event):
            def __init__(self):
                self.type = "event-a"

        class EventB(Event):
            def __init__(self):
                self.type = "event-b"

        # Act
        @EventA.handler("event-a")
        def handler_a(event, timestamp, context=None):
            pass

        @EventB.handler("event-b")
        def handler_b(event, timestamp, context=None):
            pass

        # Assert - Both use Event._handlers (class-level)
        assert "event-a" in Event._handlers
        assert "event-b" in Event._handlers

    def test_handler_registration(self):
        """Test 3-arg handler registration.

        Given:
            Handler function with 3 parameters (event, timestamp, context)
        When:
            Decorate with @Event.handler()
        Then:
            Handler registered successfully
        """

        # Arrange & Act
        @Event.handler("test-event")
        def handler(event, timestamp, context=None):
            pass

        # Assert
        assert "test-event" in Event._handlers
        assert handler in Event._handlers["test-event"]

    def test_handler_responds_to_all_registered_types(
        self, concrete_event_class, event_spy
    ):
        """Test handler invoked for all registered types.

        Given:
            Multiple event types specified
        When:
            Register handler via decorator
        Then:
            Handler invoked for any of the specified types
        """
        # Arrange
        Event._handlers["type1"] = [event_spy]
        Event._handlers["type2"] = [event_spy]

        event1 = concrete_event_class("type1")
        event2 = concrete_event_class("type2")

        # Act
        event1.emit()
        event2.emit()
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 2

    # Emission without handlers (EV-013 to EV-014)

    def test_no_op_emission_no_handlers(self, concrete_event_class):
        """Test no-op emission when no handlers registered.

        Given:
            Event instance with no registered handlers
        When:
            Call event.emit() with and without context
        Then:
            Method returns immediately with no error
        """
        # Arrange
        event = concrete_event_class("test-event")

        # Act & Assert - no exception raised
        event.emit()
        event.emit(context={"key": "value"})

    def test_type_specific_no_op(self, concrete_event_class, event_spy):
        """Test type-specific no-op emission.

        Given:
            Event type "type-a" with handlers, emitting "type-b"
        When:
            Call event.emit() for unregistered type
        Then:
            No handlers invoked, returns immediately
        """
        # Arrange
        Event._handlers["type-a"] = [event_spy]
        event = concrete_event_class("type-b")

        # Act
        event.emit()

        # Assert
        assert len(event_spy.calls) == 0

    def test_handler_receives_event_timestamp_context(
        self, concrete_event_class, event_spy
    ):
        """Test handler receives all three arguments.

        Given:
            Handler with signature (event, timestamp, context=None) registered
        When:
            Emit event without context
        Then:
            Handler called with (event, timestamp, None)
        """
        # Arrange
        Event._handlers["test-event"] = [event_spy]
        event = concrete_event_class("test-event")

        # Act
        event.emit()
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 1
        emitted_event, timestamp, context = event_spy.calls[0]
        assert emitted_event is event
        assert isinstance(timestamp, int)
        assert context is None

    def test_timestamp_generation(self, concrete_event_class, event_spy):
        """Test timestamp generation.

        Given:
            Handler registered
        When:
            Emit event
        Then:
            Handler receives positive integer timestamp
        """
        # Arrange
        Event._handlers["test-event"] = [event_spy]
        event = concrete_event_class("test-event")

        # Act
        event.emit()
        Event.flush()

        # Assert
        _, timestamp, _ = event_spy.calls[0]
        assert isinstance(timestamp, int)
        assert timestamp > 0

    def test_multiple_handler_invocation_order(self, concrete_event_class):
        """Test multiple handler invocation order.

        Given:
            Multiple handlers for same type
        When:
            Emit event
        Then:
            All handlers invoked in registration order
        """
        # Arrange
        call_order = []

        def handler1(event, timestamp, context=None):
            call_order.append(1)

        def handler2(event, timestamp, context=None):
            call_order.append(2)

        def handler3(event, timestamp, context=None):
            call_order.append(3)

        Event._handlers["test-event"] = [handler1, handler2, handler3]
        event = concrete_event_class("test-event")

        # Act
        event.emit()
        Event.flush()

        # Assert
        assert call_order == [1, 2, 3]

    # Handler emission (EV-020 to EV-022)

    def test_handler_without_context(self, concrete_event_class, event_spy):
        """Test handler without context.

        Given:
            Handler with signature (event, timestamp, context=None)
        When:
            Emit event without context and flush
        Then:
            Handler called with (event, timestamp, None)
        """
        # Arrange
        Event._handlers["test-event"] = [event_spy]
        event = concrete_event_class("test-event")

        # Act
        event.emit()
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 1
        emitted_event, timestamp, context = event_spy.calls[0]
        assert emitted_event is event
        assert isinstance(timestamp, int)
        assert context is None

    def test_handler_with_context(self, concrete_event_class, event_spy):
        """Test handler with context.

        Given:
            Handler with signature (event, timestamp, context=None)
        When:
            Emit event with context dict and flush
        Then:
            Handler called with (event, timestamp, context_dict)
        """
        # Arrange
        Event._handlers["test-event"] = [event_spy]
        event = concrete_event_class("test-event")
        context_data = {"proxy_id": "123", "extra": "data"}

        # Act
        event.emit(context=context_data)
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 1
        emitted_event, timestamp, context = event_spy.calls[0]
        assert context is context_data

    def test_explicit_none_context(self, concrete_event_class, event_spy):
        """Test explicit None context.

        Given:
            Handler registered
        When:
            Emit with context=None explicitly and flush
        Then:
            Handler receives None for context parameter
        """
        # Arrange
        Event._handlers["test-event"] = [event_spy]
        event = concrete_event_class("test-event")

        # Act
        event.emit(context=None)
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 1
        _, _, context = event_spy.calls[0]
        assert context is None

    # Mixed signatures (EV-023 to EV-024)

    # Exception handling (EV-025 to EV-027)

    def test_exception_isolation(self, concrete_event_class, caplog):
        """Test exception isolation via handler thread.

        Given:
            Handler that raises ValueError
        When:
            Emit event and flush
        Then:
            Exception is logged, does not propagate to caller
        """

        # Arrange
        def failing_handler(event, timestamp, context=None):
            raise ValueError("Handler failed")

        Event._handlers["test-event"] = [failing_handler]
        event = concrete_event_class("test-event")

        # Act - emit does not raise
        with caplog.at_level(logging.ERROR):
            event.emit()
            Event.flush()

        # Assert
        assert any(
            "Exception in event handler" in record.message for record in caplog.records
        )

    def test_exception_doesnt_stop_handler_chain(
        self,
        concrete_event_class,
        event_spy,
        caplog,
    ):
        """Test exception doesn't stop handler chain.

        Given:
            Two handlers: first raises exception, second is valid
        When:
            Emit event and flush
        Then:
            Both handlers invoked; exception in first doesn't stop second
        """

        # Arrange
        def failing_handler(event, timestamp, context=None):
            raise ValueError("First handler failed")

        Event._handlers["test-event"] = [failing_handler, event_spy]
        event = concrete_event_class("test-event")

        # Act
        with caplog.at_level(logging.ERROR):
            event.emit()
            Event.flush()

        # Assert - exception logged
        assert any(
            "Exception in event handler" in record.message for record in caplog.records
        )

        # Assert - second handler still called
        assert len(event_spy.calls) == 1

    def test_handler_manages_own_exceptions(self, concrete_event_class, caplog):
        """Test handler manages own exceptions.

        Given:
            Handler with try-except that catches its own error
        When:
            Emit event and flush
        Then:
            Handler completes without logging an exception
        """
        # Arrange
        handler_completed = []

        def self_managing_handler(event, timestamp, context=None):
            try:
                raise ValueError("Internal error")
            except ValueError:
                handler_completed.append(True)

        Event._handlers["test-event"] = [self_managing_handler]
        event = concrete_event_class("test-event")

        # Act
        with caplog.at_level(logging.ERROR):
            event.emit()
            Event.flush()

        # Assert - no exception logged
        assert not any(
            "Exception in event handler" in record.message for record in caplog.records
        )

        # Assert - handler completed
        assert len(handler_completed) == 1

    # Edge cases (EV-028 to EV-029)

    def test_class_level_handler_registry(self, concrete_event_class, event_spy):
        """Test class-level handler registry.

        Given:
            Handler registered after event instance created
        When:
            Emit existing event instance and flush
        Then:
            Handler still invoked (class-level registry)
        """
        # Arrange
        event = concrete_event_class("test-event")

        # Act - register handler after event instance created
        Event._handlers["test-event"] = [event_spy]
        event.emit()
        Event.flush()

        # Assert
        assert len(event_spy.calls) == 1

    def test_event_mutation_visibility(self, concrete_event_class):
        """Test event mutation visibility.

        Given:
            Handler that modifies event instance
        When:
            Emit to multiple handlers and flush
        Then:
            Subsequent handlers see modified state
        """
        # Arrange
        modifications = []

        def modifying_handler(event, timestamp, context=None):
            event.data["modified"] = True

        def observing_handler(event, timestamp, context=None):
            modifications.append(event.data.get("modified", False))

        Event._handlers["test-event"] = [modifying_handler, observing_handler]
        event = concrete_event_class("test-event")

        # Act
        event.emit()
        Event.flush()

        # Assert - second handler sees mutation
        assert len(modifications) == 1
        assert modifications[0] is True

    # Property-based tests (PBT-001 to PBT-004)

    @given(event_type=st.text())
    def test_handler_invocation_property(self, event_type):
        """Property-based test: handler invocation for any event type.

        Given:
            Any valid event type string (Hypothesis st.text() strategy)
        When:
            Register handler, emit, and flush
        Then:
            Handler invoked exactly once
        """

        # Arrange
        class TestEvent(Event):
            def __init__(self, type: str):
                self.type = type

        calls = []

        def spy(event, timestamp, context=None):
            calls.append((event, timestamp, context))

        Event._handlers.clear()
        Event._handlers[event_type] = [spy]
        event = TestEvent(event_type)

        # Act
        event.emit()
        Event.flush()

        # Assert
        assert len(calls) == 1

    @given(
        context=st.one_of(
            st.none(),
            st.dictionaries(st.text(), st.integers()),
            st.text(),
            st.integers(),
            st.binary(min_size=1000, max_size=10000),  # large objects
        )
    )
    def test_context_identity_property(self, context):
        """Property-based test: context identity across all types.

        Given:
            Arbitrary context types: None, dict, string, int, large objects
        When:
            Emit event with context and flush
        Then:
            Context received by handler equals emitted context (identity preserved)
        """

        # Arrange
        class TestEvent(Event):
            def __init__(self, type: str):
                self.type = type

        calls = []

        def spy(event, timestamp, context=None):
            calls.append((event, timestamp, context))

        Event._handlers.clear()
        Event._handlers["test-event"] = [spy]
        event = TestEvent("test-event")

        # Act
        event.emit(context=context)
        Event.flush()

        # Assert
        assert len(calls) == 1
        _, _, received_context = calls[0]
        assert received_context is context

    @given(num_handlers=st.integers(min_value=1, max_value=100))
    def test_handler_count_invariant(self, num_handlers):
        """Property-based test: handler count invariant.

        Given:
            N handlers registered (0 < N < 100, Hypothesis)
        When:
            Emit event and flush
        Then:
            All N handlers invoked exactly once each
        """

        # Arrange
        class TestEvent(Event):
            def __init__(self, type: str):
                self.type = type

        Event._handlers.clear()
        call_counts = []

        for i in range(num_handlers):
            calls = []
            call_counts.append(calls)

            def create_handler(call_list):
                def handler(event, timestamp, context=None):
                    call_list.append(1)

                return handler

            Event._handlers.setdefault("test-event", []).append(
                create_handler(call_counts[i])
            )

        event = TestEvent("test-event")

        # Act
        event.emit()
        Event.flush()

        # Assert
        for calls in call_counts:
            assert len(calls) == 1

    @given(num_emissions=st.integers(min_value=1, max_value=50))
    def test_emission_independence_property(self, num_emissions):
        """Property-based test: emission independence.

        Given:
            Event emitted M times (0 < M < 50, Hypothesis)
        When:
            Multiple emissions and flush
        Then:
            Each emission invokes all handlers independently
        """

        # Arrange
        class TestEvent(Event):
            def __init__(self, type: str):
                self.type = type

        calls = []

        def spy(event, timestamp, context=None):
            calls.append((event, timestamp, context))

        Event._handlers.clear()
        Event._handlers["test-event"] = [spy]
        event = TestEvent("test-event")

        # Act
        for _ in range(num_emissions):
            event.emit()

        Event.flush()

        # Assert
        assert len(calls) == num_emissions
