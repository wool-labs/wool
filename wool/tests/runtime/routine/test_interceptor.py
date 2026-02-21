from __future__ import annotations

import cloudpickle
import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.routine.interceptor import WoolInterceptor
from wool.runtime.routine.interceptor import get_registered_interceptors
from wool.runtime.routine.interceptor import interceptor


@pytest.fixture(autouse=True)
def clear_interceptor_registry():
    """Clear the global interceptor registry before and after each test."""
    from wool.runtime.routine.interceptor import _registered_interceptors

    _registered_interceptors.clear()
    yield
    _registered_interceptors.clear()


@pytest.fixture
def sample_task(mocker):
    """Provide a mock Task for testing."""
    task = mocker.MagicMock()
    task.id = "test-task-123"
    task.callable = mocker.MagicMock()
    task.args = ()
    task.kwargs = {}
    return task


@pytest.fixture
def mock_grpc_context(mocker):
    """Provide a mock gRPC context."""
    context = mocker.MagicMock()
    context.cancel = mocker.MagicMock()
    return context


@pytest.fixture
def mock_request(sample_task, mocker):
    """Provide a mock protobuf request with serialized task."""
    request = mocker.MagicMock()
    request.task = cloudpickle.dumps(sample_task)
    return request


async def create_mock_response_stream(*events):
    """Create a mock async response stream."""
    for event in events:
        yield event


def create_passthrough_interceptor():
    """Create a passthrough interceptor that yields None."""

    async def passthrough(task):
        response_stream = yield None
        async for event in response_stream:
            yield event

    return passthrough


def create_task_modifying_interceptor(modification_fn):
    """Create an interceptor that modifies the task."""

    async def modifier(task):
        modified_task = modification_fn(task)
        response_stream = yield modified_task
        async for event in response_stream:
            yield event

    return modifier


def create_stream_wrapping_interceptor(wrapper_fn):
    """Create an interceptor that wraps the response stream."""

    async def wrapper(task):
        response_stream = yield None
        async for event in response_stream:
            yield wrapper_fn(event)

    return wrapper


def create_failing_interceptor(exception, fail_stage="pre-dispatch"):
    """Create an interceptor that raises an exception."""

    async def failing(task):
        if fail_stage == "pre-dispatch":
            raise exception
        response_stream = yield None
        if fail_stage == "stream-wrapping":
            raise exception
        async for event in response_stream:
            yield event

    return failing


def create_order_tracking_interceptor(interceptor_id, order_list, phase="both"):
    """Create an interceptor that tracks execution order."""

    async def order_tracker(task):
        if phase in ("both", "pre-dispatch"):
            order_list.append(("pre", interceptor_id))
        response_stream = yield None
        if phase in ("both", "stream-wrapping"):
            order_list.append(("stream", interceptor_id))
        async for event in response_stream:
            yield event

    return order_tracker


async def collect_stream_events(stream):
    """Collect all events from an async iterator."""
    events = []
    async for event in stream:
        events.append(event)
    return events


@st.composite
def valid_passthrough_interceptor_list(draw):
    """Generate a list of 0-5 passthrough interceptors."""
    count = draw(st.integers(min_value=0, max_value=5))
    return [create_passthrough_interceptor() for _ in range(count)]


@st.composite
def event_stream_strategy(draw):
    """Generate lists of 0-100 hashable events."""
    return draw(
        st.lists(
            st.one_of(st.text(), st.integers(), st.tuples(st.text(), st.integers())),
            min_size=0,
            max_size=100,
        )
    )


class TestInterceptorDecorator:
    """Tests for the @interceptor decorator."""

    def test_registers_function_in_global_registry(self):
        """Interceptor decorator registers function in global registry.

        Given:
            An interceptor function
        When:
            Decorated with @interceptor
        Then:
            The function is added to the global registry
        """

        async def my_interceptor(task):
            response_stream = yield None
            async for event in response_stream:
                yield event

        decorated = interceptor(my_interceptor)

        assert my_interceptor in get_registered_interceptors()
        assert decorated is my_interceptor

    def test_returns_original_function_unchanged(self):
        """Interceptor decorator returns original function unchanged.

        Given:
            An interceptor function
        When:
            Decorated with @interceptor
        Then:
            The original function is returned unchanged
        """

        async def my_interceptor(task):
            response_stream = yield None
            async for event in response_stream:
                yield event

        original_id = id(my_interceptor)
        decorated = interceptor(my_interceptor)

        assert id(decorated) == original_id
        assert decorated is my_interceptor

    def test_registers_multiple_functions_in_order(self):
        """Interceptor decorator registers multiple functions in order.

        Given:
            Multiple interceptor functions
        When:
            Each decorated with @interceptor
        Then:
            All functions are added to the registry in order
        """

        async def interceptor1(task):
            response_stream = yield None
            async for event in response_stream:
                yield event

        async def interceptor2(task):
            response_stream = yield None
            async for event in response_stream:
                yield event

        async def interceptor3(task):
            response_stream = yield None
            async for event in response_stream:
                yield event

        interceptor(interceptor1)
        interceptor(interceptor2)
        interceptor(interceptor3)

        registered = get_registered_interceptors()
        assert len(registered) == 3
        assert registered[0] is interceptor1
        assert registered[1] is interceptor2
        assert registered[2] is interceptor3

    def test_allows_duplicate_registration(self):
        """Interceptor decorator allows duplicate registration.

        Given:
            The same interceptor function
        When:
            Decorated with @interceptor twice
        Then:
            The function appears twice in the registry
        """

        async def my_interceptor(task):
            response_stream = yield None
            async for event in response_stream:
                yield event

        interceptor(my_interceptor)
        interceptor(my_interceptor)

        registered = get_registered_interceptors()
        assert len(registered) == 2
        assert registered[0] is my_interceptor
        assert registered[1] is my_interceptor


class TestGetRegisteredInterceptors:
    """Tests for get_registered_interceptors()."""

    def test_returns_empty_list_when_no_interceptors(self):
        """Get registered interceptors returns empty list when none registered.

        Given:
            No registered interceptors
        When:
            get_registered_interceptors() is called
        Then:
            An empty list is returned
        """
        result = get_registered_interceptors()

        assert result == []
        assert isinstance(result, list)

    def test_returns_single_registered_interceptor(self):
        """Get registered interceptors returns single registered interceptor.

        Given:
            One registered interceptor
        When:
            get_registered_interceptors() is called
        Then:
            A list containing the interceptor is returned
        """

        async def my_interceptor(task):
            response_stream = yield None
            async for event in response_stream:
                yield event

        interceptor(my_interceptor)

        result = get_registered_interceptors()
        assert len(result) == 1
        assert result[0] is my_interceptor

    def test_returns_multiple_interceptors_in_order(self):
        """Get registered interceptors returns all in registration order.

        Given:
            Multiple registered interceptors
        When:
            get_registered_interceptors() is called
        Then:
            A list with all interceptors in registration order is returned
        """

        async def interceptor1(task):
            response_stream = yield None
            async for event in response_stream:
                yield event

        async def interceptor2(task):
            response_stream = yield None
            async for event in response_stream:
                yield event

        interceptor(interceptor1)
        interceptor(interceptor2)

        result = get_registered_interceptors()
        assert len(result) == 2
        assert result[0] is interceptor1
        assert result[1] is interceptor2

    def test_returns_copy_not_reference(self):
        """Get registered interceptors returns copy not reference.

        Given:
            Registered interceptors exist
        When:
            get_registered_interceptors() is called and list is modified
        Then:
            The original registry is not affected
        """

        async def my_interceptor(task):
            response_stream = yield None
            async for event in response_stream:
                yield event

        interceptor(my_interceptor)

        result1 = get_registered_interceptors()
        result1.append("fake")  # type: ignore

        result2 = get_registered_interceptors()
        assert len(result2) == 1
        assert result2[0] is my_interceptor


class TestWoolInterceptor:
    """Tests for the WoolInterceptorBridge class."""

    # ------------------------------------------------------------------------
    # Instantiation Tests
    # ------------------------------------------------------------------------

    def test_stores_interceptors_on_instantiation(self):
        """WoolInterceptorBridge stores interceptors on instantiation.

        Given:
            A list of interceptor functions
        When:
            WoolInterceptorBridge is instantiated
        Then:
            The bridge stores the interceptors
        """
        interceptor1 = create_passthrough_interceptor()
        interceptor2 = create_passthrough_interceptor()

        bridge = WoolInterceptor([interceptor1, interceptor2])

        assert bridge._interceptors == [interceptor1, interceptor2]

    def test_instantiates_with_empty_list(self):
        """WoolInterceptorBridge instantiates with empty list.

        Given:
            An empty interceptor list
        When:
            WoolInterceptorBridge is instantiated
        Then:
            The bridge is created successfully
        """
        bridge = WoolInterceptor([])

        assert bridge._interceptors == []

    # ------------------------------------------------------------------------
    # Early Exit and Bypass Tests
    # ------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_exits_early_with_no_interceptors(self, mock_grpc_context, mocker):
        """WoolInterceptorBridge exits early with no interceptors.

        Given:
            A bridge with no interceptors
        When:
            intercept() is called for a dispatch method
        Then:
            The method is called directly without processing
        """
        bridge = WoolInterceptor([])

        mock_method = mocker.AsyncMock(return_value="direct_result")
        mock_request = mocker.MagicMock()

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        assert result == "direct_result"
        mock_method.assert_called_once_with(mock_request, mock_grpc_context)
        mock_grpc_context.cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_bypasses_non_dispatch_methods(self, mock_grpc_context, mocker):
        """WoolInterceptorBridge bypasses non-dispatch methods.

        Given:
            A bridge with interceptors
        When:
            intercept() is called for a non-dispatch method
        Then:
            The method is called directly without interceptors
        """

        async def failing_interceptor(task):
            raise RuntimeError("Should not be called")

        bridge = WoolInterceptor([failing_interceptor])
        mock_method = mocker.AsyncMock(return_value="stop_result")
        mock_request = mocker.MagicMock()

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/stop"
        )

        assert result == "stop_result"
        mock_method.assert_called_once_with(mock_request, mock_grpc_context)

    # ------------------------------------------------------------------------
    # Task Modification Tests
    # ------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_passthrough_interceptor_yields_none(
        self, sample_task, mock_request, mock_grpc_context
    ):
        """Passthrough interceptor yields None dispatches task unmodified.

        Given:
            A bridge with a passthrough interceptor that yields None
        When:
            intercept() is called for a dispatch method
        Then:
            The task is dispatched unmodified
        """
        passthrough = create_passthrough_interceptor()
        bridge = WoolInterceptor([passthrough])

        async def mock_method(req, ctx):
            # Verify task wasn't modified
            task = cloudpickle.loads(req.task)
            assert task.id == sample_task.id
            return create_mock_response_stream("event1", "event2")

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        events = await collect_stream_events(result)
        assert events == ["event1", "event2"]

    @pytest.mark.asyncio
    async def test_task_modification_via_yield(
        self, sample_task, mock_request, mock_grpc_context, mocker
    ):
        """Task modification via yield serializes modified task.

        Given:
            A bridge with an interceptor that yields a modified task
        When:
            intercept() is called for a dispatch method
        Then:
            The modified task is serialized and dispatched
        """

        def modify_task(task):
            modified = mocker.MagicMock()
            modified.id = "modified-task-456"
            modified.callable = task.callable
            modified.args = task.args
            modified.kwargs = task.kwargs
            return modified

        modifier = create_task_modifying_interceptor(modify_task)
        bridge = WoolInterceptor([modifier])

        async def mock_method(req, ctx):
            task = cloudpickle.loads(req.task)
            assert task.id == "modified-task-456"
            return create_mock_response_stream("event1")

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        events = await collect_stream_events(result)
        assert events == ["event1"]

    @pytest.mark.asyncio
    async def test_multiple_passthrough_interceptors(
        self, sample_task, mock_request, mock_grpc_context
    ):
        """Multiple passthrough interceptors process task in order.

        Given:
            A bridge with multiple interceptors that yield None
        When:
            intercept() is called for a dispatch method
        Then:
            All interceptors process the task in order
        """
        order = []

        def create_tracking_passthrough(idx):
            async def tracker(task):
                order.append(f"pre-{idx}")
                response_stream = yield None
                order.append(f"stream-{idx}")
                async for event in response_stream:
                    yield event

            return tracker

        bridge = WoolInterceptor(
            [
                create_tracking_passthrough(1),
                create_tracking_passthrough(2),
                create_tracking_passthrough(3),
            ]
        )

        async def mock_method(req, ctx):
            return create_mock_response_stream("event")

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        await collect_stream_events(result)

        # Pre-dispatch: forward order, stream-wrapping: reverse order
        assert order == [
            "pre-1",
            "pre-2",
            "pre-3",
            "stream-3",
            "stream-2",
            "stream-1",
        ]

    @pytest.mark.asyncio
    async def test_chained_task_modification(
        self, sample_task, mock_request, mock_grpc_context, mocker
    ):
        """Chained task modification passes through modifications.

        Given:
            A bridge with multiple task-modifying interceptors
        When:
            intercept() is called for a dispatch method
        Then:
            Each interceptor receives task modified by previous ones
        """
        modifications = []

        def create_modifier(suffix):
            def modify(task):
                modified = mocker.MagicMock()
                modified.id = task.id + f"-{suffix}"
                modified.callable = task.callable
                modified.args = task.args
                modified.kwargs = task.kwargs
                modifications.append(modified.id)
                return modified

            return create_task_modifying_interceptor(modify)

        bridge = WoolInterceptor([create_modifier("A"), create_modifier("B")])

        async def mock_method(req, ctx):
            task = cloudpickle.loads(req.task)
            assert task.id == "test-task-123-A-B"
            return create_mock_response_stream("event")

        await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        assert modifications == ["test-task-123-A", "test-task-123-A-B"]

    # ------------------------------------------------------------------------
    # Stream Wrapping Tests
    # ------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_stream_wrapping(self, sample_task, mock_request, mock_grpc_context):
        """Stream wrapping returns events from wrapped stream.

        Given:
            A bridge with an interceptor that returns a wrapped stream
        When:
            intercept() is called for a dispatch method
        Then:
            Events from the wrapped stream are returned
        """
        wrapper = create_stream_wrapping_interceptor(lambda event: f"wrapped-{event}")
        bridge = WoolInterceptor([wrapper])

        async def mock_method(req, ctx):
            return create_mock_response_stream("event1", "event2")

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        events = await collect_stream_events(result)
        assert events == ["wrapped-event1", "wrapped-event2"]

    @pytest.mark.asyncio
    async def test_stream_filtering(self, sample_task, mock_request, mock_grpc_context):
        """Stream filtering returns only filtered events.

        Given:
            A bridge with an interceptor that filters stream events
        When:
            intercept() is called for a dispatch method
        Then:
            Only filtered events are returned to the client
        """

        async def filtering_interceptor(task):
            response_stream = yield None
            async for event in response_stream:
                if "keep" in event:
                    yield event

        bridge = WoolInterceptor([filtering_interceptor])

        async def mock_method(req, ctx):
            return create_mock_response_stream("keep1", "drop", "keep2", "drop")

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        events = await collect_stream_events(result)
        assert events == ["keep1", "keep2"]

    @pytest.mark.asyncio
    async def test_stream_event_injection(
        self, sample_task, mock_request, mock_grpc_context
    ):
        """Stream event injection returns original and injected events.

        Given:
            A bridge with an interceptor that injects additional events
        When:
            intercept() is called for a dispatch method
        Then:
            Both original and injected events are returned
        """

        async def injecting_interceptor(task):
            response_stream = yield None
            yield "injected-start"
            async for event in response_stream:
                yield event
            yield "injected-end"

        bridge = WoolInterceptor([injecting_interceptor])

        async def mock_method(req, ctx):
            return create_mock_response_stream("event1", "event2")

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        events = await collect_stream_events(result)
        assert events == ["injected-start", "event1", "event2", "injected-end"]

    @pytest.mark.asyncio
    async def test_multiple_stream_wrappers_reverse_order(
        self, sample_task, mock_request, mock_grpc_context
    ):
        """Multiple stream wrappers wrap stream in reverse order.

        Given:
            A bridge with multiple stream-wrapping interceptors
        When:
            intercept() is called for a dispatch method
        Then:
            Interceptors wrap the stream in reverse order
        """
        wrapper1 = create_stream_wrapping_interceptor(lambda e: f"[{e}]")
        wrapper2 = create_stream_wrapping_interceptor(lambda e: f"<{e}>")
        wrapper3 = create_stream_wrapping_interceptor(lambda e: f"{{{e}}}")

        bridge = WoolInterceptor([wrapper1, wrapper2, wrapper3])

        async def mock_method(req, ctx):
            return create_mock_response_stream("x")

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        events = await collect_stream_events(result)
        # Reverse order: wrapper3, wrapper2, wrapper1
        assert events == ["[<{x}>]"]

    # ------------------------------------------------------------------------
    # Error Handling Tests
    # ------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_pre_dispatch_exception_propagation(
        self, sample_task, mock_request, mock_grpc_context
    ):
        """Pre-dispatch exception propagates to caller.

        Given:
            A bridge with interceptor that raises exception before yielding
        When:
            intercept() is called for a dispatch method
        Then:
            The exception propagates to the caller
        """
        failing = create_failing_interceptor(ValueError("test error"), "pre-dispatch")
        bridge = WoolInterceptor([failing])

        async def mock_method(req, ctx):
            return create_mock_response_stream("should-not-reach")

        with pytest.raises(ValueError, match="test error"):
            await bridge.intercept(
                mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
            )

    @pytest.mark.asyncio
    async def test_pre_dispatch_stop_async_iteration(
        self, sample_task, mock_request, mock_grpc_context
    ):
        """Pre-dispatch StopAsyncIteration treats as passthrough.

        Given:
            A bridge with interceptor that raises StopAsyncIteration
        When:
            intercept() is called for a dispatch method
        Then:
            The interceptor is treated as a passthrough
        """

        async def stop_iteration_interceptor(task):
            # Make this an async generator by using yield
            if False:
                yield
            # This will cause StopAsyncIteration when asend(None) is called

        bridge = WoolInterceptor([stop_iteration_interceptor])

        async def mock_method(req, ctx):
            return create_mock_response_stream("event")

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        events = await collect_stream_events(result)
        assert events == ["event"]

    @pytest.mark.asyncio
    async def test_stream_wrapping_exception_propagation(
        self, sample_task, mock_request, mock_grpc_context
    ):
        """Stream wrapping exception propagates to caller.

        Given:
            A bridge with interceptor raising exception during wrapping
        When:
            intercept() is called for a dispatch method
        Then:
            The exception propagates to the caller
        """
        failing = create_failing_interceptor(
            RuntimeError("stream error"), "stream-wrapping"
        )
        bridge = WoolInterceptor([failing])

        async def mock_method(req, ctx):
            return create_mock_response_stream("event")

        with pytest.raises(RuntimeError, match="stream error"):
            await bridge.intercept(
                mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
            )

    @pytest.mark.asyncio
    async def test_stream_wrapping_stop_async_iteration(
        self, sample_task, mock_request, mock_grpc_context
    ):
        """Stream wrapping StopAsyncIteration uses original stream.

        Given:
            A bridge with interceptor raising StopAsyncIteration wrapping
        When:
            intercept() is called for a dispatch method
        Then:
            The original stream is used
        """

        async def stop_iteration_wrapper(task):
            yield None
            # Just return without yielding events - causes StopAsyncIteration
            return

        bridge = WoolInterceptor([stop_iteration_wrapper])

        async def mock_method(req, ctx):
            return create_mock_response_stream("event1", "event2")

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        events = await collect_stream_events(result)
        assert events == ["event1", "event2"]

    @pytest.mark.asyncio
    async def test_dispatch_method_exception_propagation(
        self, sample_task, mock_request, mock_grpc_context
    ):
        """Dispatch method exception propagates to caller.

        Given:
            A bridge with an interceptor
        When:
            The underlying dispatch method raises an exception
        Then:
            The exception propagates to the caller
        """
        passthrough = create_passthrough_interceptor()
        bridge = WoolInterceptor([passthrough])

        async def failing_method(req, ctx):
            raise RuntimeError("dispatch failed")

        with pytest.raises(RuntimeError, match="dispatch failed"):
            await bridge.intercept(
                failing_method,
                mock_request,
                mock_grpc_context,
                "/wool.Worker/dispatch",
            )

    # ------------------------------------------------------------------------
    # Full Lifecycle Test
    # ------------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_full_interceptor_lifecycle(
        self, sample_task, mock_request, mock_grpc_context, mocker
    ):
        """Full interceptor lifecycle applies modifications then wrapping.

        Given:
            A bridge with interceptors that modify tasks and wrap streams
        When:
            intercept() is called for a dispatch method
        Then:
            Task modifications apply before dispatch, stream wrapping after
        """
        lifecycle_events = []

        async def full_lifecycle_interceptor(task):
            lifecycle_events.append("pre-dispatch")

            # Modify task
            modified = mocker.MagicMock()
            modified.id = "lifecycle-modified"
            modified.callable = task.callable
            modified.args = task.args
            modified.kwargs = task.kwargs

            response_stream = yield modified

            lifecycle_events.append("stream-wrapping")

            async for event in response_stream:
                yield f"wrapped-{event}"

        bridge = WoolInterceptor([full_lifecycle_interceptor])

        async def mock_method(req, ctx):
            task = cloudpickle.loads(req.task)
            lifecycle_events.append(f"dispatch-{task.id}")
            return create_mock_response_stream("event")

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        events = await collect_stream_events(result)

        assert lifecycle_events == [
            "pre-dispatch",
            "dispatch-lifecycle-modified",
            "stream-wrapping",
        ]
        assert events == ["wrapped-event"]

    # ------------------------------------------------------------------------
    # Property-Based Tests
    # ------------------------------------------------------------------------

    @pytest.mark.asyncio
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(interceptor_list=valid_passthrough_interceptor_list())
    async def test_passthrough_idempotency_property(
        self, sample_task, mock_request, mock_grpc_context, interceptor_list
    ):
        """Property-based test: Passthrough interceptor idempotency.

        Given:
            Any list of valid passthrough interceptors
        When:
            intercept() is called for dispatch
        Then:
            All events from original stream returned unchanged
        """
        bridge = WoolInterceptor(interceptor_list)

        original_events = ["event1", "event2", "event3"]

        async def mock_method(req, ctx):
            return create_mock_response_stream(*original_events)

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        output_events = await collect_stream_events(result)
        assert output_events == original_events

    @pytest.mark.asyncio
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(
        events=event_stream_strategy(),
        passthrough_count=st.integers(min_value=0, max_value=5),
    )
    async def test_event_count_preservation_property(
        self, sample_task, mock_request, mock_grpc_context, events, passthrough_count
    ):
        """Property-based test: Event count preservation.

        Given:
            Any event stream (0-100 events) and 0-5 passthrough interceptors
        When:
            Events flow through the interceptor chain
        Then:
            Output event count exactly equals input event count
        """
        interceptors = [
            create_passthrough_interceptor() for _ in range(passthrough_count)
        ]
        bridge = WoolInterceptor(interceptors)

        async def mock_method(req, ctx):
            return create_mock_response_stream(*events)

        result = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )

        output_events = await collect_stream_events(result)
        assert len(output_events) == len(events)

    @pytest.mark.asyncio
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(interceptor_count=st.integers(min_value=1, max_value=10))
    async def test_interceptor_ordering_determinism_property(
        self, sample_task, mock_request, mock_grpc_context, interceptor_count
    ):
        """Property-based test: Interceptor ordering determinism.

        Given:
            Any Task and 1-10 order-tracking interceptors
        When:
            intercept() called for dispatch multiple times with same interceptors
        Then:
            Pre-dispatch forward order, stream-wrapping reverse order
        """
        # Create order-tracking interceptors
        order1 = []
        order2 = []

        interceptors = [
            create_order_tracking_interceptor(i, order1)
            for i in range(interceptor_count)
        ]

        bridge = WoolInterceptor(interceptors)

        async def mock_method(req, ctx):
            return create_mock_response_stream("event")

        # First execution
        result1 = await bridge.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )
        await collect_stream_events(result1)

        # Second execution with new order tracking
        interceptors2 = [
            create_order_tracking_interceptor(i, order2)
            for i in range(interceptor_count)
        ]
        bridge2 = WoolInterceptor(interceptors2)

        result2 = await bridge2.intercept(
            mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
        )
        await collect_stream_events(result2)

        # Verify deterministic ordering
        assert order1 == order2

        # Verify forward order for pre-dispatch
        pre_dispatch_order = [item[1] for item in order1 if item[0] == "pre"]
        assert pre_dispatch_order == list(range(interceptor_count))

        # Verify reverse order for stream-wrapping
        stream_order = [item[1] for item in order1 if item[0] == "stream"]
        assert stream_order == list(reversed(range(interceptor_count)))

    @pytest.mark.asyncio
    @settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(
        exception_type=st.sampled_from(
            [ValueError, RuntimeError, PermissionError, TypeError, OSError]
        ),
        interceptor_position=st.integers(min_value=0, max_value=4),
        fail_stage=st.sampled_from(["pre-dispatch", "stream-wrapping"]),
    )
    async def test_error_propagation_universality_property(
        self,
        sample_task,
        mock_request,
        mock_grpc_context,
        exception_type,
        interceptor_position,
        fail_stage,
    ):
        """Property-based test: Error propagation universality.

        Given:
            Any exception type at any interceptor position in any phase
        When:
            intercept() is called for dispatch
        Then:
            Exception propagates to caller
        """
        # Create interceptor chain with one failing interceptor
        interceptors = []
        for i in range(5):
            if i == interceptor_position:
                interceptors.append(
                    create_failing_interceptor(exception_type("test error"), fail_stage)
                )
            else:
                interceptors.append(create_passthrough_interceptor())

        bridge = WoolInterceptor(interceptors)

        async def mock_method(req, ctx):
            return create_mock_response_stream("event")

        # Verify exception propagates
        with pytest.raises(exception_type, match="test error"):
            result = await bridge.intercept(
                mock_method, mock_request, mock_grpc_context, "/wool.Worker/dispatch"
            )
            # For stream-wrapping errors, we need to consume the stream
            if fail_stage == "stream-wrapping":
                await collect_stream_events(result)
