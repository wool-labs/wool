from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import Protocol

import cloudpickle
import grpc.aio
from grpc_interceptor.server import AsyncServerInterceptor

if TYPE_CHECKING:
    from wool.runtime.routine.task import Task

# Global registry for decorator-registered interceptors
_registered_interceptors: list[InterceptorLike] = []


# public
class InterceptorLike(Protocol):
    """Protocol defining the Wool interceptor interface.

    Interceptors are async generators that wrap task execution, allowing
    modification of tasks before dispatch and manipulation of response
    streams during execution.

    Interceptors execute in three phases:

    1. **Pre-dispatch**: Code before the first ``yield`` runs before the
       task is dispatched. The interceptor can yield a modified
       :class:`Task` or ``None`` to use the original task.

    2. **Stream processing**: The ``yield`` expression receives the response
       stream as an :class:`AsyncIterator`. The interceptor returns an async
       generator that wraps this stream, allowing events to be modified,
       filtered, or injected.

    3. **Cleanup**: Code after the ``return`` statement (in a ``finally``
       block) runs after stream completion or cancellation.

    **Basic logging interceptor:**

    .. code-block:: python

        async def log_interceptor(task: Task) -> AsyncGenerator:
            print(f"Starting task: {task.id}")

            # Yield None to use unmodified task
            response_stream = yield None

            # Wrap and yield events from the response stream
            try:
                async for event in response_stream:
                    print(f"Event: {event}")
                    yield event
            finally:
                print(f"Task complete: {task.id}")

    **Task modification interceptor:**

    .. code-block:: python

        async def rbac_interceptor(task: Task) -> AsyncGenerator:
            # Validate permissions before dispatch
            if not has_permission(task):
                raise PermissionError("Unauthorized")

            # Modify task metadata
            modified_task = task.with_metadata(user=current_user())
            response_stream = yield modified_task

            # Pass through all events
            async for event in response_stream:
                yield event

    **Stream filtering interceptor:**

    .. code-block:: python

        async def filter_interceptor(task: Task) -> AsyncGenerator:
            response_stream = yield None

            async for event in response_stream:
                # Filter out certain event types
                if should_include(event):
                    yield event

    **Error handling:**

    Exceptions raised by interceptors propagate to the client and cancel
    the stream. The gRPC call fails with the interceptor's exception.
    Applications must handle errors appropriately:

    .. code-block:: python

        async def safe_interceptor(task: Task) -> AsyncGenerator:
            try:
                response_stream = yield None
                async for event in response_stream:
                    yield event
            except Exception as e:
                # Log error, send alert, etc.
                logger.error(f"Task failed: {e}")
                # Re-raise to propagate to client
                raise

    :param task:
        The work task being dispatched.
    :returns:
        An async generator that yields the modified task (or None) and
        returns an async iterator wrapping the response stream.
    """

    def __call__(
        self, task: Task
    ) -> AsyncGenerator[Task | None, AsyncIterator | None]: ...


# public
def interceptor(func: InterceptorLike) -> InterceptorLike:
    """Register a Wool interceptor globally.

    Decorated interceptors are automatically applied to all workers that
    don't specify explicit interceptors. Use this for cross-cutting
    concerns like logging, metrics, or distributed tracing.

    **Usage:**

    .. code-block:: python

        from wool.runtime.routine.interceptor import interceptor


        @interceptor
        async def metrics_interceptor(task):
            start_time = time.time()
            response_stream = yield None

            try:
                async for event in response_stream:
                    yield event
            finally:
                duration = time.time() - start_time
                record_metric("task_duration", duration)

    Workers automatically include registered interceptors:

    .. code-block:: python

        # This worker uses metrics_interceptor automatically
        worker = LocalWorker("my-worker")

    To use only explicit interceptors (ignoring registered ones):

    .. code-block:: python

        # Only use explicit interceptors, not registered ones
        worker = LocalWorker("my-worker", interceptors=[custom_interceptor])

    :param func:
        The interceptor function to register.
    :returns:
        The original function, unchanged.
    """
    _registered_interceptors.append(func)
    return func


def get_registered_interceptors() -> list[InterceptorLike]:
    """Get all globally registered interceptors.

    :returns:
        List of interceptors registered with the :func:`@interceptor
        <interceptor>` decorator.
    """
    return _registered_interceptors.copy()


class WoolInterceptor(AsyncServerInterceptor):
    """Bridges Wool interceptors to gRPC's interceptor interface.

    Converts high-level Wool interceptor semantics (task modification,
    stream wrapping) into gRPC's low-level interceptor protocol. Only
    applies to ``dispatch`` RPC calls (unary-stream).

    This class is an implementation detail - users should work with
    :class:`WoolInterceptor` functions and the :func:`@interceptor
    <interceptor>` decorator.

    :param interceptors:
        Wool interceptor functions to apply.
    """

    def __init__(self, interceptors: list[InterceptorLike]):
        self._interceptors = interceptors

    async def intercept(
        self,
        method: Callable[[Any, grpc.aio.ServicerContext], Awaitable[Any]],
        request_or_iterator: Any,
        context: grpc.aio.ServicerContext,
        method_name: str,
    ) -> Any:
        """Intercept gRPC service calls.

        Only applies interceptors to ``dispatch`` calls. Other RPC methods
        (like ``stop``) bypass interception.

        :param method:
            The gRPC service method being called.
        :param request_or_iterator:
            The request object or request iterator.
        :param context:
            The gRPC servicer context.
        :param method_name:
            The name of the method being called (e.g.,
            "/wool.Worker/dispatch").
        :returns:
            The response or response iterator from the method.
        """
        # Exit early if no interceptors registered
        if not self._interceptors:
            return await method(request_or_iterator, context)

        # Only intercept dispatch calls
        if not method_name.endswith("/dispatch"):
            return await method(request_or_iterator, context)

        # Deserialize task from protobuf request
        task: Task = cloudpickle.loads(request_or_iterator.task)

        # Apply all interceptors in order, keeping generators alive
        modified_task = task
        active_generators = []
        for interceptor_func in self._interceptors:
            try:
                # Start the interceptor generator
                gen = interceptor_func(modified_task)

                # Advance to first yield - get potentially modified task
                task_modification = await gen.asend(None)

                # Update task if interceptor returned a modified version
                if task_modification is not None:
                    modified_task = task_modification

                # Store generator for stream wrapping phase
                active_generators.append(gen)

            except StopAsyncIteration:
                # Interceptor didn't yield - treat as passthrough
                active_generators.append(None)
            except Exception:
                # Interceptor raised error - propagate to caller
                raise

        # If task was modified, update the protobuf request
        if modified_task is not task:
            # Create new request with modified task
            request_or_iterator.task = cloudpickle.dumps(modified_task)

        # Call the actual dispatch method
        response_stream = await method(request_or_iterator, context)

        # Wrap response stream with interceptors (in reverse order)
        for gen in reversed(active_generators):
            # Skip interceptors that didn't create generators
            if gen is None:
                continue

            try:
                # Send the response stream - generator will start yielding events
                try:
                    first_event = await gen.asend(response_stream)

                    # The generator is now yielding events - wrap it
                    async def _create_wrapped_stream(
                        generator: AsyncGenerator,
                        first: Any,
                    ) -> AsyncGenerator:
                        yield first
                        async for event in generator:
                            yield event

                    response_stream = _create_wrapped_stream(gen, first_event)
                except StopAsyncIteration:
                    # Generator finished without yielding - use original stream
                    pass

            except Exception:
                # Stream wrapping failed - propagate to caller
                raise

        return response_stream
