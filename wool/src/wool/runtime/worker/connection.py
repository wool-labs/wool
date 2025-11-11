from __future__ import annotations

import asyncio
from typing import AsyncIterator
from typing import Final
from typing import Generic
from typing import TypeAlias
from typing import TypeVar

import cloudpickle
import grpc.aio

from wool.runtime import protobuf as pb
from wool.runtime.work import WorkTask

_DispatchCall: TypeAlias = grpc.aio.UnaryStreamCall[pb.task.Task, pb.worker.Response]

_T = TypeVar("_T")


class _DispatchStream(Generic[_T]):
    """Async iterator wrapper for streaming task results from workers.

    Handles iteration over gRPC response streams and deserializes
    task results or raises exceptions received from remote workers.

    :param call:
        The underlying gRPC response stream.
    """

    def __init__(self, call: _DispatchCall):
        self._call = call
        self._iter = aiter(call)

    def __aiter__(self) -> AsyncIterator[_T]:
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> _T:
        """Get the next response from the stream.

        :returns:
            The next task result from the worker.
        :raises StopAsyncIteration:
            When the stream is exhausted.
        :raises UnexpectedResponse:
            If the response is neither a result nor an exception.
        :raises Exception:
            Any exception raised by the task execution is re-raised.
        """
        try:
            response = await anext(self._iter)
            if response.HasField("result"):
                return cloudpickle.loads(response.result.dump)
            elif response.HasField("exception"):
                raise cloudpickle.loads(response.exception.dump)
            else:
                raise UnexpectedResponse(
                    f"Expected 'result' or 'exception' response, "
                    f"received '{response.WhichOneof('payload')}'"
                )
        except Exception:
            try:
                self._call.cancel()
            except Exception:
                pass
            raise


# public
class UnexpectedResponse(Exception):
    """Raised when a worker returns an unexpected response type.

    This exception indicates a protocol violation where the worker's
    response doesn't match the expected format (e.g., missing acknowledgment
    or returning an unrecognized payload type).
    """


# public
class RpcError(Exception):
    """Raised when a gRPC call to a worker fails with a non-transient error.

    Non-transient errors indicate persistent issues with the worker that
    are unlikely to be resolved by retrying (e.g., invalid arguments,
    unimplemented methods, permission denied).
    """


# public
class TransientRpcError(RpcError):
    """Raised when a gRPC call to a worker fails with a transient error.

    Transient errors indicate temporary issues that may be resolved by
    retrying the operation, such as:

    - ``UNAVAILABLE``: Worker temporarily unavailable
    - ``DEADLINE_EXCEEDED``: Request took too long
    - ``RESOURCE_EXHAUSTED``: Worker temporarily overloaded
    """


# public
class WorkerConnection:
    """gRPC connection to a worker for task dispatch.

    Maintains a persistent channel to a worker with concurrency limiting.
    Handles connection lifecycle and error classification (transient vs
    permanent failures).

    **Usage:**

    .. code-block:: python

        conn = WorkerConnection("localhost:50051")
        async for result in conn.dispatch(task):
            process(result)
        await conn.close()

    :param target:
        Worker URI. Supports multiple formats:

        - ``host:port`` - DNS name or IP with port
        - ``dns://host:port`` - Explicit DNS resolution
        - ``ipv4:address:port`` - IPv4 address
        - ``ipv6:[address]:port`` - IPv6 address
        - ``unix:path`` - Unix domain socket

        Examples: ``localhost:50051``, ``192.0.2.1:50051``
    :param limit:
        Maximum concurrent task dispatches.
    """

    TRANSIENT_ERRORS: Final = {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }

    def __init__(
        self,
        target: str,
        *,
        limit: int = 100,
    ):
        if limit <= 0:
            raise ValueError("Limit must be positive")
        self._channel = grpc.aio.insecure_channel(
            target,
            options=[
                ("grpc.keepalive_time_ms", 10000),
                ("grpc.keepalive_timeout_ms", 5000),
                ("grpc.http2.max_pings_without_data", 0),
                ("grpc.http2.min_time_between_pings_ms", 10000),
                ("grpc.max_receive_message_length", 100 * 1024 * 1024),
                ("grpc.max_send_message_length", 100 * 1024 * 1024),
            ],
        )
        self._stub = pb.worker.WorkerStub(self._channel)
        self._semaphore = asyncio.Semaphore(limit)

    async def dispatch(
        self,
        task: WorkTask,
        *,
        timeout: float | None = None,
    ) -> AsyncIterator[pb.task.Result]:
        """Dispatch a task to the remote worker for execution.

        Sends the task to the worker via gRPC, waits for acknowledgment,
        and returns an async iterator that streams back results. Respects
        concurrency limits and applies timeout to the dispatch phase only
        (semaphore acquisition and acknowledgment).

        :param task:
            The :class:`WorkTask` instance to dispatch to the worker.
        :param timeout:
            Timeout in seconds for semaphore acquisition and task
            acknowledgment. If ``None``, no timeout is applied. Does not
            apply to the execution phase.
        :returns:
            An async iterator that yields task results from the worker.
        :raises TransientRpcError:
            If the worker returns a transient RPC error (UNAVAILABLE,
            DEADLINE_EXCEEDED, or RESOURCE_EXHAUSTED).
        :raises RpcError:
            If the worker returns a non-transient RPC error.
        :raises UnexpectedResponse:
            If the worker doesn't acknowledge the task.
        :raises TimeoutError:
            If the timeout is exceeded during dispatch phase.
        :raises ValueError:
            If the timeout value is not positive.
        """
        if timeout is not None and timeout <= 0:
            raise ValueError("Dispatch timeout must be positive")

        try:
            call = await self._dispatch(task, timeout)
        except grpc.RpcError as error:
            if error.code() in self.TRANSIENT_ERRORS:
                raise TransientRpcError from error
            else:
                raise RpcError from error

        return self._execute(call)

    async def close(self):
        """Close the connection and release all resources.

        Gracefully closes the underlying gRPC channel to the remote
        worker and cleans up any associated resources.
        """
        await self._channel.close()

    async def _dispatch(self, task, timeout):
        async with asyncio.timeout(timeout):
            await self._semaphore.acquire()
            try:
                call: _DispatchCall = self._stub.dispatch(task.to_protobuf())
                try:
                    response = await anext(aiter(call))
                    if not response.HasField("ack"):
                        raise UnexpectedResponse(
                            f"Expected 'ack' response, "
                            f"received '{response.WhichOneof('payload')}'"
                        )
                except (Exception, asyncio.CancelledError):
                    try:
                        call.cancel()
                    except Exception:
                        pass
                    raise
            except (Exception, asyncio.CancelledError):
                self._semaphore.release()
                raise
        return call

    async def _execute(self, call):
        try:
            async for result in _DispatchStream(call):
                yield result
        except (Exception, asyncio.CancelledError):
            try:
                call.cancel()
            except Exception:
                pass
            raise
        finally:
            self._semaphore.release()
