from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import AsyncIterator
from typing import Final
from typing import Generic
from typing import TypeAlias
from typing import TypeVar

import cloudpickle
import grpc.aio

from wool.runtime import protobuf as pb
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.routine.task import Task
from wool.runtime.worker.base import ChannelCredentialsType
from wool.runtime.worker.base import resolve_channel_credentials

_DispatchCall: TypeAlias = grpc.aio.UnaryStreamCall[pb.task.Task, pb.worker.Response]

_T = TypeVar("_T")


@dataclass
class _Channel:
    """Internal holder for a pooled gRPC channel and its resources."""

    channel: grpc.aio.Channel
    stub: pb.worker.WorkerStub
    semaphore: asyncio.Semaphore


def _channel_factory(key):
    """Create a new :class:`_Channel` for the given pool key.

    :param key:
        Tuple of ``(target, credentials, limit)``.
    :returns:
        A new :class:`_Channel` instance.
    """
    target, credentials, limit = key
    resolved = resolve_channel_credentials(credentials)
    options = [
        ("grpc.max_receive_message_length", 100 * 1024 * 1024),
        ("grpc.max_send_message_length", 100 * 1024 * 1024),
    ]
    if resolved is not None:
        ch = grpc.aio.secure_channel(target, resolved, options=options)
    else:
        ch = grpc.aio.insecure_channel(target, options=options)
    return _Channel(ch, pb.worker.WorkerStub(ch), asyncio.Semaphore(limit))


async def _channel_finalizer(ch: _Channel):
    """Close the gRPC channel held by a :class:`_Channel`.

    :param ch:
        The :class:`_Channel` to finalize.
    """
    await ch.channel.close()


_channel_pool: ResourcePool[_Channel] = ResourcePool(
    factory=_channel_factory, finalizer=_channel_finalizer, ttl=60
)


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
        self._closed = False

    def __aiter__(self) -> AsyncIterator[_T]:
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> _T:
        """Get the next response from the stream.

        :returns:
            The next task result from the worker.
        :raises StopAsyncIteration:
            When the stream is exhausted or after aclose() is called.
        :raises UnexpectedResponse:
            If the response is neither a result nor an exception.
        :raises Exception:
            Any exception raised by the task execution is re-raised.
        """
        if self._closed:
            raise StopAsyncIteration

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

    async def aclose(self) -> None:
        """Close the async generator and cancel the underlying gRPC call.

        This method provides proper cleanup for async generators decorated
        with @routine. When called, it cancels the gRPC stream to the worker,
        which triggers cleanup on the worker side.

        Implements the async generator protocol's aclose() method to match
        native Python async generator behavior. This method is idempotent
        and can be safely called multiple times.
        """
        if self._closed:
            return

        self._closed = True
        try:
            self._call.cancel()
        except Exception:
            pass

    async def asend(self, value):
        """Send a value into the async generator (not supported).

        :param value:
            The value to send into the generator.
        :raises NotImplementedError:
            Bidirectional communication via asend() is not yet supported
            for distributed async generators. Use pull-based iteration with
            __anext__() instead.
        """
        raise NotImplementedError(
            "asend() is not supported for distributed async generators. "
            "Only pull-based iteration via __anext__() and aclose() are supported."
        )

    async def athrow(self, typ, val=None, tb=None):
        """Throw an exception into the async generator (not supported).

        :param typ:
            The exception type to throw.
        :param val:
            The exception value.
        :param tb:
            The exception traceback.
        :raises NotImplementedError:
            Bidirectional communication via athrow() is not yet supported
            for distributed async generators. Exceptions can only be raised
            from the worker side during iteration.
        """
        raise NotImplementedError(
            "athrow() is not supported for distributed async generators. "
            "Only pull-based iteration via __anext__() and aclose() are supported."
        )


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

    def __init__(
        self,
        code: grpc.StatusCode | None = None,
        details: str | None = None,
    ):
        self.code = code
        self.details = details
        if code is not None and details is not None:
            super().__init__(f"{code.name}: {details}")
        elif code is not None:  # pragma: no cover
            super().__init__(code.name)
        elif details is not None:  # pragma: no cover
            super().__init__(details)
        else:  # pragma: no cover
            super().__init__()


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

    Acquires pooled gRPC channels keyed by ``(target, credentials,
    limit)``.  Each :meth:`dispatch` call obtains a reference-counted
    channel from the module-level pool, primes an async generator that
    holds its own reference, then releases the dispatch-scope reference.
    The channel stays alive until the caller finishes consuming the
    result stream.

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
    :param credentials:
        Optional channel credentials for TLS/mTLS connections.
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
        credentials: ChannelCredentialsType = None,
    ):
        if limit <= 0:
            raise ValueError("Limit must be positive")

        self._target = target
        self._credentials = credentials
        self._limit = limit
        self._key = (target, credentials, limit)

    async def dispatch(
        self,
        task: Task,
        *,
        timeout: float | None = None,
    ) -> AsyncIterator[pb.task.Result]:
        """Dispatch a task to the remote worker for execution.

        Sends the task to the worker via gRPC, waits for acknowledgment,
        and returns an async iterator that streams back results. Respects
        concurrency limits and applies timeout to the dispatch phase only
        (semaphore acquisition and acknowledgment).

        :param task:
            The :class:`Task` instance to dispatch to the worker.
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

        ch = await _channel_pool.acquire(self._key)
        try:
            try:
                call = await self._dispatch(ch, task, timeout)
            except grpc.RpcError as error:
                code = error.code()
                details = error.details() or str(error)
                if code in self.TRANSIENT_ERRORS:
                    raise TransientRpcError(code, details) from error
                else:
                    raise RpcError(code, details) from error

            gen = self._execute(call)
            await gen.__anext__()  # Prime: _execute acquires its own ref
        except BaseException:
            await _channel_pool.release(self._key)
            raise

        await _channel_pool.release(self._key)
        return gen

    async def close(self):
        """Close the connection and release all pooled resources.

        Clears the pooled channel entry for this connection's key.
        Idempotent: safe to call multiple times or on connections
        that were never used.
        """
        try:
            await _channel_pool.clear(self._key)
        except KeyError:
            pass

    async def _dispatch(self, ch, task, timeout):
        async with asyncio.timeout(timeout):
            await ch.semaphore.acquire()
            try:
                call: _DispatchCall = ch.stub.dispatch(task.to_protobuf())
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
                ch.semaphore.release()
                raise
        return call

    async def _execute(self, call):
        ch = await _channel_pool.acquire(self._key)
        try:
            yield  # Priming yield — signals dispatch() that ref is held
            stream = _DispatchStream(call)
            try:
                async for result in stream:
                    yield result
            except (Exception, GeneratorExit, asyncio.CancelledError):
                try:
                    await stream.aclose()
                except Exception:  # pragma: no cover
                    pass
                raise
            finally:
                ch.semaphore.release()
        finally:
            await _channel_pool.release(self._key)
