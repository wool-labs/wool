from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import AsyncGenerator
from typing import Final
from typing import Generic
from typing import TypeAlias
from typing import TypeVar
from typing import cast

import cloudpickle
import grpc.aio

import wool
from wool import protocol
from wool.runtime.context import _Context
from wool.runtime.context import _dumps
from wool.runtime.context import build_stream_frame_payload
from wool.runtime.context import build_task_frame_payload
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.routine.task import PassthroughSerializer
from wool.runtime.routine.task import Task
from wool.runtime.worker.base import ChannelOptions

_DispatchCall: TypeAlias = grpc.aio.StreamStreamCall[protocol.Request, protocol.Response]
_PoolKey: TypeAlias = tuple[str, grpc.ChannelCredentials | None, ChannelOptions]

_T = TypeVar("_T")


@dataclass
class _Channel:
    """Internal holder for a pooled gRPC channel and its resources."""

    channel: grpc.aio.Channel
    stub: protocol.WorkerStub
    semaphore: asyncio.Semaphore

    async def close(self):
        """Close the underlying gRPC channel."""
        await self.channel.close()


def _channel_factory(key):
    """Create a new :class:`_Channel` for the given pool key.

    :param key:
        Tuple of ``(target, credentials, options)``.
    :returns:
        A new :class:`_Channel` instance.
    """
    target, credentials, options = key
    grpc_options = [
        ("grpc.max_receive_message_length", options.max_receive_message_length),
        ("grpc.max_send_message_length", options.max_send_message_length),
        ("grpc.keepalive_time_ms", options.keepalive_time_ms),
        ("grpc.keepalive_timeout_ms", options.keepalive_timeout_ms),
        (
            "grpc.keepalive_permit_without_calls",
            int(options.keepalive_permit_without_calls),
        ),
        ("grpc.http2.max_pings_without_data", options.max_pings_without_data),
        ("grpc.max_concurrent_streams", options.max_concurrent_streams),
        (
            "grpc.default_compression_algorithm",
            options.compression.value,
        ),
    ]
    if credentials is not None:
        channel = grpc.aio.secure_channel(target, credentials, options=grpc_options)
    else:
        channel = grpc.aio.insecure_channel(target, options=grpc_options)
    stub = protocol.WorkerStub(channel)
    return _Channel(channel, stub, asyncio.Semaphore(options.max_concurrent_streams))


async def _channel_finalizer(channel: _Channel):
    """Close the gRPC channel held by a :class:`_Channel`.

    :param channel:
        The :class:`_Channel` to finalize.
    """
    await channel.close()


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

    def __init__(self, call: _DispatchCall, task: Task):
        self._call = call
        self._task = task
        self._step = 0
        self._iter = aiter(call)
        self._closed = False
        self._running = False

    async def __anext__(self) -> _T:
        """Get the next response from the stream.

        Sends a ``next`` request to the server to advance the remote
        generator, then reads and returns the next result.

        :returns:
            The next task result from the worker.
        :raises StopAsyncIteration:
            When the stream is exhausted or after aclose() is called.
        :raises RuntimeError:
            If another iteration is already in progress.
        :raises UnexpectedResponse:
            If the response is neither a result nor an exception.
        :raises Exception:
            Any exception raised by the task execution is re-raised.
        """
        if self._closed:  # pragma: no cover
            raise StopAsyncIteration
        if self._running:  # pragma: no cover
            raise RuntimeError("anext(): asynchronous generator is already running")
        self._running = True
        try:
            vars_dict, lineage_hex = build_stream_frame_payload()
            request = protocol.Request(
                next=protocol.Void(),
                vars=vars_dict,
                lineage_id=lineage_hex,
            )
            await self._call.write(request)
            result = await self._read_next()
            self._step += 1
            return result
        finally:
            self._running = False

    async def _read_next(self) -> _T:
        """Read the next response from the stream without writing.

        Used by :meth:`asend` and :meth:`athrow` which have already
        written their own request to the stream.

        If the response carries a non-empty ``vars`` map, the
        back-propagated var mutations are applied to the current
        context before the result is returned.

        :returns:
            The next task result from the worker.
        """
        try:
            response = await anext(self._iter)
            if response.vars:
                _Context.apply(dict(response.vars))
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
        if self._closed:  # pragma: no cover
            return

        self._closed = True
        try:
            self._call.cancel()
        except Exception:
            pass

    async def asend(self, value):
        """Send a value into the remote async generator.

        Serializes *value*, writes it as a ``Message`` frame to the
        bidirectional stream, and returns the next yielded result.

        :param value:
            The value to send into the generator.
        :returns:
            The next yielded value from the remote generator.
        :raises StopAsyncIteration:
            When the remote generator is exhausted or the stream
            has been closed.
        :raises RuntimeError:
            If another iteration is already in progress.
        """
        if self._closed:  # pragma: no cover
            raise StopAsyncIteration
        if self._running:  # pragma: no cover
            raise RuntimeError("anext(): asynchronous generator is already running")
        self._running = True
        try:
            dump = _dumps(value)
            vars_dict, lineage_hex = build_stream_frame_payload()
            request = protocol.Request(
                send=protocol.Message(dump=dump),
                vars=vars_dict,
                lineage_id=lineage_hex,
            )
            await self._call.write(request)
            result = await self._read_next()
            self._step += 1
            return result
        finally:
            self._running = False

    async def athrow(self, typ, val=None, tb=None):
        """Throw an exception into the remote async generator.

        Serializes the exception and sends it as a ``Message`` frame.
        The remote generator receives the exception via ``athrow()``
        and may handle or propagate it.

        :param typ:
            The exception type or instance to throw.
        :param val:
            The exception value (if *typ* is a type).
        :param tb:
            The exception traceback.
        :returns:
            The next yielded value from the remote generator.
        :raises StopAsyncIteration:
            When the remote generator is exhausted or the stream
            has been closed.
        :raises RuntimeError:
            If another iteration is already in progress.
        """
        if self._closed:  # pragma: no cover
            raise StopAsyncIteration
        if self._running:  # pragma: no cover
            raise RuntimeError("athrow(): asynchronous generator is already running")
        self._running = True
        try:
            if isinstance(typ, BaseException):  # pragma: no cover
                exc = typ
            elif val is not None:
                exc = val
            else:  # pragma: no cover
                exc = typ()

            dump = _dumps(exc)
            vars_dict, lineage_hex = build_stream_frame_payload()
            request = protocol.Request(
                throw=protocol.Message(dump=dump),
                vars=vars_dict,
                lineage_id=lineage_hex,
            )
            await self._call.write(request)
            result = await self._read_next()
            self._step += 1
            return result
        finally:
            self._running = False


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
        elif details is not None:
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
    options)``.  Each :meth:`dispatch` call obtains a reference-counted
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
    :param credentials:
        Optional channel credentials for TLS/mTLS connections.
    :param options:
        Optional channel options controlling gRPC message
        size limits, keepalive, concurrency, and compression.
        See :class:`ChannelOptions` for defaults.  The
        ``max_concurrent_streams`` field sizes the per-channel
        concurrency semaphore.
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
        credentials: grpc.ChannelCredentials | None = None,
        options: ChannelOptions | None = None,
    ):
        self._target = target
        self._credentials = credentials
        self._options = options if options is not None else ChannelOptions()
        self._key: _PoolKey = (target, credentials, self._options)
        self._uds_key: _PoolKey | None = None

    async def dispatch(
        self,
        task: Task,
        *,
        timeout: float | None = None,
    ) -> AsyncGenerator[protocol.Message, None]:
        """Dispatch a task to the remote worker for execution.

        Sends the task to the worker via gRPC, waits for acknowledgment,
        and returns an async iterator that streams back results. Respects
        concurrency limits and applies timeout to the dispatch phase only
        (semaphore acquisition and acknowledgment).

        .. note::

           When dispatching to the current worker process (self-dispatch),
           a :class:`PassthroughSerializer` is used so the four payload
           fields (callable, args, kwargs, proxy) are stored in-process
           instead of being cloudpickled.  The request still travels
           through gRPC so the full streaming protocol is preserved.

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

        if (
            metadata := wool.__worker_metadata__
        ) is not None and metadata.address == self._target:
            serializer = PassthroughSerializer()
            if (uds_address := wool.__worker_uds_address__) is not None:
                key = (uds_address, None, self._options)
                self._uds_key = key
            else:
                key = self._key
        else:
            serializer = None
            key = self._key

        channel = await _channel_pool.acquire(key)
        try:
            try:
                call = await self._dispatch(
                    channel, task.to_protobuf(serializer=serializer), timeout
                )
            except grpc.RpcError as error:
                code = error.code()
                details = error.details() or str(error)
                if code in self.TRANSIENT_ERRORS:
                    raise TransientRpcError(code, details) from error
                else:
                    raise RpcError(code, details) from error

            stream = self._execute(call, task, key)
            await stream.__anext__()  # Prime: _execute acquires its own ref
        except BaseException:
            await _channel_pool.release(key)
            raise

        await _channel_pool.release(key)
        return cast(AsyncGenerator[protocol.Message, None], stream)

    async def close(self):
        """Close the connection and release all pooled resources.

        Clears the pooled channel entries for both the TCP key and,
        if a UDS address is available, the UDS key. Idempotent: safe
        to call multiple times or on connections that were never used.
        """
        try:
            await _channel_pool.clear(self._key)
        except KeyError:
            pass
        if self._uds_key is not None:
            try:
                await _channel_pool.clear(self._uds_key)
            except KeyError:
                pass

    async def _dispatch(
        self,
        channel: _Channel,
        task_msg: protocol.Task,
        timeout: float | None,
    ) -> _DispatchCall:
        async with asyncio.timeout(timeout):
            await channel.semaphore.acquire()
            try:
                call: _DispatchCall = channel.stub.dispatch()
                try:
                    vars_dict, lineage_hex = build_task_frame_payload()
                    request = protocol.Request(
                        task=task_msg,
                        vars=vars_dict,
                        lineage_id=lineage_hex,
                    )
                    await call.write(request)
                    response = await anext(aiter(call))
                    if response.HasField("nack"):
                        raise RpcError(
                            details=f"Task rejected by worker: {response.nack.reason}"
                        )
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
                channel.semaphore.release()
                raise
        return call

    async def _execute(
        self, call: _DispatchCall, task: Task, key: _PoolKey
    ) -> AsyncGenerator[protocol.Message | None, None]:
        channel = await _channel_pool.acquire(key)
        try:
            yield  # Priming yield — signals dispatch() that ref is held
            stream = _DispatchStream(call, task)
            try:
                sent = None
                result = await anext(stream)
                while True:
                    try:
                        sent = yield result
                    except GeneratorExit:
                        await stream.aclose()
                        return
                    except BaseException as exc:
                        result = await stream.athrow(type(exc), exc)
                    else:
                        result = await stream.asend(sent)
            except StopAsyncIteration:
                return
            except (Exception, asyncio.CancelledError):
                try:
                    await stream.aclose()
                except Exception:  # pragma: no cover
                    pass
                raise
            finally:
                channel.semaphore.release()
        finally:
            await _channel_pool.release(key)
