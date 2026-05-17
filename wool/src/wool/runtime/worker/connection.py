from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any
from typing import AsyncGenerator
from typing import Coroutine
from typing import Final
from typing import Generic
from typing import TypeAlias
from typing import TypeVar
from typing import cast

import grpc.aio

import wool
from wool import protocol
from wool.runtime.context import current_snapshot
from wool.runtime.context import decode_snapshot
from wool.runtime.context import encode_snapshot
from wool.runtime.context import merge_snapshot
from wool.runtime.context import snapshot_has_state
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.routine.task import Task
from wool.runtime.serializer import PassthroughSerializer
from wool.runtime.serializer import Serializer
from wool.runtime.serializer import _passthrough_pool
from wool.runtime.worker.base import ChannelOptions

_DispatchCall: TypeAlias = grpc.aio.StreamStreamCall[protocol.Request, protocol.Response]
_PoolKey: TypeAlias = tuple[str, grpc.ChannelCredentials | None, ChannelOptions]

_T = TypeVar("_T")

_log = logging.getLogger(__name__)


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


async def clear_channel_pool() -> None:
    """Close and clear every gRPC channel in the process-wide pool.

    Invalidates cached channels across every pool key, including
    UDS targets.
    """
    await _channel_pool.clear()


_TEARDOWN_TIMEOUT: Final = 60.0


async def _complete_teardown(teardown: Coroutine[Any, Any, None]) -> None:
    """Drive *teardown* to completion, immune to caller cancellation.

    Resource teardown registered on an :class:`AsyncExitStack`
    awaits its release callbacks. When the caller task carries a
    pending cancellation — externally via ``task.cancel()`` or after
    a worker-side ``CancelledError`` bumped ``cancelling()`` in
    :meth:`_DispatchStream._read_next` — asyncio would pre-empt the
    next suspending teardown ``await`` and skip the remaining
    callbacks, leaking a pooled resource reference.

    Running *teardown* as a shielded child task gives it an
    independent cancellation state, so its ``await`` boundaries run
    uninterrupted. A cancellation observed while waiting is deferred
    and re-raised once teardown finishes, so the caller still
    observes the cancel.

    A teardown-side exception other than ``CancelledError`` propagates
    and supersedes a deferred cancel, mirroring ``finally`` precedence.
    ``KeyboardInterrupt`` and ``SystemExit`` are captured off the child
    task — where they would otherwise escape straight to the event-loop
    runner via ``Task.__step`` — and re-raised in the caller's context.
    If teardown does not finish within :data:`_TEARDOWN_TIMEOUT` the
    caller is unblocked and the shielded task is left running detached
    so the release still completes.
    """
    interrupt: KeyboardInterrupt | SystemExit | None = None

    async def _run() -> None:
        # Capture process-level signals here, inside the child task's
        # own frame: a ``KeyboardInterrupt``/``SystemExit`` raised by a
        # task escapes to the event-loop runner rather than to the
        # awaiter, so it must not be left to propagate out of the task.
        nonlocal interrupt
        try:
            await teardown
        except (KeyboardInterrupt, SystemExit) as exc:
            interrupt = exc

    task = asyncio.ensure_future(_run())
    deferred: asyncio.CancelledError | None = None
    while True:
        try:
            async with asyncio.timeout(_TEARDOWN_TIMEOUT):
                await asyncio.shield(task)
        except TimeoutError:
            # Teardown is wedged — only reachable under pathological
            # pool-lock contention. Stop blocking the caller; the
            # shielded task keeps running so the release still
            # completes, just not synchronously.
            _log.warning(
                "Routine teardown exceeded %.0fs; pooled-resource "
                "release deferred to a detached task.",
                _TEARDOWN_TIMEOUT,
            )
            break
        except asyncio.CancelledError as exc:
            if not task.done():
                # Caller cancelled mid-teardown — keep the shielded
                # task running and re-await it on the next iteration.
                deferred = exc
                continue
            raise
        break
    if interrupt is not None:
        raise interrupt
    if deferred is not None:
        raise deferred


class _DispatchStream(Generic[_T]):
    """Async iterator wrapper for streaming task results from workers.

    Handles iteration over gRPC response streams and deserializes
    task results or raises exceptions received from remote workers.

    :param call:
        The underlying gRPC response stream.
    """

    def __init__(
        self,
        call: _DispatchCall,
        task: Task,
        serializer: Serializer | None = None,
    ):
        self._call = call
        self._task = task
        self._serializer: Serializer = (
            serializer if serializer is not None else wool.__serializer__
        )
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
            If the response payload is unrecognised (neither a
            result nor an exception), if a result or exception
            dump cannot be deserialised (e.g. cloudpickle version
            skew, missing class on the caller's path, truncated
            bytes, etc.), or if the worker ships a non-:class:`Exception`
            :class:`BaseException` payload other than
            :class:`asyncio.CancelledError` (e.g. :class:`KeyboardInterrupt`,
            :class:`SystemExit`, user-defined :class:`BaseException`
            subclasses, etc.).
        :raises asyncio.CancelledError:
            When the worker-side routine raises
            :class:`asyncio.CancelledError` from its body (or is
            externally cancelled and propagates the
            ``CancelledError`` out). Mirrors stdlib's ``await task``
            semantics where ``raise CancelledError`` from the
            awaitee is indistinguishable from
            ``task.cancel()`` — both transition the task to
            ``CANCELLED`` and the caller's ``await`` raises
            ``CancelledError``. The caller task's ``cancelling()``
            count is incremented synchronously with the raise,
            mirroring stdlib's local-cancel state shape so that
            ``if cancelling() > 0: raise`` re-raise gates and
            ``current_task().uncancel()`` absorbers behave
            identically for worker-side and local cancels.
        :raises Exception:
            The worker-side routine's exception, re-raised in its
            original class. The class is narrowed to
            :class:`Exception` for non-:class:`CancelledError`
            :class:`BaseException` subclasses (:class:`KeyboardInterrupt`,
            :class:`SystemExit`, or user-defined :class:`BaseException`
            subclasses): these are degraded to
            :class:`UnexpectedResponse` so process-level signals
            cannot be smuggled across the wire and trip caller-side
            signal handlers. :class:`UnexpectedResponse` is not a
            :class:`RpcError` subclass, so the load balancer treats
            it as a caller-fault and does not evict the worker.
            Caller-side gRPC cancellation arrives via a different
            path, not via this exception.
        """
        if self._closed:  # pragma: no cover
            raise StopAsyncIteration
        if self._running:  # pragma: no cover
            raise RuntimeError("anext(): asynchronous generator is already running")
        self._running = True
        try:
            request = protocol.Request(
                next=protocol.Void(),
                context=encode_snapshot(current_snapshot(), serializer=self._serializer),
            )
            await self._call.write(request)
            result = await self._read_next()
            return result
        finally:
            self._running = False

    async def _read_next(self) -> _T:
        """Read the next response from the stream without writing —
        for paths that have already written their own request.

        Merges the response's wire context into the caller's active
        snapshot — variable mutations and consumed-token state both
        ride back-propagation.

        :returns:
            The next task result from the worker.
        """
        try:
            response = await anext(self._iter)
            # Wool treats response context as ancillary state. Per-var
            # decode failures aggregate inside
            # :func:`~wool.runtime.context.snapshot.decode_snapshot`
            # and surface as a
            # :class:`BaseExceptionGroup` only under strict mode; on the
            # primary-signal path we bundle them with the worker
            # exception (or the result-bearing response's group) so
            # callers can extract both signals via ``except*``.
            decode_failures: list[BaseException] = []
            try:
                incoming_snapshot = decode_snapshot(
                    response.context, serializer=self._serializer
                )
            except BaseExceptionGroup as eg:
                decode_failures.extend(eg.exceptions)
            else:
                if snapshot_has_state(incoming_snapshot):
                    merge_snapshot(incoming_snapshot)
            if response.HasField("result"):
                try:
                    result = self._serializer.loads(response.result.dump)
                except Exception as exc:
                    # Degrade malformed result payloads to
                    # :class:`UnexpectedResponse` so callers can
                    # ``except UnexpectedResponse`` uniformly while
                    # the original pickle/import failure remains on
                    # ``__cause__`` for diagnostic chains. Load
                    # balancer treats this as caller-fault and does
                    # not evict the worker (typically a version
                    # skew on a shared result class).
                    raise UnexpectedResponse(
                        "Worker shipped a malformed result payload"
                    ) from exc
                if decode_failures:
                    raise BaseExceptionGroup(
                        "response context decode failed",
                        decode_failures,
                    )
                return result
            elif response.HasField("exception"):
                # Degrade malformed exception payloads (cloudpickle
                # version skew, missing class on the caller's path,
                # truncated bytes, worker-side serializer bug) to
                # :class:`UnexpectedResponse` so the load balancer
                # treats it as a caller-fault and does not evict the
                # worker for what is typically a version-skew issue.
                # Mirrors the non-Exception payload degradation
                # below; the parse-phase Nack path keeps its
                # :class:`RpcError` fallback because worker-side
                # parse rejection has different worker-health
                # semantics than a routine-time decode mismatch.
                try:
                    worker_exc = self._serializer.loads(response.exception.dump)
                except Exception as exc:
                    # Preserve the original pickle/import failure
                    # via manual ``__cause__`` chaining — we assign
                    # ``worker_exc`` and continue into the
                    # narrowing + note-attachment block below, so
                    # ``raise X from Y`` syntax isn't applicable
                    # here. The later ``raise worker_exc`` honors
                    # the manually-set ``__cause__`` identically to
                    # ``raise X from Y``.
                    worker_exc = UnexpectedResponse(
                        "Worker shipped a malformed exception payload"
                    )
                    worker_exc.__cause__ = exc
                    worker_exc.__suppress_context__ = True
                # See ``__anext__``'s ``:raises Exception:`` /
                # ``:raises asyncio.CancelledError:`` for the
                # narrowing contract. ``CancelledError`` is allowed
                # to propagate raw to mirror stdlib's ``await
                # task`` semantics where a routine that self-raises
                # ``CancelledError`` is indistinguishable from one
                # that was externally cancelled. Other non-Exception
                # ``BaseException`` subclasses are degraded to
                # :class:`UnexpectedResponse` (not :class:`RpcError`)
                # so process-level signals cannot be smuggled and
                # the load balancer does not evict the worker for a
                # routine-level fault.
                if not isinstance(worker_exc, (Exception, asyncio.CancelledError)):
                    worker_exc = UnexpectedResponse(
                        "Worker shipped a non-Exception payload in "
                        f"Response.exception: {type(worker_exc).__name__}"
                    )
                if decode_failures:
                    # Attach decode failures to the worker exception
                    # rather than wrap both in a
                    # :class:`BaseExceptionGroup`. Mirrors the
                    # worker's encode-side handling
                    # (:mod:`wool.runtime.worker.service`), so the
                    # caller's existing ``except`` against the
                    # routine's exception class keeps matching —
                    # users don't have to migrate to ``except*``.
                    try:
                        for w in decode_failures:
                            worker_exc.add_note(f"wool context warning: {w}")
                    except (AttributeError, TypeError):
                        pass
                    try:
                        setattr(
                            worker_exc,
                            "__wool_context_warnings__",
                            decode_failures,
                        )
                    except AttributeError:
                        pass
                # Mirror stdlib's local-cancel state shape: bump
                # ``current_task().cancelling()`` synchronously and
                # forward the worker's cancel message so idiomatic
                # ``except CancelledError`` patterns
                # (``if cancelling() > 0: raise`` re-raise gates,
                # ``current_task().uncancel()`` absorbers) and any
                # caller that introspects task state behave
                # identically for worker-side and local cancels. The
                # next-cycle ``CancelledError`` that ``Task.cancel()``
                # schedules is suppressed by ``uncancel()`` per
                # asyncio's contract.
                if isinstance(worker_exc, asyncio.CancelledError):
                    current = asyncio.current_task()
                    if current is not None:
                        cancel_msg = worker_exc.args[0] if worker_exc.args else None
                        current.cancel(cancel_msg)
                raise worker_exc
            else:
                raise UnexpectedResponse(
                    f"Expected 'result' or 'exception' response, "
                    f"received '{response.WhichOneof('payload')}'"
                )
        except BaseException:
            # Cancel the underlying gRPC call on any abnormal exit
            # — including ``asyncio.CancelledError`` (a
            # ``BaseException`` subclass), so cancellation
            # propagates without leaking the in-flight call.
            # Mirrors stdlib ``await agen.__anext__()`` cleanup
            # semantics: any non-normal-return exit triggers
            # resource cleanup before re-raising.
            #
            # The inner cancel-swallow is ``Exception``, not
            # ``BaseException``: this is cleanup-during-cleanup,
            # so a ``KeyboardInterrupt`` mid-cancel should
            # propagate rather than be silently dropped.
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
            raise RuntimeError("asend(): asynchronous generator is already running")
        self._running = True
        try:
            request = protocol.Request(
                send=protocol.Message(dump=self._serializer.dumps(value)),
                context=encode_snapshot(current_snapshot(), serializer=self._serializer),
            )
            await self._call.write(request)
            result = await self._read_next()
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

            request = protocol.Request(
                throw=protocol.Message(dump=self._serializer.dumps(exc)),
                context=encode_snapshot(current_snapshot(), serializer=self._serializer),
            )
            await self._call.write(request)
            result = await self._read_next()
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
    """Raised when a gRPC call to a worker fails with a non-transient
    error.

    Non-transient errors indicate persistent issues with the worker
    that are unlikely to be resolved by retrying (e.g., invalid
    arguments, unimplemented methods, permission denied,
    server-side bugs, version skew).

    **Worker-health exception contract.** Load-balancer strategies
    treat exception classes from :meth:`WorkerConnection.dispatch`
    as a three-way classification:

    - :class:`TransientRpcError` — worker is hiccupping
      (``UNAVAILABLE`` / ``DEADLINE_EXCEEDED`` /
      ``RESOURCE_EXHAUSTED``); the strategy should **skip** to
      the next worker without eviction. The worker may recover.
    - :class:`RpcError` (non-transient) — worker is unhealthy
      (``INTERNAL``, ``FAILED_PRECONDITION``, malformed Nack
      dump, version skew, etc.); the strategy should **evict**.
      Today's binary policy is "evict on first occurrence";
      health-aware forgiveness (N-strikes) is a follow-up.
    - Any other class — caller-fault (parse-phase failures
      re-raised as the original exception type, caller-side
      encode failures, programming bugs); the strategy
      **propagates** to the caller without touching the pool.

    Strategy authors implementing :class:`LoadBalancerLike` MUST
    honor this contract: a strategy that catches :class:`Exception`
    indiscriminately will silently evict workers on every
    caller-side bug, wiping the pool over time.
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

    **Cleanup semantics on cancellation.** Every code path that owns
    an in-flight gRPC call wraps its body in
    ``try / except BaseException`` so that ``asyncio.CancelledError``
    (a :class:`BaseException` subclass, not :class:`Exception`) still
    triggers ``call.cancel()`` before re-raising. The cancel itself
    is swallowed at :class:`Exception` (not :class:`BaseException`) —
    cleanup-during-cleanup should let ``KeyboardInterrupt`` propagate
    rather than silently drop a process-level signal.

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
           instead of being serialized.  The request still travels
           through gRPC so the full streaming protocol is preserved.

        **Context decode failures (caller-side).**
        Each response frame may carry a back-propagated wire context
        that needs decoding before the caller can merge worker-side
        mutations. Wire context is **ancillary state** under wool's
        protocol contract: per-entry decode failures emit
        :class:`wool.ContextDecodeWarning` instances inside
        :func:`~wool.runtime.context.snapshot.decode_snapshot`. Under
        the warnings system's default filter these surface once as
        warnings and decoding returns the partial snapshot; under a
        filter that promotes :class:`wool.ContextDecodeWarning` to an
        error, :func:`~wool.runtime.context.snapshot.decode_snapshot`
        aggregates the per-entry exceptions into a
        :class:`BaseExceptionGroup` and raises in place of returning.
        Caller-side handling after loading the primary signal:

        * On a result frame, if decoding aggregated, the
          :class:`BaseExceptionGroup` raises in place of the return —
          strict mode loses the primary value but every decode
          failure surfaces, not just the first.
        * On an exception frame, decode failures are attached to
          the worker exception via PEP 678 ``__notes__`` (visible
          in tracebacks) and a ``__wool_context_warnings__``
          attribute (programmatic access), mirroring the worker's
          encode-side handling. The worker exception class is
          preserved so the caller's existing
          ``except RoutineError`` continues to catch without
          migration to ``except*``. Under the default filter the
          per-entry warnings emit once during decode and the worker
          exception raises unwrapped.

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
            DEADLINE_EXCEEDED, or RESOURCE_EXHAUSTED) or the local
            dispatch-phase timeout fires (also classified as
            DEADLINE_EXCEEDED).
        :raises RpcError:
            If the worker returns a non-transient RPC error or
            rejects with a Nack whose dumped exception cannot be
            deserialized (malformed-dump fallback).
        :raises UnexpectedResponse:
            If the worker doesn't acknowledge the task.
        :raises ValueError:
            If the timeout value is not positive.

        If the worker rejects the task during the parse phase due
        to a malformed task payload, the original exception class
        is deserialized from the Nack and re-raised so the caller
        observes the actual failure class rather than an opaque
        protocol error. A malformed Nack payload falls back to
        :class:`RpcError`.

        Encode-side failures (e.g. a strict-mode
        :class:`BaseExceptionGroup` of
        :class:`wool.ContextDecodeWarning` peers raised by
        :func:`~wool.runtime.context.snapshot.encode_snapshot` when
        an unpicklable :class:`wool.ContextVar` value is set)
        propagate unwrapped:
        the load-balancer contract treats only :class:`RpcError`
        instances as worker-health concerns, so a caller-side encode
        failure surfaces directly to the caller rather than evicting
        workers.
        """
        if timeout is not None and timeout <= 0:
            raise ValueError("Dispatch timeout must be positive")

        if (
            metadata := wool.__worker_metadata__
        ) is not None and metadata.address == self._target:
            use_passthrough = True
            if (uds_address := wool.__worker_uds_address__) is not None:
                key = (uds_address, None, self._options)
                self._uds_key = key
            else:
                key = self._key
        else:
            use_passthrough = False
            key = self._key

        stream = self._execute(task, key, use_passthrough, timeout)
        try:
            await stream.__anext__()  # Prime: pins resources + handshake
        except grpc.RpcError as error:
            code = error.code()
            details = error.details() or str(error)
            if code in self.TRANSIENT_ERRORS:
                raise TransientRpcError(code, details) from error
            else:
                raise RpcError(code, details) from error
        except asyncio.TimeoutError as error:
            # Local dispatch-phase timeout is the same semantic as
            # gRPC DEADLINE_EXCEEDED — request took too long. Wrap
            # so the load-balancer contract only needs to know
            # about :class:`RpcError`. Worker isn't presumed
            # unhealthy; transient-class makes the LB skip without
            # eviction.
            raise TransientRpcError(
                grpc.StatusCode.DEADLINE_EXCEEDED,
                "Local dispatch-phase timeout exceeded",
            ) from error

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

    async def _handshake(
        self,
        call: _DispatchCall,
        wire_task: protocol.Task,
        serializer: PassthroughSerializer | None,
    ) -> None:
        """Send the dispatch request and wait for the worker's
        acknowledgement. Caller is responsible for channel-permit
        and call-cancel lifecycle; :meth:`_execute` pins both on
        its exit stack so any failure here triggers the registered
        cleanup callbacks during unwind.

        On a Nack (parse-phase worker rejection), re-raises the
        worker's original exception unchanged. On a malformed
        Nack payload (loads raises, or yields a non-Exception),
        falls back to :class:`RpcError`.
        """
        request = protocol.Request(
            task=wire_task,
            context=encode_snapshot(current_snapshot(), serializer=serializer),
        )
        await call.write(request)
        response = await anext(aiter(call))
        if response.HasField("nack"):
            # Every Nack carries a typed parse-phase exception.
            # Deserialize and re-raise so the caller observes the
            # actual failure class rather than an opaque RpcError.
            # Envelope-level rejections (e.g., protocol-version
            # mismatch) ride gRPC status codes, not Nack — those
            # land in :meth:`dispatch`'s ``except grpc.RpcError``
            # arm instead.
            nack_serializer = (
                serializer if serializer is not None else wool.__serializer__
            )
            try:
                raised = nack_serializer.loads(response.nack.exception.dump)
            except Exception:
                raised = None
            # Narrowed to ``Exception`` to match
            # ``Rejected.original``'s typed contract (worker
            # constructs ``Rejected`` only from
            # ``except Exception``). A worker that ships a
            # non-``Exception`` ``BaseException`` would be a worker
            # bug; degrade to :class:`RpcError` rather than smuggle
            # cancel/interrupt signals across the wire. A malformed
            # dump (loads raises) lands here too.
            if isinstance(raised, Exception):
                raise raised from None
            raise RpcError(details="Task rejected by worker (malformed Nack payload)")
        if not response.HasField("ack"):
            raise UnexpectedResponse(
                f"Expected 'ack' response, received '{response.WhichOneof('payload')}'"
            )

    async def _execute(
        self,
        task: Task,
        key: _PoolKey,
        use_passthrough: bool,
        timeout: float | None,
    ) -> AsyncGenerator[protocol.Message | None, None]:
        """Async generator that owns the full dispatch lifecycle.

        Pins the passthrough serializer (self-dispatch only), the
        channel pool ref, the channel-concurrency permit, and the
        gRPC call's cancel hook on a single :class:`AsyncExitStack`.
        Completes the handshake before yielding to the caller; any
        exit path — setup failure, priming-yield ``GeneratorExit``,
        mid-stream exception, natural end of stream — unwinds the
        stack and releases every resource exactly once.

        The stack unwind is driven through :func:`_complete_teardown`
        so the release callbacks run to completion even when the
        caller task is mid-cancellation — otherwise a pending
        ``CancelledError`` could pre-empt ``AsyncExitStack.__aexit__``
        and leak a pooled channel reference.
        """
        stack = AsyncExitStack()
        try:
            if use_passthrough:
                # Pin the per-task passthrough serializer for the
                # streaming generator's lifetime. Self-dispatch
                # only — cross-process dispatch uses cloudpickle.
                serializer = await stack.enter_async_context(
                    _passthrough_pool.get(task.id)
                )
            else:
                serializer = None

            channel = await stack.enter_async_context(_channel_pool.get(key))
            wire_task = task.to_protobuf(serializer=serializer)

            # Acquire the concurrency permit and complete the
            # handshake under the dispatch-phase timeout.
            # ``Semaphore.acquire()`` is cancel-safe: if cancelled
            # before it returns, no permit is taken; if it returns,
            # the next line (sync) registers the release. The two-
            # step "acquire then register" is therefore atomic with
            # respect to cancellation.
            async with asyncio.timeout(timeout):
                await channel.semaphore.acquire()
                stack.callback(channel.semaphore.release)

                call: _DispatchCall = channel.stub.dispatch()

                # Cancel the in-flight gRPC call on any unwind.
                # Swallow ``Exception`` (not ``BaseException``) so
                # a buggy stub's ``cancel()`` does not replace
                # whatever exception is unwinding the stack;
                # cleanup-during-cleanup.
                def _safe_cancel() -> None:
                    try:
                        call.cancel()
                    except Exception:
                        pass

                stack.callback(_safe_cancel)
                await self._handshake(call, wire_task, serializer)

            # Priming yield. All resources are pinned on the stack
            # and the worker has acknowledged the task. The
            # caller's ``__anext__`` prime returns here.
            yield

            stream = _DispatchStream(call, task, serializer=serializer)
            try:
                sent = None
                result = await anext(stream)
                while True:
                    try:
                        sent = yield result
                    except GeneratorExit:
                        # Short-circuit before ``except
                        # BaseException`` below catches and
                        # ``athrow``s the GeneratorExit into the
                        # inner stream. Cancellation of the
                        # in-flight gRPC call happens via the
                        # AsyncExitStack's ``_safe_cancel``
                        # callback on stack unwind — single
                        # resource ownership, single cancel.
                        return
                    except BaseException as exc:
                        result = await stream.athrow(type(exc), exc)
                    else:
                        result = await stream.asend(sent)
            except StopAsyncIteration:
                return
            # Other abnormal exits (``asyncio.CancelledError``,
            # routine exceptions, mid-stream gRPC errors) propagate
            # uncaught; the AsyncExitStack's ``_safe_cancel``
            # callback fires on unwind to cancel the in-flight
            # gRPC call.
        finally:
            # Shield the stack unwind from caller cancellation so
            # every pooled-resource release callback runs — see
            # :func:`_complete_teardown`. ``aclose()`` drives each
            # registered ``__aexit__`` with no exception info; that
            # is equivalent to the implicit ``async with`` exit only
            # because every context manager on this stack is
            # exception-agnostic.
            await _complete_teardown(stack.aclose())
