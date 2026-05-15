from __future__ import annotations

import asyncio
import logging
import pickle
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass
from inspect import isawaitable
from typing import AsyncIterator
from typing import Awaitable
from typing import Final
from typing import Protocol
from typing import runtime_checkable

from grpc import StatusCode
from grpc.aio import AbortError
from grpc.aio import ServicerContext

import wool
from wool import protocol
from wool.runtime.context import attached
from wool.runtime.context import install_task_factory
from wool.runtime.discovery import __subscriber_pool__
from wool.runtime.resourcepool import ResourcePool
from wool.runtime.routine.task import Task
from wool.runtime.serializer import Serializer
from wool.runtime.worker.session import DispatchSession
from wool.runtime.worker.session import Rejected

_log = logging.getLogger(__name__)

_DRAIN_TIMEOUT: Final[float] = 5.0
"""Wall-clock timeout in seconds for the multi-generation task drain
in :meth:`WorkerService._destroy_worker_loop`. Generous enough for a
normal chain of ``finally``-scheduled cleanup tasks to unwind, short
enough not to stall worker-loop teardown; past this timeout the drain
gives up, with the daemon-thread reap as the backstop."""


def _safely_serialize_exception(
    serializer: Serializer,
    exc: BaseException,
) -> bytes:
    """Serialize *exc*, preserving the exception class when the
    original instance carries un-picklable state.

    Prevents un-picklable exception state from converting a
    wool-class failure on the wire into a generic gRPC stream
    error on the caller side. The negotiated serializer is
    always either :class:`PassthroughSerializer` (which can
    never fail — it stashes by reference and returns a token)
    or :class:`CloudpickleSerializer` (which fails on
    un-picklable input via :class:`pickle.PickleError`,
    :class:`TypeError` for un-picklable C types,
    :class:`AttributeError` for un-picklable local closures, or
    :class:`RecursionError` for deeply self-referential graphs).

    **Type-preserving fallback.** Stdlib exception pickling
    round-trips ``(type, args, __dict__)``, dropping
    :attr:`__traceback__`, :attr:`__cause__`, :attr:`__context__`,
    and :attr:`__suppress_context__` — those four are not part of
    the exception's identity. :attr:`__notes__` and other
    ``__dict__`` attributes (including the wool-private
    ``__wool_context_warnings__`` set by the dispatch handler when
    strict-mode :class:`wool.ContextDecodeWarning` peers fire on
    snapshot encode) survive the round-trip. When a routine-level
    exception accumulates state that drags an un-picklable C-level
    object into the graph (e.g. a worker-thread frame on
    :attr:`__traceback__` reachable via :attr:`__cause__`), the
    first ``dumps`` raises but the exception's *class* and *args*
    are still picklable on their own. Reconstruct a clean instance
    and reship — the caller's ``except RoutineError`` still
    matches, mirroring the stdlib pickle contract for exceptions.
    Side-channel attachments (notes, ``__wool_context_warnings__``)
    are lost on the reconstructed instance because the fallback
    builds a fresh ``cls(*exc.args)``; the wire-survival guarantee
    holds only on the primary path.

    If even ``cls(*exc.args)`` cannot be constructed or pickled
    (constructor side effects, unpicklable args), demote to a
    stdlib :class:`RuntimeError` carrying the original class
    name and message — always picklable, so the third ``dumps``
    cannot fail.
    """
    try:
        return serializer.dumps(exc)
    except (pickle.PickleError, TypeError, AttributeError, RecursionError):
        pass
    try:
        cls = type(exc)
        clean = cls(*exc.args)
        return serializer.dumps(clean)
    except Exception:
        # Honor the docstring's "third dumps cannot fail" promise:
        # any reconstruction failure (over-eager ``__init__``
        # validation, custom ``__new__``, un-picklable args, etc.)
        # — not just the narrow pickle/type/recursion set — must
        # fall through to the always-picklable ``RuntimeError``
        # demotion. ``KeyboardInterrupt``/``SystemExit`` still
        # propagate since they are ``BaseException``-only.
        cls_name = type(exc).__name__
        # Guard the f-string. ``__str__`` is user-overridable and
        # can raise (touches per-instance state, delegates to a
        # buggy ``__repr__``, etc.). The bare class name is a
        # string attribute lookup and cannot raise — last resort
        # so the safety net always succeeds.
        try:
            message = f"{cls_name}: {exc!s}"
        except Exception:
            message = cls_name
        return serializer.dumps(RuntimeError(message))


def _attach_strict_mode_warnings(exc: BaseException, encode_exc: BaseException) -> None:
    """Attach strict-mode :class:`wool.ContextDecodeWarning` peers to a
    routine exception.

    When strict mode promotes :class:`wool.ContextDecodeWarning` to an
    exception, the post-run snapshot encode (``session.context.to_protobuf``)
    raises a :class:`BaseExceptionGroup` of warning peers. Attach the peers
    to *exc* via PEP 678 ``__notes__`` (visible in tracebacks) and a
    ``__wool_context_warnings__`` attribute (programmatic access). The
    routine exception's type is preserved, so the caller's existing
    ``except RoutineError:`` clause continues to catch — no migration to
    ``except*`` or ``except ExceptionGroup`` required.

    Both attachment paths are best-effort. ``add_note`` may raise on a
    subclass with an overridden ``__setattr__`` or an unusual C-level
    storage policy; ``setattr`` may raise ``AttributeError`` on frozen
    dataclass exceptions or slotted layouts without ``__dict__``. Either
    is swallowed so the routine's primary signal still ships — the
    warnings simply will not ride on a type that rejects them.

    Lifted out of the dispatch handler's terminal-exception clause as a
    flat sync helper so coverage tooling on Python 3.11 (``sys.settrace``)
    can track it; the deeply nested original lived inside an async
    generator's nested except arm and was opaque to pre-PEP-669 tracing.

    :param exc:
        The routine's primary exception, annotated in place.
    :param encode_exc:
        The encode-time failure carrying the warning peers.
    """
    if isinstance(encode_exc, BaseExceptionGroup):
        context_warnings: list[BaseException] = list(encode_exc.exceptions)
    else:
        context_warnings = [encode_exc]
    try:
        for w in context_warnings:
            exc.add_note(f"wool context warning: {w}")
    except (AttributeError, TypeError):
        pass
    try:
        setattr(exc, "__wool_context_warnings__", context_warnings)
    except AttributeError:
        pass


# public
@dataclass(frozen=True)
class BackpressureContext:
    """Snapshot of worker state provided to backpressure hooks.

    :param active_task_count:
        Number of tasks currently executing on this worker.
    :param task:
        The incoming :class:`~wool.runtime.routine.task.Task` being
        evaluated for admission.
    """

    active_task_count: int
    task: Task


# public
@runtime_checkable
class BackpressureLike(Protocol):
    """Protocol for backpressure hooks.

    A backpressure hook determines whether an incoming task should be
    rejected. Return ``True`` to **reject** the task (apply
    backpressure) or ``False`` to **accept** it.

    When a task is rejected the worker responds with gRPC
    ``RESOURCE_EXHAUSTED``, which the load balancer treats as
    transient and skips to the next worker.

    Pass ``None`` (the default) to accept all tasks unconditionally.

    The hook runs after the caller's wire-shipped ContextVar snapshot
    is applied to the handler's context, so a hook that reads a
    :class:`wool.ContextVar` (e.g., a tenant id) observes the caller's
    value for that dispatch. This enables tenant- or request-scoped
    admission decisions without plumbing values through the
    :class:`BackpressureContext` explicitly.

    Both sync and async implementations are supported::

        def sync_hook(ctx: BackpressureContext) -> bool:
            return ctx.active_task_count >= 4


        async def async_hook(ctx: BackpressureContext) -> bool:
            return ctx.active_task_count >= 4
    """

    def __call__(self, ctx: BackpressureContext) -> bool | Awaitable[bool]:
        """Evaluate whether to reject the incoming task.

        :param ctx:
            Snapshot of the worker's current state and the incoming
            task.
        :returns:
            ``True`` to reject the task, ``False`` to accept it.
        """
        ...


class _ReadOnlyEvent:
    """A read-only wrapper around :class:`asyncio.Event`.

    Provides access to check if an event is set and wait for it to be
    set, but prevents external code from setting or clearing the event.

    :param event:
        The underlying :class:`asyncio.Event` to wrap.
    """

    def __init__(self, event: asyncio.Event):
        self._event = event

    def is_set(self) -> bool:
        """Check if the event is set.

        :returns:
            ``True`` if the event is set, ``False`` otherwise.
        """
        return self._event.is_set()

    async def wait(self) -> None:
        """Wait until the underlying event is set."""
        await self._event.wait()


class WorkerService(protocol.WorkerServicer):
    """gRPC service for task execution.

    Implements the worker gRPC interface for receiving and executing
    tasks. Runs tasks in the current asyncio event loop and streams
    results back to the client.

    Handles graceful shutdown by rejecting new tasks while allowing
    in-flight tasks to complete. Exposes :attr:`stopping` and
    :attr:`stopped` events for lifecycle monitoring.

    :param backpressure:
        Optional admission control hook. See
        :class:`BackpressureLike`. ``None`` (default) accepts all
        tasks unconditionally.
    """

    _docket: set[DispatchSession]
    _stopped: asyncio.Event
    _stopping: asyncio.Event
    _loop_pool: ResourcePool[tuple[asyncio.AbstractEventLoop, threading.Thread]]

    def __init__(self, *, backpressure: BackpressureLike | None = None):
        self._stopped = asyncio.Event()
        self._stopping = asyncio.Event()
        self._docket = set()
        self._backpressure = backpressure
        # Budget for the loop-teardown join, set by :meth:`_stop`
        # from the StopRequest's ``timeout``. ``0`` means "do not
        # synchronously wait" (worker thread closes its own loop
        # after ``run_forever`` returns; ``daemon=True`` reaps it
        # at process exit if not joined); positive bounds the
        # wait; ``None`` means "wait indefinitely" — caller asked
        # for unlimited graceful shutdown via a negative or
        # missing StopRequest timeout. The default below is
        # ``None`` only because the loop pool's finalizer
        # schedules its own ``loop.stop()``, so an unbounded join
        # still returns once ``run_forever`` exits.
        self._stop_timeout: float | None = None
        self._loop_pool = ResourcePool(
            factory=self._create_worker_loop,
            finalizer=self._destroy_worker_loop,
            ttl=0,
        )

    @property
    def stopping(self) -> _ReadOnlyEvent:
        """Read-only event signaling that the service is stopping.

        :returns:
            A :class:`_ReadOnlyEvent`.
        """
        return _ReadOnlyEvent(self._stopping)

    @property
    def stopped(self) -> _ReadOnlyEvent:
        """Read-only event signaling that the service has stopped.

        :returns:
            A :class:`_ReadOnlyEvent`.
        """
        return _ReadOnlyEvent(self._stopped)

    async def dispatch(
        self,
        request_iterator: AsyncIterator[protocol.Request],
        context: ServicerContext,
    ) -> AsyncIterator[protocol.Response]:
        """Execute a task in the current event loop.

        Reads the first :class:`~wool.protocol.Request` from
        the bidirectional stream to obtain the :class:`Task`, then
        schedules it for execution. For async generators, subsequent
        ``Message`` frames are forwarded into the generator via
        ``asend()``.

        **Context serialization failures (worker-side).**
        Wire context is **ancillary state** under wool's protocol
        contract: a failure to serialize the post-run snapshot or to
        deserialize an incoming context (initial request or
        mid-stream frame) is non-fatal in non-strict mode but fatal
        in strict mode. Both modes emit a
        :class:`wool.ContextDecodeWarning` for each failure.

        *Non-strict mode (default).* The routine still runs — with a
        fresh empty context as fallback when initial-frame
        deserialization fails — and the back-propagated snapshot is
        replaced with an empty context when post-run serialization
        fails. A snapshot serialization failure that coincides with
        a routine exception rides back as peers in a
        :class:`BaseExceptionGroup` (extending an existing group
        when the routine exception is already grouped) rather than
        as nested causes, so the caller observes both signals at
        the same level.

        *Strict mode* (e.g.,
        ``PYTHONWARNINGS=error::wool.ContextDecodeWarning``). The
        warning promotes to an exception. The dispatch handler
        catches :class:`wool.ContextDecodeWarning` raised before the
        routine starts and ships it via the routine-exception
        channel — the routine does not run — so the caller catches
        the same warning class symmetrically with caller-side strict
        mode rather than seeing a generic gRPC error. Promotions
        raised after the routine starts surface through the
        existing routine-exception machinery.

        **Wire-protocol invariant.** A ``Nack`` is only emitted
        pre-Ack; once an ``Ack`` has been yielded, all further
        terminal signals ride on ``Response.exception``. The dispatch
        FSM is ``Ack? (Result* (Exception | ε)) | Nack``. Code that
        emits a Nack after an Ack would violate the caller-side
        consumer contract in :class:`WorkerConnection`.

        :param request_iterator:
            The incoming bidirectional request stream.
        :param context:
            The :class:`grpc.aio.ServicerContext` for this request.
        :yields:
            One of four terminal shapes per dispatch.

            **Parse-failure path** — a single ``Response`` whose
            ``nack`` payload carries the parse-time failure (with
            ``exception`` set to the dumped original cause). No
            preceding ``Ack``. Triggered by malformed task id,
            unpicklable serializer hint, strict-mode
            :class:`wool.ContextDecodeWarning`, cloudpickle errors
            on the task callable, ImportError on a missing module,
            or non-async callable.

            **Routine-success path** — an ``Ack`` Response, then
            one (coroutine) or many (async-generator) ``result``
            Responses, then stream end.

            **Routine-failure path** — an ``Ack`` Response,
            optionally followed by zero or more ``result``
            Responses, then a single terminal ``exception``
            Response carrying the dumped routine / handler-level
            failure plus a ``context`` snapshot.

            **Routine-failure-with-encode-failure variant** — same
            as the routine-failure path except the terminal
            ``Response`` drops the ``context`` field. The post-run
            snapshot itself failed to serialize (strict-mode
            :class:`wool.ContextDecodeWarning`); the encode peers
            are attached to the routine exception via PEP 678
            ``__notes__`` and a ``__wool_context_warnings__``
            attribute, so the caller-visible exception class is
            preserved.

            **Operator pre-emption.** A worker-side graceful
            shutdown cancels in-flight dispatches and the underlying
            ``CancelledError`` ships unchanged on the routine-failure
            path. Callers observe ``CancelledError`` from
            ``await routine()`` regardless of whether the cancel
            was caller-initiated, routine-self-raised, or operator-
            initiated — mirroring stdlib's ``await task`` semantics
            where ``task.cancel()`` from any source produces the
            same observable.

        """
        if self._stopping.is_set():
            await context.abort(
                StatusCode.UNAVAILABLE, "Worker service is shutting down"
            )

        async with self._loop_pool.get("worker") as (loop, _):
            # Instantiate before ``async with`` so a ``Rejected`` raised
            # from :meth:`DispatchSession.__aenter__` (parse-phase failure)
            # leaves ``session`` bound for the ``except Rejected`` arm's
            # access to ``session.serializer`` (initialized to the default
            # cloudpickle in :meth:`__init__`, replaced with the negotiated
            # serializer only on successful parse).
            session = DispatchSession(request_iterator, loop)

            # Register a deterministic cancellation propagation hook
            # via the gRPC context's ``add_done_callback``. Necessary
            # because async-generator-task cancellation propagation
            # through ``await response_queue.get()`` is unreliable on
            # Python 3.11 + Linux: the gRPC framework's cancellation
            # of the handler task does not always wake the handler's
            # suspension at the response queue, leaving the routine
            # to run to natural completion after the caller has gone
            # away. The done callback fires from gRPC's internal
            # thread when the RPC reaches a terminal state; on a
            # client-side cancellation it fires with
            # ``context.cancelled()`` == True, and we schedule
            # :meth:`DispatchSession.cancel` on the main loop via
            # ``call_soon_threadsafe`` so the routine task is
            # cancelled cross-loop on the same path
            # ``WorkerService._cancel`` uses for graceful shutdown.
            # ``DispatchSession.cancel`` is idempotent, so the
            # dispatch handler's own except-clause cancel is a no-op
            # if this callback raced ahead. Avoids the watcher-task
            # pattern (a polling background task) because awaiting a
            # cancelled task in the dispatch generator's finally
            # block deadlocks under Python 3.11's async-generator
            # cancellation handling on Linux.
            main_loop = asyncio.get_running_loop()

            def _propagate_cancel_on_done(ctx) -> None:
                if not ctx.cancelled():
                    return
                try:
                    main_loop.call_soon_threadsafe(
                        lambda: main_loop.create_task(session.cancel())
                    )
                except RuntimeError:
                    # Main loop already closed (graceful shutdown
                    # raced us). Nothing to propagate to; the
                    # session's resources are torn down by the
                    # surrounding context-manager unwind.
                    pass

            # grpc.aio's :meth:`ServicerContext.add_done_callback` is
            # typed in typeshed as
            # ``Callable[[_DoneCallback[_TRequest, _TResponse]], None]``
            # where ``_DoneCallback`` is a generic callable *class*.
            # A plain function does not satisfy that nominal class
            # type, but the runtime call accepts any callable — see
            # grpcio's implementation, which only calls the object
            # with a single ``ctx`` argument. The ignore documents
            # the discrepancy between the stub's nominal type and
            # the structural runtime contract.
            context.add_done_callback(_propagate_cancel_on_done)  # pyright: ignore[reportArgumentType]

            try:
                async with session:
                    if self._backpressure is not None:
                        backpressure = self._backpressure
                        # ``guarded=False`` — the dispatch task is not
                        # running the routine itself, only reading
                        # caller-shipped wool.ContextVar values for the
                        # hook. The single-task ownership of
                        # ``session.context`` belongs to the worker
                        # task scheduled lazily on the first
                        # ``__aiter__`` call below; entering the
                        # guard here would race that scheduling
                        # under ``Context._lock``.
                        try:
                            with attached(session.context, guarded=False):
                                decision = backpressure(
                                    BackpressureContext(
                                        active_task_count=len(self._docket),
                                        task=session.task,
                                    )
                                )
                                if isawaitable(decision):
                                    decision = await decision
                        except Exception:
                            # User-supplied backpressure hook crashed.
                            # Log so the operator notices, then abort
                            # with INTERNAL so the caller-side maps
                            # to RpcError and the load-balancer takes
                            # over rotation. Treating a hook bug as
                            # eviction-worthy is intentional under
                            # today's binary LB policy; health-aware
                            # forgiveness (N-strikes) is a follow-up.
                            _log.exception("Backpressure hook raised; aborting dispatch")
                            await context.abort(
                                StatusCode.INTERNAL,
                                "Backpressure hook raised",
                            )
                        if decision:
                            await context.abort(
                                StatusCode.RESOURCE_EXHAUSTED,
                                "Task rejected by backpressure hook",
                            )

                    async with self._tracked(session, context):
                        yield protocol.Response(
                            ack=protocol.Ack(version=protocol.__version__)
                        )
                        try:
                            async for response in session:
                                yield response.to_protobuf(serializer=session.serializer)
                        except (Exception, asyncio.CancelledError) as e:
                            # Cancel the session before drain on the
                            # error path so a routine suspended
                            # inside an ``await`` observes
                            # ``CancelledError`` instead of running
                            # to natural completion after the caller
                            # has gone away. The success path skips
                            # cancel — the worker has already exited
                            # and closed the response stream, and a
                            # spurious cancel here would race the
                            # drain's own cancellation handling.
                            #
                            # Swallow cancel-time failures. Re-raising
                            # them inside this except would replace
                            # ``e``, demoting the routine's primary
                            # signal to ``__context__`` and shipping
                            # the cancel failure to the caller.
                            try:
                                await session.cancel()
                            except BaseException:
                                pass
                            # All worker-side and main-side failures
                            # land here: routine exceptions raised in
                            # :func:`_step` propagate through the
                            # response queue and out of
                            # :meth:`DispatchSession.__aiter__` raw;
                            # pre-stream worker setup failures surface
                            # via :meth:`_ResponseQueue.get` raising on
                            # close; mid-stream context-decode /
                            # update failures escape :func:`_step` the
                            # same way; handler-level failures (e.g.
                            # ``response.to_protobuf`` raising) raise
                            # directly here; gRPC stream cancellation
                            # raises ``CancelledError`` mid-iteration.
                            # Drain the worker before snapshotting
                            # ``session.context``: worker-failure
                            # paths arrive with the worker already
                            # finalized (so drain is a no-op), but
                            # cancellation and main-loop handler-
                            # level failures leave the worker mid-
                            # ``_step``, racing the snapshot's read
                            # of ``_data`` against the worker's
                            # ``work_ctx.update`` /
                            # ``work_ctx.to_protobuf`` writes.
                            # :meth:`DispatchSession.drain` is
                            # idempotent — :meth:`__aexit__` will
                            # call it again on the way out. On the
                            # external-cancellation path drain may
                            # re-raise ``CancelledError`` before
                            # the snapshot can be built; the gRPC
                            # stream is being torn down anyway, so
                            # losing the terminal Response is
                            # acceptable — the caller has no
                            # consumer left.
                            await session.drain()
                            # Unwrap PEP 525's auto-conversion for
                            # coroutine routines so the caller's
                            # ``await routine()`` surfaces the
                            # original :class:`StopAsyncIteration`
                            # raw — matching stdlib coroutine
                            # semantics. The wrap happens in
                            # :meth:`DispatchSession._iterate` (the
                            # asyncgen transport layer): when a
                            # coroutine raises StopAsyncIteration,
                            # _ResponseQueue.get re-raises it inside
                            # _iterate's body, and PEP 525 converts
                            # it to ``RuntimeError("async generator
                            # raised StopAsyncIteration")`` with the
                            # original SAI on ``__cause__``.
                            # Streaming routines keep the
                            # RuntimeError shape — that already
                            # matches stdlib ``async for x in
                            # agen()`` semantics.
                            if (
                                not session.streaming
                                and isinstance(e, RuntimeError)
                                and isinstance(e.__cause__, StopAsyncIteration)
                            ):
                                e = e.__cause__
                            try:
                                wire_context = session.context.to_protobuf(
                                    serializer=session.serializer
                                )
                            except Exception as encode_exc:
                                # Strict-mode-only path: attach the
                                # encoded ``ContextDecodeWarning``
                                # peers to ``e`` so the caller's
                                # ``except RoutineError`` clause keeps
                                # matching. See
                                # :func:`_attach_strict_mode_warnings`
                                # for the attachment contract and
                                # rationale. Drops the post-run
                                # ``context`` field on the wire (the
                                # snapshot itself failed); peers ride
                                # on the routine exception via PEP 678
                                # ``__notes__`` and
                                # ``__wool_context_warnings__``.
                                _attach_strict_mode_warnings(e, encode_exc)
                                yield protocol.Response(
                                    exception=protocol.Message(
                                        dump=_safely_serialize_exception(
                                            session.serializer, e
                                        )
                                    ),
                                )
                            else:
                                yield protocol.Response(
                                    exception=protocol.Message(
                                        dump=_safely_serialize_exception(
                                            session.serializer, e
                                        )
                                    ),
                                    context=wire_context,
                                )
            except Rejected as e:
                # Parse-phase failure (malformed task payload).
                # Reported via Nack so the client deserializes the
                # dumped exception and re-raises it as the actual
                # failure class rather than an opaque RpcError. The
                # dump uses ``session.serializer`` — the negotiated
                # serializer if parse got past serializer setup,
                # falling back to ``wool.__serializer__``
                # (cloudpickle) for early-fail paths. Same path as
                # ``Response.exception`` post-Ack — symmetry on the
                # wire.
                yield protocol.Response(
                    nack=protocol.Nack(
                        exception=protocol.Message(
                            dump=_safely_serialize_exception(
                                session.serializer, e.original
                            )
                        ),
                    ),
                )
                return
            except AbortError:
                # Intentional ``context.abort(...)`` calls inside the
                # try block (backpressure rejection, hook-crash) raise
                # ``AbortError`` which subclasses ``Exception``. Let
                # them propagate so the gRPC framework reports the
                # operator-chosen status code; do NOT mis-log them as
                # an unexpected server-side bug.
                raise
            except Exception:
                # Unexpected server-side bug (dispatch handler crash,
                # library error, or any other failure that escapes
                # the routine-failure and parse-failure paths above).
                # Log so the operator notices, then abort with
                # INTERNAL so the caller-side maps to RpcError and
                # the load balancer takes over rotation. Today's
                # binary LB policy evicts on first RpcError; health-
                # aware forgiveness (N-strikes) is a follow-up.
                _log.exception("Unexpected dispatch handler error")
                await context.abort(StatusCode.INTERNAL, "Internal server error")

    async def stop(
        self,
        request: protocol.StopRequest,
        context: ServicerContext | None,
    ) -> protocol.Void:
        """Stop the worker service and its thread.

        Gracefully shuts down the worker thread and signals the server
        to stop accepting new requests. This method is idempotent and
        can be called multiple times safely.

        :param request:
            The protobuf stop request containing the wait timeout.
        :param context:
            The :class:`grpc.aio.ServicerContext` for this request.
        :returns:
            An empty protobuf response indicating completion.
        """
        if self._stopping.is_set():
            return protocol.Void()
        await self._stop(timeout=request.timeout)
        return protocol.Void()

    @staticmethod
    def _create_worker_loop(
        key,
    ) -> tuple[asyncio.AbstractEventLoop, threading.Thread]:
        """Create a new event loop running on a dedicated daemon thread.

        The thread target wraps :meth:`asyncio.AbstractEventLoop.run_forever`
        in a ``try/finally`` that calls :meth:`asyncio.AbstractEventLoop.close`
        once ``run_forever`` returns. Closing the loop from the worker
        thread (rather than the caller's thread inside
        :meth:`_destroy_worker_loop`) eliminates the race that produced
        ``RuntimeError("Cannot close a running event loop")`` when a
        caller-side close raced the still-active ``run_forever``.

        :param key:
            The :class:`ResourcePool` cache key (unused).
        :returns:
            A tuple of the event loop and the thread running it.
        """
        loop = asyncio.new_event_loop()
        install_task_factory(loop)

        def _run_then_close():
            try:
                loop.run_forever()
            finally:
                loop.close()

        thread = threading.Thread(target=_run_then_close, daemon=True)
        thread.start()
        return loop, thread

    def _destroy_worker_loop(
        self,
        loop_thread: tuple[asyncio.AbstractEventLoop, threading.Thread],
    ) -> None:
        """Schedule worker-loop shutdown and optionally join the thread.

        Drains successive generations of pending tasks on the
        worker loop, then signals the loop to stop. A cancelled
        task's ``finally`` clause can schedule a second generation
        of tasks (e.g. follow-up cleanup, fire-and-forget logging,
        further cancellations, etc.); the drain cancels and awaits
        successive generations until none remain or
        :data:`_DRAIN_TIMEOUT` elapses, so the loop closes without
        leaking ``Task was destroyed but it is pending!`` warnings.
        If a routine schedules cleanup-of-cleanup past the budget,
        the drain stops anyway and the daemon-thread reap remains
        the backstop. The loop is closed by the worker
        thread itself (see :meth:`_create_worker_loop`'s
        ``_run_then_close`` target), not from this caller's thread —
        eliminating the close-while-running race.

        Joins the worker thread for up to :attr:`_stop_timeout`
        seconds (set by :meth:`_stop` from the StopRequest's
        ``timeout``). ``timeout=0`` means "do not wait"; positive
        values bound the synchronous wait; ``None`` means "wait
        indefinitely" (caller asked for unlimited graceful shutdown).
        If the join times out, the daemon thread is reaped at
        process exit — the loop will still close itself once
        ``run_forever`` returns.

        :param loop_thread:
            A tuple of the event loop and the thread running it.
        """
        loop, thread = loop_thread

        async def _shutdown():
            current = asyncio.current_task()
            deadline = loop.time() + _DRAIN_TIMEOUT
            leaked: list[asyncio.Task] = []
            try:
                while True:
                    pending = [
                        task for task in asyncio.all_tasks() if task is not current
                    ]
                    if not pending:
                        break
                    for task in pending:
                        task.cancel()
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        leaked = pending
                        break
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*pending, return_exceptions=True),
                            timeout=remaining,
                        )
                    except TimeoutError:
                        leaked = pending
                        break
                if leaked:
                    _log.warning(
                        f"Worker-loop teardown drain timed out after "
                        f"{_DRAIN_TIMEOUT}s; {len(leaked)} task(s) still pending."
                    )
            finally:
                loop.stop()

        try:
            loop.call_soon_threadsafe(lambda: loop.create_task(_shutdown()))
        except RuntimeError:
            # Loop is already closed (e.g., this finalizer was
            # invoked twice, or some external party closed it).
            # Nothing to schedule; the thread has already exited
            # via the ``_run_then_close`` finally clause.
            return

        timeout = self._stop_timeout
        if timeout is None or timeout > 0:
            thread.join(timeout=timeout)

    @asynccontextmanager
    async def _tracked(
        self,
        session: DispatchSession,
        context: ServicerContext,
    ) -> AsyncIterator[None]:
        """Add *session* to :attr:`_docket` for the duration of the
        yield, removing it on exit.

        The docket is the registry of in-flight
        :class:`DispatchSession` instances that :meth:`_stop`
        pre-empts on graceful shutdown. The CM scope mirrors the
        dispatch handler's iteration scope, so an in-flight
        dispatch is always either tracked or already finalized.

        Re-checks :attr:`_stopping` on entry to close the
        check-to-register window in :meth:`dispatch` — a concurrent
        :meth:`_stop` between the entry gate and docket registration
        would otherwise admit a session that :meth:`_preempt` never
        sees, leaving it to be torn down indirectly by loop-pool
        teardown rather than the explicit cancel path.
        """
        if self._stopping.is_set():
            await session.cancel()
            await context.abort(
                StatusCode.UNAVAILABLE, "Worker service is shutting down"
            )
        self._docket.add(session)
        try:
            yield
        finally:
            self._docket.discard(session)

    async def _stop(self, *, timeout: float | None = 0) -> None:
        if timeout is not None and timeout < 0:
            timeout = None
        # Stash the StopRequest's timeout for the loop-teardown
        # finalizer (read by :meth:`_destroy_worker_loop`) before
        # any ``await`` so it is always set when ``_loop_pool.clear``
        # later invokes the finalizer. ``timeout=0`` (the default)
        # → don't synchronously join; positive → bound the join;
        # ``None`` (caller sent negative or omitted) → wait
        # indefinitely.
        self._stop_timeout = timeout
        self._stopping.set()
        await self._preempt(timeout=timeout)
        try:
            if proxy_pool := wool.__proxy_pool__.get():
                await proxy_pool.clear()
            if subscriber_pool := __subscriber_pool__.get():
                await subscriber_pool.clear()
        finally:
            await self._loop_pool.clear()
            self._stopped.set()

    async def _preempt(self, *, timeout: float | None = 0) -> None:
        """Drain or cancel in-flight tasks in the docket.

        The service-wide pre-emption entry point. Waits for running
        tasks to complete or cancels them depending on the timeout
        value. Calls :meth:`DispatchSession.cancel` on each session
        in the docket when forced cancellation is required, which
        propagates :class:`asyncio.CancelledError` to the routine.
        The caller observes ``CancelledError`` from
        ``await routine()``, matching stdlib's ``task.cancel()``
        semantics — operator pre-emption is indistinguishable from
        caller-side cancel or routine-self-raised cancel on the
        wire.

        :param timeout:
            Maximum time to wait for tasks to complete. If 0 (default),
            tasks are canceled immediately. If None, waits indefinitely.
            If a positive number, waits for that many seconds before
            canceling tasks.

        .. note::
            If a timeout occurs while waiting for tasks to complete,
            the method recursively calls itself with a timeout of 0
            to cancel all remaining tasks immediately.
        """
        if self._docket and timeout == 0:
            await asyncio.gather(
                *(s.cancel() for s in self._docket), return_exceptions=True
            )
        elif self._docket:
            try:
                await asyncio.wait_for(self._await(), timeout=timeout)
            except asyncio.TimeoutError:
                return await self._preempt(timeout=0)

    async def _await(self):
        while self._docket:
            await asyncio.sleep(0)
