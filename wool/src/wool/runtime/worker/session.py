"""Per-dispatch routine handler.

Layered abstractions cover the worker-side dispatch lifetime:

- :class:`_RequestQueue` / :class:`_ResponseQueue` — cross-loop
  queues bridging the gRPC main loop and the worker loop.
- :func:`_step` — inner routine stepper. One step per request,
  yields one :class:`_Response`. Routine-shape variation
  (coroutine = one step; async-generator = N steps) lives here.
- :class:`DispatchSession` — per-dispatch async context manager
  and iterator that owns parse, lazy worker scheduling, drive,
  drain, and cancel.

Each class's docstring carries the per-piece detail.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from contextlib import AsyncExitStack
from dataclasses import dataclass
from inspect import isasyncgenfunction
from inspect import iscoroutinefunction
from typing import Any
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Coroutine
from typing import Final
from typing import Literal
from typing import assert_never
from typing import cast
from uuid import UUID

import wool
from wool import protocol
from wool.runtime.context import Snapshot
from wool.runtime.context import current_snapshot
from wool.runtime.context import decode_snapshot
from wool.runtime.context import encode_snapshot
from wool.runtime.context import install_snapshot
from wool.runtime.context import merge_snapshot
from wool.runtime.context import snapshot_has_state
from wool.runtime.context.snapshot import _current_owner_ref
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import _unpickle_serializer
from wool.runtime.routine.task import routine_scope
from wool.runtime.serializer import PassthroughSerializer
from wool.runtime.serializer import Serializer
from wool.runtime.serializer import _passthrough_pool
from wool.runtime.worker.connection import _complete_teardown

__all__ = ["DispatchSession", "Rejected"]

_log = logging.getLogger(__name__)


class _EndOfStream:
    """Marker type for the end-of-stream sentinel pushed onto
    :class:`_RequestQueue` and :class:`_ResponseQueue` to wake a
    suspended ``get`` after :meth:`close`. Identity is unique by
    construction (one instance, :data:`_EOS`); the dedicated type
    parameterizes both queues precisely without falling back to
    ``object`` or a string ``Literal``.
    """


_EOS: Final[_EndOfStream] = _EndOfStream()
"""Singleton sentinel marking end of a queue-based dispatch stream."""


@dataclass
class _Response:
    """One frame on the response side of the dispatch protocol.

    Carries a successful step result and the post-step
    :class:`protocol.Context` snapshot so the handler can ship
    caller-visible mutations on the Response. Failures never reach
    this type — they propagate raw out of :func:`_step` and ship
    through the dispatch handler's terminal-exception clause
    instead.
    """

    result: Any
    context: protocol.Context

    def to_protobuf(self, *, serializer: Serializer) -> protocol.Response:
        """Build a :class:`protocol.Response` from this frame.

        Serializes ``result`` via *serializer* and attaches the
        post-step context (ID + var snapshot) on the response.

        :param serializer:
            Negotiated serializer for the payload (cloudpickle for
            cross-process dispatch; :class:`PassthroughSerializer`
            for self-dispatch).
        """
        return protocol.Response(
            result=protocol.Message(dump=serializer.dumps(self.result)),
            context=self.context,
        )


@dataclass
class _Request:
    """One request on the dispatch protocol.

    Wire-decoded on the caller (main-loop) side via
    :meth:`from_protobuf`, pushed cross-loop to the worker, and
    consumed by :func:`_step` on the worker loop.

    :param action:
        The async-generator step verb: ``"next"`` advances without
        a value (``asend(None)``; also synthesized for coroutine
        routines that take a single step), ``"send"`` advances
        with a payload (``asend(payload)``), ``"throw"`` injects
        an exception (``athrow(payload)``).
    :param payload:
        The decoded payload for ``send``/``throw``; ``None`` for
        ``next``.
    :param caller_wire_context:
        The caller's :class:`protocol.Context` (to be decoded and
        merged into the active work snapshot before the step runs).
    """

    action: Literal["next", "send", "throw"]
    payload: Any
    caller_wire_context: protocol.Context

    @classmethod
    def from_protobuf(
        cls,
        request: protocol.Request,
        *,
        serializer: Serializer,
    ) -> _Request:
        """Decode a :class:`protocol.Request` into a request object.

        Reads the ``payload`` oneof and decodes ``send``/``throw``
        bodies via *serializer*. Any pickled :class:`wool.ContextVar`
        / :class:`wool.Token` in the payload reconstitutes against the
        process-wide variable and token registries — pure identity,
        with no dependence on which snapshot is active. The
        ``request.context`` field is forwarded as
        ``caller_wire_context`` for the worker-loop side to decode and
        merge into the active snapshot before the routine step runs.

        :param request:
            The incoming :class:`protocol.Request`.
        :param serializer:
            Negotiated serializer for the payload.
        :raises ValueError:
            If the ``payload`` oneof is unset or unknown — the wire
            envelope parsed cleanly but carries no recognizable
            iteration command.
        """
        match request.WhichOneof("payload"):
            case "next":
                return cls("next", None, request.context)
            case "send":
                value = serializer.loads(request.send.dump)
                return cls("send", value, request.context)
            case "throw":
                exc = serializer.loads(request.throw.dump)
                return cls("throw", exc, request.context)
            case _:  # pragma: no cover — defensive default for proto oneof
                raise ValueError(
                    f"unknown request payload oneof: {request.WhichOneof('payload')!r}"
                )


class _RequestQueue:
    """Cross-loop queue carrying gRPC request envelopes from the
    main (gRPC) loop to the worker loop's :func:`_step` driver.

    Producers on the main loop push :class:`protocol.Request`
    envelopes via :meth:`put`. The consumer on the worker loop pulls
    them via :meth:`get`, which decodes each envelope into a
    :class:`_Request` via :meth:`_Request.from_protobuf` before
    returning. Decoding on the worker side keeps payload
    deserialization (which may reconstitute pickled
    :class:`wool.ContextVar` / :class:`wool.Token` instances) inside
    the worker-loop task that runs the routine under the work
    snapshot.

    Closure: :meth:`close` pushes a sentinel so :meth:`get` returns
    :data:`None` once the producer side is done.
    """

    def __init__(
        self,
        worker_loop: asyncio.AbstractEventLoop,
        *,
        serializer: Serializer,
    ) -> None:
        self._queue: asyncio.Queue[protocol.Request | _EndOfStream] = asyncio.Queue()
        self._worker_loop = worker_loop
        self._serializer = serializer

    def put(self, request: protocol.Request) -> None:
        """Push a :class:`protocol.Request` onto the queue.

        Cross-loop safe — schedules the put on the worker loop via
        :func:`asyncio.AbstractEventLoop.call_soon_threadsafe`.
        """
        self._worker_loop.call_soon_threadsafe(self._queue.put_nowait, request)

    async def get(self) -> _Request | None:
        """Pop the next decoded :class:`_Request`, or :data:`None`
        when the queue has been :meth:`close`\\ d.

        Awaitable on the worker loop only.
        """
        item = await self._queue.get()
        if isinstance(item, _EndOfStream):
            return None
        return _Request.from_protobuf(item, serializer=self._serializer)

    def close(self) -> None:
        """Signal end of input by pushing the close sentinel.
        Cross-loop safe."""
        self._worker_loop.call_soon_threadsafe(self._queue.put_nowait, _EOS)


class _ResponseQueue:
    """Cross-loop queue carrying :class:`_Response` frames from the
    worker loop's :func:`_step` driver back to the main (gRPC)
    loop's :meth:`DispatchSession.__aiter__`.

    Producers on the worker loop push frames via :meth:`put` and
    signal end-of-stream via :meth:`close`. The consumer on the
    main loop pulls them via :meth:`get`, which returns :data:`None`
    after a clean termination (the routine exhausted or returned)
    and **raises** the worker task's underlying exception when the
    worker died — the queue holds a reference to the
    worker-completion :class:`concurrent.futures.Future` so the
    sentinel-and-failure check co-locates with the close sentinel
    that triggers it. The exception propagates out of
    :meth:`DispatchSession.__aiter__` for the dispatch handler's
    terminal-exception clause to ship.
    """

    def __init__(
        self,
        main_loop: asyncio.AbstractEventLoop,
        worker_done: concurrent.futures.Future,
    ) -> None:
        # Unbounded by necessity: both response-frame pushes (the
        # data path) and ``_EOS`` pushes (close + ``_on_done``)
        # share this queue via ``put_nowait``, so a hard cap would
        # need to leave headroom for one or two sentinel slots. The
        # actual invariant — bounded by producer/consumer
        # alternation in :func:`_run` and
        # :meth:`DispatchSession._iterate` to ≤1 response in flight
        # — is enforced structurally there: the worker pushes one
        # response, then awaits the next request before pushing
        # again. A future change that decouples that cadence
        # (prefetch, batching) needs to add explicit backpressure
        # here rather than relying on this queue to provide it.
        self._queue: asyncio.Queue[_Response | _EndOfStream] = asyncio.Queue()
        self._main_loop = main_loop
        self._worker_done = worker_done

    def put(self, response: _Response) -> None:
        """Push a :class:`_Response` onto the queue.

        Cross-loop safe — schedules the put on the main loop via
        :func:`asyncio.AbstractEventLoop.call_soon_threadsafe`.
        """
        self._main_loop.call_soon_threadsafe(self._queue.put_nowait, response)

    async def get(self) -> _Response | None:
        """Pop the next response, or :data:`None` after a clean
        :meth:`close`.

        **Raises** the worker task's exception when the close
        sentinel arrives and ``worker_done`` carries one —
        surfacing worker failures (pre-stream, routine-time, or
        cancellation) up to :meth:`DispatchSession.__aiter__` so they
        propagate to the dispatch handler's terminal-exception
        clause.

        Awaitable on the main loop only.
        """
        result = await self._queue.get()
        if isinstance(result, _EndOfStream):
            # The worker-completion future is the synchronization
            # primitive: when the worker dies with an exception,
            # ``worker_done`` is set before the close sentinel is
            # observable here, so reading the exception (if any)
            # surfaces worker failures alongside the EOS sentinel.
            # A clean routine end may close before the worker task
            # finishes, in which case ``worker_done`` is still
            # pending — return ``None`` either way.
            if self._worker_done.done():
                exc = self._worker_done.exception()
                if exc is not None:
                    raise exc
            return None
        return result

    def close(self) -> None:
        """Signal end of responses by pushing the close sentinel.
        Cross-loop safe."""
        self._main_loop.call_soon_threadsafe(self._queue.put_nowait, _EOS)


async def _step(
    routine: Coroutine | AsyncGenerator,
    streaming: bool,
    request: _Request,
    *,
    serializer: Serializer,
) -> _Response:
    """Drive *routine* through one *request* and return the
    corresponding :class:`_Response`.

    Decodes the caller's wire context, merges it into the active
    work snapshot, then steps the routine (``await routine`` for
    coroutines; ``asend|athrow`` for async-generators). Returns a
    result-bearing :class:`_Response` carrying the post-step
    snapshot.

    Most exceptions propagate raw — :class:`StopAsyncIteration`,
    routine-raised exceptions, snapshot encode failures — because
    the dispatch handler ships the next failure it catches as the
    routine's terminal frame on the wire. A
    :class:`BaseExceptionGroup` from the per-step caller-context
    decode is rewrapped with a "mid-stream" label so it's
    distinguishable from the initial-frame variant in tracebacks;
    the rewrap preserves the umbrella class so the constructor's
    auto-downgrade still routes Exception-only peers along the
    routine-failure path and leaves non-Exception peers (e.g.
    ``KeyboardInterrupt``) to tear the dispatch down rather than
    ship as a typed response.
    """
    try:
        incoming = decode_snapshot(request.caller_wire_context, serializer=serializer)
    except BaseExceptionGroup as eg:
        raise BaseExceptionGroup(
            "mid-stream request context decode failed",
            list(eg.exceptions),
        ) from eg
    if snapshot_has_state(incoming):
        merge_snapshot(incoming)
    if streaming:
        gen = cast(AsyncGenerator, routine)
        match request.action:
            case "next":
                value = await gen.asend(None)
            case "send":
                value = await gen.asend(request.payload)
            case "throw":
                value = await gen.athrow(request.payload)
            case _:  # pragma: no cover
                assert_never(request.action)
    else:
        value = await cast(Coroutine, routine)
    return _Response(
        result=value,
        context=encode_snapshot(current_snapshot(), serializer=serializer),
    )


class Rejected(Exception):
    """Raised by :meth:`DispatchSession.__aenter__` when the dispatch
    parse phase fails — serializer setup, Wool context snapshot
    decode, :class:`wool.Task` rebuild, or routine-type validation.

    The dispatch handler catches this and replies with a Nack
    whose ``exception`` field carries :attr:`original` serialized
    via the session's own ``serializer`` attribute (the negotiated
    serializer if it was materialized before the failure, falling
    back to ``wool.__serializer__`` for early-fail paths). Same
    path as a routine-time failure's ``Response.exception``.

    :param original:
        The actual parse-phase exception to ship.
    """

    def __init__(self, original: Exception) -> None:
        super().__init__(f"{type(original).__name__}: {original!s}")
        self.original = original


class DispatchSession:
    """Per-dispatch worker-side handler.

    Async context manager and async iterator that owns the routine's
    worker-side lifetime end-to-end:

    - **Parse phase** (``__aenter__``) reads the first
      :class:`protocol.Request` off *request_iterator* and parses
      it: resolves the negotiated serializer (cloudpickle for
      cross-process dispatch, a per-task
      :class:`PassthroughSerializer` from :data:`_passthrough_pool`
      for self-dispatch), decodes the caller's Wool context
      snapshot, rebuilds the wool.Task, and validates the routine
      type. Failures are wrapped in :class:`Rejected` so the
      dispatch handler can surface them via Nack-with-exception. A
      first-request read failure (empty iterator, gRPC error)
      propagates raw — no parsed payload exists to serialize for the
      caller.

      The worker-loop driver is **not** scheduled here; that
      happens lazily on the first ``__aiter__`` call so the
      dispatch handler can run pre-iteration decisions
      (backpressure) against the parsed task and snapshot on the
      main-loop thread before the worker task re-stamps the work
      snapshot onto its own thread.

    - **Iteration** (``__aiter__``) schedules the worker driver on
      first call and drives the request/response loop on the main
      loop. Sets up cross-loop :class:`_RequestQueue` /
      :class:`_ResponseQueue` and submits a worker-loop task that
      enters :func:`routine_scope` for the parsed task and drives the
      routine through :func:`_step`. The
      :class:`concurrent.futures.Future` held by the response
      queue surfaces pre-stream worker failures so they propagate
      out of :meth:`_ResponseQueue.get` rather than hang
      iteration. Forwards each subsequent
      :class:`protocol.Request` from *request_iterator* through
      the request queue and yields one :class:`_Response` per
      response. The coroutine path synthesizes a single ``"next"``
      request. Pre-stream worker failures raise out of
      :meth:`_ResponseQueue.get` and propagate raw — the dispatch
      handler's terminal-exception clause builds the wire response
      from ``self.snapshot`` and the dumped exception.

    - **Teardown** (``__aexit__``) calls :meth:`drain` (close
      request queue + await ``worker_done``) before unwinding
      the exit stack. Worker exceptions surfaced via
      ``worker_done`` are silently swallowed — pre-stream and
      routine-time failures already shipped via the
      terminal-exception clause, so logging here would
      double-surface the same signal. Post-iteration teardown
      failures (``routine_scope``'s cleanup raising after a clean run)
      currently fall into the same swallow; restoring an
      operator-visible log for that specific case is a
      follow-up.

    - **Cancellation** (``cancel``) sets a flag, cancels the
      worker driver task on the worker loop, and pushes ``_EOS``
      onto the response queue. The worker-task cancellation is
      what propagates :class:`asyncio.CancelledError` into a
      routine mid-``_step``; without it, a compute-bound or
      sleeping routine would run to natural completion after the
      caller has gone away. Used by the service's docket-cancel
      path on shutdown and by the dispatch handler as the on-exit
      cleanup hook. Idempotent.

    Public attributes ``.task``, ``.snapshot``, ``.serializer`` are
    populated on enter for use by the dispatch handler (e.g.,
    backpressure).

    :param request_iterator:
        The bidirectional request stream. The first frame is read
        in ``__aenter__``; subsequent frames are forwarded by
        ``__aiter__`` for async-generator routines.
    :param worker_loop:
        The worker loop (acquired from the service's loop pool by
        the dispatch handler) that owns this dispatch's routine
        execution.
    """

    task: Task
    snapshot: Snapshot
    serializer: Serializer

    def __init__(
        self,
        request_iterator: AsyncIterator[protocol.Request],
        worker_loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._request_iterator = request_iterator
        self._worker_loop = worker_loop
        self._stack = AsyncExitStack()
        self._streaming: bool = False
        self._cancelled: bool = False
        self._request_queue: _RequestQueue | None = None
        self._response_queue: _ResponseQueue | None = None
        self._worker_done: concurrent.futures.Future | None = None
        self._worker_task: asyncio.Task | None = None
        self._iterator: AsyncGenerator[_Response, None] | None = None
        # Default to cloudpickle so any pre-negotiation parse
        # failure (StopAsyncIteration, malformed task id, bad
        # serializer hint) still has a sane serializer in scope
        # for :class:`Rejected` to ship the dumped exception.
        # ``__aenter__`` overwrites with the negotiated serializer
        # on successful parse — same path as
        # ``Response.exception`` post-Ack.
        self.serializer: Serializer = wool.__serializer__

    @property
    def streaming(self) -> bool:
        """Whether the parsed task is an async-generator routine.

        Set by :meth:`__aenter__` after the first request is parsed
        and the callable is validated. Read-only — exposed so the
        dispatch handler can decide whether to unwrap PEP 525's
        synthesized ``RuntimeError("async generator raised
        StopAsyncIteration")`` back to its original
        :class:`StopAsyncIteration` for coroutine routines,
        without reaching across the privacy boundary.
        """
        return self._streaming

    async def _safe_aclose_stack(self) -> None:
        """Defensively close :attr:`_stack` and swallow routine
        teardown failures.

        Used by :meth:`__aenter__`'s error arms so an aclose
        failure (e.g. resource teardown raising) cannot replace
        the original parse error en route to the dispatch
        handler's Nack channel. ``KeyboardInterrupt`` and
        ``SystemExit`` propagate — process-level signals must
        not be silently dropped during cleanup.
        """
        try:
            await _complete_teardown(self._stack.aclose())
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            pass

    async def __aenter__(self) -> DispatchSession:
        await self._stack.__aenter__()
        # Read the first request. An empty request stream (caller
        # never wrote a frame) raises StopAsyncIteration; convert
        # to a Rejected with a clear protocol-level message so
        # the dispatch handler ships it via the Nack channel
        # instead of letting the bare sentinel escape and risk
        # PEP 479 conversion to RuntimeError further up. Other
        # client-disconnect failures (CancelledError, etc.) still
        # propagate raw.
        try:
            request = await anext(aiter(self._request_iterator))
        except StopAsyncIteration:
            await self._safe_aclose_stack()
            raise Rejected(
                ValueError("empty request stream; expected initial Task frame"),
            ) from None
        except BaseException:
            await self._safe_aclose_stack()
            raise
        try:
            # Validate the first frame's payload oneof before any
            # field access so a malformed frame surfaces a clear
            # protocol error rather than a downstream UUID parse
            # failure on the default-empty Task.id.
            if request.WhichOneof("payload") != "task":
                raise ValueError(
                    "first request must carry a Task in its `payload` "
                    f"oneof; observed {request.WhichOneof('payload')!r}"
                )
            task_id = UUID(request.task.id)
            is_passthrough = False
            if request.task.HasField("serializer"):
                sniff = _unpickle_serializer(request.task.serializer)
                is_passthrough = isinstance(sniff, PassthroughSerializer)
            if is_passthrough:
                self.serializer = await self._stack.enter_async_context(
                    _passthrough_pool.get(task_id)
                )
            else:
                self.serializer = wool.__serializer__

            try:
                self.snapshot = decode_snapshot(
                    request.context, serializer=self.serializer
                )
            except BaseExceptionGroup as eg:
                # Rewrap with the umbrella class so the
                # constructor's auto-downgrade decides the
                # propagation path: all-Exception peers (today's
                # only case via :class:`ContextDecodeWarning`)
                # produce an :class:`ExceptionGroup`, which the
                # outer ``except Exception`` arm wraps as
                # :class:`Rejected` and ships via Nack-with-
                # exception. A non-Exception peer (e.g. a
                # ``CancelledError`` or ``KeyboardInterrupt``)
                # would keep the result as a true
                # :class:`BaseExceptionGroup`, falling through to
                # the ``except BaseException`` arm below where it
                # propagates raw — Nack is the wrong channel for
                # cancellation/interrupt signals; they should
                # tear the dispatch task down rather than be
                # encoded as a typed parse rejection.
                raise BaseExceptionGroup(
                    "request context decode failed",
                    list(eg.exceptions),
                ) from eg

            self.task = Task.from_protobuf(request.task)

            if not (
                iscoroutinefunction(self.task.callable)
                or isasyncgenfunction(self.task.callable)
            ):
                raise ValueError(
                    "Expected coroutine function or async generator function"
                )

            self._streaming = isasyncgenfunction(self.task.callable)
        except Exception as e:
            await self._safe_aclose_stack()
            raise Rejected(e) from None
        except BaseException:
            await self._safe_aclose_stack()
            raise
        # Register drain on the exit stack so it runs as part of the
        # LIFO unwind. Pushed last so it pops first — the worker is
        # drained before the passthrough-pool serializer is released.
        # If drain raises (e.g. a ``CancelledError`` reaching it
        # during graceful shutdown), the stack still unwinds the
        # remaining callbacks, so resources entered above are
        # always released.
        self._stack.push_async_callback(self.drain)
        return self

    def _schedule_worker(self) -> None:
        """Set up the cross-loop request/response queues and
        schedule the worker driver. Called lazily from
        ``__aiter__`` on the first iteration so that backpressure
        and other pre-iteration decisions run on the main-loop
        thread, which owns the freshly decoded work snapshot, before
        the worker task re-stamps that snapshot onto its own thread.

        Short-circuits when :meth:`cancel` was called before the
        first :meth:`__aiter__`: the queues stay ``None`` and
        :meth:`_iterate` returns an empty stream.
        """
        if self._cancelled:
            return
        main_loop = asyncio.get_running_loop()
        worker_done: concurrent.futures.Future = concurrent.futures.Future()
        request_queue = _RequestQueue(self._worker_loop, serializer=self.serializer)
        response_queue = _ResponseQueue(main_loop, worker_done)
        self._request_queue = request_queue
        self._response_queue = response_queue

        work_task = self.task
        work_snapshot = self.snapshot
        serializer = self.serializer
        streaming = self._streaming

        def _start():
            async def _run():
                try:
                    # Install the decoded work snapshot — but only when
                    # the caller actually shipped Wool state. An empty
                    # caller frame decodes to a stateless snapshot;
                    # installing it would arm the routine's context for
                    # nothing, so a routine that never touches a
                    # wool.ContextVar would run armed (guard live)
                    # rather than as a plain contextvars.Context. Gating
                    # on ``snapshot_has_state`` keeps the worker honest
                    # to the armed-gating contract; ``_step``'s
                    # ``merge_snapshot`` still arms lazily if a later
                    # mid-stream frame carries state.
                    #
                    # When state is present, the snapshot is re-stamped
                    # so this worker-loop thread and task own the chain
                    # — it was decoded on the main loop's thread, which
                    # owned it for the backpressure hook. The routine
                    # and every ``_step`` run under it. The worker loop
                    # has Wool's task factory installed, so this
                    # coroutine is wrapped in ``_forked_scope``; that
                    # fork is a no-op here because ``_start`` runs in
                    # the worker loop's unarmed base context. This
                    # ``install_snapshot`` — not the factory — is what
                    # arms the task, onto the caller's chain.
                    if snapshot_has_state(work_snapshot):
                        install_snapshot(
                            work_snapshot.evolve(
                                owner=threading.get_ident(),
                                owner_task=_current_owner_ref(),
                            )
                        )
                    async with routine_scope(work_task) as routine:
                        while (request := await request_queue.get()) is not None:
                            try:
                                response = await _step(
                                    routine,
                                    streaming,
                                    request,
                                    serializer=serializer,
                                )
                            except StopAsyncIteration:
                                # Streaming SAI = clean end-of-stream;
                                # break the driver loop. Coroutine SAI
                                # propagates so the asyncgen-transport
                                # path (:meth:`_iterate`) ships it;
                                # ``WorkerService.dispatch`` unwraps
                                # PEP 525's synthesized RuntimeError
                                # back to the original SAI for the
                                # coroutine path so the wire matches
                                # stdlib ``await coro()`` semantics.
                                if not streaming:
                                    raise
                                break
                            response_queue.put(response)
                            if not streaming:
                                break
                    response_queue.close()
                finally:
                    # Publish the final work snapshot so the dispatch
                    # handler's terminal-exception path can encode
                    # worker-side variable mutations. ``drain``
                    # synchronizes the read: by the time the handler
                    # reads ``self.snapshot`` the worker task is done.
                    self.snapshot = current_snapshot() or work_snapshot
                    # Stop the producer side immediately on any
                    # ``_run`` exit, including mid-frame routine
                    # exceptions that unwind past the ``async with
                    # routine_scope`` block. Pre-fix, the producer's
                    # ``_iterate`` kept queueing frames until its
                    # own ``finally`` (or external teardown via
                    # :meth:`drain`) closed the queue, leaving a
                    # window where requests accumulated against a
                    # worker that had already failed.
                    request_queue.close()

            coro = _run()
            try:
                task = self._worker_loop.create_task(coro)
            except BaseException as e:
                # Late-loop-closure or task-factory failure:
                # ``call_soon_threadsafe`` succeeded earlier (loop
                # was open at scheduling time) but ``create_task``
                # raises here because the loop has since closed
                # (or the factory itself rejected the coroutine).
                # ``create_task`` never took ownership of ``coro``,
                # so close it explicitly — an orphaned coroutine
                # leaks a "coroutine was never awaited" RuntimeWarning
                # at GC. Settle ``worker_done`` so :meth:`drain` does
                # not await an unresolved future, and close the
                # response queue so any pending
                # :meth:`_ResponseQueue.get` returns immediately.
                coro.close()
                worker_done.set_exception(e)
                response_queue.close()
                return
            self._worker_task = task
            # Re-check cancellation: cancel() may have raced our
            # creation. cancel() sets _cancelled before reading
            # _worker_task, and we set _worker_task before reading
            # _cancelled — between those two stores any interleaving
            # ends here with the task cancelled. Same-loop cancel is
            # safe to invoke directly (we are the worker loop).
            if self._cancelled:
                task.cancel()

            def _on_done(t: asyncio.Task):
                # ``worker_done`` is the worker-completion future
                # owned by this handler; this is its sole writer,
                # so no done-state guard is needed. Surface
                # cancellation as a ``CancelledError`` on the
                # future (rather than cancelling the future
                # itself) so the consumer side observes
                # cancellation through the same exception channel
                # as routine-time failures, instead of seeing it
                # as a clean termination.
                if t.cancelled():
                    worker_done.set_exception(asyncio.CancelledError())
                else:
                    exc = t.exception()
                    if exc is not None:
                        worker_done.set_exception(exc)
                    else:
                        worker_done.set_result(None)
                # Wake any pending ``response_queue.get()`` so the
                # main loop can observe worker termination — the
                # happy path arrives via the close already pushed
                # by ``_run``; the setup-failure path needs this
                # nudge to escape an otherwise-indefinite await on
                # a frame that will never arrive.
                response_queue.close()

            task.add_done_callback(_on_done)

        # ``self._worker_done`` is the marker drain() uses to decide
        # whether to await the worker task. Assign it only after
        # scheduling succeeds — if call_soon_threadsafe raises
        # (closed worker loop), drain must short-circuit instead of
        # awaiting a future that will never be resolved.
        self._worker_loop.call_soon_threadsafe(_start)
        self._worker_done = worker_done

    async def drain(self) -> None:
        """Close the request queue and await the worker driver to
        complete. Idempotent — safe to call multiple times.

        After this returns, ``self.snapshot`` is no longer being
        updated by the worker, so the dispatch handler can safely
        encode it for the terminal-exception response.

        Worker exceptions raised during the drain are swallowed
        but logged at ``WARNING``: pre-stream and routine-time
        failures already propagated out of ``__aiter__`` and
        shipped via the dispatch handler's terminal-exception
        clause, so this log line is largely a defensive surface
        for post-iteration teardown failures (``routine_scope``'s
        cleanup raising after the routine completed cleanly) —
        without it those regress silently. Re-shipping the
        exception is not viable: the dispatch handler has
        already completed its terminal-frame yield by the time
        ``__aexit__`` calls drain, so the wire is closed.

        Cancellation of the awaiting task itself (an externally-
        injected ``CancelledError`` rather than one already set
        on ``worker_done``) propagates so the surrounding
        ``__aexit__`` and dispatch handler honor cancellation.

        Tolerates a closed worker loop: when the service's
        graceful-shutdown sequence destroys the worker loop before
        the dispatch handler's ``__aexit__`` (or terminal-exception
        clause) runs, ``call_soon_threadsafe`` raises
        ``RuntimeError("Event loop is closed")`` — already-drained
        workers leave the queues no-op-able. Swallowed.
        """
        if self._request_queue is not None:
            try:
                self._request_queue.close()
            except RuntimeError:
                pass
        if self._worker_done is not None:
            try:
                await asyncio.wrap_future(self._worker_done)
            except asyncio.CancelledError:
                # ``CancelledError`` reaching us through
                # ``wrap_future`` covers two cases. The worker task
                # died with cancellation — the routine's
                # cancellation already shipped via the dispatch
                # handler's terminal-exception clause, so we
                # swallow. Or the awaiting task itself was
                # cancelled — wrap_future's bidirectional chain
                # transitions ``worker_done`` to ``CANCELLED`` (or
                # it was still pending) and we must propagate.
                # Distinguish by inspecting the worker future's
                # state directly — robust against upstream callers
                # that absorb their own cancellation signals before
                # re-entering this frame.
                if self._worker_done.cancelled():
                    raise
                worker_exc = (
                    self._worker_done.exception() if self._worker_done.done() else None
                )
                if not isinstance(worker_exc, asyncio.CancelledError):
                    raise
            except Exception:
                _log.warning(
                    "DispatchSession.drain swallowed worker teardown exception",
                    exc_info=True,
                )

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Shield the stack unwind from caller cancellation so the
        # pooled passthrough-serializer release runs to completion —
        # see :func:`_complete_teardown`. The registered managers
        # never suppress, so discarding the suppression return value
        # (always falsy here) is behaviour-preserving.
        await _complete_teardown(self._stack.aclose())

    def __aiter__(self) -> AsyncIterator[_Response]:
        if self._iterator is None:
            self._schedule_worker()
            self._iterator = self._iterate()
        return self._iterator

    async def _iterate(self) -> AsyncGenerator[_Response, None]:
        """Drive the request/response loop on the main loop.

        Forwards each :class:`protocol.Request` to the request
        queue and yields one :class:`_Response` per response
        received. Coroutine path synthesizes a single ``"next"``
        request. Pre-stream worker failures raise out of
        :meth:`_ResponseQueue.get` and propagate to the dispatch
        handler's terminal-exception clause.

        Raises :class:`asyncio.CancelledError` when :meth:`cancel`
        has been invoked — mirroring stdlib's ``await task``
        semantics where ``task.cancel()`` from any source (caller,
        routine self-raise, operator preempt) surfaces as
        ``CancelledError`` to the awaiter. This applies both to
        cancellation observed before iteration starts (the early
        check below short-circuits) and to in-flight cancellation
        that breaks the loop early (the trailing check raises after
        the body exits).
        """
        if self._cancelled:
            raise asyncio.CancelledError()
        # Invariant: ``_schedule_worker`` assigned both queues
        # unless ``_cancelled`` was set, in which case the check
        # above already raised. Assertions narrow the types from
        # ``... | None`` to non-None for pyright.
        assert self._request_queue is not None
        assert self._response_queue is not None
        request_queue = self._request_queue
        response_queue = self._response_queue
        # Track whether cancel drove the body exit. Set at every
        # cancel-observation point inside the body so the trailing
        # check below can distinguish "cancel caused the break"
        # from "cancel arrived after natural completion". Matches
        # stdlib ``task.cancel()``: cancel-before-completion
        # surfaces as ``CancelledError``; cancel-after-completion
        # is a no-op. The pre-fix unconditional check would ship a
        # spurious trailing exception frame on a routine the
        # caller has already observed completing — invisible to
        # user code through wool's public API, but counted by any
        # interceptor or OpenTelemetry attribute deriving outcome
        # from the last wire frame.
        cancel_induced_exit = False
        if self._streaming:
            try:
                async for protobuf_request in self._request_iterator:
                    if self._cancelled:
                        cancel_induced_exit = True
                        break
                    try:
                        request_queue.put(protobuf_request)
                    except RuntimeError:
                        # Mirror :meth:`drain`'s tolerance: when
                        # the worker loop has been torn down
                        # mid-stream (graceful shutdown landing
                        # between two main-loop pumps),
                        # ``call_soon_threadsafe`` raises
                        # ``RuntimeError("Event loop is
                        # closed")``. The dispatch is no longer
                        # serviceable; break cleanly so the
                        # stream terminates without
                        # misattributing the loop teardown as a
                        # routine failure.
                        break
                    response = await response_queue.get()
                    if response is None:
                        # ``_EOS`` arrived. Two producers can push
                        # it: :meth:`cancel` closing the response
                        # queue explicitly, or :meth:`_on_done`
                        # closing it after the worker task
                        # finalizes. The cancel-induced path is
                        # the one the trailing check guards
                        # against — the queue's pre-pushed _EOS
                        # may have raced :meth:`_on_done` settling
                        # ``worker_done`` with the actual
                        # ``CancelledError``.
                        if self._cancelled:
                            cancel_induced_exit = True
                        break
                    yield response
                else:
                    # Request iterator exhausted naturally (client
                    # closed the write side). If cancel arrived
                    # while we were suspended on the iterator, the
                    # in-loop ``if self._cancelled`` check never
                    # ran — surface the cancel here so the
                    # operator-preempt contract still ships
                    # ``CancelledError`` to the caller. Mirrors
                    # stdlib's ``await task`` semantics where
                    # ``task.cancel()`` from any source produces
                    # the same observable.
                    if self._cancelled:
                        cancel_induced_exit = True
            finally:
                # Honor the request-stream EOF (or any other
                # exit from the streaming loop) immediately by
                # closing the request queue, so the worker's
                # ``_run`` while-loop exits at its next
                # ``request_queue.get`` rather than waiting for
                # ``__aexit__`` → ``drain()`` to push the
                # sentinel. ``close()`` is idempotent (it just
                # pushes ``_EOS``) so the eventual
                # second close from drain is a no-op. Mirrors
                # :meth:`drain`'s tolerance for a closed worker
                # loop: during graceful shutdown the loop pool
                # may have torn the worker loop down already,
                # in which case ``call_soon_threadsafe`` raises
                # ``RuntimeError("Event loop is closed")`` —
                # the sentinel push is moot at that point.
                try:
                    request_queue.close()
                except RuntimeError:
                    pass
        else:
            # Mirror the streaming branch's ``RuntimeError`` guard:
            # a worker loop torn down between ``__aiter__``'s
            # ``_schedule_worker`` and this first put raises
            # ``RuntimeError("Event loop is closed")`` out of
            # ``call_soon_threadsafe``. Exit cleanly rather than
            # ship a transport-teardown failure as a routine fault.
            try:
                request_queue.put(protocol.Request(next=protocol.Void()))
            except RuntimeError:
                return
            response = await response_queue.get()
            if response is None:
                # See the streaming branch's ``_EOS`` comment.
                if self._cancelled:
                    cancel_induced_exit = True
            else:
                yield response
        if cancel_induced_exit:
            raise asyncio.CancelledError()

    async def cancel(self) -> None:
        """Signal cancellation. Idempotent. Cross-task safe.

        Sets a flag observed by :meth:`_schedule_worker` (so a
        cancellation arriving before the first :meth:`__aiter__`
        short-circuits the worker schedule) and by :meth:`_iterate`
        (so iteration surfaces :class:`asyncio.CancelledError` at
        the next yield boundary — mirroring stdlib's ``await task``
        semantics where ``task.cancel()`` from any source produces
        the same observable), cancels the worker driver task on the
        worker loop so a routine mid-``_step`` (e.g.,
        ``await asyncio.sleep(...)``) receives a
        :class:`asyncio.CancelledError` rather than running to
        natural completion, and pushes ``_EOS`` onto the response
        queue so any suspended :meth:`_ResponseQueue.get` returns
        ``None`` and unblocks the iterator.

        Worker-task cancellation is scheduled via
        ``loop.call_soon_threadsafe`` to remain cross-loop safe,
        and tolerates a closed worker loop (the dispatch is no
        longer serviceable; the existing ``RuntimeError`` swallow
        on ``call_soon_threadsafe`` matches :meth:`drain`'s
        tolerance).

        Unlike a direct ``aclose()`` on :attr:`_iterator`, this is
        safe to call from a task other than the one driving the
        iterator — no ``RuntimeError("asynchronous generator is
        already running")`` is possible because no aclose is
        attempted.

        Suspension caveat. The three signals — ``_cancelled`` flag,
        worker-task cancel, ``_EOS`` push — together unblock
        :meth:`_iterate`'s ``_ResponseQueue.get`` suspensions and any
        inter-step ``_cancelled`` observation. They do NOT interrupt
        a request-iterator read in flight: a streaming dispatch
        idling on ``async for protobuf_request in
        self._request_iterator`` (between the dispatch handler's
        last yield and the caller's next ``asend``/``anext``) only
        observes ``_cancelled`` after a new frame arrives or after
        the gRPC layer tears the stream down. The operator-preempt
        path (:meth:`WorkerService._preempt`) relies on the broader
        gRPC server shutdown to cancel the stream and close that
        gap; ``cancel()`` alone does not match stdlib
        ``task.cancel()`` for that specific suspension.
        """
        self._cancelled = True
        worker_task = self._worker_task
        if worker_task is not None:
            try:
                self._worker_loop.call_soon_threadsafe(worker_task.cancel)
            except RuntimeError:
                # Worker loop already torn down — nothing to cancel.
                pass
        if self._response_queue is not None:
            self._response_queue.close()
