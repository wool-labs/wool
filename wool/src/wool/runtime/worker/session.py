"""Per-dispatch routine handler.

Layered abstractions cover the worker-side dispatch lifetime:

- :class:`_RequestQueue` / :class:`_ResponseQueue` — cross-loop
  queues bridging the gRPC main loop and the worker loop.
- :func:`_drive_step` — inner routine stepper. One step per
  request, runs inside the chain's cached
  :class:`contextvars.Context`. Routine-shape variation (coroutine
  = one step; async-generator = N steps) lives here.
- :class:`DispatchSession` — per-dispatch async context manager
  and iterator that owns parse, lazy worker scheduling, drive,
  drain, and cancel.

Each class's docstring carries the per-piece detail.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import copy
import logging
from contextlib import AsyncExitStack
from inspect import isasyncgenfunction
from inspect import iscoroutinefunction
from typing import Any
from typing import AsyncGenerator
from typing import AsyncIterator
from typing import Coroutine
from typing import Final
from typing import TypeVar
from typing import assert_never
from typing import cast
from uuid import UUID
from weakref import WeakValueDictionary

import wool
from wool import protocol
from wool.protocol.frame import ExceptionResponseFrame
from wool.protocol.frame import Frame
from wool.protocol.frame import NextRequestFrame
from wool.protocol.frame import RequestFrame
from wool.protocol.frame import ResponseFrame
from wool.protocol.frame import ResultResponseFrame
from wool.protocol.frame import SendRequestFrame
from wool.protocol.frame import TaskRequestFrame
from wool.protocol.frame import ThrowRequestFrame
from wool.runtime.context.base import current_wire_context
from wool.runtime.context.manifest import _ContextManifest
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import routine_scope
from wool.runtime.serializer import Serializer
from wool.runtime.worker.connection import _complete_teardown

_T = TypeVar("_T")

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


class _WorkerLoopClosed(Exception):
    """Internal control-flow signal: the worker loop was torn down
    during dispatch.

    Raised by :meth:`_RequestQueue.put` when
    ``call_soon_threadsafe`` rejects because the worker loop is
    closed. The streaming and coroutine branches of
    :meth:`DispatchSession._iterate` catch this specifically to break
    cleanly without misclassifying the graceful teardown as a routine
    failure.

    Extends :class:`Exception` (not :class:`RuntimeError`) so broad
    ``except RuntimeError:`` patterns elsewhere can't accidentally
    swallow the signal. Matches stdlib's control-flow-signal
    convention — :class:`StopIteration` and :class:`StopAsyncIteration`
    both extend :class:`Exception` directly.
    """


def _empty_manifest() -> _ContextManifest:
    """Return a fresh empty :class:`_ContextManifest`.

    Used as the default for :attr:`DispatchSession.decoded` when the
    initial dispatch frame carries no wire context
    (``RequestFrame.context`` is ``None``). The backpressure hook
    reads ``session.decoded.decoded_vars`` which on the returned
    manifest is an empty dict — a present-but-empty mapping keeps the
    attribute shape consistent regardless of whether the inbound frame
    carried any chain state. Returned fresh per call to avoid sharing
    a mutable manifest across dispatches.
    """
    from uuid import uuid4

    return _ContextManifest(
        chain_id=uuid4(),
        decoded_vars={},
        reset_vars=frozenset(),
        stub_pins=frozenset(),
        decode_error=None,
    )


class _RequestQueue:
    """Cross-loop queue carrying gRPC request envelopes from the
    main (gRPC) loop to the worker loop's :func:`_step` driver.

    Producers on the main loop push :class:`protocol.Request`
    envelopes via :meth:`put`. The consumer on the worker loop pulls
    them via :meth:`get`, which decodes each envelope into a
    :class:`~wool.protocol.frame.RequestFrame` via
    :meth:`RequestFrame.from_protobuf` before returning. Decoding on
    the worker side keeps payload deserialization (which may
    reconstitute pickled :class:`wool.ContextVar` instances) inside
    the worker-loop task that runs the routine under the work context.

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

        Raises :class:`_WorkerLoopClosed` if the worker loop has
        already been torn down (``call_soon_threadsafe`` rejects
        with ``RuntimeError("Event loop is closed")``). Callers
        catch that typed signal specifically to break cleanly
        without misclassifying graceful teardown as a routine
        failure. Other :class:`RuntimeError` instances propagate
        unchanged — protocol violations remain visible rather than
        being swallowed by a broad ``except RuntimeError`` clause.
        """
        try:
            self._worker_loop.call_soon_threadsafe(self._queue.put_nowait, request)
        except RuntimeError as e:
            if self._worker_loop.is_closed():
                raise _WorkerLoopClosed() from e
            raise

    async def get(self) -> "protocol.Request | None":
        """Pop the next raw :class:`protocol.Request`, or :data:`None`
        when the queue has been :meth:`close`\\ d.

        Decoding is deferred to the consumer (``_run``) so a single
        malformed wire envelope can be surfaced as a typed terminal
        on the response stream rather than killing the worker driver
        task mid-stream — see F7.

        Awaitable on the worker loop only.
        """
        item = await self._queue.get()
        if isinstance(item, _EndOfStream):
            return None
        return item

    def close(self) -> None:
        """Signal end of input by pushing the close sentinel.
        Cross-loop safe."""
        self._worker_loop.call_soon_threadsafe(self._queue.put_nowait, _EOS)


class _ResponseQueue:
    """Cross-loop queue carrying :class:`ResponseFrame` instances from
    the worker loop's :func:`_step` driver back to the main (gRPC)
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
        self._queue: asyncio.Queue[ResponseFrame | _EndOfStream] = asyncio.Queue()
        self._main_loop = main_loop
        self._worker_done = worker_done

    def put(self, response: ResponseFrame) -> None:
        """Push a :class:`ResponseFrame` onto the queue.

        Cross-loop safe — schedules the put on the main loop via
        :func:`asyncio.AbstractEventLoop.call_soon_threadsafe`.
        """
        self._main_loop.call_soon_threadsafe(self._queue.put_nowait, response)

    async def get(self) -> ResponseFrame | None:
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


def _create_step_task(
    coro: Coroutine[Any, Any, _T],
    *,
    loop: asyncio.AbstractEventLoop,
    context: contextvars.Context,
) -> asyncio.Task[_T]:
    """Create an :class:`asyncio.Task` that runs *coro* in *context*,
    bypassing Wool's fork-on-task factory.

    The per-step driver constructs each step's task directly via the
    :class:`asyncio.Task` constructor rather than ``loop.create_task``.
    ``loop.create_task`` routes through whatever task factory the loop
    has installed — Wool's factory included — and Wool's factory forks
    the child onto a fresh chain. For an *internal* driver task whose
    job is to drive a chain that already exists on the registry,
    forking would mint a new chain id and break the registry lookup.
    Going through the :class:`asyncio.Task` constructor skips the
    factory and runs *coro* in the supplied *context* unchanged.

    Localised here so the bypass site is identifiable in a grep, and
    so a future change that needs uvloop's native task class can swap
    the construction in one place without touching every caller.
    """
    return asyncio.Task(coro, loop=loop, context=context)


async def _drive_step(
    routine: Any,
    streaming: bool,
    request: RequestFrame,
    work_ctx: contextvars.Context,
    *,
    serializer: Serializer,
    loop: asyncio.AbstractEventLoop,
) -> Any:
    """Drive one step of the worker's per-routine loop.

    Builds the step coroutine from the request kind (``asend`` /
    ``athrow`` for an async-generator routine, the routine itself
    for a coroutine routine), runs it inside *work_ctx* as a freshly
    constructed :class:`asyncio.Task`, and returns the step's
    yielded or returned value.

    Q1 (part 1) — restored as the top-level ``_drive_step`` symbol
    the module docstring advertises. Previously inlined into the
    closure inside :meth:`DispatchSession._schedule_worker._start._run`;
    pulled out so the per-step build-and-execute is a single-purpose
    function readable on its own.

    The session loop in :meth:`DispatchSession._schedule_worker`
    wraps the call to capture the post-step wire context, pin
    :attr:`DispatchSession._final_wire_context`, and translate the
    routine's exception types into the streaming-vs-coroutine end-
    of-stream signalling. *serializer* is reserved for the optional
    in-step wire-encode path; the current body has no use for it
    but keeps the symbol on the signature so :meth:`Frame.mount` and
    related can grow into ``_drive_step`` without an interface break.

    :param routine:
        The active routine — either the coroutine itself (non-
        streaming) or an :class:`AsyncGenerator` (streaming).
    :param streaming:
        ``True`` when *routine* is an async generator and each
        request drives one ``asend`` / ``athrow``.
    :param request:
        The decoded request frame. Determines the step's flavor
        (``Next``/``Send``/``Throw`` for streaming).
    :param work_ctx:
        The cached :class:`contextvars.Context` for the request's
        chain. The step task is constructed against this context so
        backing-variable writes ride the chain's bindings.
    :param serializer:
        Reserved for in-step wire encode/decode; not used in the
        current body.
    :param loop:
        The worker loop the step task is bound to.

    :returns:
        The value the step coroutine yielded or returned.

    :raises BaseException:
        Whatever the routine raises propagates — the caller's
        wrapper translates :class:`StopAsyncIteration` (streaming
        end-of-stream) and pins the terminal wire context for the
        routine-failure path.
    """
    del serializer  # reserved for future in-step encode plumbing
    step_coro: Coroutine[Any, Any, Any]
    if streaming:
        gen = cast(AsyncGenerator, routine)
        if isinstance(request, NextRequestFrame):
            step_coro = gen.asend(None)
        elif isinstance(request, SendRequestFrame):
            step_coro = gen.asend(request.payload)
        elif isinstance(request, ThrowRequestFrame):
            step_coro = gen.athrow(request.payload)
        else:  # pragma: no cover
            assert_never(request)
    else:
        step_coro = cast(Coroutine[Any, Any, Any], routine)

    step_task = _create_step_task(step_coro, loop=loop, context=work_ctx)
    try:
        return await step_task
    finally:
        # Defensive: cancel the step task if the await was preempted
        # (e.g., driver cancellation) so a routine mid-step doesn't
        # run to natural completion after the caller has gone away.
        # No-op for normally completed step tasks — ``cancel()`` on a
        # done task returns ``False`` and the suppressed ``await``
        # resolves immediately.
        if not step_task.done():
            step_task.cancel()
            try:
                await step_task
            except BaseException:
                pass


class Rejected(Exception):
    """Raised by :meth:`DispatchSession.__aenter__` when the dispatch
    parse phase fails — Wool context decode,
    :class:`wool.Task` rebuild, or routine-type validation.

    The dispatch handler catches this and replies with a Nack
    whose ``exception`` field carries :attr:`original` serialized
    via the session's ``serializer`` attribute (always
    ``wool.__serializer__``, cloudpickle). Same path as a
    routine-time failure's ``Response.exception``.

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
      it: decodes the caller's Wool context, rebuilds
      the wool.Task, and validates the routine type. Failures are
      wrapped in :class:`Rejected` so the dispatch handler can
      surface them via Nack-with-exception. A first-request read
      failure (empty iterator, gRPC error) propagates raw — no
      parsed payload exists to serialize for the caller.

      The worker-loop driver is **not** scheduled here; that
      happens lazily on the first ``__aiter__`` call. The dispatch
      handler runs pre-iteration decisions (backpressure) against
      the parsed task and the decoded
      :attr:`_ContextManifest.decoded_vars
      <wool.runtime.context.manifest._ContextManifest.decoded_vars>`
      mapping off :attr:`decoded` before the worker task is
      scheduled.

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
      from :attr:`_final_wire_context` (encoded by the worker task
      inside its own Context) and the dumped exception.

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

    Public attributes ``.task``, ``.decoded``, ``.serializer`` are
    populated on enter for use by the dispatch handler (e.g.,
    backpressure, which dry-run-mounts ``.decoded``).

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
    decoded: _ContextManifest
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
        self._iterator: AsyncGenerator[ResponseFrame, None] | None = None
        # The initial :class:`RequestFrame` decoded in __aenter__ — its
        # context manifest (when non-None) carries the caller's wire
        # context state plus every ``wool.Token`` captured from the
        # task args/kwargs payload. The worker driver's initialization
        # mounts it inside its own ``contextvars.Context`` before the
        # routine is constructed.
        self._initial_frame: RequestFrame | None = None
        # The worker driver encodes its final wire context inside the
        # work Context (Context.to_protobuf reads the backing variables)
        # and publishes it here for the dispatch handler's terminal-
        # exception path. ``_final_encode_error`` carries a strict-mode
        # encode failure instead.
        self._final_wire_context: protocol.Context | None = None
        self._final_encode_error: BaseException | None = None
        # All dispatch serializes through cloudpickle; this one
        # serializer covers the payload, the context frames, and
        # any :class:`Rejected` dumped exception (including pre-parse
        # failures such as StopAsyncIteration or a malformed frame).
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
            # Decode the initial frame. Strict-mode decode failures
            # land on ``frame.context.decode_error`` rather than
            # raising mid-decode — surface them up-front so the outer
            # ``except Exception`` arm wraps them in ``Rejected`` for
            # Nack transport (preserving the caller's ``except
            # ContextDecodeError`` semantics).
            decoded = Frame.from_protobuf(request, serializer=self.serializer)
            assert isinstance(decoded, TaskRequestFrame)
            self._initial_frame = decoded
            if (
                self._initial_frame.context is not None
                and self._initial_frame.context.decode_error is not None
            ):
                raise self._initial_frame.context.decode_error
            assert isinstance(self._initial_frame.payload, Task)
            self.task = self._initial_frame.payload
            # Backpressure inspection on the dispatch handler reads
            # ``session.decoded`` — surface the manifest there. An
            # empty manifest (no wire context) is absent on the frame;
            # fall back to a stateless one so callers see a consistent
            # attribute shape.
            self.decoded = self._initial_frame.context or _empty_manifest()

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
        # LIFO unwind. If drain raises (e.g. a ``CancelledError``
        # reaching it during graceful shutdown), the stack still
        # unwinds the remaining callbacks.
        self._stack.push_async_callback(self.drain)
        return self

    def _schedule_worker(self) -> None:
        """Set up the cross-loop request/response queues and
        schedule the worker driver. Called lazily from
        ``__aiter__`` on the first iteration — after the dispatch
        handler has already run its pre-iteration decisions
        (backpressure) against the decoded
        :attr:`_ContextManifest.decoded_vars
        <wool.runtime.context.manifest._ContextManifest.decoded_vars>`
        mapping off :attr:`decoded`. The worker driver's
        initialization frame mounts :attr:`decoded` for real inside
        its own :class:`contextvars.Context`.

        Short-circuits when :meth:`cancel` was called before the
        first :meth:`__aiter__`: the queues stay ``None`` and
        :meth:`_iterate` returns an empty stream.

        **Closure capture chain.** Q19 — the worker driver
        (``_start`` → ``_run`` → ``_on_done``) is structured as
        three nested closures rather than a dataclass-shaped
        ``_WorkerDriver`` because the per-step state
        (``request_queue``, ``response_queue``, ``worker_done``,
        ``serializer``, ``streaming``, ``worker_loop``, ``work_task``)
        is captured fresh per dispatch from the session attributes,
        and the closure form keeps the capture site adjacent to its
        consumers. Captures, by layer:

        * Top-level locals in :meth:`_schedule_worker` (this method):
          ``main_loop``, ``worker_done``, ``request_queue``,
          ``response_queue``, ``work_task``, ``serializer``,
          ``streaming``, ``worker_loop``. Re-bound from
          ``self.task`` / ``self.serializer`` / ``self._streaming`` /
          ``self._worker_loop`` so ``_run``'s loop body can read
          them without going through ``self`` (cheaper in the hot
          path).
        * ``_start`` closes over the above; constructs the worker
          coroutine via ``_run()`` and schedules it on the worker
          loop. ``_on_done`` (nested inside ``_start``) closes over
          ``worker_done`` and ``response_queue`` so the task's
          completion callback can settle the future and wake any
          pending main-loop consumer.
        * ``_run`` (the driver coroutine) additionally allocates a
          local ``chain_registry: WeakValueDictionary[UUID,
          contextvars.Context]`` per dispatch — the per-routine
          chain → cached-context registry — and ``loop`` (rebound
          from ``worker_loop`` so the closure body avoids a
          ``asyncio.get_running_loop()`` per iteration). Mutates
          ``self._final_wire_context`` /
          ``self._final_encode_error`` on the routine-failure path
          (the only writes back to ``self``).

        A flatter dataclass-shaped ``_WorkerDriver`` is a possible
        follow-up; the current closure form is the minimum that
        keeps the captures readable per-layer and threads the
        chain registry's lifetime to the dispatch's lifetime
        naturally (it goes out of scope with ``_run``).
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
        serializer = self.serializer
        streaming = self._streaming
        worker_loop = self._worker_loop

        def _start():
            async def _run():
                # The worker loop is the running loop here — capture
                # it explicitly so :func:`_create_step_task` can
                # construct step tasks bound to it without a fresh
                # :func:`asyncio.get_running_loop` per iteration.
                loop = worker_loop
                # Per-routine chain registry: each chain id seen on a
                # request frame allocates (or re-enters) a cached
                # stdlib.Context that scopes the chain's bindings on
                # this worker. Held weakly — when the chain's last
                # ``wool.Token`` (whose ``tok_ctx`` is the cached
                # Context) drops, the entry is auto-removed. A
                # subsequent frame for the GC'd chain id allocates a
                # fresh Context and recovers state from the wire
                # manifest. See ``docs/design/per-frame-context.md``.
                chain_registry: WeakValueDictionary[UUID, contextvars.Context] = (
                    WeakValueDictionary()
                )

                try:
                    async with routine_scope(work_task) as routine:
                        while (raw_request := await request_queue.get()) is not None:
                            # Decode per-frame inside the loop so a
                            # single malformed wire envelope ships a
                            # typed terminal on the response stream
                            # rather than propagating raw out of the
                            # worker driver and surfacing as an opaque
                            # task death. The dispatch is no longer
                            # serviceable after a decode failure
                            # (the wire framing is broken), so we
                            # ship the decode error as an
                            # ExceptionResponseFrame and break.
                            try:
                                decoded = Frame.from_protobuf(
                                    raw_request, serializer=serializer
                                )
                            except Exception as decode_err:
                                response_queue.put(
                                    ExceptionResponseFrame.for_send(
                                        decode_err, serializer=serializer
                                    )
                                )
                                break
                            assert isinstance(decoded, RequestFrame)
                            request = decoded
                            manifest = request.context
                            if manifest is None:
                                # Unarmed caller: the frame omits its
                                # ``context`` field per the lazy-elide
                                # design. Run the step in a fresh
                                # ``contextvars.copy_context()`` with no
                                # wool chain installed — there is no
                                # chain identity to propagate and no
                                # backing state to apply. The registry
                                # stays untouched: there's no chain id
                                # to key on.
                                ctx: contextvars.Context = contextvars.copy_context()
                            else:
                                # Armed caller: resolve the chain's
                                # cached Context, allocating on first
                                # sighting. The ``copy_context`` snapshot
                                # inherits the driver task's bindings
                                # (``routine_scope`` has already entered
                                # ``task.runtime_context`` and set
                                # ``wool.__proxy__``), so the chain runs
                                # against the worker's baseline non-Wool
                                # contextvars.
                                chain_id = manifest.chain_id
                                cached = chain_registry.get(chain_id)
                                if cached is None:
                                    cached = contextvars.copy_context()
                                    chain_registry[chain_id] = cached
                                ctx = cached

                            # Q4 — single mount entry point. Frame.mount
                            # routes through the unified
                            # :func:`_install_manifest` pipeline inside
                            # ``ctx.run(...)`` and handles the
                            # exception-leaf decode-error-chaining
                            # (ThrowRequestFrame's
                            # ``_chains_decode_onto_payload`` walk) so
                            # the worker driver no longer needs the
                            # hand-rolled apply/walk block.
                            request.mount(ctx)

                            # Drive the step via the top-level
                            # :func:`_drive_step` helper. The helper
                            # builds the per-step coroutine (asend /
                            # athrow / coroutine-itself) and runs it
                            # inside the cached context via a Task
                            # constructed directly (bypassing the
                            # loop's task factory; see
                            # :func:`_create_step_task`).
                            try:
                                value = await _drive_step(
                                    routine,
                                    streaming,
                                    request,
                                    ctx,
                                    serializer=serializer,
                                    loop=loop,
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
                            except BaseException:
                                # Routine raised mid-step. Pin the
                                # chain's wire state for the dispatch
                                # handler's terminal-exception clause
                                # *inside* the step's ``ctx`` so the
                                # encoded snapshot reflects the chain
                                # the routine actually ran on (not the
                                # most-recent ``last_ctx`` — which under
                                # cross-chain interleaving might point
                                # at a sibling chain). Encode errors
                                # land on ``_final_encode_error`` so
                                # the handler chains them onto the
                                # routine's failure via ``__cause__``.
                                try:
                                    self._final_wire_context = ctx.run(
                                        current_wire_context, serializer=serializer
                                    )
                                except BaseException as encode_err:
                                    self._final_encode_error = encode_err
                                    self._final_wire_context = None
                                raise

                            # Encode the post-step wire state from
                            # within the cached Context so the response
                            # reflects the routine's mutations to the
                            # chain's bindings. Pin the captured state
                            # to ``self._final_wire_context`` as the
                            # step commits: the dispatch handler's
                            # terminal-exception path reads this field
                            # without re-encoding, and pinning here
                            # associates it with the step's chain *by
                            # construction* (inside the same
                            # ``ctx.run`` that produced it), eliminating
                            # the cross-chain-interleaving provenance
                            # ambiguity a finally-block encode of
                            # ``last_ctx`` would have. A strict-mode
                            # encode failure pins the error and
                            # re-raises so the dispatch handler's
                            # terminal-exception path ships the
                            # encode error on the wire.
                            try:
                                captured = ctx.run(
                                    current_wire_context, serializer=serializer
                                )
                            except BaseException as encode_err:
                                self._final_encode_error = encode_err
                                self._final_wire_context = None
                                raise
                            self._final_wire_context = captured
                            response_queue.put(
                                ResultResponseFrame.for_send(
                                    value,
                                    serializer=serializer,
                                    wire_context=captured,
                                )
                            )
                            if not streaming:
                                break
                    response_queue.close()
                finally:
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

    def terminal_response(
        self,
        exception: BaseException,
        *,
        serializer: Serializer,
    ) -> ResponseFrame:
        """Build the terminal :class:`ExceptionResponseFrame` for a
        routine-failure dispatch.

        Q3 — owns the encode-error vs. lazy-wire-frame decision and
        the PEP 525 ``StopAsyncIteration`` unwrap. Closes the privacy
        boundary that previously had :class:`WorkerService.dispatch`
        reading :attr:`_final_wire_context` / :attr:`_final_encode_error`
        directly.

        Callers MUST have awaited :meth:`drain` before invoking this
        so the worker task's in-Context encode publish has settled.
        The dispatch handler then yields
        ``session.terminal_response(e, serializer=s).to_protobuf()``
        as the wire-side terminal frame.

        Two distinct shapes ride out:

        * **Lazy-wire-frame** (the worker stayed armed or stayed
          unarmed cleanly): ship the routine exception alongside the
          worker's final wire context.
        * **Strict-mode encode failure**: the worker's in-Context
          :meth:`Context.to_protobuf` raised — typically a
          :class:`wool.ContextDecodeError` aggregating per-var
          warnings. Chain the encode error as ``__cause__`` on a
          *copy* of the routine exception (the live instance may be
          a module-level singleton — for example, an interpreter-
          cached :class:`StopAsyncIteration` — or already propagating
          elsewhere, so mutating its ``__cause__`` /
          ``__suppress_context__`` would alter globally observable
          state) and ship the result with no wire-context payload.

        Coroutine routines that raised :class:`StopAsyncIteration`
        get the PEP 525 ``RuntimeError("async generator raised
        StopAsyncIteration")`` wrapping unwrapped so the caller's
        ``await routine()`` surfaces the original SAI raw — matching
        stdlib coroutine semantics. Streaming routines keep the
        ``RuntimeError`` shape; that already matches stdlib
        ``async for x in agen()`` semantics.

        :param exception:
            The routine-time exception captured by the dispatch
            handler.
        :param serializer:
            The serializer to use for the response frame — typically
            ``session.serializer``.

        :returns:
            An :class:`ExceptionResponseFrame` ready to encode via
            :meth:`Frame.to_protobuf`.
        """
        e = exception
        # PEP 525 SAI unwrap (coroutine-only): the streaming
        # transport in :meth:`_iterate` synthesizes the
        # ``RuntimeError("async generator raised
        # StopAsyncIteration")`` wrapping when a coroutine raises
        # :class:`StopAsyncIteration`. Unwrap so the caller sees the
        # original SAI.
        if (
            not self._streaming
            and isinstance(e, RuntimeError)
            and isinstance(e.__cause__, StopAsyncIteration)
        ):
            e = e.__cause__
        if self._final_encode_error is not None:
            # Strict-mode encode failure: copy ``e`` before mutating
            # so the live (possibly globally-shared) instance is not
            # touched. Chain the encode error as ``__cause__`` for
            # diagnostic visibility while keeping the caller's
            # primary ``except`` clause matching the original type.
            e = copy.copy(e)
            e.__cause__ = self._final_encode_error
            e.__suppress_context__ = True
            return ExceptionResponseFrame.for_send(
                e,
                serializer=serializer,
                # The encode error means there is no trustworthy
                # final wire context to ship; explicit
                # ``wire_context=None`` suppresses the field
                # entirely (and avoids an unrelated auto-capture
                # from the main-loop scope).
                wire_context=None,
            )
        # Lazy-wire-frame: when the worker stayed unarmed the
        # captured wire context is ``None`` and
        # :meth:`Frame.to_protobuf` omits the optional ``context``
        # field; the caller's apply-back skips the absent field.
        return ExceptionResponseFrame.for_send(
            e,
            serializer=serializer,
            wire_context=self._final_wire_context,
        )

    async def drain(self) -> None:
        """Close the request queue and await the worker driver to
        complete. Idempotent — safe to call multiple times.

        After this returns, the worker task has published its final
        :attr:`_final_wire_context` (or :attr:`_final_encode_error`),
        so the dispatch handler can safely read it for the
        terminal-exception response.

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
        # registered drain callback runs to completion — see
        # :func:`_complete_teardown`. The registered managers never
        # suppress, so discarding the suppression return value
        # (always falsy here) is behaviour-preserving.
        await _complete_teardown(self._stack.aclose())

    def __aiter__(self) -> AsyncIterator[ResponseFrame]:
        if self._iterator is None:
            self._schedule_worker()
            self._iterator = self._iterate()
        return self._iterator

    async def _iterate(self) -> AsyncGenerator[ResponseFrame, None]:
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
                    except _WorkerLoopClosed:
                        # The worker loop has been torn down
                        # mid-stream (graceful shutdown landing
                        # between two main-loop pumps).
                        # :meth:`_RequestQueue.put` raises the
                        # typed signal specifically for this case
                        # so a broad ``except RuntimeError`` here
                        # would not silently swallow an unrelated
                        # protocol violation. Break cleanly — the
                        # dispatch is no longer serviceable.
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
            # Read the caller's prime ``NextRequestFrame`` off the
            # request iterator and forward it to the worker. The
            # frame carries the caller's auto-captured wire context;
            # synthesising a context-less ``Request(next=Void())``
            # would bypass the per-frame wire-context propagation
            # the worker driver expects (mid-stream frames carry
            # the manifest; boundary frames don't). Streaming uses
            # ``async for`` over the iterator at the top of this
            # branch — the coroutine branch reads exactly one frame.
            #
            # Fall back to a context-less synthetic ``Next`` if the
            # caller closed the write side before sending the prime
            # frame (``StopAsyncIteration``): the routine still runs,
            # just without caller wire context.
            try:
                prime = await anext(aiter(self._request_iterator))
            except StopAsyncIteration:
                prime = protocol.Request(next=protocol.Void())
            # Mirror the streaming branch: a worker loop torn down
            # between ``__aiter__``'s ``_schedule_worker`` and this
            # first put raises :class:`_WorkerLoopClosed` out of
            # :meth:`_RequestQueue.put`. Exit cleanly rather than
            # ship a transport-teardown failure as a routine fault.
            try:
                request_queue.put(prime)
            except _WorkerLoopClosed:
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
