"""Per-dispatch routine handler.

Layered abstractions cover the worker-side dispatch lifetime:

- `_RequestQueue` / `_ResponseQueue` — cross-loop
  queues bridging the gRPC main loop and the worker loop.
- `_drive_step` — inner routine stepper. One step per
  request, runs inside the chain's cached
  `contextvars.Context`. Routine-shape variation (coroutine
  = one step; async-generator = N steps) lives here.
- `DispatchSession` — per-dispatch async context manager
  and iterator that owns parse, lazy worker scheduling, drive,
  drain, and cancel.

Each class's docstring carries the per-piece detail.
"""

from __future__ import annotations

__all__ = ["DispatchSession", "Rejected"]

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
from wool.runtime.context.exceptions import ChainSerializationError
from wool.runtime.context.manifest import ChainManifest
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import routine_scope
from wool.runtime.serializer import Serializer
from wool.runtime.worker.connection import _complete_teardown
from wool.runtime.worker.frame import ExceptionResponseFrame
from wool.runtime.worker.frame import Frame
from wool.runtime.worker.frame import NextRequestFrame
from wool.runtime.worker.frame import RequestFrame
from wool.runtime.worker.frame import ResponseFrame
from wool.runtime.worker.frame import ResultResponseFrame
from wool.runtime.worker.frame import SendRequestFrame
from wool.runtime.worker.frame import TaskRequestFrame
from wool.runtime.worker.frame import ThrowRequestFrame

_T = TypeVar("_T")

_log = logging.getLogger(__name__)


class Rejected(Exception):
    """Raised by `DispatchSession.__aenter__` when the dispatch
    parse phase fails — Wool chain-manifest decode,
    `wool.Task` rebuild, or routine-type validation.

    The dispatch handler catches this and replies with a Nack
    whose ``exception`` field carries `original` serialized
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
      `protocol.Request` off *request_iterator* and parses
      it: decodes the caller's Wool chain manifest, rebuilds
      the wool.Task, and validates the routine type. Failures are
      wrapped in `Rejected` so the dispatch handler can
      surface them via Nack-with-exception. A first-request read
      failure (empty iterator, gRPC error) propagates raw — no
      parsed payload exists to serialize for the caller.

      The worker-loop driver is **not** scheduled here; that
      happens lazily on the first ``__aiter__`` call. The dispatch
      handler runs pre-iteration decisions (backpressure) against
      the parsed task and the decoded
      `ChainManifest.vars
      <wool.runtime.context.manifest.ChainManifest.vars>`
      mapping off `decoded` before the worker task is
      scheduled.

    - **Iteration** (``__aiter__``) schedules the worker driver on
      first call and drives the request/response loop on the main
      loop. Sets up cross-loop `_RequestQueue` /
      `_ResponseQueue` and submits a worker-loop task that
      enters `routine_scope` for the parsed task and drives the
      routine through `_drive_step`. The
      `concurrent.futures.Future` held by the response
      queue surfaces pre-stream worker failures so they propagate
      out of `_ResponseQueue.get` rather than hang
      iteration. Forwards each subsequent
      `protocol.Request` from *request_iterator* through
      the request queue and yields one `ResponseFrame` per
      response. The coroutine path synthesizes a single ``"next"``
      request. Pre-stream worker failures raise out of
      `_ResponseQueue.get` and propagate raw — the dispatch
      handler's terminal-exception clause builds the wire response
      from `_final_wire_chain_manifest` (encoded by the worker task
      inside its own Chain) and the dumped exception.

    - **Teardown** (``__aexit__``) calls `drain` (close
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
      what propagates `asyncio.CancelledError` into a
      routine mid-``_drive_step``; without it, a compute-bound or
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
    decoded: ChainManifest
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
        # The initial `RequestFrame` decoded in __aenter__ — its
        # chain manifest (when non-None) carries the caller's chain
        # manifest state plus every ``wool.Token`` captured from the
        # task args/kwargs payload. The worker driver's initialization
        # mounts it inside its own ``contextvars.Context`` before the
        # routine is constructed.
        self._initial_frame: RequestFrame | None = None
        # The worker driver encodes its final chain manifest inside the
        # work Chain (Chain.to_manifest reads the backing variables)
        # and publishes it here for the dispatch handler's terminal-
        # exception path. ``_final_encode_error`` carries a strict-mode
        # encode failure instead.
        self._final_wire_chain_manifest: protocol.ChainManifest | None = None
        self._final_encode_error: BaseException | None = None
        # All dispatch serializes through cloudpickle; this one
        # serializer covers the payload, the chain-manifest frames, and
        # any `Rejected` dumped exception (including pre-parse
        # failures such as StopAsyncIteration or a malformed frame).
        self.serializer: Serializer = wool.__serializer__

    @property
    def streaming(self) -> bool:
        """Whether the parsed task is an async-generator routine.

        Set by `__aenter__` after the first request is parsed
        and the callable is validated. Read-only — exposed so the
        dispatch handler can decide whether to unwrap PEP 525's
        synthesized ``RuntimeError("async generator raised
        StopAsyncIteration")`` back to its original
        `StopAsyncIteration` for coroutine routines,
        without reaching across the privacy boundary.
        """
        return self._streaming

    async def _safe_aclose_stack(self) -> None:
        """Defensively close `_stack` and swallow routine
        teardown failures.

        Used by `__aenter__`'s error arms so an aclose
        failure (e.g., resource teardown raising) cannot replace
        the original parse error en route to the dispatch
        handler's Nack channel. Only `Exception` subclasses are
        swallowed; `BaseException` signals
        (``KeyboardInterrupt``, ``SystemExit``,
        ``CancelledError``) propagate — process-level and
        cancellation signals must not be silently dropped during
        cleanup.
        """
        try:
            await _complete_teardown(self._stack.aclose())
        except Exception:  # pragma: no cover — no failing callback on the stack yet
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
            # Decode the initial frame. A strict-mode decode failure is
            # captured as the frame's ``chain_manifest`` value rather
            # than raised mid-decode — surface it up-front so the outer
            # ``except Exception`` arm wraps it in ``Rejected`` for Nack
            # transport (preserving the caller's ``except
            # ChainSerializationError`` semantics).
            decoded = Frame.from_protobuf(request, serializer=self.serializer)
            assert isinstance(decoded, TaskRequestFrame)
            self._initial_frame = decoded
            manifest = self._initial_frame.chain_manifest
            if isinstance(manifest, ChainSerializationError):
                raise manifest
            assert isinstance(self._initial_frame.payload, Task)
            self.task = self._initial_frame.payload
            # Backpressure inspection on the dispatch handler reads
            # ``session.decoded`` — surface the manifest there. An
            # empty manifest (no chain-manifest state) is absent on the
            # frame; fall back to a stateless one so callers see a
            # consistent attribute shape.
            self.decoded = manifest or ChainManifest.empty()

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
        # LIFO unwind. If drain raises (e.g., a ``CancelledError``
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
        `ChainManifest.vars
        <wool.runtime.context.manifest.ChainManifest.vars>`
        mapping off `decoded`. The worker driver's
        initialization frame mounts `decoded` for real inside
        its own `contextvars.Context`.

        Short-circuits when `cancel` was called before the
        first `__aiter__`: the queues stay ``None`` and
        `_iterate` returns an empty stream.

        **Closure capture chain.** The worker driver
        (``_start`` → ``_run`` → ``_on_done``) is structured as
        three nested closures rather than a dataclass-shaped
        ``_WorkerDriver`` because the per-step state
        (``request_queue``, ``response_queue``, ``worker_done``,
        ``serializer``, ``streaming``, ``worker_loop``, ``work_task``)
        is captured fresh per dispatch from the session attributes,
        and the closure form keeps the capture site adjacent to its
        consumers. Captures, by layer:

        * Top-level locals in `_schedule_worker` (this method):
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
          contextvars.Context]`` per dispatch — the wool chain id to
          cached-context map every frame resolves through — and
          ``loop`` (rebound from ``worker_loop`` so the closure body
          avoids a ``asyncio.get_running_loop()`` per iteration).
          Mutates ``self._final_wire_chain_manifest`` /
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
                # it explicitly so `_create_step_task` can
                # construct step tasks bound to it without a fresh
                # `asyncio.get_running_loop` per iteration.
                loop = worker_loop
                # Per-dispatch chain registry: each wool chain id seen
                # on a request frame maps to the cached
                # ``contextvars.Context`` that scopes that chain's
                # bindings on this worker. Frames sharing a chain id
                # reuse one context, so a routine's cross-yield var
                # state — a ``set`` before a yield and its ``reset``
                # after — lands in the same context (stdlib async-gen
                # parity). Frames carrying *distinct* chain ids within
                # one stream get distinct contexts, so they do not
                # pollute each other (stdlib drives each step in the
                # caller's own context). Held weakly: within a dispatch
                # the live ``ctx`` local keeps the entry alive; the
                # registry is dropped wholesale when ``_run`` returns.
                chain_registry: WeakValueDictionary[UUID, contextvars.Context] = (
                    WeakValueDictionary()
                )
                # Working context for unarmed frames. An unarmed caller
                # propagates no chain id, so there is nothing to key on
                # yet; successive unarmed frames share this one context
                # (so a routine's contextvar mutations carry across
                # yields) and it is left *unarmed* — a stateless
                # dispatch never installs a wool Chain, so a plain
                # ``asyncio.to_thread`` offload inside the routine
                # copies a bare context and never trips ChainContention.
                # If the routine's own ``set`` arms it, the post-step
                # hook below indexes it in ``chain_registry`` under the
                # minted chain id, so the back-propagated next frame
                # (now carrying that id) reuses it.
                dispatch_ctx: contextvars.Context | None = None
                # A routine argument can be a live ``wool.Token``. Such tokens
                # were reconstituted when the initial TaskRequestFrame was
                # decoded in the parse phase (``__aenter__``), so they sit on
                # that frame's ``_wire_tokens``. But parse only reads the task
                # off that frame — it never mounts it, and mounting is what
                # anchors a token — mints its native reset binding in the
                # chain's context so the routine can ``reset`` it on this
                # worker. The only frames mounted here are the mid-stream
                # Next/Send/Throw ones, and the caller chain those arg tokens
                # belong to is armed by the *first* of them. So carry the arg
                # tokens over to that first frame and let its mount anchor them
                # alongside the chain they rode in on. Once only: after that
                # mount they are anchored (or, if the caller shipped no chain
                # to anchor into, left as orphans reclaimed with the dispatch),
                # and every later frame just re-mounts that same chain —
                # re-carrying them would be redundant.
                initial_wire_tokens = (
                    list(self._initial_frame._wire_tokens)
                    if self._initial_frame is not None
                    else []
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
                            # Narrow to the mid-stream request subset
                            # (initial TaskRequestFrame is consumed by
                            # the parse phase in ``__aenter__``; only
                            # Next/Send/Throw flow through this loop)
                            # so `_drive_step` gets the precise
                            # union without an extra cast.
                            assert isinstance(
                                decoded,
                                (NextRequestFrame, SendRequestFrame, ThrowRequestFrame),
                            )
                            request = decoded
                            manifest = request.chain_manifest
                            if manifest is None or isinstance(
                                manifest, ChainSerializationError
                            ):
                                # Unarmed frame, or a deferred decode
                                # failure with no chain id to route to:
                                # drive it in the dispatch's single
                                # working context, created lazily and
                                # left *unarmed*. Successive unarmed
                                # frames share it so the routine's
                                # contextvar mutations carry across
                                # yields; a stateless routine never
                                # installs a wool Chain, so the worker
                                # context stays unarmed. For a decode
                                # failure, ``request.mount`` below raises
                                # it (or chains it onto a throw payload)
                                # — nothing is actually mounted.
                                if dispatch_ctx is None:
                                    dispatch_ctx = contextvars.copy_context()
                                ctx = dispatch_ctx
                            else:
                                # Armed frame: key on the propagated
                                # chain id. A miss allocates a fresh
                                # context, so frames from distinct chain
                                # ids within one stream stay isolated. A
                                # chain the routine itself armed from the
                                # working context was indexed here after
                                # that step (see the post-step hook
                                # below), so the back-propagated frame
                                # carrying that id reuses the same
                                # context. The ``copy_context`` snapshot
                                # inherits the driver task's bindings
                                # (``routine_scope`` has entered
                                # ``task.runtime_context`` and set
                                # ``wool.__proxy__``).
                                cached = chain_registry.get(manifest.id)
                                if cached is None:
                                    cached = contextvars.copy_context()
                                    chain_registry[manifest.id] = cached
                                ctx = cached

                            # First mounted frame only: prepend the arg tokens
                            # carried over from the initial TaskRequestFrame
                            # (see above) so this mount anchors them onto the
                            # caller chain, then clear them so subsequent frames
                            # don't re-apply them.
                            if initial_wire_tokens:
                                request._wire_tokens = [
                                    *initial_wire_tokens,
                                    *request._wire_tokens,
                                ]
                                initial_wire_tokens = []
                            request.mount(ctx)

                            # Drive the step via the top-level
                            # `_drive_step` helper. The helper
                            # builds the per-step coroutine (asend /
                            # athrow / coroutine-itself) and runs it
                            # inside the cached context via a Task
                            # constructed directly (bypassing the
                            # loop's task factory; see
                            # `_create_step_task`).
                            try:
                                value = await _drive_step(
                                    routine,
                                    streaming,
                                    request,
                                    ctx,
                                    loop=loop,
                                )
                            except StopAsyncIteration:
                                # Streaming SAI = clean end-of-stream;
                                # break the driver loop. Coroutine SAI
                                # propagates so the asyncgen-transport
                                # path (`_iterate`) ships it;
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
                                # chain's manifest for the dispatch
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
                                    self._final_wire_chain_manifest = ctx.run(
                                        lambda: (
                                            wool.__chain__.get()
                                            .to_manifest()
                                            .to_protobuf(serializer=serializer)
                                        )
                                    )
                                except LookupError:
                                    self._final_wire_chain_manifest = None
                                except BaseException as encode_err:
                                    self._final_encode_error = encode_err
                                    self._final_wire_chain_manifest = None
                                raise

                            # Encode the post-step chain manifest from
                            # within the cached contextvars.Context so the response
                            # reflects the routine's mutations to the
                            # chain's bindings. Pin the captured state
                            # to ``self._final_wire_chain_manifest`` as the
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
                                    lambda: (
                                        wool.__chain__.get()
                                        .to_manifest()
                                        .to_protobuf(serializer=serializer)
                                    )
                                )
                            except LookupError:
                                captured = None
                            except BaseException as encode_err:
                                self._final_encode_error = encode_err
                                self._final_wire_chain_manifest = None
                                raise
                            self._final_wire_chain_manifest = captured
                            # If the routine armed the dispatch's unarmed
                            # working context — its own first ``set``
                            # mints a chain on this worker — index that
                            # context under the new chain id. The next
                            # frame carries that id once the ``set``
                            # back-propagates and arms the caller, so it
                            # resolves to this same context: that is what
                            # lets a ``set`` before a yield and its
                            # ``reset`` after share one context. A
                            # non-None ``captured`` means the context is
                            # armed.
                            if ctx is dispatch_ctx and captured is not None:
                                armed = ctx.run(wool.__chain__.get)
                                chain_registry.setdefault(armed.id, ctx)
                            response_queue.put(
                                ResultResponseFrame.for_send(
                                    value,
                                    serializer=serializer,
                                    wire_chain_manifest=captured,
                                )
                            )
                            if not streaming:
                                break
                    response_queue.close()
                finally:
                    # Stop the producer side immediately on any
                    # ``_run`` exit, including mid-frame routine
                    # exceptions that unwind past the ``async with
                    # routine_scope`` block. Otherwise the producer's
                    # ``_iterate`` keeps queueing frames until its own
                    # ``finally`` (or external teardown via `drain`)
                    # closes the queue, leaving a window where requests
                    # accumulate against a worker that has already
                    # failed.
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
                # at GC. Settle ``worker_done`` so `drain` does
                # not await an unresolved future, and close the
                # response queue so any pending
                # `_ResponseQueue.get` returns immediately.
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
                # Drop the session's strong reference to the completed
                # worker task. ``_run`` closes over ``self``, so
                # ``session -> _worker_task -> coro -> session`` forms a
                # cycle reclaimable only by the cyclic GC; clearing the
                # reference here breaks it so refcounting reclaims the
                # session, task, and per-fork contexts promptly instead
                # of letting them accumulate between GC passes. Safe
                # because ``_worker_task`` is already ``None`` before
                # scheduling and on a scheduling failure, so every reader
                # must already tolerate ``None``; clearing it here adds no
                # state a correct reader isn't already guarding against.
                self._worker_task = None

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
        """Build the terminal `ExceptionResponseFrame` for a
        routine-failure dispatch.

        Owns the encode-error vs. lazy-wire-frame decision and
        the PEP 525 ``StopAsyncIteration`` unwrap, keeping
        `_final_wire_chain_manifest` / `_final_encode_error` access
        encapsulated here.

        Callers MUST have awaited `drain` before invoking this
        so the worker task's in-context encode publish has settled.
        The dispatch handler then yields
        ``session.terminal_response(e, serializer=s).to_protobuf()``
        as the wire-side terminal frame.

        Two distinct shapes ride out:

        * **Lazy-wire-frame** (the worker stayed armed or stayed
          unarmed cleanly): ship the routine exception alongside the
          worker's final chain manifest.
        * **Strict-mode encode failure**: the worker's in-context
          `ChainManifest.to_protobuf` raised — typically a
          `wool.ChainSerializationError` aggregating per-var
          warnings. Chain the encode error as ``__cause__`` on a
          *copy* of the routine exception (the live instance may be
          a module-level singleton — for example, an interpreter-
          cached `StopAsyncIteration` — or already propagating
          elsewhere, so mutating its ``__cause__`` /
          ``__suppress_context__`` would alter globally observable
          state) and ship the result with no chain-manifest payload.

        Coroutine routines that raised `StopAsyncIteration`
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
            An `ExceptionResponseFrame` ready to encode via
            `Frame.to_protobuf`.
        """
        e = exception
        # PEP 525 SAI unwrap (coroutine-only): the streaming
        # transport in `_iterate` synthesizes the
        # ``RuntimeError("async generator raised
        # StopAsyncIteration")`` wrapping when a coroutine raises
        # `StopAsyncIteration`. Unwrap so the caller sees the
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
                # final chain manifest to ship; explicit
                # ``wire_chain_manifest=None`` suppresses the field
                # entirely (and avoids an unrelated auto-capture
                # from the main-loop scope).
                wire_chain_manifest=None,
            )
        # Lazy-wire-frame: when the worker stayed unarmed the
        # captured chain manifest is ``None`` and
        # `Frame.to_protobuf` omits the optional ``context``
        # field; the caller's apply-back skips the absent field.
        return ExceptionResponseFrame.for_send(
            e,
            serializer=serializer,
            wire_chain_manifest=self._final_wire_chain_manifest,
        )

    async def drain(self) -> None:
        """Close the request queue and await the worker driver to
        complete. Idempotent — safe to call multiple times.

        After this returns, the worker task has published its final
        `_final_wire_chain_manifest` (or `_final_encode_error`),
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
        # `_complete_teardown`. The registered managers never
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

        Forwards each `protocol.Request` to the request
        queue and yields one `ResponseFrame` per response
        received. Coroutine path synthesizes a single ``"next"``
        request. Pre-stream worker failures raise out of
        `_ResponseQueue.get` and propagate to the dispatch
        handler's terminal-exception clause.

        Raises `asyncio.CancelledError` when `cancel`
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
        # is a no-op. An unconditional check would ship a
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
                        # `_RequestQueue.put` raises the
                        # typed signal specifically for this case
                        # so a broad ``except RuntimeError`` here
                        # would not silently swallow an unrelated
                        # protocol violation. Break cleanly — the
                        # dispatch is no longer serviceable.
                        break
                    response = await response_queue.get()
                    if response is None:
                        # ``_EOS`` arrived. Two producers can push
                        # it: `cancel` closing the response
                        # queue explicitly, or `_on_done`
                        # closing it after the worker task
                        # finalizes. The cancel-induced path is
                        # the one the trailing check guards
                        # against — the queue's pre-pushed _EOS
                        # may have raced `_on_done` settling
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
                # `drain`'s tolerance for a closed worker
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
            # frame carries the caller's auto-captured chain manifest;
            # synthesising a manifest-less ``Request(next=Void())``
            # would bypass the per-frame chain-manifest propagation
            # the worker driver expects (mid-stream frames carry
            # the manifest; boundary frames don't). Streaming uses
            # ``async for`` over the iterator at the top of this
            # branch — the coroutine branch reads exactly one frame.
            #
            # Fall back to a manifest-less synthetic ``Next`` if the
            # caller closed the write side before sending the prime
            # frame (``StopAsyncIteration``): the routine still runs,
            # just without the caller's chain manifest.
            try:
                prime = await anext(aiter(self._request_iterator))
            except StopAsyncIteration:
                prime = protocol.Request(next=protocol.Void())
            # Mirror the streaming branch: a worker loop torn down
            # between ``__aiter__``'s ``_schedule_worker`` and this
            # first put raises `_WorkerLoopClosed` out of
            # `_RequestQueue.put`. Exit cleanly rather than
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

        Sets a flag observed by `_schedule_worker` (so a
        cancellation arriving before the first `__aiter__`
        short-circuits the worker schedule) and by `_iterate`
        (so iteration surfaces `asyncio.CancelledError` at
        the next yield boundary — mirroring stdlib's ``await task``
        semantics where ``task.cancel()`` from any source produces
        the same observable), cancels the worker driver task on the
        worker loop so a routine mid-``_drive_step`` (e.g.,
        ``await asyncio.sleep(...)``) receives a
        `asyncio.CancelledError` rather than running to
        natural completion, and pushes ``_EOS`` onto the response
        queue so any suspended `_ResponseQueue.get` returns
        ``None`` and unblocks the iterator.

        Worker-task cancellation is scheduled via
        ``loop.call_soon_threadsafe`` to remain cross-loop safe,
        and tolerates a closed worker loop (the dispatch is no
        longer serviceable; the existing ``RuntimeError`` swallow
        on ``call_soon_threadsafe`` matches `drain`'s
        tolerance).

        Unlike a direct ``aclose()`` on `_iterator`, this is
        safe to call from a task other than the one driving the
        iterator — no ``RuntimeError("asynchronous generator is
        already running")`` is possible because no aclose is
        attempted.

        Suspension caveat. The three signals — ``_cancelled`` flag,
        worker-task cancel, ``_EOS`` push — together unblock
        `_iterate`'s ``_ResponseQueue.get`` suspensions and any
        inter-step ``_cancelled`` observation. They do NOT interrupt
        a request-iterator read in flight: a streaming dispatch
        idling on ``async for protobuf_request in
        self._request_iterator`` (between the dispatch handler's
        last yield and the caller's next ``asend``/``anext``) only
        observes ``_cancelled`` after a new frame arrives or after
        the gRPC layer tears the stream down. The operator-preempt
        path (`WorkerService._preempt`) relies on the broader
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


class _EndOfStream:
    """Marker type for the end-of-stream sentinel pushed onto
    `_RequestQueue` and `_ResponseQueue` to wake a
    suspended ``get`` after `close`. Identity is unique by
    construction (one instance, `_EOS`); the dedicated type
    parameterizes both queues precisely without falling back to
    ``object`` or a string ``Literal``.
    """


_EOS: Final[_EndOfStream] = _EndOfStream()
"""Singleton sentinel marking end of a queue-based dispatch stream."""


class _WorkerLoopClosed(Exception):
    """Internal control-flow signal: the worker loop was torn down
    during dispatch.

    Raised by `_RequestQueue.put` when
    ``call_soon_threadsafe`` rejects because the worker loop is
    closed. The streaming and coroutine branches of
    `DispatchSession._iterate` catch this specifically to break
    cleanly without misclassifying the graceful teardown as a routine
    failure.

    Extends `Exception` (not `RuntimeError`) so broad
    ``except RuntimeError:`` patterns elsewhere can't accidentally
    swallow the signal. Matches stdlib's control-flow-signal
    convention — `StopIteration` and `StopAsyncIteration`
    both extend `Exception` directly.
    """


class _RequestQueue:
    """Cross-loop queue carrying gRPC request envelopes from the
    main (gRPC) loop to the worker loop's `_run` driver.

    Producers on the main loop push `protocol.Request`
    envelopes via `put`. The consumer on the worker loop pulls
    them via `get`, which decodes each envelope into a
    `~wool.runtime.worker.frame.RequestFrame` via
    `RequestFrame.from_protobuf` before returning. Decoding on
    the worker side keeps payload deserialization (which may
    reconstitute pickled `wool.ContextVar` instances) inside
    the worker-loop task that runs the routine under the work context.

    Closure: `close` pushes a sentinel so `get` returns
    `None` once the producer side is done.
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
        """Push a `protocol.Request` onto the queue.

        Cross-loop safe — schedules the put on the worker loop via
        `asyncio.AbstractEventLoop.call_soon_threadsafe`.

        Raises `_WorkerLoopClosed` if the worker loop has
        already been torn down (``call_soon_threadsafe`` rejects
        with ``RuntimeError("Event loop is closed")``). Callers
        catch that typed signal specifically to break cleanly
        without misclassifying graceful teardown as a routine
        failure. Other `RuntimeError` instances propagate
        unchanged — protocol violations remain visible rather than
        being swallowed by a broad ``except RuntimeError`` clause.
        """
        try:
            self._worker_loop.call_soon_threadsafe(self._queue.put_nowait, request)
        except RuntimeError as e:
            if self._worker_loop.is_closed():
                raise _WorkerLoopClosed() from e
            # A non-closed-loop ``RuntimeError`` from
            # ``call_soon_threadsafe`` is not reproducible without mocking
            # the loop; surface it rather than swallow it.
            raise  # pragma: no cover

    async def get(self) -> protocol.Request | None:
        """Pop the next request from the queue.

        Decoding is deferred to the consumer (``_run``) so a single
        malformed wire envelope can be surfaced as a typed terminal
        on the response stream rather than killing the worker driver
        task mid-stream.

        Awaitable on the worker loop only.
        """
        item = await self._queue.get()
        if isinstance(item, _EndOfStream):
            return None
        return item

    def close(self) -> None:
        """Signal end of input by pushing the close sentinel."""
        self._worker_loop.call_soon_threadsafe(self._queue.put_nowait, _EOS)


class _ResponseQueue:
    """Cross-loop queue carrying `ResponseFrame` instances from
    the worker loop's `_run` driver back to the main (gRPC)
    loop's `DispatchSession.__aiter__`.

    Producers on the worker loop push frames via `put` and
    signal end-of-stream via `close`. The consumer on the
    main loop pulls them via `get`, which returns `None`
    after a clean termination (the routine exhausted or returned)
    and **raises** the worker task's underlying exception when the
    worker died — the queue holds a reference to the
    worker-completion `concurrent.futures.Future` so the
    sentinel-and-failure check co-locates with the close sentinel
    that triggers it. The exception propagates out of
    `DispatchSession.__aiter__` for the dispatch handler's
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
        # alternation in `_run` and
        # `DispatchSession._iterate` to ≤1 response in flight
        # — is enforced structurally there: the worker pushes one
        # response, then awaits the next request before pushing
        # again. A future change that decouples that cadence
        # (prefetch, batching) needs to add explicit backpressure
        # here rather than relying on this queue to provide it.
        self._queue: asyncio.Queue[ResponseFrame | _EndOfStream] = asyncio.Queue()
        self._main_loop = main_loop
        self._worker_done = worker_done

    def put(self, response: ResponseFrame) -> None:
        """Push a `ResponseFrame` onto the queue.

        Cross-loop safe — schedules the put on the main loop via
        `asyncio.AbstractEventLoop.call_soon_threadsafe`.
        """
        self._main_loop.call_soon_threadsafe(self._queue.put_nowait, response)

    async def get(self) -> ResponseFrame | None:
        """Pop the next response, or `None` after a clean
        `close`.

        **Raises** the worker task's exception when the close
        sentinel arrives and ``worker_done`` carries one —
        surfacing worker failures (pre-stream, routine-time, or
        cancellation) up to `DispatchSession.__aiter__` so they
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
    """Create an `asyncio.Task` that runs *coro* in *context*,
    bypassing Wool's fork-on-task factory.

    The per-step driver constructs each step's task directly via the
    `asyncio.Task` constructor rather than ``loop.create_task``.
    ``loop.create_task`` routes through whatever task factory the loop
    has installed — Wool's factory included — and Wool's factory forks
    the child onto a fresh chain. For an *internal* driver task whose
    job is to drive a chain that already exists on the registry,
    forking would mint a new chain id and break the registry lookup.
    Going through the `asyncio.Task` constructor skips the
    factory and runs *coro* in the supplied *context* unchanged.

    Localised here so the bypass site is identifiable in a grep, and
    so a future change that needs uvloop's native task class can swap
    the construction in one place without touching every caller.
    """
    return asyncio.Task(coro, loop=loop, context=context)


async def _drive_step(
    routine: Any,
    streaming: bool,
    request: NextRequestFrame | SendRequestFrame | ThrowRequestFrame,
    work_ctx: contextvars.Context,
    *,
    loop: asyncio.AbstractEventLoop,
) -> Any:
    """Drive one step of the worker's per-routine loop.

    Builds the step coroutine from the request kind (``asend`` /
    ``athrow`` for an async-generator routine, the routine itself
    for a coroutine routine), runs it inside *work_ctx* as a freshly
    constructed `asyncio.Task`, and returns the step's
    yielded or returned value.

    The top-level per-step build-and-execute, kept a single-purpose
    function readable on its own.

    The session loop in `DispatchSession._schedule_worker`
    wraps the call to capture the post-step chain manifest, pin
    `DispatchSession._final_wire_chain_manifest`, and translate the
    routine's exception types into the streaming-vs-coroutine end-
    of-stream signalling.

    :param routine:
        The active routine — either the coroutine itself (non-
        streaming) or an `AsyncGenerator` (streaming).
    :param streaming:
        ``True`` when *routine* is an async generator and each
        request drives one ``asend`` / ``athrow``.
    :param request:
        The decoded request frame. Determines the step's flavor
        (``Next``/``Send``/``Throw`` for streaming).
    :param work_ctx:
        The cached `contextvars.Context` for the request's
        chain. The step task is constructed against this context so
        backing-variable writes ride the chain's bindings.
    :param loop:
        The worker loop the step task is bound to.

    :returns:
        The value the step coroutine yielded or returned.

    :raises BaseException:
        Whatever the routine raises propagates — the caller's
        wrapper translates `StopAsyncIteration` (streaming
        end-of-stream) and pins the terminal chain manifest for the
        routine-failure path.
    """
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
        # Defensive: cancel the step task if the await was preempted so a
        # routine mid-step doesn't run to natural completion after the
        # caller has gone away. Under real cancellation
        # ``asyncio.Task.cancel`` on the driver propagates through
        # ``_fut_waiter`` to the step task, so the step is already
        # ``done()`` here — this guard only fires for a step that ignores
        # cancellation, a state real routines do not reach, hence the
        # no-cover pragma.
        if not step_task.done():  # pragma: no cover
            step_task.cancel()
            try:
                await step_task
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                pass
