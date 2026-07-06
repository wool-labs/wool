"""Test routines for integration tests.

This module defines all ``@wool.routine`` decorated functions used by the
integration test suite. It is NOT a test file — no ``test_`` prefix — so
pytest will not collect it directly.

Routines are organized by dimension:
- D1 (RoutineShape): coroutine vs async generator vs nested variants
- D8 (RoutineBinding): module function vs instance/class/static method
"""

import asyncio
import contextvars
import functools
import inspect
import os
from enum import Enum
from enum import auto

import wool


class ContextVarPattern(Enum):
    """Dispatch pattern a test scenario applies to a wool.ContextVar.

    Defined here rather than in conftest because the enum travels
    through the TEST_PATTERNS dict across the wire: the worker-side
    decorators and helpers read the members during dispatch, so the
    class must live in a module that is importable on the worker.
    """

    NONE = auto()
    ROUND_TRIP = auto()
    LOCAL_RESET = auto()
    DOWNSTREAM_OVERWRITE = auto()
    DOWNSTREAM_RESET = auto()
    UPSTREAM_RESET = auto()
    PER_YIELD = auto()
    MID_STREAM_FORWARD = auto()
    # The caller mints a token and ferries it via _RESET_TOKENS; the worker
    # consumes it and immediately re-sets the var. The caller then asserts
    # the stale token stays rejected — the fresh set must not erase the
    # consumed id from the chain's spent ledger.
    TOKEN_REMOTE_RESET_THEN_FRESH_SET = auto()


# Module-level wool.ContextVars used by the propagation integration tests.
# They live at module level so cloudpickle imports this module on the
# worker when it unpickles a routine defined here. The import causes
# the worker's own wool.ContextVar instances to self-register in the
# process-wide registry under their ``"<namespace>:<name>"`` keys;
# caller-side contexts with matching keys resolve to the same logical
# var on the worker.
TENANT_ID: wool.ContextVar[str] = wool.ContextVar("tenant_id", default="unknown")
REGION: wool.ContextVar[str] = wool.ContextVar("region", default="global")
TRACE_ID: wool.ContextVar[str] = wool.ContextVar("trace_id", default="none")

# Carries pattern instructions from the caller to the worker. When empty
# (the default), the ``context_pattern_aware`` decorator is a transparent
# no-op, leaving existing tests unaffected.
TEST_PATTERNS: wool.ContextVar[dict] = wool.ContextVar("_test_patterns", default={})

# Carries reset tokens from an outer worker to an inner worker for the
# DOWNSTREAM_RESET pattern. The outer decorator stores tokens here after
# setting vars; the inner decorator reads them and calls reset().
_RESET_TOKENS: wool.ContextVar[dict] = wool.ContextVar("_reset_tokens", default={})


def _resolve_var(name: str) -> wool.ContextVar:
    """Look up a wool.ContextVar by logical name at call time.

    Resolves against the module's live globals each call. This
    indirection exists so that routines can take logical var names
    from the caller-supplied pattern dict (e.g., ``"tenant_id"``)
    and map them to the module-level ``ContextVar`` instance.
    """
    return globals()[name.upper()]


def _execute_patterns(patterns, *, step=None):
    """Execute context-var mutation patterns on the worker side.

    Reads the patterns dict and mutates the appropriate wool.ContextVars.
    For PER_YIELD, the ``step`` argument controls which iteration label
    to apply.
    """
    for var_name, pattern in patterns.items():
        var = _resolve_var(var_name)
        match pattern:
            case ContextVarPattern.ROUND_TRIP:
                var.set(f"worker-mutated-{var_name}")
            case ContextVarPattern.LOCAL_RESET:
                token = var.set(f"temp-{var_name}")
                var.reset(token)
            case ContextVarPattern.PER_YIELD:
                if step is not None:
                    var.set(f"step-{step}")
            case ContextVarPattern.MID_STREAM_FORWARD:
                # Forward pattern: the caller mutates the var to a
                # per-step value before each ``__anext__``; the worker
                # asserts the forward-propagated value reached this
                # frame. A mismatch raises, surfacing as a dispatch
                # exception on the caller's ``__anext__``.
                if step is not None:
                    observed = var.get()
                    assert observed == f"step-{step}", (
                        f"MID_STREAM_FORWARD {var_name}: worker expected "
                        f"forward-propagated 'step-{step}', got {observed!r}"
                    )
            case ContextVarPattern.DOWNSTREAM_OVERWRITE:
                var.set(f"inner-overwrite-{var_name}")
            case ContextVarPattern.DOWNSTREAM_RESET:
                # Inner worker reads the token deposited by the outer
                # worker via _RESET_TOKENS and resets the var.
                tokens = _RESET_TOKENS.get()
                if var_name in tokens:
                    var.reset(tokens[var_name])
            case ContextVarPattern.UPSTREAM_RESET:
                var.set(f"inner-set-{var_name}")
            case ContextVarPattern.TOKEN_REMOTE_RESET_THEN_FRESH_SET:
                # Consume the caller-minted token ferried via
                # _RESET_TOKENS, then immediately re-set the var. The
                # fresh set must not erase the consumed id from the
                # spent ledger — the caller asserts its stale copy
                # still raises after the response merges home.
                tokens = _RESET_TOKENS.get()
                if var_name in tokens:
                    var.reset(tokens[var_name])
                var.set(f"fresh-{var_name}")


def _pre_nested_setup(patterns):
    """Set up context vars before a nested dispatch (outer worker side).

    For DOWNSTREAM_OVERWRITE the outer worker sets the var; the inner
    worker will overwrite. For DOWNSTREAM_RESET the outer worker sets
    the var and deposits a token so the inner worker can reset.

    Also rewrites TEST_PATTERNS so the inner worker sees the "inner"
    side of each nested pattern: DOWNSTREAM_OVERWRITE and
    DOWNSTREAM_RESET are passed through (``_execute_patterns`` handles
    them), and UPSTREAM_RESET is passed through so the inner worker
    sets the var.
    """
    tokens_for_inner = {}
    for var_name, pattern in patterns.items():
        var = _resolve_var(var_name)
        match pattern:
            case ContextVarPattern.DOWNSTREAM_OVERWRITE:
                var.set(f"outer-set-{var_name}")
            case ContextVarPattern.DOWNSTREAM_RESET:
                token = var.set(f"outer-set-{var_name}")
                tokens_for_inner[var_name] = token
            case ContextVarPattern.UPSTREAM_RESET:
                pass  # inner will set; outer resets after return
    if tokens_for_inner:
        _RESET_TOKENS.set(tokens_for_inner)


def _post_nested_teardown(patterns):
    """Clean up after a nested dispatch returns (outer worker side).

    For UPSTREAM_RESET the outer worker overwrites the var that the
    inner worker set with a sentinel value, signalling that the outer
    worker has completed its post-nested teardown step.
    """
    for var_name, pattern in patterns.items():
        var = _resolve_var(var_name)
        if pattern is ContextVarPattern.UPSTREAM_RESET:
            # The inner worker set this var; the outer worker now
            # overwrites it with a sentinel to mark teardown.
            var.set(f"outer-reset-{var_name}")


NESTED_PATTERNS: frozenset[ContextVarPattern] = frozenset(
    {
        ContextVarPattern.DOWNSTREAM_OVERWRITE,
        ContextVarPattern.DOWNSTREAM_RESET,
        ContextVarPattern.UPSTREAM_RESET,
    }
)

# Patterns executed once per generator step (with a ``step`` index)
# rather than once before the first yield. ``PER_YIELD`` mutates the
# var per step (back-propagation direction); ``MID_STREAM_FORWARD``
# asserts the caller's per-step mutation reached the worker frame
# (forward-propagation direction).
_PER_STEP_PATTERNS: frozenset[ContextVarPattern] = frozenset(
    {
        ContextVarPattern.PER_YIELD,
        ContextVarPattern.MID_STREAM_FORWARD,
    }
)


def context_pattern_aware(fn):
    """Decorator that reads TEST_PATTERNS and executes context-var mutations.

    When TEST_PATTERNS is empty (the default), this is a transparent no-op.
    Must be applied between ``@wool.routine`` and the function definition so
    the decorator runs inside the worker context after propagation.

    For nested patterns (DOWNSTREAM_OVERWRITE, DOWNSTREAM_RESET,
    UPSTREAM_RESET) the decorator distinguishes between the outer and
    inner role by checking for a ``_inner`` flag in the patterns dict.
    The outer worker sets up vars, rewrites TEST_PATTERNS with the
    ``_inner`` flag, and calls the nested routine. The inner worker
    sees the flag and runs ``_execute_patterns`` directly.

    The async-generator variant delegates ``asend``, ``athrow``, and
    ``aclose`` to the underlying generator so protocols beyond plain
    ``async for`` work correctly.
    """
    if inspect.isasyncgenfunction(fn):

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            patterns = TEST_PATTERNS.get()
            gen = fn(*args, **kwargs)
            if not patterns:
                # Transparent passthrough — delegate all protocols.
                try:
                    value = await gen.__anext__()
                    while True:
                        try:
                            sent = yield value
                            value = await gen.asend(sent)
                        except GeneratorExit:
                            await gen.aclose()
                            return
                        except StopAsyncIteration:
                            return
                        except BaseException as exc:
                            value = await gen.athrow(type(exc), exc)
                except StopAsyncIteration:
                    return

            is_inner = patterns.get("_inner", False)

            nested_patterns = {k: v for k, v in patterns.items() if v in NESTED_PATTERNS}
            non_nested = {
                k: v
                for k, v in patterns.items()
                if k != "_inner" and v not in NESTED_PATTERNS
            }

            if nested_patterns and is_inner:
                _execute_patterns(nested_patterns)
            elif nested_patterns:
                _pre_nested_setup(nested_patterns)
                inner_patterns = dict(patterns)
                inner_patterns["_inner"] = True
                TEST_PATTERNS.set(inner_patterns)

            # Execute simple patterns (except the per-step ones).
            non_yield_simple = {
                k: v for k, v in non_nested.items() if v not in _PER_STEP_PATTERNS
            }
            if non_yield_simple:
                _execute_patterns(non_yield_simple)

            per_yield = {k: v for k, v in non_nested.items() if v in _PER_STEP_PATTERNS}
            step = 0
            try:
                value = await gen.__anext__()
                while True:
                    if per_yield:
                        _execute_patterns(per_yield, step=step)
                    step += 1
                    try:
                        sent = yield value
                        value = await gen.asend(sent)
                    except GeneratorExit:
                        await gen.aclose()
                        if nested_patterns and not is_inner:
                            _post_nested_teardown(nested_patterns)
                        return
                    except StopAsyncIteration:
                        if nested_patterns and not is_inner:
                            _post_nested_teardown(nested_patterns)
                        return
                    except BaseException as exc:
                        value = await gen.athrow(type(exc), exc)
            except StopAsyncIteration:
                if nested_patterns and not is_inner:
                    _post_nested_teardown(nested_patterns)
                return

        return wrapper

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        patterns = TEST_PATTERNS.get()
        if not patterns:
            return await fn(*args, **kwargs)

        is_inner = patterns.get("_inner", False)

        nested_patterns = {k: v for k, v in patterns.items() if v in NESTED_PATTERNS}
        non_nested = {
            k: v
            for k, v in patterns.items()
            if k != "_inner" and v not in NESTED_PATTERNS
        }

        if nested_patterns and is_inner:
            # Inner worker: execute the patterns directly.
            _execute_patterns(nested_patterns)
        elif nested_patterns:
            # Outer worker: set up vars, then rewrite
            # TEST_PATTERNS with the _inner flag so the
            # inner worker knows to execute directly.
            _pre_nested_setup(nested_patterns)
            inner_patterns = dict(patterns)
            inner_patterns["_inner"] = True
            TEST_PATTERNS.set(inner_patterns)

        # Execute simple patterns (ROUND_TRIP, LOCAL_RESET)
        if non_nested:
            _execute_patterns(non_nested)

        result = await fn(*args, **kwargs)

        if nested_patterns and not is_inner:
            _post_nested_teardown(nested_patterns)
        return result

    return wrapper


@wool.routine
@context_pattern_aware
async def add(a: int, b: int) -> int:
    """Simple coroutine that returns the sum of two integers."""
    return a + b


@wool.routine
@context_pattern_aware
async def gen_range(n: int):
    """Async generator that yields integers 0..n-1."""
    for i in range(n):
        yield i


@wool.routine
@context_pattern_aware
async def gen_range_one_yield():
    """Async generator that yields exactly one value, then exhausts.

    Single-yield variant of :func:`gen_range` for the unified-driver
    happy-path tests. The driver's iteration loop must serve the
    first ``__anext__`` with a value and the second ``__anext__``
    with ``StopAsyncIteration``, exiting cleanly.
    """
    yield 0


@wool.routine
@context_pattern_aware
async def echo_send(n: int):
    """Async generator with asend support.

    Yields "ready" first, then echoes back any value sent via asend().
    """
    value = yield "ready"
    for _ in range(n):
        value = yield value


@wool.routine
@context_pattern_aware
async def resilient_counter(start: int):
    """Async generator with athrow support.

    Yields incrementing integers from *start*. When a ValueError is
    thrown, resets the counter to 0 and continues.
    """
    counter = start
    while True:
        try:
            yield counter
            counter += 1
        except ValueError:
            counter = 0


@wool.routine
@context_pattern_aware
async def closeable_gen():
    """Async generator for aclose testing.

    Yields "alive" in a loop until closed.
    """
    while True:
        yield "alive"


@wool.routine
@context_pattern_aware
async def cancellable_sleep(sentinel_path: str, duration: float = 30.0):
    """Coroutine that sleeps for *duration* and records its termination
    reason at *sentinel_path*.

    Used by integration tests that pin cross-process cancellation
    propagation. The routine writes a ``"started"`` marker immediately
    before suspending on :func:`asyncio.sleep` so a caller can poll for
    suspension instead of guessing a fixed delay; on
    :class:`asyncio.CancelledError` the marker is overwritten with
    ``"cancelled"``, on natural sleep completion it is overwritten with
    ``"completed"``.

    :param sentinel_path:
        Filesystem path the routine writes its termination reason
        to. The caller polls this file after cancelling to verify
        the worker-side routine actually unwound rather than being
        orphaned.
    :param duration:
        Sleep duration in seconds. Should comfortably exceed the
        caller's cancel-then-await window.
    """
    try:
        with open(sentinel_path, "w") as f:
            f.write("started")
        await asyncio.sleep(duration)
    except asyncio.CancelledError:
        with open(sentinel_path, "w") as f:
            f.write("cancelled")
        raise
    else:
        with open(sentinel_path, "w") as f:
            f.write("completed")


@wool.routine
@context_pattern_aware
async def cancellable_gen(sentinel_path: str):
    """Async generator that yields ``"alive"`` forever and records its
    cleanup reason at *sentinel_path*.

    Companion to :func:`cancellable_sleep` for the async-generator
    cancellation paths. Writes ``"cleaned_up"`` to the sentinel file
    when the generator is closed (caller ``aclose`` or ``break`` out
    of ``async for`` routed through gRPC) — exiting the
    ``try/finally`` block, regardless of whether the exit was driven
    by :class:`GeneratorExit` (aclose) or :class:`asyncio.CancelledError`
    (caller task cancel).

    :param sentinel_path:
        Filesystem path the generator writes ``"cleaned_up"`` to on
        teardown. The caller polls this file after closing to verify
        the worker-side routine ran its ``finally`` block.
    """
    try:
        while True:
            yield "alive"
    finally:
        with open(sentinel_path, "w") as f:
            f.write("cleaned_up")


@wool.routine
@context_pattern_aware
async def self_cancel_coroutine():
    """Coroutine that raises :class:`asyncio.CancelledError` from its
    body without being externally cancelled.

    Mirrors stdlib's ``await task`` semantics where a coroutine that
    self-raises :class:`asyncio.CancelledError` is indistinguishable
    from one that was externally cancelled — both transition the
    task to ``CANCELLED`` and the caller's ``await`` raises
    :class:`asyncio.CancelledError`. Wool's wire must preserve this
    contract; the caller's ``await`` on this routine should raise
    :class:`asyncio.CancelledError` raw, not :class:`RpcError` or
    :class:`UnexpectedResponse`.
    """
    raise asyncio.CancelledError("self-raised from routine body")


@wool.routine
async def add_then_schedule_cleanup(a: int, b: int, sentinel_path: str) -> int:
    """Coroutine that returns the sum and schedules an orphaned cleanup
    task on the worker loop from its ``finally`` clause.

    Models the fire-and-forget cleanup pattern behind issue #202: a
    routine whose ``finally`` schedules further work on the worker
    loop. The scheduled task (:func:`_drain_probe_first_gen`) outlives
    the dispatch and is left for worker-loop teardown to drain.

    :param a:
        First addend.
    :param b:
        Second addend.
    :param sentinel_path:
        Filesystem path threaded through the cleanup chain; the
        deepest generation writes to it so the caller can verify the
        teardown drain reached every generation.
    :returns:
        The sum ``a + b``.
    """
    try:
        return a + b
    finally:
        asyncio.get_running_loop().create_task(_drain_probe_first_gen(sentinel_path))


async def _drain_probe_first_gen(sentinel_path: str) -> None:
    """First-generation orphan task scheduled by
    :func:`add_then_schedule_cleanup`.

    Awaits indefinitely until worker-loop teardown cancels it, then
    schedules the second generation from its own ``finally`` clause —
    the generation a single-pass shutdown drain never observes.
    """
    try:
        await asyncio.Event().wait()
    finally:
        asyncio.get_running_loop().create_task(_drain_probe_second_gen(sentinel_path))


async def _drain_probe_second_gen(sentinel_path: str) -> None:
    """Second-generation orphan task scheduled by
    :func:`_drain_probe_first_gen`.

    Writes ``"drained"`` to *sentinel_path* from its ``finally``
    clause. The file appears only if the worker-loop teardown drain
    cancels and awaits this generation; a single-pass drain leaves it
    pending and unstarted, so the absence of the file flags the
    issue #202 regression.
    """
    try:
        await asyncio.Event().wait()
    finally:
        with open(sentinel_path, "w") as f:
            f.write("drained")


@wool.routine
@context_pattern_aware
async def nested_add(a: int, b: int) -> int:
    """Coroutine that dispatches to ``add``, triggering nested dispatch."""
    return await add(a, b)


@wool.routine
@context_pattern_aware
async def nested_gen(n: int):
    """Async generator that yields from ``gen_range``, nested streaming."""
    async for item in gen_range(n):
        yield item


@wool.routine
async def get_pid() -> int:
    """Coroutine that returns the worker process id.

    Used to observe which worker process executed a dispatch.
    """
    return os.getpid()


@wool.routine
async def nested_pid_fanout(n: int) -> tuple:
    """Report the outer worker's pid and the pids of ``n`` nested dispatches.

    Each nested `get_pid` dispatch goes through the pool rather than
    self-executing on the outer worker. Returns
    ``(outer_pid, [inner_pids...])``.
    """
    return os.getpid(), [await get_pid() for _ in range(n)]


class Routines:
    """Test class providing instance, class, and static method bindings."""

    @wool.routine
    @context_pattern_aware
    async def instance_add(self, a: int, b: int) -> int:
        """Instance method coroutine."""
        return a + b

    @wool.routine
    @context_pattern_aware
    async def instance_gen(self, n: int):
        """Instance method async generator."""
        for i in range(n):
            yield i

    @wool.routine
    @context_pattern_aware
    async def instance_echo_send(self, n: int):
        """Instance method async generator with asend."""
        value = yield "ready"
        for _ in range(n):
            value = yield value

    @wool.routine
    @context_pattern_aware
    async def instance_resilient_counter(self, start: int):
        """Instance method async generator with athrow."""
        counter = start
        while True:
            try:
                yield counter
                counter += 1
            except ValueError:
                counter = 0

    @wool.routine
    @context_pattern_aware
    async def instance_closeable_gen(self):
        """Instance method async generator for aclose."""
        while True:
            yield "alive"

    @classmethod
    @wool.routine
    @context_pattern_aware
    async def class_add(cls, a: int, b: int) -> int:
        """Class method coroutine."""
        return a + b

    @classmethod
    @wool.routine
    @context_pattern_aware
    async def class_gen(cls, n: int):
        """Class method async generator."""
        for i in range(n):
            yield i

    @staticmethod
    @wool.routine
    @context_pattern_aware
    async def static_add(a: int, b: int) -> int:
        """Static method coroutine."""
        return a + b

    @staticmethod
    @wool.routine
    @context_pattern_aware
    async def static_gen(n: int):
        """Static method async generator."""
        for i in range(n):
            yield i


@wool.routine
@context_pattern_aware
async def get_tenant_id() -> str:
    """Coroutine that returns the worker-side value of TENANT_ID.

    Used to verify that a wool.ContextVar value set on the caller is
    propagated into the worker's context before the routine runs.
    """
    return TENANT_ID.get()


@wool.routine
async def reset_tenant_id_token(token) -> str:
    """Reset a caller-minted TENANT_ID token on the worker.

    The token crosses the wire as an argument; resetting it on the worker
    both restores TENANT_ID (returned for inspection) and records the token
    as consumed, which the response frame propagates back so the caller's
    original token also reads as used — cross-process single-use.
    """
    TENANT_ID.reset(token)
    return TENANT_ID.get()


@wool.routine
async def describe_tenant_id_token(token) -> tuple:
    """Receive a TENANT_ID token without resetting it.

    Exercises lossless token transport: any token — including an orphan
    minted in a context the worker is not continuing — must cross the wire,
    reconstitute into a usable object, and travel back, without raising.
    Returns the token itself alongside its var's name so the caller can
    assert the reconstituted object is fully usable.
    """
    return token, token.var.name


async def _reset_and_report(token) -> tuple:
    """Reset TENANT_ID by *token* and report the outcome as a two-tuple.

    Shared tail for the report-style token routines: ``("ok",
    restored_value)`` when the reset succeeds, or ``(type_name, message)``
    when it raises — so a caller can assert the failure without relying on a
    builtin exception type surviving the wire.
    """
    try:
        TENANT_ID.reset(token)
    except Exception as error:
        return type(error).__name__, str(error)
    return "ok", TENANT_ID.get()


@wool.routine
async def set_tenant_id_and_return_token(value: str):
    """Set TENANT_ID on the worker and return the minted token.

    The token rides the response frame back to the caller, whose context
    continues the worker's (the routine was awaited), so the caller may
    reset it — restoring the pre-call value — exactly as if the set had
    happened in an awaited local coroutine.
    """
    return TENANT_ID.set(value)


@wool.routine
async def reset_tenant_id_token_report(token) -> tuple:
    """Attempt to reset a TENANT_ID token on the worker and report the outcome.

    Catch-and-report variant of `reset_tenant_id_token` for cases where
    the reset is expected to fail — the caller asserts on the reported
    exception type name and message rather than relying on a builtin
    exception type surviving the wire. Returns ``(pid, "ok", restored_value)``
    on success or ``(pid, type_name, message)`` on failure.
    """
    return os.getpid(), *await _reset_and_report(token)


@wool.routine
async def reset_and_return_tenant_id_token(token):
    """Reset a caller-minted TENANT_ID token on the worker and return it.

    The token arrives live, is consumed by the reset, and travels back in
    the return value already marked used — the caller can assert the used
    state is visible on the returned copy and that resetting either copy
    raises the single-use RuntimeError.
    """
    TENANT_ID.reset(token)
    return token


@wool.routine
async def mint_orphan_tenant_id_token():
    """Mint a TENANT_ID token inside a sibling stdlib context on the worker.

    The set runs inside ``contextvars.copy_context().run``, so no live
    context continues the minting context — the token is an orphan by
    construction. It travels back in the return value so the caller can
    assert lossless transport and the different-Context reset failure.
    """
    sibling = contextvars.copy_context()
    return sibling.run(TENANT_ID.set, "worker-sibling")


@wool.routine
async def relay_reset_tenant_id_token(token) -> str:
    """Relay a TENANT_ID token through a nested dispatch that resets it.

    The token crosses two hops — caller to outer worker, outer worker to
    inner worker — before `reset_tenant_id_token` consumes it. The
    consumed state rides each response frame back so the caller's original
    token also reads as used.
    """
    return await reset_tenant_id_token(token)


@wool.routine
async def nested_reset_then_report(token) -> tuple:
    """Consume a token via nested dispatch, then retry the reset locally.

    The inner dispatch resets the token; its response frame carries the
    consumed state back one hop, so the outer worker's local retry must
    fail the single-use gate. Returns ``(inner_value, type_name, message)``
    on retry failure or ``(inner_value, "ok", current_value)`` if the retry
    unexpectedly succeeds.
    """
    inner = await reset_tenant_id_token(token)
    return inner, *await _reset_and_report(token)


@wool.routine
async def outer_mint_inner_reset() -> tuple:
    """Mint a token on the outer worker and consume it via nested dispatch.

    The outer routine sets TENANT_ID, sends the minted token to
    `reset_tenant_id_token` on an inner worker (an awaited dispatch,
    so the inner worker continues the minting context), then retries the
    reset locally. Returns ``(inner_value, type_name, message)`` on retry
    failure or ``(inner_value, "ok", current_value)`` if the retry
    unexpectedly succeeds.
    """
    token = TENANT_ID.set("outer-mint")
    inner = await reset_tenant_id_token(token)
    return inner, *await _reset_and_report(token)


@wool.routine
async def stream_reset_tenant_id_token(token):
    """Async generator that resets a caller-minted token between yields.

    Yields the pre-reset TENANT_ID value, resets the token, then yields
    the restored value. The consumed state rides the step frame back to
    the caller mid-stream, before the generator is exhausted.
    """
    yield TENANT_ID.get()
    TENANT_ID.reset(token)
    yield TENANT_ID.get()


@wool.routine
async def set_tenant_id_and_yield_token(value: str, count: int):
    """Async generator that sets TENANT_ID and yields the minted token first.

    After yielding the token, suspends and yields the current TENANT_ID
    value *count* times. The token's live anchor must survive each
    per-yield re-mount so the caller can reset it exactly once after
    exhaustion.
    """
    token = TENANT_ID.set(value)
    yield token
    for _ in range(count):
        await asyncio.sleep(0)
        yield TENANT_ID.get()


@wool.routine
async def stream_reset_token_twice(token):
    """Async generator that resets the same token before two yields.

    The first reset consumes the token; the second violates single-use and
    raises before the second yield, surfacing as a mid-stream dispatch
    error on the caller's ``__anext__``.
    """
    TENANT_ID.reset(token)
    yield "first"
    TENANT_ID.reset(token)
    yield "never"


@wool.routine
async def stream_recv_and_reset():
    """Async generator that receives a token via ``asend`` and resets it.

    Yields ``"ready"``, receives a caller-minted token on the mid-stream
    ``asend`` frame, resets it, and yields the restored TENANT_ID value.
    Exercises token transport and consumption on the ``asend`` path.
    """
    received = yield "ready"
    TENANT_ID.reset(received)
    yield TENANT_ID.get()


@wool.routine
@context_pattern_aware
async def stream_tenant_id(count: int):
    """Async generator that yields TENANT_ID.get() *count* times.

    A sleep(0) between yields forces the generator to suspend, so each
    subsequent read happens in the restored worker context rather than
    returning a cached value from a single frame. Used as the
    regression guard for the unified-driver async-generator fix
    (issue #187 collapsed the pre-existing ``_stream_from_worker``
    into :class:`DispatchSession`).
    """
    for _ in range(count):
        yield TENANT_ID.get()
        await asyncio.sleep(0)


@wool.routine
@context_pattern_aware
async def nested_get_tenant_id() -> str:
    """Coroutine that reads TENANT_ID via a nested dispatch.

    Used to verify that propagated values survive through the nested
    dispatch chain when one routine invokes another.
    """
    return await get_tenant_id()


@wool.routine
@context_pattern_aware
async def streaming_nested_get_tenant_id(count: int):
    """Async generator that nested-dispatches ``get_tenant_id`` each iteration.

    Mutates TENANT_ID to a per-iteration value, dispatches
    ``get_tenant_id`` (nested), and yields the observed value.
    Verifies that the ``_current_task`` and chain context set by
    the worker for the streaming routine remain active across the
    generator's lifespan — without that, the nested dispatch cannot
    find the caller's task and the propagation chain breaks.
    """
    for i in range(count):
        TENANT_ID.set(f"step-{i}")
        observed = await get_tenant_id()
        yield observed


@wool.routine
@context_pattern_aware
async def mutate_and_read_tenant_id() -> str:
    """Coroutine that mutates TENANT_ID on the worker and returns it.

    The caller is expected to assert that its own TENANT_ID value
    remains unchanged after this routine returns — the worker's
    mutation is scoped to its isolated context copy per stdlib
    copy-on-inherit semantics.
    """
    TENANT_ID.set("mutated_on_worker")
    return TENANT_ID.get()


@wool.routine
@context_pattern_aware
async def read_multi_vars() -> tuple[str, str]:
    """Coroutine that reads TENANT_ID and REGION simultaneously.

    Used to verify that multiple wool.ContextVars in the registry are
    all propagated and restored correctly through a single dispatch.
    """
    return TENANT_ID.get(), REGION.get()


@wool.routine
@context_pattern_aware
async def stream_and_mutate_tenant_id(count: int):
    """Async generator that yields TENANT_ID then mutates on the last iteration.

    Yields TENANT_ID.get() for *count - 1* iterations, then sets
    TENANT_ID to ``"final-mutation"`` and yields that value on the
    last iteration. Used to verify that an async generator mutation
    on the worker is back-propagated to the caller after iteration
    completes.
    """
    for i in range(count):
        if i == count - 1:
            TENANT_ID.set("final-mutation")
        yield TENANT_ID.get()
        await asyncio.sleep(0)


@wool.routine
@context_pattern_aware
async def mutate_on_each_yield(count: int):
    """Async generator that mutates TENANT_ID on every iteration.

    On each iteration sets TENANT_ID to ``"step-{i}"`` and yields the
    new value. Used to verify that per-yield back-propagation updates
    the caller's wool.ContextVar after each iteration.
    """
    for i in range(count):
        TENANT_ID.set(f"step-{i}")
        yield TENANT_ID.get()
        await asyncio.sleep(0)


@wool.routine
async def return_current_chain_id_hex() -> str:
    """Coroutine that returns the worker-side context ``chain_id`` hex.

    Used to verify that a dispatch boundary correctly arms the worker
    on the caller's chain. The worker installs the caller's decoded
    context via ``install_context``, so its ``chain_id`` equals the
    caller's (or the child's, when dispatched from an asyncio child
    task that has forked the chain).
    """

    context = wool.__chain__.get(None)
    assert context is not None
    return context.id.hex


@wool.routine
async def stream_chain_id_hex(count: int):
    """Async generator that yields the worker-side context ``chain_id`` hex.

    The streaming counterpart of :func:`return_current_chain_id_hex`. A
    ``sleep(0)`` between yields forces the generator to suspend, so each
    read happens after a genuine resume across an ``__anext__``
    boundary. Used to verify that two interleaved async-generator
    dispatches both observe the caller's shared chain id.
    """

    for _ in range(count):
        context = wool.__chain__.get(None)
        assert context is not None
        yield context.id.hex
        await asyncio.sleep(0)


@wool.routine
async def append_to_list(items: list, value) -> list:
    """Coroutine that appends *value* to *items* in place and returns it.

    Used by the unified-driver argument-copy tests. The worker receives
    a serialized copy of *items*, so the in-place ``append`` mutates
    only the worker's copy and the caller's original list object is
    left unchanged.
    """
    items.append(value)
    return items


@wool.routine
async def append_on_each_yield(items: list, values: list):
    """Async generator that appends each value in *values* to *items*
    across successive yields.

    On every iteration it appends the next value in *values* to
    *items* in place and yields a snapshot of the worker-side list.
    The caller's original *items* object is unaffected because the
    worker operates on a serialized copy of the argument.
    """
    for value in values:
        items.append(value)
        yield list(items)
        await asyncio.sleep(0)


@wool.routine
async def touch_argument(argument):
    """Coroutine that returns its single argument unchanged.

    A trivial single-argument routine used by the unpicklable-argument
    dispatch tests — the routine body never runs when the argument
    cannot be serialized, since the failure happens at the dispatch
    serialization boundary.
    """
    return argument


@wool.routine
async def mutate_then_raise_tenant_id(value: str) -> str:
    """Coroutine that sets ``TENANT_ID`` to *value* then raises ValueError.

    Used by exception-path back-propagation tests — the worker's mutation
    should reach the caller via the exception's context path.
    """
    TENANT_ID.set(value)
    raise ValueError("mutate_then_raise_tenant_id")


@wool.routine
async def yield_then_mutate_and_raise(sentinel: str):
    """Async generator that yields once, then sets ``TENANT_ID`` and raises.

    Yields ``"ready"`` first so the caller can iterate once, then sets
    ``TENANT_ID`` to *sentinel* before raising ``ValueError``. Used to
    verify that mid-stream mutations are back-propagated through the
    exception context.
    """
    yield "ready"
    TENANT_ID.set(sentinel)
    raise ValueError("yield_then_mutate_and_raise")


@wool.routine
async def spawn_and_mutate_tenant_id() -> tuple[str, str]:
    """Coroutine: parent sets, spawns child that mutates, parent reads.

    Stdlib copy-on-fork parity: the child task runs with a COPY of the
    parent's stdlib Context, so the child's mutation does not leak
    back into the parent. Returns ``(child_read, parent_read)`` for
    assertion.
    """
    TENANT_ID.set("parent")

    async def _child():
        TENANT_ID.set("child")
        return TENANT_ID.get()

    child_value = await asyncio.create_task(_child())
    parent_value = TENANT_ID.get()
    return child_value, parent_value


@wool.routine
async def parent_sets_child_reads() -> str:
    """Coroutine: parent sets TENANT_ID, child task reads without mutating.

    Verifies that a stdlib-fork child inherits the parent's pre-fork
    value (copy-on-inherit).
    """
    TENANT_ID.set("parent-set")

    async def _child():
        return TENANT_ID.get()

    return await asyncio.create_task(_child())


@wool.routine
async def two_children_mutate_tenant_id() -> tuple[str, str, str]:
    """Coroutine that spawns two children mutating TENANT_ID to distinct values.

    Returns ``(a_value, b_value, parent_value)``. Neither child's
    mutation should leak into the other's context nor into the parent.
    """

    async def _child(value: str) -> str:
        TENANT_ID.set(value)
        return TENANT_ID.get()

    a_value, b_value = await asyncio.gather(_child("alpha"), _child("beta"))
    parent_value = TENANT_ID.get()
    return a_value, b_value, parent_value


@wool.routine
async def stream_tenant_id_echo(count: int):
    """Async generator that yields ``TENANT_ID.get()`` on each iteration.

    Used by forward-propagation mid-stream tests — between iterations
    the caller mutates the var; each new yield should observe the
    latest caller value.
    """
    for _ in range(count):
        yield TENANT_ID.get()
        await asyncio.sleep(0)


@wool.routine
async def echo_tenant_id_on_send(count: int):
    """Async generator that echoes ``TENANT_ID.get()`` each asend round-trip.

    First yields ``"ready"``; then for each sent value echoes
    ``TENANT_ID.get()``. The caller mutates ``TENANT_ID`` before each
    ``asend``; the echoed value should track the caller's current
    value at the moment of each send.
    """
    yield "ready"
    for _ in range(count):
        _ = yield TENANT_ID.get()


@wool.routine
async def read_on_athrow():
    """Async generator that reads ``TENANT_ID`` inside an athrow handler.

    Yields once; on ``athrow`` yields ``TENANT_ID.get()`` from inside
    the handler, then returns. Used to verify that a forward-propagated
    mutation reaches the worker in the frame of an ``athrow`` call.
    """
    try:
        yield "ready"
    except BaseException:
        yield TENANT_ID.get()
        return


@wool.routine
async def read_tenant_id_only() -> str:
    """Coroutine that reads ``TENANT_ID`` only.

    The caller may additionally set an unregistered-on-worker var; the
    dispatch must complete and the routine must still see
    ``TENANT_ID`` regardless of whether the unknown key arrives on the
    wire.
    """
    return TENANT_ID.get()


@wool.routine
async def declare_and_read_unregistered_key(
    namespace: str, name: str, default: str
) -> str:
    """Declare a :class:`wool.ContextVar` matching a wire-shipped key
    that the worker had not registered before the dispatch arrived, and
    return its observed value.

    Exercises the stub-promotion path: the wire frame creates a stub
    in the registry with the caller's value applied to the active
    context; the in-routine ``ContextVar(name, namespace=...)`` call
    finds the stub and promotes it in place, preserving the
    wire-applied value on the new authoritative declaration.
    """
    var = wool.ContextVar(name, namespace=namespace, default=default)
    return var.get()


@wool.routine
async def read_dispatch_timeout() -> float | None:
    """Return the worker-side value of the ambient ``dispatch_timeout``.

    Verifies that a caller-side :class:`wool.RuntimeContext`
    rides through the dispatch wire frame and is restored on the
    worker before the routine body executes — independent of the
    :class:`wool.ContextVar` propagation path.
    """
    from wool.runtime.context.runtime import dispatch_timeout

    return dispatch_timeout.get()


@wool.routine
async def mutate_then_nested_get_tenant_id(mid_value: str) -> str:
    """Mutate ``TENANT_ID`` to *mid_value* then nested-dispatch ``get_tenant_id``.

    Verifies the nested-dispatch propagation diagram: a routine that
    mutates a :class:`wool.ContextVar` mid-flight and then dispatches
    a nested routine should propagate the mid-flight value to the
    nested worker, not the caller's pre-dispatch value.
    """
    TENANT_ID.set(mid_value)
    return await get_tenant_id()


def _decode_bomb_rebuild():
    """Rebuild callable referenced by :class:`DecodeBomb`'s ``__reduce__``.

    Raises unconditionally — when a pickled :class:`DecodeBomb` is
    unpickled (the worker-side ``decode_context`` step), this fires
    and the per-entry decode fails. The caller never unpickles its own
    outgoing context, so the failure is worker-side only.
    """
    raise RuntimeError("decode bomb detonated on unpickle")


class DecodeBomb:
    """A value that pickles cleanly but raises when unpickled.

    Models a version-skew payload: ``encode_context`` on the caller
    pickles it without error (``__reduce__`` just stores the rebuild
    tuple), but the worker's ``decode_context`` calls
    :func:`_decode_bomb_rebuild`, which raises. Used to drive the
    worker-side context-decode-failure → Nack path.
    """

    def __reduce__(self):
        return (_decode_bomb_rebuild, ())


@wool.routine
async def set_tenant_then_crash_worker(value: str) -> str:
    """Set ``TENANT_ID`` then hard-crash the worker process.

    Sets the var (arming the chain), then calls ``os._exit`` so the
    worker subprocess dies mid-dispatch without a graceful response.
    The caller should observe an ``RpcError`` / ``UnexpectedResponse``
    and its own context state must stay intact — no half-merged
    back-propagation from the crashed worker.
    """
    import os

    TENANT_ID.set(value)
    os._exit(70)


@wool.routine
async def set_tenant_then_sleep(value: str, sentinel_path: str, duration: float = 30.0):
    """Set ``TENANT_ID`` then sleep — for armed-context cancellation.

    Writes ``"started"`` to *sentinel_path* immediately before
    suspending on :func:`asyncio.sleep` so the caller can poll for
    suspension. On :class:`asyncio.CancelledError` the marker is
    overwritten with ``"cancelled"``. The var mutation is the partial
    state whose fate under cancellation the caller pins.
    """
    TENANT_ID.set(value)
    try:
        with open(sentinel_path, "w") as f:
            f.write("started")
        await asyncio.sleep(duration)
    except asyncio.CancelledError:
        with open(sentinel_path, "w") as f:
            f.write("cancelled")
        raise


@wool.routine
async def set_and_reset_tenant_across_yield(value: str):
    """Async generator that sets ``TENANT_ID``, yields, resets, yields.

    First sets ``TENANT_ID`` to *value* and yields ``"set"``; then
    resets the var via the token from that set and yields ``"reset"``.
    The caller observes the per-yield back-propagation across the
    set→reset boundary: its own ``TENANT_ID`` tracks the value after
    the first yield and reverts after the second.
    """
    token = TENANT_ID.set(value)
    yield "set"
    TENANT_ID.reset(token)
    yield "reset"


@wool.routine
async def read_unbound_default_less_var(namespace: str, name: str) -> str:
    """Declare a default-less :class:`wool.ContextVar` and ``get()`` it.

    The var has no constructor default and is unbound, so ``get()``
    with no argument raises :class:`LookupError`. The exception must
    surface to the caller through the exception back-propagation path.
    """
    var = wool.ContextVar(name, namespace=namespace)
    return var.get()


@wool.routine
async def count_wool_context_vars() -> int:
    """Return the count of wool-owned ``contextvars.ContextVar``s.

    Enumerates ``contextvars.copy_context()`` and counts entries whose
    name carries the ``__wool`` prefix — the context variable plus one
    backing variable per bound :class:`wool.ContextVar`.
    """
    import contextvars as _cv

    return sum(1 for var in _cv.copy_context() if var.name.startswith("__wool"))


@wool.routine
async def reenter_armed_chain_off_owner_thread(value: str) -> str:
    """Arm the routine's chain, then read the var off the owner thread.

    Sets ``TENANT_ID`` (arming the routine's chain on the worker's
    loop thread), then hands a ``wool.ContextVar`` access to a worker
    thread via :func:`asyncio.to_thread`. ``asyncio.to_thread`` copies
    the surrounding ``contextvars`` context — chain UUID and owner
    included — into the executor thread, so the off-thread ``get()``
    re-enters an armed chain from a thread other than its owner and
    raises :class:`wool.ChainContention`. The exception surfaces
    to the caller through the exception back-propagation path.
    """
    TENANT_ID.set(value)
    return await asyncio.to_thread(TENANT_ID.get)


@wool.routine
async def read_var_off_thread_via_wool_to_thread(value: str) -> str:
    """Arm the routine's chain, then read the var off-thread via wool.to_thread.

    Sets ``TENANT_ID`` (arming the routine's chain on the worker's loop
    thread), then offloads the var read to a worker thread via
    :func:`wool.to_thread`. Unlike :func:`asyncio.to_thread` — which
    copies the armed chain verbatim and trips
    :class:`wool.ChainContention` off the owner thread —
    ``wool.to_thread`` forks the chain onto a fresh, detached chain
    owned by the worker thread, so the off-thread ``get()`` re-arms
    cleanly and observes the forked copy of the value.
    """
    TENANT_ID.set(value)
    return await wool.to_thread(TENANT_ID.get)


async def _drive_proxy_collision_on_worker() -> wool.ChainContention:
    """Drive two tasks sharing one fresh context into a proxy collision.

    Runs inside a dispatched routine, whose own chain is already armed
    and owned by the routine task. A FRESH `contextvars.Context` is used
    — not `contextvars.copy_context` — so it starts unarmed and is shared
    (not forked) by the two child tasks: the first child enters a proxy
    in it and owns it, and the second child's entry into that context it
    does not own raises `wool.ChainContention` with kind ``"task"``. The
    events serialize the interleave so the first always enters first and
    stays live until the second has contended. The proxies are lazy, so
    entry incurs no discovery or worker startup.
    """
    wool.install_task_factory()
    loop = asyncio.get_running_loop()
    shared = contextvars.Context()
    armed = asyncio.Event()
    first_can_finish = asyncio.Event()

    async def first() -> None:
        async with wool.WorkerProxy("proxy-collision-pool", lazy=True):
            armed.set()
            await first_can_finish.wait()

    async def second():
        await armed.wait()
        try:
            async with wool.WorkerProxy("proxy-collision-pool", lazy=True):
                return None
        except wool.ChainContention as exc:
            return exc
        finally:
            first_can_finish.set()

    first_task = loop.create_task(first(), context=shared)
    second_task = loop.create_task(second(), context=shared)
    observed, _ = await asyncio.gather(second_task, first_task)
    assert isinstance(observed, wool.ChainContention)
    return observed


@wool.routine
async def report_proxy_collision_in_shared_context() -> str:
    """Trigger a worker-side proxy collision and return its contention kind.

    Drives two tasks sharing one fresh context into a `wool.WorkerProxy`
    collision on the worker, catches the resulting `wool.ChainContention`,
    and returns its ``kind`` (``"task"``) as a plain string — proving the
    guard fired on the worker without relying on the exception crossing
    the wire.
    """
    return (await _drive_proxy_collision_on_worker()).kind


@wool.routine
async def raise_proxy_collision_in_shared_context() -> None:
    """Trigger a worker-side proxy collision and re-raise it to the caller.

    Lets the worker-side `wool.ChainContention` propagate through the
    routine-exception back-propagation channel so the caller observes it
    directly (relies on the contention being wire-safe).
    """
    raise await _drive_proxy_collision_on_worker()


@wool.routine
async def stream_proxy_collision_in_shared_context():
    """Stream a worker-side proxy collision: yield ``"ready"``, then its kind.

    Async-generator variant of
    `report_proxy_collision_in_shared_context`: yields ``"ready"`` first,
    then drives the worker-side collision and yields its contention
    ``kind`` (``"task"``) — exercising the async-generator dispatch
    driver.
    """
    yield "ready"
    yield (await _drive_proxy_collision_on_worker()).kind


@wool.routine
async def enter_proxies_in_separate_tasks_on_worker() -> str:
    """Enter two proxies in separate forked tasks on the worker (no collision).

    Each ``asyncio.gather`` child forks a fresh, child-owned chain via
    the task factory, so concurrent proxy entries inside a dispatched
    routine never contend. Returns ``"ok"``.
    """

    async def use_proxy() -> None:
        async with wool.WorkerProxy("proxy-collision-pool", lazy=True):
            await asyncio.sleep(0)

    await asyncio.gather(use_proxy(), use_proxy())
    return "ok"


# Key-addressed oracle routines used by the token-parity op scripts
# (tests/integration/_token_ops.py). Each addresses a wool.ContextVar
# by its ``(namespace, name)`` key so tests can mint per-test variables
# without touching the module-level ones above.

#: Sentinel reported by the oracle routines when the addressed variable
#: is unbound. A distinctive string (script values are picklable
#: primitives) rather than an object sentinel so it compares equal
#: across process boundaries.
ORACLE_UNSET = "<wool-oracle-unset>"

# Per-process cache backing ``_resolve_oracle_var``. A second
# ``wool.ContextVar`` declaration for an already-declared (non-stub)
# key raises ContextVarCollision, so repeated dispatches to the same
# worker must reuse the instance minted by the first resolution.
_ORACLE_VARS: dict = {}


def _resolve_oracle_var(namespace: str, name: str) -> wool.ContextVar:
    """Get-or-create the :class:`wool.ContextVar` keyed (*namespace*, *name*).

    The first resolution in a process declares the variable — promoting
    a wire-seeded stub in place when the dispatch frame registered the
    key before the routine body ran (the
    :func:`declare_and_read_unregistered_key` path) — and caches it;
    every later resolution returns the cached instance, keeping
    worker-side resolution idempotent across repeated dispatches.
    """
    key = (namespace, name)
    var = _ORACLE_VARS.get(key)
    if var is None:
        var = wool.ContextVar(name, namespace=namespace)
        _ORACLE_VARS[key] = var
    return var


async def _oracle_reset_outcome(namespace: str, name: str, token) -> tuple:
    """Reset the (*namespace*, *name*) oracle var by *token*; report the outcome.

    Shared tail for the oracle reset routines — never raises for the
    documented reset rejections. Returns ``("ok", "", value_after)`` on
    success or ``(type_name, str(error), value_after)`` when the reset
    raises RuntimeError, ValueError, or TypeError, where ``value_after``
    is the variable's current value or :data:`ORACLE_UNSET` when unbound.
    """
    var = _resolve_oracle_var(namespace, name)
    try:
        var.reset(token)
    except (RuntimeError, ValueError, TypeError) as error:
        return type(error).__name__, str(error), var.get(ORACLE_UNSET)
    return "ok", "", var.get(ORACLE_UNSET)


@wool.routine
async def oracle_set(namespace: str, name: str, value):
    """Set the (*namespace*, *name*) oracle var on the worker; return the token.

    The minted :class:`wool.Token` rides home on the response frame.
    The awaiting caller's context continues the minting context, so the
    token may later be reset caller-side — or dispatched onward and
    reset on another worker — exactly as if the set had happened in an
    awaited local coroutine.
    """
    return _resolve_oracle_var(namespace, name).set(value)


@wool.routine
async def oracle_reset(namespace: str, name: str, token) -> tuple:
    """Attempt to reset the (*namespace*, *name*) oracle var by *token*.

    Never raises transport-side. Returns
    ``(outcome_kind, message, value_after)`` where ``outcome_kind`` is
    ``"ok"``, ``"RuntimeError"``, ``"ValueError"``, or ``"TypeError"``,
    ``message`` is ``str(exception)`` (empty on success), and
    ``value_after`` is the variable's current worker-side value or
    :data:`ORACLE_UNSET` when unbound.
    """
    return await _oracle_reset_outcome(namespace, name, token)


@wool.routine
async def oracle_get(namespace: str, name: str, default=None):
    """Return the (*namespace*, *name*) oracle var's worker-side value.

    Falls back to *default* when the variable is unbound — callers that
    need to distinguish "unbound" pass a sentinel such as
    :data:`ORACLE_UNSET`.
    """
    return _resolve_oracle_var(namespace, name).get(default)


@wool.routine
async def oracle_set_report(namespace: str, name: str, value) -> tuple:
    """Set the (*namespace*, *name*) oracle var; return ``(pid, token)``.

    Attributed variant of :func:`oracle_set` — the worker's
    ``os.getpid()`` rides alongside the minted token so multi-worker
    tests can attribute each mint to the process that performed it.
    """
    return os.getpid(), _resolve_oracle_var(namespace, name).set(value)


@wool.routine
async def oracle_reset_report(namespace: str, name: str, token) -> tuple:
    """Attempt a (*namespace*, *name*) oracle reset; report with the worker pid.

    Attributed variant of :func:`oracle_reset`: returns
    ``(pid, outcome_kind, message, value_after)`` so multi-worker tests
    can attribute the reset attempt to the process that performed it.
    """
    return os.getpid(), *await _oracle_reset_outcome(namespace, name, token)


@wool.routine
async def nested_oracle_set(namespace: str, name: str, value):
    """Mint a (*namespace*, *name*) token on an inner worker via nested dispatch.

    The outer worker dispatches :func:`oracle_set` back into the pool, so
    the token is minted one hop deeper and rides home through both
    response frames — the "inner worker mints, caller resets" topology.
    """
    return await oracle_set(namespace, name, value)


@wool.routine
async def nested_oracle_set_report(namespace: str, name: str, value) -> tuple:
    """Nested-mint a (*namespace*, *name*) token; report both worker pids.

    Attributed variant of :func:`nested_oracle_set`: returns
    ``(outer_pid, inner_pid, token)`` so tests can attribute the mint to
    the inner process the outer worker dispatched to.
    """
    inner_pid, token = await oracle_set_report(namespace, name, value)
    return os.getpid(), inner_pid, token
