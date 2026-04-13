"""Test routines for integration tests.

This module defines all ``@wool.routine`` decorated functions used by the
integration test suite. It is NOT a test file — no ``test_`` prefix — so
pytest will not collect it directly.

Routines are organized by dimension:
- D1 (RoutineShape): coroutine vs async generator vs nested variants
- D8 (RoutineBinding): module function vs instance/class/static method
"""

import asyncio
import functools
import inspect

import wool

# Module-level wool.ContextVars used by the propagation integration tests.
# They live at module level so cloudpickle imports this module on the
# worker when it unpickles a routine defined here, which registers the
# vars in the worker's wool.ContextVar._registry before RuntimeContext
# restoration looks them up by name.
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


def _var(name: str) -> "wool.ContextVar":
    """Look up a wool.ContextVar by logical name at call time.

    Resolves against the module's live globals so that attribute
    substitutions performed by wool's manifest machinery on the
    worker (``sys.modules['integration.routines'].TENANT_ID = ...``)
    are visible to every caller. A module-level ``dict`` built at
    import time would retain stale references to the pre-substitution
    instances; ``globals()[name.upper()]`` re-resolves each call.
    """
    return globals()[name.upper()]


# Deprecated legacy alias. Existing call sites use ``_var(name)``.
VARS = {"tenant_id": TENANT_ID, "region": REGION, "trace_id": TRACE_ID}


def _execute_patterns(patterns, *, step=None):
    """Execute context-var mutation patterns on the worker side.

    Reads the patterns dict and mutates the appropriate wool.ContextVars.
    For PER_YIELD, the ``step`` argument controls which iteration label
    to apply.
    """
    for var_name, pattern in patterns.items():
        var = _var(var_name)
        match pattern:
            case "ROUND_TRIP":
                var.set(f"worker-mutated-{var_name}")
            case "LOCAL_RESET":
                token = var.set(f"temp-{var_name}")
                var.reset(token)
            case "PER_YIELD":
                if step is not None:
                    var.set(f"step-{step}")
            case "DOWNSTREAM_OVERWRITE":
                var.set(f"inner-overwrite-{var_name}")
            case "DOWNSTREAM_RESET":
                # Inner worker reads the token deposited by the outer
                # worker via _RESET_TOKENS and resets the var.
                tokens = _RESET_TOKENS.get()
                if var_name in tokens:
                    var.reset(tokens[var_name])
            case "UPSTREAM_RESET":
                var.set(f"inner-set-{var_name}")


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
        var = _var(var_name)
        match pattern:
            case "DOWNSTREAM_OVERWRITE":
                var.set(f"outer-set-{var_name}")
            case "DOWNSTREAM_RESET":
                token = var.set(f"outer-set-{var_name}")
                tokens_for_inner[var_name] = token
            case "UPSTREAM_RESET":
                pass  # inner will set; outer resets after return
    if tokens_for_inner:
        _RESET_TOKENS.set(tokens_for_inner)


def _post_nested_teardown(patterns):
    """Clean up after a nested dispatch returns (outer worker side).

    For UPSTREAM_RESET the outer worker resets the var that the inner
    worker set, using a token captured before the nested call.
    """
    for var_name, pattern in patterns.items():
        var = _var(var_name)
        if pattern == "UPSTREAM_RESET":
            # The inner worker set this var; the outer worker now
            # resets it back to whatever it was before.
            var.set(f"outer-reset-{var_name}")


_NESTED_PATTERN_NAMES = frozenset(
    {
        "DOWNSTREAM_OVERWRITE",
        "DOWNSTREAM_RESET",
        "UPSTREAM_RESET",
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

            nested_patterns = {
                k: v for k, v in patterns.items() if v in _NESTED_PATTERN_NAMES
            }
            non_nested = {
                k: v
                for k, v in patterns.items()
                if k != "_inner" and v not in _NESTED_PATTERN_NAMES
            }

            if nested_patterns and is_inner:
                _execute_patterns(nested_patterns)
            elif nested_patterns:
                _pre_nested_setup(nested_patterns)
                inner_patterns = dict(patterns)
                inner_patterns["_inner"] = True
                TEST_PATTERNS.set(inner_patterns)

            # Execute simple patterns (except PER_YIELD).
            non_yield_simple = {k: v for k, v in non_nested.items() if v != "PER_YIELD"}
            if non_yield_simple:
                _execute_patterns(non_yield_simple)

            per_yield = {k: v for k, v in non_nested.items() if v == "PER_YIELD"}
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

        nested_patterns = {
            k: v for k, v in patterns.items() if v in _NESTED_PATTERN_NAMES
        }
        non_nested = {
            k: v
            for k, v in patterns.items()
            if k != "_inner" and v not in _NESTED_PATTERN_NAMES
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
async def nested_add(a: int, b: int) -> int:
    """Coroutine that dispatches to ``add``, triggering nested dispatch."""
    return await add(a, b)


@wool.routine
@context_pattern_aware
async def nested_gen(n: int):
    """Async generator that yields from ``gen_range``, nested streaming."""
    async for item in gen_range(n):
        yield item


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
    propagated into the worker's RuntimeContext before the routine
    runs.
    """
    return TENANT_ID.get()


@wool.routine
@context_pattern_aware
async def stream_tenant_id(count: int):
    """Async generator that yields TENANT_ID.get() *count* times.

    A sleep(0) between yields forces the generator to suspend, so each
    subsequent read happens in the restored worker context rather than
    returning a cached value from a single frame. Used as the
    regression guard for the _stream_from_worker async-generator fix.
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
    all snapshotted and restored correctly through a single dispatch.
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
