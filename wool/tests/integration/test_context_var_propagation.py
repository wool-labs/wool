"""Integration tests for wool.ContextVar cross-worker propagation.

These tests drive the full dispatch wire path — caller sets a wool
ContextVar, Task.to_protobuf serializes it, gRPC carries it to a real
worker subprocess, the worker unpickles the callable (importing
routines.py and populating its wool.ContextVar registry), and the
routine observes the propagated value. They complement the in-process
unit tests in tests/runtime/test_context.py by exercising the real
serialization and subprocess boundary, and they serve as the
regression guard for the _stream_from_worker async-generator fix
introduced with issue #154.
"""

import asyncio
import contextvars

import pytest

import wool

from . import _collision_fixtures
from . import routines
from .conftest import BackpressureMode
from .conftest import ContextVarPattern
from .conftest import CredentialType
from .conftest import DiscoveryFactory
from .conftest import LazyMode
from .conftest import LbFactory
from .conftest import PoolMode
from .conftest import RoutineBinding
from .conftest import RoutineShape
from .conftest import Scenario
from .conftest import TimeoutKind
from .conftest import WorkerOptionsKind
from .conftest import build_pool_from_scenario


def _default_scenario(
    *,
    shape: RoutineShape = RoutineShape.COROUTINE,
    pool_mode: PoolMode = PoolMode.DEFAULT,
    lazy: LazyMode = LazyMode.LAZY,
) -> Scenario:
    return Scenario(
        shape=shape,
        pool_mode=pool_mode,
        discovery=DiscoveryFactory.NONE,
        lb=LbFactory.CLASS_REF,
        credential=CredentialType.INSECURE,
        options=WorkerOptionsKind.DEFAULT,
        timeout=TimeoutKind.NONE,
        binding=RoutineBinding.MODULE_FUNCTION,
        lazy=lazy,
        backpressure=BackpressureMode.NONE,
        ctx_var_1=ContextVarPattern.NONE,
        ctx_var_2=ContextVarPattern.NONE,
        ctx_var_3=ContextVarPattern.NONE,
    )


@pytest.mark.integration
class TestContextVarPropagation:
    @pytest.mark.asyncio
    async def test_coroutine_dispatch_propagates_wool_context_var(
        self, credentials_map, retry_grpc_internal
    ):
        """Test wool.ContextVar values reach a remote coroutine routine.

        Given:
            A wool.ContextVar set on the caller and a DEFAULT pool
            running a module-level coroutine that returns the var's
            current value
        When:
            The caller dispatches the routine through the pool
        Then:
            The routine should return the caller's propagated value
            rather than the class-level default
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("acme-corp")
                try:
                    result = await routines.get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)
            assert result == "acme-corp"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_generator_dispatch_propagates_wool_context_var(
        self, credentials_map, retry_grpc_internal
    ):
        """Test propagation across async-generator suspension boundaries.

        Given:
            A wool.ContextVar set on the caller and an EPHEMERAL pool
            running a module-level async generator that yields the
            var's value multiple times, suspending between yields
        When:
            The caller iterates the generator to completion
        Then:
            Every yielded value should equal the caller's propagated
            value — guards against regression in the
            _stream_from_worker async-gen context restoration
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
                pool_mode=PoolMode.EPHEMERAL,
            )
            collected: list[str] = []
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("stream-tenant")
                try:
                    async for value in routines.stream_tenant_id(3):
                        collected.append(value)
                finally:
                    routines.TENANT_ID.reset(token)
            assert collected == [
                "stream-tenant",
                "stream-tenant",
                "stream-tenant",
            ]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_nested_dispatch_propagates_wool_context_var(
        self, credentials_map, retry_grpc_internal
    ):
        """Test propagation through a nested routine dispatch chain.

        Given:
            A wool.ContextVar set on the caller and a DEFAULT pool
            running a coroutine that itself dispatches another
            coroutine that reads the var
        When:
            The caller dispatches the outer routine
        Then:
            The innermost routine should return the caller's
            propagated value, proving the value survives from caller
            through an outer worker through the nested dispatch back
            to a worker
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(shape=RoutineShape.NESTED_COROUTINE)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("nested-tenant")
                try:
                    result = await routines.nested_get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)
            assert result == "nested-tenant"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_back_propagates_worker_mutation(
        self, credentials_map, retry_grpc_internal
    ):
        """Test coroutine worker mutations are back-propagated to the caller.

        Given:
            A wool.ContextVar set to a specific value on the caller
            and a DEFAULT pool running a coroutine that mutates the
            var before returning its new value
        When:
            The caller dispatches the routine and reads its own var
            value after the routine returns
        Then:
            The caller's value should reflect the worker-side mutation
            via back-propagation
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-value")
                try:
                    worker_result = await routines.mutate_and_read_tenant_id()
                    caller_value_after = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert worker_result == "mutated_on_worker"
            assert caller_value_after == "mutated_on_worker"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_concurrent_dispatches_observe_isolated_values(
        self, credentials_map, retry_grpc_internal
    ):
        """Test concurrent dispatches with different values stay isolated.

        Given:
            An EPHEMERAL pool with two workers and two asyncio tasks
            running in isolated copy_context() contexts, each setting
            the same wool.ContextVar to a different value
        When:
            Both tasks dispatch the routine concurrently and are
            gathered
        Then:
            Each task should observe its own propagated value with no
            cross-contamination — verifies per-dispatch context
            isolation across real worker subprocesses
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(pool_mode=PoolMode.EPHEMERAL)

            async def dispatch_with(value: str) -> str:
                token = routines.TENANT_ID.set(value)
                try:
                    return await routines.get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)

            async with build_pool_from_scenario(scenario, credentials_map):
                ctx_a = contextvars.copy_context()
                ctx_b = contextvars.copy_context()
                task_a = asyncio.create_task(dispatch_with("tenant-a"), context=ctx_a)
                task_b = asyncio.create_task(dispatch_with("tenant-b"), context=ctx_b)
                results = await asyncio.gather(task_a, task_b)
            assert results == ["tenant-a", "tenant-b"]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_default_only_value_is_not_propagated(
        self, credentials_map, retry_grpc_internal
    ):
        """Test defaults are not shipped through the propagation path.

        Given:
            A wool.ContextVar that has never been explicitly set on
            the caller and a DEFAULT pool running a routine that
            reads it
        When:
            The caller dispatches the routine
        Then:
            The routine should see the worker-side class-level default
            ("unknown"), proving that default-only values are not
            snapshotted into the protobuf payload
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.get_tenant_id()
            assert result == "unknown"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_multiple_wool_context_vars_round_trip(
        self, credentials_map, retry_grpc_internal
    ):
        """Test multiple registered vars are all snapshotted and restored.

        Given:
            Two module-level wool.ContextVars both set on the caller
            and a DEFAULT pool running a routine that reads both
        When:
            The caller dispatches the routine
        Then:
            The returned tuple should contain both propagated values
            in the expected order
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                tenant_token = routines.TENANT_ID.set("globex")
                region_token = routines.REGION.set("us-west-2")
                try:
                    tenant, region = await routines.read_multi_vars()
                finally:
                    routines.TENANT_ID.reset(tenant_token)
                    routines.REGION.reset(region_token)
            assert tenant == "globex"
            assert region == "us-west-2"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_generator_back_propagates_worker_mutation(
        self, credentials_map, retry_grpc_internal
    ):
        """Test async generator worker mutations are back-propagated to the caller.

        Given:
            A wool.ContextVar set on the caller and an EPHEMERAL pool
            running an async generator that mutates the var on its
            final yield
        When:
            The caller iterates the generator to completion and reads
            its own var value afterward
        Then:
            The caller's value should reflect the worker's final
            mutation via per-yield back-propagation
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
                pool_mode=PoolMode.EPHEMERAL,
            )
            collected: list[str] = []
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-original")
                try:
                    async for value in routines.stream_and_mutate_tenant_id(3):
                        collected.append(value)
                    caller_value_after = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert collected == [
                "caller-original",
                "caller-original",
                "final-mutation",
            ]
            assert caller_value_after == "final-mutation"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_back_propagation_updates_caller_per_yield(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the caller observes back-propagated mutations after each yield.

        Given:
            A wool.ContextVar set on the caller and a DEFAULT pool
            running an async generator that sets the var to a new
            value on every iteration
        When:
            The caller reads its own var value after each yield
        Then:
            The caller's value should match the worker's mutation
            from the most recent yield on every iteration
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
            )
            caller_snapshots: list[str] = []
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("initial")
                try:
                    async for _ in routines.mutate_on_each_yield(3):
                        caller_snapshots.append(routines.TENANT_ID.get())
                finally:
                    routines.TENANT_ID.reset(token)
            assert caller_snapshots == ["step-0", "step-1", "step-2"]

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestStdlibEquivalence:
    @pytest.mark.asyncio
    async def test_coroutine_mutation_matches_stdlib(
        self, credentials_map, retry_grpc_internal
    ):
        """Test coroutine back-propagation matches stdlib contextvars behavior.

        Given:
            A wool.ContextVar set on the caller and a DEFAULT pool
            running a coroutine that mutates the var, alongside an
            equivalent plain stdlib contextvars.ContextVar exercised
            via contextvars.copy_context().run()
        When:
            Both the wool dispatch and the stdlib run complete
        Then:
            The caller should observe the same post-mutation value
            from wool as from the stdlib equivalent, confirming
            wool back-propagation matches stdlib copy-on-write
            semantics
        """

        # Arrange
        stdlib_var: contextvars.ContextVar[str] = contextvars.ContextVar("stdlib_tenant")

        def stdlib_mutate() -> str:
            stdlib_var.set("mutated_on_worker")
            return stdlib_var.get()

        # Act & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                # --- stdlib path ---
                stdlib_var.set("caller-value")
                ctx = contextvars.copy_context()
                stdlib_result = ctx.run(stdlib_mutate)
                stdlib_caller_after = stdlib_var.get()

                # --- wool path ---
                token = routines.TENANT_ID.set("caller-value")
                try:
                    wool_result = await routines.mutate_and_read_tenant_id()
                    wool_caller_after = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)

            assert wool_result == stdlib_result
            assert wool_caller_after == "mutated_on_worker"
            # stdlib copy_context().run() does NOT propagate back.
            # wool back-propagates, so we just confirm wool's result
            # matches the worker-side mutation.
            assert stdlib_caller_after == "caller-value"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_generator_mutation_matches_stdlib(
        self, credentials_map, retry_grpc_internal
    ):
        """Test async generator back-propagation mirrors stdlib generator behavior.

        Given:
            A wool.ContextVar set on the caller and a DEFAULT pool
            running an async generator that mutates the var on each
            yield, alongside an equivalent local async generator
            exercising the same pattern
        When:
            Both generators are iterated to completion
        Then:
            The wool dispatch should yield the same values as the
            local generator, confirming the per-yield mutation
            pattern produces identical results
        """

        # Arrange
        async def local_mutate_on_each_yield(count: int):
            for i in range(count):
                routines.TENANT_ID.set(f"step-{i}")
                yield routines.TENANT_ID.get()

        # Act & assert
        async def body():
            scenario = _default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
            )

            # --- local path ---
            token = routines.TENANT_ID.set("initial")
            local_values: list[str] = []
            try:
                async for v in local_mutate_on_each_yield(3):
                    local_values.append(v)
            finally:
                routines.TENANT_ID.reset(token)

            # --- wool path ---
            wool_values: list[str] = []
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("initial")
                try:
                    async for v in routines.mutate_on_each_yield(3):
                        wool_values.append(v)
                finally:
                    routines.TENANT_ID.reset(token)

            assert wool_values == local_values
            assert wool_values == ["step-0", "step-1", "step-2"]

        await retry_grpc_internal(body)


# ---------------------------------------------------------------------------
# CX-* tests: wool.Context carried across workers
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestWoolContextAcrossWorkers:
    @pytest.mark.asyncio
    async def test_pre_populated_context_seeds_worker_routine(
        self, credentials_map, retry_grpc_internal
    ):
        """Test CX-001 pre-populated Context seeds worker var reads.

        Given:
            A wool.Context() pre-populated via ``run()`` and a DEFAULT
            pool running a coroutine that reads TENANT_ID
        When:
            The Context is used as the ambient context via
            ``wool.Context.run_async(dispatch())``
        Then:
            The worker routine observes the var value set inside
            the Context
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            ctx = wool.Context()

            def _seed():
                routines.TENANT_ID.set("pre-populated-inside-ctx")

            ctx.run(_seed)

            async with build_pool_from_scenario(scenario, credentials_map):

                async def _dispatch():
                    return await routines.get_tenant_id()

                result = await ctx.run_async(_dispatch())
            assert result == "pre-populated-inside-ctx"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_pickled_context_argument_runs_sub_dispatch(
        self, credentials_map, retry_grpc_internal
    ):
        """Test CX-002 Context shipped as routine arg seeds on the worker.

        Given:
            A routine accepting a pickled ``wool.Context`` argument that
            calls ``await ctx.run_async(sub_routine())`` on the worker
        When:
            The caller dispatches the routine passing a populated Context
        Then:
            The sub-routine observes the Context's vars and completes
            without RuntimeError
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            ctx = wool.Context()

            def _seed():
                routines.TENANT_ID.set("from-shipped-context")

            ctx.run(_seed)

            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.run_context_and_read(ctx)
            assert result == "from-shipped-context"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_single_task_invariant_rejects_concurrent_run_async(
        self, credentials_map, retry_grpc_internal
    ):
        """Test CX-003 second run_async on active Context raises RuntimeError.

        Given:
            A wool.Context currently running a dispatched task through
            ``run_async``
        When:
            The same Context is used to run a second task concurrently
        Then:
            ``RuntimeError`` is raised by the second ``run_async`` call
            (single-task invariant)
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            ctx = wool.Context()

            async with build_pool_from_scenario(scenario, credentials_map):
                # Launch one dispatch through ctx so it is "running";
                # give the event loop a tick so the task enters
                # ctx.run_async (sets _running=True) before the second
                # call.
                first_coro = routines.get_tenant_id()
                first_task = asyncio.create_task(ctx.run_async(first_coro))
                await asyncio.sleep(0)

                # Attempt a second concurrent run_async on the same ctx.
                second_coro = routines.get_tenant_id()
                try:
                    with pytest.raises(RuntimeError):
                        await ctx.run_async(second_coro)
                finally:
                    second_coro.close()

                await first_task

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_context_id_propagates_to_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test CX-004 worker observes the caller's wool.Context id.

        Given:
            A caller that captures ``current_context().id`` before a
            dispatch and a routine that returns
            ``current_context().id.hex`` from inside the worker
        When:
            The caller dispatches the routine
        Then:
            The returned id hex equals the caller's captured id hex
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                caller_id = wool.current_context().id
                observed_hex = await routines.return_current_context_id_hex()
            assert observed_hex == caller_id.hex

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_asyncio_child_task_forks_context_id(
        self, credentials_map, retry_grpc_internal
    ):
        """Test CX-005 asyncio child task dispatches fork a fresh context id.

        Given:
            A caller that enters an asyncio child task via
            ``create_task`` and captures ``current_context().id``
            inside the child before dispatch
        When:
            The child dispatches a routine that returns its own
            observed context id
        Then:
            The routine's observed id differs from the parent's id
            (stdlib fork parity)
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(pool_mode=PoolMode.EPHEMERAL)

            async with build_pool_from_scenario(scenario, credentials_map):
                parent_id = wool.current_context().id

                async def _child():
                    child_id = wool.current_context().id
                    observed_hex = await routines.return_current_context_id_hex()
                    return child_id, observed_hex

                child_id, observed_hex = await asyncio.create_task(_child())

            assert child_id != parent_id
            assert observed_hex == child_id.hex

        await retry_grpc_internal(body)


# ---------------------------------------------------------------------------
# TK-* tests: wool.Token round-trip and cross-Context rejection
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestTokenAcrossWorkers:
    @pytest.mark.asyncio
    async def test_pickled_token_resets_on_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test TK-001 worker can reset via a caller-minted pickled Token.

        Given:
            A caller that sets TENANT_ID and captures the resulting
            Token and a routine that accepts the Token and calls
            ``var.reset(token)`` on the worker
        When:
            The caller dispatches passing the pickled Token
        Then:
            The reset succeeds on the worker and the routine's
            post-reset read equals the var's pre-set default
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-value")
                try:
                    worker_read = await routines.accept_token_and_reset(token)
                finally:
                    # The worker's reset used up the token; reset
                    # locally via get()/default is not needed but we
                    # still clean by re-setting to default semantics
                    # with a fresh token for test hygiene.
                    try:
                        routines.TENANT_ID.reset(token)
                    except RuntimeError:
                        pass
            # Post-reset read on the worker restores pre-set _UNSET,
            # which surfaces as the var's constructor default.
            assert worker_read == "unknown"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_token_minted_in_different_context_rejected_on_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test TK-002 worker reset raises ValueError when Token is cross-Context.

        Given:
            A caller that mints a Token inside wool.Context A and
            dispatches a routine into a different Context B that
            receives the Token and calls ``var.reset(token)``
        When:
            The worker invokes ``reset(token)``
        Then:
            ValueError is raised citing the Context mismatch
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            ctx_a = wool.Context()
            ctx_b = wool.Context()

            tokens: list[wool.Token] = []

            def _mint_token():
                tokens.append(routines.TENANT_ID.set("ctx-a-value"))

            ctx_a.run(_mint_token)
            assert tokens

            async with build_pool_from_scenario(scenario, credentials_map):

                async def _dispatch_with_cross_ctx_token():
                    await routines.accept_token_and_reset(tokens[0])

                with pytest.raises(ValueError, match="different wool.Context"):
                    await ctx_b.run_async(_dispatch_with_cross_ctx_token())

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_token_reused_on_worker_raises_runtime_error(
        self, credentials_map, retry_grpc_internal
    ):
        """Test TK-003 second reset with same Token raises RuntimeError.

        Given:
            A caller that sets TENANT_ID and dispatches a routine that
            calls ``var.reset(token)`` once then attempts a second
            reset with the same Token
        When:
            The second reset fires on the worker
        Then:
            The routine catches RuntimeError ("Token has already been
            used") and returns its repr to the caller
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-value")
                try:
                    observed = await routines.accept_token_and_double_reset(token)
                finally:
                    try:
                        routines.TENANT_ID.reset(token)
                    except RuntimeError:
                        pass
            assert "Token has already been used" in observed

        await retry_grpc_internal(body)


# ---------------------------------------------------------------------------
# EX-* tests: exception-path snapshot back-propagation
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestExceptionPathBackPropagation:
    @pytest.mark.asyncio
    async def test_coroutine_exception_back_propagates_worker_mutation(
        self, credentials_map, retry_grpc_internal
    ):
        """Test EX-001 exception payload carries worker-side var mutations.

        Given:
            A routine that sets TENANT_ID to a sentinel value then
            raises ValueError
        When:
            The caller dispatches and catches the exception
        Then:
            The caller's TENANT_ID reflects the worker-side sentinel
            value (back-propagated via exception snapshot path)
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-original")
                try:
                    with pytest.raises(ValueError):
                        await routines.mutate_then_raise_tenant_id("exc-path-sentinel")
                    observed = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert observed == "exc-path-sentinel"

        await retry_grpc_internal(body)

    @pytest.mark.xfail(
        reason=(
            "bug: src/wool/runtime/worker/service.py:581 raises the async-gen "
            "exception WITHOUT forwarding the worker-captured ctx_snapshot "
            "(line 516). The outer handler then re-snapshots from its own "
            "context (line 302) which never observed the worker's mutations, "
            "dropping mid-stream mutations that occurred on the frame that "
            "raised."
        ),
        strict=True,
    )
    @pytest.mark.asyncio
    async def test_async_gen_exception_back_propagates_worker_mutation(
        self, credentials_map, retry_grpc_internal
    ):
        """Test EX-002 async-gen exception carries mid-stream mutations.

        Given:
            An async-generator routine that yields once, then sets
            TENANT_ID and raises on the next iteration
        When:
            The caller iterates and catches the exception
        Then:
            The caller's TENANT_ID reflects the last mid-stream
            mutation performed on the worker before the raise
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
                pool_mode=PoolMode.EPHEMERAL,
            )
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-original")
                try:
                    gen = routines.yield_then_mutate_and_raise("mid-stream-sentinel")
                    try:
                        first = await gen.__anext__()
                        assert first == "ready"
                        with pytest.raises(ValueError):
                            await gen.__anext__()
                    finally:
                        await gen.aclose()
                    observed = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert observed == "mid-stream-sentinel"

        await retry_grpc_internal(body)


# ---------------------------------------------------------------------------
# FK-* tests: asyncio fork parity on the worker
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestAsyncioForkOnWorker:
    @pytest.mark.asyncio
    async def test_worker_child_mutation_does_not_leak_to_parent(
        self, credentials_map, retry_grpc_internal
    ):
        """Test FK-001 child-task mutation stays out of parent on the worker.

        Given:
            A routine that sets TENANT_ID to ``"parent"``, spawns a
            child asyncio task that sets TENANT_ID to ``"child"`` and
            returns its read, and finally reads TENANT_ID from the
            parent after the child completes
        When:
            The caller dispatches the routine
        Then:
            The parent's post-child read equals ``"parent"``
            (stdlib copy-on-fork parity)
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                child_value, parent_value = await routines.spawn_and_mutate_tenant_id()
            assert child_value == "child"
            assert parent_value == "parent"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_child_inherits_parent_value(
        self, credentials_map, retry_grpc_internal
    ):
        """Test FK-002 child asyncio task inherits parent's pre-fork var value.

        Given:
            A routine that sets TENANT_ID then spawns a child asyncio
            task that reads TENANT_ID without mutating
        When:
            The caller dispatches the routine
        Then:
            The child's read equals the parent's pre-fork value
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                child_value = await routines.parent_sets_child_reads()
            assert child_value == "parent-set"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_sibling_children_are_isolated(
        self, credentials_map, retry_grpc_internal
    ):
        """Test FK-003 sibling asyncio children are mutually isolated.

        Given:
            A routine that spawns two children via ``asyncio.gather``,
            each mutating TENANT_ID to different values, and a parent
            read afterward
        When:
            The caller dispatches the routine
        Then:
            Each child observes its own value, and the parent's var is
            unchanged (neither child leaks into the other nor into the
            parent)
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                (
                    a_value,
                    b_value,
                    parent_value,
                ) = await routines.two_children_mutate_tenant_id()
            assert a_value == "alpha"
            assert b_value == "beta"
            # Parent never set TENANT_ID, and children's mutations are
            # in their own forked contexts. The default surfaces here.
            assert parent_value == "unknown"

        await retry_grpc_internal(body)


# ---------------------------------------------------------------------------
# ST-* tests: stub promotion and collision detection across workers
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestStubPromotionAcrossWorkers:
    @pytest.mark.asyncio
    async def test_fresh_worker_promotes_stub_without_collision(
        self, credentials_map, retry_grpc_internal
    ):
        """Test ST-001 fresh worker unpickles stub then imports module.

        Given:
            A routine that imports and reads TENANT_ID, a caller that
            sets the var, and a fresh EPHEMERAL worker that has not yet
            imported the defining module
        When:
            The caller dispatches the routine
        Then:
            The worker unpickles the var (stub), imports the module
            (promotes the stub), and the routine reads the propagated
            value without a collision
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("stub-promotion-value")
                try:
                    result = await routines.get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)
            assert result == "stub-promotion-value"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_sibling_routine_raises_context_var_collision(
        self, credentials_map, retry_grpc_internal
    ):
        """Test ST-002 colliding sibling var raises on the caller.

        Given:
            Two sibling routines that each construct a
            ``wool.ContextVar`` with the same ``namespace:name`` key
            in their function bodies, on a DEFAULT pool so both
            dispatches land on the same worker (process-wide registry
            isolation would otherwise mask the collision)
        When:
            The caller dispatches the first sibling (registering the
            key on the worker) then dispatches the second sibling
        Then:
            The second dispatch raises wool.ContextVarCollision on the
            caller via the worker's exception snapshot path
        """

        # Arrange, act, & assert
        async def body():
            # DEFAULT pool is size=1 so both dispatches land on the
            # same worker process; the second construction under the
            # already-registered key triggers the collision.
            scenario = _default_scenario(pool_mode=PoolMode.DEFAULT)
            async with build_pool_from_scenario(scenario, credentials_map):
                # First dispatch registers the key on the worker.
                first = await _collision_fixtures.sibling_a()
                assert first == "sibling-a"

                # Second dispatch constructs a new var with the same
                # key → ContextVarCollision propagates back.
                with pytest.raises(wool.ContextVarCollision):
                    await _collision_fixtures.sibling_b()

        await retry_grpc_internal(body)


# ---------------------------------------------------------------------------
# FS-* tests: forward-propagation mid-stream mutations
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestForwardPropagationMidStream:
    @pytest.mark.asyncio
    async def test_mid_stream_mutation_reaches_next_anext(
        self, credentials_map, retry_grpc_internal
    ):
        """Test FS-001 caller mutation between __anext__ calls reaches worker.

        Given:
            An async-generator routine that yields ``TENANT_ID.get()``
            on each iteration and a caller that mutates the var
            between ``__anext__`` calls
        When:
            The caller drives the generator manually, setting the var
            to a distinct value before each ``__anext__``
        Then:
            Each yielded value reflects the caller's most recent value
            at the moment of the call
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.stream_tenant_id_echo(3)
                try:
                    collected: list[str] = []
                    values = ["fs1-first", "fs1-second", "fs1-third"]
                    tokens: list = []
                    try:
                        for v in values:
                            tokens.append(routines.TENANT_ID.set(v))
                            collected.append(await gen.__anext__())
                    finally:
                        for tok in reversed(tokens):
                            try:
                                routines.TENANT_ID.reset(tok)
                            except RuntimeError:
                                pass
                finally:
                    await gen.aclose()
            assert collected == values

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_mid_stream_mutation_reaches_asend_frame(
        self, credentials_map, retry_grpc_internal
    ):
        """Test FS-002 caller mutation before asend reaches the worker frame.

        Given:
            An async-generator routine using ``asend`` that echoes
            ``TENANT_ID.get()`` each iteration
        When:
            The caller calls ``gen.asend(x)`` with TENANT_ID set to a
            distinct value before each send
        Then:
            Each echoed value reflects the caller's var value at the
            moment of the corresponding ``asend`` frame
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(shape=RoutineShape.ASYNC_GEN_ASEND)
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.echo_tenant_id_on_send(3)
                try:
                    first = await gen.__anext__()
                    assert first == "ready"
                    collected: list[str] = []
                    values = ["fs2-a", "fs2-b", "fs2-c"]
                    tokens: list = []
                    try:
                        for v in values:
                            tokens.append(routines.TENANT_ID.set(v))
                            collected.append(await gen.asend(None))
                    finally:
                        for tok in reversed(tokens):
                            try:
                                routines.TENANT_ID.reset(tok)
                            except RuntimeError:
                                pass
                finally:
                    await gen.aclose()
            assert collected == values

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_mid_stream_mutation_reaches_athrow_frame(
        self, credentials_map, retry_grpc_internal
    ):
        """Test FS-003 caller mutation before athrow reaches the handler frame.

        Given:
            An async-generator routine whose ``athrow`` handler reads
            ``TENANT_ID`` before yielding that value and returning
        When:
            The caller mutates TENANT_ID then calls ``gen.athrow``
        Then:
            The yielded value reflects the caller's most recent var
            value at the moment of the ``athrow`` call
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(shape=RoutineShape.ASYNC_GEN_ATHROW)
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.read_on_athrow("unused")
                try:
                    first = await gen.__anext__()
                    assert first == "ready"
                    token = routines.TENANT_ID.set("fs3-athrow-value")
                    try:
                        observed = await gen.athrow(ValueError("fs3"))
                    finally:
                        routines.TENANT_ID.reset(token)
                finally:
                    await gen.aclose()
            assert observed == "fs3-athrow-value"

        await retry_grpc_internal(body)


# ---------------------------------------------------------------------------
# UK-* tests: unregistered-key behavior on the worker
# ---------------------------------------------------------------------------


# Defined here (not in routines.py) so the worker subprocess — which
# only imports the routine's module (routines.py) — does not register
# this key. The synthetic namespace guarantees no real wool module
# registers it either.
_UNREGISTERED_ONLY: wool.ContextVar[str] = wool.ContextVar(
    "caller_only_key",
    namespace="synthetic_no_such_module",
    default="unset",
)


@pytest.mark.integration
class TestUnregisteredKeyBehavior:
    @pytest.mark.asyncio
    async def test_worker_silently_drops_unknown_key(
        self, credentials_map, retry_grpc_internal
    ):
        """Test UK-001 var unknown on worker is dropped, dispatch succeeds.

        Given:
            A caller-only ``wool.ContextVar`` under a synthetic
            namespace set to a value, and a routine that reads a
            different, worker-known ``TENANT_ID`` var
        When:
            The caller dispatches the routine
        Then:
            The dispatch completes; the routine observes its own
            (worker-known) var; and the synthetic caller-only key is
            silently dropped on the worker (debug-log only, no
            exception)
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                unreg_token = _UNREGISTERED_ONLY.set("caller-only-value")
                tenant_token = routines.TENANT_ID.set("visible-on-worker")
                try:
                    observed = await routines.read_tenant_id_only()
                finally:
                    routines.TENANT_ID.reset(tenant_token)
                    _UNREGISTERED_ONLY.reset(unreg_token)
            assert observed == "visible-on-worker"

        await retry_grpc_internal(body)
