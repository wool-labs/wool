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
import warnings

import pytest

import wool
from wool.runtime.context import attached
from wool.runtime.context import dispatch_timeout

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
    async def test_coroutine_mutation_is_visible_in_return_value(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a coroutine routine's mutation is readable via its own return value.

        Given:
            A wool.ContextVar set on the caller and a DEFAULT pool
            running a coroutine that mutates the var and returns the
            new value
        When:
            The caller dispatches the routine and awaits its return
            value
        Then:
            The returned value should equal the worker-side mutation
            — the routine observes the var it just set
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-value")
                try:
                    worker_result = await routines.mutate_and_read_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)
            assert worker_result == "mutated_on_worker"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_mutation_back_propagates_to_caller(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a coroutine routine's mutation reaches the caller after dispatch.

        Given:
            A wool.ContextVar set on the caller and a DEFAULT pool
            running a coroutine that mutates the var before returning
        When:
            The caller dispatches the routine and reads its own var
            value after the routine returns
        Then:
            The caller's value should equal the worker-side mutation
            — back-propagation applies the routine's change to the
            caller's Context
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-value")
                try:
                    await routines.mutate_and_read_tenant_id()
                    caller_value_after = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert caller_value_after == "mutated_on_worker"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_concurrent_dispatches_observe_isolated_values(
        self, credentials_map, retry_grpc_internal
    ):
        """Test concurrent dispatches with different values stay isolated.

        Given:
            An EPHEMERAL pool with two workers and two asyncio tasks
            each setting the same wool.ContextVar to a different value
        When:
            Both tasks dispatch the routine concurrently and are
            gathered
        Then:
            Each task should observe its own propagated value with no
            cross-contamination — the wool task factory's copy-on-fork
            isolates the two dispatches end-to-end across real worker
            subprocesses.
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(pool_mode=PoolMode.EPHEMERAL)
            unset_marker = "UNSET"

            async def dispatch_with(value: str) -> str:
                token = routines.TENANT_ID.set(value)
                try:
                    return await routines.get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)

            async with build_pool_from_scenario(scenario, credentials_map):
                parent_initial = routines.TENANT_ID.get(unset_marker)
                task_a = asyncio.create_task(dispatch_with("tenant-a"))
                task_b = asyncio.create_task(dispatch_with("tenant-b"))
                results = await asyncio.gather(task_a, task_b)
                parent_final = routines.TENANT_ID.get(unset_marker)
            assert results == ["tenant-a", "tenant-b"]
            assert parent_final == parent_initial

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
    async def test_async_generator_mutation_is_visible_in_yields(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an async generator routine's mutations appear in yielded values.

        Given:
            A wool.ContextVar set on the caller and an EPHEMERAL pool
            running an async generator that reads the var on each
            yield and mutates it on the final yield
        When:
            The caller collects every value the generator yields
        Then:
            The yielded sequence should interleave the caller-set
            value with the routine's final mutation — per-yield
            var state is visible in the return channel
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
                finally:
                    routines.TENANT_ID.reset(token)
            assert collected == [
                "caller-original",
                "caller-original",
                "final-mutation",
            ]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_generator_mutation_back_propagates_to_caller(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an async generator's mutation reaches the caller after exhaustion.

        Given:
            A wool.ContextVar set on the caller and an EPHEMERAL pool
            running an async generator that mutates the var on its
            final yield
        When:
            The caller iterates the generator to completion and reads
            its own var value afterward
        Then:
            The caller's value should equal the routine's final
            mutation — back-propagation applies the final yield's
            change to the caller's Context
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
                    async for _ in routines.stream_and_mutate_tenant_id(3):
                        pass
                    caller_value_after = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
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

    @pytest.mark.asyncio
    async def test_async_generator_dispatch_matches_in_process_baseline(
        self, credentials_map, retry_grpc_internal
    ):
        """Test remote async-generator per-yield mutations match the in-process baseline.

        Given:
            A wool.ContextVar, a DEFAULT pool running an async
            generator that mutates the var on each yield, and a
            local async generator that exercises the same pattern
            in-process (both using wool.ContextVar — this is a wool
            self-baseline, not a stdlib comparison; stdlib generators
            share the task context with their caller rather than
            owning an isolated worker context)
        When:
            Both generators are iterated to completion
        Then:
            The wool dispatch yields the same values as the local
            generator, confirming the per-yield mutation pattern
            round-trips through the remote dispatch pipeline
            without clobbering intermediate yields
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

            token = routines.TENANT_ID.set("initial")
            local_values: list[str] = []
            try:
                async for v in local_mutate_on_each_yield(3):
                    local_values.append(v)
            finally:
                routines.TENANT_ID.reset(token)

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


@pytest.mark.integration
class TestStdlibEquivalence:
    @pytest.mark.asyncio
    async def test_coroutine_mutation_matches_stdlib(
        self, credentials_map, retry_grpc_internal
    ):
        """Test coroutine back-propagation diverges from stdlib copy-on-write.

        Given:
            A wool.ContextVar set on the caller and a DEFAULT pool
            running a coroutine that mutates the var, alongside an
            equivalent plain stdlib contextvars.ContextVar exercised
            via contextvars.copy_context().run()
        When:
            Both the wool dispatch and the stdlib run complete
        Then:
            Both paths return the same worker-side mutation result,
            but only wool back-propagates the mutation to the
            caller — stdlib's copy_context().run() leaves the
            caller-side var untouched, while wool's dispatch causes
            the caller to observe the worker's set value
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
                # — stdlib path —
                stdlib_var.set("caller-value")
                ctx = contextvars.copy_context()
                stdlib_result = ctx.run(stdlib_mutate)
                stdlib_caller_after = stdlib_var.get()

                # — wool path —
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


@pytest.mark.integration
class TestWoolContextAcrossWorkers:
    @pytest.mark.asyncio
    async def test_caller_context_id_propagates_to_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test worker observes the caller's wool.Context id.

        Given:
            A caller that captures ``current_context().id`` before a
            dispatch and a routine that returns
            ``current_context().id.hex`` from inside the worker
        When:
            The caller dispatches the routine
        Then:
            It should return the same id hex as the caller's
            captured id hex
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
        """Test asyncio child task dispatches fork a fresh context id.

        Given:
            A caller that enters an asyncio child task via
            ``create_task`` and captures ``current_context().id``
            inside the child before dispatch
        When:
            The child dispatches a routine that returns its own
            observed context id
        Then:
            The routine should observe a different id from the
            parent's id (stdlib fork parity)
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

    @pytest.mark.asyncio
    async def test_seeded_context_dispatch_propagates_var_bindings(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a Context pre-populated with var bindings ships those
        bindings to the worker when the dispatch runs under it.

        Given:
            A freshly-constructed ``wool.Context`` (distinct from the
            implicit current Context) populated with a TENANT_ID
            binding via ``Context.run``, and a routine that returns
            the var's observed value
        When:
            The caller invokes the dispatch inside a
            ``with attached(seed):`` block
        Then:
            The routine should return the seed value — the wire
            snapshot picks up the seeded binding from the active
            Context regardless of whether that Context was the
            implicit current one or an explicitly constructed peer
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                seed = wool.Context()
                seed.run(lambda: routines.TENANT_ID.set("seed-value"))

                with attached(seed):
                    result = await routines.get_tenant_id()
            assert result == "seed-value"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestTokenAcrossWorkers:
    @pytest.mark.asyncio
    async def test_pickled_token_resets_on_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test worker can reset via a caller-minted pickled Token.

        Given:
            A caller that sets TENANT_ID and captures the resulting
            Token and a routine that accepts the Token and calls
            ``var.reset(token)`` on the worker
        When:
            The caller dispatches passing the pickled Token
        Then:
            The reset should succeed on the worker and the routine's
            post-reset read should equal the var's pre-set default
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-value")
                try:
                    worker_read = await routines.accept_token_and_reset(token)
                finally:
                    # The worker's reset may have consumed the local
                    # token via back-propagation; only reset locally
                    # if the token wasn't already used.
                    if not token.used:
                        routines.TENANT_ID.reset(token)
            # Post-reset read on the worker restores pre-set Undefined,
            # which surfaces as the var's constructor default.
            assert worker_read == "unknown"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_token_reused_on_worker_raises_runtime_error(
        self, credentials_map, retry_grpc_internal
    ):
        """Test second reset with same Token raises RuntimeError.

        Given:
            A caller that sets TENANT_ID and dispatches a routine that
            calls ``var.reset(token)`` once then attempts a second
            reset with the same Token
        When:
            The second reset fires on the worker
        Then:
            The routine should catch RuntimeError ("Token has already
            been used") and return its repr to the caller
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-value")
                try:
                    observed = await routines.accept_token_and_double_reset(token)
                finally:
                    if not token.used:
                        routines.TENANT_ID.reset(token)
            assert "Token has already been used" in observed

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_reset_after_worker_consumption_raises(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a caller reset of a worker-consumed Token raises.

        Given:
            A caller that sets TENANT_ID to "X", dispatches a routine
            that consumes the Token via var.reset(token) on the
            worker, and then sets TENANT_ID to "Y" after the dispatch
            returns
        When:
            The caller invokes var.reset(token) a second time locally
            — the worker already consumed the Token, and the
            caller has a later set that must not be silently
            reverted
        Then:
            The second reset should raise RuntimeError (Token is
            logically single-use across processes, not just
            in-process) and the caller's post-set value "Y" must
            remain intact
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("X")
                # Worker consumes the Token via var.reset(token).
                await routines.accept_token_and_reset(token)
                # Caller installs a fresh value AFTER the worker
                # consumed the Token. A correct implementation must
                # reject the caller's second reset and preserve "Y".
                y_token = routines.TENANT_ID.set("Y")
                try:
                    with pytest.raises(RuntimeError, match="already been used"):
                        routines.TENANT_ID.reset(token)
                    assert routines.TENANT_ID.get() == "Y"
                finally:
                    routines.TENANT_ID.reset(y_token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_reset_after_async_gen_consumed_token_raises(
        self, credentials_map, retry_grpc_internal
    ):
        """Test consumed-token state back-propagates from an async gen.

        Given:
            A caller that sets TENANT_ID to "X", iterates an async-
            generator routine that consumes the Token on one of its
            yields, and then sets TENANT_ID to "Y" after exhaustion
        When:
            The caller invokes var.reset(token) locally — the
            generator already consumed the Token on the worker
        Then:
            The reset should raise RuntimeError (per-yield back-
            propagation carries the consumed-token set to the
            caller just like coroutine back-propagation) and the
            caller's post-set value "Y" must remain intact
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("X")
                async for _ in routines.accept_token_and_reset_on_yield(token):
                    pass
                y_token = routines.TENANT_ID.set("Y")
                try:
                    with pytest.raises(RuntimeError, match="already been used"):
                        routines.TENANT_ID.reset(token)
                    assert routines.TENANT_ID.get() == "Y"
                finally:
                    routines.TENANT_ID.reset(y_token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_reset_of_caller_consumed_token_raises(
        self, credentials_map, retry_grpc_internal
    ):
        """Test forward-propagated consumed tokens reject a worker reset.

        Given:
            A caller that sets TENANT_ID, consumes the resulting
            Token locally via var.reset(token), and then dispatches
            a routine that tries to reset the same (already-consumed)
            Token on the worker
        When:
            The dispatch runs — forward-propagation carries the
            caller's consumed-token set to the worker's scoped
            Context before the routine body executes
        Then:
            The worker's var.reset(token) call should raise
            RuntimeError and the exception should surface to the
            caller's await
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("X")
                routines.TENANT_ID.reset(token)  # caller consumes first
                with pytest.raises(RuntimeError, match="already been used"):
                    await routines.accept_token_and_reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_minted_token_is_reusable_on_caller_then_rejects_reuse(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker-minted Token round-trips and stays single-use.

        Given:
            A routine that mints a Token via TENANT_ID.set(...) on
            the worker and returns it to the caller, followed by
            the caller consuming that Token locally via
            TENANT_ID.reset(token)
        When:
            The caller invokes TENANT_ID.reset(token) a second time
        Then:
            The second reset should raise RuntimeError — the
            Token is logically single-use regardless of which side
            minted it, and the identity round-trip back from the
            worker must preserve that contract
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = await routines.mint_tenant_token("W")
                routines.TENANT_ID.reset(token)  # consume once locally
                with pytest.raises(RuntimeError, match="already been used"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_reset_of_worker_minted_then_caller_consumed_token_raises(
        self, credentials_map, retry_grpc_internal
    ):
        """Test forward-prop rejects worker reset of a worker-minted consumed Token.

        Given:
            A worker that mints a Token and returns it to the
            caller; the caller consumes the Token locally via
            TENANT_ID.reset(token); and a second routine dispatch
            that passes the same (now-consumed) Token to a worker
            that attempts TENANT_ID.reset(token)
        When:
            The second dispatch runs — forward-propagation carries
            the caller's consumed-token state into the second
            worker's scoped Context before the routine body executes
        Then:
            The second worker's var.reset(token) should raise
            RuntimeError and the exception should surface to the
            caller's await, mirroring the caller-minted-Token case
            for a Token that originated on the worker instead of on
            the caller
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = await routines.mint_tenant_token("W")
                routines.TENANT_ID.reset(token)
                with pytest.raises(RuntimeError, match="already been used"):
                    await routines.accept_token_and_reset(token)

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestExceptionPathBackPropagation:
    @pytest.mark.asyncio
    async def test_coroutine_exception_back_propagates_worker_mutation(
        self, credentials_map, retry_grpc_internal
    ):
        """Test exception payload carries worker-side var mutations.

        Given:
            A routine that sets TENANT_ID to a sentinel value then
            raises ValueError
        When:
            The caller dispatches and catches the exception
        Then:
            The caller's TENANT_ID should reflect the worker-side
            sentinel value (back-propagated via exception snapshot
            path)
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-original")
                try:
                    with pytest.raises(ValueError, match="mutate_then_raise_tenant_id"):
                        await routines.mutate_then_raise_tenant_id("exc-path-sentinel")
                    observed = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert observed == "exc-path-sentinel"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_exception_back_propagates_worker_mutation(
        self, credentials_map, retry_grpc_internal
    ):
        """Test async-gen exception carries mid-stream mutations.

        Given:
            An async-generator routine that yields once, then sets
            TENANT_ID and raises on the next iteration
        When:
            The caller iterates and catches the exception
        Then:
            The caller's TENANT_ID should reflect the last mid-stream
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
                        match = "yield_then_mutate_and_raise"
                        with pytest.raises(ValueError, match=match):
                            await gen.__anext__()
                    finally:
                        await gen.aclose()
                    observed = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert observed == "mid-stream-sentinel"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestAsyncioForkOnWorker:
    @pytest.mark.asyncio
    async def test_worker_child_mutation_does_not_leak_to_parent(
        self, credentials_map, retry_grpc_internal
    ):
        """Test child-task mutation stays out of parent on the worker.

        Given:
            A routine that sets TENANT_ID to ``"parent"``, spawns a
            child asyncio task that sets TENANT_ID to ``"child"`` and
            returns its read, and finally reads TENANT_ID from the
            parent after the child completes
        When:
            The caller dispatches the routine
        Then:
            It should return the original value for the parent's
            post-child read (stdlib copy-on-fork parity)
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
        """Test child asyncio task inherits parent's pre-fork var value.

        Given:
            A routine that sets TENANT_ID then spawns a child asyncio
            task that reads TENANT_ID without mutating
        When:
            The caller dispatches the routine
        Then:
            It should return the parent's pre-fork value for the
            child's read
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
        """Test sibling asyncio children are mutually isolated.

        Given:
            A routine that spawns two children via ``asyncio.gather``,
            each mutating TENANT_ID to different values, and a parent
            read afterward
        When:
            The caller dispatches the routine
        Then:
            Each child should observe its own value, and the parent's
            var should remain unchanged (neither child leaks into the
            other nor into the parent)
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


@pytest.mark.integration
class TestStubPromotionAcrossWorkers:
    @pytest.mark.asyncio
    async def test_fresh_worker_promotes_stub_without_collision(
        self, credentials_map, retry_grpc_internal
    ):
        """Test fresh worker unpickles stub then imports module.

        Given:
            A routine that imports and reads TENANT_ID, a caller that
            sets the var, and a fresh EPHEMERAL worker that has not yet
            imported the defining module
        When:
            The caller dispatches the routine
        Then:
            The worker should unpickle the var (stub), import the
            module (promote the stub), and the routine should read
            the propagated value without a collision
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
        """Test colliding sibling var raises on the caller.

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
            The second dispatch should raise wool.ContextVarCollision
            on the caller via the worker's exception snapshot path
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
                with pytest.raises(
                    wool.ContextVarCollision,
                    match=_collision_fixtures.COLLIDING_KEY,
                ):
                    await _collision_fixtures.sibling_b()

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestForwardPropagationMidStream:
    @pytest.mark.asyncio
    async def test_mid_stream_mutation_reaches_next_anext(
        self, credentials_map, retry_grpc_internal
    ):
        """Test caller mutation between __anext__ calls reaches worker.

        Given:
            An async-generator routine that yields ``TENANT_ID.get()``
            on each iteration and a caller that mutates the var
            between ``__anext__`` calls
        When:
            The caller drives the generator manually, setting the var
            to a distinct value before each ``__anext__``
        Then:
            Each yielded value should reflect the caller's most recent
            value at the moment of the call
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
                            if not tok.used:
                                routines.TENANT_ID.reset(tok)
                finally:
                    await gen.aclose()
            assert collected == values

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_mid_stream_mutation_reaches_asend_frame(
        self, credentials_map, retry_grpc_internal
    ):
        """Test caller mutation before asend reaches the worker frame.

        Given:
            An async-generator routine using ``asend`` that echoes
            ``TENANT_ID.get()`` each iteration
        When:
            The caller calls ``gen.asend(x)`` with TENANT_ID set to a
            distinct value before each send
        Then:
            Each echoed value should reflect the caller's var value at
            the moment of the corresponding ``asend`` frame
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
                            if not tok.used:
                                routines.TENANT_ID.reset(tok)
                finally:
                    await gen.aclose()
            assert collected == values

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_mid_stream_mutation_reaches_athrow_frame(
        self, credentials_map, retry_grpc_internal
    ):
        """Test caller mutation before athrow reaches the handler frame.

        Given:
            An async-generator routine whose ``athrow`` handler reads
            ``TENANT_ID`` before yielding that value and returning
        When:
            The caller mutates TENANT_ID then calls ``gen.athrow``
        Then:
            The yielded value should reflect the caller's most recent
            var value at the moment of the ``athrow`` call
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(shape=RoutineShape.ASYNC_GEN_ATHROW)
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.read_on_athrow()
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

    @pytest.mark.asyncio
    async def test_concurrent_mid_stream_mutations_remain_serialized(
        self, credentials_map, retry_grpc_internal
    ):
        """Test parallel async-generator dispatches with mid-stream
        mutations remain isolated under concurrent load.

        Given:
            An EPHEMERAL pool sized to host multiple concurrent
            workers and three asyncio tasks each opening its own
            async-generator dispatch and driving three mid-stream
            iterations, mutating ``TENANT_ID`` to a per-dispatch
            value before each ``__anext__``
        When:
            All three tasks are gathered to run concurrently
        Then:
            Each dispatch should observe only its own caller's
            sequence of values across iterations — no cross-
            contamination, no torn reads. The queue-handshake
            serialization between the gRPC handler and worker loop
            is what holds this invariant; this test guards against
            a regression that would surface as cross-dispatch
            value bleed in the collected sequences.
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
                pool_mode=PoolMode.EPHEMERAL,
            )

            async def dispatch_with_prefix(prefix: str) -> list[str]:
                values = [f"{prefix}-1", f"{prefix}-2", f"{prefix}-3"]
                gen = routines.stream_tenant_id_echo(3)
                collected: list[str] = []
                tokens: list = []
                try:
                    try:
                        for v in values:
                            tokens.append(routines.TENANT_ID.set(v))
                            collected.append(await gen.__anext__())
                    finally:
                        for tok in reversed(tokens):
                            if not tok.used:
                                routines.TENANT_ID.reset(tok)
                finally:
                    await gen.aclose()
                return collected

            async with build_pool_from_scenario(scenario, credentials_map):
                results = await asyncio.gather(
                    dispatch_with_prefix("alpha"),
                    dispatch_with_prefix("beta"),
                    dispatch_with_prefix("gamma"),
                )

            assert results[0] == ["alpha-1", "alpha-2", "alpha-3"]
            assert results[1] == ["beta-1", "beta-2", "beta-3"]
            assert results[2] == ["gamma-1", "gamma-2", "gamma-3"]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_concurrent_asend_against_single_generator_raises(
        self, credentials_map, retry_grpc_internal
    ):
        """Test two concurrent ``asend`` calls against the same wool
        async-generator behave like Python native: one succeeds, the
        other raises RuntimeError.

        Given:
            An async-generator routine driven past its initial
            ``ready`` yield, with two ``asend`` coroutines created
            but not yet awaited
        When:
            Both coroutines are awaited concurrently via
            ``asyncio.gather(..., return_exceptions=True)``
        Then:
            Exactly one returns a value (the worker echoes the
            current ``TENANT_ID``); the other raises
            ``RuntimeError`` whose message matches Python's native
            async-generator concurrency error — the dispatch
            stream's ``_running`` guard refuses concurrent
            invocation, mirroring the stdlib semantics
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(shape=RoutineShape.ASYNC_GEN_ASEND)
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.echo_tenant_id_on_send(2)
                try:
                    ready = await gen.__anext__()
                    assert ready == "ready"
                    token = routines.TENANT_ID.set("concurrent-asend-value")
                    try:
                        co1 = gen.asend(None)
                        co2 = gen.asend(None)
                        outcomes = await asyncio.gather(co1, co2, return_exceptions=True)
                    finally:
                        routines.TENANT_ID.reset(token)
                finally:
                    await gen.aclose()

            successes = [o for o in outcomes if not isinstance(o, BaseException)]
            failures = [o for o in outcomes if isinstance(o, BaseException)]
            assert len(successes) == 1
            assert len(failures) == 1
            assert successes[0] == "concurrent-asend-value"
            assert isinstance(failures[0], RuntimeError)
            assert "asynchronous generator is already running" in str(failures[0])

        await retry_grpc_internal(body)


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
        """Test var unknown on worker is dropped, dispatch succeeds.

        Given:
            A caller-only ``wool.ContextVar`` under a synthetic
            namespace set to a value, and a routine that reads a
            different, worker-known ``TENANT_ID`` var
        When:
            The caller dispatches the routine
        Then:
            The dispatch should complete; the routine should observe
            its own (worker-known) var; and the synthetic caller-only
            key should be silently dropped on the worker (debug-log
            only, no exception)
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

    @pytest.mark.asyncio
    async def test_worker_stubs_unknown_key_visible_after_late_declaration(
        self, credentials_map, retry_grpc_internal
    ):
        """Test wire stub becomes visible once worker declares the var.

        Given:
            A caller-only ``wool.ContextVar`` under a synthetic
            namespace set to a value, and a routine that declares a
            matching ``ContextVar`` on the worker after the wire
            frame has arrived (the worker had no prior registration
            for this key)
        When:
            The caller dispatches the routine
        Then:
            The routine's in-body ``ContextVar(name, namespace=...)``
            call should find the wire-shipped stub in the registry
            and promote it in place; ``get()`` should return the
            caller's wire-shipped value rather than the constructor's
            default
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                unreg_token = _UNREGISTERED_ONLY.set("late-declared-value")
                try:
                    observed = await routines.declare_and_read_unregistered_key(
                        "synthetic_no_such_module",
                        "caller_only_key",
                        "default-fallback",
                    )
                finally:
                    _UNREGISTERED_ONLY.reset(unreg_token)
            assert observed == "late-declared-value"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestCallerSideTaskFactoryFork:
    @pytest.mark.asyncio
    async def test_caller_child_task_inherits_var_through_dispatch(
        self, credentials_map, retry_grpc_internal
    ):
        """Test caller child asyncio task inherits var and dispatches correctly.

        Given:
            A caller that sets TENANT_ID and spawns an asyncio child
            task via ``create_task`` which dispatches a routine that
            reads the var from the worker
        When:
            The child task dispatches the routine
        Then:
            The routine should return the caller's propagated value,
            proving the child task inherited the parent's context and
            the dispatch propagated it to the worker
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("parent-caller-value")
                try:

                    async def _child():
                        return await routines.get_tenant_id()

                    result = await asyncio.create_task(_child())
                finally:
                    routines.TENANT_ID.reset(token)
            assert result == "parent-caller-value"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_child_dispatch_mutation_does_not_leak_to_parent(
        self, credentials_map, retry_grpc_internal
    ):
        """Test caller child task's back-propagated mutation stays isolated.

        Given:
            A caller that sets TENANT_ID and spawns an asyncio child
            task via ``create_task`` with a copied ``contextvars``
            context, where the child dispatches a routine that mutates
            the var on the worker
        When:
            The child task completes and the parent reads its own
            TENANT_ID
        Then:
            The parent's value should remain unchanged because the
            child task's back-propagation is scoped to its own
            ``contextvars.Context`` copy
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("parent-original")
                try:
                    ctx = contextvars.copy_context()

                    async def _child():
                        worker_read = await routines.mutate_and_read_tenant_id()
                        child_after = routines.TENANT_ID.get()
                        return worker_read, child_after

                    task = asyncio.create_task(_child(), context=ctx)
                    worker_read, child_after = await task
                    parent_after = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert worker_read == "mutated_on_worker"
            assert child_after == "mutated_on_worker"
            assert parent_after == "parent-original"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestSequentialDispatchIsolation:
    @pytest.mark.asyncio
    async def test_sequential_dispatches_do_not_bleed_context(
        self, credentials_map, retry_grpc_internal
    ):
        """Test sequential dispatches do not leak var state between calls.

        Given:
            A DEFAULT pool, a first dispatch that mutates TENANT_ID on
            the worker (back-propagating to the caller), and a caller
            that resets the var to a fresh value before the second
            dispatch
        When:
            The second dispatch reads TENANT_ID on the worker
        Then:
            The second dispatch should observe the caller's freshly set
            value, not the residual mutation from the first dispatch,
            proving each dispatch snapshots the caller's current context
            independently
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                # First dispatch: worker mutates to "mutated_on_worker"
                token1 = routines.TENANT_ID.set("first-dispatch-value")
                try:
                    first_result = await routines.mutate_and_read_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token1)

                # Second dispatch: caller sets a fresh value
                token2 = routines.TENANT_ID.set("second-dispatch-value")
                try:
                    second_result = await routines.get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token2)

            assert first_result == "mutated_on_worker"
            assert second_result == "second-dispatch-value"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestSelfDispatchStreamingVarMutation:
    @pytest.mark.asyncio
    async def test_self_dispatch_streaming_var_mutation_between_yields(
        self, credentials_map, retry_grpc_internal
    ):
        """Test self-dispatch streaming applies caller var mutations per yield.

        Given:
            A DEFAULT pool (size=1, guaranteed self-dispatch) running an
            async-generator routine that yields ``TENANT_ID.get()`` on
            each iteration
        When:
            The caller mutates TENANT_ID to a distinct value between
            each ``__anext__`` call
        Then:
            Each yielded value reflects the caller's latest mutation,
            proving that per-frame forward-propagation through the
            PassthroughSerializer self-dispatch path applies
            ``PassthroughSerializer.loads`` for streaming var updates
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
                pool_mode=PoolMode.DEFAULT,
            )
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.stream_tenant_id_echo(3)
                try:
                    collected: list[str] = []
                    values = ["sds-first", "sds-second", "sds-third"]
                    tokens: list = []
                    try:
                        for v in values:
                            tokens.append(routines.TENANT_ID.set(v))
                            collected.append(await gen.__anext__())
                    finally:
                        for tok in reversed(tokens):
                            if not tok.used:
                                routines.TENANT_ID.reset(tok)
                finally:
                    await gen.aclose()
            assert collected == values

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestDurablePoolContextPropagation:
    @pytest.mark.asyncio
    async def test_durable_pool_propagates_wool_context_var(
        self, credentials_map, retry_grpc_internal
    ):
        """Test wool.ContextVar propagation works through a DURABLE pool.

        Given:
            A DURABLE pool backed by a manually started worker
            discovered via LocalDiscovery and a caller that sets
            TENANT_ID before dispatch
        When:
            The caller dispatches a coroutine that reads TENANT_ID on
            the worker
        Then:
            The routine should return the caller's propagated value,
            proving the serialization and restoration path works
            identically for DURABLE pools
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(pool_mode=PoolMode.DURABLE)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("durable-tenant")
                try:
                    result = await routines.get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)
            assert result == "durable-tenant"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestMergedWireShapeEndToEnd:
    """End-to-end coverage for the merged wire shape introduced
    when ``protocol.Context.vars`` became ``repeated ContextVar``
    with optional ``value`` and ``consumed_tokens`` fields under
    the same entry. Each test exercises a caller setup whose
    Context carries both a current value AND a consumed-token id
    for the same var — a corner the prior shape (``map<str,bytes>``
    plus ``repeated ConsumedToken``) could not express in a single
    entry — and verifies the dispatch path round-trips both pieces
    of state to the worker.
    """

    @pytest.mark.asyncio
    async def test_single_dispatch_carries_value_and_consumed_token(
        self, credentials_map, retry_grpc_internal
    ):
        """Test one dispatch propagates a current value and a
        consumed-token id under the same merged wire entry.

        Given:
            A caller that ran ``token = TENANT_ID.set("X")``, then
            ``TENANT_ID.reset(token)``, then ``TENANT_ID.set("Y")``
            — the var carries a current value "Y" alongside a
            locally-consumed token under the same identity, with a
            strong reference held to the token
        When:
            The caller dispatches ``read_value_and_attempt_reset``
            passing the consumed token as the argument
        Then:
            The routine should observe ``TENANT_ID.get() == "Y"``
            on the worker AND ``TENANT_ID.reset(token)`` should
            raise ``RuntimeError("Token has already been used")``
            — confirming the merged entry round-trips both the
            value and the consumed-token id to the worker, where
            the wire-promoted Token correctly reports as already
            used
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("X")
                routines.TENANT_ID.reset(token)
                y_token = routines.TENANT_ID.set("Y")
                try:
                    value, reset_outcome = await routines.read_value_and_attempt_reset(
                        token
                    )
                finally:
                    routines.TENANT_ID.reset(y_token)
            assert value == "Y"
            assert "Token has already been used" in reset_outcome

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_consumed_token_carries_across_two_sequential_dispatches(
        self, credentials_map, retry_grpc_internal
    ):
        """Test two sequential dispatches forward the same merged
        entry to the worker on each frame.

        Given:
            A caller that ran ``token = TENANT_ID.set("X")``, then
            ``TENANT_ID.reset(token)``, then ``TENANT_ID.set("Y")``,
            with a strong reference held to the consumed token
        When:
            The caller dispatches ``get_tenant_id`` first (which
            takes no arguments), then dispatches
            ``accept_token_and_reset`` passing the consumed token
        Then:
            The first dispatch returns "Y" — the value rode forward
            in the merged entry — and the second dispatch raises
            RuntimeError ("Token has already been used") on the
            worker, confirming the consumed-token id rode forward
            in the same merged entry on both dispatches
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("X")
                routines.TENANT_ID.reset(token)
                y_token = routines.TENANT_ID.set("Y")
                try:
                    first_value = await routines.get_tenant_id()
                    assert first_value == "Y"
                    with pytest.raises(RuntimeError, match="already been used"):
                        await routines.accept_token_and_reset(token)
                finally:
                    routines.TENANT_ID.reset(y_token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_merged_entry_rides_forward_across_async_gen_frames(
        self, credentials_map, retry_grpc_internal
    ):
        """Test per-frame forward propagation preserves the merged
        entry's value across every ``__anext__`` boundary of an
        async-generator routine.

        Given:
            A caller that ran ``token = TENANT_ID.set("X")``, then
            ``TENANT_ID.reset(token)``, then ``TENANT_ID.set("Y")``
            — same setup as the single-dispatch case but the routine
            is an async generator that yields ``TENANT_ID.get()`` on
            each iteration
        When:
            The caller iterates ``stream_tenant_id(count=3)`` to
            completion
        Then:
            Every yield equals "Y" — the value rides the merged
            entry on each per-frame request — and a subsequent
            local ``TENANT_ID.reset(token)`` still raises
            RuntimeError, confirming the streaming back-propagation
            preserved the caller's consumed-token state rather than
            silently clobbering it
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("X")
                routines.TENANT_ID.reset(token)
                y_token = routines.TENANT_ID.set("Y")
                try:
                    yielded: list[str] = []
                    async for value in routines.stream_tenant_id(3):
                        yielded.append(value)
                    assert yielded == ["Y", "Y", "Y"]
                    with pytest.raises(RuntimeError, match="already been used"):
                        routines.TENANT_ID.reset(token)
                finally:
                    routines.TENANT_ID.reset(y_token)

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestExplicitWoolContextBindingAcrossWorkers:
    @pytest.mark.asyncio
    async def test_explicit_wool_context_binding_propagates_var_to_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test wool.create_task with an explicit wool.Context binds and dispatches.

        Given:
            A pre-populated wool.Context seeded with a TENANT_ID
            value via Context.run, a child task created with
            wool.create_task(coro, context=ctx), and a routine that
            reads TENANT_ID on the worker
        When:
            The child task awaits the dispatched routine
        Then:
            The routine should observe the explicitly bound
            wool.Context's TENANT_ID value, proving the wool task
            factory routes the explicit Context across the wire
            independently of the caller's implicit current Context
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                outer_token = routines.TENANT_ID.set("outer-caller-value")
                try:
                    bound_ctx = wool.Context()
                    bound_ctx.run(lambda: routines.TENANT_ID.set("explicit-bound-value"))

                    async def _child():
                        return await routines.get_tenant_id()

                    task = wool.create_task(_child(), context=bound_ctx)
                    result = await task
                finally:
                    routines.TENANT_ID.reset(outer_token)
            assert result == "explicit-bound-value"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_concurrent_dispatch_under_same_wool_context_raises(
        self, credentials_map, retry_grpc_internal
    ):
        """Test two concurrent tasks bound to the same wool.Context fail.

        Given:
            A single wool.Context and two child tasks each created
            via wool.create_task(coro, context=same_ctx) that await
            a remote routine
        When:
            Both tasks are gathered concurrently
        Then:
            One task should complete successfully and the other
            should raise RuntimeError because at most one task may
            run inside a given wool.Context at a time — the wool
            task factory's _wool_scoped first-task-wins guard fires
            before the second task acquires _guard
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                shared_ctx = wool.Context()
                shared_ctx.run(lambda: routines.TENANT_ID.set("shared-context-value"))

                async def _slow_dispatch():
                    # An ``asyncio.sleep(0)`` lets the scheduler park
                    # the first task before the second is created, so
                    # both create_task calls execute while the first
                    # is still mid-dispatch and the second hits the
                    # first-task-wins guard.
                    await asyncio.sleep(0)
                    return await routines.get_tenant_id()

                first_coro = _slow_dispatch()
                second_coro = _slow_dispatch()
                first = wool.create_task(first_coro, context=shared_ctx)
                with warnings.catch_warnings(record=True) as captured:
                    warnings.simplefilter("always", category=RuntimeWarning)
                    try:
                        second = wool.create_task(second_coro, context=shared_ctx)
                    except RuntimeError as exc:
                        # First-task-wins guard fired synchronously inside
                        # the factory before the second task was even
                        # scheduled. Close the unawaited coroutine and
                        # await the first to get its successful result.
                        second_coro.close()
                        successes = [await first]
                        failures = [exc]
                    else:
                        outcomes = await asyncio.gather(
                            first, second, return_exceptions=True
                        )
                        successes = [
                            o for o in outcomes if not isinstance(o, BaseException)
                        ]
                        failures = [o for o in outcomes if isinstance(o, BaseException)]
                    # Force collection in this frame so any "coroutine
                    # was never awaited" warning surfaces inside the
                    # catch_warnings scope rather than at teardown.
                    import gc

                    gc.collect()
                leaked = [
                    w
                    for w in captured
                    if issubclass(w.category, RuntimeWarning)
                    and "was never awaited" in str(w.message)
                ]
                assert leaked == [], (
                    "Guard-rejected coroutine must be closed by _wool_scoped, "
                    f"not leaked at GC; saw: {[str(w.message) for w in leaked]}"
                )

            assert len(failures) == 1
            assert isinstance(failures[0], RuntimeError)
            assert "first-task-wins" in str(failures[0])
            assert successes == ["shared-context-value"]

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestRuntimeContextDispatchTimeoutAcrossWorkers:
    @pytest.mark.asyncio
    async def test_caller_runtime_context_dispatch_timeout_visible_on_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test caller-side dispatch_timeout overrides ride the wire.

        Given:
            A caller that wraps a dispatch in
            ``with wool.RuntimeContext(dispatch_timeout=X):`` and a
            routine that returns the worker-side value of
            ``dispatch_timeout.get()``
        When:
            The caller dispatches the routine inside the override block
        Then:
            The routine should observe the caller's override value,
            proving the RuntimeContext snapshot rode through the
            Task.runtime_context wire field and was restored on the
            worker before the routine body executed
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                with wool.RuntimeContext(dispatch_timeout=12.5):
                    observed = await routines.read_dispatch_timeout()
            assert observed == 12.5

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_dispatch_timeout_var_propagates_without_runtime_context(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the ambient dispatch_timeout var alone propagates.

        Given:
            A caller that sets the module-level ``dispatch_timeout``
            stdlib ContextVar (no explicit RuntimeContext block) and a
            routine that returns the worker-side value
        When:
            The caller dispatches the routine
        Then:
            The routine should observe the caller's set value because
            ``RuntimeContext.get_current`` captures the ambient
            ``dispatch_timeout`` at Task construction time and the
            captured snapshot rides the wire
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = dispatch_timeout.set(7.25)
                try:
                    observed = await routines.read_dispatch_timeout()
                finally:
                    dispatch_timeout.reset(token)
            assert observed == 7.25

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestContextDecodeWarningAcrossWorkers:
    @pytest.mark.asyncio
    async def test_unpicklable_var_value_emits_decode_warning_and_dispatch_completes(
        self, credentials_map, retry_grpc_internal
    ):
        """Test unpicklable wool.ContextVar value is dropped and dispatch survives.

        Given:
            A caller that sets ``TENANT_ID`` to a known value and a
            second wool.ContextVar (REGION) to an unpicklable lambda,
            and a routine that reads ``TENANT_ID``
        When:
            The caller dispatches the routine under default warning
            filters
        Then:
            ``wool.ContextDecodeWarning`` should be emitted on the
            caller side for the unpicklable var; the dispatch should
            still complete; and the routine should observe the
            propagated ``TENANT_ID`` value — primary signal preserved,
            ancillary failure surfaced as a warning
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                tenant_token = routines.TENANT_ID.set("survives-encode")
                # Local lambdas are not picklable across processes
                # (cloudpickle handles many cases, but a closure over
                # a local non-importable scope is rejected by the
                # default pickle protocol via cloudpickle.dumps when
                # paired with a file-local symbol that has no qualname
                # path the worker can resolve). Use an open file
                # handle as a robust unpicklable sentinel.
                import socket

                unpicklable = socket.socket()
                try:
                    region_token = routines.REGION.set(unpicklable)  # pyright: ignore[reportArgumentType]
                    try:
                        with warnings.catch_warnings(record=True) as captured:
                            warnings.simplefilter(
                                "always", category=wool.ContextDecodeWarning
                            )
                            result = await routines.read_tenant_id_only()
                    finally:
                        routines.REGION.reset(region_token)
                finally:
                    unpicklable.close()
                    routines.TENANT_ID.reset(tenant_token)
            decode_warnings = [
                w for w in captured if issubclass(w.category, wool.ContextDecodeWarning)
            ]
            assert decode_warnings, (
                f"Expected at least one ContextDecodeWarning, got {captured!r}"
            )
            assert any("region" in str(w.message) for w in decode_warnings), (
                f"Expected the warning to name the offending var; "
                f"got {[str(w.message) for w in decode_warnings]!r}"
            )
            assert result == "survives-encode"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestWoolCopyContextWithDispatch:
    @pytest.mark.asyncio
    async def test_wool_copy_context_seeded_value_propagates_under_attach(
        self, credentials_map, retry_grpc_internal
    ):
        """Test wool.copy_context.run sets a value the dispatch ships.

        Given:
            A caller that calls ``wool.copy_context()`` to fork the
            current wool.Context, runs a setter inside the forked
            Context to seed a TENANT_ID value, and dispatches the
            routine while ``attached`` to the forked Context
        When:
            The dispatched routine reads TENANT_ID
        Then:
            The routine should return the seeded value, not the
            outer caller's value, proving the forked copy is the
            active source of truth and ships its bindings to the
            worker independently of the implicit current Context
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                outer_token = routines.TENANT_ID.set("outer-original")
                try:
                    forked = wool.copy_context()
                    forked.run(lambda: routines.TENANT_ID.set("forked-seed"))

                    with attached(forked):
                        result = await routines.get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(outer_token)
            assert result == "forked-seed"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_wool_copy_context_has_distinct_id_from_source(
        self, credentials_map, retry_grpc_internal
    ):
        """Test wool.copy_context produces a fresh logical chain id.

        Given:
            A caller that captures ``wool.current_context().id`` then
            calls ``wool.copy_context()`` and dispatches a routine
            that returns the worker-side ``current_context().id.hex``
            inside the forked Context
        When:
            The dispatch runs under ``with attached(forked):``
        Then:
            The worker-observed id should equal the forked Context's
            id and differ from the outer caller's captured id —
            ``copy_context`` mints a fresh chain id rather than
            reusing the source's
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                outer_id = wool.current_context().id
                forked = wool.copy_context()

                with attached(forked):
                    observed_hex = await routines.return_current_context_id_hex()
            assert observed_hex == forked.id.hex
            assert forked.id != outer_id

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestNestedDispatchMidChainMutation:
    @pytest.mark.asyncio
    async def test_outer_worker_mid_routine_mutation_reaches_nested_inner_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test outer routine mutation propagates to a nested dispatch.

        Given:
            A caller that sets TENANT_ID to "alpha", an EPHEMERAL pool
            sized to permit two distinct workers, and an outer routine
            that mutates TENANT_ID to "beta" before dispatching
            ``get_tenant_id`` to a nested worker
        When:
            The caller dispatches the outer routine
        Then:
            The outer routine should return "beta" — the inner worker
            observed the outer's mid-routine mutation, not the
            caller's pre-dispatch value — and the caller should
            observe "beta" after the dispatch returns, completing the
            bidirectional propagation chain across two worker hops
        """

        # Arrange, act, & assert
        async def body():
            scenario = _default_scenario(
                shape=RoutineShape.NESTED_COROUTINE,
                pool_mode=PoolMode.EPHEMERAL,
            )
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("alpha")
                try:
                    inner_observed = await routines.mutate_then_nested_get_tenant_id(
                        "beta"
                    )
                    caller_after = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert inner_observed == "beta"
            assert caller_after == "beta"

        await retry_grpc_internal(body)


def _tenant_aware_backpressure_hook(ctx):
    """Module-level (picklable) sync hook that rejects when TENANT_ID == "reject-me".

    Reads ``routines.TENANT_ID`` to verify the caller's wire-shipped
    wool.ContextVar snapshot has been applied to the worker's
    handler context before the hook runs (per the dispatch
    handler's documented ordering).
    """
    return routines.TENANT_ID.get() == "reject-me"


@pytest.mark.integration
class TestBackpressureReadsCallerShippedContextVar:
    @pytest.mark.asyncio
    async def test_backpressure_hook_observes_caller_tenant_var(
        self, retry_grpc_internal
    ):
        """Test backpressure hook reads the caller's wool.ContextVar value.

        Given:
            A single-worker pool whose backpressure hook returns True
            (reject) when ``routines.TENANT_ID.get() == "reject-me"``,
            and accepts otherwise.
        When:
            Two coroutine dispatches run sequentially under different
            caller-side TENANT_ID values: first "reject-me", then
            "ok".
        Then:
            The first dispatch should raise NoWorkersAvailable
            (RESOURCE_EXHAUSTED from the hook), and the second should
            succeed — proving the hook observes the caller's wire-
            shipped TENANT_ID, not a stale or default value.
        """
        from functools import partial

        from wool.runtime.loadbalancer.base import NoWorkersAvailable
        from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
        from wool.runtime.worker.local import LocalWorker
        from wool.runtime.worker.pool import WorkerPool

        # Arrange, act, & assert
        async def body():
            pool = WorkerPool(
                size=1,
                loadbalancer=RoundRobinLoadBalancer,
                worker=partial(
                    LocalWorker, backpressure=_tenant_aware_backpressure_hook
                ),
            )

            async with pool:
                reject_token = routines.TENANT_ID.set("reject-me")
                try:
                    with pytest.raises(NoWorkersAvailable):
                        await routines.add(1, 2)
                finally:
                    routines.TENANT_ID.reset(reject_token)

                accept_token = routines.TENANT_ID.set("ok")
                try:
                    result = await routines.add(1, 2)
                finally:
                    routines.TENANT_ID.reset(accept_token)
            assert result == 3

        await retry_grpc_internal(body)
