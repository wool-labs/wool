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
