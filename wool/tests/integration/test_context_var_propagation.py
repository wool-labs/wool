"""Integration tests for wool.ContextVar cross-worker propagation.

These tests drive the full dispatch wire path — caller sets a wool
ContextVar, encode_context serializes it, gRPC carries it to a real
worker subprocess, the worker unpickles the callable (importing
routines.py and populating its wool.ContextVar registry), and the
routine observes the propagated value. They complement the in-process
unit tests in tests/runtime/test_context.py by exercising the real
serialization and subprocess boundary, and they serve as the
regression guard for the unified-driver async-generator fix
originally introduced with issue #154 and carried forward into the
collapsed :class:`DispatchSession` by issue #187.
"""

import asyncio
import contextvars
import warnings

import grpc
import pytest

import wool
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import UnexpectedResponse

from . import _collision_fixtures
from . import routines
from .conftest import PoolMode
from .conftest import RoutineShape
from .conftest import build_pool_from_scenario
from .conftest import default_scenario


@pytest.mark.integration
class TestContextVarPropagation:
    @pytest.mark.asyncio
    async def test_coroutine_dispatch_should_propagate_wool_context_var(
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
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("acme-corp")
                try:
                    result = await routines.get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)
            assert result == "acme-corp"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_generator_dispatch_should_propagate_wool_context_var(
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
            value — guards against regression in the unified driver's
            async-gen per-yield context restoration
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(
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
    async def test_nested_dispatch_should_propagate_wool_context_var(
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
            scenario = default_scenario(shape=RoutineShape.NESTED_COROUTINE)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("nested-tenant")
                try:
                    result = await routines.nested_get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)
            assert result == "nested-tenant"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_mutation_should_be_visible_in_return_value(
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
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-value")
                try:
                    worker_result = await routines.mutate_and_read_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)
            assert worker_result == "mutated_on_worker"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_mutation_should_back_propagate_to_caller(
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
            caller's Chain
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
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
    async def test_coroutine_mutation_should_back_propagate_to_unarmed_caller(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a coroutine routine arms a previously-unarmed caller.

        Given:
            An unarmed caller (no prior ``wool.ContextVar.set``) and a
            DEFAULT pool running a coroutine that performs the first
            ``var.set`` on the worker.
        When:
            The caller dispatches the routine and reads its own var
            value after the routine returns.
        Then:
            The caller should observe the worker-side value — the
            worker mints a fresh chain on the first ``var.set``,
            ships it back on the result frame's chain manifest, and
            the caller's apply-back arms the previously-unarmed
            chain with the worker's bindings. This is the
            stdlib-parity contract: ``await routine_that_sets(x)``
            makes ``x`` visible to the caller afterward.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                # Pre-dispatch baseline: caller is unarmed — the var
                # resolves through to its constructor default
                # ("unknown") since no chain has set it.
                assert routines.TENANT_ID.get() == "unknown"
                await routines.mutate_and_read_tenant_id()
                caller_value_after = routines.TENANT_ID.get()
            assert caller_value_after == "mutated_on_worker"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_concurrent_dispatches_should_observe_isolated_values(
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
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
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
    async def test_default_only_value_should_not_be_propagated(
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
            captured into the protobuf payload
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.get_tenant_id()
            assert result == "unknown"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_multiple_wool_context_vars_should_round_trip(
        self, credentials_map, retry_grpc_internal
    ):
        """Test multiple registered vars are all propagated and restored.

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
            scenario = default_scenario()
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
    async def test_async_generator_mutation_should_be_visible_in_yields(
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
            scenario = default_scenario(
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
    async def test_async_generator_mutation_should_back_propagate_to_caller(
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
            change to the caller's Chain
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(
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
    async def test_async_gen_per_yield_should_back_propagate_to_unarmed_caller(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an async-generator routine arms a previously-unarmed caller per yield.

        Given:
            An unarmed caller (no prior ``wool.ContextVar.set``) and a
            DEFAULT pool running an async generator that performs the
            first ``var.set`` on the worker on every iteration.
        When:
            The caller iterates the generator and reads its own var
            value after each yield.
        Then:
            * The pre-dispatch read resolves through to the var's
              constructor default — the caller is unarmed.
            * Each per-yield snapshot equals the worker's most-recent
              ``var.set`` — back-propagation arms the previously-
              unarmed caller via the response apply-back, then
              updates the binding on every subsequent yield's mount.
            * After exhaustion the caller observes the final yield's
              binding, the stdlib-parity contract for
              ``async for x in agen()`` when the routine sets state.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
            )
            caller_snapshots: list[str] = []
            async with build_pool_from_scenario(scenario, credentials_map):
                # Pre-dispatch baseline: caller is unarmed.
                assert routines.TENANT_ID.get() == "unknown"
                async for _ in routines.mutate_on_each_yield(3):
                    caller_snapshots.append(routines.TENANT_ID.get())
                caller_final = routines.TENANT_ID.get()
            assert caller_snapshots == ["step-0", "step-1", "step-2"]
            assert caller_final == "step-2"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_back_propagation_should_update_caller_per_yield(
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
            scenario = default_scenario(
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
    async def test_async_generator_dispatch_should_match_in_process_baseline(
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
            owning an isolated worker chain)
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
            scenario = default_scenario(
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
class TestWoolContextAcrossWorkers:
    @pytest.mark.asyncio
    async def test_caller_chain_id_should_propagate_to_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the worker observes the same chain id as the caller.

        Given:
            A caller that arms its context by setting a wool.ContextVar
            and a routine that returns the worker-side context
            chain id hex.
        When:
            The caller dispatches the routine.
        Then:
            It should return the caller's own chain id hex —
            confirming the worker is armed on the caller's chain via
            install_context.
        """

        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                # Arrange
                routines.TENANT_ID.set("armed")
                caller = wool.__chain__.get(None)
                assert caller is not None

                # Act
                observed_hex = await routines.return_current_chain_id_hex()

            # Assert
            assert observed_hex == caller.id.hex

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_asyncio_child_task_should_fork_chain_id(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an asyncio child task runs on a freshly forked chain id.

        Given:
            An armed caller that enters an asyncio child task via
            ``create_task`` and captures the parent and child chain
            ids.
        When:
            The child dispatches a routine.
        Then:
            It should observe the child's forked chain id on the
            worker — the child's chain differs from the parent's
            (stdlib copy-on-fork parity) and the worker arms on the
            child's chain.
        """

        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)

            async with build_pool_from_scenario(scenario, credentials_map):
                # Arrange
                routines.TENANT_ID.set("armed")
                parent = wool.__chain__.get(None)
                assert parent is not None
                parent_id = parent.id

                async def _child():
                    child = wool.__chain__.get(None)
                    assert child is not None

                    # Act
                    observed_hex = await routines.return_current_chain_id_hex()
                    return child.id, observed_hex

                child_id, observed_hex = await asyncio.create_task(_child())

            # Assert
            assert child_id != parent_id
            assert observed_hex == child_id.hex

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_seeded_context_dispatch_should_propagate_var_bindings(
        self, credentials_map, retry_grpc_internal
    ):
        """Test var bindings seeded in a copied context ship to the worker.

        Given:
            A caller that seeds a TENANT_ID binding then copies the
            live context with ``contextvars.copy_context``, and a
            routine that returns the var's observed value.
        When:
            The caller invokes the dispatch inside the copied
            context's ``run``.
        Then:
            It should return the seed value — the chain manifest picks
            up the seeded binding from the copied context's run.
        """

        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                # Arrange
                token = routines.TENANT_ID.set("seed-value")
                try:
                    forked = contextvars.copy_context()
                finally:
                    routines.TENANT_ID.reset(token)

                # Act
                # forked.run() activates the copied context only long
                # enough for ensure_future to schedule the dispatch, so
                # the routine's chain manifest is encoded from the seeded
                # context; the await then completes outside run().
                result = await forked.run(
                    lambda: asyncio.ensure_future(routines.get_tenant_id())
                )

            # Assert
            assert result == "seed-value"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestExceptionPathBackPropagation:
    @pytest.mark.asyncio
    async def test_coroutine_exception_should_back_propagate_worker_mutation(
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
            sentinel value (back-propagated via exception context
            path)
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
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
    async def test_async_gen_exception_should_back_propagate_worker_mutation(
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
            scenario = default_scenario(
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
    async def test_worker_child_mutation_should_not_leak_to_parent(
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
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                child_value, parent_value = await routines.spawn_and_mutate_tenant_id()
            assert child_value == "child"
            assert parent_value == "parent"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_child_should_inherit_parent_value(
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
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                child_value = await routines.parent_sets_child_reads()
            assert child_value == "parent-set"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_sibling_children_should_be_isolated(
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
            scenario = default_scenario()
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
    async def test_fresh_worker_should_promote_stub_without_collision(
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
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("stub-promotion-value")
                try:
                    result = await routines.get_tenant_id()
                finally:
                    routines.TENANT_ID.reset(token)
            assert result == "stub-promotion-value"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_sibling_routine_should_raise_context_var_collision(
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
            on the caller via the worker's exception context path
        """

        # Arrange, act, & assert
        async def body():
            # DEFAULT pool is size=1 so both dispatches land on the
            # same worker process; the second construction under the
            # already-registered key triggers the collision.
            scenario = default_scenario(pool_mode=PoolMode.DEFAULT)
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
    # NOTE: the plain ``async for`` mid-stream forward-propagation case
    # (formerly ``test_mid_stream_mutation_reaches_next_anext``) is now
    # the ``ContextVarPattern.MID_STREAM_FORWARD`` dimension member —
    # the covering array in ``test_integration.py`` crosses it with
    # ``pool_mode``/``binding``/``credential`` automatically. Only the
    # ``asend``/``athrow`` shapes and the concurrency cases remain
    # here: the per-step forward driver in ``invoke_routine`` covers
    # ``ASYNC_GEN_ANEXT`` only, so those shapes are genuinely distinct.
    @pytest.mark.asyncio
    async def test_mid_stream_mutation_should_reach_asend_frame(
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
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ASEND)
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
                            routines.TENANT_ID.reset(tok)
                finally:
                    await gen.aclose()
            assert collected == values

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_mid_stream_mutation_should_reach_athrow_frame(
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
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ATHROW)
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
    async def test_concurrent_mid_stream_mutations_should_remain_serialized(
        self, credentials_map, retry_grpc_internal
    ):
        """Test concurrent async-gen dispatches keep mid-stream mutations isolated.

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
            scenario = default_scenario(
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
    async def test_concurrent_asend_against_single_generator_should_raise(
        self, credentials_map, retry_grpc_internal
    ):
        """Test concurrent asend on one wool async-gen: one succeeds, one raises.

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
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ASEND)
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
    async def test_worker_should_silently_drop_unknown_key(
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
            scenario = default_scenario()
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
    async def test_worker_should_expose_stubbed_unknown_key_when_late_declared(
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
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
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
class TestCallerSideChildTaskDispatch:
    """Caller-side child asyncio task forks the chain and dispatches.

    Consolidates the formerly-separate ``TestCallerSideTaskFactoryFork``
    and ``TestForkedChildTaskDispatchAcrossWorkers`` classes — both
    exercised the same "a child task created with ``create_task`` forks
    the parent chain and dispatches a routine" scenario, differing only
    in ``pool_mode``. The inheritance case is parametrized over
    ``pool_mode`` rather than duplicated across two class bodies.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pool_mode",
        [PoolMode.DEFAULT, PoolMode.EPHEMERAL],
        ids=lambda m: m.name,
    )
    async def test_caller_child_task_should_inherit_var_through_dispatch(
        self, pool_mode, credentials_map, retry_grpc_internal
    ):
        """Test caller child asyncio task inherits var and dispatches correctly.

        Given:
            A caller that sets TENANT_ID and spawns an asyncio child
            task via ``create_task`` which dispatches a routine that
            reads the var from the worker, against a DEFAULT
            (self-dispatch) and an EPHEMERAL (cross-process) pool
        When:
            The child task dispatches the routine
        Then:
            The routine should return the caller's propagated value,
            proving the child task's forked chain carries the parent's
            variable bindings through the dispatch to the worker
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=pool_mode)
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
    async def test_caller_child_dispatch_mutation_should_not_leak_to_parent(
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
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
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

    @pytest.mark.asyncio
    async def test_concurrent_child_dispatches_should_be_isolated(
        self, credentials_map, retry_grpc_internal
    ):
        """Test two concurrent child-task dispatches stay isolated.

        Given:
            An armed caller and two child tasks created via
            asyncio.create_task, each setting TENANT_ID to a distinct
            value before dispatching a routine that reads it.
        When:
            Both tasks are gathered concurrently.
        Then:
            Each routine should observe its own task's value — the
            task factory forks each child onto a fresh chain so
            mutations do not cross between siblings.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                routines.TENANT_ID.set("caller-value")

                async def _dispatch(value: str) -> str:
                    routines.TENANT_ID.set(value)
                    await asyncio.sleep(0)
                    return await routines.get_tenant_id()

                first = asyncio.create_task(_dispatch("tenant-a"))
                second = asyncio.create_task(_dispatch("tenant-b"))
                results = await asyncio.gather(first, second)

            assert results == ["tenant-a", "tenant-b"]

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestSequentialDispatchIsolation:
    @pytest.mark.asyncio
    async def test_sequential_dispatches_should_not_bleed_context(
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
            proving each dispatch captures the caller's current context
            independently
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
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
    async def test_self_dispatch_streaming_should_apply_var_mutation_between_yields(
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
            Each yielded value should reflect the caller's latest
            mutation, proving that per-frame forward-propagation
            through the self-dispatch path applies streaming var
            updates via cloudpickle.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(
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
                            routines.TENANT_ID.reset(tok)
                finally:
                    await gen.aclose()
            assert collected == values

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestDurablePoolContextPropagation:
    @pytest.mark.asyncio
    async def test_durable_pool_should_propagate_wool_context_var(
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
            scenario = default_scenario(pool_mode=PoolMode.DURABLE)
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
    when ``protocol.ChainManifest.vars`` became ``repeated ContextVar``
    with optional ``value`` and ``consumed_tokens`` fields under
    the same entry. Each test exercises a caller setup whose
    Chain carries both a current value AND a consumed-token id
    for the same var — a corner the prior shape (``map<str,bytes>``
    plus ``repeated ConsumedToken``) could not express in a single
    entry — and verifies the dispatch path round-trips both pieces
    of state to the worker.
    """

    @pytest.mark.asyncio
    async def test_merged_entry_should_ride_forward_across_async_gen_frames(
        self, credentials_map, retry_grpc_internal
    ):
        """Test per-frame propagation preserves the merged entry across anext frames.

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
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
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
class TestSerializationWarningAcrossWorkers:
    @pytest.mark.asyncio
    async def test_unpicklable_var_value_should_emit_warning_and_complete_dispatch(
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
            ``wool.SerializationWarning`` should be emitted on the
            caller side for the unpicklable var; the dispatch should
            still complete; and the routine should observe the
            propagated ``TENANT_ID`` value — primary signal preserved,
            ancillary failure surfaced as a warning
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
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
                                "always", category=wool.SerializationWarning
                            )
                            result = await routines.read_tenant_id_only()
                    finally:
                        routines.REGION.reset(region_token)
                finally:
                    unpicklable.close()
                    routines.TENANT_ID.reset(tenant_token)
            decode_warnings = [
                w for w in captured if issubclass(w.category, wool.SerializationWarning)
            ]
            assert decode_warnings, (
                f"Expected at least one SerializationWarning, got {captured!r}"
            )
            assert any("region" in str(w.message) for w in decode_warnings), (
                f"Expected the warning to name the offending var; "
                f"got {[str(w.message) for w in decode_warnings]!r}"
            )
            assert result == "survives-encode"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_unpicklable_var_value_should_raise_error_when_strict_mode(
        self, credentials_map, retry_grpc_internal
    ):
        """Test caller-side strict mode raises ChainSerializationError.

        Given:
            A caller that sets a wool.ContextVar to an unpicklable
            value, with ``warnings.simplefilter("error",
            category=wool.SerializationWarning)`` active for the
            duration of the dispatch attempt.
        When:
            The caller dispatches a routine — encode_context
            discovers the unencodable var.
        Then:
            It should raise a ``wool.ChainSerializationError`` aggregating
            ``wool.SerializationWarning`` instances on
            ``.warnings``, with the offending var named in the
            warning. The dispatch must NOT leave the caller — strict
            mode promotes the warning before the wire frame is
            constructed, and the load balancer's worker-health
            contract treats only ``RpcError`` as a health concern,
            so the error propagates unwrapped to the caller rather
            than triggering worker eviction and a
            ``NoWorkersAvailable`` fallback.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                import socket

                unpicklable = socket.socket()
                try:
                    region_token = routines.REGION.set(unpicklable)  # pyright: ignore[reportArgumentType]
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter(
                                "error", category=wool.SerializationWarning
                            )
                            with pytest.raises(wool.ChainSerializationError) as exc_info:
                                await routines.read_tenant_id_only()
                    finally:
                        routines.REGION.reset(region_token)
                finally:
                    unpicklable.close()
            warnings_list = exc_info.value.warnings
            assert all(
                isinstance(w, wool.SerializationWarning) for w in warnings_list
            ), f"Expected only SerializationWarning items, got {warnings_list!r}"
            assert any("region" in str(w) for w in warnings_list), (
                f"Expected the offending var to be named in a warning; "
                f"got {[str(w) for w in warnings_list]!r}"
            )

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestNestedDispatchMidChainMutation:
    @pytest.mark.asyncio
    async def test_outer_worker_mid_routine_mutation_should_reach_nested_inner_worker(
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
            scenario = default_scenario(
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


@pytest.mark.integration
class TestWorkerSideContextDecodeFailure:
    @pytest.mark.asyncio
    async def test_worker_side_decode_failure_should_drop_var_and_complete_dispatch(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker-side chain-manifest decode failure degrades gracefully.

        Given:
            A caller that sets ``REGION`` to a value that pickles
            cleanly on the caller but raises when unpickled on the
            worker (a version-skew shape), plus a worker-known
            ``TENANT_ID``, dispatched through an EPHEMERAL pool under
            the default warning filter
        When:
            The caller dispatches a routine that reads ``TENANT_ID`` —
            ``decode_context`` on the worker cannot decode the
            ``REGION`` entry in the dispatch's initial frame
        Then:
            The dispatch should complete, the worker should drop the
            offending ``REGION`` entry and emit a
            ``SerializationWarning``, and the routine should still
            observe its own ``TENANT_ID`` — the version-skew shape
            degrades gracefully under default filters rather than
            failing the whole dispatch.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                tenant_token = routines.TENANT_ID.set("decode-fail-tenant")
                # REGION carries a value that pickles fine caller-side
                # but detonates when the worker unpickles it.
                region_token = routines.REGION.set(routines.DecodeBomb())  # pyright: ignore[reportArgumentType]
                try:
                    with warnings.catch_warnings(record=True):
                        warnings.simplefilter(
                            "always", category=wool.SerializationWarning
                        )
                        observed = await routines.read_tenant_id_only()
                finally:
                    routines.REGION.reset(region_token)
                    routines.TENANT_ID.reset(tenant_token)
            # The undecodable REGION entry was dropped on the worker;
            # the decodable TENANT_ID still reached the routine.
            assert observed == "decode-fail-tenant"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestWorkerCrashMidDispatch:
    @pytest.mark.asyncio
    async def test_worker_crash_mid_dispatch_should_leave_caller_context_intact(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker crash mid-dispatch leaves the caller's context intact.

        Given:
            An armed caller that set ``TENANT_ID`` and an EPHEMERAL
            pool whose worker hard-exits its process mid-routine after
            mutating its own copy of ``TENANT_ID``
        When:
            The caller dispatches the crashing routine
        Then:
            The caller should observe a dispatch error (a broken
            stream surfaces as a gRPC / wool RpcError or an
            UnexpectedResponse), and its own ``TENANT_ID`` must still
            equal the value it set — no half-merged back-propagation
            rode back from the dead worker.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-pre-crash")
                try:
                    with pytest.raises((grpc.RpcError, RpcError, UnexpectedResponse)):
                        await routines.set_tenant_then_crash_worker("worker-mutation")
                    # The crashed worker's partial mutation must not
                    # have merged into the caller's context.
                    caller_value = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert caller_value == "caller-pre-crash"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestCancellationWithArmedContext:
    @pytest.mark.asyncio
    async def test_cancel_armed_dispatch_should_leave_caller_context_intact(
        self, credentials_map, retry_grpc_internal, tmp_path
    ):
        """Test cancelling a dispatch that armed a context preserves caller state.

        Given:
            An armed caller and an EPHEMERAL pool running a routine
            that sets ``TENANT_ID`` to a worker-side value, then
            sleeps — the routine has a live, mutated ``wool.ContextVar``
            when the cancellation arrives
        When:
            The caller cancels the dispatch task after the routine has
            suspended on its sleep
        Then:
            The caller's ``await`` should raise
            ``asyncio.CancelledError``, the worker-side routine should
            run its ``except`` arm (sentinel ``"cancelled"``), and the
            caller's own ``TENANT_ID`` must equal the value it set —
            the partial worker mutation is cleanly dropped under
            cancellation, not half-merged back.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            sentinel = tmp_path / "armed_cancel_sentinel.txt"
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-armed-value")
                try:
                    task = asyncio.create_task(
                        routines.set_tenant_then_sleep(
                            "worker-armed-mutation", str(sentinel), 30.0
                        )
                    )
                    # Wait deterministically for the routine to arm its
                    # context and suspend on the sleep.
                    for _ in range(150):
                        if sentinel.exists() and sentinel.read_text() == "started":
                            break
                        await asyncio.sleep(0.1)
                    else:
                        raise AssertionError(
                            "routine never wrote ``started`` — worker "
                            "startup or dispatch handshake hung"
                        )
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                    # Poll for the worker's cancel arm to run.
                    for _ in range(150):
                        if sentinel.read_text() == "cancelled":
                            break
                        await asyncio.sleep(0.1)
                    caller_value = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            assert sentinel.read_text() == "cancelled"
            # The cancelled dispatch's partial worker mutation must not
            # have corrupted the caller's own armed value.
            assert caller_value == "caller-armed-value"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestMidStreamContextDecodeFailure:
    @pytest.mark.asyncio
    async def test_mid_stream_decode_failure_should_drop_var_and_continue_stream(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a malformed context on a mid-stream frame degrades gracefully.

        Given:
            An async-generator routine echoing ``TENANT_ID`` per
            ``asend``, and a caller that — after the first frame —
            additionally sets ``REGION`` to a value that pickles
            cleanly but fails to decode on the worker
        When:
            The caller drives the generator with ``asend``, so the
            malformed ``REGION`` rides the mid-stream request frame
            and ``_step``'s per-step ``decode_context`` cannot decode
            it
        Then:
            The worker should drop the undecodable ``REGION`` entry
            and continue the stream — the echoed value still tracks
            the decodable ``TENANT_ID``, proving the mid-stream decode
            path degrades gracefully under default filters.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ASEND)
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.echo_tenant_id_on_send(2)
                try:
                    first = await gen.__anext__()
                    assert first == "ready"
                    tenant_token = routines.TENANT_ID.set("mid-stream-tenant")
                    # REGION now carries a worker-undecodable value;
                    # it rides the asend frame's context.
                    region_token = routines.REGION.set(routines.DecodeBomb())  # pyright: ignore[reportArgumentType]
                    try:
                        with warnings.catch_warnings(record=True):
                            warnings.simplefilter(
                                "always", category=wool.SerializationWarning
                            )
                            echoed = await gen.asend(None)
                    finally:
                        routines.REGION.reset(region_token)
                        routines.TENANT_ID.reset(tenant_token)
                finally:
                    await gen.aclose()
            # The malformed REGION was dropped; the decodable
            # TENANT_ID still reached the mid-stream worker frame.
            assert echoed == "mid-stream-tenant"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestConcurrentDispatchesShareParentVar:
    @pytest.mark.asyncio
    async def test_concurrent_fan_out_should_read_parent_set_var_without_contamination(
        self, credentials_map, retry_grpc_internal
    ):
        """Test concurrent fan-out dispatches read a parent-set var cleanly.

        Given:
            A parent that sets ``TENANT_ID`` once (the request-scoped
            tenant-id pattern) and an EPHEMERAL pool, then fans out
            two concurrent child-task dispatches that each read the
            var and mutate their own copy
        When:
            Both dispatches are gathered
        Then:
            Each dispatch should observe the parent-set value, and
            after the gather the parent's own ``TENANT_ID`` must be
            unchanged — neither child's back-propagated mutation
            cross-contaminates the other or the parent.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("request-scoped-tenant")
                try:

                    async def _fan_out(suffix: str) -> tuple[str, str]:
                        # Each child reads the parent-set value, then
                        # mutates its own forked copy.
                        before = await routines.get_tenant_id()
                        worker_after = await routines.mutate_and_read_tenant_id()
                        return before, worker_after

                    results = await asyncio.gather(
                        asyncio.create_task(_fan_out("a")),
                        asyncio.create_task(_fan_out("b")),
                    )
                    parent_after = routines.TENANT_ID.get()
                finally:
                    routines.TENANT_ID.reset(token)
            # Both children saw the parent's request-scoped value.
            assert results[0][0] == "request-scoped-tenant"
            assert results[1][0] == "request-scoped-tenant"
            # Each child's worker mutation is its own.
            assert results[0][1] == "mutated_on_worker"
            assert results[1][1] == "mutated_on_worker"
            # The parent's var is untouched by either child's
            # back-propagation — the child tasks fork the chain.
            assert parent_after == "request-scoped-tenant"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestAsyncGenSetAndResetAcrossYield:
    @pytest.mark.asyncio
    async def test_async_gen_set_then_reset_should_back_propagate_per_yield(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an async-gen set+reset across a yield back-propagates per frame.

        Given:
            An async-generator routine that sets ``TENANT_ID`` and
            yields, then resets the var via the set's own Token and
            yields again
        When:
            The caller iterates the generator, reading its own
            ``TENANT_ID`` after each yield
        Then:
            After the first yield the caller should observe the
            worker's set value, and after the second yield it should
            observe the var reverted to its default — per-yield
            back-propagation carries both the set and the reset.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                caller_reads: list[str] = []
                gen = routines.set_and_reset_tenant_across_yield("worker-set-value")
                try:
                    async for marker in gen:
                        caller_reads.append(routines.TENANT_ID.get())
                        assert marker in ("set", "reset")
                finally:
                    await gen.aclose()
            # After the first yield the set is visible; after the
            # second the reset reverted the var to its default.
            assert caller_reads == ["worker-set-value", "unknown"]

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestCopyContextWidthAcrossWorkers:
    @pytest.mark.asyncio
    async def test_copy_context_should_enumerate_one_plus_n_after_dispatch(
        self, credentials_map, retry_grpc_internal
    ):
        """Test copy_context enumerates 1+N wool variables after a dispatch.

        Given:
            A caller that has entered a pool — which arms its context — and
            a routine that counts wool-owned `contextvars.ContextVar`s
            visible in a worker-side `contextvars.copy_context`.
        When:
            The caller binds two more `wool.ContextVar`s and dispatches the
            counting routine.
        Then:
            The caller's own copy_context should enumerate exactly two wool
            variables — the chain variable plus one arming backing marker —
            confirming that entering a proxy arms the context; and the
            worker — running with the propagated marker plus the two user
            variables — should report ``1 + 3`` (the chain variable plus
            one backing per bound var).
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                # Entering the pool arms the context, so the caller carries
                # the chain variable plus one arming backing marker.
                armed = sorted(
                    var.name
                    for var in contextvars.copy_context()
                    if var.name.startswith("__wool")
                )

                tenant_token = routines.TENANT_ID.set("width-tenant")
                region_token = routines.REGION.set("width-region")
                try:
                    worker_count = await routines.count_wool_context_vars()
                finally:
                    routines.REGION.reset(region_token)
                    routines.TENANT_ID.reset(tenant_token)
            assert len(armed) == 2
            assert "__wool_chain__" in armed
            # 1 chain variable + 3 backing variables: the arming marker plus
            # the two bound wool.ContextVars propagated to the worker.
            assert worker_count == 4

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestRoutineLookupErrorBackPropagation:
    @pytest.mark.asyncio
    async def test_get_on_unbound_default_less_var_should_surface_lookup_error(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a routine LookupError on an unbound var surfaces to the caller.

        Given:
            A routine that declares a default-less ``wool.ContextVar``
            and calls ``get()`` on it while it is unbound
        When:
            The caller dispatches the routine
        Then:
            The ``LookupError`` raised inside the routine should
            surface to the caller's ``await`` through the exception
            back-propagation path.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                with pytest.raises(LookupError):
                    await routines.read_unbound_default_less_var(
                        "synthetic_unbound_ns", "never_bound_key"
                    )

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestMultiWorkerFanOutWithContext:
    @pytest.mark.asyncio
    async def test_same_value_should_fan_out_to_distinct_workers_independently(
        self, credentials_map, retry_grpc_internal
    ):
        """Test one armed caller fans the same value to distinct workers.

        Given:
            An armed caller that set ``TENANT_ID`` once and an
            EPHEMERAL pool with two worker processes
        When:
            The caller fans out two concurrent dispatches that each
            mutate their worker-side copy of the var
        Then:
            Both dispatches should observe the caller's value as
            their starting point and each should report its own
            worker-side mutation — each worker mounts the caller's
            context independently with no shared mutable state.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("fan-out-seed")
                try:
                    seen = await asyncio.gather(
                        routines.get_tenant_id(),
                        routines.get_tenant_id(),
                    )
                    mutated = await asyncio.gather(
                        routines.mutate_and_read_tenant_id(),
                        routines.mutate_and_read_tenant_id(),
                    )
                finally:
                    routines.TENANT_ID.reset(token)
            # Both workers mounted the caller's seed independently.
            assert seen == ["fan-out-seed", "fan-out-seed"]
            assert mutated == ["mutated_on_worker", "mutated_on_worker"]

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestChainContentionAcrossDispatch:
    @pytest.mark.asyncio
    async def test_off_owner_thread_var_access_should_raise_chain_contention(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker re-entering its armed chain off-thread raises.

        Given:
            An EPHEMERAL pool running a routine that sets a
            ``wool.ContextVar`` — arming its chain on the worker loop
            thread — then reads the same var from a worker thread via
            ``asyncio.to_thread``, which copies the armed chain into
            the executor thread
        When:
            The caller dispatches the routine
        Then:
            The off-owner-thread ``get()`` should raise
            ``wool.ChainContention`` and that exception should
            surface to the caller's ``await`` through the exception
            back-propagation path.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                with pytest.raises(wool.ChainContention):
                    await routines.reenter_armed_chain_off_owner_thread(
                        "armed-on-loop-thread"
                    )

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_wool_to_thread_should_fork_armed_chain_off_owner_thread(
        self, credentials_map, retry_grpc_internal
    ):
        """Test wool.to_thread forks a worker's armed chain off-thread cleanly.

        Given:
            An EPHEMERAL pool running a routine that sets a
            ``wool.ContextVar`` — arming its chain on the worker loop
            thread — then reads the same var from a worker thread via
            ``wool.to_thread``, the supported context-propagating
            offload that forks the chain onto a fresh, detached chain
            owned by the worker thread.
        When:
            The caller dispatches the routine.
        Then:
            The off-thread ``get()`` should return the value the
            routine set — the fork copies the caller's bindings under a
            new chain UUID owned by the worker thread, so the read
            re-arms cleanly rather than tripping
            ``wool.ChainContention``.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.read_var_off_thread_via_wool_to_thread(
                    "forked-off-thread"
                )
                assert result == "forked-off-thread"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_interleaved_async_gen_dispatches_should_share_caller_chain(
        self, credentials_map, retry_grpc_internal
    ):
        """Test interleaving two async-generator dispatches on one chain.

        Given:
            An armed caller whose context carries a single chain id and
            an EPHEMERAL pool, with two concurrently-open async-generator
            dispatches of a routine that yields the worker-side chain id.
        When:
            The caller advances the two generators in strict alternation
            with ``anext`` from its own single task, never advancing
            both at once.
        Then:
            It should drive both to exhaustion without raising
            wool.ChainContention, and every yielded value should
            equal the caller's chain id — serialized interleaving never
            runs the shared chain from two runners at once.
        """

        async def body():
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
                pool_mode=PoolMode.EPHEMERAL,
            )
            async with build_pool_from_scenario(scenario, credentials_map):
                # Arrange
                routines.TENANT_ID.set("armed")
                caller = wool.__chain__.get(None)
                assert caller is not None
                a = routines.stream_chain_id_hex(3)
                b = routines.stream_chain_id_hex(3)

                # Act
                collected: list[str] = []
                try:
                    for _ in range(3):
                        collected.append(await anext(a))
                        collected.append(await anext(b))
                finally:
                    await a.aclose()
                    await b.aclose()

            # Assert
            assert collected == [caller.id.hex] * 6

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestProxyCollisionAcrossDispatch:
    @pytest.mark.asyncio
    async def test_worker_proxy_collision_should_report_task_contention(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a proxy collision inside a dispatched routine fires on the worker.

        Given:
            A DEFAULT pool running a routine that spawns two tasks sharing
            one fresh `contextvars.Context` on the worker loop, each
            entering its own `wool.WorkerProxy`, and catches the second
            entry's contention.
        When:
            The caller dispatches the routine.
        Then:
            It should return ``"task"`` — the contention guard fired inside
            the dispatched routine on the worker, and the kind crossed the
            wire as a plain string.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.report_proxy_collision_in_shared_context()
            assert result == "task"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_proxy_collision_should_surface_chain_contention(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a re-raised worker proxy collision reaches the caller intact.

        Given:
            A DEFAULT pool running a routine that drives a worker-side
            `wool.WorkerProxy` collision and re-raises the resulting
            `wool.ChainContention` instead of catching it.
        When:
            The caller dispatches the routine.
        Then:
            The caller should catch a `wool.ChainContention` with kind
            ``"task"`` — the cross-task contention survives the
            worker-to-caller exception channel as itself rather than
            degrading to a plain `RuntimeError`.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                with pytest.raises(wool.ChainContention) as exc_info:
                    await routines.raise_proxy_collision_in_shared_context()
            assert exc_info.value.kind == "task"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_separate_task_proxy_entry_should_not_raise(
        self, credentials_map, retry_grpc_internal
    ):
        """Test concurrent forked proxy entries inside a routine do not contend.

        Given:
            A DEFAULT pool running a routine that enters two
            `wool.WorkerProxy` contexts in separate forked tasks
            (`asyncio.gather`) on the worker.
        When:
            The caller dispatches the routine.
        Then:
            It should return ``"ok"`` with no `wool.ChainContention` — the
            task factory forks a fresh, child-owned chain per task, so
            legitimate concurrent proxy use on the worker never trips.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.enter_proxies_in_separate_tasks_on_worker()
            assert result == "ok"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_worker_proxy_collision_should_report_contention(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker proxy collision fires under the async-gen driver.

        Given:
            A DEFAULT pool running an async-generator routine that yields
            ``"ready"``, then drives a worker-side `wool.WorkerProxy`
            collision and yields the contention kind.
        When:
            The caller iterates the dispatched generator.
        Then:
            It should yield ``["ready", "task"]`` — the guard fires
            identically under the async-generator dispatch driver and the
            kind streams back over the wire.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.stream_proxy_collision_in_shared_context()
                collected = [value async for value in gen]
            assert collected == ["ready", "task"]

        await retry_grpc_internal(body)
