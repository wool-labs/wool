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
    async def test_token_reset_on_worker_should_consume_it_for_the_caller(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token reset on a worker is consumed for the caller too.

        Given:
            A caller-minted TENANT_ID token dispatched to a worker as an
            argument, where the worker resets it.
        When:
            The caller resets the same token after the dispatch returns.
        Then:
            It should raise RuntimeError — the worker's reset propagated
            the consumed state back on the response frame, enforcing
            single-use across processes.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("acme-corp")
                observed = await routines.reset_tenant_id_token(token)
                assert observed == "unknown"
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_orphan_token_should_transport_losslessly_without_raising(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an orphaned token crosses the wire and back without raising.

        Given:
            A TENANT_ID token minted inside a sibling stdlib context (via
            copy_context().run) that the dispatching context does not
            continue, passed to a routine that inspects but does not reset
            it.
        When:
            The routine is dispatched and the token travels to the worker
            and back in the return value.
        Then:
            No error should surface anywhere in transport, and the returned
            token should be a usable object naming the original variable —
            failure is deferred exclusively to a reset attempt, which
            raises stdlib's different-Context ValueError.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                ambient = routines.TENANT_ID.set("ambient")
                try:
                    sibling = contextvars.copy_context()
                    orphan = sibling.run(routines.TENANT_ID.set, "sibling")
                    returned, var_name = await routines.describe_tenant_id_token(orphan)
                    assert var_name == routines.TENANT_ID.name
                    assert returned.var.name == routines.TENANT_ID.name
                    with pytest.raises(
                        ValueError, match="was created in a different Context"
                    ):
                        routines.TENANT_ID.reset(returned)
                finally:
                    routines.TENANT_ID.reset(ambient)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_orphan_token_reset_on_worker_should_raise_different_context(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker cannot reset a token minted in a sibling context.

        Given:
            A TENANT_ID token minted inside a sibling stdlib context sharing
            the dispatching context's chain — so the chain id matches but
            the minting context is not the one the worker continues.
        When:
            A routine dispatched from the ambient context resets the token
            on the worker.
        Then:
            The reset should raise stdlib's "was created in a different
            Context" ValueError — token ownership follows the exact minting
            context, not the chain.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                ambient = routines.TENANT_ID.set("ambient")
                try:
                    sibling = contextvars.copy_context()
                    orphan = sibling.run(routines.TENANT_ID.set, "sibling")
                    _, kind, message = await routines.reset_tenant_id_token_report(
                        orphan
                    )
                    assert kind == "ValueError"
                    assert "was created in a different Context" in message
                finally:
                    routines.TENANT_ID.reset(ambient)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_minted_token_should_be_resettable_by_the_caller(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token minted on a worker resets in the awaiting caller.

        Given:
            A routine that sets TENANT_ID on the worker and returns the
            minted token to the awaiting caller.
        When:
            The caller resets the returned token.
        Then:
            The set should have propagated back to the caller (awaited
            dispatch continues the caller's context), and the reset should
            succeed there, restoring the pre-call default — stdlib parity
            with an awaited local coroutine performing the same set.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = await routines.set_tenant_id_and_return_token("worker-set")
                assert routines.TENANT_ID.get() == "worker-set"
                routines.TENANT_ID.reset(token)
                assert routines.TENANT_ID.get() == "unknown"

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


@pytest.mark.integration
class TestTokenSingleUseAcrossProcesses:
    @pytest.mark.asyncio
    async def test_second_reset_on_worker_should_report_used_token(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token consumed on a worker refuses a second worker reset.

        Given:
            A caller-minted TENANT_ID token already consumed by a
            dispatched reset on a DEFAULT pool.
        When:
            The caller dispatches a second reset attempt of the same
            token via the catch-and-report routine.
        Then:
            It should report RuntimeError with the single-use message,
            and the caller's own local reset should raise the same
            RuntimeError — the consumed state is authoritative on both
            sides of the wire.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("acme-corp")
                observed = await routines.reset_tenant_id_token(token)
                assert observed == "unknown"
                _, kind, message = await routines.reset_tenant_id_token_report(token)
                assert kind == "RuntimeError"
                assert "has already been used once" in message
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_consumed_token_dispatched_should_report_used_gate_first(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a locally consumed token fails the used gate on the worker.

        Given:
            A caller that mints a TENANT_ID token and consumes it with a
            successful LOCAL reset before any dispatch.
        When:
            The caller dispatches the catch-and-report reset routine
            with the consumed token.
        Then:
            It should report RuntimeError — not ValueError — proving the
            used gate fires before the context gate on the receiving
            side of the wire.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("acme-corp")
                routines.TENANT_ID.reset(token)
                assert routines.TENANT_ID.get() == "unknown"
                _, kind, message = await routines.reset_tenant_id_token_report(token)
                assert kind == "RuntimeError"
                assert kind != "ValueError"
                assert "has already been used once" in message

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_consumed_token_relayed_between_workers_should_report_used(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the consumption ledger reaches a worker that never saw the reset.

        Given:
            An EPHEMERAL pool with two workers and a TENANT_ID token
            consumed by a dispatched reset that reported one worker pid.
        When:
            The caller dispatches the report routine repeatedly (up to
            eight times) so at least one dispatch is likely to land on
            the other worker process.
        Then:
            Every post-consumption dispatch should report RuntimeError
            with the single-use message — the ledger rides every frame in
            both directions, so a worker that never performed the reset
            still refuses the token.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("acme-corp")
                first_pid, first_kind, _ = await routines.reset_tenant_id_token_report(
                    token
                )
                assert first_kind == "ok"
                # Round-robin over the two-worker pool reaches the other
                # worker within a few dispatches; keep dispatching until a
                # different pid confirms the cross-worker relay was exercised.
                observed_other_pid = False
                for _ in range(8):
                    pid, kind, message = await routines.reset_tenant_id_token_report(
                        token
                    )
                    assert kind == "RuntimeError"
                    assert "has already been used once" in message
                    if pid != first_pid:
                        observed_other_pid = True
                        break
                assert observed_other_pid, (
                    "the report never landed on a second worker, so the "
                    "cross-worker ledger relay was not exercised"
                )

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_returned_used_token_should_carry_used_state_to_caller(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token reset on the worker returns visibly used.

        Given:
            A caller-minted TENANT_ID token dispatched to a routine that
            resets it and returns the token itself.
        When:
            The caller inspects the returned token and attempts to reset
            both the returned copy and the original.
        Then:
            The returned token's repr should carry the ``used`` marker,
            and both reset attempts should raise the single-use
            RuntimeError — every copy anywhere reads as consumed.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("acme-corp")
                returned = await routines.reset_and_return_tenant_id_token(token)
                assert "used " in repr(returned)
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(returned)
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_re_set_after_remote_consumption_should_mint_fresh_token(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the var stays fully usable after a token is consumed remotely.

        Given:
            A TENANT_ID token consumed by a dispatched reset, then a
            fresh caller-side set minting a second token.
        When:
            The caller dispatches a reset of the second token and then
            retries it locally.
        Then:
            The remote reset should succeed (restoring the default), the
            local retry should raise the single-use RuntimeError —
            consumption is per-token, not per-var — and the first token
            should stay consumed: its remote use survives the fresh set,
            so a local reset of it must also raise the single-use
            RuntimeError.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token1 = routines.TENANT_ID.set("first")
                await routines.reset_tenant_id_token(token1)
                token2 = routines.TENANT_ID.set("fresh")
                observed = await routines.reset_tenant_id_token(token2)
                assert observed == "unknown"
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token2)
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token1)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_used_token_transport_should_be_lossless(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a consumed token still crosses the wire without raising.

        Given:
            A TENANT_ID token consumed by a dispatched reset on a
            DEFAULT pool.
        When:
            The caller passes the used token to a routine that inspects
            it without resetting and returns it.
        Then:
            No error should surface in transport, the returned token
            should be a usable object naming the original variable, and
            a caller reset of the returned copy should raise the
            single-use RuntimeError.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("acme-corp")
                await routines.reset_tenant_id_token(token)
                returned, var_name = await routines.describe_tenant_id_token(token)
                assert var_name == routines.TENANT_ID.name
                assert returned.var.name == routines.TENANT_ID.name
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(returned)

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestTokenOwnershipTopologies:
    @pytest.mark.asyncio
    async def test_create_task_child_dispatch_should_report_different_context(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token dispatched from a child task arrives orphaned.

        Given:
            A parent-minted TENANT_ID token and an asyncio child task
            created via ``create_task`` that dispatches the
            catch-and-report reset routine with it — the fork drops the
            chain's live token ids.
        When:
            The child's dispatch reports and the parent then resets the
            token locally.
        Then:
            The worker should report stdlib's different-Context
            ValueError, and the parent's local reset should still
            succeed afterward — the failed remote attempt did not
            consume the token.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("parent-mint")

                async def _child():
                    return await routines.reset_tenant_id_token_report(token)

                _, kind, message = await asyncio.create_task(_child())
                assert kind == "ValueError"
                assert "was created in a different Context" in message
                routines.TENANT_ID.reset(token)
                assert routines.TENANT_ID.get() == "unknown"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_unarmed_dispatcher_sibling_token_should_report_different_context(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an orphan token from a never-armed dispatcher stays an orphan.

        Given:
            A dispatching task that never sets any wool var and a
            TENANT_ID token minted inside a stdlib ``copy_context``
            sibling.
        When:
            The caller dispatches the describe routine and then the
            catch-and-report reset routine with the orphan.
        Then:
            Transport should succeed, the reset should report stdlib's
            different-Context ValueError, and a subsequent dispatch of
            ``get_tenant_id`` should still observe the default — the
            sibling's set never leaked into the dispatching context.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                sibling = contextvars.copy_context()
                orphan = sibling.run(routines.TENANT_ID.set, "sibling-mint")
                returned, var_name = await routines.describe_tenant_id_token(orphan)
                assert var_name == routines.TENANT_ID.name
                assert returned.var.name == routines.TENANT_ID.name
                _, kind, message = await routines.reset_tenant_id_token_report(orphan)
                assert kind == "ValueError"
                assert "was created in a different Context" in message
                assert await routines.get_tenant_id() == "unknown"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_twice_travelled_orphan_should_stay_usable(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an orphan token survives two round trips unchanged.

        Given:
            A TENANT_ID token minted in a stdlib ``copy_context``
            sibling and already round-tripped once through the describe
            routine.
        When:
            The returned token is dispatched through the describe
            routine a second time and the caller finally resets the
            twice-travelled copy.
        Then:
            Both trips should transport losslessly, and the final reset
            should raise stdlib's different-Context ValueError — orphan
            state is preserved, never corrupted or upgraded, across
            repeated travel.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                ambient = routines.TENANT_ID.set("ambient")
                try:
                    sibling = contextvars.copy_context()
                    orphan = sibling.run(routines.TENANT_ID.set, "sibling")
                    first, _ = await routines.describe_tenant_id_token(orphan)
                    second, var_name = await routines.describe_tenant_id_token(first)
                    assert var_name == routines.TENANT_ID.name
                    assert second.var.name == routines.TENANT_ID.name
                    with pytest.raises(
                        ValueError, match="was created in a different Context"
                    ):
                        routines.TENANT_ID.reset(second)
                finally:
                    routines.TENANT_ID.reset(ambient)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_minted_orphan_should_refuse_caller_reset(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker-minted sibling token cannot be reset by the caller.

        Given:
            A routine that mints a TENANT_ID token inside a stdlib
            ``copy_context`` sibling on the worker and returns it.
        When:
            The caller attempts to reset the returned token.
        Then:
            It should raise stdlib's different-Context ValueError, and
            the caller's own TENANT_ID should be unchanged — the
            sibling-scoped set never back-propagated.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                before = routines.TENANT_ID.get()
                orphan = await routines.mint_orphan_tenant_id_token()
                with pytest.raises(
                    ValueError, match="was created in a different Context"
                ):
                    routines.TENANT_ID.reset(orphan)
                assert routines.TENANT_ID.get() == before

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_gather_sibling_dispatches_should_orphan_token_for_both(
        self, credentials_map, retry_grpc_internal
    ):
        """Test gathered sibling dispatches both see the token as orphaned.

        Given:
            An EPHEMERAL pool, a caller-minted TENANT_ID token, and two
            reset-report dispatches gathered concurrently — each gather
            member runs in a forked child task whose chain drops live
            token ids.
        When:
            Both siblings report, then the minting task itself awaits a
            direct reset dispatch and finally retries locally.
        Then:
            Both siblings should report the different-Context
            ValueError without consuming the token, the direct awaited
            dispatch should succeed (continuation of the minting
            context), and the final local reset should raise the
            single-use RuntimeError.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("gather-mint")
                reports = await asyncio.gather(
                    routines.reset_tenant_id_token_report(token),
                    routines.reset_tenant_id_token_report(token),
                )
                for _, kind, message in reports:
                    assert kind == "ValueError"
                    assert "was created in a different Context" in message
                observed = await routines.reset_tenant_id_token(token)
                assert observed == "unknown"
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_mint_with_armed_caller_should_restore_pre_call_value(
        self, credentials_map, retry_grpc_internal
    ):
        """Test resetting a worker-minted token restores the caller's own value.

        Given:
            A caller that armed TENANT_ID with its own value before
            dispatching a routine that sets the var on the worker and
            returns the minted token.
        When:
            The caller resets the returned token.
        Then:
            The var should restore to the caller's pre-call value — not
            the constructor default — because the worker's token
            captured the propagated caller binding as its old value.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                pre_token = routines.TENANT_ID.set("caller-pre")
                try:
                    returned = await routines.set_tenant_id_and_return_token(
                        "worker-set"
                    )
                    assert routines.TENANT_ID.get() == "worker-set"
                    routines.TENANT_ID.reset(returned)
                    assert routines.TENANT_ID.get() == "caller-pre"
                finally:
                    routines.TENANT_ID.reset(pre_token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_worker_minted_token_reset_from_child_task_should_raise(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a child task cannot reset a token owned by the parent's context.

        Given:
            A token minted on the worker and returned to the awaiting
            caller (whose context continues the minting context), and an
            asyncio child task created via ``create_task``.
        When:
            The child performs a LOCAL reset of the token, then the
            parent resets it after the child completes.
        Then:
            The child's reset should raise stdlib's different-Context
            ValueError (the fork drops live ids), and the parent's reset
            should succeed, restoring the default.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                returned = await routines.set_tenant_id_and_return_token("worker-set")
                assert routines.TENANT_ID.get() == "worker-set"

                async def _child():
                    with pytest.raises(
                        ValueError, match="was created in a different Context"
                    ):
                        routines.TENANT_ID.reset(returned)
                    return True

                assert await asyncio.create_task(_child())
                routines.TENANT_ID.reset(returned)
                assert routines.TENANT_ID.get() == "unknown"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_sequential_worker_mints_should_reset_independent_old_values(
        self, credentials_map, retry_grpc_internal
    ):
        """Test two worker-minted tokens restore their own captured old values.

        Given:
            Two sequential dispatches that each set TENANT_ID on the
            worker ("first" then "second") and return their minted
            tokens, plus an inline stdlib contextvars baseline computing
            the same set/set/reset/reset sequence.
        When:
            The caller resets the first token, reads the var, resets the
            second token, and reads again.
        Then:
            Each token should restore its own captured old value —
            resetting token1 restores the pre-call state, resetting
            token2 restores "first" — matching the stdlib baseline
            exactly, with the final read equal to "first".
        """

        # Arrange, act, & assert
        async def body():
            # Inline stdlib baseline for the same FIFO reset order.
            baseline_var: contextvars.ContextVar[str] = contextvars.ContextVar(
                "fifo_baseline", default="unknown"
            )
            baseline_token1 = baseline_var.set("first")
            baseline_token2 = baseline_var.set("second")
            baseline_var.reset(baseline_token1)
            baseline_after_first = baseline_var.get()
            baseline_var.reset(baseline_token2)
            baseline_final = baseline_var.get()

            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token1 = await routines.set_tenant_id_and_return_token("first")
                token2 = await routines.set_tenant_id_and_return_token("second")
                assert routines.TENANT_ID.get() == "second"
                routines.TENANT_ID.reset(token1)
                after_first = routines.TENANT_ID.get()
                routines.TENANT_ID.reset(token2)
                final = routines.TENANT_ID.get()

            assert after_first == baseline_after_first == "unknown"
            assert final == baseline_final == "first"

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestTokenNestedDispatch:
    @pytest.mark.asyncio
    async def test_relay_reset_should_restore_and_consume_for_caller(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token consumed two hops away restores and consumes for the caller.

        Given:
            An EPHEMERAL pool with two workers and a caller-minted
            TENANT_ID token dispatched through a relay routine that
            nested-dispatches the actual reset.
        When:
            The relay returns and the caller reads its own var and then
            retries the reset locally.
        Then:
            The relay should return the restored default, the caller
            should observe the restored value, and the local retry
            should raise the single-use RuntimeError — the consumed
            state crossed both hops back.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(pool_mode=PoolMode.EPHEMERAL)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("relay-mint")
                observed = await routines.relay_reset_tenant_id_token(token)
                assert observed == "unknown"
                assert routines.TENANT_ID.get() == "unknown"
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_nested_reset_then_outer_retry_should_report_used_token(
        self, credentials_map, retry_grpc_internal
    ):
        """Test the ledger rides the inner response frame back to the outer worker.

        Given:
            A caller-minted TENANT_ID token dispatched to a routine that
            consumes it via a nested dispatch and then retries the reset
            locally on the outer worker.
        When:
            The routine returns its report and the caller retries the
            reset locally.
        Then:
            The inner reset should have restored the default, the outer
            retry should report the single-use RuntimeError (the ledger
            rode the inner response frame back one hop), and the
            caller's own retry should raise the same RuntimeError.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("nested-mint")
                inner, kind, message = await routines.nested_reset_then_report(token)
                assert inner == "unknown"
                assert kind == "RuntimeError"
                assert "already been used once" in message
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_outer_mint_inner_reset_should_consume_for_the_outer_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker-minted token consumed downstream reads used upstream.

        Given:
            A routine that mints a TENANT_ID token on the outer worker,
            has an awaited nested dispatch consume it, and then retries
            the reset locally.
        When:
            The caller dispatches the routine and reads its own var
            afterward.
        Then:
            The inner dispatch should observe the restored default, the
            outer retry should report the single-use RuntimeError, and
            the caller's TENANT_ID should be unchanged — the mint and
            reset cancelled out before back-propagation.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario()
            async with build_pool_from_scenario(scenario, credentials_map):
                before = routines.TENANT_ID.get()
                inner, kind, message = await routines.outer_mint_inner_reset()
                assert inner == "unknown"
                assert kind == "RuntimeError"
                assert "already been used once" in message
                assert routines.TENANT_ID.get() == before

        await retry_grpc_internal(body)


@pytest.mark.integration
class TestTokenStreaming:
    @pytest.mark.asyncio
    async def test_stream_reset_should_consume_the_token_mid_stream(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a mid-stream reset consumes the caller's token before exhaustion.

        Given:
            A caller-minted TENANT_ID token passed to an async-generator
            routine that yields the current value, resets the token, and
            yields the restored value.
        When:
            The caller drives the first two yields and attempts a local
            reset before exhausting the generator.
        Then:
            The first yield should observe the caller's value, the
            second the restored default, and the local reset should
            already raise the single-use RuntimeError — the ledger rode
            the step frame back mid-stream.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("caller-set")
                gen = routines.stream_reset_tenant_id_token(token)
                try:
                    first = await gen.__anext__()
                    assert first == "caller-set"
                    second = await gen.__anext__()
                    assert second == "unknown"
                    with pytest.raises(RuntimeError, match="already been used once"):
                        routines.TENANT_ID.reset(token)
                    with pytest.raises(StopAsyncIteration):
                        await gen.__anext__()
                finally:
                    await gen.aclose()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_token_yielded_mid_stream_should_survive_remounts_once(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a worker-minted token yielded early stays resettable exactly once.

        Given:
            An async-generator routine that sets TENANT_ID, yields the
            minted token on its first yield, and yields the current
            value three more times across suspensions.
        When:
            The caller collects the token, exhausts the stream, resets
            the token, and retries the reset.
        Then:
            After exhaustion the caller should observe the worker-set
            value, the first reset should succeed restoring the default,
            and the retry should raise the single-use RuntimeError — the
            token's anchor survived every per-yield re-mount exactly
            once.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.set_tenant_id_and_yield_token("stream-set", 3)
                try:
                    token = await gen.__anext__()
                    async for _ in gen:
                        pass
                finally:
                    await gen.aclose()
                assert routines.TENANT_ID.get() == "stream-set"
                routines.TENANT_ID.reset(token)
                assert routines.TENANT_ID.get() == "unknown"
                with pytest.raises(RuntimeError, match="already been used once"):
                    routines.TENANT_ID.reset(token)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_stream_double_reset_should_raise_on_the_second_step(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a second reset of the same token inside a stream raises mid-stream.

        Given:
            A caller-minted TENANT_ID token passed to an async-generator
            routine that resets it before each of two yields.
        When:
            The caller drives the first yield and then requests the
            second.
        Then:
            The first yield should arrive with the caller observing the
            restored default, and the second ``__anext__`` should raise
            the single-use RuntimeError surfaced from the worker's
            in-stream violation.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ANEXT)
            async with build_pool_from_scenario(scenario, credentials_map):
                token = routines.TENANT_ID.set("twice-mint")
                gen = routines.stream_reset_token_twice(token)
                try:
                    first = await gen.__anext__()
                    assert first == "first"
                    assert routines.TENANT_ID.get() == "unknown"
                    with pytest.raises(RuntimeError, match="already been used once"):
                        await gen.__anext__()
                finally:
                    await gen.aclose()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_token_sent_via_asend_should_reset_on_the_worker(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a token shipped on an asend frame resets and consumes correctly.

        Given:
            An async-generator routine that yields ``"ready"``, receives
            a token via ``asend``, resets it, and yields the restored
            value — with the caller minting the token only after the
            stream is already open.
        When:
            The caller sends the token mid-stream and reads its own var
            after the echo.
        Then:
            The echoed value should be the restored default, the caller
            should observe the restored value, and a local reset should
            raise the single-use RuntimeError — the asend frame carried
            the live token out and the ledger rode the step frame back.
        """

        # Arrange, act, & assert
        async def body():
            scenario = default_scenario(shape=RoutineShape.ASYNC_GEN_ASEND)
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.stream_recv_and_reset()
                try:
                    ready = await gen.__anext__()
                    assert ready == "ready"
                    token = routines.TENANT_ID.set("mid")
                    echoed = await gen.asend(token)
                    assert echoed == "unknown"
                    assert routines.TENANT_ID.get() == "unknown"
                    with pytest.raises(RuntimeError, match="already been used once"):
                        routines.TENANT_ID.reset(token)
                finally:
                    await gen.aclose()

        await retry_grpc_internal(body)
