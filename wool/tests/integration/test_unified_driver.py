"""Integration tests for the unified worker/driver dispatch shape (#187).

Each test pins one happy-path shape from the unified-driver design.
Scenarios are constructed explicitly per shape rather than threaded
through the pairwise covering array — the goal is to lock the
observable behavior of every distinct dispatch verb against a real
gRPC + worker-process boundary.

Tests are observable at the proxy / async-iterator boundary: routines
are dispatched through ``WorkerPool``, results are read off the proxy
via ``await``, ``async for``, ``asend``, ``athrow``, or ``aclose``,
and back-propagated wool.ContextVar state is observed via the
caller's own ``var.get()`` after the dispatch returns.
"""

import asyncio
import socket
import warnings

import pytest

import wool
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import UnexpectedResponse

from . import routines
from .conftest import BackpressureMode
from .conftest import PoolMode
from .conftest import RoutineShape
from .conftest import StrictWarnings
from .conftest import build_pool_from_scenario
from .conftest import default_scenario


@pytest.mark.integration
class TestUnifiedDriverShape:
    @pytest.mark.asyncio
    async def test_coroutine_dispatch_completes_across_process_boundary(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a single coroutine dispatch round-trips across the worker boundary.

        Given:
            A coroutine routine with no caller-side wool.ContextVar
            mutations and an EPHEMERAL pool (cross-process worker so
            the dispatch exercises the real gRPC wire and cloudpickle
            serialization path).
        When:
            The caller dispatches the routine once and awaits the
            result.
        Then:
            It should return the routine's result and exit cleanly,
            with no residual back-propagated state.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.EPHEMERAL,
            )
            tenant_before = routines.TENANT_ID.get()
            region_before = routines.REGION.get()

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.add(2, 3)

            # Assert
            assert result == 5
            # The routine never set TENANT_ID/REGION on the worker, so
            # the caller's context must equal its pre-dispatch value
            # — the back-propagation path is silent when the worker
            # made no mutations.
            assert routines.TENANT_ID.get() == tenant_before
            assert routines.REGION.get() == region_before

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_dispatch_exhausts_after_single_yield(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an async-generator yielding once terminates with StopAsyncIteration.

        Given:
            An async-generator routine that yields a single value and
            an EPHEMERAL pool (cross-process worker).
        When:
            The caller drives the proxy manually with ``__anext__``
            past the single yield.
        Then:
            The first ``__anext__`` should yield the value and the
            second should raise :class:`StopAsyncIteration`.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT_SINGLE,
                pool_mode=PoolMode.EPHEMERAL,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.gen_range_one_yield()
                first = await gen.__anext__()
                with pytest.raises(StopAsyncIteration):
                    await gen.__anext__()

            # Assert
            assert first == 0

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_dispatch_exhausts_after_multiple_yields(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an async-generator that yields N times exhausts after N values.

        Given:
            An async-generator routine that yields integers
            ``0..N-1`` and an EPHEMERAL pool (cross-process worker).
        When:
            The caller iterates the proxy with ``async for`` until
            exhaustion.
        Then:
            The caller should observe exactly the yielded sequence and
            a clean termination of the iterator.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
                pool_mode=PoolMode.EPHEMERAL,
            )
            collected: list[int] = []

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                async for value in routines.gen_range(4):
                    collected.append(value)

            # Assert
            assert collected == [0, 1, 2, 3]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_dispatch_with_send_decodes_payload_under_attached_context(
        self, credentials_map, retry_grpc_internal
    ):
        """Test ``asend`` payloads round-trip through the unified driver.

        Given:
            An async-generator routine that echoes the value sent via
            ``asend`` back as the next yielded value, and an
            EPHEMERAL pool (cross-process worker so the asend
            payload exercises the cloudpickle decode path).
        When:
            The caller alternates ``__anext__`` and ``asend(value)``.
        Then:
            Each echoed value should equal the value the caller sent
            on the corresponding ``asend`` frame.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ASEND,
                pool_mode=PoolMode.EPHEMERAL,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.echo_send(2)
                try:
                    first = await gen.__anext__()
                    echoed_a = await gen.asend("alpha")
                    echoed_b = await gen.asend("beta")
                finally:
                    await gen.aclose()

            # Assert
            assert first == "ready"
            assert echoed_a == "alpha"
            assert echoed_b == "beta"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_dispatch_with_throw_delivers_exception_into_routine(
        self, credentials_map, retry_grpc_internal
    ):
        """Test ``athrow`` injects an exception into the running routine.

        Given:
            An async-generator routine that catches ``ValueError`` and
            resets its counter to ``0``, and an EPHEMERAL pool
            (cross-process worker so the athrow payload exercises
            the cloudpickle decode path).
        When:
            The caller advances the routine once, then calls
            ``athrow(ValueError())`` to inject an exception, then
            iterates once more.
        Then:
            The yielded value after the throw should reflect the
            routine's reset state, proving the exception was
            delivered into the worker frame.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ATHROW,
                pool_mode=PoolMode.EPHEMERAL,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.resilient_counter(7)
                try:
                    first = await gen.__anext__()
                    after_throw = await gen.athrow(ValueError("inject"))
                finally:
                    await gen.aclose()

            # Assert
            assert first == 7
            assert after_throw == 0

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_dispatch_with_aclose_terminates_stream_cleanly(
        self, credentials_map, retry_grpc_internal
    ):
        """Test ``aclose`` cancels the proxy stream without leaking.

        Given:
            An async-generator routine that loops forever yielding a
            sentinel and an EPHEMERAL pool (cross-process worker).
        When:
            The caller advances the routine once and then calls
            ``aclose`` on the proxy.
        Then:
            ``aclose`` should return without error, the second
            ``__anext__`` should raise ``StopAsyncIteration``, and the
            pool should exit cleanly when the context manager unwinds.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ACLOSE,
                pool_mode=PoolMode.EPHEMERAL,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.closeable_gen()
                first = await gen.__anext__()
                await gen.aclose()
                with pytest.raises(StopAsyncIteration):
                    await gen.__anext__()

            # Assert
            assert first == "alive"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_dispatch_uses_cloudpickle_serializer_in_self_dispatch(
        self, credentials_map, retry_grpc_internal
    ):
        """Test self-dispatch through DEFAULT pool round-trips a coroutine.

        Given:
            A coroutine routine and a DEFAULT pool (single in-process
            worker so the dispatch target matches the caller process;
            self-dispatch serializes the payload through cloudpickle,
            the same path as cross-process dispatch).
        When:
            The caller awaits the routine once.
        Then:
            The routine result should round-trip through cloudpickle
            without raising.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.add(11, 31)

            # Assert
            assert result == 42

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_dispatch_uses_cloudpickle_serializer_cross_process(
        self, credentials_map, retry_grpc_internal
    ):
        """Test cross-process dispatch round-trips a coroutine.

        Given:
            A coroutine routine and an EPHEMERAL pool (subprocess
            workers so the dispatch target does not match the caller
            process, structurally selecting the cloudpickle serializer
            for the payload).
        When:
            The caller awaits the routine once.
        Then:
            The routine result should round-trip through the
            cross-process serialization path without raising.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.EPHEMERAL,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.add(11, 31)

            # Assert
            assert result == 42

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_dispatch_uses_cloudpickle_serializer_in_self_dispatch(
        self, credentials_map, retry_grpc_internal
    ):
        """Test self-dispatch streams an async-generator through cloudpickle.

        Given:
            An async-generator routine yielding three values and a
            DEFAULT pool (single in-process worker so the dispatch
            target matches the caller process; self-dispatch serializes
            each frame through cloudpickle).
        When:
            The caller iterates the proxy to exhaustion.
        Then:
            All three frames should be observed in order and the
            iterator should terminate cleanly.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
                pool_mode=PoolMode.DEFAULT,
            )
            collected: list[int] = []

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                async for value in routines.gen_range(3):
                    collected.append(value)

            # Assert
            assert collected == [0, 1, 2]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_completes_under_strict_mode_with_decodable_vars(
        self, credentials_map, retry_grpc_internal
    ):
        """Test strict-mode warnings filter does not perturb the happy path.

        Given:
            A coroutine routine with caller-side wool.ContextVars set
            to fully-decodable values and a DEFAULT pool, with
            ``warnings.simplefilter("error",
            category=wool.ContextDecodeWarning)`` active for the
            dispatch.
        When:
            The caller dispatches the routine.
        Then:
            The dispatch should complete without raising and without
            emitting a single ``wool.ContextDecodeWarning``, since
            every shipped var is decodable on the worker.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
                strict_warnings=StrictWarnings.ALL_DECODABLE,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                tenant_token = routines.TENANT_ID.set("strict-tenant")
                region_token = routines.REGION.set("strict-region")
                try:
                    with warnings.catch_warnings(record=True) as captured:
                        warnings.simplefilter(
                            "error", category=wool.ContextDecodeWarning
                        )
                        result = await routines.get_tenant_id()
                finally:
                    routines.REGION.reset(region_token)
                    routines.TENANT_ID.reset(tenant_token)

            # Assert
            assert result == "strict-tenant"
            decode_warnings = [
                w for w in captured if issubclass(w.category, wool.ContextDecodeWarning)
            ]
            assert decode_warnings == [], (
                f"Expected no ContextDecodeWarning under strict mode "
                f"with decodable vars, got {decode_warnings!r}"
            )

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_nested_async_gen_dispatch_keeps_context_live_across_yields(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a streaming routine's nested dispatches see the streaming task as caller.

        Given:
            A streaming routine that mutates ``TENANT_ID`` to a
            per-step value and nested-dispatches ``get_tenant_id``
            on every yield, and an EPHEMERAL pool.
        When:
            The caller iterates the outer generator to exhaustion.
        Then:
            Each yielded value should equal the per-step value the
            outer worker set, proving ``_current_task`` and
            ``wool.Context`` stay live across the streaming routine's
            lifespan and every nested dispatch finds the streaming
            task as the caller.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.NESTED_ASYNC_GEN_READBACK,
                pool_mode=PoolMode.EPHEMERAL,
            )
            collected: list[str] = []

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                async for value in routines.streaming_nested_get_tenant_id(3):
                    collected.append(value)

            # Assert
            assert collected == ["step-0", "step-1", "step-2"]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_dispatch_admits_after_sync_backpressure_accept(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a sync backpressure-accept hook gates dispatch on success.

        Given:
            A coroutine routine and a DEFAULT pool whose worker
            installs a sync backpressure hook that accepts every
            task.
        When:
            The caller dispatches the routine.
        Then:
            The hook should run, admission should succeed, and the
            routine should return its result.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
                backpressure=BackpressureMode.SYNC,
            )

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.add(4, 5)

            # Assert
            assert result == 9

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_dispatch_admits_after_async_backpressure_accept(
        self, credentials_map, retry_grpc_internal
    ):
        """Test an async backpressure-accept hook gates streaming dispatch on success.

        Given:
            An async-generator routine and a DEFAULT pool whose worker
            installs an async backpressure hook that accepts every
            task.
        When:
            The caller iterates the routine to exhaustion.
        Then:
            The hook should run once at admission, every subsequent
            frame should pass without re-running the hook, and the
            iterator should terminate cleanly.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ANEXT,
                pool_mode=PoolMode.DEFAULT,
                backpressure=BackpressureMode.ASYNC,
            )
            collected: list[int] = []

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                async for value in routines.gen_range(3):
                    collected.append(value)

            # Assert
            assert collected == [0, 1, 2]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_caller_cancel_propagates_to_worker_and_runs_finally(
        self, credentials_map, retry_grpc_internal, tmp_path
    ):
        """Test caller-initiated cancel propagates to the worker routine's ``finally``.

        Mirrors stdlib's ``task.cancel()`` semantics: cancelling the
        caller's task should inject :class:`asyncio.CancelledError`
        into the running coroutine, the routine should unwind its
        ``try/finally``, and the caller's ``await task`` should
        raise :class:`asyncio.CancelledError`.

        Given:
            A long-sleeping coroutine routine with a ``try/finally``
            that writes its termination reason to a sentinel file,
            dispatched through an EPHEMERAL pool (cross-process
            worker so the cancel signal must traverse gRPC).
        When:
            The caller wraps the dispatch in a task, lets it start
            running on the worker, then cancels the caller task.
        Then:
            The caller's ``await task`` should raise
            :class:`asyncio.CancelledError`, AND the sentinel file
            should contain ``"cancelled"`` — proving the worker-side
            routine received the cancellation and ran its
            ``finally`` block rather than being orphaned.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.EPHEMERAL,
            )
            sentinel = tmp_path / "termination_reason.txt"

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                task = asyncio.create_task(
                    routines.cancellable_sleep(str(sentinel), 30.0)
                )
                # Wait deterministically for the routine to write its
                # ``"started"`` marker — the routine writes this just
                # before suspending on ``asyncio.sleep`` so cancelling
                # afterwards is guaranteed to land on the suspended
                # routine and exercise its ``except CancelledError``
                # arm. Bound at 15s to tolerate slow subprocess
                # startup on contended CI hosts.
                for _ in range(150):
                    if sentinel.exists() and sentinel.read_text() == "started":
                        break
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError(
                        "routine never wrote ``started`` sentinel — "
                        "dispatch handshake or worker startup hung"
                    )
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

                # Poll for the worker's ``finally`` to overwrite the
                # marker. The window is the gRPC stream teardown +
                # worker-thread context unwind; 15s tolerates CI load.
                for _ in range(150):
                    if sentinel.read_text() == "cancelled":
                        break
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError(
                        "worker never overwrote ``started`` with "
                        "``cancelled`` within 15s — caller cancel did "
                        "not propagate to the worker, or the routine "
                        "was orphaned"
                    )

            # Assert
            assert sentinel.read_text() == "cancelled"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_caller_aclose_triggers_worker_finally(
        self, credentials_map, retry_grpc_internal, tmp_path
    ):
        """Test ``aclose`` on the caller-side proxy runs the
        worker-side ``finally`` block.

        Companion to the coroutine cancel test for the
        async-generator path. Confirms that ``aclose`` (or breaking
        out of ``async for``, which routes through the wrapper's
        ``finally: await stream.aclose()``) terminates the remote
        routine cleanly rather than leaving it running.

        Given:
            An async-generator routine that yields ``"alive"``
            forever inside a ``try/finally`` that writes
            ``"cleaned_up"`` to a sentinel file, dispatched through
            an EPHEMERAL pool.
        When:
            The caller advances the routine once and then calls
            ``aclose`` on the proxy.
        Then:
            The sentinel file should contain ``"cleaned_up"``,
            proving the worker-side ``finally`` ran rather than
            the routine being orphaned.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ACLOSE,
                pool_mode=PoolMode.EPHEMERAL,
            )
            sentinel = tmp_path / "cleanup_reason.txt"

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                gen = routines.cancellable_gen(str(sentinel))
                first = await gen.__anext__()
                await gen.aclose()

                # Poll up to 15s for the sentinel — the worker's
                # ``finally`` runs after the gRPC stream tears down
                # and tolerates CI load.
                for _ in range(150):
                    if sentinel.exists():
                        break
                    await asyncio.sleep(0.1)

            # Assert
            assert first == "alive"
            assert sentinel.exists(), (
                "worker-side ``finally`` never wrote the sentinel — "
                "caller aclose did not propagate to the worker, or "
                "the generator was orphaned"
            )
            assert sentinel.read_text() == "cleaned_up"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_self_raised_cancelled_error_surfaces_as_cancelled_error(
        self, credentials_map, retry_grpc_internal
    ):
        """Test routine-self-raised CancelledError surfaces raw on the caller.

        Mirrors stdlib's ``await task`` semantics where a coroutine
        that self-raises :class:`asyncio.CancelledError` is
        indistinguishable from one that was externally cancelled —
        both transition the task to ``CANCELLED`` and the caller's
        ``await`` raises :class:`asyncio.CancelledError`. Wool must
        preserve this contract across the wire: a routine's
        explicit ``raise asyncio.CancelledError`` should reach the
        caller as a real :class:`asyncio.CancelledError` so user
        code can ``except asyncio.CancelledError`` (and chain the
        cancellation through the caller's own task as asyncio
        expects).

        Given:
            A coroutine routine whose body is
            ``raise asyncio.CancelledError(...)``, dispatched
            through an EPHEMERAL pool (cross-process so the wire
            round-trip is exercised).
        When:
            The caller awaits the routine.
        Then:
            The caller should observe :class:`asyncio.CancelledError`
            raised by the ``await`` — not a degraded
            :class:`RpcError` or :class:`UnexpectedResponse`.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.EPHEMERAL,
            )

            # Act & assert
            async with build_pool_from_scenario(scenario, credentials_map):
                with pytest.raises(asyncio.CancelledError) as exc_info:
                    await routines.self_cancel_coroutine()

                # Belt-and-suspenders: ``CancelledError`` must not be
                # degraded to an ``RpcError`` (which would trigger
                # load-balancer eviction) or to an
                # ``UnexpectedResponse``.
                assert not isinstance(exc_info.value, RpcError)
                assert not isinstance(exc_info.value, UnexpectedResponse)

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_caller_cancel_midstream_releases_channel_pool_ref(
        self, credentials_map, retry_grpc_internal, tmp_path
    ):
        """Test caller cancellation mid-stream releases the pooled
        channel reference.

        Given:
            An async-generator routine yielding ``"alive"`` forever
            inside a ``try/finally``, dispatched through an
            EPHEMERAL pool (cross-process worker) and consumed by a
            task that is cancelled while awaiting a value.
        When:
            The caller task is cancelled mid-stream and awaited.
        Then:
            It should raise :class:`asyncio.CancelledError`, run the
            worker-side ``finally``, and return the caller's channel
            pool to its pre-dispatch referenced-entry count — no
            leaked reference.
        """

        async def body():
            # Arrange
            from wool.runtime.worker import connection

            scenario = default_scenario(
                shape=RoutineShape.ASYNC_GEN_ACLOSE,
                pool_mode=PoolMode.EPHEMERAL,
            )
            sentinel = tmp_path / "cleanup_reason.txt"

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                baseline = connection._channel_pool.stats.referenced_entries
                collected = []
                started = asyncio.Event()

                async def consume():
                    gen = routines.cancellable_gen(str(sentinel))
                    collected.append(await gen.__anext__())
                    started.set()
                    # Park awaiting the next value — the caller
                    # cancels the task here, mid-stream.
                    collected.append(await gen.__anext__())

                task = asyncio.create_task(consume())
                await asyncio.wait_for(started.wait(), timeout=15)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

                # Poll up to 15s for the worker's ``finally`` to
                # write the sentinel — it runs after the gRPC stream
                # tears down and tolerates CI load.
                for _ in range(150):
                    if sentinel.exists() and sentinel.read_text() == "cleaned_up":
                        break
                    await asyncio.sleep(0.1)

                # Assert
                assert collected == ["alive"]
                assert sentinel.read_text() == "cleaned_up"
                assert connection._channel_pool.stats.referenced_entries == baseline

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_dispatch_copies_mutable_argument_in_self_dispatch(
        self, credentials_map, retry_grpc_internal
    ):
        """Test self-dispatch hands the routine a copy of a mutable argument.

        Given:
            An in-place-mutating coroutine routine and a DEFAULT pool
            (single in-process worker so the dispatch target matches
            the caller process).
        When:
            The caller dispatches the routine passing a list it still
            holds a reference to.
        Then:
            The returned worker-side list should contain the appended
            value while the caller's original list object stays
            unchanged — proving self-dispatch copies its arguments.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
            )
            caller_list = [1, 2, 3]

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                returned = await routines.append_to_list(caller_list, 99)

            # Assert
            assert returned == [1, 2, 3, 99]
            assert caller_list == [1, 2, 3]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_dispatch_copies_mutable_argument_cross_process(
        self, credentials_map, retry_grpc_internal
    ):
        """Test cross-process dispatch hands the routine a copy of a mutable argument.

        Given:
            An in-place-mutating coroutine routine and an EPHEMERAL
            pool (subprocess workers so the dispatch target does not
            match the caller process and the argument is necessarily
            serialized).
        When:
            The caller dispatches the routine passing a list it still
            holds a reference to.
        Then:
            The returned worker-side list should contain the appended
            value while the caller's original list object stays
            unchanged — the baseline that self-dispatch must match.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.EPHEMERAL,
            )
            caller_list = [1, 2, 3]

            # Act
            async with build_pool_from_scenario(scenario, credentials_map):
                returned = await routines.append_to_list(caller_list, 99)

            # Assert
            assert returned == [1, 2, 3, 99]
            assert caller_list == [1, 2, 3]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_dispatch_with_unpicklable_argument_in_self_dispatch(
        self, credentials_map, retry_grpc_internal
    ):
        """Test self-dispatch raises when an argument cannot be serialized.

        Given:
            A single-argument coroutine routine and a DEFAULT pool
            (single in-process worker), with the argument set to an
            open socket — an object cloudpickle cannot serialize.
        When:
            The caller dispatches the routine passing the unpicklable
            argument.
        Then:
            The dispatch should raise :class:`TypeError` at the
            serialization boundary — self-dispatch serializes the
            payload exactly like cross-process dispatch.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.DEFAULT,
            )
            unpicklable = socket.socket()

            # Act & assert
            try:
                async with build_pool_from_scenario(scenario, credentials_map):
                    with pytest.raises(TypeError):
                        await routines.touch_argument(unpicklable)
            finally:
                unpicklable.close()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_coroutine_dispatch_with_unpicklable_argument_cross_process(
        self, credentials_map, retry_grpc_internal
    ):
        """Test cross-process dispatch raises when an argument cannot be serialized.

        Given:
            A single-argument coroutine routine and an EPHEMERAL pool
            (subprocess workers), with the argument set to an open
            socket — an object cloudpickle cannot serialize.
        When:
            The caller dispatches the routine passing the unpicklable
            argument.
        Then:
            The dispatch should raise :class:`TypeError` at the
            serialization boundary — the baseline failure mode that
            self-dispatch must match in kind.
        """

        async def body():
            # Arrange
            scenario = default_scenario(
                shape=RoutineShape.COROUTINE,
                pool_mode=PoolMode.EPHEMERAL,
            )
            unpicklable = socket.socket()

            # Act & assert
            try:
                async with build_pool_from_scenario(scenario, credentials_map):
                    with pytest.raises(TypeError):
                        await routines.touch_argument(unpicklable)
            finally:
                unpicklable.close()

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_async_gen_dispatch_copies_mutable_argument_across_yields(
        self, credentials_map, retry_grpc_internal
    ):
        """Test a streaming dispatch copies a mutable argument mutated across yields.

        Given:
            An async-generator routine that appends to a mutable list
            argument on each yield, dispatched once through a DEFAULT
            pool (self-dispatch) and once through an EPHEMERAL pool
            (cross-process).
        When:
            The caller iterates each dispatch to exhaustion while
            holding a reference to the list it passed in.
        Then:
            Both dispatches should yield the same growing snapshots
            and leave the caller's original list object unchanged
            after full iteration.
        """

        async def body():
            # Arrange
            scenarios = [
                default_scenario(
                    shape=RoutineShape.ASYNC_GEN_ANEXT,
                    pool_mode=PoolMode.DEFAULT,
                ),
                default_scenario(
                    shape=RoutineShape.ASYNC_GEN_ANEXT,
                    pool_mode=PoolMode.EPHEMERAL,
                ),
            ]
            results: list[list[list[int]]] = []

            # Act & assert
            for scenario in scenarios:
                caller_list: list[int] = [0]
                async with build_pool_from_scenario(scenario, credentials_map):
                    collected = [
                        snapshot
                        async for snapshot in routines.append_on_each_yield(
                            caller_list, [1, 2, 3]
                        )
                    ]
                # The caller's own list object is never mutated.
                assert caller_list == [0]
                results.append(collected)

            # Assert
            assert results[0] == [[0, 1], [0, 1, 2], [0, 1, 2, 3]]
            assert results[0] == results[1]

        await retry_grpc_internal(body)
