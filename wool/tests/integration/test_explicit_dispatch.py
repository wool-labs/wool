"""End-to-end tests for explicit single-dispatch through a given pool (#196).

What `wool.WorkerPool.dispatch` adds over an ambient `wool.routine`
call is *which* pool serves it. The unit suite pins that the pool
reserves no argument and hands its own proxy to the routine layer; only
real workers can witness that a call lands on the pool named by the
receiver rather than the one in context, which is what this module
covers.

The composition axes — credentials, discovery, pool lifetime,
backpressure, context-var patterns — are covered by the ``EXPLICIT_*``
members of ``RoutineShape`` in the pairwise suite instead, so the tests
here stay on the arrangements that witness the override itself.

Pools here are plain and ephemeral with ``spawn=1``, so a worker pid
identifies the pool that ran a routine.
"""

import asyncio

import pytest

import wool

from . import routines

pytestmark = pytest.mark.integration

#: The same bound the other integration modules run under, so a worker
#: child that wedges before reporting its metadata fails the test
#: rather than hanging it.
_INTEGRATION_TIMEOUT = 30


class TestWorkerPoolDispatch:
    @pytest.mark.asyncio
    async def test_dispatch_should_match_an_ambient_call(self, retry_grpc_internal):
        """Test an explicit dispatch returns what an ambient call returns.

        Given:
            An entered pool that is also the ambient one.
        When:
            A coroutine routine is called ambiently and dispatched
            explicitly through the same pool.
        Then:
            It should return the same result both ways.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as pool:
                    # Act
                    ambient = await routines.add(1, 2)
                    explicit = await pool.dispatch(routines.add, 1, 2)

            # Assert
            assert ambient == 3
            assert explicit == ambient

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_route_to_the_receiver_when_another_pool_is_active(
        self, retry_grpc_internal
    ):
        """Test the receiving pool overrides the ambient one.

        Given:
            Two entered pools, the inner of which is ambient.
        When:
            A routine is dispatched explicitly through the outer pool.
        Then:
            It should run on the outer pool's worker.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as outer:
                    async with wool.WorkerPool(spawn=1):
                        ambient = await routines.get_pid()

                        # Act
                        explicit = await outer.dispatch(routines.get_pid)

            # Assert
            assert explicit != ambient

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_restore_the_ambient_pool_when_another_is_active(
        self, retry_grpc_internal
    ):
        """Test an explicit dispatch leaves the ambient pool serving.

        Given:
            Two entered pools, the inner of which is ambient.
        When:
            A routine is dispatched explicitly through the outer pool
            and then called plainly.
        Then:
            The plain call should land on the ambient pool.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as outer:
                    async with wool.WorkerPool(spawn=1):
                        ambient = await routines.get_pid()

                        # Act
                        await outer.dispatch(routines.get_pid)
                        after = await routines.get_pid()

            # Assert
            assert after == ambient

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_isolate_concurrent_dispatches_when_gathered(
        self, retry_grpc_internal
    ):
        """Test interleaved explicit and ambient dispatches do not collide.

        Given:
            Two entered pools, the inner of which is ambient.
        When:
            Explicit dispatches through the outer pool run concurrently
            with plain calls under `asyncio.gather`.
        Then:
            Every explicit call should land on the outer pool and every
            plain call on the ambient one.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as outer:
                    async with wool.WorkerPool(spawn=1):
                        ambient = await routines.get_pid()
                        explicit = await outer.dispatch(routines.get_pid)

                        # Act
                        results = await asyncio.gather(
                            *(outer.dispatch(routines.get_pid) for _ in range(4)),
                            *(routines.get_pid() for _ in range(4)),
                        )

            # Assert
            assert set(results[:4]) == {explicit}
            assert set(results[4:]) == {ambient}

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_route_nested_calls_when_another_pool_is_active(
        self, retry_grpc_internal
    ):
        """Test the override reaches the routine's own nested dispatches.

        Given:
            Two entered pools, the inner of which is ambient.
        When:
            A routine that dispatches nested calls is dispatched
            explicitly through the outer pool.
        Then:
            Both the routine and its nested calls should run on the
            outer pool's worker.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as outer:
                    async with wool.WorkerPool(spawn=1):
                        ambient = await routines.get_pid()

                        # Act
                        host, nested = await outer.dispatch(
                            routines.nested_pid_fanout, 2
                        )

            # Assert
            assert host != ambient
            assert nested == [host, host]

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_pass_through_colliding_keyword_names(
        self, retry_grpc_internal
    ):
        """Test no keyword is reserved by the dispatch signature.

        Given:
            An entered pool and a routine with parameters named
            ``pool`` and ``routine``.
        When:
            Both are supplied as keywords to the pool.
        Then:
            It should deliver both to the routine unshadowed.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as pool:
                    # Act
                    result = await pool.dispatch(
                        routines.echo_colliding_names, pool="p", routine="r"
                    )

            # Assert
            assert result == ("p", "r")

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_deliver_every_argument_when_bound_to_an_instance(
        self, retry_grpc_internal
    ):
        """Test a bound method keeps its receiver and its arguments.

        Given:
            An entered pool and a routine bound to an instance.
        When:
            The bound method is dispatched with its full argument list.
        Then:
            It should return what an ambient call returns.
        """

        async def body():
            # Arrange
            host = routines.Routines()
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as pool:
                    ambient = await host.instance_add(1, 2)

                    # Act
                    explicit = await pool.dispatch(host.instance_add, 1, 2)

            # Assert
            assert ambient == 3
            assert explicit == ambient

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_deliver_every_argument_when_bound_to_a_class(
        self, retry_grpc_internal
    ):
        """Test a classmethod keeps its class and its arguments.

        Given:
            An entered pool and a routine bound to a class.
        When:
            The classmethod is dispatched with its full argument list.
        Then:
            It should return what an ambient call returns.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as pool:
                    ambient = await routines.Routines.class_add(1, 2)

                    # Act
                    explicit = await pool.dispatch(routines.Routines.class_add, 1, 2)

            # Assert
            assert ambient == 3
            assert explicit == ambient

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_stream_an_async_generator_routine(
        self, retry_grpc_internal
    ):
        """Test an async generator routine dispatches as an iterable.

        Given:
            An entered pool.
        When:
            An async generator routine is dispatched and iterated.
        Then:
            It should yield the same values an ambient call yields.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as pool:
                    ambient = [value async for value in routines.gen_range(4)]

                    # Act
                    explicit = [
                        value async for value in pool.dispatch(routines.gen_range, 4)
                    ]

            # Assert
            assert ambient == [0, 1, 2, 3]
            assert explicit == ambient

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_forward_asend_to_an_async_generator_routine(
        self, retry_grpc_internal
    ):
        """Test a sent value reaches a dispatched routine.

        Given:
            An entered pool and a dispatched async generator routine
            that echoes what is sent to it.
        When:
            A value is sent to the returned generator.
        Then:
            It should carry the value to the worker and return what the
            routine yielded in response.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as pool:
                    stream = pool.dispatch(routines.echo_send, 1)
                    ready = await stream.__anext__()

                    # Act
                    echoed = await stream.asend("value")
                    await stream.aclose()

            # Assert
            assert ready == "ready"
            assert echoed == "value"

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_forward_athrow_to_an_async_generator_routine(
        self, retry_grpc_internal
    ):
        """Test a thrown exception reaches a dispatched routine.

        Given:
            An entered pool and a dispatched async generator routine
            that resets its counter when a ValueError is thrown in.
        When:
            A ValueError is thrown into the returned generator.
        Then:
            It should deliver the exception to the worker and return the
            value the routine yielded in response.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as pool:
                    stream = pool.dispatch(routines.resilient_counter, 5)
                    first = await stream.__anext__()

                    # Act
                    reset = await stream.athrow(ValueError("reset"))
                    await stream.aclose()

            # Assert
            assert first == 5
            assert reset == 0

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_propagate_an_exception_raised_by_the_routine(
        self, retry_grpc_internal
    ):
        """Test a routine's own exception reaches the explicit caller.

        Given:
            Two entered pools, the inner of which is ambient, and a
            routine that raises.
        When:
            The routine is dispatched explicitly through the outer pool.
        Then:
            It should surface the routine's own exception and leave the
            ambient pool serving plain calls.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as outer:
                    async with wool.WorkerPool(spawn=1):
                        ambient = await routines.get_pid()

                        # Act
                        with pytest.raises(ValueError, match="mutate_then_raise"):
                            await outer.dispatch(
                                routines.mutate_then_raise_tenant_id, "acme"
                            )

                        # Assert
                        assert await routines.get_pid() == ambient

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_restore_the_ambient_pool_when_a_stream_is_abandoned(
        self, retry_grpc_internal
    ):
        """Test abandoning a dispatched stream leaves the ambient pool serving.

        Given:
            Two entered pools, the inner of which is ambient.
        When:
            An explicitly dispatched async generator is abandoned
            part-way through its frames.
        Then:
            The plain calls that follow should land on the ambient pool.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as outer:
                    async with wool.WorkerPool(spawn=1):
                        ambient = await routines.get_pid()

                        # Act
                        async for value in outer.dispatch(routines.gen_range, 10):
                            if value == 2:
                                break

                        # Assert
                        assert await routines.get_pid() == ambient

        await retry_grpc_internal(body)

    @pytest.mark.asyncio
    async def test_dispatch_should_run_the_routine_under_the_callers_chain(
        self, retry_grpc_internal
    ):
        """Test a caller's context variables reach an explicitly dispatched routine.

        Given:
            A caller that set a `wool.ContextVar`, and an entered pool.
        When:
            A routine that reads the variable is dispatched explicitly.
        Then:
            The worker should observe the caller's value, as it does for
            an ambient call.
        """

        async def body():
            # Arrange
            async with asyncio.timeout(_INTEGRATION_TIMEOUT):
                async with wool.WorkerPool(spawn=1) as pool:
                    routines.TENANT_ID.set("acme")

                    # Act
                    ambient = await routines.get_tenant_id()
                    explicit = await pool.dispatch(routines.get_tenant_id)

            # Assert
            assert ambient == "acme"
            assert explicit == ambient

        await retry_grpc_internal(body)
