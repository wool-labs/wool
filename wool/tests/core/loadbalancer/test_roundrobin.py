from asyncio import TimeoutError
from uuid import uuid4

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

from wool._work import WoolTask
from wool.core.discovery.base import WorkerInfo
from wool.core.loadbalancer.base import LoadBalancerContext
from wool.core.loadbalancer.base import NoWorkersAvailable
from wool.core.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.core.worker.connection import RpcError
from wool.core.worker.connection import TransientRpcError
from wool.core.worker.connection import WorkerConnection


@st.composite
def dispatch_side_effects(draw, min_size: int, max_size: int):
    """Generate behavior sequence for a single task dispatch.

    Generates a list of failure side effects. Failure modes may be any of
    Exception, RpcError, TransientRpcError, or TimeoutError.

    :param draw:
        Hypothesis draw function
    :param min_size:
        Minimum number of failures to generate
    :param max_size:
        Maximum number of failures to generate

    :returns:
        List of side effects (exceptions)
    """
    return draw(
        st.lists(
            st.sampled_from(
                [Exception(), RpcError(), TransientRpcError(), TimeoutError()]
            ),
            min_size=min_size,
            max_size=max_size,
        )
    )


class TestRoundRobinLoadBalancer:
    @pytest.mark.asyncio
    @settings(
        max_examples=16,
        suppress_health_check=[
            HealthCheck.function_scoped_fixture,
        ],
    )
    @given(data=st.data())
    async def test_dispatch(
        self,
        mocker: MockerFixture,
        mock_connection_resource_factory,
        dispatch_side_effect_factory,
        data: st.DataObject,
    ):
        """Test tasks dispatch successfully when there's at least one healthy
        worker available.

        Given:
            A load balancer with one or more workers with randomized behavior
        When:
            One or more tasks are dispatched
        Then:
            The tasks should be dispatched to workers in round-robin order,
            with unhealthy workers being removed from the loadbalancer
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()
        worker_count = 8
        mock_workers = {}

        for i in range(worker_count):
            mock_connection = mocker.create_autospec(WorkerConnection, instance=True)
            mock_connection.dispatch = mocker.AsyncMock()
            worker_info = WorkerInfo(
                uid=uuid4(),
                host="localhost",
                port=50051,
                pid=1000 + i,
                version="1.0.0",
            )
            mock_workers[worker_info] = mock_connection
            resource_factory = mock_connection_resource_factory(
                worker_info, mock_workers
            )
            ctx.add_worker(worker_info, resource_factory)

        async def routine():
            return "Hello world!"

        mock_proxy = mocker.MagicMock(id="mock-proxy")

        task = WoolTask(
            id=uuid4(),
            callable=routine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Track dispatch attempts to verify round-robin behavior
        tasks_dispatched = []
        previous_worker = None
        expected_worker = None

        # Act
        i = 0
        while i < 16 and len(ctx.workers) > 1:
            i += 1
            # Capture snapshot of workers at the start of this iteration
            workers_remaining = list(ctx.workers.keys())
            workers_remaining_count = len(workers_remaining)

            workers_attempted = []
            side_effects = data.draw(
                dispatch_side_effects(min_size=0, max_size=workers_remaining_count - 1)
            )
            side_effects += ["success"]
            index_of_first_worker_attempted = len(workers_attempted)

            # Draw a random timeout value
            timeout = data.draw(
                st.one_of(st.none(), st.integers(min_value=1, max_value=60))
            )

            if previous_worker:
                # Get next worker in line
                expected_worker = workers_remaining[
                    (workers_remaining.index(previous_worker) + 1)
                    % len(workers_remaining)
                ]

            side_effect_iterator = iter(side_effects)

            make_dispatch_side_effect = dispatch_side_effect_factory(
                workers_attempted, side_effect_iterator, tasks_dispatched
            )

            # Reset dispatch side effect and call count on all remaining workers
            for worker_info in workers_remaining:
                mock_connection = mock_workers[worker_info]
                mock_connection.dispatch.reset_mock()
                mock_connection.dispatch.side_effect = make_dispatch_side_effect(
                    worker_info
                )

            result = await lb.dispatch(task, context=ctx, timeout=timeout)
            assert result == "success", "Task should complete successfully"

            # Verify dispatch was called the expected number of times
            # (number of failures + 1 for success)
            actual_dispatch_calls = sum(
                mock_workers[w].dispatch.call_count for w in workers_remaining
            )
            expected_dispatch_calls = len(side_effects)
            assert actual_dispatch_calls == expected_dispatch_calls

            # Verify dispatch was called with correct timeout on attempted workers
            for worker in workers_attempted:
                mock_workers[worker].dispatch.assert_called_with(task, timeout=timeout)

            if expected_worker:
                # Verify the next worker in line following the previous
                # dispatch was the first worker to attempt dispatching the current task
                first_worker = workers_attempted[index_of_first_worker_attempted]
                assert first_worker == expected_worker

            previous_worker = workers_attempted[-1]

            # Verify that a task was successfully dispatched at each iteration
            assert len(tasks_dispatched) == i

    @pytest.mark.asyncio
    @settings(
        deadline=5000000,
        max_examples=16,
        suppress_health_check=[
            HealthCheck.function_scoped_fixture,
        ],
    )
    @given(data=st.data())
    @pytest.mark.asyncio
    async def test_dispatch_no_workers_available(
        self,
        mocker: MockerFixture,
        mock_connection_resource_factory,
        dispatch_side_effect_factory,
        data: st.DataObject,
    ):
        """Test task dispatch fails when there are no healthy workers available.

        Given:
            A load balancer with one or more workers with a randomized failure mode
        When:
            One or more tasks are dispatched
        Then:
            The tasks should be dispatched to workers in round-robin order and
            raise NoWorkersAvailable, with unhealthy workers being removed from
            the loadbalancer
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()
        worker_count = 8
        mock_workers = {}

        for i in range(worker_count):
            mock_connection = mocker.create_autospec(WorkerConnection, instance=True)
            mock_connection.dispatch = mocker.AsyncMock()
            worker_info = WorkerInfo(
                uid=uuid4(),
                host="localhost",
                port=50051,
                pid=1000 + i,
                version="1.0.0",
            )
            mock_workers[worker_info] = mock_connection
            resource_factory = mock_connection_resource_factory(
                worker_info, mock_workers
            )
            ctx.add_worker(worker_info, resource_factory)

        async def routine():
            return "Hello world!"

        mock_proxy = mocker.MagicMock(id="mock-proxy")

        task = WoolTask(
            id=uuid4(),
            callable=routine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Track dispatch attempts to verify round-robin behavior
        tasks_dispatched = []

        # Act
        i = 0
        while i < 16 and len(ctx.workers) > 0:
            i += 1
            # Capture snapshot of workers at the start of this iteration
            workers_remaining = list(ctx.workers.keys())
            workers_remaining_count = len(workers_remaining)

            workers_attempted = []
            side_effects = data.draw(
                dispatch_side_effects(
                    min_size=workers_remaining_count, max_size=workers_remaining_count
                )
            )
            side_effect_iterator = iter(side_effects)

            # Draw a random timeout value
            timeout = data.draw(
                st.one_of(st.none(), st.integers(min_value=1, max_value=60))
            )

            make_dispatch_side_effect = dispatch_side_effect_factory(
                workers_attempted, side_effect_iterator, tasks_dispatched
            )

            # Reset dispatch side effect and call count on all remaining workers
            for worker_info in workers_remaining:
                mock_connection = mock_workers[worker_info]
                mock_connection.dispatch.reset_mock()
                mock_connection.dispatch.side_effect = make_dispatch_side_effect(
                    worker_info
                )

            with pytest.raises(NoWorkersAvailable):
                await lb.dispatch(task, context=ctx, timeout=timeout)

            # Verify dispatch was called on all remaining workers
            # (all workers fail, so all should be attempted)
            actual_dispatch_calls = sum(
                mock_workers[w].dispatch.call_count for w in workers_remaining
            )
            expected_dispatch_calls = workers_remaining_count
            assert actual_dispatch_calls == expected_dispatch_calls

            # Verify dispatch was called with correct timeout on attempted workers
            for worker in workers_attempted:
                mock_workers[worker].dispatch.assert_called_with(task, timeout=timeout)

            # Verify the next worker in line following the previous
            # dispatch was the first worker to attempt dispatching the current task
            assert workers_attempted[0] == workers_remaining[0]

            # Verify that no tasks were successfully dispatched
            assert len(tasks_dispatched) == 0
