from asyncio import TimeoutError
from types import MappingProxyType
from uuid import uuid4

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

from wool._connection import Connection
from wool._connection import RpcError
from wool._connection import TransientRpcError
from wool._loadbalancer import LoadBalancerContext
from wool._loadbalancer import NoWorkersAvailable
from wool._loadbalancer import RoundRobinLoadBalancer
from wool._resource_pool import ResourcePool
from wool._work import WoolTask
from wool._worker_discovery import WorkerInfo

connection_pool = ResourcePool(Connection)


@st.composite
def loadbalancer_context(draw):
    """Generate a LoadBalancerContext seeded with 1-8 workers.

    Creates a LoadBalancerContext populated with randomly generated workers.
    Each worker has unique identifying information and a mock connection
    resource factory.

    :param draw:
        Hypothesis draw function

    :returns:
        LoadBalancerContext instance with 1-8 workers
    """
    worker_count = draw(st.integers(min_value=1, max_value=8))
    ctx = LoadBalancerContext()

    for i in range(worker_count):
        worker_info = WorkerInfo(
            uid=f"worker-{i}",
            host="localhost",
            port=50051 + i,
            pid=1000 + i,
            version="1.0.0",
        )
        ctx.add_worker(worker_info, lambda: connection_pool.get(worker_info))

    return ctx


class TestLoadBalancerContext:
    @given(loadbalancer_context())
    def test_workers_property_immutability(self, context: LoadBalancerContext):
        """Test that the ``workers`` property is immutable.

        Given:
            A load balancer context with one or more workers registered
        When:
            A worker is added, updated, or deleted via the ``workers`` property
        Then:
            The context's workers are unchanged
        """
        assert isinstance(context.workers, MappingProxyType)

    @given(loadbalancer_context())
    def test_add_worker(self, context: LoadBalancerContext):
        """Test that a worker can be added to the context.

        Given:
            A load balancer context with one or more workers registered
        When:
            A worker is added to the context
        Then:
            The new worker is present and the existing workers in the context
            are unchanged
        """
        original_workers = dict(context.workers)

        # Add a new worker
        worker = WorkerInfo(
            uid="new-worker",
            host="localhost",
            port=60000,
            pid=9999,
            version="1.0.0",
        )

        def factory():
            return connection_pool.get(worker_info)

        context.add_worker(worker, factory)

        # Verify the new worker is present
        assert worker in context.workers
        assert context.workers[worker] == factory

        # Verify existing workers are unchanged
        for worker_info in original_workers:
            assert worker_info in context.workers
            assert context.workers[worker_info] == original_workers[worker_info]

    @given(loadbalancer_context())
    def test_update_worker(self, context: LoadBalancerContext):
        """Test that a worker already in the context can be updated.

        Given:
            A load balancer context with one or more workers registered
        When:
            One of the workers in the context is updated
        Then:
            The updated worker reflects the updates accurately and the other
            existing workers in the context are unchanged
        """
        original_workers = dict(context.workers)
        worker_to_update = next(iter(context.workers.keys()))

        # Create a different factory function
        def factory():
            return connection_pool.get(worker_info)

        # Update the worker with a new factory
        context.update_worker(worker_to_update, factory)

        # Verify the worker was updated
        assert context.workers[worker_to_update] == factory

        # Verify other workers are unchanged
        for worker_info in original_workers:
            if worker_info != worker_to_update:
                assert context.workers[worker_info] == original_workers[worker_info]

    @given(loadbalancer_context())
    def test_upsert_existing_worker(self, context: LoadBalancerContext):
        """Test that a worker already in the context can be updated with the
        ``upsert`` flag.

        Given:
            A load balancer context with one or more workers registered
        When:
            One of the workers in the context is updated with the
            ``upsert`` flag
        Then:
            The updated worker reflects the updates accurately and the other
            existing workers in the context are unchanged
        """
        original_workers = dict(context.workers)
        worker_to_update = next(iter(context.workers.keys()))

        # Create a different factory function
        def factory():
            return connection_pool.get(worker_info)

        # Update the worker with a new factory using upsert=True
        context.update_worker(worker_to_update, factory, upsert=True)

        # Verify the worker was updated
        assert context.workers[worker_to_update] == factory

        # Verify other workers are unchanged
        for worker_info in original_workers:
            if worker_info != worker_to_update:
                assert context.workers[worker_info] == original_workers[worker_info]

    @given(loadbalancer_context())
    def test_upsert_new_worker(self, context: LoadBalancerContext):
        """Test that updating a non-existent worker with the ``upsert`` flag
        adds the worker to the context.

        Given:
            A load balancer context with one or more workers registered
        When:
            A non-existent worker is updated with the ``upsert`` flag
        Then:
            The upserted worker is present and the existing workers in the
            context are unchanged
        """
        original_workers = dict(context.workers)

        # Upsert a new worker
        new_worker = WorkerInfo(
            uid="upserted-worker",
            host="localhost",
            port=60000,
            pid=9999,
            version="1.0.0",
        )

        def factory():
            return connection_pool.get(worker_info)

        context.update_worker(new_worker, factory, upsert=True)

        # Verify the new worker is present
        assert new_worker in context.workers
        assert context.workers[new_worker] == factory

        # Verify existing workers are unchanged
        for worker_info in original_workers:
            assert worker_info in context.workers
            assert context.workers[worker_info] == original_workers[worker_info]

    @given(loadbalancer_context())
    def test_remove_worker(self, context: LoadBalancerContext):
        """Test that a worker can be removed from the context.

        Given:
            A load balancer context with one or more workers registered
        When:
            A worker is removed from the context
        Then:
            The removed worker is no longer present and the existing workers
            in the context are unchanged
        """
        original_workers = dict(context.workers)
        worker_to_remove = next(iter(context.workers.keys()))

        # Remove the worker
        context.remove_worker(worker_to_remove)

        # Verify the worker was removed
        assert worker_to_remove not in context.workers

        # Verify other workers are unchanged
        for worker_info in original_workers:
            if worker_info != worker_to_remove:
                assert worker_info in context.workers
                assert context.workers[worker_info] == original_workers[worker_info]


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


@pytest.fixture
def dispatch_side_effect_factory():
    """Factory for creating dispatch side effect functions.

    Returns a function that creates make_dispatch_side_effect functions with
    the provided dependencies.
    """

    def factory(workers_attempted: list, side_effect_iterator, tasks_dispatched: list):
        """Create a make_dispatch_side_effect function.

        :param workers_attempted:
            List to track which workers attempted the dispatch.
        :param side_effect_iterator:
            Iterator over side effects (exceptions or return values).
        :param tasks_dispatched:
            List to track successfully dispatched task results.

        :returns:
            A function that creates dispatch side effect functions for workers.
        """

        def make_dispatch_side_effect(worker_info):
            async def dispatch_side_effect(task, *, timeout=None):
                del timeout
                workers_attempted.append(worker_info)
                side_effect = next(side_effect_iterator)
                if isinstance(side_effect, Exception):
                    raise side_effect
                else:
                    tasks_dispatched.append(task)
                    return side_effect

            return dispatch_side_effect

        return make_dispatch_side_effect

    return factory


@pytest.fixture
def mock_connection_resource_factory(mocker: MockerFixture):
    """Factory for creating mock connection resource factories.

    Returns a function that creates connection resource factories for workers.
    """

    def factory(worker_info: WorkerInfo, mock_workers: dict):
        """Create a connection resource factory for the specified worker.

        :param worker_info:
            The worker info for which to create the factory.
        :param mock_workers:
            Dictionary mapping worker info to mock connections.

        :returns:
            A callable that returns a mock connection resource.
        """
        mock_connection_resource = mocker.MagicMock(
            __aenter__=mocker.AsyncMock(return_value=mock_workers[worker_info]),
            __aexit__=mocker.AsyncMock(return_value=None),
        )
        return lambda: mock_connection_resource

    return factory


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
            mock_connection = mocker.create_autospec(Connection, instance=True)
            mock_connection.dispatch = mocker.AsyncMock()
            worker_id = f"worker-{i}"
            worker_info = WorkerInfo(
                uid=worker_id,
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
            mock_connection = mocker.create_autospec(Connection, instance=True)
            mock_connection.dispatch = mocker.AsyncMock()
            worker_id = f"worker-{i}"
            worker_info = WorkerInfo(
                uid=worker_id,
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
