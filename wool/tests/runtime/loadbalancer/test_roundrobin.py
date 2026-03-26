import asyncio
from asyncio import TimeoutError
from uuid import uuid4

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from pytest_mock import MockerFixture

from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.routine.task import Task
from wool.runtime.routine.task import WorkerProxyLike
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata


@st.composite
def dispatch_side_effects(
    draw, min_size: int, max_size: int, include_transient: bool = True
):
    """Generate behavior sequence for a single task dispatch.

    Generates a list of failure side effects. Failure modes may be any of
    Exception, RpcError, TransientRpcError, or TimeoutError.

    :param draw:
        Hypothesis draw function
    :param min_size:
        Minimum number of failures to generate
    :param max_size:
        Maximum number of failures to generate
    :param include_transient:
        Whether to include TransientRpcError in the possible failures.
        When False, only non-transient errors are generated (useful for
        testing scenarios where workers should be removed).

    :returns:
        List of side effects (exceptions)
    """
    if include_transient:
        error_types = [Exception(), RpcError(), TransientRpcError(), TimeoutError()]
    else:
        # Only non-transient errors - workers will be removed after these
        error_types = [Exception(), RpcError(), TimeoutError()]

    return draw(
        st.lists(
            st.sampled_from(error_types),
            min_size=min_size,
            max_size=max_size,
        )
    )


class TestRoundRobinLoadBalancer:
    def test_isinstance_satisfies_protocol(self):
        """Test RoundRobinLoadBalancer satisfies the
        LoadBalancerLike protocol.

        Given:
            A concrete RoundRobinLoadBalancer instance
        When:
            Checked against the LoadBalancerLike protocol
        Then:
            It should satisfy the protocol
        """
        # Act & assert
        assert isinstance(RoundRobinLoadBalancer(), LoadBalancerLike)

    def test___reduce___with_populated_index(self):
        """Test pickle roundtrip excludes runtime connection state.

        Given:
            A RoundRobinLoadBalancer with a populated _index
        When:
            Pickled and unpickled
        Then:
            It should restore with an empty _index and a fresh _lock
        """
        # Arrange
        import pickle
        from asyncio import Lock

        lb = RoundRobinLoadBalancer()
        context = LoadBalancerContext()
        lb._index[context] = 3

        # Act
        restored = pickle.loads(pickle.dumps(lb))

        # Assert
        assert isinstance(restored, RoundRobinLoadBalancer)
        assert restored._index == {}
        assert isinstance(restored._lock, Lock)

    @pytest.mark.asyncio
    async def test_dispatch_with_empty_context(
        self,
        mocker: MockerFixture,
    ):
        """Test dispatch raises NoWorkersAvailable with empty
        context.

        Given:
            A load balancer with an empty context (no workers)
        When:
            A task is dispatched
        Then:
            NoWorkersAvailable is raised
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()

        async def routine():
            return "Hello world!"

        mock_proxy = mocker.MagicMock(spec=WorkerProxyLike, id="mock-proxy")

        task = Task(
            id=uuid4(),
            callable=routine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await lb.dispatch(task, context=ctx)

    @pytest.mark.asyncio
    @settings(
        max_examples=16,
        suppress_health_check=[
            HealthCheck.function_scoped_fixture,
        ],
    )
    @given(data=st.data())
    async def test_dispatch_with_healthy_workers(
        self,
        mocker: MockerFixture,
        dispatch_side_effect_factory,
        data: st.DataObject,
    ):
        """Test tasks dispatch successfully when there's at least
        one healthy worker available.

        Given:
            A load balancer with one or more workers with randomized
            behavior
        When:
            One or more tasks are dispatched
        Then:
            The tasks should be dispatched to workers in round-robin
            order, with unhealthy workers being removed from the
            loadbalancer
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()
        worker_count = 8
        mock_workers = {}

        for i in range(worker_count):
            mock_connection = mocker.create_autospec(WorkerConnection, instance=True)
            mock_connection.dispatch = mocker.AsyncMock()
            metadata = WorkerMetadata(
                uid=uuid4(),
                address="localhost:50051",
                pid=1000 + i,
                version="1.0.0",
            )
            mock_workers[metadata] = mock_connection
            ctx.add_worker(metadata, mock_connection)

        async def routine():
            return "Hello world!"

        mock_proxy = mocker.MagicMock(spec=WorkerProxyLike, id="mock-proxy")

        task = Task(
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
            for metadata in workers_remaining:
                mock_connection = mock_workers[metadata]
                mock_connection.dispatch.reset_mock()
                mock_connection.dispatch.side_effect = make_dispatch_side_effect(
                    metadata
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
    async def test_dispatch_with_all_workers_failing(
        self,
        mocker: MockerFixture,
        dispatch_side_effect_factory,
        data: st.DataObject,
    ):
        """Test task dispatch fails when there are no healthy workers
        available.

        Given:
            A load balancer with one or more workers with a
            randomized failure mode
        When:
            One or more tasks are dispatched
        Then:
            The tasks should be dispatched to workers in round-robin
            order and raise NoWorkersAvailable, with unhealthy
            workers being removed from the loadbalancer
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()
        worker_count = 8
        mock_workers = {}

        for i in range(worker_count):
            mock_connection = mocker.create_autospec(WorkerConnection, instance=True)
            mock_connection.dispatch = mocker.AsyncMock()
            metadata = WorkerMetadata(
                uid=uuid4(),
                address="localhost:50051",
                pid=1000 + i,
                version="1.0.0",
            )
            mock_workers[metadata] = mock_connection
            ctx.add_worker(metadata, mock_connection)

        async def routine():
            return "Hello world!"

        mock_proxy = mocker.MagicMock(spec=WorkerProxyLike, id="mock-proxy")

        task = Task(
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
            # Use only non-transient errors since transient errors cause retries
            # and we want exactly one dispatch per worker before NoWorkersAvailable
            side_effects = data.draw(
                dispatch_side_effects(
                    min_size=workers_remaining_count,
                    max_size=workers_remaining_count,
                    include_transient=False,
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
            for metadata in workers_remaining:
                mock_connection = mock_workers[metadata]
                mock_connection.dispatch.reset_mock()
                mock_connection.dispatch.side_effect = make_dispatch_side_effect(
                    metadata
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

    @pytest.mark.asyncio
    async def test_dispatch_with_concurrent_tasks(
        self,
        mocker: MockerFixture,
    ):
        """Test concurrent dispatches are distributed across
        different workers.

        Given:
            A load balancer with 4 workers
        When:
            4 tasks are dispatched concurrently (in parallel)
        Then:
            Each task goes to a different worker (not all to
            worker 0)
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()
        worker_count = 4
        mock_workers = {}
        workers_that_received_dispatch = []

        for i in range(worker_count):
            mock_connection = mocker.create_autospec(WorkerConnection, instance=True)
            metadata = WorkerMetadata(
                uid=uuid4(),
                address=f"localhost:{50051 + i}",
                pid=1000 + i,
                version="1.0.0",
            )

            # Create dispatch function that tracks which worker received it
            async def dispatch_fn(task, *, timeout=None, m=metadata):
                workers_that_received_dispatch.append(m)
                return f"result-{m.address}"

            mock_connection.dispatch = mocker.AsyncMock(side_effect=dispatch_fn)
            mock_workers[metadata] = mock_connection
            ctx.add_worker(metadata, mock_connection)

        async def routine():
            return "Hello world!"

        mock_proxy = mocker.MagicMock(spec=WorkerProxyLike, id="mock-proxy")

        tasks = [
            Task(
                id=uuid4(),
                callable=routine,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )
            for _ in range(4)
        ]

        # Act
        results = await asyncio.gather(
            *[lb.dispatch(task, context=ctx) for task in tasks]
        )

        # Assert
        assert len(results) == 4
        unique_workers = set(w.uid for w in workers_that_received_dispatch)
        assert len(unique_workers) == 4, (
            f"Expected 4 unique workers, got {len(unique_workers)}. "
            f"Workers used: {[w.address for w in workers_that_received_dispatch]}"
        )

    @pytest.mark.asyncio
    async def test_dispatch_with_lock_release_on_success(
        self,
        mocker: MockerFixture,
    ):
        """Test that worker lock is released after successful
        dispatch.

        Given:
            A load balancer with workers, one dispatch completes
        When:
            Another dispatch is initiated
        Then:
            The previously-busy worker is available again for
            selection
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()
        mock_workers = {}
        workers_that_received_dispatch = []

        mock_connection = mocker.create_autospec(WorkerConnection, instance=True)
        metadata = WorkerMetadata(
            uid=uuid4(),
            address="localhost:50051",
            pid=1000,
            version="1.0.0",
        )

        mock_connection.dispatch = mocker.AsyncMock(
            side_effect=lambda t, timeout=None: (
                workers_that_received_dispatch.append(metadata),
                "success",
            )[-1]
        )
        mock_workers[metadata] = mock_connection
        ctx.add_worker(metadata, mock_connection)

        async def routine():
            return "Hello world!"

        mock_proxy = mocker.MagicMock(spec=WorkerProxyLike, id="mock-proxy")

        task1 = Task(
            id=uuid4(),
            callable=routine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )
        task2 = Task(
            id=uuid4(),
            callable=routine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Act
        await lb.dispatch(task1, context=ctx)
        await lb.dispatch(task2, context=ctx)

        # Assert
        assert len(workers_that_received_dispatch) == 2
        assert workers_that_received_dispatch[0] == workers_that_received_dispatch[1]

    @pytest.mark.asyncio
    async def test_dispatch_with_transient_error_retry(
        self,
        mocker: MockerFixture,
    ):
        """Test that worker lock is released after transient error.

        Given:
            A load balancer with workers, dispatch fails with
            TransientRpcError
        When:
            The worker is retried on subsequent dispatch
        Then:
            Worker remains in pool and lock is released for retry
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()
        mock_workers = {}
        dispatch_count = [0]

        mock_connection = mocker.create_autospec(WorkerConnection, instance=True)
        metadata = WorkerMetadata(
            uid=uuid4(),
            address="localhost:50051",
            pid=1000,
            version="1.0.0",
        )

        async def dispatch_with_transient_then_success(task, *, timeout=None):
            dispatch_count[0] += 1
            if dispatch_count[0] == 1:
                raise TransientRpcError()
            return "success"

        mock_connection.dispatch = mocker.AsyncMock(
            side_effect=dispatch_with_transient_then_success
        )
        mock_workers[metadata] = mock_connection
        ctx.add_worker(metadata, mock_connection)

        async def routine():
            return "Hello world!"

        mock_proxy = mocker.MagicMock(spec=WorkerProxyLike, id="mock-proxy")

        task = Task(
            id=uuid4(),
            callable=routine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Act & assert
        with pytest.raises(NoWorkersAvailable):
            await lb.dispatch(task, context=ctx)

        assert dispatch_count[0] == 1
        assert len(ctx.workers) == 1

        result = await lb.dispatch(task, context=ctx)

        assert result == "success"
        assert dispatch_count[0] == 2
        assert len(ctx.workers) == 1

    @pytest.mark.asyncio
    async def test_dispatch_with_worker_removal(
        self,
        mocker: MockerFixture,
    ):
        """Test that worker lock is cleaned up when worker is
        removed.

        Given:
            A load balancer with workers, dispatch fails with
            non-transient error
        When:
            Worker is removed from the pool
        Then:
            Worker lock is cleaned up and subsequent dispatches
            route correctly
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()
        mock_workers = {}

        for i in range(2):
            mock_connection = mocker.create_autospec(WorkerConnection, instance=True)
            metadata = WorkerMetadata(
                uid=uuid4(),
                address=f"localhost:{50051 + i}",
                pid=1000 + i,
                version="1.0.0",
            )

            if i == 0:
                mock_connection.dispatch = mocker.AsyncMock(
                    side_effect=Exception("fatal error")
                )
            else:
                mock_connection.dispatch = mocker.AsyncMock(return_value="success")

            mock_workers[metadata] = mock_connection
            ctx.add_worker(metadata, mock_connection)

        async def routine():
            return "Hello world!"

        mock_proxy = mocker.MagicMock(spec=WorkerProxyLike, id="mock-proxy")

        task = Task(
            id=uuid4(),
            callable=routine,
            args=(),
            kwargs={},
            proxy=mock_proxy,
        )

        # Act
        result = await lb.dispatch(task, context=ctx)

        # Assert
        assert result == "success"
        assert len(ctx.workers) == 1

    @pytest.mark.asyncio
    async def test_dispatch_with_overflow_tasks(
        self,
        mocker: MockerFixture,
    ):
        """Test that waiting tasks are assigned to different workers
        in round-robin.

        Given:
            A load balancer with 4 workers, dispatches blocked by
            locks
        When:
            8 tasks are dispatched concurrently (more than workers)
        Then:
            Each worker receives exactly 2 tasks (round-robin
            distribution)
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()
        worker_count = 4
        mock_workers = {}
        workers_that_received_dispatch: list = []
        dispatch_count = [0]
        all_dispatches_queued = asyncio.Event()
        proceed_with_dispatch = asyncio.Event()

        for i in range(worker_count):
            mock_connection = mocker.create_autospec(WorkerConnection, instance=True)
            metadata = WorkerMetadata(
                uid=uuid4(),
                address=f"localhost:{50051 + i}",
                pid=1000 + i,
                version="1.0.0",
            )

            async def dispatch_fn(task, *, timeout=None, m=metadata):
                dispatch_count[0] += 1
                workers_that_received_dispatch.append(m)
                if dispatch_count[0] >= 8:
                    all_dispatches_queued.set()
                await proceed_with_dispatch.wait()
                return f"result-{m.address}"

            mock_connection.dispatch = mocker.AsyncMock(side_effect=dispatch_fn)
            mock_workers[metadata] = mock_connection
            ctx.add_worker(metadata, mock_connection)

        async def routine():
            return "Hello world!"

        mock_proxy = mocker.MagicMock(spec=WorkerProxyLike, id="mock-proxy")

        tasks = [
            Task(
                id=uuid4(),
                callable=routine,
                args=(),
                kwargs={},
                proxy=mock_proxy,
            )
            for _ in range(8)
        ]

        # Act
        dispatch_futures = [
            asyncio.create_task(lb.dispatch(task, context=ctx)) for task in tasks
        ]

        try:
            await asyncio.wait_for(all_dispatches_queued.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            pass

        proceed_with_dispatch.set()
        results = await asyncio.gather(*dispatch_futures)

        # Assert
        assert len(results) == 8
        worker_task_counts = {}
        for w in workers_that_received_dispatch:
            worker_task_counts[w.uid] = worker_task_counts.get(w.uid, 0) + 1

        assert len(worker_task_counts) == 4, (
            f"Expected 4 workers, got {len(worker_task_counts)}"
        )
        for uid, count in worker_task_counts.items():
            assert count == 2, f"Expected 2 tasks per worker, worker {uid} got {count}"
