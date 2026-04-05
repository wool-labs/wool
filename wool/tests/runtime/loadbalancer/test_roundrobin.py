import asyncio
from uuid import uuid4

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.loadbalancer.base import DelegatingLoadBalancerLike
from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.worker.connection import RpcError
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata


def _make_context(worker_count: int) -> tuple[LoadBalancerContext, list[WorkerMetadata]]:
    """Build a LoadBalancerContext seeded with ``worker_count`` workers.

    :returns:
        Tuple of ``(context, ordered_metadata_list)`` so tests can
        assert yielded candidates against insertion order.
    """
    ctx = LoadBalancerContext()
    ordered: list[WorkerMetadata] = []
    for i in range(worker_count):
        metadata = WorkerMetadata(
            uid=uuid4(),
            address=f"localhost:{50051 + i}",
            pid=1000 + i,
            version="1.0.0",
        )
        connection = WorkerConnection(f"localhost:{50051 + i}")
        ctx.add_worker(metadata, connection)
        ordered.append(metadata)
    return ctx, ordered


class TestRoundRobinLoadBalancer:
    def test_isinstance_satisfies_protocol(self):
        """Test RoundRobinLoadBalancer satisfies DelegatingLoadBalancerLike.

        Given:
            A concrete RoundRobinLoadBalancer instance
        When:
            Checked against the DelegatingLoadBalancerLike protocol
        Then:
            It should satisfy the protocol
        """
        # Act & assert
        assert isinstance(RoundRobinLoadBalancer(), DelegatingLoadBalancerLike)

    def test___reduce___with_populated_index(self):
        """Test pickle roundtrip excludes runtime state.

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
    async def test_delegate_with_empty_context(self):
        """Test delegate ends immediately on an empty context.

        Given:
            A load balancer with an empty context (no workers)
        When:
            delegate() is driven with anext
        Then:
            The first anext raises StopAsyncIteration — signaling
            exhaustion so the proxy can raise NoWorkersAvailable.
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()

        # Act & assert
        gen = lb.delegate(context=ctx)
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()
        await gen.aclose()

    @pytest.mark.asyncio
    async def test_delegate_yields_first_worker_on_anext(self):
        """Test delegate yields the first worker on initial anext.

        Given:
            A load balancer with a single worker in the context
        When:
            delegate() is driven with anext
        Then:
            The first yielded candidate is the worker in the context.
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx, [worker] = _make_context(1)

        # Act
        gen = lb.delegate(context=ctx)
        metadata, connection = await gen.__anext__()

        # Assert
        assert metadata == worker
        assert connection is ctx.workers[worker]
        await gen.aclose()

    @pytest.mark.asyncio
    async def test_delegate_advances_index_after_success(self):
        """Test delegate advances the round-robin index after asend.

        Given:
            A load balancer with multiple workers and a prior
            successful dispatch to worker 0
        When:
            delegate() is driven again with anext
        Then:
            The next yielded candidate is worker 1 — the index
            advanced past the previously-successful worker.
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)

        # Act
        gen = lb.delegate(context=ctx)
        first_metadata, _ = await gen.__anext__()
        with pytest.raises(StopAsyncIteration):
            await gen.asend(first_metadata)

        gen2 = lb.delegate(context=ctx)
        second_metadata, _ = await gen2.__anext__()

        # Assert
        assert first_metadata == workers[0]
        assert second_metadata == workers[1]
        await gen2.aclose()

    @pytest.mark.asyncio
    async def test_delegate_advances_index_after_athrow(self):
        """Test delegate advances the index on athrow (failure).

        Given:
            A load balancer with multiple workers, the first
            candidate has just been yielded
        When:
            The proxy reports failure via athrow
        Then:
            The next yielded candidate is the subsequent worker.
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)

        # Act
        gen = lb.delegate(context=ctx)
        first_metadata, _ = await gen.__anext__()
        second_metadata, _ = await gen.athrow(TransientRpcError())

        # Assert
        assert first_metadata == workers[0]
        assert second_metadata == workers[1]
        await gen.aclose()

    @pytest.mark.asyncio
    async def test_delegate_exhausts_after_full_cycle(self):
        """Test delegate ends after a full cycle of failures.

        Given:
            A load balancer whose context has N workers, all failing
        When:
            athrow is called repeatedly until the generator ends
        Then:
            Exactly N candidates are yielded before StopAsyncIteration
            — the checkpoint-by-UID logic terminates the cycle.
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx, workers = _make_context(5)

        # Act
        yielded: list[WorkerMetadata] = []
        gen = lb.delegate(context=ctx)
        metadata, _ = await gen.__anext__()
        yielded.append(metadata)
        while True:
            try:
                metadata, _ = await gen.athrow(TransientRpcError())
            except StopAsyncIteration:
                break
            yielded.append(metadata)

        # Assert
        assert len(yielded) == len(workers)
        assert set(yielded) == set(workers)

    @pytest.mark.asyncio
    async def test_delegate_reacts_to_context_eviction(self):
        """Test delegate observes pool mutations between yields.

        Given:
            A load balancer with multiple workers, the first
            candidate has been yielded and the proxy (simulated here)
            evicts it from the context before calling athrow
        When:
            The proxy reports failure via athrow
        Then:
            The next yielded candidate is drawn from the mutated
            (smaller) context and is not the evicted worker.
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)

        # Act
        gen = lb.delegate(context=ctx)
        first_metadata, _ = await gen.__anext__()
        ctx.remove_worker(first_metadata)  # simulate proxy eviction
        second_metadata, _ = await gen.athrow(Exception("fatal"))

        # Assert
        assert first_metadata == workers[0]
        assert second_metadata != first_metadata
        assert second_metadata in workers
        assert first_metadata not in ctx.workers
        await gen.aclose()

    @pytest.mark.asyncio
    async def test_delegate_terminates_on_asend_success(self):
        """Test delegate terminates after asend signals success.

        Given:
            A load balancer with multiple workers, the first
            candidate has just been yielded
        When:
            The proxy reports success via asend(metadata)
        Then:
            The generator raises StopAsyncIteration — honoring the
            contract that asend is terminal.
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx, _ = _make_context(3)

        # Act
        gen = lb.delegate(context=ctx)
        metadata, _ = await gen.__anext__()

        # Assert
        with pytest.raises(StopAsyncIteration):
            await gen.asend(metadata)

    @pytest.mark.asyncio
    @settings(
        max_examples=32,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        worker_count=st.integers(min_value=2, max_value=8),
        failure_count=st.integers(min_value=0, max_value=7),
    )
    async def test_delegate_yields_expected_round_robin_sequence(
        self, worker_count: int, failure_count: int
    ):
        """Test delegate respects round-robin fairness across failures.

        Given:
            A load balancer with N workers and a prefix of F failures
            followed by one success (F < N)
        When:
            The proxy drives the generator through the sequence
        Then:
            The yielded candidates are workers[0..F] in order and the
            successful one is workers[F].
        """
        # Arrange
        failure_count = min(failure_count, worker_count - 1)
        lb = RoundRobinLoadBalancer()
        ctx, workers = _make_context(worker_count)

        # Act
        gen = lb.delegate(context=ctx)
        yielded: list[WorkerMetadata] = []
        metadata, _ = await gen.__anext__()
        yielded.append(metadata)
        for _ in range(failure_count):
            metadata, _ = await gen.athrow(TransientRpcError())
            yielded.append(metadata)
        with pytest.raises(StopAsyncIteration):
            await gen.asend(yielded[-1])

        # Assert
        assert yielded == workers[: failure_count + 1]

    @pytest.mark.asyncio
    async def test_delegate_with_concurrent_tasks_distributes_across_workers(self):
        """Test concurrent delegate drivers land on distinct workers.

        Given:
            A load balancer with 4 workers
        When:
            4 dispatches are driven concurrently through independent
            delegate() calls, each completing with asend
        Then:
            Each dispatch lands on a distinct worker (fairness).
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx, workers = _make_context(4)

        async def drive_one() -> WorkerMetadata:
            gen = lb.delegate(context=ctx)
            metadata, _ = await gen.__anext__()
            try:
                with pytest.raises(StopAsyncIteration):
                    await gen.asend(metadata)
            finally:
                await gen.aclose()
            return metadata

        # Act
        results = await asyncio.gather(*[drive_one() for _ in range(4)])

        # Assert
        assert set(results) == set(workers)

    @pytest.mark.asyncio
    async def test_delegate_with_non_transient_error_via_athrow(self):
        """Test delegate treats non-transient errors the same as transient.

        Given:
            A load balancer with multiple workers (the proxy is
            expected to have already evicted the failed worker)
        When:
            The proxy reports a non-transient error via athrow
        Then:
            The next candidate is yielded — the balancer itself does
            not classify errors or perform eviction; that's the
            proxy's job.
        """
        # Arrange
        lb = RoundRobinLoadBalancer()
        ctx, _ = _make_context(3)

        # Act
        gen = lb.delegate(context=ctx)
        first_metadata, _ = await gen.__anext__()
        # Simulate proxy-driven eviction
        ctx.remove_worker(first_metadata)
        second_metadata, _ = await gen.athrow(RpcError())

        # Assert
        assert first_metadata != second_metadata
        assert second_metadata in ctx.workers
        await gen.aclose()
