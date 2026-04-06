import asyncio
import gc
import pickle
import weakref
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.loadbalancer.roundrobin import RoundRobinLoadBalancer
from wool.runtime.worker.connection import TransientRpcError
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata

# ``delegate`` takes the routed task as its first positional argument.
# ``RoundRobinLoadBalancer`` ignores it, so a single opaque sentinel
# stands in for the task across every dispatch in this module.
_TASK = object()


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


class _ItemsCountingContext:
    """LoadBalancerContextView that counts workers.items() materializations.

    Wraps a real `LoadBalancerContext` and records how many times
    the ordered worker mapping is materialized via ``items()``, so a test
    can prove the balancer snapshots once per wrap rather than rescanning
    the mapping per candidate.
    """

    def __init__(self, context: LoadBalancerContext):
        self._context = context
        self.items_calls = 0

    @property
    def workers(self):
        real = self._context.workers
        outer = self

        class _CountingMapping:
            def __len__(self):
                return len(real)

            def __contains__(self, key):
                return key in real

            def items(self):
                outer.items_calls += 1
                return real.items()

        return _CountingMapping()


class TestRoundRobinLoadBalancer:
    def test_isinstance_satisfies_protocol(self):
        """Test RoundRobinLoadBalancer satisfies LoadBalancerLike.

        Given:
            A concrete RoundRobinLoadBalancer instance
        When:
            Checked against the LoadBalancerLike protocol
        Then:
            It should satisfy the protocol
        """
        # Act & assert
        assert isinstance(RoundRobinLoadBalancer(), LoadBalancerLike)

    @pytest.mark.asyncio
    async def test___reduce___excludes_runtime_state(self):
        """Test pickle roundtrip resets runtime balancing state.

        Given:
            A RoundRobinLoadBalancer that has been driven through one
            successful delegate cycle on a context
        When:
            Pickled and unpickled
        Then:
            The restored instance starts cycling from position zero on
            the same context — equivalent to a freshly constructed
            instance, proving the runtime state is not carried over
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)

        generator = loadbalancer.delegate(_TASK, context=ctx)
        first_used, _ = await anext(generator)
        with pytest.raises(StopAsyncIteration):
            await generator.asend(first_used)
        # Original has now advanced past workers[0] on ctx.

        # Act
        restored = pickle.loads(pickle.dumps(loadbalancer))

        # Assert
        assert isinstance(restored, RoundRobinLoadBalancer)
        restored_generator = restored.delegate(_TASK, context=ctx)
        first_after_restore, _ = await anext(restored_generator)
        await restored_generator.aclose()
        assert first_after_restore == workers[0]

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
        loadbalancer = RoundRobinLoadBalancer()
        ctx = LoadBalancerContext()

        # Act & assert
        generator = loadbalancer.delegate(_TASK, context=ctx)
        with pytest.raises(StopAsyncIteration):
            await anext(generator)
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_with_single_worker(self):
        """Test delegate yields the first worker on initial anext.

        Given:
            A load balancer with a single worker in the context
        When:
            delegate() is driven with anext
        Then:
            The first yielded candidate is the worker in the context.
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, [worker] = _make_context(1)

        # Act
        generator = loadbalancer.delegate(_TASK, context=ctx)
        metadata, connection = await anext(generator)

        # Assert
        assert metadata == worker
        assert connection is ctx.workers[worker]
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_after_prior_success(self):
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
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)

        # Act
        generator = loadbalancer.delegate(_TASK, context=ctx)
        first_metadata, _ = await anext(generator)
        with pytest.raises(StopAsyncIteration):
            await generator.asend(first_metadata)

        second_generator = loadbalancer.delegate(_TASK, context=ctx)
        second_metadata, _ = await anext(second_generator)

        # Assert
        assert first_metadata == workers[0]
        assert second_metadata == workers[1]
        await second_generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_after_athrow(self):
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
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)

        # Act
        generator = loadbalancer.delegate(_TASK, context=ctx)
        first_metadata, _ = await anext(generator)
        second_metadata, _ = await generator.athrow(TransientRpcError())

        # Assert
        assert first_metadata == workers[0]
        assert second_metadata == workers[1]
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_with_all_workers_failing(self):
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
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(5)

        # Act
        yielded: list[WorkerMetadata] = []
        generator = loadbalancer.delegate(_TASK, context=ctx)
        metadata, _ = await anext(generator)
        yielded.append(metadata)
        while True:
            try:
                metadata, _ = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                break
            yielded.append(metadata)

        # Assert
        assert len(yielded) == len(workers)
        assert set(yielded) == set(workers)

    @pytest.mark.asyncio
    async def test_delegate_with_context_eviction(self):
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
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)

        # Act
        generator = loadbalancer.delegate(_TASK, context=ctx)
        first_metadata, _ = await anext(generator)
        ctx.remove_worker(first_metadata)  # simulate proxy eviction
        second_metadata, _ = await generator.athrow(Exception("fatal"))

        # Assert
        assert first_metadata == workers[0]
        assert second_metadata != first_metadata
        assert second_metadata in workers
        assert first_metadata not in ctx.workers
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_with_asend_success(self):
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
        loadbalancer = RoundRobinLoadBalancer()
        ctx, _ = _make_context(3)

        # Act
        generator = loadbalancer.delegate(_TASK, context=ctx)
        metadata, _ = await anext(generator)

        # Assert
        with pytest.raises(StopAsyncIteration):
            await generator.asend(metadata)

    @pytest.mark.asyncio
    @settings(max_examples=32)
    @given(
        worker_count=st.integers(min_value=2, max_value=8),
        failure_count=st.integers(min_value=0, max_value=7),
    )
    async def test_delegate_with_failures_then_success(
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
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(worker_count)

        # Act
        generator = loadbalancer.delegate(_TASK, context=ctx)
        yielded: list[WorkerMetadata] = []
        metadata, _ = await anext(generator)
        yielded.append(metadata)
        for _ in range(failure_count):
            metadata, _ = await generator.athrow(TransientRpcError())
            yielded.append(metadata)
        with pytest.raises(StopAsyncIteration):
            await generator.asend(yielded[-1])

        # Assert
        assert yielded == workers[: failure_count + 1]

    @pytest.mark.asyncio
    @settings(max_examples=32)
    @given(
        worker_count=st.integers(min_value=1, max_value=8),
        dispatch_count=st.integers(min_value=1, max_value=24),
    )
    async def test_delegate_with_successive_calls(
        self, worker_count: int, dispatch_count: int
    ):
        """Test delegate rotates the starting worker across calls.

        Given:
            A load balancer with N workers driven through K successive
            delegate() calls, each completing with asend
        When:
            Each call is driven to its first candidate and then to
            success
        Then:
            Dispatch k starts at workers[k modulo N], wrapping around
            past the end of the worker list.
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(worker_count)

        # Act
        starts: list[WorkerMetadata] = []
        for _ in range(dispatch_count):
            generator = loadbalancer.delegate(_TASK, context=ctx)
            metadata, _ = await anext(generator)
            starts.append(metadata)
            with pytest.raises(StopAsyncIteration):
                await generator.asend(metadata)

        # Assert
        expected = [workers[k % worker_count] for k in range(dispatch_count)]
        assert starts == expected

    @pytest.mark.asyncio
    async def test_delegate_with_concurrent_drivers(self):
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
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(4)

        async def drive_one() -> WorkerMetadata:
            generator = loadbalancer.delegate(_TASK, context=ctx)
            metadata, _ = await anext(generator)
            try:
                with pytest.raises(StopAsyncIteration):
                    await generator.asend(metadata)
            finally:
                await generator.aclose()
            return metadata

        # Act
        results = await asyncio.gather(*[drive_one() for _ in range(4)])

        # Assert
        assert set(results) == set(workers)

    @pytest.mark.asyncio
    async def test_delegate_terminates_when_checkpoint_evicted(self):
        """Test delegate ends when the checkpoint worker is evicted.

        Given:
            A balancer whose first-yielded (checkpoint) candidate is
            evicted from the context, while every surviving worker keeps
            failing transiently forever
        When:
            The proxy drives the generator with repeated athrow
        Then:
            It should raise StopAsyncIteration rather than spin forever —
            the cycle boundary is reseeded once the checkpoint worker
            leaves the pool.
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)
        generator = loadbalancer.delegate(_TASK, context=ctx)
        first, _ = await anext(generator)
        ctx.remove_worker(first)  # proxy evicts the checkpoint (non-transient)

        # Act
        yielded: list[WorkerMetadata] = [first]
        terminated = False
        for _ in range(100):
            try:
                metadata, _ = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                terminated = True
                break
            yielded.append(metadata)

        # Assert
        assert terminated, "delegate did not terminate after checkpoint eviction"
        assert first not in ctx.workers
        assert first not in yielded[1:]

    @pytest.mark.asyncio
    async def test_delegate_skips_worker_evicted_after_snapshot(self):
        """Test delegate skips a candidate evicted after the snapshot.

        Given:
            A balancer that has snapshotted three workers and then has an
            ahead-of-cursor worker evicted before the cursor reaches it
        When:
            The proxy drives the generator to exhaustion via athrow
        Then:
            The evicted worker is never yielded — it is skipped against
            the live pool — and the generator still terminates.
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)
        generator = loadbalancer.delegate(_TASK, context=ctx)
        first, _ = await anext(generator)
        ctx.remove_worker(workers[2])  # evict a not-yet-reached worker

        # Act
        yielded: list[WorkerMetadata] = [first]
        terminated = False
        for _ in range(100):
            try:
                metadata, _ = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                terminated = True
                break
            yielded.append(metadata)

        # Assert
        assert terminated, "delegate did not terminate after mid-cycle eviction"
        assert workers[2] not in ctx.workers
        assert workers[2] not in yielded

    @pytest.mark.asyncio
    async def test_delegate_snapshots_once_per_wrap(self):
        """Test delegate materializes the worker order once per wrap.

        Given:
            A balancer cycling a 16-worker pool through one full cycle of
            transient failures, observed through a context view that
            counts ``workers.items()`` materializations
        When:
            The generator is driven to exhaustion via athrow
        Then:
            The ordered snapshot is materialized a small constant number
            of times (once per wrap), not once per candidate — proving the
            O(1) cursor replaced the former O(index) per-candidate scan.
        """
        # Arrange
        ctx, workers = _make_context(16)
        counting = _ItemsCountingContext(ctx)
        loadbalancer = RoundRobinLoadBalancer()

        # Act
        generator = loadbalancer.delegate(_TASK, context=counting)
        yielded: list[WorkerMetadata] = []
        metadata, _ = await anext(generator)
        yielded.append(metadata)
        while True:
            try:
                metadata, _ = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                break
            yielded.append(metadata)

        # Assert
        assert len(yielded) == len(workers)
        assert counting.items_calls <= 2

    @pytest.mark.asyncio
    async def test_delegate_does_not_pin_retired_contexts(self):
        """Test the balancer does not pin retired contexts against GC.

        Given:
            A shared RoundRobinLoadBalancer that has served a context the
            caller then drops every strong reference to
        When:
            A garbage collection is forced
        Then:
            The context is reclaimed — the balancer holds it only weakly,
            so a weakref to it resolves to None (no per-context leak of
            the context or the connections it references).
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, _ = _make_context(2)
        ref = weakref.ref(ctx)
        generator = loadbalancer.delegate(_TASK, context=ctx)
        await anext(generator)  # registers ctx in the balancer's rotation state
        await generator.aclose()

        # Act
        del ctx, generator
        gc.collect()

        # Assert
        assert ref() is None
