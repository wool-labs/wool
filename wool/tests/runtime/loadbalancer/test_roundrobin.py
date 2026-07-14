import asyncio
import gc
import pickle
import uuid
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
        assert yielded uids against insertion order.
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


def _evict(ctx: LoadBalancerContext, uid: uuid.UUID) -> None:
    """Evict a pooled worker by uid, as the proxy would."""
    ctx.remove_worker(ctx.workers[uid][0])


async def _advance_cursor(
    loadbalancer: RoundRobinLoadBalancer, ctx: LoadBalancerContext, dispatches: int
) -> None:
    """Drive ``dispatches`` successful delegate calls to warm the cursor.

    Each completed call advances the balancer's rotation cursor by one,
    so the next cycle starts mid-pool and wraps past the end of the
    snapshot before it exhausts.
    """
    for _ in range(dispatches):
        generator = loadbalancer.delegate(_TASK, context=ctx)
        uid = await anext(generator)
        with pytest.raises(StopAsyncIteration):
            await generator.asend(uid)


@st.composite
def refresh_injection(draw):
    """Generate a self-consistent refresh-injection scenario.

    Draws the dependent values in order so every drawn value lies in
    its own reachable domain: ``worker_count`` first, then the number
    of prior successful dispatches (which positions the balancer's
    cursor), then the position of the worker to refresh, then the yield
    after which the refresh lands. The injection step spans the whole
    cycle, so a refresh can land after the cursor wraps past the end of
    the snapshot — the case the checkpoint/boundary logic exists to
    handle.

    :param draw:
        Hypothesis draw function

    :returns:
        Tuple of ``(worker_count, warmup, position, injection_step)``
    """
    worker_count = draw(st.integers(min_value=2, max_value=6))
    warmup = draw(st.integers(min_value=0, max_value=worker_count - 1))
    position = draw(st.integers(min_value=0, max_value=worker_count - 1))
    injection_step = draw(st.integers(min_value=0, max_value=worker_count - 1))
    return worker_count, warmup, position, injection_step


class _SnapshotCountingContext:
    """LoadBalancerContextView that counts snapshot materializations.

    Wraps a real `LoadBalancerContext` and records how many times the
    worker mapping is materialized — by ``__iter__``, ``keys``,
    ``items``, or ``values`` — so a test can prove the balancer
    snapshots once per wrap rather than rescanning the mapping per
    candidate. Every other attribute delegates to the real
    `MappingProxyType`, so a balancer that materializes the mapping a
    different way fails the count assertion rather than crashing the
    double.
    """

    _MATERIALIZERS = frozenset({"keys", "items", "values"})

    def __init__(self, context: LoadBalancerContext):
        self._context = context
        self.snapshot_calls = 0

    @property
    def workers(self):
        real = self._context.workers
        outer = self

        class _CountingMapping:
            def __len__(self):
                return len(real)

            def __contains__(self, key):
                return key in real

            def __getitem__(self, key):
                return real[key]

            def __iter__(self):
                outer.snapshot_calls += 1
                return iter(real)

            def __getattr__(self, name):
                if name in outer._MATERIALIZERS:
                    outer.snapshot_calls += 1
                return getattr(real, name)

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
        first_used = await anext(generator)
        with pytest.raises(StopAsyncIteration):
            await generator.asend(first_used)
        # Original has now advanced past workers[0] on ctx.

        # Act
        restored = pickle.loads(pickle.dumps(loadbalancer))

        # Assert
        assert isinstance(restored, RoundRobinLoadBalancer)
        restored_generator = restored.delegate(_TASK, context=ctx)
        first_after_restore = await anext(restored_generator)
        await restored_generator.aclose()
        assert first_after_restore == workers[0].uid

    @pytest.mark.asyncio
    async def test_delegate_should_yield_nothing_when_context_empty(self):
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
    async def test_delegate_should_yield_sole_uid_when_one_worker_pooled(self):
        """Test delegate yields the first worker on initial anext.

        Given:
            A load balancer with a single worker in the context
        When:
            delegate() is driven with anext
        Then:
            The first yielded candidate is the uid of the worker in the
            context, and it resolves against the pool to that worker.
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, [worker] = _make_context(1)

        # Act
        generator = loadbalancer.delegate(_TASK, context=ctx)
        uid = await anext(generator)

        # Assert
        assert uid == worker.uid
        assert ctx.workers[uid][0] == worker
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_should_advance_cursor_when_prior_dispatch_succeeded(self):
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
        first_uid = await anext(generator)
        with pytest.raises(StopAsyncIteration):
            await generator.asend(first_uid)

        second_generator = loadbalancer.delegate(_TASK, context=ctx)
        second_uid = await anext(second_generator)

        # Assert
        assert first_uid == workers[0].uid
        assert second_uid == workers[1].uid
        await second_generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_should_advance_cursor_when_candidate_fails(self):
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
        first_uid = await anext(generator)
        second_uid = await generator.athrow(TransientRpcError())

        # Assert
        assert first_uid == workers[0].uid
        assert second_uid == workers[1].uid
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_should_end_cycle_when_every_worker_fails(self):
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
        yielded: list[uuid.UUID] = []
        generator = loadbalancer.delegate(_TASK, context=ctx)
        uid = await anext(generator)
        yielded.append(uid)
        while True:
            try:
                uid = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                break
            yielded.append(uid)

        # Assert
        assert len(yielded) == len(workers)
        assert set(yielded) == {worker.uid for worker in workers}

    @pytest.mark.asyncio
    async def test_delegate_should_skip_evicted_worker_when_pool_mutates_mid_cycle(self):
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
        first_uid = await anext(generator)
        _evict(ctx, first_uid)  # simulate proxy eviction
        second_uid = await generator.athrow(Exception("fatal"))

        # Assert
        assert first_uid == workers[0].uid
        assert second_uid != first_uid
        assert second_uid in {worker.uid for worker in workers}
        assert first_uid not in ctx.workers
        await generator.aclose()

    @pytest.mark.asyncio
    async def test_delegate_should_terminate_when_success_reported(self):
        """Test delegate terminates after asend signals success.

        Given:
            A load balancer with multiple workers, the first
            candidate has just been yielded
        When:
            The proxy reports success via asend(uid)
        Then:
            The generator raises StopAsyncIteration — honoring the
            contract that asend is terminal.
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, _ = _make_context(3)

        # Act
        generator = loadbalancer.delegate(_TASK, context=ctx)
        uid = await anext(generator)

        # Assert
        with pytest.raises(StopAsyncIteration):
            await generator.asend(uid)

    @pytest.mark.asyncio
    @settings(max_examples=32)
    @given(
        worker_count=st.integers(min_value=2, max_value=8),
        failure_count=st.integers(min_value=0, max_value=7),
    )
    async def test_delegate_should_offer_workers_in_order_when_failures_precede_success(
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
        yielded: list[uuid.UUID] = []
        uid = await anext(generator)
        yielded.append(uid)
        for _ in range(failure_count):
            uid = await generator.athrow(TransientRpcError())
            yielded.append(uid)
        with pytest.raises(StopAsyncIteration):
            await generator.asend(yielded[-1])

        # Assert
        assert yielded == [worker.uid for worker in workers[: failure_count + 1]]

    @pytest.mark.asyncio
    @settings(max_examples=32)
    @given(
        worker_count=st.integers(min_value=1, max_value=8),
        dispatch_count=st.integers(min_value=1, max_value=24),
    )
    async def test_delegate_should_rotate_start_when_called_successively(
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
        starts: list[uuid.UUID] = []
        for _ in range(dispatch_count):
            generator = loadbalancer.delegate(_TASK, context=ctx)
            uid = await anext(generator)
            starts.append(uid)
            with pytest.raises(StopAsyncIteration):
                await generator.asend(uid)

        # Assert
        expected = [workers[k % worker_count].uid for k in range(dispatch_count)]
        assert starts == expected

    @pytest.mark.asyncio
    async def test_delegate_should_offer_distinct_workers_when_drivers_concurrent(self):
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

        async def drive_one() -> uuid.UUID:
            generator = loadbalancer.delegate(_TASK, context=ctx)
            uid = await anext(generator)
            try:
                with pytest.raises(StopAsyncIteration):
                    await generator.asend(uid)
            finally:
                await generator.aclose()
            return uid

        # Act
        results = await asyncio.gather(*[drive_one() for _ in range(4)])

        # Assert
        assert set(results) == {worker.uid for worker in workers}

    @pytest.mark.asyncio
    async def test_delegate_should_terminate_when_checkpoint_evicted(self):
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
        ctx, _ = _make_context(3)
        generator = loadbalancer.delegate(_TASK, context=ctx)
        first = await anext(generator)
        _evict(ctx, first)  # proxy evicts the checkpoint (non-transient)

        # Act
        yielded: list[uuid.UUID] = [first]
        terminated = False
        for _ in range(100):
            try:
                uid = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                terminated = True
                break
            yielded.append(uid)

        # Assert
        assert terminated, "delegate did not terminate after checkpoint eviction"
        assert first not in ctx.workers
        assert first not in yielded[1:]

    @pytest.mark.asyncio
    async def test_delegate_should_skip_worker_when_evicted_after_snapshot(self):
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
        first = await anext(generator)
        ctx.remove_worker(workers[2])  # evict a not-yet-reached worker

        # Act
        yielded: list[uuid.UUID] = [first]
        terminated = False
        for _ in range(100):
            try:
                uid = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                terminated = True
                break
            yielded.append(uid)

        # Assert
        assert terminated, "delegate did not terminate after mid-cycle eviction"
        assert workers[2].uid not in ctx.workers
        assert workers[2].uid not in yielded

    @pytest.mark.asyncio
    async def test_delegate_should_exhaust_cycle_when_sole_worker_updated(self):
        """Test a mid-cycle refresh does not re-offer the sole worker.

        Given:
            A single-worker context whose worker is refreshed with a
            same-uid changed record and a new connection after its
            first yield
        When:
            The proxy reports the failure via athrow
        Then:
            It should exhaust the cycle — the worker's one attempt was
            already consumed
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(1)
        stale = workers[0]
        fresh = WorkerMetadata(
            uid=stale.uid,
            address="localhost:60000",
            pid=9999,
            version="1.0.0",
        )
        generator = loadbalancer.delegate(_TASK, context=ctx)
        first = await anext(generator)
        ctx.update_worker(fresh, WorkerConnection(fresh.address))

        # Act & assert
        assert first == stale.uid
        with pytest.raises(StopAsyncIteration):
            await generator.athrow(TransientRpcError())

    @pytest.mark.asyncio
    async def test_delegate_should_reoffer_uid_when_called_after_update(self):
        """Test a refreshed sole worker is offered again on the next call.

        Given:
            A single-worker context whose worker was refreshed with a
            same-uid changed record and a new connection while a
            delegate cycle was in flight, and that cycle has since
            exhausted
        When:
            A new delegate call is driven with anext
        Then:
            It should offer the same uid again
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(1)
        stale = workers[0]
        fresh = WorkerMetadata(
            uid=stale.uid,
            address="localhost:60000",
            pid=9999,
            version="1.0.0",
        )
        generator = loadbalancer.delegate(_TASK, context=ctx)
        await anext(generator)
        ctx.update_worker(fresh, WorkerConnection(fresh.address))
        with pytest.raises(StopAsyncIteration):
            await generator.athrow(TransientRpcError())

        # Act
        retry = loadbalancer.delegate(_TASK, context=ctx)
        uid = await anext(retry)
        await retry.aclose()

        # Assert — the balancer names workers; it is the pool that
        # carries the record, so a refresh cannot drop a worker from
        # the rotation.
        assert uid == stale.uid

    @pytest.mark.asyncio
    async def test_delegate_should_visit_uid_once_when_worker_updated_mid_cycle(self):
        """Test a mid-cycle refresh neither skips nor repeats the worker.

        Given:
            A three-worker context in which a not-yet-reached worker is
            refreshed with a same-uid changed record and a new
            connection after the first yield
        When:
            The proxy drives the generator to exhaustion via athrow
        Then:
            It should still visit each of the three uids exactly once
            and terminate
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)
        stale = workers[2]
        fresh = WorkerMetadata(
            uid=stale.uid,
            address="localhost:60000",
            pid=9999,
            version="1.0.0",
        )
        generator = loadbalancer.delegate(_TASK, context=ctx)
        first = await anext(generator)
        ctx.update_worker(fresh, WorkerConnection(fresh.address))

        # Act
        yielded: list[uuid.UUID] = [first]
        terminated = False
        for _ in range(100):
            try:
                uid = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                terminated = True
                break
            yielded.append(uid)

        # Assert — the refreshed worker keeps its place in the cycle
        # because a refresh changes the pooled record, not the uid the
        # balancer cycles on.
        assert terminated, "delegate did not terminate after mid-cycle update"
        assert yielded == [workers[0].uid, workers[1].uid, workers[2].uid]

    @pytest.mark.asyncio
    async def test_delegate_should_keep_boundary_when_checkpoint_updated(self):
        """Test the cycle boundary survives a checkpoint refresh.

        Given:
            A three-worker context whose first-yielded (checkpoint)
            worker is refreshed with a same-uid changed record
            immediately after its yield, so its uid remains in the pool
            under the fresh record
        When:
            The proxy drives the generator to termination via athrow
        Then:
            It should terminate without livelock when the boundary uid
            recurs, yielding the checkpoint uid exactly once and each
            uid at most twice
        """
        # Arrange
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(3)
        checkpoint = workers[0]
        fresh = WorkerMetadata(
            uid=checkpoint.uid,
            address="localhost:60000",
            pid=9999,
            version="1.0.0",
        )
        generator = loadbalancer.delegate(_TASK, context=ctx)
        first = await anext(generator)
        ctx.update_worker(fresh, WorkerConnection(fresh.address))

        # Act
        yielded: list[uuid.UUID] = [first]
        terminated = False
        for _ in range(100):
            try:
                uid = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                terminated = True
                break
            yielded.append(uid)

        # Assert
        assert terminated, "delegate did not terminate after checkpoint update"
        assert yielded.count(checkpoint.uid) == 1
        assert len(yielded) <= 2 * len(workers)
        uid_counts: dict = {}
        for uid in yielded:
            uid_counts[uid] = uid_counts.get(uid, 0) + 1
        assert all(count <= 2 for count in uid_counts.values())

    @pytest.mark.asyncio
    @settings(max_examples=50, deadline=None)
    @given(injection=refresh_injection())
    async def test_delegate_should_terminate_when_update_lands_at_any_step(
        self, injection: tuple[int, int, int, int]
    ):
        """Test refresh timing and position never break cycle termination.

        Given:
            A context of 2-6 workers, a drawn count of prior successful
            dispatches leaving the balancer's cursor anywhere in the
            pool, a drawn position of the worker to refresh, and a
            drawn injection step anywhere in the cycle — including
            after the cursor has wrapped past the end of the snapshot —
            with every dispatch failing transiently
        When:
            The refresh is injected after the drawn yield while athrow
            drives the generator to termination
        Then:
            It should always terminate, yielding at most two candidates
            per uid and at most two full cycles in total
        """
        # Arrange
        worker_count, warmup, position, injection_step = injection
        loadbalancer = RoundRobinLoadBalancer()
        ctx, workers = _make_context(worker_count)
        await _advance_cursor(loadbalancer, ctx, warmup)
        target = workers[position]
        fresh = WorkerMetadata(
            uid=target.uid,
            address="localhost:60000",
            pid=9999,
            version="1.0.0",
        )
        generator = loadbalancer.delegate(_TASK, context=ctx)

        # Act & assert
        yielded: list[uuid.UUID] = []
        terminated = False
        uid = await anext(generator)
        yielded.append(uid)
        if injection_step == 0:
            ctx.update_worker(fresh, WorkerConnection(fresh.address))
        for current in range(1, 3 * worker_count + 4):
            try:
                uid = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                terminated = True
                break
            yielded.append(uid)
            if current == injection_step:
                ctx.update_worker(fresh, WorkerConnection(fresh.address))

        assert terminated, "delegate did not terminate within the bound"
        assert len(yielded) <= 2 * worker_count
        uid_counts: dict = {}
        for uid in yielded:
            uid_counts[uid] = uid_counts.get(uid, 0) + 1
        assert all(count <= 2 for count in uid_counts.values())

    @pytest.mark.asyncio
    async def test_delegate_should_snapshot_once_per_wrap(self):
        """Test delegate materializes the worker order once per wrap.

        Given:
            A balancer cycling a 16-worker pool through one full cycle of
            transient failures, observed through a context view that
            counts snapshot materializations
        When:
            The generator is driven to exhaustion via athrow
        Then:
            The ordered snapshot is materialized a small constant number
            of times (once per wrap), not once per candidate — proving the
            O(1) cursor replaced the former O(index) per-candidate scan.
        """
        # Arrange
        ctx, workers = _make_context(16)
        counting = _SnapshotCountingContext(ctx)
        loadbalancer = RoundRobinLoadBalancer()

        # Act
        generator = loadbalancer.delegate(_TASK, context=counting)
        yielded: list[uuid.UUID] = []
        uid = await anext(generator)
        yielded.append(uid)
        while True:
            try:
                uid = await generator.athrow(TransientRpcError())
            except StopAsyncIteration:
                break
            yielded.append(uid)

        # Assert
        assert len(yielded) == len(workers)
        assert counting.snapshot_calls <= 2

    @pytest.mark.asyncio
    async def test_delegate_should_release_context_when_retired(self):
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
