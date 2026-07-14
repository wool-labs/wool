from dataclasses import replace
from types import MappingProxyType
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.runtime.loadbalancer.base import DispatchingLoadBalancerLike
from wool.runtime.loadbalancer.base import LoadBalancerContext
from wool.runtime.loadbalancer.base import LoadBalancerContextLike
from wool.runtime.loadbalancer.base import LoadBalancerContextView
from wool.runtime.loadbalancer.base import LoadBalancerLike
from wool.runtime.worker.connection import WorkerConnection
from wool.runtime.worker.metadata import WorkerMetadata

_MUTABLE_FIELDS = ("address", "pid", "version", "tags", "extra", "secure", "options")


@st.composite
def worker_metadata(draw):
    """Generate a WorkerMetadata with varied non-uid fields.

    Generated ports and pids deliberately exclude the sentinel values
    used by example-based tests (ports 60000/61000, pid 9999) so a
    record replaced with those sentinels always differs from any
    generated record.

    :param draw:
        Hypothesis draw function

    :returns:
        WorkerMetadata instance with randomized fields
    """
    port = draw(st.integers(min_value=50000, max_value=59999))
    return WorkerMetadata(
        uid=uuid4(),
        address=f"localhost:{port}",
        pid=draw(st.integers(min_value=1, max_value=9998)),
        version=draw(st.sampled_from(["1.0.0", "1.2.3", "2.0.1"])),
        tags=frozenset(
            draw(st.sets(st.sampled_from(["cpu", "gpu", "arm", "x86"]), max_size=3))
        ),
        extra=MappingProxyType(
            draw(
                st.dictionaries(
                    st.sampled_from(["zone", "rack", "tier"]),
                    st.text(alphabet="abcdef", min_size=1, max_size=4),
                    max_size=2,
                )
            )
        ),
        secure=draw(st.booleans()),
    )


@st.composite
def loadbalancer_context(draw, min_workers: int = 1):
    """Generate a LoadBalancerContext seeded with workers.

    Creates a LoadBalancerContext populated with randomly generated workers.
    Each worker has a unique uid, varied metadata fields, and a connection.

    :param draw:
        Hypothesis draw function

    :param min_workers:
        Minimum number of workers to seed; the maximum is always 8.

    :returns:
        LoadBalancerContext instance with ``min_workers``-8 workers
    """
    worker_count = draw(st.integers(min_value=min_workers, max_value=8))
    ctx = LoadBalancerContext()

    for _ in range(worker_count):
        metadata = draw(worker_metadata())
        ctx.add_worker(metadata, WorkerConnection(metadata.address))

    return ctx


def _mutate(metadata: WorkerMetadata, fields) -> WorkerMetadata:
    """Return a same-uid copy of metadata with the given fields changed.

    Every mutation differs from the original value by construction, so
    the returned record is never equal to the original.
    """
    mutations = {}
    for field in fields:
        match field:
            case "address":
                host, _, port = metadata.address.rpartition(":")
                mutations["address"] = f"{host}:{int(port) + 1}"
            case "pid":
                mutations["pid"] = metadata.pid + 1
            case "version":
                mutations["version"] = f"{metadata.version}.post1"
            case "tags":
                mutations["tags"] = metadata.tags | {"mutated"}
            case "extra":
                mutations["extra"] = MappingProxyType(
                    {**metadata.extra, "mutated": "true"}
                )
            case "secure":
                mutations["secure"] = not metadata.secure
            case "options":
                mutations["options"] = replace(
                    metadata.options,
                    max_concurrent_streams=metadata.options.max_concurrent_streams + 1,
                )
    return replace(metadata, **mutations)


class TestLoadBalancerContextLike:
    def test_isinstance_with_conforming_class(self):
        """Test a conforming class satisfies the protocol.

        Given:
            A class implementing all LoadBalancerContextLike methods
        When:
            Checked against the LoadBalancerContextLike protocol
        Then:
            It should satisfy the protocol
        """

        # Arrange
        class Conforming:
            @property
            def workers(self):
                return MappingProxyType({})

            def add_worker(self, metadata, connection):
                pass

            def update_worker(self, metadata, connection):
                pass

            def remove_worker(self, metadata):
                pass

        # Act & assert
        assert isinstance(Conforming(), LoadBalancerContextLike)

    def test_isinstance_with_non_conforming_class(self):
        """Test a non-conforming class does not satisfy the protocol.

        Given:
            A class missing one or more LoadBalancerContextLike
            methods
        When:
            Checked against the LoadBalancerContextLike protocol
        Then:
            It should not satisfy the protocol
        """

        # Arrange
        class NonConforming:
            @property
            def workers(self):
                return MappingProxyType({})

            def add_worker(self, metadata, connection):
                pass

        # Act & assert
        assert not isinstance(NonConforming(), LoadBalancerContextLike)


class TestLoadBalancerContextView:
    def test_isinstance_with_conforming_class(self):
        """Test a conforming class satisfies the protocol.

        Given:
            A class exposing the read-only view surface — a ``workers``
            property
        When:
            Checked against the LoadBalancerContextView protocol
        Then:
            It should satisfy the protocol
        """

        # Arrange
        class Conforming:
            @property
            def workers(self):
                return MappingProxyType({})

        # Act & assert
        assert isinstance(Conforming(), LoadBalancerContextView)

    def test_isinstance_with_non_conforming_class(self):
        """Test a non-conforming class does not satisfy the protocol.

        Given:
            A class missing the workers property
        When:
            Checked against the LoadBalancerContextView protocol
        Then:
            It should not satisfy the protocol
        """

        # Arrange
        class NonConforming:
            pass

        # Act & assert
        assert not isinstance(NonConforming(), LoadBalancerContextView)

    def test_isinstance_with_concrete_context(self):
        """Test LoadBalancerContext satisfies LoadBalancerContextView.

        Given:
            A concrete LoadBalancerContext instance
        When:
            Checked against the LoadBalancerContextView protocol
        Then:
            It should satisfy the protocol — the mutable context is
            also a valid read-only view.
        """
        # Act & assert
        assert isinstance(LoadBalancerContext(), LoadBalancerContextView)


class TestDispatchingLoadBalancerLike:
    def test_isinstance_with_conforming_class(self):
        """Test a conforming class satisfies the protocol.

        Given:
            A class implementing the dispatch method
        When:
            Checked against the DispatchingLoadBalancerLike protocol
        Then:
            It should satisfy the protocol
        """

        # Arrange
        class Conforming:
            async def dispatch(self, task, *, context, timeout=None):
                pass

        # Act & assert
        assert isinstance(Conforming(), DispatchingLoadBalancerLike)

    def test_isinstance_with_non_conforming_class(self):
        """Test a non-conforming class does not satisfy the protocol.

        Given:
            A class missing the dispatch method
        When:
            Checked against the DispatchingLoadBalancerLike protocol
        Then:
            It should not satisfy the protocol
        """

        # Arrange
        class NonConforming:
            pass

        # Act & assert
        assert not isinstance(NonConforming(), DispatchingLoadBalancerLike)


class TestLoadBalancerLike:
    def test_isinstance_with_delegating_implementation(self):
        """Test a delegating load balancer satisfies the protocol.

        Given:
            A class implementing the delegate method
        When:
            Checked against the LoadBalancerLike protocol
        Then:
            It should satisfy the protocol
        """

        # Arrange
        class Delegating:
            async def delegate(self, task, *, context):
                if False:
                    yield  # pragma: no cover

        # Act & assert
        assert isinstance(Delegating(), LoadBalancerLike)

    def test_isinstance_with_dispatching_only_implementation(self):
        """Test a dispatch-only load balancer does not satisfy the protocol.

        Given:
            A class implementing only dispatch, with no delegate
            method (a DispatchingLoadBalancerLike)
        When:
            Checked against the LoadBalancerLike protocol
        Then:
            It should not satisfy the protocol — a dispatch-only
            balancer is a DispatchingLoadBalancerLike, not a
            LoadBalancerLike.
        """

        # Arrange
        class Dispatching:
            async def dispatch(self, task, *, context, timeout=None):
                pass

        # Act & assert
        assert not isinstance(Dispatching(), LoadBalancerLike)

    def test_isinstance_with_non_conforming_class(self):
        """Test a non-conforming class does not satisfy the protocol.

        Given:
            A class implementing neither delegate nor dispatch
        When:
            Checked against the LoadBalancerLike protocol
        Then:
            It should not satisfy the protocol
        """

        # Arrange
        class NonConforming:
            pass

        # Act & assert
        assert not isinstance(NonConforming(), LoadBalancerLike)

    def test_isinstance_with_dual_implementation(self):
        """Test a class implementing both protocols satisfies both.

        Given:
            A class implementing both delegate and dispatch
        When:
            Checked against both protocols individually
        Then:
            It should satisfy both — this edge case supports
            balancers that want to implement the new protocol while
            keeping the old one available for backwards compatibility.
        """

        # Arrange
        class Dual:
            async def delegate(self, task, *, context):
                if False:
                    yield  # pragma: no cover

            async def dispatch(self, task, *, context, timeout=None):
                pass

        dual = Dual()

        # Act & assert
        assert isinstance(dual, LoadBalancerLike)
        assert isinstance(dual, DispatchingLoadBalancerLike)


class TestLoadBalancerContext:
    def test_isinstance_with_concrete_context(self):
        """Test LoadBalancerContext satisfies the protocol.

        Given:
            A concrete LoadBalancerContext instance
        When:
            Checked against the LoadBalancerContextLike protocol
        Then:
            It should satisfy the protocol
        """
        # Act & assert
        assert isinstance(LoadBalancerContext(), LoadBalancerContextLike)

    @given(loadbalancer_context())
    @settings(max_examples=50)
    def test_workers_should_reject_mutation(self, context: LoadBalancerContext):
        """Test that the workers mapping cannot be mutated by a caller.

        Given:
            A load balancer context with one or more workers
            registered
        When:
            A caller attempts to assign through the workers mapping
        Then:
            It should raise TypeError — the pool is mutated only
            through the context's own methods
        """
        # Arrange
        uid = next(iter(context.workers))

        # Act & assert
        with pytest.raises(TypeError):
            context.workers[uid] = None  # type: ignore[index]

    @given(loadbalancer_context(min_workers=2))
    @settings(max_examples=50)
    def test_workers_should_reflect_mutations_when_view_captured_earlier(
        self, context: LoadBalancerContext
    ):
        """Test that a captured workers view stays live across mutations.

        Given:
            A load balancer context holding two or more workers, whose
            workers view was captured before any mutation
        When:
            A worker is admitted, a second refreshed, and a third
            evicted
        Then:
            It should reflect all three mutations through the
            previously captured view
        """
        # Arrange
        view = context.workers
        (stale, _), (evicted, _) = list(context.workers.values())[:2]
        refreshed = _mutate(stale, ["address", "pid"])
        refreshed_connection = WorkerConnection(refreshed.address)
        admitted = WorkerMetadata(
            uid=uuid4(),
            address="localhost:61000",
            pid=9999,
            version="1.0.0",
        )
        admitted_connection = WorkerConnection(admitted.address)

        # Act
        context.add_worker(admitted, admitted_connection)
        context.update_worker(refreshed, refreshed_connection)
        context.remove_worker(evicted)

        # Assert
        assert view[admitted.uid] == (admitted, admitted_connection)
        assert view[refreshed.uid] == (refreshed, refreshed_connection)
        assert evicted.uid not in view

    @given(loadbalancer_context())
    @settings(max_examples=50)
    def test_add_worker_should_admit_worker_when_uid_absent(
        self, context: LoadBalancerContext
    ):
        """Test that a worker can be admitted to the context.

        Given:
            A load balancer context with one or more workers
            registered
        When:
            A worker with an unseen uid is admitted
        Then:
            It should be present under its uid, mapped to its record
            and connection, leaving the existing workers unchanged
        """
        # Arrange
        original = dict(context.workers)
        worker = WorkerMetadata(
            uid=uuid4(),
            address="localhost:61000",
            pid=9999,
            version="1.0.0",
        )
        connection = WorkerConnection(worker.address)

        # Act
        context.add_worker(worker, connection)

        # Assert
        assert context.workers[worker.uid] == (worker, connection)
        assert len(context.workers) == len(original) + 1
        for uid, entry in original.items():
            assert context.workers[uid] == entry

    @given(loadbalancer_context())
    @settings(max_examples=50)
    def test_add_worker_should_replace_entry_when_uid_present(
        self, context: LoadBalancerContext
    ):
        """Test that re-admitting a uid replaces its entry.

        Given:
            A load balancer context with one or more workers
            registered
        When:
            A worker is re-admitted under the same uid with changed
            metadata and a new connection
        Then:
            It should replace that worker's entry without changing the
            pool size or disturbing the other workers
        """
        # Arrange
        original = dict(context.workers)
        stale, _ = next(iter(context.workers.values()))
        changed = _mutate(stale, ["address", "pid"])
        connection = WorkerConnection(changed.address)

        # Act
        context.add_worker(changed, connection)

        # Assert
        assert context.workers[stale.uid] == (changed, connection)
        assert len(context.workers) == len(original)
        for uid, entry in original.items():
            if uid != stale.uid:
                assert context.workers[uid] == entry

    @given(loadbalancer_context())
    @settings(max_examples=50)
    def test_add_worker_should_readmit_worker_when_uid_previously_removed(
        self, context: LoadBalancerContext
    ):
        """Test that a previously evicted uid can be re-admitted.

        Given:
            A load balancer context from which one worker was evicted
        When:
            A changed record carrying that uid is admitted again
        Then:
            It should hold exactly one entry for that uid, carrying the
            new record and connection
        """
        # Arrange
        original = dict(context.workers)
        evicted, _ = next(iter(context.workers.values()))
        context.remove_worker(evicted)
        readmitted = _mutate(evicted, ["address", "pid"])
        connection = WorkerConnection(readmitted.address)

        # Act
        context.add_worker(readmitted, connection)

        # Assert
        assert context.workers[evicted.uid] == (readmitted, connection)
        assert len(context.workers) == len(original)

    @given(
        loadbalancer_context(),
        st.sets(st.sampled_from(_MUTABLE_FIELDS), min_size=1),
    )
    @settings(max_examples=50)
    def test_update_worker_should_refresh_entry_when_any_field_subset_changes(
        self, context: LoadBalancerContext, fields: set[str]
    ):
        """Test that updates address a worker by uid across every field.

        Given:
            A load balancer context and a same-uid record mutated in
            any non-empty subset of the non-uid metadata fields
        When:
            The worker is refreshed with the mutated record and a new
            connection
        Then:
            It should carry the mutated record and the new connection
            under the worker's uid, leaving the other workers unchanged
        """
        # Arrange
        original = dict(context.workers)
        stale, _ = next(iter(context.workers.values()))
        changed = _mutate(stale, fields)
        connection = WorkerConnection(changed.address)

        # Act
        context.update_worker(changed, connection)

        # Assert
        assert context.workers[stale.uid] == (changed, connection)
        assert len(context.workers) == len(original)
        for uid, entry in original.items():
            if uid != stale.uid:
                assert context.workers[uid] == entry

    @given(loadbalancer_context(), st.integers(min_value=2, max_value=5))
    @settings(max_examples=50)
    def test_update_worker_should_retain_single_entry_when_refreshed_repeatedly(
        self, context: LoadBalancerContext, refreshes: int
    ):
        """Test that successive refreshes never accumulate entries.

        Given:
            A load balancer context and a chain of 2-5 successive
            pairwise-distinct variants of one worker's record, each
            paired with a fresh connection
        When:
            The worker is refreshed with each variant in order
        Then:
            It should hold exactly one entry for that uid, carrying the
            final variant and the final connection
        """
        # Arrange
        original = dict(context.workers)
        base, _ = next(iter(context.workers.values()))
        host, _, port = base.address.rpartition(":")
        variants = [
            replace(base, pid=base.pid + i, address=f"{host}:{int(port) + i}")
            for i in range(1, refreshes + 1)
        ]
        connections = [WorkerConnection(variant.address) for variant in variants]

        # Act
        for variant, connection in zip(variants, connections):
            context.update_worker(variant, connection)

        # Assert
        assert context.workers[base.uid] == (variants[-1], connections[-1])
        assert len(context.workers) == len(original)

    @given(loadbalancer_context())
    @settings(max_examples=50)
    def test_update_worker_should_regress_entry_when_record_stale(
        self, context: LoadBalancerContext
    ):
        """Test that writes are last-writer-wins, even with a stale record.

        Given:
            A load balancer context whose worker has been refreshed, so
            an earlier record of it is now stale
        When:
            The worker is refreshed again using the stale record
        Then:
            It should regress the entry to the stale record — the
            context does not order writes, so the last writer wins
        """
        # Arrange
        captured, _ = next(iter(context.workers.values()))
        captured_connection = WorkerConnection(captured.address)
        fresher = _mutate(captured, ["address", "pid"])
        context.update_worker(fresher, WorkerConnection(fresher.address))

        # Act
        context.update_worker(captured, captured_connection)

        # Assert
        assert context.workers[captured.uid] == (captured, captured_connection)

    def test_update_worker_should_preserve_order_when_entry_refreshed(self):
        """Test that refreshing a worker does not perturb rotation order.

        Given:
            A load balancer context holding three workers in a known
            order
        When:
            The middle worker is refreshed with a changed record
        Then:
            It should preserve the workers mapping's iteration order —
            the order round-robin rotates through
        """
        # Arrange
        context = LoadBalancerContext()
        workers = [
            WorkerMetadata(
                uid=uuid4(),
                address=f"localhost:{60001 + i}",
                pid=8001 + i,
                version="1.0.0",
            )
            for i in range(3)
        ]
        for worker in workers:
            context.add_worker(worker, WorkerConnection(worker.address))
        refreshed = _mutate(workers[1], ["address", "pid"])

        # Act
        context.update_worker(refreshed, WorkerConnection(refreshed.address))

        # Assert
        assert list(context.workers) == [worker.uid for worker in workers]

    @given(loadbalancer_context())
    @settings(max_examples=50)
    def test_update_worker_should_ignore_record_when_uid_absent(
        self, context: LoadBalancerContext
    ):
        """Test that refreshing an unadmitted worker does nothing.

        Given:
            A load balancer context and a clone of an existing worker's
            fields carrying a fresh uid
        When:
            The clone is passed to update_worker
        Then:
            It should do nothing — refreshing never admits a worker,
            and matching is by uid rather than by field equality
        """
        # Arrange
        original = dict(context.workers)
        existing, _ = next(iter(context.workers.values()))
        clone = replace(existing, uid=uuid4())

        # Act
        context.update_worker(clone, WorkerConnection(clone.address))

        # Assert
        assert clone.uid not in context.workers
        assert dict(context.workers) == original

    @given(loadbalancer_context())
    @settings(max_examples=50)
    def test_update_worker_should_not_resurrect_worker_when_evicted(
        self, context: LoadBalancerContext
    ):
        """Test that eviction fully retires a uid against later refreshes.

        Given:
            A load balancer context from which one worker was evicted
        When:
            A changed record carrying that uid is passed to
            update_worker
        Then:
            It should not resurrect the worker — the uid stays absent
        """
        # Arrange
        original = dict(context.workers)
        evicted, _ = next(iter(context.workers.values()))
        context.remove_worker(evicted)
        changed = _mutate(evicted, ["address", "pid"])

        # Act
        context.update_worker(changed, WorkerConnection(changed.address))

        # Assert
        assert evicted.uid not in context.workers
        assert len(context.workers) == len(original) - 1

    @given(
        loadbalancer_context(),
        st.sets(st.sampled_from(_MUTABLE_FIELDS), min_size=1),
    )
    @settings(max_examples=50)
    def test_remove_worker_should_evict_entry_when_any_field_subset_changes(
        self, context: LoadBalancerContext, fields: set[str]
    ):
        """Test that eviction addresses a worker by uid across every field.

        Given:
            A load balancer context and a same-uid record mutated in
            any non-empty subset of the non-uid metadata fields
        When:
            The worker is evicted using the mutated record
        Then:
            It should evict the worker, leaving the other workers
            unchanged
        """
        # Arrange
        original = dict(context.workers)
        stale, _ = next(iter(context.workers.values()))
        changed = _mutate(stale, fields)

        # Act
        context.remove_worker(changed)

        # Assert
        assert stale.uid not in context.workers
        assert len(context.workers) == len(original) - 1
        for uid, entry in original.items():
            if uid != stale.uid:
                assert context.workers[uid] == entry

    @given(loadbalancer_context())
    @settings(max_examples=50)
    def test_remove_worker_should_ignore_record_when_uid_absent(
        self, context: LoadBalancerContext
    ):
        """Test that eviction matches by uid, never by field equality.

        Given:
            A load balancer context and a clone of an existing worker's
            fields carrying a fresh uid
        When:
            The clone is passed to remove_worker
        Then:
            It should do nothing — the existing worker keeps its entry
        """
        # Arrange
        original = dict(context.workers)
        existing, _ = next(iter(context.workers.values()))
        clone = replace(existing, uid=uuid4())

        # Act
        context.remove_worker(clone)

        # Assert
        assert dict(context.workers) == original

    def test_remove_worker_should_ignore_record_when_context_empty(self):
        """Test that evicting from an empty context does nothing.

        Given:
            A load balancer context with no workers registered
        When:
            A worker is passed to remove_worker
        Then:
            It should do nothing and raise nothing
        """
        # Arrange
        context = LoadBalancerContext()
        absent = WorkerMetadata(
            uid=uuid4(),
            address="localhost:60000",
            pid=9999,
            version="1.0.0",
        )

        # Act
        context.remove_worker(absent)

        # Assert
        assert len(context.workers) == 0

    @given(
        loadbalancer_context(),
        st.lists(
            st.tuples(
                st.sampled_from(("add", "update", "remove")),
                st.integers(min_value=0, max_value=2),
                st.booleans(),
            ),
            max_size=20,
        ),
    )
    @settings(max_examples=50)
    def test_workers_should_mirror_uid_keyed_model_when_operations_interleave(
        self,
        context: LoadBalancerContext,
        operations: list[tuple[str, int, bool]],
    ):
        """Test the workers mapping against a uid-keyed reference model.

        Given:
            A seeded load balancer context and a generated sequence of
            admit, refresh, and evict operations over a pool of three
            uids, each write drawing either a mutated record or an
            equal copy of that uid's last record
        When:
            The operations are applied in order alongside a plain
            uid-keyed dict model with the documented semantics
        Then:
            After every operation the workers mapping should equal the
            model exactly — one entry per live uid, each carrying that
            uid's last-written record and connection
        """
        # Arrange
        model = dict(context.workers)
        bases = [
            WorkerMetadata(
                uid=uuid4(),
                address=f"localhost:{40000 + slot}",
                pid=100 + slot,
                version="1.0.0",
            )
            for slot in range(3)
        ]

        # Act & assert
        for operation, slot, mutate in operations:
            last = model.get(bases[slot].uid)
            base = bases[slot] if last is None else last[0]
            record = replace(base, pid=base.pid + 1) if mutate else replace(base)
            connection = WorkerConnection(record.address)
            if operation == "add":
                context.add_worker(record, connection)
                model[record.uid] = (record, connection)
            elif operation == "update":
                context.update_worker(record, connection)
                if record.uid in model:
                    model[record.uid] = (record, connection)
            else:
                context.remove_worker(record)
                model.pop(record.uid, None)

            assert dict(context.workers) == model
