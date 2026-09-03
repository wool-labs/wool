import asyncio
import gc
import logging
import threading
import time
import warnings
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies

from wool.runtime.resourcepool import Resource
from wool.runtime.resourcepool import ResourcePool


@strategies.composite
def factory_functions(draw):
    """Generate various factory function types with consistent interfaces."""
    factory_type = draw(
        strategies.sampled_from(
            [
                "sync_simple",
                "async_simple",
                "sync_lambda",
                "async_lambda",
                "callable",
                "awaitable",
            ]
        )
    )

    if factory_type == "sync_simple":

        def sync_factory(key):
            obj = Mock()
            obj.name = f"sync-{key}"
            obj.created_by = "sync_simple"
            return obj

        return sync_factory

    elif factory_type == "async_simple":

        async def async_factory(key):
            obj = Mock()
            obj.name = f"async-{key}"
            obj.created_by = "async_simple"
            return obj

        return async_factory

    elif factory_type == "sync_lambda":

        def sync_lambda_factory(key):
            return SimpleNamespace(name=f"lambda-{key}", created_by="sync_lambda")

        return lambda key: sync_lambda_factory(key)

    elif factory_type == "async_lambda":

        async def async_lambda_factory(key):
            return SimpleNamespace(name=f"async-lambda-{key}", created_by="async_lambda")

        return lambda key: async_lambda_factory(key)

    elif factory_type == "callable":

        class CallableLike:
            def __call__(self, key):
                return self.sync_factory(key)

            def sync_factory(self, key):
                obj = Mock()
                obj.name = f"callable-{key}"
                obj.created_by = "callable"
                return obj

        return CallableLike()

    elif factory_type == "awaitable":

        class AwaitableLike:
            def __init__(self, key) -> None:
                self.key = key

            def __await__(self):
                return self.async_factory().__await__()

            async def async_factory(self):
                obj = Mock()
                obj.name = f"awaitable-{self.key}"
                obj.created_by = "awaitable"
                return obj

        return AwaitableLike


@strategies.composite
def finalizer_functions(draw):
    """Generate various finalizer function types."""
    finalizer_type = draw(
        strategies.sampled_from(
            [
                None,
                "sync_simple",
                "async_simple",
                "sync_lambda",
                "async_lambda",
            ]
        )
    )

    if finalizer_type is None:
        return None

    elif finalizer_type == "sync_simple":

        def simple_sync_finalizer(obj):
            assert obj is not None

        return simple_sync_finalizer

    elif finalizer_type == "async_simple":

        async def simple_async_finalizer(obj):
            assert obj is not None

        return simple_async_finalizer

    elif finalizer_type == "sync_lambda":

        def sync_lambda_finalizer(obj):
            assert obj is not None

        return lambda obj: sync_lambda_finalizer(obj)

    elif finalizer_type == "async_lambda":

        async def async_lambda_finalizer(obj):
            assert obj is not None

        return lambda obj: async_lambda_finalizer(obj)


@pytest.fixture
def mock_resource_factory():
    """Create a mock factory with consistent behavior."""
    factory = Mock()
    factory.return_value = Mock(name="test-resource")
    return factory


@pytest.fixture
def mock_finalizer():
    """Create a mock finalizer that tracks calls."""
    return AsyncMock()


@pytest.fixture
def resource_pool_immediate_cleanup(mock_resource_factory, mock_finalizer):
    """Create a resource pool with TTL=0 for immediate cleanup testing."""
    return ResourcePool(factory=mock_resource_factory, finalizer=mock_finalizer, ttl=0)


@pytest.fixture
def retired_entry_pool(mocker):
    """Build a long-TTL pool holding one entry retired while referenced.

    Returns the pool, its finalizer mock and its factory mock. The
    factory yields ``"first"`` then ``"second"``, so a test can prove
    eviction by acquiring again and getting the second object.
    """
    factory = mocker.Mock(side_effect=["first", "second"])
    finalizer = mocker.AsyncMock()
    pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)
    return pool, finalizer, factory


@pytest.fixture
def expiry_race_pool(mocker):
    """Build a short-TTL pool whose lock can be parked via a blocker key.

    Returns the pool, its finalizer mock, the list of factory calls,
    and the event that releases the parked ``blocker`` acquire.
    """
    release_blocker = asyncio.Event()
    factory_calls = []

    async def factory(key):
        factory_calls.append(key)
        if key == "blocker":
            await release_blocker.wait()
        return f"obj-{key}"

    finalizer = mocker.AsyncMock()
    pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=0.05)
    return pool, finalizer, factory_calls, release_blocker


async def _queue_behind_fired_cleanup(pool, factory_calls, queued_coroutine):
    """Race a fired TTL cleanup against an operation queued on the pool lock.

    Caches and releases ``expired`` so its TTL timer arms, parks an
    acquire of ``blocker`` inside its factory — the factory runs under
    the pool lock, so the lock stays held — then queues the given
    operation on the (FIFO) lock and waits for the timer to fire so
    its cleanup task queues behind that operation. Returns the blocker
    and queued-operation tasks.
    """
    async with pool.get("expired"):
        pass

    blocker_task = asyncio.create_task(pool.acquire("blocker"))

    async def blocker_parked():
        while "blocker" not in factory_calls:
            await asyncio.sleep(0)

    await asyncio.wait_for(blocker_parked(), timeout=2.0)

    queued_task = asyncio.create_task(queued_coroutine)

    async def cleanup_task_spawned():
        # The armed timer already counts as pending; wait until the
        # pending work is the fired timer's cleanup *task*.
        while not isinstance(pool.pending_cleanup.get("expired"), asyncio.Task):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(cleanup_task_spawned(), timeout=2.0)
    return blocker_task, queued_task


@pytest.fixture
def counting_factory():
    """Create a factory that counts how many times it's called."""

    class CountingFactory:
        def __init__(self):
            self.call_count = 0

        def __call__(self, _key):
            self.call_count += 1
            return f"resource-{self.call_count}"

    return CountingFactory()


class TestResourcePool:
    def test_acquire_should_serialize_when_contended_on_a_later_loop(self):
        """Test a pool outliving one loop still serializes on the next.

        Given:
            A pool bound to one event loop by a contended acquire there,
            and that loop has since closed.
        When:
            Two callers contend the same pool on a fresh loop.
        Then:
            The pool should rebind, so the second caller waits for the
            first rather than raising and a process-global pool -- the
            module-level channel pool being one -- keeps working past the
            loop that first bound it.
        """
        # Arrange
        pool = ResourcePool(lambda key: object())

        async def contend():
            held = asyncio.Event()
            waited = False

            async def second():
                nonlocal waited
                await held.wait()
                async with pool._lock:
                    waited = True

            async with pool._lock:
                task = asyncio.ensure_future(second())
                held.set()
                await asyncio.sleep(0)
            await task
            return waited

        # Act
        first_loop = asyncio.run(contend())
        second_loop = asyncio.run(contend())

        # Assert
        assert first_loop is True
        assert second_loop is True

    @staticmethod
    @strategies.composite
    def setup(draw, *, max_key_count=5):
        """Generate a ResourcePool with varied initial resource states.

        Creates a pool with 0-max_key_count resources using the public API
        to create realistic pool states for property-based testing.

        :param draw:
            The Hypothesis draw function for generating test data.
        :param max_key_count:
            Maximum number of keys to create resources for.
        :returns:
            An async function that when called returns a tuple of
            (ResourcePool, factory, list of resources, list of keys).
        """
        factory = draw(factory_functions())
        finalizer = draw(finalizer_functions())
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=0)
        created_resources = []
        keys = []

        async def setup():
            for i in range(draw(strategies.integers(0, max_key_count))):
                key = f"resource-{i}"
                keys.append(key)

                # Create the initial resource using public API and track it
                async with pool.get(key) as resource:
                    created_resources.append(resource)

                # The resource is now in the pool with TTL=0, so it should be immediately
                # cleaned up. We verify pool behavior through public interface

            return pool, factory, created_resources, keys

        return setup

    @pytest.mark.asyncio
    @settings(max_examples=50, deadline=None)
    @given(setup=setup())
    async def test_get_should_return_resource_instance(self, setup):
        """Test that get returns a Resource instance.

        Given:
            A pool with various initial resource states
        When:
            get() is called with a test key
        Then:
            Should return a Resource instance
        """
        # Arrange
        pool, _, _, _ = await setup()

        # Act
        resource_acquisition = pool.get("test-key")

        # Assert
        assert isinstance(resource_acquisition, Resource)

    @pytest.mark.asyncio
    async def test_release_should_decrement_reference_counts(self):
        """Test releasing resources decrements reference counts properly.

        Given:
            A pool with resources that have active references
        When:
            Resources are released via pool.release()
        Then:
            Should properly decrement ref counts or cleanup and remove resources
        """
        # Arrange - Create pool with TTL to keep resources after context exit
        mock_factory = Mock()
        pool = ResourcePool(factory=mock_factory, ttl=60)

        # Create some test resources
        test_keys = ["key1", "key2", "key3"]
        for i, key in enumerate(test_keys):
            mock_factory.return_value = f"resource-{i}"
            async with pool.get(key):
                pass  # Creates and caches the resource

        # Verify initial state
        assert pool.stats.total_entries == len(test_keys)
        assert pool.stats.referenced_entries == 0  # All released from context

        # Now manually acquire some resources to test release
        await pool.acquire("key1")
        await pool.acquire("key2")

        assert pool.stats.referenced_entries == 2

        # Act & assert
        await pool.release("key1")
        assert pool.stats.referenced_entries == 1

        await pool.release("key2")
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_release_should_not_affect_existing_resources_when_key_nonexistent(
        self, counting_factory
    ):
        """Test releasing a nonexistent key is a silent no-op.

        Given:
            A pool with some existing resources
        When:
            Release is called with a nonexistent key
        Then:
            Should exit without affecting existing resources
        """
        # Arrange
        pool = ResourcePool(factory=counting_factory, ttl=1.0)

        # Create some resources to establish initial state
        keys = ["key1", "key2"]
        for key in keys:
            async with pool.get(key):
                pass  # Just acquire and release to populate cache

        initial_cache_size = pool.stats.total_entries

        # Act & assert
        # Try to release a nonexistent key
        await pool.release("nonexistent")

        # Should not affect existing resources
        assert pool.stats.total_entries == initial_cache_size
        # All keys should have zero references (since they were released)
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_release_should_raise_value_error_when_zero_reference_count(self):
        """Test releasing key with zero ref count raises ValueError.

        Given:
            A pool with a resource that has zero reference count
        When:
            Release is called on that key
        Then:
            Should raise ValueError indicating reference count is already
            zero
        """
        # Arrange
        # Create a new resource with unique key using a pool with TTL > 0
        # so the resource stays in cache after release
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        ttl_pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)

        unique_key = "test-zero-ref-count"
        mock_resource = Mock()
        mock_resource.name = unique_key
        mock_factory.return_value = mock_resource

        # Act & assert
        # Acquire and release once to get ref count to 0 (but stays in cache due to TTL)
        async with ttl_pool.get(unique_key):
            pass

        # Now try to release again - should raise ValueError
        with pytest.raises(
            ValueError,
            match=f"Reference count for key '{unique_key}' is already 0",
        ):
            await ttl_pool.release(unique_key)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "ttl, retire_first",
        [(0, False), (60, True)],
        ids=["zero-ttl", "retired-while-referenced"],
    )
    async def test_finalizer_should_still_evict_entry_when_raising_base_exception(
        self, ttl, retire_first
    ):
        """Test a cancelled finalizer still evicts the cache entry.

        Given:
            A pool that finalizes inline — either because it has no TTL
            or because the entry was retired by ``expire`` while still
            referenced — whose finalizer raises ``CancelledError`` — a
            ``BaseException``, not an ``Exception`` — on its first call,
            modelling cleanup that runs under a cancelled teardown
        When:
            A resource is acquired and released, driving the inline
            cleanup whose finalizer raises
        Then:
            The ``CancelledError`` propagates, but the torn-down entry
            is still evicted, so the next acquire is a cache miss that
            builds a fresh resource via the factory rather than handing
            back the finalized one
        """

        # Arrange
        finalizer_calls = {"count": 0}

        async def finalizer(obj):
            finalizer_calls["count"] += 1
            if finalizer_calls["count"] == 1:
                # First cleanup runs under cancellation.
                raise asyncio.CancelledError()

        factory = Mock(
            side_effect=[
                SimpleNamespace(name="first"),
                SimpleNamespace(name="second"),
            ]
        )
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=ttl)

        # Act
        # Acquire then release: rc -> 0 drives inline cleanup, whose
        # finalizer raises CancelledError out of the release.
        with pytest.raises(asyncio.CancelledError):
            async with pool.get("key"):
                if retire_first:
                    await pool.expire("key")

        # Assert
        # The finalized resource must not survive in the cache.
        assert pool.stats.total_entries == 0
        # The next acquire is therefore a miss that builds a fresh
        # resource, never the torn-down one.
        async with pool.get("key") as resource:
            assert resource.name == "second"
        assert factory.call_count == 2

    def test_acquire_should_rebuild_resource_when_bound_loop_closed(self, mocker):
        """Test acquire rebuilds a resource whose loop has closed.

        Given:
            A pool whose cached entry, released with a pending TTL timer,
            belongs to an event loop that has since closed.
        When:
            The same key is acquired from a fresh event loop.
        Then:
            It should rebind to the fresh loop, drop the orphaned entry
            without running the finalizer, invoke the factory again, and
            leave no pending cleanup.
        """
        # Arrange
        factory = mocker.Mock(return_value="obj")
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)
        closed_loop = asyncio.new_event_loop()

        async def acquire_and_release():
            async with pool.get("key"):
                pass

        closed_loop.run_until_complete(acquire_and_release())
        closed_loop.close()

        # Act
        acquired = asyncio.run(pool.acquire("key"))

        # Assert
        assert acquired == "obj"
        assert factory.call_count == 2
        finalizer.assert_not_awaited()
        assert pool.pending_cleanup == {}
        assert pool.stats.total_entries == 1

    def test_clear_should_skip_orphans_when_bound_loop_closed(self, mocker):
        """Test clear drops, without finalizing, what another loop left.

        Given:
            A pool whose cached entry belongs to an event loop that has
            since closed.
        When:
            The pool is cleared from a fresh event loop.
        Then:
            It should drop the orphaned entry without running its
            finalizer -- the resource cannot be closed from another loop
            -- and finish with an empty cache and no pending cleanup.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(
            factory=mocker.Mock(return_value="obj"), finalizer=finalizer, ttl=60
        )
        closed_loop = asyncio.new_event_loop()

        async def acquire_and_release():
            async with pool.get("key"):
                pass

        closed_loop.run_until_complete(acquire_and_release())
        closed_loop.close()

        # Act
        asyncio.run(pool.clear())

        # Assert
        finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 0
        assert pool.pending_cleanup == {}

    def test_acquire_should_raise_when_bound_to_another_running_loop(self, mocker):
        """Test a pool refuses a second loop while its own is running.

        Given:
            A pool bound to an event loop that is still running on
            another thread.
        When:
            The pool is used from a second event loop.
        Then:
            It should raise RuntimeError naming the other running loop
            rather than rebinding, so one pool never serves two live
            loops.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)
        live_loop = asyncio.new_event_loop()
        thread = threading.Thread(target=live_loop.run_forever, daemon=True)
        thread.start()
        try:
            asyncio.run_coroutine_threadsafe(pool.acquire("key"), live_loop).result(
                timeout=5
            )

            # Act & assert
            with pytest.raises(RuntimeError, match="another running event loop"):
                asyncio.run(pool.acquire("other"))
        finally:
            live_loop.call_soon_threadsafe(live_loop.stop)
            thread.join(timeout=5)
            live_loop.close()

    def test_acquire_should_rebind_when_bound_loop_closed(self, mocker):
        """Test a fresh loop starts from an empty cache.

        Given:
            A pool that cached and released an entry on an event loop
            that has since closed, leaving that entry's TTL timer behind.
        When:
            A different key is acquired from a fresh event loop.
        Then:
            It should hold only the new entry: the old one and its timer
            are gone, and its finalizer never ran.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(
            factory=mocker.Mock(return_value="obj"), finalizer=finalizer, ttl=60
        )
        closed_loop = asyncio.new_event_loop()

        async def acquire_and_release():
            async with pool.get("key"):
                pass

        closed_loop.run_until_complete(acquire_and_release())
        assert "key" in pool.pending_cleanup
        closed_loop.close()

        # Act
        asyncio.run(pool.acquire("other"))

        # Assert
        assert pool.stats.total_entries == 1
        assert pool.pending_cleanup == {}
        finalizer.assert_not_awaited()

    def test_acquire_should_warn_when_dropping_referenced_orphan(self, mocker, caplog):
        """Test a still-referenced orphan is reported as a leak.

        Given:
            A pool whose bound loop closed while an entry was still
            referenced.
        When:
            The pool is used from a fresh event loop.
        Then:
            It should log one WARNING from wool.runtime.resourcepool
            reporting the referenced entry it dropped without finalizing.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)
        closed_loop = asyncio.new_event_loop()
        closed_loop.run_until_complete(pool.acquire("key"))
        closed_loop.close()

        # Act
        with caplog.at_level(logging.WARNING, logger="wool.runtime.resourcepool"):
            asyncio.run(pool.acquire("other"))

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert "1 referenced" in records[0].getMessage()

    def test_acquire_should_not_warn_when_dropping_idle_orphan(self, mocker, caplog):
        """Test idle orphans are dropped silently.

        Given:
            A pool whose bound loop closed with only idle entries cached
            (released, awaiting their TTL).
        When:
            The pool is used from a fresh event loop.
        Then:
            It should drop the idle entries with a DEBUG record and no
            WARNING from wool.runtime.resourcepool, since nothing was in
            use when the loop stopped.
        """
        # Arrange
        pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)
        closed_loop = asyncio.new_event_loop()

        async def acquire_and_release():
            async with pool.get("key"):
                pass

        closed_loop.run_until_complete(acquire_and_release())
        closed_loop.close()

        # Act
        with caplog.at_level(logging.DEBUG, logger="wool.runtime.resourcepool"):
            asyncio.run(pool.acquire("other"))

        # Assert
        records = [r for r in caplog.records if r.name == "wool.runtime.resourcepool"]
        assert [r.levelno for r in records] == [logging.DEBUG]

    def test_acquire_should_rebind_when_bound_loop_stopped_but_not_closed(self, mocker):
        """Test liveness is whether the bound loop runs, not whether it closed.

        Given:
            A pool bound to an event loop that has stopped running but
            has not been closed.
        When:
            The pool is used from a second event loop.
        Then:
            It should rebind rather than raise -- a loop that is not
            running cannot be contending the mutex -- and rebuild the
            resource on the new loop.
        """
        # Arrange
        factory = mocker.Mock(return_value="obj")
        pool = ResourcePool(factory=factory, ttl=60)
        stopped_loop = asyncio.new_event_loop()
        stopped_loop.run_until_complete(pool.acquire("key"))
        try:
            # Act
            acquired = asyncio.run(pool.acquire("key"))
        finally:
            stopped_loop.close()

        # Assert
        assert acquired == "obj"
        assert factory.call_count == 2

    def test_release_should_ignore_stale_timer_from_a_loop_the_pool_left(self, mocker):
        """Test a TTL timer left on an earlier loop cannot touch a later loop's cache.

        Given:
            A pool that released an entry on one event loop, scheduling
            its TTL timer there, then rebound to a second loop that
            acquired the same key and still holds it.
        When:
            The first loop resumes long enough for that stale timer to
            fire.
        Then:
            It should leave the second loop's entry untouched -- still
            cached and still referenced, its finalizer never run --
            rather than finalize it or rebind the pool.
        """
        # Arrange
        finalizer = mocker.AsyncMock()
        pool = ResourcePool(
            factory=mocker.Mock(return_value="obj"), finalizer=finalizer, ttl=0.05
        )
        first_loop = asyncio.new_event_loop()

        async def acquire_and_release():
            async with pool.get("key"):
                pass

        first_loop.run_until_complete(acquire_and_release())
        asyncio.run(pool.acquire("key"))
        try:
            # Act
            first_loop.run_until_complete(asyncio.sleep(0.1))
        finally:
            first_loop.close()

        # Assert
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 1
        finalizer.assert_not_awaited()

    def test_release_should_leave_no_pending_task_when_loop_closes_before_ttl(
        self, mocker
    ):
        """Test release defers cleanup without parking a task on the loop.

        Given:
            A pool with a positive TTL whose resource is released on
            a dedicated event loop.
        When:
            The loop is closed and garbage-collected before the TTL
            elapses.
        Then:
            It should leave no pending task on the loop and emit no
            RuntimeWarning when the deferred cleanup is collected.
        """
        # Arrange
        loop = asyncio.new_event_loop()

        def create_release_and_drop():
            pool = ResourcePool(factory=mocker.Mock(return_value="obj"), ttl=60)

            async def acquire_release():
                async with pool.get("key"):
                    pass

            loop.run_until_complete(acquire_release())

        # Act
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            create_release_and_drop()
            pending = asyncio.all_tasks(loop)
            loop.close()
            gc.collect()

        # Assert
        assert pending == set()
        assert not [w for w in caught if issubclass(w.category, RuntimeWarning)]

    @pytest.mark.asyncio
    async def test_acquire_should_cancel_in_flight_cleanup_when_reacquired_after_expiry(
        self, expiry_race_pool
    ):
        """Test acquire cancels a fired cleanup racing on the pool lock.

        Given:
            A pool whose expired key's TTL timer has fired while the
            pool lock is held by another key's acquire, so the
            spawned cleanup task and a queued re-acquire of the
            expired key both wait on the lock with the re-acquire
            first
        When:
            The lock holder completes and the queued re-acquire runs
        Then:
            It should cancel the in-flight cleanup, return the cached
            object without re-invoking the factory or finalizer, and
            leave the key without pending cleanup
        """
        # Arrange
        pool, finalizer, factory_calls, release_blocker = expiry_race_pool
        blocker_task, reacquire_task = await _queue_behind_fired_cleanup(
            pool, factory_calls, pool.acquire("expired")
        )

        # Act
        release_blocker.set()
        acquired = await reacquire_task
        await blocker_task

        # Assert
        assert acquired == "obj-expired"
        assert factory_calls.count("expired") == 1
        finalizer.assert_not_awaited()
        assert "expired" not in pool.pending_cleanup
        assert pool.stats.total_entries == 2

    @pytest.mark.asyncio
    async def test_expire_should_cancel_in_flight_cleanup_when_expired_after_ttl(
        self, expiry_race_pool
    ):
        """Test expire cancels a fired cleanup racing on the pool lock.

        Given:
            A pool whose expired key's TTL timer has fired while the
            pool lock is held by another key's acquire, so the
            spawned cleanup task and a queued expiry of the expired
            key both wait on the lock with the expiry first
        When:
            The lock holder completes and the queued expiry runs
        Then:
            It should cancel the in-flight cleanup, still run the
            finalizer exactly once, and evict the entry
        """
        # Arrange
        pool, finalizer, factory_calls, release_blocker = expiry_race_pool
        blocker_task, clear_task = await _queue_behind_fired_cleanup(
            pool, factory_calls, pool.expire("expired")
        )

        # Act
        release_blocker.set()
        await clear_task
        await blocker_task

        # Assert
        finalizer.assert_awaited_once_with("obj-expired")
        assert "expired" not in pool.pending_cleanup
        assert pool.stats.total_entries == 1

    @pytest.mark.asyncio
    @settings(max_examples=50, deadline=None)
    @given(
        operations=strategies.lists(
            strategies.tuples(
                strategies.sampled_from(["acquire", "release", "expire"]),
                strategies.sampled_from(["a", "b", "c"]),
            ),
            max_size=30,
        )
    )
    async def test_release_should_maintain_bookkeeping_invariants(self, operations):
        """Test acquire, release and expire keep bookkeeping consistent.

        Given:
            Any interleaved sequence of acquire, release and expire
            operations over a small key domain, where releases are
            applied only while a reference is held
        When:
            The sequence is applied step by step to a long-TTL pool
        Then:
            It should evict a retired key the instant the release that
            drops its last reference returns, keeping total entries,
            referenced entries, pending cleanup and the finalized
            objects equal to the model's at every step
        """
        # Arrange
        finalized = []
        pool = ResourcePool(
            factory=lambda key: f"obj-{key}",
            finalizer=finalized.append,
            ttl=60,
        )
        model_refcount = {}
        model_doomed = set()
        model_finalized = []

        # Act & assert
        for operation, key in operations:
            if operation == "acquire":
                await pool.acquire(key)
                model_refcount[key] = model_refcount.get(key, 0) + 1
                model_doomed.discard(key)
            elif operation == "expire":
                if key in model_refcount:
                    if model_refcount[key] > 0:
                        model_doomed.add(key)
                    else:
                        del model_refcount[key]
                        model_finalized.append(f"obj-{key}")
                await pool.expire(key)
            elif model_refcount.get(key, 0) > 0:
                await pool.release(key)
                model_refcount[key] -= 1
                if model_refcount[key] == 0 and key in model_doomed:
                    model_doomed.discard(key)
                    del model_refcount[key]
                    model_finalized.append(f"obj-{key}")

            stats = pool.stats
            assert stats.total_entries == len(model_refcount)
            assert stats.referenced_entries == sum(
                1 for count in model_refcount.values() if count > 0
            )
            assert set(pool.pending_cleanup) == {
                key for key, count in model_refcount.items() if count == 0
            }
            assert finalized == model_finalized

    @pytest.mark.asyncio
    async def test_clear_should_finalize_all_resources(self):
        """Test clearing the pool calls finalizer on all resources.

        Given:
            A pool with resources
        When:
            Clear is called without specific key
        Then:
            All resources should be finalized and cache cleared
        """
        # Arrange - Create pool with TTL to keep resources after context exit
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)

        # Create some resources
        test_resources = []
        for i in range(3):
            mock_resource = Mock(name=f"resource-{i}")
            test_resources.append(mock_resource)
            mock_factory.return_value = mock_resource
            async with pool.get(f"key-{i}"):
                pass  # Creates and caches the resource

        # Verify initial state
        assert pool.stats.total_entries == 3

        # Act
        await pool.clear()

        # Assert
        # All resources should be cleaned up and cache cleared
        assert pool.stats.total_entries == 0

        # Finalizer should have been called for all resources
        assert mock_finalizer.call_count == 3

    @pytest.mark.asyncio
    async def test_expire_should_leave_other_entries_when_key_expired(self):
        """Test expiring one key retires only that key.

        Given:
            A pool holding two unreferenced entries under a long TTL.
        When:
            One of them is expired.
        Then:
            It should finalize that entry alone, leaving the other cached
            and its resource untouched.
        """
        # Arrange
        finalized = []

        async def factory(key):
            return f"obj-{key}"

        async def finalizer(resource):
            finalized.append(resource)

        pool = ResourcePool(factory, finalizer=finalizer, ttl=3600)
        async with pool:
            await pool.acquire("key1")
            await pool.acquire("key2")
            await pool.release("key1")
            await pool.release("key2")
            assert pool.stats.total_entries == 2

            # Act
            await pool.expire("key1")

            # Assert
            assert finalized == ["obj-key1"]
            assert pool.stats.total_entries == 1
            assert "key2" in pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_should_finalize_immediately_when_unreferenced(self):
        """Test expiring an unreferenced entry skips its remaining TTL.

        Given:
            A long-TTL pool holding an unreferenced entry whose TTL timer is
            pending.
        When:
            expire() is called with that entry's key.
        Then:
            It should run the finalizer immediately, remove the entry, and
            leave no pending cleanup — no TTL wait.
        """
        # Arrange
        mock_factory = Mock(return_value="resource")
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)
        async with pool.get("key"):
            pass  # Released: the TTL timer is now armed.
        assert "key" in pool.pending_cleanup

        # Act
        await pool.expire("key")

        # Assert
        mock_finalizer.assert_awaited_once_with("resource")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_expire_should_not_finalize_while_referenced(self):
        """Test expiring a referenced entry leaves the in-flight user alone.

        Given:
            A long-TTL pool holding an entry with an active reference.
        When:
            expire() is called.
        Then:
            It should leave the resource unfinalized and the entry cached —
            an in-flight user is never torn out from under.
        """
        # Arrange
        mock_factory = Mock(return_value="resource")
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)
        await pool.acquire("key")

        # Act
        await pool.expire("key")

        # Assert
        mock_finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 1

    @pytest.mark.asyncio
    async def test_release_should_finalize_retired_entry_when_last_reference_released(
        self, retired_entry_pool
    ):
        """Test a retired entry is finalized as soon as its users drain.

        Given:
            A long-TTL pool holding an entry that has been retired by
            ``expire`` while still referenced.
        When:
            The last reference is released.
        Then:
            It should have awaited the finalizer before the release
            returns, without waiting out the TTL and without leaving
            pending cleanup behind for the loop to drain.
        """
        # Arrange
        pool, finalizer, _ = retired_entry_pool
        await pool.acquire("key")
        await pool.expire("key")

        # Act
        await pool.release("key")

        # Assert
        finalizer.assert_awaited_once_with("first")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_release_should_not_finalize_retired_entry_when_references_remain(
        self, retired_entry_pool
    ):
        """Test a retired entry survives a release that does not drain it.

        Given:
            A long-TTL pool holding an entry with two live references
            that has been retired by ``expire`` while referenced.
        When:
            Only one of the two references is released.
        Then:
            It should leave the resource unfinalized and the entry
            cached and still referenced, with no cleanup pending.
        """
        # Arrange
        pool, finalizer, _ = retired_entry_pool
        await pool.acquire("key")
        await pool.acquire("key")
        await pool.expire("key")

        # Act
        await pool.release("key")

        # Assert
        finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 1
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_release_should_evict_retired_entry_when_cancelled_mid_finalizer(
        self, mocker
    ):
        """Test cancelling a release mid-finalize still evicts the entry.

        Given:
            A long-TTL pool holding an entry retired by ``expire`` while
            still referenced, whose finalizer parks on an event so the
            release is suspended inside it.
        When:
            The releasing task is cancelled while the finalizer is
            parked.
        Then:
            It should raise ``CancelledError`` and still evict the entry,
            so no torn-down resource is handed back to a later acquire.
        """
        # Arrange
        parked = asyncio.Event()
        factory = mocker.Mock(side_effect=["first", "second"])

        async def finalizer(_):
            parked.set()
            await asyncio.Event().wait()

        pool = ResourcePool(factory=factory, finalizer=finalizer, ttl=60)
        await pool.acquire("key")
        await pool.expire("key")
        release = asyncio.ensure_future(pool.release("key"))
        # Bounded: a regression that never enters the finalizer must
        # fail here rather than idle out the pool's own TTL.
        await asyncio.wait_for(parked.wait(), timeout=2.0)

        # Act & assert
        release.cancel()
        with pytest.raises(asyncio.CancelledError):
            await release

        assert pool.stats.total_entries == 0
        assert await pool.acquire("key") == "second"

    def test_release_should_finalize_retired_entry_when_loop_ends_immediately(
        self, mocker, caplog
    ):
        """Test a release during shutdown closes the resource before returning.

        Given:
            A long-TTL pool holding an entry retired by ``expire`` while
            still referenced, on a loop that closes as soon as the last
            reference is released.
        When:
            That release is awaited and the loop is closed with no
            further iterations.
        Then:
            It should have run the finalizer to completion before
            returning, leaving no pending task on the loop and no
            destroyed-while-pending report from asyncio.
        """
        # Arrange
        closed = []

        # The suspension point is load-bearing: a finalizer that never
        # awaits would finish inside a single loop step, so this test
        # could not tell an inline finalize from deferred work the loop
        # happens to run before closing.
        async def finalizer(obj):
            await asyncio.sleep(0)
            closed.append(obj)

        pool = ResourcePool(
            factory=mocker.Mock(return_value="obj"), finalizer=finalizer, ttl=60
        )
        loop = asyncio.new_event_loop()

        async def acquire_and_retire():
            await pool.acquire("key")
            await pool.expire("key")

        loop.run_until_complete(acquire_and_retire())

        # Act
        with caplog.at_level(logging.ERROR, logger="asyncio"):
            loop.run_until_complete(pool.release("key"))
            pending = asyncio.all_tasks(loop)
            loop.close()
            gc.collect()

        # Assert
        assert closed == ["obj"]
        assert pending == set()
        assert pool.stats.total_entries == 0
        assert not [
            record
            for record in caplog.records
            if "Task was destroyed" in record.getMessage()
        ]

    @pytest.mark.asyncio
    async def test_expire_should_resurrect_entry_when_reacquired(self):
        """Test re-acquiring an expired entry cancels its doom.

        Given:
            A long-TTL pool holding a referenced entry that has been
            expired.
        When:
            The key is acquired again before the references drain and both
            references are then released.
        Then:
            It should keep the entry cached on the normal TTL schedule — the
            re-acquire resurrects it — with the finalizer never called.
        """
        # Arrange
        mock_factory = Mock(return_value="resource")
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)
        async with pool:
            await pool.acquire("key")
            await pool.expire("key")

            # Act
            await pool.acquire("key")  # Resurrection: clears the mark.
            await pool.release("key")
            await pool.release("key")

            # Assert
            mock_finalizer.assert_not_awaited()
            assert pool.stats.total_entries == 1
            assert "key" in pool.pending_cleanup  # Normal TTL schedule.

    @pytest.mark.asyncio
    async def test_expire_should_not_raise_when_key_unknown(self):
        """Test expiring an uncached key is a silent no-op.

        Given:
            A pool that has never cached the given key.
        When:
            expire() is called with that key.
        Then:
            It should neither raise nor invoke the finalizer.
        """
        # Arrange
        mock_factory = Mock(return_value="resource")
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=60)

        # Act
        await pool.expire("missing")

        # Assert
        mock_finalizer.assert_not_awaited()
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_ttl_cleanup_should_schedule_resource_removal(self):
        """Test TTL-based cleanup schedules and executes properly.

        Given:
            A pool with TTL > 0
        When:
            A resource reference count reaches 0
        Then:
            Should schedule cleanup after TTL expires
        """
        # Arrange
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=0.1)

        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        key = "ttl-test"

        # Act
        # Acquire and immediately release
        async with pool.get(key) as resource:
            assert resource is mock_resource
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 1

        # Resource should still be in cache with cleanup deferred
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0
        assert pool.stats.pending_cleanup == 1
        mock_finalizer.assert_not_called()

        # Assert
        # Wait for cleanup to complete using polling with timeout
        start_time = time.time()
        while (key in pool.pending_cleanup) and (time.time() - start_time < 2.0):
            await asyncio.sleep(0.01)

        # Resource should now be cleaned up
        assert pool.stats.total_entries == 0
        mock_finalizer.assert_called_once_with(mock_resource)

    @pytest.mark.asyncio
    async def test_ttl_cleanup_should_be_cancelled_when_reacquired(self):
        """Test TTL cleanup is cancelled when resource is reacquired.

        Given:
            A pool with TTL > 0 and a scheduled cleanup
        When:
            The resource is reacquired before TTL expires
        Then:
            Cleanup should be cancelled and resource kept
        """
        # Arrange
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=0.1)

        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        key = "ttl-cancel-test"

        # Act
        # Acquire and release to schedule cleanup
        async with pool.get(key):
            pass

        # Should be scheduled for cleanup
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0
        assert pool.stats.pending_cleanup == 1

        # Reacquire the resource while cleanup is still waiting
        async with pool.get(key) as resource:
            # Assert - cleanup should be cancelled and resource reused
            assert resource is mock_resource
            assert pool.stats.referenced_entries == 1

        # After reacquisition and release, verify finalizer wasn't called
        # (which would indicate the original resource was preserved)
        mock_finalizer.assert_not_called()

        # Resource should still exist due to TTL
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_stats_should_return_accurate_counts(self):
        """Test stats method returns accurate cache statistics.

        Given:
            A pool with various resource states
        When:
            Stats property is accessed
        Then:
            Should return accurate counts for entries, references, and pending
            timers or tasks
        """
        # Arrange
        mock_factory = Mock()
        mock_finalizer = AsyncMock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=0.1)

        # Guard: a fresh pool reports zero across all stats.
        stats = pool.stats
        assert stats.total_entries == 0
        assert stats.referenced_entries == 0
        assert stats.pending_cleanup == 0

        # Act
        # Add some resources
        mock_factory.side_effect = [Mock() for _ in range(3)]

        async with pool.get("key1"):  # ref_count = 1 while in context
            async with pool.get("key2"):  # ref_count = 1 while in context
                async with pool.get("key3"):  # will be released immediately
                    # Assert while all resources are active
                    stats = pool.stats
                    assert stats.total_entries == 3
                    assert stats.referenced_entries == 3  # All active
                    assert stats.pending_cleanup == 0  # None scheduled yet

    @pytest.mark.asyncio
    async def test_async_context_manager_should_clear_resources(self):
        """Test ResourcePool as async context manager clears all on exit.

        Given:
            A ResourcePool with resources
        When:
            Used as async context manager and then exited
        Then:
            Should clear all resources on exit
        """
        # Arrange
        mock_factory = Mock()
        mock_finalizer = AsyncMock()

        # Act & assert
        async with ResourcePool(factory=mock_factory, finalizer=mock_finalizer) as pool:
            mock_resource = Mock()
            mock_factory.return_value = mock_resource

            async with pool.get("test-key"):
                assert pool.stats.total_entries == 1

        # After context exit, cache should be cleared
        assert pool.stats.total_entries == 0
        mock_finalizer.assert_called_once_with(mock_resource)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ttl", [0, 0.1, 1, 1.1, 10, 10.1])
    async def test_ttl_should_schedule_cleanup_based_on_value(self, ttl):
        """Test specific TTL values defer or run cleanup accordingly.

        Given:
            A pool with specific TTL value
        When:
            A resource is acquired and released
        Then:
            It should finalize immediately for TTL 0 and defer
            cleanup for positive TTLs
        """
        # Arrange
        mock_factory = Mock(return_value=Mock(name="test-obj"))
        mock_finalizer = Mock()
        pool = ResourcePool(factory=mock_factory, finalizer=mock_finalizer, ttl=ttl)

        # Act
        async with pool.get("test-key"):
            pass

        # Assert
        if ttl == 0:
            mock_finalizer.assert_called_once()
            assert pool.stats.total_entries == 0
            assert pool.stats.pending_cleanup == 0
        else:
            mock_finalizer.assert_not_called()
            assert pool.stats.total_entries == 1
            assert pool.stats.pending_cleanup == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "ttl, retire_first",
        [(0, False), (60, True)],
        ids=["zero-ttl", "retired-while-referenced"],
    )
    async def test_finalizer_should_catch_exception_and_remove_resource(
        self, ttl, retire_first
    ):
        """Test finalizer exceptions are caught and logged.

        Given:
            A pool that finalizes inline — either because it has no TTL
            or because the entry was retired by ``expire`` while still
            referenced — whose finalizer raises an exception
        When:
            Resource cleanup occurs
        Then:
            Exception should be caught and resource still removed
        """
        # Arrange
        mock_factory = Mock()

        async def failing_finalizer(_):
            raise ValueError("Finalizer failed")

        pool = ResourcePool(factory=mock_factory, finalizer=failing_finalizer, ttl=ttl)

        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        key = "test"

        # Act & assert
        # This should not raise despite finalizer failing
        async with pool.get(key):
            if retire_first:
                await pool.expire(key)

        # Resource should still be cleaned up
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_concurrent_should_maintain_consistency_when_acquire_release_same_key(
        self, counting_factory
    ):
        """Test concurrent operations on same key maintain consistency.

        Given:
            A resource pool with TTL
        When:
            Multiple coroutines acquire and release the same key concurrently
        Then:
            Resource pool should maintain consistency and not leak resources
        """
        # Arrange
        pool = ResourcePool(factory=counting_factory, ttl=0.1)

        # Act
        async def acquire_release_worker():
            async with pool.get("shared-key") as resource:
                await asyncio.sleep(0.01)  # Small delay to increase contention
                return resource

        # Run multiple concurrent workers
        tasks = [acquire_release_worker() for _ in range(10)]
        results = await asyncio.gather(*tasks)

        # Assert
        # All workers should get the same resource instance (cached)
        assert len(set(results)) == 1  # All got the same resource
        # Factory should only be called once despite concurrent access
        assert counting_factory.call_count == 1
        # Pool should be consistent after all operations
        assert pool.stats.total_entries <= 1  # 0 or 1 depending on TTL timing

    @pytest.mark.asyncio
    async def test_resource_pool_should_cleanup_immediately_when_zero_ttl(
        self, resource_pool_immediate_cleanup, mock_resource_factory, mock_finalizer
    ):
        """Test TTL=0 performs immediate cleanup as expected.

        Given:
            A resource pool with TTL=0
        When:
            A resource is acquired and released
        Then:
            Should perform immediate cleanup without scheduling
        """
        # Arrange
        pool = resource_pool_immediate_cleanup
        mock_resource = Mock()
        mock_resource_factory.return_value = mock_resource

        # Act
        async with pool.get("test-key") as resource:
            # While in context, resource should exist
            assert resource is mock_resource
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 1

        # Assert
        # After context exit with TTL=0, should be immediately cleaned up
        assert pool.stats.total_entries == 0
        assert pool.stats.referenced_entries == 0
        assert pool.stats.pending_cleanup == 0  # No pending cleanup tasks
        mock_finalizer.assert_awaited_once_with(mock_resource)

    @pytest.mark.asyncio
    async def test_get_should_handle_none_key(self):
        """Test resource pool handles None key appropriately.

        Given:
            A resource pool
        When:
            get() is called with None key
        Then:
            Should handle None key as a valid cache key
        """
        # Arrange
        mock_factory = Mock()
        mock_resource = Mock()
        mock_factory.return_value = mock_resource
        pool = ResourcePool(factory=mock_factory, ttl=0)

        # Act & assert
        # None should be treated as a valid key
        async with pool.get(None) as resource:
            assert resource is mock_resource

        # Resource should be cleaned up after use
        assert pool.stats.total_entries == 0


class TestResource:
    """Test suite for the Resource class."""

    @pytest.mark.asyncio
    async def test_context_manager_should_auto_release(self):
        """Test Resource as async context manager.

        Given:
            A Resource instance from a pool
        When:
            Used as async context manager
        Then:
            Should auto-acquire on enter and auto-release on exit
        """
        # Arrange
        mock_factory = Mock()
        mock_resource = Mock()
        mock_resource.name = "context-resource"
        mock_factory.return_value = mock_resource

        pool = ResourcePool(factory=mock_factory, ttl=0)

        # Act & assert
        # Use Resource as context manager
        async with pool.get("test-key") as resource:
            assert resource is mock_resource
            assert pool.stats.total_entries == 1
            assert pool.stats.referenced_entries == 1

        # Should be automatically cleaned up after context exit
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body_error",
        [None, KeyError("boom")],
        ids=["clean-exit", "body-raises"],
    )
    async def test_context_manager_should_finalize_when_retired_in_body(
        self, retired_entry_pool, body_error
    ):
        """Test exiting the context finalizes a resource retired inside it.

        Given:
            A long-TTL pool whose key is retired by ``expire`` from
            inside an ``async with pool.get(key)`` body while still
            referenced, where the body then either returns or raises.
        When:
            The context manager exits, normally or on the exceptional
            unwind.
        Then:
            It should have awaited the finalizer before the statement
            following the block runs, leaving the entry evicted with no
            cleanup pending and any original exception propagating
            unchanged.
        """
        # Arrange
        pool, finalizer, _ = retired_entry_pool
        guard = pytest.raises(KeyError, match="boom") if body_error else nullcontext()

        # Act & assert
        with guard:
            async with pool.get("key"):
                await pool.expire("key")
                # Guard: still referenced, so nothing is finalized yet.
                finalizer.assert_not_awaited()
                if body_error:
                    raise body_error

        finalizer.assert_awaited_once_with("first")
        assert pool.stats.total_entries == 0
        assert not pool.pending_cleanup

    @pytest.mark.asyncio
    async def test_resource_should_have_no_manual_release_method(self):
        """Test Resource has no manual release method.

        Given:
            A Resource instance
        When:
            Checking for release method
        Then:
            Should not have a release method
        """
        # Arrange
        mock_factory = Mock()
        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        pool = ResourcePool(factory=mock_factory, ttl=0)

        resource_acquisition = pool.get("test-key")

        # Act & assert
        # Manual release method should not exist
        assert not hasattr(resource_acquisition, "release")

    @pytest.mark.asyncio
    async def test_resource_should_stay_cached_when_ttl_set(self):
        """Test Resource lifecycle with TTL keeps resource in cache.

        Given:
            A Resource instance with TTL pool
        When:
            Used as context manager
        Then:
            Should handle lifecycle correctly and resource stays cached due to TTL
        """
        # Arrange
        mock_factory = Mock()
        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        pool = ResourcePool(factory=mock_factory, ttl=60)  # Use TTL to keep resource

        resource_acquisition = pool.get("test-key")

        # Use as context manager
        async with resource_acquisition as resource:
            assert resource is mock_resource
            assert pool.stats.referenced_entries == 1

        # Resource should still exist due to TTL but no longer referenced
        assert pool.stats.total_entries == 1
        assert pool.stats.referenced_entries == 0

    @pytest.mark.asyncio
    async def test_context_manager_should_handle_lifecycle(self):
        """Test using Resource only as context manager.

        Given:
            A Resource instance
        When:
            Used only as context manager
        Then:
            Should handle acquisition and release correctly
        """
        # Arrange
        mock_factory = Mock()
        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        pool = ResourcePool(factory=mock_factory, ttl=0)

        # Act & assert
        # Use only as context manager
        async with pool.get("test-key") as resource:
            assert resource is mock_resource
            assert pool.stats.referenced_entries == 1

        # After context exit, should be cleaned up (TTL=0)
        assert pool.stats.total_entries == 0

    @pytest.mark.asyncio
    async def test_acquire_should_raise_runtime_error_when_acquired_twice(self):
        """Test that re-acquiring the same Resource instance raises error.

        Given:
            A Resource that has been used as context manager once
        When:
            Attempting to use it as context manager again
        Then:
            Should raise RuntimeError
        """
        mock_factory = Mock()
        mock_resource = Mock()
        mock_factory.return_value = mock_resource

        pool = ResourcePool(factory=mock_factory, ttl=0)
        resource_acquisition = pool.get("test-key")

        # First use as context manager
        async with resource_acquisition as resource:
            assert resource is mock_resource

        # Second use as context manager should fail
        with pytest.raises(RuntimeError, match="Cannot re-acquire a resource"):
            async with resource_acquisition:
                pass

    @pytest.mark.asyncio
    async def test_resource_context_should_propagate_acquire_exception(self):
        """Test Resource context manager handles acquire exceptions properly.

        Given:
            A Resource instance from a pool that fails during acquire
        When:
            Entering the context manager
        Then:
            Should propagate the exception and set _acquired to False
        """
        # Arrange
        mock_pool = AsyncMock()
        mock_pool.acquire.side_effect = RuntimeError("Acquire failed")

        resource = Resource(pool=mock_pool, key="test-key")

        # Act & assert
        with pytest.raises(RuntimeError, match="Acquire failed"):
            async with resource:
                pass

        # Verify _acquired was set to False during exception handling
        assert resource._acquired is False

    @pytest.mark.asyncio
    async def test_resource_context_should_raise_runtime_error_when_not_acquired(self):
        """Test Resource release when not acquired raises RuntimeError.

        Given:
            A Resource instance that was never acquired
        When:
            Attempting to exit context without entering properly
        Then:
            Should raise RuntimeError indicating resource was not acquired
        """
        # Arrange
        mock_pool = AsyncMock()
        resource = Resource(pool=mock_pool, key="test-key")

        # Act & assert - manually call __aexit__ without calling __aenter__
        with pytest.raises(
            RuntimeError, match="Cannot release a resource that was not acquired"
        ):
            await resource.__aexit__(None, None, None)

    @pytest.mark.asyncio
    async def test_resource_context_should_raise_runtime_error_when_already_released(
        self,
    ):
        """Test Resource release when already released raises RuntimeError.

        Given:
            A Resource instance that was already released
        When:
            Attempting to exit context again after normal usage
        Then:
            Should raise RuntimeError indicating resource was already released
        """
        # Arrange
        mock_pool = AsyncMock()
        mock_resource = Mock()
        mock_pool.acquire.return_value = mock_resource

        resource = Resource(pool=mock_pool, key="test-key")

        # Use normally once (which sets _released = True)
        async with resource:
            pass

        # Act & assert - manually call __aexit__ again
        with pytest.raises(
            RuntimeError,
            match="Cannot release a resource that has already been released",
        ):
            await resource.__aexit__(None, None, None)
