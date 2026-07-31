import pickle
import threading
import time
from datetime import timedelta

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from wool.utilities.refreshing import Refreshing


def _counter():
    """Return a factory yielding 0, 1, 2, … and the list of its invocations."""
    calls = []

    def factory():
        calls.append(len(calls))
        return len(calls) - 1

    return factory, calls


def _module_factory():
    """Return a constant; a module-level callable so it is picklable."""
    return 7


class TestRefreshing:
    """Test suite for Refreshing."""

    def test___init___should_raise_when_fresh_for_negative(self):
        """Test a negative freshness interval is rejected at construction.

        Given:
            A factory and a negative fresh_for.
        When:
            Refreshing is instantiated.
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="fresh_for"):
            Refreshing(lambda: 1, fresh_for=timedelta(seconds=-1))

    def test___init___should_raise_when_stale_for_negative(self):
        """Test a negative staleness interval is rejected at construction.

        Given:
            A factory and a negative stale_for.
        When:
            Refreshing is instantiated.
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="stale_for"):
            Refreshing(
                lambda: 1, fresh_for=timedelta(0), stale_for=timedelta(seconds=-1)
            )

    def test___init___should_not_call_factory(self):
        """Test construction derives nothing.

        Given:
            A counting factory.
        When:
            Refreshing is instantiated.
        Then:
            It should leave the resource Empty, having called the factory
            zero times.
        """
        # Arrange
        factory, calls = _counter()

        # Act
        resource = Refreshing(factory, fresh_for=timedelta(seconds=60))

        # Assert
        assert calls == []
        assert resource.peek() is None

    def test_peek_should_return_none_when_empty(self):
        """Test peek never derives a value.

        Given:
            An Empty resource.
        When:
            peek() is called.
        Then:
            It should return None without calling the factory.
        """
        # Arrange
        factory, calls = _counter()
        resource = Refreshing(factory, fresh_for=timedelta(seconds=60))

        # Act
        value = resource.peek()

        # Assert
        assert value is None
        assert calls == []

    def test_peek_should_return_value_when_fresh(self):
        """Test peek serves a fresh value without derivation.

        Given:
            A resource whose value was just derived, with a wide freshness
            interval.
        When:
            peek() is called.
        Then:
            It should return the cached value and leave the factory count
            unchanged.
        """
        # Arrange
        factory, calls = _counter()
        resource = Refreshing(factory, fresh_for=timedelta(seconds=60))
        resource.get()

        # Act
        value = resource.peek()

        # Assert
        assert value == 0
        assert calls == [0]

    def test_peek_should_return_none_when_refresh_owed(self):
        """Test peek reports that a factory call is owed.

        Given:
            A resource with a zero freshness interval and one derived value,
            so the value is Stale with no refresh running.
        When:
            peek() is called.
        Then:
            It should return None — the value is not servable without a
            refresh — and still not call the factory.
        """
        # Arrange
        factory, calls = _counter()
        resource = Refreshing(factory, fresh_for=timedelta(0))
        resource.get()

        # Act
        value = resource.peek()

        # Assert
        assert value is None
        assert calls == [0]

    def test_get_should_call_factory_once_when_fresh(self):
        """Test a fresh value is served without re-deriving it.

        Given:
            A resource with a wide freshness interval.
        When:
            get() is called repeatedly.
        Then:
            It should return the same value every time, having called the
            factory exactly once.
        """
        # Arrange
        factory, calls = _counter()
        resource = Refreshing(factory, fresh_for=timedelta(seconds=60))

        # Act
        values = [resource.get() for _ in range(5)]

        # Assert
        assert values == [0] * 5
        assert calls == [0]

    def test_get_should_rederive_when_stale(self):
        """Test the caller that owes a refresh runs it and gets the new value.

        Given:
            A resource with a zero freshness interval, so every value past
            the first is Stale.
        When:
            get() is called twice.
        Then:
            It should derive a new value on the second call and return it.
        """
        # Arrange
        factory, calls = _counter()
        resource = Refreshing(factory, fresh_for=timedelta(0))

        # Act
        first = resource.get()
        second = resource.get()

        # Assert
        assert (first, second) == (0, 1)
        assert calls == [0, 1]

    def test_get_should_serve_previous_when_refresh_in_flight(self):
        """Test concurrent readers are not queued behind a slow refresh.

        Given:
            A resource with one derived value, zero freshness, and another
            thread already inside a factory call gated on an event.
        When:
            Several readers call get() while that refresh is in flight.
        Then:
            It should serve every one of them the previous value and call
            the factory exactly once for the whole stampede.
        """
        # Arrange
        entered = threading.Event()
        release = threading.Event()
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) > 1:
                entered.set()
                assert release.wait(timeout=5.0)
            return len(calls) - 1

        resource = Refreshing(factory, fresh_for=timedelta(0))
        resource.get()
        driver = threading.Thread(target=resource.get)
        driver.start()
        assert entered.wait(timeout=5.0)

        # Act
        values = [resource.get() for _ in range(5)]

        # Assert
        assert values == [0] * 5
        release.set()
        driver.join(timeout=5.0)
        assert calls == [0, 1]

    def test_get_should_share_one_invocation_when_empty_and_concurrent(self):
        """Test a cold stampede collapses to a single factory call.

        Given:
            An Empty resource whose factory blocks until released.
        When:
            Several threads call get() concurrently.
        Then:
            It should call the factory once and hand every thread that same
            value — single-flight, since nothing is servable meanwhile.
        """
        # Arrange
        release = threading.Event()
        calls = []

        def factory():
            calls.append(len(calls))
            assert release.wait(timeout=5.0)
            return "derived"

        resource = Refreshing(factory, fresh_for=timedelta(seconds=60))
        values = []
        threads = [
            threading.Thread(target=lambda: values.append(resource.get()))
            for _ in range(5)
        ]

        # Act
        for thread in threads:
            thread.start()
        release.set()
        for thread in threads:
            thread.join(timeout=5.0)

        # Assert
        assert values == ["derived"] * 5
        assert calls == [0]

    def test_get_should_raise_when_empty_and_factory_raises(self):
        """Test a first derivation's failure reaches the caller.

        Given:
            An Empty resource whose factory raises.
        When:
            get() is called.
        Then:
            It should propagate the exception, since there is no previous
            value to serve instead.
        """
        # Arrange
        resource = Refreshing(
            lambda: (_ for _ in ()).throw(RuntimeError("boom")),
            fresh_for=timedelta(0),
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="boom"):
            resource.get()

    def test_get_should_serve_previous_when_refresh_raises(self):
        """Test a failed refresh is absorbed by the previous value.

        Given:
            A resource with one derived value whose factory then raises.
        When:
            get() is called past the freshness interval.
        Then:
            It should return the previous value and let no exception
            escape — a failed refresh is fail-open.
        """
        # Arrange
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) > 1:
                raise RuntimeError("boom")
            return "first"

        resource = Refreshing(factory, fresh_for=timedelta(0))
        resource.get()

        # Act
        value = resource.get()

        # Assert
        assert value == "first"
        assert calls == [0, 1]

    def test_get_should_retry_immediately_when_refresh_raises(self):
        """Test a failed refresh does not start a freshness interval.

        Given:
            A resource with a wide freshness interval whose second factory
            call raises and whose third succeeds.
        When:
            refresh() fails and get() is called again.
        Then:
            It should consult the factory again rather than serving the
            previous value for the whole interval.
        """
        # Arrange
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) == 2:
                raise RuntimeError("blip")
            return len(calls) - 1

        resource = Refreshing(factory, fresh_for=timedelta(seconds=60))
        resource.get()
        resource.refresh()

        # Act
        value = resource.refresh()

        # Assert
        assert value == 2
        assert calls == [0, 1, 2]

    def test_get_should_wait_when_expired(self):
        """Test material past its staleness bound is no longer served.

        Given:
            A resource with zero freshness and zero staleness, so a derived
            value is immediately Expired, whose factory then raises.
        When:
            get() is called again.
        Then:
            It should raise rather than serve the expired value — the
            fail-open fallback does not apply past stale_for.
        """
        # Arrange
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) > 1:
                raise RuntimeError("boom")
            return "first"

        resource = Refreshing(factory, fresh_for=timedelta(0), stale_for=timedelta(0))
        resource.get()

        # Act & assert
        with pytest.raises(RuntimeError, match="boom"):
            resource.get()

    def test_get_should_serve_indefinitely_when_stale_for_none(self):
        """Test an unbounded staleness window never stops serving.

        Given:
            A resource with zero freshness, no staleness bound, one derived
            value, and a factory that raises from then on.
        When:
            get() is called repeatedly.
        Then:
            It should keep serving the previous value rather than raising.
        """
        # Arrange
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) > 1:
                raise RuntimeError("boom")
            return "first"

        resource = Refreshing(factory, fresh_for=timedelta(0), stale_for=None)
        resource.get()

        # Act
        values = [resource.get() for _ in range(3)]

        # Assert
        assert values == ["first"] * 3

    def test___init___should_raise_when_timeout_not_positive(self):
        """Test a non-positive wait bound is rejected at construction.

        Given:
            A factory and a zero timeout, which would abandon every refresh
            before it could begin.
        When:
            Refreshing is instantiated.
        Then:
            It should raise ValueError.
        """
        # Act & assert
        with pytest.raises(ValueError, match="timeout"):
            Refreshing(lambda: 1, fresh_for=timedelta(0), timeout=timedelta(0))

    def test_get_should_raise_when_joined_refresh_exceeds_timeout(self):
        """Test a reader does not park forever on someone else's hung refresh.

        Given:
            An Empty resource with a short wait bound whose factory hangs,
            already being derived by another thread — so this reader joins a
            flight rather than running one.
        When:
            get() is called.
        Then:
            It should stop waiting and raise TimeoutError, since nothing is
            servable in the meantime.
        """
        # Arrange
        entered = threading.Event()
        release = threading.Event()

        def factory():
            entered.set()
            assert release.wait(timeout=5.0)
            return "eventually"

        resource = Refreshing(
            factory, fresh_for=timedelta(0), timeout=timedelta(milliseconds=50)
        )
        driver = threading.Thread(target=resource.get, daemon=True)
        driver.start()
        assert entered.wait(timeout=5.0)

        # Act & assert
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            resource.get()
        assert time.monotonic() - started < 2.0, "reader waited out the factory"
        release.set()
        driver.join(timeout=5.0)

    def test_get_should_retry_after_abandoning_a_hung_refresh(self):
        """Test an abandoned refresh stops gating later reads.

        Given:
            An Empty resource whose first derivation hangs on another thread
            and is abandoned by a joining reader, and whose next factory call
            returns promptly.
        When:
            get() is called again after the abandonment.
        Then:
            It should start a fresh derivation and return its value, rather
            than joining the abandoned flight forever.
        """
        # Arrange
        entered = threading.Event()
        release = threading.Event()
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) == 1:
                entered.set()
                assert release.wait(timeout=5.0)
            return f"value-{len(calls)}"

        resource = Refreshing(
            factory, fresh_for=timedelta(0), timeout=timedelta(milliseconds=50)
        )
        driver = threading.Thread(target=resource.get, daemon=True)
        driver.start()
        assert entered.wait(timeout=5.0)
        with pytest.raises(TimeoutError):
            resource.get()

        # Act
        value = resource.get()

        # Assert
        assert value == "value-2"
        release.set()
        driver.join(timeout=5.0)

    def test_get_should_serve_previous_when_joined_refresh_times_out(self):
        """Test a servable value absorbs a joined refresh that times out.

        Given:
            A resource holding a value past its staleness bound — so it is
            Expired and readers must wait — being refreshed by another thread
            whose factory hangs, with stale_for wide enough that the previous
            value is still servable.
        When:
            A reader joins that refresh and its wait expires.
        Then:
            It should return the previous value rather than raising, since
            fail-open applies to a timed-out refresh as to a failed one.
        """
        # Arrange
        entered = threading.Event()
        release = threading.Event()
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) > 1:
                entered.set()
                assert release.wait(timeout=5.0)
            return "first"

        resource = Refreshing(
            factory, fresh_for=timedelta(0), timeout=timedelta(milliseconds=50)
        )
        resource.get()
        driver = threading.Thread(target=resource.get, daemon=True)
        driver.start()
        assert entered.wait(timeout=5.0)

        # Act
        value = resource.get()

        # Assert
        assert value == "first"
        release.set()
        driver.join(timeout=5.0)

    @pytest.mark.asyncio
    async def test___await___should_raise_when_joined_refresh_exceeds_timeout(self):
        """Test the awaited form honors the wait bound too.

        Given:
            An Empty resource with a short wait bound whose factory hangs on
            another thread.
        When:
            The resource is awaited, so the loop joins that flight.
        Then:
            It should stop waiting and raise TimeoutError without having
            blocked the event loop.
        """
        # Arrange
        entered = threading.Event()
        release = threading.Event()

        def factory():
            entered.set()
            assert release.wait(timeout=5.0)
            return "eventually"

        resource = Refreshing(
            factory, fresh_for=timedelta(0), timeout=timedelta(milliseconds=50)
        )
        driver = threading.Thread(target=resource.get, daemon=True)
        driver.start()
        assert entered.wait(timeout=5.0)

        # Act & assert
        with pytest.raises(TimeoutError):
            await resource
        release.set()
        driver.join(timeout=5.0)

    def test_refresh_should_rederive_when_fresh(self):
        """Test refresh ignores the freshness interval.

        Given:
            A resource with a wide freshness interval and a derived value.
        When:
            refresh() is called.
        Then:
            It should consult the factory anyway and return the new value.
        """
        # Arrange
        factory, calls = _counter()
        resource = Refreshing(factory, fresh_for=timedelta(seconds=3600))
        resource.get()

        # Act
        value = resource.refresh()

        # Assert
        assert value == 1
        assert calls == [0, 1]

    def test_invalidate_should_rederive_on_next_read(self):
        """Test invalidate returns the resource to Empty.

        Given:
            A resource with a wide freshness interval and a derived value.
        When:
            invalidate() is called and the value read again.
        Then:
            It should consult the factory rather than serving the discarded
            value, and peek() should report nothing servable in between.
        """
        # Arrange
        factory, calls = _counter()
        resource = Refreshing(factory, fresh_for=timedelta(seconds=3600))
        resource.get()

        # Act
        resource.invalidate()
        peeked = resource.peek()
        value = resource.get()

        # Assert
        assert peeked is None
        assert value == 1
        assert calls == [0, 1]

    def test_on_error_should_report_whether_previous_value_served(self):
        """Test the error hook distinguishes absorbed from propagated failures.

        Given:
            A resource whose first factory call raises and whose second
            succeeds before a third raises.
        When:
            The failures are driven through get().
        Then:
            It should report served_stale False for the failure with nothing
            cached and True for the one a previous value absorbed.
        """
        # Arrange
        reported = []
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) in (1, 3):
                raise RuntimeError("boom")
            return "value"

        resource = Refreshing(
            factory,
            fresh_for=timedelta(0),
            on_error=lambda error, served: reported.append(served),
        )

        # Act
        with pytest.raises(RuntimeError):
            resource.get()
        resource.get()
        resource.get()

        # Assert
        assert reported == [False, True]

    def test_on_refresh_should_pair_each_value_with_the_one_it_replaced(self):
        """Test the refresh hook reports the value each commit displaced.

        Given:
            A resource over a factory yielding a new value every call.
        When:
            Several refreshes are driven through get().
        Then:
            It should report each committed value alongside its
            predecessor, with None for the first, so a caller can compare
            successive values without shadowing the resource's own state.
        """
        # Arrange
        reported = []
        factory, _ = _counter()
        resource = Refreshing(
            factory,
            fresh_for=timedelta(0),
            on_refresh=lambda value, previous: reported.append((value, previous)),
        )

        # Act
        resource.get()
        resource.get()
        resource.get()

        # Assert
        assert reported == [(0, None), (1, 0), (2, 1)]

    def test_on_refresh_should_not_report_a_failed_refresh(self):
        """Test the refresh hook fires only for refreshes that commit.

        Given:
            A resource whose second factory call raises while a previous
            value stays servable.
        When:
            The failing refresh is driven through get().
        Then:
            It should report only the two commits, since a failed refresh
            replaces nothing.
        """
        # Arrange
        reported = []
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) == 2:
                raise RuntimeError("boom")
            return f"value-{len(calls)}"

        resource = Refreshing(
            factory,
            fresh_for=timedelta(0),
            on_refresh=lambda value, previous: reported.append((value, previous)),
        )

        # Act
        resource.get()
        resource.get()
        resource.get()

        # Assert
        assert reported == [("value-1", None), ("value-3", "value-1")]

    def test_on_refresh_should_report_in_commit_order_when_a_flight_lands_late(self):
        """Test late-landing refreshes are paired by commit order.

        Given:
            A resource whose first refresh outlives the timeout of the
            caller waiting on it, so the next read starts a replacement
            that commits while the abandoned one is still running.
        When:
            The abandoned invocation finally commits.
        Then:
            It should be paired with the replacement's value rather than
            with what was current when it started — the ordering a caller
            comparing successive values depends on.
        """
        # Arrange
        reported = []
        release = threading.Event()
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) == 1:
                release.wait(5.0)
                return "slow-first"
            return f"fast-{len(calls)}"

        resource = Refreshing(
            factory,
            fresh_for=timedelta(0),
            timeout=timedelta(seconds=0.05),
            on_refresh=lambda value, previous: reported.append((value, previous)),
        )
        joiner = threading.Thread(target=resource.get)
        joiner.start()

        # Act
        with pytest.raises(TimeoutError):
            resource.get()
        # The abandoned flight freed the slot but started nothing; this read
        # is what commits a replacement while the first is still running.
        resource.get()
        release.set()
        joiner.join(5.0)

        # Assert
        assert reported == [("fast-2", None), ("slow-first", "fast-2")]

    @pytest.mark.asyncio
    async def test___await___should_return_value_when_fresh(self):
        """Test awaiting a fresh resource yields its value.

        Given:
            A resource with a wide freshness interval and a derived value.
        When:
            The resource is awaited.
        Then:
            It should yield the cached value without consulting the factory.
        """
        # Arrange
        factory, calls = _counter()
        resource = Refreshing(factory, fresh_for=timedelta(seconds=60))
        resource.get()

        # Act
        value = await resource

        # Assert
        assert value == 0
        assert calls == [0]

    @pytest.mark.asyncio
    async def test___await___should_derive_value_when_empty(self):
        """Test awaiting an Empty resource derives its value off the loop.

        Given:
            An Empty resource whose factory blocks briefly.
        When:
            The resource is awaited.
        Then:
            It should yield the derived value, having run the factory
            somewhere other than the event loop.
        """
        # Arrange
        loop_thread = threading.get_ident()
        ran_on = []

        def factory():
            ran_on.append(threading.get_ident())
            return "derived"

        resource = Refreshing(factory, fresh_for=timedelta(seconds=60))

        # Act
        value = await resource

        # Assert
        assert value == "derived"
        assert ran_on and loop_thread not in ran_on

    @pytest.mark.asyncio
    async def test___await___should_not_block_loop_when_refresh_owed(self):
        """Test a stale await returns immediately and refreshes in background.

        Given:
            A resource with zero freshness, one derived value, and a factory
            whose next call blocks on an event.
        When:
            The resource is awaited while that refresh is owed.
        Then:
            It should yield the previous value without waiting for the
            factory, so the loop is never delayed by it.
        """
        # Arrange
        release = threading.Event()
        calls = []

        def factory():
            calls.append(len(calls))
            if len(calls) > 1:
                assert release.wait(timeout=5.0)
            return len(calls) - 1

        resource = Refreshing(factory, fresh_for=timedelta(0))
        resource.get()

        # Act
        started = time.monotonic()
        value = await resource
        elapsed = time.monotonic() - started

        # Assert
        assert value == 0
        assert elapsed < 1.0, "await waited for the gated refresh"
        release.set()

    @pytest.mark.asyncio
    async def test___await___should_raise_when_empty_and_factory_raises(self):
        """Test a failed first derivation propagates to an awaiting caller.

        Given:
            An Empty resource whose factory raises.
        When:
            The resource is awaited.
        Then:
            It should propagate the exception.
        """
        # Arrange
        resource = Refreshing(
            lambda: (_ for _ in ()).throw(RuntimeError("boom")),
            fresh_for=timedelta(0),
        )

        # Act & assert
        with pytest.raises(RuntimeError, match="boom"):
            await resource

    def test_pickle_roundtrip_should_reset_cache(self):
        """Test an unpickled copy starts Empty.

        Given:
            A resource with a derived value and a picklable factory.
        When:
            It is pickled and unpickled.
        Then:
            The copy should hold no value, since monotonic timestamps are
            meaningless in another process, and should derive one on read.
        """
        # Arrange
        resource = Refreshing(_module_factory, fresh_for=timedelta(seconds=60))
        resource.get()

        # Act
        restored = pickle.loads(pickle.dumps(resource))

        # Assert
        assert restored.peek() is None
        assert restored.get() == 7

    @given(
        fresh=st.floats(min_value=0.0, max_value=10.0),
        stale=st.one_of(st.none(), st.floats(min_value=0.0, max_value=10.0)),
    )
    @settings(max_examples=50)
    def test_get_should_return_factory_output_for_any_interval(self, fresh, stale):
        """Test the first read always yields the factory's value.

        Given:
            Any non-negative freshness interval and any staleness bound.
        When:
            A newly constructed resource is read.
        Then:
            It should return exactly what the factory produced, since an
            Empty resource has nothing else it could serve.
        """
        # Arrange
        resource = Refreshing(
            lambda: "only",
            fresh_for=timedelta(seconds=fresh),
            stale_for=None if stale is None else timedelta(seconds=stale),
        )

        # Act
        value = resource.get()

        # Assert
        assert value == "only"
