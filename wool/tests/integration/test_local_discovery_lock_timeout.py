"""End-to-end tests for LocalDiscovery lock-timeout bounding (#316).

These are targeted standalone tests rather than pairwise scenarios. A
wedged discovery lock is a teardown/failure shape: the publish that
contends it raises `TimeoutError`, aborting pool entry or logging a
failed drop announcement. That would break the pairwise array's single
dispatch-**success** oracle (`test_dispatch_pairwise`), exactly as
capacity exhaustion does (see ``test_local_discovery_capacity.py``). The
uncontended happy path — where the lock is always acquired on the first
attempt and ``lock_timeout`` never fires — is already exercised through
every HYBRID pairwise row with the default ``lock_timeout=30.0``, so a new
`DiscoveryFactory` member would add no observable coverage.

The lock holder runs in a distinct interpreter, so the contention these
tests exercise crosses a process boundary. The `_HOLDER_SCRIPT`
subprocess acquires the lock through the production `_lock` context
manager over the production registry handle, guaranteeing the same file
and mechanism (``portalocker.LOCK_EX | LOCK_NB``) the publisher waits on.
Driving the private `_lock` this way is a deliberate exception to the
Test Guide's "no private references" rule: it is a cross-process harness
to establish a genuinely held lock — no public API holds the discovery
lock open across a wait — while every assertion targets public behavior,
and rebuilding the registry's path in the harness would duplicate more
private detail and drift. Unit-level coverage of the timeout lives in
``tests/runtime/discovery/test_local.py``.

The lock is taken on the registry file itself, so it cannot be held
before the namespace exists. The holder therefore reports that it is
waiting, then polls until an owner has created the registry and takes the
lock the moment it appears; `_locked` waits for that second handshake
wherever the owner is in place before the holder runs.
"""

import asyncio
import logging
import multiprocessing
import time
import uuid

import pytest

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.metadata import WorkerMetadata
from wool.runtime.worker.pool import WorkerPool

from . import routines
from .conftest import _TIMEOUT
from .conftest import _ensure_killed
from .conftest import _iter_leaf_exceptions
from .conftest import _pid_alive
from .conftest import release_subprocess
from .conftest import spawn_script_subprocess

# Hold the namespace's discovery lock in a separate interpreter until
# released via stdin, through the production `_lock` so the held lock is
# the one a publisher contends. Reports "waiting" as soon as it is
# running and "locked" once it holds the lock; see the module docstring.
_HOLDER_SCRIPT = """
import asyncio
import sys
import time

from wool.runtime.discovery.exceptions import DiscoveryNamespaceNotFound
from wool.runtime.discovery.local import _lock
from wool.runtime.discovery.local import _open_registry


async def main():
    namespace = sys.argv[1]
    print("waiting", flush=True)
    deadline = time.monotonic() + 30
    while True:
        try:
            registry = _open_registry(namespace)
            break
        except DiscoveryNamespaceNotFound:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(0.001)
    async with _lock(registry, namespace=namespace, timeout=None):
        print("locked", flush=True)
        await asyncio.to_thread(sys.stdin.readline)


asyncio.run(main())
"""


def _locked(holder, timeout=_TIMEOUT):
    """Wait for a spawned `_HOLDER_SCRIPT` to report that it holds the lock."""
    assert holder.stdout is not None
    line = holder.stdout.readline().strip()
    assert line == "locked", f"holder failed to take the lock: {line!r}"


@pytest.mark.integration
class TestCrossProcessLockTimeout:
    @pytest.mark.asyncio
    async def test_publish_should_raise_timeout_error_when_holder_wedges_lock(self):
        """Test a cross-process lock holder surfaces as a bounded TimeoutError.

        Given:
            An owner holding the namespace's registry, an independent
            subprocess holding that namespace's discovery lock, and a
            Publisher borrowing the registry with a one-second
            lock_timeout
        When:
            The publisher publishes a worker while the holder still holds
            the lock
        Then:
            It should raise TimeoutError after roughly the timeout rather
            than hanging forever, proving real cross-process lock
            arbitration is surfaced as a bounded failure.
        """
        # Arrange
        namespace = f"lock-wedge-{uuid.uuid4().hex[:12]}"
        metadata = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )
        lock_timeout = 1.0
        holder = None

        # Act & assert — an owner holds the registry the publisher
        # borrows, so what the publish contends is the lock alone.
        try:
            publisher = LocalDiscovery.Publisher(namespace, lock_timeout=lock_timeout)
            with LocalDiscovery(namespace):
                holder = spawn_script_subprocess(
                    _HOLDER_SCRIPT, namespace, ready_line="waiting"
                )
                _locked(holder)
                async with publisher:
                    start = time.monotonic()
                    with pytest.raises(TimeoutError):
                        await asyncio.wait_for(
                            publisher.publish("worker-added", metadata),
                            timeout=lock_timeout + 5,
                        )
                    elapsed = time.monotonic() - start

            # Bounded, not instant and not forever: it genuinely waited on
            # the contended lock (≥ half the timeout) and returned well
            # before the wait_for safety net (< timeout + 4, itself under the
            # timeout + 5 net), leaving slack for event-loop starvation on CI.
            assert lock_timeout / 2 <= elapsed < lock_timeout + 4
        finally:
            release_subprocess(holder)


@pytest.mark.integration
class TestPoolTeardownLockTimeout:
    @pytest.mark.asyncio
    async def test___aexit___should_reap_worker_when_drop_announce_lock_wedged(
        self, caplog
    ):
        """Test a wedged drop announcement still reaps under an unbounded exit.

        Given:
            A hybrid WorkerPool with a one-second discovery lock_timeout and
            shutdown_timeout=None, so teardown runs against no deadline
            and nothing is cancelled, that has entered and dispatched a
            routine, then an independent subprocess that wedges the
            discovery lock
        When:
            The pool exits so its worker-dropped announcement contends the
            wedged lock
        Then:
            It should bound teardown by the lock timeout, log that it
            could not announce the worker with a TimeoutError and without
            logging a stop timeout, and still reap the worker process.
        """
        # Arrange
        namespace = f"lock-teardown-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}
        holder = None
        spawned = []

        # Act
        try:
            with caplog.at_level(logging.ERROR, logger="wool.runtime.worker.pool"):
                async with asyncio.timeout(_TIMEOUT):
                    async with WorkerPool(
                        spawn=1,
                        discovery=LocalDiscovery(namespace, lock_timeout=1.0),
                        shutdown_timeout=None,
                    ):
                        spawned = [
                            child.pid
                            for child in multiprocessing.active_children()
                            if child.pid not in before
                        ]
                        assert await routines.add(1, 2) == 3
                        # Wedge the lock only after the worker-added
                        # announcement has already succeeded on a free lock.
                        holder = await asyncio.to_thread(
                            spawn_script_subprocess,
                            _HOLDER_SCRIPT,
                            namespace,
                            ready_line="waiting",
                        )
                        await asyncio.to_thread(_locked, holder)
                    # Exiting here fires the worker-dropped announcement
                    # against the wedged lock.

            # Assert — join finished children so an exited-but-unreaped
            # worker cannot masquerade as alive under os.kill(pid, 0).
            multiprocessing.active_children()
            assert spawned
            for pid in spawned:
                assert not _pid_alive(pid)
            # Vacuity guard — the drop failed against the wedged lock, so
            # the reap is not satisfied by a drop that succeeded.
            announce_failures = [
                record
                for record in caplog.records
                if "could not announce" in record.getMessage()
            ]
            assert announce_failures
            for record in announce_failures:
                assert record.exc_info is not None
                assert isinstance(record.exc_info[1], TimeoutError)
            # The publish TimeoutError is logged as an announcement failure
            # and never as a worker that would not stop.
            assert not any(
                "stopped waiting" in record.getMessage() for record in caplog.records
            )
        finally:
            release_subprocess(holder)
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)


@pytest.mark.integration
class TestPoolEntryLockTimeout:
    @pytest.mark.asyncio
    async def test___aenter___should_abort_when_worker_announce_lock_wedged(self):
        """Test pool entry aborts when the worker-added lock is wedged.

        Given:
            An independent subprocess waiting to take the discovery lock,
            and a hybrid WorkerPool with a one-second lock_timeout whose
            own entry creates the namespace the subprocess is waiting on
        When:
            The pool is entered, so the subprocess takes the lock as soon
            as the registry exists and the pool's worker-added
            announcement contends it
        Then:
            It should abort entry with an ExceptionGroup carrying a
            TimeoutError and leave no worker process alive.
        """
        # Arrange — the holder cannot take the lock before the pool
        # creates the registry, so it is already running and polling when
        # that happens; it wins by the length of a worker spawn. A lost
        # race surfaces as a failure below, never as a silent pass.
        namespace = f"lock-entry-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}
        holder = spawn_script_subprocess(_HOLDER_SCRIPT, namespace, ready_line="waiting")

        # Act & assert
        try:
            with pytest.raises(ExceptionGroup) as excinfo:
                async with asyncio.timeout(_TIMEOUT):
                    async with WorkerPool(
                        spawn=1,
                        discovery=LocalDiscovery(namespace, lock_timeout=1.0),
                    ):
                        pass

            leaves = list(_iter_leaf_exceptions(excinfo.value))
            assert any(isinstance(leaf, TimeoutError) for leaf in leaves), (
                f"expected a TimeoutError, got: {leaves!r}"
            )

            # Join finished children so an exited-but-unreaped worker cannot
            # masquerade as alive.
            multiprocessing.active_children()
            leaked = [
                child.pid
                for child in multiprocessing.active_children()
                if child.pid not in before
            ]
            assert leaked == []
        finally:
            release_subprocess(holder)
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)
