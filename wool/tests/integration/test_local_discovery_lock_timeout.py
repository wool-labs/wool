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

Genuine cross-process contention also requires a distinct interpreter: on
POSIX, `fcntl`/`portalocker` advisory locks are per-process, so a second
acquisition inside this process would not conflict. The `_HOLDER_SCRIPT`
subprocess acquires the lock through the production `_lock` context manager
itself, guaranteeing the same lock-file path (`_short_hash`) and mechanism
(``portalocker.LOCK_EX | LOCK_NB``) the publisher waits on. Driving the
private `_lock` this way is a deliberate exception to Test Guide §2's
"no private references" rule: it is a cross-process harness to establish a
genuinely held lock — no public API holds the discovery lock open across a
wait — while every assertion targets public behavior, and reconstructing
the lock-file path in the harness would duplicate more private detail and
drift. Unit-level coverage of the timeout lives in
``tests/runtime/discovery/test_local.py``.
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
from .conftest import _ensure_killed
from .conftest import _iter_leaf_exceptions
from .conftest import _pid_alive
from .conftest import release_subprocess
from .conftest import spawn_script_subprocess

_TIMEOUT = 30

# Acquire the namespace's discovery lock in a separate interpreter and hold
# it until released via stdin. Uses the production `_lock` so the held lock
# is byte-for-byte the one a publisher contends — a same-process holder
# cannot conflict on POSIX advisory locks.
_HOLDER_SCRIPT = """
import asyncio
import sys

from wool.runtime.discovery.local import _lock


async def main():
    namespace = sys.argv[1]
    async with _lock(namespace, timeout=None):
        print("locked", flush=True)
        sys.stdin.readline()


asyncio.run(main())
"""


@pytest.mark.integration
class TestCrossProcessLockTimeout:
    @pytest.mark.asyncio
    async def test_publish_should_raise_timeout_error_when_holder_wedges_lock(self):
        """Test a cross-process lock holder surfaces as a bounded TimeoutError.

        Given:
            An independent subprocess holding the namespace's discovery
            lock, and a Publisher with a one-second lock_timeout
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
        holder = spawn_script_subprocess(_HOLDER_SCRIPT, namespace, ready_line="locked")

        # Act & assert
        try:
            publisher = LocalDiscovery.Publisher(namespace, lock_timeout=lock_timeout)
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
            shutdown_timeout=None that has entered and dispatched a routine,
            then an independent subprocess that wedges the discovery lock
        When:
            The pool exits so its worker-dropped announcement contends the
            wedged lock
        Then:
            It should bound teardown by the lock timeout instead of hanging
            forever, log that it could not announce the worker, and still
            reap the worker process.
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
                            ready_line="locked",
                        )
                    # Exiting here fires the worker-dropped announcement
                    # against the wedged lock.

            # Assert — join finished children so an exited-but-unreaped
            # worker cannot masquerade as alive under os.kill(pid, 0).
            multiprocessing.active_children()
            assert spawned
            for pid in spawned:
                assert not _pid_alive(pid)
            # Vacuity guard — the drop genuinely failed against the wedged
            # lock, so the reap is not satisfied by a drop that succeeded.
            assert any(
                "could not announce" in record.getMessage() for record in caplog.records
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
            An independent subprocess holding the discovery lock before pool
            entry, and a hybrid WorkerPool with a one-second lock_timeout
        When:
            The pool is entered so its worker-added announcement contends the
            wedged lock
        Then:
            It should abort entry with an ExceptionGroup carrying a
            TimeoutError and leave no worker process alive.
        """
        # Arrange
        namespace = f"lock-entry-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}
        holder = spawn_script_subprocess(_HOLDER_SCRIPT, namespace, ready_line="locked")

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
