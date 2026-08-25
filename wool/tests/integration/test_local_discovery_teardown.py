"""End-to-end tests for LocalDiscovery teardown and ownership (#291, #300).

These are targeted standalone tests rather than pairwise scenarios:
issue #291's reproduction shape — rapid same-namespace teardown and
respawn with overlapping pool lifecycles — cannot be expressed through
``build_pool_from_scenario``'s single-yield nested-context contract,
and the cross-process cases need an independent interpreter to claim a
namespace, or to remove the shared segment out from under a live owner.
Since #300 a namespace has exactly one owner, so the pools that overlap
one here borrow it through a subscriber rather than entering it
themselves. Unit-level simulations of the same contracts live in
``tests/runtime/discovery/test_local.py``.
"""

import asyncio
import contextlib
import logging
import multiprocessing
import os
import signal
import subprocess
import sys
import uuid

import pytest

from wool.runtime.discovery.exceptions import DiscoveryNamespaceNotFound
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.worker.pool import WorkerPool

from . import routines
from .conftest import _TIMEOUT

_OWNER_SCRIPT = """
import sys

from wool.runtime.discovery.local import LocalDiscovery

with LocalDiscovery(sys.argv[1]):
    print("ready", flush=True)
    sys.stdin.readline()
print("clean-exit", flush=True)
"""

_ATTACHER_SCRIPT = """
import sys

from multiprocessing.shared_memory import SharedMemory

from wool.runtime.discovery.local import _short_hash

# Remove the owner's segment out from under it. This used to be a side
# effect of entering LocalDiscovery as a non-owner, whose tracked attach
# had this interpreter's resource tracker reclaim the segment at exit;
# that attach was untracked as of #336 and no longer exists at all as of
# #300, so the removal is explicit here. What these tests need is only
# that the segment vanishes externally.
segment = SharedMemory(name=_short_hash(sys.argv[1]), create=False)
segment.close()
segment.unlink()
print("unlinked", flush=True)
"""

_LEAKED_OWNER_SCRIPT = """
import sys
from contextlib import ExitStack

from wool.runtime.discovery.local import LocalDiscovery

stack = ExitStack()
stack.enter_context(LocalDiscovery(sys.argv[1]))
print("ready", flush=True)
sys.stdin.readline()
print("leaking-context", flush=True)
"""


@pytest.mark.integration
class TestSameNamespaceRespawn:
    @pytest.mark.asyncio
    async def test___aexit___should_unwind_cleanly_when_namespace_respawned_rapidly(
        self, retry_grpc_internal
    ):
        """Test rapid same-namespace pool teardown and respawn cycles.

        Given:
            One namespace shared by three successive WorkerPool
            lifecycles, each with a fresh LocalDiscovery instance
        When:
            Each pool is entered, dispatches a routine, and exits
            back-to-back with no delay between cycles
        Then:
            It should return the dispatch result every cycle and
            unwind every teardown cleanly, leaving no live worker
            process after each exit.
        """
        # Arrange
        spawned: list[int] = []

        # Act & assert
        async def body():
            namespace = f"respawn-{uuid.uuid4().hex[:12]}"
            for _ in range(3):
                before = {child.pid for child in multiprocessing.active_children()}
                async with asyncio.timeout(_TIMEOUT):
                    async with WorkerPool(spawn=1, discovery=LocalDiscovery(namespace)):
                        cycle_spawned = [
                            child.pid
                            for child in multiprocessing.active_children()
                            if child.pid not in before
                        ]
                        spawned.extend(cycle_spawned)
                        assert await routines.add(1, 2) == 3
                # Join any finished children so an exited-but-unreaped
                # worker cannot masquerade as alive under os.kill(pid, 0).
                multiprocessing.active_children()
                # Cardinality first: a cycle that spawned nothing would
                # satisfy the loop below vacuously, and later cycles
                # would silently re-check the previous cycle's pids.
                assert len(cycle_spawned) == 1
                for pid in cycle_spawned:
                    assert not _pid_alive(pid)

        try:
            await retry_grpc_internal(body)
        finally:
            for pid in spawned:
                _ensure_killed(pid)


@pytest.mark.integration
class TestOverlappingNamespaceLifecycles:
    @pytest.mark.asyncio
    async def test___aexit___should_unwind_cleanly_when_borrowing_pool_exits_first(
        self, retry_grpc_internal
    ):
        """Test LIFO overlap of an owner pool and a borrowing pool.

        Given:
            An owner pool "a" holding a namespace, with a durable pool
            "b" nested inside it that borrows a's registry through a
            subscriber rather than entering the namespace itself
        When:
            Pool b dispatches to a's worker and exits while pool a
            remains entered
        Then:
            It should keep pool a fully functional after b's exit
            and unwind both teardowns cleanly with no leaked worker.
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}

        # Act & assert — every per-attempt value is minted inside the
        # body, so a retry claims a fresh namespace rather than one the
        # previous attempt may have left held.
        async def body():
            namespace = f"overlap-lifo-{uuid.uuid4().hex[:12]}"
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool("a", spawn=1, discovery=LocalDiscovery(namespace)):
                    owner_worker = await routines.get_pid()
                    assert await routines.add(1, 2) == 3
                    async with WorkerPool(
                        discovery=LocalDiscovery.Subscriber(namespace)
                    ):
                        # Vacuity guard: pin that b dispatched to *the
                        # owner's* worker through the borrowed registry.
                        # Without it the test leans on the default
                        # quorum=1 to prove b discovered anything, and
                        # quorum=None is a supported choice.
                        assert await routines.get_pid() == owner_worker
                        assert await routines.add(2, 3) == 5
                    assert await routines.add(3, 4) == 7

        try:
            await retry_grpc_internal(body)

            # Assert — no worker outlived its pool
            leaked = [
                child.pid
                for child in multiprocessing.active_children()
                if child.pid not in before
            ]
            assert leaked == []
        finally:
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)

    @pytest.mark.asyncio
    async def test___aexit___should_unwind_cleanly_when_owner_pool_exits_first(
        self, retry_grpc_internal
    ):
        """Test the issue's repro: the registry owner exits first.

        Given:
            An owner pool "a" running in a background task and a
            durable pool "b" borrowing a's registry, entered in the
            test task with a completed dispatch
        When:
            Pool a exits first, orphaning b, b dispatches again, b
            exits, and a fresh pool "c" claims the freed namespace
        Then:
            It should resolve b's post-orphaning dispatch to one of two
            defined outcomes without hanging, unwind b's exit cleanly
            despite the registry having been reclaimed under it, and
            serve the respawned pool c.
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}

        # Act & assert — namespace, events and the owner closure are all
        # minted per attempt: a retry that reused an already-set Event
        # would let the owner exit before pool b ever bound.
        async def body():
            namespace = f"overlap-owner-first-{uuid.uuid4().hex[:12]}"
            owner_up = asyncio.Event()
            release_owner = asyncio.Event()
            owner_worker = None

            async def owner():
                nonlocal owner_worker
                async with WorkerPool("a", spawn=1, discovery=LocalDiscovery(namespace)):
                    assert await routines.add(1, 2) == 3
                    owner_worker = await routines.get_pid()
                    owner_up.set()
                    await release_owner.wait()

            async with asyncio.timeout(_TIMEOUT):
                owner_task = asyncio.create_task(owner())
                try:
                    await owner_up.wait()
                    async with WorkerPool(
                        discovery=LocalDiscovery.Subscriber(namespace)
                    ):
                        # Vacuity guard: b really is serving from the
                        # owner's worker through the borrowed registry.
                        assert await routines.get_pid() == owner_worker
                        assert await routines.add(2, 3) == 5

                        release_owner.set()
                        await owner_task

                        # The owner has taken its worker and its registry.
                        # Which of the two outcomes b sees is a genuine
                        # race — the owner announces "worker-dropped"
                        # before unlinking, so whether b observed the drop
                        # decides it. Pinning either alone would be flaky;
                        # what must hold is that b resolves promptly to
                        # one of them rather than hanging.
                        try:
                            async with asyncio.timeout(10):
                                assert await routines.add(3, 4) == 7
                        except NoWorkersAvailable:
                            pass
                    async with WorkerPool(
                        "c", spawn=1, discovery=LocalDiscovery(namespace)
                    ):
                        assert await routines.add(4, 5) == 9
                finally:
                    release_owner.set()
                    if not owner_task.done():
                        owner_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await owner_task

        try:
            await retry_grpc_internal(body)
        finally:
            # Hygiene, not assertion: kill any stragglers so a worker
            # this test failed to reap cannot pollute the session.
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)


@pytest.mark.integration
class TestDiscoveryFailureIsolation:
    @pytest.mark.asyncio
    async def test___aexit___should_reap_workers_when_registry_vanishes(
        self, retry_grpc_internal, caplog
    ):
        """Test a pool reaps its workers despite failed drop announcements.

        Given:
            A pool holding its own namespace with two spawned workers,
            whose registry is then removed by an independent
            interpreter while the pool is still entered
        When:
            The pool exits, so both ``worker-dropped`` announcements
            fail against the vanished registry
        Then:
            It should leave neither worker alive, report exactly two
            announcement failures carrying DiscoveryNamespaceNotFound
            and the dropped worker's uid, and log no stop failure —
            reaping does not depend on discovery health (#298).
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}
        pids: list[int] = []

        # Arrange and act failures use ``pytest.fail`` rather than
        # ``assert`` so a broken setup stays distinguishable from a
        # genuine leak.
        async def body():
            # Reset per attempt: a retry that appended to the previous
            # attempt's pids would fail the cardinality assertion below
            # with a fabricated leak instead of the original error.
            pids.clear()
            caplog.clear()
            namespace = f"vanished-{uuid.uuid4().hex[:12]}"

            async with asyncio.timeout(_TIMEOUT):
                before_pool = {child.pid for child in multiprocessing.active_children()}
                async with WorkerPool(spawn=2, discovery=LocalDiscovery(namespace)):
                    if await routines.add(1, 2) != 3:
                        pytest.fail("pool failed to dispatch before the registry went")
                    pids.extend(
                        child.pid
                        for child in multiprocessing.active_children()
                        if child.pid not in before_pool
                    )
                    # Act — remove the registry out from under the live
                    # pool, from an interpreter that neither owns nor
                    # borrows it. Off-loop so the blocking subprocess
                    # cannot stall the workers' connections.
                    attacher = await asyncio.to_thread(
                        subprocess.run,
                        [sys.executable, "-c", _ATTACHER_SCRIPT, namespace],
                        capture_output=True,
                        text=True,
                        timeout=_TIMEOUT,
                    )
                    if "unlinked" not in attacher.stdout:
                        pytest.fail(
                            f"failed to remove the registry: {attacher.stderr!r}"
                        )

        try:
            with caplog.at_level(logging.ERROR, "wool.runtime.worker.pool"):
                await retry_grpc_internal(body)

            # Assert — join any finished children first so an
            # exited-but-unreaped worker cannot masquerade as alive
            # under os.kill(pid, 0)
            multiprocessing.active_children()
            # Cardinality first: an empty ``pids`` would satisfy the
            # loop below vacuously. Two workers also prove one failed
            # announcement does not strand its sibling.
            assert len(pids) == 2
            for pid in pids:
                assert not _pid_alive(pid)

            # Vacuity guard: the reap above proves nothing unless the
            # drop announcements actually failed, and failed for the
            # reason under test. Asserting the exception type rather
            # than a log substring is what ties this to the registry
            # having vanished rather than to any other discovery fault.
            announce_failures = [
                record
                for record in caplog.records
                if record.levelno == logging.ERROR
                and "could not announce" in record.getMessage()
            ]
            assert len(announce_failures) == 2
            for record in announce_failures:
                assert record.exc_info is not None
                assert isinstance(record.exc_info[1], DiscoveryNamespaceNotFound)
                assert getattr(record, "undropped_worker_uid", None) is not None

            # Assert — the stops themselves succeeded. Reverting #298's
            # fix abandons them, which surfaces here rather than only
            # in process liveness.
            assert not any(
                "could not stop worker" in record.getMessage()
                for record in caplog.records
            )
        finally:
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)
            for pid in pids:
                _ensure_killed(pid)


@pytest.mark.integration
class TestCrossProcessTeardown:
    def test___exit___should_unwind_cleanly_when_tracker_unlinks_segment(self):
        """Test owner teardown after a genuine external unlink.

        Given:
            An owner LocalDiscovery entered in its own interpreter,
            and an independent interpreter that removed the shared
            segment out from under it. (Since #300 the non-owner attach
            this once relied on no longer exists; the resource-tracker
            contract itself is covered in test_local_discovery_tracker.)
        When:
            The owner is released, exits its context, and its
            interpreter shuts down
        Then:
            It should exit with status 0 and no traceback — the
            vanished segment aborts neither the context exit nor the
            atexit fallback at interpreter shutdown.
        """
        # Arrange
        namespace = f"tracker-{uuid.uuid4().hex[:12]}"
        owner = subprocess.Popen(
            [sys.executable, "-c", _OWNER_SCRIPT, namespace],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert owner.stdin is not None and owner.stdout is not None
            assert owner.stdout.readline().strip() == "ready"

            attacher = subprocess.run(
                [sys.executable, "-c", _ATTACHER_SCRIPT, namespace],
                capture_output=True,
                text=True,
                timeout=_TIMEOUT,
            )
            assert attacher.returncode == 0
            # Vacuity guard — the segment really did vanish before the
            # owner exits, so the owner's teardown is exercised against a
            # missing segment rather than passing on a live one.
            assert "unlinked" in attacher.stdout

            # Act — release the owner to exit its context and shut
            # down its interpreter
            owner.stdin.write("\n")
            owner.stdin.flush()
            stdout, stderr = owner.communicate(timeout=_TIMEOUT)

            # Assert
            assert owner.returncode == 0
            assert "clean-exit" in stdout
            assert "Traceback" not in stderr
        finally:
            if owner.poll() is None:
                owner.kill()
                owner.wait(timeout=10)

    def test___enter___should_arm_fallback_that_survives_shutdown_when_leaked(self):
        """Test the shutdown fallback tolerates a vanished segment.

        Given:
            An owner LocalDiscovery entered in its own interpreter and
            never exited, and an independent interpreter that removed
            the shared segment out from under it
        When:
            The owner interpreter shuts down with the fallback still
            armed
        Then:
            It should exit with status 0 and no traceback — the armed
            fallback suppresses the missing segment instead of
            crashing interpreter shutdown.
        """
        # Arrange
        namespace = f"leaked-{uuid.uuid4().hex[:12]}"
        owner = subprocess.Popen(
            [sys.executable, "-c", _LEAKED_OWNER_SCRIPT, namespace],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert owner.stdin is not None and owner.stdout is not None
            assert owner.stdout.readline().strip() == "ready"

            attacher = subprocess.run(
                [sys.executable, "-c", _ATTACHER_SCRIPT, namespace],
                capture_output=True,
                text=True,
                timeout=_TIMEOUT,
            )
            assert attacher.returncode == 0
            # Vacuity guard — the segment really did vanish before the
            # owner shuts down.
            assert "unlinked" in attacher.stdout

            # Act — the owner returns from its script with the
            # context still open, so atexit fires the armed fallback
            # against the vanished segment
            owner.stdin.write("\n")
            owner.stdin.flush()
            stdout, stderr = owner.communicate(timeout=_TIMEOUT)

            # Assert — the owner's own tracker warns about its stale
            # registration on stderr (its unlink raised before the
            # tracker unregistration), so assert traceback absence
            # rather than a clean stderr
            assert owner.returncode == 0
            assert "leaking-context" in stdout
            assert "Traceback" not in stderr
        finally:
            if owner.poll() is None:
                owner.kill()
                owner.wait(timeout=10)


def _pid_alive(pid: int) -> bool:
    """Return whether a process with the given pid currently exists."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _ensure_killed(pid: int | None) -> None:
    """Best-effort SIGKILL so a failing run cannot leak a worker."""
    if pid is not None and _pid_alive(pid):
        with contextlib.suppress(OSError):
            os.kill(pid, signal.SIGKILL)
