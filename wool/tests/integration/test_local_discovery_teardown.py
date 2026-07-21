"""End-to-end tests for LocalDiscovery teardown under namespace reuse (#291).

These are targeted standalone tests rather than pairwise scenarios:
issue #291's reproduction shape — rapid same-namespace teardown and
respawn with overlapping pool lifecycles — cannot be expressed through
``build_pool_from_scenario``'s single-yield nested-context contract,
and the cross-process case needs independent interpreters whose
multiprocessing resource trackers genuinely unlink the shared segment
(CPython bpo-38119). Unit-level simulations of the same contracts live
in ``tests/runtime/discovery/test_local.py``.
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

from wool.runtime.discovery.local import LocalDiscovery
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

from wool.runtime.discovery.local import LocalDiscovery

with LocalDiscovery(sys.argv[1]):
    pass
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
        namespace = f"respawn-{uuid.uuid4().hex[:12]}"
        spawned: list[int] = []

        # Act & assert
        async def body():
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
    async def test___aexit___should_unwind_cleanly_when_non_owner_pool_exits_first(
        self, retry_grpc_internal
    ):
        """Test LIFO overlap of two same-namespace pools.

        Given:
            An owner pool "a" on a namespace with a non-owner pool
            "b" on the same namespace nested inside it, workers
            partitioned by tag
        When:
            Pool b dispatches and exits while pool a remains entered
        Then:
            It should keep pool a fully functional after b's exit
            and unwind both teardowns cleanly with no leaked worker.
        """
        # Arrange
        namespace = f"overlap-lifo-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}

        # Act & assert
        async def body():
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool("a", spawn=1, discovery=LocalDiscovery(namespace)):
                    assert await routines.add(1, 2) == 3
                    async with WorkerPool(
                        "b", spawn=1, discovery=LocalDiscovery(namespace)
                    ):
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
        """Test the issue's repro: the segment owner exits first.

        Given:
            An owner pool "a" running in a background task and a
            non-owner pool "b" on the same namespace entered in the
            test task with a completed dispatch
        When:
            Pool a exits first, then pool b dispatches again and
            exits, and a fresh pool "c" enters the same namespace
        Then:
            It should keep pool b functional after the owner's
            teardown, unwind b's exit cleanly, and serve the
            respawned pool c.
        """
        # Arrange
        namespace = f"overlap-owner-first-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}
        owner_up = asyncio.Event()
        release_owner = asyncio.Event()

        async def owner():
            async with WorkerPool("a", spawn=1, discovery=LocalDiscovery(namespace)):
                assert await routines.add(1, 2) == 3
                owner_up.set()
                await release_owner.wait()

        # Act & assert
        async def body():
            async with asyncio.timeout(_TIMEOUT):
                owner_task = asyncio.create_task(owner())
                try:
                    await owner_up.wait()
                    async with WorkerPool(
                        "b", spawn=1, discovery=LocalDiscovery(namespace)
                    ):
                        assert await routines.add(2, 3) == 5
                        release_owner.set()
                        await owner_task
                        assert await routines.add(3, 4) == 7
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

    @pytest.mark.asyncio
    async def test___aexit___should_reap_worker_when_owner_pool_exited_first(
        self, retry_grpc_internal, caplog
    ):
        """Test a surviving pool reaps its worker despite owner exit.

        Given:
            An owner pool "a" running in a background task and a
            non-owner pool "b" on the same namespace with its worker
            pid captured
        When:
            Pool a exits first and pool b then exits
        Then:
            It should leave no pool-b worker process alive after b's
            exit.
        """
        # Arrange
        namespace = f"overlap-reap-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}
        owner_up = asyncio.Event()
        release_owner = asyncio.Event()
        b_pids: list[int] = []

        # Arrange and act failures use ``pytest.fail`` rather than
        # ``assert`` so a broken setup stays distinguishable from a
        # genuine leak.
        async def owner():
            async with WorkerPool("a", spawn=1, discovery=LocalDiscovery(namespace)):
                if await routines.add(1, 2) != 3:
                    pytest.fail("pool a failed to dispatch before the owner was pinned")
                owner_up.set()
                await release_owner.wait()

        # Act
        async def body():
            async with asyncio.timeout(_TIMEOUT):
                owner_task = asyncio.create_task(owner())
                try:
                    await owner_up.wait()
                    before_b = {child.pid for child in multiprocessing.active_children()}
                    async with WorkerPool(
                        "b", spawn=1, discovery=LocalDiscovery(namespace)
                    ):
                        if await routines.add(2, 3) != 5:
                            pytest.fail("pool b failed to dispatch before owner exit")
                        b_pids.extend(
                            child.pid
                            for child in multiprocessing.active_children()
                            if child.pid not in before_b
                        )
                        release_owner.set()
                        await owner_task
                finally:
                    release_owner.set()
                    if not owner_task.done():
                        owner_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await owner_task

        try:
            with caplog.at_level(logging.ERROR, "wool.runtime.worker.pool"):
                await retry_grpc_internal(body)

            # Assert — join any finished children first so an
            # exited-but-unreaped worker cannot masquerade as alive
            # under os.kill(pid, 0)
            multiprocessing.active_children()
            # Cardinality first: an empty ``b_pids`` would satisfy the
            # loop below vacuously.
            assert len(b_pids) == 1
            for pid in b_pids:
                assert not _pid_alive(pid)

            # Vacuity guard: the reap above proves nothing unless b's
            # drop announcement actually failed. #300 would let that
            # publish succeed, reaping b's worker without exercising
            # #298's fix at all — so pin that the announcement really
            # did fail here. If #300 lands, this fails loudly rather
            # than rotting into a green test that guards nothing.
            assert any(
                "could not announce" in record.getMessage()
                for record in caplog.records
                if record.levelno == logging.ERROR
            )
        finally:
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)
            for pid in b_pids:
                _ensure_killed(pid)


@pytest.mark.integration
class TestCrossProcessTeardown:
    def test___exit___should_unwind_cleanly_when_tracker_unlinks_segment(self):
        """Test owner teardown after a genuine external unlink.

        Given:
            An owner LocalDiscovery entered in its own interpreter,
            and an independent attacher interpreter that entered and
            exited the same namespace, whose multiprocessing resource
            tracker then unlinked the shared segment at exit
            (CPython bpo-38119)
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
            # Vacuity guard — the attacher's tracker really unlinked
            # the segment (warning about the leak) before the owner
            # exits; if CPython ever stops tracking attaches this
            # fails loudly instead of passing without exercising the
            # fix.
            assert "leaked shared_memory" in attacher.stderr

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
            An owner LocalDiscovery entered in its own interpreter
            and never exited, and an independent attacher interpreter
            whose resource tracker unlinked the shared segment at
            exit
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
            # Vacuity guard — the attacher's tracker really unlinked
            # the segment before the owner shuts down.
            assert "leaked shared_memory" in attacher.stderr

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
