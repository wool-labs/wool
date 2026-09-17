"""End-to-end tests for LocalDiscovery teardown and ownership.

These are targeted standalone tests outside the pairwise scenarios.
Rapid same-namespace teardown and respawn with overlapping pool
lifecycles cannot be expressed through ``build_pool_from_scenario``'s
single-yield nested-context contract, and the cross-process cases need
an independent interpreter to claim a namespace, or to remove the
registry out from under a live owner. Unit-level simulations of
the same contracts live in ``tests/runtime/discovery/test_local.py``.
"""

import asyncio
import contextlib
import logging
import multiprocessing
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from tests.helpers import namespace_directory
from wool.runtime.discovery.exceptions import DiscoveryNamespaceInUse
from wool.runtime.discovery.exceptions import DiscoveryNamespaceNotFound
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.worker.pool import WorkerPool

from . import routines
from .conftest import _TIMEOUT
from .conftest import release_subprocess
from .conftest import spawn_script_subprocess

#: Passed to owner interpreters so the workers they spawn can import
#: this suite's routines when unpickling a dispatch. Spawned children
#: inherit the parent's ``sys.path``, so setting it in the owner is
#: enough. Passed explicitly rather than relying on an inherited
#: working directory.
_TESTS_ROOT = Path(__file__).resolve().parents[1]

#: Own a namespace until released via stdin, then exit its context.
_OWNER_SCRIPT = """
import sys

from wool.runtime.discovery.local import LocalDiscovery

with LocalDiscovery(sys.argv[1]):
    print("ready", flush=True)
    sys.stdin.readline()
print("clean-exit", flush=True)
"""

_ATTACHER_SCRIPT = """
import os
import sys

# Remove the owner's registry out from under it. The tests need only
# that it vanishes externally, so the path is passed in rather than
# derived here.
os.unlink(sys.argv[1])
print("unlinked", flush=True)
"""

_XPROC_OWNER_SCRIPT = """
import asyncio
import sys

sys.path.insert(0, sys.argv[2])

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.local import LocalWorker


async def main():
    namespace = sys.argv[1]
    with LocalDiscovery(namespace):
        worker = LocalWorker()
        await worker.start()
        publisher = LocalDiscovery.Publisher(namespace)
        async with publisher:
            await publisher.publish("worker-added", worker.metadata)
            print("ready", flush=True)
            await asyncio.to_thread(sys.stdin.readline)
            await publisher.publish("worker-dropped", worker.metadata)
        await worker.stop()
    print("clean-exit", flush=True)


asyncio.run(main())
"""

_CLAIMANT_SCRIPT = """
import sys
import time

from wool.runtime.discovery.exceptions import DiscoveryNamespaceInUse
from wool.runtime.discovery.local import LocalDiscovery

namespace = sys.argv[1]
if len(sys.argv) > 2:
    # Spin to a shared wall-clock deadline so every claimant attempts the
    # create within the same instant; a sequential probe would pass against
    # a check-then-create implementation, which is the race this test
    # guards against.
    deadline = float(sys.argv[2])
    while time.time() < deadline:
        pass
try:
    with LocalDiscovery(namespace):
        print("claimed", flush=True)
except DiscoveryNamespaceInUse as error:
    print(f"rejected {error}", flush=True)
"""

#: Bind and release a borrowing publisher on a namespace this process
#: does not own, publishing a worker through it.
_BORROWER_SCRIPT = """
import asyncio
import sys
import uuid

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.metadata import WorkerMetadata


async def main():
    namespace = sys.argv[1]
    metadata = WorkerMetadata(
        uid=uuid.uuid4(), address="localhost:50051", pid=1, version="1.0"
    )
    async with LocalDiscovery.Publisher(namespace) as publisher:
        await publisher.publish("worker-added", metadata)
        print("published", flush=True)
        async for event in LocalDiscovery.Subscriber(namespace, poll_interval=0.05):
            print("discovered", flush=True)
            break
        await publisher.publish("worker-dropped", metadata)


asyncio.run(main())
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
                # satisfy the loop below vacuously.
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
            subscriber
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
                        # Pin that b's dispatch reached the owner's
                        # worker through the borrowed registry.
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
        """Test overlapping pools when the registry owner exits first.

        Given:
            An owner pool "a" running in a background task and a
            durable pool "b" borrowing a's registry, entered in the
            test task with a completed dispatch
        When:
            Pool a exits first, orphaning b, b dispatches again, b
            exits, and a fresh pool "c" claims the freed namespace
        Then:
            It should raise NoWorkersAvailable for b's post-orphaning
            dispatch within ten seconds, unwind b's exit cleanly after
            the owner reclaimed the registry, and serve the respawned
            pool c.
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
                        # Vacuity guard: b serves from the owner's
                        # worker through the borrowed registry.
                        assert await routines.get_pid() == owner_worker
                        assert await routines.add(2, 3) == 5

                        release_owner.set()
                        await owner_task

                        # The owner has stopped its worker and reclaimed
                        # its registry, so b has no live worker whether or
                        # not it observed the drop. The timeout turns a
                        # hung dispatch into a failure.
                        with pytest.raises(NoWorkersAvailable):
                            async with asyncio.timeout(10):
                                await routines.add(3, 4)
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
            and the undropped worker's uid, and log no stop failure.
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}
        pids: list[int] = []

        # Arrange and act failures use ``pytest.fail`` rather than
        # ``assert`` so a broken setup stays distinguishable from a
        # genuine leak.
        async def body():
            # Reset per attempt: pids and records left by a failed
            # attempt would break the exact-count assertions below even
            # when the retry succeeds.
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
                        [
                            sys.executable,
                            "-c",
                            _ATTACHER_SCRIPT,
                            str(namespace_directory(namespace) / "registry"),
                        ],
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
class TestCrossProcessOwnership:
    def test___enter___should_raise_when_another_process_owns_the_namespace(self):
        """Test ownership is asserted across process boundaries.

        Given:
            An owner LocalDiscovery holding a namespace in its own
            interpreter
        When:
            An independent interpreter enters the same namespace
        Then:
            It should raise DiscoveryNamespaceInUse and leave the owner
            able to exit cleanly.
        """
        # Arrange
        namespace = f"claim-{uuid.uuid4().hex[:12]}"
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

            # Act
            claimant = subprocess.run(
                [sys.executable, "-c", _CLAIMANT_SCRIPT, namespace],
                capture_output=True,
                text=True,
                timeout=_TIMEOUT,
            )

            # Assert — the claim was refused. Match whole lines so the
            # check is independent of the rejection message's wording.
            assert claimant.returncode == 0
            lines = claimant.stdout.splitlines()
            assert "claimed" not in lines
            assert any(line.startswith("rejected ") for line in lines)
            assert namespace in claimant.stdout

            # Assert — the refusal left the owner's registry intact
            owner.stdin.write("\n")
            owner.stdin.flush()
            stdout, stderr = owner.communicate(timeout=_TIMEOUT)
            assert owner.returncode == 0
            assert "clean-exit" in stdout
            assert "Traceback" not in stderr
        finally:
            if owner.poll() is None:
                owner.kill()
                owner.wait(timeout=10)

    @pytest.mark.parametrize("death", ["clean-exit", "killed"])
    def test___enter___should_admit_a_successor_when_the_owner_dies(self, death):
        """Test a namespace outlives its owner's death, however it dies.

        Given:
            An owner LocalDiscovery holding a namespace in its own
            interpreter, released either to exit cleanly or killed
            outright with SIGKILL so neither its context exit nor its
            atexit fallback runs
        When:
            A fresh interpreter claims the same namespace afterwards
        Then:
            It should admit the successor in both cases; after a kill,
            the claim dies with the process and the successor replaces
            the registry the kill stranded.
        """
        # Arrange
        namespace = f"handoff-{uuid.uuid4().hex[:12]}"
        owner = spawn_script_subprocess(
            _OWNER_SCRIPT, namespace, ready_line="ready", timeout=_TIMEOUT
        )

        try:
            if death == "clean-exit":
                assert owner.stdin is not None
                owner.stdin.write("\n")
                owner.stdin.flush()
                assert owner.wait(timeout=_TIMEOUT) == 0
            else:
                owner.kill()
                owner.wait(timeout=_TIMEOUT)
        finally:
            release_subprocess(owner)

        # Act
        successor = subprocess.run(
            [sys.executable, "-c", _CLAIMANT_SCRIPT, namespace],
            capture_output=True,
            text=True,
            timeout=_TIMEOUT,
        )

        # Assert
        assert successor.returncode == 0, successor.stderr
        assert "claimed" in successor.stdout, (
            f"a {death} owner left its namespace unclaimable: {successor.stdout!r}"
        )

    def test___enter___should_admit_exactly_one_of_several_racing_claimants(self):
        """Test simultaneous claims resolve to a single owner.

        Given:
            Four independent interpreters spin-waiting on one shared
            wall-clock deadline, all naming the same fresh namespace
        When:
            The deadline passes and all four claim the namespace at once
        Then:
            It should admit exactly one and reject the other three with
            DiscoveryNamespaceInUse naming the namespace.
        """
        # Arrange
        namespace = f"race-{uuid.uuid4().hex[:12]}"
        deadline = time.time() + 2.0
        claimants = [
            subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    _CLAIMANT_SCRIPT,
                    namespace,
                    str(deadline),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for _ in range(4)
        ]

        # Act
        try:
            outcomes = [proc.communicate(timeout=_TIMEOUT) for proc in claimants]
        finally:
            for proc in claimants:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=10)

        # Assert
        stdouts = [stdout.strip() for stdout, _ in outcomes]
        assert all(proc.returncode == 0 for proc in claimants), [
            stderr for _, stderr in outcomes
        ]
        assert sum(line == "claimed" for line in stdouts) == 1
        rejections = [line for line in stdouts if line.startswith("rejected ")]
        assert len(rejections) == 3
        # A rejection that named no namespace would be unactionable.
        assert all(repr(namespace) in line for line in rejections)

    def test___enter___should_claim_the_namespace_when_a_child_owner_is_killed(self):
        """Test a killed child owner's claim dies with its process.

        Given:
            An owner LocalDiscovery entered in a multiprocessing child of
            this process and then killed outright with SIGKILL, so
            neither its context exit nor its atexit fallback runs
        When:
            This process claims the same namespace
        Then:
            It should claim it and leave the namespace fully working —
            the claim is held by the dead process's descriptor, which the
            kernel released, and the registry the kill stranded is
            replaced rather than adopted.
        """
        # Arrange
        namespace = f"stranded-{uuid.uuid4().hex[:12]}"
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        owner = context.Process(target=_hold_namespace, args=(namespace, ready))
        owner.start()
        directory = namespace_directory(namespace)
        try:
            assert ready.wait(_TIMEOUT), "the child never claimed the namespace"
            owner.kill()
            owner.join(_TIMEOUT)
            # Vacuity guard — the kill left the registry behind, so the
            # claim below is made against residue rather than a clean
            # namespace.
            assert (directory / "registry").exists()

            # Act & assert
            with LocalDiscovery(namespace) as successor:
                assert successor.namespace == namespace
                claimant = subprocess.run(
                    [sys.executable, "-c", _CLAIMANT_SCRIPT, namespace],
                    capture_output=True,
                    text=True,
                    timeout=_TIMEOUT,
                )
                # Assert — the successor now holds the namespace itself
                assert "claimed" not in claimant.stdout, claimant.stdout

            # Assert — and its exit reclaimed everything
            assert not directory.exists()
        finally:
            _ensure_killed(owner.pid)


@pytest.mark.integration
class TestPoolNamespaceOwnership:
    @pytest.mark.asyncio
    async def test___aenter___should_raise_when_the_namespace_is_already_owned(
        self, retry_grpc_internal, caplog
    ):
        """Test a second pool cannot claim a live pool's namespace.

        Given:
            An entered pool owning a namespace and serving one worker
        When:
            A second WorkerPool is entered with a fresh LocalDiscovery
            on that same namespace
        Then:
            It should raise DiscoveryNamespaceInUse unwrapped, spawn no
            worker of its own, and leave the owning pool still
            dispatching and able to reap its worker without a failed
            announcement.
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}

        async def body():
            caplog.clear()
            namespace = f"pool-claim-{uuid.uuid4().hex[:12]}"
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(spawn=1, discovery=LocalDiscovery(namespace)):
                    assert await routines.add(1, 2) == 3
                    contender = {
                        child.pid for child in multiprocessing.active_children()
                    }

                    # Act & assert — unwrapped, not an ExceptionGroup:
                    # the claim fails before any worker is spawned.
                    with pytest.raises(DiscoveryNamespaceInUse) as excinfo:
                        async with WorkerPool(
                            spawn=1, discovery=LocalDiscovery(namespace)
                        ):
                            pass

                    assert excinfo.value.namespace == namespace
                    assert {
                        child.pid for child in multiprocessing.active_children()
                    } == contender

                    # Assert — the owning pool is untouched by the refusal
                    assert await routines.add(3, 4) == 7

            # Assert — the owning pool announced its worker's drop. An
            # __enter__ that reclaimed on the FileExistsError path would
            # break that announcement, and the reap alone would not show
            # it.
            assert not any(
                "could not announce" in record.getMessage()
                for record in caplog.records
                if record.levelno == logging.ERROR
            )

        try:
            with caplog.at_level(logging.ERROR, "wool.runtime.worker.pool"):
                await retry_grpc_internal(body)
        finally:
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)

    @pytest.mark.asyncio
    async def test___aenter___should_raise_when_no_owner_holds_the_namespace(self):
        """Test a durable pool over an unowned namespace fails visibly.

        Given:
            A namespace no LocalDiscovery has entered
        When:
            A durable WorkerPool borrowing it is entered eagerly with a
            short quorum timeout
        Then:
            It should raise DiscoveryNamespaceNotFound naming the
            namespace, and leave the namespace claimable afterwards.
        """
        # Arrange
        namespace = f"pool-unowned-{uuid.uuid4().hex[:12]}"

        # Act & assert. The failure surfaces only when the quorum wait
        # expires (#376), so a short quorum_timeout bounds the test.
        with pytest.raises(DiscoveryNamespaceNotFound) as excinfo:
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(
                    discovery=LocalDiscovery.Subscriber(namespace),
                    lazy=False,
                    quorum_timeout=2.0,
                ):
                    pass

        assert excinfo.value.namespace == namespace

        # Assert — the rejected borrow created nothing
        with LocalDiscovery(namespace) as claimed:
            assert claimed.namespace == namespace


@pytest.mark.integration
class TestCrossProcessBorrowing:
    @pytest.mark.asyncio
    async def test___aenter___should_dispatch_when_another_process_owns_the_namespace(
        self, retry_grpc_internal
    ):
        """Test a durable pool borrows a namespace owned elsewhere.

        Given:
            An independent interpreter owning a namespace and hosting
            the only worker in it
        When:
            A durable WorkerPool in this interpreter borrows that
            namespace through a bare subscriber and dispatches
        Then:
            It should return the routine's result from a worker
            outside this interpreter's process tree.
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}

        async def body():
            namespace = f"xproc-{uuid.uuid4().hex[:12]}"
            owner = spawn_script_subprocess(
                _XPROC_OWNER_SCRIPT,
                namespace,
                str(_TESTS_ROOT),
                ready_line="ready",
                timeout=_TIMEOUT,
            )
            try:
                async with asyncio.timeout(_TIMEOUT):
                    async with WorkerPool(
                        discovery=LocalDiscovery.Subscriber(namespace)
                    ):
                        # Act & assert
                        assert await routines.add(20, 22) == 42
                        serving = await routines.get_pid()

                        # Assert — the serving worker is the owner's. A
                        # pool that spawned its own worker would satisfy
                        # the dispatch alone, and its worker is a live
                        # child here while the pool is entered.
                        assert serving != os.getpid()
                        assert serving not in {
                            child.pid for child in multiprocessing.active_children()
                        }
            finally:
                # The harness kills the owner, which by contract leaves
                # its namespace behind for a successor to reclaim; no
                # successor follows here, so clear it.
                release_subprocess(owner)
                shutil.rmtree(namespace_directory(namespace), ignore_errors=True)

        try:
            await retry_grpc_internal(body)
        finally:
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)

    def test___aexit___should_keep_the_registry_when_a_borrower_exits(self):
        """Test a borrowing publisher's exit leaves the owner's registry.

        Given:
            A namespace this process owns, and an independent
            interpreter that binds a borrowing publisher on it,
            publishes a worker, and releases it
        When:
            That interpreter exits
        Then:
            It should leave the registry in place for the owner — only
            the owner reclaims a registry, so a borrower's exit must not
            take the namespace down with it.
        """
        # Arrange
        namespace = f"borrower-{uuid.uuid4().hex[:12]}"

        # Act
        with LocalDiscovery(namespace):
            borrower = subprocess.run(
                [sys.executable, "-c", _BORROWER_SCRIPT, namespace],
                capture_output=True,
                text=True,
                timeout=_TIMEOUT,
            )

            # Assert
            assert borrower.returncode == 0, borrower.stderr
            assert "published" in borrower.stdout, borrower.stdout
            assert "Traceback" not in borrower.stderr, borrower.stderr
            # The owner is still inside its context, so a fresh
            # borrower must still be able to bind.
            assert asyncio.run(_binds(namespace))

    def test___aiter___should_keep_the_registry_when_a_subscriber_exits(self):
        """Test a borrowing subscriber's exit leaves the owner's registry.

        Given:
            A namespace this process owns, and an independent
            interpreter that publishes a worker through a borrowing
            publisher and iterates a borrowing subscriber to its first
            event
        When:
            That interpreter exits
        Then:
            It should leave the registry in place for the owner, a
            read-side borrow claiming no more of the namespace than a
            write-side one.
        """
        # Arrange
        namespace = f"borrower-sub-{uuid.uuid4().hex[:12]}"

        # Act
        with LocalDiscovery(namespace):
            borrower = subprocess.run(
                [sys.executable, "-c", _BORROWER_SCRIPT, namespace],
                capture_output=True,
                text=True,
                timeout=_TIMEOUT,
            )

            # Assert
            assert borrower.returncode == 0, borrower.stderr
            assert "discovered" in borrower.stdout, borrower.stdout
            assert "Traceback" not in borrower.stderr, borrower.stderr
            assert asyncio.run(_binds(namespace))


@pytest.mark.integration
class TestNamespaceResidue:
    @pytest.mark.asyncio
    async def test___aexit___should_leave_no_residue_when_pools_cycle(self):
        """Test repeated default-namespace pools accumulate nothing.

        Given:
            The root directory LocalDiscovery keeps namespaces in, and a
            default namespace per pool — a fresh uuid, so no lifecycle
            reuses another's files
        When:
            Three WorkerPools each enter, dispatch, and exit
        Then:
            It should leave neither a namespace directory nor a lock
            file behind, which is what accumulated an inode per
            lifecycle for the life of the host.
        """
        # Arrange
        root = namespace_directory("probe").parent
        namespaces = set(root.glob("wool-workerpool-*"))
        locks = set(root.glob("wool-lock-*"))

        # Act
        for _ in range(3):
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(spawn=1):
                    assert await routines.add(1, 2) == 3

        # Assert
        assert set(root.glob("wool-workerpool-*")) - namespaces == set()
        assert set(root.glob("wool-lock-*")) - locks == set()


@pytest.mark.integration
class TestCrossProcessTeardown:
    def test___exit___should_unwind_cleanly_when_registry_removed_externally(self):
        """Test owner teardown after an external removal.

        Given:
            An owner LocalDiscovery entered in its own interpreter,
            and an independent interpreter that removed its registry
            out from under it
        When:
            The owner is released, exits its context, and its
            interpreter shuts down
        Then:
            It should exit with status 0 and no traceback — the
            vanished registry aborts neither the context exit nor the
            atexit fallback at interpreter shutdown.
        """
        # Arrange
        namespace = f"external-{uuid.uuid4().hex[:12]}"
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
                [
                    sys.executable,
                    "-c",
                    _ATTACHER_SCRIPT,
                    str(namespace_directory(namespace) / "registry"),
                ],
                capture_output=True,
                text=True,
                timeout=_TIMEOUT,
            )
            assert attacher.returncode == 0
            # Vacuity guard — the registry vanished before the owner
            # exits, so the owner's teardown runs against a missing
            # registry.
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
        """Test the shutdown fallback tolerates a vanished registry.

        Given:
            An owner LocalDiscovery entered in its own interpreter and
            never exited, and an independent interpreter that removed
            its registry out from under it
        When:
            The owner interpreter shuts down with the fallback still
            armed
        Then:
            It should exit with status 0 and no traceback — the armed
            fallback suppresses the missing registry instead of
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
                [
                    sys.executable,
                    "-c",
                    _ATTACHER_SCRIPT,
                    str(namespace_directory(namespace) / "registry"),
                ],
                capture_output=True,
                text=True,
                timeout=_TIMEOUT,
            )
            assert attacher.returncode == 0
            # Vacuity guard — the registry vanished before the owner
            # shuts down.
            assert "unlinked" in attacher.stdout

            # Act — the owner returns from its script with the
            # context still open, so atexit fires the armed fallback
            # against the vanished registry
            owner.stdin.write("\n")
            owner.stdin.flush()
            stdout, stderr = owner.communicate(timeout=_TIMEOUT)

            # Assert
            assert owner.returncode == 0
            assert "leaking-context" in stdout
            assert "Traceback" not in stderr
        finally:
            if owner.poll() is None:
                owner.kill()
                owner.wait(timeout=10)


async def _binds(namespace: str) -> bool:
    """Return whether a fresh borrowing publisher can bind ``namespace``."""
    try:
        async with LocalDiscovery.Publisher(namespace):
            return True
    except DiscoveryNamespaceNotFound:
        return False


def _hold_namespace(namespace: str, ready) -> None:
    """Own ``namespace`` in a child process until killed."""
    with LocalDiscovery(namespace):
        ready.set()
        time.sleep(_TIMEOUT)


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
    """Kill ``pid`` with SIGKILL, best-effort, so a failing run leaks no worker."""
    if pid is not None and _pid_alive(pid):
        with contextlib.suppress(OSError):
            os.kill(pid, signal.SIGKILL)
