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

from tests.helpers import discovery_root
from tests.helpers import namespace_directory
from tests.helpers import registry_path
from wool.runtime.discovery.exceptions import DiscoveryNamespaceInUse
from wool.runtime.discovery.exceptions import DiscoveryNamespaceNotFound
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.loadbalancer.base import NoWorkersAvailable
from wool.runtime.worker.metadata import WorkerMetadata
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
        if len(sys.argv) > 3:
            # Hold the claim past every rival's attempt. Releasing it the
            # instant it is taken would let a claimant scheduled late claim
            # an unheld namespace and report itself a winner too, which is a
            # second winner the mutual-exclusion assertion would count.
            hold = float(sys.argv[3])
            while time.time() < hold:
                time.sleep(0.01)
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

#: Iterate a borrowing subscriber to its first event on a namespace this
#: process does not own, holding no publisher at all -- the read-side
#: borrow, whose exit path releases no blocks and writes to no registry.
_SUBSCRIBER_SCRIPT = """
import asyncio
import sys

from wool.runtime.discovery.local import LocalDiscovery


async def main():
    namespace = sys.argv[1]
    async for event in LocalDiscovery.Subscriber(namespace, poll_interval=0.05):
        print("discovered", flush=True)
        break


asyncio.run(main())
"""

#: Bind a borrowing publisher on a namespace this process owns, then wait
#: to be released before publishing each worker named on the command line,
#: reporting the outcome of each. Waits again at the end so the reader
#: that discovers a worker sees it while this publisher is still bound.
#: A publisher whose owner has exited reports "stranded" rather than
#: dying with a traceback at its ready line.
_ORPHAN_SCRIPT = """
import asyncio
import sys
import uuid

from wool.runtime.discovery.exceptions import DiscoveryCapacityExhausted
from wool.runtime.discovery.exceptions import DiscoveryNamespaceNotFound
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.metadata import WorkerMetadata


async def main():
    namespace, *uids = sys.argv[1:]
    async with LocalDiscovery.Publisher(namespace) as publisher:
        print("bound", flush=True)
        await asyncio.to_thread(sys.stdin.readline)
        for uid in uids:
            metadata = WorkerMetadata(
                uid=uuid.UUID(uid), address="localhost:50051", pid=1, version="1.0"
            )
            try:
                await publisher.publish("worker-added", metadata)
            except DiscoveryCapacityExhausted as error:
                print(f"refused {error.capacity}", flush=True)
            except DiscoveryNamespaceNotFound:
                print("stranded", flush=True)
            else:
                print("published", flush=True)
        await asyncio.to_thread(sys.stdin.readline)


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
                            str(registry_path(namespace)),
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
            It should admit the successor in both cases, from opposite
            starting states: a clean exit reclaims the registry before
            the successor arrives, while a kill strands one for the
            successor to replace.
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
                # The namespace is gone, observed across the process
                # boundary through the public API: a borrower finds
                # nothing to bind. Without this the two arms would
                # assert the same thing and the parametrization would
                # carry no information.
                assert not asyncio.run(_binds(namespace))
            else:
                owner.kill()
                owner.wait(timeout=_TIMEOUT)
                # Nothing ran to clean up, so the registry the successor
                # is about to replace is really there to be replaced.
                assert registry_path(namespace).exists()
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
        # Arrange — the winner holds its claim past the deadline every
        # rival attempts at, so each loser's attempt necessarily lands
        # while the claim is held rather than after it was released.
        namespace = f"race-{uuid.uuid4().hex[:12]}"
        deadline = time.time() + 2.0
        hold = deadline + 2.0
        claimants = [
            subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    _CLAIMANT_SCRIPT,
                    namespace,
                    str(deadline),
                    str(hold),
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
            # Vacuity guard — the kill left the whole generation behind,
            # so the claim below is made against residue rather than a
            # clean namespace, and its sweep has something to remove.
            assert registry_path(namespace).exists()

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

        # Assert — the rejected borrow created nothing. Claiming the
        # namespace afterwards cannot show that on its own, since a claim
        # makes the directory either way; the empty directory a borrow
        # leaves behind is the shape that accumulated an inode per
        # rejected borrow for the life of the host.
        assert not namespace_directory(namespace).exists()
        with LocalDiscovery(namespace) as claimed:
            assert claimed.namespace == namespace


@pytest.mark.integration
class TestCrossProcessBorrowing:
    @pytest.mark.asyncio
    async def test___aenter___should_dispatch_when_another_process_owns_the_namespace(
        self, retry_grpc_internal, namespaces
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
            namespace = namespaces("xproc")
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
                # successor follows here, so the `namespaces` fixture
                # clears it.
                release_subprocess(owner)

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

    @pytest.mark.asyncio
    async def test_publish_should_strand_a_borrower_across_processes(self, namespaces):
        """Test a stranded publisher never reaches a successor's registry.

        Given:
            A publisher in an independent interpreter bound to a
            namespace this process owned at a capacity of eight, that
            owner having since exited, and a successor this process has
            entered on the same namespace
        When:
            The stranded interpreter publishes a worker
        Then:
            It should report itself stranded rather than publishing, and
            the successor's own subscriber should discover nothing — a
            binding ends with the owner it was made against, so a
            successor inherits no borrowers from its predecessor.
        """
        # Arrange
        namespace = namespaces("orphan-xproc")
        worker = uuid.uuid4()
        orphan = None
        successor = contextlib.ExitStack()
        try:
            with LocalDiscovery(namespace, capacity=8):
                orphan = spawn_script_subprocess(
                    _ORPHAN_SCRIPT, namespace, str(worker), ready_line="bound"
                )
            # The owner is gone and a live foreign borrower still holds a
            # descriptor, so the successor's claim also shows that a
            # borrower alone never makes a namespace look in use.
            discovery = successor.enter_context(LocalDiscovery(namespace, capacity=8))

            # Act
            assert orphan.stdin is not None and orphan.stdout is not None
            orphan.stdin.write("\n")
            orphan.stdin.flush()
            published = await asyncio.to_thread(orphan.stdout.readline)

            # Assert
            assert published.strip() == "stranded", published
            assert not await _discovers(discovery, worker, timeout=2)
        finally:
            release_subprocess(orphan)
            successor.close()

    @pytest.mark.asyncio
    async def test___aiter___should_yield_when_another_process_publishes(
        self, namespaces
    ):
        """Test a write in one process wakes a watcher in another.

        Given:
            A namespace this process owns holding one announced worker,
            a subscriber with no poll interval iterated past that first
            event so it is bound and watching, and an independent
            interpreter holding a borrowing publisher open
        When:
            That interpreter publishes a second worker
        Then:
            It should yield a worker-added event for the second worker,
            the notification carrying a write across a process boundary
            with no rescan interval to fall back on.
        """
        # Arrange — the already-announced worker is the handshake that
        # proves the subscriber is watching before the remote publish, so
        # nothing here waits on a clock. With no poll interval the only
        # path to the second worker is the notification itself.
        namespace = namespaces("notify-xproc")
        local = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=1, version="1.0"
        )
        remote = uuid.uuid4()
        discovered: set = set()
        seen = asyncio.Event()
        orphan = None

        async def collect(subscriber):
            async for event in subscriber:
                discovered.add(event.metadata.uid)
                seen.set()

        try:
            with LocalDiscovery(namespace) as discovery:
                async with LocalDiscovery.Publisher(namespace) as publisher:
                    await publisher.publish("worker-added", local)
                    subscriber = discovery.subscribe()
                    task = asyncio.create_task(collect(subscriber))
                    try:
                        async with asyncio.timeout(_TIMEOUT):
                            await seen.wait()
                        assert discovered == {local.uid}

                        orphan = spawn_script_subprocess(
                            _ORPHAN_SCRIPT, namespace, str(remote), ready_line="bound"
                        )

                        # Act
                        assert orphan.stdin is not None
                        assert orphan.stdout is not None
                        orphan.stdin.write("\n")
                        orphan.stdin.flush()
                        published = await asyncio.to_thread(orphan.stdout.readline)
                        assert published.strip() == "published", published

                        # Assert — drained rather than read one event at a
                        # time, because a rescan re-reports workers it
                        # already knows alongside the new one.
                        async with asyncio.timeout(_TIMEOUT):
                            while remote not in discovered:
                                seen.clear()
                                await seen.wait()
                    finally:
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
        finally:
            release_subprocess(orphan)

    @pytest.mark.asyncio
    async def test___aiter___should_keep_the_registry_when_a_subscriber_exits(self):
        """Test a borrowing subscriber's exit leaves the owner's registry.

        Given:
            A namespace this process owns holding an announced worker,
            and an independent interpreter that iterates a borrowing
            subscriber to its first event while holding no publisher
        When:
            That interpreter exits
        Then:
            It should leave the worker discoverable to the owner, a
            read-side borrow claiming no more of the namespace than a
            write-side one.
        """
        # Arrange — the worker is announced from this process, so the
        # child borrows only to read. A child that also published would
        # exercise the write-side exit path and could not distinguish
        # this contract from the publisher case above.
        namespace = f"borrower-sub-{uuid.uuid4().hex[:12]}"
        worker = WorkerMetadata(
            uid=uuid.uuid4(), address="localhost:50051", pid=1, version="1.0"
        )

        async with asyncio.timeout(_TIMEOUT):
            with LocalDiscovery(namespace) as discovery:
                async with LocalDiscovery.Publisher(namespace) as publisher:
                    await publisher.publish("worker-added", worker)

                    # Act
                    borrower = await asyncio.to_thread(
                        subprocess.run,
                        [sys.executable, "-c", _SUBSCRIBER_SCRIPT, namespace],
                        capture_output=True,
                        text=True,
                        timeout=_TIMEOUT,
                    )

                    # Assert
                    assert borrower.returncode == 0, borrower.stderr
                    assert "discovered" in borrower.stdout, borrower.stdout
                    assert "Traceback" not in borrower.stderr, borrower.stderr
                    # Stronger than a bind: the owner's own subscriber
                    # still reaches the worker's block, so the departed
                    # reader took neither the registry nor what it names.
                    discovered = await _discovers(discovery, worker.uid)
                    assert discovered


@pytest.mark.integration
class TestNamespaceResidue:
    @pytest.mark.asyncio
    async def test___exit___should_reclaim_the_directory_when_a_borrower_is_killed(
        self, namespaces
    ):
        """Test a killed borrower leaves the owner nothing to strand.

        Given:
            A namespace this process owns, and an independent
            interpreter that binds a borrowing publisher, publishes a
            worker, and is then killed with SIGKILL so nothing of its
            own ever runs again
        When:
            The owner exits its context
        Then:
            It should reclaim the registry and the whole namespace
            directory with it, leaving a fresh borrower nothing to bind
            and nothing on disk — the owner owns every artifact, so no
            reclaim depends on a borrower surviving to run it.
        """
        # Arrange — this was the residue of #345, pinned while a killed
        # borrower's blocks had no live process left to reclaim them.
        # The owner reclaims them now, so the assertion is inverted.
        namespace = namespaces("killed-borrower")
        worker = uuid.uuid4()
        borrower = None
        try:
            with LocalDiscovery(namespace):
                borrower = spawn_script_subprocess(
                    _ORPHAN_SCRIPT, namespace, str(worker), ready_line="bound"
                )
                assert borrower.stdin is not None and borrower.stdout is not None
                borrower.stdin.write("\n")
                borrower.stdin.flush()
                published = await asyncio.to_thread(borrower.stdout.readline)
                assert published.strip() == "published", published

                # Act
                borrower.kill()
                borrower.wait(timeout=_TIMEOUT)

            # Assert
            assert not await _binds(namespace)
            assert not namespace_directory(namespace).exists()
        finally:
            release_subprocess(borrower)

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
            It should create exactly one namespace directory per
            lifecycle, under a name no other lifecycle uses, and remove
            each one on the way out, leaving nothing behind — the inode
            per lifecycle that accumulated for the life of the host.
        """
        # Arrange — a pool's default namespace is `pool-<uuid>`, so its
        # directory is `<root>/wool/pool-*`. Matching the
        # directory each lifecycle actually creates is what makes the
        # removal observable: a pattern that matched nothing would report
        # an absence of residue it never looked for.
        root = discovery_root()
        root.mkdir(parents=True, exist_ok=True)
        pattern = "pool-*"
        before = set(root.glob(pattern))
        created = []

        # Act
        for _ in range(3):
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(spawn=1):
                    assert await routines.add(1, 2) == 3
                    # The lifecycle is live, so its directory is there to
                    # be named — and named is what lets the assertion
                    # below be about a removal rather than an absence.
                    live = set(root.glob(pattern)) - before
                    assert len(live) == 1, live
                    created.append(live.pop())

        # Assert
        assert [directory for directory in created if directory.exists()] == []
        assert set(root.glob(pattern)) - before == set()
        # Every lifecycle mints its own namespace, so no two share a
        # registry and one pool's teardown cannot reclaim another's.
        assert len(set(created)) == len(created)
        # Containment: everything a lifecycle creates lives under the one
        # fixed `wool/` entry, so the root the namespaces sit beside gains
        # nothing per lifecycle. This is what makes `rm -rf <root>/wool`
        # a complete reclaim rather than a partial one.
        assert [entry for entry in root.parent.glob("wool-*")] == []


@pytest.mark.integration
class TestCrossProcessTeardown:
    def test___exit___should_keep_a_successors_namespace_when_its_directory_is_removed(
        self,
    ):
        """Test a stale owner's exit spares the namespace it no longer holds.

        Given:
            An owner LocalDiscovery entered in its own interpreter whose
            whole namespace directory a third party then removes, and a
            successor in this process that claims the same namespace
            while that first owner is still inside its context
        When:
            The first owner is released and exits its context
        Then:
            It should leave the successor's namespace intact — a fresh
            borrowing publisher still binds it — an owner's teardown
            reaching only what its own claim covers, never a namespace a
            different owner now holds.
        """
        # Arrange
        namespace = f"swept-{uuid.uuid4().hex[:12]}"
        owner = spawn_script_subprocess(
            _OWNER_SCRIPT, namespace, ready_line="ready", timeout=_TIMEOUT
        )
        successor = contextlib.ExitStack()
        try:
            # A temporary-file sweeper takes the whole directory, not
            # just the registry: with the directory gone the claim the
            # first owner holds no longer guards the name, so the
            # namespace is genuinely free for a successor.
            shutil.rmtree(namespace_directory(namespace))
            successor.enter_context(LocalDiscovery(namespace))
            # Vacuity guard — two owners really are live at once, which
            # is the only state in which the first one's teardown could
            # reach the second one's files.
            assert asyncio.run(_binds(namespace))

            # Act
            assert owner.stdin is not None
            owner.stdin.write("\n")
            owner.stdin.flush()
            assert owner.wait(timeout=_TIMEOUT) == 0

            # Assert
            assert asyncio.run(_binds(namespace))
            assert namespace_directory(namespace).exists()
        finally:
            release_subprocess(owner)
            successor.close()
            shutil.rmtree(namespace_directory(namespace), ignore_errors=True)

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
                    str(registry_path(namespace)),
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
                    str(registry_path(namespace)),
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


async def _discovers(discovery: LocalDiscovery, uid, timeout: float = 10.0) -> bool:
    """Return whether ``discovery``'s own subscriber reaches ``uid``.

    Reads the worker's metadata block as well as its registry slot, so a
    registration naming a block nothing can open does not count as
    discovered.
    """
    subscriber = discovery.subscribe(poll_interval=0.05)
    try:
        async with asyncio.timeout(timeout):
            async for event in subscriber:
                if event.metadata.uid == uid:
                    return True
    except TimeoutError:
        return False
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
