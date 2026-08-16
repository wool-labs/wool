"""End-to-end tests for resource-tracker silence around shared memory.

Covers both construction paths: an attach must not register the segment
it maps (#336), and a create that fails partway through must not
unregister a name it never registered (#340).

These are targeted standalone tests rather than pairwise scenarios. The
symptom `_attach` and `_create` document is emitted by the
`multiprocessing` resource tracker — a separate process that inherits the
interpreter's stderr and outlives the work — while the interpreter itself
exits 0. Neither half of that observable is reachable from
`build_pool_from_scenario`, which yields a running system inside the
pytest process with no subprocess stderr to inspect and a dispatch oracle
that stays green throughout. Both paths are also interpreter-global
properties, not dimensions the `Scenario` model can vary.

Only the attach tests carry a ``mode`` parametrization. `_attach` really
does branch on the interpreter version, and forcing the sub-3.13 leg is
the only way to reach it from 3.13. `_create` has no such branch: the
stray unregister reproduces unpatched on every supported version, because
3.13's ``self._track`` guard on `SharedMemory.unlink` is satisfied for a
segment created with the default ``track=True``. Splitting those tests by
version would run identical code twice and assert a version-conditionality
that does not exist.

Coverage of both paths lives in ``tests/runtime/discovery/test_local.py``
— a subprocess child is invisible to coverage, whose `multiprocessing`
concurrency support instruments `multiprocessing.Process` and not
`subprocess.Popen`. These tests are behavioral evidence, not coverage.

Liveness properties belong here for a second reason. The lock both
construction paths take is process-global, so a test that wedges it inside
the pytest process strands every later test in the session; run in a
child, the same wedge is reaped by `_run`'s timeout and reported against
the one test that caused it.
"""

import os
import subprocess
import sys
import uuid
from multiprocessing.shared_memory import SharedMemory

import pytest

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.discovery.local import _short_hash

from .conftest import _TIMEOUT

#: Announce, re-announce, update, and drop workers on a namespace this
#: interpreter owns, so the same segments are attached repeatedly and
#: then unlinked by their creator — the sequence that faults. ``mode``
#: selects the attach path; "legacy" forces the branch interpreters
#: below 3.13 take, which is otherwise unreachable on 3.13.
_PUBLISH_SCRIPT = """
import asyncio
import sys
import uuid
from types import SimpleNamespace

import wool.runtime.discovery.local as local
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.metadata import WorkerMetadata

mode, namespace = sys.argv[1:3]
if mode == "legacy":
    local.sys = SimpleNamespace(version_info=(3, 12, 0, "final", 0))

# Report the path that actually ran, observed from the constructor's
# arguments rather than by re-reading the version the override set:
# only the modern path passes ``track``.
_mapping = local.SharedMemory
_paths = set()


def _observing(*args, **kwargs):
    if not kwargs.get("create"):
        _paths.add("native" if "track" in kwargs else "legacy")
    return _mapping(*args, **kwargs)


local.SharedMemory = _observing


async def main():
    workers = [
        WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:5005%d" % i,
            pid=100 + i,
            version="1.0",
        )
        for i in range(3)
    ]
    with LocalDiscovery(namespace):
        async with LocalDiscovery.Publisher(namespace) as publisher:
            for worker in workers:
                await publisher.publish("worker-added", worker)
                await publisher.publish("worker-added", worker)
                await publisher.publish("worker-updated", worker)
            for worker in workers:
                await publisher.publish("worker-dropped", worker)
    print("segments=%d" % (len(workers) + 1), flush=True)


asyncio.run(main())
print("path=" + ",".join(sorted(_paths)), flush=True)
print("done", flush=True)
"""

#: Attach to a namespace this interpreter does not own, then exit. The
#: creating process must still find its segment afterwards: only a creator
#: may unlink, and a tracked attach would have this process's tracker do it
#: on the way out (bpo-38119).
_ATTACHER_SCRIPT = """
import sys
from types import SimpleNamespace

import wool.runtime.discovery.local as local
from wool.runtime.discovery.local import LocalDiscovery

mode, namespace = sys.argv[1:3]
if mode == "legacy":
    local.sys = SimpleNamespace(version_info=(3, 12, 0, "final", 0))

with LocalDiscovery(namespace):
    print("attached", flush=True)
print("done", flush=True)
"""

#: The superseded pattern, in pure standard library: attach to a segment
#: this interpreter created, undo the attach's registration, then unlink.
#: Deliberately independent of `wool`, so it keeps reproducing the fault
#: however ``local.py`` is later refactored.
_SUPERSEDED_SCRIPT = """
import sys
from multiprocessing import resource_tracker
from multiprocessing.shared_memory import SharedMemory

name = sys.argv[1]
creator = SharedMemory(name=name, create=True, size=64)
try:
    for _ in range(3):
        attached = SharedMemory(name=name)
        resource_tracker.unregister(attached._name, "shared_memory")
        attached.close()
finally:
    creator.close()
    creator.unlink()
print("done", flush=True)
"""


#: Fail the truncation a ``create=True`` construction performs, so it
#: reaches the constructor's ``except OSError: self.unlink()`` handler
#: with the name never registered. ``site`` selects which of Wool's two
#: create sites is broken: the namespace segment `LocalDiscovery` owns,
#: or a worker's metadata block. Truncation is the injection point
#: because only a create truncates — an attach skips it — so the
#: address-space remap every publish performs keeps working. The errno is
#: reported back, not just the exception class, so the assertion pins the
#: failure this script injected rather than any `OSError` the child might
#: raise for an unrelated reason.
_CREATE_SCRIPT = """
import asyncio
import errno
import os
import sys
import uuid

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.metadata import WorkerMetadata

site, namespace = sys.argv[1:3]
_real_ftruncate = os.ftruncate


def _enospc(fd, length):
    raise OSError(errno.ENOSPC, "No space left on device")


def _reported(error):
    return "raised=" + errno.errorcode.get(error.errno, str(error.errno))


async def main():
    if site == "owner":
        os.ftruncate = _enospc
        try:
            with LocalDiscovery(namespace):
                return "unexpectedly-entered"
        except OSError as error:
            return _reported(error)
        finally:
            os.ftruncate = _real_ftruncate

    worker = WorkerMetadata(
        uid=uuid.uuid4(),
        address="localhost:50051",
        pid=101,
        version="1.0",
    )
    with LocalDiscovery(namespace):
        async with LocalDiscovery.Publisher(namespace) as publisher:
            os.ftruncate = _enospc
            try:
                await publisher.publish("worker-added", worker)
                return "unexpectedly-published"
            except OSError as error:
                return _reported(error)
            finally:
                os.ftruncate = _real_ftruncate


print(asyncio.run(main()), flush=True)
print("done", flush=True)
"""

#: The create fault, in pure standard library and with nothing patched: a
#: ``create=True`` construction for more memory than any machine can back,
#: so the failure is the operating system's rather than a simulation. The
#: constructor then unlinks — and unregisters — a name
#: `resource_tracker.register` never reached, since the registration
#: happens after the block that unlinks. Independent of `wool` *and* of
#: the injection technique the rest of this file uses, so it keeps
#: reproducing the fault however ``local.py`` is refactored and however
#: that technique is later changed.
#:
#: 2**48 bytes fails on both supported platforms, though at different
#: syscalls: the mapping on macOS, which truncates sparsely (``ENOMEM``),
#: and the truncation on Linux, whose ``/dev/shm`` is a bounded tmpfs
#: (``ENOSPC``). Both raise inside the constructor's guarded block, which
#: is the only property this control depends on.
_UNGUARDED_CREATE_SCRIPT = """
import sys
from multiprocessing.shared_memory import SharedMemory

name = sys.argv[1]
try:
    SharedMemory(name=name, create=True, size=2 ** 48)
    print("unexpectedly-created", flush=True)
except OSError:
    print("raised", flush=True)
print("done", flush=True)
"""

#: Enter a namespace this interpreter does not own, losing the create race
#: and falling back to an attach. The two constructions take the same
#: non-reentrant lock, so a suppression window widened across
#: `LocalDiscovery.__enter__`'s ``try``/``except`` wedges here — and a
#: wedge in a child is reaped by `_run`'s timeout, where the same wedge on
#: a daemon thread inside pytest would strand the lock and hang every
#: later test in the session.
_LOSER_SCRIPT = """
import sys
from types import SimpleNamespace

import wool.runtime.discovery.local as local
from wool.runtime.discovery.local import LocalDiscovery

mode, namespace = sys.argv[1:3]
if mode == "legacy":
    # Below 3.13 the attach takes the lock too, which is the only
    # configuration in which the two constructions can contend.
    local.sys = SimpleNamespace(version_info=(3, 12, 0, "final", 0))

with LocalDiscovery(namespace):
    print("attached", flush=True)
print("done", flush=True)
"""

#: Fork from inside an open suppression window and have the child create a
#: segment of its own. The child inherits the lock held and the shims
#: installed, so without `os.register_at_fork` restoring both it wedges on
#: the first construction and never prints its marker.
_FORK_SCRIPT = """
import os
import sys
import threading

import wool.runtime.discovery.local as local
from wool.runtime.discovery.local import LocalDiscovery

namespace = sys.argv[1]
inside = threading.Event()
release = threading.Event()
_mapping = local.SharedMemory


def _blocking(*args, **kwargs):
    # Park inside the window, so the fork below copies the lock held.
    inside.set()
    release.wait(10)
    return _mapping(*args, **kwargs)


def _hold():
    local.SharedMemory = _blocking
    try:
        with LocalDiscovery(namespace + "-held"):
            pass
    finally:
        local.SharedMemory = _mapping


holder = threading.Thread(target=_hold, daemon=True)
holder.start()
inside.wait(10)

pid = os.fork()
if pid == 0:
    try:
        with LocalDiscovery(namespace + "-child"):
            print("child-entered", flush=True)
        os._exit(0)
    except BaseException:
        os._exit(3)

_, status = os.waitpid(pid, 0)
print("child-status=%d" % (status >> 8), flush=True)
release.set()
holder.join(10)
print("done", flush=True)
"""


def _run(script, *args):
    """Run script in a fresh interpreter, returning the completed process.

    `subprocess.run` reads stderr to EOF, which matters here: the pipe's
    write end is held by the resource tracker as well as the child, so EOF
    is the only point at which the tracker's output is guaranteed flushed.
    A `subprocess.Popen.wait` followed by a read would race it.
    """
    return subprocess.run(
        [sys.executable, "-c", script, *args],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT,
    )


@pytest.mark.integration
class TestCrossProcessTracker:
    """Pin tracker silence and segment ownership across a real process."""

    @pytest.mark.parametrize("mode", ["legacy", "native"])
    def test_publish_should_emit_no_tracker_output_when_workers_cycle(self, mode):
        """Test a worker lifecycle leaves the resource tracker silent.

        Given:
            An independent interpreter owning a namespace of its own, on
            the attach path taken where ``track`` is unavailable or on
            whichever path its own version selects.
        When:
            It announces, re-announces, updates, and drops three workers,
            then exits so its resource tracker drains.
        Then:
            It should exit 0 with no tracker traceback on stderr — the
            failure no exit code reflects. Below 3.13 the unforced case
            exercises the reported conditions with nothing simulated.
        """
        # Act
        result = _run(_PUBLISH_SCRIPT, mode, f"tracker-{uuid.uuid4().hex[:12]}")

        # Assert
        assert result.returncode == 0, result.stderr
        assert "done" in result.stdout, result.stdout
        if mode == "legacy":
            assert "path=legacy" in result.stdout, result.stdout
        assert "KeyError" not in result.stderr, result.stderr
        assert "Traceback" not in result.stderr, result.stderr
        assert "resource_tracker" not in result.stderr, result.stderr

    @pytest.mark.parametrize("mode", ["legacy", "native"])
    def test___enter___should_keep_the_segment_when_an_attacher_exits(self, mode):
        """Test a non-owner's exit leaves the owner's segment mapped.

        Given:
            A namespace this process created, and an independent
            interpreter that enters and leaves it as a non-owner on either
            attach path.
        When:
            That interpreter exits and its resource tracker drains.
        Then:
            It should leave the segment mapped and warn about no leak —
            only the process that created a segment may unlink it, and a
            tracked attach would have the attacher reclaim it instead
            (bpo-38119).
        """
        # Arrange
        namespace = f"tracker-{uuid.uuid4().hex[:12]}"

        # Act
        with LocalDiscovery(namespace):
            result = _run(_ATTACHER_SCRIPT, mode, namespace)

            # Assert — the owner is still inside its context, so the
            # segment must still be there for it to map.
            assert result.returncode == 0, result.stderr
            assert "attached" in result.stdout, result.stdout
            assert "leaked shared_memory" not in result.stderr, result.stderr
            assert "KeyError" not in result.stderr, result.stderr
            SharedMemory(name=_short_hash(namespace)).close()

    def test___enter___should_emit_no_tracker_output_when_the_create_fails(self):
        """Test a failed namespace create leaves the resource tracker silent.

        Given:
            An independent interpreter whose filesystem fails the
            truncation every shared-memory create performs, entering a
            namespace it would own.
        When:
            The entry raises, the interpreter exits, and its resource
            tracker drains.
        Then:
            It should exit 0 with no tracker traceback on stderr — the
            constructor's own unlink issues an unregister for a name it
            never registered, and the exit code does not reflect it.
        """
        # Act
        result = _run(_CREATE_SCRIPT, "owner", f"tracker-{uuid.uuid4().hex[:12]}")

        # Assert — the errno marker rules out a vacuous pass: silence
        # proves nothing if the child never reached a failing create, and
        # the exception class alone would accept any stray `OSError`.
        assert result.returncode == 0, result.stderr
        assert "raised=ENOSPC" in result.stdout, result.stdout
        assert "done" in result.stdout, result.stdout
        assert "KeyError" not in result.stderr, result.stderr
        assert "Traceback" not in result.stderr, result.stderr
        assert "resource_tracker" not in result.stderr, result.stderr

    def test_publish_should_emit_no_tracker_output_when_a_block_create_fails(self):
        """Test a failed block create leaves the resource tracker silent.

        Given:
            An independent interpreter owning a namespace of its own,
            whose filesystem fails the truncation for the duration of one
            worker announcement.
        When:
            The publish raises, the interpreter exits, and its resource
            tracker drains.
        Then:
            It should exit 0 with no tracker traceback on stderr, covering
            the pool's create site as well as the namespace's.
        """
        # Act
        result = _run(_CREATE_SCRIPT, "publisher", f"tracker-{uuid.uuid4().hex[:12]}")

        # Assert
        assert result.returncode == 0, result.stderr
        assert "raised=ENOSPC" in result.stdout, result.stdout
        assert "done" in result.stdout, result.stdout
        assert "KeyError" not in result.stderr, result.stderr
        assert "Traceback" not in result.stderr, result.stderr
        assert "resource_tracker" not in result.stderr, result.stderr

    @pytest.mark.parametrize("mode", ["legacy", "native"])
    def test___enter___should_admit_the_attach_when_the_create_loses(self, mode):
        """Test losing the create race still admits the attach that follows.

        Given:
            A namespace this process owns, and an independent interpreter
            entering it as a non-owner — on the attach path that takes the
            same lock the create does, or on whichever path its own
            version selects.
        When:
            That interpreter's create raises FileExistsError and the
            attach runs from the handler.
        Then:
            It should attach and exit within the bound — a suppression
            window held open across the handler would wedge on a lock its
            own frame still owns, and running it in a child keeps that
            wedge from stranding the lock for the rest of this session.
        """
        # Arrange
        namespace = f"tracker-{uuid.uuid4().hex[:12]}"

        # Act
        with LocalDiscovery(namespace):
            result = _run(_LOSER_SCRIPT, mode, namespace)

            # Assert
            assert result.returncode == 0, result.stderr
            assert "attached" in result.stdout, result.stdout
            assert "done" in result.stdout, result.stdout

    @pytest.mark.skipif(not hasattr(os, "register_at_fork"), reason="POSIX-only guard")
    def test___enter___should_admit_a_create_in_a_child_forked_mid_window(self):
        """Test a child forked inside the suppression window can still create.

        Given:
            An independent interpreter with one thread parked inside an
            open suppression window, so a fork copies the lock held and
            the shims installed.
        When:
            It forks and the child enters a namespace of its own.
        Then:
            It should report the child entering and exiting 0 — the
            at-fork handler replaces the inherited lock and unwinds the
            inherited shims, without which the child wedges on its first
            construction.
        """
        # Act
        result = _run(_FORK_SCRIPT, f"tracker-{uuid.uuid4().hex[:12]}")

        # Assert
        assert result.returncode == 0, result.stderr
        assert "child-entered" in result.stdout, result.stdout
        assert "child-status=0" in result.stdout, result.stdout
        assert "done" in result.stdout, result.stdout

    def test_unlink_should_emit_tracker_output_when_a_create_fails(self):
        """Test an unguarded failed create faults, so silence means something.

        Given:
            An independent interpreter creating a segment larger than the
            system can back, using the standard library alone and patching
            nothing, so the constructor unlinks a name it never registered.
        When:
            It runs to completion and its resource tracker drains.
        Then:
            It should exit 0 yet print a tracker KeyError, proving these
            tests observe the fault they assert the absence of, and that
            an exit code alone cannot detect it.
        """
        # Act
        result = _run(_UNGUARDED_CREATE_SCRIPT, f"tracker-{uuid.uuid4().hex[:12]}")

        # Assert
        assert result.returncode == 0, result.stderr
        assert "raised" in result.stdout, result.stdout
        assert "done" in result.stdout, result.stdout
        assert "KeyError" in result.stderr, result.stderr
        assert "resource_tracker" in result.stderr, result.stderr

    def test_unregister_should_emit_tracker_output_when_an_attach_undoes_it(self):
        """Test the superseded pattern still faults, so silence means something.

        Given:
            An independent interpreter reproducing the pattern this fix
            replaced — attach, undo the attach's registration, unlink —
            against a segment it created itself.
        When:
            It runs to completion and its resource tracker drains.
        Then:
            It should exit 0 yet print a tracker KeyError, proving these
            tests observe the fault they assert the absence of, and that
            an exit code alone cannot detect it.
        """
        # Act
        result = _run(_SUPERSEDED_SCRIPT, f"tracker-{uuid.uuid4().hex[:12]}")

        # Assert
        assert result.returncode == 0, result.stderr
        assert "done" in result.stdout, result.stdout
        assert "KeyError" in result.stderr, result.stderr
        assert "resource_tracker" in result.stderr, result.stderr
