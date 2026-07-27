"""End-to-end tests for resource-tracker silence during attach (#336).

These are targeted standalone tests rather than pairwise scenarios. The
symptom is a traceback printed by the ``multiprocessing`` resource
tracker — a *separate* process that inherits the interpreter's stderr and
outlives the work — while the interpreter itself exits 0. Neither half of
that observable is reachable from ``build_pool_from_scenario``, which
yields a running system inside the pytest process with no subprocess
stderr to inspect and a dispatch oracle that stays green throughout. The
attach path is also an interpreter-global property, not a dimension the
``Scenario`` model can vary.

Reading stderr must run to EOF, which is why every test here uses
``subprocess.run``: the pipe's write end is held by the tracker as well
as the child, so EOF is the only point at which the tracker's output is
guaranteed flushed. ``proc.wait()`` followed by a read would race.

Coverage of the sub-3.13 branch lives in
``tests/runtime/discovery/test_local.py`` — a ``subprocess`` child is
invisible to ``coverage``, whose ``concurrency = multiprocessing``
instruments ``multiprocessing.Process`` rather than ``subprocess.Popen``.
These tests are behavioral evidence, not coverage.
"""

import subprocess
import sys
import uuid

import pytest

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

print(
    "branch=" + ("native" if local.sys.version_info >= (3, 13) else "legacy"),
    flush=True,
)


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
print("done", flush=True)
"""

#: The superseded pattern, in pure standard library: attach to a segment
#: this interpreter created, undo the attach's registration, then unlink.
#: Deliberately independent of `wool`, so it keeps reproducing the fault
#: however `local.py` is later refactored.
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


def _run(script, *args):
    """Run script in a fresh interpreter, returning the completed process."""
    return subprocess.run(
        [sys.executable, "-c", script, *args],
        capture_output=True,
        text=True,
        timeout=_TIMEOUT,
    )


@pytest.mark.integration
class TestCrossProcessTracker:
    def test_publish_should_emit_no_tracker_output_when_track_unsupported(self):
        """Test a worker lifecycle below 3.13 leaves the tracker silent.

        Given:
            An independent interpreter forced onto the attach path taken
            below 3.13, owning a namespace of its own.
        When:
            It announces, re-announces, updates, and drops three workers,
            then exits so its resource tracker drains.
        Then:
            It should exit 0 having taken that path, with no tracker
            traceback on stderr — the failure no exit code reflects.
        """
        # Act
        result = _run(_PUBLISH_SCRIPT, "legacy", f"tracker-{uuid.uuid4().hex[:12]}")

        # Assert
        assert result.returncode == 0, result.stderr
        assert "branch=legacy" in result.stdout, result.stdout
        assert "done" in result.stdout, result.stdout
        assert "KeyError" not in result.stderr, result.stderr
        assert "Traceback" not in result.stderr, result.stderr
        assert "resource_tracker" not in result.stderr, result.stderr

    def test_publish_should_emit_no_tracker_output_on_the_running_interpreter(self):
        """Test a worker lifecycle leaves the tracker silent as shipped.

        Given:
            An independent interpreter taking whichever attach path its
            own version selects, owning a namespace of its own.
        When:
            It announces, re-announces, updates, and drops three workers,
            then exits so its resource tracker drains.
        Then:
            It should exit 0 with no tracker traceback on stderr — on an
            interpreter below 3.13 this reproduces the reported failure
            with nothing forced.
        """
        # Act
        result = _run(_PUBLISH_SCRIPT, "native", f"tracker-{uuid.uuid4().hex[:12]}")

        # Assert
        assert result.returncode == 0, result.stderr
        assert "done" in result.stdout, result.stdout
        assert "KeyError" not in result.stderr, result.stderr
        assert "Traceback" not in result.stderr, result.stderr
        assert "resource_tracker" not in result.stderr, result.stderr

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
