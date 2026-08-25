"""End-to-end tests for resource-tracker silence during attach (#336).

These are targeted standalone tests rather than pairwise scenarios. The
symptom `_attach` documents is emitted by the `multiprocessing` resource
tracker — a separate process that inherits the interpreter's stderr and
outlives the work — while the interpreter itself exits 0. Neither half of
that observable is reachable from `build_pool_from_scenario`, which
yields a running system inside the pytest process with no subprocess
stderr to inspect and a dispatch oracle that stays green throughout. The
attach path is also an interpreter-global property, not a dimension the
`Scenario` model can vary.

Coverage of the sub-3.13 branch lives in
``tests/runtime/discovery/test_local.py`` — a subprocess child is
invisible to coverage, whose ``concurrency = multiprocessing`` setting
instruments `multiprocessing.Process` rather than `subprocess.Popen`.
These tests are behavioral evidence, not coverage.
"""

import subprocess
import sys
import uuid

import pytest

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.discovery.local import _attach
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
import asyncio
import sys
from types import SimpleNamespace

import wool.runtime.discovery.local as local
from wool.runtime.discovery.local import LocalDiscovery

mode, namespace = sys.argv[1:3]
if mode == "legacy":
    local.sys = SimpleNamespace(version_info=(3, 12, 0, "final", 0))


async def main():
    # Borrow the owner's registry. Binding maps a segment this
    # interpreter did not create, which is the mapping that must stay
    # untracked; since #300 only the owner's process may enter the
    # namespace itself.
    async with LocalDiscovery.Publisher(namespace):
        print("attached", flush=True)


asyncio.run(main())
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
    def test___aenter___should_keep_the_segment_when_a_borrower_exits(self, mode):
        """Test a borrower's exit leaves the owner's segment mapped.

        Given:
            A namespace this process owns, and an independent
            interpreter that binds and releases a borrowing publisher on
            it via either attach path.
        When:
            That interpreter exits and its resource tracker drains.
        Then:
            It should leave the segment mapped and warn about no leak —
            only the process that created a segment may unlink it, and a
            tracked attach would have the borrower reclaim it instead
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
            # Probe untracked. A plain `SharedMemory(...)` here would
            # register the name with this process's resource tracker,
            # which would then try to unlink it at session exit after
            # the owner already had — the very fault this file asserts
            # the absence of.
            _attach(_short_hash(namespace)).close()

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
