"""End-to-end tests for LocalDiscovery worker re-announcement (#321).

These are targeted standalone tests rather than pairwise scenarios: a
``WorkerPool`` announces each worker exactly once on entry, so the
re-announce shape — the same UID published ``"worker-added"`` twice —
cannot be driven through the pairwise array's dispatch oracle and needs
a bare ``LocalDiscovery.Publisher``. The cross-process case matters
because POSIX advisory file locks are per-process, so only an
independent interpreter exercises real publish-lock arbitration and
attach-by-name against a metadata block created in another process.
Unit-level coverage of the re-add contract lives in
``tests/runtime/discovery/test_local.py``.
"""

import asyncio
import uuid

import pytest

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.metadata import WorkerMetadata

from .conftest import _TIMEOUT
from .conftest import release_subprocess
from .conftest import spawn_script_subprocess

_READD_SCRIPT = """
import asyncio
import sys
import uuid

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.metadata import WorkerMetadata


async def main():
    namespace, uid, version = sys.argv[1:4]
    metadata = WorkerMetadata(
        uid=uuid.UUID(uid),
        address="localhost:50051",
        pid=123,
        version=version,
    )
    async with LocalDiscovery.Publisher(namespace) as publisher:
        await publisher.publish("worker-added", metadata)
        print("ready", flush=True)
        await asyncio.get_running_loop().run_in_executor(None, sys.stdin.readline)


asyncio.run(main())
"""


@pytest.mark.integration
class TestCrossProcessReadd:
    @pytest.mark.asyncio
    async def test_publish_should_retire_worker_when_readded_from_subprocess(self):
        """Test a single drop retires a worker re-announced cross-process.

        Given:
            A worker published at version 1.0 in the test process, an
            independent subprocess that has re-announced the same UID at
            version 2.0 under real cross-process lock arbitration, and a
            subscriber that has discovered the bumped version
        When:
            The test-process publisher publishes a single
            "worker-dropped"
        Then:
            It should emit worker-dropped — the cross-process re-announce
            refreshed the registration in place, leaving nothing a
            single drop cannot reclaim.
        """
        # Arrange
        namespace = f"readd-{uuid.uuid4().hex[:12]}"
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )

        refreshed_seen = asyncio.Event()
        dropped_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                if event.metadata.uid != worker.uid:
                    continue
                if event.metadata.version == "2.0":
                    refreshed_seen.set()
                if event.type == "worker-dropped":
                    dropped_seen.set()
                    break

        proc = None
        async with asyncio.timeout(_TIMEOUT):
            with LocalDiscovery(namespace) as discovery:
                publisher = LocalDiscovery.Publisher(namespace)
                try:
                    async with publisher:
                        await publisher.publish("worker-added", worker)

                        # Arrange — an independent interpreter re-announces
                        # the same UID; the subscriber's discovery of the
                        # bumped version proves the refresh landed.
                        proc = spawn_script_subprocess(
                            _READD_SCRIPT,
                            namespace,
                            str(worker.uid),
                            "2.0",
                            ready_line="ready",
                        )
                        subscriber = discovery.subscribe(poll_interval=0.05)
                        task = asyncio.create_task(collect(subscriber))
                        try:
                            await asyncio.wait_for(refreshed_seen.wait(), timeout=10)

                            # Act
                            await publisher.publish("worker-dropped", worker)

                            # Assert
                            await asyncio.wait_for(dropped_seen.wait(), timeout=10)
                        finally:
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass
                finally:
                    release_subprocess(proc)

    @pytest.mark.asyncio
    async def test_publish_should_keep_worker_when_readd_subprocess_exits(self):
        """Test a re-announcer's exit leaves the live block intact.

        Given:
            A worker published in the test process and re-announced at a
            bumped version by an independent subprocess that has since
            exited while the worker remains registered
        When:
            A subscriber in the test process then iterates the namespace
        Then:
            It should discover the worker at the bumped version — the
            exited re-announcer's resource tracker must not have
            unlinked the live metadata block.
        """
        # Arrange
        namespace = f"readd-exit-{uuid.uuid4().hex[:12]}"
        worker = WorkerMetadata(
            uid=uuid.uuid4(),
            address="localhost:50051",
            pid=123,
            version="1.0",
        )

        refreshed_seen = asyncio.Event()

        async def collect(subscriber):
            async for event in subscriber:
                if event.metadata.uid != worker.uid:
                    continue
                if event.metadata.version == "2.0":
                    refreshed_seen.set()
                    break

        async with asyncio.timeout(_TIMEOUT):
            with LocalDiscovery(namespace) as discovery:
                publisher = LocalDiscovery.Publisher(namespace)
                async with publisher:
                    await publisher.publish("worker-added", worker)

                    # Act — the re-announcer exits while the worker is
                    # still registered; its resource tracker runs at exit.
                    proc = spawn_script_subprocess(
                        _READD_SCRIPT,
                        namespace,
                        str(worker.uid),
                        "2.0",
                        ready_line="ready",
                    )
                    release_subprocess(proc)
                    await asyncio.sleep(0.3)

                    # Assert
                    subscriber = discovery.subscribe(poll_interval=0.05)
                    task = asyncio.create_task(collect(subscriber))
                    try:
                        await asyncio.wait_for(refreshed_seen.wait(), timeout=10)
                    except asyncio.TimeoutError:
                        pytest.fail("Worker lost after re-announcer exit")
                    finally:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
