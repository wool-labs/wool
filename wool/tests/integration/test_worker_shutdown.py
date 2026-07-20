"""Integration tests for worker shutdown, reaping, and orphan prevention."""

import asyncio
import contextlib
import logging
import multiprocessing
import os
import signal
import subprocess
import sys
import time
import uuid

import grpc
import grpc.aio
import pytest

from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.pool import WorkerPool
from wool.runtime.worker.process import WorkerProcess

from . import routines
from .conftest import _DirectDiscovery
from .conftest import build_pool_from_scenario
from .conftest import default_scenario

#: Hang the ``worker-dropped`` announcement rather than failing it,
#: modelling a discovery service that has stopped responding rather
#: than one that errors outright. Mirrors the unit suite's sentinel of
#: the same name, so one convention governs both.
_HANG = object()

# This helper script must stay a `python -c` string: it has no
# `__main__` guard, so as a script file the spawn bootstrap's re-import
# of `__main__` would re-run it in every worker child.
_PARENT_SCRIPT = """
import asyncio

from wool.runtime.worker.local import LocalWorker


async def main():
    worker = LocalWorker(shutdown_grace_period=5.0)
    await worker.start(timeout=30)
    assert worker.metadata is not None
    print(worker.metadata.pid, flush=True)
    await asyncio.sleep(300)


asyncio.run(main())
"""

# `python -c` string for the same reason as `_PARENT_SCRIPT` above.
_ABANDON_SCRIPT = """
import asyncio
import sys

from wool.runtime.worker.local import LocalWorker


async def main():
    worker = LocalWorker(shutdown_grace_period=5.0)
    await worker.start(timeout=30)
    assert worker.metadata is not None
    print(worker.metadata.pid, flush=True)
    # Hold here so the test owns the moment abandonment begins.
    sys.stdin.readline()
    # Return without stopping the worker — abandonment distilled.


asyncio.run(main())
"""


@pytest.mark.integration
class TestWorkerLoopDrain:
    @pytest.mark.asyncio
    async def test_graceful_shutdown_drains_second_generation_cleanup_tasks(
        self, tmp_path, credentials_map, retry_grpc_internal
    ):
        """Test that worker-loop teardown drains every generation of
        pending cleanup tasks.

        Given:
            A routine whose finally clause schedules an orphaned cleanup
            task that, when cancelled during teardown, schedules a
            further cleanup task.
        When:
            A worker pool is dispatched the routine and then torn down.
        Then:
            It should drain every generation, so the deepest cleanup
            task runs its finally clause and writes its sentinel file.
        """
        # Arrange
        sentinel = tmp_path / "drain-sentinel.txt"
        scenario = default_scenario()

        # Act
        async def body():
            async with build_pool_from_scenario(scenario, credentials_map):
                result = await routines.add_then_schedule_cleanup(1, 2, str(sentinel))
                assert result == 3

        await retry_grpc_internal(body)

        # Assert
        assert sentinel.read_text() == "drained"


@pytest.mark.integration
class TestWorkerOrphanPrevention:
    @pytest.mark.asyncio
    async def test_stop_should_leave_no_live_worker_process(self):
        """Test stop fully reaps the worker subprocess before returning.

        Given:
            A started LocalWorker backed by a real subprocess.
        When:
            stop() is called.
        Then:
            It should return only once the worker subprocess no
            longer exists.
        """
        # Arrange
        worker = LocalWorker(shutdown_grace_period=30.0)
        await worker.start(timeout=30)
        assert worker.metadata is not None
        pid = worker.metadata.pid
        assert _pid_alive(pid)

        try:
            # Act
            await worker.stop(timeout=30)

            # Assert
            assert not _pid_alive(pid)
        finally:
            _ensure_killed(pid)

    def test_worker_should_exit_when_parent_is_killed(self):
        """Test a worker never outlives an abruptly killed parent.

        Given:
            A parent process that started a LocalWorker and holds no
            teardown path.
        When:
            The parent is killed with SIGKILL, bypassing all graceful
            shutdown.
        Then:
            The orphaned worker subprocess should detect parent death
            and exit within the grace window.
        """
        # Arrange
        helper = subprocess.Popen(
            [sys.executable, "-c", _PARENT_SCRIPT],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        worker_pid = None
        try:
            assert helper.stdout is not None
            worker_pid = int(helper.stdout.readline().strip())
            assert _pid_alive(worker_pid)

            # Act
            helper.kill()
            helper.wait(timeout=10)

            # Assert
            deadline = time.monotonic() + 15.0
            while _pid_alive(worker_pid) and time.monotonic() < deadline:
                time.sleep(0.1)
            assert not _pid_alive(worker_pid)
        finally:
            if helper.poll() is None:
                helper.kill()
                helper.wait(timeout=10)
            _ensure_killed(worker_pid)

    def test_interpreter_exit_should_not_block_when_worker_abandoned(self):
        """Test an abandoned worker cannot wedge parent interpreter exit.

        Given:
            A parent process that started a LocalWorker and reaches
            interpreter exit without ever stopping it, leaving
            multiprocessing's atexit handler to deal with the live
            child.
        When:
            The parent interpreter exits.
        Then:
            The parent should exit cleanly within a bound instead of
            blocking in the atexit join, and the abandoned worker
            subprocess should be terminated rather than orphaned.
        """
        # Arrange
        helper = subprocess.Popen(
            [sys.executable, "-c", _ABANDON_SCRIPT],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        worker_pid = None
        try:
            assert helper.stdout is not None
            assert helper.stdin is not None
            worker_pid = int(helper.stdout.readline().strip())
            assert _pid_alive(worker_pid)

            # Act
            helper.stdin.write("go\n")
            helper.stdin.flush()
            returncode = helper.wait(timeout=30)

            # Assert
            assert returncode == 0
            deadline = time.monotonic() + 15.0
            while _pid_alive(worker_pid) and time.monotonic() < deadline:
                time.sleep(0.1)
            assert not _pid_alive(worker_pid)
        finally:
            if helper.poll() is None:
                helper.kill()
                helper.wait(timeout=10)
            _ensure_killed(worker_pid)

    @pytest.mark.asyncio
    async def test_pool_exit_should_leave_no_live_worker_processes(self):
        """Test pool teardown reaps every spawned worker process.

        Given:
            An ephemeral WorkerPool with two spawned local workers.
        When:
            The pool context exits normally.
        Then:
            It should leave no worker subprocess alive.
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}
        spawned = []

        try:
            # Act
            async with WorkerPool("orphan-test", spawn=2):
                spawned = [
                    child.pid
                    for child in multiprocessing.active_children()
                    if child.pid not in before
                ]
                assert len(spawned) == 2

            # Assert
            for pid in spawned:
                assert not _pid_alive(pid)
        finally:
            for pid in spawned:
                _ensure_killed(pid)

    @pytest.mark.asyncio
    async def test_pool_exit_should_tolerate_crashed_worker(self):
        """Test pool teardown survives a worker that already died.

        Given:
            An entered single-worker pool whose worker subprocess was
            killed mid-context.
        When:
            The pool context exits.
        Then:
            It should complete teardown without raising and leave the
            worker's corpse reaped.
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}
        spawned = []

        try:
            # Act
            async with WorkerPool("orphan-test", spawn=1):
                spawned = [
                    child.pid
                    for child in multiprocessing.active_children()
                    if child.pid not in before
                ]
                assert len(spawned) == 1
                os.kill(spawned[0], signal.SIGKILL)
                await asyncio.sleep(0.2)

            # Assert
            assert not _pid_alive(spawned[0])
        finally:
            for pid in spawned:
                _ensure_killed(pid)

    @pytest.mark.asyncio
    async def test_pool_exit_should_reap_workers_when_teardown_cancelled(self):
        """Test a cancelled teardown still reaps every spawned worker.

        Given:
            An entered WorkerPool with two spawned local workers, under
            an asyncio.timeout armed only once the workers are up, so
            the cancellation lands in teardown rather than startup.
        When:
            The timeout cancels the pool's teardown at its first await.
        Then:
            It should still leave no worker subprocess alive, since each
            worker's reap runs in an executor thread that a cancelled
            stop cannot call off.
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}
        spawned = []
        cancelled = False

        try:
            # Act
            try:
                async with asyncio.timeout(None) as cancellation:
                    async with WorkerPool("cancelled-teardown-test", spawn=2):
                        spawned = [
                            child.pid
                            for child in multiprocessing.active_children()
                            if child.pid not in before
                        ]
                        assert len(spawned) == 2
                        # Arm only now, with a deadline already behind
                        # us: the cancellation then lands at teardown's
                        # first await, whatever teardown costs.
                        loop = asyncio.get_running_loop()
                        cancellation.reschedule(loop.time())
            except TimeoutError:
                cancelled = True

            # Assert
            assert cancelled
            deadline = time.monotonic() + 30.0
            while (
                any(_pid_alive(pid) for pid in spawned) and time.monotonic() < deadline
            ):
                await asyncio.sleep(0.1)
            for pid in spawned:
                assert not _pid_alive(pid)
        finally:
            for pid in spawned:
                _ensure_killed(pid)

    def test_worker_should_exit_gracefully_when_sigtermed(self):
        """Test a worker turns SIGTERM into a graceful stop.

        Given:
            A started real WorkerProcess.
        When:
            SIGTERM is sent to the worker process.
        Then:
            It should exit with exit code 0 within a bounded join, i.e.,
            the installed handler's graceful stop rather than the
            signal's default disposition.
        """
        # Arrange
        process = WorkerProcess(shutdown_grace_period=5.0)
        process.start(timeout=30)
        pid = process.pid
        assert pid is not None
        assert _pid_alive(pid)

        try:
            # Act
            os.kill(pid, signal.SIGTERM)
            process.join(timeout=15)

            # Assert
            assert process.exitcode == 0
        finally:
            _ensure_killed(pid)

    def test_reap_should_terminate_worker_without_stop_rpc(self):
        """Test reap force-terminates a worker that was never stopped.

        Given:
            A started real WorkerProcess that never receives a stop
            RPC.
        When:
            reap() is called with a short timeout.
        Then:
            It should return with the worker subprocess terminated.
        """
        # Arrange
        process = WorkerProcess(shutdown_grace_period=5.0)
        process.start(timeout=30)
        pid = process.pid
        assert pid is not None
        assert _pid_alive(pid)

        # Act
        try:
            process.reap(timeout=0.5)

            # Assert
            assert not _pid_alive(pid)
        finally:
            _ensure_killed(pid)

    @pytest.mark.asyncio
    async def test_stop_should_kill_unresponsive_worker(self):
        """Test stop force-kills a worker that cannot respond.

        Given:
            A started LocalWorker whose subprocess is suspended with
            SIGSTOP, so it can neither serve the stop RPC nor honor
            SIGTERM.
        When:
            stop() is awaited with a short timeout.
        Then:
            It should surface the RPC deadline within a bound and
            leave the worker subprocess killed, rather than hang on
            the deadline-less stop RPC.
        """
        # Arrange
        worker = LocalWorker(shutdown_grace_period=5.0)
        await worker.start(timeout=30)
        assert worker.metadata is not None
        pid = worker.metadata.pid
        assert _pid_alive(pid)

        try:
            os.kill(pid, signal.SIGSTOP)

            # Act
            with pytest.raises(grpc.aio.AioRpcError) as excinfo:
                await asyncio.wait_for(worker.stop(timeout=0.5), timeout=30)

            # Assert
            assert excinfo.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
            assert not _pid_alive(pid)
        finally:
            _ensure_killed(pid)

    @pytest.mark.asyncio
    async def test___aexit___should_reap_workers_when_drop_announcement_fails(
        self, caplog
    ):
        """Test a pool reaps its workers despite a broken publisher.

        Given:
            A pool of two workers on a real LocalDiscovery whose
            worker-dropped announcement raises PermissionError, a
            discovery failure unrelated to segment ownership
        When:
            The async-with block exits
        Then:
            It should leave no worker subprocess alive and report one
            announcement failure per worker, rather than abandoning the
            stops behind the failed announcements
        """
        # Arrange
        namespace = f"drop-fails-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}
        pids: list[int] = []

        try:
            # Act
            with caplog.at_level(logging.ERROR, "wool.runtime.worker.pool"):
                with LocalDiscovery(namespace) as discovery:
                    async with asyncio.timeout(30):
                        async with WorkerPool(
                            spawn=2,
                            shutdown_timeout=10.0,
                            discovery=_DirectDiscovery(
                                discovery,
                                _BrokenDropPublisher(
                                    discovery.publisher,
                                    PermissionError("discovery is not writable"),
                                ),
                            ),
                        ):
                            pids.extend(
                                child.pid
                                for child in multiprocessing.active_children()
                                if child.pid not in before
                            )

            # Assert — join any finished children first so an
            # exited-but-unreaped worker cannot masquerade as alive
            # under os.kill(pid, 0)
            multiprocessing.active_children()
            assert len(pids) == 2
            for pid in pids:
                assert not _pid_alive(pid)
            errors = [
                r.getMessage() for r in caplog.records if r.levelno == logging.ERROR
            ]
            assert sum("announce" in message for message in errors) == 2
        finally:
            # The Act belongs inside this `try`: this test constructs the
            # scenario where stragglers exist, so an Act that raises is
            # exactly when cleanup matters most.
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)
            for pid in pids:
                _ensure_killed(pid)

    @pytest.mark.asyncio
    async def test___aexit___should_reap_worker_when_drop_announcement_hangs(self):
        """Test a pool reaps its worker despite a wedged publisher.

        Given:
            A pool on a real LocalDiscovery whose worker-dropped
            announcement never returns, as an unresponsive discovery
            service leaves it
        When:
            The async-with block exits and the shutdown deadline
            cancels the pending announcement
        Then:
            It should leave no worker subprocess alive, rather than
            abandoning the stop inside the cancelled announcement
        """
        # Arrange
        namespace = f"drop-hangs-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}
        pids: list[int] = []

        try:
            # Act
            with LocalDiscovery(namespace) as discovery:
                async with asyncio.timeout(30):
                    async with WorkerPool(
                        spawn=1,
                        shutdown_timeout=5.0,
                        discovery=_DirectDiscovery(
                            discovery,
                            _BrokenDropPublisher(discovery.publisher, _HANG),
                        ),
                    ):
                        pids.extend(
                            child.pid
                            for child in multiprocessing.active_children()
                            if child.pid not in before
                        )

            # Assert — join any finished children first so an
            # exited-but-unreaped worker cannot masquerade as alive
            # under os.kill(pid, 0)
            multiprocessing.active_children()
            assert len(pids) == 1
            for pid in pids:
                assert not _pid_alive(pid)
        finally:
            # The Act belongs inside this `try`: if the fix regresses and
            # the hang wedges teardown until `asyncio.timeout(30)` fires,
            # the Act raises and live workers would otherwise escape.
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)
            for pid in pids:
                _ensure_killed(pid)


class _BrokenDropPublisher:
    """Wraps a real publisher, breaking only ``worker-dropped``.

    Every other event, and the publisher's context-manager protocol,
    delegate to the wrapped publisher: `LocalDiscovery.Publisher` is an
    async context manager, and hiding that would send
    ``WorkerPool._enter_context`` down the passthrough path, leaving the
    real publisher neither entered nor exited and teardown's publisher
    cleanup a no-op. Passing `_HANG` hangs the announcement instead of
    raising it, modelling an unresponsive discovery service.
    """

    def __init__(self, publisher, error):
        self._publisher = publisher
        self._error = error
        self.bind_host = publisher.bind_host

    async def __aenter__(self):
        await self._publisher.__aenter__()
        return self

    async def __aexit__(self, *args):
        return await self._publisher.__aexit__(*args)

    async def publish(self, type, metadata):
        if type == "worker-dropped":
            # `_HANG` never returns; the shutdown deadline ends the wait.
            if self._error is _HANG:
                await asyncio.Event().wait()
            raise self._error
        return await self._publisher.publish(type, metadata)


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
    """Best-effort SIGKILL so a failing run cannot leak the orphan under test."""
    if pid is not None and _pid_alive(pid):
        with contextlib.suppress(OSError):
            os.kill(pid, signal.SIGKILL)
