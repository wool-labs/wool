"""End-to-end tests for gRPC fork-handler suppression in workers (#387).

Targeted standalone tests rather than pairwise scenarios: the claims are
properties of the spawn boundary, invariant across every composition
axis the scenario matrix varies. The unit suite pins what wool puts into
the environment around the spawn; only a real worker can witness that it
arrived, for the import-time reason recorded in `WorkerProcess`.

The two environment tests are the portable guard and are what CI checks.
The stderr pair below them is a macOS-only observable — the suppressed
output comes from gRPC's poll poller, which Linux does not select — so
its anti-vacuity control runs on developer machines only.

What these tests deliberately do not pin is the SIGPIPE kill the issue
reports. That failure is machine-state-dependent, so a test for it would
be silent on a clean machine and flaky on a loaded one. The gRPC atfork
output leaking into a live subprocess's stderr pipe is deterministic,
and is what is pinned here.
"""

import sys

import pytest

import wool

from .routines import probe_subprocess_stderr
from .routines import read_grpc_fork_support

pytestmark = pytest.mark.integration

# The suppressed line gRPC's postfork handler writes into whichever
# descriptor it finds; matching it keeps the control test sensitive to
# gRPC falling silent rather than to any stderr noise at all.
_ATFORK_OUTPUT = "FD from fork parent still in poll list"


class TestWorkerForkSupport:
    @pytest.mark.asyncio
    async def test_start_should_disable_grpc_fork_support_in_the_worker_process(
        self, monkeypatch
    ):
        """Test a spawned worker runs with gRPC fork support disabled.

        Given:
            An environment carrying no GRPC_ENABLE_FORK_SUPPORT setting
        When:
            A routine dispatched to a real spawned worker reads the
            variable from the worker process's own environment
        Then:
            It should report "0"
        """
        # Arrange
        monkeypatch.delenv("GRPC_ENABLE_FORK_SUPPORT", raising=False)

        # Act
        async with wool.WorkerPool(spawn=1):
            setting = await read_grpc_fork_support()

        # Assert
        assert setting == "0"

    @pytest.mark.asyncio
    async def test_start_should_hand_the_worker_the_embedder_s_setting(
        self, monkeypatch
    ):
        """Test an embedder's setting rides the spawn into the worker.

        Given:
            An environment in which the embedder set
            GRPC_ENABLE_FORK_SUPPORT
        When:
            A routine dispatched to a real spawned worker reads the
            variable from the worker process's own environment
        Then:
            It should report that value rather than wool's default
        """
        # Arrange
        monkeypatch.setenv("GRPC_ENABLE_FORK_SUPPORT", "1")

        # Act
        async with wool.WorkerPool(spawn=1):
            setting = await read_grpc_fork_support()

        # Assert
        assert setting == "1"

    @pytest.mark.asyncio
    async def test_start_should_keep_subprocess_stderr_free_of_grpc_output(
        self, monkeypatch
    ):
        """Test subprocesses launched from a routine own their stderr.

        Given:
            A default worker pool, so the worker runs with gRPC's fork
            handlers disabled, and gRPC logging left at info
        When:
            A routine launches subprocesses on the forking launch path
            and captures each one's stderr
        Then:
            Every launch should exit 0 with nothing on stderr
        """
        # Arrange
        monkeypatch.delenv("GRPC_ENABLE_FORK_SUPPORT", raising=False)
        monkeypatch.setenv("GRPC_VERBOSITY", "info")

        # Act
        async with wool.WorkerPool(spawn=1):
            results = await probe_subprocess_stderr()

        # Assert
        assert results
        assert all(result == (0, "") for result in results), results

    @pytest.mark.skipif(
        sys.platform != "darwin",
        reason="the poll poller's atfork logging is the macOS observable (#387)",
    )
    @pytest.mark.asyncio
    async def test_start_should_leak_grpc_output_when_embedder_enables_fork_support(
        self, monkeypatch
    ):
        """Test the suppressed gRPC output is real and still reachable.

        Given:
            An embedder who set GRPC_ENABLE_FORK_SUPPORT=1, so the
            worker keeps gRPC's fork handlers armed, and gRPC logging
            left at info
        When:
            A routine launches subprocesses on the forking launch path
            and captures each one's stderr
        Then:
            At least one launch should carry gRPC's postfork output
        """
        # Arrange
        monkeypatch.setenv("GRPC_ENABLE_FORK_SUPPORT", "1")
        monkeypatch.setenv("GRPC_VERBOSITY", "info")

        # Act
        async with wool.WorkerPool(spawn=1):
            results = await probe_subprocess_stderr()

        # Assert
        # The returncode is deliberately unasserted: armed handlers
        # recreate the descriptor race, so a killed child here is the
        # bug being demonstrated, not a test failure.
        assert any(_ATFORK_OUTPUT in stderr for _, stderr in results), results
