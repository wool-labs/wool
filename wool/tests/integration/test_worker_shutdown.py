"""Integration tests for worker-loop teardown task draining."""

import pytest

from . import routines
from .conftest import build_pool_from_scenario
from .conftest import default_scenario


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
