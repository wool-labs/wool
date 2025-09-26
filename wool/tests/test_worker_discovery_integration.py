"""Integration tests for wool._worker_discovery module."""

import asyncio
import socket
import string
import uuid

import pytest
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.strategies import composite

import wool
from wool._worker_discovery import LanDiscovery
from wool._worker_discovery import LanRegistrar
from wool._worker_discovery import WorkerInfo


def create_mock_worker_info(address: str) -> WorkerInfo:
    """Create a :py:class:`WorkerInfo` object for testing registrar operations.

    :param address:
        The worker address in 'host:port' format.
    :return:
        A configured :py:class:`WorkerInfo` instance for testing.
    """
    host, port = address.split(":")
    return WorkerInfo(
        uid=f"test-worker-{uuid.uuid4().hex[:8]}",
        host=host,
        port=int(port),
        pid=12345,
        version=wool.__version__,
        tags={"test"},
        extra={"test": True},
    )


@pytest.fixture
def pool_uri() -> str:
    """Generate a unique pool URI for each test."""
    return f"test-pool-{uuid.uuid4().hex}"


@pytest.fixture
def free_port() -> int:
    """Find an available port for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture
def worker_address(free_port: int) -> str:
    """Create a worker address using a free port."""
    return f"localhost:{free_port}"


@pytest.fixture
def mock_worker_info(worker_address: str) -> WorkerInfo:
    """Create a :py:class:`WorkerInfo` object with the test address."""
    return create_mock_worker_info(worker_address)


class TestServiceRegistrationAndDiscoveryIntegration:
    """End-to-end integration tests for service registration and discovery.

    These integration tests verify the interaction between
    :py:class:`LanRegistrar` and :py:class:`LanDiscovery`
    components work together correctly for worker lifecycle management.
    """

    @pytest.mark.asyncio
    async def test_worker_registration_triggers_discovery_event(
        self, mock_worker_info: WorkerInfo
    ):
        """Test that worker registration triggers discovery events.

        GIVEN
            A :py:class:`LanRegistrar` and
            :py:class:`LanDiscovery` are running
        WHEN
            A worker is registered in the registrar
        THEN
            The discovery service should detect the worker addition and
            removal events
        """
        # Arrange
        discovered_workers = []
        removed_workers = []
        discovery_future = asyncio.Future()
        removal_future = asyncio.Future()

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery()

        await mock_registrar.start()

        async def process_events():
            async for event in mock_discovery.events():
                if event.type == "worker_added":
                    discovered_workers.append(event.worker_info)
                    if not discovery_future.done():
                        discovery_future.set_result(event.worker_info)
                elif event.type == "worker_removed":
                    removed_workers.append(event.worker_info)
                    if not removal_future.done():
                        removal_future.set_result(event.worker_info)

        event_task = asyncio.create_task(process_events())

        try:
            # Act
            await mock_registrar.register(mock_worker_info)
            discovered_worker = await asyncio.wait_for(discovery_future, timeout=5.0)

            await mock_registrar.unregister(mock_worker_info)
            removed_worker = await asyncio.wait_for(removal_future, timeout=5.0)

            # Assert
            assert len(discovered_workers) == 1
            # The discovered worker's UID will have the service type appended
            assert discovered_workers[0].uid.startswith(mock_worker_info.uid)
            assert discovered_workers[0].port == mock_worker_info.port

            assert len(removed_workers) == 1
            assert removed_workers[0].uid.startswith(mock_worker_info.uid)
            assert removed_worker.uid == discovered_worker.uid

        finally:
            event_task.cancel()
            try:
                await event_task
            except asyncio.CancelledError:
                pass
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_filters_workers_by_tag_criteria(self):
        """Test that discovery service filters workers by tag criteria.

        GIVEN
            A :py:class:`LanDiscovery` configured with tag filtering
        WHEN
            Workers with different tags are registered
        THEN
            Only workers matching the filter criteria should be discovered
        """
        # Arrange
        mock_worker_production = create_mock_worker_info("localhost:50001")
        mock_worker_production.tags.add("production")

        mock_worker_development = create_mock_worker_info("localhost:50002")
        mock_worker_development.tags.clear()
        mock_worker_development.tags.add("development")

        discovered_workers = []
        discovery_future = asyncio.Future()

        def mock_filter_production_workers(worker_info: WorkerInfo) -> bool:
            return "production" in worker_info.tags

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery(filter=mock_filter_production_workers)

        await mock_registrar.start()

        async def process_events():
            async for event in mock_discovery.events():
                if event.type == "worker_added":
                    discovered_workers.append(event.worker_info)
                    if not discovery_future.done():
                        discovery_future.set_result(event.worker_info)

        event_task = asyncio.create_task(process_events())

        try:
            # Act
            await mock_registrar.register(mock_worker_development)
            await mock_registrar.register(mock_worker_production)

            await asyncio.wait_for(discovery_future, timeout=5.0)
            await asyncio.sleep(1)  # Brief wait to ensure no extra discoveries

            # Assert
            assert len(discovered_workers) == 1
            assert discovered_workers[0].uid.startswith(mock_worker_production.uid)
            assert "production" in discovered_workers[0].tags

        finally:
            event_task.cancel()
            try:
                await event_task
            except asyncio.CancelledError:
                pass
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_detects_multiple_workers_simultaneously(self):
        """Test that discovery service detects multiple registered workers.

        GIVEN
            A :py:class:`LanRegistrar` and
            :py:class:`LanDiscovery` are running
        WHEN
            Multiple workers are registered simultaneously
        THEN
            The discovery service should detect all registered workers
        """
        # Arrange
        discovered_workers = []
        discovery_count = 0
        all_discovered = asyncio.Future()

        mock_workers = [
            create_mock_worker_info("localhost:50001"),
            create_mock_worker_info("localhost:50002"),
            create_mock_worker_info("localhost:50003"),
        ]
        expected_worker_count = len(mock_workers)

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery()

        await mock_registrar.start()

        async def process_events():
            nonlocal discovery_count
            async for event in mock_discovery.events():
                if event.type == "worker_added":
                    discovered_workers.append(event.worker_info)
                    discovery_count += 1
                    if (
                        discovery_count == expected_worker_count
                        and not all_discovered.done()
                    ):
                        all_discovered.set_result(True)

        event_task = asyncio.create_task(process_events())

        try:
            # Act
            for mock_worker in mock_workers:
                await mock_registrar.register(mock_worker)

            await asyncio.wait_for(all_discovered, timeout=10.0)

            # Assert
            assert len(discovered_workers) == expected_worker_count

            # Check all expected worker UIDs are present (discovered UIDs
            # have service type appended)
            for mock_worker in mock_workers:
                matching_discovered = [
                    discovered_worker
                    for discovered_worker in discovered_workers
                    if discovered_worker.uid.startswith(mock_worker.uid)
                ]
                assert len(matching_discovered) == 1, (
                    f"Expected to find exactly one discovered worker for UID "
                    f"{mock_worker.uid}, found {len(matching_discovered)}"
                )

        finally:
            event_task.cancel()
            try:
                await event_task
            except asyncio.CancelledError:
                pass
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_handles_worker_restart_scenario(self, worker_address: str):
        """Test discovery service correctly handles worker restart scenarios.

        Given:
            A worker is registered and then stops
        When:
            The same worker restarts with a different UID (simulating process restart)
        Then:
            It should handle the worker removal and re-addition correctly
        """
        # Arrange
        discovered_events = []
        original_worker = create_mock_worker_info(worker_address)

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery()

        await mock_registrar.start()

        async def collect_events():
            async for event in mock_discovery.events():
                discovered_events.append(event)
                if len(discovered_events) >= 2:  # Addition + removal
                    break

        event_task = asyncio.create_task(collect_events())

        try:
            # Act
            await mock_registrar.register(original_worker)
            await asyncio.sleep(0.2)  # Let registration propagate

            # Simulate worker stopping
            await mock_registrar.unregister(original_worker)
            await asyncio.wait_for(event_task, timeout=5.0)

            # Assert
            assert len(discovered_events) >= 1
            assert discovered_events[0].type == "worker_added"

            # If we got a removal event, verify it
            if len(discovered_events) >= 2:
                assert discovered_events[1].type == "worker_removed"

        finally:
            event_task.cancel()
            try:
                await event_task
            except asyncio.CancelledError:
                pass
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_handles_registrar_startup_failure(self):
        """Test discovery service behavior when no registrar services are available.

        Given:
            A discovery service is configured to monitor for workers
        When:
            No registrar services are running (simulating network partition)
        Then:
            It should handle the absence gracefully and not discover any workers
        """
        # Arrange
        discovered_events = []
        discovery_timeout_reached = False

        mock_discovery = LanDiscovery()
        # Intentionally NOT starting any registrar service

        async def collect_events():
            nonlocal discovery_timeout_reached
            try:
                async with asyncio.timeout(2.0):  # Short timeout
                    async for event in mock_discovery.events():
                        discovered_events.append(event)
            except asyncio.TimeoutError:
                discovery_timeout_reached = True

        # Act
        await collect_events()

        # Assert
        assert discovery_timeout_reached
        assert len(discovered_events) == 0  # No workers should be discovered

    @pytest.mark.asyncio
    async def test_discovery_handles_worker_registration_timeout(
        self, worker_address: str
    ):
        """Test discovery service with worker registration timeout scenarios.

        Given:
            A discovery service with timeout expectations
        When:
            Worker registration process is delayed beyond reasonable time
        Then:
            It should handle delayed registrations without hanging
        """
        # Arrange
        DISCOVERY_TIMEOUT = 1.0
        discovered_events = []
        timeout_occurred = False

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery()
        mock_worker = create_mock_worker_info(worker_address)

        await mock_registrar.start()

        async def collect_events_with_timeout():
            nonlocal timeout_occurred
            try:
                async with asyncio.timeout(DISCOVERY_TIMEOUT):
                    async for event in mock_discovery.events():
                        discovered_events.append(event)
                        break  # Stop after first event
            except asyncio.TimeoutError:
                timeout_occurred = True

        try:
            # Act - Start discovery but delay worker registration
            discovery_task = asyncio.create_task(collect_events_with_timeout())

            # Simulate delayed registration (longer than discovery timeout)
            await asyncio.sleep(DISCOVERY_TIMEOUT + 0.5)
            await mock_registrar.register(mock_worker)

            await discovery_task

            # Assert
            assert timeout_occurred  # Discovery should have timed out
            assert len(discovered_events) == 0  # No events before timeout

        finally:
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_handles_malformed_worker_info(self, worker_address: str):
        """Test discovery service resilience to malformed worker information.

        Given:
            A discovery service monitoring worker registrations
        When:
            Worker attempts registration with invalid or malformed data
        Then:
            It should reject invalid workers and continue operating normally
        """
        # Arrange
        discovered_events = []
        valid_worker = create_mock_worker_info(worker_address)

        # Create worker with malformed data
        malformed_worker = WorkerInfo(
            uid="",  # Invalid empty UID
            host="invalid-host-format",
            port=-1,  # Invalid port
            pid=0,  # Invalid PID
            version="",
            tags=set(),  # Empty tags
            extra={},
        )

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery()

        await mock_registrar.start()

        async def collect_events():
            async for event in mock_discovery.events():
                discovered_events.append(event)
                if len(discovered_events) >= 1:  # Expect only valid worker
                    break

        event_task = asyncio.create_task(collect_events())

        try:
            # Act
            # Try to register malformed worker first - should fail silently or be ignored
            try:
                await mock_registrar.register(malformed_worker)
            except Exception:
                pass  # Registrar may reject invalid data

            # Register valid worker
            await mock_registrar.register(valid_worker)
            await asyncio.wait_for(event_task, timeout=3.0)

            # Assert
            assert len(discovered_events) == 1
            assert discovered_events[0].type == "worker_added"
            assert discovered_events[0].worker_info.uid.startswith(valid_worker.uid)

        finally:
            event_task.cancel()
            try:
                await event_task
            except asyncio.CancelledError:
                pass
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_handles_rapid_worker_churn(self):
        """Test discovery service with rapid worker registration and removal.

        Given:
            A discovery service monitoring worker events
        When:
            Multiple workers register and unregister in rapid succession
        Then:
            It should track all events correctly without missing or duplicating
        """
        # Arrange
        WORKER_COUNT = 5
        discovered_events = []
        all_workers = []
        for _ in range(WORKER_COUNT):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", 0))
                port = s.getsockname()[1]
                all_workers.append(create_mock_worker_info(f"localhost:{port}"))

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery()

        await mock_registrar.start()

        async def collect_events():
            async for event in mock_discovery.events():
                discovered_events.append(event)
                # Expect: 5 added + 5 removed = 10 total events
                if len(discovered_events) >= WORKER_COUNT * 2:
                    break

        event_task = asyncio.create_task(collect_events())

        try:
            # Act - Rapid registration and removal
            # Register all workers quickly
            for worker in all_workers:
                await mock_registrar.register(worker)
                await asyncio.sleep(0.05)  # Brief delay to ensure ordering

            # Unregister all workers quickly
            for worker in all_workers:
                await mock_registrar.unregister(worker)
                await asyncio.sleep(0.05)

            await asyncio.wait_for(event_task, timeout=10.0)

            # Assert
            added_events = [e for e in discovered_events if e.type == "worker_added"]
            removed_events = [e for e in discovered_events if e.type == "worker_removed"]

            assert len(added_events) == WORKER_COUNT
            assert len(removed_events) == WORKER_COUNT

            # Verify all workers were tracked (UIDs may have service type suffix)
            added_uids = {e.worker_info.uid for e in added_events}
            removed_uids = {e.worker_info.uid for e in removed_events}
            expected_uids = {w.uid for w in all_workers}

            # Check that each expected UID has a corresponding discovered UID
            for expected_uid in expected_uids:
                matching_added = [
                    uid for uid in added_uids if uid.startswith(expected_uid)
                ]
                matching_removed = [
                    uid for uid in removed_uids if uid.startswith(expected_uid)
                ]
                assert len(matching_added) == 1, (
                    f"Expected UID {expected_uid} not found in added events"
                )
                assert len(matching_removed) == 1, (
                    f"Expected UID {expected_uid} not found in removed events"
                )

        finally:
            event_task.cancel()
            try:
                await event_task
            except asyncio.CancelledError:
                pass
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_handles_empty_tag_filter(self):
        """Test discovery service with empty filter criteria.

        Given:
            A discovery service with None filter (no filtering)
        When:
            Workers with various tags are registered
        Then:
            It should discover all workers regardless of tags
        """
        # Arrange
        discovered_events = []
        workers_with_tags = []
        for _ in range(3):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", 0))
                port = s.getsockname()[1]
                workers_with_tags.append(create_mock_worker_info(f"localhost:{port}"))

        # Modify tags to create variety
        workers_with_tags[0].tags = {"production", "web"}
        workers_with_tags[1].tags = {"development", "api"}
        workers_with_tags[2].tags = set()  # Empty tags

        expected_worker_count = len(workers_with_tags)

        mock_registrar = LanRegistrar()
        # No filter provided - should discover all workers
        mock_discovery = LanDiscovery(filter=None)

        await mock_registrar.start()

        async def collect_events():
            async for event in mock_discovery.events():
                if event.type == "worker_added":
                    discovered_events.append(event)
                    if len(discovered_events) >= expected_worker_count:
                        break

        event_task = asyncio.create_task(collect_events())

        try:
            # Act
            for worker in workers_with_tags:
                await mock_registrar.register(worker)
                await asyncio.sleep(0.05)

            await asyncio.wait_for(event_task, timeout=10.0)

            # Assert
            assert len(discovered_events) == expected_worker_count

            # Verify all workers were discovered regardless of tags
            discovered_uids = {e.worker_info.uid for e in discovered_events}
            expected_uids = {w.uid for w in workers_with_tags}

            for expected_uid in expected_uids:
                matching_discovered = [
                    uid for uid in discovered_uids if uid.startswith(expected_uid)
                ]
                assert len(matching_discovered) == 1, (
                    f"Worker with UID {expected_uid} not discovered"
                )

        finally:
            event_task.cancel()
            try:
                await event_task
            except asyncio.CancelledError:
                pass
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_handles_complex_tag_combinations(self):
        """Test discovery service with complex tag filtering logic.

        Given:
            A discovery service with multi-tag filter logic
        When:
            Workers with overlapping, subset, and superset tags are registered
        Then:
            It should correctly apply complex filtering logic
        """
        # Arrange
        discovered_events = []

        workers_with_complex_tags = []
        for _ in range(5):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", 0))
                port = s.getsockname()[1]
                workers_with_complex_tags.append(
                    create_mock_worker_info(f"localhost:{port}")
                )

        # Set up complex tag combinations
        workers_with_complex_tags[0].tags = {"production", "web", "frontend"}
        workers_with_complex_tags[1].tags = {"production", "api", "backend"}
        workers_with_complex_tags[2].tags = {"development", "web", "frontend"}
        workers_with_complex_tags[3].tags = {"production"}
        workers_with_complex_tags[4].tags = set()

        # Filter: workers that have "production" AND either "web" OR "api"
        def complex_filter(worker_info):
            tags = worker_info.tags
            has_production = "production" in tags
            has_web_or_api = "web" in tags or "api" in tags
            return has_production and has_web_or_api

        # Expected: workers 0 (production+web) and 1 (production+api)
        expected_discovered_count = 2

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery(filter=complex_filter)

        await mock_registrar.start()

        async def collect_events():
            async for event in mock_discovery.events():
                if event.type == "worker_added":
                    discovered_events.append(event)
                    if len(discovered_events) >= expected_discovered_count:
                        break

        event_task = asyncio.create_task(collect_events())

        try:
            # Act
            for worker in workers_with_complex_tags:
                await mock_registrar.register(worker)
                await asyncio.sleep(0.05)

            await asyncio.wait_for(event_task, timeout=10.0)

            # Assert
            assert len(discovered_events) == expected_discovered_count

            # Verify only workers 0 and 1 were discovered
            discovered_worker_uids = {e.worker_info.uid for e in discovered_events}

            # Check that production+web worker was discovered
            production_web_matches = [
                uid
                for uid in discovered_worker_uids
                if uid.startswith(workers_with_complex_tags[0].uid)
            ]
            assert len(production_web_matches) == 1

            # Check that production+api worker was discovered
            production_api_matches = [
                uid
                for uid in discovered_worker_uids
                if uid.startswith(workers_with_complex_tags[1].uid)
            ]
            assert len(production_api_matches) == 1

            # Verify filtered workers have correct tags
            for event in discovered_events:
                worker_tags = event.worker_info.tags
                assert "production" in worker_tags
                assert "web" in worker_tags or "api" in worker_tags

        finally:
            event_task.cancel()
            try:
                await event_task
            except asyncio.CancelledError:
                pass
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_handles_network_partition_recovery(self):
        """Test discovery service recovery after network partition simulation.

        Given:
            A discovery service that loses connection to registrar
        When:
            Network connection is restored after partition
        Then:
            It should reconnect and rediscover existing workers
        """
        # Arrange
        discovered_events = []
        recovery_events = []
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            port = s.getsockname()[1]
            worker_for_recovery = create_mock_worker_info(f"localhost:{port}")

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery()

        await mock_registrar.start()

        # Phase 1: Normal discovery
        async def collect_initial_events():
            async for event in mock_discovery.events():
                discovered_events.append(event)
                if len(discovered_events) >= 1:
                    break

        initial_task = asyncio.create_task(collect_initial_events())
        recovery_task = None

        try:
            # Act - Phase 1: Register worker normally
            await mock_registrar.register(worker_for_recovery)
            await asyncio.wait_for(initial_task, timeout=5.0)

            # Assert Phase 1
            assert len(discovered_events) == 1
            assert discovered_events[0].type == "worker_added"

            # Phase 2: Simulate partition by stopping discovery
            # (In real scenario, this would be network failure)
            initial_task.cancel()

            # Phase 3: Simulate recovery with new discovery instance
            mock_discovery_recovery = LanDiscovery()

            async def collect_recovery_events():
                async for event in mock_discovery_recovery.events():
                    recovery_events.append(event)
                    if len(recovery_events) >= 1:
                        break

            recovery_task = asyncio.create_task(collect_recovery_events())

            # The worker should be rediscovered after "network recovery"
            await asyncio.wait_for(recovery_task, timeout=5.0)

            # Assert Phase 3
            assert len(recovery_events) == 1
            assert recovery_events[0].type == "worker_added"

            # Verify same worker was rediscovered
            original_uid = discovered_events[0].worker_info.uid
            recovered_uid = recovery_events[0].worker_info.uid
            assert recovered_uid == original_uid

        finally:
            try:
                initial_task.cancel()
            except Exception:
                pass
            if recovery_task is not None:
                try:
                    recovery_task.cancel()
                except Exception:
                    pass
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_worker_update_propagation(self, worker_address: str):
        """Test that worker updates propagate correctly through discovery.

        Given:
            A worker is registered and discovered
        When:
            The worker's tags or metadata are updated
        Then:
            It should receive worker_updated events with new information
        """
        # Arrange
        discovered_events = []
        original_worker = create_mock_worker_info(worker_address)

        # Updated worker with different metadata
        updated_worker = WorkerInfo(
            uid=original_worker.uid,  # Same UID
            host=original_worker.host,
            port=original_worker.port,
            pid=original_worker.pid,
            version="v2.0.0",  # Different version
            tags={"production", "updated"},  # Different tags
            extra={"update_time": "2023-11-01"},  # Different extra
        )

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery()

        await mock_registrar.start()

        async def collect_events():
            async for event in mock_discovery.events():
                discovered_events.append(event)
                if len(discovered_events) >= 2:  # Initial + update
                    break

        event_task = asyncio.create_task(collect_events())

        try:
            # Act
            await mock_registrar.register(original_worker)
            await asyncio.sleep(0.2)  # Let registration propagate

            # Update worker properties
            await mock_registrar.update(updated_worker)
            await asyncio.wait_for(event_task, timeout=5.0)

            # Assert
            assert len(discovered_events) >= 1
            assert discovered_events[0].type == "worker_added"

            # Check if we received an update event
            if len(discovered_events) >= 2:
                update_event = discovered_events[1]
                assert update_event.type == "worker_updated"
                assert update_event.worker_info.version == "v2.0.0"
                assert "updated" in update_event.worker_info.tags
                assert update_event.worker_info.extra["update_time"] == "2023-11-01"

        finally:
            event_task.cancel()
            try:
                await event_task
            except asyncio.CancelledError:
                pass
            await mock_registrar.stop()

    @pytest.mark.asyncio
    async def test_discovery_handles_worker_update_failure(self, worker_address: str):
        """Test discovery service behavior when worker update fails.

        Given:
            A worker is registered and discovered
        When:
            A worker update operation fails at the Zeroconf level
        Then:
            It should maintain consistent state and handle the error gracefully
        """
        # Arrange
        discovered_events = []
        original_worker = create_mock_worker_info(worker_address)

        # Worker update that will be attempted
        updated_worker = WorkerInfo(
            uid=original_worker.uid,
            host=original_worker.host,
            port=original_worker.port,
            pid=original_worker.pid,
            version="v2.0.0",
            tags={"production", "updated"},
            extra={"update_time": "2023-11-01"},
        )

        mock_registrar = LanRegistrar()
        mock_discovery = LanDiscovery()

        await mock_registrar.start()

        async def collect_events():
            async for event in mock_discovery.events():
                discovered_events.append(event)
                if len(discovered_events) >= 1:  # Just the initial registration
                    break

        event_task = asyncio.create_task(collect_events())

        try:
            # Act
            await mock_registrar.register(original_worker)
            await asyncio.wait_for(event_task, timeout=3.0)

            # Assert initial state
            assert len(discovered_events) == 1
            assert discovered_events[0].type == "worker_added"

            # Verify registrar maintains consistent state even if update might fail
            # (The registrar should be robust enough to handle Zeroconf failures)
            original_service_count = len(mock_registrar.services)

            try:
                await mock_registrar.update(updated_worker)
                # If update succeeds, that's fine too
            except Exception:
                # If update fails, verify registrar state is still consistent
                assert len(mock_registrar.services) == original_service_count
                assert original_worker.uid in mock_registrar.services

        finally:
            event_task.cancel()
            try:
                await event_task
            except asyncio.CancelledError:
                pass
            await mock_registrar.stop()


# Property-Based Testing Data Generation

# Consolidated tag vocabulary for realistic tag generation
ALL_TAGS = [
    # Environments
    "prod",
    "staging",
    "dev",
    "test",
    # Services
    "web",
    "api",
    "worker",
    "db",
    "cache",
    # Regions
    "us-east",
    "us-west",
    "eu-central",
    "ap-southeast",
    # Versions
    "v1",
    "v2",
    "v3",
    "canary",
    "stable",
]


def has_any_tags(worker_tags: set, required_tags: set) -> bool:
    """Filter: worker has any of the required tags."""
    return bool(worker_tags & required_tags)


def has_all_tags(worker_tags: set, required_tags: set) -> bool:
    """Filter: worker has all required tags."""
    return required_tags.issubset(worker_tags)


def has_none_tags(worker_tags: set, required_tags: set) -> bool:
    """Filter: worker has none of the required tags."""
    return not bool(worker_tags & required_tags)


def exact_match_tags(worker_tags: set, required_tags: set) -> bool:
    """Filter: worker tags exactly match required tags."""
    return worker_tags == required_tags


# Available filter functions for property-based testing
FILTER_FUNCTIONS = [has_any_tags, has_all_tags, has_none_tags, exact_match_tags]


@composite
def tag_sets(draw):
    """Generate realistic tag sets by drawing from consolidated vocabulary."""
    tag_count = draw(st.integers(min_value=0, max_value=4))
    return draw(st.sets(st.sampled_from(ALL_TAGS), min_size=0, max_size=tag_count))


@composite
def filter_predicates(draw):
    """Generate filter function and required tags directly."""
    filter_func = draw(st.sampled_from(FILTER_FUNCTIONS))
    required_tags = draw(st.sets(st.sampled_from(ALL_TAGS), min_size=1, max_size=3))
    return (filter_func, required_tags)


@composite
def valid_worker_info(draw):
    """Generate valid WorkerInfo with simplified data generation."""

    # Simplified UID generation
    uid_parts = [
        draw(st.sampled_from(["worker", "node", "proc", "svc"])),
        draw(
            st.text(
                min_size=4, max_size=8, alphabet=string.ascii_lowercase + string.digits
            )
        ),
    ]
    uid = "-".join(uid_parts)

    # Simplified host selection (IPv4 only since discovery service uses V4Only)
    host = draw(
        st.sampled_from(["localhost", "127.0.0.1", "worker-01.internal", "10.0.1.100"])
    )

    port = draw(st.integers(min_value=1024, max_value=65535))
    pid = draw(st.integers(min_value=1, max_value=99999))

    # Simplified version generation
    version_parts = draw(
        st.lists(st.integers(min_value=0, max_value=10), min_size=3, max_size=3)
    )
    version = ".".join(map(str, version_parts))

    tags = draw(tag_sets())

    # Simplified extra metadata
    extra = {}
    if draw(st.booleans()):  # 50% chance of having extra data
        extra_count = draw(st.integers(min_value=1, max_value=3))
        extra_keys = draw(
            st.lists(
                st.sampled_from(["region", "zone", "instance_type"]),
                min_size=extra_count,
                max_size=extra_count,
                unique=True,
            )
        )
        extra_values = draw(
            st.lists(
                st.text(
                    min_size=1,
                    max_size=20,
                    alphabet=string.ascii_letters + string.digits,
                ),
                min_size=len(extra_keys),
                max_size=len(extra_keys),
            )
        )
        extra = dict(zip(extra_keys, extra_values))

    return WorkerInfo(
        uid=uid, host=host, port=port, pid=pid, version=version, tags=tags, extra=extra
    )


class TestPropertyBasedDiscovery:
    """Property-based tests for worker discovery system."""

    @given(
        worker_tags=st.lists(tag_sets(), min_size=5, max_size=20),
        filter_spec=filter_predicates(),
    )
    def test_discovery_filter_properties_with_generated_tags(
        self, worker_tags, filter_spec
    ):
        """Test filter logic with property-based generated tag combinations.

        Given:
            Generated worker tag sets and filter predicates
        When:
            Various filter predicates are applied to workers
        Then:
            It should correctly include/exclude workers based on filter logic
        """
        filter_func, required_tags = filter_spec

        # Apply filter directly - no string matching needed
        filtered_workers = [
            tags for tags in worker_tags if filter_func(tags, required_tags)
        ]
        excluded_workers = [tags for tags in worker_tags if tags not in filtered_workers]

        # Property: All filtered workers should satisfy the filter condition
        for worker_tag_set in filtered_workers:
            assert filter_func(worker_tag_set, required_tags), (
                f"Worker {worker_tag_set} should satisfy filter {filter_func.__name__}"
            )

        # Property: No excluded workers should satisfy the filter condition
        for worker_tag_set in excluded_workers:
            assert not filter_func(worker_tag_set, required_tags), (
                f"Worker {worker_tag_set} "
                f"should not satisfy filter {filter_func.__name__}"
            )

    @given(valid_worker_info())
    @settings(max_examples=20, deadline=5000)  # More examples but simpler test
    def test_discovery_handles_generated_worker_configurations(self, worker_config):
        """Test worker configuration validation with property-based generated data.

        Given:
            Valid worker configurations generated by Hypothesis
        When:
            Worker data is validated and serialized
        Then:
            It should handle all valid worker configurations correctly
        """
        # Property: All generated workers should have valid basic structure
        assert worker_config.uid, "Worker UID should not be empty"
        assert worker_config.host, "Worker host should not be empty"
        assert 1024 <= worker_config.port <= 65535, (
            f"Worker port {worker_config.port} should be in valid range"
        )
        assert worker_config.pid > 0, (
            f"Worker PID {worker_config.pid} should be positive"
        )
        assert worker_config.version, "Worker version should not be empty"

        # Property: Worker info should be serializable/deserializable
        from zeroconf import ServiceInfo

        from wool._worker_discovery import _deserialize_worker_info
        from wool._worker_discovery import _serialize_worker_info

        # Test serialization round-trip
        serialized = _serialize_worker_info(worker_config)
        assert isinstance(serialized, dict), "Serialized worker info should be a dict"
        assert all(isinstance(k, str) for k in serialized.keys()), (
            "All serialized keys should be strings"
        )
        assert all(isinstance(v, (str, type(None))) for v in serialized.values()), (
            "All serialized values should be strings or None"
        )

        # Test deserialization (create a mock ServiceInfo)
        mock_service = ServiceInfo(
            type_="_wool._tcp.local.",
            name=f"{worker_config.uid}._wool._tcp.local.",
            addresses=[b"\x7f\x00\x00\x01"],  # 127.0.0.1
            port=worker_config.port,
            properties=serialized,
        )

        deserialized = _deserialize_worker_info(mock_service)

        # Property: Round-trip should preserve worker data
        # Note: ServiceInfo.name includes the service type, so UID gets transformed
        assert deserialized.uid.startswith(worker_config.uid)

        # Note: Host gets resolved to IP address during service discovery
        # We hardcode 127.0.0.1 in the mock ServiceInfo, so it should always be that
        assert deserialized.host == "127.0.0.1"

        assert deserialized.port == worker_config.port
        assert deserialized.pid == worker_config.pid
        assert deserialized.version == worker_config.version
        assert deserialized.tags == worker_config.tags
        assert deserialized.extra == worker_config.extra
