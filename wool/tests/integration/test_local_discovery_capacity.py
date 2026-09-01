"""End-to-end tests for LocalDiscovery capacity enforcement.

These are targeted standalone tests rather than pairwise scenarios:
capacity **exhaustion** deliberately under-provisions the discovery
segment so a worker announcement fails, aborting pool entry. That is a
dispatch *failure*, which would break the pairwise array's single
dispatch-**success** oracle (`test_dispatch_pairwise`). The
capacity-bounded happy path — where capacity comfortably admits the
spawned workers — is exercised through the pairwise array via the
`DiscoveryFactory.LOCAL_CAPACITY_BOUNDED` shape. Unit-level coverage of
the cap lives in ``tests/runtime/discovery/test_local.py``.
"""

import asyncio
import multiprocessing
import uuid

import pytest

from wool import protocol
from wool.runtime.discovery.exceptions import DiscoveryCapacityExhausted
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.metadata import WorkerMetadata
from wool.runtime.worker.pool import WorkerPool

from . import routines
from .conftest import _TIMEOUT
from .conftest import _ensure_killed
from .conftest import _iter_leaf_exceptions


@pytest.mark.integration
class TestLocalDiscoveryCapacity:
    @pytest.mark.asyncio
    async def test___aenter___should_raise_when_spawn_exceeds_capacity(self):
        """Test pool entry fails when spawned workers exceed capacity.

        Given:
            A hybrid WorkerPool spawning more workers than the declared
            LocalDiscovery capacity of one slot
        When:
            The pool is entered so every worker announces itself
        Then:
            It should abort entry with an ExceptionGroup carrying a
            DiscoveryCapacityExhausted, and leave no spawned worker process
            alive.
        """
        # Arrange
        namespace = f"capacity-exhaust-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}

        # Act & assert
        try:
            with pytest.raises(ExceptionGroup) as excinfo:
                async with asyncio.timeout(_TIMEOUT):
                    async with WorkerPool(
                        spawn=3, discovery=LocalDiscovery(namespace, capacity=1)
                    ):
                        pass

            leaves = list(_iter_leaf_exceptions(excinfo.value))
            assert any(
                isinstance(leaf, DiscoveryCapacityExhausted) for leaf in leaves
            ), f"expected a DiscoveryCapacityExhausted, got: {leaves!r}"

            # Join finished children so an exited-but-unreaped worker
            # cannot masquerade as alive under os.kill(pid, 0).
            multiprocessing.active_children()
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
    async def test___aenter___should_admit_all_workers_when_capacity_equals_spawn(
        self, retry_grpc_internal
    ):
        """Test a pool admits every worker when capacity equals its spawn.

        Given:
            A hybrid WorkerPool spawning exactly as many workers as the
            declared LocalDiscovery capacity
        When:
            The pool is entered and a routine is dispatched
        Then:
            It should enter cleanly, admit every worker, return the
            routine result, and leave no worker process alive after exit.
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}

        async def body():
            namespace = f"capacity-ok-{uuid.uuid4().hex[:12]}"
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(
                    spawn=2, discovery=LocalDiscovery(namespace, capacity=2)
                ):
                    assert await routines.add(1, 2) == 3

        # Act & assert
        try:
            await retry_grpc_internal(body)

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
    async def test_publish_should_raise_when_borrower_exceeds_owner_capacity(
        self, retry_grpc_internal
    ):
        """Test the owner's capacity binds every borrower of its registry.

        Given:
            A pool holding a namespace at capacity one, with its single
            spawned worker already registered
        When:
            A publisher borrowing that registry registers a further
            worker
        Then:
            It should raise DiscoveryCapacityExhausted — the owner's
            stamped cap of one governs, and a borrower is offered no
            capacity of its own with which to raise it.
        """
        # Arrange
        before = {child.pid for child in multiprocessing.active_children()}

        async def body():
            namespace = f"capacity-borrower-{uuid.uuid4().hex[:12]}"
            intruder = WorkerMetadata(
                uid=uuid.uuid4(),
                address="127.0.0.1:50051",
                pid=1,
                version=protocol.__version__,
            )
            async with asyncio.timeout(_TIMEOUT):
                async with WorkerPool(
                    spawn=1, discovery=LocalDiscovery(namespace, capacity=1)
                ):
                    assert await routines.add(1, 2) == 3

                    # Act & assert — the sole slot is taken by the
                    # owner's own worker
                    async with LocalDiscovery.Publisher(namespace) as borrower:
                        with pytest.raises(DiscoveryCapacityExhausted):
                            await borrower.publish("worker-added", intruder)

        # Act & assert. That a borrower has no capacity argument at all
        # is a constructor contract, pinned in the unit suite; asserting
        # it here would add a second behaviour to this test and run it
        # against an already-reclaimed registry.
        try:
            await retry_grpc_internal(body)
        finally:
            for child in multiprocessing.active_children():
                if child.pid not in before:
                    _ensure_killed(child.pid)
