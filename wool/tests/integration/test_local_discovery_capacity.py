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

from wool.runtime.discovery.exceptions import DiscoveryCapacityExhausted
from wool.runtime.discovery.local import LocalDiscovery
from wool.runtime.worker.pool import WorkerPool

from . import routines
from .conftest import _ensure_killed

_TIMEOUT = 30


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
        namespace = f"capacity-ok-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}

        async def body():
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
    async def test___aenter___should_raise_when_non_owner_exceeds_owner_capacity(self):
        """Test a pool attaching as a non-owner is bound by the owner's cap.

        Given:
            A pre-entered owner LocalDiscovery holding the namespace at
            capacity one, and a hybrid WorkerPool whose discovery attaches
            to the same namespace as a non-owner declaring capacity 128 and
            spawning two workers
        When:
            The pool is entered so both workers announce themselves
        Then:
            It should abort entry with an ExceptionGroup carrying a
            DiscoveryCapacityExhausted and leave no worker process alive — the
            owner's stamped cap of one governs and the non-owner's declared
            128 is ignored.
        """
        # Arrange — an owner already holds the namespace at capacity one.
        namespace = f"capacity-nonowner-{uuid.uuid4().hex[:12]}"
        before = {child.pid for child in multiprocessing.active_children()}

        # Act & assert
        try:
            with LocalDiscovery(namespace, capacity=1):
                with pytest.raises(ExceptionGroup) as excinfo:
                    async with asyncio.timeout(_TIMEOUT):
                        async with WorkerPool(
                            spawn=2,
                            discovery=LocalDiscovery(namespace, capacity=128),
                        ):
                            pass

                leaves = list(_iter_leaf_exceptions(excinfo.value))
                assert any(
                    isinstance(leaf, DiscoveryCapacityExhausted) for leaf in leaves
                ), f"expected a DiscoveryCapacityExhausted, got: {leaves!r}"

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


def _iter_leaf_exceptions(exc: BaseException):
    """Yield the non-group leaf exceptions of a (possibly nested) group."""
    if isinstance(exc, BaseExceptionGroup):
        for sub in exc.exceptions:
            yield from _iter_leaf_exceptions(sub)
    else:
        yield exc
