"""Probe: a partition stranded by a closed loop retains no subscriber.

Run in its own interpreter because it closes an event loop without
draining it, deliberately stranding a referenced pool entry. That leaves
an un-finalized async generator and a dead partition behind, which the
rest of the suite should not have to run alongside.

`ResourcePool` documents two paths that evict without running
finalizers, and the partition-discard path is one of them. Nothing can
close the source or cancel the fanout's pull task there — there is no
live loop to do it on. What must not happen is unbounded *retention*:
the per-subscriber state once lived in class-level mappings keyed on the
subscriber, whose values referenced their own keys, so an entry dropped
this way was immortal for the life of the process.
"""

from __future__ import annotations

import asyncio
import gc
import sys
import uuid
import weakref

from wool.runtime.discovery.base import DiscoveryEvent
from wool.runtime.discovery.pool import SubscriberMeta
from wool.runtime.discovery.pool import install_subscriber_pool
from wool.runtime.worker.metadata import WorkerMetadata

_built: list[weakref.ReferenceType] = []


def _fail(reason: str) -> None:
    print(f"FAIL: {reason}", file=sys.stderr)
    raise SystemExit(1)


def _event() -> DiscoveryEvent:
    return DiscoveryEvent(
        "worker-added",
        metadata=WorkerMetadata(
            uid=uuid.uuid4(), address="127.0.0.1:50051", pid=1, version="1.0.0"
        ),
    )


class _Endless(metaclass=SubscriberMeta, key=lambda cls, tag: tag):
    def __init__(self, tag: str) -> None:
        self.tag = tag
        _built.append(weakref.ref(self))

    async def _shutdown(self) -> None:
        pass

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        while True:
            yield _event()


def main() -> None:
    install_subscriber_pool()
    shared = _Endless("stranded-key")

    async def hold_open():
        iteration = aiter(shared)
        await anext(iteration)
        # Returned rather than closed, so the entry is still referenced
        # when the loop below closes under it.
        return iteration

    stranding_loop = asyncio.new_event_loop()
    stranded = stranding_loop.run_until_complete(hold_open())
    # Closed without draining: no `shutdown_asyncgens`, so the entry is
    # never released and its finalizer never runs.
    stranding_loop.close()

    if not _built:
        _fail("the probe never built a subscriber")

    async def touch_pool():
        iteration = aiter(shared)
        await anext(iteration)
        await iteration.aclose()

    # Any mutating operation from a live loop discards the dead
    # partition, dropping its entries without finalizing them.
    asyncio.run(touch_pool())

    del stranded
    gc.collect()

    retained = [reference for reference in _built if reference() is not None]
    if retained:
        _fail(
            f"{len(retained)} of {len(_built)} subscribers survived the "
            "discard of the partition that held them"
        )
    print("OK")


if __name__ == "__main__":
    main()
