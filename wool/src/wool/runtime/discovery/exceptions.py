"""The discovery subsystem's exceptions.

Single home for the typed errors the discovery backends raise.
"""

from __future__ import annotations

from uuid import UUID

from wool.exceptions import WoolError


# public
class DiscoveryCapacityExhausted(WoolError):
    """Raised when a registration would exceed the segment's capacity.

    A namespace's shared-memory segment is sized to a fixed number of
    worker slots by its owner (`LocalDiscovery`'s ``capacity``). Once that
    many workers are registered, publishing another raises this — a typed
    signal a caller can catch by class rather than by matching a message
    string. The stamped capacity, when known, is available as ``capacity``.

    This is a transient, namespace-wide condition: dropping a worker or
    raising the capacity frees a slot, so a retry can succeed.
    """

    def __init__(self, capacity: int | None = None):
        self.capacity = capacity
        detail = "" if capacity is None else f" (capacity {capacity})"
        super().__init__(f"No available slots in shared memory registrar{detail}")


# public
class DiscoveryBlockExhausted(WoolError):
    """Raised when serialized worker metadata exceeds its block.

    A worker's metadata lives in a fixed-size shared-memory block created
    at its first registration (`LocalDiscovery`'s ``block_size``). A
    publish whose serialized metadata does not fit that block raises this,
    leaving the prior registration intact. The attempted payload size in
    bytes, when known, is available as ``size``.

    Unlike `DiscoveryCapacityExhausted`, this is a permanent, per-worker
    condition: retrying with the same metadata can never succeed — shrink
    the metadata, or re-register the worker under a larger ``block_size``.
    """

    def __init__(self, size: int | None = None):
        self.size = size
        detail = "" if size is None else f" ({size} bytes)"
        super().__init__(f"Worker metadata exceeds its registered block{detail}")


# public
class DiscoveryWorkerNotFound(WoolError):
    """Raised when an update targets a worker that is not registered.

    ``worker-updated`` requires an existing registration — unlike
    ``worker-added``, which registers a new worker or refreshes an
    existing one. The unmatched worker's UID, when known, is available as
    ``uid``.
    """

    def __init__(self, uid: UUID | None = None):
        self.uid = uid
        detail = "" if uid is None else f" {uid}"
        super().__init__(f"Worker{detail} not found in address space")


# public
class DiscoveryNamespaceInUse(WoolError):
    """Raised when claiming a namespace whose registry already exists.

    The registry belongs to a live owner or persists from a killed one;
    see `LocalDiscovery`.

    :param namespace:
        The namespace whose claim was rejected, when known.
    :param segment:
        Name of the shared-memory segment backing the existing registry,
        when known. Remove it only once no live process owns the
        namespace; removing a live owner's segment lets a second owner
        claim the namespace.
    """

    def __init__(self, namespace: str | None = None, segment: str | None = None):
        self.namespace = namespace
        self.segment = segment
        super().__init__(namespace, segment)

    def __str__(self) -> str:
        detail = "" if self.namespace is None else f" {self.namespace!r}"
        hint = (
            ""
            if self.segment is None
            else f"; if no live process owns it, remove shared memory {self.segment!r}"
        )
        return f"Discovery namespace{detail} is already in use{hint}"


# public
class DiscoveryNamespaceNotFound(WoolError):
    """Raised when a borrower binds a namespace that has no registry.

    No owner has created the registry yet, or its owner has exited and
    reclaimed it. See `LocalDiscovery` for the borrowing and orphaning
    contract.

    :param namespace:
        The namespace whose registry was not found, when known.
    """

    def __init__(self, namespace: str | None = None):
        self.namespace = namespace
        super().__init__(namespace)

    def __str__(self) -> str:
        detail = "" if self.namespace is None else f" {self.namespace!r}"
        return f"No discovery registry for namespace{detail}"
