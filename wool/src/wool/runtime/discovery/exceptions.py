"""The discovery subsystem's exceptions.

Single home for the typed errors the discovery backends raise.

.. rubric:: Implementation notes

Each exception passes its fields positionally to ``super().__init__``,
so the fallback in the worker's exception serializer, which rebuilds an
exception as ``cls(*exc.args)``, restores them (see
`wool.runtime.worker.frame`). A field left out of ``args`` arrives as
``None``, and a keyword-only field kept in ``args`` fails the rebuild.
"""

from __future__ import annotations

from uuid import UUID

from wool.exceptions import WoolError


# public
class DiscoveryCapacityExhausted(WoolError):
    """Raised when a registration would exceed the registry's capacity.

    A namespace's registry holds a fixed number of worker slots, set by
    its owner (`LocalDiscovery`'s ``capacity``). Once that many workers
    are registered, publishing another raises this.

    The condition is transient and namespace-wide. Dropping a worker
    frees a slot, so a retry can succeed. The capacity is fixed for the
    registry's lifetime.

    :param capacity:
        The number of worker slots the namespace's owner stamped into
        the registry, when known.
    """

    def __init__(self, capacity: int | None = None):
        self.capacity = capacity
        super().__init__(capacity)

    def __str__(self) -> str:
        detail = "" if self.capacity is None else f" (capacity {self.capacity})"
        return f"No available slots in discovery registry{detail}"


# public
class DiscoveryBlockExhausted(WoolError):
    """Raised when serialized worker metadata exceeds its block.

    A worker's metadata lives in a fixed-size shared-memory block created
    at its first registration (`LocalDiscovery`'s ``block_size``). A
    publish whose serialized metadata does not fit that block raises this,
    leaving the prior registration intact.

    The condition is permanent and per-worker: a retry with the same
    metadata fails. Shrink the metadata, or re-register the worker under
    a larger ``block_size``.

    :param size:
        The attempted payload size in bytes, when known.
    """

    def __init__(self, size: int | None = None):
        self.size = size
        super().__init__(size)

    def __str__(self) -> str:
        detail = "" if self.size is None else f" ({self.size} bytes)"
        return f"Worker metadata exceeds its registered block{detail}"


# public
class DiscoveryWorkerNotFound(WoolError):
    """Raised when an update targets a worker that is not registered.

    ``worker-updated`` requires an existing registration.
    ``worker-added`` registers a new worker or refreshes an existing one.

    :param uid:
        The unmatched worker's UID, when known.
    """

    def __init__(self, uid: UUID | None = None):
        self.uid = uid
        super().__init__(uid)

    def __str__(self) -> str:
        detail = "" if self.uid is None else f" {self.uid}"
        return f"Worker{detail} not found in discovery registry"


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
