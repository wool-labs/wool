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
    frees a slot, so a retry can succeed. The ceiling itself does not
    grow.

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

    A worker's metadata lives in a fixed-size block file created
    at its first registration (`LocalDiscovery`'s ``block_size``). A
    publish whose serialized metadata does not fit that block raises this,
    leaving the prior registration intact.

    The condition is permanent and per-worker: a retry with the same
    metadata fails. Shrink the metadata. A block's size is fixed at the
    worker's first registration and a re-registration writes into that
    same block, so publishing again through a publisher configured with
    a larger ``block_size`` does not enlarge it; see
    `LocalDiscovery.Publisher.publish`.

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
    """Raised when claiming a namespace another process still holds.

    A namespace is in use while any process holding its claim lives. That
    is the owner's process, and anything forked from it after entry; see
    `LocalDiscovery` for the ownership contract and what ends a claim.

    :param namespace:
        The namespace whose claim was rejected, when known.
    """

    def __init__(self, namespace: str | None = None):
        self.namespace = namespace
        super().__init__(namespace)

    def __str__(self) -> str:
        detail = "" if self.namespace is None else f" {self.namespace!r}"
        return f"Discovery namespace{detail} is already in use"


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
