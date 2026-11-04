"""The discovery subsystem's exceptions.

Single home for the typed errors the discovery backends raise.

.. rubric:: Implementation notes

Each exception passes its fields positionally to ``super().__init__``,
so the fallback in the worker's exception serializer, which rebuilds an
exception as ``cls(*exc.args)``, restores them (see
`wool.runtime.worker.frame`). A keyword-only field cannot survive that
rebuild in either direction: riding ``args`` makes the call raise for
too many positional arguments, and staying out of ``args`` either raises
for a missing argument or — where the field has a default — succeeds
while silently substituting it, which loses the transmitted value
without reporting anything. Hence every field is positional and
required.
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
        the registry.
    """

    def __init__(self, capacity: int):
        self.capacity = capacity
        super().__init__(capacity)

    def __str__(self) -> str:
        return f"No available slots in discovery registry (capacity {self.capacity})"


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
    same block, and the size itself is the one the namespace's owner
    stamped — no publisher chooses it, so no publisher can enlarge it.
    The ceiling is therefore settled where the namespace is entered, by
    its owner's `LocalDiscovery` ``block_size``, and cannot be raised
    for a namespace already in use: a second entry while the incumbent
    owner lives raises `DiscoveryNamespaceInUse`. See `LocalDiscovery`
    for who owns a namespace's artifacts and what a borrower of one may
    assume.

    :param size:
        The attempted payload size in bytes.
    """

    def __init__(self, size: int):
        self.size = size
        super().__init__(size)

    def __str__(self) -> str:
        return f"Worker metadata exceeds its registered block ({self.size} bytes)"


# public
class DiscoveryWorkerNotFound(WoolError):
    """Raised when an update targets a worker that is not registered.

    ``worker-updated`` requires an existing registration.
    ``worker-added`` registers a new worker or refreshes an existing one.

    :param uid:
        The unmatched worker's UID.
    """

    def __init__(self, uid: UUID):
        self.uid = uid
        super().__init__(uid)

    def __str__(self) -> str:
        return f"Worker {self.uid} not found in discovery registry"


# public
class DiscoveryNamespaceInUse(WoolError):
    """Raised when claiming a namespace another process still holds.

    A namespace is in use while any process holding its claim lives. That
    is the owner's process, and anything forked from it after entry; see
    `LocalDiscovery` for the ownership contract and what ends a claim.

    :param namespace:
        The namespace whose claim was rejected.
    """

    def __init__(self, namespace: str):
        self.namespace = namespace
        super().__init__(namespace)

    def __str__(self) -> str:
        return f"Discovery namespace {self.namespace!r} is already in use"


# public
class DiscoveryNamespaceContended(WoolError):
    """Raised when a namespace's claim could not be taken.

    The namespace's directory was replaced under an entry more times
    than it retries, so no claim it took ever named the directory the
    namespace's path resolves to. Unlike `DiscoveryNamespaceInUse`, no
    process holds the claim: entry failed because the directory kept
    changing underneath it, not because anything owns it. A retry meets
    the same contention, so the remedy is a root nothing else writes
    to. See `LocalDiscovery` for the ownership contract.

    :param namespace:
        The namespace whose claim could not be taken.
    """

    def __init__(self, namespace: str):
        self.namespace = namespace
        super().__init__(namespace)

    def __str__(self) -> str:
        return (
            f"Discovery namespace {self.namespace!r} was replaced faster than it "
            "could be claimed"
        )


# public
class DiscoveryNamespaceNotFound(WoolError):
    """Raised when no live owner serves a borrower's binding.

    Raised at a bind where no owner has created the namespace yet, and
    again at any later operation by a borrower whose owner has since
    gone: a binding ends with the owner it was made against, so a
    publisher's next publish and a subscriber's next scan both fail
    rather than reaching a successor or serving what they last read.
    This holds whether that owner exited or was killed outright. A
    borrower that wants to follow the namespace re-binds after this
    error. See `LocalDiscovery` for the ownership contract.

    The condition is per-binding, not per-namespace. A successor that
    claimed the namespace after this binding's owner left does not
    serve it, so this is raised while that successor is live and
    owning the namespace -- which is why neither the summary above nor
    the rendered message states anything about the namespace's own
    ownership.

    :param namespace:
        The namespace this binding was made against.
    """

    def __init__(self, namespace: str):
        self.namespace = namespace
        super().__init__(namespace)

    def __str__(self) -> str:
        return (
            f"Discovery namespace {self.namespace!r} has no live owner for this binding"
        )
