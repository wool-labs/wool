"""The discovery subsystem's exceptions.

Single home for the typed errors the discovery backends raise.
"""

from __future__ import annotations

from wool.exceptions import WoolError


# public
class DiscoveryCapacityExhausted(WoolError):
    """Raised when a registration would exceed the segment's capacity.

    A namespace's shared-memory segment is sized to a fixed number of
    worker slots by its owner (`LocalDiscovery`'s ``capacity``). Once that
    many workers are registered, publishing another raises this — a typed
    signal a caller can catch by class rather than by matching a message
    string. The stamped capacity, when known, is available as ``capacity``.
    """

    def __init__(self, capacity: int | None = None):
        self.capacity = capacity
        detail = "" if capacity is None else f" (capacity {capacity})"
        super().__init__(f"No available slots in shared memory registrar{detail}")
