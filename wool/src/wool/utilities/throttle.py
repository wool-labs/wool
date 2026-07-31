from __future__ import annotations

import time
from collections.abc import Hashable


class Throttle:
    """Rate-limit warnings about a condition that recurs at traffic rate.

    A condition worth warning about once is rarely worth warning about on
    every occurrence: a worker that fails its handshake is retried forever,
    a credential factory that churns is re-consulted per refresh. Left
    unbounded, either floods the log at a rate set by traffic rather than
    by how much there is to say.

    Occurrences are tracked per ``key`` and compared by ``detail``, a string
    summarizing what happened. The cadence contract:

    - The first occurrence of a key emits immediately.
    - Identical occurrences then emit at most once per ``interval``, and
      each emission reports how many it suppressed since the last one.
    - An occurrence whose ``detail`` differs from the last one seen emits
      immediately, because a changed detail is new information.

    Callers `discard` a key once its condition clears, so a recurrence
    emits fresh rather than being folded into the previous incident, and
    state stays bounded by the number of live keys rather than growing
    with occurrences.

    Not thread-safe. Confine each instance to one thread or event loop —
    `due` reads and writes its state with no await point between, so a
    single-threaded caller needs no lock.

    :param interval:
        Minimum seconds between emissions for an unchanged ``detail``.
    """

    def __init__(self, interval: float) -> None:
        self._interval = interval
        self._state: dict[Hashable, tuple[str, float, int]] = {}

    def due(self, key: Hashable = None, detail: str = "") -> tuple[bool, int]:
        """Record an occurrence and decide whether to emit it.

        Returns a pair rather than an optional count so that a due
        emission with nothing suppressed — the common first occurrence —
        cannot be mistaken for a suppressed one.

        :param key:
            What the condition is about, e.g. the worker it concerns.
            Defaults to ``None``, the single slot a caller tracking one
            global condition wants.
        :param detail:
            A summary of this occurrence. A change from the last one seen
            emits immediately.
        :returns:
            ``(emit, suppressed)`` — whether to emit, and how many
            occurrences were suppressed since the last emission. The count
            is meaningful only when ``emit`` is true.
        """
        now = time.monotonic()
        previous = self._state.get(key)
        if previous is not None:
            last_detail, last_emit, suppressed = previous
            if detail == last_detail and now - last_emit < self._interval:
                self._state[key] = (last_detail, last_emit, suppressed + 1)
                return False, suppressed + 1
        suppressed = previous[2] if previous is not None else 0
        self._state[key] = (detail, now, 0)
        return True, suppressed

    def discard(self, key: Hashable = None) -> None:
        """Forget a key so its next occurrence emits fresh.

        :param key:
            The key to forget. Unknown keys are ignored.
        """
        self._state.pop(key, None)
