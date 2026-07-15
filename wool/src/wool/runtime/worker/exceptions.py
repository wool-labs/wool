"""The worker subsystem's shared exceptions and warnings.

`UnparsableVersionWarning` is emitted when a protocol version string
cannot be parsed into a comparable version — most visibly by the
`WorkerProxy` admission gate for its own local protocol version.
"""

from __future__ import annotations

from wool.exceptions import WoolWarning


# public
class UnparsableVersionWarning(WoolWarning):
    """Emitted when a protocol version string cannot be parsed.

    `parse_version` returned ``None`` for the version string — it is
    not valid PEP 440, typically because a distribution's metadata is
    missing and its version falls back to a non-version sentinel such
    as ``"unknown"``.  The `WorkerProxy` admission gate emits this for
    its own local protocol version: an unparsable local version makes
    the version half of the gate reject every worker, so the pool stays
    empty because of the proxy's own misconfiguration rather than an
    absent fleet.  Filter this category to ``"error"`` (via
    `warnings.filterwarnings`) to fail loudly instead.
    """
