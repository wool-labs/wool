"""The worker subsystem's shared exceptions and warnings.

Categories the worker subsystem raises or emits; one emitted from a
single module may live beside its emitter instead.
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
    the version arm of the gate reject every worker, so the pool stays
    empty because of the proxy's own misconfiguration rather than an
    absent fleet.  Filter this category to ``"error"`` (via
    `warnings.filterwarnings`) to fail loudly instead.
    """


# public
class SlowCredentialResolutionWarning(WoolWarning):
    """Emitted when resolving a worker's own credentials was slow.

    A worker resolves its credentials once before it serves. The
    resolution is awaited, so it never stalls the worker's event loop,
    but a factory that takes seconds still delays the worker becoming
    available and is indistinguishable from a hung start to whoever is
    waiting for it. Emitted so the cost is attributable to the factory
    rather than to wool.
    """


# public
class IneffectivePeersWarning(WoolWarning):
    """Emitted when a configured ``peers`` value gates nothing.

    A ``peers`` value that normalizes to no names at all (e.g., a blank
    string, an empty iterable, an iterable of blanks) is not a widened
    accept-list: it leaves the provider in the *unconfigured* state
    `WorkerProxy` documents. Users who want strict behaviour can elevate
    the category to an error via `warnings.filterwarnings`.
    """
