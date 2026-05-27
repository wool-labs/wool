"""Unified Wool exception and warning hierarchy (Q15 + Q30 + Q16).

All Wool-domain runtime exceptions descend from a single
:class:`WoolError` umbrella so callers can catch the whole
domain with one ``except`` clause::

    try:
        ...
    except wool.WoolError:
        ...

Serialization failures live under their own branch with two shapes:

* :class:`SerializationError` — atomic (raised directly for result-
  payload encode failures, F10). Carries ``.cause`` and an optional
  ``.value_repr``.
* :class:`ContextSerializationError` — aggregator subclass for the
  strict-mode context encode/decode loop. Adds a ``.warnings`` tuple
  collecting every :class:`SerializationWarning` promoted to error
  by ``warnings.filterwarnings("error", ...)``.

Warnings emit through a single flat :class:`SerializationWarning`
class with optional structured fields per emission site:

* Per-var context-encode / context-decode sites populate
  ``var_key`` and ``direction`` (``"encode"`` / ``"decode"``).
* The exception-fidelity fallback site (F26 / ``_safely_serialize_exception``)
  populates ``original_class``, ``lost_args``, and
  ``lost_cause_chain``.

Strict-mode promotion uses category-level filtering — callers do
not need to distinguish per-var failures from exception-fidelity
emissions::

    warnings.filterwarnings("error", category=wool.SerializationWarning)

Back-compat: the previous ``ContextDecodeError`` and
``ContextDecodeWarning`` names remain as aliases for
:class:`ContextSerializationError` and :class:`SerializationWarning`
respectively, so existing callers continue to work without code
changes. Internal Wool code uses the new names.
"""

from __future__ import annotations

from typing import Any
from typing import Literal


class WoolError(RuntimeError):
    """Base class for every Wool-domain runtime exception.

    Catching :class:`WoolError` matches every typed signal Wool raises
    from its own runtime — :class:`ChainContention`,
    :class:`TaskFactoryDisplaced`, :class:`ContextVarCollision`, and
    the :class:`SerializationError` branch. Inherits from
    :class:`RuntimeError` so existing ``except RuntimeError`` clauses
    keep matching.
    """


class SerializationError(WoolError):
    """Raised when a value cannot be serialised across the Wool wire.

    The atomic case — used directly for result-payload encode failures
    on the dispatch handler (F10). The strict-mode aggregator
    (:class:`ContextSerializationError`) subclasses this to add a
    ``.warnings`` tuple over the per-var failures it collected.

    :ivar cause:
        The underlying exception (pickle error, type error, etc.) that
        produced the failure. Also carried on ``__cause__`` when raised
        via ``raise SerializationError(...) from underlying``.
    :ivar value_repr:
        Optional ``repr()``-style preview of the value that failed to
        encode, useful for diagnostics when the cause exception's own
        message does not name the offending value.
    """

    def __init__(
        self,
        *args: Any,
        cause: BaseException | None = None,
        value_repr: str | None = None,
    ) -> None:
        super().__init__(*args)
        self.cause = cause
        self.value_repr = value_repr


class ContextSerializationError(SerializationError):
    """Aggregator raised when one or more wire-context entries fail.

    Carries every promoted :class:`SerializationWarning` from the
    surrounding ``Context.to_protobuf`` / ``ContextManifest.from_protobuf``
    loop in :attr:`args`. Use :attr:`warnings` for type-narrowed
    programmatic access.

    Strict mode promotes warnings to errors via
    ``warnings.filterwarnings("error", category=SerializationWarning)``;
    the encode/decode loops catch each promoted warning and aggregate
    them into a single :class:`ContextSerializationError` raised at
    the end of the loop so every bad variable surfaces, not just the
    first.

    When a routine *also* raises a primary exception, the aggregator
    rides on that primary's ``__cause__`` via ``raise primary from
    decode_err`` — the routine's existing ``except`` clauses keep
    matching unchanged.
    """

    @property
    def warnings(self) -> "tuple[SerializationWarning, ...]":
        """The promoted :class:`SerializationWarning` instances.

        Narrowed view of :attr:`args` for callers that want the
        warnings without iterating ``args`` manually.
        """
        return tuple(w for w in self.args if isinstance(w, SerializationWarning))


class SerializationWarning(RuntimeWarning):
    """Wire-protocol serialization warning.

    Emitted for non-fatal serialization issues — wool's wire protocol
    treats context propagation and exception fidelity as ancillary
    state; per-variable failures and exception-class reconstruction
    fallbacks never preempt the primary signal. A
    :class:`SerializationWarning` is emitted so callers that depend on
    that state can detect the inconsistency.

    Callers that prefer strict semantics can promote the warning to
    an exception::

        import warnings
        import wool

        warnings.filterwarnings("error", category=wool.SerializationWarning)

    Under strict mode, per-variable encode/decode failures are
    collected into a single :class:`ContextSerializationError` raised
    after the loop completes.

    Optional structured fields are populated per emission site:

    * Per-variable encode/decode (``Context.to_protobuf``,
      ``ContextManifest.from_protobuf``): ``var_key`` and
      ``direction``.
    * Exception-fidelity fallback (``_safely_serialize_exception``):
      ``original_class``, ``lost_args``, ``lost_cause_chain``.
    * Both sites populate ``cause`` when an underlying exception is
      available, and chain it via ``__cause__`` on the warning
      instance.
    """

    def __init__(
        self,
        *args: Any,
        cause: BaseException | None = None,
        var_key: tuple[str, str] | None = None,
        direction: Literal["encode", "decode"] | None = None,
        original_class: type | None = None,
        lost_args: tuple[Any, ...] | None = None,
        lost_cause_chain: int | None = None,
    ) -> None:
        super().__init__(*args)
        self.cause = cause
        self.var_key = var_key
        self.direction = direction
        self.original_class = original_class
        self.lost_args = lost_args
        self.lost_cause_chain = lost_cause_chain


# Back-compat aliases. Previous names continue to work; new code
# should prefer the unified names defined above.
ContextDecodeError = ContextSerializationError
ContextDecodeWarning = SerializationWarning
