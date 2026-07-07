"""Wool's wire-abstraction types — the manifests that ride in place of
live context state.

A *manifest* is the wire-side abstraction of a live type: it travels in
place of the concrete thing and knows nothing of the live runtime. This
module houses both. `ContextVarManifest` is the identity/data layer of a
single variable — its ``(namespace, name)`` key, default, and backing —
which `wool.ContextVar` subclasses to add live behavior. `ChainManifest`
is the decoded snapshot of a whole chain. ("Stub" is reserved for the
``_stub`` *state*, which either a bare manifest or a promoted variable
can occupy; it is no longer a type.)

A `ChainManifest` is the successfully-deserialised state of a wire
`~wool.protocol.ChainManifest`: the variable bindings, reset signals,
and stub pins recovered from the wire. Distinct from `Chain` (the live
chain): the manifest holds its values inline rather than in the
contextvar backings, and it knows nothing about the live runtime — it
neither reads backings nor arms a context. It is the staging form on
both sides of the wire.

`ChainManifest.from_protobuf` and `ChainManifest.to_protobuf` are the
codec, the two `protocol.ChainManifest` ↔ `ChainManifest` halves.
Crossing the `ChainManifest` ↔ `Chain` boundary — the half that touches
the backings — lives on `Chain`: `Chain.to_manifest` snapshots a live
chain into a manifest, and `Chain.from_manifest` drains a manifest into
the backings and arms it.

`Frame.from_protobuf` decodes the optional wire ``context`` field and
stores either the manifest or a `ChainSerializationError` on the decoded
frame. `Frame.mount` drives the install through `Chain.from_manifest`.
User code does not construct manifests; the frame layer is the only
entry into the codec.
"""

from __future__ import annotations

import contextvars
import warnings
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import NoReturn
from typing import SupportsIndex
from typing import TypeVar
from uuid import UUID
from uuid import uuid4

import wool
from wool import protocol as _protocol
from wool.runtime.context.exceptions import ChainSerializationError
from wool.runtime.context.exceptions import SerializationWarning
from wool.runtime.context.registry import lock
from wool.runtime.context.registry import var_registry
from wool.runtime.typing import Undefined
from wool.runtime.typing import UndefinedType

if TYPE_CHECKING:
    from wool.runtime.serializer import Serializer as _Serializer

T = TypeVar("T")


class ContextVarManifest(Generic[T]):
    """The wire-abstraction of a single context variable.

    Carries the ``(namespace, name)`` key, the constructor default, and
    the backing `contextvars.ContextVar` — the identity a variable holds
    independently of any live `~wool.runtime.context.chain.Chain`.
    `~wool.runtime.context.var.ContextVar` subclasses this and adds the
    chain-bound `get`/`set`/`reset` behavior; a bare
    ``ContextVarManifest`` is what `resolve_stub` mints on the receiver
    when a wire frame references a key no local variable has declared
    yet.

    Instances are process-wide singletons per key, registered in
    `~wool.runtime.context.registry.var_registry`. The ``_stub`` flag
    records whether an instance is still an undeclared placeholder — a
    *state* a bare manifest or a promoted `ContextVar` can occupy;
    promotion (see `~wool.runtime.context.var.promote`) clears it.

    Not part of the public surface — user code constructs
    `wool.ContextVar`, never this base directly.
    """

    __slots__ = (
        "_name",
        "_namespace",
        "_key",
        "_default",
        "_stub",
        "_backing",
        "__weakref__",
    )

    _name: str
    _namespace: str
    _key: tuple[str, str]
    _default: T | UndefinedType
    _stub: bool
    _backing: contextvars.ContextVar[T | UndefinedType]

    def __repr__(self) -> str:
        default_part = (
            f" default={self._default!r}" if self._default is not Undefined else ""
        )
        return (
            f"<wool.ContextVar name={self._name!r} "
            f"namespace={self._namespace!r}{default_part} at 0x{id(self):x}>"
        )

    def __reduce_ex__(self, _protocol: SupportsIndex) -> NoReturn:
        """Reject vanilla pickling.

        `copy.copy` and `copy.deepcopy` route through ``__reduce_ex__`` and
        are rejected too.

        :raises TypeError:
            Always.

        .. rubric:: Implementation notes

        ContextVar identity is registered against the process-wide
        `~wool.runtime.context.registry.var_registry`; restoring an instance
        outside Wool's dispatch path bypasses the stub-promotion and
        collision-detection that
        `~wool.runtime.context.var.ContextVar._reconstitute` relies on, and a
        registry-bound ContextVar has no meaningful copy semantics. Wool's own
        pickler consults ``reducer_override`` (and therefore
        `~wool.runtime.context.var.ContextVar.__wool_reduce__`) before
        ``__reduce_ex__``, so this guard is invisible to Wool's serialization.
        """
        raise TypeError(
            "wool.ContextVar cannot be pickled via vanilla pickle/cloudpickle; "
            "it is serialized automatically when dispatched through Wool's "
            "runtime."
        )

    @property
    def name(self) -> str:
        """The variable's name, matching the `contextvars.ContextVar` API."""
        return self._name

    @property
    def namespace(self) -> str:
        """The namespace this variable belongs to."""
        return self._namespace

    @classmethod
    def _build(
        cls,
        key: tuple[str, str],
        default: Any,
        *,
        stub: bool,
    ) -> ContextVarManifest[Any]:
        """Construct a `ContextVarManifest` instance with field assignment.

        Single source of truth for the ``object.__new__`` +
        per-field-assignment idiom shared by
        `~wool.runtime.context.var.ContextVar.__new__` (the user-facing
        construction path, which builds the subtype) and `resolve_stub`
        (the wire-boundary path). The instance is *not* registered in
        `~wool.runtime.context.registry.var_registry` — callers do that
        under the registry lock.

        .. rubric:: Implementation notes

        The backing stdlib variable is created once and shared by every
        chain for the process lifetime; it carries no ``contextvars``-level
        default — `~wool.runtime.context.var.ContextVar.get` owns the
        default-resolution ladder, and "unset" is the
        `~wool.runtime.typing.Undefined` sentinel value.
        """
        namespace, name = key
        instance: ContextVarManifest[Any] = object.__new__(cls)
        instance._name = name
        instance._namespace = namespace
        instance._key = key
        instance._default = default
        instance._stub = stub
        instance._backing = contextvars.ContextVar(
            f"__wool_var__:{namespace}:{name}", default=Undefined
        )
        return instance


def resolve_stub(
    key: tuple[str, str],
    *,
    default: Any = Undefined,
) -> ContextVarManifest[Any]:
    """Return the `ContextVarManifest` registered under *key*, minting a
    stub-state one if no authoritative declaration exists yet.

    Unifies the two ingress paths that may encounter an unregistered
    variable key on a receiving process: the pickle-embedded
    `ContextVar` instance path (via
    `~wool.runtime.context.var.ContextVar._reconstitute`) and the
    chain-manifest path (via `ChainManifest.from_protobuf`). Both route
    through this helper so a lazy-import receiver converges on a single
    instance per key regardless of whether the value arrived as a bare
    wire entry or embedded in a pickled variable reference.

    Pass *default* to seed the constructor default before promotion when
    that information is available on the ingress side (the pickle path
    carries it; the chain-manifest path does not).

    .. rubric:: Implementation notes

    A freshly created stub is registered in
    `~wool.runtime.context.registry.var_registry` (a
    `weakref.WeakValueDictionary`, so it needs a strong referent to
    survive). It is held by the embedding object graph (the
    pickle-embedded ingress) or by the decoded
    `~wool.runtime.context.chain.Chain`'s ``stubs`` (the chain-manifest
    ingress) until the receiver's user code declares the real variable,
    at which point `~wool.runtime.context.var.ContextVar.__new__`
    promotes it in place. A promoted variable remains in ``stubs`` for
    the rest of that chain's life — harmless, since a declared variable
    is a process-wide singleton anyway. If the receiver never declares
    the variable, it is collected with whatever held it and the
    propagated value is dropped.
    """
    with lock:
        existing = var_registry.get(key)
        if existing is not None:
            # Fold the supplied default into a default-less stub: the
            # chain-manifest path supplies no default (wire bytes don't
            # carry it), but the pickle-embedded path does. Whichever
            # ingress encounters the key second must not silently
            # discard a known default.
            if (
                existing._stub
                and existing._default is Undefined
                and default is not Undefined
            ):
                existing._default = default
            return existing
        # The backing variable is created with the stub and preserved
        # across promotion (``promote`` keeps the same instance), so a
        # value applied to the backing before the receiver declares the
        # real variable survives the promotion.
        stub = ContextVarManifest._build(key, default, stub=True)
        var_registry[key] = stub
        return stub


@dataclass
class ChainManifest:
    """A deserialised, successfully-decoded chain manifest.

    Carries the decoded chain state — the var-to-value mapping and the
    reset signals — inline, ready for `Chain.from_manifest` to drain into
    the backings. A decode *failure* is never a ChainManifest:
    `Frame.from_protobuf` captures it as a
    `~wool.runtime.context.exceptions.ChainSerializationError` instead.

    Present on a `Frame` iff the frame carried non-empty receive-side
    state: a chain manifest with bindings, resets, or token bookkeeping
    (spent or unspent token IDs). An empty chain manifest decodes to
    ``None`` so `Frame.mount` can no-op.

    The transition into a live `Chain` is one-way and lives on `Chain`:
    `Chain.from_manifest` drains `vars` into the backing variables,
    merges the decoded state onto a live receiver (or seeds a fresh chain
    when unarmed), and arms the result. `Frame.mount` is the entry point.

    :param spent_tokens:
        Spent (consumed) `~wool.runtime.context.token.Token` IDs per var key
        — the cross-process single-use ledger (see
        `~wool.runtime.context.chain.Chain.spent_tokens` for the contract).
    :param unspent_tokens:
        Live (unspent) `~wool.runtime.context.token.Token` IDs per var key,
        consulted by the receiver's mount to anchor wire tokens (see
        `~wool.runtime.context.token.anchor_tokens`).

    .. rubric:: Implementation notes

    ``spent_tokens`` and ``unspent_tokens`` are chain-local and ride every
    frame, snapshotted by `~wool.runtime.context.chain.Chain.to_manifest`
    and unioned by `~wool.runtime.context.chain.Chain.from_manifest`. Each
    piggybacks on the var entry that already exists for the value or reset it
    accompanies, never emitting a standalone entry (a value-less entry is
    unambiguously a reset).
    """

    id: UUID
    vars: dict[ContextVarManifest[Any], Any]
    resets: frozenset[tuple[str, str]]
    stubs: frozenset[ContextVarManifest[Any]]
    spent_tokens: dict[tuple[str, str], frozenset[str]] = field(default_factory=dict)
    unspent_tokens: dict[tuple[str, str], frozenset[str]] = field(default_factory=dict)

    @property
    def has_state(self) -> bool:
        """Report whether this manifest carries observable state to install.

        Bindings, resets, or token bookkeeping (spent or unspent IDs) all
        count; an all-empty manifest has nothing to install and mounts as a
        no-op.

        .. rubric:: Implementation notes

        Token bookkeeping counts because the ledger ingest and the
        pending-anchor drain live inside the mount, so a manifest carrying
        only bookkeeping must still transit it.
        """
        return bool(self.vars or self.resets or self.spent_tokens or self.unspent_tokens)

    @classmethod
    def empty(cls) -> ChainManifest:
        """Return a fresh empty manifest carrying a new chain ID.

        The default for `DispatchSession.decoded` when the initial dispatch
        frame carries no chain manifest.

        .. rubric:: Implementation notes

        A present-but-empty manifest keeps ``session.decoded.vars`` an empty
        dict so the backpressure hook's attribute access stays
        shape-consistent whether or not the inbound frame carried chain
        state. Returned fresh per call to avoid sharing mutable state across
        dispatches.
        """
        return cls(
            id=uuid4(),
            vars={},
            resets=frozenset(),
            stubs=frozenset(),
        )

    @classmethod
    def from_protobuf(
        cls,
        wire: _protocol.ChainManifest,
        *,
        serializer: _Serializer,
    ) -> ChainManifest:
        """Decode a wire `~wool.protocol.ChainManifest` into a manifest.

        Pure decode — resolves variable identities and deserialises
        values but never touches a `contextvars.Context`. Walks
        ``wire.vars`` once: each variable identity resolves through the
        process-wide `wool.ContextVar` registry (or registers a
        stub if undeclared, pinned into the returned manifest). An
        entry carrying a ``value`` populates `vars`; an
        entry with no ``value`` records a reset-to-no-value signal in
        `resets`.

        Decode failures emit `SerializationWarning` and the
        offending entry is skipped — surviving entries decode normally.
        A malformed wire chain ID falls back to a fresh UUID. Under
        strict mode the failures aggregate into a single
        `ChainSerializationError` raised after the decode loop — no
        partial manifest is surfaced.

        :raises ChainSerializationError:
            Under strict mode, when one or more entries fail to decode.

        .. rubric:: Implementation notes

        `Frame.from_protobuf` captures a raised `ChainSerializationError`
        as the frame's ``chain_manifest`` value instead of letting it
        propagate, so `Frame.mount` (and, for the initial dispatch frame,
        `DispatchSession.__aenter__`) can raise it or walk-and-append it
        onto an exception payload's ``__context__`` chain rather than
        preempting the payload.
        """
        failures: list[SerializationWarning] = []
        # Chain-ID parse failure is a *structural* protocol
        # error, distinct from per-var data errors. Raise
        # unconditionally regardless of strict-mode warning filter:
        # without a valid chain ID the receiver cannot correlate
        # subsequent frames against the same logical caller, and a
        # silently-replaced ``uuid4()`` would route follow-up frames
        # to a fresh cached contextvars.Context (silent state loss).
        try:
            chain_id = UUID(hex=wire.id) if wire.id else uuid4()
        except ValueError as e:
            raise ChainSerializationError(
                SerializationWarning(
                    f"Failed to decode chain ID {wire.id!r}: {e}",
                    cause=e,
                    direction="decode",
                ),
            ) from e
        vars: dict[ContextVarManifest[Any], Any] = {}
        resets: set[tuple[str, str]] = set()
        stubs: set[ContextVarManifest[Any]] = set()
        spent_tokens: dict[tuple[str, str], frozenset[str]] = {}
        unspent_tokens: dict[tuple[str, str], frozenset[str]] = {}
        failed_keys: set[tuple[str, str]] = set()
        seen_keys: set[tuple[str, str]] = set()
        for entry in wire.vars:
            var_key = (entry.namespace, entry.name)
            # Duplicate keys on the wire are explicitly undefined
            # behaviour; the first occurrence wins. A value-failed
            # first occurrence pins the key in ``failed_keys``, so a
            # later legitimate reset signal for the same key is
            # dropped. Emit a typed warning so the encoder bug surfaces
            # without disturbing the first-occurrence-wins semantics.
            if var_key in seen_keys:
                try:
                    warnings.warn(
                        SerializationWarning(
                            f"Duplicate wool.ContextVar key {var_key!r} in wire "
                            f"context — second occurrence ignored",
                            var_key=var_key,
                            direction="decode",
                        ),
                        stacklevel=2,
                    )
                except SerializationWarning as raised:
                    failures.append(raised)
                continue
            seen_keys.add(var_key)
            # Spent/unspent token IDs ride independently of the value/reset
            # state, so capture them before any value-decode failure can
            # ``continue`` past this entry.
            if entry.spent_tokens:
                spent_tokens[var_key] = frozenset(entry.spent_tokens)
            if entry.unspent_tokens:
                unspent_tokens[var_key] = frozenset(entry.unspent_tokens)
            var = resolve_stub(var_key)
            if var._stub:
                stubs.add(var)
            if entry.HasField("value"):
                try:
                    vars[var] = serializer.loads(entry.value)
                except Exception as e:
                    failed_keys.add(var_key)
                    try:
                        warnings.warn(
                            SerializationWarning(
                                f"Failed to deserialize wool.ContextVar "
                                f"{var_key!r}: {e}",
                                cause=e,
                                var_key=var_key,
                                direction="decode",
                            ),
                            stacklevel=2,
                        )
                    except SerializationWarning as raised:
                        failures.append(raised)
            if var_key in failed_keys:
                # A variable whose value failed to deserialize is absent
                # from ``vars``; recording its reset signal would
                # let a subsequent merge read a phantom reset and drop
                # a live binding on the receiver.
                continue
            if not entry.HasField("value"):
                # No value on a surviving entry: the variable is in a
                # reset-to-no-value state in the sender's chain.
                resets.add(var_key)
                # For every reset-only entry, ensure the
                # receiver-side ContextVar is pinned via ``stubs``
                # regardless of ``_stub`` status. Without this pin a
                # garbage-collectible non-stub instance can drop
                # between decode and apply, leaving
                # ``var_registry.get(key)`` returning ``None`` and the
                # reset silently swallowed.
                stubs.add(var)
        if failures:
            raise ChainSerializationError(*failures)
        return cls(
            id=chain_id,
            vars=vars,
            resets=frozenset(resets),
            stubs=frozenset(stubs),
            spent_tokens=spent_tokens,
            unspent_tokens=unspent_tokens,
        )

    def to_protobuf(
        self,
        *,
        serializer: _Serializer | None = None,
    ) -> _protocol.ChainManifest:
        """Serialise this manifest to a wire `~wool.protocol.ChainManifest`.

        The encode half of the send path: `Chain.to_manifest` captures the
        live backing values inline on this manifest, and this method turns
        them into wire bytes. Each entry in `vars` emits one
        `~wool.protocol.ContextVar` with a populated ``value``; each key in
        `resets` emits a value-less entry so the reset propagates
        regardless of source. The two source sets are disjoint by
        construction (asserted in `Chain.to_manifest`), so every key emits
        exactly one entry.

        Per-variable encode failures emit `SerializationWarning` and the
        offending key is suppressed entirely — its reset signal included,
        so the receiver cannot read a phantom reset for a variable whose
        value failed to ship. Under strict mode
        (``PYTHONWARNINGS=error::wool.SerializationWarning``) the failures
        aggregate into a single `ChainSerializationError` raised after the
        loop.

        Encoding is deterministic — identical manifest state encodes to
        byte-identical frames across processes, preserving content-addressed
        caching and replay-style fingerprinting.

        :raises ChainSerializationError:
            Under strict mode, when one or more variables fail to encode.

        .. rubric:: Implementation notes

        Determinism comes from sorted emission: entries in sorted key order,
        and each entry's ``spent_tokens`` and ``unspent_tokens`` ID lists
        likewise sorted — there is no manifest-level ID list — so the frame
        is byte-identical despite hash-randomised ``frozenset``/``set``
        iteration.
        """
        if serializer is None:
            serializer = wool.__serializer__
        wire = _protocol.ChainManifest(id=self.id.hex)
        failures: list[SerializationWarning] = []
        encoded_values: dict[tuple[str, str], bytes] = {}
        failed_keys: set[tuple[str, str]] = set()
        for var, value in self.vars.items():
            try:
                encoded_values[var._key] = serializer.dumps(value)
            except Exception as e:
                failed_keys.add(var._key)
                try:
                    warnings.warn(
                        SerializationWarning(
                            f"Failed to serialize wool.ContextVar {var._key!r}: {e}",
                            cause=e,
                            var_key=var._key,
                            direction="encode",
                        ),
                        stacklevel=2,
                    )
                except SerializationWarning as raised:
                    failures.append(raised)
        # Failed keys are suppressed entirely — no phantom resets (see the docstring).
        reset_keys = {key for key in self.resets if key not in failed_keys}
        # Sorted emission — the byte-determinism guarantee in the docstring.
        for var_key in sorted(set(encoded_values) | reset_keys):
            namespace, name = var_key
            entry = wire.vars.add(namespace=namespace, name=name)
            if var_key in encoded_values:
                entry.value = encoded_values[var_key]
            # Spent/unspent IDs ride on the entry that already exists for
            # this key (a spent key is in vars∪resets; an unspent key is
            # value-bearing → both have an entry). Sorted per the docstring.
            spent = self.spent_tokens.get(var_key)
            if spent:
                entry.spent_tokens.extend(sorted(spent))
            unspent = self.unspent_tokens.get(var_key)
            if unspent:
                entry.unspent_tokens.extend(sorted(unspent))
        if failures:
            raise ChainSerializationError(*failures)
        return wire
