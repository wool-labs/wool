"""The decoded-but-unmounted view of a wire context.

A :class:`_ContextManifest` is the deserialised state of a wire
:class:`~wool.protocol.Context`: the variable bindings, reset signals,
and stub pins recovered from the wire, plus any deferred decode error
to surface at mount time. Distinct from :class:`Context` (the live
chain): the manifest is a wire-view POD, never installed, never
serving as the active chain state.

:func:`_parse_context` is the entry point for wire ingress —
:meth:`Frame.from_protobuf` calls it on the optional wire ``context``
field and stores the resulting manifest on the decoded frame.
:meth:`Frame.mount` is the single mount entry point that consumes
the manifest, routing through :meth:`Context._update` (armed-receiver
branch) or :meth:`Context._from_manifest` (unarmed-receiver branch).

Privatized (Q14) — user code does not construct manifests. The
class and its decoder live behind underscore-prefixed names so the
public ``wool.protocol`` surface does not advertise a wire-internal
type. ``Frame.mount`` is the only public entry into the mount pipeline.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Any
from uuid import UUID
from uuid import uuid4

if TYPE_CHECKING:
    from wool import protocol as _protocol
    from wool.runtime.context.errors import ContextDecodeError as _ContextDecodeError
    from wool.runtime.context.var import ContextVar as _ContextVar
    from wool.runtime.serializer import Serializer as _Serializer


@dataclass
class _ContextManifest:
    """A deserialised but unmounted wire context.

    Carries the decoded chain state — var-to-value mapping and reset
    signals — plus any deferred decode error to surface on mount.

    Present on a :class:`Frame` iff the frame has non-empty receive-
    side state: a wire context with bindings or resets. An empty wire
    context decodes to ``None`` so :meth:`Frame.mount` can no-op.

    Mount is a one-way transition into a live :class:`Context`,
    driven by :meth:`Frame.mount`: it arms a fresh chain if the
    receiver is unarmed, merges the decoded state, and drains
    :attr:`decoded_vars` into backing variables.
    """

    chain_id: UUID
    decoded_vars: dict["_ContextVar[Any]", Any]
    reset_vars: frozenset[tuple[str, str]]
    stub_pins: frozenset["_ContextVar[Any]"]
    decode_error: "_ContextDecodeError | None" = None

    @classmethod
    def from_protobuf(
        cls,
        wire: "_protocol.Context",
        *,
        serializer: "_Serializer",
    ) -> _ContextManifest:
        """Decode a wire :class:`~wool.protocol.Context` into a manifest.

        Pure decode — resolves variable identities and deserialises
        values but never touches a :class:`contextvars.Context`. Walks
        ``wire.vars`` once: each variable identity resolves through the
        process-wide :class:`wool.ContextVar` registry (or registers a
        stub if undeclared, pinned into the returned manifest). An
        entry carrying a ``value`` populates :attr:`decoded_vars`; an
        entry with no ``value`` records a reset-to-no-value signal in
        :attr:`reset_vars`.

        Decode failures emit :class:`ContextDecodeWarning` and the
        offending entry is skipped — surviving entries decode normally.
        A malformed wire chain ID falls back to a fresh UUID. Under
        strict mode the failures aggregate into a single
        :class:`ContextDecodeError` raised after the decode loop — no
        partial manifest is surfaced. Callers route the exception
        through :func:`_parse_context`, which captures it on the
        manifest's :attr:`decode_error` field; :meth:`Frame.mount` is
        the single re-raise site so the caller-side exception-frame
        path can walk-and-append the decode error onto the worker
        exception's ``__context__`` chain.

        :raises ContextDecodeError:
            Under strict mode, when one or more entries fail to decode.
        """
        from wool.runtime.context.errors import ContextDecodeError
        from wool.runtime.context.errors import ContextDecodeWarning
        from wool.runtime.context.stub import resolve_stub

        failures: list[ContextDecodeWarning] = []
        # F30 — chain-id parse failure is a *structural* protocol
        # error, distinct from per-var data errors. Raise
        # unconditionally regardless of strict-mode warning filter:
        # without a valid chain id the receiver cannot correlate
        # subsequent frames against the same logical caller, and a
        # silently-replaced ``uuid4()`` would route follow-up frames
        # to a fresh cached Context (silent state loss).
        try:
            chain_id = UUID(hex=wire.id) if wire.id else uuid4()
        except ValueError as e:
            raise ContextDecodeError(
                ContextDecodeWarning(
                    f"Failed to decode wire context id {wire.id!r}: {e}"
                ),
            ) from e
        decoded_vars: dict[_ContextVar[Any], Any] = {}
        reset_vars: set[tuple[str, str]] = set()
        stub_pins: set[_ContextVar[Any]] = set()
        failed_keys: set[tuple[str, str]] = set()
        seen_keys: set[tuple[str, str]] = set()
        for entry in wire.vars:
            var_key = (entry.namespace, entry.name)
            # F27 — duplicate keys on the wire are explicitly
            # undefined behaviour, but the actual previous behaviour
            # was a silent one-way ratchet: a value-failed first
            # occurrence pinned the key in ``failed_keys`` so a later
            # legitimate reset signal for the same key was silently
            # dropped. Emit a typed warning so the encoder bug surfaces
            # without changing the ratchet semantics (the first
            # occurrence still wins).
            if var_key in seen_keys:
                try:
                    warnings.warn(
                        f"Duplicate wool.ContextVar key {var_key!r} in wire "
                        f"context — second occurrence ignored",
                        ContextDecodeWarning,
                        stacklevel=2,
                    )
                except ContextDecodeWarning as raised:
                    failures.append(raised)
                continue
            seen_keys.add(var_key)
            var = resolve_stub(var_key)
            if var._stub:
                stub_pins.add(var)
            if entry.HasField("value"):
                try:
                    decoded_vars[var] = serializer.loads(entry.value)
                except Exception as e:
                    failed_keys.add(var_key)
                    try:
                        warnings.warn(
                            f"Failed to deserialize wool.ContextVar {var_key!r}: {e}",
                            ContextDecodeWarning,
                            stacklevel=2,
                        )
                    except ContextDecodeWarning as raised:
                        failures.append(raised)
            if var_key in failed_keys:
                # A variable whose value failed to deserialize is absent
                # from ``decoded_vars``; recording its reset signal would
                # let a subsequent merge read a phantom reset and drop
                # a live binding on the receiver.
                continue
            if not entry.HasField("value"):
                # No value on a surviving entry: the variable is in a
                # reset-to-no-value state in the sender's chain.
                reset_vars.add(var_key)
                # F40 — for every reset-only entry, ensure the
                # receiver-side ContextVar is pinned via ``stub_pins``
                # regardless of ``_stub`` status. Without this pin a
                # garbage-collectible non-stub instance can drop
                # between decode and apply, leaving
                # ``var_registry.get(key)`` returning ``None`` and the
                # reset silently swallowed.
                stub_pins.add(var)
        if failures:
            raise ContextDecodeError(*failures)
        return cls(
            chain_id=chain_id,
            decoded_vars=decoded_vars,
            reset_vars=frozenset(reset_vars),
            stub_pins=frozenset(stub_pins),
            decode_error=None,
        )


def _parse_context(
    wire: "_protocol.Context | None",
    *,
    serializer: "_Serializer",
) -> _ContextManifest | None:
    """Build a :class:`_ContextManifest` from a wire-context proto.

    Returns ``None`` when the frame carries no wire context. Strict-
    mode :class:`ContextDecodeError` failures from
    :meth:`_ContextManifest.from_protobuf` are captured on the
    returned manifest's :attr:`decode_error` field; the deferred raise
    happens in :meth:`Frame.mount` so the caller-side exception-frame
    path can walk-and-append the error onto the worker exception's
    ``__context__`` chain.
    """
    from wool.runtime.context.errors import ContextDecodeError

    if wire is None:
        return None
    try:
        return _ContextManifest.from_protobuf(wire, serializer=serializer)
    except ContextDecodeError as e:
        return _ContextManifest(
            chain_id=uuid4(),
            decoded_vars={},
            reset_vars=frozenset(),
            stub_pins=frozenset(),
            decode_error=e,
        )


__all__ = ["_ContextManifest", "_parse_context"]
