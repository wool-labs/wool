"""The decoded-but-unmounted view of a chain manifest.

A `ChainManifest` is the successfully-deserialised state of a wire
`~wool.protocol.ChainManifest`: the variable bindings, reset signals,
and stub pins recovered from the wire. Distinct from `Chain` (the live
chain): the manifest holds its values inline rather than in the
contextvar backings, serving as the active chain state only once
mounted.

`ChainManifest.from_protobuf` is the decoder; it raises
`ChainSerializationError` on a strict-mode failure. `Frame.from_protobuf`
calls it on the optional wire ``context`` field and stores either the
manifest or that error on the decoded frame. `Frame.mount` drives the
install: it calls `ChainManifest.mount`, which drains the manifest's
values into the backings, merges onto a live receiver chain (or seeds
fresh state when unarmed), and arms the result via `Chain.mount`. User
code does not construct manifests; `Frame.mount` is the only entry into
the mount pipeline.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Any
from uuid import UUID
from uuid import uuid4

from wool.runtime.context.chain import Chain
from wool.runtime.context.exceptions import ChainSerializationError
from wool.runtime.context.exceptions import SerializationWarning
from wool.runtime.context.registry import var_registry
from wool.runtime.context.var import resolve_stub

if TYPE_CHECKING:
    from wool import protocol as _protocol
    from wool.runtime.context.var import ContextVar as _ContextVar
    from wool.runtime.serializer import Serializer as _Serializer


@dataclass
class ChainManifest:
    """A deserialised, successfully-decoded chain manifest.

    Carries the decoded chain state — the var-to-value mapping and the
    reset signals — ready to drain into the backings on mount. A decode
    *failure* is never a ChainManifest: `Frame.from_protobuf` captures it
    as a `~wool.runtime.context.exceptions.ChainSerializationError`
    instead.

    Present on a `Frame` iff the frame carried non-empty receive-side
    state: a chain manifest with bindings or resets. An empty chain
    manifest decodes to ``None`` so `Frame.mount` can no-op.

    Mount is a one-way transition into a live `Chain`: `mount` drains
    `vars` into the backing variables, merges the decoded state onto a
    live receiver (or seeds a fresh chain when unarmed), and arms the
    result through `Chain.mount`. `Frame.mount` is the entry point.
    """

    id: UUID
    vars: dict[_ContextVar[Any], Any]
    resets: frozenset[tuple[str, str]]
    stubs: frozenset[_ContextVar[Any]]

    @classmethod
    def empty(cls) -> ChainManifest:
        """Return a fresh empty manifest carrying a new chain id.

        The default for `DispatchSession.decoded` when the initial
        dispatch frame carries no chain manifest. A present-but-empty
        manifest keeps ``session.decoded.vars`` an empty dict so the
        backpressure hook's attribute access stays shape-consistent
        whether or not the inbound frame carried chain state. Returned
        fresh per call to avoid sharing mutable state across dispatches.
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
        partial manifest is surfaced. `Frame.from_protobuf` captures
        that error as the frame's ``chain_manifest`` value instead of
        letting it propagate, so `Frame.mount` (and, for the initial
        dispatch frame, `DispatchSession.__aenter__`) can raise it or
        walk-and-append it onto an exception payload's ``__context__``
        chain rather than preempting the payload.

        :raises ChainSerializationError:
            Under strict mode, when one or more entries fail to decode.
        """
        failures: list[SerializationWarning] = []
        # Chain-id parse failure is a *structural* protocol
        # error, distinct from per-var data errors. Raise
        # unconditionally regardless of strict-mode warning filter:
        # without a valid chain id the receiver cannot correlate
        # subsequent frames against the same logical caller, and a
        # silently-replaced ``uuid4()`` would route follow-up frames
        # to a fresh cached contextvars.Context (silent state loss).
        try:
            chain_id = UUID(hex=wire.id) if wire.id else uuid4()
        except ValueError as e:
            raise ChainSerializationError(
                SerializationWarning(
                    f"Failed to decode chain id {wire.id!r}: {e}",
                    cause=e,
                    direction="decode",
                ),
            ) from e
        vars: dict[_ContextVar[Any], Any] = {}
        resets: set[tuple[str, str]] = set()
        stubs: set[_ContextVar[Any]] = set()
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
        )

    def mount(
        self,
        *,
        stamp_owner: bool,
        install_factory: bool,
        merge_with: Chain | None = None,
    ) -> Chain:
        """Drain this manifest into the backings and arm it as the live chain.

        The wire-ingress counterpart to `Chain.mount`, entered through
        `Frame.mount`. Drains the decoded `vars` into their backing
        variables, builds the receiver chain — merging onto a live
        receiver when *merge_with* is given (the receiver keeps its
        chain id), or seeding fresh state from this manifest alone when
        it is ``None`` — and delegates the arming (reset rewind, owner
        stamp, task factory, `wool.__chain__` set) to `Chain.mount`.

        :param stamp_owner:
            Forwarded to `Chain.mount`. Caller-side mounts pass
            ``True``; the worker-side per-step driver passes ``False``
            because the cached `contextvars.Context` already owns the
            chain.
        :param install_factory:
            Forwarded to `Chain.mount`. Caller-side mounts pass
            ``True``; the worker-side driver passes ``False`` to bypass
            the loop's task factory.
        :param merge_with:
            The live receiver chain to union this manifest onto, or
            ``None`` to seed a fresh chain from the manifest alone.

        """
        # Drain values into the backings first so the built Chain
        # observes them via ``vars``. Backing writes mint stray native
        # tokens — discarded with the local scope.
        for var, value in self.vars.items():
            var._backing.set(value)
        # Build the vars index from the manifest, optionally unioned
        # with the live receiver's bindings (the receiver keeps its
        # chain id through the merge).
        manifest_vars = frozenset(self.vars.keys())
        if merge_with is None:
            merged_vars = manifest_vars
            resets = self.resets
            stubs = self.stubs
            chain_id = self.id
        else:
            merged_vars = merge_with.vars | manifest_vars
            manifest_touched = {var._key for var in manifest_vars} | self.resets
            resets = self.resets | (merge_with.resets - manifest_touched)
            stubs = merge_with.stubs | self.stubs
            chain_id = merge_with.id
        # Reset-only keys drop out of ``vars`` so the reset propagates;
        # the rewind itself happens in Chain.mount.
        for key in self.resets:
            receiver_var = var_registry.get(key)
            if receiver_var is not None:
                merged_vars = merged_vars - {receiver_var}
        target = Chain(id=chain_id, vars=merged_vars, resets=resets, stubs=stubs)
        return target.mount(stamp_owner=stamp_owner, install_factory=install_factory)


__all__ = ["ChainManifest"]
