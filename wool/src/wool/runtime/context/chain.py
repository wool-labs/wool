"""The chain-state model.

Provides `Chain`: the immutable record of a logical
execution chain that the rest of the context subsystem mutates,
ships across the wire, forks per task, and polices for contention.
"""

from __future__ import annotations

import asyncio
import threading
import weakref
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from typing import TYPE_CHECKING
from typing import Any
from uuid import UUID
from uuid import uuid4

import wool
from wool.runtime.context.factory import ensure_task_factory_installed
from wool.runtime.context.manifest import ChainManifest
from wool.runtime.context.registry import var_registry
from wool.runtime.context.token import anchor_tokens
from wool.runtime.typing import Undefined

if TYPE_CHECKING:
    from collections.abc import Iterable

    from wool.runtime.context.manifest import ContextVarManifest
    from wool.runtime.context.token import Token


@dataclass(frozen=True, eq=False)
class Chain:
    """Immutable index of Wool chain state.

    Wool chain state — the logical-chain ID, the bound `wool.ContextVar`
    instances, and their resets — rides in one Wool-owned
    `contextvars.ContextVar` as a `Chain`. The chain is an *index*, not a
    value store: each variable's value lives in its own backing
    `contextvars.ContextVar`.

    A chain identifies one serial branch of the async call tree — the
    logical call stack descending from the most recent
    `asyncio.create_task` fork, executing strictly in sequence. It is not a
    single `contextvars.Context`: it spans every context copy descended
    from its arming (event-loop callbacks, `contextvars.Context.run`
    re-entries, the worker-side context a dispatch arms with the caller's
    ID), which is what gives a routine awaited on a worker the same context
    continuity as a local await. `asyncio.create_task` forks onto a fresh
    chain, so concurrent branches never mutate one another's state;
    entering a chain the running execution does not own raises
    `wool.ChainContention`.

    Arming a context with a chain makes the Wool-owned variable a permanent
    member of it: an armed context carries ``1 + N`` Wool-owned variables
    (the chain, plus one backing per bound variable), so a
    `contextvars.copy_context` of it enumerates them all, while an unarmed
    context is indistinguishable from a plain `contextvars.Context`.
    Because chain and backings live in a `contextvars.Context`, Wool state
    obeys standard context semantics — copied into new tasks and event-loop
    callbacks, observed and mutated under `contextvars.Context.run`, and
    isolated to each copy.

    **Lifecycle.** A `Chain` is always *live* — fresh (constructed or
    ``_fork``-ed) or mounted (installed in a context via `mount`).
    Decoded-but-unmounted wire state lives on
    `~wool.runtime.context.manifest.ChainManifest`; `from_manifest`
    installs it.

    :param id:
        UUID identifying the logical execution chain. Re-minted on every
        task fork; a worker preserves the caller's ID when arming an
        inbound wire frame.
    :param thread:
        OS thread (`threading.get_ident`) that owns this chain; the
        contention guard compares it against the accessing thread. Stamped
        by `mount` (``0`` until then).
    :param task:
        Weak reference to the `asyncio.Task` that owns this chain — ``None``
        outside any task (synchronous code, or a `wool.to_thread` worker
        thread). The contention guard compares it against the running task.
        Process-local; never crosses the wire.
    :param vars:
        Bound `wool.ContextVar` instances. ``X in vars`` iff ``X``'s
        backing resolves to a non-`~wool.runtime.typing.Undefined` value in
        the active `contextvars.Context`.
    :param resets:
        ``(namespace, name)`` keys reset to no prior value and not since
        re-set — the token-independent "drop this variable" signal for the
        wire merge, surviving even if the resetting token is collected
        first.
    :param stubs:
        Undeclared-stub `wool.ContextVar` instances observed while decoding
        a manifest, held strongly so a lazy-import receiver can promote them
        on declaration.
    :param unspent_tokens:
        Live (unspent) `wool.Token` IDs per ``(namespace, name)`` key. The
        receiver's mount consults it to decide which wire tokens to anchor
        (see `~wool.runtime.context.token.anchor_tokens`).
    :param spent_tokens:
        Consumed (spent) `wool.Token` IDs per ``(namespace, name)`` key —
        the cross-process single-use ledger. An ID present under its key
        has been reset somewhere in this chain lineage, so any further reset
        of it, in this or any process the chain reaches, raises
        `RuntimeError`.

    .. rubric:: Implementation notes

    **Indexing asymmetry.** ``vars`` keys on `wool.ContextVar` instance
    identity (safe because the process-wide var registry enforces a
    singleton per ``(namespace, name)`` key); ``resets`` keys on the
    ``(namespace, name)`` tuple directly so the reset signal survives a
    wire round-trip without requiring the receiver to have declared the
    variable. The two indices reference the same logical concept under
    different key spaces by design.

    **Token ledgers.** ``unspent_tokens`` and ``spent_tokens`` are excluded
    from identity but snapshotted onto the wire (by `to_manifest`, unioned
    by `from_manifest`), so token bookkeeping rides every frame. A `reset`
    adds the consumed ID to ``spent_tokens``, and a fresh `set` of that var
    retains it: a token consumed off-origin leaves its local ``_used`` flag
    unset, so this ledger is the sole witness of the consumption and pruning
    it would readmit a double reset. The ledger therefore grows for the
    chain's lifetime; bounding that growth to token lifespan is issue #226's
    ownership model.
    """

    id: UUID = field(default_factory=uuid4)
    # A chain instance's thread and task are runtime bookkeeping, not
    # part of the chain's identity, so we exclude them from comparison.
    thread: int = field(default=0, repr=False, compare=False)
    task: weakref.ref[asyncio.Future[Any]] | None = field(
        default=None, repr=False, compare=False
    )
    vars: frozenset[ContextVarManifest[Any]] = field(default_factory=frozenset)
    resets: frozenset[tuple[str, str]] = field(default_factory=frozenset)
    stubs: frozenset[ContextVarManifest[Any]] = field(default_factory=frozenset)
    unspent_tokens: dict[tuple[str, str], frozenset[str]] = field(
        default_factory=dict, repr=False, compare=False
    )
    spent_tokens: dict[tuple[str, str], frozenset[str]] = field(
        default_factory=dict, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        """Coerce the container fields to frozensets."""
        if not isinstance(self.vars, frozenset):
            object.__setattr__(self, "vars", frozenset(self.vars))
        if not isinstance(self.resets, frozenset):
            object.__setattr__(self, "resets", frozenset(self.resets))
        if not isinstance(self.stubs, frozenset):
            object.__setattr__(self, "stubs", frozenset(self.stubs))
        for name in ("unspent_tokens", "spent_tokens"):
            mapping = getattr(self, name)
            if not isinstance(mapping, dict) or not all(
                isinstance(tokens, frozenset) for tokens in mapping.values()
            ):
                object.__setattr__(
                    self,
                    name,
                    {key: frozenset(tokens) for key, tokens in dict(mapping).items()},
                )

    def to_manifest(self) -> ChainManifest:
        """Snapshot this live chain into a decoded `ChainManifest`.

        The send-side counterpart to `from_manifest`. Reads each bound
        variable's value from its backing `contextvars.ContextVar` in the
        *calling* `contextvars.Context` and captures it inline on the
        returned manifest, ready for
        `~wool.runtime.context.manifest.ChainManifest.to_protobuf` to
        serialise. A chain spans many contexts, so the requirement is
        membership, not identity: this method *must* run inside a context
        that carries this chain's bindings — run anywhere else, the reads
        observe that context's values (or none), not this chain's.

        A variable whose backing resolves to
        `~wool.runtime.typing.Undefined` is absent from the snapshot; a
        variable reset to no prior value still rides along via ``resets``
        so the reset propagates regardless of source. This is a pure read
        — it never serialises, so it cannot raise a serialisation error.

        .. rubric:: Implementation notes

        The bound-key singleton and the bound/reset disjointness
        invariants are asserted here, at the snapshot boundary, so any
        manifest reaching ``to_protobuf`` is already well-formed: ``vars``
        holds singletons keyed by ``(namespace, name)`` (enforced at
        construction via ``var_registry`` and `ContextVarCollision`), and
        no key is both bound and reset-pending.
        """
        assert len({(v.namespace, v.name) for v in self.vars}) == len(self.vars), (
            "singleton invariant violated: duplicate var keys in Chain.vars"
        )
        assert not (self.resets & {v._key for v in self.vars}), (
            "disjointness invariant violated: keys both bound and reset-pending in Chain"
        )
        vars: dict[ContextVarManifest[Any], Any] = {}
        for var in self.vars:
            value = var._backing.get(Undefined)
            if value is Undefined:
                continue
            vars[var] = value
        return ChainManifest(
            id=self.id,
            vars=vars,
            resets=self.resets,
            stubs=self.stubs,
            spent_tokens=self.spent_tokens,
            unspent_tokens=self.unspent_tokens,
        )

    def mount(
        self,
        *,
        owned: bool = True,
        wire_tokens: Iterable[Token] = (),
        **changes: Any,
    ) -> Chain:
        """Arm this chain as the live chain in the current `contextvars.Context`.

        Installs an evolved copy of this chain — re-stamped so the
        current thread (and, when *owned*, the owning task) hold it —
        and applies each `resets` signal by rewinding the matching
        backing, when declared in this process, to
        `~wool.runtime.typing.Undefined` (idempotent for resets the
        chain already carried). Must run inside the owning task's real
        `contextvars.Context` so backing-variable state and any native
        tokens minted afterward bind to it.

        ``mount`` is the shared arming step. The local path
        (`wool.ContextVar.set` on first-arm, `wool.ContextVar.reset`,
        and the per-task fork) arms an already-live chain directly; the
        wire-ingress path arrives through `from_manifest`, which drains
        and merges the decoded state into a fresh chain and then
        delegates here.

        ``mount`` is also the keystone arming boundary: every arming —
        caller-side or wire-ingress — transits through here and
        unconditionally ensures Wool's task factory is on the running
        loop via
        `~wool.runtime.context.factory.ensure_task_factory_installed`
        (a no-op once installed). That call doubles as the displacement
        tripwire: if a third-party factory has displaced Wool's, it
        raises `wool.TaskFactoryDisplaced` — so a worker-side mount
        surfaces displacement at frame ingress, not only on the next
        `wool.ContextVar` set.

        :param owned:
            When ``True`` (the default), stamp the current asyncio task
            as the chain's owner so the cross-task contention guard
            (`assert_chain_owner`) can arbitrate by task identity. The
            worker-side per-step driver passes ``False``: its cached
            `contextvars.Context` is driven by a succession of distinct
            step-tasks, so the chain is owned thread-wise and
            task-agnostically — stamping any one step-task would make
            the next trip the guard. The OS-thread stamp is applied
            either way.
        :param wire_tokens:
            Wire tokens reconstituted during the frame decode, anchored
            here if live in this chain — see
            `~wool.runtime.context.token.anchor_tokens` for the gate. Empty
            for the local arming paths.
        :param changes:
            Optional chain-delta keywords folded into the single owner-stamp
            evolve — the set path passes its ``vars``/``resets``/token-map
            update here so arming builds one Chain, not two. Empty for the
            reset, fork, and wire-ingress paths, which arm an already-evolved
            chain.
        :returns:
            The installed Chain — the evolved copy with the owner
            re-stamped.

        .. rubric:: Implementation notes

        ``anchor_tokens`` runs here because mount is the arming boundary
        that executes inside the chain's target context (the worker's
        cached context or the caller's task) — exactly where a reset must
        be valid from.

        Both the anchor pass and the reset-drain read the *installed*
        copy rather than ``self``, so a ``resets`` delta folded in through
        *changes* is already in effect: a set-path fold that cleared a
        var's key leaves that var's backing untouched by the drain, and
        `wool.ContextVar.set` writes the new value straight after mount.
        """
        ensure_task_factory_installed()
        task = None
        if owned:
            try:
                task = asyncio.current_task()
            except RuntimeError:
                pass
        # Single evolve: fold the owner stamp and *changes* into one Chain.
        installed = self._evolve(
            thread=threading.get_ident(),
            task=weakref.ref(task) if task is not None else None,
            **changes,
        )
        # Anchor the wire tokens live in this chain; a no-op when empty.
        anchor_tokens(wire_tokens, installed)
        # Rewind each reset signal's backing to Undefined.
        for key in installed.resets:
            receiver_var = var_registry.get(key)
            if receiver_var is not None:
                receiver_var._backing.set(Undefined)
        wool.__chain__.set(installed)
        return installed

    @classmethod
    def from_manifest(
        cls,
        manifest: ChainManifest,
        *,
        owned: bool,
        merge_with: Chain | None = None,
        wire_tokens: Iterable[Token] = (),
    ) -> Chain:
        """Drain a decoded `ChainManifest` into the backings and arm it.

        The wire-ingress counterpart to `to_manifest`, entered through
        `Frame.mount`. Drains the manifest's decoded values into their
        backing variables, builds the receiver chain — merging onto a live
        receiver when *merge_with* is given (the receiver keeps its chain
        ID), or seeding fresh state from the manifest alone when it is
        ``None`` — and delegates the arming (reset rewind, owner stamp,
        task factory, `wool.__chain__` set) to `mount`.

        :param manifest:
            The decoded-but-unmounted chain snapshot to install.
        :param owned:
            Forwarded to `mount`. Caller-side mounts pass ``True``; the
            worker-side per-step driver passes ``False`` because the
            cached `contextvars.Context` is driven by successive
            step-tasks rather than owned by one.
        :param merge_with:
            The live receiver chain to union the manifest onto, or ``None``
            to seed a fresh chain from the manifest alone.
        :param wire_tokens:
            Forwarded to `mount`, which anchors the ones live in the
            mounted chain — see `~wool.runtime.context.token.anchor_tokens`
            for the gate.

        .. rubric:: Implementation notes

        Values drain into the backings first so the built `Chain` observes
        them through ``vars``. Those backing writes mint stray native
        tokens, discarded with the local scope.

        On a merge, both token maps union per var: an awaited dispatch
        continues the caller's context, so a token live (or spent) on
        either side of the hop is live (or spent) in the continuation. The
        spent union is what rejects here a token already reset upstream.

        The anchor ledger (``unspent_tokens``) is then pruned by the
        consumed ledger (``spent_tokens``) on both the fresh and merged
        paths, so an ID a reset has spent never lingers as anchor-eligible.
        This drops no reachable reset — `wool.ContextVar.reset`'s used-gate
        rejects a spent ID before the anchor gate is consulted — while
        bounding the anchor ledger to live tokens rather than growing it
        one ID per frame.

        Reset-only keys drop out of ``vars`` so the reset propagates; the
        backing rewind itself happens in `mount`.
        """
        # Drain manifest values into the backings first.
        for var, value in manifest.vars.items():
            var._backing.set(value)
        # Build the vars index, unioned with the receiver on a merge.
        manifest_vars = frozenset(manifest.vars.keys())
        if merge_with is None:
            merged_vars = manifest_vars
            resets = manifest.resets
            stubs = manifest.stubs
            chain_id = manifest.id
            unspent_tokens = dict(manifest.unspent_tokens)
            spent_tokens = dict(manifest.spent_tokens)
        else:
            merged_vars = merge_with.vars | manifest_vars
            manifest_touched = {var._key for var in manifest_vars} | manifest.resets
            resets = manifest.resets | (merge_with.resets - manifest_touched)
            stubs = merge_with.stubs | manifest.stubs
            chain_id = merge_with.id
            # Union both token maps per var across the hop.
            unspent_tokens = dict(merge_with.unspent_tokens)
            for key, tokens in manifest.unspent_tokens.items():
                unspent_tokens[key] = unspent_tokens.get(key, frozenset()) | tokens
            spent_tokens = dict(merge_with.spent_tokens)
            for key, tokens in manifest.spent_tokens.items():
                spent_tokens[key] = spent_tokens.get(key, frozenset()) | tokens
        # Prune spent IDs out of the anchor ledger, both paths.
        for key, spent in spent_tokens.items():
            live = unspent_tokens.get(key, frozenset()) - spent
            if live:
                unspent_tokens[key] = live
            else:
                unspent_tokens.pop(key, None)
        # Drop reset-only keys from vars; mount does the rewind.
        for key in manifest.resets:
            receiver_var = var_registry.get(key)
            if receiver_var is not None:
                merged_vars = merged_vars - {receiver_var}
        target = cls(
            id=chain_id,
            vars=merged_vars,
            resets=resets,
            stubs=stubs,
            unspent_tokens=unspent_tokens,
            spent_tokens=spent_tokens,
        )
        return target.mount(owned=owned, wire_tokens=wire_tokens)

    def _evolve(self, **changes: Any) -> Chain:
        """Return a copy of this chain with *changes* applied."""
        return replace(self, **changes)

    def _fork(self) -> Chain:
        """Fork this chain into a fresh logical chain.

        The fork inherits the variable bindings (the ``vars`` index) and
        stub pins on a freshly minted ``id``, with no owner stamps — like
        any fresh Chain, it acquires its owners at the subsequent `mount`.
        The reset signals and token ledgers are dropped, so the fork
        starts clean: a `wool.Token` minted in the parent chain is already
        incompatible with the fork's chain for `wool.ContextVar.reset`.
        The backing variables' values ride the fork natively — the forked
        task runs in a `contextvars.copy_context` copy. This is the
        copy-on-fork the task factory applies at every task creation.

        **Re-handoff is undefined behaviour.** A fork minted on one
        thread/task and then re-driven elsewhere — e.g., a
        `wool.to_thread` worker's fork captured back into the
        loop thread and passed to `asyncio.create_task` —
        is unsupported. It will fail loudly only at the next
        `wool.ContextVar` access on the re-handed chain, via
        the chain-contention guard. Forks are intended to be owned and
        retired by the scope that created them.

        .. rubric:: Implementation notes

        ``unspent_tokens`` is dropped because the child snapshots none of
        the parent's live token IDs under any var, so a parent-minted wire
        token finds no anchor in the child (see
        `~wool.runtime.context.token.anchor_tokens`). ``spent_tokens`` is
        dropped too: a parent-spent token cannot be re-reset in the child
        — its native anchor belongs to the parent's `contextvars.Context`,
        so reset raises there regardless — so the child need carry no
        consumed ledger, the same bound-per-branch discipline the live IDs
        follow.
        """
        return Chain(
            id=uuid4(),
            vars=self.vars,
            stubs=self.stubs,
        )
