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
from wool.runtime.typing import Undefined

if TYPE_CHECKING:
    from wool.runtime.context.manifest import ContextVarManifest


@dataclass(frozen=True, eq=False)
class Chain:
    """Immutable index of Wool chain state.

    Wool chain state — i.e., the logical-chain UUID, the set of bound
    `wool.ContextVar` instances, and their resets — rides in a
    single Wool-owned `contextvars.ContextVar` as a
    `Chain` instance. The chain is an *index*, not a value
    store: each `wool.ContextVar`'s value lives in its own
    backing `contextvars.ContextVar`.

    A chain identifies one serial branch of the program's async call
    tree: the logical call stack descending from the most recent
    `asyncio.create_task` fork, on which every frame executes
    strictly in sequence. The branch is not one
    `contextvars.Context` — it spans every context copy
    descended from its arming, e.g., event-loop callback copies, explicit
    `contextvars.Context.run` re-entries, even the worker-side
    context a dispatch arms with the caller's chain id — which is what
    gives a routine awaited on a worker the same context continuity
    as a process-local await. `asyncio.create_task` always
    forks onto a fresh chain, the chain-level analogue of stdlib
    copy-on-fork, so concurrent branches can never mutate one
    another's chain state; an execution that enters a chain it does
    not own has circumvented that discipline and fails loudly instead
    (see `wool.ChainContention`).

    The Wool-owned `contextvars.ContextVar` becomes a permanent
    member of any `contextvars.Context` once that context is
    armed with a `Chain` instance. An armed context
    additionally carries one backing variable per bound
    `wool.ContextVar`, so a `contextvars.copy_context`
    of an armed context enumerates ``1 + N`` Wool-owned variables.
    An unarmed context holds none of them and is indistinguishable
    from a plain `contextvars.Context`. A context is armed when a
    chain is mounted into it (see *Lifecycle*).

    Because the chain and every backing context variable live in a
    `contextvars.Context`, Wool state is governed by standard
    context semantics: it's copied into new tasks at creation (by
    default) and into event-loop callbacks at scheduling; code run
    explicitly inside a captured context — e.g., via
    `contextvars.Context.run` — observes and mutates the Wool
    state that context carries; and writes made inside a copy stay in
    that copy, never leaking back into the originating scope.

    **Lifecycle**

    A `Chain` instance is strictly *live*:
    fresh (constructed directly or via `_fork`) or mounted
    (installed in a `contextvars.Context` via `mount`).
    Decoded-but-unmounted wire state lives on
    `~wool.runtime.context.manifest.ChainManifest` and is
    installed by `from_manifest`. `mount` is the single transition
    from pure data → installed: it stamps ``thread`` / ``task`` from
    the calling scope and applies any pending resets.

    **Indexing asymmetry**

    ``vars`` keys on `wool.ContextVar`
    instance identity (safe because the process-wide var registry
    enforces a singleton per ``(namespace, name)`` key); ``resets``
    keys on the ``(namespace, name)`` tuple directly so the reset
    signal survives a wire round-trip without requiring the receiver
    to have declared the variable. The two indices reference the same
    logical concept under different key spaces by design.

    :param id:
        UUID identifying the logical execution chain. Defaults to a
        freshly minted UUID. Re-minted on every task fork by the Wool
        task factory; an explicit value lets a worker preserve the
        caller's chain id when arming on an inbound wire frame.
    :param thread:
        `threading.get_ident` of the OS thread that owns this
        chain — stamped by `mount`. Defaults to ``0`` (no owner)
        so construction is pure data; `mount` is the single
        owner-stamping site. The chain-contention guard compares it
        against the accessing thread; see `wool.ChainContention`.
    :param task:
        Weak reference to the `asyncio.Task` that owns this
        chain — stamped by `mount`. Defaults to ``None`` (no
        owner). ``None`` also marks a chain armed outside any task
        (synchronous code, or a `wool.to_thread` worker thread).
        The chain-contention guard compares it against the running
        task so a second task entering the chain fails loud. This
        field is process-local and never crosses the wire.
    :param vars:
        Set of bound `wool.ContextVar` instances. Membership
        is the index of "bound in this chain": ``X in vars`` is
        equivalent to ``X._backing`` resolving to a
        non-`~wool.runtime.typing.Undefined` value in the active
        `contextvars.Context`.
    :param resets:
        ``(namespace, name)`` keys of variables reset to no prior
        value and not since re-set. The token-independent "drop this
        variable" signal for the wire merge (`from_manifest`); survives
        even if the resetting token is collected before the reset
        propagates.
    :param stubs:
        Undeclared-stub `wool.ContextVar` instances observed
        while decoding a chain manifest, held strongly so a lazy-import
        receiver can still promote them when it declares the variable.
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

    def __post_init__(self) -> None:
        """Coerce the container fields to frozensets.

        Callers — including `dataclasses.replace` — may pass any
        iterable; coercion preserves the frozen facade regardless.
        """
        if not isinstance(self.vars, frozenset):
            object.__setattr__(self, "vars", frozenset(self.vars))
        if not isinstance(self.resets, frozenset):
            object.__setattr__(self, "resets", frozenset(self.resets))
        if not isinstance(self.stubs, frozenset):
            object.__setattr__(self, "stubs", frozenset(self.stubs))

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
        so the reset propagates regardless of source.

        This is a pure read — it never serialises, so it cannot raise a
        serialisation error. The bound-key singleton and the
        bound/reset disjointness invariants are asserted here, at the
        snapshot boundary, so any manifest reaching ``to_protobuf`` is
        already well-formed: ``vars`` holds singletons keyed by
        ``(namespace, name)`` (enforced at construction via
        ``var_registry`` and `ContextVarCollision`), and no key is both
        bound and reset-pending.
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
        )

    def mount(self, *, owned: bool = True) -> Chain:
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
        :returns:
            The installed Chain — the evolved copy with the owner
            re-stamped.
        """
        ensure_task_factory_installed()
        for key in self.resets:
            receiver_var = var_registry.get(key)
            if receiver_var is not None:
                receiver_var._backing.set(Undefined)
        task = None
        if owned:
            try:
                task = asyncio.current_task()
            except RuntimeError:
                pass
        installed = self._evolve(
            thread=threading.get_ident(),
            task=weakref.ref(task) if task is not None else None,
        )
        wool.__chain__.set(installed)
        return installed

    @classmethod
    def from_manifest(
        cls,
        manifest: ChainManifest,
        *,
        owned: bool,
        merge_with: Chain | None = None,
    ) -> Chain:
        """Drain a decoded `ChainManifest` into the backings and arm it.

        The wire-ingress counterpart to `to_manifest`, entered through
        `Frame.mount`. Drains the manifest's decoded values into their
        backing variables, builds the receiver chain — merging onto a live
        receiver when *merge_with* is given (the receiver keeps its chain
        id), or seeding fresh state from the manifest alone when it is
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
        """
        # Drain values into the backings first so the built Chain
        # observes them via ``vars``. Backing writes mint stray native
        # tokens — discarded with the local scope.
        for var, value in manifest.vars.items():
            var._backing.set(value)
        # Build the vars index from the manifest, optionally unioned with
        # the live receiver's bindings (the receiver keeps its chain id
        # through the merge).
        manifest_vars = frozenset(manifest.vars.keys())
        if merge_with is None:
            merged_vars = manifest_vars
            resets = manifest.resets
            stubs = manifest.stubs
            chain_id = manifest.id
        else:
            merged_vars = merge_with.vars | manifest_vars
            manifest_touched = {var._key for var in manifest_vars} | manifest.resets
            resets = manifest.resets | (merge_with.resets - manifest_touched)
            stubs = merge_with.stubs | manifest.stubs
            chain_id = merge_with.id
        # Reset-only keys drop out of ``vars`` so the reset propagates; the
        # rewind itself happens in mount.
        for key in manifest.resets:
            receiver_var = var_registry.get(key)
            if receiver_var is not None:
                merged_vars = merged_vars - {receiver_var}
        target = cls(id=chain_id, vars=merged_vars, resets=resets, stubs=stubs)
        return target.mount(owned=owned)

    def _evolve(self, **changes: Any) -> Chain:
        """Return a copy of this chain with *changes* applied."""
        return replace(self, **changes)

    def _fork(self) -> Chain:
        """Fork this chain into a fresh logical chain.

        The fork inherits the variable bindings (the ``vars`` index)
        and stub pins on a freshly minted ``id``, with no owner
        stamps — like any fresh Chain, it acquires its owners at the
        subsequent `mount`. The reset signals are dropped: a
        `wool.Token` minted in the parent chain is already
        incompatible with the fork's chain for
        `wool.ContextVar.reset`, so the fork starts clean. The
        backing variables' values ride the fork natively — the forked
        task runs in a `contextvars.copy_context` copy. This is
        the copy-on-fork the task factory applies at every task
        creation.

        **Re-handoff is undefined behaviour.** A fork minted on one
        thread/task and then re-driven elsewhere — e.g., a
        `wool.to_thread` worker's fork captured back into the
        loop thread and passed to `asyncio.create_task` —
        is unsupported. It will fail loudly only at the next
        `wool.ContextVar` access on the re-handed chain, via
        the chain-contention guard. Forks are intended to be owned and
        retired by the scope that created them.
        """
        return Chain(
            id=uuid4(),
            vars=self.vars,
            stubs=self.stubs,
        )
