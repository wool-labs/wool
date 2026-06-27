"""The chain-state model.

Provides `Chain`: the immutable record of a logical
execution chain that the rest of the context subsystem mutates,
ships across the wire, forks per task, and polices for contention.
"""

from __future__ import annotations

import asyncio
import threading
import warnings
import weakref
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from typing import TYPE_CHECKING
from typing import Any
from uuid import UUID
from uuid import uuid4

import wool
from wool import protocol
from wool.runtime.context.exceptions import ChainSerializationError
from wool.runtime.context.exceptions import SerializationWarning
from wool.runtime.context.factory import _ensure_task_factory_installed
from wool.runtime.context.registry import var_registry
from wool.runtime.typing import Undefined

if TYPE_CHECKING:
    from wool.runtime.context.var import ContextVar
    from wool.runtime.serializer import Serializer


def _current_task() -> asyncio.Future[Any] | None:
    """Return the running task, or ``None`` outside a running task."""
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


@dataclass(frozen=True, eq=False)
class Chain:
    """Immutable index of Wool chain state.

    Wool chain state — i.e., the logical-chain UUID, the set of bound
    `wool.ContextVar` instances, and their resets — rides in a
    single Wool-owned `contextvars.ContextVar` as a
    `wool.Chain` instance. The chain is an *index*, not a value
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
    armed with a `wool.Chain` instance. An armed context
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
    installed by `~wool.runtime.context.manifest.ChainManifest.mount`.
    `mount` is the single transition from pure data →
    installed: it stamps ``thread`` / ``task`` from the calling
    scope and applies any pending resets.

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
        variable" signal for the wire merge
        (`~wool.runtime.context.manifest.ChainManifest.mount`);
        survives even if the resetting token is collected before the
        reset propagates.
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
    vars: frozenset[ContextVar[Any]] = field(default_factory=frozenset)
    resets: frozenset[tuple[str, str]] = field(default_factory=frozenset)
    stubs: frozenset[ContextVar[Any]] = field(default_factory=frozenset)

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

    def _evolve(self, **changes: Any) -> Chain:
        """Return a copy of this chain with *changes* applied."""
        return replace(self, **changes)

    def to_protobuf(
        self,
        *,
        serializer: Serializer | None = None,
    ) -> protocol.ChainManifest:
        """Encode this chain to a wire `protocol.ChainManifest`.

        Each observable variable in the context, i.e., carrying a value or
        in an unset state, emits one `protocol.ContextVar` entry.
        Default-only variables are absent.

        **Value source**

        `Chain` is strictly live, so each entry's ``value`` is
        read from the variable's backing `contextvars.ContextVar`
        in the *calling* `contextvars.Context`. A chain spans
        many contexts, so the requirement is membership, not identity:
        this method *must* run inside a context that carries this
        chain's bindings — run anywhere else, the reads observe that
        context's values (or none), not this chain's.

        A variable reset to no prior value still rides the wire, with
        no ``value``, so the reset propagates regardless of source.

        Per-variable encode failures emit `SerializationWarning`
        and the offending key is suppressed entirely — its reset
        signal included, so the receiver cannot read a phantom reset
        for a variable whose value failed to ship. Under strict mode
        (``PYTHONWARNINGS=error::wool.SerializationWarning``) the
        failures aggregate into a single `ChainSerializationError`
        raised after the loop.

        Encoding is deterministic: entries are emitted in sorted key
        order, so identical chain state encodes to byte-identical
        frames across processes — preserving content-addressed
        caching and replay-style fingerprinting despite
        hash-randomised set iteration.

        The wire's ``vars`` field carries both kinds of entry: a
        bound variable (from ``self.vars``) emits an entry with a
        ``value``, and a reset-pending key (from ``self.resets``)
        emits a value-less entry. Because the two source sets are
        disjoint by construction, every key emits exactly one entry — a
        key in both would emit a value entry and a reset signal for
        the same variable, leaving receiver state dependent on wire
        entry order. Entries are likewise unique by
        ``(namespace, name)``: ``self.vars`` holds singletons keyed
        by ``(namespace, name)``, enforced at construction via
        ``var_registry`` and `ContextVarCollision`. Both
        invariants are assert-checked at encode time. Receivers may
        treat duplicate keys on the wire as undefined behaviour — Wool's
        encoder will never produce them.

        :raises ChainSerializationError:
            Under strict mode, when one or more variables fail to encode.
        """
        # Singleton and disjointness invariants — see the docstring.
        assert len({(v.namespace, v.name) for v in self.vars}) == len(self.vars), (
            "singleton invariant violated: duplicate var keys in Chain.vars"
        )
        assert not (self.resets & {v._key for v in self.vars}), (
            "disjointness invariant violated: keys both bound and reset-pending in Chain"
        )
        if serializer is None:
            serializer = wool.__serializer__
        wire = protocol.ChainManifest(id=self.id.hex)
        failures: list[SerializationWarning] = []
        encoded_values: dict[tuple[str, str], bytes] = {}
        failed_keys: set[tuple[str, str]] = set()
        for var in self.vars:
            value = var._backing.get(Undefined)
            if value is Undefined:
                continue
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
        if failures:
            raise ChainSerializationError(*failures)
        return wire

    def mount(self, *, stamp_owner: bool = True, install_factory: bool = True) -> Chain:
        """Arm this chain as the live chain in the current `contextvars.Context`.

        Installs an evolved copy of this chain — re-stamped so the
        current thread (and, with ``stamp_owner``, the owning task)
        hold it — and applies each `resets` signal by rewinding the
        matching backing, when declared in this process, to
        `~wool.runtime.typing.Undefined` (idempotent for resets the
        chain already carried). Must run inside the owning task's real
        `contextvars.Context` so backing-variable state and any native
        tokens minted afterward bind to it.

        ``mount`` is the shared arming step. The local path
        (`wool.ContextVar.set` on first-arm, `wool.ContextVar.reset`,
        and the per-task fork) arms an already-live chain directly; the
        wire-ingress path arrives through `ChainManifest.mount`, which
        drains and merges the decoded state into a fresh chain and then
        delegates here.

        ``mount`` is also the keystone arming boundary: every local
        arming transits through here, ensuring Wool's task factory is
        on the running loop via
        `~wool.runtime.context.factory._ensure_task_factory_installed`
        (see that function for the install semantics).

        :param stamp_owner:
            When ``True`` (the local default), stamp the current task
            as the chain owner. The worker-side per-step driver passes
            ``False`` so the cached `contextvars.Context` keeps
            ownership and `_assert_chain_owner` falls back to
            registry-driven isolation.
        :param install_factory:
            When ``True`` (the local default), ensure Wool's task
            factory is on the running loop. The worker-side per-step
            driver passes ``False`` to bypass the loop factory.
        :returns:
            The installed Chain — the evolved copy with owner stamps
            re-applied.
        """
        if install_factory:
            _ensure_task_factory_installed()
        for key in self.resets:
            receiver_var = var_registry.get(key)
            if receiver_var is not None:
                receiver_var._backing.set(Undefined)
        owner = _current_task() if stamp_owner else None
        installed = self._evolve(
            thread=threading.get_ident(),
            task=weakref.ref(owner) if owner is not None else None,
        )
        wool.__chain__.set(installed)
        return installed

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
