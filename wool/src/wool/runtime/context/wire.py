"""Wire codec helpers for the Wool chain context.

Houses the two functions that bridge the live :class:`Context` model
to the protobuf wire envelope:

* :func:`current_wire_context` — encode the active Wool chain to
  :class:`protocol.Context`. Returns ``None`` when the surrounding
  context is unarmed so callers can omit the optional wire field.
* :func:`_install_manifest` — drain a decoded
  :class:`~wool.runtime.context.manifest._ContextManifest` into the
  matching backing variables, apply reset signals, and install a
  fresh :class:`Context` capturing the resulting state. Single
  source of truth for both caller-side (Frame.mount) and worker-side
  (per-step driver) install paths.
* :func:`_apply_manifest` — back-compat wrapper around
  :func:`_install_manifest` for the worker-side ``ctx.run`` site
  that historically called this name.

Distinct from :mod:`wool.runtime.context.base` (the chain-state
data model) and :mod:`wool.runtime.context.manifest` (the
decoded-but-unmounted view): :mod:`~wool.runtime.context.wire`
owns the encode/apply side of the wire round-trip, so the model
modules stay focused on chain semantics and the wire bindings stay
focused on the protobuf surface.
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from wool import protocol
from wool.runtime.context.base import Context
from wool.runtime.context.base import _install_context
from wool.runtime.context.base import current_context
from wool.runtime.context.registry import var_registry
from wool.runtime.typing import Undefined

if TYPE_CHECKING:
    from wool.runtime.context.manifest import _ContextManifest
    from wool.runtime.serializer import Serializer


def _install_manifest(
    manifest: "_ContextManifest",
    *,
    stamp_owner: bool,
    install_factory: bool,
    merge_with: Context | None = None,
) -> Context:
    """Single install pipeline for caller-side and worker-side mounts.

    Q1 (part 2) + Q2 (final): collapses the three previous install
    pipelines (:meth:`Context.mount` after :meth:`Context._update`,
    :meth:`Context.mount` after :meth:`Context._from_manifest`, and
    :func:`_apply_manifest`) into one function. The variations among
    the three sites — owner stamping, task-factory installation, and
    receiver-side merge — are expressed as keyword arguments, so the
    drain/reset/install body lives in exactly one place.

    The drain into backings runs first so any subsequent
    :class:`Context.to_protobuf` observation reads the values via the
    backings (the post-Q28 contract). Reset-only keys rewind their
    backings to :data:`~wool.runtime.typing.Undefined`. The new
    :class:`Context` records the manifest's chain id (or the merge
    target's chain id when *merge_with* is provided — the receiver
    keeps its chain identity through a merge) and a ``data`` index
    derived from the manifest (and optionally unioned with the merge
    target's bindings).

    :param manifest:
        The decoded wire state to install.
    :param stamp_owner:
        When ``True``, stamp the current thread and task as the
        chain owners. Caller-side mount sites pass ``True``; the
        worker-side per-step driver passes ``False`` because the
        cached :class:`contextvars.Context` already owns the chain.
    :param install_factory:
        When ``True``, ensure Wool's task factory is installed on
        the running loop. Caller-side mounts pass ``True``; the
        worker-side driver bypasses the factory entirely (see
        :func:`~wool.runtime.worker.session._create_step_task`) so
        passes ``False``.
    :param merge_with:
        When provided, union *manifest*'s data with the receiver's
        bindings and preserve the receiver's chain id — the live-
        receiver branch of the caller-side mount pattern. ``None``
        is the unarmed-receiver and worker-side install path: a
        fresh :class:`Context` is built from the manifest alone.

    :returns:
        The newly installed :class:`Context`.

    :raises ContextDecodeError:
        When ``manifest.decode_error`` is set (strict-mode wire-
        decode failure deferred to this site).
    """
    if manifest.decode_error is not None:
        raise manifest.decode_error
    # Local imports break the ``base.py ↔ wire.py`` and ``base.py ↔
    # factory.py`` cycles — base.py imports wire.py at module load,
    # so wire.py cannot pull ``_current_owner_ref`` from base.py at
    # the same time. Factory import is similarly deferred for the
    # existing cycle.
    from wool.runtime.context.base import _current_owner_ref

    if install_factory:
        from wool.runtime.context.factory import _ensure_task_factory_installed

        _ensure_task_factory_installed()
    # Drain values into backings first so the resulting Wool Context
    # observes them in ``data``. Backing writes mint stray native
    # tokens — discarded with the local scope.
    for var, value in manifest.decoded_vars.items():
        var._backing.set(value)
    # Apply reset signals: a variable reset to no prior value on the
    # sender's chain rewinds to the Undefined sentinel here.
    for key in manifest.reset_vars:
        receiver_var = var_registry.get(key)
        if receiver_var is not None:
            receiver_var._backing.set(Undefined)
    # Build the data index from the manifest. Reset-only keys are
    # excluded — the backing rewound to Undefined and the var is no
    # longer bound in this chain.
    manifest_data = frozenset(manifest.decoded_vars.keys())
    if merge_with is None:
        data = manifest_data
        reset_vars = manifest.reset_vars
        stub_pins = manifest.stub_pins
        chain_id = manifest.chain_id
    else:
        # Live-receiver merge: union receiver's bindings with the
        # manifest's, preserve the receiver's chain id, and let the
        # manifest's resets win for keys it touched (the receiver's
        # resets survive only for keys the manifest did not touch).
        data = merge_with.data | manifest_data
        manifest_touched = {var._key for var in manifest_data} | manifest.reset_vars
        reset_vars = manifest.reset_vars | (merge_with.reset_vars - manifest_touched)
        stub_pins = merge_with.stub_pins | manifest.stub_pins
        chain_id = merge_with.chain_id
    # Reset-only keys drop out of ``data`` so the reset propagates.
    for key in manifest.reset_vars:
        receiver_var = var_registry.get(key)
        if receiver_var is not None:
            data = data - {receiver_var}
    # Build and install. ``_owning_task`` is set only when the
    # caller-side mount requests it — the worker-side driver leaves
    # it ``None`` so ``_assert_chain_owner`` falls back to the
    # registry-driven isolation between chains.
    installed = Context(
        chain_id=chain_id,
        _owning_thread=threading.get_ident(),
        _owning_task=_current_owner_ref() if stamp_owner else None,
        data=data,
        reset_vars=reset_vars,
        stub_pins=stub_pins,
    )
    _install_context(installed)
    return installed


def _apply_manifest(manifest: "_ContextManifest") -> None:
    """Worker-side install entry point: no owner stamps, no factory.

    Back-compat wrapper around :func:`_install_manifest`. The
    worker's per-step driver schedules ``ctx.run(_apply_manifest,
    manifest)`` inside the chain's cached
    :class:`contextvars.Context`; the cached context already owns
    the chain and the driver bypasses the task factory entirely, so
    neither stamping nor factory-install is needed.
    """
    _install_manifest(
        manifest,
        stamp_owner=False,
        install_factory=False,
    )


def current_wire_context(*, serializer: "Serializer") -> protocol.Context | None:
    """Encode the active Wool chain to wire, or return ``None`` when unarmed.

    Single funnel for the "encode current chain" pattern used by the
    worker connection (caller-side request frames) and worker session
    (worker-side response frames). When the surrounding context is
    unarmed there is nothing to propagate, so the function returns
    :data:`None`; callers omit the optional ``context`` /
    ``caller_wire_context`` field on the wire frame entirely
    (proto3's HasField semantics). A receiver that sees the field
    absent skips the apply path; a receiver that sees it present
    knows the sender intentionally propagated a real chain.
    """
    current = current_context()
    if current is None:
        return None
    return current.to_protobuf(serializer=serializer)


__all__ = ["_apply_manifest", "_install_manifest", "current_wire_context"]
