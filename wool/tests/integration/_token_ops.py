"""Op-script model for cross-process wool.Token parity checks.

NOT a test file — no ``test_`` prefix — so pytest does not collect it.

A *script* is a sequence of frozen op dataclasses (:class:`SetOp`,
:class:`ResetOp`, :class:`GetOp`), each tagged with the actor that
performs it: ``"caller"`` (the dispatching test process) or
``"worker"`` (a pool subprocess, reached through the key-addressed
``oracle_*`` routines in :mod:`tests.integration.routines`). One script
can be executed two ways:

* :func:`run_stdlib_oracle` — locally, against a fresh
  :class:`contextvars.ContextVar`, ignoring actor tags entirely. This
  is the ground truth: a Wool dispatch chain is a logical continuation
  of one context, so actor placement must not change outcomes.
* :func:`run_wool_script` — against a :class:`wool.ContextVar` inside
  an entered pool context, with caller ops applied locally and worker
  ops dispatched to real worker subprocesses.

Both executors return one :class:`Observation` per op;
:func:`observations_match` compares the traces and
:func:`describe_mismatch` renders a per-op diff for assertion messages.

Script values must be picklable primitives (int/str) — worker ops ship
them across the dispatch wire — and must not collide with the
:data:`UNSET` sentinel string.
"""

from __future__ import annotations

import contextvars
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import zip_longest
from typing import Any
from typing import Final

from hypothesis import strategies as st

import wool

from . import routines

#: Actor tag for ops executed locally by the dispatching test process.
CALLER: Final = "caller"

#: Actor tag for ops dispatched to a worker subprocess.
WORKER: Final = "worker"

_ACTORS: Final = (CALLER, WORKER)

#: Sentinel recorded as an :class:`Observation` value when the variable
#: is unbound. Shared with the worker-side oracle routines so worker
#: and caller observations of "unset" compare equal.
UNSET: Final = routines.ORACLE_UNSET


@dataclass(frozen=True)
class _Op:
    """Base op: the actor that performs it (``CALLER`` or ``WORKER``)."""

    actor: str

    def __post_init__(self) -> None:
        if self.actor not in _ACTORS:
            raise ValueError(f"actor must be one of {_ACTORS}, got {self.actor!r}")


@dataclass(frozen=True)
class SetOp(_Op):
    """Set the variable to *value* (a picklable int/str)."""

    value: Any


@dataclass(frozen=True)
class ResetOp(_Op):
    """Reset the variable with the *token_index*-th minted token.

    ``token_index`` indexes the ordered list of tokens the script has
    minted so far, normalized modulo the list length — indexes are
    drawn with replacement, so an already-consumed token may be
    retried. A reset drawn before any set is a no-op observation.
    """

    token_index: int


@dataclass(frozen=True)
class GetOp(_Op):
    """Read the variable's current value."""


Op = SetOp | ResetOp | GetOp


@dataclass(frozen=True)
class Observation:
    """Per-op record comparable between the wool and stdlib executors.

    ``kind`` is the op kind (``"set"`` / ``"reset"`` / ``"get"``);
    ``value`` is the variable's value observed at the op's actor site
    after the op (:data:`UNSET` when unbound); ``error`` is the raised
    — or worker-reported — exception class name, ``None`` on success.
    """

    kind: str
    value: Any
    error: str | None


def scripts(*, max_ops: int = 12) -> st.SearchStrategy[list[Op]]:
    """Hypothesis strategy drawing op scripts over both actors.

    Values are picklable primitives (int/str) per the module contract;
    token indexes are drawn with replacement over ``range(max_ops)``
    and normalized modulo the minted-token count at execution time.
    """
    actors = st.sampled_from(_ACTORS)
    ops = st.one_of(
        st.builds(SetOp, actor=actors, value=st.integers() | st.text()),
        st.builds(
            ResetOp,
            actor=actors,
            token_index=st.integers(min_value=0, max_value=max_ops - 1),
        ),
        st.builds(GetOp, actor=actors),
    )
    return st.lists(ops, max_size=max_ops)


def run_stdlib_oracle(script: Sequence[Op]) -> list[Observation]:
    """Execute *script* on a fresh stdlib ContextVar in one local context.

    Actor tags are ignored — every op runs locally. That is the point:
    location transparency means actor placement must not change
    outcomes, so this all-local trace is the ground truth
    :func:`run_wool_script` is compared against. Reset rejections are
    captured as the exception class name; the observed value is
    ``var.get(UNSET)`` after each op.
    """
    var: contextvars.ContextVar[Any] = contextvars.ContextVar(
        f"token_ops_oracle_{uuid.uuid4().hex}"
    )
    tokens: list[contextvars.Token[Any]] = []
    observations: list[Observation] = []
    for op in script:
        error: str | None = None
        if isinstance(op, SetOp):
            kind = "set"
            tokens.append(var.set(op.value))
        elif isinstance(op, ResetOp):
            kind = "reset"
            if tokens:
                try:
                    var.reset(tokens[op.token_index % len(tokens)])
                except (RuntimeError, ValueError, TypeError) as exc:
                    error = type(exc).__name__
        elif isinstance(op, GetOp):
            kind = "get"
        else:
            raise TypeError(f"unknown op type: {op!r}")
        observations.append(Observation(kind, var.get(UNSET), error))
    return observations


async def run_wool_script(
    script: Sequence[Op],
    var: wool.ContextVar[Any],
) -> list[Observation]:
    """Execute *script* against *var*, dispatching worker ops to a live pool.

    Caller ops run locally; worker ops dispatch the ``oracle_*``
    routines, addressed by ``var.namespace`` / ``var.name``. Tokens
    minted on either side land in ONE shared ordered list (mint order =
    script order), mirroring :func:`run_stdlib_oracle`'s token list.
    Each observation records the value at the op's actor site: caller
    ops read ``var.get(UNSET)`` locally (for worker sets this checks
    back-propagation), worker resets report the worker-side
    ``value_after``, and worker gets return the worker-side read.

    Must run inside an entered pool context; *var* should be declared
    without a constructor default for parity with the default-less
    stdlib oracle variable.
    """
    namespace = var.namespace
    name = var.name
    tokens: list[Any] = []
    observations: list[Observation] = []
    for op in script:
        error: str | None = None
        if isinstance(op, SetOp):
            kind = "set"
            if op.actor == CALLER:
                tokens.append(var.set(op.value))
            else:
                tokens.append(await routines.oracle_set(namespace, name, op.value))
            value = var.get(UNSET)
        elif isinstance(op, ResetOp):
            kind = "reset"
            if not tokens:
                value = var.get(UNSET)
            else:
                token = tokens[op.token_index % len(tokens)]
                if op.actor == CALLER:
                    try:
                        var.reset(token)
                    except (RuntimeError, ValueError, TypeError) as exc:
                        error = type(exc).__name__
                    value = var.get(UNSET)
                else:
                    outcome_kind, _message, value = await routines.oracle_reset(
                        namespace, name, token
                    )
                    if outcome_kind != "ok":
                        error = outcome_kind
        elif isinstance(op, GetOp):
            kind = "get"
            if op.actor == CALLER:
                value = var.get(UNSET)
            else:
                value = await routines.oracle_get(namespace, name, UNSET)
        else:
            raise TypeError(f"unknown op type: {op!r}")
        observations.append(Observation(kind, value, error))
    return observations


def observations_match(
    wool_observations: Sequence[Observation],
    stdlib_observations: Sequence[Observation],
) -> bool:
    """Return True when the two observation traces are identical."""
    return list(wool_observations) == list(stdlib_observations)


def describe_mismatch(
    wool_observations: Sequence[Observation],
    stdlib_observations: Sequence[Observation],
) -> str:
    """Render a per-op wool-vs-stdlib diff for assertion messages.

    Divergent rows are flagged with ``!!``; missing rows (unequal trace
    lengths) render as ``None`` on the shorter side.
    """
    lines = ["wool vs stdlib observation trace:"]
    pairs = zip_longest(wool_observations, stdlib_observations)
    for index, (wool_obs, stdlib_obs) in enumerate(pairs):
        marker = "  " if wool_obs == stdlib_obs else "!!"
        lines.append(f"{marker} [{index}] wool={wool_obs!r} stdlib={stdlib_obs!r}")
    return "\n".join(lines)
