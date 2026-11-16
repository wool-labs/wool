"""The context subsystem's exceptions and warnings.

Single home for every error and warning the context subsystem
raises. The serialization branch — `SerializationError`, its
strict-mode aggregator `ChainSerializationError`, and the
non-fatal `SerializationWarning` — sits alongside `ChainContention`
(the chain-ownership guard's signal), `ContextVarCollision` (raised
on a duplicate variable key), and `TaskFactoryDisplaced` (raised
when a third-party task factory displaces Wool's).
"""

from __future__ import annotations

import asyncio
from typing import Any
from typing import Literal
from uuid import UUID

from wool.exceptions import WoolError
from wool.exceptions import WoolWarning


# public
class SerializationError(WoolError):
    """Raised when a value cannot be serialized across the Wool wire.

    Raised atomically when a single value fails to encode. The
    strict-mode aggregator `ChainSerializationError` subclasses
    this to collect per-variable chain-manifest failures, so catching
    `SerializationError` matches every wire serialization failure,
    atomic or aggregated.

    :param cause:
        The underlying exception that produced the failure. Also
        carried on ``__cause__`` when raised via exception chaining.
    :param value_repr:
        Optional ``repr()``-style preview of the value that failed to
        encode, for diagnostics when the cause exception's own message
        does not name the offending value.

    ``args`` carries ``(message, cause, value_repr)`` so the worker can
    reconstruct the structured fields with ``type(error)(*error.args)``.
    ``str(error)`` renders only the message, as before.
    """

    def __init__(
        self,
        message: object = "",
        cause: BaseException | None = None,
        value_repr: str | None = None,
    ) -> None:
        super().__init__(message, cause, value_repr)
        self.cause = cause
        self.value_repr = value_repr

    def __str__(self) -> str:
        return str(self.args[0])


# public
class ChainSerializationError(SerializationError):
    """Aggregator raised when a value cannot be serialized across the Wool wire.

    Strict mode (see `SerializationWarning` for promotion recipe)
    turns per-variable serialization warnings into errors. The wire
    codec catches each promoted warning and raises the batch as a
    single `ChainSerializationError` once its encode or decode loop
    completes, so every bad variable surfaces, not just the first.

    When a routine also raises a primary exception, the aggregator
    rides on that primary's ``__cause__``. Any existing ``except``
    clauses wrapping the routine keep matching unchanged.

    :param warnings:
        The promoted `SerializationWarning` instances, in emission
        order. They are kept on the `warnings` attribute; the
        exception's own message is a synthesized summary, so ``str()``
        reads as a count of failures rather than a tuple of warnings.
        The warnings themselves are stored in ``args`` for reconstruction.
    """

    def __init__(self, *warnings: SerializationWarning) -> None:
        self.warnings: tuple[SerializationWarning, ...] = tuple(
            w for w in warnings if isinstance(w, SerializationWarning)
        )
        # The aggregator's identity is its warnings rather than the atomic
        # error's message/cause/value_repr tuple. Both reconstruct from args.
        WoolError.__init__(self, *self.warnings)
        self.cause = None
        self.value_repr = None

    def __str__(self) -> str:
        count = len(self.warnings)
        noun = "variable" if count == 1 else "variables"
        return f"{count} context {noun} failed to serialize across the wire"


# public
class SerializationWarning(WoolWarning):
    """Emitted when a value cannot be serialized across the Wool wire.

    Wool's wire protocol treats chain propagation and exception
    fidelity as ancillary state: a failure there never preempts the
    routine's primary signal, i.e., its return value or raised exception.
    The failure is reported through this warning instead, so callers
    that depend on the ancillary state can detect the inconsistency.

    Every emission site shares this one class, so callers that prefer
    strict semantics promote serialization failures to errors with a
    single category-level filter::

        import warnings
        import wool

        warnings.filterwarnings("error", category=wool.SerializationWarning)

    Under strict mode the wire codec aggregates the promoted
    per-variable warnings into a single `ChainSerializationError`
    (see that class for the aggregation contract).

    The structured fields below identify the failure programmatically;
    each may be ``None`` if the emitting site does not supply it.

    :param cause:
        The underlying exception, if applicable.
    :param var_key:
        The ``(namespace, name)`` identity of the variable whose value
        failed to serialize.
    :param direction:
        Which side of the wire hop failed.
    :param original_type:
        The exception class that fallback reconstruction replaced when
        a routine's exception could not be rebuilt with full fidelity.
    """

    def __init__(
        self,
        *args: Any,
        cause: BaseException | None = None,
        var_key: tuple[str, str] | None = None,
        direction: Literal["encode", "decode"] | None = None,
        original_type: type | None = None,
    ) -> None:
        super().__init__(*args)
        self.cause = cause
        self.var_key = var_key
        self.direction = direction
        self.original_type = original_type


_KIND_MESSAGES: dict[str, str] = {
    "thread": (
        "wool.ContextVar accessed from thread {current_thread} but chain "
        "{chain_id} is owned by thread {owning_thread}; an armed Wool "
        "context cannot be shared across OS threads in parallel. Use "
        "wool.to_thread to offload work onto a fresh, detached chain."
    ),
    "task": (
        "wool.ContextVar accessed by task {current_task} but chain "
        "{chain_id} is owned by task {owning_task}; a Wool chain cannot "
        "be entered by two tasks at once. Each task must run on "
        "its own chain — create child tasks the ordinary way (the task "
        "factory forks a fresh chain per task), or pass a fresh "
        "contextvars.copy_context() to each create_task instead of "
        "sharing one."
    ),
    "create_task": (
        "the same armed contextvars.Context was passed to create_task "
        "while an earlier task running in it is still live (chain "
        "{chain_id}). An armed context cannot be shared across "
        "concurrently-live tasks — both tasks would corrupt each other's "
        "Wool state through the single context it holds. Omit context= "
        "(the default copies the context per task) or pass a fresh "
        "contextvars.copy_context() to each task."
    ),
}


# public
class ChainContention(WoolError):
    """Raised when a Wool chain is entered by a thread or task that does
    not own it.

    Wool enforces strictly serial execution within a logical chain: at
    most one OS thread *and* one `asyncio.Task` may run code
    under a given chain at a time. The guard has two dimensions — an
    OS-thread check and an asyncio-task check within a single thread —
    and engages only on an *armed* context (one carrying Wool chain
    state). It fires at the point a `wool.ContextVar` is read
    or written, not at a boundary crossing; offloaded code that never
    touches a Wool variable is never flagged. An armed
    `contextvars.Context` re-passed to
    `asyncio.create_task` is also rejected up front (the factory
    installed by `wool.install_task_factory` performs this
    rejection; see `wool.runtime.context.factory`) — the
    ``"create_task"`` kind below distinguishes that case.

    The supported way to run Wool-aware work on another OS thread is
    `wool.to_thread`, which forks a fresh, detached chain for
    the worker.

    See ``wool/src/wool/runtime/context/README.md`` for the model
    context.

    :param chain_id:
        UUID of the chain whose ownership was violated.
    :param kind:
        ``"thread"`` for a cross-thread access, ``"task"`` for a
        cross-task access, ``"create_task"`` for an armed context
        re-passed to `asyncio.create_task`.
    :param owning_thread:
        Owner thread identity, when *kind* is ``"thread"``.
    :param current_thread:
        Offending thread identity, when *kind* is ``"thread"``.
    :param owning_task:
        Owner task, when *kind* is ``"task"``; after a cross-process hop
        the task's ``repr`` string instead (see `__reduce__`).
    :param current_task:
        Offending task, when *kind* is ``"task"``; after a cross-process
        hop the task's ``repr`` string instead (see `__reduce__`).
    """

    chain_id: UUID
    kind: Literal["thread", "task", "create_task"]
    owning_thread: int | None
    current_thread: int | None
    # Post-wire ``str`` form documented on the ``owning_task`` /
    # ``current_task`` params (see ``__reduce__``).
    owning_task: asyncio.Future[Any] | str | None
    current_task: asyncio.Future[Any] | str | None

    def __init__(
        self,
        chain_id: UUID,
        kind: Literal["thread", "task", "create_task"],
        owning_thread: int | None = None,
        current_thread: int | None = None,
        owning_task: asyncio.Future[Any] | str | None = None,
        current_task: asyncio.Future[Any] | str | None = None,
    ) -> None:
        # Validate ``kind`` explicitly so an unknown value raises a
        # typed ``ValueError`` from a known origin rather than a bare
        # ``KeyError`` from inside the exception's own constructor.
        # The Literal annotation guards static call sites; this guard
        # covers dynamic call sites (most notably ``__reduce__``-driven
        # cross-process reconstruction where a forward-compat receiver
        # might decode a ``kind`` value it does not know).
        if kind not in _KIND_MESSAGES:
            raise ValueError(
                f"unknown ChainContention kind: {kind!r}; "
                f"expected one of {sorted(_KIND_MESSAGES)}"
            )
        # Keep local task identities accessible on the attributes, but put
        # only their diagnostic representations in the reconstructible args.
        # Strings already received over the wire must not gain another repr.
        owning_repr = (
            repr(owning_task)
            if owning_task is not None and not isinstance(owning_task, str)
            else owning_task
        )
        current_repr = (
            repr(current_task)
            if current_task is not None and not isinstance(current_task, str)
            else current_task
        )
        super().__init__(
            chain_id, kind, owning_thread, current_thread, owning_repr, current_repr
        )
        self.chain_id = chain_id
        self.kind = kind
        self.owning_thread = owning_thread
        self.current_thread = current_thread
        self.owning_task = owning_task
        self.current_task = current_task

    def __reduce__(self) -> tuple[Any, ...]:
        # Skip __dict__, which retains live tasks locally. The structured
        # args suffice for both normal pickling and the worker's fallback.
        return type(self), self.args

    def __str__(self) -> str:
        return _KIND_MESSAGES[self.kind].format(
            chain_id=self.chain_id,
            owning_thread=self.owning_thread,
            current_thread=self.current_thread,
            owning_task=self.args[4],
            current_task=self.args[5],
        )


def _reconstruct_chain_contention(
    chain_id: UUID,
    kind: Literal["thread", "task", "create_task"],
    owning_thread: int | None,
    current_thread: int | None,
    owning_task: asyncio.Future[Any] | str | None,
    current_task: asyncio.Future[Any] | str | None,
) -> ChainContention:
    """Reconstruct payloads pickled before ChainContention used structured args."""
    return ChainContention(
        chain_id=chain_id,
        kind=kind,
        owning_thread=owning_thread,
        current_thread=current_thread,
        owning_task=owning_task,
        current_task=current_task,
    )


# public
class ContextVarCollision(WoolError):
    """Raised on construction of a second ContextVar under an existing key.

    Keys must be unique within the inferred package namespace. Library
    authors should pass ``namespace=`` explicitly when constructing
    variables from shared factory code; application code can rely on
    the implicit package-name inference.

    Detection is best-effort under garbage collection: the process-wide
    variable registry holds `ContextVar` instances weakly, so a
    key frees up once its previous instance is collected and a later
    construction under that key then succeeds instead of colliding. In
    practice `ContextVar` instances are module-level singletons
    held for the process lifetime, so a genuine collision always
    raises.
    """


# public
class TaskFactoryDisplaced(WoolError):
    """Raised when Wool's task factory has been displaced by a later one.

    Wool installs its task factory on a loop, composing with any
    factory already present. A third-party factory installed *after*
    Wool's silently drops Wool's wrapping: child tasks created on that
    loop thereafter no longer fork onto fresh chains — copy-on-fork is
    lost. A non-forked child inherits its parent's owning-task
    identity and trips `wool.ChainContention` on its first
    `wool.ContextVar` access, or, when the parent has already
    finished, silently reuses the parent's chain identity.

    The displacement is detected reactively: Wool cannot intercept
    `loop.set_task_factory`, so it is noticed only on the next
    `wool.ContextVar` access (or other path that arms a chain).
    Raised unconditionally — not as a warning that callers opt to
    promote — because displacement is structurally fatal to chain
    propagation across every subsequent task on the loop, materially
    different from per-variable wire-state corruption
    (`wool.SerializationWarning`) which can degrade gracefully
    on individual entries.

    Install Wool's task factory last, or compose factories manually,
    to avoid the displacement entirely.
    """
