# Context

Wool maintains its own context system parallel to Python's `contextvars`. A `wool.Context` is a self-contained snapshot of `wool.ContextVar` bindings plus a UUID identifying the logical execution chain. The two systems do not share state: `contextvars.Context.run()` does not fork or clear a `wool.Context`, and `wool.Context.run()` does not touch the surrounding `contextvars.Context`. Wool's task factory is the boundary at which Wool's fork-on-task semantics engage; `contextvars` continues to work exactly as it would normally.

## Per-scope binding

Each `wool.Context` is keyed in a process-wide registry by either the current `asyncio.Task` (for async code) or the current `threading.Thread` (for sync code). `wool.current_context()` returns the live binding for the current scope, creating one lazily if no `wool.Context` is bound yet. `wool.copy_context()` produces a shallow copy of the current scope's `wool.Context` with a fresh logical-chain UUID.

The first time any Wool API is touched on a running loop, the loop self-installs Wool's task factory. This is idempotent — repeated calls on the same loop are no-ops — and composes with any user-installed factory. Calling `wool.install_task_factory(loop)` explicitly is supported but rarely necessary; the only ordering hazard is a third-party factory installed *after* Wool's, which silently breaks copy-on-fork for subsequently-created tasks.

## Fork-on-task

When a new `asyncio.Task` is created on a loop with Wool's task factory installed, the factory examines the `context=` kwarg:

- `context=wool_ctx` — the new task is bound to *that* `wool.Context` for its lifetime; concurrent attempts to bind a second task to the same `wool.Context` raise `RuntimeError` as soon as the second task starts running.
- `context=stdlib_ctx` — the stdlib `contextvars.Context` is forwarded verbatim, and Wool forks the parent task's `wool.Context` (or constructs a fresh one if no parent is bound) to bind to the child.
- `context=None` — same fork behaviour as the stdlib path; the child receives a copy of the parent's `wool.Context` under a fresh chain UUID.

The fresh chain UUID matters because Tokens are scoped by `wool.Context` UUID. A child task cannot reset a Token minted in its parent's chain — the same isolation that makes concurrent tasks on a worker safe.

To get fresh stdlib *and* fresh Wool state in a synchronous block, compose the two:

```python
import contextvars
import wool

wool.Context().run(contextvars.Context().run, fn, *args, **kwargs)
```

## `wool.ContextVar`

`ContextVar` mirrors `contextvars.ContextVar` at the surface — `get()`, `set()`, `reset()` — but its identity model is structural, not object-based. Every `ContextVar` has a `(namespace, name)` key. Two `ContextVar(name="foo", namespace="bar")` calls in the same process resolve to the *same* singleton via the process-wide var registry; collisions on the same key with mismatched declarations raise `ContextVarCollision`. The namespace is inferred from the top-level package of the calling frame when not passed explicitly, which keeps wire keys stable when modules are refactored deeper within the same package.

The `(namespace, name)` identity is what survives pickle round-trips. A `wool.ContextVar` arriving on the wire from a worker reduces by `(namespace, name)` and re-resolves to the same instance on the receiver. Pickling is gated through Wool's pickler; vanilla `pickle.dumps` is rejected via `__reduce_ex__` to keep the registry-bound semantics from leaking into ad-hoc serialization.

Values are stored in the current `wool.Context` — one per `asyncio.Task`, one per thread for sync code. `get()` returns the value bound in the active `wool.Context`, falling back to the supplied default, then the constructor default, then raising `LookupError`. `set()` writes into the active `wool.Context` and returns a `Token`.

## `wool.Token`

Tokens carry a UUID and the `(namespace, name)` of the var they reset. Live tokens are deduplicated process-wide, so pickle round-trips within a process preserve identity — a token round-tripped through `cloudpickle` resolves back to the same instance. Across the wire, the consumed-state bit rides on the wire frame so a worker's `reset()` correctly observes consumption that originated upstream, and vice versa.

Tokens are scoped to the `wool.Context` UUID in which `set()` ran. `ContextVar.reset(token)` raises `ValueError` if the active `wool.Context` UUID differs from the one the token was minted under. This holds across pickle round-trips: a token round-tripped through the wire still carries its originating chain id, and reset only succeeds when the receiver is operating in the same logical chain. `Token.MISSING` is the singleton sentinel returned for `old_value` when the var had no prior binding.

## Wire propagation

Each gRPC dispatch frame carries a `Context` protobuf message containing:

- The `wool.Context` UUID (preserved across the call so dispatches under the same chain resolve to the same logical id).
- A list of `ContextVar` entries, each with `(namespace, name)`, an optional serialized value, and consumed-token UUIDs that have reset the var in the sender's chain.

`Context.to_protobuf()` snapshots the sender's bound vars and consumed-token store; only vars that are bound or have consumed tokens emit entries (default-only values are absent). `Context.from_protobuf()` merges each entry into the receiver's `wool.Context`: the var resolves through the var registry (or registers a stub if the receiver hasn't yet imported the declaring module), the value is deserialized into the binding, and consumed-token UUIDs either flip the corresponding live tokens to consumed or are stashed for later reset propagation if no live token has been seen yet.

On the worker side, a fresh `wool.Context` is bound to the worker-loop task, the wire vars are merged in via `Context.update(...)`, and the routine runs inside that `wool.Context`. On completion, the resulting `wool.Context` is snapshotted via `to_protobuf()` and returned in the response so the caller sees mutations made on the worker. Async generators bidirectionally exchange context at every yield/next boundary using the same machinery.

## Single-task-per-Context

A `wool.Context` enforces "at most one task running inside it at a time." Two layers cooperate:

- A re-entry check on `wool.Context` itself: any attempt to enter a `wool.Context` that is already running user code raises `RuntimeError`. `Context.run()` and `attached(ctx)` both gate user code through this check, so the discipline holds regardless of which entry point installed the `wool.Context`.
- A one-shot binding contract on the task factory: a task may be bound to a `wool.Context` exactly once at creation. A duplicate binding raises `ContextAlreadyBound` rather than silently stomping the prior chain UUID.

When a task is created with `context=wool_ctx`, the binding is pinned to that task for its lifetime. A second task targeting the same `wool.Context` while the first is still mid-flight raises immediately as it starts running — even when the two attempts span asyncio loop ticks.

## Binding APIs

Two public entry points install a `wool.Context` for a scope:

- `attached(ctx)` — synchronous context manager. Installs `ctx` on the current scope; concurrent entry from another task raises `RuntimeError` immediately. Restores the previous binding on exit.
- `wool.create_task(coro, context=ctx)` — pre-binds `ctx` to a freshly-spawned `asyncio.Task` for the coroutine's lifetime. Equivalent at runtime to `asyncio.create_task(coro, context=ctx)` when Wool's task factory is installed; the helper exists purely as a typing shim because stdlib's `context=` is typed for `contextvars.Context` and `wool.Context` cannot subclass it.

Both routes enforce the single-task-per-Context rule. `Context.run(fn, *args, **kwargs)` is the sync-equivalent of `attached(self)`.

## Stub promotion

A `wool.ContextVar` may arrive on the wire — embedded in a pickled object graph or as an entry in a wire `Context` snapshot — before the receiver's process has imported the module that declares it. To preserve the propagated value across that gap, the receiver registers a placeholder `wool.ContextVar` under the wire key and pins it to the active `wool.Context` so it stays alive.

Two ingress paths share the same placeholder slot:

- A pickled `wool.ContextVar` reconstituting on the receiver carries the original constructor default; the placeholder adopts it.
- A wire `Context` snapshot's var entry carries no default; the placeholder is created default-less and adopts a default later if the pickle path encounters the same key first.

When the receiver's user code eventually constructs `wool.ContextVar(...)` under the placeholder's key, the placeholder is promoted in place — the existing instance is returned to the caller, preserving any wire state and reference identity already accumulated. If the receiver never declares the var, the placeholder is collected with its pinning `wool.Context` and the propagated value is dropped.

## `RuntimeContext` and `dispatch_timeout`

`RuntimeContext` carries block-scoped runtime option overrides — currently just `dispatch_timeout` — separate from the `ContextVar` snapshot in `wool.Context`. It auto-captures on every `Task` at construction time and rides the wire on every dispatch, encoded by `RuntimeContext.to_protobuf()`, so the worker restores the caller's effective `dispatch_timeout` before running the routine.

```python
import wool

with wool.RuntimeContext(dispatch_timeout=30):
    result = await my_routine()
```

`dispatch_timeout` itself is a stdlib `contextvars.ContextVar[float | None]`, exposed at module scope so `RuntimeContext.__enter__` / `__exit__` can `set` and `reset` it through the standard `contextvars` mechanism. This is the one place inside the context subpackage where stdlib `contextvars` is used directly: `RuntimeContext` is a stdlib-flavoured override surface, distinct from `wool.Context` (which carries `wool.ContextVar` snapshots).

A bare `RuntimeContext()` with no explicit `dispatch_timeout` substitutes the live `dispatch_timeout` value at encode time, so a `Task` constructed for wire transport propagates the encoder's effective timeout without the caller having to materialize the override explicitly.

## Decode failures

Wire-context decode is **ancillary state** in Wool's protocol contract: a failure to decode an incoming context never preempts the routine's primary signal (its return value or raised exception). Both `Context.to_protobuf` and `Context.from_protobuf` emit `wool.ContextDecodeWarning` for per-var encode/decode failures and skip the offending entry; surviving vars decode normally. A malformed wire context UUID falls back to a fresh UUID with the failure recorded as the same warning class.

Promote the warning to an error to opt into strict mode:

```python
import warnings
import wool

warnings.filterwarnings("error", category=wool.ContextDecodeWarning)
```

Under strict mode, per-var failures aggregate into a single `BaseExceptionGroup` raised after the encode/decode loop completes — every bad var surfaces, not just the first. Worker-side strict mode is enabled via `PYTHONWARNINGS="error::wool.ContextDecodeWarning"`, which `multiprocessing` propagates to spawned subprocesses by default. The worker ships the promoted warning back through the routine-exception channel, so the caller catches the same `wool.ContextDecodeWarning` class symmetrically. See the top-level [`wool/README.md`](../../../../README.md#decode-failure-semantics) for the full lenient/inspect/strict modes.
