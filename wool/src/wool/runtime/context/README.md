# Context

Wool's context model is founded directly on Python's stdlib `contextvars`. There is one context system, not two. `wool.ContextVar` is a `contextvars`-backed variable that additionally (a) propagates across worker boundaries and (b) opts its execution chain into Wool's linearity rules. Wool owns a single stdlib `contextvars.ContextVar` that carries all `wool.ContextVar` state as an immutable context; everything else in this subsystem reads, rebuilds, or transports that context.

Because Wool state rides in stdlib `contextvars`, it propagates with stdlib visibility across every conformant event loop — uvloop included — and across every *cooperative* asyncio scheduling edge: `call_soon`/`call_later`/`call_at`, `add_reader`/`add_writer`/`add_signal_handler`, and `Future.add_done_callback`, each of which shares the scheduling scope's chain unchanged. Child task creation propagates too, but as a *fork* — the child inherits the parent's variable values on a freshly minted `chain_id`, not the parent's chain itself (see [Fork-on-task](#fork-on-task)). No event-loop interception is involved. OS-thread offload is the one deliberate exception — an armed chain entered from a second OS thread fails loud rather than propagating silently; see [the chain-contention guard](#the-chain-contention-guard) below.

## The context

Wool chain state is held as a single immutable `Context` in one Wool-owned stdlib `contextvars.ContextVar`. A `Context` carries:

- `chain_id` — a UUID identifying the logical execution chain.
- `_owning_thread` — the OS thread that owns the chain (the thread dimension of the chain-contention guard, below).
- `_owning_task` — the `asyncio.Task` that owns the chain, or `None` when it was armed outside any task (the task dimension of the chain-contention guard, below).
- `data` — the set of `wool.ContextVar` instances bound in this chain (an index; each variable's value lives in its own backing `contextvars.ContextVar`).
- `reset_vars` — `(namespace, name)` keys of variables reset to no prior value upstream, propagated so the receiver applies the reset signal.
- `stub_pins` — undeclared-stub `wool.ContextVar` instances kept alive for a lazy-import receiver.

`wool.ContextVar.get` reads its key from the active context; `set` and `reset` rebuild the context and reinstall it. The context is immutable, so it can be shared by reference across callbacks, timers, and child tasks without intercepting the loop — a callback that sets a variable produces a *new* context scoped to its own `contextvars.Context` copy, leaving the scheduling scope's context untouched.

## Armed-gating

Enforcement is **armed-gated**. The Wool-owned context variable defaults to unset; while it is unset the surrounding context is *unarmed* and behaves as a plain `contextvars.Context` — no chain UUID and no guard. A context is armed by either of two events: the first `wool.ContextVar.set()` on it, or a merge of incoming wire state — a worker receiving a dispatch frame, or a caller merging a routine's response back. A process that neither sets a `wool.ContextVar` nor dispatches a routine that propagates context pays nothing at all: the task factory is never installed, so task creation is untouched.

Arming mints a fresh `chain_id`, installs the first `Context`, and self-installs the task factory on the running loop. Self-install is a no-op when there is no running loop — an armed context still works, but child tasks created on a later-started loop will not fork onto fresh chains until the factory is installed (call `wool.install_task_factory(loop)` explicitly in that case). Once installed, the factory is loop-wide: every task created on that loop thereafter — Wool-related or not — carries a small constant cost, its context registered for the chain-contention guard. Only a process that never arms any context pays nothing.

## `wool.ContextVar`

`ContextVar` mirrors `contextvars.ContextVar` at the surface — `get()`, `set()`, `reset()` — with one addition: an optional `namespace`. Its identity model is structural, not object-based. Every `ContextVar` has a `(namespace, name)` key; two `ContextVar(name="foo", namespace="bar")` calls in the same process resolve to the *same* singleton via the process-wide var registry, and collisions on the same key with mismatched declarations raise `ContextVarCollision`. The namespace is inferred from the top-level package of the calling frame when not passed explicitly, which keeps wire keys stable when modules are refactored deeper within the same package.

The `(namespace, name)` identity is what survives pickle round-trips. A `wool.ContextVar` arriving on the wire from a worker reduces by `(namespace, name)` and re-resolves to the same instance on the receiver. Pickling is gated through Wool's pickler; vanilla `pickle.dumps` is rejected via `__reduce_ex__` to keep the registry-bound semantics from leaking into ad-hoc serialization.

`get()` returns the value bound in the active context, falling back to the supplied default, then the constructor default, then raising `LookupError`. `set()` rebuilds the context with the new binding and returns a `Token`.

## `wool.Token`

Tokens carry a UUID, the `(namespace, name)` of the variable they reset, and the `chain_id` of the context in which `set()` ran. Live tokens are deduplicated process-wide, so pickle round-trips within a process preserve identity. Across the wire, the consumed-state bit rides on the wire frame so a worker's `reset()` correctly observes consumption that originated upstream, and vice versa.

`ContextVar.reset(token)` raises `ValueError` if the active context's `chain_id` differs from the one the token was minted under — the same isolation that makes concurrent tasks on a worker safe, since a task fork mints a fresh chain. This holds across pickle round-trips. `Token.MISSING` is the singleton sentinel returned for `old_value` when the variable had no prior binding.

## Fork-on-task

On every loop where Wool gets armed, it installs a task factory (composed with any user-installed factory; Wool's factory must be installed *last* — a third-party factory installed after it silently drops copy-on-fork for subsequently-created tasks, and the next `wool.ContextVar` access raises `wool.TaskFactoryDisplaced` once Wool detects the displacement). Every task the factory creates — armed or unarmed — carries one Wool `add_done_callback` (visible as `cb=[...]` in `repr()`) and has its `contextvars.Context` registered for the chain-contention guard.

When a new `asyncio.Task` is created in an *armed* context, the factory additionally wraps the child coroutine in a `_forked_scope` coroutine so the child runs under a *forked* context: it inherits the parent's variable bindings but receives a freshly minted `chain_id`. The wrapper has visible consequences for an armed task: `Task.get_coro()` and `repr()`'s coroutine field reflect `_forked_scope` rather than the user coroutine, tracebacks gain one wrapper frame, and the wrapper coroutine surfaces in `TaskGroup`/`gather` exception output. A task created in an *unarmed* context is not wrapped — its coroutine, `get_coro()`, and auto-generated name match a plain `asyncio.Task` created on the same loop (the `Task-N` counter is per loop implementation) — but it still carries the Wool done-callback and guard registration noted above, so it is not byte-for-byte identical to a task created without Wool's factory.

The fresh chain UUID is correct fork semantics: it isolates concurrent child tasks from each other and from their parent, and it is why a child task cannot `reset` a `Token` minted in its parent's chain.

## The chain-contention guard

Wool enforces strictly serial execution within a chain: at most one OS thread *and* one `asyncio.Task` may run code under a given chain at a time. The guard has two dimensions. The thread dimension compares the accessing OS thread against the chain's `owner` thread; the task dimension compares the running `asyncio.Task` against the chain's `owner_task` — the task that armed or forked the chain. Both are armed-gated — they engage only once a chain exists.

Cooperatively-scheduled work on the loop's thread never trips the guard. Event-loop callbacks and timers scheduled on the loop thread inherit the scheduling scope's context and run on the loop's own thread with no running task, so they share the chain but can never run concurrently with its owner. The exception is `loop.call_soon_threadsafe` called from another OS thread: it captures *that* thread's context, so a callback scheduled from a thread that armed its own chain runs the chain off its owner thread and raises `wool.ChainContention` — by design, since the chain genuinely spans two threads. Child tasks never trip the guard for the opposite reason: the task factory forks every child onto a fresh `chain_id` and a fresh `owner_task`, so a child never enters the parent's chain at all.

Genuine OS-thread parallelism does trip the thread dimension. Plain `asyncio.to_thread` from an armed context copies the surrounding `contextvars` context — chain UUID and all — into an executor thread, placing a second runner on one chain in parallel; the first `wool.ContextVar` access from that thread raises `wool.ChainContention`. The thread dimension keys on the chain's `owner` — the OS thread captured when the chain was armed — not on detected concurrency: any `wool.ContextVar` access from a different OS thread raises, even a strictly sequential, never-concurrent resume of an armed context on a second thread by non-asyncio code. That is a deliberate divergence from plain `contextvars`, which permits sequential cross-thread reuse; `wool.to_thread` is the supported cross-thread path.

Two `asyncio.Task` objects sharing one chain trip the task dimension. The task factory raises `wool.ChainContention` up front when an *armed* `contextvars.Context` already driving a live task is handed to a second `create_task` — that creation-time rejection is itself armed-gated, so sharing an *unarmed* context across tasks is permitted, exactly as stdlib asyncio permits it. If an unarmed shared context is armed *later*, the `owner_task` guard catches the second task the moment it touches a `wool.ContextVar` on a chain another live task already owns. Each task should still run in its own context: omit `context=` (asyncio copies it per task by default) or pass a fresh `contextvars.copy_context()` to each.

`wool.to_thread(func, *args, **kwargs)` is the supported alternative. It mirrors `asyncio.to_thread` but forks Wool chain state: the worker thread runs under a freshly minted, **detached** chain (a copy of the caller's bindings under a new `chain_id` owned by the worker thread, with no merge-back). Unlike a `@wool.routine` dispatch — which back-propagates the worker's `wool.ContextVar` mutations to the caller — a `wool.to_thread` offload is write-isolated: mutations the offloaded function makes are not visible to the caller, matching `asyncio.to_thread`'s copy-in semantics. Use it whenever Wool-aware work must run in another OS thread.

## Wire propagation

Each gRPC dispatch frame carries a `Context` protobuf message containing the chain's `chain_id` and a list of `ContextVar` entries — each with `(namespace, name)` and an optional serialized value. An absent value signals a reset-to-no-prior-value on the sender's chain. (Cross-process token transport is deferred — see https://github.com/wool-labs/wool/issues/231.)

- `Context.to_protobuf()` captures the sender's bindings; only variables that are bound or have a reset signal emit entries (default-only values are absent).
- `Context.from_protobuf()` decodes a wire `Context` protobuf into a `Context`: each variable resolves through the var registry (or registers a stub if the receiver hasn't yet imported the declaring module), and values are deserialized and held on the returned `Context` (exposed read-only via `Context.vars`) awaiting mount/update. The `Context.vars` property exposes the decoded values read-only so a backpressure hook can inspect caller-shipped values without applying them to any `contextvars.Context`.
- `Context.update(source)` is a pure data combine with `dict.update` semantics — it returns a new `Context` representing the union of `self` (the receiver) and `source` (the override), with `source` winning every key it touches. The result preserves `self`'s `chain_id`, `_owning_thread`, and `_owning_task`, and carries `source`'s pending values (exposed via `Context.vars`) for a subsequent `Context.mount()` to drain into backing variables and apply. The canonical wire-apply pattern is `(current_context() or Context()).update(incoming).mount()` — the caller uses it to fold the worker's response context back into the calling chain; the worker uses it for mid-stream per-step requests. The worker's *initial* dispatch frame installs the decoded context wholesale via `Context.mount()` alone (next paragraph), no update.

On the worker side, the caller's context is decoded, installed under the worker-loop task (re-stamped so the worker thread owns the chain), and the routine runs inside it. On completion the resulting context is encoded onto the response so the caller sees mutations made on the worker. Async generators bidirectionally exchange context at every yield/next boundary using the same machinery.

## Stub promotion

A `wool.ContextVar` may arrive on the wire — embedded in a pickled object graph or as an entry in a wire `Context` message — before the receiver's process has imported the module that declares it. To preserve the propagated value across that gap, the receiver registers a placeholder `wool.ContextVar` (a *stub*) under the wire key.

Two ingress paths share the same placeholder slot via `resolve_stub`: a pickled `wool.ContextVar` reconstituting on the receiver (which carries the original constructor default), and a wire `Context` entry (which carries no default and is created default-less). A stub stays alive — kept by the embedding object graph for a pickled instance, or by the decoded `Context`'s `stub_pins` for a wire entry — until the receiver's user code constructs `wool.ContextVar(...)` under its key, at which point the placeholder is promoted in place. If the receiver never declares the variable, the stub is collected with whatever held it and the propagated value is dropped.

## `RuntimeContext` and `dispatch_timeout`

`RuntimeContext` carries block-scoped runtime option overrides — currently just `dispatch_timeout` — separate from the `wool.ContextVar` context. It auto-captures on every `Task` at construction time and rides the wire on every dispatch, encoded by `RuntimeContext.to_protobuf()`, so the worker restores the caller's effective `dispatch_timeout` before running the routine.

```python
import wool

with wool.RuntimeContext(dispatch_timeout=30):
    result = await my_routine()
```

`dispatch_timeout` itself is a plain stdlib `contextvars.ContextVar[float | None]`, distinct from the Wool-owned context variable. `RuntimeContext.__enter__` / `__exit__` `set` and `reset` it through the standard `contextvars` mechanism.

## Decode failures

Wire-context decode is **ancillary state** in Wool's protocol contract: a failure to decode an incoming context never preempts the routine's primary signal (its return value or raised exception). Both `Context.to_protobuf` and `Context.from_protobuf` emit `wool.ContextDecodeWarning` for per-variable encode/decode failures and skip the offending entry; surviving variables decode normally. A malformed wire chain UUID falls back to a fresh UUID with the failure recorded as the same warning class.

Promote the warning to an error to opt into strict mode:

```python
import warnings
import wool

warnings.filterwarnings("error", category=wool.ContextDecodeWarning)
```

Under strict mode, per-entry failures aggregate into a single `wool.ContextDecodeError` (a `RuntimeError` subclass with the warnings on `.warnings`) raised after the encode/decode loop completes — every bad entry surfaces, not just the first. Strict mode is **fatal to the whole frame**: the partial Context that `from_protobuf` accumulated (the surviving entries that decoded successfully) is *not* surfaced — every entry on the wire frame is discarded along with the failing one. Callers that need partial application semantics under strict mode must run a non-strict decode first.

Worker-side strict mode is enabled via `PYTHONWARNINGS="error::wool.ContextDecodeWarning"`, which `multiprocessing` propagates to spawned subprocesses by default. The worker ships the aggregated `wool.ContextDecodeError` back through the routine-exception channel, so the caller catches the same error class symmetrically. When a routine *also* raises a primary exception, the `wool.ContextDecodeError` rides on it as `__cause__` via `raise primary from decode_err`, preserving the primary's class so `except RoutineError:` keeps matching. See the top-level [`wool/README.md`](../../../../README.md#decode-failure-semantics) for the full lenient/inspect/strict modes.

`wool.TaskFactoryDisplaced` follows different semantics: it is **not** ancillary state and **not** a tunable warning. Factory displacement is structurally fatal to chain propagation across every subsequent task on the loop, so `TaskFactoryDisplaced` is raised unconditionally — there is no strict-mode opt-in because there is no graceful-mode default. The raise escapes the next user-Wool operation that triggers detection (including `ContextManifest.mount`'s response merge) and preempts the routine's primary signal. The primary signal remains visible via `__context__` chaining. This asymmetry is deliberate: factory displacement is loud-fail-fast on infrastructure breakage, distinct from per-variable wire corruption (`wool.ContextDecodeWarning`) where graceful degradation is the right default.

## Exceptions and warnings

The context subsystem publishes five typed signals on the `wool` barrel. They appear inline above; this list collects them so callers know what to catch and what to promote:

- `wool.ChainContention` (exception) — raised when an armed chain is entered by a thread or asyncio task other than the one that owns it (see [the chain-contention guard](#the-chain-contention-guard) above). It is a `RuntimeError` subclass carrying structured diagnostic fields (`chain_id`, `kind`, `owning_thread`/`current_thread`, `owning_task`/`current_task`) in addition to the human-readable message.
- `wool.ContextVarCollision` (exception) — raised on construction of a second `wool.ContextVar` under an existing `(namespace, name)` key. Library authors should pass `namespace=` explicitly when constructing variables from shared factory code; application code can rely on the implicit package-name inference.
- `wool.ContextDecodeWarning` (warning) — emitted when a wire `Context` fails to encode or decode (see [Decode failures](#decode-failures) above). A `RuntimeWarning` subclass; promote to an exception via `warnings.filterwarnings("error", ...)` (or `PYTHONWARNINGS="error::wool.ContextDecodeWarning"` on workers) to opt into strict mode.
- `wool.ContextDecodeError` (exception, `RuntimeError` subclass) — raised when wire encode or decode fails under strict mode. Aggregates the promoted `wool.ContextDecodeWarning` instances on `.warnings` (a tuple). Routine code typically does not see this directly: a result-frame decode failure raises it as the routine's primary; an exception-frame decode failure rides on the routine's exception as `__cause__` via `raise from`.
- `wool.TaskFactoryDisplaced` (exception, `RuntimeError` subclass) — raised when Wool's task factory has been displaced from a loop it was previously installed on. Subsequent child tasks no longer fork onto fresh chains; a non-forked child inherits its parent's owning-task identity and trips `wool.ChainContention` on its first `wool.ContextVar` access. Detected reactively on the next `wool.ContextVar` access; raised unconditionally because displacement is structurally fatal to chain propagation across every subsequent task on the loop. Install Wool's factory last, or compose factories manually, to avoid this.
