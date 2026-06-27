# Context

Wool's context model is founded directly on Python's stdlib `contextvars`. There is one context system, not two. `wool.ContextVar` is a `contextvars`-backed variable that additionally (a) propagates across worker boundaries and (b) opts its execution chain into Wool's linearity rules. Wool owns a single stdlib `contextvars.ContextVar` (`wool.__chain__`) that carries all `wool.ContextVar` state as an immutable `Chain`; everything else in this subsystem reads, rebuilds, or transports that chain.

The goal is stdlib parity with context continuity across workers: a routine awaited on a worker behaves exactly as a process-local await would. Stdlib asyncio has two context behaviors — a plain `await` *shares* the caller's context (the callee's mutations are visible after the await), and `asyncio.create_task` *copies* it (the child's mutations are isolated). Wool extends both across the wire: a dispatch ships the caller's chain state out and merges the worker's mutations back, so a remote await is indistinguishable from a local one, while `create_task` forks — exactly as it would locally.

Because Wool state rides in stdlib `contextvars`, it propagates with stdlib visibility across every conformant event loop — uvloop included — and across every *cooperative* asyncio scheduling edge: `call_soon`/`call_later`/`call_at`, `add_reader`/`add_writer`/`add_signal_handler`, and `Future.add_done_callback`, each of which inherits the scheduling scope's chain unchanged. Child task creation propagates too, but as a *fork* — the child inherits the parent's variable values on a freshly minted chain id, not the parent's chain itself (see [Fork-on-task](#fork-on-task)). No event-loop interception is involved. Non-owner entry is the one deliberate exception — a chain entered by an OS thread or task that does not own it (a plain `asyncio.to_thread` offload, an armed context re-run on another thread via `Context.run`, an armed context handed to a directly instantiated `asyncio.Task`) fails loud rather than propagating silently; see [the chain-contention guard](#the-chain-contention-guard) below.

## The chain

A chain is one serial branch of the program's async call tree — the logical call stack descending from the most recent `asyncio.create_task` fork, on which every frame executes strictly in sequence. Plain awaits, generator yields, callbacks, and worker dispatches all extend the branch; `create_task` always starts a new one. The chain is what survives the process hop: a worker arms onto the caller's chain id because it genuinely is the same branch, executing elsewhere for a while.

Chain state is held as a single immutable `Chain` in the Wool-owned `wool.__chain__` variable: the chain id, the owner stamps the contention guard arbitrates, the index of bound variables (`vars`), pending reset signals (`resets`), and stub pins (`stubs`) — see the `Chain` class docstring for the field-by-field contract. The chain is an *index*, not a value store: each `wool.ContextVar`'s value lives in its own backing `contextvars.ContextVar`.

`wool.ContextVar.get` reads its key via the active chain; `set` and `reset` rebuild the chain and reinstall it. The chain is immutable, so it can be shared by reference across callbacks, timers, and child tasks without intercepting the loop — a callback that sets a variable produces a *new* chain scoped to its own `contextvars.Context` copy, leaving the scheduling scope's chain untouched.

## Armed-gating

Enforcement is **armed-gated**. The Wool-owned context variable defaults to unset; while it is unset the surrounding context is *unarmed* and behaves as a plain `contextvars.Context` — no chain UUID and no guard. A context is armed by either of two events: the first `wool.ContextVar.set()` on it, or a merge of incoming wire state — a worker receiving a dispatch frame, or a caller merging a routine's response back. A process that neither sets a `wool.ContextVar` nor dispatches a routine that propagates chain state pays nothing at all: the task factory is never installed, so task creation is untouched.

Arming mints a fresh chain id, installs the first `Chain`, and self-installs the task factory on the running loop. Self-install is a no-op when there is no running loop — an armed context still works, but child tasks created on a later-started loop will not fork onto fresh chains until the factory is installed (call `wool.install_task_factory(loop)` explicitly in that case). Once installed, the factory is loop-wide: every task created on that loop thereafter — Wool-related or not — carries a small constant cost, its context registered for the chain-contention guard. Only a process that never arms any context pays nothing.

## `wool.ContextVar`

`ContextVar` mirrors `contextvars.ContextVar` at the surface — `get()`, `set()`, `reset()` — with one addition: an optional `namespace`. Its identity model is structural, not object-based. Every `ContextVar` has a `(namespace, name)` key; two `ContextVar(name="foo", namespace="bar")` calls in the same process resolve to the *same* singleton via the process-wide var registry, and collisions on the same key with mismatched declarations raise `ContextVarCollision`. The namespace is inferred from the top-level package of the calling frame when not passed explicitly, which keeps wire keys stable when modules are refactored deeper within the same package.

The `(namespace, name)` identity is what survives pickle round-trips. A `wool.ContextVar` arriving on the wire from a worker reduces by `(namespace, name)` and re-resolves to the same instance on the receiver. Pickling is gated through Wool's pickler; vanilla `pickle.dumps` is rejected via `__reduce_ex__` to keep the registry-bound semantics from leaking into ad-hoc serialization.

`get()` returns the value bound in the active context, falling back to the supplied default, then the constructor default, then raising `LookupError`. `set()` rebuilds the context with the new binding and returns a `Token`.

## `wool.Token`

`wool.Token` *is* `contextvars.Token` — re-exported, not wrapped. `set()` returns the native token, `reset(token)` spends it natively, and `Token.MISSING` is stdlib's sentinel for no-prior-binding. Tokens are process-local handles and are not serializable: they never ride the wire, and cross-process token transport is deferred (see [#231](https://github.com/wool-labs/wool/issues/231)).

`ContextVar.reset(token)` defers to native validation — stdlib raises `ValueError` for a token spent twice, minted by another variable, or minted in a different `contextvars.Context` — and that last rejection is what keeps concurrent tasks isolated, since a forked task runs in a context copy where the parent's tokens are foreign. Wool layers only its chain bookkeeping on top: a reset that rewinds to no prior value records the pending reset signal so the drop propagates over the wire (see [Wire propagation](#wire-propagation)).

## Fork-on-task

On every loop where Wool gets armed, it installs a task factory (composed with any user-installed factory; Wool's factory must be installed *last* — a third-party factory installed after it silently drops copy-on-fork for subsequently-created tasks, and the next `wool.ContextVar` access raises `wool.TaskFactoryDisplaced` once Wool detects the displacement). Every task the factory creates — armed or unarmed — carries one Wool `add_done_callback` (visible as `cb=[...]` in `repr()`) and has its `contextvars.Context` registered for the chain-contention guard.

When a new `asyncio.Task` is created in an *armed* context, the factory additionally wraps the child coroutine in a `_forked_scope` coroutine so the child runs under a *forked* context: it inherits the parent's variable bindings but receives a freshly minted chain id. The wrapper has visible consequences for an armed task: `Task.get_coro()` and `repr()`'s coroutine field reflect `_forked_scope` rather than the user coroutine, tracebacks gain one wrapper frame, and the wrapper coroutine surfaces in `TaskGroup`/`gather` exception output. A task created in an *unarmed* context is not wrapped — its coroutine, `get_coro()`, and auto-generated name match a plain `asyncio.Task` created on the same loop (the `Task-N` counter is per loop implementation) — but it still carries the Wool done-callback and guard registration noted above, so it is not byte-for-byte identical to a task created without Wool's factory.

The fresh chain UUID is correct fork semantics: it isolates concurrent child tasks from each other and from their parent, and it is why a child task cannot `reset` a `Token` minted in its parent's chain.

## The chain-contention guard

Wool enforces strictly serial execution within a chain: at most one OS thread *and* one `asyncio.Task` may run code under a given chain at a time. The guard has two dimensions. The thread dimension compares the accessing OS thread against the chain's owning thread; the task dimension compares the running `asyncio.Task` against the chain's owning task — both stamped at mount (the `Chain.thread` and `Chain.task` fields). Both are armed-gated — they engage only once a chain exists.

Cooperatively-scheduled work on the loop's thread never trips the guard. Event-loop callbacks and timers scheduled on the loop thread inherit the scheduling scope's context and run on the loop's own thread with no running task, so they share the chain but can never run concurrently with its owner. The exception is `loop.call_soon_threadsafe` called from another OS thread: it captures *that* thread's context, so a callback scheduled from a thread that armed its own chain runs the chain off its owner thread and raises `wool.ChainContention` — by design, since the chain genuinely spans two threads. Child tasks never trip the guard for the opposite reason: the task factory forks every child onto a fresh chain with its own owner stamps, so a child never enters the parent's chain at all.

Genuine OS-thread parallelism does trip the thread dimension. Plain `asyncio.to_thread` from an armed context copies the surrounding `contextvars` context — chain UUID and all — into an executor thread, placing a second runner on one chain in parallel; the first `wool.ContextVar` access from that thread raises `wool.ChainContention`. The thread dimension keys on the chain's owning thread — captured when the chain was armed — not on detected concurrency: any `wool.ContextVar` access from a different OS thread raises, even a strictly sequential, never-concurrent resume of an armed context on a second thread by non-asyncio code. That is a deliberate divergence from plain `contextvars`, which permits sequential cross-thread reuse; `wool.to_thread` is the supported cross-thread path.

Two `asyncio.Task` objects sharing one chain trip the task dimension. The task factory raises `wool.ChainContention` up front when an *armed* `contextvars.Context` already driving a live task is handed to a second `create_task` — that creation-time rejection is itself armed-gated, so sharing an *unarmed* context across tasks is permitted, exactly as stdlib asyncio permits it. If an unarmed shared context is armed *later*, the task dimension catches the second task the moment it touches a `wool.ContextVar` on a chain another live task already owns. The same access-time catch covers direct `asyncio.Task(...)` instantiation, which bypasses the loop's task factory entirely — no fork occurs, so the new task shares its context's chain and trips the guard on its first `wool.ContextVar` access. Each task should still run in its own context: omit `context=` (asyncio copies it per task by default) or pass a fresh `contextvars.copy_context()` to each.

`wool.to_thread(func, *args, **kwargs)` is the supported alternative. It mirrors `asyncio.to_thread` but forks Wool chain state: the worker thread runs under a freshly minted, **detached** chain (a copy of the caller's bindings under a new chain id owned by the worker thread, with no merge-back). Unlike a `@wool.routine` dispatch — which back-propagates the worker's `wool.ContextVar` mutations to the caller — a `wool.to_thread` offload is write-isolated: mutations the offloaded function makes are not visible to the caller, matching `asyncio.to_thread`'s copy-in semantics. Use it whenever Wool-aware work must run in another OS thread.

## Wire propagation

Each gRPC dispatch frame carries a `ChainManifest` protobuf message containing the chain's id and a list of `ContextVar` entries — each with `(namespace, name)` and an optional serialized value. An absent value signals a reset-to-no-prior-value on the sender's chain. (Cross-process token transport is deferred — see https://github.com/wool-labs/wool/issues/231.)

- `Chain.to_protobuf()` encodes the sender's chain: bound variables emit valued entries read live from their backings, pending resets emit value-less entries, and emission is sorted so identical chain state encodes byte-identically. Only variables that are bound or reset-pending emit entries (default-only values are absent).
- `ChainManifest.from_protobuf()` decodes a wire message into an inert manifest: each entry resolves through the var registry (or registers a stub if the receiver hasn't yet imported the declaring module) and values are deserialized onto the manifest — decoded but applied to no `contextvars.Context`, so a backpressure hook can inspect caller-shipped values without installing them.
- `ChainManifest.mount()` is the single install pipeline: it drains the manifest's values into backing variables, applies reset signals, and installs the resulting `Chain`. A live receiver merges — the manifest wins the keys it touches and the receiver keeps its chain id; an unarmed receiver, or the worker's initial dispatch frame, installs the manifest wholesale under the sender's chain id.

On the worker side, the caller's chain manifest is decoded, installed under the worker-loop task (re-stamped so the worker thread owns the chain), and the routine runs inside it. On completion the resulting chain manifest is encoded onto the response so the caller sees mutations made on the worker. Async generators bidirectionally exchange chain manifests at every yield/next boundary using the same machinery.

## Stub promotion

A `wool.ContextVar` may arrive on the wire — embedded in a pickled object graph or as an entry in a wire `ChainManifest` message — before the receiver's process has imported the module that declares it. To preserve the propagated value across that gap, the receiver registers a placeholder `wool.ContextVar` (a *stub*) under the wire key.

Two ingress paths share the same placeholder slot via `resolve_stub`: a pickled `wool.ContextVar` reconstituting on the receiver (which carries the original constructor default), and a wire `ChainManifest` entry (which carries no default and is created default-less). A stub stays alive — kept by the embedding object graph for a pickled instance, or by the decoded manifest's `stubs` for a wire entry — until the receiver's user code constructs `wool.ContextVar(...)` under its key, at which point the placeholder is promoted in place. If the receiver never declares the variable, the stub is collected with whatever held it and the propagated value is dropped.

## `RuntimeContext` and `dispatch_timeout`

`RuntimeContext` carries block-scoped runtime option overrides — currently just `dispatch_timeout` — separate from the `wool.ContextVar` context. It auto-captures on every `Task` at construction time and rides the wire on every dispatch, encoded by `RuntimeContext.to_protobuf()`, so the worker restores the caller's effective `dispatch_timeout` before running the routine.

```python
import wool

with wool.RuntimeContext(dispatch_timeout=30):
    result = await my_routine()
```

`dispatch_timeout` itself is a plain stdlib `contextvars.ContextVar[float | None]`, distinct from the Wool-owned context variable. `RuntimeContext.__enter__` / `__exit__` `set` and `reset` it through the standard `contextvars` mechanism.

## Decode failures

Chain-manifest decode is **ancillary state** in Wool's protocol contract: a failure to decode an incoming chain manifest never preempts the routine's primary signal (its return value or raised exception). Both `Chain.to_protobuf` and `ChainManifest.from_protobuf` emit `wool.SerializationWarning` for per-variable encode/decode failures and skip the offending entry (a failed encode suppresses the variable's reset signal too, so the receiver cannot read a phantom reset); surviving variables decode normally. A malformed wire chain id is the exception: without it the receiver cannot correlate frames to a chain, so it raises `wool.ChainSerializationError` unconditionally — a structural protocol error, not ancillary per-variable state.

Promote the warning to an error to opt into strict mode:

```python
import warnings
import wool

warnings.filterwarnings("error", category=wool.SerializationWarning)
```

Under strict mode, per-entry failures aggregate into a single `wool.ChainSerializationError` (a `wool.WoolError` subclass with the warnings on `.warnings`) raised after the encode/decode loop completes — every bad entry surfaces, not just the first. Strict mode is **fatal to the whole frame**: the partial chain manifest that `from_protobuf` accumulated (the surviving entries that decoded successfully) is *not* surfaced — every entry on the wire frame is discarded along with the failing one. Callers that need partial application semantics under strict mode must run a non-strict decode first.

Worker-side strict mode is enabled via `PYTHONWARNINGS="error::wool.SerializationWarning"`, which `multiprocessing` propagates to spawned subprocesses by default. The worker ships the aggregated `wool.ChainSerializationError` back through the routine-exception channel, so the caller catches the same error class symmetrically. When a routine *also* raises a primary exception, the `wool.ChainSerializationError` rides on it as `__cause__` via `raise primary from decode_err`, preserving the primary's class so `except RoutineError:` keeps matching. See the top-level [`wool/README.md`](../../../../README.md#decode-failure-semantics) for the full lenient/inspect/strict modes.

`wool.TaskFactoryDisplaced` follows different semantics: it is **not** ancillary state and **not** a tunable warning. Factory displacement is structurally fatal to chain propagation across every subsequent task on the loop, so `TaskFactoryDisplaced` is raised unconditionally — there is no strict-mode opt-in because there is no graceful-mode default. The raise escapes the next user-Wool operation that triggers detection (including the response-merge install path) and preempts the routine's primary signal. The primary signal remains visible via `__context__` chaining. This asymmetry is deliberate: factory displacement is loud-fail-fast on infrastructure breakage, distinct from per-variable wire corruption (`wool.SerializationWarning`) where graceful degradation is the right default.

## Exceptions and warnings

The context subsystem publishes six typed signals on the `wool` barrel. They appear inline above; this list collects them so callers know what to catch and what to promote:

- `wool.ChainContention` (exception) — raised when a chain is entered by a thread or asyncio task other than the one that owns it (see [the chain-contention guard](#the-chain-contention-guard) above). It is a `wool.WoolError` subclass carrying structured diagnostic fields (`chain_id`, `kind`, `owning_thread`/`current_thread`, `owning_task`/`current_task`) in addition to the human-readable message.
- `wool.ContextVarCollision` (exception) — raised on construction of a second `wool.ContextVar` under an existing `(namespace, name)` key. Library authors should pass `namespace=` explicitly when constructing variables from shared factory code; application code can rely on the implicit package-name inference.
- `wool.SerializationWarning` (warning) — emitted when a wire `ChainManifest` fails to encode or decode (see [Decode failures](#decode-failures) above). A `wool.WoolWarning` subclass; promote to an exception via `warnings.filterwarnings("error", ...)` (or `PYTHONWARNINGS="error::wool.SerializationWarning"` on workers) to opt into strict mode.
- `wool.SerializationError` (exception, `wool.WoolError` subclass) — the base raised when a single value fails to encode across the wire. Its strict-mode subclass `wool.ChainSerializationError` aggregates per-variable chain-manifest failures, so catching `wool.SerializationError` matches every wire serialization failure, atomic or aggregated.
- `wool.ChainSerializationError` (exception, `wool.WoolError` subclass) — raised when wire encode or decode fails under strict mode. Aggregates the promoted `wool.SerializationWarning` instances on `.warnings` (a tuple). Routine code typically does not see this directly: a result-frame decode failure raises it as the routine's primary; an exception-frame decode failure rides on the routine's exception as `__cause__` via `raise from`.
- `wool.TaskFactoryDisplaced` (exception, `wool.WoolError` subclass) — raised when Wool's task factory has been displaced from a loop it was previously installed on. Subsequent child tasks no longer fork onto fresh chains; a non-forked child inherits its parent's owning-task identity and trips `wool.ChainContention` on its first `wool.ContextVar` access. Detected reactively on the next `wool.ContextVar` access; raised unconditionally because displacement is structurally fatal to chain propagation across every subsequent task on the loop. Install Wool's factory last, or compose factories manually, to avoid this.
