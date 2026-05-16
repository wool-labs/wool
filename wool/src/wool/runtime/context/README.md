# Context

Wool's context model is founded directly on Python's stdlib `contextvars`. There is one context system, not two. `wool.ContextVar` is a `contextvars`-backed variable that additionally (a) propagates across worker boundaries and (b) opts its execution chain into Wool's linearity rules. Wool owns a single stdlib `contextvars.ContextVar` that carries all `wool.ContextVar` state as an immutable snapshot; everything else in this subsystem reads, rebuilds, or transports that snapshot.

Because Wool state rides in stdlib `contextvars`, it propagates with stdlib visibility across every conformant event loop — uvloop included — and across every *cooperative* asyncio scheduling edge: task creation, `call_soon`/`call_later`/`call_at`, `add_reader`/`add_writer`/`add_signal_handler`, and `Future.add_done_callback`. No event-loop interception is involved. OS-thread offload is the one deliberate exception — an armed chain entered from a second OS thread fails loud rather than propagating silently; see [the concurrent-entry guard](#the-concurrent-entry-guard) below.

## The snapshot

Wool chain state is held as a single immutable `Snapshot` in one Wool-owned stdlib `contextvars.ContextVar`. A `Snapshot` carries:

- `chain_id` — a UUID identifying the logical execution chain.
- `owner` — the OS thread that owns the chain (the thread dimension of the concurrent-entry guard, below).
- `owner_task` — the `asyncio.Task` that owns the chain, or `None` when it was armed outside any task (the task dimension of the concurrent-entry guard, below).
- `data` — the map of `wool.ContextVar` to its current value.
- `consumed` — the consumed-token log: `wool.Token` UUID to the `(namespace, name)` of the variable it reset.
- `stub_pins` — undeclared-stub `wool.ContextVar` instances kept alive for a lazy-import receiver.

`wool.ContextVar.get` reads its key from the active snapshot; `set` and `reset` rebuild the snapshot and reinstall it. The snapshot is immutable, so it can be shared by reference across callbacks, timers, and child tasks without intercepting the loop — a callback that sets a variable produces a *new* snapshot scoped to its own `contextvars.Context` copy, leaving the scheduling scope's snapshot untouched.

## Armed-gating

Enforcement is **armed-gated**. The Wool-owned snapshot variable defaults to unset; while it is unset the surrounding context is *unarmed* and behaves as a plain `contextvars.Context` — no chain UUID and no guard. A context is armed by either of two events: the first `wool.ContextVar.set()` on it, or a merge of incoming wire state — a worker receiving a dispatch frame, or a caller merging a routine's response back. A process that neither sets a `wool.ContextVar` nor dispatches a routine that propagates context pays nothing at all: the task factory is never installed, so task creation is untouched.

Arming mints a fresh `chain_id`, installs the first `Snapshot`, and self-installs the task factory on the running loop. Self-install is a no-op when there is no running loop — an armed context still works, but child tasks created on a later-started loop will not fork onto fresh chains until the factory is installed (call `wool.install_task_factory(loop)` explicitly in that case). Once installed, the factory is loop-wide: every task created on that loop thereafter — Wool-related or not — carries a small constant cost, its context registered for the concurrent-entry guard. Only a process that never arms any context pays nothing.

## `wool.ContextVar`

`ContextVar` mirrors `contextvars.ContextVar` at the surface — `get()`, `set()`, `reset()` — with one addition: an optional `namespace`. Its identity model is structural, not object-based. Every `ContextVar` has a `(namespace, name)` key; two `ContextVar(name="foo", namespace="bar")` calls in the same process resolve to the *same* singleton via the process-wide var registry, and collisions on the same key with mismatched declarations raise `ContextVarCollision`. The namespace is inferred from the top-level package of the calling frame when not passed explicitly, which keeps wire keys stable when modules are refactored deeper within the same package.

The `(namespace, name)` identity is what survives pickle round-trips. A `wool.ContextVar` arriving on the wire from a worker reduces by `(namespace, name)` and re-resolves to the same instance on the receiver. Pickling is gated through Wool's pickler; vanilla `pickle.dumps` is rejected via `__reduce_ex__` to keep the registry-bound semantics from leaking into ad-hoc serialization.

`get()` returns the value bound in the active snapshot, falling back to the supplied default, then the constructor default, then raising `LookupError`. `set()` rebuilds the snapshot with the new binding and returns a `Token`.

## `wool.Token`

Tokens carry a UUID, the `(namespace, name)` of the variable they reset, and the `chain_id` of the snapshot in which `set()` ran. Live tokens are deduplicated process-wide, so pickle round-trips within a process preserve identity. Across the wire, the consumed-state bit rides on the wire frame so a worker's `reset()` correctly observes consumption that originated upstream, and vice versa.

`ContextVar.reset(token)` raises `ValueError` if the active snapshot's `chain_id` differs from the one the token was minted under — the same isolation that makes concurrent tasks on a worker safe, since a task fork mints a fresh chain. This holds across pickle round-trips. `Token.MISSING` is the singleton sentinel returned for `old_value` when the variable had no prior binding.

## Fork-on-task

Wool installs a task factory on every loop it runs on (composed with any user-installed factory; Wool's factory must be installed *last* — a third-party factory installed after it silently drops copy-on-fork for subsequently-created tasks, and the next `wool.ContextVar` access emits a `RuntimeWarning` once Wool detects the displacement). Every task the factory creates — armed or unarmed — carries one Wool `add_done_callback` (visible as `cb=[...]` in `repr()`) and has its `contextvars.Context` registered for the concurrent-entry guard.

When a new `asyncio.Task` is created in an *armed* context, the factory additionally wraps the child coroutine in a `_forked_scope` coroutine so the child runs under a *forked* snapshot: it inherits the parent's variable bindings but receives a freshly minted `chain_id`. The wrapper has visible consequences for an armed task: `Task.get_coro()` and `repr()`'s coroutine field reflect `_forked_scope` rather than the user coroutine, tracebacks gain one wrapper frame, and the wrapper coroutine surfaces in `TaskGroup`/`gather` exception output. A task created in an *unarmed* context is not wrapped — its coroutine, `get_coro()`, and auto-generated name match a plain `asyncio.Task` — but it still carries the Wool done-callback and guard registration noted above, so it is not byte-for-byte identical to a task created without Wool's factory.

The fresh chain UUID is correct fork semantics: it isolates concurrent child tasks from each other and from their parent, and it is why a child task cannot `reset` a `Token` minted in its parent's chain.

## The concurrent-entry guard

Wool enforces strictly serial execution within a chain: at most one OS thread *and* one `asyncio.Task` may run code under a given chain at a time. The guard has two dimensions. The thread dimension compares the accessing OS thread against the chain's `owner` thread; the task dimension compares the running `asyncio.Task` against the chain's `owner_task` — the task that armed or forked the chain. Both are armed-gated — they engage only once a chain exists.

Cooperatively-scheduled work on the loop's thread never trips the guard. Event-loop callbacks and timers scheduled on the loop thread inherit the scheduling scope's snapshot and run on the loop's own thread with no running task, so they share the chain but can never run concurrently with its owner. The exception is `loop.call_soon_threadsafe` called from another OS thread: it captures *that* thread's context, so a callback scheduled from a thread that armed its own chain runs the chain off its owner thread and raises `wool.ConcurrentChainEntry` — by design, since the chain genuinely spans two threads. Child tasks never trip the guard for the opposite reason: the task factory forks every child onto a fresh `chain_id` and a fresh `owner_task`, so a child never enters the parent's chain at all.

Genuine OS-thread parallelism does trip the thread dimension. Plain `asyncio.to_thread` from an armed context copies the surrounding `contextvars` context — chain UUID and all — into an executor thread, placing a second runner on one chain in parallel; the first `wool.ContextVar` access from that thread raises `wool.ConcurrentChainEntry`.

Two `asyncio.Task` objects sharing one chain trip the task dimension. The task factory raises `wool.ConcurrentChainEntry` up front when an *armed* `contextvars.Context` already driving a live task is handed to a second `create_task` — that creation-time rejection is itself armed-gated, so sharing an *unarmed* context across tasks is permitted, exactly as stdlib asyncio permits it. If an unarmed shared context is armed *later*, the `owner_task` guard catches the second task the moment it touches a `wool.ContextVar` on a chain another live task already owns. Each task should still run in its own context: omit `context=` (asyncio copies it per task by default) or pass a fresh `contextvars.copy_context()` to each.

`wool.to_thread(func, *args, **kwargs)` is the supported alternative. It mirrors `asyncio.to_thread` but forks Wool chain state: the worker thread runs under a freshly minted, **detached** chain (a copy of the caller's bindings under a new `chain_id` owned by the worker thread, with no merge-back). Unlike a `@wool.routine` dispatch — which back-propagates the worker's `wool.ContextVar` mutations to the caller — a `wool.to_thread` offload is write-isolated: mutations the offloaded function makes are not visible to the caller, matching `asyncio.to_thread`'s copy-in semantics. Use it whenever Wool-aware work must run in another OS thread.

## Wire propagation

Each gRPC dispatch frame carries a `Context` protobuf message containing the snapshot's `chain_id` and a list of `ContextVar` entries — each with `(namespace, name)`, an optional serialized value, and consumed-token UUIDs that reset the variable in the sender's chain.

- `encode_snapshot()` snapshots the sender's bindings and consumed-token log; only variables that are bound or have consumed tokens emit entries (default-only values are absent).
- `decode_snapshot()` decodes a wire `Context` into a `Snapshot`: each variable resolves through the var registry (or registers a stub if the receiver hasn't yet imported the declaring module), values are deserialized, and consumed-token UUIDs flip the matching live tokens to consumed.
- `merge_snapshot()` applies a decoded snapshot into the active one — variable mutations and reset signals both propagate. It runs on the caller (merging the worker's snapshot back from the response) and on the worker for mid-stream per-step requests; the worker's *initial* dispatch frame instead installs the decoded snapshot wholesale (next paragraph), not via a merge.

On the worker side, the caller's snapshot is decoded, installed under the worker-loop task (re-stamped so the worker thread owns the chain), and the routine runs inside it. On completion the resulting snapshot is encoded onto the response so the caller sees mutations made on the worker. Async generators bidirectionally exchange context at every yield/next boundary using the same machinery.

## Stub promotion

A `wool.ContextVar` may arrive on the wire — embedded in a pickled object graph or as an entry in a wire `Context` snapshot — before the receiver's process has imported the module that declares it. To preserve the propagated value across that gap, the receiver registers a placeholder `wool.ContextVar` (a *stub*) under the wire key.

Two ingress paths share the same placeholder slot via `resolve_stub`: a pickled `wool.ContextVar` reconstituting on the receiver (which carries the original constructor default), and a wire `Context` snapshot entry (which carries no default and is created default-less). A stub stays alive — kept by the embedding object graph for a pickled instance, or by the decoded `Snapshot`'s `stub_pins` for a wire entry — until the receiver's user code constructs `wool.ContextVar(...)` under its key, at which point the placeholder is promoted in place. If the receiver never declares the variable, the stub is collected with whatever held it and the propagated value is dropped.

## `RuntimeContext` and `dispatch_timeout`

`RuntimeContext` carries block-scoped runtime option overrides — currently just `dispatch_timeout` — separate from the `wool.ContextVar` snapshot. It auto-captures on every `Task` at construction time and rides the wire on every dispatch, encoded by `RuntimeContext.to_protobuf()`, so the worker restores the caller's effective `dispatch_timeout` before running the routine.

```python
import wool

with wool.RuntimeContext(dispatch_timeout=30):
    result = await my_routine()
```

`dispatch_timeout` itself is a plain stdlib `contextvars.ContextVar[float | None]`, distinct from the Wool-owned snapshot variable. `RuntimeContext.__enter__` / `__exit__` `set` and `reset` it through the standard `contextvars` mechanism.

## Decode failures

Wire-context decode is **ancillary state** in Wool's protocol contract: a failure to decode an incoming context never preempts the routine's primary signal (its return value or raised exception). Both `encode_snapshot` and `decode_snapshot` emit `wool.ContextDecodeWarning` for per-variable encode/decode failures and skip the offending entry; surviving variables decode normally. A malformed wire chain UUID falls back to a fresh UUID with the failure recorded as the same warning class.

Promote the warning to an error to opt into strict mode:

```python
import warnings
import wool

warnings.filterwarnings("error", category=wool.ContextDecodeWarning)
```

Under strict mode, per-entry failures aggregate into a single `BaseExceptionGroup` raised after the encode/decode loop completes — every bad entry surfaces, not just the first. Worker-side strict mode is enabled via `PYTHONWARNINGS="error::wool.ContextDecodeWarning"`, which `multiprocessing` propagates to spawned subprocesses by default. The worker ships the promoted warning back through the routine-exception channel, so the caller catches the same `wool.ContextDecodeWarning` class symmetrically. See the top-level [`wool/README.md`](../../../../README.md#decode-failure-semantics) for the full lenient/inspect/strict modes.
