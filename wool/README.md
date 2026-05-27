![](https://raw.githubusercontent.com/wool-labs/wool/refs/heads/main/assets/woolly.png)

**Wool** is a distributed Python runtime that executes tasks in a horizontally scalable pool of agnostic worker processes without introducing a centralized scheduler or control plane. Instead, Wool routines are dispatched directly to a decentralized peer-to-peer network of workers. Cluster lifecycle and node orchestration can remain with purpose-built tools like Kubernetes — Wool focuses solely on distributed execution.

Any async function or generator can be made remotely executable with a single decorator. Serialization, routing, and transport are handled automatically. From the caller's perspective, the function retains its original async semantics — return types, streaming, cancellation, and exceptions all behave as expected.

Wool provides best-effort, at-most-once execution. There is no built-in coordination state, retry logic, or durable task tracking. Those concerns remain application-defined.

## Installation

### Using pip

```sh
pip install wool
```

Wool publishes release candidates for major and minor releases, use the `--pre` flag to install them:

```sh
pip install --pre wool
```

### Cloning from GitHub

```sh
git clone https://github.com/wool-labs/wool.git
cd wool
pip install ./wool
```

## Quick start

```python
import asyncio
import wool


@wool.routine
async def add(x: int, y: int) -> int:
    return x + y


async def main():
    async with wool.WorkerPool(spawn=4):
        result = await add(1, 2)
        print(result)  # 3


asyncio.run(main())
```

## Routines

A Wool routine is an async function decorated with `@wool.routine`. When called, the function is serialized and dispatched to a worker in the pool, with the result streamed back to the caller. Invocation is transparent — you call a routine like any async function, with no special method required. For coroutines, `routine(args)` returns a coroutine and dispatch occurs on `await`. For async generators, `routine(args)` returns an async generator and dispatch occurs on first iteration.

```python
@wool.routine
async def fib(n: int) -> int:
    if n <= 1:
        return n
    async with asyncio.TaskGroup() as tg:
        a = tg.create_task(fib(n - 1))
        b = tg.create_task(fib(n - 2))
    return a.result() + b.result()
```

Async generators are also supported for streaming results:

```python
@wool.routine
async def fib(n: int):
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, a + b
```

The decorated function, its arguments, returned or yielded values, and exceptions must all be serializable via `cloudpickle`. Instance, class, and static methods are all supported.

### Dispatch gate

Under the hood, the `@wool.routine` decorator replaces the function with a wrapper that checks a `do_dispatch` context variable. This is a `ContextVar[bool]` that defaults to `True` and acts as a dispatch gate — when `True`, calling a routine packages the call into a task and sends it to a remote worker. Workers set `do_dispatch` to `False` before executing the function body, preventing infinite re-dispatch. The variable is restored to `True` for any nested `@wool.routine` calls within the function, so those dispatch normally to other workers.

### Coroutines vs. async generators

Coroutines and async generators follow different dispatch paths. A coroutine dispatches as a single request-response: the worker runs the function and returns one result. An async generator uses pull-based bidirectional streaming: the client sends `next`/`send`/`throw` commands, the worker advances the generator one step per command and streams each yielded value back. The worker pauses between yields until the client requests the next value.

## Tasks

A task is a dataclass that encapsulates everything needed for remote execution: a unique ID, the async callable, its args/kwargs, a serialized `WorkerProxy` (enabling the receiving worker to dispatch its own tasks to peers), an optional timeout, and caller-tracking for nested tasks. Tasks are created automatically when a `@wool.routine`-decorated function is invoked — you never construct one manually.

### Nested task tracking

When a task is created inside an already-executing task, the new task automatically captures the parent task's UUID into its own `caller` field. This builds a parent-to-child chain so the system can trace which task spawned which.

### Proxy serialization

Each task carries a serialized `WorkerProxy`. When a task executes on a remote worker, it may call other `@wool.routine` functions (nested dispatch). The deserialized proxy is set as the worker's active proxy context variable, giving the remote task the ability to dispatch sub-tasks to other workers in the pool.

### Serialization

Task serialization has two layers. [cloudpickle](https://github.com/cloudpipe/cloudpickle) serializes Python objects — the callable, args, kwargs, and proxy — into bytes. cloudpickle is used instead of the standard `pickle` module because it can serialize objects that `pickle` cannot, including interactively-defined functions, closures, and lambdas. This is essential because `@wool.routine`-decorated functions and their arguments must be fully serializable for transmission to arbitrary worker processes.

[Protocol Buffers](https://protobuf.dev/) provides the wire format. Scalar task metadata (id, caller, tag, timeout) maps directly to protobuf fields, while Python-specific objects are nested as cloudpickle byte blobs. The protobuf definitions in the `proto/` directory define the gRPC wire protocol for task dispatch, acknowledgment, and result streaming between workers.

## Context propagation

Python's `contextvars.ContextVar` cannot be pickled — it's a C extension type that explicitly blocks serialization — so ambient state has no built-in way to cross process boundaries. `wool.ContextVar` solves this by mirroring the stdlib API (`get`, `set`, `reset`) and adding automatic propagation across the dispatch chain.

```python
import asyncio

import wool

tenant_id: wool.ContextVar[str] = wool.ContextVar("tenant_id", default="unknown")


@wool.routine
async def handle_request() -> str:
    return tenant_id.get()


async def main():
    async with wool.WorkerPool(spawn=2):
        token = tenant_id.set("acme-corp")
        try:
            result = await handle_request()
            print(result)  # "acme-corp" — propagated to the worker
        finally:
            tenant_id.reset(token)


asyncio.run(main())
```

Two construction modes are supported:

- `wool.ContextVar("name")` — no default; `get()` raises `LookupError` until a value is set.
- `wool.ContextVar("name", default=...)` — `get()` returns the default when the variable has no value in the current context.

`set()` returns a `Token` whose `reset()` restores the prior value (or the default if none was set), mirroring the stdlib `contextvars` API. Tokens are single-use across the logical chain — see Limitations below for the cross-task and cross-process scoping rules.

Each var's namespace is inferred from the top-level package of the calling frame, producing a `"<namespace>:<name>"` key that is stable across every process in the cluster. Library authors constructing vars from shared factory code should pass `namespace=` explicitly to avoid collisions with application-scope vars under the same package.

### How propagation works

At dispatch time, Wool encodes the active chain context — the `wool.ContextVar` bindings explicitly `set()` in the current chain, default-only values excluded — into a `Context` protobuf message. The message carries the chain id plus one entry per bound (or reset) variable, each holding the variable's `(namespace, name)` key and its cloudpickled value, and rides every dispatch frame.

A `wool.ContextVar` is identity, not storage: it serializes (through Wool's own pickler — vanilla `pickle` is rejected) as just its `(namespace, name)` key and constructor default, never a value. Values travel only in the wire context, so a `wool.ContextVar` referenced across a task's args, kwargs, and the context all reconstitutes to the same local instance on the receiver. If the receiver has not yet imported the module that declares the variable, a placeholder "stub" instance is registered under the key; when the declaring `wool.ContextVar(...)` call later runs, it promotes the stub in place, preserving reference identity and any propagated value.

On the worker, the routine runs under the caller's chain — a context decoded from the dispatch frame, carrying the caller's chain id and propagated values. When the worker returns (or yields), the resulting context is encoded onto the gRPC response and merged on the caller side, so worker-side mutations flow back automatically. For async generators, the caller also attaches its current context to each iteration request, enabling bidirectional state exchange between caller and worker at every yield/next boundary.

### Isolation

Each dispatched routine runs under the caller's chain, and concurrent tasks on the same worker each carry their own context — concurrent tasks with different values for the same variable never interfere. Worker-side mutations (via `set()`) are back-propagated to the caller when the task returns or yields, but they do not leak to other concurrent tasks: each `asyncio.create_task` child forks a copy of the parent's context under a fresh chain id (copy-on-fork via Wool's task factory), so concurrent execution paths never share mutable Wool state and bidirectional value propagation stays coherent under the transparent-dispatch model.

### Decode failure semantics

Context propagation is **ancillary state** in wool's wire protocol — a separate channel from the routine's primary signal (its return value or raised exception). When a wire context fails to decode (cross-version pickle skew, custom class missing on the receiver, on-wire corruption of a single var value), wool never preempts the primary signal to surface the ancillary failure. The routine's outcome is delivered, and the failure is reported via Python's standard `warnings` mechanism with a `wool.ContextDecodeWarning` so callers can decide how to respond.

Three modes are available, and they compose with the standard Python warnings system rather than wool-specific API:

| Mode | How to enable | Behavior |
| ---- | ------------- | -------- |
| Lenient (default) | _no opt-in_ | Decode failure emits `wool.ContextDecodeWarning`; primary signal returned. Caller-side exception frames also receive the failure on `__notes__`. |
| Inspect | `warnings.catch_warnings(record=True)` | Decode failure captured into a list; primary signal returned. Standard pattern for "best effort with audit trail". |
| Strict | `warnings.filterwarnings("error", category=wool.ContextDecodeWarning)` | Decode failure raises (the warning is promoted to an exception); primary signal lost. |

The lenient default keeps wool useful for callers that treat tracing-style state as advisory. Strict mode is for callers whose correctness depends on context state and prefer to fail fast. Inspect mode is the right choice when you want both the primary signal and visibility into ancillary failures:

```python
import warnings
import wool

with warnings.catch_warnings(record=True) as captured:
    warnings.simplefilter("always", category=wool.ContextDecodeWarning)
    result = await some_routine()  # always returns
    decode_failures = [w for w in captured if issubclass(w.category, wool.ContextDecodeWarning)]
if decode_failures:
    log.warning("context propagation degraded for %d frame(s)", len(decode_failures))
```

The same semantics apply on both sides of the wire: the worker emits `ContextDecodeWarning` when a request context fails to decode (each unreadable entry is dropped individually so the routine still runs under whatever partial context decoded; only when the whole frame is unreadable does the worker context remain unarmed), and the caller emits `ContextDecodeWarning` when a response context fails to decode (and delivers the result anyway). On the caller side, an exception-frame decode failure additionally rides on the routine's exception via `__notes__` so the failure surfaces in tracebacks. On the worker side, a context encode failure that coincides with a routine exception rides similarly on the routine exception via `__notes__`. There is no `ExceptionGroup` chaining and no wrapper-exception API to learn — just a standard warning class and standard `try/except` around primary signals.

#### Worker-side strict mode

Strict mode applies symmetrically on the worker side via Python's standard `PYTHONWARNINGS` environment variable, which `multiprocessing` propagates to spawned worker subprocesses by default:

```bash
export PYTHONWARNINGS="error::wool.ContextDecodeWarning"
python my_app.py
```

Or programmatically before constructing the pool:

```python
import os
os.environ["PYTHONWARNINGS"] = "error::wool.ContextDecodeWarning"

import wool

async with wool.WorkerPool():
    ...   # workers spawned now promote the warning to an exception
```

When the worker promotes the warning to an exception, wool ships it back through the routine-exception channel, so the caller catches the exact same `wool.ContextDecodeWarning` class — symmetric with caller-side strict mode. No `RpcError` to special-case, no out-of-band wire metadata.

### Task forking and thread offload

`wool.ContextVar` state rides in stdlib `contextvars`, so it propagates into child tasks, event-loop callbacks, timers, and `Future` done-callbacks with no special API — exactly as stdlib `contextvars` does. Wool's task factory (self-installed on the running loop the first time a `wool.ContextVar` is set, or explicitly via `wool.install_task_factory(loop)`) adds one thing: every child `asyncio.Task` created in an armed context is forked onto a fresh chain id, so concurrent tasks never share a mutable chain. Two caveats: if other libraries also install a task factory, Wool's must be installed *last* — a factory installed after Wool's silently drops fork-on-task, and a later `wool.ContextVar` access raises `wool.TaskFactoryDisplaced` (a `RuntimeError` subclass) once Wool detects it has been displaced — and an armed task's coroutine is wrapped, so `Task.get_coro()` and `repr()`'s coroutine field reflect the wrapper rather than the user coroutine. Every task the factory creates carries one Wool done-callback (visible as `cb=[...]` in `repr()`); a task created in an unarmed context is otherwise coroutine-identical to a plain `asyncio.Task` created on the same loop — its auto-generated `Task-N` name draws from that loop implementation's own counter.

Offloading to another OS thread is the one case that needs care. Wool enforces a single runner per chain, so a plain `asyncio.to_thread()` from a context that has set a `wool.ContextVar` would place a second runner on the caller's chain in genuine parallelism — the first `wool.ContextVar` access in the worker thread raises `wool.ChainContention`. Use `wool.to_thread()` instead; it mirrors `asyncio.to_thread` but forks a fresh, detached chain for the worker thread:

```python
result = await wool.to_thread(cpu_bound, payload)
```

A bare `loop.run_in_executor(None, func)` is different again: stdlib `run_in_executor` copies no context at all, so `func` runs with no Wool chain — no `wool.ChainContention`, but no propagation either, and `wool.ContextVar` reads in the worker fall through to their defaults. Reach for `wool.to_thread()` whenever the offloaded work needs the caller's bindings.

### Backpressure hooks

`BackpressureLike` hooks run with the caller's propagated `wool.ContextVar` context installed, so a hook can read caller-provided values (e.g., a tenant id) to make admission decisions without the caller having to plumb them through the `BackpressureContext` explicitly.

### Runtime options

`wool.RuntimeContext` carries block-scoped runtime-option overrides — currently just the dispatch timeout — separate from the `wool.ContextVar` context. Used as a context manager it overrides the ambient timeout for every `@wool.routine` dispatch in the block, and it is auto-captured on every `Task` at construction so the worker restores the caller's effective timeout before running the routine:

```python
import wool

with wool.RuntimeContext(dispatch_timeout=30):
    result = await my_routine()
```

The underlying `dispatch_timeout` is a plain stdlib `contextvars.ContextVar[float | None]` (`None` means no timeout), distinct from the Wool-owned context variable that carries `wool.ContextVar` state.

### Limitations

- **Values must be _cloudpicklable_.** A `TypeError` naming the offending variable is raised at dispatch time if serialization fails.
- **Only explicitly set values propagate.** A variable that has never been `set()` (only has a class-level default) is not included in the context — the worker falls through to its own default.
- **Receivers must eventually declare the var.** Until the worker imports the module that constructs the var, the wire-shipped value is held on a stub kept alive by the decoded context; a later `wool.ContextVar(...)` declaration promotes the stub and the propagated value applies transparently. If the worker never declares the var, the stub is collected with that context and the value is dropped.
- **Tokens are scoped to their originating chain.** A `Token` minted inside a task cannot be reset from a different chain — including after crossing an `asyncio.create_task` fork boundary, since child tasks receive a fresh chain id. Reset the token in the same logical chain that produced it, or use `var.set(...)` to install a new value without relying on the token.
- **Wire keys are tied to the top-level package name.** Renaming the top-level package (e.g., `myapp` → `myapp_v2`) changes every var's wire key, so a rolling deploy that has callers and workers on different top-level names will silently drop propagated values on the mismatched side. Keep the top-level package name stable across rolling deploys, or bridge the transition with explicit `namespace=` overrides. Moving a module deeper within the same top-level package is safe — the key is the package root, not the full module path.

## Worker pools

`WorkerPool` is the main entry point for running routines. It orchestrates worker subprocess lifecycles, discovery, and load-balanced dispatch. The pool supports four configurations depending on which arguments are provided:

| Mode | `spawn` | `discovery` | `lease` | Behavior |
| ---- | ------- | ----------- | ------- | -------- |
| Default | omitted | omitted | optional | Spawns `cpu_count` local workers with internal `LocalDiscovery`. |
| Ephemeral | set | omitted | optional | Spawns N local workers with internal `LocalDiscovery`. |
| Durable | omitted | set | optional | No workers spawned; connects to existing workers via discovery. |
| Hybrid | set | set | optional | Spawns local workers and discovers remote workers through the same protocol. |

**Default** — no arguments needed:

```python
async with wool.WorkerPool():
    result = await my_routine()
```

**Ephemeral** — spawn a fixed number of local workers, optionally with tags:

```python
async with wool.WorkerPool("gpu-capable", spawn=4):
    result = await gpu_task()
```

**Durable** — connect to workers already running on the network:

```python
async with wool.WorkerPool(discovery=wool.LanDiscovery()):
    result = await my_routine()
```

**Hybrid** — spawn local workers and discover remote ones:

```python
async with wool.WorkerPool(spawn=4, discovery=wool.LanDiscovery()):
    result = await my_routine()
```

`spawn` controls how many workers the pool starts — it does not cap the total number of workers available. In Hybrid mode, additional workers may join via discovery beyond the initial `spawn`.

`lease` caps how many additionally discovered workers the pool will admit. The total pool capacity is `spawn + lease` when both are set, or just `lease` for discovery-only pools. Defaults to `None` (unbounded). The lease count is a cap on admission, not a reservation — discovered workers may serve multiple pools simultaneously, and there is no guarantee that a leased slot will remain filled for the life of the pool.

```python
# Spawn 4 local workers, accept up to 4 more from discovery (8 total)
async with wool.WorkerPool(spawn=4, lease=4, discovery=wool.LanDiscovery()):
    result = await my_routine()

# Durable pool capped at 10 discovered workers
async with wool.WorkerPool(discovery=wool.LanDiscovery(), lease=10):
    result = await my_routine()
```

`lazy` controls whether the pool's internal `WorkerProxy` defers startup until the first task is dispatched. Defaults to `True`. The pool propagates this flag to every `WorkerProxy` it constructs, and each task serializes the proxy (including the flag) so that workers receiving the task inherit the same laziness setting. With `lazy=True`, worker subprocesses that never invoke nested `@wool.routine` calls avoid the cost of discovery subscription and worker-sentinel task setup entirely. Set `lazy=False` to start proxies eagerly — useful when you want connections established before the first dispatch.

```python
# Eager proxy startup — connections established before first dispatch
async with wool.WorkerPool(spawn=4, lazy=False):
    result = await my_routine()
```

## Workers

A worker is a separate OS process hosting a gRPC server with two RPCs: `dispatch` (bidirectional streaming for task execution) and `stop` (graceful shutdown). Tasks execute on a dedicated asyncio event loop in a separate daemon thread, so that long-running or CPU-intensive task code does not block the main gRPC event loop. This keeps the worker responsive to new dispatches, stop requests, and concurrent streaming interactions with in-flight tasks.

The dispatch RPC uses bidirectional streaming — both client and server send messages on the same gRPC stream concurrently. The client sends a task submission, then iteration commands (`next`, `send`, `throw`) for generators. The server sends an acknowledgment, then result or exception frames. This enables pull-based flow control where the client dictates pacing.

## Discovery

Workers discover each other through pluggable discovery backends with no central coordinator. Each worker carries a full `WorkerProxy` enabling direct peer-to-peer task dispatch — every node is both client and server. Discovery separates publishing (announcing worker lifecycle events) from subscribing (reacting to them).

### Discovery events

A `DiscoveryEvent` pairs a type — one of `worker-added`, `worker-dropped`, or `worker-updated` — with the affected worker's `WorkerMetadata` (UID, address, pid, version, tags, security flag, and transport options). Discovery subscribers yield these events as the set of known workers changes.

### Built-in protocols

Wool ships with two discovery protocols:

- **`LocalDiscovery`** — shared-memory IPC for single-machine pools. Publishers write worker metadata into a named shared memory region (`multiprocessing.SharedMemory`), using cross-process file locking (`portalocker`) for synchronization. Subscribers attach to the same region, diff its contents against a local cache, and emit discovery events for changes. A notification file touched by publishers after each write wakes subscribers via `watchdog` filesystem monitoring, with optional fallback polling. This is the default when no discovery is specified.

- **`LanDiscovery`** — Zeroconf DNS-SD (`_wool._tcp.local.`) for network-wide discovery. Publishers register, update, and unregister `ServiceInfo` records via `AsyncZeroconf`. Subscribers use `AsyncServiceBrowser` to listen for service changes and convert Zeroconf callbacks into Wool `DiscoveryEvent`s. No central coordinator or shared state is required.

Custom discovery protocols are supported via structural subtyping — implement the `DiscoveryLike` protocol and pass it to `WorkerPool`.

## Load balancing

The load balancer decides which worker handles each dispatched task. The `WorkerProxy` maintains a load balancer and a context of discovered workers with gRPC connections. It waits for at least one worker to become available, then the load balancer selects one.

Wool ships with `RoundRobinLoadBalancer` (the default), which maintains a per-context index that cycles through the ordered worker list. On each dispatch it tries the worker at the current index: on success, it advances the index and returns the result stream; on transient error, it skips to the next worker; on non-transient error, it evicts the worker from the context. It gives up after one full cycle of all workers.

Custom load balancers are supported via structural subtyping — implement the `LoadBalancerLike` protocol and pass it to `WorkerPool`:

```python
async with wool.WorkerPool(spawn=4, loadbalancer=my_balancer):
    result = await my_routine()
```

### Transient vs. non-transient errors

Transient errors are the gRPC status codes `UNAVAILABLE`, `DEADLINE_EXCEEDED`, and `RESOURCE_EXHAUSTED` — temporary conditions that may resolve on retry to the same or another worker. Non-transient errors are all other gRPC failures (e.g., `INVALID_ARGUMENT`, `PERMISSION_DENIED`) indicating persistent problems. The load balancer skips transient-error workers but evicts non-transient-error workers from the pool.

### Self-describing worker connections

Workers are self-describing: each worker advertises its gRPC transport configuration via `ChannelOptions` in its `WorkerMetadata`. When a client discovers a worker, it reads the advertised options and configures its channel to match — message sizes, keepalive intervals, concurrency limits, and compression are all set automatically. There is no separate client-side configuration step; the worker's metadata is the single source of truth for how to connect to it.

A `WorkerConnection` is a gRPC client managing a pooled channel to a single worker address. It serializes and sends a task over a bidirectional stream, waits for an acknowledgment, then returns an async generator that streams results back. The channel's concurrency semaphore is sized by the worker's advertised `max_concurrent_streams`, and gRPC errors are classified as transient or non-transient for the load balancer.

Channels are pooled with reference counting and a 60-second TTL. A dispatch acquires a pool reference, and the result stream holds its own reference to keep the channel alive during streaming. There is no pool-level health checking — dead channels are detected reactively when a dispatch attempt fails, and the failed worker is removed from the load balancer context by the error classification logic.

### Transport configuration

Transport options are split into two tiers:

- **`ChannelOptions`** — settings that both the server and client apply symmetrically. Workers advertise these via `WorkerMetadata` so clients connect with identical settings. Includes message sizes (`max_receive_message_length`, `max_send_message_length`), keepalive (`keepalive_time_ms`, `keepalive_timeout_ms`, `keepalive_permit_without_calls`, `max_pings_without_data`), flow control (`max_concurrent_streams`), and compression (`compression`).

- **`WorkerOptions`** — composes a `ChannelOptions` instance with server-only settings that are not communicated to clients: `http2_min_recv_ping_interval_without_data_ms` (minimum allowed client ping interval), `max_ping_strikes` (ping violations before GOAWAY), and optional connection lifecycle limits (`max_connection_idle_ms`, `max_connection_age_ms`, `max_connection_age_grace_ms`).

All options default to gRPC's own defaults. Pass a `WorkerOptions` instance to `LocalWorker` or `WorkerProcess` to customize:

```python
from wool.runtime.worker.base import ChannelOptions, WorkerOptions
from wool.runtime.worker.local import LocalWorker

options = WorkerOptions(
    channel=ChannelOptions(
        keepalive_time_ms=10_000,
        keepalive_timeout_ms=5_000,
        max_concurrent_streams=50,
    ),
    max_connection_idle_ms=300_000,
)

async with wool.WorkerPool(
    spawn=4,
    worker=lambda *tags, credentials=None: LocalWorker(
        *tags, credentials=credentials, options=options,
    ),
):
    result = await my_routine()
```

### Connection failure detection

gRPC runs over HTTP/2, which provides connection-level error signaling via GOAWAY and RST_STREAM frames. When a peer drops abruptly, the OS eventually closes the TCP socket and the HTTP/2 layer surfaces the broken connection to gRPC as a stream error.

Wool configures HTTP/2 keepalive pings on both client and server channels. When keepalive is enabled (the default), both sides send periodic PING frames and expect a PONG response within `keepalive_timeout_ms`. If no response arrives, the connection is considered dead and gRPC raises an error on the next operation. This detects silently dead connections — where the remote peer is unreachable but no TCP reset was received — without waiting for the next application-level read or write.

On the client side, a dead worker surfaces as `UNAVAILABLE` (transient), causing the load balancer to skip to the next worker. On the worker side, a disconnected client is detected when the stream iterator terminates, at which point the worker closes the running generator if one is active.

## Security

`WorkerCredentials` is a frozen dataclass holding PEM-encoded CA certificate, worker key, and worker cert bytes plus a `mutual` flag. It exposes `server_credentials` and `client_credentials` properties that produce the appropriate gRPC TLS objects. Workers bind a secure gRPC port when credentials are present, and proxies open secure channels to connect.

```python
creds = wool.WorkerCredentials.from_files(
    ca_path="certs/ca-cert.pem",
    key_path="certs/worker-key.pem",
    cert_path="certs/worker-cert.pem",
    mutual=True,
)

async with wool.WorkerPool(spawn=4, credentials=creds):
    result = await my_routine()
```

### Mutual TLS vs. one-way TLS

With mutual TLS (`mutual=True`), the server requires client authentication — both sides present and verify certificates signed by the same CA. With one-way TLS (`mutual=False`), the server presents its certificate for the client to verify, but the client remains anonymous at the transport layer. The `mutual` flag controls `require_client_auth` on the server and whether the client includes its key and cert when opening the channel.

### Discovery security filtering

Each `WorkerMetadata` carries a `secure` boolean flag set at startup based on whether the worker was given credentials. The `WorkerProxy` applies a security filter to discovery events: a proxy with credentials only accepts workers with `secure=True`, and a proxy without credentials only accepts workers with `secure=False`. This prevents secure proxies from connecting to insecure workers and vice versa, but does not guard against incompatible credentials between two secure peers (e.g., certificates signed by different CAs).

If a TLS handshake fails (e.g., incompatible, invalid, or expired certificates), gRPC surfaces it as an `RpcError`. The `WorkerConnection` classifies the error by status code using the same transient/non-transient logic as any other gRPC failure — there is no TLS-specific error handling path.

## Error handling

A dispatch crosses two processes and several stages on each side; failures can originate at any of them. The caller's observable depends on **which phase** failed and **whether the source was wool-internal or user code**. The contract is summarized below and detailed phase by phase.

| Phase | Wool-internal failure → caller sees | User-code failure → caller sees | Load balancer action |
| ----- | ----------------------------------- | ------------------------------- | -------------------- |
| Caller-side request encoding | n/a (no transport involved yet) | Original `Exception` (unwrapped) | None — no worker contacted |
| gRPC handshake | `TransientRpcError` (transient codes) or `RpcError` (non-transient, incl. `FAILED_PRECONDITION` for version mismatch) | n/a | Skip on transient; evict on non-transient |
| Worker-side request decoding | `Rejected.original` re-raised on the caller (typed) | Strict-mode `wool.ContextDecodeError` re-raised on the caller | None — typed re-raise |
| Routine execution | n/a | Original routine exception (type and traceback preserved); ancillary `wool.ContextDecodeError` chained on `__cause__` | None |
| Worker-side response encoding | Routine exception chained from strict-mode `wool.ContextDecodeError` via `__cause__` | n/a | None |
| Caller-side response decoding | `UnexpectedResponse` (malformed payload, missing class, version skew) | n/a | None — caller-fault, worker is healthy |
| Post-execution teardown | Swallowed (the wire is already closed) | n/a | n/a |

In every case the caller sees a single exception — wool does not wrap routine failures in `ExceptionGroup` or any wool-specific wrapper class. Ancillary signals (context decode failures, context encode failures coincident with a routine exception) ride on the primary exception as `__cause__` via `raise primary from decode_err`, so existing `except RoutineError:` clauses keep matching and the decode error remains visible in the traceback through cause chaining.

```python
try:
    result = await my_routine()
except ValueError as e:
    print(f"Task failed: {e}")
```

### Caller-side request encoding

Before dispatch, the caller's `WorkerConnection` serializes the routine callable, args, kwargs, and the serialized `WorkerProxy` into a protobuf via `cloudpickle`. A failure here is a user-code bug (a routine argument that closes over an unpicklable object, a circular reference, etc.) and is **not a worker-health concern**. The original exception propagates unwrapped to the caller and the load balancer takes no action; no worker was contacted.

### gRPC handshake

The handshake opens the bidirectional stream, writes the task frame, and reads the first response. Failures classify by gRPC status code:

- **Transient** (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`) — surfaces as `TransientRpcError`. The load balancer skips to the next worker. `NoWorkersAvailable` is raised if a full cycle of all workers is exhausted without success.
- **Non-transient** (everything else, including `FAILED_PRECONDITION` for protocol-version mismatch and `INTERNAL` for handler-side bugs) — surfaces as `RpcError`. The load balancer evicts the worker from its context.

Version compatibility is checked by `VersionInterceptor` **before** the dispatch handler runs. A mismatch aborts the RPC with `FAILED_PRECONDITION`; the caller observes `RpcError(FAILED_PRECONDITION)` like any other handshake rejection. (The `Nack` frame retains its in-stream role only for parse-phase rejections — see the next section.)

### Worker-side request decoding

`DispatchSession.__aenter__` is the worker's parse phase: it reads the first request frame, decodes the caller's context and rebuilds the `wool.Task` (both via `cloudpickle`), and validates that the callable is an async function or async generator.

Failures here wrap in `Rejected` and surface via a `Nack` frame whose `exception` payload carries the original failure (cloudpickle-dumped). The caller deserializes and re-raises, so the user observes the **actual failure class**, not an opaque RPC error:

- Malformed task id, cloudpickle errors on the routine callable, ImportError on a missing module, non-async callable → original `Exception` re-raised on the caller.
- Strict-mode promoted `wool.ContextDecodeWarning` (operator set `warnings.filterwarnings("error", category=wool.ContextDecodeWarning)` in the worker subprocess) → the promoted warnings aggregate into a `wool.ContextDecodeError` that ships through the same Nack-with-exception path and re-raises on the caller as `wool.ContextDecodeError`. The default lenient mode emits the warning and runs the routine against a fresh empty context (see Context propagation > Decode failure semantics).

Parse-phase rejections reflect a user-code or version-skew issue, not a worker-health issue. The load balancer does not evict the worker.

### Routine execution

The worker drives one `_step` per request frame inside `routine_scope` (the worker-loop task that runs the routine under the work context). Three terminal shapes are possible:

- **Clean completion** — the routine returns (coroutine) or raises `StopAsyncIteration` (async generator), and the response stream ends.
- **Routine exception** — the worker serializes the original exception with `cloudpickle` (using [tbpickle](https://github.com/wool-labs/tbpickle) to make stack frames picklable) and ships it on a terminal `Response.exception` frame. The caller deserializes and re-raises, **preserving the original type and traceback**. The user's `except RoutineError:` clause matches as written; the load balancer takes no action.
- **Operator pre-emption** — graceful shutdown cancels in-flight dispatches; the underlying `CancelledError` ships through the routine-exception channel. See _Cancellation_ below.

If the routine raises an exception that drags an unpicklable object into its graph (e.g., a C-level frame reachable via `__traceback__`/`__cause__`), the worker's `_safely_serialize_exception` falls back to reconstructing the exception cleanly via `cls(*exc.args)` — **preserving the exception class** so the user's `except RoutineError:` clause still matches. Only if even the clean reconstruction fails to pickle does the fallback demote to a stdlib `RuntimeError`.

### Worker-side response encoding

After each successful step, the dispatch handler builds a `protocol.Response`: it dumps the result via `cloudpickle` and attaches the post-step context.

- **Result dump fails** (un-picklable yielded value) — the failure surfaces as a handler-side exception during response encoding. The dispatch handler drains the worker, reads the final wire context published by the worker task via `session._final_wire_context`, and ships a terminal `Response.exception` carrying the encode failure. Caller observes the dump exception; no worker eviction.
- **Strict-mode context encode failure during a routine exception** — the worker task's final-encode step raised a `wool.ContextDecodeError` aggregating per-var warnings during the terminal-exception path. The handler reads the encode failure from the worker (alongside `session._final_wire_context`) and chains it onto the routine's exception as `__cause__` via `raise routine_exc from encode_err`, so the **routine exception's type is preserved**. The terminal response drops the `context` field. The caller's `except RoutineError:` clause still matches; the encode error remains visible in the traceback through cause chaining.

`DispatchSession.__aexit__` registers `drain` on its exit stack precisely because of this path: a result-dump failure mid-stream leaves the worker still running, and drain must complete before the handler reads `session._final_wire_context` for the terminal frame — otherwise the read races the worker task still publishing the final wire context.

### Caller-side response decoding

The caller's `DispatchStream` parses each response frame in turn. Malformed payloads — typically caused by version skew on a shared result class, a missing class on the caller's `sys.path`, or truncated bytes — degrade to `UnexpectedResponse` with the original pickle/import failure on `__cause__`. The load balancer treats this as caller-fault: the worker is healthy, the wire frame is valid, only the caller cannot reconstruct the payload. No eviction.

A worker that ships a non-`Exception` `BaseException` payload (other than `CancelledError`, which propagates raw to honor stdlib `await task` semantics) is degraded to `UnexpectedResponse`. This is a worker-bug guard: process-level signals like `KeyboardInterrupt` and `SystemExit` cannot be smuggled across the wire.

### Post-execution teardown

`DispatchSession.__aexit__` calls `drain` (close the request queue, await the worker driver) before unwinding the exit stack. Worker exceptions surfacing during drain are swallowed silently: pre-stream and routine-time failures already shipped via the terminal-exception clause, and the wire is closed by the time `__aexit__` runs — a re-raise here cannot reach the caller.

A worker-loop teardown driven by the service's `stop` RPC respects the timeout argument; see the worker README's _Shutdown timeout_ section.

### Backpressure hooks

A `BackpressureLike` hook runs on the dispatch handler after request decoding succeeds and before the routine is scheduled. Two failure modes:

- **Hook returned `True`** (admission rejected) — handler aborts with `RESOURCE_EXHAUSTED`. Caller observes `TransientRpcError(RESOURCE_EXHAUSTED)`; the load balancer skips to the next worker.
- **Hook raised** — handler logs the failure and aborts with `INTERNAL`. Caller observes `RpcError(INTERNAL)`; the load balancer evicts the worker. Treating a hook bug as eviction-worthy is intentional under today's binary load-balancer policy — a crashing hook on a worker is a worker-health concern from the pool's perspective. Health-aware forgiveness (e.g., N-strikes before eviction) is a future enhancement.

### Cancellation

Cancellation reaches the dispatch via three routes; all three resolve to the same observable on the caller:

- **Caller cancels its `await routine()`** — the caller's `WorkerConnection` cancels the gRPC call on the way out; the worker observes the stream tear-down, the dispatch handler invokes `DispatchSession.cancel`, the worker task is cancelled on its own loop, and any routine suspended inside an `await` receives `CancelledError`.
- **Routine self-raises `CancelledError`** — the cancellation ships through the routine-exception channel unchanged.
- **Operator preempts** (worker graceful shutdown) — `WorkerService._cancel` calls `DispatchSession.cancel` on every in-flight dispatch; same effect as caller-initiated.

In all three cases the caller's `await routine()` raises `asyncio.CancelledError`, matching stdlib's `await task` semantics where `task.cancel()` from any source produces the same observable.

## Architecture

The following diagram shows the full lifecycle of a wool worker pool — from startup and discovery through task dispatch to teardown.

```mermaid
sequenceDiagram
    participant Client
    participant Routine
    participant Pool
    participant Discovery
    participant Loadbalancer
    participant Worker

    %% -- 1. Pool startup --
    rect rgb(0, 0, 0, 0)
        Note over Client, Worker: Worker pool startup

        Client ->> Pool: create pool (spawn, discovery, loadbalancer)
        activate Client
        Pool ->> Pool: resolve mode from spawn and discovery

        opt If spawn specified, spawn ephemeral workers
            loop Per worker
                Pool ->> Worker: spawn worker
                Worker ->> Worker: start process, bind gRPC server
                Worker -->> Pool: worker metadata (host, port, tags)
                Pool ->> Discovery: publish "worker added"
            end
        end

        Pool ->> Pool: create proxy (discovery subscriber, loadbalancer)
        Pool -->> Client: pool ready
        deactivate Client
    end

    %% -- 2. Discovery --
    rect rgb(0, 0, 0, 0)
        Note over Discovery, Loadbalancer: Worker discovery

        par Worker discovery
            loop Per worker lifecycle event
                Discovery -->> Loadbalancer: worker event
                activate Discovery
                alt Worker-added
                    Loadbalancer ->> Loadbalancer: add worker
                else Worker-updated
                    Loadbalancer ->> Loadbalancer: update worker
                else Worker-dropped
                    Loadbalancer ->> Loadbalancer: remove worker
                end
                deactivate Discovery
            end
        end
    end

    %% -- 3. Task dispatch --
    rect rgb(0, 0, 0, 0)
        Note over Client, Worker: Task dispatch

        Client ->> Routine: invoke wool routine
        activate Client
        Routine ->> Routine: create task, serialize via cloudpickle
        Routine ->> Loadbalancer: route task

        loop Until handshake resolves or all workers exhausted
            Loadbalancer ->> Loadbalancer: select next worker
            Loadbalancer ->> Worker: open stream, write task frame
            Worker ->> Worker: VersionInterceptor; DispatchSession.__aenter__ (parse)
            alt Ack
                Worker -->> Loadbalancer: Ack
                Loadbalancer ->> Loadbalancer: break
            else Nack with cloudpickled parse-failure exception
                Worker -->> Loadbalancer: Nack
                Loadbalancer ->> Loadbalancer: deserialize, re-raise (no eviction)
                Loadbalancer -->> Routine: typed exception
                Routine -->> Client: re-raise
            else Transient gRPC error (UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED)
                Loadbalancer ->> Loadbalancer: skip, continue
            else Non-transient gRPC error (incl. FAILED_PRECONDITION on version mismatch)
                Loadbalancer ->> Loadbalancer: evict worker, continue
            end
        end
        opt All workers exhausted
            Loadbalancer -->> Client: raise NoWorkersAvailable
        end

        Worker ->> Worker: DispatchSession.__aiter__ schedules worker driver lazily
        Worker ->> Worker: routine_scope enters; routine runs under the work context

        alt Coroutine (single synthesized "next" request)
            Worker ->> Worker: _step advances coroutine, serializes result + context
            Worker -->> Routine: Response.result + context
            Routine ->> Routine: deserialize, apply back-propagated context
            Routine -->> Client: return result
        else Async generator (one step per client iteration command)
            loop Each iteration
                Client ->> Routine: next / send / throw
                Routine ->> Worker: iteration request frame
                Worker ->> Worker: _step advances generator
                Worker -->> Routine: Response.result + context
                Routine ->> Routine: deserialize, apply back-propagated context
                Routine -->> Client: yield result
            end
        else Routine or encoding exception
            Worker ->> Worker: drain worker, read final wire context published by worker task
            Worker -->> Routine: terminal Response.exception (cloudpickled, may include __notes__)
            Routine ->> Routine: deserialize exception (preserves type and traceback)
            Routine -->> Client: re-raise
        end
        deactivate Client
    end

    %% -- 4. Teardown --
    rect rgb(0, 0, 0, 0)
        Note over Client, Worker: Worker pool teardown

        Client ->> Pool: exit pool
        activate Client

        Pool ->> Pool: stop proxy

        opt Stop ephemeral workers
            loop Per worker
                Pool ->> Discovery: publish "worker dropped"
                Discovery -->> Loadbalancer: worker event
                Loadbalancer ->> Loadbalancer: remove worker
                Pool ->> Worker: stop worker
                Worker ->> Worker: stop service, exit process
            end
        end

        Pool ->> Discovery: close discovery
        Pool -->> Client: pool exited
        deactivate Client
    end
```

## License

This project is licensed under the Apache License Version 2.0.
