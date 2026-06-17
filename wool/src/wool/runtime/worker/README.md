# Workers

Workers are the execution layer where `@wool.routine` calls actually run. Each worker is an isolated subprocess hosting a gRPC server with its own asyncio event loop. The `WorkerPool` orchestrates their lifecycle — starting, stopping, and publishing them to discovery. Internally, the pool uses a `WorkerProxy` to route dispatched tasks across workers through a load balancer.

## Pool modes

`WorkerPool` supports four configurations depending on which arguments are provided:

| Mode | `spawn` | `discovery` | Behavior |
| ---- | ------- | ----------- | -------- |
| Default | omitted | omitted | Spawns `cpu_count` local workers with internal `LocalDiscovery`. |
| Ephemeral | set | omitted | Spawns N local workers with internal `LocalDiscovery`. |
| Durable | omitted | set | No workers spawned; connects to existing workers via discovery. |
| Hybrid | set | set | Spawns local workers and discovers remote workers through the same protocol. |

**Default** — no arguments needed:

```python
import wool

async with wool.WorkerPool():
    result = await my_routine()
```

**Ephemeral** with tags — spawn local workers:

```python
import wool

async with wool.WorkerPool("gpu-capable", spawn=4):
    result = await gpu_task()
```

**Durable** — connect to workers already running on the network:

```python
import wool

async with wool.WorkerPool(discovery=wool.LanDiscovery()):
    result = await my_routine()
```

**Hybrid** — spawn local workers and discover remote ones:

```python
import wool

async with wool.WorkerPool(spawn=4, discovery=wool.LanDiscovery()):
    result = await my_routine()
```

> `spawn` controls how many workers are spawned by the pool — it does not cap the total number of workers available. In Hybrid mode, additional workers may join via discovery beyond the initial `spawn`. The `lease` parameter caps how many additional discovered workers the pool will accept. The total pool capacity is `spawn + lease` when both are set. The lease count is a cap on admission, not a reservation — discovered workers may serve multiple pools simultaneously, and there is no guarantee that a leased slot will remain filled for the life of the pool:
>
> ```python
> import wool
>
> # Spawn 4 local workers, accept up to 4 more from discovery (8 total)
> async with wool.WorkerPool(spawn=4, lease=4, discovery=wool.LanDiscovery()):
>     result = await my_routine()
>
> # Durable pool capped at 10 discovered workers
> async with wool.WorkerPool(discovery=wool.LanDiscovery(), lease=10):
>     result = await my_routine()
> ```

## Worker lifecycle

`WorkerLike` is the protocol that defines the worker interface. Wool's built-in implementations use the `Worker` ABC, which provides a template-method pattern: the public `start()` and `stop()` methods are `@final`, enforce precondition checks, and delegate to abstract `_start()` and `_stop()` hooks. Custom implementations do not need to extend `Worker`; they only need to satisfy `WorkerLike`.

`WorkerLike` properties:

| Property | Type | Description |
| -------- | ---- | ----------- |
| `uid` | `UUID` | Unique identifier assigned at construction. |
| `metadata` | `WorkerMetadata \| None` | Full metadata including address, tags, version, and transport options. `None` before `start()`. |
| `tags` | `set[str]` | Capability tags for filtering and selection. |
| `extra` | `dict[str, Any]` | Arbitrary key-value metadata. |
| `address` | `str \| None` | gRPC target address (e.g. `"host:port"`, `"unix:path"`). `None` before `start()`. |

`LocalWorker` is the built-in implementation:

- **start**: Spawns a `WorkerProcess` subprocess. The subprocess creates a gRPC server, binds to the configured host and port (port 0 selects an available port), and sends the actual port back to the parent via a multiprocessing pipe. The parent constructs `WorkerMetadata` from the resolved address.
- **stop**: Sends a gRPC `stop` RPC to the subprocess. The subprocess sets a stopping flag — once set, new dispatches are rejected with `UNAVAILABLE` — then drains or cancels in-flight tasks according to the timeout, stops the gRPC server with a grace period, and exits.

### Durable workers

A standalone worker deployed to serve off-host, e.g., a container or VM that outlives any one pool, must bind beyond loopback and publish itself:

```python
worker = LocalWorker(host="0.0.0.0")
await worker.start()
async with LanDiscovery("my-pool").publisher as publisher:
    await publisher.publish("worker-added", worker.metadata)
    ...
```

The wildcard bind makes the publisher auto-resolve a routable advertised address (the pod or machine IP). Leaving the worker on its loopback default and publishing it over LAN discovery emits a `LoopbackAdvertisementWarning`, since off-host subscribers cannot reach it.

### Custom workers

`WorkerPool` accepts either factory protocol for its `worker` parameter. A `WorkerFactory` declares a keyword-passable `host` and receives the bind host prescribed by the pool's discovery publisher, so factory-customized workers stay reachable wherever the publisher advertises them. `LocalWorker` itself qualifies, as does a partial of it that leaves `host` unset. A `BoundWorkerFactory` declares no `host` and owns its own binding. A bound factory always wins (a partial that pre-supplies `host` is treated as bound). The pool classifies a factory by inspecting its signature for an explicitly declared `host` (see the `WorkerFactory` docstring for details):

```python
class WorkerFactory(Protocol):
    def __call__(
        self, *tags: str, credentials: WorkerCredentials | None = None,
        host: str,
    ) -> WorkerLike: ...

class BoundWorkerFactory(Protocol):
    def __call__(
        self, *tags: str, credentials: WorkerCredentials | None = None,
    ) -> WorkerLike: ...
```

Custom workers need only satisfy the `WorkerLike` protocol and host a gRPC server implementing the worker service protocol at its reported `address`.

The following example extends `LocalWorker` with automatic crash recovery. A background monitor checks `WorkerProcess.is_alive()` periodically and restarts the subprocess if it has exited unexpectedly:

```python
import asyncio
import logging

from wool.runtime.worker.local import LocalWorker
from wool.runtime.worker.process import WorkerProcess

logger = logging.getLogger(__name__)


class ResilientWorker(LocalWorker):
    """LocalWorker that automatically restarts on crash."""

    def __init__(self, *tags, check_interval: float = 5.0, **kwargs):
        super().__init__(*tags, **kwargs)
        self._check_interval = check_interval
        self._monitor_task = None

    async def _start(self, timeout=None):
        await super()._start(timeout=timeout)
        self._monitor_task = asyncio.create_task(self._monitor())

    async def _stop(self, timeout=None):
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        await super()._stop(timeout=timeout)

    async def _restart(self):
        """Replace the dead process, reusing the original port."""
        self._worker_process = WorkerProcess(
            host=self._worker_process.host,
            port=self._worker_process.port,
            server_credentials=self._server_credentials,
        )
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, lambda: self._worker_process.start(timeout=None)
        )
        logger.info("Worker %s restarted at %s", self.uid, self.address)

    async def _monitor(self):
        """Periodically check if the worker process is alive."""
        while True:
            await asyncio.sleep(self._check_interval)
            if not self._worker_process.is_alive():
                logger.warning("Worker %s crashed, restarting", self.uid)
                await self._restart()
```

Plug it into the pool with a factory:

```python
import wool
from functools import partial

async with wool.WorkerPool(
    spawn=4,
    worker=partial(ResilientWorker, check_interval=10.0),
):
    result = await my_routine()
```

## Task execution

Each worker subprocess has a two-loop architecture:

- The **gRPC event loop** runs the gRPC server (`WorkerService`). It receives dispatch RPCs, sends acknowledgments, and streams results back.
- A dedicated **worker event loop** runs on a daemon thread. Routines are scheduled here so that long-running work never blocks gRPC operations.

Per-dispatch, the gRPC handler instantiates a `DispatchSession` — an async context manager and async iterator that owns the dispatch's full worker-side lifecycle as a uniform driver for both coroutine and async-generator Wool routines. The session has four phases:

1. **Parsing** (`__aenter__`) — reads the first request frame, decodes the caller's `wool.Context` snapshot and rebuilds the `wool.Task` (both via `cloudpickle`), and validates the routine type. Failures wrap in `Rejected` and surface via a `Nack` carrying the typed exception (see _Exception flow_ below).
2. **Iteration** (`__aiter__`) — schedules the worker driver lazily on first call so the handler's pre-iteration decisions (i.e., backpressure hook) run before the worker takes the `wool.Context` guard. The worker driver enters a routine-scoped context manager for the parsed task and drives the routine iteratively, with the cross-loop bridge mediated by a pair of queues. Coroutine Wool routines synthesize a single `next` request internally; async-generator routines are driven by iteration commands issued by the client, mirroring the standard library's async-generator semantics.
3. **Teardown** (`__aexit__`) — drains the worker driver and unwinds the exit stack. Drain is registered as an exit-stack callback so resource release runs even if drain itself raises.
4. **Cancellation** (`cancel`) — sets a flag observed by both the iteration loop and the deferred scheduler, cancels the worker driver task on the worker loop so a routine suspended inside an `await` receives `CancelledError`, and pushes an end-of-stream frame onto the response queue. Cancellation is idempotent and cross-task safe (no `aclose` of the iterator, so a `cancel` call from any task — including the service's preemption path during graceful shutdown — does not race the driving task).

The dispatch handler decodes the wire context into a fresh `wool.Context` inside the session's parse phase and schedules the routine on the worker loop with that instance as the `context=` argument to `loop.create_task`. Wool's event loop task factory routes the explicit `wool.Context` through its scoped-binding path, registering the same instance against the worker task — not a copy — so mutations under the worker task are observable to the handler when it later snapshots the `wool.Context` for back-propagation.

### Context decode failures

Wire context is **ancillary state** in Wool's protocol contract: a failure to decode an incoming context — whether on the initial dispatch frame, a mid-stream frame, or a back-propagated response — never preempts the routine's primary signal (its return value or raised exception). The worker's contract on each side:

- **Initial-frame decode failure (request).** The routine still runs, with a fresh empty `wool.Context` as fallback. A `wool.ContextDecodeWarning` is emitted on the worker.
- **Mid-stream decode failure (request).** The current iteration continues without applying the upstream merge. A `wool.ContextDecodeWarning` is emitted on the worker.
- **Snapshot encode failure (response).** The back-propagated wire context is replaced with an empty context; the response still carries the routine's result or exception. A `wool.ContextDecodeWarning` is emitted on the worker. When the snapshot encode failure coincides with a routine exception, the snapshot failure additionally rides on the routine exception via `__notes__` so the caller's traceback shows both signals.

The caller side mirrors this contract: response-context decode failures emit `wool.ContextDecodeWarning` on the caller and never preempt the routine's outcome. See the top-level [`wool/README.md`](../../../../README.md#decode-failure-semantics) for the full lenient/inspect/strict modes.

Worker-side strict mode is enabled via Python's standard `PYTHONWARNINGS` environment variable (which `multiprocessing` propagates to spawned worker subprocesses by default). When the worker promotes the warning to an exception, the dispatch handler catches it before the routine starts and ships it via the routine-exception channel, so the caller observes a `wool.ContextDecodeWarning` raised — symmetric with caller-side strict mode rather than a generic gRPC error. Promotions raised after the routine starts surface through the existing routine-exception machinery.

### Exception flow

Worker-side failures route through one of three exit channels. See the top-level [_Error handling_](../../../../README.md#error-handling) section for the full caller-side picture across all dispatch phases.

| Source | Surface | Caller observes |
| ------ | ------- | --------------- |
| Parse-phase failure (`Rejected` from `__aenter__`) | `Nack` frame with cloudpickled `original` exception | Original exception re-raised, type and traceback preserved |
| Routine-time exception (raised inside `_step`) | Terminal `Response.exception` with cloudpickle-dumped exception + post-step context snapshot | Original exception re-raised, type and traceback preserved |
| Handler-level encoding failure (result dump fails, strict-mode context encode raises) | Same terminal `Response.exception` channel; either ships the encode failure directly (result dump) or attaches `wool.ContextDecodeWarning` peers to the routine exception via PEP 678 `__notes__` (context encode during routine exception) | Either the encode failure or the routine exception with notes attached |

`VersionInterceptor` aborts incoming requests with `FAILED_PRECONDITION` before the dispatch handler runs; that surfaces on the caller as a non-transient `RpcError` and is **not** routed through the `Nack` channel.

The `Nack` frame's purpose is to ship a typed parse-phase exception so the caller observes the **actual failure class** rather than an opaque RPC error. A `Nack` only appears pre-Ack; once the dispatch handler yields an `Ack`, all further terminal signals ride on `Response.exception`. The dispatch FSM is `Ack? (Result* (Exception | ε)) | Nack`.

Operator-initiated cancellation (graceful shutdown) flows through the routine-exception channel: `WorkerService._cancel` invokes `DispatchSession.cancel` on every in-flight dispatch, the worker task is cancelled on the worker loop, and `CancelledError` rides on the terminal frame. The caller's `await routine()` raises `CancelledError` — indistinguishable from caller-initiated or routine-self-cancellation, matching stdlib's `await task` semantics.

### Dispatch protocol

The `dispatch` RPC is bidirectional-streaming. Both coroutines and async generators share the same stream, but differ in how iteration proceeds:

- **Coroutines** — the server sends an ack followed by a single result (or exception). The client does not write after the initial request.
- **Async generators** — the server sends an ack, then the client drives each iteration by writing a command frame (`next`, `send`, or `throw`). The server advances the generator and responds with the yielded value (or exception) before waiting for the next command. The generator advances only on receipt of a client command.

### Shutdown timeout

The `stop` RPC accepts a `timeout` argument that controls how in-flight tasks are handled:

| `timeout` | Behavior |
| --------- | -------- |
| `0` | Cancel all in-flight tasks immediately. |
| `> 0` | Wait up to N seconds, then cancel remaining tasks. |
| `< 0` | Wait indefinitely for all tasks to complete. |

Signal handlers map `SIGTERM` to timeout 0 (cancel immediately) and `SIGINT` to timeout -1 (wait indefinitely).

### Nested routines

Worker subprocesses can dispatch tasks to other workers. Each subprocess is configured with a `ResourcePool` of `WorkerProxy` instances (via `wool.__proxy_pool__`), so `@wool.routine` calls within a task transparently route to the target pool. Spinning up a `WorkerProxy` is not free — it involves establishing a discovery subscription, starting a worker-sentinel task (a background coroutine that keeps the proxy's connection context alive), and opening gRPC connections — so the resource pool caches proxies with a configurable TTL (default 60 seconds, set via `proxy_pool_ttl` on `LocalWorker`). If the interval between dispatches for a given pool on a given worker is shorter than the TTL, the cached proxy is reused. If it exceeds the TTL, the proxy is finalized and must be recreated on the next dispatch. Tuning `proxy_pool_ttl` above the expected dispatch interval keeps proxies warm and avoids this cold-start overhead.

Proxies on worker subprocesses are lazy by default — the `WorkerPool` propagates its `lazy` flag to every `WorkerProxy` it constructs, and each task serializes the proxy (including the flag) so that workers receiving the task inherit the same laziness setting. A lazy proxy defers discovery subscription and worker-sentinel task setup until its first `dispatch()` call, so workers that never invoke nested routines pay no startup cost.

## Connections

`WorkerProxy` is the client-side bridge between routines and workers. It manages discovery, connection pooling, and load-balanced dispatch.

### Construction modes

| Mode | Parameter | Description |
| ---- | --------- | ----------- |
| Pool URI | `pool_uri` | Subscribes to `LocalDiscovery` with the URI as namespace and tag filter. |
| Discovery | `discovery` | Accepts any `DiscoverySubscriberLike` or `Factory` thereof. |
| Static | `workers` | Takes a sequence of `WorkerMetadata` directly — no discovery needed. |

### Lazy startup

`WorkerProxy` accepts a `lazy` parameter (default `True`) that controls when the proxy actually starts — i.e., when it subscribes to discovery, launches the worker sentinel task, and initializes the load balancer context.

| `lazy` | `enter()` / `__aenter__` | `dispatch()` | `exit()` on un-started proxy |
| ------ | ------------------------ | ------------- | ----------------------------- |
| `True` | Sets context var only | Calls `start()` on first call (retrying on a later call if it failed), then dispatches | No-op (safe to call) |
| `False` | Sets context var, calls `start()` | Raises `RuntimeError` if not started | Raises `RuntimeError` |

When `lazy=True`, concurrent `dispatch()` calls use a double-checked lock to ensure the proxy starts exactly once. The `lazy` flag is preserved through `cloudpickle` serialization, so proxies sent to worker subprocesses as part of a task retain their laziness setting.

### Context lifecycle

Both `WorkerPool` and `WorkerProxy` are **single-use** async context managers. Once entered and exited, the same instance cannot be entered again — create a new instance instead. Attempting to call `enter()` or `__aenter__()` a second time raises `RuntimeError`. This prevents silent state corruption from reentrant or repeated context usage (e.g., accidentally nesting `async with proxy:` blocks or calling `enter()` in a retry loop).

```python
# Correct — one instance per context
async with wool.WorkerPool(spawn=4):
    await my_routine()

# Need another pool? Create a new instance.
async with wool.WorkerPool(spawn=4):
    await my_routine()
```

### Self-describing connections

Workers are self-describing: each worker advertises its gRPC transport configuration via `ChannelOptions` in its `WorkerMetadata`. When a client discovers a worker, it reads the advertised options and configures its channel to match — message sizes, keepalive intervals, concurrency limits, and compression are all set automatically. There is no separate client-side configuration step; the worker's metadata is the single source of truth for how to connect to it.

### Connection pooling

`WorkerConnection` is a lightweight facade that dispatches tasks over pooled gRPC channels. Channels are cached at the module level in a `ResourcePool` keyed by `(target, credential fingerprint, options)`, with a 60-second TTL — idle channels are finalized after the TTL expires. Keying on a content fingerprint rather than a credentials object is what lets rotated material yield a fresh channel while unchanged material reuses the pooled one (see _Security_). Each channel's concurrency semaphore is sized by the worker's advertised `max_concurrent_streams`.

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

### Error classification

| Error | gRPC codes | Load balancer behavior |
| ----- | ---------- | ---------------------- |
| `TransientRpcError` | `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED` | Retry on next worker. |
| `HandshakeError` | `UNAUTHENTICATED`, or `UNAVAILABLE` with TLS evidence | Evict worker, record rejection. |
| `RpcError` | All others | Remove worker from context, retry next. |

`HandshakeError` is a non-transient `RpcError` subclass raised when the TLS/mTLS handshake or peer authentication with a worker fails (wrong CA, identity mismatch, expired/rejected cert, plaintext-vs-encrypted). It carries a typed `reason` (`CERT_VERIFY`, `IDENTITY_MISMATCH`, `PEER_UNAUTHENTICATED`, `PLAINTEXT_VS_ENCRYPTED`, `TLS_HANDSHAKE`). Because a handshake failure is recoverable — the worker may adopt rotated credentials out of band — the load balancer **skips the worker without eviction** (like a transient error) and emits a per-rejection warning log, leaving it in the pool to retry on a later dispatch. When a full dispatch cycle drains with at least one handshake rejection and no non-handshake failure, the load balancer raises `AllWorkersUnauthenticated` (a subclass of `NoWorkersAvailable`, so existing `except NoWorkersAvailable` handlers keep working) carrying the per-worker errors — so a fleet-wide credential misconfiguration is distinguishable from an empty pool. Aggregation over time is left to log/metrics tooling rather than retained in memory.

### Security filter

Proxies enforce security compatibility during discovery. A proxy configured with credentials only connects to workers with `secure=True` metadata; a proxy without credentials only connects to `secure=False` workers. This prevents mixed security configurations within a pool.

## Security

`WorkerCredentials` is a frozen dataclass that loads PEM certificates and produces gRPC credentials for both sides of a connection.

| Mode | `mutual` | Server verified | Client verified |
| ---- | -------- | --------------- | --------------- |
| mTLS (default) | `True` | Yes | Yes |
| One-way TLS | `False` | Yes | No |

```python
import wool

creds = wool.WorkerCredentials.from_files(
    ca_path="certs/ca-cert.pem",
    key_path="certs/worker-key.pem",
    cert_path="certs/worker-cert.pem",
    mutual=True,
)

async with wool.WorkerPool(spawn=4, credentials=creds):
    result = await my_routine()
```

Credential flow: `server_credentials` are passed to spawned workers for their gRPC servers; `client_credentials` are passed to the proxy for outgoing connections and to workers for the stop RPC.

### Credential providers: identity and rotation

`WorkerCredentials` describes a fixed snapshot of material verified against the dialed address. For dynamic-address platforms (Kubernetes, ECS/Fargate) where a worker's address is assigned at startup and credentials are rotated out of band, supply a **credential provider** instead — anywhere `credentials=` is accepted (`WorkerPool`, `LocalWorker`, `WorkerProxy`). A bare `WorkerCredentials` is wrapped in a static provider automatically, so existing deployments are unaffected.

`WorkerCredentials.provider_from_files(...)` builds one:

```python
import wool

# Verify discovered workers against a stable logical identity (the cert's
# SAN), not the dynamically assigned address; re-read the PEM files when
# they rotate so a long-running fleet adopts new material without restart.
provider = wool.WorkerCredentials.provider_from_files(
    ca_path="certs/ca-cert.pem",
    key_path="certs/worker-key.pem",
    cert_path="certs/worker-cert.pem",
    identity="wool-worker.svc",
    reload=True,
)

async with wool.WorkerPool(spawn=4, credentials=provider):
    result = await my_routine()
```

- **Identity-based verification.** When `identity` is set, the client verifies the worker's server certificate against that logical identity (via gRPC's `ssl_target_name_override`) rather than the address it dialed. Full chain and SAN verification are preserved — only the *name* checked against the certificate changes — so a worker presenting a cert that does not match the identity, or is not signed by the trusted CA, is still rejected. A single configured identity applies to every worker in the pool, regardless of address.

- **Rotation without restart.** A reloading provider re-reads its PEM files when they change on disk. The client channel pool is keyed by a content fingerprint, so unchanged material reuses pooled channels while rotated material yields fresh ones on the next connection; in-flight dispatches finish on their existing channel. The worker server adopts rotated material per new connection via `grpc.dynamic_ssl_server_credentials`. Rotation replaces the cert/key/CA bytes — not the mutual-TLS mode, which is fixed at startup.

Providers are picklable: one supplied to a worker crosses into the worker subprocess, while the proxy re-resolves its provider from the ambient credential context (it is never serialized across the dispatch boundary).

### Local self-dispatch socket

For nested routines that dispatch back to the worker's own address, the worker exposes an additional **insecure** Unix-domain-socket port and routes self-dispatch over it (a worker never does TLS against itself). That socket serves the full dispatch service with no transport authentication, so its reachability is confined to the worker's own user: it is bound inside a per-worker `0700` directory under a short base directory (`$XDG_RUNTIME_DIR` where set, else `/tmp` — short to stay within the platform's `AF_UNIX` path limit). This assumes the local host is a trust boundary — any process running as the same uid can dispatch to the worker over the socket. On a shared or multi-tenant host, isolate workers by uid (or container/network namespace) accordingly.

### Discovery-plane trust

Identity-based mTLS secures the **dispatch** plane; it does not authenticate the **discovery** plane. A worker self-advertises its `WorkerMetadata` — address, `secure` flag, tags, and channel options — over whatever discovery mechanism is in use (LAN multicast, shared-memory, a custom `Discovery`), none of which is authenticated, so the advertisement is forgeable by anything that can write to that plane. The proxy-side security filter that drops workers whose advertised `secure` flag disagrees with the client's credential posture is therefore a **compatibility gate, not a trust boundary**: it prevents a plaintext/encrypted mismatch, not a malicious advertisement. Actual confidentiality and integrity rest entirely on the mTLS handshake performed when a connection is made — a forged advertisement still cannot complete the handshake without a CA-trusted certificate (and, when an identity is configured, one whose SAN matches). Authenticating the discovery plane itself is future work; until then, treat discovery as an untrusted hint and the handshake as the trust boundary.
