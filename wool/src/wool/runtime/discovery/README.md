# Worker discovery

Wool workers compose a decentralized peer-to-peer network - conceptually a singular pool. From a client's perspective, however, a worker pool is actually an abstraction defined by its discovery protocol. Discovery protocols describe to a client how to find workers, and which of those workers to consider when routing work.

## Publisher-subscriber pattern

Discovery separates **publishing** (announcing worker lifecycle events) from **subscribing** (reacting to them).

A `WorkerPool` initialized with a `discovery` protocol will subscribe to it and consider any discovered workers when routing work. If the pool also spawns any workers (e.g., because it was initialized with a finite `size`), those workers' lifecycle events will be published according to the specified protocol as well, making them available to the network.

Worker lifecycle event types:

| Event            | Meaning                                        |
| ---------------- | ---------------------------------------------- |
| `worker-added`   | A worker has joined the pool.                  |
| `worker-dropped` | A worker has left the pool or is unresponsive. |
| `worker-updated` | A worker's metadata has changed.               |

## Protocols

Wool ships with two discovery protocols - `LocalDiscovery` and `LanDiscovery`.

`LocalDiscovery`

Shared-memory IPC for single-machine pools. This is the default when you create an ephemeral `WorkerPool` without specifying a discovery protocol. No network, no configuration — workers and subscribers communicate through a shared memory region identified by a namespace string. File-based locking ensures consistency across processes.

`LanDiscovery`

Zeroconf DNS-SD (`_wool._tcp.local.`) for network-wide discovery. Workers are advertised as DNS-SD service records on the local network. Subscribers browse for these services and receive events as workers come and go. No central coordinator required.

Both protocols optionally accept a filter predicate for targeted subscriptions.

## Composability — bring your own discovery

In addition to the aforementioned discovery protocols, Wool supports custom protocols via structural subtyping.

`WorkerPool` accepts `DiscoveryLike | Factory[DiscoveryLike]` for its `discovery` parameter. The `Factory` type alias covers bare instances, context managers, async context managers, callables, and awaitables. The runtime unwraps automatically:

```python
Factory: TypeAlias = (
    Awaitable[T]
    | AsyncContextManager[T]
    | ContextManager[T]
    | Callable[[], T | Awaitable[T] | AsyncContextManager[T] | ContextManager[T]]
)
```

This means you can pass a discovery instance directly, wrap it in a context manager for lifecycle management, or provide a factory callable — `WorkerPool` handles all cases.

### `DiscoveryLike` protocol

Protocols must implement `DiscoveryLike`:

```python
class DiscoveryLike(Protocol):
    @property
    def publisher(self) -> DiscoveryPublisherLike: ...

    @property
    def subscriber(self) -> DiscoverySubscriberLike: ...

    def subscribe(
        self, filter: PredicateFunction | None = None
    ) -> DiscoverySubscriberLike: ...
```

`DiscoveryPublisherLike` exposes a single method:

```python
async def publish(self, type: DiscoveryEventType, metadata: WorkerMetadata) -> None: ...
```

`DiscoverySubscriberLike` is an async iterable of `DiscoveryEvent`:

```python
def __aiter__(self) -> AsyncIterator[DiscoveryEvent]: ...
```

### Custom discovery example

A sketch of a Redis-backed discovery backend:

```python
import wool
from contextlib import asynccontextmanager


class RedisDiscovery:
    """Implements DiscoveryLike via Redis pub/sub."""

    def __init__(self, redis_url: str):
        self._url = redis_url
        self._publisher = RedisPublisher(redis_url)
        self._subscriber = RedisSubscriber(redis_url)

    @property
    def publisher(self) -> wool.DiscoveryPublisherLike:
        return self._publisher

    @property
    def subscriber(self) -> wool.DiscoverySubscriberLike:
        return self._subscriber

    def subscribe(self, filter=None) -> wool.DiscoverySubscriberLike:
        return RedisSubscriber(self._url, filter=filter)


# Wrap in an async context manager factory for lifecycle management
@asynccontextmanager
async def redis_discovery():
    svc = RedisDiscovery("redis://localhost:6379")
    await svc.connect()
    try:
        yield svc
    finally:
        await svc.close()
```

## Usage examples

### Durable pool

Connect to workers that are already running. No workers are spawned by the pool itself:

```python
import wool

# Local
async with wool.WorkerPool(discovery=wool.LocalDiscovery("my-namespace")):
    result = await my_routine()

# LAN
async with wool.WorkerPool(discovery=wool.LanDiscovery()):
    result = await my_routine()
```

### Hybrid pool

Spawn local workers **and** discover existing workers through the same
protocol. Spawned workers are published and made available to other clients:

```python
import wool

# Local
async with wool.WorkerPool(size=4, discovery=wool.LocalDiscovery("my-namespace")):
    result = await my_routine()

# LAN
async with wool.WorkerPool(size=4, discovery=wool.LanDiscovery()):
    result = await my_routine()
```

## How it fits together

```text
worker starts
  → publisher emits "worker-added"
    → subscriber receives DiscoveryEvent
      → proxy updates routing table
        → wool routines dispatch to available workers
```

The reverse on shutdown:

```text
pool exits
  → publisher emits "worker-dropped"
    → subscriber receives DiscoveryEvent
      → proxy removes worker from routing
        → worker process stops
```
