# Worker discovery

Wool workers compose a decentralized peer-to-peer network. From a client's perspective, a worker pool is an abstraction defined by its discovery protocol. Discovery protocols describe to a client how to find workers, and which of those workers to consider when routing work.

## Publisher-subscriber pattern

Discovery separates **publishing** (announcing worker lifecycle events) from **subscribing** (reacting to them).

A `WorkerPool` initialized with a `discovery` protocol will subscribe to it and consider any discovered workers when routing work. If the pool also spawns any workers (e.g., because it was initialized with a finite `spawn`), those workers' lifecycle events will be published according to the specified protocol as well, making them available to the network.

Worker lifecycle event types:

| Event            | Meaning                                        |
| ---------------- | ---------------------------------------------- |
| `worker-added`   | A worker has joined the pool.                  |
| `worker-dropped` | A worker has left the pool or is unresponsive. |
| `worker-updated` | A worker's metadata has changed.               |

## Protocols

Wool ships with two discovery protocols — `LocalDiscovery` and `LanDiscovery`.

`LocalDiscovery`

File-backed IPC for single-machine pools, and the default for a `WorkerPool` created without a discovery protocol. Processes on one host share a registry of workers identified by a namespace string, with no network and no configuration.

A namespace has exactly one **owner**, the entered `LocalDiscovery` instance holding the namespace's claim, which ends when the owner exits or its process dies. The owner owns every artifact of the namespace — the registry, the notification file and every worker's metadata block — so all of them are freed when it goes. `LocalDiscovery.Publisher` and `LocalDiscovery.Subscriber` **borrow** the namespace, and a borrower's binding ends with its owner: one that outlives its owner fails loudly with `DiscoveryNamespaceNotFound` at its next read or write rather than reaching a successor or serving a stale snapshot. The `LocalDiscovery` docstring defines the three roles and the errors each one raises.

`LanDiscovery`

Zeroconf DNS-SD (`_wool._tcp.local.`) for network-wide discovery. Workers are advertised as DNS-SD service records on the local network, and subscribers browse for these services and receive events as workers come and go. A service type has no owner, so any number of pools publish and subscribe on it concurrently.

Both protocols optionally accept a filter predicate for targeted subscriptions.

## Composability — bring your own discovery

Wool supports custom discovery protocols via structural subtyping.

`WorkerPool` accepts any `DiscoveryLike`, and a durable pool also accepts a bare `DiscoverySubscriberLike`, as an instance or any `Factory` form; see `WorkerPool`'s `discovery` parameter and `resolved`.

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

`DiscoveryPublisherLike` exposes a `publish` method and a `bind_host` attribute declaring where workers advertised through the publisher should listen — see the protocol's docstring for the contract:

```python
bind_host: str


async def publish(self, type: DiscoveryEventType, metadata: WorkerMetadata) -> None: ...
```

`DiscoverySubscriberLike` is an async iterable of `DiscoveryEvent`:

```python
def __aiter__(self) -> AsyncIterator[DiscoveryEvent]: ...
```

### Implementing a custom discovery protocol

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

Connect to workers that are already running. The pool spawns no workers and publishes nothing, so it accepts a bare subscriber. With `LocalDiscovery`, this is how a process borrows a namespace another process owns:

```python
import wool

# Local: borrow the registry of whichever instance owns "my-namespace"
async with wool.WorkerPool(discovery=wool.LocalDiscovery.Subscriber("my-namespace")):
    result = await my_routine()

# LAN
async with wool.WorkerPool(discovery=wool.LanDiscovery()):
    result = await my_routine()
```

### Hybrid pool

Spawn local workers and admit workers that other processes publish through the same protocol. The pool publishes its workers, so it needs a full `DiscoveryLike`. With `LocalDiscovery`, the pool owns the namespace and other processes borrow it:

```python
import wool

# Local: owns "my-namespace" for the life of the block
async with wool.WorkerPool(spawn=4, discovery=wool.LocalDiscovery("my-namespace")):
    result = await my_routine()

# LAN
async with wool.WorkerPool(spawn=4, discovery=wool.LanDiscovery()):
    result = await my_routine()
```
