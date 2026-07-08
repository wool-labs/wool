# Load balancing

The load balancer is the selection layer between Wool routines and workers. When a routine is called, the load balancer decides **which worker** handles the task. Actual dispatch, retry, and worker eviction are owned by `WorkerProxy` — the load balancer's only responsibility is routing policy.

## Context-based selection

Every `delegate` call receives a `LoadBalancerContextView` — a read-only view of the workers and their connections for a specific pool. Each `WorkerPool` maintains its own underlying context (via its `WorkerProxy`), so a single load balancer instance can be reused across multiple pools while keeping selection scoped to the correct one. Routines are always routed to the workers belonging to the pool they were called through, regardless of how many share the same load balancer. Discovery events populate the context automatically; the load balancer never needs to know how workers are found, only that they exist.

The `LoadBalancerContextView` exposes just a `workers` property — a `MappingProxyType` over the pool's current membership. Delegating balancers are typed against this read-only view, so selection is written against pool membership alone; eviction on failure is the `WorkerProxy`'s responsibility.

## Built-in load balancer

Wool ships with `RoundRobinLoadBalancer`, the default when no load balancer is explicitly specified.

`RoundRobinLoadBalancer` cycles through workers, advancing the index on each yielded candidate. On the failure retry path (reported by the proxy via `athrow`), it yields the next worker. After one full cycle without success, the generator terminates and the proxy raises `NoWorkersAvailable`. The balancer contains no dispatch code, no error classification, and no eviction logic — the proxy handles all three.

## Composability — bring your own load balancer

Wool supports custom load balancers via structural subtyping.

`WorkerPool` and `WorkerProxy` accept any `LoadBalancerLike` (or `Factory[LoadBalancerLike]`). The `Factory` type alias covers bare instances, context managers, async context managers, callables, and awaitables. You can pass a load balancer instance directly, wrap it in a context manager for lifecycle management, or provide a factory callable — `WorkerPool` manages it appropriately.

### `LoadBalancerLike` protocol

Custom load balancers implement a single method:

```python
class LoadBalancerLike(Protocol):
    def delegate(
        self,
        task: Task,
        *,
        context: LoadBalancerContextView,
    ) -> AsyncGenerator[
        tuple[WorkerMetadata, WorkerConnection],
        WorkerMetadata | None,
    ]: ...
```

`task` is the `Task` being routed, passed read-only as the first positional argument. A balancer MAY key its routing on it — hashing `task.tag` for sticky routing, or dispatching by `task.callable` for task-type specialization — or ignore it entirely, as `RoundRobinLoadBalancer` does.

### Contract

The proxy drives the `delegate` generator: the balancer selects and orders worker candidates, and the proxy dispatches to each, retries on failure, and evicts unhealthy workers. Selection lives in the balancer; dispatch, retry, and eviction live in the proxy. The proxy reports each candidate's outcome back through the generator's `anext`, `athrow`, and `asend` signals — the `LoadBalancerLike` docstring documents them signal by signal and is the single source of truth for the handshake.

### Implementing a custom load balancer

A task-aware balancer that routes by a stable hash of the task's tag, so tasks sharing a tag stick to the same worker (consistent-hashing / affinity routing):

```python
import wool


class StickyTagBalancer:
    """Route each task to a worker chosen by hashing its tag."""

    async def delegate(
        self,
        task: wool.Task,
        *,
        context: wool.LoadBalancerContextView,
    ):
        workers = list(context.workers.items())
        if not workers:
            return

        # Consistent-hashing key: prefer the caller-supplied tag so
        # tasks sharing a tag route together, falling back to the task
        # id so untagged tasks still route deterministically.
        key = task.tag or str(task.id)
        start = hash(key) % len(workers)

        # Probe the hashed worker first, then walk the ring so a single
        # unhealthy worker does not strand the task.
        for offset in range(len(workers)):
            metadata, connection = workers[(start + offset) % len(workers)]
            try:
                yield metadata, connection
            except Exception:
                # Dispatch failed (reported via athrow). Advance to the
                # next worker on the ring. Catch Exception (not
                # BaseException) so GeneratorExit and CancelledError
                # propagate correctly.
                continue
            # Control returns here only when the proxy signals success
            # via asend; the contract requires the generator to
            # terminate after a success.
            return
```

A worker-keyed balancer ignores `task` and ranks by worker state instead — here, by in-flight task count, yielding the least-loaded worker first:

```python
import wool


class LeastLoadedBalancer:
    """Yield workers in ascending order of in-flight task count."""

    def __init__(self):
        self._in_flight: dict[str, int] = {}

    async def delegate(
        self,
        task: wool.Task,
        *,
        context: wool.LoadBalancerContextView,
    ):
        # Rank workers by current in-flight count (ascending).
        ranked = sorted(
            context.workers.items(),
            key=lambda item: self._in_flight.get(item[0].uid, 0),
        )

        for metadata, connection in ranked:
            self._in_flight[metadata.uid] = (
                self._in_flight.get(metadata.uid, 0) + 1
            )
            try:
                yield metadata, connection
            except Exception:
                # Dispatch failed. Decrement and try the next candidate.
                # Catch Exception (not BaseException) so GeneratorExit
                # and CancelledError propagate correctly.
                self._in_flight[metadata.uid] -= 1
                continue
            # Success signaled via asend; the contract requires the
            # generator to terminate here. The in-flight count stays
            # incremented for the life of this dispatch; the matching
            # decrement happens out-of-band when the stream drains (not
            # shown — requires a lifecycle hook beyond this protocol).
            return
```

## Usage examples

### Default load balancer

When no `loadbalancer` argument is provided, `WorkerPool` uses `RoundRobinLoadBalancer`:

```python
import wool

async with wool.WorkerPool(spawn=4):
    result = await my_routine()
```

### Custom load balancer

Pass any `LoadBalancerLike` instance (or factory) to `WorkerPool`:

```python
import wool

balancer = LeastLoadedBalancer()

async with wool.WorkerPool(spawn=4, loadbalancer=balancer):
    result = await my_routine()
```

## Deprecated: `DispatchingLoadBalancerLike`

`DispatchingLoadBalancerLike` is the deprecated dispatch-owning protocol; `LoadBalancerLike` is its replacement. A `DispatchingLoadBalancerLike` implementation owns the full dispatch-retry-evict loop itself, whereas a `LoadBalancerLike` implementation only selects candidates and lets the proxy own dispatch, retry, and eviction. Passing a `DispatchingLoadBalancerLike` to `WorkerProxy` or `WorkerPool` still works during the transition but emits a `DeprecationWarning`; support for the dispatch-based protocol will be removed in the next major release. Migrate custom implementations to `LoadBalancerLike` by extracting the selection logic and dropping the retry and eviction boilerplate — the proxy now owns it.
