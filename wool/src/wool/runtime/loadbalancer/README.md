# Load balancing

The load balancer is the selection layer between Wool routines and workers. When a routine is called, the load balancer decides **which worker** handles the task. Actual dispatch, retry, and worker eviction are owned by `WorkerProxy` — the load balancer's only responsibility is routing policy.

## Context-based selection

Every `delegate` call receives a `LoadBalancerContextView` — an immutable view of the workers and their connections for a specific pool. Each `WorkerPool` maintains its own underlying context (via its `WorkerProxy`), so a single load balancer instance can be reused across multiple pools while keeping selection scoped to the correct one. Routines are always routed to the workers belonging to the pool they were called through, regardless of how many share the same load balancer. Discovery events populate the context automatically; the load balancer never needs to know how workers are found, only that they exist.

The read-only `LoadBalancerContextView` exposes just a `workers` property. Load balancers observe pool membership but cannot mutate it — eviction on failure is the `WorkerProxy`'s job. This split is enforced at the protocol level.

## Built-in load balancer

Wool ships with `RoundRobinLoadBalancer`, the default when no load balancer is explicitly specified.

`RoundRobinLoadBalancer` cycles through workers, advancing the index on each yielded candidate. On the failure retry path (reported by the proxy via `athrow`), it yields the next worker. After one full cycle without success, the generator terminates and the proxy raises `NoWorkersAvailable`. The balancer contains no dispatch code, no error classification, and no eviction logic — the proxy handles all three.

## Composability — bring your own load balancer

Wool supports custom load balancers via structural subtyping.

`WorkerPool` and `WorkerProxy` accept any `DelegatingLoadBalancerLike` (or `Factory[DelegatingLoadBalancerLike]`). The `Factory` type alias covers bare instances, context managers, async context managers, callables, and awaitables. You can pass a load balancer instance directly, wrap it in a context manager for lifecycle management, or provide a factory callable — `WorkerPool` manages it appropriately.

### `DelegatingLoadBalancerLike` protocol

Custom load balancers implement a single method:

```python
class DelegatingLoadBalancerLike(Protocol):
    def delegate(
        self,
        *,
        context: LoadBalancerContextView,
    ) -> AsyncGenerator[
        tuple[WorkerMetadata, WorkerConnection],
        WorkerMetadata | None,
    ]: ...
```

### Contract

The proxy drives the `delegate` generator through three signals:

- `anext()` — request the next worker candidate.
- `athrow(exc)` — report that the previous candidate's dispatch failed. On transient errors (`TransientRpcError`) the proxy leaves the worker in the pool; on non-transient errors the proxy evicts the worker from the context **before** throwing into the generator, so the balancer observes the capacity change before choosing the next candidate.
- `asend(metadata)` — report that the previous candidate's dispatch succeeded, enabling per-dispatch bookkeeping (sticky routing, in-flight counts, affinity).

**`asend` is terminal.** The generator MUST `return` (or let control fall off the end) after recording a success. Yielding another candidate after `asend` is a protocol violation — the proxy raises `RuntimeError` at the call site with the offending value in the message.

When the generator ends without producing a successful candidate (e.g., all candidates fail), the proxy raises `NoWorkersAvailable`.

### Implementing a custom load balancer

A sketch of a least-loaded balancer that tracks in-flight counts across dispatches:

```python
import wool


class LeastLoadedBalancer:
    """Yields workers in ascending order of in-flight task count."""

    def __init__(self):
        self._in_flight: dict[str, int] = {}

    async def delegate(
        self,
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
                sent = yield metadata, connection
            except BaseException:
                # Dispatch failed. Decrement and try the next candidate.
                self._in_flight[metadata.uid] -= 1
                continue

            if sent is not None:
                # Success recorded via asend(metadata). Contract: the
                # generator must terminate after a success signal.
                # The in-flight count stays incremented for the life
                # of this dispatch; decrement happens out-of-band when
                # the stream drains (not shown — requires a lifecycle
                # hook beyond this protocol's scope).
                return
```

Compared to implementing the deprecated `DispatchingLoadBalancerLike`, the delegate form has no stream-forwarding code, no error classification, and no eviction calls — the proxy does all of that.

## Usage examples

### Default load balancer

When no `loadbalancer` argument is provided, `WorkerPool` uses `RoundRobinLoadBalancer`:

```python
import wool

async with wool.WorkerPool(spawn=4):
    result = await my_routine()
```

### Custom load balancer

Pass any `DelegatingLoadBalancerLike` instance (or factory) to `WorkerPool`:

```python
import wool

balancer = LeastLoadedBalancer()

async with wool.WorkerPool(spawn=4, loadbalancer=balancer):
    result = await my_routine()
```

## Deprecated: `DispatchingLoadBalancerLike`

The previous `LoadBalancerLike` protocol — which required implementations to own the full dispatch-retry-evict loop — has been renamed to `DispatchingLoadBalancerLike` and is deprecated. Passing a `DispatchingLoadBalancerLike` to `WorkerProxy` or `WorkerPool` still works during the transition but emits a `DeprecationWarning` at startup. Support for the dispatch-based protocol will be removed in the next major release. Migrate custom implementations to `DelegatingLoadBalancerLike` by extracting the selection logic and dropping the retry/eviction boilerplate — the proxy now owns it.

During the transition, the `LoadBalancerLike` alias resolves to a union of both protocols so existing code keeps type-checking. In the next major release, `LoadBalancerLike` will narrow to `DelegatingLoadBalancerLike` alone.
