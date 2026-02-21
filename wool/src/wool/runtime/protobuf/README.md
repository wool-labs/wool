# Protobuf Subpackage

Wire protocol definitions and generated Python bindings for wool's
distributed task dispatch system.

## Dispatch Wire Protocol

The `Worker.dispatch` RPC uses a server-streaming pattern. The client
sends a single `Task` message and receives a stream of `Response`
messages:

```
Client                          Worker
  |                               |
  |── Task ──────────────────────>|
  |                               |
  |<──────── Response(Ack) ───────|   (or Nack on rejection)
  |<──────── Response(Result) ────|   (one or more results)
  |<──────── Response(Exception) ─|   (on failure)
  |                               |
```

### Response Sequence

1. **Ack** — The worker accepted the task and started processing.
   Carries the worker's `version` string for observability.
2. **Nack** — The worker rejected the task. The `reason` field
   describes why (e.g., major version mismatch). No further
   responses follow a Nack.
3. **Result** — A cloudpickle-serialized return value. Coroutine
   tasks yield exactly one result; async generator tasks yield one
   per iteration.
4. **Exception** — A cloudpickle-serialized exception from the
   remote execution. Terminates the stream.

## Serialization Strategy

Wool uses a hybrid serialization approach:

- **Protobuf envelope** — Structured metadata fields (`id`,
  `version`, `caller`, `timeout`, etc.) are native protobuf fields
  for efficient parsing and forward compatibility.
- **cloudpickle payloads** — The `callable`, `args`, `kwargs`, and
  `proxy` fields are serialized with cloudpickle and stored as
  `bytes` fields. This allows arbitrary Python objects to be
  transmitted without schema changes.
- **Results and exceptions** — `Result.dump` and `Exception.dump`
  are cloudpickle-serialized bytes.

## Python Binding Layout

Bindings are generated from `.proto` files in `wool/protobuf/` by
the `hatch-protobuf` build plugin (wired into `pyproject.toml`).
Regenerate with:

```bash
uv pip install -e './wool[dev]'
```

| Source | Generated | Wrapper |
|--------|-----------|---------|
| `wool/protobuf/task.proto` | `task_pb2.py`, `task_pb2.pyi` | `task.py` |
| `wool/protobuf/worker.proto` | `worker_pb2.py`, `worker_pb2.pyi`, `worker_pb2_grpc.py` | `worker.py` |

The wrapper modules (`task.py`, `worker.py`) re-export symbols from
the generated `_pb2` modules with clean names. Import from the
wrappers, not the generated files directly:

```python
from wool.runtime import protobuf as pb

task_msg = pb.task.Task(version="1.0.0", id="...")
response = pb.worker.Response(ack=pb.worker.Ack(version="1.0.0"))
```

## Schema Evolution Rules

- **Additive-only within a major version.** New fields may be
  appended with new field numbers. Existing field numbers and types
  must not change within the same major version.
- **Major version = wire compatibility boundary.** A major version
  bump permits breaking changes to the protobuf schema (field
  renumbering, type changes, removal).
- **Field 1 is always `version`.** The `Task` message reserves
  field 1 for the version string. This invariant enables
  pre-deserialization version extraction (see below).

## Version Compatibility

Wool enforces major-version compatibility at two layers:

### Discovery-time Filtering

`WorkerProxy` applies a version filter during worker discovery.
Workers whose major version differs from the client's are excluded
from the load balancer and never receive tasks.

### Dispatch-time Interception

`VersionInterceptor` is a gRPC server interceptor that extracts the
version field from raw request bytes *before* full deserialization.
This uses `TaskVersionEnvelope` — a minimal protobuf message
containing only `string version = 1` — which can parse field 1 from
any `Task` wire format, including future incompatible versions.

If the client's major version differs from the worker's, the
interceptor yields a `Nack` response without attempting full
deserialization. This prevents deserialization errors when the wire
format has changed across major versions.

If the version field is empty (e.g., from an older client that
predates version tagging), the interceptor passes the request
through without checking — preserving backwards compatibility.
