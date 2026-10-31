# Threading & Scaling Considerations

## Current execution model

`BaseServer` remains synchronous. Each serviced `DataChannel` occupies an executor worker
and a receive thread. Application thread usage is approximately twice the active stream
count, plus gRPC/native and application support threads. Existing synchronous subclass
hooks need no asyncio integration.

Set `ServerConfig.max_workers` explicitly for the expected peak connection count and
headroom. The default is `min(32, (available_cpu_count or 1) + 4)`, using
`os.process_cpu_count()` when available and `os.cpu_count()` otherwise.

gRPC admission capacity equals the worker count. Excess RPCs receive
`RESOURCE_EXHAUSTED` instead of waiting indefinitely for an executor slot. `BaseClient`
propagates this as `GrpcResourceExhaustedError`, including before the welcome message.
Retry with bounded backoff or increase capacity; do not retry in a tight loop.

## Logging and routing overhead

Console logging defaults to `INFO`; rotating-file logging retains `INTERNAL_DEBUG`
for early-release diagnostics. No default diagnostic coverage has been removed.
To explicitly reduce tracing for a performance experiment, configure the logger
before constructing the server/client:

```python
import logging
from pookiepy.logger import get_logger

get_logger(name="broker", file_log_level=logging.INFO)
```

The logger name must match the server name; client logger names use `Client-<name>`.
Cached loggers retain their first configuration. Handler levels filter output, but
the logger remains at `INTERNAL_DEBUG`, so trace records are still created. Enabled
file tracing writes synchronously and can limit throughput.

`DataRegister` snapshots subscribers under a per-topic lock, then performs nonblocking
queue writes outside that lock. Broadcast cost remains linear in subscriber count.
Queue-depth checks are skipped when the configured threshold cannot be exceeded by a
bounded queue. Full-queue and queue-growth warnings share one five-second cooldown per
register. Logs sample affected subscribers; delivery results still report every dropped
recipient.

## Queue policy

Per-client notification queues remain **unbounded by default** (`max_queue_elements=0`).
Choose a finite limit explicitly to protect memory against slow subscribers. When a
bounded queue is full, the newest message for that subscriber is dropped; other
subscribers continue receiving. This is load shedding, not publisher backpressure,
reliable delivery, or a normal-path throughput optimization.

```python
from pookiepy.baseserver import BaseServer, ServerConfig

server = BaseServer(port=50051, name="broker", config=ServerConfig(
    max_workers=200,
    max_queue_elements=1024,
))
```

Manual calls to `add_data_for_message_name()` return `DeliveryResult` with delivered
and dropped recipient IDs. Automatic fan-out does not expose a new delivery hook.
Enqueue results are not client acknowledgements. Reliable workflows need application-level
acknowledgements/retries or a durable broker.

Size queues for payload size and acceptable subscriber lag. An element limit is not a
byte limit; shared references, protobuf objects, transport buffers, and native memory also
contribute to process memory. The default unbounded queues retain their memory-growth risk.

## Stream lifecycle

Idle senders retain `queue.get(timeout=1)`: roughly one timeout wakeup per idle stream
per second. This does not impose a one-second message delivery delay. Exit detection can
take up to that polling interval; no exit registry or configurable receive-thread join
has been added.

A shared registration/cleanup lock prevents acceptance hooks returning late from
recreating subscriptions after disconnection. Receive-hook failures produce gRPC
`INTERNAL`. Transport stop and executor cleanup run even when startup or the shutdown
hook fails. These are correctness improvements, not throughput gains.

Each `shutdown()` call invokes `on_shutdown()`, including the cleanup call from
`serve_forever()`, preserving the existing contract. Hooks should tolerate repeated
cleanup and use bounded I/O. Disconnect hooks can overlap receive hooks still finishing.
The server watchdog checks the global exit event every `shutdown_poll_interval` seconds.

## Measure before changing concurrency models

No significant throughput or latency gain has been established for these changes.
Admission control improves overload errors, queue bounds are optional memory protection,
and warning throttling limits log storms. Compare throughput with identical logging and
queue settings before attributing a speedup to threading or routing changes.

Blocked threads release the GIL and use little CPU. Thread-stack virtual reservations are
not equivalent to resident RAM. Measure resident/private memory, virtual memory, thread
count, context switches, queue drops, throughput, and p50/p95/p99 latency on the deployment
OS. No universally safe reduced stack size or client-count migration threshold exists.

Compare idle connections, busy fan-out, slow subscribers, blocking hooks, and CPU-heavy
hooks with identical logging, payloads, TLS, compression, and buffering. Test connection
churn and shutdown as well as steady-state throughput. Timer jitter alone does not prove
GIL contention.

Only prototype an optional `grpc.aio` backend if connection-long workers dominate measured
resource use. Async tasks can reduce thread count for mostly idle connections, but do not
remove fan-out CPU cost, serialization, or the GIL. Per-message executor dispatch can add
overhead for short synchronous hooks. Preserve the existing synchronous backend and API.

An async implementation needs concurrent receive/send tasks, bounded hook admission,
ordered per-stream dispatch, and thread-safe bridges for external publishers. A blanket
`asyncio.to_thread()` wrapper does not preserve thread affinity or synchronous context
methods such as `context.abort()`. Async gRPC contexts must stay on their owning thread.
Native async consumers should have explicit async hooks; legacy subclasses must not be
silently moved onto a different execution contract.

Neither shared C-core usage nor switching to `grpclib`, Go, or C++ establishes a throughput
gain by itself. Require matched benchmarks and a clear resource/SLO win before migration.
