# Threading & Scaling Considerations

## The core constraint

`BaseServer` uses a `ThreadPoolExecutor`. Each connected client occupies **one thread for its
entire connection lifetime** --- the `DataChannel` generator holds the thread until the client
disconnects. Additionally, `_handle_client_receive` spawns a second daemon thread per client.

$$\text{threads} = \text{clients} \times 2$$

The default `ServerConfig(max_workers=None)` resolves to `min(32, os.cpu_count() + 4)`.
On a typical 8-core machine: **12 workers = hard cap of 12 simultaneous clients**.
The 13th client does not get refused --- it silently stalls at `_check_connection()` and times
out after `connection_check_timeout` (default 5 s) with a `GrpcConnectionError`. No server-side
warning is emitted.

**Minimum fix:** Always set `max_workers` explicitly:

```python
ServerConfig(max_workers=200)  # set to expected peak concurrent clients
```

---

## Thread cost breakdown

Threads blocked on `queue.get()` release the GIL and consume near-zero CPU.
The real cost is **memory** (stack) and **OS scheduler slots**.

| Platform | Default stack/thread | 1000 clients (×2 threads) |
|---|---|---|
| Linux (default) | 8 MB | ~16 GB --- not viable |
| Linux (`ulimit -s 256`) | 256 KB | ~500 MB --- acceptable |
| Windows | ~1 MB | ~2 GB --- marginal |
| Windows (reduced) | ~256 KB | ~500 MB --- acceptable |

**Correction:** the previous `ulimit -s 64` (64 KB) recommendation is risky for CPython.
The C stack backs interpreter recursion, exception handling, and calls into gRPC's C core ---
64 KB can trigger stack-overflow crashes (segfault, not a catchable Python exception) under
deeper call chains, e.g. logging formatting, nested exception handling, or gRPC callbacks.
Use **256 KB** as a tested floor and validate under the app's actual worst-case call depth
before deploying a reduced stack size in production; the "1000 clients" column above has been
adjusted accordingly (128 MB → 500 MB estimate).

```bash
ulimit -s 256   # 256 KB stack; set in the launch script or systemd unit
```

---

## Scaling tiers

| Client count | Recommended approach |
|---|---|
| < 100 | Current threading model. Set `max_workers` explicitly. |
| 100–500 | Same + reduce thread stack size on Linux (test at 256 KB first). |
| 500–5000 | Migrate `BaseServer` to `grpc.aio` + `asyncio.Queue`. Keep sync hooks via `asyncio.to_thread()`. |
| 5000+ | Python as leaf client only. Use Go or C++ for the fan-out broker. |

---

## Path to `grpc.aio` (500–5000 clients)

`grpcio` ships `grpc.aio` --- same library, no new dependency. The servicer becomes an async
coroutine; concurrent streams share one event loop instead of one thread each. The exact
number of OS threads used internally depends on the async gRPC C-core executor and is not a
fixed "2–4" --- treat that as "a small, roughly constant number" rather than a guaranteed count,
and verify via profiling for the target deployment.

User-facing hook signatures stay **synchronous**. Sync hooks are dispatched via
`asyncio.to_thread()` so user subclasses require no changes:

```python
# server side --- internal change only
async def DataChannel(self, request_iterator, context):
    # Incoming side: dispatch each request to a worker thread so sync hooks don't
    # block the event loop.
    async for request in request_iterator:
        result = await asyncio.to_thread(self.on_receive, peer, request)

    # Outgoing side (not shown above, but required): the current sync implementation
    # concurrently reads request_iterator AND yields from notification_queue in the
    # same generator via a background thread. In grpc.aio this requires two
    # concurrently-running coroutines (e.g. asyncio.gather of a receive-loop task and
    # a send-loop task pulling from an asyncio.Queue) since a single async generator
    # cannot both consume request_iterator and yield outgoing messages independently.
```

Required internal changes:
- `DataRegister`: `threading.Lock` → `asyncio.Lock`, `queue.Queue` → `asyncio.Queue`
- `BaseServer.serve_forever()`: `grpc.server(...)` → `grpc.aio.server(...)`
- `BaseServer.DataChannel`: `def` → `async def`, split into a concurrent receive task and
  send task (the current single-generator structure that both reads `request_iterator` and
  yields from `notification_queue` cannot be ported as-is --- see caveat above)
- `BaseClient` is unaffected --- it runs in its own process/thread already

`BaseClient` does **not** need to change for the `grpc.aio` server migration. This migration
is a genuine rewrite of the streaming loop, not a mechanical `def`→`async def` swap --- scope
the effort accordingly.

---

## Why not `grpclib`?

`grpclib` is a third-party pure-Python asyncio gRPC implementation (no C core).
It requires the same asyncio migration as `grpc.aio` but loses the C-core performance
advantage of `grpcio`. Not recommended --- `grpc.aio` gives asyncio scalability with
the same C-core throughput.
