Project-specific performance evaluation for grpchook.

---

## 1) Asyncio default event loop (Proactor on Windows)

Status in grpchook: Not applicable today.

Reason:
- grpchook server uses sync grpc.server + ThreadPoolExecutor, not grpc.aio.
- Event-loop choice does not control current DataChannel runtime path.

Effort to adopt:
- High.
- Requires grpc.aio migration of server stream loop and queue/lock internals.

---

## 2) Zero-copy protobuf techniques

Status in grpchook: Partly valid in principle, not true end-to-end in current API.

Reason:
- Current stub path serializes/deserializes protobuf Message objects each hop.
- bytePayload helps avoid Struct overhead, but protobuf framing/parse still happens.

Effort to improve:
- Medium for partial gains (favor bytePayload, reduce object churn, benchmark).
- High for true zero-copy end-to-end (transport/serializer redesign).

---

## 3) Large streaming chunks (512 KB - 2 MB)

Status in grpchook: Correct direction, workload-dependent target.

Reason:
- Larger application messages usually improve throughput by amortizing per-message overhead.
- Exact sweet spot depends on payload type, latency, CPU, compression, and concurrency.

Effort to adopt:
- Low: expose payload-size guidance and benchmark profiles.
- Medium: add built-in chunk/reassembly helpers and safer defaults for large messages.
