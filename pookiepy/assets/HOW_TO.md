# HOW_TO.md - Developer API Reference (LLM-Optimized)

Purpose: hand to LLM for pookiepy project generation/scaffolding. Contains supported API surface, lifecycle hooks, patterns.

## Core Concept

One bidirectional gRPC stream per client. Client declares:
- `provides`: message names it sends
- `requires`: message names it wants

Server routes by `messageName` (no extra RPC methods needed).

## Imports

```python
from pookiepy.baseserver import BaseServer, Peer, ServerConfig
from pookiepy.baseclient import BaseClient, ClientConfig
from pookiepy.tools import generate_message, struct_to_json, json_to_struct, evaluate_history
from pookiepy.exceptions import GrpcEmpty, ClientExit, GrpcConnectionError
import pookiepy.message_pb2 as message_pb2
```

## Server

Subclass `BaseServer`; override only needed hooks.

```python
class MyServer(BaseServer):
    def __init__(self, port):
        super().__init__(port=port, name="MyServer")

    def on_init(self):
        # once after __init__; safe place for state setup
        self.my_cache = {}

    def on_client_connect(self, request: message_pb2.Message, context) -> bool:
        # first message from each client, before registration
        # request.metaInfo.clientInfo: uuid, name, provides, requires
        # return False => reject (PERMISSION_DENIED abort)
        return True

    def on_client_accepted(self, peer: Peer, request: message_pb2.Message):
        # after accept + full registration; peer fields now populated
        pass

    def on_client_disconnect(self, peer: Peer):
        # when client stream fully closed (accepted clients only)
        pass

    def on_receive(self, peer: Peer, request: message_pb2.Message) -> bool:
        # subsequent client messages
        # True  => auto fan-out to clients requiring this messageName
        # False => drop/handle manually (unicast/cache/transform)
        name = request.metaInfo.messageName
        if name == "my_request":
            self._handle_request(peer, request)
            return False
        return True

    def on_data_yield(self, peer: Peer, data: message_pb2.Message):
        # just before server-side yield to client stream
        # telemetry/metrics hook; not delivery confirmation
        pass

    def _handle_request(self, peer: Peer, request: message_pb2.Message):
        response = generate_message("my_response", struct_payload={"result": 42})
        # unicast to requester
        self._data_register.add_data_for_message_name(
            peer.client_id, "my_response", response, target_client_id=peer.client_id
        )

    def on_shutdown(self):
        # shutdown cleanup
        pass
```

### Start

```python
server = MyServer(port=50051)
server.serve_forever()   # blocks until KeyboardInterrupt or shutdown()
```

### Shutdown from another thread

```python
server.shutdown()        # sets exit event; serve_forever() returns
```

### Server push to all subscribers

```python
# send to all clients requiring "my_topic"
msg = generate_message("my_topic", struct_payload={"value": 1.0})
self._data_register.add_data_for_message_name("", "my_topic", msg)
# "" clientId => no sender to skip; reaches all requiring "my_topic"
```

### `ServerConfig` (optional)

```python
config = ServerConfig(
    max_workers=10,            # thread pool size (>= expected concurrent clients)
    max_queue_elements=0,      # per-client queue depth (0 = unlimited)
    shutdown_poll_interval=0.1,
)
BaseServer(port=50051, config=config)
```

### `Peer` fields

| Field | Type | Value |
|---|---|---|
| `peer.client_id` | `str` | UUID generated per connection |
| `peer.name` | `str` | Human-readable name from client |
| `peer.session_id` | `str` | Server-side session UUID |
| `peer.peer` | `str` | Raw gRPC peer string (IP) |

## Client

Subclass `BaseClient`; set `provides` and `requires`.

```python
class MyClient(BaseClient):
    def __init__(self, port):
        super().__init__(
            name="my-client",
            port=port,
            provides=["my_request"],      # message names client sends
            requires=["my_response"],     # message names client receives
        )

    def on_init(self):
        # after each connection (initial + reconnect)
        pass

    def on_receive(self, data: message_pb2.Message) -> bool:
        # called by spin()/spin_forever() per message
        name = data.metaInfo.messageName
        payload = struct_to_json(data.payload.structPayload)  # dict
        return True

    def on_data_yield(self, data: message_pb2.Message):
        # just before client-side yield to gRPC stream
        # telemetry/metrics hook; not server ack/delivery confirmation
        pass

    def on_shutdown(self):
        # during disconnect()
        pass
```

### Connect/run

```python
client = MyClient(port=50051)   # connects immediately in __init__

# option A: hook-based (non-blocking send + blocking receive loop)
client.send_data(generate_message("my_request"))
client.spin_forever(timeout=5.0)   # calls on_receive(); stops on timeout/disconnect

# option B: manual polling
client.send_data(generate_message("my_request"))
try:
    msg = client.get_data(timeout=5.0)   # block up to 5s
except GrpcEmpty:
    pass   # timeout
except ClientExit:
    pass   # disconnected

# option C: context manager (auto-disconnect on exit; reconnects if reused)
with MyClient(port=50051) as client:
    client.send_data(generate_message("my_request"))
    client.wait_done()     # waits until message yielded to gRPC
    msg = client.get_data(timeout=5.0)
```

### `ClientConfig` (optional)

```python
config = ClientConfig(
    receive_queue_maxsize=0,        # 0 = unlimited
    connection_check_timeout=5.0,   # seconds waiting for server welcome
    ext_metadata=[],                # extra (key, value) gRPC call metadata tuples
    compression=None,               # optional grpc.Compression.Gzip / Deflate
    grpc_options=[
        ("grpc.keepalive_time_ms", 180000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.keepalive_without_calls", True),
    ],
)
BaseClient(..., config=config)
```

Auth token injection without subclassing:

```python
client = MyClient(port=50051, config=ClientConfig(
    ext_metadata=[("x-api-key", "my-secret")]
))
```

### Key client methods

| Method | Signature | Purpose |
|---|---|---|
| `send_data` | `(msg: Message, add_history=False)` | Enqueue send. `messageName` must be in `provides`; `add_history` appends first `DataPoint`. |
| `get_data` | `(timeout=None) → Message` | Poll receive queue. `None`=wait forever, `0`=non-blocking. |
| `wait_done` | `(additional_sleep=0.5)` | Block until all queued sends yielded to gRPC. |
| `spin` | `(timeout=None) → bool` | One `get_data` then `on_receive`; `False` on timeout/disconnect. |
| `spin_forever` | `(timeout=None)` | Loop `spin` until `False`. |
| `disconnect` | `()` | Stop threads, close channel. |

## Features LLM Should Know

### Compression

Server/client support optional gRPC compression; enable both sides for symmetric behavior.

```python
from grpc import Compression

server = BaseServer(
    port=50051,
    config=ServerConfig(compression=Compression.Gzip),
)

client = MyClient(
    port=50051,
    config=ClientConfig(compression=Compression.Gzip),
)
```

### History + latency tracing

Messages may carry per-hop `history`. Use `add_history=True` on send to append first hop automatically. Use `evaluate_history(data, log_callback)` later for per-hop latency.

```python
from pookiepy.tools import evaluate_history

msg = generate_message("my_topic", struct_payload={"x": 1})
client.send_data(msg, add_history=True)

# later, when reply arrives:
evaluate_history(reply, lambda point: print(point))
```

### Timers

`timedevent` provides drift-compensated periodic scheduling.

```python
from pookiepy.timer import timedevent

with timedevent(s=0.01, n=100) as te:
    for tick in te:
        ...
```

### Schema compatibility

Framework auto-attaches schema-version metadata on each stream call. Server rejects schema mismatch with `FAILED_PRECONDITION`.

### Logging

Prefer `self.logger` in `BaseServer`/`BaseClient` subclasses. Built-in logger supports console + rotating file logs and custom levels: `INTERNAL_INFO`, `INTERNAL_DEBUG`.

## Custom Interface (Runtime Proto)

Use custom `.proto` instead of bundled one without editing `pookiepy/`. Proto must keep same message/service structure: `Message`, `ClientProvides`, `ServerProvides`, `StreamStub`, `StreamServicer`.

### Compile/register at startup

```python
from pookiepy.custom_interface import compile_and_register

# compile my_proto/message.proto; register as pookiepy.message_pb2 / pookiepy.message_pb2_grpc
pb2, pb2_grpc = compile_and_register(
    proto_path="my_proto/message.proto",
    package="pookiepy",        # replaces built-in modules under this package name
    out_dir="my_proto/",       # optional; temp dir if omitted
)
```

Call before importing `BaseServer`/`BaseClient`. After registration, pookiepy internals automatically use custom modules.

### Typical layout

```text
my_project/
    my_proto/
        message.proto       # custom proto (same service structure)
    _proto_setup.py         # side-effect import: compile + register
    server.py
    client.py
```

`_proto_setup.py`:

```python
from pathlib import Path
from pookiepy.custom_interface import compile_and_register

compile_and_register(
    proto_path=Path(__file__).parent / "my_proto" / "message.proto",
    package="pookiepy",
    out_dir=Path(__file__).parent / "my_proto",
)
```

`server.py` / `client.py`:

```python
import _proto_setup  # must be first; registers custom proto before pookiepy imports
from pookiepy.baseserver import BaseServer
```

### Lower-level functions

| Function | Purpose |
|---|---|
| `compile_proto(proto_path, out_dir=None) → Path` | Run `grpc_tools.protoc`; return output dir. |
| `load_pb_modules_from_dir(dir_path, package, register=True) → (pb2, pb2_grpc)` | Load generated `message_pb2.py` + `message_pb2_grpc.py` from directory. |
| `validate_interface(pb2, pb2_grpc)` | Assert required symbols exist; raise `RuntimeError` otherwise. |
| `resolve_modules(message_module, grpc_module, module_path, package)` | Multi-mode resolver: accepts module objects, import strings, or directory path; falls back to bundled modules. |

## Messages

### Create

```python
from pookiepy.tools import generate_message

# dict payload (JSON-like)
msg = generate_message("my_topic", struct_payload={"key": "value", "num": 1})

# bytes payload
msg = generate_message("my_topic", byte_payload=b"\x00\x01\x02")

# empty payload (signal/event)
msg = generate_message("server-exit")
```

### Read received message

```python
name   = data.metaInfo.messageName
msg_id = data.metaInfo.messageId           # UUID hex string (auto-set)
resp_to = data.metaInfo.responseToId       # request id this message answers, if any

# struct payload -> dict
payload = struct_to_json(data.payload.structPayload)

# bytes payload
raw = data.payload.bytePayload             # bytes
```

### Correct request response

```python
reply = generate_message("my_response", struct_payload={"ok": True})
reply.metaInfo.responseToId = request.metaInfo.messageId
self.send_data(reply)
```

Rules:
- Keep `reply.metaInfo.messageId` as new message id.
- Copy original request id into `reply.metaInfo.responseToId`.
- Do not overwrite `messageId` with request id.
- For streaming replies, every chunk uses same `responseToId`.

### Request/Response Policy (Minimalistic by design)

`pookiepy` intentionally keeps request/response as a message convention, not a dedicated client API.

Why:
- Keeps `BaseClient` and `BaseServer` small and predictable.
- Avoids extra state machines, locks, and hidden queue-routing behavior in the framework core.
- Works for mixed traffic (request/response + fire-and-forget) using the same hook/polling loop.

Recommended approach:
- Sender stores `request_id = request.metaInfo.messageId`.
- Responder sets `reply.metaInfo.responseToId = request_id`.
- Requester matches with `reply.metaInfo.responseToId == request_id`.
- Non-matching messages continue through normal hook/polling flow.

### Match response on requester side

```python
request = generate_message("my_request", struct_payload={"text": "hello"}, add_metadata=True)
request_id = request.metaInfo.messageId
self.send_data(request)

while True:
    reply = self.get_data(timeout=5.0)
    if reply.metaInfo.responseToId != request_id:
        continue
    break
```

Rules:
- Compare `reply.metaInfo.responseToId` with original request id.
- Do not correlate via `reply.metaInfo.messageId`.

## Routing Rules

| Scenario | How |
|---|---|
| Fan-out to all subscribers | `on_receive()` returns `True` |
| Drop / manual handling | `on_receive()` returns `False` |
| Unicast to one client | `self._data_register.add_data_for_message_name(sender_id, name, msg, target_client_id=target_id)` |
| Server push (no sender) | `self._data_register.add_data_for_message_name("", name, msg)` |

Delivery only to clients whose `requires` contains `messageName`. If none require it, message is silently dropped.

## Exceptions

| Exception | When |
|---|---|
| `GrpcEmpty` | `get_data(timeout)` expired without message. |
| `ClientExit` | `get_data()` interrupted because client disconnected. |
| `GrpcConnectionError` | Connection failed, or `wait_done()` called while disconnected. |
| `GrpcTimeoutError` | RPC `DEADLINE_EXCEEDED`. |
| `GrpcValueError` | `messageName` not in `provides`, or wrong type passed to `send_data`. |

## SSL / TLS (optional)

```python
# server
creds = grpc.ssl_server_credentials([(private_key_bytes, cert_chain_bytes)])
BaseServer(port=50051, ssl_credentials=creds)

# client
creds = grpc.ssl_channel_credentials(root_certificates=ca_cert_bytes)
BaseClient(..., config=ClientConfig(ssl_credentials=creds))
```

## Reconnect Pattern

```python
client = MyClient(port=50051)

with client:
    client.send_data(...)
# client disconnected here

# reconnect by re-entering context manager
with client:
    client.send_data(...)   # fresh connection, new UUID
```

## Logging

If class inherits from `BaseServer`/`BaseClient`, prefer built-in instance logger:

```python
class MyServer(BaseServer):
    def on_receive(self, peer, request):
        self.logger.info("received %s", request.metaInfo.messageName)
        return True
```

Use `get_logger(...)` mainly in static methods/helper modules where `self` is unavailable:

```python
from pookiepy.logger import get_logger
logger = get_logger(name="MyComponent")   # returns GrpcLogger
logger.setLevel("DEBUG")                   # syncs console + file handler; keep "INFO" default
```

Defaults:
- Console level: `INFO`.
- File logs: `INTERNAL_DEBUG` (level 5), path `%TEMP%/grpcLogs/<name>_YYYYMMDD.log`.