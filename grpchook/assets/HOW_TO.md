# HOW_TO.md --- Developer API Reference

## Core Concept

All messages flow through one bidirectional gRPC stream per client. Clients declare `provides` (names of messages they send) and `requires` (names they want to receive). The server routes based on `messageName`; no separate RPCs needed.

---

## Imports

```python
from grpchook.baseserver import BaseServer, Peer, ServerConfig
from grpchook.baseclient import BaseClient, ClientConfig
from grpchook.tools import generate_message, struct_to_json, json_to_struct, evaluate_history
from grpchook.exceptions import GrpcEmpty, ClientExit, GrpcConnectionError
import grpchook.message_pb2 as message_pb2
```

---

## Creating a Server

Subclass `BaseServer`. Override only the hooks you need.

```python
class MyServer(BaseServer):
    def __init__(self, port):
        super().__init__(port=port, name="MyServer")

    def on_init(self):
        # called once after __init__; safe to set up state here
        self.my_cache = {}

    def on_client_connect(self, request: message_pb2.Message, context) -> bool:
        # called on first message from each client, before the client is registered
        # request.metaInfo.clientInfo has: uuid, name, provides, requires
        # return False to reject (triggers PERMISSION_DENIED abort)
        return True

    def on_client_accepted(self, peer: Peer, request: message_pb2.Message):
        # called after a client has been accepted and fully registered
        # peer fields are populated at this point
        pass

    def on_client_disconnect(self, peer: Peer):
        # called when a client stream is fully closed (after accepted clients only)
        pass

    def on_receive(self, peer: Peer, request: message_pb2.Message) -> bool:
        # called for every subsequent message from a client
        # return True  → auto fan-out to all clients that require this messageName
        # return False → drop; handle manually (e.g. unicast, cache, transform)
        name = request.metaInfo.messageName
        if name == "my_request":
            self._handle_request(peer, request)
            return False  # we handle routing ourselves
        return True       # let the framework fan-out

    def _handle_request(self, peer: Peer, request: message_pb2.Message):
        response = generate_message("my_response", struct_payload={"result": 42})
        # unicast back to requester only
        self._data_register.add_data_for_message_name(
            peer.client_id, "my_response", response, target_client_id=peer.client_id
        )

    def on_shutdown(self):
        # called during shutdown; clean up resources
        pass
```

### Start the server

```python
server = MyServer(port=50051)
server.serve_forever()   # blocks until KeyboardInterrupt or shutdown()
```

### Shutdown from another thread

```python
server.shutdown()        # sets exit event; serve_forever() returns
```

### Server-initiated push to all subscribers

```python
# push to all clients that require "my_topic"
msg = generate_message("my_topic", struct_payload={"value": 1.0})
self._data_register.add_data_for_message_name("", "my_topic", msg)
# "" as clientId = no sender to skip → delivered to everyone who requires "my_topic"
```

### ServerConfig (optional)

```python
config = ServerConfig(
    max_workers=10,            # thread pool size (>= expected concurrent clients)
    max_queue_elements=0,      # per-client queue depth (0 = unlimited)
    shutdown_poll_interval=0.1,
)
BaseServer(port=50051, config=config)
```

### Peer object fields

| Field | Type | Value |
|---|---|---|
| `peer.client_id` | `str` | UUID generated per connection |
| `peer.name` | `str` | Human-readable name sent by client |
| `peer.session_id` | `str` | Server-side session UUID |
| `peer.peer` | `str` | Raw gRPC peer string (IP) |

---

## Creating a Client

Subclass `BaseClient`. Set `provides` and `requires`.

```python
class MyClient(BaseClient):
    def __init__(self, port):
        super().__init__(
            name="my-client",
            port=port,
            provides=["my_request"],      # message names this client will send
            requires=["my_response"],     # message names this client wants to receive
        )

    def on_init(self):
        # called after each connection (initial + reconnect)
        pass

    def on_receive(self, data: message_pb2.Message) -> bool:
        # called by spin() / spin_forever() for each received message
        name = data.metaInfo.messageName
        payload = struct_to_json(data.payload.structPayload)  # dict
        return True

    def on_shutdown(self):
        # called during disconnect()
        pass
```

### Connect and run

```python
client = MyClient(port=50051)   # connects immediately in __init__

# option A: hook-based (non-blocking send + blocking receive loop)
client.send_data(generate_message("my_request"))
client.spin_forever(timeout=5.0)   # calls on_receive() per message; returns on timeout or disconnect

# option B: manual polling
client.send_data(generate_message("my_request"))
try:
    msg = client.get_data(timeout=5.0)   # blocks up to 5s
except GrpcEmpty:
    pass   # timeout
except ClientExit:
    pass   # disconnected

# option C: context manager (auto-disconnect on exit; reconnects if reused)
with MyClient(port=50051) as client:
    client.send_data(generate_message("my_request"))
    client.wait_done()     # wait until message is yielded to gRPC
    msg = client.get_data(timeout=5.0)
```

### ClientConfig (optional)

```python
config = ClientConfig(
    receive_queue_maxsize=0,        # 0 = unlimited
    connection_check_timeout=5.0,   # seconds to wait for server welcome
    ext_metadata=[],               # extra (key, value) gRPC call metadata tuples
)
BaseClient(..., config=config)
```

Example --- injecting an auth token without subclassing:

```python
client = MyClient(port=50051, config=ClientConfig(
    ext_metadata=[("x-api-key", "my-secret")]
))
```

### Client key methods

| Method | Signature | Purpose |
|---|---|---|
| `send_data` | `(msg: Message, add_history=False)` | Enqueue message for sending. `messageName` must be in `provides`. `add_history` appends first `DataPoint`. |
| `get_data` | `(timeout=None) → Message` | Poll receive queue. `None`=wait forever, `0`=non-blocking. |
| `wait_done` | `(additional_sleep=0.5)` | Block until all enqueued sends have been yielded to gRPC. |
| `spin` | `(timeout=None) → bool` | One `get_data` → `on_receive`. Returns `False` on timeout/disconnect. |
| `spin_forever` | `(timeout=None)` | Loop `spin` until `False`. |
| `disconnect` | `()` | Stop all threads, close channel. |

---

## Custom Interface (Runtime Proto)

Use a custom `.proto` instead of the bundled one --- without modifying `grpchook/`.
The proto must define the same message/service structure (`Message`, `ClientProvides`, `ServerProvides`, `StreamStub`, `StreamServicer`).

### Compile and register at startup

```python
from grpchook.custom_interface import compile_and_register

# Compiles my_proto/message.proto, registers as grpchook.message_pb2 / grpchook.message_pb2_grpc
pb2, pb2_grpc = compile_and_register(
    proto_path="my_proto/message.proto",
    package="grpchook",        # replaces the built-in modules under this package name
    out_dir="my_proto/",       # optional; temp dir used if omitted
)
```

Call this **before** importing `BaseServer` / `BaseClient`. Once registered, all grpchook internals pick up the custom modules automatically.

### Typical project layout

```
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
from grpchook.custom_interface import compile_and_register

compile_and_register(
    proto_path=Path(__file__).parent / "my_proto" / "message.proto",
    package="grpchook",
    out_dir=Path(__file__).parent / "my_proto",
)
```

`server.py` / `client.py`:

```python
import _proto_setup  # must be first --- registers custom proto before grpchook imports
from grpchook.baseserver import BaseServer
```

### Lower-level functions

| Function | Purpose |
|---|---|
| `compile_proto(proto_path, out_dir=None) → Path` | Run `grpc_tools.protoc`; return output dir |
| `load_pb_modules_from_dir(dir_path, package, register=True) → (pb2, pb2_grpc)` | Load generated `message_pb2.py` + `message_pb2_grpc.py` from dir |
| `validate_interface(pb2, pb2_grpc)` | Assert required symbols present; raise `RuntimeError` if not |
| `resolve_modules(message_module, grpc_module, module_path, package)` | Multi-mode resolver: accepts module objects, import strings, or dir path; falls back to bundled |

---

## Messages

### Create a message

```python
from grpchook.tools import generate_message

# with dict payload (JSON-like)
msg = generate_message("my_topic", struct_payload={"key": "value", "num": 1})

# with bytes payload
msg = generate_message("my_topic", byte_payload=b"\x00\x01\x02")

# empty (e.g. signal/event)
msg = generate_message("server-exit")
```

### Read a received message

```python
name   = data.metaInfo.messageName
msg_id = data.metaInfo.messageId           # UUID hex string (set automatically)

# struct payload → dict
payload = struct_to_json(data.payload.structPayload)

# bytes payload
raw = data.payload.bytePayload             # bytes
```

---

## Routing Rules

| Scenario | How |
|---|---|
| Fan-out to all subscribers | `on_receive()` returns `True` |
| Drop / handle manually | `on_receive()` returns `False` |
| Unicast to one client | `self._data_register.add_data_for_message_name(sender_id, name, msg, target_client_id=target_id)` |
| Server-push (no sender) | `self._data_register.add_data_for_message_name("", name, msg)` |

Data is only delivered to clients that have `messageName` in their `requires` list. If no client requires the name, the message is silently dropped.

---

## Exceptions

| Exception | When |
|---|---|
| `GrpcEmpty` | `get_data(timeout)` expired with no message |
| `ClientExit` | `get_data()` interrupted because client disconnected |
| `GrpcConnectionError` | Connection failed or `wait_done()` called while disconnected |
| `GrpcTimeoutError` | RPC `DEADLINE_EXCEEDED` |
| `GrpcValueError` | `messageName` not in `provides`, or wrong type passed to `send_data` |

---

## SSL / TLS (optional)

```python
# server
creds = grpc.ssl_server_credentials([(private_key_bytes, cert_chain_bytes)])
BaseServer(port=50051, ssl_credentials=creds)

# client
creds = grpc.ssl_channel_credentials(root_certificates=ca_cert_bytes)
BaseClient(..., config=ClientConfig(ssl_credentials=creds))
```

---

## Reconnect Pattern

```python
client = MyClient(port=50051)

with client:
    client.send_data(...)
# client is disconnected here

# reconnect by re-entering context manager
with client:
    client.send_data(...)   # fresh connection, new UUID
```

---

## Logging

If your class inherits from `BaseServer` or `BaseClient`, prefer the built-in instance logger:

```python
class MyServer(BaseServer):
    def on_receive(self, peer, request):
        self.logger.info("received %s", request.metaInfo.messageName)
        return True
```

Use `get_logger(...)` mainly in static methods or helper modules where `self` is not available:

```python
from grpchook.logger import get_logger
logger = get_logger(name="MyComponent")   # returns GrpcLogger
logger.setLevel("DEBUG")   # syncs console + file handler; use "INFO" by default
```

Default console level: `INFO`. File logs by default at `INTERNAL_DEBUG` (level 5) written to `%TEMP%/grpcLogs/<name>_YYYYMMDD.log`.