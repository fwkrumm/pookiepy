# AGENTS.md

Python gRPC bidirectional-streaming framework. Subclass `BaseServer`/`BaseClient`, override hooks. Base handles all gRPC plumbing.

## Layout

```
grpchook/baseserver.py       # server base (StreamServicer)
grpchook/baseclient.py       # client base
grpchook/data_register.py         # server-side msg routing: messageName→clientId→queue
grpchook/exceptions.py            # exception hierarchy
grpchook/logger.py                # GrpcLogger + rotating file logger
grpchook/tools.py                 # set_metadata, generate_message, evaluate_history
grpchook/timer.py                 # high-precision periodic timer (multiprocessing)
grpchook/schema_version.py        # SHA-256 proto fingerprint for compat check
grpchook/custom_interface.py      # runtime .proto compile+load
grpchook/message.proto         # proto source (one service, one bidirectional RPC)
grpchook/message_pb2*.py       # generated --- DO NOT EDIT
```

Regen proto: `python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. --pyi_out=. grpchook/message.proto`

## Proto --- Message fields

| Field | Type | Purpose |
|---|---|---|
| `messageId` | `string` | correlation ID (optional) |
| `messageName` | `string` | routing key via `provides`/`requires` |
| `clientInfo` | `ClientProvides` | first msg on connect: UUID, `provides`, `requires` |
| `serverInfo` | `ServerProvides` | welcome reply: server UUID |
| `payload` | `Payload` | `bytes bytePayload` or `Struct structPayload` |
| `history` | `repeated DataPoint` | per-hop timestamps + perf_counter |

## BaseServer --- [grpchook/BaseServer.py](grpchook/BaseServer.py)

```python
BaseServer(port, name, ip="[::]" global_exit_event=None, ssl_credentials=None, config=None)
# config = ServerConfig(max_workers, max_queue_elements, shutdown_poll_interval, server_options)
```

Connect flow: `Peer` created → `notification_queue` registered → `_receive_thread()` starts → first msg: `on_client_connect()` + register `requires` in `DataRegister` + enqueue welcome → subsequent msgs: `on_receive()` → main thread yields queue → disconnect: remove from `DataRegister`.

Schema check: reads `SCHEMA_VERSION_METADATA_KEY` from metadata → `FAILED_PRECONDITION` on mismatch.

**Hooks:**
| Method | Signature | Return |
|---|---|---|
| `on_init` | `()` | --- |
| `on_shutdown` | `()` | --- |
| `on_client_connect` | `(request, context)` | `bool` True=accept |
| `on_client_accepted` | `(peer, request)` | --- |
| `on_client_disconnect` | `(peer)` | --- |
| `on_receive` | `(peer, request)` | `bool` True=fan-out |

**Other:** `serve_forever()`, `shutdown()`, `_add_static_data(name, msg)`, `_get_static_data(name)`

## BaseClient --- [grpchook/BaseClient.py](grpchook/BaseClient.py)

```python
BaseClient(identifier, port, provides=None, requires=None, ip="localhost",
           config=None)
# config = ClientConfig(receive_queue_maxsize, connection_check_timeout, ssl_credentials, grpc_options)
```

Init → `_setup_connection()` (new UUID, channel/stub/queues, `run_event`) → `run()` → `_connect()` + `_start_receive_thread()` + `_check_connection()` → `on_init()`.

UUID regenerated each `_setup_connection()` → prevents `DataRegister` race on fast reconnect.

**Threads:** `_request_generator()` blocks `send_queue` (1s timeout) → yields to gRPC. `_receive_loop()` reads stream → `receive_queue`. `run_event` cleared on `disconnect()` stops all.

**Methods:**
| Method | Purpose |
|---|---|
| `send_data(msg, add_history=False)` | validate `messageName` in `provides`, enqueue; `add_history` appends first `DataPoint` |
| `get_data(timeout)` | poll `receive_queue`; raises `GrpcEmpty`/`ClientExit` |
| `wait_done(additional_sleep=0.5)` | block until `send_queue.join()` + grace sleep (yielded to gRPC, not ACKed) |
| `disconnect()` | clear `run_event`, cancel stream, close channel, join thread |
| `spin(timeout=None)` | `get_data()` → `on_receive()`; False on timeout/disconnect |
| `spin_forever(timeout=None)` | loop `spin()` until False |

**Hooks:** `on_init` (after each `_setup_connection()`), `on_receive(data)`, `on_shutdown`

**Context manager:** `with client:` → `__enter__` reconnects if disconnected, `__exit__` disconnects. Reusable.

## DataRegister --- [grpchook/data_register.py](grpchook/data_register.py)

`dict[messageName → dict[clientId → queue.Queue]]`. Thread-safe: `_meta_lock` + per-messageName locks.

| Method | Purpose |
|---|---|
| `add_notification_queue_for_message_name(client_id, message_name, queue)` | subscribe |
| `remove_notification_queues_for_client(client_id)` | deregister on disconnect |
| `add_data_for_message_name(client_id, message_name, data, target_client_id=None)` | fan-out, skip sender; `target_client_id`=unicast |

## Exceptions --- [grpchook/exceptions.py](grpchook/exceptions.py)

| Exception | Raised when |
|---|---|
| `GrpcTimeoutError` | `DEADLINE_EXCEEDED` |
| `GrpcConnectionError` | channel/connect failure |
| `GrpcResourceExhaustedError` | msg exceeds size limit |
| `GrpcServerNoAnswerReceivedError` | no server response |
| `GrpcValueError` | bad arg types |
| `ClientExit` | `run_event` cleared during `get_data()` |
| `GrpcEmpty` | `get_data()` timeout |

## grpchook Utils

**Logger** (`grpchook/logger.py`): `get_logger(name)` → `GrpcLogger`. Custom levels `INTERNAL_INFO=7`, `INTERNAL_DEBUG=5`. Console `coloredlogs` default `INFO`. File `%TEMP%/grpcLogs/<name>_YYYYMMDD.log` daily rotation 30d at `INTERNAL_DEBUG`.

**Tools** (`grpchook/tools.py`): `set_metadata(msg)` auto-sets `messageId`+`timestamp`. `generate_message(name, byte_payload, struct_payload)` → `Message`. `struct_to_json`/`json_to_struct`. `evaluate_history(data, log_callback)` → per-hop latency.

**Timer** (`grpchook/timer.py`): `timedevent(s, n)` context manager --- drift-compensated, RT priority.
```python
with timedevent(s=0.01, n=100) as te:
    for tick in te: ...
```

**Schema version** (`grpchook/schema_version.py`): SHA-256 `FileDescriptorProto` → `SCHEMA_VERSION_METADATA_KEY`.

**Custom interface** (`grpchook/custom_interface.py`): `compile_proto(proto_path, out_dir)` + `load_module(...)` --- runtime proto compile/load without touching `grpchook/`.

## Design Patterns

1. One `DataChannel` stream per client --- all data through it.
2. `provides`/`requires` → `DataRegister` fan-out routing.
3. `messageName` = routing key (string), not separate RPCs.
4. Hook subclassing --- override `on_receive`, `on_client_connect`, etc.
5. `with client:` auto-reconnects; new UUID each connect.

## Dependencies

```
grpcio>=1.76.0  grpcio-tools>=1.73.1  protobuf>=6.31.0  coloredlogs>=15.0  psutil>=5.0.0
```

## TODOs

- Schema mismatch shows hash only, no human-readable label.
- `wait_done()` = yielded to gRPC, not server ACK.
