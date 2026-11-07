# AGENTS.md

Python gRPC bidirectional-streaming framework. Subclass `BaseServer`/`BaseClient`, override hooks. Base handles all gRPC plumbing.

## Layout

```
pookiepy/baseserver.py       # server base (StreamServicer)
pookiepy/baseclient.py       # client base
pookiepy/data_register.py         # server-side msg routing: messageName→clientId→queue
pookiepy/exceptions.py            # exception hierarchy
pookiepy/logger.py                # GrpcLogger + rotating file logger
pookiepy/tools.py                 # set_metadata, generate_message, evaluate_history
pookiepy/timer.py                 # high-precision periodic timer (multiprocessing)
pookiepy/schema_version.py        # schema-version metadata key for compat check
pookiepy/custom_interface.py      # validated precompiled protobuf module injection
pookiepy/message.proto         # proto source (one service, one bidirectional RPC)
pookiepy/message_pb2*.py       # generated --- DO NOT EDIT
```

Regen proto: `python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. --pyi_out=. pookiepy/message.proto`

Regen custom integration fixture with runtime-compatible pins:
`uv run --isolated --with grpcio-tools==1.81.0 --with protobuf==6.33.5 python -m grpc_tools.protoc -I tests/integration/custom_interface --python_out=tests/integration/custom_interface --grpc_python_out=tests/integration/custom_interface --pyi_out=tests/integration/custom_interface tests/integration/custom_interface/custom_if/message.proto`

## Proto --- PookieMessage fields

| Field | Type | Purpose |
|---|---|---|
| `messageId` | `string` | correlation ID (optional) |
| `messageName` | `string` | routing key via `provides`/`requires` |
| `clientInfo` | `ClientProvides` | first msg on connect: UUID, `provides`, `requires` |
| `serverInfo` | `ServerProvides` | welcome reply: server UUID |
| `payload` | `Payload` | `bytes bytePayload` or `Struct structPayload` |
| `history` | `repeated DataPoint` | per-hop timestamps + perf_counter |

## BaseServer --- [pookiepy/BaseServer.py](pookiepy/BaseServer.py)

```python
BaseServer(port, name, ip="[::]" global_exit_event=None, ssl_credentials=None, config=None,
           proto_interface=None)
# config: max_workers, max_queue_elements=0, shutdown_poll_interval, schema_version,
#         server_options
```

Connect flow: `Peer` created → `notification_queue` registered → `_receive_thread()` starts → first msg: `on_client_connect()` + register `requires` in `DataRegister` + enqueue welcome → subsequent msgs: `on_receive()` → main thread yields queue → disconnect: remove from `DataRegister`.

Schema check: compares `ClientConfig.schema_version` vs `ServerConfig.schema_version` from `SCHEMA_VERSION_METADATA_KEY` metadata. Both empty → warn and allow. Any mismatch → `FAILED_PRECONDITION`.

**Hooks:**
| Method | Signature | Return |
|---|---|---|
| `on_init` | `()` | --- |
| `on_shutdown` | `()` | --- |
| `on_client_connect` | `(request, context)` | `bool` True=accept |
| `on_client_accepted` | `(peer, request)` | --- |
| `on_client_disconnect` | `(peer)` | --- |
| `on_receive` | `(peer, request)` | `bool` True=fan-out |

**Lifecycle:** idle senders retain one-second queue polling; each `shutdown()` invokes `on_shutdown`. RPC admission is capped at effective worker count; excess clients raise `GrpcResourceExhaustedError`. Queue bounds are opt-in; full bounded queues drop newest. Manual routing returns `DeliveryResult`; warnings are rate-limited. Hooks must cooperate with shutdown.

**Other:** `serve_forever()`, `shutdown()`, `_add_static_data(name, msg)`, `_get_static_data(name)`

## BaseClient --- [pookiepy/BaseClient.py](pookiepy/BaseClient.py)

```python
BaseClient(identifier, port, provides=None, requires=None, ip="localhost",
           config=None, proto_interface=None)
# config = ClientConfig(receive_queue_maxsize, connection_check_timeout, schema_version, ssl_credentials, grpc_options)
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
| `spin(timeout=None)` | `get_data()` → `on_receive()`; raises on timeout/disconnect |
| `spin_forever(timeout=None)` | loop `spin()`; stop on timeout/disconnect or `StopSpin` |

**Hooks:** `on_init` (after each `_setup_connection()`), `on_receive(data)`, `on_shutdown`

**Context manager:** `with client:` → `__enter__` reconnects if disconnected, `__exit__` disconnects. Reusable.

## DataRegister --- [pookiepy/data_register.py](pookiepy/data_register.py)

`dict[messageName → dict[clientId → queue.Queue]]`. Thread-safe: `_meta_lock` + per-messageName locks.

| Method | Purpose |
|---|---|
| `add_notification_queue_for_message_name(client_id, message_name, queue)` | subscribe |
| `remove_notification_queues_for_client(client_id)` | deregister on disconnect |
| `add_data_for_message_name(client_id, message_name, data, target_client_id=None)` | fan-out, skip sender; `target_client_id`=unicast |

## Exceptions --- [pookiepy/exceptions.py](pookiepy/exceptions.py)

| Exception | Raised when |
|---|---|
| `GrpcTimeoutError` | `DEADLINE_EXCEEDED` |
| `GrpcConnectionError` | channel/connect failure |
| `GrpcResourceExhaustedError` | resource limit, including excess concurrent RPCs or message size |
| `GrpcServerNoAnswerReceivedError` | no server response |
| `GrpcValueError` | bad arg types |
| `GrpcCustomInterfaceError` | helper expects bundled payload field names but active custom proto differs |
| `ClientExit` | `run_event` cleared during `get_data()` |
| `GrpcEmpty` | `get_data()` timeout |
| `StopSpin` | explicit signal to stop `spin_forever()` without disconnect |

## pookiepy Utils

**Logger** (`pookiepy/logger.py`): `get_logger(name)` → `GrpcLogger`. Custom levels `INTERNAL_INFO=7`, `INTERNAL_DEBUG=5`. Console defaults to `INFO`; file retains `INTERNAL_DEBUG` tracing. File `%TEMP%/grpcLogs/<name>_YYYYMMDD.log` daily rotation 30d. Reduced file tracing is opt-in before constructing server/client; cached loggers keep their first configuration.

**Tools** (`pookiepy/tools.py`): `set_metadata(msg)` auto-sets `messageId`+`timestamp`. `generate_message(name, byte_payload, struct_payload)` → `PookieMessage`. `struct_to_json`/`json_to_struct`. `evaluate_history(data, log_callback)` → per-hop latency.

**Timer** (`pookiepy/timer.py`): `TimedEvent` context manager (canonical) + `timedevent(s, n)` alias (compat) --- drift-compensated, RT priority.
```python
with TimedEvent(s=0.01, n=100) as te:
    for tick in te: ...
```

**Schema version** (`pookiepy/schema_version.py`): metadata-key constant only. Actual schema/version string comes from `ClientConfig.schema_version` / `ServerConfig.schema_version`.

**Custom interface** (`pookiepy/custom_interface.py`): compile protobuf modules externally, construct `ProtoInterface(message_pb2, message_pb2_grpc)`, then pass `proto_interface=` to each client/server. No runtime compilation or global module replacement.

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

## Test boundaries

- Core framework unit tests belong directly in `tests/`; do not import or test `examples/` components there.
- Unit tests for `examples/` belong in `tests/examples/`. Keep this directory without `__init__.py` so core `unittest` discovery does not recurse into it.
- Core suite: `uv run python -m unittest discover -s tests -p "test_*.py"`.
- Example suite: `uv run python -m unittest discover -s tests/examples -p "test_*.py"`. Keep external services mocked; install optional dependencies from `requirements_examples.txt` only for example jobs.
- CI, weekly gRPC compatibility, and weekly latest-Python workflows run examples in separate `Unit test examples` jobs. Keep framework integration scenarios in `tests/integration/`.

## Latest Python CI probes

- CI structure and helper ownership: `.github/readme.md`. Keep workflow matrices and policies in callers; reuse `.github/actions/python-check` for Python suites and `.github/actions/setup-python` for tooling setup.
- Treat floating `3.x` jobs as non-blocking compatibility probes, not supported-version guarantees. Keep pinned supported-Python checks mandatory.
- Before installing dependencies in a latest-Python probe, use `uv pip install --dry-run --only-binary grpcio,grpcio-tools` with the job's actual requirement files. Give the probe step an ID and `continue-on-error: true`.
- If dependency resolution or required gRPC wheels are unavailable, emit a warning and skip dependency installation, protobuf generation, and tests. Gate those steps on the probe's `outcome == 'success'`; in mixed matrices, let pinned Python versions run normally without this gate.
- Use the same setup-python interpreter for probing, installation, and execution: set step `env.UV_PYTHON` from the setup-python step's `outputs.python-path`. Never interpolate step outputs directly into shell `run` commands.
- Apply this pattern to new latest-Python jobs, including example tests. Do not loosen dependency pins or enable gRPC source builds to make a probe pass.

## TODOs

- `wait_done()` = yielded to gRPC, not server ACK.

## Compatibility documentation workflow

- When changing a breaking public API, signature, return value, exception, or user-visible behavior, create or update `docs/required_adjustments/<version>.md`.
- Base adjustment notes on the branch diff against `master` and state exact migration actions for users upgrading from the previous version.
- Keep adjustment notes concise. Distinguish required migrations from optional configuration changes.
- Do not edit generated protobuf files directly; document schema compatibility changes separately when proto regeneration is required.
