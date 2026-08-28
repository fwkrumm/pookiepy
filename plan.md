# Custom protobuf interface refactor

## Goal

Remove global protobuf-module replacement and make custom interfaces explicit dependencies.

Current behavior relies on `compile_and_register()` replacing:

- `pookiepy.message_pb2`
- `pookiepy.message_pb2_grpc`

This creates import-order dependence and prevents multiple interfaces from safely coexisting.

This project is pre-release, so removing the current registration API is acceptable.

## Target API

Users compile or load an interface once:

```python
from pookiepy.custom_interface import compile_interface

proto = compile_interface("path/to/message.proto")
```

Then pass it to client/server instances:

```python
client = MyClient(
    port=50051,
    proto_interface=proto,
)
```

```python
server = MyServer(
    port=50051,
    proto_interface=proto,
)
```

The bundled interface remains the default when no custom interface is supplied.

## Interface object

Add an immutable `ProtoInterface` container:

```python
@dataclass(frozen=True)
class ProtoInterface:
    message_pb2: ModuleType
    message_pb2_grpc: ModuleType
```

It should validate both modules during construction.

Supported constructors or helpers:

- `compile_interface(proto_path, out_dir=None)`
- `load_interface_from_dir(dir_path)`
- `load_interface_from_modules(message_pb2, message_pb2_grpc)`
- `load_interface_from_imports(message_name, grpc_name)`

Keep implementation surface small. Public API only needs `compile_interface()` plus direct module injection if useful.

## custom_interface.py changes

### Remove

Remove global-registration behavior:

- `register_modules()`
- `compile_and_register()`
- `sys.modules` replacement under `pookiepy.message_pb2`
- temporary bare `message_pb2` registration if avoidable

`compile_proto()` may also be removed as public API. Move its subprocess logic into `compile_interface()` or make it private as `_compile_proto()`.

Do not mutate generated module names after loading.

### Replace loading

Compile generated files into a unique temporary or requested output directory.

Load generated modules under a unique package namespace, or require generated files to use package-safe imports.

Avoid modifying global module state.

### Strengthen validation

Validate protobuf descriptors, not only Python attributes.

Required messages:

- `Message`
- `MetaInformation`
- `DataPoint`
- `ClientProvides`
- `ServerProvides`
- `Payload`

Required `Message` fields:

- `metaInfo`
- `history`
- `payload`

Required `MetaInformation` fields:

- `timestamp`
- `messageId`
- `responseToId`
- `clientInfo`
- `serverInfo`
- `messageName`

Required gRPC symbols:

- `StreamStub`
- `StreamServicer`
- `add_StreamServicer_to_server`

Validate that `Stream` contains bidirectional `DataChannel`.

Raise one clear custom-interface exception describing missing symbols, fields, or RPCs.

## BaseClient changes

In `pookiepy/baseclient.py`:

1. Remove module-level generated-interface imports.
2. Add an optional `proto_interface` constructor argument.
3. Resolve the bundled interface when the argument is omitted.
4. Store `self._message_pb2` and `self._message_pb2_grpc`.
5. Replace every module-level protobuf reference with instance references.
6. Construct stubs through `self._message_pb2_grpc.StreamStub(self.channel)`.
7. Update type annotations to avoid hard-coded generated classes, or use protocol-style typing.

Preserve existing default behavior for users who do not provide a custom interface.

## BaseServer changes

In `pookiepy/baseserver.py`:

1. Remove module-level generated-interface imports.
2. Add an optional `proto_interface` constructor argument.
3. Resolve the bundled interface when the argument is omitted.
4. Store `self._message_pb2` and `self._message_pb2_grpc`.
5. Replace all message construction and annotations using module-level imports.
6. Change `class BaseServer(message_pb2_grpc.StreamServicer):` to `class BaseServer:`.
7. Register the server through the selected interface:
   `self._message_pb2_grpc.add_StreamServicer_to_server(self, server)`.

`BaseServer` already implements `DataChannel`; generated inheritance is not required for registration.

## Import and lifecycle requirements

A custom interface must be loadable:

- after `pookiepy` is imported
- before or after `BaseClient`/`BaseServer` are imported
- without changing existing entries in `sys.modules`
- more than once in one process
- alongside a different custom interface

The client and server must use the same wire-compatible `.proto` schema. Keep application-level schema-version validation unchanged.

## Tests

Update existing custom-interface integration tests:

- Stop importing a side-effect setup module.
- Call `compile_interface()` explicitly.
- Pass the returned interface to both client and server.
- Verify bundled `pookiepy.message_pb2` remains unchanged.
- Verify custom modules are not registered as `pookiepy.message_pb2`.
- Verify two custom interfaces can coexist.
- Verify two clients can use different interface objects in one process, if supported.

Add unit tests for:

- compilation failure
- missing generated files
- missing required messages
- missing required fields
- missing `DataChannel`
- module-object loading
- import-string loading
- directory loading
- default bundled-interface resolution
- no `sys.modules` pollution

Run the full suite with `uv`.

## Documentation

Update the README, custom-interface integration documentation, and generated CLI examples that reference `compile_and_register()`.

Document the explicit interface flow:

```python
from pookiepy.custom_interface import compile_interface

proto_interface = compile_interface("message.proto")
client = MyClient(port=50051, proto_interface=proto_interface)
```

Remove documentation describing module replacement or import-order requirements.

## Migration policy

This is pre-release. Remove old APIs directly:

- `compile_proto`
- `compile_and_register`
- `register_modules`

No deprecation shim is required.

If compatibility becomes necessary later, retain `compile_and_register()` only as an explicitly documented legacy global-registration mode.

## Completion criteria

- No production code depends on replacing `pookiepy.message_pb2`.
- No custom-interface setup relies on import side effects.
- `BaseClient` and `BaseServer` accept explicit interface objects.
- Default bundled interface still works.
- Multiple interfaces can coexist without module collisions.
- Full test suite passes.
- Documentation uses only the explicit interface API.

## Implementation order

1. Add `ProtoInterface` and loader.
2. Refactor `BaseClient`.
3. Refactor `BaseServer`.
4. Update tests and examples.
5. Remove `compile_proto`, `compile_and_register()`, and `register_modules()`.
6. Update documentation.
