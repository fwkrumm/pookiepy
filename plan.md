# Explicit precompiled protobuf interfaces

## Goal

Replace global protobuf-module registration with one explicit dependency-injection path. Keep bundled behavior unchanged when no custom interface is supplied.

## Sole custom-interface workflow

Users compile their `.proto` outside pookiepy, import both generated modules, construct one interface, then inject it:

```python
from my_proto import message_pb2, message_pb2_grpc
from pookiepy.custom_interface import ProtoInterface

proto_interface = ProtoInterface(message_pb2, message_pb2_grpc)
client = MyClient(port=50051, proto_interface=proto_interface)
server = MyServer(port=50051, proto_interface=proto_interface)
```

No runtime compiler, loader, import-string, directory, registration, or alternate module-pair API exists.

## Implementation

1. Make `ProtoInterface` a frozen two-module container with descriptor validation.
2. Remove runtime compilation, dynamic loading, subprocess calls, and `sys.modules` mutation.
3. Inject the interface into `BaseClient` and `BaseServer`; use bundled modules only when omitted.
4. Remove generated `StreamServicer` inheritance and register each server through its selected module.
5. Inject the selected `Message` type into `DataRegister` to preserve strict validation.
6. Let `generate_message()` accept the same optional interface object.
7. Update custom integration tests to import checked-in precompiled modules directly.
8. Update CLI skeletons and documentation to show only the sole workflow above.
9. Document migration from removed APIs in the next required-adjustments note.

## Validation

`ProtoInterface` reports all missing requirements in one `GrpcCustomInterfaceError`:

- messages: `Message`, `MetaInformation`, `DataPoint`, `ClientProvides`, `ServerProvides`, `Payload`
- `Message`: `metaInfo`, `history`, `payload`
- `MetaInformation`: `timestamp`, `messageId`, `responseToId`, `clientInfo`, `serverInfo`, `messageName`
- gRPC: `StreamStub`, `StreamServicer`, `add_StreamServicer_to_server`
- bidirectional `Stream.DataChannel` using `Message` in both directions

## Design constraints

- Explicit over implicit.
- One obvious custom-interface path.
- No hidden filesystem, import, compilation, or global-state behavior.
- Small validation functions with one task each.
- No transport, queue, hook, routing, wire-schema, or schema-version redesign.
- Generated protobuf files are regenerated externally, never edited manually.

## Completion criteria

- Bundled users need no changes.
- Custom clients and servers use instance-selected modules.
- Multiple non-conflicting generated interfaces can coexist in one process.
- No production custom-interface code invokes subprocesses or mutates `sys.modules`.
- Removed API names appear only in migration documentation and negative tests.
- Unit, integration, CLI, and lint checks pass through `uv`.
