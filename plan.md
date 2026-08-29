# Breaking rename: `Message` → `PookieMessage`

## Goal

Rename Pookiepy’s generic protocol-envelope protobuf message from `Message` to
`PookieMessage`. This separates it from
`google.protobuf.message.Message`, the generic protobuf base class.

This is a breaking protocol/API change. Release it as the next breaking version.

## Compatibility impact

- Preserve field numbers, field names, package name, and RPC name.
- Serialized field layout remains unchanged.
- gRPC service descriptors change from `message.proto.v3.Message` to
  `message.proto.v3.PookieMessage`.
- Old and new clients/servers must not be mixed.
- Add `docs/required_adjustments/<next-breaking-version>.md` with migration
  instructions and incompatibility warning.

## Implementation plan

1. Rename `message Message` to `message PookieMessage` in
	`pookiepy/message.proto`.
2. Update `Stream.DataChannel` request and response types.
3. Apply same schema rename to the custom-interface fixture proto.
4. Regenerate all protobuf Python and typing bindings with `uv`; never edit
	generated files manually.
5. Replace concrete runtime references to
	`self._message_pb2.Message` with
	`self._message_pb2.PookieMessage` in `BaseClient`, `BaseServer`,
	`DataRegister`, and helpers.
6. Update `ProtoInterface` required-symbol and descriptor validation to require
	`PookieMessage`.
7. Update `generate_message()` to construct `PookieMessage`.
8. Update runtime error text, docstrings, type hints, CLI code, tests,
	integrations, examples, README, HOW_TO, and repository guidance.
9. Keep Google’s generic base type distinct:

	```python
	from google.protobuf.message import Message as ProtobufMessage
	```

	`ProtobufMessage` means generic Google protobuf base class;
	`message_pb2.PookieMessage` means Pookiepy protocol envelope.
10. Search for stale `message_pb2.Message`, `.Message`, `Message.DESCRIPTOR`,
	 `message.proto.v3.Message`, and validation/error text. Classify intentional
	 Google protobuf references; remove stale Pookiepy references.

## Custom-interface requirements

`ProtoInterface` must validate:

- `PookieMessage`
- `MetaInformation`, `DataPoint`, `ClientProvides`, `ServerProvides`, `Payload`
- `PookieMessage` fields: `metaInfo`, `history`, `payload`
- bidirectional `Stream.DataChannel` using `PookieMessage`
- required generated gRPC symbols

Custom users must regenerate their interface and replace
`message_pb2.Message()` with `message_pb2.PookieMessage()`.

## Tests and validation

- Add bundled-interface symbol and descriptor tests.
- Add custom-interface validation and communication tests.
- Verify old `Message` symbol is absent from generated Pookiepy modules.
- Verify wrong protobuf types remain rejected.
- Run unit tests, integration tests, CLI tests, and Pylint through `uv`.
- Build package and verify generated descriptors.
- Confirm generated files match proto sources.

## Completion criteria

- Bundled and custom interfaces use `PookieMessage`.
- No stale Pookiepy `Message` references remain outside migration notes.
- Generated bindings are regenerated, not hand-edited.
- Documentation gives explicit migration examples.
- Old/new protocol incompatibility is documented.

---

# Previous completed plan: explicit precompiled protobuf interfaces
