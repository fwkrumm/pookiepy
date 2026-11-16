---
name: pookiepy
description: Build, modify, debug, and test Python pookiepy applications using BaseServer/BaseClient hooks, bidirectional gRPC streams, provides/requires routing, request-response messages, or custom protobuf interfaces. Use for pookiepy-specific work, not generic gRPC development.
compatibility: Python 3.10 or later with pookiepy installed in the active environment. No network access is needed to install this bundled skill.
---

# Pookiepy development

## Purpose and scope

Implement application behavior through pookiepy hooks. Reuse the base classes for
channels, streams, queues, handshakes, and lifecycle handling. Do not add a separate
RPC for each application message.

## Procedure

1. Inspect the active environment's pookiepy version, relevant application code,
   tests, and repository instructions. Match the installed API; do not assume an
   example from another release has the same signature.
2. Identify message names, senders (`provides`), receivers (`requires`), payload
   schema, and whether responses or application acknowledgements are required.
3. Choose the bundled protobuf interface unless the task needs a custom one.
   For custom schemas, read [Custom Interface](references/API_REFERENCE.md#custom-interface)
   before editing. Compile outside pookiepy and inject a `ProtoInterface`.
4. Subclass `BaseServer` or `BaseClient`; override only necessary hooks. Read
   [Server](references/API_REFERENCE.md#server) or
   [Client](references/API_REFERENCE.md#client) before changing lifecycle behavior.
5. Implement routing and error handling explicitly. Use bounded receive waits.
   Keep unrelated messages in their normal dispatch flow.
6. Add focused tests for the changed behavior and run the project's documented
   checks. Include a real message exchange when changing routing or correlation.
7. Report changed behavior, configuration assumptions, exact validation commands,
   results, and anything not verified.

For TLS, compression, queue limits, logging, timers, history, and exception details,
load only the relevant section of [API_REFERENCE.md](references/API_REFERENCE.md).
This reference belongs to the bundled pookiepy release; it is not a promise of
compatibility with arbitrary installed versions.

## Invariants and lifecycle

- Each client uses one bidirectional `Stream.DataChannel` stream.
- Routing and identifiers are under `message.metaInfo`: `messageName`, `messageId`,
  and `responseToId`. They are not top-level message fields.
- `on_receive` returning `True` on the server enables fan-out to subscribers,
  excluding the sender. Return `False` after manual handling to avoid double sends.
- Unicast also requires the destination client to subscribe to the response name.
  Manual routing uses the protected `_data_register`; inspect `DeliveryResult`.
  Its `delivered` entries mean enqueued, not received or processed by the peer.
- `BaseClient` connects during construction. Its `on_init` runs before channel,
  queues, and handshake setup on every connection attempt, including reconnection.
  Do not send from this hook.
- `with client:` disconnects at exit and reconnects a previously disconnected client.
  Reconnection creates a new UUID; do not persist routing targets across connections.
- `wait_done()` and `on_data_yield` confirm only local hand-off to gRPC, not peer
  receipt. Use an application-level reply if processing confirmation is required.
- Hooks are synchronous. Bound external I/O and make shutdown cleanup repeatable;
  server `shutdown()` invokes `on_shutdown` on every call.

## Configuration and constraints

- Configure clients with `ClientConfig` and servers with `ServerConfig`.
- A server worker is occupied for each active stream. Set `max_workers` for the
  required concurrency; excess streams can raise `GrpcResourceExhaustedError`.
- Queue bounds are opt-in. Full bounded server subscriber queues drop newest data.
  Handle `DeliveryResult.dropped`; retry only when the application permits it.
- With no explicit schema label, the bundled interface uses pookiepy's default
  label. Custom interfaces default to an empty label. Set the same application-owned
  `schema_version` on both sides for an explicit check. Unequal labels fail with
  `FAILED_PRECONDITION`; two empty labels warn and allow the connection.
- Never edit generated protobuf Python files manually, compile `.proto` files at
  runtime, or replace global protobuf modules. Both endpoints must be wire-compatible.
- Helpers using `structPayload`/`bytePayload` require those fields in the active
  schema. Otherwise construct messages with the custom generated classes.
- Prefer `self.logger` in hooks and `jacl4py.logger.get_logger` in helper modules.
  `logger.setLevel` changes the logger threshold, not handler thresholds.
- Import timers from `simpytick.timer`. Use `backend="thread"` after starting gRPC;
  guard process-backed timer creation with an importable script's main guard.
- Do not embed real credentials in examples or generated code. Follow the
  application's existing secret-loading mechanism.

## Example: correlated request and response

Assumptions: the bundled interface, pookiepy installed, an available port 50051,
and two local processes. The client below sends one request; a mixed-traffic client
must dispatch unrelated responses instead of discarding them.

Save this server as `server.py` and start it before the client:

```python
from pookiepy.baseserver import BaseServer
from pookiepy.tools import generate_message


class MyServer(BaseServer):
    def __init__(self, port: int = 50051):
        super().__init__(port=port, name="example-server")

    def on_receive(self, peer, request):
        if request.metaInfo.messageName != "my_request":
            return True
        reply = generate_message("my_response", struct_payload={"ok": True})
        reply.metaInfo.responseToId = request.metaInfo.messageId
        result = self._data_register.add_data_for_message_name(
            peer.client_id, "my_response", reply, target_client_id=peer.client_id
        )
        if result.dropped:
            self.logger.warning("Response not queued for %s", result.dropped)
        return False


if __name__ == "__main__":
    MyServer().serve_forever()
```

Save this client as `client.py` and run it in a second terminal:

```python
from pookiepy.baseclient import BaseClient
from pookiepy.tools import generate_message, struct_to_json


class MyClient(BaseClient):
    def __init__(self, port: int = 50051):
        super().__init__(
            port=port,
            name="example-client",
            provides=["my_request"],
            requires=["my_response"],
        )


def request_once(port: int = 50051):
    with MyClient(port=port) as client:
        request = generate_message("my_request", add_metadata=True)
        client.send_data(request)
        reply = client.get_data(timeout=5.0)
        if reply.metaInfo.responseToId != request.metaInfo.messageId:
            raise RuntimeError("Unexpected response correlation ID")
        payload = struct_to_json(reply.payload.structPayload)
        if payload != {"ok": True}:
            raise RuntimeError(f"Unexpected response payload: {payload}")
        return payload


if __name__ == "__main__":
    print(request_once())
```

`add_metadata=True` assigns the request ID before it is read for correlation.
Do not copy the request ID into the reply's own `messageId`. Every streamed reply
chunk uses the same `responseToId` and its own message ID.

## Installation and usage

From the intended project root, run `python -m pookiepy --install-skills` to
install this complete skill for all four supported tool targets:

| Per-tool command | Project-relative skill directory |
|---|---|
| `--install-skills-github` | `.github/skills/pookiepy/` |
| `--install-skills-claude` | `.claude/skills/pookiepy/` |
| `--install-skills-cursor` | `.cursor/skills/pookiepy/` |
| `--install-skills-codex` | `.agents/skills/pookiepy/` |

Each directory contains `SKILL.md` and `references/API_REFERENCE.md`. Per-tool flags
may be combined. Required directories are created, but optional empty `scripts/`
and `assets/` directories are not: this is an instruction-only skill.

The current working directory is the install root; the CLI does not search for a
Git root, detect installed IDEs, install into home directories, change IDE settings,
or download resources. Use the same command on Windows, macOS, and Linux.
Identical files are left untouched. Conflicting files, symlinks in destination
paths, or non-directory parents fail with a nonzero exit before any skill files
are written. Review and move conflicting files, then retry. An I/O failure during
writing can leave a partial install; fix its cause and rerun.

Some tools recognize several of these directories and can list duplicate copies.
Use a per-tool command instead of the all-tools command when that is undesirable.
Discovery depends on tool/harness version and policy. Open a fresh agent session,
check its skill picker, explicitly invoke `pookiepy`, and verify it reads this file
and its reference. Then test automatic selection with a pookiepy-specific request.
Installation does not guarantee activation. Commit project skills when remote or
cloud agents need them; files on a local machine are not automatically uploaded.

Example request: "Use the pookiepy skill to add a correlated response to my client
and server, preserving handling for other message names."

## Completion checks

- Required message names and schema versions agree between endpoints.
- A response is correlated by `responseToId`, not by reusing `messageId`.
- Timeouts, disconnects, queue limits, and cleanup have explicit behavior.
- Relevant tests pass, or the exact unverified checks and blockers are reported.
- No generated protobuf files or unrelated project behavior changed.
