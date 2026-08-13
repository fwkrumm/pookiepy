# ToDo

- [ ] Add custom grpc interface support

# pookiepy TypeScript BaseClient

TypeScript port of pookiepy BaseClient design. Goal: same developer experience and lifecycle contract, implemented with idiomatic Node async primitives.

This subfolder is self-contained. Run Node commands from `.typescript/` while Python packaging and tests remain rooted in repo top level.

## What this port preserves

- Handshake lifecycle: open channel -> start receive pipeline -> send first `clientInfo` -> wait welcome -> first response must contain `serverInfo`.
- Async send/receive queues with backpressure-safe receive queue.
- `waitDone()` semantics: confirms handoff to gRPC stream write path, not server application ACK.
- Fatal stream errors routed fast into caller path (`run()` and `getData()`).
- Reconnect creates fresh per-connection UUID and resets handshake/session state.
- Hook-based API: subclass `BaseClient`, override hooks, run client.

## Install

```bash
npm install
npm run proto:gen
```

## Usage

```ts
import { BaseClient, Message, MetaInformation } from "./src";

class TextClient extends BaseClient {
  protected override async onInit(): Promise<void> {
    const msg = new Message();
    const meta = new MetaInformation();
    meta.setMessagename("text-out");
    msg.setMetainfo(meta);
    this.sendData(msg);
  }

  protected override async onReceive(data: Message): Promise<boolean> {
    console.log("received", data.getMetainfo()?.toObject());
    return true;
  }

  protected override async onShutdown(): Promise<void> {
    console.log("client shutdown");
  }
}

async function main(): Promise<void> {
  const client = new TextClient({
    port: 50051,
    name: "text-client",
    provides: ["text-out"],
    requires: ["text-in"]
  });

  await client.run();
  await client.spinForever(1_000);
}

void main();
```

## Hook contract

- `onInit()` runs after successful handshake.
- `onReceive(message)` runs for each inbound message consumed by `spin()`/`spinForever()`.
- `onShutdown()` runs during disconnect.
- `onDataYield(message)` runs right before message handoff to gRPC stream write path.

## Scripts

- `npm run proto:gen` generate protobuf/grpc-js stubs from `../pookiepy/message.proto`
- `npm run typecheck` strict TypeScript check
- `npm run lint` lint source and tests
- `npm run test` run unit + integration-style tests

See `AGENTS.md` in this folder for layout and maintenance conventions.

## Notes

- No Python context manager/destructor emulation in this TS port.
- Compression is passed through gRPC channel options when configured.
