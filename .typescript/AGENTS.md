# AGENTS.md

TypeScript `BaseClient` port for pookiepy. Keep this subfolder isolated from Python package/runtime.

## Layout

```
.typescript/src/base-client.ts              # public BaseClient implementation
.typescript/src/config.ts                   # client config defaults + merge helper
.typescript/src/errors.ts                   # TS error hierarchy mirroring Python contract
.typescript/src/queue/                      # async queue primitives for send/receive semantics
.typescript/src/state/client-state.ts       # explicit client state machine
.typescript/src/transport/grpc-transport.ts # grpc-js adapter + error mapping
.typescript/src/utils/                      # metadata/history helpers
.typescript/src/generated/pookiepy/         # generated protobuf/grpc-js bindings
.typescript/tests/                          # unit + integration-style tests
.typescript/scripts/generate-proto.js       # Windows-safe proto generation wrapper
```

## Commands

Run from `.typescript/`:

```bash
npm install
npm run proto:gen
npm run typecheck
npm run lint
npm run test
```

## Conventions

1. Public app DX stays hook-based: extend `BaseClient`, override hooks, no direct grpc-js plumbing in app subclasses.
2. Preserve handshake contract: open channel, start receive path, send `clientInfo`, require first response `serverInfo`.
3. Keep runtime modules small and focused; avoid monolith files and hidden globals.
4. Generated code stays under `src/generated/`; do not hand-edit generated files.
5. Tests should cover behavior contract first: handshake, timeout, fast stream errors, `waitDone()`, reconnect UUID reset.