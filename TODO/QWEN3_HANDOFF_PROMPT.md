# Handoff Prompt for Qwen3 - Rust gRPC Server Implementation

---

## Context

You are implementing a Rust port of the Python gRPC server in `baseserver.py`. Work from the current Rust scaffold:

- `core.rs` - Core server logic and state management
- `grpc.rs` - gRPC service implementation
- `threading.rs` - Async task handling
- `server_main.rs` - Entry point and startup

---

## Objective

Turn this from a prototype into a **behaviorally correct implementation**, not just a stub. The goal is parity with the Python server, not innovation.

---

## Required Work

### 1. Implement Real Schema/Version Checking

```rust
// Pseudocode guidance:
- Read incoming gRPC metadata key used by Python server
- Compare against server schema fingerprint/version in state
- Reject mismatches with FAILED_PRECONDITION status code
```

**Acceptance**: Version mismatch returns proper error, valid schemas proceed.

---

### 2. Implement Real Tonic Server Startup

```rust
// Pseudocode guidance:
- Replace sleep-loop placeholder in core.rs
- Start real Tonic server bound to configured ip/port
- Register streaming service with server builder
- Hook shutdown to global exit event (Ctrl+C, signals)
```

**Acceptance**: Server starts correctly, graceful shutdown works.

---

### 3. Implement Proper Routing Semantics

Follow Python pattern from `data_register.py`:

```rust
// Structure equivalent:
messageName -> clientId -> queue

- Support subscription from client requires field
- Support broadcast to all subscribers of message type
- Skip sender when broadcasting (don't deliver to originator)
- Support optional unicast target client by clientId
```

**Acceptance**: Routing matches Python semantics exactly.

---

### 4. Implement Correct Disconnect Cleanup

```rust
// Pseudocode guidance:
- Remove only disconnected client's subscriptions
- Do NOT wipe all routing state or other subscribers
- Clean up per-client data structures safely
```

**Acceptance**: One disconnect doesn't affect remaining clients.

---

### 5. Implement Correct Connection Lifecycle

```rust
// Pseudocode guidance:
- Increment connected-client count on connect/accept
- Decrement on disconnect (after cleanup)
- Keep max_workers semantics consistent with Python server
```

**Acceptance**: Account accurate, limit enforced.

---

### 6. Refactor Async State Ownership

```rust
// Pseudocode guidance:
- Avoid borrowing outer self/context inside spawned tasks
- Use owned Arc<State> for shared mutable state
- Pass explicit service state to task closures
```

**Acceptance**: No borrow checker issues in async contexts.

---

### 7. Use Canonical Proto Contract

```rust
// Pseudocode guidance:
- Keep Rust message model aligned with message.proto
- Prefer generated Rust types over handwritten placeholder structs
- Ensure field names and semantics match proto exactly
```

**Acceptance**: All messages compile from `.proto` definitions.

---

### 8. Add Tests for Critical Paths

Write tests covering:

| Test | Description |
|------|-------------|
| schema_mismatch_rejection | Wrong version returns FAILED_PRECONDITION |
| welcome_before_register | Sequence handled correctly |
| broadcast_fanout | All subscribers receive, sender excluded |
| disconnect_cleanup | Other clients unaffected |
| reject_on_connect | max_workers exceeded = rejection |

---

## Important Constraints

1. **Do not leave placeholder logic** in the hot path
2. **Prefer correctness and parity** with Python server over convenience
3. **If uncertain**, preserve Python behavior rather than inventing a new one
4. **Maintain backward compatibility** where possible without compromising correctness

---

## Deliverables

- [ ] All 8 required work items completed
- [ ] No placeholder/stub logic remaining in production code paths
- [ ] Tests written and passing for all critical paths
- [ ] Code compiles with no warnings related to async/borrowing
- [ ] Behavior matches Python server under equivalent conditions

---

## Questions to Resolve During Implementation

If you encounter uncertainty about specific behaviors:

1. Check `data_register.py` in the Python codebase first
2. Examine gRPC metadata handling in `baseserver.py`
3. When in doubt, prefer Python behavior over "Rust idioms" that differ
4. Document any deviations from Python semantics for review

---

**Ready to begin implementation.**
