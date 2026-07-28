# Qwen3 Implementation Tasks - Rust Port of Python gRPC Server

## Objective
Transform the prototype into a behaviorally correct implementation with parity to the Python server.

---

## Task 1: Implement Real Schema Validation

**Priority**: 🔴 Critical  
**Location**: `core.rs` (register/connect methods)

### Requirements
- Read incoming metadata key used by Python server for schema/version
- Compare against server schema fingerprint/version stored in state
- Return gRPC `FAILED_PRECONDITION` on mismatch
- Include clear error message indicating expected vs. provided version

### Acceptance Criteria
- Schema mismatch during connect/register returns proper error code
- Valid schemas proceed normally through handler chain
- Error messages are actionable for clients

---

## Task 2: Replace Placeholder Server Startup

**Priority**: 🔴 Critical  
**Location**: `core.rs` (main loop)

### Requirements
- Remove sleep-loop placeholder in `server_main.rs` / `core.rs`
- Build real Tonic server using `tonic::transport::Server`
- Bind to configured IP:port from environment/config
- Register the streaming service with the server
- Hook graceful shutdown to global exit event (`Ctrl+C`, signal handlers)

### Acceptance Criteria
- Server binds correctly and logs startup confirmation
- Service methods are callable via gRPC clients
- Graceful shutdown completes within timeout, cleans up connections

---

## Task 3: Rebuild Routing Model

**Priority**: 🟠 High  
**Location**: `core.rs`, routing logic in threading.rs

### Requirements
- Follow Python pattern from `data_register.py`
- Use structure equivalent to: `messageName -> clientId -> queue`
- Support subscription from client `requires` field
- Support broadcast to all subscribers of a message type
- Skip sender when broadcasting (don't deliver to originator)
- Support optional unicast target client by clientId

### Acceptance Criteria
- Subscribers receive only messages they require
- Broadcasts fanout correctly to all relevant clients
- Sender is excluded from their own broadcasted messages
- Unicast routing works for targeted delivery

---

## Task 4: Fix Disconnect Cleanup

**Priority**: 🟠 High  
**Location**: `core.rs` (disconnect handler)

### Requirements
- Remove only the disconnected client's subscriptions
- Do not wipe all subscribers or global routing state
- Clean up per-client data structures safely
- Ensure no dangling references remain after cleanup

### Acceptance Criteria
- One client disconnect doesn't affect other connected clients
- Routing table remains intact for remaining subscribers
- No memory leaks from orphaned task handles

---

## Task 5: Fix Connection Accounting

**Priority**: 🟡 Medium  
**Location**: `core.rs` (connect/disconnect handlers)

### Requirements
- Increment connected-client count on connect/accept
- Decrement on disconnect (successful cleanup)
- Enforce `max_workers` behavior when limit reached
- Track state atomically with proper synchronization

### Acceptance Criteria
- Connected client count is accurate at all times
- New connections rejected when max_workers exceeded
- Count persists correctly across server restarts (if applicable)

---

## Task 6: Refactor Async Ownership

**Priority**: 🟠 High  
**Location**: All spawned tasks, threading.rs

### Requirements
- Remove fragile borrowed-self patterns inside spawned tasks
- Use owned `Arc<State>` for shared mutable state
- Pass explicit service state to task closures
- Avoid lifetime issues with outer context references

### Acceptance Criteria
- No compile warnings about borrows in async contexts
- Tasks can outlive the spawning scope safely
- State is accessible without unsafe code or raw pointers

---

## Task 7: Replace Handwritten Proto Scaffolding

**Priority**: 🟡 Medium  
**Location**: `message.rs` / proto-generated files

### Requirements
- Use generated Rust types from canonical proto in `message.proto`
- Keep field names and semantics compatible with Python server
- Prefer generated code over handwritten placeholder structs
- Ensure all required message fields are properly mapped

### Acceptance Criteria
- All gRPC messages compile from `.proto` definitions
- Field names match proto specification exactly
- Serialization/deserialization works bidirectionally

---

## Task 8: Add Comprehensive Tests

**Priority**: 🟡 Medium  
**Location**: `tests/` directory

### Test Cases Required

| Test Name | Description | Expected Behavior |
|-----------|-------------|-------------------|
| schema_mismatch_rejection | Connect with wrong version | Returns FAILED_PRECONDITION |
| welcome_before_register | Register before receiving welcome | Sequence handled correctly |
| broadcast_fanout | Send to topic with multiple subscribers | All receive, sender excluded |
| disconnect_cleanup | Disconnect one client | Others remain unaffected |
| reject_on_connect | Exceed max_workers | Connection rejected immediately |

### Acceptance Criteria
- All tests pass in CI pipeline
- Tests cover happy and error paths
- Test coverage >80% for core modules

---

## Implementation Notes

- **Do not leave placeholder logic** in the hot path
- **Prefer correctness and parity** with Python server over convenience
- **If uncertain**, preserve Python behavior rather than inventing a new one
- **Maintain backward compatibility** where possible
