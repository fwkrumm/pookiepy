# Rust Port Gaps & Blockers

## Status: Prototype (Not Behavior-Equivalent to Python Server)

### Current State Assessment
The Rust port is still a prototype, not a complete or behavior-equivalent port of the Python server.

---

## Main Blockers

| # | Issue | Location | Severity |
|---|-------|----------|----------|
| 1 | Real schema check is still stubbed | `core.rs` | 🔴 Critical |
| 2 | Real Tonic server startup is missing | `core.rs` | 🔴 Critical |
| 3 | Routing model simplified (topic->queue) vs Python-style (topic->clientId->queue) | `core.rs`, `data_register.py` | 🟠 High |
| 4 | Disconnect cleanup is destructive (wipes all subscribers) | `core.rs` | 🟠 High |
| 5 | Connected-client accounting incomplete | `core.rs` | 🟡 Medium |

---

## Risk Summary

- **Schema validation**: Currently stubbed, no version checking on connect/register
- **Server lifecycle**: Placeholder sleep-loop instead of real Tonic server
- **Routing semantics**: Missing clientId granularity in message routing
- **Cleanup behavior**: Destructive to all subscribers vs. per-client cleanup
- **Accounting**: No connected client count enforcement

---

## Recommended Priority Order

1. 🔴 Implement schema validation (blocker for production)
2. 🔴 Implement real Tonic server startup
3. 🟠 Fix routing model semantics
4. 🟠 Fix disconnect cleanup behavior
5. 🟡 Complete connection accounting
