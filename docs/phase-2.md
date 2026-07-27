# Phase 2 — Election over libp2p

What Phase 2 delivers, what it depends on, and how you know it is done.

**Phase 2 goal:** Raft leader election runs over a custom `/libp2p-raft/1.0.0` unary RPC path so a 3-node example elects **one stable** Leader and two Followers.

Maps to Tasks 1–8 in [`docs/superpowers/plans/2026-07-27-phase-2-election-over-libp2p.md`](superpowers/plans/2026-07-27-phase-2-election-over-libp2p.md) (includes Phase 0 networking shell).

---

## Where Phase 2 sits

| Phase | Deliverable |
|-------|-------------|
| 0 | Handler + PeerMap + echo/RPC shell |
| **1** | Engine election + MemoryStorage (unit tests) |
| **2** | Wire votes + heartbeats; 3-node elects stable leader |
| 3 | AppendEntries replication + propose + commit |

---

## Delivered

| Area | Notes |
|------|--------|
| `PeerMap` | NodeId ↔ PeerId + seed addrs |
| `RaftHandler` | ReadyUpgrade unary framed RPC, concurrent substreams |
| `RaftBehaviour` | Owns `RaftEngine`, deadline Sleep, PendingRequest (no vote RPC retry) |
| Engine AE accept | Empty heartbeat resets election deadline; stale term rejected |
| `echo_two_peers` | Two-peer smoke (connection + role change) |
| `three_node` | Exactly 1 Leader / 2 Followers, same term, stable ≥ 1s |

Provider-review constraints applied: term guards on AE, no RequestVote retries, keep-alive / idle timeout, inbound `NotifyHandler::One`, stability window.

---

## Done when

```bash
cargo test
cargo run --example echo_two_peers
cargo run --example three_node
```

All pass / exit 0.

---

## Out of scope

- `propose` / commit / `Event::Committed`
- Non-empty AppendEntries log matching + `next_index` decrement
- Snapshots / membership change
- YAML/TOML config loading

---

## Next

Phase 3 (Task 6 in parent plan): replication, propose, commit.
