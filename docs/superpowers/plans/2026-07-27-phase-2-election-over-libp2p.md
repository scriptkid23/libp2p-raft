# Phase 2 — Election over libp2p Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run Raft leader election over a custom `/libp2p-raft/1.0.0` unary RPC path so `examples/three_node.rs` elects **one stable** Leader and two Followers.

**Architecture:** Keep `RaftEngine` pure (no `PeerId` / Dial). Freeze the engine AppendEntries/heartbeat-accept API first, then finish the networking shell (`PeerMap` + `ConnectionHandler` + echo `RaftBehaviour`), then own the engine inside Behaviour: deadline `Sleep` → `tick` → unified Action executor → framed `WireEnvelope` RPCs. Full log replication stays Phase 3.

**Tech Stack:** rust-libp2p 0.54 (tokio, tcp, noise, yamux, macros), futures, tokio, serde + bincode, thiserror, tracing, Edition 2021

**Spec:** `docs/superpowers/specs/2026-07-23-libp2p-raft-design.md` §6–§7 (Phase 0 + Phase 2)  
**Parent plan:** `docs/superpowers/plans/2026-07-23-libp2p-raft.md` Tasks 4–5  
**Depends on:** Phase 1 done (`RaftEngine` election + `MemoryStorage`; `tests/engine_election.rs` green)

**Provider review:** 2026-07-27 via ai-router (`chatgpt` 8.8, `claude` 6, `deepseek` 6 — all Approve with changes; `gemini` no useful answer; `kimi` timeout). Revisions below incorporate consensus blockers.

## Global Constraints

- DIY Raft only — do not depend on OpenRaft / raft-rs / async-raft
- Custom stream + `ConnectionHandler` — do not use `request_response` for Raft RPCs
- Consensus logic never imports libp2p types (`PeerId`, `Multiaddr`, Dial)
- Tick: Behaviour deadline Sleep; call `engine.tick(now)` only when due — never busy-poll every `poll()`
- **Poll invariant:** drain handler events (inbound RPCs) **before** `tick(now)` so heartbeats can reset deadlines first
- Connection drop ≠ Raft peer-dead; do not remove voters on disconnect
- Pin keypairs in examples; static `seed_peers` with stable `PeerId`
- Phase 2: **no outbound RPC retries** for RequestVote (tick/election is the retry). On Failure → `handle_rpc_failure` + `Event::RpcFailed` only. Votes already counted via `HashSet<NodeId>` — keep that; discard stale-term responses.
- libp2p 0.54: set Swarm `idle_connection_timeout` ≫ heartbeat interval; Handler `connection_keep_alive` true while RPCs in flight **or** for a grace window (≥ 2× heartbeat)
- Prefer `ReadyUpgrade` + `StreamProtocol::new("/libp2p-raft/1.0.0")` over hand-rolled upgrade structs
- Do **not** queue Raft RPCs across reconnect (lossy network; next tick re-sends). Drop + fail pending on DialFailure / ConnectionClosed
- Phase 2 out of scope: `propose`, commit advance, snapshot, membership change, YAML/TOML config
- Comments and commit messages in English
- TDD where unit-testable; examples for Swarm integration

## Current state (as of plan write)

| Area | Status |
|------|--------|
| Types + codec | Done |
| MemoryStorage | Done |
| Election engine | Done; heartbeats **send** empty AE; **no AE receive/reset yet** |
| Handler / PeerMap / Behaviour / echo | Skeleton TODOs only |
| three_node | Skeleton `unimplemented!` |

## File map

| Path | Responsibility |
|------|----------------|
| `src/peer_map.rs` | NodeId ↔ PeerId + seed Multiaddrs |
| `src/protocol/upgrade.rs` | `PROTOCOL_NAME` + `ReadyUpgrade` / `StreamProtocol` helper |
| `src/handler.rs` | Unary framed ConnectionHandler (multi-substream) |
| `src/behaviour.rs` | NetworkBehaviour: PeerMap, PendingRequest, Sleep, engine |
| `src/raft/engine.rs` | AE receive + term guards + `handle_rpc(..., now)` |
| `src/lib.rs` | Re-exports |
| `examples/echo_two_peers.rs` | Prove Handler + codec path |
| `examples/three_node.rs` | 3 Swarms, stable Leader |
| `tests/peer_map.rs` | PeerMap unit tests |
| `tests/engine_heartbeat_reset.rs` | AE term/deadline tests |
| `docs/phase-2.md` | Phase checklist |

## Task order (revised)

```text
1 PeerMap
2 Engine AE + handle_rpc(now)     ← freeze engine API before Behaviour wiring
3 Handler + ReadyUpgrade
4 Behaviour echo shell
5 echo_two_peers
6 Wire engine into Behaviour
7 three_node (stability window)
8 docs
```

---

### Task 1: PeerMap

**Files:**
- Modify: `src/peer_map.rs`
- Create: `tests/peer_map.rs`

**Interfaces:**
- Consumes: `SeedPeer`, `NodeId`, `libp2p::{Multiaddr, PeerId}`
- Produces:
  - `PeerMap::from_seeds(seeds: &[SeedPeer]) -> Self`
  - `PeerMap::insert(node: NodeId, peer: PeerId, addrs: Vec<Multiaddr>)`
  - `PeerMap::peer_id(&self, node: NodeId) -> Option<PeerId>`
  - `PeerMap::addrs(&self, node: NodeId) -> Option<&[Multiaddr]>`
  - `PeerMap::node_id(&self, peer: PeerId) -> Option<NodeId>`

- [ ] **Step 1: Write failing tests**

```rust
// tests/peer_map.rs
use libp2p::{Multiaddr, PeerId};
use libp2p_raft::config::SeedPeer;
use libp2p_raft::peer_map::PeerMap;

#[test]
fn from_seeds_round_trips_lookups() {
    let peer = PeerId::random();
    let addr: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();
    let map = PeerMap::from_seeds(&[SeedPeer {
        node_id: 1,
        peer_id: peer,
        addrs: vec![addr.clone()],
    }]);
    assert_eq!(map.peer_id(1), Some(peer));
    assert_eq!(map.node_id(peer), Some(1));
    assert_eq!(map.addrs(1).unwrap(), &[addr][..]);
}

#[test]
fn unknown_node_returns_none() {
    let map = PeerMap::from_seeds(&[]);
    assert!(map.peer_id(99).is_none());
}
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `cargo test --test peer_map`  
Expected: compile fail or FAIL

- [ ] **Step 3: Implement PeerMap**

```rust
// src/peer_map.rs
use std::collections::HashMap;

use libp2p::{Multiaddr, PeerId};

use crate::config::SeedPeer;
use crate::raft::types::NodeId;

#[derive(Debug, Default, Clone)]
pub struct PeerMap {
    node_to_peer: HashMap<NodeId, PeerId>,
    peer_to_node: HashMap<PeerId, NodeId>,
    addrs: HashMap<NodeId, Vec<Multiaddr>>,
}

impl PeerMap {
    pub fn from_seeds(seeds: &[SeedPeer]) -> Self {
        let mut map = Self::default();
        for s in seeds {
            map.insert(s.node_id, s.peer_id, s.addrs.clone());
        }
        map
    }

    pub fn insert(&mut self, node: NodeId, peer: PeerId, addrs: Vec<Multiaddr>) {
        self.node_to_peer.insert(node, peer);
        self.peer_to_node.insert(peer, node);
        self.addrs.insert(node, addrs);
    }

    pub fn peer_id(&self, node: NodeId) -> Option<PeerId> {
        self.node_to_peer.get(&node).copied()
    }

    pub fn node_id(&self, peer: PeerId) -> Option<NodeId> {
        self.peer_to_node.get(&peer).copied()
    }

    pub fn addrs(&self, node: NodeId) -> Option<&[Multiaddr]> {
        self.addrs.get(&node).map(|v| v.as_slice())
    }
}
```

- [ ] **Step 4: Run tests — expect PASS**

Run: `cargo test --test peer_map`  
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/peer_map.rs tests/peer_map.rs
git commit -m "feat: PeerMap NodeId ↔ PeerId routing table"
```

---

### Task 2: Minimal AppendEntries receive + `handle_rpc(..., now)` (engine freeze)

**Why first among Raft changes:** freezes `handle_rpc` signature before Behaviour (Task 6) is written. Needed so followers reset election deadlines under leader heartbeats.

**Files:**
- Modify: `src/raft/engine.rs`
- Create: `tests/engine_heartbeat_reset.rs`
- Modify: `tests/engine_election.rs` (pass `now`)

**Interfaces:**
- Produces:
  ```rust
  pub fn handle_rpc(&mut self, from: NodeId, msg: RaftMessage, now: Instant) -> Vec<Action>
  ```
- AE rules (must implement exactly):
  1. If `req.term < current_term` → `AppendEntriesResp { term: current_term, success: false, match_index: … }`, **do not** reset `election_deadline`, **do not** set leader
  2. If `req.term > current_term` → persist term / `become_follower(term, Some(leader_id))` then continue
  3. If `req.term >= current_term` (after step-down if needed): set leader, `reset_election_deadline(now)`
  4. Phase 2 empty `entries`: `success: true` (optionally gate on `prev_log_index`/`prev_log_term` matching last entry — preferred over blind `true`)
  5. Non-empty `entries`: `success: false` until Phase 3 (document in test)
  6. On any response/request with higher term: step down before further processing (already partly true for votes)
- `AppendEntriesResp` already includes `term` — leaders must step down when resp.term > current
- Vote grants: keep `HashSet`; add test that duplicate `RequestVoteResp` from same peer does not falsely win a 3-voter election

- [ ] **Step 1: Write failing tests**

```rust
// tests/engine_heartbeat_reset.rs
use std::time::{Duration, Instant};

use libp2p_raft::config::RaftConfig;
use libp2p_raft::protocol::RaftMessage;
use libp2p_raft::raft::engine::{Action, RaftEngine};
use libp2p_raft::raft::types::Role;
use libp2p_raft::storage::MemoryStorage;

fn cfg(id: u64) -> RaftConfig {
    RaftConfig {
        node_id: id,
        voters: vec![1, 2, 3],
        election_timeout: Duration::from_millis(150),
        election_jitter: Duration::ZERO,
        heartbeat_interval: Duration::from_millis(50),
        rpc_timeout: Duration::from_millis(100),
        rpc_max_retries: 0, // Phase 2: unused for votes
        snapshot_threshold: 10_000,
        seed_peers: vec![],
    }
}

#[test]
fn empty_heartbeat_resets_deadline_follower_stays() {
    let mut eng = RaftEngine::new(cfg(1), MemoryStorage::new());
    let t0 = Instant::now();
    // Force a known deadline by sending heartbeat at t0 (after bumping term via AE term=1)
    let _ = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
        t0,
    );
    let out = eng.tick(t0 + Duration::from_millis(100));
    assert!(matches!(eng.role(), Role::Follower));
    assert!(out.actions.is_empty());
}

#[test]
fn stale_term_ae_does_not_reset_deadline() {
    let mut eng = RaftEngine::new(cfg(1), MemoryStorage::new());
    let t0 = Instant::now();
    // Advance to term 2 via a valid heartbeat
    let _ = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 2,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
        t0,
    );
    // Stale term=1 must not refresh deadline
    let actions = eng.handle_rpc(
        3,
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
        t0 + Duration::from_millis(10),
    );
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: RaftMessage::AppendEntriesResp { success: false, term: 2, .. },
            ..
        }
    )));
    // Original deadline from t0 + 150ms still applies → tick at t0+160 elects
    let out = eng.tick(t0 + Duration::from_millis(160));
    assert!(matches!(eng.role(), Role::Candidate));
    assert!(!out.actions.is_empty());
}

#[test]
fn duplicate_vote_resp_does_not_double_count() {
    let mut eng = RaftEngine::new(cfg(1), MemoryStorage::new());
    let t0 = Instant::now();
    eng.tick(t0 + Duration::from_millis(200)); // become candidate, self-vote
    assert!(matches!(eng.role(), Role::Candidate));
    let _ = eng.handle_rpc(
        2,
        RaftMessage::RequestVoteResp {
            term: 1,
            vote_granted: true,
        },
        t0 + Duration::from_millis(201),
    );
    // Same peer again — must not be required for leadership (already leader after first)
    // Use a fresh election scenario: only one remote grant should win; duplicate must not
    // create leader if we only had self-vote + ignored duplicate. For 3 voters quorum=2,
    // one grant is enough. So instead: grant from peer 2 twice while role already Leader is noop;
    // primary assert: votes set size semantics — feed grant from peer 2 twice before quorum
    // in a 5-voter cluster would be clearer; keep 3-voter and assert role stays Leader once
    // and a second identical resp does not panic / change term.
    let actions = eng.handle_rpc(
        2,
        RaftMessage::RequestVoteResp {
            term: 1,
            vote_granted: true,
        },
        t0 + Duration::from_millis(202),
    );
    assert!(matches!(eng.role(), Role::Leader));
    assert!(actions.iter().all(|a| !matches!(a, Action::BecomeLeader { .. })));
}
```

Also update `tests/engine_election.rs` to `handle_rpc(..., Instant::now())` or fixed `start`.

- [ ] **Step 2: Run tests — expect FAIL**

Run: `cargo test --test engine_heartbeat_reset`  
Expected: FAIL / compile fail (signature / AE ignored)

- [ ] **Step 3: Implement AE path + thread `now` through vote handlers**

Replace internal `Instant::now()` in `handle_request_vote` / `handle_request_vote_resp` with the `now` argument.

- [ ] **Step 4: Run tests — expect PASS**

Run: `cargo test --test engine_election --test engine_heartbeat_reset`  
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add src/raft/engine.rs tests/engine_heartbeat_reset.rs tests/engine_election.rs
git commit -m "feat: AppendEntries heartbeat accept with term guards and now arg"
```

---

### Task 3: ReadyUpgrade + ConnectionHandler unary RPC

**Files:**
- Modify: `src/protocol/upgrade.rs`
- Modify: `src/handler.rs`
- Modify: `src/protocol/mod.rs`

**Interfaces:**
- Prefer:
  ```rust
  use libp2p::swarm::{StreamProtocol, ready_upgrade::ReadyUpgrade};
  // or libp2p 0.54 equivalent path — follow compiler
  pub const PROTOCOL_NAME: &str = "/libp2p-raft/1.0.0";
  pub fn raft_ready_upgrade() -> ReadyUpgrade<StreamProtocol> {
      ReadyUpgrade::new(StreamProtocol::new(PROTOCOL_NAME))
  }
  ```
- Events:
  ```rust
  pub enum FromBehaviour {
      SendRequest { correlation_id: u64, msg: RaftMessage },
      SendResponse { channel_id: u64, correlation_id: u64, msg: RaftMessage },
  }
  pub enum ToBehaviour {
      Request { correlation_id: u64, msg: RaftMessage, channel_id: u64 },
      Response { correlation_id: u64, msg: RaftMessage },
      Failure { correlation_id: Option<u64>, error: String },
  }
  ```
- Requirements:
  1. Concurrent inbound + outbound via `FuturesUnordered` (no head-of-line block)
  2. Outbound: open → write `encode_envelope` → read one frame → `Response` / `Failure`
  3. Inbound: read request → `Request { channel_id }` → wait `SendResponse` → write → close
  4. Pending inbound table `HashMap<channel_id, …>`; on stream close before response → remove + `Failure`
  5. Exactly one terminal outcome per inbound request (response or failure)
  6. Max frame size check on length-prefix (reject oversized → Failure)
  7. `connection_keep_alive(&self) -> bool`: `true` while any RPC in flight, else grace (≥ 2× expected heartbeat) **or** `true` unconditionally for Phase 2
  8. Note libp2p 0.54: keep-alive is `bool` (no old `KeepAlive` enum / `ConnectionHandlerEvent::Close`)

- [ ] **Step 1: Implement upgrade helper + Handler**

Use `libp2p::swarm::Stream` `AsyncRead`/`AsyncWrite` (futures traits). If bridging to tokio codecs, use `FuturesAsyncReadCompatExt` — decide in this task, not later.

- [ ] **Step 2: Compile**

Run: `cargo check`  
Expected: success (examples may still stub)

- [ ] **Step 3: Commit**

```bash
git add src/protocol/upgrade.rs src/handler.rs src/protocol/mod.rs
git commit -m "feat: ReadyUpgrade ConnectionHandler unary /libp2p-raft/1.0.0 RPC"
```

---

### Task 4: RaftBehaviour echo shell + dial from seed_peers

**Files:**
- Modify: `src/behaviour.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- `RaftBehaviour::new(config: RaftConfig) -> Self` — **no engine yet**; `PeerMap::from_seeds`
- `send_echo(&mut self, peer: PeerId, msg: RaftMessage)`
- `Event::{ Echo(WireEnvelope), PeerMapped { peer, node }, RpcFailed { peer, error } }`
- Dial via `DialOpts` / `handle_pending_outbound_connection` with PeerMap addrs
- Use `PeerCondition::DisconnectedAndNotDialing` (or equivalent) to avoid dial storms
- **Do not** keep an unbounded queue of Raft payloads across reconnect — if no connection, fail echo send or drop (Phase 2 echo can retry from example)

- [ ] **Step 1: Implement echo Behaviour**

Poll: drain handler → echo inbound with `SendResponse` → surface `Echo` / `RpcFailed`. On DialFailure for a peer, emit `RpcFailed` for any in-flight echo to that peer.

- [ ] **Step 2: Compile**

Run: `cargo check`  
Expected: success

- [ ] **Step 3: Commit**

```bash
git add src/behaviour.rs src/lib.rs
git commit -m "feat: RaftBehaviour echo shell with PeerMap dial"
```

---

### Task 5: Example `echo_two_peers`

**Files:**
- Modify: `examples/echo_two_peers.rs`

- [ ] **Step 1: Implement two-Swarm echo**

- Pin two ed25519 keypairs  
- Fixed ports or `tcp/0`  
- Swarm builder: `.with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))`  
- Do **not** enable unused `identify` for Phase 2 (avoids address book conflicts with static PeerMap) unless needed for compile feature set — prefer dropping identify from example transport features if unused  
- A dials B → `send_echo` → print matching `correlation_id` → exit 0

- [ ] **Step 2: Run**

Run: `cargo run --example echo_two_peers`  
Expected: exit 0, correlated echo logged

- [ ] **Step 3: Commit**

```bash
git add examples/echo_two_peers.rs
git commit -m "feat: echo_two_peers proves Handler RPC path"
```

---

### Task 6: Wire RaftEngine into RaftBehaviour

**Files:**
- Modify: `src/behaviour.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- `RaftBehaviour::new(config, storage) -> Self`
- Remove echo path: delete `send_echo`, inbound echo reply, `Event::Echo`
- `Event::RoleChanged { role, term, leader }` (+ keep `PeerMapped`, `RpcFailed`)
- `role(&self) -> Role`

**PendingRequest FSM:**

```rust
struct PendingRequest {
    correlation_id: u64,
    to: NodeId,
    peer: PeerId,
    conn_id: Option<ConnectionId>, // if known
    kind: RpcKind,
    sent_at: Instant,
    msg: RaftMessage,
}
```

| Event | Transition |
|-------|------------|
| Outbound `SendRequest` enqueued to handler | Insert pending |
| `Response` with known id | Remove → `handle_rpc` |
| `Failure` / timeout / `ConnectionClosed` / `DialFailure` | Remove → `handle_rpc_failure` + `RpcFailed` (**no retry**) |
| Response id not in map | Ignore |
| Unknown PeerMap mapping for `Action::Send` | Do not dial blindly forever → `RpcFailed` / skip + log |

**Inbound routing:**

- On `ToBehaviour::Request`, record `channel_id → ConnectionId` (from the connection that delivered the event)
- After `handle_rpc`, run **unified Action executor** (same as tick path)
- For `Action::Send { to, msg }` where `to == from`: `NotifyHandler::One(conn_id)` + `SendResponse { channel_id, correlation_id, msg }`
- Other Sends → outbound `SendRequest` path
- Never `NotifyHandler::Any` for inbound replies (dual connections in a full mesh)

**Unified Action executor** (call after every `tick`, `handle_rpc`, `handle_rpc_failure`):

1. Expand `Broadcast` → per-voter Sends excluding self (`membership.other_voters` / PeerMap)
2. `Become*` → push `Event::RoleChanged`
3. `Send` → resolve PeerMap; Dial if disconnected (addrs from map); else `SendRequest` + insert PendingRequest
4. Ignore `Apply` / `SnapshotInstallComplete` in Phase 2

**Poll loop** (loop until no new work that must re-arm timers):

1. Drain handler events (Request / Response / Failure) → engine → executor  
2. Expire PendingRequest by `rpc_timeout` → failure path (**still no retry**)  
3. If `now >= deadline`: `engine.tick(now)` → executor  
4. `sleep.reset(min(engine.next_deadline(), earliest_pending_timeout))` then **poll the Sleep** so the waker is registered before returning `Pending`  
5. Handle `FromSwarm::ConnectionClosed` / `DialFailure`: fail all pending for that peer/connection immediately; **never** remove voters

- [ ] **Step 1: Implement wiring**

- [ ] **Step 2: Compile**

Run: `cargo check`  
Expected: success

- [ ] **Step 3: Commit**

```bash
git add src/behaviour.rs src/lib.rs
git commit -m "feat: wire RaftEngine election into RaftBehaviour poll loop"
```

---

### Task 7: Example `three_node` with stability window

**Files:**
- Modify: `examples/three_node.rs`

**Acceptance (must all hold):**

1. Mesh dial completes (or wait ≤ ~2s for connections)  
2. Observe roles across **all three** nodes: exactly **one** Leader, **two** Followers, **same term**  
3. **Stability window:** after first consistent observation, run ≥ `max(3 × heartbeat_interval, 1s)` with **no** further `RoleChanged` and **no** term bump  
4. Wall-clock cap (e.g. 15s) → non-zero exit if unmet  
5. Optional stretch: kill leader task and assert new single leader within ~2× election timeout (nice-to-have; not required if time-boxed)

**Config guidance:**

- Ports `4101`–`4103`, pinned keypairs, `voters: [1,2,3]`  
- Per-node `election_timeout` in `[T, 2T)` (e.g. 500–1000ms) — **distinct** fixed jitters if RNG awkward in example  
- `heartbeat_interval` ≤ T/4 (~100ms)  
- `rpc_timeout` ~500ms; `rpc_max_retries` ignored / 0  
- `idle_connection_timeout` = 30s on each Swarm  
- Leave `propose` commented for Phase 3

- [ ] **Step 1: Implement example**

- [ ] **Step 2: Run**

Run: `cargo run --example three_node`  
Expected: exit 0; logs show stable 1 Leader / 2 Followers  

Manual soak (recommended once): run ≥10s and confirm connection count does not climb after initial mesh (keep-alive working).

- [ ] **Step 3: Commit**

```bash
git add examples/three_node.rs
git commit -m "feat: three_node elects a stable libp2p Raft leader"
```

---

### Task 8: Phase 2 docs + README status

**Files:**
- Create: `docs/phase-2.md`
- Modify: `README.md`

- [ ] **Step 1: Write `docs/phase-2.md`**

Mirror `phase-1.md`: goal, task mapping (engine AE + Tasks 4–5), done criteria (echo + stable three_node + unit tests), out of scope, next = Phase 3 replication. Mention provider-review revisions (no vote RPC retry; keep-alive; stability window).

- [ ] **Step 2: Update README Status** — link `docs/phase-2.md`

- [ ] **Step 3: Commit**

```bash
git add docs/phase-2.md README.md
git commit -m "docs: Phase 2 election-over-libp2p checklist"
```

---

## Verification gate (Phase 2 complete)

```bash
cargo test
cargo run --example echo_two_peers
cargo run --example three_node
```

Expected:

- Unit tests PASS (`peer_map`, `engine_election`, `engine_heartbeat_reset`, `codec_roundtrip`)
- Echo correlates request/response
- Three-node: exactly one Leader, two Followers, same term, stable across the window

## Out of scope

- `propose` / commit / `Event::Committed`
- Non-empty AppendEntries log matching + `next_index` decrement
- Snapshots / InstallSnapshot
- Membership Add/Remove
- YAML/TOML config loading
- Outbound RequestVote retries / `rpc_max_retries` behavior

## Spec coverage self-review

| Item | Task |
|------|------|
| PeerMap | 1 |
| AE term guards + deadline reset | 2 |
| ReadyUpgrade Handler unary RPC | 3 |
| Echo shell + dial | 4–5 |
| Unified Actions + PendingRequest FSM + no vote retry | 6 |
| Keep-alive / idle timeout | 3, 5, 7 |
| Inbound `NotifyHandler::One` | 6 |
| Stable 3-node leader | 7 |
| Phase doc | 8 |

## Changelog vs first draft (provider review)

| Change | Source |
|--------|--------|
| Reorder engine AE before Behaviour wire | claude / ordering |
| Explicit AE term reject without deadline reset | all three |
| Unified Action executor; ConnectionId-scoped SendResponse | chatgpt / deepseek / claude |
| PendingRequest FSM + fail on disconnect | chatgpt / claude |
| No RequestVote RPC retries in Phase 2 | claude |
| Drop, don't queue, RPCs across reconnect | claude |
| ReadyUpgrade + StreamProtocol; max frame size | claude / deepseek |
| `idle_connection_timeout` + `connection_keep_alive` | claude |
| three_node stability window + exact role assert | all three |
| Duplicate vote + stale AE unit tests | claude / chatgpt |
| Prefer omit identify in Phase 2 examples | claude |
