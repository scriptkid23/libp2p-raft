# libp2p-raft Design Spec

**Date:** 2026-07-23  
**Status:** Approved for planning  
**Goal:** Learning / research crate — DIY mini-Raft as a libp2p `NetworkBehaviour` with a custom stream protocol and `ConnectionHandler`.

## 1. Intent & constraints

### Purpose
Build a Raft behaviour for rust-libp2p to learn:

- How `NetworkBehaviour` + `ConnectionHandler` interact
- How a consensus state machine maps onto libp2p I/O
- Full mini-Raft: leader election, log replication, snapshots, basic membership

### Explicit decisions
| Topic | Choice |
|--------|--------|
| Scope | Full mini-Raft (election + log + snapshot + basic membership) |
| Raft implementation | DIY (not OpenRaft / raft-rs) |
| Transport | Custom stream protocol + `ConnectionHandler` |
| Persistence | `Storage` trait + **in-memory** impl only for now |
| Where Raft lives | Inside `NetworkBehaviour` (timers via `poll()`), but as a **pure `RaftEngine` module** owned by the behaviour |
| Architecture | Approach 2: pure engine + behaviour adapter |

### Out of scope (v1)
- OpenRaft / wrapping external Raft crates
- RocksDB / sled / redb backends
- Gossipsub for Raft RPCs
- Kademlia-based discovery as a hard dependency
- Joint consensus (multi-phase membership)
- Pipelined AppendEntries / production metrics

## 2. Architecture overview

```
Swarm
 └── RaftBehaviour
      ├── RaftEngine        (election, log, snapshot, membership — pure sync SM)
      ├── MemoryStorage     (implements Storage)
      ├── PeerMap           (PeerId ↔ NodeId)
      └── ConnectionHandler (custom stream /libp2p-raft/1.0.0)
```

### Layer responsibilities

| Layer | Owns | Does NOT own |
|--------|------|----------------|
| `RaftEngine` | term, vote, log, commit, role, membership, logical timers | `PeerId`, streams, dial |
| `RaftBehaviour` | engine, peer map, outbound queue, Action → NotifyHandler | Raft algorithm details |
| `ConnectionHandler` | substream open / framed read-write / close | elections, log semantics |
| `Storage` | hard state, log, snapshot bytes | networking |

### Node identity
- Stable `NodeId = u64` in the membership config
- `PeerMap` maps `NodeId` ↔ `PeerId`
- libp2p keypairs must be pinned (no regenerate); PeerId drift breaks identity

## 3. Crate layout

```
libp2p-raft/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── behaviour.rs
│   ├── handler.rs
│   ├── protocol/
│   │   ├── mod.rs
│   │   ├── messages.rs
│   │   ├── codec.rs
│   │   └── upgrade.rs
│   ├── raft/
│   │   ├── mod.rs
│   │   ├── engine.rs
│   │   ├── types.rs
│   │   ├── log.rs
│   │   ├── snapshot.rs
│   │   └── membership.rs
│   ├── storage/
│   │   ├── mod.rs
│   │   └── memory.rs
│   ├── peer_map.rs
│   ├── config.rs
│   └── error.rs
├── examples/
│   └── three_node.rs
└── tests/
    ├── engine_election.rs
    └── cluster_smoke.rs
```

## 4. Protocol

### Protocol ID
`/libp2p-raft/1.0.0`

### Framing
- Each message: `u32` big-endian length + `bincode` (serde) payload
- One connection may open many substreams
- Default RPC model: one substream = one request/response pair (unary)
- Snapshots: **sequential unary RPCs** — each chunk is its own request/response with increasing `offset`; final chunk has `done = true` (keeps Handler model uniform; no long-lived multi-message stream in MVP)

### Wire messages

```
RaftMessage
├── RequestVote { term, candidate_id, last_log_index, last_log_term }
├── RequestVoteResp { term, vote_granted }
├── AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries[], leader_commit }
├── AppendEntriesResp { term, success, match_index }
├── InstallSnapshot { term, leader_id, last_included_index, last_included_term, offset, data, done }
├── InstallSnapshotResp { term }
└── Heartbeat = AppendEntries with entries = []
```

Membership changes are carried as special **log entries** (`EntryType::Config`), not as a separate critical-path RPC (optional helper messages may exist later but are not required for MVP correctness).

### Snapshot streaming
Leader sends multiple `InstallSnapshot` messages with increasing `offset`; final chunk has `done = true`. Follower ACKs each chunk with `InstallSnapshotResp`.

### Non-goals for protocol
- No Gossipsub for Raft RPCs
- Discovery (if added later) stays outside this protocol

## 5. RaftEngine API

Engine is a **synchronous, pure** state machine: no async, no libp2p types. Behaviour calls it from `poll()`.

```rust
pub struct RaftEngine<S: Storage> { /* ... */ }

impl<S: Storage> RaftEngine<S> {
    pub fn tick(&mut self, now: Instant) -> Vec<Action>;
    pub fn handle_rpc(&mut self, from: NodeId, msg: RaftMessage) -> Vec<Action>;
    pub fn propose(&mut self, data: Vec<u8>) -> Result<Index, RaftError>;
    pub fn propose_membership(&mut self, cfg: Membership) -> Result<Index, RaftError>;
    pub fn apply_ready(&mut self) -> Vec<LogEntry>;
}
```

### Actions (engine → behaviour)

```rust
pub enum Action {
    Send { to: NodeId, msg: RaftMessage },
    Broadcast { msg: RaftMessage },
    Apply { entries: Vec<LogEntry> },
    BecomeLeader { term: Term },
    BecomeFollower { term: Term, leader: Option<NodeId> },
    BecomeCandidate { term: Term },
    SnapshotInstallComplete { index: Index },
}
```

### Timers
- Election timeout (follower/candidate) with jitter
- Heartbeat interval (leader)
- `tick(now)` compares deadlines and emits send/role-change actions

### Roles
Follower → Candidate → Leader (standard Raft).

### Membership (basic)
- Config = set of voting `NodeId`s
- Change = single log entry `EntryType::Config(Membership)`
- **MVP uses single-step config change** (not joint consensus)
- Document this as a learning limitation; unsafe under concurrent membership changes in production sense
- Leader replicates only to nodes in the current config

### Snapshot (MVP)
- When log length exceeds a configured threshold, leader creates snapshot `{last_index, last_term, conf, state_blob}`, stores via `Storage`, truncates prefix of log
- Lagging followers receive chunked `InstallSnapshot` instead of full replay

### Storage trait

```rust
trait Storage {
    fn hard_state(&self) -> HardState;
    fn set_hard_state(&mut self, hs: HardState);
    fn append(&mut self, entries: &[LogEntry]);
    fn truncate_from(&mut self, index: Index);
    fn entry(&self, index: Index) -> Option<LogEntry>;
    fn last_index_term(&self) -> (Index, Term);
    fn snapshot(&self) -> Option<Snapshot>;
    fn install_snapshot(&mut self, snap: Snapshot);
}
```

Initial implementation: `MemoryStorage` only.

## 6. Behaviour ↔ Handler ↔ Swarm

### Event flow

```
App / Swarm
    │ propose / dial / consume Event
    ▼
RaftBehaviour
    │ poll():
    │   1. drain Handler → ToBehaviour
    │   2. engine.handle_rpc / engine.tick → Actions
    │   3. Actions → NotifyHandler / ToSwarm
    ▼
ConnectionHandler
    │ /libp2p-raft/1.0.0 framed I/O
```

### Handler ↔ Behaviour events

| Direction | Event | Meaning |
|-----------|--------|---------|
| Behaviour → Handler | `SendRequest(RaftMessage)` | open outbound substream, write request, await one response |
| Behaviour → Handler | `SendResponse(RaftMessage)` | answer an open inbound request |
| Handler → Behaviour | `Request { peer, msg, channel_id }` | inbound RPC for engine |
| Handler → Behaviour | `Response { peer, msg }` | outbound RPC completed |
| Handler → Behaviour | `Failure { peer, err }` | dial/stream/timeout — **not** Raft peer-dead |

### Public Swarm events

```rust
pub enum Event {
    RoleChanged { role: Role, term: Term, leader: Option<NodeId> },
    Committed { entries: Vec<LogEntry> },
    MembershipChanged { members: HashSet<NodeId> },
    SnapshotInstalled { index: Index },
    PeerMapped { peer: PeerId, node: NodeId },
    RpcFailed { peer: PeerId, error: Error },
}
```

### Peer routing
- `Action::Send { to, .. }` → `PeerMap` → `PeerId` → `NotifyHandler` on that connection
- No connection yet → `ToSwarm::Dial` and/or queue until `ConnectionEstablished`
- Unknown mapping → drop + `RpcFailed` (explicit, not silent)

### Keepalive & failures
- Handler keeps connection alive while RPCs are pending
- Idle connections may close; election timers must **not** depend on connection up/down
- Connection drop is observability (`RpcFailed` / retry on Raft timers), not instant peer removal from membership

### Behaviour public API

```rust
impl RaftBehaviour {
    pub fn propose(&mut self, data: Vec<u8>) -> Result<Index, Error>;
    pub fn propose_membership(&mut self, nodes: HashSet<NodeId>) -> Result<Index, Error>;
    pub fn role(&self) -> Role;
    pub fn commit_index(&self) -> Index;
}
```

## 7. Implementation phases

| Phase | Deliverable |
|-------|-------------|
| 0 | Crate scaffold: Behaviour + Handler + protocol upgrade + echo RPC between 2 peers |
| 1 | `RaftEngine` election-only + `MemoryStorage` hard state; pure unit tests (no Swarm) |
| 2 | Wire RequestVote through Behaviour; 3-node elects a leader (`examples/three_node`) |
| 3 | AppendEntries + log + commit + `propose`; replicate client commands |
| 4 | Snapshot compaction + chunked InstallSnapshot |
| 5 | Basic membership (single-step config entry) + apply/notify |

## 8. Testing strategy

- **Engine unit tests (primary):** election timeout → candidate; majority → leader; log mismatch reject; commit advance; snapshot truncate; membership apply
- **Codec tests:** length-delimited bincode round-trip
- **Integration (later):** in-memory or TCP Swarm, 3 nodes — smoke election + one propose

## 9. Pitfalls

1. Election timeout must exceed p99 dial + stream setup latency on libp2p
2. Connection drop ≠ Raft failure
3. Pin keypairs; PeerId must stay stable
4. Never block `poll()` on heavy I/O (memory storage keeps this easy)
5. Single-step membership is a learning simplification — document the limitation
6. Always chunk snapshots; avoid unbounded Handler queues

## 10. Stack

- rust-libp2p (tokio features), serde + bincode, futures, thiserror, tracing
- Edition 2021, stable Rust

## 11. Provider consensus (design research)

Multi-provider review (ChatGPT, Claude, DeepSeek; Gemini timeout; Kimi incomplete) agreed on:

- Prefer a thin networking adapter around a consensus core
- Do not use Gossipsub for Raft RPCs
- Separate PeerId connectivity from Raft membership/voting
- For this learning project: keep a pure `RaftEngine` owned by Behaviour rather than burying algorithm code inside `behaviour.rs`

Deviation from typical production advice: DIY Raft and Behaviour-owned engine (not OpenRaft + separate async task) — intentional for learning goals.

## 12. Success criteria

- Unit-testable Raft election/log/snapshot/membership without Swarm
- Custom `/libp2p-raft/1.0.0` stream + ConnectionHandler working end-to-end
- 3-node example elects a leader and commits at least one client proposal
- Clear module boundaries documented above remain intact
```
