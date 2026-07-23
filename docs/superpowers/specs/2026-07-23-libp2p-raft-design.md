# libp2p-raft Design Spec

**Date:** 2026-07-23  
**Status:** Approved for planning (patched after 3-round provider review)  
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
| Replication reject handling | **Simple `next_index` decrement** (not conflict-index hints) |
| Tick scheduling | **Deadline Sleep** reset on events; call `tick(now)` only when due |
| Membership changes | **Add OR Remove exactly one node** at a time; reject while pending |

### Out of scope (v1)
- OpenRaft / wrapping external Raft crates
- RocksDB / sled / redb backends
- Gossipsub for Raft RPCs
- Kademlia-based discovery as a hard dependency
- Joint consensus (multi-phase membership)
- Pipelined AppendEntries / production metrics
- Snapshot mid-transfer resume (restart from offset 0 on failure)
- Sophisticated outbound drop policies / conflict-index optimization

## 2. Architecture overview

```
Swarm
 └── RaftBehaviour
      ├── RaftEngine        (election, log, snapshot, membership — pure sync SM)
      ├── MemoryStorage     (implements Storage)
      ├── PeerMap           (NodeId ↔ PeerId + seed Multiaddrs)
      ├── PendingRequests   (correlation_id → in-flight RPC)
      └── ConnectionHandler (custom stream /libp2p-raft/1.0.0)
```

### Layer responsibilities

| Layer | Owns | Does NOT own |
|--------|------|----------------|
| `RaftEngine` | term, vote, log, commit, role, membership, logical deadlines | `PeerId`, streams, dial, correlation IDs |
| `RaftBehaviour` | engine, peer map, pending RPCs, outbound queue, Action → NotifyHandler, Sleep/deadline wake | Raft algorithm details |
| `ConnectionHandler` | substream open / framed read-write / close | elections, log semantics |
| `Storage` | hard state, log, snapshot bytes (atomic batch writes) | networking |

### Node identity & bootstrap
- Stable `NodeId = u64` in the membership config
- `PeerMap` maps `NodeId` ↔ `PeerId`
- libp2p keypairs must be pinned (no regenerate); PeerId drift breaks identity
- **Static bootstrap** via config (required for 3-node demo):

```rust
pub struct SeedPeer {
    pub node_id: NodeId,
    pub peer_id: PeerId,
    pub addrs: Vec<Multiaddr>,
}

pub struct RaftConfig {
    pub node_id: NodeId,
    pub seed_peers: Vec<SeedPeer>,
    pub election_timeout: Duration,   // base; jitter applied in engine
    pub heartbeat_interval: Duration,
    pub rpc_timeout: Duration,
    pub rpc_max_retries: u32,         // e.g. 1 then surface failure
    pub snapshot_threshold: u64,      // log length before compact
    // ...
}
```

On startup, Behaviour dials each seed `Multiaddr`, stores mappings, and must have membership NodeIds configured **before** the first election timer fires. Unreachable seeds: log + continue.

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
    ├── engine_replication.rs
    └── cluster_smoke.rs
```

## 4. Protocol

### Protocol ID
`/libp2p-raft/1.0.0`

### Framing
- Each message: `u32` big-endian length + `bincode` (serde) payload
- One connection may open many substreams
- Default RPC model: one substream = one request/response pair (unary)
- Snapshots: **sequential unary RPCs** — each chunk is its own request/response with increasing `offset`; final chunk has `done = true`
- On snapshot transfer failure: **restart from offset 0** (no resume)

### Correlation
Every outbound request carries `correlation_id: u64` (Behaviour-assigned). Responses echo the same id. Handler/Behaviour match responses to `PendingRequest` before forwarding payload to the engine. Engine messages themselves may omit correlation; Behaviour wraps wire envelope:

```rust
pub struct WireEnvelope {
    pub correlation_id: u64,
    pub msg: RaftMessage,
}
```

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

Membership changes are carried as special **log entries** (`EntryType::Config`), not as a separate critical-path RPC.

### Snapshot streaming
Leader sends multiple `InstallSnapshot` messages with increasing `offset`; final chunk has `done = true`. Follower ACKs each chunk with `InstallSnapshotResp`. Failure → restart from offset 0.

### Non-goals for protocol
- No Gossipsub for Raft RPCs
- Discovery (if added later) stays outside this protocol

## 5. RaftEngine API

Engine is a **synchronous, pure** state machine: no async, no libp2p types. Behaviour calls it from `poll()` when deadlines fire or RPCs arrive.

```rust
pub struct RaftEngine<S: Storage> { /* ... */ }

impl<S: Storage> RaftEngine<S> {
    /// Drive timers. Returns actions + next absolute wake deadline.
    pub fn tick(&mut self, now: Instant) -> TickOutcome;

    pub fn handle_rpc(&mut self, from: NodeId, msg: RaftMessage) -> Vec<Action>;

    /// Notify engine that an outbound RPC timed out / failed after retries.
    /// Engine must ignore stale results after term change.
    pub fn handle_rpc_failure(&mut self, to: NodeId, kind: RpcKind) -> Vec<Action>;

    pub fn propose(&mut self, data: Vec<u8>) -> Result<Index, RaftError>;
    pub fn propose_membership(&mut self, change: MembershipChange) -> Result<Index, RaftError>;
    pub fn apply_ready(&mut self) -> Vec<LogEntry>;

    pub fn next_deadline(&self) -> Instant;
}

pub struct TickOutcome {
    pub actions: Vec<Action>,
    pub next_deadline: Instant,
}

pub enum MembershipChange {
    AddNode(NodeId),
    RemoveNode(NodeId),
}

pub enum RpcKind {
    RequestVote,
    AppendEntries,
    InstallSnapshot,
}
```

### Actions (engine → behaviour)

```rust
pub enum Action {
    /// Send to one NodeId. Behaviour expands Broadcast into multiple Send.
    Send { to: NodeId, msg: RaftMessage },
    /// Semantics: send to all voting members except self (sequential unary RPCs).
    Broadcast { msg: RaftMessage },
    Apply { entries: Vec<LogEntry> },
    BecomeLeader { term: Term },
    BecomeFollower { term: Term, leader: Option<NodeId> },
    BecomeCandidate { term: Term },
    SnapshotInstallComplete { index: Index },
}
```

### Timers
- Election timeout (follower/candidate) with jitter — tracked as absolute deadlines inside engine
- Heartbeat interval (leader)
- `tick(now)` only when `now >= next_deadline`
- Behaviour owns a `Sleep`/`Delay` reset to `engine.next_deadline()` after every state-affecting event

### Roles
Follower → Candidate → Leader (standard Raft).

### Replication (leader)
- Per-follower `next_index` and `match_index`
- On become leader: `next_index[peer] = last_log_index + 1`, `match_index[peer] = 0`
- On `AppendEntriesResp { success: false }`: **simple decrement**  
  `next_index[peer] = max(1, min(next_index[peer] - 1, match_index[peer] + 1))` then retry
- On success: advance `match_index` / `next_index`; commit when majority has `match_index >= N` and entry term == current term
- Always validate inbound RPC `term` before applying; ignore stale responses after term change

### Membership (basic)
- Config = set of voting `NodeId`s
- Change = single log entry `EntryType::Config` for **exactly one** `AddNode` or `RemoveNode`
- Reject `propose_membership` while `pending_change.is_some()` (prior config entry not yet committed)
- **Not joint consensus** — unsafe for arbitrary multi-node swaps; learning limitation documented
- Leader replicates only to nodes in the current config

### Snapshot (MVP)
- When log length exceeds `snapshot_threshold`, leader creates snapshot `{last_index, last_term, conf, state_blob}`, stores via `Storage`, truncates prefix of log
- Lagging followers receive chunked `InstallSnapshot` instead of full replay
- Mid-transfer failure: restart from offset 0

### Storage trait (atomic writes)

```rust
trait Storage {
    fn hard_state(&self) -> HardState;
    fn entry(&self, index: Index) -> Option<LogEntry>;
    fn last_index_term(&self) -> (Index, Term);
    fn truncate_from(&mut self, index: Index);
    fn snapshot(&self) -> Option<Snapshot>;
    fn install_snapshot(&mut self, snap: Snapshot);

    /// Single atomic batch: append entries and/or update hard state together.
    /// Engine MUST use this before granting a vote or responding to AppendEntries
    /// that advances durable state. No separate append-then-save_hard_state sequence.
    fn persist(
        &mut self,
        hard_state: Option<HardState>,
        entries: &[LogEntry],
    ) -> Result<(), StorageError>;
}
```

`MemoryStorage` implements atomicity as a single in-memory state swap (trivial but teaches the contract).

## 6. Behaviour ↔ Handler ↔ Swarm

### Event flow

```
App / Swarm
    │ propose / dial / consume Event
    ▼
RaftBehaviour
    │ owns Sleep → wake at next_deadline
    │ poll():
    │   1. drain Handler → ToBehaviour (match correlation_id)
    │   2. on RPC timeout: retry up to rpc_max_retries, else engine.handle_rpc_failure
    │   3. if now >= deadline: engine.tick(now)
    │   4. Actions → NotifyHandler / Dial / ToSwarm events
    │   5. reset Sleep to engine.next_deadline()
    ▼
ConnectionHandler
    │ /libp2p-raft/1.0.0 framed I/O (multiple concurrent substreams OK)
```

### RPC lifecycle (Behaviour)

```rust
struct PendingRequest {
    correlation_id: u64,
    to: NodeId,
    peer: PeerId,
    kind: RpcKind,
    sent_at: Instant,
    attempts: u32,
    // payload retained for retry if needed
}
```

- Assign `correlation_id` on each outbound `Send`
- On response: match id → drop pending → `engine.handle_rpc`
- On timeout: if `attempts < rpc_max_retries` resend; else `engine.handle_rpc_failure` and emit `RpcFailed`
- Engine ignores results whose `term` is stale relative to current term

### Handler ↔ Behaviour events

| Direction | Event | Meaning |
|-----------|--------|---------|
| Behaviour → Handler | `SendRequest { correlation_id, msg }` | open outbound substream, write envelope, await one response |
| Behaviour → Handler | `SendResponse { channel_id, correlation_id, msg }` | answer an open inbound request |
| Handler → Behaviour | `Request { peer, correlation_id, msg, channel_id }` | inbound RPC for engine |
| Handler → Behaviour | `Response { peer, correlation_id, msg }` | outbound RPC completed |
| Handler → Behaviour | `Failure { peer, correlation_id, err }` | dial/stream/timeout — **not** Raft peer-dead |

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
- `Action::Broadcast` → expand to `Send` for each voting member except self
- No connection yet → `ToSwarm::Dial { peer_id, addrs from seed }` and queue until `ConnectionEstablished`
- Unknown mapping → drop + emit `RpcFailed` (explicit, not silent)

### Keepalive & failures
- Handler keeps connection alive while RPCs are pending
- Idle connections may close; election timers must **not** depend on connection up/down
- Connection drop is observability (`RpcFailed` / retry on Raft timers), not instant peer removal from membership

### Behaviour public API

```rust
impl RaftBehaviour {
    pub fn propose(&mut self, data: Vec<u8>) -> Result<Index, Error>;
    pub fn propose_membership(&mut self, change: MembershipChange) -> Result<Index, Error>;
    pub fn role(&self) -> Role;
    pub fn commit_index(&self) -> Index;
}
```

## 7. Implementation phases

| Phase | Deliverable |
|-------|-------------|
| 0 | Crate scaffold: Behaviour + Handler + protocol upgrade + correlated echo RPC between 2 peers |
| 1 | `RaftEngine` election-only + `MemoryStorage.persist`; pure unit tests (no Swarm) |
| 2 | Wire RequestVote + seed_peers dial; 3-node elects a leader (`examples/three_node`) |
| 3 | AppendEntries + next_index decrement + commit + `propose` |
| 4 | Snapshot compaction + chunked InstallSnapshot (restart offset 0 on fail) |
| 5 | Membership Add/Remove one-at-a-time + pending_change gate + apply/notify |

## 8. Testing strategy

- **Engine unit tests (primary):** election timeout → candidate; majority → leader; log mismatch reject + next_index decrement; commit advance; snapshot truncate; membership add/remove one + reject while pending
- **Codec tests:** length-delimited bincode + correlation envelope round-trip
- **Behaviour unit-ish:** PendingRequest timeout → retry → failure
- **Integration (later):** TCP Swarm, 3 nodes — smoke election + one propose

## 9. Pitfalls

1. Election timeout must exceed p99 dial + stream setup latency on libp2p
2. Connection drop ≠ Raft failure
3. Pin keypairs; PeerId must stay stable
4. Never block `poll()` on heavy I/O (memory storage keeps this easy)
5. Single add/remove membership only — document limitation vs joint consensus
6. Always chunk snapshots; restart offset 0 on failure
7. Stale RPC after term change must be ignored (validate term on every inbound result)
8. Do not busy-poll `tick` every `poll()` — wake on deadline Sleep

## 10. Stack

- rust-libp2p (tokio features), serde + bincode, futures, thiserror, tracing
- Edition 2021, stable Rust

## 11. Provider consensus (design research)

### Initial architecture review
Multi-provider review (ChatGPT, Claude, DeepSeek; Gemini timeout; Kimi incomplete) agreed on:

- Prefer a thin networking adapter around a consensus core
- Do not use Gossipsub for Raft RPCs
- Separate PeerId connectivity from Raft membership/voting
- Keep a pure `RaftEngine` owned by Behaviour

### 3-round spec review (2026-07-23)
| Round | Outcome |
|-------|---------|
| 1 | Scores ~7–8.8; unanimous Approve with changes |
| 2 | Ranked blockers; top patches for storage, replication, RPC lifecycle, bootstrap, tick, membership |
| 3 | Unanimous **Approve after applying 6 patches**; Go for implementation plan |

Deviation from typical production advice: DIY Raft and Behaviour-owned engine (not OpenRaft + separate async task) — intentional for learning goals.

## 12. Success criteria

- Unit-testable Raft election/log/snapshot/membership without Swarm
- Custom `/libp2p-raft/1.0.0` stream + ConnectionHandler working end-to-end with correlation IDs
- Static `seed_peers` bootstrap; 3-node example elects a leader and commits at least one client proposal
- Atomic `Storage::persist`; simple next_index decrement on reject; deadline-driven tick
- Membership limited to one Add/Remove at a time with pending gate
- Clear module boundaries documented above remain intact
