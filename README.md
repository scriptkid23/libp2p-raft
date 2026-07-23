# libp2p-raft

Learning / research crate: a **DIY mini-Raft** exposed as a rust-libp2p **`NetworkBehaviour`**, with a **custom** `ConnectionHandler` and stream protocol `/libp2p-raft/1.0.0`.

> **Not production Raft.** Intentional simplifications: in-memory storage (no disk durability), single-step membership (add/remove one node), simple `next_index` decrement, no OpenRaft.

**Why a custom `ConnectionHandler` instead of `libp2p::request_response`?**  
Learning goal: understand Behaviour ↔ Handler lifecycle, substream framing, and unary RPC plumbing end-to-end. Production code would often compose `request_response`; we hand-roll it on purpose.

**Docs**
- Design spec: [`docs/superpowers/specs/2026-07-23-libp2p-raft-design.md`](docs/superpowers/specs/2026-07-23-libp2p-raft-design.md)
- Implementation plan: [`docs/superpowers/plans/2026-07-23-libp2p-raft.md`](docs/superpowers/plans/2026-07-23-libp2p-raft.md)

---

## Goals

- Learn how `NetworkBehaviour` + `ConnectionHandler` interact under a Swarm
- Map a pure consensus state machine onto libp2p I/O
- Cover mini-Raft: **election (RequestVote) → log replication (AppendEntries) → snapshot → basic membership**

---

## Architecture

### Glossary (libp2p newcomers)

| Term | Meaning here |
|------|----------------|
| **Swarm** | libp2p event loop: dials, accepts connections, polls behaviours |
| **PeerId** | libp2p identity from a **pinned keypair** (do not regenerate) |
| **NodeId** | Raft identity (`u64`), stable in membership config |
| **Connection** | Encrypted multiplexed link Swarm manages between two PeerIds |
| **ConnectionHandler** | Per-connection worker: opens `/libp2p-raft/1.0.0` substreams, reads/writes bytes |
| **NetworkBehaviour** | Swarm-facing logic: owns handlers' commands, emits dials & app events |
| **Unary RPC** | One substream = one request + one response, then close (not a long-lived bi-di session) |
| **Action** | Pure output from `RaftEngine` (e.g. `Send`); Behaviour translates to Dial / NotifyHandler |

### Big picture

```text
┌──────────────────────────────────────────────────────────────┐
│ App / examples/three_node.rs                                 │
│  SwarmBuilder (TCP+Noise+Yamux) · listen · dial · event loop │
│  propose() / consume Event                                   │
└────────────────────────────┬─────────────────────────────────┘
                             │ plugs Behaviour into
                             ▼
┌──────────────────────────────────────────────────────────────┐
│ Swarm                          ★ PROVIDED BY libp2p          │
│  polls Behaviour · performs Dial · manages connections       │
└────────────────────────────┬─────────────────────────────────┘
                             │ poll / ToSwarm / handler events
                             ▼
┌──────────────────────────────────────────────────────────────┐
│ RaftBehaviour                  ★ WE WRITE (behaviour.rs)     │
│  owns: RaftEngine, MemoryStorage, PeerMap,                   │
│        PendingRequests, poll-driven deadline timer state     │
│  maps: Action::Send → (ToSwarm::Dial if needed)              │
│                     → NotifyHandler(SendRequest)             │
│  matches responses by correlation_id                         │
└──────────────┬───────────────────────────────┬───────────────┘
               │ sync calls                    │ per connection
               ▼                               ▼
┌──────────────────────────┐    ┌──────────────────────────────┐
│ RaftEngine ★ WE WRITE    │    │ ConnectionHandler ★ WE WRITE │
│ (raft/engine.rs)         │    │ (handler.rs)                 │
│ pure sync SM             │    │ /libp2p-raft/1.0.0           │
│ NO PeerId / Dial / stream│    │ unary framed substreams      │
│ tick / handle_rpc        │    │ length + bincode             │
│ → Vec<Action>            │    └──────────────────────────────┘
└────────────┬─────────────┘
             │ persist()
             ▼
┌──────────────────────────┐
│ Storage ★ WE WRITE       │
│ MemoryStorage            │
│ persist() atomic *in     │
│ process* (not on disk)   │
└──────────────────────────┘
```

**Threading model:** single Swarm event loop. `RaftEngine` is called synchronously from `NetworkBehaviour::poll` — no background consensus thread. Engine is `Send`-friendly state, but not driven on its own task.

**Swarm ownership:** this crate does **not** build or own a Swarm. It only provides `RaftBehaviour` for apps to plug into `SwarmBuilder` (keeps the library transport-agnostic). Swarm construction lives in `examples/` (and your app).

### libp2p provides vs we write

| ★ Provided by **libp2p** | ★ We **write** |
|--------------------------|----------------|
| `Swarm` / `SwarmBuilder` | `RaftBehaviour` |
| TCP / Noise / Yamux | `ConnectionHandler` + `/libp2p-raft/1.0.0` |
| `dial` / `listen_on` / connection events | `WireEnvelope` + codec |
| `NetworkBehaviour` / `ConnectionHandler` **traits** | `RaftEngine` (DIY Raft) |
| Stream multiplexing | `Storage` / `MemoryStorage` |
| | `PeerMap`, `PendingRequest`, examples |

### Layer responsibilities

| Layer | Owns | Does **not** own |
|--------|------|------------------|
| App / Swarm | Transport, listen/dial loop, consuming events | Raft rules |
| RaftBehaviour | Engine, peer map, pending RPCs, deadline wake, Action → network | Election/log algorithm details |
| ConnectionHandler | Open/read/write/close framed substreams | Term, votes, commit index |
| RaftEngine | Term, role, log, membership, logical deadlines | `PeerId`, dial, streams |
| Storage | In-memory hard state + log + snapshot; `persist` is one in-process atomic update | Disk durability / networking |

### Sequence (propose → commit)

```text
App              Swarm         Behaviour         Engine          Handler         Peer
 │                │               │                │               │              │
 │ propose(data)  │               │                │               │              │
 │───────────────►│──────────────►│ handle propose │               │              │
 │                │               │───────────────►│               │              │
 │                │               │◄── Action::Send│               │              │
 │                │◄─ ToSwarm::Dial (if no conn) ─│               │              │
 │                │── connection ─►│               │               │              │
 │                │               │ NotifyHandler ─► open substream │              │
 │                │               │                │               │── WireEnv ──►│
 │                │               │                │               │◄─ response ──│
 │                │               │◄ correlation_id│               │              │
 │                │               │───────────────►│ handle_rpc    │              │
 │                │               │◄── Apply/…     │               │              │
 │◄── Event::Committed ───────────│                │               │              │
```

Same pattern for **election**: Sleep deadline → `engine.tick` → `Broadcast`/`Send` **RequestVote** → responses → majority → `BecomeLeader` → `Event::RoleChanged`. On leader failure, followers' election timeouts fire and a new election runs (standard Raft); connection loss alone does **not** remove a node from membership.

### Control flows (short)

1. **Timer** — Behaviour deadline timer fires → `engine.tick(now)` → Actions → Behaviour may request **Dial via `ToSwarm`** (engine never dials) → Handler unary RPC → match `correlation_id` → `handle_rpc`.
2. **Propose** — leader `persist` append → AppendEntries to followers → majority `match_index` → commit → `Event::Committed`.
3. **Inbound** — Handler reads request → `handle_rpc` → optional `SendResponse` on same unary RPC.
4. **Failure** — timeout → retry ≤ `rpc_max_retries` → `handle_rpc_failure` + `Event::RpcFailed`. **libp2p connection drop ≠ Raft membership remove**; membership only changes via `propose_membership`.

### Identity & bootstrap

- Raft **`NodeId = u64`**
- Each NodeId is statically mapped for the cluster lifetime to exactly one **`PeerId`** (from a pinned keypair) + seed `Multiaddr`s
- No Kademlia required for MVP — static `SeedPeer { node_id, peer_id, addrs }`
- Configure voting NodeIds **before** the first election timeout

### Protocol

| Item | Choice |
|------|--------|
| Protocol ID | `/libp2p-raft/1.0.0` |
| Framing | `u32 BE length` + bincode |
| RPC model | Unary request/response over a framed substream, then close |
| Envelope | `WireEnvelope { correlation_id, msg }` |
| Messages | RequestVote(+Resp), AppendEntries(+Resp), InstallSnapshot(+Resp) |
| Heartbeat | Empty AppendEntries |
| Snapshot | Sequential unary chunks; on failure **restart from offset 0** (no resume) |
| Not used | Gossipsub for Raft RPCs |

### RaftEngine surface

```text
tick(now)            → TickOutcome { actions, next_deadline }
handle_rpc(from,msg) → Vec<Action>
handle_rpc_failure   → Vec<Action>
propose(data)        → Index          // leader only
propose_membership(AddNode | RemoveNode) → Index
apply_ready()        → committed entries
```

**Actions:** `Send`, `Broadcast` (Behaviour expands to per-peer `Send`), `Apply`, `BecomeLeader|Follower|Candidate`, `SnapshotInstallComplete`.

**Replication:** `next_index` / `match_index`. On reject → **simple decrement**. New followers start with `next_index = leader_last_log_index + 1`, `match_index = 0`.

**Membership:** only one Add **or** Remove at a time; reject while pending. Not joint consensus.

**Storage:** `persist(hard_state, entries)` — one in-process atomic update (teaches the Raft durability *contract*; process restart still loses all state).

### Tick scheduling

Engine holds absolute deadlines. Behaviour resets a deadline timer after state-changing events and calls `tick(now)` **only when due** (no busy-poll every `poll()`).

### Crate layout

```text
src/
  behaviour.rs   # NetworkBehaviour adapter
  handler.rs     # ConnectionHandler
  peer_map.rs · config.rs · error.rs
  protocol/      # messages, codec, upgrade
  raft/          # engine, types, log, snapshot, membership
  storage/       # trait + MemoryStorage
examples/
  three_node.rs  # builds Swarms + demo cluster
tests/
  engine_*.rs    # pure engine tests (no Swarm) — engine is independently unit-testable
  codec_roundtrip.rs
```

### Implementation phases

| Phase | Deliverable |
|-------|-------------|
| 0 | Scaffold + correlated echo RPC (2 peers) |
| 1 | Engine election + MemoryStorage (unit tests) |
| 2 | Wire votes; 3-node elects leader |
| 3 | AppendEntries + propose + commit |
| 4 | Snapshots |
| 5 | Membership add/remove one-at-a-time |

---

## Status

Design approved (incl. provider patch round). Implementation plan ready. Library code not fully scaffolded yet — follow the plan linked above.

## License

MIT OR Apache-2.0 (intended)
