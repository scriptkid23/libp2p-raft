//! Pure Raft state machine — no libp2p / PeerId / Dial imports.
//!
//! Plan: Task 1 types → Task 3 election → Task 6 replication → Task 7 snapshot → Task 8 membership.

pub mod engine;
pub mod log;
pub mod membership;
pub mod snapshot;
pub mod types;

// TODO(Task 3+): re-export RaftEngine, Action, TickOutcome, MembershipChange, RpcKind
