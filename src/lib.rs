//! libp2p-raft — DIY mini-Raft as a rust-libp2p `NetworkBehaviour`.
//!
//! Architecture (see design spec):
//! - `RaftEngine` — pure sync state machine (no libp2p types)
//! - `RaftBehaviour` — owns engine, PeerMap, PendingRequest, deadline Sleep
//! - `ConnectionHandler` — unary framed RPCs on `/libp2p-raft/1.0.0`

#![allow(dead_code)]

pub mod behaviour;
pub mod config;
pub mod error;
pub mod handler;
pub mod peer_map;
pub mod protocol;
pub mod raft;
pub mod storage;

pub use config::{RaftConfig, SeedPeer};
pub use error::{Error, RaftError, StorageError};
pub use raft::types::{Index, NodeId, Role, Term};
pub use raft::{Action, RaftEngine, RpcKind, TickOutcome};
pub use storage::{MemoryStorage, Storage};
