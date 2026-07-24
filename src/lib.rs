//! libp2p-raft — DIY mini-Raft as a rust-libp2p `NetworkBehaviour`.
//!
//! Architecture (see design spec):
//! - `RaftEngine` — pure sync state machine (no libp2p types)
//! - `RaftBehaviour` — owns engine, PeerMap, PendingRequest, deadline Sleep
//! - `ConnectionHandler` — unary framed RPCs on `/libp2p-raft/1.0.0`
//!
//! Constraints:
//! - DIY Raft only (no OpenRaft / raft-rs)
//! - Custom Handler (not `request_response`)
//! - `Storage::persist` is the only hard-state + log write path

#![allow(dead_code)] // skeleton: remove as modules are filled in

pub mod behaviour;
pub mod config;
pub mod error;
pub mod handler;
pub mod peer_map;
pub mod protocol;
pub mod raft;
pub mod storage;

// TODO(Task 1+): re-export public types once they exist
// pub use config::{RaftConfig, SeedPeer};
// pub use error::{Error, RaftError, StorageError};
// pub use behaviour::{Event, RaftBehaviour};
// pub use raft::types::{Index, NodeId, Role, Term};
