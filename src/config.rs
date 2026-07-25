use std::time::Duration;

use libp2p::{Multiaddr, PeerId};

use crate::raft::types::NodeId;

#[derive(Debug, Clone)]
pub struct SeedPeer {
    pub node_id: NodeId,
    pub peer_id: PeerId,
    pub addrs: Vec<Multiaddr>,
}

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: NodeId,
    pub voters: Vec<NodeId>,
    pub seed_peers: Vec<SeedPeer>,
    pub election_timeout: Duration,
    pub election_jitter: Duration,
    pub heartbeat_interval: Duration,
    pub rpc_timeout: Duration,
    pub rpc_max_retries: u32,
    pub snapshot_threshold: u64,
}
