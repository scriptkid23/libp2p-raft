//! NodeId ↔ PeerId + seed Multiaddrs.
//!
//! Used by Behaviour for dial / NotifyHandler routing.
//! Engine never sees PeerId; only Behaviour resolves NodeId via this map.

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
