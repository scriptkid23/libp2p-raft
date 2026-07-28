//! Static cluster topology from a TOML file (`config/cluster*.toml`).

use std::fs;
use std::net::ToSocketAddrs;
use std::path::Path;

use libp2p::Multiaddr;
use serde::Deserialize;

use crate::config::SeedPeer;
use crate::node_identity::peer_id_for_node;
use crate::raft::types::NodeId;

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
    pub voters: Vec<NodeId>,
    /// When set, this node proposes `"hello"` after ~1s as stable leader.
    #[serde(default)]
    pub propose_hello_node: Option<NodeId>,
    pub nodes: Vec<ClusterNodeSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterNodeSpec {
    pub id: NodeId,
    pub host: String,
    pub port: u16,
}

impl ClusterConfig {
    pub fn load(path: &Path) -> Result<Self, String> {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("read cluster config {}: {e}", path.display()))?;
        toml::from_str(&raw).map_err(|e| format!("parse cluster config {}: {e}", path.display()))
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.iter().map(|n| n.id).collect()
    }

    pub fn listen_port(&self, node_id: NodeId) -> Option<u16> {
        self.nodes
            .iter()
            .find(|n| n.id == node_id)
            .map(|n| n.port)
    }

    pub fn seed_peers(&self, self_id: NodeId) -> Result<Vec<SeedPeer>, String> {
        let mut out = Vec::new();
        for node in &self.nodes {
            if node.id == self_id {
                continue;
            }
            let addr = resolve_host_port(&node.host, node.port)?;
            let peer_id = peer_id_for_node(node.id)
                .map_err(|e| format!("peer id for node {}: {e}", node.id))?;
            out.push(SeedPeer {
                node_id: node.id,
                peer_id,
                addrs: vec![addr],
            });
        }
        Ok(out)
    }
}

pub fn resolve_host_port(host: &str, port: u16) -> Result<Multiaddr, String> {
    let sock = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("resolve {host}:{port}: {e}"))?
        .next()
        .ok_or_else(|| format!("could not resolve {host}:{port}"))?;
    let ip = sock.ip();
    let addr: Multiaddr = if ip.is_ipv4() {
        format!("/ip4/{ip}/tcp/{port}")
    } else {
        format!("/ip6/{ip}/tcp/{port}")
    }
    .parse()
    .map_err(|e| format!("multiaddr for {host}:{port}: {e}"))?;
    Ok(addr)
}
