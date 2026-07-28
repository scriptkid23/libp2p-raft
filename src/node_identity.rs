//! Deterministic libp2p identity per Raft `NodeId` (demo / test clusters).

use libp2p::identity::ed25519;
use libp2p::identity::Keypair;
use libp2p::PeerId;

use crate::error::Error;
use crate::raft::types::NodeId;

/// Same key for a given `node_id` on every process (no key files).
pub fn keypair_for_node(node_id: NodeId) -> Result<Keypair, Error> {
    let mut seed = [0u8; 32];
    seed[0] = node_id as u8;
    seed[1..9].copy_from_slice(b"libp2prf");
    let secret = ed25519::SecretKey::try_from_bytes(seed)
        .map_err(|e| Error::Rpc(format!("invalid node key seed: {e}")))?;
    Ok(Keypair::from(ed25519::Keypair::from(secret)))
}

pub fn peer_id_for_node(node_id: NodeId) -> Result<PeerId, Error> {
    Ok(PeerId::from(keypair_for_node(node_id)?.public()))
}
