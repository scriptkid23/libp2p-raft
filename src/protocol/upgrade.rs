//! libp2p protocol upgrade for `/libp2p-raft/1.0.0`.

use libp2p::core::upgrade::ReadyUpgrade;
use libp2p::StreamProtocol;

pub const PROTOCOL_NAME: &str = "/libp2p-raft/1.0.0";

pub fn raft_protocol() -> StreamProtocol {
    StreamProtocol::new(PROTOCOL_NAME)
}

pub fn raft_ready_upgrade() -> ReadyUpgrade<StreamProtocol> {
    ReadyUpgrade::new(raft_protocol())
}
