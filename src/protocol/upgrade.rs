//! libp2p protocol upgrade for `/libp2p-raft/1.0.0`.
//!
//! Plan: Task 1 stub (const) / Task 4 (full negotiate + stream upgrade).

// Protocol name negotiated on each Raft substream.
// TODO(Task 1): pub const PROTOCOL_NAME: &str = "/libp2p-raft/1.0.0";

// TODO(Task 4): UpgradeInfo / InboundUpgrade / OutboundUpgrade (or libp2p helper)
// TODO(Task 4): after upgrade, stream is framed by codec (handler owns read/write)
