//! Wire protocol: messages, length-delimited codec, libp2p upgrade.
//!
//! Protocol ID: `/libp2p-raft/1.0.0`
//! Framing: u32 BE length + bincode payload
//! RPC model: unary (one substream = one request/response)

pub mod codec;
pub mod messages;
pub mod upgrade;

// TODO(Task 1): re-export encode_envelope, decode_envelope, RaftMessage, WireEnvelope, PROTOCOL_NAME
