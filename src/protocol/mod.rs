//! Wire protocol: messages, length-delimited codec, libp2p upgrade.

pub mod codec;
pub mod messages;
pub mod upgrade;

pub use codec::{decode_envelope, encode_envelope};
pub use messages::{RaftMessage, WireEnvelope};
pub use upgrade::PROTOCOL_NAME;
