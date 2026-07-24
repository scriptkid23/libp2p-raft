//! Wire codec round-trip tests.
//!
//! Plan: Task 1 — length-delimited bincode + correlation envelope.

// TODO(Task 1): use libp2p_raft::protocol::{decode_envelope, encode_envelope, RaftMessage, WireEnvelope};

#[test]
fn request_vote_roundtrip() {
    // TODO(Task 1): encode RequestVote WireEnvelope { correlation_id: 42, ... }
    // TODO(Task 1): decode; assert correlation_id + term + candidate_id
    unimplemented!("Task 1: codec_roundtrip")
}
