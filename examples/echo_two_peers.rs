//! Temporary 2-peer echo — proves Handler + codec path (Task 4).
//! May be deleted or kept as a learning example in Task 9.
//!
//! Run: `cargo run --example echo_two_peers`
//! Expected: both sides log correlated request/response.

fn main() {
    // TODO(Task 4): build two Swarms (TCP + Noise + Yamux) with RaftBehaviour echo shell
    // TODO(Task 4): pin keypairs; dial peer A → B
    // TODO(Task 4): send one WireEnvelope; print response with matching correlation_id
    unimplemented!("Task 4: echo_two_peers skeleton")
}
