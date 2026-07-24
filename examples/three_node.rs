//! 3-node demo cluster — election then propose/commit.
//!
//! Plan: Task 5 (elect leader) → Task 6 (propose + wait Committed).
//! Run: `cargo run --example three_node`
//!
//! Constraints: pinned keypairs; static seed_peers: Vec<(NodeId, PeerId, Multiaddr)>.

fn main() {
    // TODO(Task 5): three Swarms (one tokio runtime or tasks); pinned keypairs
    // TODO(Task 5): seed_peers fully connected before first election timeout
    // TODO(Task 5): wait until one reports Role::Leader (Event::RoleChanged)
    // TODO(Task 6): leader propose one command; wait Event::Committed
    // TODO(Task 8 optional): membership demo behind comment/flag
    unimplemented!("Task 5/6: three_node skeleton")
}
