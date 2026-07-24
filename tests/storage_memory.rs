//! MemoryStorage unit tests.
//!
//! Plan: Task 2 — atomic persist + truncate_from.

// TODO(Task 2): use libp2p_raft::storage::{MemoryStorage, Storage};
// TODO(Task 2): use libp2p_raft::raft::types::{HardState, LogEntry, EntryType};

#[test]
fn persist_hard_state_and_entries_atomically() {
    // TODO(Task 2): persist Some(hs) + &[entry]; assert hard_state() and entry(1)
    unimplemented!("Task 2: persist_hard_state_and_entries_atomically")
}

#[test]
fn truncate_from_removes_suffix() {
    // TODO(Task 2): persist two entries; truncate_from(2); entry(2) none, entry(1) some
    unimplemented!("Task 2: truncate_from_removes_suffix")
}
