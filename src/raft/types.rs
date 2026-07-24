//! Core Raft type aliases and durable structures.
//!
//! Plan: Task 1 — shared by storage, engine, protocol.

// TODO(Task 1): pub type NodeId = u64;
// TODO(Task 1): pub type Term = u64;
// TODO(Task 1): pub type Index = u64;
//
// TODO(Task 1): pub enum Role { Follower, Candidate, Leader }
// TODO(Task 1): pub struct HardState { current_term, voted_for: Option<NodeId> }
// TODO(Task 1): pub enum EntryType { Command(Vec<u8>), Config(MembershipChange) } // Config in Task 8
// TODO(Task 1): pub struct LogEntry { index, term, entry_type }
// TODO(Task 1): pub struct Snapshot { last_included_index, last_included_term, conf, state_blob }
