//! In-memory `Storage` — teaches atomic persist contract (not disk durable).
//!
//! Plan: Task 2 (+ Task 7 snapshot install).

// TODO(Task 2): pub struct MemoryStorage { hard_state, log: Vec/Map, snapshot: Option }
// TODO(Task 2): MemoryStorage::new()
// TODO(Task 2): persist updates hard_state + entries in one method body (no intermediate public observation)
// TODO(Task 2): truncate_from removes log suffix from index inclusive
// TODO(Task 7): install_snapshot replaces state + truncates prefix as needed
