//! Persistence trait — hard state, log, snapshot.
//!
//! Plan: Task 2.
//! Constraint: `persist` is the only write path for hard state + log appends (atomic batch).

pub mod memory;

// TODO(Task 2): pub trait Storage {
//     fn hard_state(&self) -> HardState;
//     fn entry(&self, index: Index) -> Option<LogEntry>;
//     fn last_index_term(&self) -> (Index, Term);  // empty log → (0, 0)
//     fn truncate_from(&mut self, index: Index);
//     fn snapshot(&self) -> Option<Snapshot>;
//     fn install_snapshot(&mut self, snap: Snapshot);
//     fn persist(hard_state: Option<HardState>, entries: &[LogEntry]) -> Result<(), StorageError>;
// }
//
// TODO(Task 2): re-export MemoryStorage
