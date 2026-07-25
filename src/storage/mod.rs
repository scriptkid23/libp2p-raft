use crate::error::StorageError;
use crate::raft::types::{HardState, Index, LogEntry, Snapshot, Term};

pub mod memory;

pub use memory::MemoryStorage;

pub trait Storage {
    fn hard_state(&self) -> HardState;
    fn entry(&self, index: Index) -> Option<LogEntry>;
    fn last_index_term(&self) -> (Index, Term);
    fn truncate_from(&mut self, index: Index);
    fn snapshot(&self) -> Option<Snapshot>;
    fn install_snapshot(&mut self, snap: Snapshot);
    fn persist(
        &mut self,
        hard_state: Option<HardState>,
        entries: &[LogEntry],
    ) -> Result<(), StorageError>;
}
