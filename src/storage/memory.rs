use crate::error::StorageError;
use crate::raft::types::{HardState, Index, LogEntry, Snapshot, Term};
use crate::storage::Storage;

#[derive(Debug, Clone)]
pub struct MemoryStorage {
    hard_state: HardState,
    entries: Vec<LogEntry>,
    snapshot: Option<Snapshot>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            hard_state: HardState::default(),
            entries: Vec::new(),
            snapshot: None,
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn hard_state(&self) -> HardState {
        self.hard_state.clone()
    }

    fn entry(&self, index: Index) -> Option<LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize).cloned()
    }

    fn last_index_term(&self) -> (Index, Term) {
        match self.entries.last() {
            Some(e) => (e.index, e.term),
            None => (0, 0),
        }
    }

    fn truncate_from(&mut self, index: Index) {
        if index == 0 {
            self.entries.clear();
            return;
        }
        let keep = (index - 1) as usize;
        if keep < self.entries.len() {
            self.entries.truncate(keep);
        }
    }

    fn snapshot(&self) -> Option<Snapshot> {
        self.snapshot.clone()
    }

    fn install_snapshot(&mut self, snap: Snapshot) {
        self.snapshot = Some(snap);
    }

    fn persist(
        &mut self,
        hard_state: Option<HardState>,
        entries: &[LogEntry],
    ) -> Result<(), StorageError> {
        if let Some(hs) = hard_state {
            self.hard_state = hs;
        }

        for entry in entries {
            let idx = entry.index as usize;
            if idx == 0 {
                return Err(StorageError::Msg("log index must be >= 1".into()));
            }
            let slot = idx - 1;
            if slot == self.entries.len() {
                self.entries.push(entry.clone());
            } else if slot < self.entries.len() {
                self.entries[slot] = entry.clone();
                self.entries.truncate(idx);
            } else {
                return Err(StorageError::Msg(format!(
                    "non-contiguous append at index {}",
                    entry.index
                )));
            }
        }

        Ok(())
    }
}
