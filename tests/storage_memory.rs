use libp2p_raft::raft::types::{EntryType, HardState, LogEntry};
use libp2p_raft::storage::{MemoryStorage, Storage};

#[test]
fn persist_hard_state_and_entries_atomically() {
    let mut s = MemoryStorage::new();
    let hs = HardState {
        current_term: 3,
        voted_for: Some(1),
    };
    let e = LogEntry {
        index: 1,
        term: 3,
        entry_type: EntryType::Command(vec![1, 2, 3]),
    };
    s.persist(Some(hs.clone()), &[e.clone()]).unwrap();
    assert_eq!(s.hard_state(), hs);
    assert_eq!(s.entry(1).unwrap(), e);
}

#[test]
fn truncate_from_removes_suffix() {
    let mut s = MemoryStorage::new();
    let entries = vec![
        LogEntry {
            index: 1,
            term: 1,
            entry_type: EntryType::Command(vec![]),
        },
        LogEntry {
            index: 2,
            term: 1,
            entry_type: EntryType::Command(vec![]),
        },
    ];
    s.persist(None, &entries).unwrap();
    s.truncate_from(2);
    assert!(s.entry(2).is_none());
    assert!(s.entry(1).is_some());
}
