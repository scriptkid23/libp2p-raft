//! Pure RaftEngine log replication / commit tests (Phase 3 Task 1+).

use std::time::{Duration, Instant};

use libp2p_raft::config::RaftConfig;
use libp2p_raft::protocol::RaftMessage;
use libp2p_raft::raft::engine::{Action, RaftEngine};
use libp2p_raft::raft::types::{EntryType, LogEntry};
use libp2p_raft::storage::{MemoryStorage, Storage};

fn cfg(id: u64) -> RaftConfig {
    RaftConfig {
        node_id: id,
        voters: vec![1, 2, 3],
        election_timeout: Duration::from_millis(150),
        election_jitter: Duration::ZERO,
        heartbeat_interval: Duration::from_millis(50),
        rpc_timeout: Duration::from_millis(100),
        rpc_max_retries: 0,
        snapshot_threshold: 10_000,
        seed_peers: vec![],
    }
}

fn entry(index: u64, term: u64, data: &[u8]) -> LogEntry {
    LogEntry {
        index,
        term,
        entry_type: EntryType::Command(data.to_vec()),
    }
}

#[test]
fn follower_appends_entries_and_advances_commit() {
    let mut eng = RaftEngine::new(cfg(1), MemoryStorage::new());
    let t0 = Instant::now();
    let actions = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![entry(1, 1, b"hello")],
            leader_commit: 1,
        },
        t0,
    );

    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: RaftMessage::AppendEntriesResp {
                success: true,
                match_index: 1,
                term: 1,
            },
            ..
        }
    )));
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Apply { entries } if entries.len() == 1
            && entries[0].index == 1
            && entries[0].entry_type == EntryType::Command(b"hello".to_vec())
    )));
    assert_eq!(eng.commit_index(), 1);
    assert_eq!(eng.last_applied(), 1);
}

#[test]
fn follower_rejects_prev_log_mismatch() {
    let mut storage = MemoryStorage::new();
    storage
        .persist(None, &[entry(1, 1, b"a")])
        .unwrap();
    let mut eng = RaftEngine::new(cfg(1), storage);
    let t0 = Instant::now();

    let actions = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 2,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 9, // wrong term
            entries: vec![entry(2, 2, b"b")],
            leader_commit: 0,
        },
        t0,
    );

    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: RaftMessage::AppendEntriesResp {
                success: false,
                ..
            },
            ..
        }
    )));
    assert_eq!(eng.commit_index(), 0);
    // Original entry unchanged — check via a successful AE with correct prev later.
    let actions = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 2,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        },
        t0 + Duration::from_millis(1),
    );
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: RaftMessage::AppendEntriesResp {
                success: true,
                match_index: 1,
                ..
            },
            ..
        }
    )));
}

#[test]
fn follower_duplicate_prefix_does_not_truncate() {
    let mut eng = RaftEngine::new(cfg(1), MemoryStorage::new());
    let t0 = Instant::now();
    let e1 = entry(1, 1, b"x");
    let e2 = entry(2, 1, b"y");

    let _ = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![e1.clone(), e2.clone()],
            leader_commit: 0,
        },
        t0,
    );

    // Replay same prefix — must not truncate or fail.
    let actions = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![e1, e2],
            leader_commit: 2,
        },
        t0 + Duration::from_millis(1),
    );

    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: RaftMessage::AppendEntriesResp {
                success: true,
                match_index: 2,
                ..
            },
            ..
        }
    )));
    assert_eq!(eng.commit_index(), 2);
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Apply { entries } if entries.len() == 2
    )));
}

#[test]
fn follower_mid_batch_conflict_truncates_from_conflict_only() {
    let mut storage = MemoryStorage::new();
    storage
        .persist(None, &[entry(1, 1, b"a"), entry(2, 1, b"old"), entry(3, 1, b"z")])
        .unwrap();
    let mut eng = RaftEngine::new(cfg(1), storage);
    let t0 = Instant::now();

    let actions = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 2,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![entry(2, 2, b"new"), entry(3, 2, b"n2")],
            leader_commit: 0,
        },
        t0,
    );

    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: RaftMessage::AppendEntriesResp {
                success: true,
                match_index: 3,
                ..
            },
            ..
        }
    )));

    // Index 1 preserved; 2.. rewritten. Verify via empty AE at prev=3 term=2.
    let actions = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 2,
            leader_id: 2,
            prev_log_index: 3,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 0,
        },
        t0 + Duration::from_millis(1),
    );
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: RaftMessage::AppendEntriesResp {
                success: true,
                match_index: 3,
                ..
            },
            ..
        }
    )));
}

#[test]
fn follower_commit_clamps_to_last_new() {
    let mut eng = RaftEngine::new(cfg(1), MemoryStorage::new());
    let t0 = Instant::now();
    let _ = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![entry(1, 1, b"only")],
            leader_commit: 99, // ahead of last_new
        },
        t0,
    );
    assert_eq!(eng.commit_index(), 1);
    assert_eq!(eng.last_applied(), 1);
}

#[test]
#[ignore = "Task 2: leader propose/commit not implemented yet"]
fn leader_appends_and_commits_after_majority_match() {
    unimplemented!("Task 2")
}

#[test]
#[ignore = "Task 2: leader next_index decrement not implemented yet"]
fn follower_rejects_mismatch_leader_decrements_next_index() {
    unimplemented!("Task 2")
}
