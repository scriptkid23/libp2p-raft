use std::time::{Duration, Instant};

use libp2p_raft::config::RaftConfig;
use libp2p_raft::protocol::RaftMessage;
use libp2p_raft::raft::engine::{Action, RaftEngine};
use libp2p_raft::raft::types::Role;
use libp2p_raft::storage::MemoryStorage;

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

#[test]
fn empty_heartbeat_resets_deadline_follower_stays() {
    let mut eng = RaftEngine::new(cfg(1), MemoryStorage::new());
    let t0 = Instant::now();
    let _ = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
        t0,
    );
    let out = eng.tick(t0 + Duration::from_millis(100));
    assert!(matches!(eng.role(), Role::Follower));
    assert!(out.actions.is_empty());
}

#[test]
fn stale_term_ae_does_not_reset_deadline() {
    let mut eng = RaftEngine::new(cfg(1), MemoryStorage::new());
    let t0 = Instant::now();
    let _ = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 2,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
        t0,
    );
    let actions = eng.handle_rpc(
        3,
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
        t0 + Duration::from_millis(10),
    );
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: RaftMessage::AppendEntriesResp {
                success: false,
                term: 2,
                ..
            },
            ..
        }
    )));
    let out = eng.tick(t0 + Duration::from_millis(160));
    assert!(matches!(eng.role(), Role::Candidate));
    assert!(!out.actions.is_empty());
}

#[test]
fn empty_ae_mismatched_prev_rejects_but_resets_deadline() {
    let mut eng = RaftEngine::new(cfg(1), MemoryStorage::new());
    let t0 = Instant::now();
    // Seed log so prev_log_index=1 is required for non-zero prev.
    let _ = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![libp2p_raft::raft::types::LogEntry {
                index: 1,
                term: 1,
                entry_type: libp2p_raft::raft::types::EntryType::Command(b"a".to_vec()),
            }],
            leader_commit: 0,
        },
        t0,
    );
    let actions = eng.handle_rpc(
        2,
        RaftMessage::AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 99,
            entries: vec![],
            leader_commit: 0,
        },
        t0 + Duration::from_millis(10),
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
    // Deadline was reset at t0+10 → tick at t0+100 must not elect.
    let out = eng.tick(t0 + Duration::from_millis(100));
    assert!(matches!(eng.role(), Role::Follower));
    assert!(out.actions.is_empty());
}

#[test]
fn duplicate_vote_resp_does_not_double_count() {
    let mut eng = RaftEngine::new(cfg(1), MemoryStorage::new());
    let t0 = Instant::now();
    eng.tick(t0 + Duration::from_millis(200));
    assert!(matches!(eng.role(), Role::Candidate));
    let _ = eng.handle_rpc(
        2,
        RaftMessage::RequestVoteResp {
            term: 1,
            vote_granted: true,
        },
        t0 + Duration::from_millis(201),
    );
    assert!(matches!(eng.role(), Role::Leader));
    let actions = eng.handle_rpc(
        2,
        RaftMessage::RequestVoteResp {
            term: 1,
            vote_granted: true,
        },
        t0 + Duration::from_millis(202),
    );
    assert!(matches!(eng.role(), Role::Leader));
    assert!(actions
        .iter()
        .all(|a| !matches!(a, Action::BecomeLeader { .. })));
}
