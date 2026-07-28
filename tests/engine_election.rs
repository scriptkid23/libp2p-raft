use std::time::{Duration, Instant};

use libp2p_raft::config::RaftConfig;
use libp2p_raft::protocol::RaftMessage;
use libp2p_raft::raft::engine::{Action, RaftEngine};
use libp2p_raft::raft::types::Role;
use libp2p_raft::storage::MemoryStorage;

fn cfg(id: u64, voters: &[u64]) -> RaftConfig {
    RaftConfig {
        node_id: id,
        voters: voters.to_vec(),
        election_timeout: Duration::from_millis(150),
        heartbeat_interval: Duration::from_millis(50),
        rpc_timeout: Duration::from_millis(100),
        rpc_max_retries: 1,
        snapshot_threshold: 10_000,
        seed_peers: vec![],
        election_jitter: Duration::from_millis(0),
    }
}

fn has_request_vote(actions: &[Action]) -> bool {
    actions.iter().any(|a| match a {
        Action::Send { msg, .. } | Action::Broadcast { msg } => {
            matches!(msg, RaftMessage::RequestVote { .. })
        }
        _ => false,
    })
}

#[test]
fn follower_times_out_becomes_candidate_and_requests_votes() {
    let mut eng = RaftEngine::new(cfg(1, &[1, 2, 3]), MemoryStorage::new());
    let start = Instant::now();
    let out = eng.tick(start + Duration::from_millis(200));
    assert!(matches!(eng.role(), Role::Candidate));
    assert!(has_request_vote(&out.actions));
}

#[test]
fn candidate_wins_majority_becomes_leader() {
    let mut eng = RaftEngine::new(cfg(1, &[1, 2, 3]), MemoryStorage::new());
    let start = Instant::now();
    eng.tick(start + Duration::from_millis(200));
    assert!(matches!(eng.role(), Role::Candidate));

    let actions = eng.handle_rpc(
        2,
        RaftMessage::RequestVoteResp {
            term: 1,
            vote_granted: true,
        },
        Instant::now(),
    );

    assert!(matches!(eng.role(), Role::Leader));
    assert!(actions.iter().any(|a| matches!(a, Action::BecomeLeader { term: 1 })));
}
