//! Pure RaftEngine election tests (no Swarm / networking).
//!
//! Plan: Task 3.

// TODO(Task 3): helper cfg(id, voters) with election_jitter = 0 for determinism

#[test]
fn follower_times_out_becomes_candidate_and_requests_votes() {
    // TODO(Task 3): tick with now past election deadline
    // TODO(Task 3): assert Role::Candidate
    // TODO(Task 3): assert Action::Send or Broadcast RequestVote present
    unimplemented!("Task 3: follower_times_out_becomes_candidate_and_requests_votes")
}

#[test]
fn candidate_wins_majority_becomes_leader() {
    // TODO(Task 3): start election via tick
    // TODO(Task 3): handle_rpc RequestVoteResp { vote_granted: true } from peer 2
    // TODO(Task 3): assert Role::Leader and BecomeLeader action (self vote already counted)
    unimplemented!("Task 3: candidate_wins_majority_becomes_leader")
}
