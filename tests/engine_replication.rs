//! Pure RaftEngine log replication / commit / snapshot tests.
//!
//! Plan: Task 6 (replication) / Task 7 (optional snapshot cases here or engine_snapshot.rs).

#[test]
#[ignore = "Task 6: log replication not implemented yet"]
fn leader_appends_and_commits_after_majority_match() {
    unimplemented!("Task 6: leader_appends_and_commits_after_majority_match")
}

#[test]
#[ignore = "Task 6: log replication not implemented yet"]
fn follower_rejects_mismatch_leader_decrements_next_index() {
    unimplemented!("Task 6: follower_rejects_mismatch_leader_decrements_next_index")
}

// TODO(Task 7): compact when snapshot_threshold exceeded
// TODO(Task 7): follower install replaces state
// TODO(Task 7): interrupted transfer resets offset to 0
