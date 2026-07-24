//! Pure RaftEngine log replication / commit / snapshot tests.
//!
//! Plan: Task 6 (replication) / Task 7 (optional snapshot cases here or engine_snapshot.rs).

#[test]
fn leader_appends_and_commits_after_majority_match() {
    // TODO(Task 6): elect leader; propose; feed AppendEntriesResp success from majority
    // TODO(Task 6): assert commit advances + Apply / apply_ready entries
    unimplemented!("Task 6: leader_appends_and_commits_after_majority_match")
}

#[test]
fn follower_rejects_mismatch_leader_decrements_next_index() {
    // TODO(Task 6): leader next_index starts high; follower returns success=false
    // TODO(Task 6): assert next_index decreased by 1 (floored by match_index+1 / 1)
    // Constraint: simple decrement — no conflict-index hints
    unimplemented!("Task 6: follower_rejects_mismatch_leader_decrements_next_index")
}

// TODO(Task 7): compact when snapshot_threshold exceeded
// TODO(Task 7): follower install replaces state
// TODO(Task 7): interrupted transfer resets offset to 0
