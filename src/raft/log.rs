//! Log helpers used by RaftEngine (conflict checks, slice for AppendEntries).
//!
//! Plan: Task 3 (minimal last_index_term) / Task 6 (full replication helpers).

// TODO(Task 3): helpers around Storage::last_index_term for RequestVote log check
// TODO(Task 6): prev_log_index / prev_log_term mismatch detection
// TODO(Task 6): entries slice for follower starting at next_index
// TODO(Task 6): commit rule helper — majority match_index + term == current_term
