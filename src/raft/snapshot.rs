//! Snapshot compaction + chunk assembly helpers.
//!
//! Plan: Task 7.
//! Constraint: on transfer failure, restart from offset 0 (no resume).

// TODO(Task 7): create snapshot when log length > snapshot_threshold
// TODO(Task 7): truncate log prefix after snapshot stored via Storage
// TODO(Task 7): follower chunk assembly (offset, data, done)
// TODO(Task 7): on failure / interrupt → reset expected offset to 0
// TODO(Task 7): Action::SnapshotInstallComplete when done
