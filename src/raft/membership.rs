//! Voting membership + single pending change gate.
//!
//! Plan: Task 3 (static voters) / Task 8 (Add/Remove one-at-a-time).
//! Constraint: AddNode XOR RemoveNode; reject while pending_change.is_some().
//! Not joint consensus — learning limitation.

// TODO(Task 3): pub struct Membership { voters: HashSet<NodeId> } — static for election
// TODO(Task 8): pub enum MembershipChange { AddNode(NodeId), RemoveNode(NodeId) }
// TODO(Task 8): pending_change: Option<MembershipChange> until config entry commits
// TODO(Task 8): apply committed Config entry → update voters + clear pending
// TODO(Task 8): reject propose_membership with RaftError::MembershipPending
