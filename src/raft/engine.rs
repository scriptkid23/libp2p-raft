//! `RaftEngine` — synchronous pure state machine.
//!
//! Behaviour calls from `poll()` only: tick / handle_rpc / handle_rpc_failure / propose.
//! Never imports libp2p types.
//!
//! Plan: Task 3 election → Task 6 AppendEntries → Task 7 snapshot → Task 8 membership.

// TODO(Task 3): pub struct RaftEngine<S: Storage> { id, role, term, voted_for, votes,
//     election_deadline, heartbeat_deadline, storage, membership, next_index, match_index, ... }
//
// TODO(Task 3): pub enum Action {
//     Send { to, msg },
//     Broadcast { msg },
//     Apply { entries },
//     BecomeLeader { term },
//     BecomeFollower { term, leader },
//     BecomeCandidate { term },
//     SnapshotInstallComplete { index },  // Task 7
// }
//
// TODO(Task 3): pub struct TickOutcome { actions, next_deadline }
// TODO(Task 3): pub enum RpcKind { RequestVote, AppendEntries, InstallSnapshot }
//
// TODO(Task 3): tick(now) — election timeout → Candidate, self-vote, persist, Broadcast RequestVote
// TODO(Task 3): handle_rpc RequestVote / RequestVoteResp — majority → BecomeLeader
// TODO(Task 3): next_deadline() = min of relevant absolute deadlines
// TODO(Task 3): handle_rpc_failure — ignore stale after term change
//
// TODO(Task 6): propose(data) — Leader only; persist append; replicate AppendEntries
// TODO(Task 6): heartbeats = empty AppendEntries on heartbeat_deadline
// TODO(Task 6): on AppendEntriesResp success=false → simple next_index decrement
// TODO(Task 6): commit when majority match_index >= N and log[N].term == current_term
// TODO(Task 6): apply_ready() → committed entries
//
// TODO(Task 7): compact + InstallSnapshot chunk send/receive
// TODO(Task 8): propose_membership(Add|Remove) with pending gate
