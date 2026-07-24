//! Raft RPC wire messages + envelope with correlation_id.
//!
//! Plan: Task 1 — serde Serialize/Deserialize for bincode.

// TODO(Task 1): pub struct WireEnvelope { correlation_id: u64, msg: RaftMessage }
//
// TODO(Task 1): pub enum RaftMessage {
//     RequestVote { term, candidate_id, last_log_index, last_log_term },
//     RequestVoteResp { term, vote_granted },
//     AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit },
//     AppendEntriesResp { term, success, match_index },
//     InstallSnapshot { term, leader_id, last_included_index, last_included_term, offset, data, done },
//     InstallSnapshotResp { term },
// }
// Note: Heartbeat = AppendEntries with entries = []
// Membership changes travel as EntryType::Config log entries, not a separate RPC.
