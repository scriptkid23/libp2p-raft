use serde::{Deserialize, Serialize};

use crate::raft::types::{Index, LogEntry, NodeId, Term};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireEnvelope {
    pub correlation_id: u64,
    pub msg: RaftMessage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: Index,
        last_log_term: Term,
    },
    RequestVoteResp {
        term: Term,
        vote_granted: bool,
    },
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
    },
    AppendEntriesResp {
        term: Term,
        success: bool,
        match_index: Index,
    },
    InstallSnapshot {
        term: Term,
        leader_id: NodeId,
        last_included_index: Index,
        last_included_term: Term,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    },
    InstallSnapshotResp {
        term: Term,
    },
}
