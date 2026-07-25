use crate::raft::types::{Index, Term};
use crate::storage::Storage;

/// Returns true if the candidate's log is at least as up-to-date as the local log.
pub fn log_is_up_to_date(
    storage: &impl Storage,
    candidate_last_index: Index,
    candidate_last_term: Term,
) -> bool {
    let (last_index, last_term) = storage.last_index_term();
    candidate_last_term > last_term
        || (candidate_last_term == last_term && candidate_last_index >= last_index)
}
