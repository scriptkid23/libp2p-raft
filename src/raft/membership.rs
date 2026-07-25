use std::collections::HashSet;

use crate::raft::types::NodeId;

#[derive(Debug, Clone)]
pub struct Membership {
    voters: HashSet<NodeId>,
}

impl Membership {
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            voters: voters.into_iter().collect(),
        }
    }

    pub fn voters(&self) -> &HashSet<NodeId> {
        &self.voters
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.voters.contains(&id)
    }

    pub fn len(&self) -> usize {
        self.voters.len()
    }

    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub fn other_voters(&self, self_id: NodeId) -> Vec<NodeId> {
        self.voters
            .iter()
            .copied()
            .filter(|id| *id != self_id)
            .collect()
    }
}
