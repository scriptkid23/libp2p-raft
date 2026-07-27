use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::config::RaftConfig;
use crate::protocol::messages::RaftMessage;
use crate::raft::log::log_is_up_to_date;
use crate::raft::membership::Membership;
use crate::raft::types::{HardState, Index, LogEntry, NodeId, Role, Term};
use crate::storage::Storage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcKind {
    RequestVote,
    AppendEntries,
    InstallSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Send { to: NodeId, msg: RaftMessage },
    Broadcast { msg: RaftMessage },
    Apply { entries: Vec<LogEntry> },
    BecomeLeader { term: Term },
    BecomeFollower { term: Term, leader: Option<NodeId> },
    BecomeCandidate { term: Term },
    SnapshotInstallComplete { index: Index },
}

pub struct TickOutcome {
    pub actions: Vec<Action>,
    pub next_deadline: Instant,
}

pub struct RaftEngine<S: Storage> {
    config: RaftConfig,
    storage: S,
    role: Role,
    leader: Option<NodeId>,
    votes: HashSet<NodeId>,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
    membership: Membership,
    next_index: HashMap<NodeId, Index>,
    match_index: HashMap<NodeId, Index>,
    commit_index: Index,
    last_applied: Index,
}

impl<S: Storage> RaftEngine<S> {
    pub fn new(config: RaftConfig, storage: S) -> Self {
        let now = Instant::now();
        let membership = Membership::new(config.voters.iter().copied());

        Self {
            election_deadline: now + config.election_timeout + config.election_jitter,
            heartbeat_deadline: now + config.heartbeat_interval,
            config,
            storage,
            role: Role::Follower,
            leader: None,
            votes: HashSet::new(),
            membership,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            commit_index: 0,
            last_applied: 0,
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn current_term(&self) -> Term {
        self.storage.hard_state().current_term
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    pub fn other_voters(&self) -> Vec<NodeId> {
        self.membership.other_voters(self.config.node_id)
    }

    pub fn commit_index(&self) -> Index {
        self.commit_index
    }

    pub fn last_applied(&self) -> Index {
        self.last_applied
    }

    pub fn next_deadline(&self) -> Instant {
        match self.role {
            Role::Leader => self.heartbeat_deadline.min(self.election_deadline),
            Role::Follower | Role::Candidate => self.election_deadline,
        }
    }

    pub fn tick(&mut self, now: Instant) -> TickOutcome {
        let mut actions = Vec::new();

        if now < self.next_deadline() {
            return TickOutcome {
                actions,
                next_deadline: self.next_deadline(),
            };
        }

        match self.role {
            Role::Follower | Role::Candidate if now >= self.election_deadline => {
                actions.extend(self.start_election(now));
            }
            Role::Leader if now >= self.heartbeat_deadline => {
                actions.extend(self.send_heartbeats());
                self.heartbeat_deadline = now + self.config.heartbeat_interval;
            }
            _ => {}
        }

        TickOutcome {
            actions,
            next_deadline: self.next_deadline(),
        }
    }

    pub fn handle_rpc(&mut self, from: NodeId, msg: RaftMessage, now: Instant) -> Vec<Action> {
        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(
                from,
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                now,
            ),
            RaftMessage::RequestVoteResp { term, vote_granted } => {
                self.handle_request_vote_resp(from, term, vote_granted, now)
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                from,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                now,
            ),
            RaftMessage::AppendEntriesResp { term, .. } => {
                self.handle_append_entries_resp(term, now)
            }
            _ => Vec::new(),
        }
    }

    pub fn handle_rpc_failure(&mut self, _to: NodeId, _kind: RpcKind) -> Vec<Action> {
        Vec::new()
    }

    fn start_election(&mut self, now: Instant) -> Vec<Action> {
        let term = self.current_term() + 1;
        let mut actions = Vec::new();

        self.become_candidate(term, &mut actions);
        self.reset_election_deadline(now);

        let (last_log_index, last_log_term) = self.storage.last_index_term();
        actions.push(Action::Broadcast {
            msg: RaftMessage::RequestVote {
                term,
                candidate_id: self.config.node_id,
                last_log_index,
                last_log_term,
            },
        });

        actions
    }

    fn become_candidate(&mut self, term: Term, actions: &mut Vec<Action>) {
        self.role = Role::Candidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.config.node_id);

        let hs = HardState {
            current_term: term,
            voted_for: Some(self.config.node_id),
        };
        let _ = self.storage.persist(Some(hs), &[]);

        actions.push(Action::BecomeCandidate { term });
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>, actions: &mut Vec<Action>) {
        if term > self.current_term() {
            let hs = HardState {
                current_term: term,
                voted_for: None,
            };
            let _ = self.storage.persist(Some(hs), &[]);
        }

        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.next_index.clear();
        self.match_index.clear();

        actions.push(Action::BecomeFollower { term, leader });
    }

    fn become_leader(&mut self, term: Term, now: Instant, actions: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.leader = Some(self.config.node_id);
        self.votes.clear();

        let (last_index, _) = self.storage.last_index_term();
        self.next_index.clear();
        self.match_index.clear();
        for voter in self.membership.voters() {
            if *voter != self.config.node_id {
                self.next_index.insert(*voter, last_index + 1);
                self.match_index.insert(*voter, 0);
            }
        }

        self.heartbeat_deadline = now + self.config.heartbeat_interval;
        self.reset_election_deadline(now);

        actions.push(Action::BecomeLeader { term });
    }

    fn reset_election_deadline(&mut self, now: Instant) {
        self.election_deadline = now + self.config.election_timeout + self.config.election_jitter;
    }

    fn handle_request_vote(
        &mut self,
        from: NodeId,
        term: Term,
        candidate_id: NodeId,
        last_log_index: Index,
        last_log_term: Term,
        now: Instant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        let current_term = self.current_term();

        if term < current_term {
            actions.push(Action::Send {
                to: from,
                msg: RaftMessage::RequestVoteResp {
                    term: current_term,
                    vote_granted: false,
                },
            });
            return actions;
        }

        if term > current_term {
            self.become_follower(term, None, &mut actions);
        }

        let hs = self.storage.hard_state();
        let mut vote_granted = false;

        let can_vote = hs.voted_for.is_none() || hs.voted_for == Some(candidate_id);
        if can_vote && log_is_up_to_date(&self.storage, last_log_index, last_log_term) {
            let new_hs = HardState {
                current_term: term.max(self.current_term()),
                voted_for: Some(candidate_id),
            };
            let _ = self.storage.persist(Some(new_hs), &[]);
            vote_granted = true;
            self.role = Role::Follower;
            self.leader = None;
            self.votes.clear();
            self.reset_election_deadline(now);
        }

        actions.push(Action::Send {
            to: from,
            msg: RaftMessage::RequestVoteResp {
                term: self.current_term(),
                vote_granted,
            },
        });

        actions
    }

    fn handle_request_vote_resp(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
        now: Instant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        let current_term = self.current_term();

        if term < current_term {
            return actions;
        }

        if term > current_term {
            self.become_follower(term, None, &mut actions);
            return actions;
        }

        if self.role != Role::Candidate || !vote_granted {
            return actions;
        }

        self.votes.insert(from);
        if self.votes.len() >= self.membership.quorum() {
            self.become_leader(term, now, &mut actions);
        }

        actions
    }

    fn handle_append_entries(
        &mut self,
        from: NodeId,
        term: Term,
        leader_id: NodeId,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
        now: Instant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        let current_term = self.current_term();
        let (last_index, _) = self.storage.last_index_term();

        if term < current_term {
            actions.push(Action::Send {
                to: from,
                msg: RaftMessage::AppendEntriesResp {
                    term: current_term,
                    success: false,
                    match_index: last_index,
                },
            });
            return actions;
        }

        if term > current_term {
            self.become_follower(term, Some(leader_id), &mut actions);
        } else if self.role != Role::Follower {
            self.become_follower(term, Some(leader_id), &mut actions);
        } else {
            self.leader = Some(leader_id);
        }

        // Legitimate leader contact: reset deadline even if prev_log fails.
        self.reset_election_deadline(now);

        let prev_ok = if prev_log_index == 0 {
            prev_log_term == 0
        } else {
            self.storage
                .entry(prev_log_index)
                .map(|e| e.term == prev_log_term)
                .unwrap_or(false)
        };

        if !prev_ok {
            let (last_index, _) = self.storage.last_index_term();
            actions.push(Action::Send {
                to: from,
                msg: RaftMessage::AppendEntriesResp {
                    term: self.current_term(),
                    success: false,
                    match_index: last_index,
                },
            });
            return actions;
        }

        // Conditional truncate + append: skip identical prefix; truncate only on conflict.
        let mut append_from = 0usize;
        for (i, e) in entries.iter().enumerate() {
            let idx = prev_log_index + 1 + i as Index;
            match self.storage.entry(idx) {
                Some(existing) if existing.term == e.term => {
                    append_from = i + 1;
                }
                Some(_) => {
                    self.storage.truncate_from(idx);
                    append_from = i;
                    break;
                }
                None => {
                    append_from = i;
                    break;
                }
            }
        }

        if append_from < entries.len() {
            let _ = self.storage.persist(None, &entries[append_from..]);
        }

        // last_new = index of last new entry in this RPC (Raft Fig. 2).
        let last_new = prev_log_index + entries.len() as Index;
        self.advance_commit_and_apply(leader_commit, last_new, &mut actions);

        actions.push(Action::Send {
            to: from,
            msg: RaftMessage::AppendEntriesResp {
                term: self.current_term(),
                success: true,
                match_index: last_new,
            },
        });

        actions
    }

    fn advance_commit_and_apply(
        &mut self,
        leader_commit: Index,
        last_new: Index,
        actions: &mut Vec<Action>,
    ) {
        let new_commit = leader_commit.min(last_new);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
        }
        if self.commit_index <= self.last_applied {
            return;
        }
        let mut entries = Vec::new();
        for i in (self.last_applied + 1)..=self.commit_index {
            if let Some(e) = self.storage.entry(i) {
                entries.push(e);
            }
        }
        self.last_applied = self.commit_index;
        if !entries.is_empty() {
            actions.push(Action::Apply { entries });
        }
    }

    fn handle_append_entries_resp(&mut self, term: Term, _now: Instant) -> Vec<Action> {
        let mut actions = Vec::new();
        let current_term = self.current_term();
        if term > current_term {
            self.become_follower(term, None, &mut actions);
        }
        actions
    }

    fn send_heartbeats(&self) -> Vec<Action> {
        let term = self.current_term();
        let (last_index, last_term) = self.storage.last_index_term();
        let prev_log_term = if last_index > 0 {
            self.storage
                .entry(last_index)
                .map(|e| e.term)
                .unwrap_or(last_term)
        } else {
            0
        };

        vec![Action::Broadcast {
            msg: RaftMessage::AppendEntries {
                term,
                leader_id: self.config.node_id,
                prev_log_index: last_index,
                prev_log_term,
                entries: Vec::new(),
                leader_commit: self.commit_index,
            },
        }]
    }
}
