//! `RaftBehaviour` — NetworkBehaviour adapter around `RaftEngine`.
//!
//! Plan: Task 4 (echo shell) → Task 5 (wire election) → Task 6+ (propose/commit/events).
//!
//! Owns: engine, PeerMap, PendingRequest map, deadline Sleep.
//! Maps Actions → Dial / NotifyHandler; never implements Raft algorithm itself.

// TODO(Task 4): pub struct RaftBehaviour { /* no engine yet — echo path */ }
// TODO(Task 4): send_echo(peer, payload) + Event::Echo for proving Handler path
// TODO(Task 4): dial from seed_peers via PeerMap
//
// TODO(Task 5): own RaftEngine + Storage; integrate poll loop:
//     1. drain handler events; match correlation_id; engine.handle_rpc / ignore stale
//     2. PendingRequest timeouts: retry ≤ rpc_max_retries else handle_rpc_failure
//     3. if Instant::now() >= deadline: engine.tick(now)
//     4. Action::Send / Broadcast → PeerMap → dial if needed → SendRequest
//     5. Become* → Event::RoleChanged
//     6. reset Sleep to engine.next_deadline()  // do NOT busy-poll tick every poll()
//
// TODO(Task 4/5): struct PendingRequest {
//     correlation_id, to, peer, kind, sent_at, attempts, /* payload for retry */
// }
//
// TODO(Task 5+): pub enum Event {
//     RoleChanged { role, term, leader },
//     Committed { entries },
//     MembershipChanged { members },
//     SnapshotInstalled { index },
//     PeerMapped { peer, node },
//     RpcFailed { peer, error },
// }
//
// TODO(Task 5+): pub fn propose(&mut self, data) -> Result<Index, Error>
// TODO(Task 8): pub fn propose_membership(&mut self, change) -> Result<Index, Error>
// TODO(Task 5+): pub fn role(&self) -> Role
// TODO(Task 5+): pub fn commit_index(&self) -> Index
