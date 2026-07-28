# Phase 3 — AppendEntries Replication + Propose + Commit

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Leaders can `propose` commands; logs replicate via AppendEntries; majority match advances `commit_index`; followers apply committed entries; `examples/three_node.rs` proposes one command and observes `Event::Committed` on a majority of nodes.

**Architecture:** Extend pure `RaftEngine` (still no libp2p types). Unify leader AE construction into one per-peer builder (heartbeat = empty entries when caught up). Followers run prev_log check on **every** AE (including empty). Commit uses last-new-entry index, not local last_index after an untruncated longer log. Leader ignores follower-reported match on success and derives match from its own request record. Simple next_index decrement with stale-response guards.

**Tech Stack:** Same as Phase 2 (libp2p 0.54, MemoryStorage, existing Handler/Behaviour RPC path)

**Spec:** `docs/superpowers/specs/2026-07-23-libp2p-raft-design.md` §5–§7 Phase 3  
**Parent plan:** `docs/superpowers/plans/2026-07-23-libp2p-raft.md` Task 6  
**Depends on:** Phase 2 done

**Review policy:** Before each task’s implementation, run ai-router with `claude` + `gemini`. If Gemini returns no review (session flake), retry once; if still empty, proceed with Claude findings and note the gap. Incorporate Critical/Important before coding.

**Provider review (plan draft):** Claude — Approve with changes (6/10). Gemini — failed to ingest plan (2 attempts). Patches below from Claude.

## Global Constraints

- DIY Raft only; engine never imports libp2p types
- `Storage::persist` is the only write path for hard state + log appends (truncate+append before AE response)
- Single AE builder on leader: `build_append_entries(peer)` from `next_index[peer]`; heartbeat tick uses same path
- Every follower AE (empty or not) runs prev_log check; reset election deadline on valid leader term even if prev_log fails
- On AE success: `match_index` / resp use **last new entry** = `prev_log_index + entries.len()` (not follower `storage.last_index()` if longer stale suffix)
- Commit rule: majority `match_index >= N` **and** `log[N].term == current_term`
- Stale AE resp: ignore if not Leader / wrong term; on reject only decrement if `next_index` still equals `req.prev+1`
- Truncate only at first conflicting term; identical prefix must not truncate
- No election no-op entry in Phase 3 — document Fig. 8 limitation (prior-term-only leader cannot commit until a current-term propose)
- Tick / Sleep / no vote RPC retries — keep Phase 2 Behaviour invariants; `propose()` must wake Behaviour waker
- Connection drop ≠ remove voter; next/match keyed by configured voters
- Out of scope: snapshots, membership, YAML config
- Comments/commits in English; TDD for engine

## Current state

| Area | Status |
|------|--------|
| Election + empty AE heartbeats | Done (Phase 2 empty path bypasses prev_log — **must change**) |
| `next_index` / `match_index` on become_leader | Partially present; verify re-init each election |
| Non-empty AE / propose / commit / Committed | Missing |

## File map

| Path | Responsibility |
|------|----------------|
| `src/raft/engine.rs` | unified AE, propose, commit, apply |
| `src/raft/log.rs` | helpers |
| `src/behaviour.rs` | propose + waker; Event::Committed |
| `tests/engine_replication.rs` | replication tests |
| `tests/engine_heartbeat_reset.rs` | rewrite empty AE to require matching prev |
| `examples/three_node.rs` | propose + majority Committed |
| `docs/phase-3.md` | checklist + Fig. 8 note |
| `README.md` | status |

---

### Task 1: Follower AE (unified prev_log) + commit/Apply

**Files:** `src/raft/engine.rs`, `tests/engine_replication.rs`, `tests/engine_heartbeat_reset.rs`

**Rules:**

1. Term `< current` → reject, **no** deadline reset  
2. Term `>= current` → step down if needed; set leader; **always** reset election deadline (even if prev_log fails)  
3. prev_log check (incl. empty AE): `prev_log_index==0` ⇒ `prev_log_term==0`; else local entry must exist with matching term → else `success:false`  
4. On success: for each entry at `prev+1+i`, skip if same term; else `truncate_from(idx)` then append remainder via one `persist`  
5. `last_new = prev_log_index + entries.len()`  
6. `commit_index = max(commit_index, min(leader_commit, last_new))`  
7. Emit `Action::Apply` for `(last_applied, commit_index]` in order; `last_applied = commit_index`  
8. Resp: `success:true, match_index: last_new`

**Tests:**

- `follower_appends_entries_and_advances_commit`
- `follower_rejects_prev_log_mismatch`
- `follower_duplicate_prefix_does_not_truncate` (identical AE twice)
- Rewrite heartbeat tests: empty AE with matching prev accepted; mismatched prev rejected **but** deadline still reset if term ok

- [ ] Steps: failing tests → implement → `cargo test --test engine_replication --test engine_heartbeat_reset` → commit  
  `feat: follower AppendEntries with prev_log, conditional truncate, last_new commit`

**ai-router gate:** claude + gemini on Task 1 diff before Task 2.

---

### Task 2: Leader propose + replicate + AE resp / commit

**Files:** `src/raft/engine.rs`, `tests/engine_replication.rs`

**become_leader (explicit):** for each voter ≠ self: `next_index = last+1`, `match_index = 0`. Self implicitly matches `last_index` in `maybe_commit`.

**API:**

```rust
pub fn propose(&mut self, data: Vec<u8>) -> Result<(Index, Vec<Action>), RaftError>;
```

NotLeader → Err. Persist new `Command` entry. Return replicate `Send`s from unified builder.

**Pending outbound AE record** (for resp correlation): per in-flight or per-peer last sent `{prev_log_index, entries_len}` used when handling resp.

**build_append_entries(peer):** prev = next_index[peer]-1; entries = log[next_index..=last] (empty if caught up). Heartbeat tick: Send per peer via this builder (delete identical Broadcast-only empty AE path).

**On AppendEntriesResp:**

```text
if term > current → step down
if not Leader or term != current → ignore
if success:
  match_index[p] = max(match_index[p], req.prev + req.entries_len)
  next_index[p] = match_index[p] + 1
  maybe_commit() → Apply
else:
  if next_index[p] != req.prev + 1 → ignore stale reject
  next_index[p] = max(match_index[p]+1, req.prev).max(1)  // or prev after decrement policy
  // plan default: next = max(match+1, next-1).max(1) only when not stale
  re-Send AE
```

**maybe_commit:** scan N from last down to commit+1; require `log[N].term == current_term` and count(match_index >= N including self) >= quorum; set commit; Apply `(last_applied, commit]`.

**Tests:**

- `leader_appends_and_commits_after_majority_match`
- `follower_rejects_mismatch_leader_decrements_next_index`
- `prior_term_entries_alone_do_not_commit` (Fig. 8)
- `propose_on_follower_returns_not_leader`
- `become_leader_reinitializes_next_match_index`

- [ ] Commit: `feat: leader propose, replicate, and majority commit`

**ai-router gate** before Task 3.

---

### Task 3: Behaviour propose + Event::Committed + waker

**Files:** `src/behaviour.rs`, `src/lib.rs`

```rust
pub enum Event {
    RoleChanged { .. },
    Committed { entries: Vec<LogEntry> },
    PeerMapped { .. },
    RpcFailed { .. },
}

pub fn propose(&mut self, data: Vec<u8>) -> Result<Index, Error>;
pub fn commit_index(&self) -> Index;
```

- Store `Option<Waker>`; `propose` enqueues engine actions via `execute_actions`, then `wake`
- `Action::Apply` → `Event::Committed` (no drop/reorder)
- One outbox convention (existing `pending_events`)

- [ ] Commit: `feat: Behaviour propose and Event::Committed`

**ai-router gate** before Task 4.

---

### Task 4: three_node propose + majority Committed

After stability window:

1. Leader `propose(b"hello")`
2. Collect `Event::Committed` from nodes; require **≥2** nodes see same index+payload (ideally 3)
3. `tokio::time::timeout`; non-zero exit on failure
4. Optional: follower propose → NotLeader

- [ ] Commit: `feat: three_node proposes and waits for majority Committed`

**ai-router gate** if flaky.

---

### Task 5: Docs

`docs/phase-3.md` + README. Document: no election no-op; prior-term-only commit blocked until current-term propose (Fig. 8).

- [ ] Commit: `docs: Phase 3 replication checklist`

---

## Verification gate

```bash
cargo test
cargo run --example echo_two_peers
cargo run --example three_node
```

## Changelog vs first draft (Claude review)

| Change | Why |
|--------|-----|
| last_new for match/commit | Avoid committing unsent suffix |
| Conditional truncate | Protect committed prefix |
| Unified AE + prev_log on empty | Heartbeat cannot skip log check |
| become_leader re-init explicit | Defined volatile state |
| Stale AE resp guards | Reordering safety |
| Behaviour waker on propose | Immediate send |
| three_node majority Committed | Stronger acceptance |
| Fig. 8 test + docs | Commit rule completeness |

## Out of scope

Snapshots, membership, pipelining beyond unary RPC, disk persistence, election no-op entry
