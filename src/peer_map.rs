//! NodeId ↔ PeerId + seed Multiaddrs.
//!
//! Plan: Task 4 — used by Behaviour for dial / NotifyHandler routing.
//! Engine never sees PeerId; only Behaviour resolves NodeId via this map.

// TODO(Task 4): pub struct PeerMap { ... }
// TODO(Task 4): insert from seed_peers on startup
// TODO(Task 4): lookup PeerId + addrs by NodeId
// TODO(Task 4): reverse lookup NodeId by PeerId (inbound RPC routing)
// TODO(Task 4): emit PeerMapped when mapping becomes known
