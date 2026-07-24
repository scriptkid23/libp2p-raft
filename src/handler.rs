//! `ConnectionHandler` — unary framed RPCs on `/libp2p-raft/1.0.0`.
//!
//! Plan: Task 4.
//! Owns substream open / framed read-write / close. Does NOT own Raft logic.
//!
//! Support multiple concurrent substreams (e.g. FuturesUnordered) so inbound+outbound
//! do not deadlock.

// TODO(Task 4): Handler state machine:
//     Idle | OpenOutbound | SendWrite | ReadResponse
//     | OpenInbound | ReadRequest | WriteResponse | Failed
//
// TODO(Task 4): Behaviour → Handler:
//     SendRequest { correlation_id, msg }
//     SendResponse { channel_id, correlation_id, msg }
//
// TODO(Task 4): Handler → Behaviour:
//     Request { peer, correlation_id, msg, channel_id }
//     Response { peer, correlation_id, msg }
//     Failure { peer, correlation_id, err }  // not Raft peer-dead
//
// TODO(Task 4): frame with protocol::codec encode/decode on each unary substream
