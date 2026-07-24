//! Length-delimited encode/decode for `WireEnvelope`.
//!
//! Plan: Task 1 — `encode_envelope` / `decode_envelope`.
//! Format: [u32 BE len][bincode payload]

// TODO(Task 1): pub fn encode_envelope(env: &WireEnvelope) -> Result<Vec<u8>, Error>
// TODO(Task 1): pub fn decode_envelope(bytes: &[u8]) -> Result<WireEnvelope, Error>
// TODO(Task 1): reject truncated payloads with Error::Codec
