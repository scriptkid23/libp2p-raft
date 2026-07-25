use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::error::Error;
use crate::protocol::messages::WireEnvelope;

pub fn encode_envelope(env: &WireEnvelope) -> Result<Vec<u8>, Error> {
    let payload = bincode::serialize(env).map_err(|e| Error::Codec(e.to_string()))?;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.write_u32::<BigEndian>(payload.len() as u32)
        .map_err(|e| Error::Codec(e.to_string()))?;
    out.extend_from_slice(&payload);
    Ok(out)
}

pub fn decode_envelope(bytes: &[u8]) -> Result<WireEnvelope, Error> {
    let mut cur = Cursor::new(bytes);
    let len = cur
        .read_u32::<BigEndian>()
        .map_err(|e| Error::Codec(e.to_string()))? as usize;
    let start = cur.position() as usize;
    let end = start + len;
    if bytes.len() < end {
        return Err(Error::Codec("truncated".into()));
    }
    bincode::deserialize(&bytes[start..end]).map_err(|e| Error::Codec(e.to_string()))
}
