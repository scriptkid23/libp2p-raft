use libp2p_raft::protocol::{decode_envelope, encode_envelope, RaftMessage, WireEnvelope};

#[test]
fn request_vote_roundtrip() {
    let env = WireEnvelope {
        correlation_id: 42,
        msg: RaftMessage::RequestVote {
            term: 1,
            candidate_id: 7,
            last_log_index: 0,
            last_log_term: 0,
        },
    };
    let bytes = encode_envelope(&env).expect("encode");
    let got = decode_envelope(&bytes).expect("decode");
    assert_eq!(got.correlation_id, 42);
    match got.msg {
        RaftMessage::RequestVote {
            term,
            candidate_id,
            ..
        } => {
            assert_eq!(term, 1);
            assert_eq!(candidate_id, 7);
        }
        other => panic!("unexpected {other:?}"),
    }
}
