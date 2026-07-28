//! `ConnectionHandler` — unary framed RPCs on `/libp2p-raft/1.0.0`.
//!
//! Owns substream open / framed read-write / close. Does NOT own Raft logic.
//! Supports multiple concurrent inbound and outbound unary RPCs.

use std::collections::{HashMap, VecDeque};
use std::task::{Context, Poll};

use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use libp2p::core::upgrade::ReadyUpgrade;
use libp2p::swarm::handler::{
    ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound,
};
use libp2p::swarm::{
    ConnectionHandler, ConnectionHandlerEvent, Stream, StreamProtocol, StreamUpgradeError,
    SubstreamProtocol,
};

use crate::protocol::codec::{decode_envelope, encode_envelope};
use crate::protocol::messages::{RaftMessage, WireEnvelope};
use crate::protocol::upgrade::raft_ready_upgrade;

const MAX_FRAME_BYTES: u32 = 4 * 1024 * 1024;

#[derive(Debug)]
pub enum FromBehaviour {
    SendRequest {
        correlation_id: u64,
        msg: RaftMessage,
    },
    SendResponse {
        channel_id: u64,
        correlation_id: u64,
        msg: RaftMessage,
    },
}

#[derive(Debug)]
pub enum ToBehaviour {
    Request {
        correlation_id: u64,
        msg: RaftMessage,
        channel_id: u64,
    },
    Response {
        correlation_id: u64,
        msg: RaftMessage,
    },
    Failure {
        correlation_id: Option<u64>,
        error: String,
    },
}

#[derive(Debug, Clone)]
pub struct OutboundInfo {
    correlation_id: u64,
    msg: RaftMessage,
}

enum IoOutcome {
    OutboundDone {
        correlation_id: u64,
        msg: RaftMessage,
    },
    InboundRead {
        channel_id: u64,
        stream: Stream,
        env: WireEnvelope,
    },
    InboundWritten {
        channel_id: u64,
    },
    Failed {
        correlation_id: Option<u64>,
        error: String,
    },
}

pub struct RaftHandler {
    inbound_channel_id: u64,
    pending_events: VecDeque<ToBehaviour>,
    pending_outbound: VecDeque<OutboundInfo>,
    inflight: FuturesUnordered<BoxFuture<'static, IoOutcome>>,
    response_txs: HashMap<u64, oneshot::Sender<(u64, RaftMessage)>>,
    outbound_in_flight: usize,
}

impl Default for RaftHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftHandler {
    pub fn new() -> Self {
        Self {
            inbound_channel_id: 1,
            pending_events: VecDeque::new(),
            pending_outbound: VecDeque::new(),
            inflight: FuturesUnordered::new(),
            response_txs: HashMap::new(),
            outbound_in_flight: 0,
        }
    }

    fn alloc_channel_id(&mut self) -> u64 {
        let id = self.inbound_channel_id;
        self.inbound_channel_id = self.inbound_channel_id.wrapping_add(1);
        id
    }

    fn push_outbound_io(&mut self, mut stream: Stream, info: OutboundInfo) {
        let correlation_id = info.correlation_id;
        let req = WireEnvelope {
            correlation_id: info.correlation_id,
            msg: info.msg,
        };
        self.outbound_in_flight += 1;
        self.inflight.push(Box::pin(async move {
            if let Err(e) = write_frame(&mut stream, &req).await {
                return IoOutcome::Failed {
                    correlation_id: Some(correlation_id),
                    error: e,
                };
            }
            match read_frame(&mut stream).await {
                Ok(env) => IoOutcome::OutboundDone {
                    correlation_id: env.correlation_id,
                    msg: env.msg,
                },
                Err(e) => IoOutcome::Failed {
                    correlation_id: Some(correlation_id),
                    error: e,
                },
            }
        }));
    }

    fn push_inbound_read(&mut self, mut stream: Stream) {
        let channel_id = self.alloc_channel_id();
        self.inflight.push(Box::pin(async move {
            match read_frame(&mut stream).await {
                Ok(env) => IoOutcome::InboundRead {
                    channel_id,
                    stream,
                    env,
                },
                Err(e) => IoOutcome::Failed {
                    correlation_id: None,
                    error: e,
                },
            }
        }));
    }

    fn push_inbound_write(
        &mut self,
        mut stream: Stream,
        channel_id: u64,
        request_correlation_id: u64,
        rx: oneshot::Receiver<(u64, RaftMessage)>,
    ) {
        self.inflight.push(Box::pin(async move {
            let (corr, resp_msg) = match rx.await {
                Ok(v) => v,
                Err(_) => {
                    return IoOutcome::Failed {
                        correlation_id: Some(request_correlation_id),
                        error: "inbound response cancelled".into(),
                    };
                }
            };
            let env = WireEnvelope {
                correlation_id: corr,
                msg: resp_msg,
            };
            match write_frame(&mut stream, &env).await {
                Ok(()) => IoOutcome::InboundWritten { channel_id },
                Err(e) => IoOutcome::Failed {
                    correlation_id: Some(request_correlation_id),
                    error: e,
                },
            }
        }));
    }
}

impl ConnectionHandler for RaftHandler {
    type FromBehaviour = FromBehaviour;
    type ToBehaviour = ToBehaviour;
    type InboundProtocol = ReadyUpgrade<StreamProtocol>;
    type OutboundProtocol = ReadyUpgrade<StreamProtocol>;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = OutboundInfo;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(raft_ready_upgrade(), ())
    }

    fn connection_keep_alive(&self) -> bool {
        // Phase 2: keep connections alive across unary RPCs / heartbeats.
        true
    }

    fn on_behaviour_event(&mut self, event: FromBehaviour) {
        match event {
            FromBehaviour::SendRequest {
                correlation_id,
                msg,
            } => {
                self.pending_outbound.push_back(OutboundInfo {
                    correlation_id,
                    msg,
                });
            }
            FromBehaviour::SendResponse {
                channel_id,
                correlation_id,
                msg,
            } => {
                if let Some(tx) = self.response_txs.remove(&channel_id) {
                    let _ = tx.send((correlation_id, msg));
                } else {
                    self.pending_events.push_back(ToBehaviour::Failure {
                        correlation_id: Some(correlation_id),
                        error: format!("unknown inbound channel_id {channel_id}"),
                    });
                }
            }
        }
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: stream,
                ..
            }) => {
                self.push_inbound_read(stream);
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: stream,
                info,
            }) => {
                self.push_outbound_io(stream, info);
            }
            ConnectionEvent::DialUpgradeError(DialUpgradeError { info, error }) => {
                let err = match error {
                    StreamUpgradeError::Timeout => "outbound upgrade timeout".into(),
                    StreamUpgradeError::Apply(_) => "outbound upgrade apply error".into(),
                    StreamUpgradeError::NegotiationFailed => {
                        "outbound protocol negotiation failed".into()
                    }
                    StreamUpgradeError::Io(e) => format!("outbound upgrade io: {e}"),
                };
                self.pending_events.push_back(ToBehaviour::Failure {
                    correlation_id: Some(info.correlation_id),
                    error: err,
                });
            }
            _ => {}
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        loop {
            if let Some(ev) = self.pending_events.pop_front() {
                return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(ev));
            }

            if let Some(info) = self.pending_outbound.pop_front() {
                return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                    protocol: SubstreamProtocol::new(raft_ready_upgrade(), info),
                });
            }

            match self.inflight.poll_next_unpin(cx) {
                Poll::Ready(Some(outcome)) => {
                    match outcome {
                        IoOutcome::OutboundDone {
                            correlation_id,
                            msg,
                        } => {
                            self.outbound_in_flight =
                                self.outbound_in_flight.saturating_sub(1);
                            self.pending_events.push_back(ToBehaviour::Response {
                                correlation_id,
                                msg,
                            });
                        }
                        IoOutcome::InboundRead {
                            channel_id,
                            stream,
                            env,
                        } => {
                            let (tx, rx) = oneshot::channel();
                            self.response_txs.insert(channel_id, tx);
                            let correlation_id = env.correlation_id;
                            let msg = env.msg;
                            self.push_inbound_write(stream, channel_id, correlation_id, rx);
                            self.pending_events.push_back(ToBehaviour::Request {
                                correlation_id,
                                msg,
                                channel_id,
                            });
                        }
                        IoOutcome::InboundWritten { channel_id } => {
                            self.response_txs.remove(&channel_id);
                        }
                        IoOutcome::Failed {
                            correlation_id,
                            error,
                        } => {
                            if correlation_id.is_some() {
                                self.outbound_in_flight =
                                    self.outbound_in_flight.saturating_sub(1);
                            }
                            self.pending_events.push_back(ToBehaviour::Failure {
                                correlation_id,
                                error,
                            });
                        }
                    }
                    continue;
                }
                Poll::Ready(None) | Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }
}

async fn write_frame(stream: &mut Stream, env: &WireEnvelope) -> Result<(), String> {
    let bytes = encode_envelope(env).map_err(|e| e.to_string())?;
    if bytes.len() > (MAX_FRAME_BYTES as usize) + 4 {
        return Err("encoded frame too large".into());
    }
    stream
        .write_all(&bytes)
        .await
        .map_err(|e| format!("write: {e}"))?;
    stream.flush().await.map_err(|e| format!("flush: {e}"))?;
    Ok(())
}

async fn read_frame(stream: &mut Stream) -> Result<WireEnvelope, String> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("read len: {e}"))?;
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME_BYTES {
        return Err(format!("frame too large: {len}"));
    }
    let mut payload = vec![0u8; len as usize];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| format!("read payload: {e}"))?;
    let mut full = Vec::with_capacity(4 + payload.len());
    full.extend_from_slice(&len_buf);
    full.extend_from_slice(&payload);
    decode_envelope(&full).map_err(|e| e.to_string())
}
