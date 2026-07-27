//! `RaftBehaviour` — NetworkBehaviour adapter around `RaftEngine`.
//!
//! Phase 2 Task 4: echo shell (no engine yet).

use std::collections::{HashMap, HashSet, VecDeque};
use std::task::{Context, Poll};

use libp2p::core::transport::PortUse;
use libp2p::core::Endpoint;
use libp2p::swarm::behaviour::FromSwarm;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{
    ConnectionDenied, ConnectionId, NetworkBehaviour, NotifyHandler, THandler, THandlerInEvent,
    THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};

use crate::config::RaftConfig;
use crate::error::Error;
use crate::handler::{FromBehaviour, RaftHandler, ToBehaviour};
use crate::peer_map::PeerMap;
use crate::protocol::messages::{RaftMessage, WireEnvelope};
use crate::raft::types::NodeId;

#[derive(Debug)]
pub enum Event {
    Echo(WireEnvelope),
    PeerMapped { peer: PeerId, node: NodeId },
    RpcFailed { peer: PeerId, error: Error },
}

pub struct RaftBehaviour {
    peer_map: PeerMap,
    connected: HashMap<PeerId, HashSet<ConnectionId>>,
    pending_events: VecDeque<ToSwarm<Event, FromBehaviour>>,
    next_correlation_id: u64,
    /// Outbound echo correlation_id → peer (for Failure mapping).
    outbound_peers: HashMap<u64, PeerId>,
}

impl RaftBehaviour {
    pub fn new(config: RaftConfig) -> Self {
        let peer_map = PeerMap::from_seeds(&config.seed_peers);
        let mut pending_events = VecDeque::new();
        for seed in &config.seed_peers {
            pending_events.push_back(ToSwarm::GenerateEvent(Event::PeerMapped {
                peer: seed.peer_id,
                node: seed.node_id,
            }));
        }
        Self {
            peer_map,
            connected: HashMap::new(),
            pending_events,
            next_correlation_id: 1,
            outbound_peers: HashMap::new(),
        }
    }

    pub fn peer_map(&self) -> &PeerMap {
        &self.peer_map
    }

    /// Dial a peer using addresses from PeerMap. No-ops if mapping unknown.
    pub fn dial_seed(&mut self, peer: PeerId) {
        let Some(node) = self.peer_map.node_id(peer) else {
            return;
        };
        let Some(addrs) = self.peer_map.addrs(node).map(|a| a.to_vec()) else {
            return;
        };
        if addrs.is_empty() {
            return;
        }
        self.pending_events.push_back(ToSwarm::Dial {
            opts: DialOpts::peer_id(peer)
                .condition(PeerCondition::DisconnectedAndNotDialing)
                .addresses(addrs)
                .build(),
        });
    }

    pub fn send_echo(&mut self, peer: PeerId, msg: RaftMessage) {
        if !self.connected.contains_key(&peer) {
            self.pending_events.push_back(ToSwarm::GenerateEvent(Event::RpcFailed {
                peer,
                error: Error::Rpc("not connected".into()),
            }));
            return;
        }
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);
        self.outbound_peers.insert(correlation_id, peer);
        self.pending_events.push_back(ToSwarm::NotifyHandler {
            peer_id: peer,
            handler: NotifyHandler::Any,
            event: FromBehaviour::SendRequest {
                correlation_id,
                msg,
            },
        });
    }

}

impl NetworkBehaviour for RaftBehaviour {
    type ConnectionHandler = RaftHandler;
    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(RaftHandler::new())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _addr: &Multiaddr,
        _role_override: Endpoint,
        _port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(RaftHandler::new())
    }

    fn handle_pending_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        _addresses: &[Multiaddr],
        _effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        let Some(peer) = maybe_peer else {
            return Ok(vec![]);
        };
        let Some(node) = self.peer_map.node_id(peer) else {
            return Ok(vec![]);
        };
        Ok(self
            .peer_map
            .addrs(node)
            .map(|a| a.to_vec())
            .unwrap_or_default())
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(e) => {
                self.connected
                    .entry(e.peer_id)
                    .or_default()
                    .insert(e.connection_id);
            }
            FromSwarm::ConnectionClosed(e) => {
                if let Some(set) = self.connected.get_mut(&e.peer_id) {
                    set.remove(&e.connection_id);
                    if set.is_empty() {
                        self.connected.remove(&e.peer_id);
                    }
                }
            }
            FromSwarm::DialFailure(e) => {
                if let Some(peer) = e.peer_id {
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::RpcFailed {
                            peer,
                            error: Error::Rpc(format!("dial failure: {:?}", e.error)),
                        }));
                }
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {
            ToBehaviour::Request {
                correlation_id,
                msg,
                channel_id,
            } => {
                // Echo: reply with the same envelope contents.
                self.pending_events.push_back(ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::One(connection_id),
                    event: FromBehaviour::SendResponse {
                        channel_id,
                        correlation_id,
                        msg: msg.clone(),
                    },
                });
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::Echo(WireEnvelope {
                        correlation_id,
                        msg,
                    })));
            }
            ToBehaviour::Response {
                correlation_id,
                msg,
            } => {
                self.outbound_peers.remove(&correlation_id);
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::Echo(WireEnvelope {
                        correlation_id,
                        msg,
                    })));
            }
            ToBehaviour::Failure {
                correlation_id,
                error,
            } => {
                let peer = correlation_id
                    .and_then(|id| self.outbound_peers.remove(&id))
                    .unwrap_or(peer_id);
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::RpcFailed {
                        peer,
                        error: Error::Rpc(error),
                    }));
            }
        }
    }

    fn poll(&mut self, _cx: &mut Context<'_>) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(ev) = self.pending_events.pop_front() {
            return Poll::Ready(ev);
        }
        Poll::Pending
    }
}
