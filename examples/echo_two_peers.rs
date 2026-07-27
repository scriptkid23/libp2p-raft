//! 2-peer smoke test — dial + Raft RPC path after Behaviour owns the engine.
//! Run: `cargo run --example echo_two_peers`

use std::error::Error;
use std::time::Duration;

use futures::StreamExt;
use libp2p::identity::Keypair;
use libp2p::swarm::SwarmEvent;
use libp2p::{Multiaddr, PeerId, SwarmBuilder};
use libp2p_raft::config::{RaftConfig, SeedPeer};
use libp2p_raft::storage::MemoryStorage;
use libp2p_raft::{Event, RaftBehaviour};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let kp_a = Keypair::generate_ed25519();
    let kp_b = Keypair::generate_ed25519();
    let peer_a = PeerId::from(kp_a.public());
    let peer_b = PeerId::from(kp_b.public());

    let addr_a: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse()?;
    let addr_b: Multiaddr = "/ip4/127.0.0.1/tcp/4002".parse()?;

    let cfg_a = RaftConfig {
        node_id: 1,
        voters: vec![1, 2],
        seed_peers: vec![SeedPeer {
            node_id: 2,
            peer_id: peer_b,
            addrs: vec![addr_b.clone()],
        }],
        election_timeout: Duration::from_millis(400),
        election_jitter: Duration::from_millis(50),
        heartbeat_interval: Duration::from_millis(100),
        rpc_timeout: Duration::from_secs(2),
        rpc_max_retries: 0,
        snapshot_threshold: 10_000,
    };
    let cfg_b = RaftConfig {
        node_id: 2,
        voters: vec![1, 2],
        seed_peers: vec![SeedPeer {
            node_id: 1,
            peer_id: peer_a,
            addrs: vec![addr_a.clone()],
        }],
        election_timeout: Duration::from_millis(600),
        election_jitter: Duration::from_millis(80),
        heartbeat_interval: Duration::from_millis(100),
        rpc_timeout: Duration::from_secs(2),
        rpc_max_retries: 0,
        snapshot_threshold: 10_000,
    };

    let mut swarm_a = SwarmBuilder::with_existing_identity(kp_a)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|_| RaftBehaviour::new(cfg_a, MemoryStorage::new()))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
        .build();

    let mut swarm_b = SwarmBuilder::with_existing_identity(kp_b)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|_| RaftBehaviour::new(cfg_b, MemoryStorage::new()))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
        .build();

    swarm_a.listen_on(addr_a)?;
    swarm_b.listen_on(addr_b)?;

    let mut a_listening = false;
    let mut b_listening = false;
    while !(a_listening && b_listening) {
        tokio::select! {
            ev = swarm_a.select_next_some() => {
                if let SwarmEvent::NewListenAddr { .. } = ev {
                    a_listening = true;
                }
            }
            ev = swarm_b.select_next_some() => {
                if let SwarmEvent::NewListenAddr { .. } = ev {
                    b_listening = true;
                }
            }
        }
    }

    swarm_a.behaviour_mut().dial_seed(peer_b);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut connected = false;

    loop {
        if tokio::time::Instant::now() > deadline {
            return Err("timeout waiting for election RPC path".into());
        }
        tokio::select! {
            ev = swarm_a.select_next_some() => {
                match ev {
                    SwarmEvent::ConnectionEstablished { .. } => {
                        connected = true;
                    }
                    SwarmEvent::Behaviour(Event::RoleChanged { role, term, .. }) => {
                        println!("echo path ok: peer A role={role:?} term={term} connected={connected}");
                        return Ok(());
                    }
                    _ => {}
                }
            }
            ev = swarm_b.select_next_some() => {
                if let SwarmEvent::Behaviour(Event::RoleChanged { role, term, .. }) = ev {
                    println!("echo path ok: peer B role={role:?} term={term}");
                    return Ok(());
                }
            }
        }
    }
}
