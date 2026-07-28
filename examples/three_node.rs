//! 3-node demo — elect a stable leader over libp2p.
//! Run: `cargo run --example three_node`

use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, Instant};

use futures::future::FutureExt;
use futures::StreamExt;
use libp2p::identity::Keypair;
use libp2p::swarm::SwarmEvent;
use libp2p::{Multiaddr, PeerId, SwarmBuilder};
use libp2p_raft::config::{RaftConfig, SeedPeer};
use libp2p_raft::raft::types::{EntryType, Role};
use libp2p_raft::storage::MemoryStorage;
use libp2p_raft::{Event, RaftBehaviour};
use tracing_subscriber::EnvFilter;

struct Node {
    swarm: libp2p::Swarm<RaftBehaviour<MemoryStorage>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let kp1 = Keypair::generate_ed25519();
    let kp2 = Keypair::generate_ed25519();
    let kp3 = Keypair::generate_ed25519();
    let p1 = PeerId::from(kp1.public());
    let p2 = PeerId::from(kp2.public());
    let p3 = PeerId::from(kp3.public());

    let a1: Multiaddr = "/ip4/127.0.0.1/tcp/4101".parse()?;
    let a2: Multiaddr = "/ip4/127.0.0.1/tcp/4102".parse()?;
    let a3: Multiaddr = "/ip4/127.0.0.1/tcp/4103".parse()?;

    let seeds = |self_id: u64| -> Vec<SeedPeer> {
        [
            (1u64, p1, a1.clone()),
            (2, p2, a2.clone()),
            (3, p3, a3.clone()),
        ]
        .into_iter()
        .filter(|(id, _, _)| *id != self_id)
        .map(|(node_id, peer_id, addr)| SeedPeer {
            node_id,
            peer_id,
            addrs: vec![addr],
        })
        .collect()
    };

    let make_cfg = |id: u64, election_ms: u64, jitter_ms: u64| RaftConfig {
        node_id: id,
        voters: vec![1, 2, 3],
        seed_peers: seeds(id),
        election_timeout: Duration::from_millis(election_ms),
        election_jitter: Duration::from_millis(jitter_ms),
        heartbeat_interval: Duration::from_millis(100),
        rpc_timeout: Duration::from_millis(500),
        rpc_max_retries: 0,
        snapshot_threshold: 10_000,
    };

    let mut n1 = Node {
        swarm: build_swarm(kp1, make_cfg(1, 500, 10), a1.clone())?,
    };
    let mut n2 = Node {
        swarm: build_swarm(kp2, make_cfg(2, 700, 40), a2.clone())?,
    };
    let mut n3 = Node {
        swarm: build_swarm(kp3, make_cfg(3, 900, 70), a3.clone())?,
    };

    wait_listeners(&mut n1, &mut n2, &mut n3).await;

    // Full mesh dial.
    n1.swarm.behaviour_mut().dial_seed(p2);
    n1.swarm.behaviour_mut().dial_seed(p3);
    n2.swarm.behaviour_mut().dial_seed(p1);
    n2.swarm.behaviour_mut().dial_seed(p3);
    n3.swarm.behaviour_mut().dial_seed(p1);
    n3.swarm.behaviour_mut().dial_seed(p2);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let wall = Instant::now();
    let mut roles: HashMap<u64, (Role, u64)> = HashMap::new();
    let mut stable_since: Option<Instant> = None;
    let mut proposed = false;
    let mut committed_nodes: HashMap<u64, bool> = HashMap::new();

    let mut tick = tokio::time::interval(Duration::from_millis(50));
    while wall.elapsed() < Duration::from_secs(20) {
        tokio::select! {
            ev = n1.swarm.select_next_some() => {
                handle_ev(1, ev, &mut roles, &mut stable_since, &mut committed_nodes);
            }
            ev = n2.swarm.select_next_some() => {
                handle_ev(2, ev, &mut roles, &mut stable_since, &mut committed_nodes);
            }
            ev = n3.swarm.select_next_some() => {
                handle_ev(3, ev, &mut roles, &mut stable_since, &mut committed_nodes);
            }
            _ = tick.tick() => {
                drain_ready(1, &mut n1.swarm, &mut roles, &mut stable_since, &mut committed_nodes);
                drain_ready(2, &mut n2.swarm, &mut roles, &mut stable_since, &mut committed_nodes);
                drain_ready(3, &mut n3.swarm, &mut roles, &mut stable_since, &mut committed_nodes);
            }
        }

        roles.insert(1, (n1.swarm.behaviour().role(), n1.swarm.behaviour().current_term()));
        roles.insert(2, (n2.swarm.behaviour().role(), n2.swarm.behaviour().current_term()));
        roles.insert(3, (n3.swarm.behaviour().role(), n3.swarm.behaviour().current_term()));

        if let Some((ok, term)) = consistent_quorum(&roles) {
            if ok {
                if stable_since.is_none() {
                    stable_since = Some(Instant::now());
                    println!("quorum ok term={term}: {:?}", roles);
                }
                if !proposed && stable_since.unwrap().elapsed() >= Duration::from_secs(1) {
                    println!("stable leader for 1s: {:?}", roles);
                    let leader_swarm = match roles.iter().find(|(_, (r, _))| matches!(r, Role::Leader)) {
                        Some((1, _)) => &mut n1.swarm,
                        Some((2, _)) => &mut n2.swarm,
                        Some((3, _)) => &mut n3.swarm,
                        _ => return Err("no leader to propose".into()),
                    };
                    let idx = leader_swarm.behaviour_mut().propose(b"hello".to_vec())?;
                    println!("proposed hello at index={idx}");
                    proposed = true;
                }
            } else {
                stable_since = None;
            }
        } else {
            stable_since = None;
        }

        // Require Event::Committed carrying b"hello" on a majority (not commit_index alone).
        if proposed && committed_nodes.values().filter(|c| **c).count() >= 2 {
            println!(
                "majority Committed hello: nodes={:?} commit_index=[{},{},{}]",
                committed_nodes.keys().collect::<Vec<_>>(),
                n1.swarm.behaviour().commit_index(),
                n2.swarm.behaviour().commit_index(),
                n3.swarm.behaviour().commit_index(),
            );
            return Ok(());
        }
    }

    Err(format!(
        "timeout; roles={roles:?} proposed={proposed} committed={committed_nodes:?}"
    )
    .into())
}

fn build_swarm(
    kp: Keypair,
    cfg: RaftConfig,
    listen: Multiaddr,
) -> Result<libp2p::Swarm<RaftBehaviour<MemoryStorage>>, Box<dyn Error>> {
    let mut swarm = SwarmBuilder::with_existing_identity(kp)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|_| RaftBehaviour::new(cfg, MemoryStorage::new()))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
        .build();
    swarm.listen_on(listen)?;
    Ok(swarm)
}

async fn wait_listeners(n1: &mut Node, n2: &mut Node, n3: &mut Node) {
    let mut ready = [false; 3];
    while !ready.iter().all(|x| *x) {
        tokio::select! {
            ev = n1.swarm.select_next_some() => {
                if let SwarmEvent::NewListenAddr { .. } = ev { ready[0] = true; }
            }
            ev = n2.swarm.select_next_some() => {
                if let SwarmEvent::NewListenAddr { .. } = ev { ready[1] = true; }
            }
            ev = n3.swarm.select_next_some() => {
                if let SwarmEvent::NewListenAddr { .. } = ev { ready[2] = true; }
            }
        }
    }
}

fn handle_ev(
    id: u64,
    ev: SwarmEvent<Event>,
    roles: &mut HashMap<u64, (Role, u64)>,
    stable_since: &mut Option<Instant>,
    committed_nodes: &mut HashMap<u64, bool>,
) {
    match ev {
        SwarmEvent::Behaviour(Event::RoleChanged { role, term, .. }) => {
            println!("node {id} -> {role:?} term={term}");
            roles.insert(id, (role, term));
            *stable_since = None;
        }
        SwarmEvent::Behaviour(Event::Committed { entries }) => {
            if entries.iter().any(|e| {
                matches!(&e.entry_type, EntryType::Command(d) if d == b"hello")
            }) {
                println!("node {id} Committed hello");
                committed_nodes.insert(id, true);
            }
        }
        _ => {}
    }
}

fn drain_ready(
    id: u64,
    swarm: &mut libp2p::Swarm<RaftBehaviour<MemoryStorage>>,
    roles: &mut HashMap<u64, (Role, u64)>,
    stable_since: &mut Option<Instant>,
    committed_nodes: &mut HashMap<u64, bool>,
) {
    while let Some(Some(ev)) = swarm.next().now_or_never() {
        handle_ev(id, ev, roles, stable_since, committed_nodes);
    }
}

/// Returns Some((true, term)) if exactly one Leader + two Followers at same term.
fn consistent_quorum(roles: &HashMap<u64, (Role, u64)>) -> Option<(bool, u64)> {
    if roles.len() < 3 {
        return None;
    }
    let terms: Vec<u64> = roles.values().map(|(_, t)| *t).collect();
    let term = terms[0];
    if terms.iter().any(|t| *t != term) {
        return Some((false, term));
    }
    let leaders = roles
        .values()
        .filter(|(r, _)| matches!(r, Role::Leader))
        .count();
    let followers = roles
        .values()
        .filter(|(r, _)| matches!(r, Role::Follower))
        .count();
    Some((leaders == 1 && followers == 2, term))
}
