use libp2p::{Multiaddr, PeerId};
use libp2p_raft::config::SeedPeer;
use libp2p_raft::peer_map::PeerMap;

#[test]
fn from_seeds_round_trips_lookups() {
    let peer = PeerId::random();
    let addr: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();
    let map = PeerMap::from_seeds(&[SeedPeer {
        node_id: 1,
        peer_id: peer,
        addrs: vec![addr.clone()],
    }]);
    assert_eq!(map.peer_id(1), Some(peer));
    assert_eq!(map.node_id(peer), Some(1));
    assert_eq!(map.addrs(1).unwrap(), &[addr][..]);
}

#[test]
fn unknown_node_returns_none() {
    let map = PeerMap::from_seeds(&[]);
    assert!(map.peer_id(99).is_none());
}
