use libp2p_raft::cluster_config::ClusterConfig;

#[test]
fn local_cluster_has_three_nodes() {
    let cfg = ClusterConfig::load("config/cluster.local.toml".as_ref()).unwrap();
    assert_eq!(cfg.voters, vec![1, 2, 3]);
    assert_eq!(cfg.node_ids(), vec![1, 2, 3]);
    let seeds = cfg.seed_peers(1).unwrap();
    assert_eq!(seeds.len(), 2);
    assert!(seeds.iter().any(|s| s.node_id == 2));
}

#[test]
fn docker_cluster_has_six_nodes() {
    let cfg = ClusterConfig::load("config/cluster.docker.toml".as_ref()).unwrap();
    assert_eq!(cfg.voters.len(), 6);
    assert_eq!(cfg.node_ids().len(), 6);
    assert_eq!(cfg.propose_hello_node, Some(1));
    let n6 = cfg.nodes.iter().find(|n| n.id == 6).unwrap();
    assert_eq!(n6.host, "172.28.0.16");
    assert_eq!(n6.port, 4106);
}
