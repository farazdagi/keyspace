use {
    keyspace::{DefaultReplicationStrategy, KeyspaceBuilder, KeyspaceError, Node},
    std::{collections::HashMap, hash::BuildHasher},
};

#[derive(Hash, Clone)]
struct SimpleNode {
    id: String,
}

impl Node for SimpleNode {
    type NodeId = String;

    fn id(&self) -> &Self::NodeId {
        &self.id
    }
}

impl SimpleNode {
    fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }
}

#[test]
fn keyspace_builder() {
    let init_nodes = vec![
        SimpleNode::new("node1"),
        SimpleNode::new("node2"),
        SimpleNode::new("node3"),
    ];
    const RF: usize = 4;

    {
        // Default keyspace (DefaultReplicationStrategy + RF=3)
        let mut init_nodes = init_nodes.clone();
        let keyspace = KeyspaceBuilder::new(init_nodes.clone()).build();
        assert!(keyspace.is_ok());

        init_nodes.pop(); // only 2 nodes left
        let keyspace = KeyspaceBuilder::new(init_nodes).build();
        assert_eq!(keyspace.err(), Some(KeyspaceError::NotEnoughNodes(3)));
    }

    {
        // Default replication strategy
        let keyspace = KeyspaceBuilder::new(init_nodes.clone())
            .with_replication_factor::<3>()
            .build();
        assert!(keyspace.is_ok());

        let keyspace = KeyspaceBuilder::new(init_nodes.clone())
            .with_replication_factor::<RF>()
            .build();
        assert_eq!(keyspace.err(), Some(KeyspaceError::NotEnoughNodes(RF)));
    }

    {
        // Default replication factor
        let keyspace = KeyspaceBuilder::new(init_nodes.clone())
            .with_replication_strategy(DefaultReplicationStrategy::new())
            .build();
        assert!(keyspace.is_ok());

        let mut init_nodes = init_nodes.clone();
        init_nodes.pop();
        let keyspace = KeyspaceBuilder::new(init_nodes)
            .with_replication_strategy(DefaultReplicationStrategy::new())
            .build();
        assert_eq!(keyspace.err(), Some(KeyspaceError::NotEnoughNodes(3)));
    }

    {
        // Explicit replication strategy and factor
        let keyspace = KeyspaceBuilder::new(init_nodes.clone())
            .with_replication_factor::<RF>()
            .with_replication_strategy(DefaultReplicationStrategy::new())
            .build();
        assert_eq!(keyspace.err(), Some(KeyspaceError::NotEnoughNodes(RF)));

        // Explicit replication strategy and factor (different order)
        let keyspace = KeyspaceBuilder::new(init_nodes.clone())
            .with_replication_strategy(DefaultReplicationStrategy::new())
            .with_replication_factor::<RF>()
            .build();
        assert_eq!(keyspace.err(), Some(KeyspaceError::NotEnoughNodes(RF)));
    }
}

#[test]
fn replica_set_fair_distribution() {
    let init_nodes = (0..10).map(|i| format!("node{}", i)).collect::<Vec<_>>();

    let keyspace = KeyspaceBuilder::new(init_nodes)
        .with_replication_factor::<1>()
        .build()
        .expect("Failed to create keyspace");

    let key_replica_pairs = vec![
        (0x0000_5678_9012_3456, vec!["node1"]),
        (0x0000_FFFF_9012_3456, vec!["node1"]),
        (0x0001_FFFF_9012_3456, vec!["node0"]),
        (0x0002_FFFF_9012_3456, vec!["node1"]),
        (0x0002_00FF_9012_3456, vec!["node1"]),
        (0x1234_5678_9012_3456, vec!["node3"]),
        (0x1234_2678_9012_3456, vec!["node3"]),
    ];
    for (key, expected_replicas) in key_replica_pairs {
        let replicas = keyspace.replicas(key).collect::<Vec<_>>();
        assert_eq!(replicas, expected_replicas);
    }

    let hasher = std::hash::RandomState::new();

    // Hash all u16 numbers into keys.
    // Get replica for each key, count how many keys landed on each replica.
    let mut replica_count = HashMap::<&String, usize>::new();
    for i in 0..=u16::MAX {
        let key = hasher.hash_one(i);
        let replicas = keyspace.replicas(key).collect::<Vec<_>>();
        let entry = replica_count.entry(replicas[0]).or_insert(0);
        *entry += 1;
    }

    // Ensure that min and max replica counts are within 8% of each other.
    let min = *replica_count.values().min().unwrap();
    let max = *replica_count.values().max().unwrap();
    let diff = max - min;
    let threshold = max - (max as f64 * 0.92) as usize;
    assert!(
        diff <= threshold,
        "Replica count difference is too high: {diff} > {threshold}"
    );
}
