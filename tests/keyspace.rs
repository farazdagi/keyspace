use {
    keyspace::{DefaultReplicationStrategy, KeyRange, KeyspaceBuilder, KeyspaceError},
    std::{collections::HashMap, hash::BuildHasher},
};

#[test]
fn keyspace_builder() {
    let init_nodes = vec!["node1", "node2", "node3"];
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

    // Up until nodes are added or removed, the version is 0.
    assert_eq!(keyspace.version(), 0);

    let key_replica_pairs = vec![
        ("key1", vec!["node0"]),
        ("key2", vec!["node1"]),
        ("key3", vec!["node9"]),
        ("key4", vec!["node9"]),
        ("key5", vec!["node3"]),
        ("key6", vec!["node2"]),
        ("key7", vec!["node6"]),
    ];
    for (key, expected_replicas) in key_replica_pairs {
        let replicas = keyspace.replicas(&key).collect::<Vec<_>>();
        assert_eq!(replicas, expected_replicas);
    }

    let hasher = std::hash::RandomState::new();

    // Hash all u16 numbers into keys.
    // Get replica for each key, count how many keys landed on each replica.
    let mut replica_count = HashMap::<_, usize>::new();
    for i in 0..=u16::MAX {
        let key = hasher.hash_one(i);
        let replicas = keyspace.replicas(&key).collect::<Vec<_>>();
        let entry = replica_count.entry(replicas[0].clone()).or_insert(0);
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

#[test]
fn add_node_migration_plan() {
    // Populate the keyspace with nodes.
    // Then add a new node and check migration plan.
    const MAX_NODES: usize = 64;
    let init_nodes = (0..MAX_NODES)
        .map(|i| format!("node{}", i))
        .collect::<Vec<_>>();

    // Create a keyspace with the initial nodes and replication factor of 3.
    let mut keyspace = KeyspaceBuilder::new(init_nodes)
        .with_replication_factor::<3>()
        .build()
        .expect("Failed to create keyspace");

    // For a given key, obtain current replicas.
    // Then add new node, check out new replicas, and make sure that migration plan
    // is correct. The key is selected so that it it lands on the new node.
    let key = 3705152965598471701u64;
    let old_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    assert_eq!(old_replicas, ["node52", "node22", "node23"]);

    let new_node = format!("node{}", MAX_NODES);
    let migrations = keyspace
        .add_node(new_node.clone())
        .expect("Failed to add node");

    // Only one node is added, so we expect migration plan to contain only one
    // target node (node into which data is pulled).
    assert_eq!(migrations.keys().len(), 1);

    let pull_intervals = migrations
        .pull_intervals(&new_node)
        .map(|interval| interval)
        .collect::<Vec<_>>();

    let new_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    assert_eq!(new_replicas, ["node52", &new_node, "node22"]);

    // Check that the migration plan is correct.
    assert_eq!(pull_intervals.len(), 3016);
    let interval = pull_intervals.first().unwrap();
    assert_eq!(
        interval.key_range(),
        &KeyRange::new(19984723346456576, Some(20266198323167232))
    );
    assert_eq!(interval.nodes(), &vec!["node38", "node59", "node49"]);
}
