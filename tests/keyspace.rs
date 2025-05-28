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
        ("key1", vec!["node7"]),
        ("key2", vec!["node8"]),
        ("key3", vec!["node6"]),
        ("key4", vec!["node0"]),
        ("key5", vec!["node1"]),
        ("key6", vec!["node5"]),
        ("key7", vec!["node5"]),
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
    // is correct. The key is selected so that it lands on the new node.
    let key = 5543511230694967434u64;
    let old_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    assert_eq!(old_replicas, ["node52", "node4", "node35"]);

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
    assert_eq!(new_replicas, [&new_node, "node52", "node4"]);

    // Check that the migration plan is correct.
    assert_eq!(pull_intervals.len(), 3078);
    let interval = pull_intervals.first().unwrap();
    assert_eq!(
        interval.key_range(),
        &KeyRange::new(7036874417766400, Some(7318349394477056))
    );
    assert_eq!(interval.nodes(), &vec!["node9", "node28", "node35"]);
}

#[test]
fn remove_node_migration_plan() {
    // Populate the keyspace with nodes.
    // Then remove a node and check migration plan.
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
    // Then remove one of nodes on which the key is replicated.
    let key = 3705152965598471701u64;
    let old_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    assert_eq!(old_replicas, ["node20", "node45", "node59"]);

    let removed_node = "node45".to_string();
    let migrations = keyspace
        .remove_node(&removed_node)
        .expect("Failed to remove node");

    // Node was removed, it was part of multiple shards, so at least those shards
    // are updated.
    assert_eq!(migrations.keys().len(), 63);

    let pull_intervals = migrations
        .pull_intervals(&removed_node)
        .map(|interval| interval)
        .collect::<Vec<_>>();
    // Node is removed, so there should be no pull intervals for it.
    assert_eq!(pull_intervals.len(), 0);

    let new_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    assert_eq!(new_replicas, ["node20", "node59", "node12"]);

    let pull_intervals = migrations
        .pull_intervals(&"node35".to_string())
        .map(|interval| interval)
        .collect::<Vec<_>>();

    // Check that the migration plan is correct.
    assert_eq!(pull_intervals.len(), 49);
    let interval = pull_intervals.first().unwrap();
    // Removed node is still source of data.
    assert_eq!(interval.nodes(), &vec!["node25", "node45", "node8"]);
    assert_eq!(
        interval.key_range(),
        &KeyRange::new(656681120665960448, Some(656962595642671104))
    );
}

#[test]
fn update_nodes_migration_plan() {
    // Populate the keyspace with nodes.
    const MAX_NODES: usize = 32;
    let init_nodes = (0..MAX_NODES)
        .map(|i| format!("node{}", i))
        .collect::<Vec<_>>();

    // Create a keyspace with the initial nodes and replication factor of 3.
    let mut keyspace = KeyspaceBuilder::new(init_nodes)
        .with_replication_factor::<3>()
        .build()
        .expect("Failed to create keyspace");

    // For a given key, obtain current replicas.
    // Then set new nodes, check out new replicas, and make sure that migration plan
    // is correct. The key is selected so that it lands on the new node.
    let key = 3705152965598471701u64;
    let old_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    assert_eq!(old_replicas, ["node20", "node12", "node29"]);

    // New nodes to add.
    let new_nodes = (0..16)
        .map(|i| format!("new_node{}", i))
        .collect::<Vec<_>>();
    // Nodes to remove.
    let removed_nodes = (0..16)
        .map(|i| format!("node{}", i + 16))
        .collect::<Vec<_>>();

    // After update, 'node0..node15' and `new_node0..new_node15` should be in the
    // keyspace.
    let migrations = keyspace
        .update_nodes(new_nodes.clone(), removed_nodes.iter())
        .expect("Failed to set nodes");

    // New nodes are added, so we expect migration plan to contain all of them.
    let node_ids = migrations.keys().collect::<Vec<_>>();
    for node in &new_nodes {
        assert!(node_ids.contains(&node));
    }

    // Removed nodes are not in the migration plan.
    for node in &removed_nodes {
        assert!(!node_ids.contains(&node));
    }

    // Non-removed nodes are still in the migration plan.
    for i in 0..16 {
        let node = format!("node{}", i);
        assert!(node_ids.contains(&&node));
    }

    assert_eq!(migrations.keys().len(), 32);

    let pull_intervals = migrations
        .pull_intervals(&"new_node0".to_string())
        .map(|interval| interval)
        .collect::<Vec<_>>();

    let new_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    // node12 remains, and node20 and node29 were replaced.
    assert_eq!(new_replicas, ["node12", "node1", "node5"]);

    // Check that the migration plan is correct.
    assert_eq!(pull_intervals.len(), 6089);
    let interval = pull_intervals.first().unwrap();
    assert_eq!(
        interval.key_range(),
        &KeyRange::new(1970324836974592, Some(2251799813685248))
    );
    assert_eq!(interval.nodes(), &vec!["node11", "node17", "node28"]);
}
