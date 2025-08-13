use {
    keyspace::{
        DefaultReplicationStrategy,
        KeyRange,
        KeyspaceBuilder,
        KeyspaceError,
        KeyspaceNode,
        ReplicationStrategy,
    },
    std::{
        collections::{HashMap, HashSet},
        hash::{BuildHasher, Hash},
    },
};

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct Node(String);

impl KeyspaceNode for Node {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.0
    }
}

impl Node {
    /// Creates a new node from a string identifier.
    pub fn new(id: &str) -> Self {
        Node(id.to_string())
    }
}

#[test]
fn keyspace_builder() {
    let init_nodes = (0..3)
        .map(|i| Node::new(&format!("node{}", i)))
        .collect::<Vec<_>>();
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
    let init_nodes = (0..10)
        .map(|i| Node::new(&format!("node{}", i)))
        .collect::<Vec<_>>();

    let keyspace = KeyspaceBuilder::new(init_nodes)
        .with_replication_factor::<1>()
        .build()
        .expect("Failed to create keyspace");

    // Up until nodes are added or removed, the version is 0.
    assert_eq!(keyspace.version(), 0);

    let key_replica_pairs = vec![
        ("key1", "node7"),
        ("key2", "node0"),
        ("key3", "node6"),
        ("key4", "node0"),
        ("key5", "node6"),
        ("key6", "node5"),
        ("key7", "node8"),
        ("key8", "node1"),
        ("key9", "node4"),
        ("key10", "node9"),
    ];
    for (key, expected_replica) in key_replica_pairs {
        let replicas = keyspace.replicas(&key).collect::<Vec<_>>();
        assert_eq!(replicas.len(), 1);
        assert_eq!(
            replicas[0].id(),
            expected_replica,
            "Replica for key '{key}' should be '{expected_replica}'"
        );
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

    // Ensure that min and max replica counts are within 7% of each other.
    let min = *replica_count.values().min().unwrap();
    let max = *replica_count.values().max().unwrap();
    let diff = max - min;
    let threshold = max - (max as f64 * 0.93) as usize;
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
        .map(|i| Node::new(&format!("node{}", i)))
        .collect::<Vec<_>>();

    // Create a keyspace with the initial nodes and replication factor of 3.
    let mut keyspace = KeyspaceBuilder::new(init_nodes)
        .with_replication_factor::<3>()
        .build()
        .expect("Failed to create keyspace");
    assert_eq!(keyspace.version(), 0);

    // For a given key, obtain current replicas.
    // Then add new node, check out new replicas, and make sure that migration plan
    // is correct. The key is selected so that it lands on the new node.
    let key = 1755092165295214000u64;
    let old_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    assert_eq!(
        old_replicas,
        ["node46", "node63", "node54"]
            .into_iter()
            .map(Node::new)
            .collect::<Vec<_>>()
    );

    let new_node = Node::new(&format!("node{}", MAX_NODES));
    let migrations = keyspace
        .add_node(new_node.clone())
        .expect("Failed to add node");
    assert_eq!(keyspace.version(), 1);
    assert_eq!(keyspace.version(), migrations.version());

    // Only one node is added, so we expect migration plan to contain only one
    // target node (node into which data is pulled).
    assert_eq!(migrations.keys().len(), 1);

    let pull_intervals = migrations
        .pull_intervals(new_node.id())
        .map(|interval| interval)
        .collect::<Vec<_>>();

    let new_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    assert_eq!(
        new_replicas,
        ["node46", new_node.id(), "node63"]
            .into_iter()
            .map(Node::new)
            .collect::<Vec<_>>()
    );

    // Check that the migration plan is correct.
    assert_eq!(pull_intervals.len(), 2978);
    let interval = pull_intervals.first().unwrap();
    assert_eq!(
        interval.key_range(),
        &KeyRange::new(2814749767106560, Some(3096224743817216))
    );
    assert_eq!(
        interval.nodes(),
        &vec!["node60", "node57", "node30"]
            .into_iter()
            .map(Node::new)
            .collect::<Vec<_>>()
    );
}

#[test]
fn remove_node_migration_plan() {
    // Populate the keyspace with nodes.
    // Then remove a node and check migration plan.
    const MAX_NODES: usize = 64;
    let init_nodes = (0..MAX_NODES)
        .map(|i| Node::new(&format!("node{}", i)))
        .collect::<Vec<_>>();

    // Create a keyspace with the initial nodes and replication factor of 3.
    let mut keyspace = KeyspaceBuilder::new(init_nodes)
        .with_replication_factor::<3>()
        .build()
        .expect("Failed to create keyspace");
    assert_eq!(keyspace.version(), 0);

    // For a given key, obtain current replicas.
    // Then remove one of nodes on which the key is replicated.
    let key = 3705152965598471701u64;
    let old_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    assert_eq!(
        old_replicas,
        ["node35", "node27", "node5"]
            .into_iter()
            .map(Node::new)
            .collect::<Vec<_>>()
    );

    let removed_node = Node::new("node45");
    let migrations = keyspace
        .remove_node(removed_node.id())
        .expect("Failed to remove node");
    assert_eq!(keyspace.version(), 1);
    assert_eq!(keyspace.version(), migrations.version());

    // Node was removed, it was part of multiple shards, so at least those shards
    // are updated.
    assert_eq!(migrations.keys().len(), 63);

    let pull_intervals = migrations
        .pull_intervals(removed_node.id())
        .collect::<Vec<_>>();
    // Node is removed, so there should be no pull intervals for it.
    assert_eq!(pull_intervals.len(), 0);

    let new_replicas = keyspace.replicas(&key).collect::<Vec<_>>();
    assert_eq!(
        new_replicas,
        ["node35", "node27", "node5"]
            .into_iter()
            .map(Node::new)
            .collect::<Vec<_>>()
    );

    let pull_intervals = migrations
        .pull_intervals(&"node35".to_string())
        .map(|interval| interval)
        .collect::<Vec<_>>();

    // Check that the migration plan is correct.
    assert_eq!(pull_intervals.len(), 52);
    let interval = pull_intervals.first().unwrap();
    // Removed node is still source of data.
    assert_eq!(
        interval.nodes(),
        &vec!["node45", "node9", "node55"]
            .into_iter()
            .map(Node::new)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        interval.key_range(),
        &KeyRange::new(569986827839078400, Some(570268302815789056))
    );
}

#[test]
fn custom_replication_strategy() {
    #[derive(Debug, Hash, PartialEq, Eq, Clone)]
    enum AvailabilityZone {
        Zone1,
        Zone2,
        Zone3,
    }

    #[derive(Debug, Hash, PartialEq, Eq, Clone)]
    struct MyNode {
        id: String,
        zone: AvailabilityZone,
    }

    impl MyNode {
        fn new(id: String, zone: AvailabilityZone) -> Self {
            MyNode { id, zone }
        }

        fn zone(&self) -> AvailabilityZone {
            self.zone.clone()
        }
    }

    impl KeyspaceNode for MyNode {
        type Id = String;

        fn id(&self) -> &Self::Id {
            &self.id
        }
    }

    struct DistinctZoneReplicationStrategy {
        used_zones: HashSet<AvailabilityZone>,
    }

    impl DistinctZoneReplicationStrategy {
        fn new() -> Self {
            DistinctZoneReplicationStrategy {
                used_zones: HashSet::new(),
            }
        }
    }

    // The `Clone` trait is required for the replication strategy,
    // as each replica set gets its own instance of the strategy.
    //
    // For more simple strategies, this can be derived.
    impl Clone for DistinctZoneReplicationStrategy {
        fn clone(&self) -> Self {
            DistinctZoneReplicationStrategy::new()
        }
    }

    impl ReplicationStrategy<MyNode> for DistinctZoneReplicationStrategy {
        fn is_eligible_replica(&mut self, node: &MyNode) -> bool {
            self.used_zones.insert(node.zone())
        }
    }

    // Create a keyspace with the (default) replication factor of 3.
    let init_nodes: Vec<MyNode> = vec![
        ("node0", AvailabilityZone::Zone1),
        ("node1", AvailabilityZone::Zone1),
        ("node2", AvailabilityZone::Zone1),
        ("node3", AvailabilityZone::Zone1),
        ("node4", AvailabilityZone::Zone2),
        ("node5", AvailabilityZone::Zone2),
        ("node6", AvailabilityZone::Zone2),
        ("node7", AvailabilityZone::Zone2),
        ("node8", AvailabilityZone::Zone2),
        ("node9", AvailabilityZone::Zone2),
        ("node10", AvailabilityZone::Zone3), // will always be selected
    ]
    .into_iter()
    .map(|(id, zone)| MyNode::new(id.to_string(), zone))
    .collect();

    let ks = KeyspaceBuilder::new(init_nodes.clone())
        .with_replication_factor::<3>()
        .with_replication_strategy(DistinctZoneReplicationStrategy::new())
        .build()
        .expect("Failed to create keyspace");

    // Ensures that each key is replicated in distinct zones.
    let key_replicas: Vec<String> = ks
        .replicas(&"key0")
        .map(|replica| replica.id().clone())
        .collect();
    // Note: "node10" is always selected as it is the only node in Zone3.
    assert_eq!(key_replicas, vec![
        "node6",  // Zone2
        "node2",  // Zone1
        "node10"  // Zone3
    ]);

    let key_replicas: Vec<String> = ks
        .replicas(&"key1")
        .map(|replica| replica.id().clone())
        .collect();
    assert_eq!(key_replicas, vec![
        "node8",  // Zone2
        "node10", // Zone3
        "node1",  // Zone1
    ]);
}

#[test]
fn migrations_and_rebalancing() {
    // For a node to be used in keyspace, it must implement `Node` trait.
    // For testing purposes, this trait is implemented for unsigned numbers,
    // `&'static str` and `String`.
    let init_nodes = vec!["node1", "node2", "node3"];

    // Create a keyspace with the (default) replication factor of 3.
    let mut ks = KeyspaceBuilder::new(init_nodes.clone())
        .build()
        .expect("Failed to create keyspace");

    // Check replicas for the key.
    let replicas = ks.replicas(&"key").collect::<Vec<_>>();
    assert_eq!(replicas.len(), 3, "There should be 3 replicas for the key");
    assert!(
        replicas
            .iter()
            .all(|node| init_nodes.iter().any(|n| n.id() == node.id())),
        "All replicas should be from initial nodes",
    );

    // Add another node, obtain migration plan.
    let migration_plan = ks.add_node("node4").expect("Failed to add node");

    // Check replicas for the for `key` -- replica set remained the same!
    // This is expected, the whole point of the keyspace is that it is not totally
    // rehashed on updates - only part of the keyspace is updated.
    let replicas = ks.replicas(&"key").collect::<Vec<_>>();
    assert_eq!(replicas.len(), 3,);
    assert!(
        replicas
            .iter()
            .all(|node| init_nodes.iter().any(|n| n.id() == node.id())),
        "All replicas should be from initial nodes",
    );

    // When it comes to `key2` its replica set should include the new node.
    let replicas = ks.replicas(&"another_key").collect::<Vec<_>>();
    assert!(
        replicas.iter().any(|node| node == &"node4"),
        "New node should be in the replica set"
    );

    // New node needs to pull keys/data from the existing nodes.
    //
    // Keyspace is sharded into shards, each shard has equal portion of the keyspace
    // (that is `[0..u64::MAX)` keyspace is evenly divided into intervals).
    //
    // Migration plan returns all such intervals (key ranges + source replicas).
    // Obtain the first interval that needs data pulled from existing nodes to the
    // new node:
    let interval = migration_plan
        .pull_intervals(&"node4")
        .next()
        .expect("No intervals found");

    // Keys that are hashed into this range will be pulled by the new node.
    let key_range = interval.key_range();
    assert_eq!(
        key_range,
        &KeyRange::new(844424930131968, Some(1125899906842624)),
    );

    // Source nodes are the ones that had data before addition and thus will provide
    // it to the new node.
    let source_nodes = interval.nodes();
    assert!(
        source_nodes.len() == 3,
        "There should be 3 source nodes for the new node"
    );
    assert!(
        source_nodes
            .iter()
            .all(|n| init_nodes.iter().any(|i| i.id() == n.id())),
        "Source nodes should be from initial nodes"
    );
}
