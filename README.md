# keyspace

[![crates.io](https://img.shields.io/crates/d/keyspace.svg)](https://crates.io/crates/keyspace)
[![docs.rs](https://docs.rs/keyspace/badge.svg)](https://docs.rs/keyspace)
[![unsafe forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](https://github.com/rust-secure-code/safety-dance/)
[![dependencies](https://deps.rs/repo/github/farazdagi/keyspace/status.svg)](https://deps.rs/repo/github/farazdagi/mpchash)

Keyspace partitioning and re-balancing for distributed systems.

## Motivation

You have a distributed system with a set of nodes, and you want to partition the data keyspace
across those nodes, fairly --- that is each node is responsible for a roughly the same portion of
the space. You want to be able to add and remove nodes with minimal data movement, and you want to
be able to query the keyspace for the target node of a key, efficiently.

Thus, you need to implement a keyspace partitioning and re-balancing algorithm that is:

- [x] Memory/space efficient: no virtual nodes, scalable to thousands of *physical* nodes.
- [x] Fair: data is uniformly distributed across partitions.
- [x] Compact: to compute the target node of a key, we only need to know the number of nodes `n`,
  and operation is `O(1)`.
- [x] Adaptive: supports node addition and removal, with close to theoretically minimal data
  movement.
- [x] Robust: supports replication out of the box.
- [x] Heterogeneous: supports weighted nodes with different storage capacities.

The idea is to allow system to grow and shrink easily, and to process millions of keys per second
efficiently. Additionally, provide a simple API exposing the keyspace data movement details, so that
the system can be re-balanced in a distributed fashion.

## Usage

The API is designed to be simple and easy to use. It provides a way to start a keyspace with some
initial nodes, add/remove nodes with minimal data movement (with migration plans calculated and
returned), query the keyspace for the target node for a given key.

### Basic example

Example below uses default replication factor, default replication strategy, and homogeneous nodes,
where each node has equal chance to be picked for a key.

``` rust
use keyspace::{KeyspaceBuilder, KeyspaceNode};

// The purpose of the keyspace is to route keys to nodes.
// To do that, we need to define a type that implements the `KeyspaceNode` trait.
// Node type holds enough information about our physical node.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct Node(String);

// To be used as a keyspace node, it must implement the trait.
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

// Each keyspace must start from a set of initial nodes.
// The node count must be at least equal to replication factor.
let init_nodes: Vec<Node> = vec!["node0", "node1", "node2"]
    .into_iter()
    .map(Node::new)
    .collect();

// Create a keyspace with the (default) replication factor of 3.
let mut ks = KeyspaceBuilder::new(init_nodes.clone())
    .build()
    .expect("Failed to create keyspace");

// Check replicas for the key.
let primary_replica = ks
    .replicas(&"key0") // iterator over replicas
    .next()
    .expect("No replicas found for the key");
assert_eq!(primary_replica.id(), "node2");
let removed_node = primary_replica; // save the node for later

let primary_replica = ks
    .replicas(&"key1") // iterator over replicas
    .next()
    .expect("No replicas found for the key");
assert_eq!(primary_replica.id(), "node1");

// Add another node, see updated replica set.
//
// Some nodes will have the new node in their replica set, however,
// the majority of keys will not change their primary replica.
ks.add_node(Node::new("node4")).expect("Failed to add node");

// Re-check primary replica for the key.
// This should not change, as the keyspace is not totally rehashed.
let primary_replica = ks
    .replicas(&"key0") // iterator over replicas
    .next()
    .expect("No replicas found for the key");
assert_eq!(primary_replica.id(), "node2");

// Some keys will have the new node in their replica set, though.
let primary_replica = ks
    .replicas(&"key1") // iterator over replicas
    .next()
    .expect("No replicas found for the key");
assert_eq!(primary_replica.id(), "node4");

// Remove a node.
// The node will not be a primary replica any more.
ks.remove_node(removed_node.id())
    .expect("Failed to remove node");

// Another primary replica should be selected for the key.
let primary_replica = ks
    .replicas(&"key0") // iterator over replicas
    .next()
    .expect("No replicas found for the key");
assert_eq!(primary_replica.id(), "node4");

// Most keys will be unaffected.
let primary_replica = ks
    .replicas(&"key1") // iterator over replicas
    .next()
    .expect("No replicas found for the key");
assert_eq!(primary_replica.id(), "node4");
```

This is only a minimal use case, real-life scenarios would likely require:

- Nodes holding more information than just an ID (physical address, availability zone etc).
- Nodes having different capacities to be used in a heterogeneous cluster.
- Full support for migrations and re-balancing, that is an ability to know which data to pull from
  what nodes on cluster updates (node additions/removals).
- For failure tolerance, keys may need to be replicated across multiple physical machines. Moreover,
  such a replication should be flexible enough, with custom replication strategies, e.g. strategy
  that ensures that replicas of a key live in different availability zones or racks.

### Custom replication factor

The `KeyspaceBuilder` provides methods to construct a keyspace with a given replication strategy
(see the next section) and factor:

``` rust
use keyspace::{KeyspaceBuilder, KeyspaceNode, DefaultReplicationStrategy};

// Explicit replication strategy and factor
let ks = KeyspaceBuilder::new(init_nodes)
    .with_replication_strategy(DefaultReplicationStrategy::new())
    .with_replication_factor::<5>()
    .build();
```

### Custom replication strategy

If only a single node is used to store a key, the system would not be fault-tolerant. Thus, keys
are, normally, stored across multiple nodes, to ensure that the data is available even if some nodes
fail. So, each key is replicated across multiple nodes, and the number of replicas is called the
replication factor.

For a given key the keyspace provides `n` target nodes, where `n` is the replication factor (see
[Keyspace::replicas()](https://docs.rs/keyspace/0.4.1/keyspace/struct.Keyspace.html#method.replicas)
method). Nodes are selected using the
[Highest Random Weight aka Rendezvous hashing algorithm](https://crates.io/crates/hrw-hash).

The `DefaultReplicationStrategy` just selects the next `n` nodes provided by a keyspace for a given
key. But this strategy can be customized to implement more sophisticated replication strategies,
like making sure that replicas of a key live in different availability zones or racks.

In the example below, we implement a custom replication strategy that ensures that replicas of a key
live in different availability zones, i.e. when `Keyspace::replicas()` is called for a key, it
returns nodes that are in different availability zones.

``` rust
/// Availability zones for the nodes.
///
/// When constructing a replica set of nodes responsible for
/// a key, we will ensure that replicas are in distinct zones.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
enum AvailabilityZone {
    Zone1,
    Zone2,
    Zone3,
}

/// Each node will have an ID and an availability zone.
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

/// Marks a type as a keyspace node.
impl KeyspaceNode for MyNode {
    type Id = String;

    fn id(&self) -> &Self::Id {
        &self.id
    }
}

/// A custom replication strategy that ensures that replicas of a key
/// live in distinct availability zones.
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
// For more simple strategies, this can be derived, of course.
impl Clone for DistinctZoneReplicationStrategy {
    fn clone(&self) -> Self {
        DistinctZoneReplicationStrategy::new()
    }
}

// The `ReplicationStrategy` trait is used to select nodes for a key.
// Selection depends on nodes already selected for the key, as each
// replica set (for a key) gets its own instance of the strategy.
impl ReplicationStrategy<MyNode> for DistinctZoneReplicationStrategy {
    fn is_eligible_replica(&mut self, node: &MyNode) -> bool {
        // Check if the node's zone is already used.
        // If it is, we cannot use this node as a replica.
        self.used_zones.insert(node.zone())
    }
}

// Initial list of nodes, each with an ID and an availability zone.
// Note that "node10" is in Zone3, and will always be selected as a replica,
// as it is the only node in that zone.
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

```

### Data re-balancing and migration plans

When a new node is added to the keyspace or an existing node is removed, the keyspace needs to
re-balance the data across the nodes. This is done by calculating a migration plan, and providing it
to the user. The migration plan contains information about which key ranges should be moved from
which source nodes to which target nodes.

``` rust
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
// Migration plan contains all such intervals (key ranges + source replicas).
// So, for a given node, we can obtain the intervals that need to be pulled
// to it from the existing nodes (list of source nodes is also provided).


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
// Since we just added a single node to `init_nodes` source nodes should be
// the same as initial nodes.
assert!(
    source_nodes
        .iter()
        .all(|n| init_nodes.iter().any(|i| i.id() == n.id())),
    "Source nodes should be from initial nodes"
);
```

When it comes to node removal, the keyspace will also provide a migration plan, and again using it
you will be able to obtain intervals (with source nodes) that need to be pulled to a given node. Then,
it is the matter of traversal of all nodes, where for each node you request `pull_intervals()`, check
if those are not empty, move data from source nodes to the target node, and finally consider removed
node as detached. 

Please note, that migrations obtained from `Keyspace::remove_node()`, will still contain
removed nodes in source nodes, as before a node can be considered removed, it should help moving data around.
However, if you detach node immediately, given that data is replicated -- data can be moved around by using other replicas in the replica sets that contained the removed node.
