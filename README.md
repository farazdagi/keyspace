# keyspace

[![crates.io](https://img.shields.io/crates/d/keyspace.svg)](https://crates.io/crates/keyspace)
[![docs.rs](https://docs.rs/keyspace/badge.svg)](https://docs.rs/keyspace)
[![unsafe forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](https://github.com/rust-secure-code/safety-dance/)
[![dependencies](https://deps.rs/repo/github/farazdagi/keyspace/status.svg)](https://deps.rs/repo/github/farazdagi/mpchash)

Keyspace partitioning and re-balancing for distributed systems.

## Motivation

Implement a keyspace partitioning and re-balancing algorithm that is:

- [x] Memory/space efficient: no virtual nodes, scalable to thousands of *physical* nodes.
- [ ] Fair: data is uniformly distributed across partitions.
- [x] Compact: to compute the target node of a key, we only need to know the number of nodes `n`,
  and operation is `O(1)`.
- [x] Adaptive: supports node addition and removal, with close to theoretically minimal data
  movement.
- [x] Robust: supports replication out of the box.
- [ ] Heterogeneous: supports weighted nodes with different storage capacities.

The idea is to allow system to grow and shrink easily, and to process millions of keys per second
efficiently. Additionally, provide a simple API exposing the keyspace data movement details, so that
the system can be re-balanced in a distributed fashion.

## Usage

The API is designed to be simple and easy to use. It provides a way to start a keyspace with some
nodes, then add/remove nodes with minimal data movement (with migration plans calculated and
returned), and finally query the keyspace for the target node of a key:

The purpose of the keyspace is to route keys to nodes. To do that, we need to define a node type
that implements the `KeyspaceNode` trait.

``` rust
use {
    keyspace::{KeyRange, KeyspaceBuilder, KeyspaceNode},
    std::{
        net::{IpAddr, SocketAddr},
        str::FromStr,
    },
};

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
let init_nodes = vec!["node0", "node1", "node2"]
    .into_iter()
    .map(Node::new)
    .collect::<Vec<Node>>();

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
ks.remove_node(&String::from("node2"))
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

This is only a minimal use case, real life scenarios would likely require:

- Nodes holding more information than just an ID.
- Heterogeneous cluster with nodes having different capacities.
- Full support for migrations and re-balancing, i.e. ability to pull data from data holding nodes on
  a node addition/removal.
- For failure tolerance, keys may need to be replicated across multiple physical machines.
- Moreover, such a replication should be flexible enough, with custom replication strategies, e.g.
  strategy that ensures that replicas of a key live in different availability zones or racks.

See the [documentation](https://docs.rs/keyspace/latest/keyspace/) for more details on such use
cases.
