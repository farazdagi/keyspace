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
struct Node {
    id: String,
    addr: SocketAddr,
}

impl Node {
    fn new(id: &str, ip: &str, port: u16) -> Self {
        Self {
            id: id.to_string(),
            addr: SocketAddr::new(IpAddr::from_str(&ip).unwrap(), port),
        }
    }
}

// For a node to be used in keyspace, it must implement `KeyspaceNode` trait.
impl KeyspaceNode for Node {
    type Id = String;

    fn id(&self) -> Self::Id {
        self.id.clone()
    }
}

// Each keyspace must start from a set of initial nodes.
// The node count must be at least replication factor number of nodes.
let init_nodes = vec![
    Node::new("node0", "127.0.0.1", 2048),
    Node::new("node1", "127.0.0.1", 2049),
    Node::new("node2", "127.0.0.1", 2050),
];

// Create a keyspace with the (default) replication factor of 3.
let mut ks = KeyspaceBuilder::new(init_nodes.clone())
    .build()
    .expect("Failed to create keyspace");

// Check replicas for the key.
let replicas = ks.replicas(&"key1").collect::<Vec<_>>();
assert_eq!(replicas.len(), 3, "There should be 3 replicas for the key");
assert!(
    replicas
        .iter()
        .all(|node| init_nodes.iter().any(|n| n.id() == node.id())),
    "All replicas should be from initial nodes",
);

// Add another node, see updated replica set.
ks.add_node(Node::new("node4", "127.0.0.1", 2051))
    .expect("Failed to add node");

// Check replicas for the for `key1` -- replica set remained the same!
// This is expected, the whole point of the keyspace is that it is not totally
// rehashed on updates - only part of the keyspace is updated.
let replicas = ks.replicas(&"key1").collect::<Vec<_>>();
assert_eq!(replicas.len(), 3,);
assert!(
    replicas
        .iter()
        .all(|node| init_nodes.iter().any(|n| n.id() == node.id())),
    "All replicas should be from initial nodes",
);

// When it comes to `key2` its replica set should include the new node.
let replicas = ks.replicas(&"key2").collect::<Vec<_>>();
assert!(
    replicas.iter().any(|node| node.id() == "node4"),
    "New node should be in the replica set"
);
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
