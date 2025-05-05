# keyspace

[![crates.io](https://img.shields.io/crates/d/keyspace.svg)](https://crates.io/crates/keyspace)
[![docs.rs](https://docs.rs/keyspace/badge.svg)](https://docs.rs/keyspace)

Key space partitioning and re-balancing for distributed systems.

## Motivation

Implement a key space partitioning and re-balancing algorithm that is:

- [ ] Memory/space efficient: scalable, no virtual nodes, for instance.
- [ ] Fair: data is evenly distributed across partitions.
- [ ] Compact: to compute the target node of a key, we only need to know the number of nodes `n`.
- [ ] Adaptive: supports node addition and removal, with close to theoretically minimal data
  movement.
- [ ] Robust: supports replication out of the box.
- [ ] Heterogeneous: supports heterogeneous nodes (e.g. different storage capacities).

The idea is to allow system to grow to thousands of nodes, and to process millions of keys per
second efficiently.
