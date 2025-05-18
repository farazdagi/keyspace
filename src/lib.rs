pub mod error;
mod interface;
mod interval;
mod migration;
mod node;
mod replication;
mod sharding;

pub use {
    error::*,
    interval::{Interval, KeyspaceInterval},
    node::{Node, NodeRef},
    replication::{DefaultReplicationStrategy, ReplicationStrategy},
};

pub struct Keyspace<N: Node, const REPLICATION_FACTOR: usize> {
    nodes: Vec<N>,
    version: u64,
}
