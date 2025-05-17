mod interval;
mod node;

use std::{hash::Hash, result::Result};

pub use {
    interval::Interval,
    node::{Node, NodeIdx, NodeRef},
};

/// Keyspace.
///
/// Manages information about nodes and their intervals in the keyspace.
/// Each node controls one or more intervals of the keyspace, and whenever a key
/// needs to be stored or retrieved, the keyspace manager will provide all the
/// replica nodes that are responsible for the key.
///
/// Whenever a node is added or removed, the keyspace manager will re-balance
/// the internal structure in a way that minimizes the amount of data that needs
/// to be moved around.
///
/// On each node addition or removal, the keyspace manager will provide a
/// migration plan, which is a list of keyspace intervals that need to be moved
/// from one node to another.
///
/// Supports replication out of the box, so that each key is stored redundantly
/// on multiple of nodes, for fault tolerance.
pub trait Keyspace<N: Node, const REPLICATION_FACTOR: usize>: Sized {
    /// Error type for the keyspace manager.
    type Error;

    /// Position of a key in the keyspace.
    type Position;

    /// Reference to a node.
    type NodeRef<'a>: NodeRef<'a, N>
    where
        Self: 'a;

    /// Migration plan has all the information needed to migrate data from one
    /// to another when a node is added or removed.
    type MigrationPlan;

    /// How to choose the redundant nodes.
    type ReplicationStrategy;

    /// Returns keyspace with the given replication strategy.
    fn with_replication_strategy(
        self,
        replication_strategy: Self::ReplicationStrategy,
    ) -> Result<Self, Self::Error>;

    /// Add a node to the keyspace.
    ///
    /// The node will claim one or more intervals of the keyspace.
    fn add_node(&self, node: N) -> Result<Self::MigrationPlan, Self::Error>;

    /// Remove a node from the keyspace.
    fn remove_node(&self, node: &N) -> Result<Self::MigrationPlan, Self::Error>;

    /// Replace all nodes in the keyspace with a new batch of nodes.
    fn set_nodes(
        &self,
        nodes: impl IntoIterator<Item = N>,
    ) -> Result<Self::MigrationPlan, Self::Error>;

    /// Returns `k` nodes responsible for the given key.
    ///
    /// The first node is assumed to be the primary node.
    fn replicas<K: Hash>(&self, key: &K, k: usize) -> impl Iterator<Item=Self::NodeRef<'_>>;

    /// Keyspace as intervals controlled by the nodes.
    ///
    /// Each interval is a half-open `[start_key..end_key)` range of controlled
    /// keys, with one or more replicas assigned.
    fn iter(&self, node: &N) -> impl Iterator<Item = Interval>;
}
