pub mod interval;
pub mod node;

#[cfg(test)]
mod node_test;

use std::{hash::Hash, ops::Range, result::Result};

pub use {
    interval::Interval,
    node::{Node,Nodes, NodeIdx, NodeRef},
};

/// Keyspace.
///
/// Provides a way to manage the keyspace and the nodes that control it.
/// Allows to add and remove nodes, and to find the node responsible for a
/// given key (or replicas, when key is stored redundantly).
///
/// On node addition or removal, the keyspace is re-balanced: data needs to be
/// moved around, using migration. The interface provides a way to get the
/// list of pending ranges to be moved.
pub trait Keyspace<N: Node, const REPLICATION_FACTOR: usize> {
    /// Position of a key in the keyspace.
    type Position;

    /// Reference to a node.
    type NodeRef<'a>: NodeRef<'a, N>
    where
        Self: 'a;

    /// Error type for the keyspace manager.
    type Error;

    /// Add a node to the keyspace.
    ///
    /// The node will claim one or more intervals of the keyspace.
    fn add(&self, node: N) -> Result<(), Self::Error>;

    /// Remove a node from the keyspace.
    fn remove(&self, node: &N) -> Result<(), Self::Error>;

    /// Returns the node responsible for the given key.
    ///
    /// Due to replication, a key may land on several nodes, this method returns
    /// the primary node responsible for the key.
    /// Whenever more than one node is needed, use
    /// [`replicas()`](Self::replicas).
    ///
    /// If the keyspace is empty (no nodes has been added), `None` is returned.
    fn node<K: Hash>(&self, key: &K) -> Option<Self::NodeRef<'_>>;

    /// Returns `k` nodes responsible for the given key.
    ///
    /// The first node is the primary node responsible for the key. It is
    /// guaranteed that the first node is the same as the one returned by
    /// [`node()`](Self::node).
    fn replicas<K: Hash>(&self, key: &K, k: usize) -> Vec<Self::NodeRef<'_>>;

    /// Returns keyspace position to which a given key will be assigned.
    fn position<K: Hash>(&self, key: &K) -> Self::Position;

    /// Returns `[start..end)` intervals of the keyspace controlled by the given
    /// node.
    ///
    /// This method is necessary to re-balance the keyspace. When a node is
    /// added or removed, data needs to be moved from one node to another.
    /// In order to do so, the current intervals controlled by the node need
    /// to be known.
    ///
    /// Whenever the node is not part of the keyspace, `None` is returned.
    fn intervals(&self, node: &N) -> Option<Vec<Interval>>;
}
