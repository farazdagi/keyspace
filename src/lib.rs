use std::hash::Hash;

/// Node that serves as a destination for data.
///
/// Node controls one or more interval of the key space.
/// Keys which fall into such an interval are routed to the node.
pub trait Node: Hash {}

/// Key space manager.
///
/// Allows to partition the key space, obtain the node responsible for a key,
/// remove a node and re-balance the key space, so that data is evenly
/// distributed across the nodes.
///
/// The implementation is assumed to be thread-safe.
pub trait Keyspace<N: Node> {
    /// A half-open interval of the key space.
    ///
    /// Range bounded inclusively below and exclusively above i.e.
    /// `[start..end)`.
    type Interval;

    /// Add a node to the key space.
    ///
    /// Depending on the implementation, the node will claim one or more
    /// intervals of the key space.
    fn add(&self, node: N);

    /// Remove a node from the key space.
    fn remove(&self, node: &N);

    /// Returns the node responsible for the given key.
    ///
    /// Due to replication, a key may land on several nodes, this method returns
    /// the primary node responsible for the key.
    /// Whenever more than one node is needed, use
    /// [`replicas()`](Self::replicas).
    ///
    /// If the key space is empty (no nodes has been added), `None` is returned.
    fn node<K: Hash>(&self, key: &K) -> Option<&N>;

    /// Returns `k` nodes responsible for the given key.
    ///
    /// The first node is the primary node responsible for the key. It is
    /// guaranteed that the first node is the same as the one returned by
    /// [`node()`](Self::node).
    fn replicas<K: Hash>(&self, key: &K, k: usize) -> Option<Vec<&N>>;

    /// Returns intervals of the key space controlled by the given node.
    ///
    /// This method is necessary to re-balance the key space. When a node is
    /// added or removed, data needs to be moved from one node to another.
    /// In order to do so, the current intervals controlled by the node need
    /// to be known.
    ///
    /// Whenever the node is not part of the key space, `None` is returned.
    fn intervals(&self, node: &N) -> Option<Vec<Self::Interval>>;
}
