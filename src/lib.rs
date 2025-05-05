use std::hash::Hash;

/// Node that serves as a destination for data.
///
/// Node controls one or more interval of the key space.
/// Keys which fall into such an interval are routed to the node.
pub trait Node: Hash {}

impl<T> Node for T where T: Hash {}

/// Reference to a node.
///
/// Internally nodes might be stored in maps or other data structures.
/// References to elements in such data structures are not valid if we cannot
/// guarantee that data structure's lifetime is longer than the reference's
/// lifetime. By wrapping the reference in a trait, we can bind the lifetime of
/// node reference and the underlying data structure.
pub trait NodeRef<'a, T: Node> {}

/// Key space manager.
///
/// Provides a way to manage the key space and the nodes that control it.
/// Allows to add and remove nodes, and to find the node responsible for a
/// given key (or replicas, when key is stored redundantly).
///
/// On node addition or removal, the key space is re-balanced: data needs to be
/// moved around, therefore the key space manager exposes the intervals
/// controlled by a node.
pub trait Keyspace<N: Node> {
    /// A half-open interval of the key space.
    ///
    /// Range bounded inclusively below and exclusively above i.e.
    /// `[start..end)`.
    type Interval;

    /// Position of a key in the key space.
    type Position;

    /// Reference to a node.
    type NodeRef<'a>: NodeRef<'a, N>
    where
        Self: 'a;

    /// Add a node to the key space.
    ///
    /// Depending on the implementation, the node will claim one or more
    /// intervals of the key space.
    fn add(&self, node: N) {
        self.add_with_capacity(node, 0)
    }

    /// Add a node and its capacity to the key space.
    ///
    /// Capacity is an arbitrary number that is used to determine what portion
    /// of the key space the node will control. Capacities of all nodes are
    /// summed up to determine the total capacity of the key space. The relative
    /// capacity of the node is then ratio of the node's capacity to the total
    /// capacity of the key space.
    fn add_with_capacity(&self, node: N, capacity: usize);

    /// Remove a node from the key space.
    ///
    /// Returns the node if it was removed, `None` otherwise.
    fn remove(&self, node: &N) -> Option<Self::NodeRef<'_>>;

    /// Returns the node responsible for the given key.
    ///
    /// Due to replication, a key may land on several nodes, this method returns
    /// the primary node responsible for the key.
    /// Whenever more than one node is needed, use
    /// [`replicas()`](Self::replicas).
    ///
    /// If the key space is empty (no nodes has been added), `None` is returned.
    fn node<K: Hash>(&self, key: &K) -> Option<Self::NodeRef<'_>>;

    /// Returns `k` nodes responsible for the given key.
    ///
    /// The first node is the primary node responsible for the key. It is
    /// guaranteed that the first node is the same as the one returned by
    /// [`node()`](Self::node).
    fn replicas<K: Hash>(&self, key: &K, k: usize) -> Option<Vec<Self::NodeRef<'_>>>;

    /// Returns key space position to which a given key will be assigned.
    fn position<K: Hash>(&self, key: &K) -> Self::Position;

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
