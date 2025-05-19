pub mod error;
mod interval;
mod migration;
mod node;
mod replication;
mod sharding;

pub use {
    error::*,
    interval::{Interval, KeyspaceInterval},
    migration::MigrationPlan,
    node::{Node, NodeRef},
    replication::{DefaultReplicationStrategy, ReplicationStrategy},
};
use {node::Nodes, std::hash::Hash};

/// Keyspace.
///
/// Manages information about nodes and their intervals in the keyspace.
/// Each node controls one or more intervals of the keyspace, and whenever a
/// key needs to be stored or retrieved, the keyspace manager will
/// provide all the replica nodes that are responsible for the key.
///
/// Whenever a node is added or removed, the keyspace manager will
/// re-balance the internal structure in a way that minimizes the amount
/// of data that needs to be moved around.
///
/// On each node addition or removal, the keyspace manager will provide a
/// migration plan, which is a list of keyspace intervals that need to be
/// moved from one node to another.
///
/// Supports replication out of the box, so that each key is stored
/// redundantly on multiple of nodes, for fault tolerance.
pub struct Keyspace<N, R = DefaultReplicationStrategy, const REPLICATION_FACTOR: usize = 3>
where
    N: Node,
    R: ReplicationStrategy,
{
    nodes: Nodes<N>,
    replication_strategy: R,
    version: u64,
}

impl<N, R, const REPLICATION_FACTOR: usize> Keyspace<N, R, REPLICATION_FACTOR>
where
    N: Node,
    R: ReplicationStrategy,
{

    /// Returns key space with the given replication strategy.
    ///
    /// The key space must be empty (i.e. no nodes are added yet).
    pub fn with_replication_strategy(self, replication_strategy: R) -> KeyspaceResult<Self> {
        if self.version != 0 {
            return Err(KeyspaceError::NonEmptyKeyspace);
        }

        Ok(Self {
            nodes: self.nodes,
            replication_strategy,
            version: self.version,
        })
    }

    /// Add a node to the keyspace.
    ///
    /// The node will claim one or more intervals of the keyspace.
    pub fn add_node(&self, node: N) -> KeyspaceResult<MigrationPlan<N>> {
        todo!()
    }

    /// Remove a node from the keyspace.
    pub fn remove_node(&self, node: &N) -> KeyspaceResult<MigrationPlan<N>> {
        todo!()
    }

    /// Replace all nodes in the keyspace with a new batch of nodes.
    pub fn set_nodes(
        &self,
        nodes: impl IntoIterator<Item = N>,
    ) -> KeyspaceResult<MigrationPlan<N>> {
        todo!()
    }

    /// Returns `REPLICATION_FACTOR` number of nodes responsible for the given
    /// key.
    ///
    /// The first node is assumed to be the primary node.
    pub fn replicas<'a, K: Hash>(&self, key: &K) -> impl Iterator<Item = &'a N> + 'a {
        std::iter::once(todo!())
    }

    /// Keyspace as intervals controlled by the nodes.
    ///
    /// Each interval is a half-open `[start_key..end_key)` range of
    /// controlled keys, with one or more replicas assigned.
    fn iter(&self, node: &N) -> impl Iterator<Item = KeyspaceInterval<'_, &'_ N>> {
        std::iter::once(todo!())
    }

    /// Keyspace version.
    ///
    /// Version is incremented each time the keyspace is modified.
    fn version(&self) -> u64 {
        self.version
    }
}

impl<N> Keyspace<N>
where
    N: Node,
{
    pub fn new() -> Self {
        Self {
            nodes: Nodes::new(),
            replication_strategy: DefaultReplicationStrategy::new(),
            version: 0,
        }
    }
}
