mod builder;
pub mod error;
mod interval;
mod migration;
mod node;
mod replication;
mod sharding;

pub use {
    builder::KeyspaceBuilder,
    error::*,
    interval::{Interval, KeyspaceInterval},
    migration::MigrationPlan,
    node::{Node, NodeRef},
    replication::{DefaultReplicationStrategy, ReplicationStrategy},
};
use {
    node::Nodes,
    sharding::{ShardIdx, Shards},
};

/// Position of a key in the key space.
pub type KeyPosition = u64;

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
pub struct Keyspace<N, R = DefaultReplicationStrategy, const RF: usize = 3>
where
    N: Node,
    R: ReplicationStrategy,
{
    nodes: Nodes<N>,
    shards: Shards<RF>,
    replication_strategy: R,
    version: u64,
}

impl<N, R, const RF: usize> Keyspace<N, R, RF>
where
    N: Node,
    R: ReplicationStrategy,
{
    /// Create new key space.
    ///
    /// Make sure that at least `RF` nodes are added to the
    /// keyspace, otherwise the keyspace will not be able to function properly.
    fn new<I: IntoIterator<Item = N>>(
        init_nodes: I,
        replication_strategy: R,
    ) -> KeyspaceResult<Self> {
        let mut nodes = Nodes::new();
        for node in init_nodes {
            nodes.insert(node)?;
        }

        let shards = Shards::new(&nodes, replication_strategy.clone())?;
        Ok(Self {
            nodes,
            shards,
            replication_strategy,
            version: 0,
        })
    }

    /// Add a node to the keyspace.
    ///
    /// The node will claim one or more intervals of the keyspace.
    pub fn add_node(&mut self, node: N) -> KeyspaceResult<MigrationPlan<N>> {
        self.nodes.insert(node)?;

        // Recalculate the shards.
        let old_shards = self.shards.clone();
        self.shards = Shards::new(&self.nodes, self.replication_strategy.clone())?;

        // Calculate migration plan from updated shards.
        let migration: MigrationPlan<N> =
            MigrationPlan::new(self.version, old_shards.iter(), self.shards.iter());

        self.version += 1;
        todo!()
    }

    /// Remove a node from the keyspace.
    pub fn remove_node(&self, id: N::NodeId) -> KeyspaceResult<MigrationPlan<N>> {
        todo!()
    }

    /// Replace all nodes in the keyspace with a new batch of nodes.
    pub fn set_nodes<I: IntoIterator<Item = N>>(
        &mut self,
        nodes: I,
    ) -> KeyspaceResult<MigrationPlan<N>> {
        // we need way to merge individual migration plans
        for node in nodes {
            self.nodes.insert(node)?;
        }
        todo!()
    }

    /// Returns replication factor (`RF`) number of nodes responsible for the
    /// given key position. Key is, normally, hashed to determine the position
    /// in the key space.
    ///
    /// The first node is assumed to be the primary node.
    pub fn replicas(&self, key_position: KeyPosition) -> impl Iterator<Item = &N> {
        let shard_idx = ShardIdx::from_position(key_position);
        let replica_set = self.shards.replica_set(shard_idx);
        replica_set
            .iter()
            .filter_map(|node_idx| self.nodes.get(*node_idx))
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
