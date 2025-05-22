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
    interval::{Interval, KeyRange, KeyspaceInterval},
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
            nodes.insert(node);
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
        self.nodes.insert(node);

        self.migration_plan()
    }

    /// Remove a node from the keyspace.
    pub fn remove_node(&mut self, node: &N) -> KeyspaceResult<MigrationPlan<N>> {
        self.nodes.remove(self.nodes.idx(node));
        self.migration_plan()
    }

    /// Replace all nodes in the keyspace with a new batch of nodes.
    pub fn set_nodes<I: IntoIterator<Item = N>>(
        &mut self,
        nodes: I,
    ) -> KeyspaceResult<MigrationPlan<N>> {
        for node in nodes {
            self.nodes.insert(node);
        }
        self.migration_plan()
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

    /// Keyspace version.
    ///
    /// Version is incremented each time the keyspace is modified.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Key space as intervals controlled by the nodes.
    ///
    /// Each interval is a half-open `[start_key..end_key)` range of controlled
    /// keys, with one or more replicas assigned.
    pub fn iter(&self) -> impl Iterator<Item = (&N, KeyRange)> {
        self.shards
            .iter()
            .flat_map(|shard| {
                // dbg!(&shard);
                let key_range = shard.key_range();
                shard
                    .replica_set()
                    .iter()
                    .map(|idx| (*idx, key_range))
                    .collect::<Vec<_>>()
            })
            .filter_map(|(idx, key_range)| {
                self.nodes.get(idx).and_then(|node| Some((node, key_range)))
            })
    }

    /// Key space intervals controlled by the given node.
    pub fn iter_node(&self, node: &N) -> impl Iterator<Item = KeyRange> {
        self.iter().filter_map(move |(other_node, key_range)| {
            // dbg!((&other_node, &key_range));
            if node == other_node {
                Some(key_range)
            } else {
                None
            }
        })
    }

    fn migration_plan(&mut self) -> KeyspaceResult<MigrationPlan<N>> {
        // Recalculate the shards.
        let old_shards = self.shards.clone();
        self.shards = Shards::new(&self.nodes, self.replication_strategy.clone())?;

        // Calculate migration plan from updated shards.
        MigrationPlan::new(&self.nodes, &old_shards, &self.shards).and_then(|plan| {
            self.version += 1;
            Ok(plan)
        })
    }
}
