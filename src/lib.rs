#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

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
    interval::{Interval, KeyRange},
    migration::MigrationPlan,
    node::{KeyspaceNode, NodeRef},
    replication::{DefaultReplicationStrategy, ReplicationStrategy},
};
use {
    node::Nodes,
    rapidhash::RapidBuildHasher,
    sharding::{ShardIdx, Shards},
    std::{
        hash::{BuildHasher, Hash},
        sync::Arc,
    },
};

/// Position of a key in the keyspace.
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
pub struct Keyspace<N, R = DefaultReplicationStrategy, const RF: usize = 3, H = RapidBuildHasher>
where
    N: KeyspaceNode,
    R: ReplicationStrategy,
    H: BuildHasher,
{
    nodes: Arc<Nodes<N>>,
    shards: Shards<N, RF>,
    replication_strategy: R,
    build_hasher: H,
    version: u64,
}

impl<N, R, const RF: usize, H> Keyspace<N, R, RF, H>
where
    N: KeyspaceNode,
    R: ReplicationStrategy,
    H: BuildHasher,
{
    /// Create new keyspace.
    ///
    /// Make sure that at least replication factor (`RF`) number of nodes are
    /// added to the keyspace, otherwise it will not be able to function
    /// properly.
    fn with_build_hasher<I: IntoIterator<Item = N>>(
        build_hasher: H,
        init_nodes: I,
        replication_strategy: R,
    ) -> KeyspaceResult<Self> {
        let nodes = Nodes::from_iter(init_nodes);
        let shards = Shards::new(&nodes, replication_strategy.clone())?;
        Ok(Self {
            nodes: Arc::new(nodes),
            shards,
            replication_strategy,
            build_hasher,
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
    pub fn remove_node(&mut self, node_id: &N::Id) -> KeyspaceResult<MigrationPlan<N>> {
        self.nodes.remove(node_id);
        self.migration_plan()
    }

    /// Returns replication factor (`RF`) number of nodes responsible for the
    /// given key position.
    ///
    /// The first node is assumed to be the primary node.
    pub fn replicas<K: Hash>(&self, key: &K) -> impl Iterator<Item = NodeRef<N>> {
        let key_position = self.build_hasher.hash_one(key);
        let shard_idx = ShardIdx::from_position(key_position);
        let replica_set = self.shards.replica_set(shard_idx);
        replica_set.iter().map(Clone::clone)
    }

    /// Keyspace version.
    ///
    /// Version is incremented each time the keyspace is modified.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Keyspace as intervals controlled by the nodes.
    ///
    /// Each interval is a half-open `[start_key..end_key)` range of controlled
    /// keys, with one or more replicas assigned.
    ///
    /// The intervals are returned as `(key range, node ref)` tuples, so that it
    /// is trivial to collect them into a map (either by ranges or by nodes).
    pub fn iter(&self) -> impl Iterator<Item = (KeyRange, NodeRef<N>)> {
        self.shards.iter().flat_map(|shard| {
            let key_range = shard.key_range();
            shard
                .replica_set()
                .iter()
                .map(|idx| (key_range, idx.clone()))
                .collect::<Vec<_>>()
        })
    }

    /// Keyspace intervals controlled by the given node.
    pub fn iter_node(&self, node_id: &N::Id) -> impl Iterator<Item = KeyRange> {
        self.iter().filter_map(move |(key_range, node)| {
            if node.id() == node_id {
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
        let new_version = self.version + 1;
        MigrationPlan::new(new_version, &old_shards, &self.shards).and_then(|plan| {
            self.version = new_version;
            Ok(plan)
        })
    }
}
