use {
    super::{
        KeyspaceError,
        KeyspaceResult,
        NodeRef,
        interval::Interval,
        node::{Node, NodeIdx, Nodes},
        sharding::Shards,
    },
    std::{collections::HashMap, fmt, hash::BuildHasher, ops::Deref},
};

/// Data migration plan.
pub struct MigrationPlan<'a, N: Node, H: BuildHasher> {
    /// Mapping of node id to the intervals that need to be migrated to it.
    intervals: HashMap<NodeIdx, Vec<Interval<Vec<NodeIdx>>>>,

    /// Reference to the nodes of the keyspace.
    nodes: &'a Nodes<N, H>,

    /// Version of keyspace.
    version: u64,
}

impl<'a, N, H> Deref for MigrationPlan<'a, N, H>
where
    N: Node,
    H: BuildHasher,
{
    type Target = HashMap<NodeIdx, Vec<Interval<Vec<NodeIdx>>>>;

    fn deref(&self) -> &Self::Target {
        &self.intervals
    }
}

impl<'a, N, H> fmt::Debug for MigrationPlan<'a, N, H>
where
    N: Node,
    H: BuildHasher,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationPlan")
            .field("intervals", &self.intervals)
            .finish_non_exhaustive()
    }
}

impl<'a, N, H> MigrationPlan<'a, N, H>
where
    N: Node,
    H: BuildHasher,
{
    /// Creates a new migration plan.
    pub(crate) fn new<const RF: usize>(
        version: u64,
        nodes: &'a Nodes<N, H>,
        old_shards: &Shards<RF>,
        new_shards: &Shards<RF>,
    ) -> KeyspaceResult<Self> {
        let mut intervals = HashMap::new();
        if old_shards.len() != new_shards.len() {
            return Err(KeyspaceError::ShardCountMismatch);
        }

        for (old_shard, new_shard) in old_shards.iter().zip(new_shards.iter()) {
            let old_replica_set = old_shard.replica_set();
            let new_replica_set = new_shard.replica_set();
            if old_replica_set == new_replica_set {
                continue;
            }
            assert_eq!(old_shard.key_range(), new_shard.key_range());

            let key_range = old_shard.key_range();
            for target_node_idx in new_replica_set.iter().copied() {
                // Ignore the nodes that are were in the old replica set.
                // No need to migrate data to them.
                if old_replica_set.contains(&target_node_idx) {
                    continue;
                }

                // Pull the data from the old replica set to the target node.
                intervals
                    .entry(target_node_idx)
                    .or_insert_with(Vec::new)
                    .push(Interval::new(
                        key_range,
                        old_replica_set.iter().copied().collect(),
                    ));
            }
        }

        Ok(Self {
            version,
            intervals,
            nodes,
        })
    }

    /// Returns the version of the migration plan.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Intervals that need to be pulled to the given node.
    pub fn pull_intervals(&self, node: &N) -> impl Iterator<Item = Interval<Vec<NodeRef<N>>>> {
        self.intervals
            .get(&self.nodes.idx(node))
            .into_iter()
            .flat_map(|intervals| {
                intervals.iter().map(|interval| {
                    let nodes = interval
                        .nodes()
                        .iter()
                        .filter_map(|idx| self.nodes.get(*idx))
                        .collect();
                    Interval::new(interval.key_range().clone(), nodes)
                })
            })
    }
}
