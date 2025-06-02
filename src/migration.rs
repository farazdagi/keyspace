use {
    super::{
        KeyspaceError,
        KeyspaceResult,
        interval::Interval,
        node::KeyspaceNode,
        sharding::Shards,
    },
    std::{collections::HashMap, fmt, ops::Deref},
};

/// Data migration plan.
pub struct MigrationPlan<N: KeyspaceNode> {
    /// Mapping of node id to the intervals that need to be migrated to it.
    intervals: HashMap<N::Id, Vec<Interval<N>>>,

    /// Version of keyspace.
    version: u64,
}

impl<N: KeyspaceNode> Deref for MigrationPlan<N> {
    type Target = HashMap<N::Id, Vec<Interval<N>>>;

    fn deref(&self) -> &Self::Target {
        &self.intervals
    }
}

impl<N> fmt::Debug for MigrationPlan<N>
where
    N: KeyspaceNode,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MigrationPlan")
            .field("intervals", &self.intervals)
            .finish_non_exhaustive()
    }
}

impl<N: KeyspaceNode> MigrationPlan<N> {
    /// Creates a new migration plan.
    pub(crate) fn new<const RF: usize>(
        version: u64,
        old_shards: &Shards<N, RF>,
        new_shards: &Shards<N, RF>,
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
            for target_node in new_replica_set.iter().cloned() {
                // Ignore the nodes that are were in the old replica set.
                // No need to migrate data to them.
                if old_replica_set.contains(&target_node) {
                    continue;
                }

                // Pull the data from the old replica set to the target node.
                intervals
                    .entry(target_node.id())
                    .or_insert_with(Vec::new)
                    .push(Interval::new(key_range, old_replica_set.iter().cloned()));
            }
        }

        Ok(Self { version, intervals })
    }

    /// Returns the version of the migration plan.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Intervals that need to be pulled to the given node.
    pub fn pull_intervals(&self, node_id: &N::Id) -> impl Iterator<Item = &Interval<N>> {
        self.intervals
            .get(node_id)
            .into_iter()
            .flat_map(|intervals| intervals.iter())
    }
}
