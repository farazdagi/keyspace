use {
    super::{
        KeyspaceError,
        KeyspaceResult,
        interval::{Interval, PendingInterval},
        node::{Node, NodeIdx, Nodes},
        replication::ReplicaSet,
        sharding::{Shard, Shards},
    },
    std::{
        collections::{HashMap, HashSet},
        hash::BuildHasher,
    },
};

/// Data migration plan.
pub struct MigrationPlan<'a, N: Node, H: BuildHasher> {
    /// Mapping of node id to the intervals that need to be migrated to it.
    intervals: HashMap<NodeIdx, Vec<Interval<HashSet<NodeIdx>>>>,

    /// Reference to the nodes of the keyspace.
    nodes: &'a Nodes<N, H>,
}

impl<'a, N, H> MigrationPlan<'a, N, H>
where
    N: Node,
    H: BuildHasher,
{
    /// Creates a new migration plan.
    pub(crate) fn new<const RF: usize>(
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

            let key_range = old_shard.key_range();
            for target_node_idx in new_replica_set.iter().copied() {
                // Ignore the nodes that are were in the old replica set.
                // No need to migrate data to them.
                if old_replica_set.contains(&target_node_idx) {
                    continue;
                }

                // Pull the data from the old replica set to the target node.
                intervals
                    .entry(old_replica_set[0])
                    .or_insert_with(Vec::new)
                    .push(Interval::new(
                        key_range,
                        new_replica_set.iter().copied().collect(),
                    ));
            }
        }

        Ok(Self { intervals, nodes })
    }

    /// Returns the version of the migration plan.
    pub fn version(&self) -> u64 {
        self.nodes.version()
    }

    /// Intervals that need to be pulled to the given node.
    pub fn pending_intervals(&self, node: &N) -> impl Iterator<Item = Interval<Vec<&N>>> {
        self.intervals
            .get(&self.nodes.idx(node))
            .into_iter()
            .flat_map(|intervals| {
                intervals.iter().map(|interval| {
                    let nodes: Vec<&N> = interval
                        .nodes()
                        .iter()
                        .filter_map(|idx| self.nodes.get(*idx))
                        .collect();
                    Interval::new(interval.key_range().clone(), nodes)
                })
            })
    }
}
