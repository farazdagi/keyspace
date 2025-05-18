use {
    super::{
        KeyspaceError,
        KeyspaceResult,
        Node,
        ReplicationStrategy,
        node::Nodes,
        replication::ReplicaSet,
    },
    crate::node::NodeIdx,
    hrw_hash::HrwNodes,
    std::ops::Deref,
};

#[derive(Debug, Clone, Copy)]
struct ShardIdx(u16);

impl Deref for ShardIdx {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ShardIdx {
    const MAX: Self = Self(u16::MAX);
}

struct ShardedKeyspace<const REPLICATION_FACTOR: usize> {
    /// Key space is uniformly divided into shards.
    ///
    /// Each shard is a replica set of nodes that are responsible for the data
    /// in that key space portion.
    shards: Vec<ReplicaSet<NodeIdx, REPLICATION_FACTOR>>,
}

impl<const REPLICATION_FACTOR: usize> ShardedKeyspace<REPLICATION_FACTOR> {
    /// Creates a new key space with each shard controlled by a replica set of
    /// nodes.
    fn new<N: Node, R: ReplicationStrategy>(
        nodes: &Nodes<N>,
        replication_strategy: R,
    ) -> KeyspaceResult<Self> {
        if nodes.len() < REPLICATION_FACTOR {
            return Err(KeyspaceError::NotEnoughNodes(REPLICATION_FACTOR));
        }

        // Highest random weight (HRW) algorithm is used to select the nodes.
        let hrw = HrwNodes::new(nodes.indexes());

        let mut shards = Vec::with_capacity(ShardIdx::MAX.0 as usize + 1);
        for idx in 0..=ShardIdx::MAX.0 {
            // Each replica set gets a fresh copy of the replication strategy.
            let mut replication_strategy = replication_strategy.clone();
            let selected_replicas = hrw.sorted(&idx).filter_map(|node_idx| {
                nodes.get(*node_idx).and_then(|node| {
                    if replication_strategy.is_eligible_replica(node) {
                        Some(*node_idx)
                    } else {
                        None
                    }
                })
            });

            shards.push(ReplicaSet::try_from_iter(selected_replicas)?);
        }

        Ok(Self { shards })
    }

    /// Returns the number of shards in the keyspace.
    fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// Returns replica set for the shard at the given index.
    fn replica_set(&self, idx: usize) -> &ReplicaSet<NodeIdx, REPLICATION_FACTOR> {
        &self.shards[idx]
    }
}
