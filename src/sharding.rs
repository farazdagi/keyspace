use {
    super::{
        KeyPosition,
        KeyspaceError,
        KeyspaceResult,
        KeyspaceNode,
        ReplicationStrategy,
        interval::KeyRange,
        node::Nodes,
        replication::ReplicaSet,
    },
    hrw_hash::HrwNodes,
    std::ops::Deref,
};

/// Shard index.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ShardIdx(u16);

impl Deref for ShardIdx {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ShardIdx {
    const MAX: Self = Self(u16::MAX);

    /// Creates a new shard index from the given key position.
    pub fn from_position(pos: KeyPosition) -> Self {
        ShardIdx((pos >> 48) as u16)
    }
}

/// Shard is a portion of the keyspace controlled by a set of nodes.
#[derive(Debug)]
pub(crate) struct Shard<'a, N: KeyspaceNode, const RF: usize> {
    idx: ShardIdx,
    replica_set: &'a ReplicaSet<N, RF>,
}

impl<'a, N: KeyspaceNode, const RF: usize> Shard<'a, N, RF> {
    /// Creates a new shard with the given index and replica set.
    pub fn new(idx: ShardIdx, replica_set: &'a ReplicaSet<N, RF>) -> Self {
        Self { idx, replica_set }
    }

    /// Returns the replica set of the shard.
    pub fn replica_set(&self) -> &ReplicaSet<N, RF> {
        self.replica_set
    }

    /// Returns the range of keys that are controlled by this shard.
    pub fn key_range(&self) -> KeyRange {
        let start = (self.idx.0 as u64) << 48;
        let end = if self.idx.0 == u16::MAX {
            None
        } else {
            Some(((self.idx.0 as u64) + 1) << 48)
        };
        KeyRange::new(start, end)
    }
}

/// Keyspace is uniformly divided into shards.
///
/// Each shard is a replica set of nodes that are responsible for the data in
/// that keyspace portion.
pub(crate) struct Shards<N: KeyspaceNode, const RF: usize>(Vec<ReplicaSet<N, RF>>);

impl<N: KeyspaceNode, const RF: usize> Clone for Shards<N, RF> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<N: KeyspaceNode, const RF: usize> Shards<N, RF> {
    /// Creates a new keyspace with each shard controlled by a replica set of
    /// nodes.
    pub fn new<R>(nodes: &Nodes<N>, replication_strategy: R) -> KeyspaceResult<Self>
    where
        N: KeyspaceNode,
        R: ReplicationStrategy,
    {
        if nodes.len() < RF {
            return Err(KeyspaceError::NotEnoughNodes(RF));
        }

        // Highest random weight (HRW) algorithm is used to select the nodes.
        let hrw = HrwNodes::new(nodes.values());

        let mut shards = Vec::with_capacity(ShardIdx::MAX.0 as usize + 1);
        for idx in 0..=ShardIdx::MAX.0 {
            // Each replica set gets a fresh copy of the replication strategy.
            let mut replication_strategy = replication_strategy.clone();
            let selected_replicas = hrw.sorted(&idx).filter_map(|node| {
                if replication_strategy.is_eligible_replica(&node) {
                    Some(node.clone())
                } else {
                    None
                }
            });

            shards.push(ReplicaSet::try_from_iter(selected_replicas)?);
        }

        Ok(Self(shards))
    }

    /// Iterator over the shards in the keyspace.
    pub fn iter(&self) -> impl Iterator<Item = Shard<N, RF>> {
        self.0
            .iter()
            .enumerate()
            .map(|(idx, replica_set)| Shard::new(ShardIdx(idx as u16), replica_set))
    }

    /// Returns the number of shards in the keyspace.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns replica set for the shard at the given index.
    pub fn replica_set(&self, idx: ShardIdx) -> &ReplicaSet<N, RF> {
        &self.0[idx.0 as usize]
    }
}
