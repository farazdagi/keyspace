use {
    super::{
        KeyPosition,
        KeyspaceError,
        KeyspaceResult,
        Node,
        ReplicationStrategy,
        interval::KeyRange,
        node::{NodeIdx, Nodes},
        replication::ReplicaSet,
    },
    hrw_hash::HrwNodes,
    std::{hash::BuildHasher, ops::Deref},
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

/// Shard is a portion of the key space controlled by a set of nodes.
#[derive(Debug)]
pub(crate) struct Shard<'a, const RF: usize> {
    idx: ShardIdx,
    replica_set: &'a ReplicaSet<NodeIdx, RF>,
}

impl<'a, const RF: usize> Shard<'a, RF> {
    /// Creates a new shard with the given index and replica set.
    pub fn new(idx: ShardIdx, replica_set: &'a ReplicaSet<NodeIdx, RF>) -> Self {
        Self { idx, replica_set }
    }

    /// Returns the index of the shard.
    pub fn idx(&self) -> ShardIdx {
        self.idx
    }

    /// Returns the replica set of the shard.
    pub fn replica_set(&self) -> &ReplicaSet<NodeIdx, RF> {
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

/// Key space is uniformly divided into shards.
///
/// Each shard is a replica set of nodes that are responsible for the data
/// in that key space portion.
#[derive(Clone)]
pub(crate) struct Shards<const RF: usize>(Vec<ReplicaSet<NodeIdx, RF>>);

impl<const RF: usize> Shards<RF> {
    /// Creates a new key space with each shard controlled by a replica set of
    /// nodes.
    pub fn new<N, R, H>(nodes: &Nodes<N, H>, replication_strategy: R) -> KeyspaceResult<Self>
    where
        N: Node,
        R: ReplicationStrategy,
        H: BuildHasher,
    {
        if nodes.len() < RF {
            return Err(KeyspaceError::NotEnoughNodes(RF));
        }

        // Highest random weight (HRW) algorithm is used to select the nodes.
        let hrw = HrwNodes::new(nodes.keys().copied());

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

        Ok(Self(shards))
    }

    /// Iterator over the shards in the keyspace.
    pub fn iter(&self) -> impl Iterator<Item = Shard<RF>> {
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
    pub fn replica_set(&self, idx: ShardIdx) -> &ReplicaSet<NodeIdx, RF> {
        &self.0[idx.0 as usize]
    }
}
