use {
    super::{KeyspaceError, KeyspaceResult, Node, NodeRef},
    std::{collections::BTreeSet, ops::Deref},
};

/// Replication strategy determines how to choose the nodes for redundancy.
///
/// Each instance of `ReplicationStrategy` is assumed to operate on a single
/// shard of the keyspace, i.e. a single replica set of nodes.
pub trait ReplicationStrategy {
    /// Checks if the given node is eligible for inclusion into a replica set.
    fn is_eligible_replica<N: Node>(&mut self, node: NodeRef<N>) -> bool;

    /// Builds a new instance of the replication strategy.
    fn clone(&self) -> Self;
}

/// Default replication strategy.
///
/// Any node is suitable for the default replication strategy.
#[derive(Debug, Clone, Copy)]
pub struct DefaultReplicationStrategy {}

impl ReplicationStrategy for DefaultReplicationStrategy {
    fn is_eligible_replica<N: Node>(&mut self, _node: NodeRef<N>) -> bool {
        true
    }

    fn clone(&self) -> Self {
        Self {}
    }
}

impl Default for DefaultReplicationStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultReplicationStrategy {
    /// Creates a new instance of the default replication strategy.
    pub fn new() -> Self {
        Self {}
    }
}

/// Set of nodes that are used to store a replica of the data.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplicaSet<N, const RF: usize>([N; RF]);

impl<N: Ord + Eq, const RF: usize> PartialEq for ReplicaSet<N, RF> {
    fn eq(&self, other: &Self) -> bool {
        let self_set: BTreeSet<_> = self.0.iter().collect();
        let other_set: BTreeSet<_> = other.0.iter().collect();
        self_set == other_set
    }
}

impl<N: Ord + Eq, const RF: usize> Eq for ReplicaSet<N, RF> {}

impl<N, const RF: usize> Deref for ReplicaSet<N, RF> {
    type Target = [N; RF];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, const RF: usize> ReplicaSet<T, RF>
where
    T: Default + Copy,
{
    pub fn try_from_iter<I: IntoIterator<Item = T>>(iter: I) -> KeyspaceResult<Self> {
        let mut items = [T::default(); RF];
        let mut count = 0;
        for (i, item) in iter.into_iter().take(RF).enumerate() {
            items[i] = item;
            count += 1;
        }
        if count < RF {
            return Err(KeyspaceError::IncompleteReplicaSet);
        }

        Ok(ReplicaSet(items))
    }
}
