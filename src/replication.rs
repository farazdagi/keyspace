use super::{KeyspaceError, KeyspaceResult, Node};

/// Replication strategy determines how to choose the nodes for redundancy.
///
/// Each instance of `ReplicationStrategy` is assumed to operate on a single
/// shard of the keyspace, i.e. a single replica set of nodes.
pub trait ReplicationStrategy {
    /// Checks if the given node is eligible for inclusion into a replica set.
    fn is_eligible_replica<N: Node>(&mut self, node: &N) -> bool;

    /// Builds a new instance of the replication strategy.
    fn clone(&self) -> Self;
}

/// Default replication strategy.
///
/// Any node is suitable for the default replication strategy.
pub struct DefaultReplicationStrategy {}

impl ReplicationStrategy for DefaultReplicationStrategy {
    fn is_eligible_replica<N: Node>(&mut self, _node: &N) -> bool {
        true
    }

    fn clone(&self) -> Self {
        Self {}
    }
}

/// Set of nodes that are used to store a replica of the data.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaSet<N, const REPLICATION_FACTOR: usize>([N; REPLICATION_FACTOR]);

impl<T, const REPLICATION_FACTOR: usize> ReplicaSet<T, REPLICATION_FACTOR>
where
    T: Default + Copy,
{
    pub fn try_from_iter<I: IntoIterator<Item = T>>(iter: I) -> KeyspaceResult<Self> {
        let mut items = [T::default(); REPLICATION_FACTOR];
        let mut count = 0;
        for (i, item) in iter.into_iter().take(REPLICATION_FACTOR).enumerate() {
            items[i] = item;
            count += 1;
        }
        if count < REPLICATION_FACTOR {
            return Err(KeyspaceError::IncompleteReplicaSet);
        }

        Ok(ReplicaSet(items))
    }
}
