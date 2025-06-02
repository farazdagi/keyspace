use {
    super::{KeyspaceError, KeyspaceResult, KeyspaceNode, NodeRef},
    std::ops::Deref,
};

/// Replication strategy determines how to choose the nodes for redundancy.
///
/// Each instance of `ReplicationStrategy` is assumed to operate on a single
/// shard of the keyspace, i.e. a single replica set of nodes.
pub trait ReplicationStrategy {
    /// Checks if the given node is eligible for inclusion into a replica set.
    fn is_eligible_replica<N: KeyspaceNode>(&mut self, node: &NodeRef<N>) -> bool;

    /// Builds a new instance of the replication strategy.
    fn clone(&self) -> Self;
}

/// Default replication strategy.
///
/// Any node is suitable for the default replication strategy.
#[derive(Debug, Clone, Copy)]
pub struct DefaultReplicationStrategy {}

impl ReplicationStrategy for DefaultReplicationStrategy {
    fn is_eligible_replica<N: KeyspaceNode>(&mut self, _node: &NodeRef<N>) -> bool {
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
#[derive(Debug)]
pub(crate) struct ReplicaSet<N: KeyspaceNode, const RF: usize>([NodeRef<N>; RF]);

impl<N: KeyspaceNode, const RF: usize> Clone for ReplicaSet<N, RF> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<N: KeyspaceNode, const RF: usize> PartialEq for ReplicaSet<N, RF> {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0.iter().all(|n| other.0.contains(n))
    }
}

impl<N: KeyspaceNode, const RF: usize> Eq for ReplicaSet<N, RF> {}

impl<N: KeyspaceNode, const RF: usize> Deref for ReplicaSet<N, RF> {
    type Target = [NodeRef<N>; RF];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<N: KeyspaceNode, const RF: usize> ReplicaSet<N, RF> {
    pub fn try_from_iter<I: IntoIterator<Item = NodeRef<N>>>(iter: I) -> KeyspaceResult<Self> {
        use std::array::from_fn;
        let mut iter = iter.into_iter();
        let mut count = 0;
        let items: [NodeRef<N>; RF] = from_fn(|_| {
            iter.next()
                .and_then(|item| {
                    count += 1;
                    Some(item)
                })
                .unwrap_or_default()
        });

        if count < RF {
            return Err(KeyspaceError::IncompleteReplicaSet);
        }

        Ok(ReplicaSet(items))
    }
}
