use super::{Node};

/// Replication strategy determines how to choose the nodes for redundancy.
///
/// Each instance of `ReplicationStrategy` is assumed to operate on a single
/// shard of the keyspace, i.e. a single replica set of nodes.
pub trait ReplicationStrategy {
    /// Checks if the given node is eligible for inclusion into a replica set.
    fn is_eligible_replica<N: Node>(&mut self, node: &N) -> bool;
}

/// Creates a new instance of the replication strategy.
pub trait ReplicationStrategyBuilder<S: ReplicationStrategy> {
    /// Builds a new instance of the replication strategy.
    fn build(&self) -> S;
}

/// Default replication strategy.
///
/// Any node is suitable for the default replication strategy.
pub struct DefaultReplicationStrategy {}

impl ReplicationStrategy for DefaultReplicationStrategy {
    fn is_eligible_replica<N: Node>(&mut self, _node: &N) -> bool {
        true
    }
}

impl ReplicationStrategyBuilder<DefaultReplicationStrategy> for DefaultReplicationStrategy {
    fn build(&self) -> Self {
        Self {}
    }
}
