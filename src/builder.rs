use super::{DefaultReplicationStrategy, Keyspace, KeyspaceResult, Node, ReplicationStrategy};

/// Keyspace builder.
pub struct KeyspaceBuilder<N: Node>(Vec<N>);

impl<N: Node> KeyspaceBuilder<N> {
    /// Create new keyspace builder.
    pub fn new<I: IntoIterator<Item = N>>(init_nodes: I) -> Self {
        Self(init_nodes.into_iter().collect())
    }

    /// Transform the builder into one with a different replication factor.
    pub fn with_replication_factor<const RF: usize>(
        self,
    ) -> KeyspaceBuilderWithReplicationFactor<N, DefaultReplicationStrategy, RF> {
        KeyspaceBuilderWithReplicationFactor(self.0, DefaultReplicationStrategy::new())
    }

    /// Transform the builder into one with a different replication strategy.
    pub fn with_replication_strategy<R: ReplicationStrategy>(
        self,
        replication_strategy: R,
    ) -> KeyspaceBuilderWithReplicationStrategy<N, R, 3> {
        KeyspaceBuilderWithReplicationStrategy(self.0, replication_strategy)
    }

    /// Build the keyspace.
    pub fn build(self) -> KeyspaceResult<Keyspace<N, DefaultReplicationStrategy, 3>> {
        Keyspace::create(self.0, DefaultReplicationStrategy::new())
    }
}

/// Keyspace builder with custom replication strategy.
pub struct KeyspaceBuilderWithReplicationStrategy<N, R, const RF: usize>(Vec<N>, R);

impl<N, R, const RF: usize> KeyspaceBuilderWithReplicationStrategy<N, R, RF>
where
    N: Node,
    R: ReplicationStrategy,
{
    /// Transform the builder into one with a different replication factor.
    pub fn with_replication_factor<const CUSTOM_RF: usize>(
        self,
    ) -> KeyspaceBuilderWithReplicationFactor<N, R, CUSTOM_RF> {
        KeyspaceBuilderWithReplicationFactor(self.0, self.1)
    }

    /// Build the keyspace with the given replication strategy and default
    /// replication factor.
    pub fn build(self) -> KeyspaceResult<Keyspace<N, R, RF>> {
        Keyspace::create(self.0, self.1)
    }
}

/// Keyspace builder with custom replication factor.
pub struct KeyspaceBuilderWithReplicationFactor<N, R, const RF: usize>(Vec<N>, R);

impl<N, R, const RF: usize> KeyspaceBuilderWithReplicationFactor<N, R, RF>
where
    N: Node,
{
    /// Transform the builder into one with a different replication strategy.
    pub fn with_replication_strategy<CustomR: ReplicationStrategy>(
        self,
        replication_strategy: CustomR,
    ) -> KeyspaceBuilderWithReplicationStrategy<N, CustomR, RF> {
        KeyspaceBuilderWithReplicationStrategy(self.0, replication_strategy)
    }

    /// Build the keyspace with the given replication factor and default
    /// replication strategy.
    pub fn build(self) -> KeyspaceResult<Keyspace<N, DefaultReplicationStrategy, RF>> {
        Keyspace::create(self.0, DefaultReplicationStrategy::new())
    }
}
