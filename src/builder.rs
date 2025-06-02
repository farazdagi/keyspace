use {
    super::{DefaultReplicationStrategy, Keyspace, KeyspaceResult, KeyspaceNode, ReplicationStrategy},
    rapidhash::RapidBuildHasher,
    std::hash::BuildHasher,
};

/// Keyspace builder.
pub struct KeyspaceBuilder<N: KeyspaceNode, H: BuildHasher = RapidBuildHasher>(Vec<N>, H);

impl<N: KeyspaceNode> KeyspaceBuilder<N> {
    /// Create new keyspace builder.
    pub fn new<I: IntoIterator<Item = N>>(init_nodes: I) -> Self {
        Self::with_build_hasher(init_nodes, RapidBuildHasher::default())
    }
}

impl<N: KeyspaceNode, H: BuildHasher> KeyspaceBuilder<N, H> {
    /// Create new keyspace builder.
    pub fn with_build_hasher<I>(init_nodes: I, build_hasher: H) -> Self
    where
        I: IntoIterator<Item = N>,
    {
        Self(init_nodes.into_iter().collect(), build_hasher)
    }

    /// Transform the builder into one with a different replication factor.
    pub fn with_replication_factor<const RF: usize>(
        self,
    ) -> KeyspaceBuilderWithReplicationFactor<N, DefaultReplicationStrategy, RF, H> {
        KeyspaceBuilderWithReplicationFactor(self.0, DefaultReplicationStrategy::new(), self.1)
    }

    /// Transform the builder into one with a different replication strategy.
    pub fn with_replication_strategy<R: ReplicationStrategy>(
        self,
        replication_strategy: R,
    ) -> KeyspaceBuilderWithReplicationStrategy<N, R, 3, H> {
        KeyspaceBuilderWithReplicationStrategy(self.0, replication_strategy, self.1)
    }

    /// Build the keyspace.
    pub fn build(self) -> KeyspaceResult<Keyspace<N, DefaultReplicationStrategy, 3, H>> {
        Keyspace::with_build_hasher(self.1, self.0, DefaultReplicationStrategy::new())
    }
}

/// Keyspace builder with custom replication strategy.
pub struct KeyspaceBuilderWithReplicationStrategy<N, R, const RF: usize, H>(Vec<N>, R, H);

impl<N, R, const RF: usize, H> KeyspaceBuilderWithReplicationStrategy<N, R, RF, H>
where
    N: KeyspaceNode,
    R: ReplicationStrategy,
    H: BuildHasher,
{
    /// Transform the builder into one with a different replication factor.
    pub fn with_replication_factor<const CUSTOM_RF: usize>(
        self,
    ) -> KeyspaceBuilderWithReplicationFactor<N, R, CUSTOM_RF, H> {
        KeyspaceBuilderWithReplicationFactor(self.0, self.1, self.2)
    }

    /// Build the keyspace with the given replication strategy and default
    /// replication factor.
    pub fn build(self) -> KeyspaceResult<Keyspace<N, R, RF, H>> {
        Keyspace::with_build_hasher(self.2, self.0, self.1)
    }
}

/// Keyspace builder with custom replication factor.
pub struct KeyspaceBuilderWithReplicationFactor<N, R, const RF: usize, H>(Vec<N>, R, H);

impl<N, R, const RF: usize, H> KeyspaceBuilderWithReplicationFactor<N, R, RF, H>
where
    N: KeyspaceNode,
    H: BuildHasher,
{
    /// Transform the builder into one with a different replication strategy.
    pub fn with_replication_strategy<CustomR: ReplicationStrategy>(
        self,
        replication_strategy: CustomR,
    ) -> KeyspaceBuilderWithReplicationStrategy<N, CustomR, RF, H> {
        KeyspaceBuilderWithReplicationStrategy(self.0, replication_strategy, self.2)
    }

    /// Build the keyspace with the given replication factor and default
    /// replication strategy.
    pub fn build(self) -> KeyspaceResult<Keyspace<N, DefaultReplicationStrategy, RF, H>> {
        Keyspace::with_build_hasher(self.2, self.0, DefaultReplicationStrategy::new())
    }
}
