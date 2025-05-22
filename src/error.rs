#[derive(Debug, PartialEq, thiserror::Error)]
pub enum KeyspaceError {
    /// Not enough nodes for replication factor
    #[error("Not enough nodes for a given replication factor: {0}")]
    NotEnoughNodes(usize),

    /// Incomplete replica set
    #[error("Incomplete replica set")]
    IncompleteReplicaSet,

    /// Keyspace is not empty
    #[error("Non-empty keyspace")]
    NonEmptyKeyspace,

    /// No more indexes available in nodes to index mapping.
    #[error("Out of indexes in nodes to index mapping")]
    OutOfIndexes,

    /// Shards not initialized
    #[error("Shards not initialized")]
    ShardsNotInitialized,

    /// Number of shards in new and old keyspace do not match
    #[error("Number of shards in new and old keyspace do not match")]
    ShardCountMismatch,
}

pub type KeyspaceResult<T> = Result<T, KeyspaceError>;
