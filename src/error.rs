#[derive(Debug, PartialEq, thiserror::Error)]
pub enum KeyspaceError {
    /// Not enough nodes for replication factor
    #[error("Not enough nodes for a given replication factor: {0}")]
    NotEnoughNodes(usize),

    /// Incomplete replica set
    #[error("Incomplete replica set")]
    IncompleteReplicaSet,

    /// Key space is not empty
    #[error("Non-empty keyspace")]
    NonEmptyKeyspace,

    /// No more indexes available in nodes to index mapping.
    #[error("Out of indexes in nodes to index mapping")]
    OutOfIndexes,
}

pub type KeyspaceResult<T> = Result<T, KeyspaceError>;
