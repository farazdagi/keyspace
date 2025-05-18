#[derive(Debug, PartialEq, thiserror::Error)]
pub enum KeyspaceError {
    /// Not enough nodes for replication factor
    #[error("Not enough nodes ({0}) for a given replication factor: {1}")]
    NotEnoughNodes(usize, usize),

    /// Incomplete replica set
    #[error("Incomplete replica set")]
    IncompleteReplicaSet,
}

pub type KeyspaceResult<T> = Result<T, KeyspaceError>;
