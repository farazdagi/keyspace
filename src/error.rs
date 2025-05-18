#[derive(Debug, PartialEq, thiserror::Error)]
pub enum KeyspaceError {
    /// Not enough nodes for replication factor
    #[error("Not enough nodes for a given replication factor: {0}")]
    NotEnoughNodes(usize),

    /// Incomplete replica set
    #[error("Incomplete replica set")]
    IncompleteReplicaSet,
}

pub type KeyspaceResult<T> = Result<T, KeyspaceError>;
