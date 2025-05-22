use {
    super::{
        KeyPosition,
        Node,
        node::{NodeIdx, Nodes},
    },
    std::{collections::HashSet, ops::Range},
};

/// A range of keys in the keyspace.
#[derive(Debug, Clone, Copy)]
pub enum KeyRange {
    Bounded(KeyPosition, KeyPosition),
    Unbounded(KeyPosition),
}

impl KeyRange {
    /// Create a new key range from the given start and end positions.
    pub(crate) fn new(start: KeyPosition, end: Option<KeyPosition>) -> Self {
        match end {
            Some(end) => KeyRange::Bounded(start, end),
            None => KeyRange::Unbounded(start),
        }
    }

    /// Check if the given key is in the range.
    ///
    /// Note not the key itself, but the hash of the key provides the position
    /// within keyspace.
    pub fn contains(&self, key_hash: KeyPosition) -> bool {
        match self {
            KeyRange::Bounded(start, end) => key_hash >= *start && key_hash < *end,
            KeyRange::Unbounded(start) => key_hash >= *start,
        }
    }
}

/// A half-open interval of the keyspace with responsible nodes assigned.
///
/// Range bounded inclusively below and exclusively above i.e.
/// `[start..end)`.
pub struct Interval<NODES> {
    key_range: KeyRange,
    nodes: NODES,
}

impl<NODES> Interval<NODES> {
    /// Creates a new interval with the given key range and nodes.
    pub(crate) fn new(key_range: KeyRange, nodes: NODES) -> Self {
        Self { key_range, nodes }
    }

    /// Returns the key range of the interval.
    pub fn key_range(&self) -> &KeyRange {
        &self.key_range
    }

    /// Returns the nodes responsible for the interval.
    pub fn nodes(&self) -> &NODES {
        &self.nodes
    }
}

/// Interval that, for a given key range, specifies source to pull data from.
///
/// A set of nodes responsible for the interval as a `HashSet`.
pub type PendingInterval<'a, N> = Interval<HashSet<&'a N>>;

/// Interval with a reference to the nodes of the keyspace.
pub type KeyspaceInterval<'a, N> = Interval<&'a [N]>;
