use super::{KeyPosition, Node, NodeRef};

/// A range of keys in the keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRange {
    Bounded(KeyPosition, KeyPosition),
    Unbounded(KeyPosition),
}

impl KeyRange {
    /// Create a new key range from the given start and end positions.
    pub fn new(start: KeyPosition, end: Option<KeyPosition>) -> Self {
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
#[derive(Debug, PartialEq, Eq)]
pub struct Interval<N: Node> {
    key_range: KeyRange,
    nodes: Vec<NodeRef<N>>,
}

impl<N: Node> Clone for Interval<N> {
    fn clone(&self) -> Self {
        Self {
            key_range: self.key_range,
            nodes: self.nodes.clone(),
        }
    }
}

impl<N: Node> Interval<N> {
    /// Creates a new interval with the given key range and nodes.
    pub(crate) fn new<I: IntoIterator<Item = NodeRef<N>>>(key_range: KeyRange, nodes: I) -> Self {
        let nodes = nodes.into_iter().collect::<Vec<_>>();
        Self { key_range, nodes }
    }

    /// Returns the key range of the interval.
    pub fn key_range(&self) -> &KeyRange {
        &self.key_range
    }

    /// Returns the nodes responsible for the interval.
    pub fn nodes(&self) -> &Vec<NodeRef<N>> {
        &self.nodes
    }
}
