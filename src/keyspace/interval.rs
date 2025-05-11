use {
    crate::keyspace::{Node, NodeIdx},
    std::ops::Range,
};

/// Position of a key in the keyspace.
type Position = u64;

/// A half-open interval of the keyspace with responsible nodes assigned.
///
/// Range bounded inclusively below and exclusively above i.e.
/// `[start..end)`.
pub struct Interval<'a> {
    key_range: Range<Position>,
    replicas: &'a [NodeIdx],
}

