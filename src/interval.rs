use {
    super::Node,
    std::{collections::HashSet, ops::Range},
};

/// Position of a key in the keyspace.
type Position = u64;

/// A half-open interval of the keyspace with responsible nodes assigned.
///
/// Range bounded inclusively below and exclusively above i.e.
/// `[start..end)`.
pub struct Interval<NODES> {
    key_range: Range<Position>,
    nodes: NODES,
}

/// Interval used to specify source to pull data from, for a given key range.
///
/// A set of nodes responsible for the interval as a `HashSet`.
pub type PendingInterval<N> = Interval<HashSet<<N as Node>::NodeId>>;

/// Interval with a reference to the nodes of the keyspace.
pub type KeyspaceInterval<'a, N> = Interval<&'a [N]>;
