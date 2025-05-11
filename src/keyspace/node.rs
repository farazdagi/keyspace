use std::{
    collections::{HashMap, VecDeque},
    fmt,
    hash::Hash,
    ops::{Index, Range},
    result::Result,
};

pub type NodeIdx = u16;

/// Node that stores data.
///
/// Node controls one or more intervals of the keyspace.
/// Keys which fall into such an interval are routed to the node (and its
/// replicas).
pub trait Node: Hash + Send + Sync + 'static {
    type NodeId: fmt::Debug + Hash + Eq;

    /// Returns the node id.
    fn id(&self) -> &Self::NodeId;

    /// Capacity of the node.
    ///
    /// The capacity is used to determine what portion of the keyspace the
    /// node will control. Since nodes are attached to keyspace portions using
    /// Highest Random Weight algorithm (HRW), the capacity affects the
    /// score of the node, thus the higher the capacity, the more likely the
    /// node will be chosen.
    ///
    /// Capacities of all nodes are summed up to determine the total capacity of
    /// the keyspace. The relative capacity of the node is then ratio of the
    /// node's capacity to the total capacity of the keyspace.
    fn capacity(&self) -> usize;
}

impl<T: Node> Node for &'static T {
    type NodeId = T::NodeId;

    fn id(&self) -> &Self::NodeId {
        (*self).id()
    }

    fn capacity(&self) -> usize {
        (*self).capacity()
    }
}

/// Reference to a node.
///
/// Internally nodes might be stored in maps or other data structures.
/// References to elements in such data structures are not valid if we cannot
/// guarantee that data structure's lifetime is longer than the reference's
/// lifetime. By wrapping the reference in a trait, we can bind the lifetime of
/// node reference and the underlying data structure.
pub trait NodeRef<'a, T: Node> {}

/// Nodes collection.
///
/// The collection assigns each node an index, which serves as a handle
/// throughout the rest of the system -- this way wherever we need to store the
/// node, we can just store the index (which is currently a `u16` number taking
/// up only two bytes).
pub struct Nodes<N: Node> {
    /// Stored nodes.
    nodes: HashMap<NodeIdx, N>,

    /// Next index that will be assigned to a node.
    ///
    /// If the free list is not empty, the next index will be taken from it.
    next_idx: NodeIdx,

    /// When a node is removed from, its index is added to this queue, so that
    /// it can be reused.
    free_list: VecDeque<NodeIdx>,
}

impl<N: Node> Default for Nodes<N> {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            next_idx: 0,
            free_list: VecDeque::new(),
        }
    }
}

impl<N: Node> Index<NodeIdx> for Nodes<N> {
    type Output = N;

    fn index(&self, idx: NodeIdx) -> &Self::Output {
        self.nodes.get(&idx).expect("Node not found")
    }
}

impl<N: Node> Nodes<N> {
    /// Creates a new empty nodes collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns index of the node with given id.
    ///
    /// Normally, all operations on nodes are done using the index, but in some
    /// cases node is is all we have, so we need to find the index of the node
    /// with given id. This will traverse the whole collection, so it is not
    /// recommended to overuse.
    pub fn idx(&self, id: &N::NodeId) -> Option<NodeIdx> {
        self.nodes.iter().find_map(
            |(idx, node)| {
                if node.id() == id { Some(*idx) } else { None }
            },
        )
    }

    /// Adds a node to the collection.
    ///
    /// Returns the index of the node in the collection.
    pub fn insert(&mut self, node: N) -> Result<NodeIdx, NodesError> {
        let idx = if let Some(idx) = self.free_list.pop_front() {
            idx
        } else {
            self.next_idx = self
                .next_idx
                .checked_add(1)
                .ok_or(NodesError::OutOfIndices)?;
            self.next_idx - 1
        };

        self.nodes.insert(idx, node);
        Ok(idx)
    }

    /// Removes and returns (if existed) a node from the collection.
    pub fn remove(&mut self, idx: NodeIdx) -> Option<N> {
        self.nodes.remove(&idx).and_then(|node| {
            self.free_list.push_back(idx);
            Some(node)
        })
    }

    /// Returns a reference to the node with given index.
    pub fn get(&self, idx: NodeIdx) -> Option<&N> {
        self.nodes.get(&idx)
    }

    /// Iterator over the nodes in the collection.
    pub fn iter(&self) -> impl Iterator<Item = (NodeIdx, &N)> {
        self.nodes.iter().map(|(idx, node)| (*idx, node))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NodesError {
    /// No more indices available.
    #[error("Out of indices")]
    OutOfIndices,
}
