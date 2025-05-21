use {
    auto_impl::auto_impl,
    rapidhash::RapidBuildHasher,
    std::{
        collections::HashMap,
        hash::{BuildHasher, Hash},
        ops::Index,
    },
};

/// Node that stores data.
///
/// Node controls one or more intervals of the keyspace.
/// Keys which fall into such an interval are routed to the node (and its
/// replicas).
#[auto_impl(&)]
pub trait Node: Hash + 'static {
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
    fn capacity(&self) -> usize {
        1
    }
}

macro_rules! impl_node {
    ($($t:ty),*) => {
        $(
            impl Node for $t {}
        )*
    };
}

impl_node!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, String);

/// Reference to a node.
///
/// Internally nodes might be stored in maps or other data structures.
/// References to elements in such data structures are not valid if we cannot
/// guarantee that data structure's lifetime is longer than the reference's
/// lifetime. By wrapping the reference in a trait, we can bind the lifetime of
/// node reference and the underlying data structure.
pub trait NodeRef<'a, T: Node> {}

impl<'a, T: Node> NodeRef<'a, T> for &'a T {}

/// Node hash.
pub(crate) type NodeIdx = u64;

/// Nodes collection.
///
/// The collection assigns each node an index (by hashing the node), which
/// serves as a handle throughout the rest of the system. This way wherever we
/// need to store the node, we store the index (which takes 8 bytes, `u64`).
pub(crate) struct Nodes<N: Node, H: BuildHasher = RapidBuildHasher> {
    nodes: HashMap<NodeIdx, N>,
    build_hasher: H,
    version: u64,
}

impl<N: Node> Default for Nodes<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Node, H: BuildHasher> Index<NodeIdx> for Nodes<N, H> {
    type Output = N;

    fn index(&self, idx: NodeIdx) -> &Self::Output {
        self.nodes.get(&idx).expect("Node not found")
    }
}

impl<N: Node> Nodes<N> {
    /// Creates a new empty nodes collection.
    pub fn new() -> Self {
        Self::with_build_hasher(RapidBuildHasher::default())
    }
}

impl<N: Node, H: BuildHasher> Nodes<N, H> {
    /// Creates a new empty nodes collection with the given hasher.
    pub fn with_build_hasher(build_hasher: H) -> Self {
        Self {
            nodes: HashMap::new(),
            build_hasher,
            version: 0,
        }
    }

    /// Adds a node to the collection.
    ///
    /// Returns the index of the node in the collection.
    pub fn insert(&mut self, node: N) -> NodeIdx {
        let idx = self.build_hasher.hash_one(&node);
        self.nodes.insert(idx, node);
        self.version += 1;

        idx
    }

    /// Removes and returns (if existed) a node from the collection.
    pub fn remove(&mut self, idx: NodeIdx) -> Option<N> {
        self.nodes.remove(&idx).and_then(|node| {
            self.version += 1;
            Some(node)
        })
    }

    /// Returns index of a given node.
    /// Normally, the index is calculated by hashing the node.
    pub fn idx(&self, node: &N) -> NodeIdx {
        self.build_hasher.hash_one(node)
    }

    /// Returns a reference to the node with given index.
    pub fn get(&self, idx: NodeIdx) -> Option<&N> {
        self.nodes.get(&idx)
    }

    /// Returns the version of the collection.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Number of nodes in the collection.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Iterator over the nodes in the collection.
    pub fn iter(&self) -> impl Iterator<Item = (NodeIdx, &N)> {
        self.nodes.iter().map(|(idx, node)| (*idx, node))
    }

    /// Iterator over the indices of the nodes in the collection.
    ///
    /// Only valid indexes are returned, i.e. indexes that are not in the free
    /// list.
    pub fn indexes(&self) -> impl Iterator<Item = NodeIdx> {
        self.nodes.keys().copied()
    }

    /// Given iterator of node indexes, returns an iterator over the nodes.
    pub fn filter_nodes<'a>(
        &'a self,
        indexes: impl Iterator<Item = NodeIdx> + 'a,
    ) -> impl Iterator<Item = &'a N> {
        indexes.filter_map(move |idx| self.nodes.get(&idx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_node(node: &TestNode, id: String, capacity: usize) {
        assert_eq!(node.id(), &id);
        assert_eq!(node.capacity(), capacity);
    }

    #[derive(Hash)]
    struct TestNode {
        id: String,
        capacity: usize,
    }

    impl Node for TestNode {
        fn capacity(&self) -> usize {
            self.capacity
        }
    }

    impl TestNode {
        fn id(&self) -> &String {
            &self.id
        }

        fn capacity(&self) -> usize {
            self.capacity
        }
    }

    #[test]
    fn basic_ops() {
        let mut nodes = Nodes::new();
        let mut indexes = vec![];

        (0..5).for_each(|i| {
            let node = TestNode {
                id: format!("node{}", i),
                capacity: 10,
            };
            let idx = nodes.insert(node);
            check_node(&nodes[idx], format!("node{}", i), 10);
            indexes.push(idx);
        });

        // Check that the nodes are in the collection
        for (idx, node) in nodes.iter() {
            check_node(&nodes[idx], node.id.clone(), node.capacity);
        }

        // Remove nodes and check that they are removed
        let remove_idx = indexes[3];
        let removed_node = nodes.remove(remove_idx).unwrap();
        assert_eq!(removed_node.id(), "node3");
        assert_eq!(nodes.len(), 4);
        assert!(nodes.get(remove_idx).is_none());
    }
}
