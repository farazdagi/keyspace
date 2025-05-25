use {
    auto_impl::auto_impl,
    rapidhash::RapidBuildHasher,
    std::{
        borrow::Borrow,
        collections::HashMap,
        hash::{BuildHasher, Hash},
        ops::{Deref, Index},
        sync::Arc,
    },
};

/// Node that stores data.
///
/// Node controls one or more intervals of the keyspace.
/// Keys which fall into such an interval are routed to the node (and its
/// replicas).
#[auto_impl(&)]
pub trait Node: std::fmt::Debug + Hash + PartialEq + Eq + 'static {
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

impl_node!(
    u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, String, str
);

/// Reference to a node.
#[derive(Debug, Hash)]
pub struct NodeRef<N>(Arc<N>);

impl<N> From<N> for NodeRef<N> {
    fn from(node: N) -> Self {
        Self(Arc::new(node))
    }
}

impl<N> Deref for NodeRef<N> {
    type Target = Arc<N>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<N> AsRef<N> for NodeRef<N> {
    fn as_ref(&self) -> &N {
        self.0.as_ref()
    }
}

impl<N> Borrow<N> for NodeRef<N> {
    fn borrow(&self) -> &N {
        self.0.as_ref()
    }
}

// NodeRef<T> == NodeRef<T>
impl<T: PartialEq> PartialEq for NodeRef<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: PartialEq> Eq for NodeRef<T> {}

// NodeRef<T> == T
impl<T: PartialEq> PartialEq<T> for NodeRef<T> {
    fn eq(&self, other: &T) -> bool {
        self.0.as_ref() == other
    }
}

// NodeRef<T> == Arc<T>
impl<T: PartialEq> PartialEq<Arc<T>> for NodeRef<T> {
    fn eq(&self, other: &Arc<T>) -> bool {
        self.0 == *other
    }
}

// NodeRef<T> == &T
impl<T: PartialEq> PartialEq<&T> for NodeRef<T> {
    fn eq(&self, other: &&T) -> bool {
        self.0.as_ref() == *other
    }
}

// NodeRef<String> == &str
impl PartialEq<&str> for NodeRef<String> {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_str() == *other
    }
}

// Vec<NodeRef<T>> == Vec<&T>
impl<T: PartialEq> PartialEq<Vec<&T>> for NodeRef<Vec<T>> {
    fn eq(&self, other: &Vec<&T>) -> bool {
        self.0.iter().zip(other.iter()).all(|(a, b)| a == *b)
    }
}

// Vec<NodeRef<String>> == Vec<&str>
impl PartialEq<Vec<&str>> for NodeRef<Vec<String>> {
    fn eq(&self, other: &Vec<&str>) -> bool {
        self.0.iter().zip(other.iter()).all(|(a, b)| a == *b)
    }
}

impl<N: Node> Clone for NodeRef<N> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<N: Node> NodeRef<N> {
    /// Creates a new node reference.
    fn new(node: N) -> Self {
        Self(Arc::new(node))
    }

    /// Returns the inner node.
    pub fn inner(&self) -> &N {
        self.0.as_ref()
    }
}

/// Node hash.
pub(crate) type NodeIdx = u64;

/// Nodes collection.
///
/// The collection assigns each node an index (by hashing the node), which
/// serves as a handle throughout the rest of the system. This way wherever we
/// need to store the node, we store the index (which takes 8 bytes, `u64`).
#[derive(Debug, Clone)]
pub(crate) struct Nodes<N: Node, H: BuildHasher = RapidBuildHasher> {
    nodes: HashMap<NodeIdx, NodeRef<N>>,
    build_hasher: H,
}

impl<N: Node, H: BuildHasher> Deref for Nodes<N, H> {
    type Target = HashMap<NodeIdx, NodeRef<N>>;
    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
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
        }
    }

    /// Adds a node to the collection.
    ///
    /// Returns the index of the node in the collection.
    pub fn insert(&mut self, node: N) -> NodeIdx {
        let idx = self.build_hasher.hash_one(&node);
        self.nodes.insert(idx, NodeRef::new(node));

        idx
    }

    /// Removes and returns (if existed) a node from the collection.
    pub fn remove(&mut self, idx: NodeIdx) -> Option<NodeRef<N>> {
        self.nodes.remove(&idx).and_then(|node| {
            Some(node)
        })
    }

    /// Returns index of a given node.
    /// Normally, the index is calculated by hashing the node.
    pub fn idx(&self, node: &N) -> NodeIdx {
        self.build_hasher.hash_one(node)
    }

    /// Returns a reference to the node with given index.
    pub fn get(&self, idx: NodeIdx) -> Option<NodeRef<N>> {
        self.nodes.get(&idx).and_then(|node| Some(node.clone()))
    }


    /// Exposes the underlying hasher.
    pub fn build_hasher(&self) -> &H {
        &self.build_hasher
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_node(node: &TestNode, id: String, capacity: usize) {
        assert_eq!(node.id(), &id);
        assert_eq!(node.capacity(), capacity);
    }

    #[derive(Hash, Debug, PartialEq, Eq)]
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
            check_node(&nodes[*idx], node.id.clone(), node.capacity);
        }

        // Remove nodes and check that they are removed
        let remove_idx = indexes[3];
        let removed_node = nodes.remove(remove_idx).unwrap();
        assert_eq!(removed_node.id(), "node3");
        assert_eq!(nodes.len(), 4);
        assert!(nodes.get(remove_idx).is_none());
    }
}
