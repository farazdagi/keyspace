use {
    auto_impl::auto_impl,
    hrw_hash::HrwNode,
    parking_lot::RwLock,
    std::{borrow::Borrow, collections::HashMap, fmt, hash::Hash, ops::Deref, sync::Arc},
};

/// Node that stores data.
///
/// Node controls one or more intervals of the keyspace.
/// Keys which fall into such an interval are routed to the node (and its
/// replicas).
#[auto_impl(&)]
pub trait KeyspaceNode: fmt::Debug + Hash + PartialEq + Eq {
    type Id: fmt::Debug + Hash + PartialEq + Eq + Clone;

    /// Returns the unique identifier of the node.
    fn id(&self) -> &Self::Id;

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

macro_rules! impl_keyspace_node {
    ($($t:ty),+) => {
        $(
            impl KeyspaceNode for $t {
                type Id = $t;

                fn id(&self) -> &Self::Id {
                    self
                }

                fn capacity(&self) -> usize {
                    1
                }
            }
        )+
    };
}

impl_keyspace_node!(String, &'static str, u8, u16, u32, u64, usize);

/// Reference to a node.
#[derive(Debug, Hash)]
pub struct NodeRef<N>(Option<Arc<N>>);

impl<N: KeyspaceNode> HrwNode for NodeRef<N> {
    fn capacity(&self) -> usize {
        match self.0.as_ref() {
            Some(node) => node.capacity(),
            None => 0,
        }
    }
}

impl<N> Default for NodeRef<N> {
    fn default() -> Self {
        Self(None)
    }
}

impl<N: KeyspaceNode> Clone for NodeRef<N> {
    fn clone(&self) -> Self {
        match self.0.as_ref() {
            Some(node) => NodeRef(Some(Arc::clone(node))),
            None => NodeRef(None),
        }
    }
}

impl<N> From<N> for NodeRef<N> {
    fn from(node: N) -> Self {
        Self(Some(Arc::new(node)))
    }
}

impl<N> Deref for NodeRef<N> {
    type Target = Arc<N>;
    fn deref(&self) -> &Self::Target {
        self.0.as_ref().expect("Cannot deref an empty NodeRef")
    }
}

impl<N> AsRef<N> for NodeRef<N> {
    fn as_ref(&self) -> &N {
        self.0.as_ref().expect("Cannot reference an empty NodeRef")
    }
}

impl<N> Borrow<N> for NodeRef<N> {
    fn borrow(&self) -> &N {
        self.0.as_ref().expect("Cannot borrow from empty NodeRef")
    }
}

// NodeRef<T> == NodeRef<T>
impl<T: PartialEq> PartialEq for NodeRef<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self.0.as_ref(), other.0.as_ref()) {
            (Some(a), Some(b)) => a.as_ref() == b.as_ref(),
            (None, None) => true,
            _ => false,
        }
    }
}

impl<T: PartialEq> Eq for NodeRef<T> {}

// NodeRef<T> == T
impl<T: PartialEq> PartialEq<T> for NodeRef<T> {
    fn eq(&self, other: &T) -> bool {
        match self.0.as_ref() {
            Some(node) => node.as_ref() == other,
            None => false,
        }
    }
}

// NodeRef<T> == Arc<T>
impl<T: PartialEq> PartialEq<Arc<T>> for NodeRef<T> {
    fn eq(&self, other: &Arc<T>) -> bool {
        match self.0.as_ref() {
            Some(node) => node.as_ref() == other.as_ref(),
            None => false,
        }
    }
}

// NodeRef<T> == &T
impl<T: PartialEq> PartialEq<&T> for NodeRef<T> {
    fn eq(&self, other: &&T) -> bool {
        match self.0.as_ref() {
            Some(node) => node.as_ref() == *other,
            None => false,
        }
    }
}

// NodeRef<String> == &str
impl PartialEq<&str> for NodeRef<String> {
    fn eq(&self, other: &&str) -> bool {
        match self.0.as_ref() {
            Some(node) => node.as_str() == *other,
            None => false,
        }
    }
}

// Vec<NodeRef<T>> == Vec<&T>
impl<T: PartialEq> PartialEq<Vec<&T>> for NodeRef<Vec<T>> {
    fn eq(&self, other: &Vec<&T>) -> bool {
        match self.0.as_ref() {
            Some(nodes) => nodes.iter().zip(other.iter()).all(|(a, b)| a == *b),
            None => other.is_empty(),
        }
    }
}

// Vec<NodeRef<String>> == Vec<&str>
impl PartialEq<Vec<&str>> for NodeRef<Vec<String>> {
    fn eq(&self, other: &Vec<&str>) -> bool {
        match self.0.as_ref() {
            Some(nodes) => nodes
                .iter()
                .zip(other.iter())
                .all(|(a, b)| a.as_str() == *b),
            None => other.is_empty(),
        }
    }
}

impl<N: KeyspaceNode> NodeRef<N> {
    /// Creates a new node reference.
    pub fn new(node: N) -> Self {
        Self(Some(Arc::new(node)))
    }

    /// Returns the inner node.
    pub fn inner(&self) -> &N {
        self.0
            .as_ref()
            .expect("Cannot get inner node from an empty NodeRef")
    }
}

/// Nodes collection.
///
/// The collection assigns each node an index (by hashing the node), which
/// serves as a handle throughout the rest of the system. This way wherever we
/// need to store the node, we store the index (which takes 8 bytes, `u64`).
#[derive(Debug, Clone)]
pub(crate) struct Nodes<N: KeyspaceNode>(Arc<RwLock<HashMap<N::Id, NodeRef<N>>>>);

impl<N: KeyspaceNode> Default for Nodes<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused)]
impl<N: KeyspaceNode> Nodes<N> {
    /// Creates a new empty nodes collection.
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    /// Creates a new nodes collection from an iterator of nodes.
    pub fn from_iter<I>(nodes: I) -> Self
    where
        I: IntoIterator<Item = N>,
    {
        Self(Arc::new(RwLock::new(HashMap::from_iter(
            nodes
                .into_iter()
                .map(|node| (node.id().clone(), NodeRef::new(node))),
        ))))
    }

    /// Adds a node to the collection.
    ///
    /// If the node with given ID was already present, the value is updated, and
    /// the old value is returned.
    pub fn insert(&self, node: N) -> Option<NodeRef<N>> {
        self.0.write().insert(node.id().clone(), NodeRef::new(node))
    }

    /// Removes and returns (if existed) a node from the collection.
    pub fn remove(&self, id: &N::Id) -> Option<NodeRef<N>> {
        self.0.write().remove(id)
    }

    /// Returns a reference to the node with given index.
    pub fn get(&self, id: N::Id) -> Option<NodeRef<N>> {
        self.0.read().get(&id).and_then(|node| Some(node.clone()))
    }

    /// Number of nodes in the collection.
    pub fn len(&self) -> usize {
        self.0.read().len()
    }

    /// Checks if the collection contains a node.
    pub fn contains(&self, id: &N::Id) -> bool {
        self.0.read().contains_key(id)
    }

    /// Node IDs in the collection.
    pub fn keys(&self) -> Vec<N::Id> {
        self.0.read().keys().map(|key| key.clone()).collect()
    }

    /// Node references in the collection.
    pub fn values(&self) -> Vec<NodeRef<N>> {
        self.0.read().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::{
            net::{IpAddr, SocketAddr},
            str::FromStr,
        },
    };

    #[derive(Debug, Hash, PartialEq, Eq, Clone)]
    struct Node {
        id: String,
        addr: SocketAddr,
        capacity: usize,
    }

    impl Node {
        fn new(id: &str, ip: &str, port: u16, capacity: usize) -> Self {
            let addr = SocketAddr::new(IpAddr::from_str(&ip).unwrap(), port);
            Self {
                id: id.to_string(),
                addr,
                capacity,
            }
        }
    }

    impl ToString for Node {
        fn to_string(&self) -> String {
            format!("{}|{}", self.addr, self.id)
        }
    }

    impl KeyspaceNode for Node {
        type Id = String;

        fn id(&self) -> &Self::Id {
            &self.id
        }

        fn capacity(&self) -> usize {
            self.capacity
        }
    }

    #[test]
    fn basic_ops() {
        let nodes = Nodes::new();

        // Insert nodes
        let node1 = Node::new("node1", "127.0.0.1", 2048, 10);
        let res = nodes.insert(node1.clone());
        assert!(res.is_none(), "Node should be inserted");
        let res = nodes.insert(Node::new("node2", "127.0.0.1", 2049, 10));
        assert!(res.is_none(), "Node should be inserted");
        let res = nodes.insert(Node::new("node3", "127.0.0.1", 2050, 10));
        assert!(res.is_none(), "Node should be inserted");

        assert_eq!(nodes.len(), 3, "There should be 3 nodes");
        assert!(nodes.keys().contains(&"node1".to_string()));
        assert!(nodes.keys().contains(&"node2".to_string()));
        assert!(nodes.keys().contains(&"node3".to_string()));

        // Update existing node
        let node1a = Node::new("node1", "127.0.0.2", 2048, 10);
        let res = nodes.insert(node1a.clone());
        assert_eq!(res.unwrap(), node1);

        // Check if the node exists
        assert!(nodes.contains(&node1a.id()));
    }
}
