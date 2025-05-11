use super::*;

fn check_node<T: Node>(node: &T, id: T::NodeId, capacity: usize) {
    assert_eq!(node.id(), &id);
    assert_eq!(node.capacity(), capacity);
}

#[derive(Hash)]
struct TestNode {
    id: String,
    capacity: usize,
}

impl Node for TestNode {
    type NodeId = String;

    fn id(&self) -> &Self::NodeId {
        &self.id
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
}

#[test]
fn nodes() {
    let mut nodes = Nodes::new();

    (0..5).for_each(|i| {
        let node = TestNode {
            id: format!("node{}", i),
            capacity: 10,
        };
        let idx = nodes.insert(node).unwrap();
        check_node(&nodes[idx], format!("node{}", i), 10);
    });

    // Check that the nodes are in the collection
    for (idx, node) in nodes.iter() {
        nodes.idx(&node.id).unwrap();
        check_node(&nodes[idx], node.id.clone(), node.capacity);
    }

    // Reuse indices.
    let remove_idx = 3;
    let removed_node = nodes.remove(remove_idx).unwrap();
    assert_eq!(removed_node.id(), "node3");
    let new_node = TestNode {
        id: "node6".to_string(),
        capacity: 40,
    };
    let new_idx = nodes.insert(new_node).unwrap();
    assert_eq!(new_idx, remove_idx);
    assert_eq!(nodes[new_idx].id(), "node6");
}
