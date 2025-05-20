use keyspace::{DefaultReplicationStrategy,  KeyspaceBuilder, KeyspaceError, Node};

#[derive(Hash, Clone)]
struct SimpleNode {
    id: String,
}

impl Node for SimpleNode {
    type NodeId = String;

    fn id(&self) -> &Self::NodeId {
        &self.id
    }
}

impl SimpleNode {
    fn new(id: &str) -> Self {
        Self { id: id.to_string() }
    }
}

#[test]
fn keyspace_builder() {
    let init_nodes = vec![
        SimpleNode::new("node1"),
        SimpleNode::new("node2"),
        SimpleNode::new("node3"),
    ];
    const RF: usize = 4;

    {
        // Default keyspace (DefaultReplicationStrategy + RF=3)
        let mut init_nodes = init_nodes.clone();
        let keyspace = KeyspaceBuilder::new(init_nodes.clone()).build();
        assert!(keyspace.is_ok());

        init_nodes.pop(); // only 2 nodes left
        let keyspace = KeyspaceBuilder::new(init_nodes).build();
        assert_eq!(keyspace.err(), Some(KeyspaceError::NotEnoughNodes(3)));
    }

    {
        // Default replication strategy
        let keyspace = KeyspaceBuilder::new(init_nodes.clone())
            .with_replication_factor::<3>()
            .build();
        assert!(keyspace.is_ok());

        let keyspace = KeyspaceBuilder::new(init_nodes.clone())
            .with_replication_factor::<RF>()
            .build();
        assert_eq!(keyspace.err(), Some(KeyspaceError::NotEnoughNodes(RF)));
    }

    {
        // Default replication factor
        let keyspace = KeyspaceBuilder::new(init_nodes.clone())
            .with_replication_strategy(DefaultReplicationStrategy::new())
            .build();
        assert!(keyspace.is_ok());

        let mut init_nodes = init_nodes.clone();
        init_nodes.pop();
        let keyspace = KeyspaceBuilder::new(init_nodes)
            .with_replication_strategy(DefaultReplicationStrategy::new())
            .build();
        assert_eq!(keyspace.err(), Some(KeyspaceError::NotEnoughNodes(3)));
    }

    {
        // Explicit replication strategy and factor
        let keyspace = KeyspaceBuilder::new(init_nodes.clone())
            .with_replication_factor::<RF>()
            .with_replication_strategy(DefaultReplicationStrategy::new())
            .build();
        assert_eq!(keyspace.err(), Some(KeyspaceError::NotEnoughNodes(RF)));

        // Explicit replication strategy and factor (different order)
        let keyspace = KeyspaceBuilder::new(init_nodes.clone())
            .with_replication_strategy(DefaultReplicationStrategy::new())
            .with_replication_factor::<RF>()
            .build();
        assert_eq!(keyspace.err(), Some(KeyspaceError::NotEnoughNodes(RF)));
    }
}
