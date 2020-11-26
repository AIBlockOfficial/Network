//! Tests for peer-to-peer communication.

use super::{Event, Node};
use crate::interfaces::NodeType;
use bincode::deserialize;
use futures::future::join_all;
use tracing::debug;

/// Check that 2 nodes can exchange arbitrary messages.
#[tokio::test]
async fn direct_messages() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut n1 = Node::new("127.0.0.1:0".parse().unwrap(), 4, NodeType::Compute)
        .await
        .unwrap();

    let mut n2 = Node::new("127.0.0.1:0".parse().unwrap(), 4, NodeType::Compute)
        .await
        .unwrap();

    n2.connect_to(n1.address()).await.unwrap();
    n2.send(n1.address(), "Hello").await.unwrap();

    if let Some(Event::NewFrame { peer: _, frame }) = n1.next_event().await {
        let recv_frame: &str = deserialize(&frame).unwrap();
        assert_eq!(recv_frame, "Hello");
    }
}

/// In this test, we
/// 1. Start N nodes (e.g., N = 64).
/// 2. node_1 will send a multicast message to F (= Fanout, e.g. 8) nodes.
/// 3. Everyone connects to node_1, and the contact list should be propagated to all other nodes.
/// 4. We check that all other nodes (node_2, node_3, ... node_N) have received the same message.
#[tokio::test]
async fn multicast() {
    const NUM_NODES: usize = 16;
    let _ = tracing_subscriber::fmt::try_init();

    // Initialize nodes.
    let mut nodes = Vec::with_capacity(NUM_NODES);

    for _i in 0..NUM_NODES {
        let mut node = Node::new("127.0.0.1:0".parse().unwrap(), NUM_NODES, NodeType::Compute)
            .await
            .unwrap();
        node.set_connect_to_handshake_contacts(true);
        nodes.push(node);
    }

    // Connect everyone in a ring.
    let first_node = nodes[0].address();
    let mut conn_handles = Vec::with_capacity(NUM_NODES);

    for i in 1..NUM_NODES {
        let mut node = nodes[i].clone();

        conn_handles.push(tokio::spawn(async move {
            node.connect_to(first_node).await.unwrap();
            debug!(?i, "connected");
        }));
    }

    join_all(conn_handles).await;

    // Send a multicast message.
    nodes[0].multicast("Hello").await.unwrap();

    // Verify that all other nodes have received the message
    for i in 1..NUM_NODES {
        if let Some(Event::NewFrame { peer: _, frame }) = nodes[i].next_event().await {
            debug!(?i, "received");

            let recv_frame: &str = deserialize(&frame).unwrap();
            assert_eq!(recv_frame, "Hello");
        }
    }
}
