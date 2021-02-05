//! Tests for peer-to-peer communication.

use super::{CommsError, Event, Node};
use crate::interfaces::NodeType;
use bincode::deserialize;
use futures::future::join_all;
use std::time::Duration;
use tokio::time;
use tracing::debug;

const TIMEOUT_TEST_WAIT_DURATION: Duration = Duration::from_millis(5000);

/// Check that 2 nodes can exchange arbitrary messages in both direction,
/// using their public address after one node connected to the other.
#[tokio::test]
async fn direct_messages() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut nodes = create_compute_nodes(2).await;
    let (mut n2, mut n1) = (nodes.pop().unwrap(), nodes.pop().unwrap());

    n2.connect_to(n1.address()).await.unwrap();
    n2.send(n1.address(), "Hello1").await.unwrap();
    n1.send(n2.address(), "Hello2").await.unwrap();

    if let Some(Event::NewFrame { peer: _, frame }) = n1.next_event().await {
        let recv_frame: &str = deserialize(&frame).unwrap();
        assert_eq!(recv_frame, "Hello1");
    }
    if let Some(Event::NewFrame { peer: _, frame }) = n2.next_event().await {
        let recv_frame: &str = deserialize(&frame).unwrap();
        assert_eq!(recv_frame, "Hello2");
    }
}

/// In this test, we
/// 1. Start N nodes (e.g., N = 64).
/// 2. node_1 will send a multicast message to F (= Fanout, e.g. 8) nodes.
/// 3. Everyone connects to node_1, and the contact list should be propagated to all other nodes.
/// 4. We check that all other nodes (node_2, node_3, ... node_N) have received the same message.
#[tokio::test]
async fn multicast() {
    let _ = tracing_subscriber::fmt::try_init();

    // Initialize nodes.
    let num_nodes = 16;
    let mut nodes = create_compute_nodes(num_nodes).await;
    nodes
        .iter_mut()
        .for_each(|n| n.set_connect_to_handshake_contacts(true));

    // Connect everyone in a ring.
    let first_node = nodes[0].address();
    let mut conn_handles = Vec::new();

    for (i, node) in nodes.iter().enumerate().skip(1) {
        let mut node = node.clone();

        conn_handles.push(tokio::spawn(async move {
            node.connect_to(first_node).await.unwrap();
            debug!(?i, "connected");
        }));
    }

    join_all(conn_handles).await;

    // Send a multicast message.
    nodes[0].multicast("Hello").await.unwrap();

    // Verify that all other nodes have received the message
    for (i, node) in nodes.iter_mut().enumerate().skip(1) {
        match time::timeout(TIMEOUT_TEST_WAIT_DURATION, node.next_event()).await {
            Ok(Some(Event::NewFrame { peer: _, frame })) => {
                debug!(?i, "received");

                let recv_frame: &str = deserialize(&frame).unwrap();
                assert_eq!(recv_frame, "Hello");
            }
            Ok(None) => {
                panic!("Channel disconnected {}", i);
            }
            Err(_) => {
                panic!("Timeout elapsed {}", i);
            }
        }
    }
}

/// Check that 2 nodes connected node are disconnected once one of them disconnect.
#[tokio::test]
async fn disconnect_connection() {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let mut nodes = create_compute_nodes(2).await;
    let (mut n2, mut n1) = (nodes.pop().unwrap(), nodes.pop().unwrap());
    n2.connect_to(n1.address()).await.unwrap();

    //
    // Act
    //
    n1.disconnect_all().await;
    n2.wait_disconnect_all().await;
    let actual2 = n2.send(n1.address(), "Hello1").await;
    let actual1 = n1.send(n2.address(), "Hello2").await;

    //
    // Assert
    //
    let actual = (actual1, actual2);
    assert!(
        matches!(
            actual,
            (Err(CommsError::PeerNotFound), Err(CommsError::PeerNotFound)),
        ),
        "{:?}",
        actual
    );
}

async fn create_compute_nodes(num_nodes: usize) -> Vec<Node> {
    let mut nodes = Vec::new();
    for _ in 0..num_nodes {
        let addr = "127.0.0.1:0".parse().unwrap();
        nodes.push(Node::new(addr, num_nodes, NodeType::Compute).await.unwrap());
    }
    nodes
}
