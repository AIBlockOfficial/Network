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
#[tokio::test(basic_scheduler)]
async fn direct_messages() {
    let _ = tracing_subscriber::fmt::try_init();

    let mut nodes = create_compute_nodes(2, 2).await;
    let (n1, tail) = nodes.split_first_mut().unwrap();
    let (n2, _) = tail.split_first_mut().unwrap();

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

    complete_compute_nodes(nodes).await;
}

/// In this test, we
/// 1. Start N nodes (e.g., N = 64).
/// 2. node_1 will send a multicast message to F (= Fanout, e.g. 8) nodes.
/// 3. Everyone connects to node_1, and the contact list should be propagated to all other nodes.
/// 4. We check that all other nodes (node_2, node_3, ... node_N) have received the same message.
#[tokio::test(basic_scheduler)]
async fn multicast() {
    let _ = tracing_subscriber::fmt::try_init();

    // Initialize nodes.
    let mut nodes = create_compute_nodes(16, 16).await;
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

    complete_compute_nodes(nodes).await;
}

/// Check that 2 nodes connected node are disconnected once one of them disconnect.
#[tokio::test(basic_scheduler)]
async fn disconnect_connection_all() {
    disconnect_connection(false).await;
}

/// Check that 2 nodes connected node are disconnected once one of them disconnect.
#[tokio::test(basic_scheduler)]
async fn disconnect_connection_some() {
    disconnect_connection(true).await;
}

async fn disconnect_connection(subset: bool) {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let mut nodes = create_compute_nodes(3, 2).await;
    let (n1, tail) = nodes.split_first_mut().unwrap();
    let (n2, tail) = tail.split_first_mut().unwrap();
    let (n3, _) = tail.split_first_mut().unwrap();
    n2.connect_to(n1.address()).await.unwrap();
    n3.connect_to(n1.address()).await.unwrap();

    //
    // Act
    //
    let (actual2, actual3) = {
        let mut joins = Vec::new();
        let subset = if subset {
            Some(vec![n3.address()])
        } else {
            joins.append(&mut n2.take_join_handle(n1.address()).await);
            None
        };
        joins.append(&mut n3.take_join_handle(n1.address()).await);
        joins.append(&mut n1.disconnect_all(subset.as_deref()).await);
        join_all(joins.into_iter()).await;

        let actual2_1 = n2.send(n1.address(), "Hello1_2").await;
        let actual1_2 = n1.send(n2.address(), "Hello2_1").await;
        let actual3_1 = n3.send(n1.address(), "Hello1_3").await;
        let actual1_3 = n1.send(n3.address(), "Hello3_1").await;

        ((actual2_1, actual1_2), (actual3_1, actual1_3))
    };

    //
    // Assert
    //
    let success2 = if subset {
        matches!(actual2, (Ok(_), Ok(_)),)
    } else {
        matches!(
            actual2,
            (Err(CommsError::PeerNotFound), Err(CommsError::PeerNotFound))
        )
    };
    let sucess3 = matches!(
        actual3,
        (Err(CommsError::PeerNotFound), Err(CommsError::PeerNotFound)),
    );
    assert!(success2 && sucess3, "{:?}", (actual2, actual3));

    complete_compute_nodes(nodes).await;
}

/// Check a node cannot connect to a node that stopped listening.
#[tokio::test(basic_scheduler)]
async fn listen_paused_resumed_stopped() {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let mut nodes = create_compute_nodes(3, 3).await;
    let (n1, tail) = nodes.split_first_mut().unwrap();
    let (n2, tail) = tail.split_first_mut().unwrap();
    let (n3, _) = tail.split_first_mut().unwrap();

    //
    // Act
    //
    n1.set_pause_listening(true).await;
    let actual_paused_to = n2.connect_to(n1.address()).await;
    let actual_paused_from = n1.connect_to(n2.address()).await;
    n1.set_pause_listening(false).await;
    let actual_resumed = n2.connect_to(n1.address()).await;

    join_all(n1.stop_listening().await).await;
    let actual_stopped = n3.connect_to(n1.address()).await;

    //
    // Assert
    //
    let actual = (
        actual_paused_to,
        actual_paused_from,
        actual_resumed,
        actual_stopped,
    );
    assert!(
        matches!(
            actual,
            (
                Err(CommsError::PeerNotFound),
                Err(CommsError::PeerNotFound),
                Ok(_),
                Err(CommsError::Io(_))
            )
        ),
        "{:?}",
        actual
    );

    complete_compute_nodes(nodes).await;
}

#[tokio::test(basic_scheduler)]
async fn connect_full_from() {
    connect_full(true).await;
}

#[tokio::test(basic_scheduler)]
async fn connect_full_to() {
    connect_full(false).await;
}

/// Check behaviour when peer list is full.
async fn connect_full(from_full: bool) {
    let _ = tracing_subscriber::fmt::try_init();

    //
    // Arrange
    //
    let mut nodes = create_compute_nodes(3, 1).await;
    let (n1, tail) = nodes.split_first_mut().unwrap();
    let (n2, tail) = tail.split_first_mut().unwrap();
    let (n3, _) = tail.split_first_mut().unwrap();

    //
    // Act
    //
    let conn_error = if from_full {
        n1.connect_to(n2.address()).await.unwrap();
        let err = n1.connect_to(n3.address()).await;
        let is_expected = matches!(err, Err(CommsError::PeerListFull));
        (err, is_expected)
    } else {
        n2.connect_to(n1.address()).await.unwrap();
        let err = n3.connect_to(n1.address()).await;
        let is_expected = matches!(err, Err(CommsError::PeerNotFound));
        (err, is_expected)
    };

    let actual3_1 = n3.send(n1.address(), "Hello3_1").await;
    let actual1_3 = n1.send(n3.address(), "Hello1_3").await;

    let actual2_1 = n2.send(n1.address(), "Hello2_1").await;
    let actual1_2 = n1.send(n2.address(), "Hello1_2").await;

    //
    // Assert
    //
    let actual = (conn_error, actual3_1, actual1_3, actual2_1, actual1_2);
    assert!(
        matches!(
            actual,
            (
                (Err(_), true),
                Err(CommsError::PeerNotFound),
                Err(CommsError::PeerNotFound),
                Ok(()),
                Ok(()),
            ),
        ),
        "{:?}",
        actual
    );

    complete_compute_nodes(nodes).await;
}

async fn create_compute_nodes(num_nodes: usize, peer_limit: usize) -> Vec<Node> {
    let mut nodes = Vec::new();
    for _ in 0..num_nodes {
        let addr = "127.0.0.1:0".parse().unwrap();
        nodes.push(
            Node::new(addr, peer_limit, NodeType::Compute)
                .await
                .unwrap(),
        );
    }
    nodes
}

async fn complete_compute_nodes(nodes: Vec<Node>) {
    for mut node in nodes.into_iter() {
        join_all(node.stop_listening().await).await;
    }
}
