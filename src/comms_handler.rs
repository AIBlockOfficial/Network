//! This module provides basic networking interfaces.

use bincode::deserialize;
use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use std::net::SocketAddr;

use crate::interfaces::Response;

#[derive(Debug, Clone)]
pub struct CommsHandler;

#[derive(Debug, Clone)]
/// An abstract communication interface in the network.
pub struct Node {
    peers: Vec<Node>,
    peer_limit: usize,
}

impl Node {
    /// Creates a new node.
    /// `address` is the socket address the node listener will use.
    pub fn new(address: SocketAddr, peer_limit: usize) -> Self {
        Self {
            peers: Vec::with_capacity(peer_limit),
            peer_limit,
        }
    }

    /// Adds a new peer to the compute node's list of peers.
    /// TODO: Could make peer_list a LRU cache later.
    ///
    /// ### Arguments
    ///
    /// * `address`     - Address of the new peer
    /// * `force_add`   - If true and the peer limit is reached, an old peer will be ejected to make space
    pub fn add_peer(&mut self, address: SocketAddr, force_add: bool) -> Response {
        let is_full = self.peers.len() >= self.peer_limit;

        if force_add && is_full {
            self.peers.truncate(self.peer_limit);
        }

        if !is_full {
            self.peers.push(Node::new(address, self.peer_limit));
            return Response {
                success: true,
                reason: "Peer added successfully",
            };
        }

        Response {
            success: false,
            reason: "Peer list is full. Unable to add new peer",
        }
    }

    /// Sends data to a peer.
    pub fn send(&mut self, peer: SocketAddr, data: impl Serialize) {}

    /// Provides a stream of requests.
    pub fn requests<ReqType: DeserializeOwned + Clone>(&mut self) -> impl Stream<Item = ReqType> {
        futures::stream::repeat(deserialize(&[1]).unwrap())
    }
}
