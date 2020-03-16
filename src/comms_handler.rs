//! This module provides basic networking interfaces.

use bincode::deserialize;
use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use std::net::SocketAddr;
use std::{error::Error, fmt, io};
use tokio::{
    self,
    net::{TcpListener, TcpStream},
    stream::StreamExt,
};

use crate::interfaces::Response;

pub type Result<T> = std::result::Result<T, CommsError>;

#[derive(Debug)]
pub enum CommsError {
    Io(io::Error),
}

impl fmt::Display for CommsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommsError::Io(err) => write!(f, "I/O error: {}", err),
        }
    }
}

impl Error for CommsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CommsError::Io(err) => Some(err),
        }
    }
}

impl From<io::Error> for CommsError {
    fn from(other: io::Error) -> Self {
        Self::Io(other)
    }
}

#[derive(Debug, Clone)]
pub struct CommsHandler;

/// An abstract communication interface in the network.
#[derive(Debug, Clone)]
pub struct Node {
    listener_address: SocketAddr,
    peers: Vec<Node>,
    incoming_peers: Vec<SocketAddr>,
    peer_limit: usize,
}

impl Node {
    /// Creates a new node.
    /// `address` is the socket address the node listener will use.
    pub fn new(address: SocketAddr, peer_limit: usize) -> Self {
        Self {
            listener_address: address,
            peers: Vec::with_capacity(peer_limit),
            incoming_peers: Vec::new(),
            peer_limit,
        }
    }

    /// Starts the listener.
    pub async fn listen(&mut self) -> Result<()> {
        let mut listener = TcpListener::bind(self.listener_address).await?;

        while let Some(new_conn) = listener.next().await {
            match new_conn {
                Ok(conn) => {
                    // TODO: have a timeout for incoming handshake to disconnect clients who are linger on without any communication
                    self.incoming_peers.push(conn.peer_addr().unwrap());
                }
                Err(e) => {
                    // TODO: proper logging
                    eprintln!("Connection failure: {:?}, {}", e, e);
                }
            }
        }

        Ok(())
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
