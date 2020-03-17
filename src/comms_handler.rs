//! This module provides basic networking interfaces.

use bincode::{deserialize, serialize};
use bytes::Bytes;
use futures::{SinkExt, Stream};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::{error::Error, fmt, io};
use tokio::{
    self,
    net::{TcpListener, TcpStream},
    stream::StreamExt,
    sync::mpsc,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub type Result<T> = std::result::Result<T, CommsError>;

#[derive(Debug)]
pub enum CommsError {
    Io(io::Error),
    PeerListFull,
    PeerNotFound,
    Serialization(bincode::Error),
}

impl fmt::Display for CommsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommsError::Io(err) => write!(f, "I/O error: {}", err),
            CommsError::PeerListFull => write!(f, "Peer list is full"),
            CommsError::PeerNotFound => write!(f, "Peer not found"),
            CommsError::Serialization(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl Error for CommsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CommsError::Io(err) => Some(err),
            CommsError::PeerListFull => None,
            CommsError::PeerNotFound => None,
            CommsError::Serialization(err) => Some(err),
        }
    }
}

impl From<io::Error> for CommsError {
    fn from(other: io::Error) -> Self {
        Self::Io(other)
    }
}

impl From<bincode::Error> for CommsError {
    fn from(other: bincode::Error) -> Self {
        Self::Serialization(other)
    }
}

/// An abstract communication interface in the network.
#[derive(Debug)]
pub struct Node {
    listener_address: SocketAddr,
    peers: HashMap<SocketAddr, Peer>,
    peer_limit: usize,
}

struct Peer {
    send_tx: mpsc::Sender<io::Result<Bytes>>,
}

impl fmt::Debug for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Peer")
    }
}

impl Node {
    /// Creates a new node.
    /// `address` is the socket address the node listener will use.
    pub fn new(address: SocketAddr, peer_limit: usize) -> Self {
        Self {
            listener_address: address,
            peers: HashMap::with_capacity(peer_limit),
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
                    println!("Accepted peer {:?}", conn.peer_addr());
                    // self.add_peer(conn, false);

                    let mut transport = Framed::new(conn, LengthDelimitedCodec::new());
                    tokio::spawn(async move {
                        while let Some(frame) = transport.next().await {
                            println!("Recvd {:?}", frame);
                        }
                    });
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
    /// * `socket`    - Peer's TcpStream socket
    /// * `force_add` - If true and the peer limit is reached, an old peer will be ejected to make space
    fn add_peer(&mut self, socket: TcpStream, force_add: bool) -> Result<()> {
        let is_full = self.peers.len() >= self.peer_limit;

        if force_add && is_full {
            // TODO: make sure it's disconnected and shut down gracefully
            let _ = self.peers.drain().take(1);
        }

        if !is_full {
            // Wrap the peer socket into the tokio codec which handles length-delimited frames.
            let peer_addr = socket.peer_addr()?;
            println!("Connected to {:?}", peer_addr);

            let mut sock = Framed::new(socket, LengthDelimitedCodec::new());
            let (send_tx, mut send_rx) = mpsc::channel(128);

            tokio::spawn(async move {
                // Redirect messages from the mpsc channel into the TCP socket
                if let Err(e) = sock.send_all(&mut send_rx).await {
                    eprintln!("Error while redirecting messages: {:?}", e);
                }
            });

            self.peers.insert(peer_addr, Peer { send_tx });

            Ok(())
        } else {
            Err(CommsError::PeerListFull)
        }
    }

    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        let stream = TcpStream::connect(peer).await?;
        let peer_addr = stream.peer_addr()?;
        self.add_peer(stream, false)?;
        self.send_bytes(peer_addr, Bytes::from("Hello"))?;

        Ok(())
    }

    fn send_bytes(&self, peer: SocketAddr, bytes: Bytes) -> Result<()> {
        let mut tx = self
            .peers
            .get(&peer)
            .ok_or(CommsError::PeerNotFound)?
            .send_tx
            .clone();

        tokio::spawn(async move {
            if let Err(e) = tx.send(Ok(bytes)).await {
                eprintln!(
                    "Error sending a frame through the message channel: {:?}, {}",
                    e, e
                );
            }
        });

        Ok(())
    }

    /// Sends data to a peer.
    pub fn send(&mut self, peer: SocketAddr, data: impl Serialize) -> Result<()> {
        let data = Bytes::from(serialize(&data)?);
        self.send_bytes(peer, data)
    }

    /// Provides a stream of incoming requests.
    pub fn requests<ReqType: DeserializeOwned + Clone>(&mut self) -> impl Stream<Item = ReqType> {
        futures::stream::repeat(deserialize(&[1]).unwrap())
    }
}
