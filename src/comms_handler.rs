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
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{error, info_span, trace, Span};
use tracing_futures::Instrument;

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
    /// This node's listener address.
    listener_address: SocketAddr,
    /// List of all connected peers.
    peers: HashMap<SocketAddr, Peer>,
    /// The max number of peers this node should handle.
    peer_limit: usize,
    /// Tracing context.
    span: Span,
}

struct Peer {
    /// Channel for sending frames to the peer.
    send_tx: mpsc::Sender<io::Result<Bytes>>,
    /// Tracing context.
    span: Span,
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
        let span = info_span!("node", ?address);
        Self {
            listener_address: address,
            peers: HashMap::with_capacity(peer_limit),
            peer_limit,
            span,
        }
    }

    /// Starts the listener.
    pub async fn listen(&mut self) -> Result<()> {
        let mut listener = TcpListener::bind(self.listener_address).await?;
        self.span.in_scope(|| trace!("listen"));

        while let Some(new_conn) = listener.next().await {
            match new_conn {
                Ok(conn) => {
                    // TODO: have a timeout for incoming handshake to disconnect clients who are linger on without any communication
                    let peer_span = info_span!(parent: &self.span, "accepted peer",
                                   peer_addr = tracing::field::debug(conn.peer_addr()));

                    self.add_peer(conn, false, peer_span)?;
                }
                Err(error) => {
                    error!(?error, "Connection failure");
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
    /// * `peer_span` - Tracing scope for this peer
    fn add_peer(&mut self, socket: TcpStream, force_add: bool, peer_span: Span) -> Result<()> {
        let is_full = self.peers.len() >= self.peer_limit;

        if force_add && is_full {
            // TODO: make sure it's disconnected and shut down gracefully
            let _ = self.peers.drain().take(1);
        }

        if !is_full {
            let peer_addr = socket.peer_addr()?;

            // Spawn the tasks to manage the peer
            let send_tx = handle_peer(socket, peer_span.clone());

            peer_span.in_scope(|| trace!("new peer"));

            self.peers.insert(
                peer_addr,
                Peer {
                    send_tx,
                    span: peer_span,
                },
            );

            Ok(())
        } else {
            Err(CommsError::PeerListFull)
        }
    }

    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        let stream = TcpStream::connect(peer).await?;
        let peer_addr = stream.peer_addr()?;
        self.add_peer(
            stream,
            false,
            info_span!(parent: &self.span, "connect_to", ?peer_addr),
        )?;
        Ok(())
    }

    fn send_bytes(&self, peer: SocketAddr, bytes: Bytes) -> Result<()> {
        let peer = self.peers.get(&peer).ok_or(CommsError::PeerNotFound)?;
        let mut tx = peer.send_tx.clone();

        tokio::spawn(
            async move {
                trace!(?bytes, "send_bytes");
                if let Err(error) = tx.send(Ok(bytes)).await {
                    error!(?error, "Error sending a frame through the message channel",);
                }
            }
            .instrument(peer.span.clone()),
        );

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

/// Manages the data exchange with the peer.
/// Accepts incoming data and decodes it to frames.
///
/// ### Arguments
///
/// * `socket` - The peer's TCP socket.
/// * `span`   - The logging scope for this peer.
fn handle_peer(
    socket: TcpStream,
    span: Span,
) -> mpsc::Sender<std::result::Result<Bytes, io::Error>> {
    let (send_tx, mut send_rx) = mpsc::channel(128);

    // Wrap the peer socket into the tokio codec which handles length-delimited frames.
    let (sock_in, sock_out) = tokio::io::split(socket);
    let mut sock_in = FramedRead::new(sock_in, LengthDelimitedCodec::new());
    let mut sock_out = FramedWrite::new(sock_out, LengthDelimitedCodec::new());

    // Spawn the sender task.
    // Redirect messages from the mpsc channel into the TCP socket
    tokio::spawn(
        async move {
            if let Err(error) = sock_out.send_all(&mut send_rx).await {
                error!(?error, "Error while redirecting messages");
            }
        }
        .instrument(span.clone()),
    );

    // Spawn the receiver task which will redirect the incoming messages into the MPSC channel.
    tokio::spawn(
        async move {
            while let Some(frame) = sock_in.next().await {
                trace!(?frame, "recv_frame");
            }
        }
        .instrument(span),
    );

    send_tx
}
