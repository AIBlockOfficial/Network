//! This module provides basic networking interfaces.

use crate::interfaces::HandshakeRequest;
use bincode::{deserialize, serialize};
use bytes::Bytes;
use futures::SinkExt;
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{error::Error, fmt, io};
use tokio::{
    self,
    net::{TcpListener, TcpStream},
    stream::StreamExt,
    sync::{mpsc, RwLock},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{error, info_span, trace, warn, Span};
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

/// Contains a shared list of connected peers.
type PeerList = Arc<RwLock<HashMap<SocketAddr, Peer>>>;

/// Events from peer.
#[derive(Debug)]
pub enum Event {
    NewFrame { peer: SocketAddr, frame: Bytes },
}

/// An abstract communication interface in the network.
#[derive(Debug)]
pub struct Node {
    /// This node's listener address.
    listener_address: SocketAddr,
    /// List of all connected peers.
    peers: PeerList,
    /// The max number of peers this node should handle.
    peer_limit: usize,
    /// Tracing context.
    span: Span,
    /// Channel to transmit incoming frames and events from peers.
    event_tx: mpsc::UnboundedSender<Event>,
    /// Incoming events from peers.
    event_rx: mpsc::UnboundedReceiver<Event>,
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
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        Self {
            listener_address: address,
            peers: Arc::new(RwLock::new(HashMap::with_capacity(peer_limit))),
            peer_limit,
            span,
            event_tx,
            event_rx,
        }
    }

    /// Starts the listener.
    pub async fn listen(&mut self) -> Result<()> {
        let mut listener = TcpListener::bind(self.listener_address).await?;

        let peers = self.peers.clone();
        let event_tx = self.event_tx.clone();
        let peer_limit = self.peer_limit;

        tokio::spawn(
            async move {
                trace!("listen");

                while let Some(new_conn) = listener.next().await {
                    match new_conn {
                        Ok(conn) => {
                            // TODO: have a timeout for incoming handshake to disconnect clients who are linger on without any communication
                            let peer_span = info_span!(
                                "accepted peer",
                                peer_addr = tracing::field::debug(conn.peer_addr())
                            );

                            let new_peer = add_peer(
                                event_tx.clone(),
                                peers.clone(),
                                peer_limit,
                                conn,
                                false,
                                peer_span,
                            )
                            .await;

                            match new_peer {
                                Ok(()) => {}
                                Err(error) => warn!(?error, "Could not add a new peer"),
                            }
                        }
                        Err(error) => {
                            warn!(?error, "Connection failure");
                        }
                    }
                }
            }
            .instrument(self.span.clone()),
        );

        Ok(())
    }

    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        let stream = TcpStream::connect(peer).await?;
        let peer_addr = stream.peer_addr()?;
        add_peer(
            self.event_tx.clone(),
            self.peers.clone(),
            self.peer_limit,
            stream,
            false,
            info_span!(parent: &self.span, "connect_to", ?peer_addr),
        )
        .await?;
        Ok(())
    }

    async fn send_bytes(&self, peer: SocketAddr, bytes: Bytes) -> Result<()> {
        let peers = self.peers.read().await;
        let peer = peers.get(&peer).ok_or(CommsError::PeerNotFound)?;

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
    pub async fn send(&mut self, peer: SocketAddr, data: impl Serialize) -> Result<()> {
        let data = Bytes::from(serialize(&data)?);
        self.send_bytes(peer, data).await
    }

    /// Blocks & waits for a next event from a peer.
    pub async fn next_event(&mut self) -> Option<Event> {
        self.event_rx.recv().await
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
    peer_addr: SocketAddr,
    event_tx: mpsc::UnboundedSender<Event>,
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

    // Spawn the receiver task which will redirect the incoming messages into the MPSC channel
    // and manage the peer state transitions.
    enum PeerState {
        WaitingForHandshake,
        Connected,
    }

    tokio::spawn(
        async move {
            let mut peer_state = PeerState::WaitingForHandshake;

            while let Some(frame) = sock_in.next().await {
                match peer_state {
                    PeerState::Connected => {
                        trace!(?frame, "recv_frame");
                        if let Err(error) = event_tx.send(Event::NewFrame {
                            peer: peer_addr,
                            frame: frame.unwrap().freeze(), // TODO: handle possible errors
                        }) {
                            warn!(?error, ?peer_addr, "event_tx.send");
                        }
                    }
                    PeerState::WaitingForHandshake => {
                        // Try to decode the handshake message.
                        let handshake = deserialize::<HandshakeRequest>(&frame.unwrap());
                        trace!(?handshake);

                        peer_state = PeerState::Connected;
                    }
                }
            }
        }
        .instrument(span),
    );

    send_tx
}

/// Adds a new peer to a list of peers.
/// TODO: Could make peer_list a LRU cache later.
///
/// ### Arguments
///
/// * `event_tx`   - Channel to transmit events from the peer.
/// * `peers_list` - Shared list of a node peers.
/// * `socket`     - A new peer's TcpStream socket.
/// * `force_add`  - If true and the peer limit is reached, an old peer will be ejected to make space.
/// * `peer_span`  - Tracing scope for this peer.
async fn add_peer(
    event_tx: mpsc::UnboundedSender<Event>,
    peers_list: PeerList,
    peer_limit: usize,
    socket: TcpStream,
    force_add: bool,
    peer_span: Span,
) -> Result<()> {
    let mut peers = peers_list.write().await;
    let is_full = peers.len() >= peer_limit;

    if force_add && is_full {
        // TODO: make sure it's disconnected and shut down gracefully
        let _ = peers.drain().take(1);
    }

    if !is_full {
        let peer_addr = socket.peer_addr()?;

        // Spawn the tasks to manage the peer
        let send_tx = handle_peer(socket, peer_addr, event_tx, peer_span.clone());

        peer_span.in_scope(|| trace!("new peer"));

        peers.insert(
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
