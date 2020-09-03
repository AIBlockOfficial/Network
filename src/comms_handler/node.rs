//! This module implements a Node type with basic network communication capabilities.
//!
//! ## Peer-to-peer system & rings
//!
//! Each node in the system acts both as a server and as a client, effectively forming a peer-to-peer network.
//!
//! Nodes use a custom TCP protocol to communicate which is message-based. The available message types can be found in [`CommMessage`](crate::interfaces::CommMessage).
//! The messages are serialized into a binary format using the `bincode` crate for efficiency (it can be easily replaced with any other [serde][serde]-compatible serialization
//! format such as JSON if needed).
//!
//! Because each node acts as a server, a _listener_ service is started automatically when you crate a new [`Node`](crate::comms_handler::node::Node).
//!
//! ### Joining the network
//!
//! When a node wants to join the network, it should initiate a connection to any known peer (i.e., a known participant of the network).
//! The address of this peer is obtained externally to the system - i.e., it can be provided in the configuration file, as a command line argument, etc.
//! This peer is our first contact: once we connect to it, it will send us its _contact list_, i.e. a list of other peers it knows, and we subsequently
//! connect to all of the other peers from this contact list.
//!
//! Once the TCP connection with the peer is established, a node sends a [`HandshakeRequest`](crate::interfaces::CommMessage::HandshakeRequest),
//! a specially crafted message which contains a [node type](crate::interfaces::NodeType) and the node's public address (which is derived from the
//! listener address). Please note that this address should be publicly available (i.e., we assume that a node is not behind a NAT or a firewall).
//!
//! In response, a node expects a [`HandshakeResponse`](crate::interfaces::CommMessage::HandshakeResponse) message which contains a contact list
//! of all other peers that form a _ring_.
//!
//! ### Rings
//!
//! Peers form _rings_ which are groups determined by the [node type](crate::interfaces::NodeType) - that is, each node type has its own associated ring.
//! We set the node type as a parameter when a [`Node`](crate::comms_handler::Node::new) instance is created.
//!
//! In a ring, each and every peer is connected to all other peers within the ring. This is a deliberate design choice that was dictated by several reasons:
//! first, it's more efficient, as we don't need to maintain complex membership lists and form complex routing patterns to disseminate messages in the network
//! (basically, we rely on the TCP/IP routing), and _all_ messages can be delivered within a single 'hop' (we don't need to relay messages through a third-party node).
//!
//! Second, it's more simple: we don't need to follow a complex membership logic in order to understand how rings work. Third, even on a regular system, we can have
//! a large number of active TCP connections which are limited mostly by a number of available file descriptors on Linux (this limit can be increased or lifted by executing
//! the `ulimit -n unlimited` command), and by available system memory in general (this can also be tweaked by [setting a network buffer size][netbuffersize]).
//!
//! If a node is the first of its type in a ring, it becomes a first contact peer for all new other nodes that want to join the network.
//!
//! ## Message types
//!
//! `CommMessage` has two message types: [`Direct`](crate::interfaces::CommMessage::Direct) (used for one-to-one communication between peers) and
//! [`Gossip`](crate::interfaces::CommMessage::Gossip) (used for multicast). Both have a parameter that's called `payload` which can contain an arbitrary
//! sequence of bytes (`Vec<u8>`). Usually, however, it should be interpreted as a serialized message specific to a given node type. For example, it can
//! encapsulate a [`ComputeMessage`](crate::interfaces::ComputeMessage) if a message is being sent within a compute ring.
//!
//! Usually, a single message type handles both requests and responses (i.e., instead of having separate `ComputeRequest` or `ComputeResponse` enums, we
//! but both types of messages within a single `ComputeMessage` type). This simplifies deserialization and handling of these
//! messages.
//!
//! ## Multicast
//!
//! Multicast messages are delivered to all nodes within a ring.
//!
//! The multicast algorithm implements the [gossip protocol][gossipproto]. The idea behind it is pretty simple: we pick a number of nodes from our contact list at random
//! (this number can be tweaked by the [`FANOUT`](crate::comms_handler::node::FANOUT) constant), and send them a [`Gossip`](crate::interfaces::CommMessage::Gossip) message.
//! Each gossip message includes three fields: the payload (serialized message contents), a number of hops already made (covered further), and a unique ID for the message.
//! Once this message is received by a peer, it's handled like a [direct message](crate::interfaces::CommMessage::Direct), but with one additional step: the gossip algorithm
//! is repeated again by the receiver - _N_ random nodes picked from the contact list and the _same_ message is retransmitted to these nodes with the `ttl` (number of hops) number
//! increased. The 'number of hops' parameter serves an important function: the [`GOSSIP_MAX_TTL`](crate::comms_handler::node::GOSSIP_MAX_TTL) constant regulates a maximum number
//! of hops that a single message can make. A message ID is chosen randomly, and it also serves an important function: each node maintains a list of
//! [_seen messsage IDs_](crate::comms_handler::node::Node::seen_gossip_messages), and if a newly received message is in that list, then it's discarded and not propagated further.
//! This guarantees that a message will eventually reach _all_ nodes within a ring with a high probability - e.g., even if a connection between node A and node B is severed, a
//! node B can send its message to a node C which will retransmit it to a node B.
//!
//! ## Resources
//!
//! This is a list of reference papers used in the implementation design:
//!
//! 1. [Probabilistic reliable dissemination in large-scale systems, A.-M. Kermarrec et al.][reliabledissemination]
//! 1. [Scalable Byzantine Fault-Tolerant Gossip, Matej Pavlovic][scalablegossip]
//! 1. [The peer sampling service: experimental evaluation of unstructured gossip-based implementations, Márk Jelasity et al.][peersampling]
//!
//! [gossipproto]: https://en.wikipedia.org/wiki/Gossip_protocol
//! [reliabledissemination]: https://ieeexplore.ieee.org/document/1189583
//! [scalablegossip]: https://www.semanticscholar.org/paper/Scalable-Byzantine-Fault-Tolerant-Gossip-Pavlovic/d8e19629323dbcf37dc86919babccbd37f35a1f5
//! [peersampling]: https://dl.acm.org/doi/10.5555/1045658.1045666
//! [serde]: https://serde.rs
//! [netbuffersize]: https://stackoverflow.com/a/7865130/168853

use super::{CommsError, Event, Result};
use crate::interfaces::{CommMessage, NodeType, Token};
use bincode::{deserialize, serialize};
use bytes::Bytes;
use futures::future::join_all;
use futures::SinkExt;
use rand::prelude::*;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::{fmt, io};
use tokio::{
    self,
    net::{TcpListener, TcpStream},
    stream::{Stream, StreamExt},
    sync::{mpsc, Mutex, Notify, RwLock},
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{error, info_span, trace, warn, Span};
use tracing_futures::Instrument;

/// Number of peers we select for gossip message retransmittion.
const FANOUT: usize = 8;

/// Max. number of gossip message retransmissions.
const GOSSIP_MAX_TTL: u8 = 4;

/// Contains a shared list of connected peers.
type PeerList = HashMap<SocketAddr, Peer>;

/// An abstract communication interface in the network.
#[derive(Debug, Clone)]
pub struct Node {
    /// This node's listener address.
    listener_address: SocketAddr,
    /// List of all connected peers.
    pub(crate) peers: Arc<RwLock<PeerList>>,
    /// Node type.
    node_type: NodeType,
    /// The max number of peers this node should handle.
    peer_limit: usize,
    /// Tracing context.
    span: Span,
    /// Channel to transmit incoming frames and events from peers.
    event_tx: mpsc::UnboundedSender<Event>,
    /// Incoming events from peers.
    event_rx: Arc<Mutex<mpsc::UnboundedReceiver<Event>>>,
    /// Contains message IDs that have been already seen and retransmitted by this node.
    // TODO: this requires further work - a list of seen gossip messages can grow unbounded,
    // and it requires to be purged periodically in order to conserve resources.
    seen_gossip_messages: Arc<RwLock<HashSet<Token>>>,
}

pub(crate) struct Peer {
    /// Channel for sending frames to the peer.
    send_tx: mpsc::Sender<io::Result<Bytes>>,
    /// Peer remote address.
    addr: SocketAddr,
    /// Peer type.
    peer_type: Option<NodeType>,
    /// Peer's public address for inbound connections.
    /// Notice that this address is different from `addr`: e.g., if a peer connects to us,
    /// `addr` will have its address with the ephemeral port, while `public_address` will contain
    /// the address that this peer is listening on.
    public_address: Option<SocketAddr>,
    /// Notification to trigger a task waiting for a handshake response.
    // TODO: move it to a separate state enum, manage state transitions in a better way
    notify_handshake_response: Option<Arc<Notify>>,
}

impl fmt::Debug for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Peer {{ addr: {:?}, peer_type: {:?} }}",
            self.addr, self.peer_type
        )
    }
}

#[derive(Debug)]
enum PeerState {
    WaitingForHandshake,
    Ready,
}

impl Node {
    /// Creates a new node.
    ///
    /// ### Arguments
    /// * `address`    - socket address the node listener will use.
    /// * `peer_limit` - the maximum number of peers that this node will handle.
    /// * `node_type`  - the node type that will be broadcasted on the network.
    pub async fn new(address: SocketAddr, peer_limit: usize, node_type: NodeType) -> Result<Self> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // TODO: wrap this socket with TLS 1.3 - https://github.com/tokio-rs/tls
        let listener = TcpListener::bind(address).await?;
        let listener_address = listener.local_addr()?;
        let span = info_span!("node", ?listener_address);

        let mut node = Self {
            listener_address,
            node_type,
            peers: Arc::new(RwLock::new(HashMap::with_capacity(peer_limit))),
            peer_limit,
            span,
            event_tx,
            event_rx: Arc::new(Mutex::new(event_rx)),
            seen_gossip_messages: Arc::new(RwLock::new(HashSet::new())),
        };
        node.listen(listener)?;

        Ok(node)
    }

    /// Handles the listener.
    fn listen(&mut self, mut listener: TcpListener) -> Result<()> {
        let node = self.clone();
        tokio::spawn(
            async move {
                trace!("listen");

                while let Some(new_conn) = listener.next().await {
                    match new_conn {
                        Ok(conn) => {
                            // TODO: have a timeout for incoming handshake to disconnect clients who linger on without any communication
                            let peer_span = info_span!(
                                "accepted peer",
                                peer_addr = tracing::field::debug(conn.peer_addr())
                            );

                            let new_peer = node.add_peer(conn, false, peer_span, true).await;
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

    /// Adds a new peer to a list of peers.
    /// TODO: Could make peer_list a LRU cache later.
    ///
    /// ### Arguments
    ///
    /// * `event_tx`     - Channel to transmit events from the peer.
    /// * `peers_list`   - Shared list of a node peers.
    /// * `socket`       - A new peer's TcpStream socket.
    /// * `force_add`    - If true and the peer limit is reached, an old peer will be ejected to make space.
    /// * `peer_span`    - Tracing scope for this peer.
    /// * `is_initiator` - If `true`, this peer has connected to us. If `false`, then _we_ are
    ///                    connecting to this peer.
    async fn add_peer(
        &self,
        socket: TcpStream,
        force_add: bool,
        peer_span: Span,
        is_initiator: bool,
    ) -> Result<()> {
        let mut peers = self.peers.write().await;
        let is_full = peers.len() >= self.peer_limit;

        if force_add && is_full {
            // TODO: make sure it's disconnected and shut down gracefully
            let _ = peers.drain().take(1);
        }

        if !is_full {
            // Spawn the tasks to manage the peer
            let peer_addr = socket.peer_addr().unwrap();
            let peer = self.handle_peer(socket, peer_span.clone(), is_initiator);

            peer_span.in_scope(|| trace!("added new peer: {:?}", peer_addr));

            peers.insert(peer_addr, peer);

            Ok(())
        } else {
            warn!(peers = tracing::field::debug(&*peers), "PeerListFull");
            Err(CommsError::PeerListFull)
        }
    }

    /// Establishes a connection to a remote peer.
    /// Compared to `connect_to`, this function will not send the handshake message and won't wait for a response.
    async fn connect_to_peer(&mut self, peer: SocketAddr) -> Result<()> {
        let stream = TcpStream::connect(peer).await?;
        let peer_addr = stream.peer_addr()?;
        self.add_peer(
            stream,
            false,
            info_span!(parent: &self.span, "connect_to", ?peer_addr),
            false,
        )
        .await?;
        Ok(())
    }

    /// Connects to a remote peer.
    ///
    /// ### Arguments
    /// * `peer` - Endpoint address of a remote peer.
    ///
    /// ### Returns
    /// * `Ok(())` if this node has successfully connected to a peer.
    /// * See [`CommsError`][error] for a list of possible errors.
    ///
    /// [error]: enum.CommsError.html
    pub async fn connect_to(&mut self, peer: SocketAddr) -> Result<()> {
        trace!(?peer, "connection attempt");
        self.connect_to_peer(peer).await?;
        self.send_handshake(peer, self.node_type).await?;

        // Wait for a handshake response
        let handshake_response = {
            let peers = self.peers.read().await;
            let peer = peers.get(&peer).ok_or(CommsError::PeerNotFound)?;
            peer.notify_handshake_response
                .as_ref()
                .ok_or(CommsError::PeerInvalidState)?
                .clone()
        };
        // TODO: make sure we have a timeout here in case if a peer is unresponsive.
        handshake_response.notified().await;

        Ok(())
    }

    /// Sends a multicast message to a group of peers in our ring.
    ///
    /// ### Errors
    /// - `CommsError::PeerListEmpty` - in case if we don't know anyone we can multicast to.
    pub async fn multicast(&mut self, data: impl Serialize) -> Result<()> {
        let peers = self.sample_peers(FANOUT).await;
        if peers.is_empty() {
            return Err(CommsError::PeerListEmpty);
        }

        let message_id = rand::thread_rng().gen();
        let payload = Bytes::from(serialize(&data)?);

        self.send_multicast(
            peers,
            CommMessage::Gossip {
                payload: payload.clone(),
                id: message_id,
                ttl: 0,
            },
        )
        .await;

        Ok(())
    }

    async fn send_multicast(&self, peers: HashSet<SocketAddr>, msg: CommMessage) {
        let mut join_handles = Vec::with_capacity(peers.len());

        trace!(?peers, "send_multicast");

        for peer in peers {
            let mut node = self.clone();
            let msg = msg.clone();

            join_handles.push(tokio::spawn(async move {
                if let Err(error) = node.send_message(peer, msg).await {
                    warn!(?error, "send_multicast");
                }
            }));
        }

        join_all(join_handles).await;
    }

    /// Returns a list of known peers who are members of the same ring as ours.
    fn ring_peers<'a>(&self, peers: &'a PeerList) -> impl Iterator<Item = SocketAddr> + 'a {
        let own_node_type = self.node_type;

        peers.values().filter_map(move |p| {
            if let Some(peer_type) = p.peer_type {
                if peer_type == own_node_type {
                    return p.public_address;
                }
            }
            None
        })
    }

    /// Select `n` random peers from known contacts.
    async fn sample_peers(&self, n: usize) -> HashSet<SocketAddr> {
        let peers = (*self.peers).read().await;

        let rng = &mut rand::thread_rng();

        peers
            .keys()
            .cloned()
            .choose_multiple(rng, n)
            .into_iter()
            .collect()
    }

    /// Sends raw bytes to a given peer.
    async fn send_bytes(
        &self,
        peer: SocketAddr,
        tx: &mut mpsc::Sender<io::Result<Bytes>>,
        bytes: Bytes,
    ) -> Result<()> {
        trace!(?bytes, ?peer, "send_bytes");
        if let Err(error) = tx.send(Ok(bytes)).await {
            error!(?error, "Error sending a frame through the message channel");
        }
        Ok(())
    }

    /// Sends data to a peer.
    pub(crate) async fn send_message(
        &mut self,
        peer_addr: SocketAddr,
        message: CommMessage,
    ) -> Result<()> {
        let data = Bytes::from(serialize(&message)?);

        let peers = self.peers.read().await;
        let peer = peers.get(&peer_addr).ok_or(CommsError::PeerNotFound)?;
        let mut tx = peer.send_tx.clone();
        self.send_bytes(peer_addr, &mut tx, data).await
    }

    /// Sends a serialized message to a given peer.
    pub async fn send(&mut self, peer: SocketAddr, data: impl Serialize) -> Result<()> {
        let payload = Bytes::from(serialize(&data)?);
        let id = rand::thread_rng().gen();
        self.send_message(peer, CommMessage::Direct { payload, id })
            .await
    }

    /// Prepares and sends a handshake message to a given peer.
    async fn send_handshake(&mut self, peer: SocketAddr, node_type: NodeType) -> Result<()> {
        self.send_message(
            peer,
            CommMessage::HandshakeRequest {
                node_type,
                public_address: self.listener_address,
            },
        )
        .await
    }

    /// Blocks & waits for a next event from a peer.
    pub async fn next_event(&mut self) -> Option<Event> {
        self.event_rx.lock().await.recv().await
    }

    /// Returns this node's listener address.
    pub fn address(&self) -> SocketAddr {
        self.listener_address
    }

    /// Handles incoming messages from a peer.
    ///
    /// ### Arguments
    /// * `peer_addr`    - address of a remote peer.
    /// * `send_tx`      - a queue to send messages to the peer.
    /// * `messages`     - stream of incoming messages.
    /// * `is_initiator` - If `true`, this peer has connected to us. If `false`, then _we_ are
    ///                    connecting to this peer.
    async fn handle_peer_recv(
        &self,
        peer_addr: SocketAddr,
        send_tx: mpsc::Sender<std::result::Result<Bytes, io::Error>>,
        mut messages: impl Stream<Item = CommMessage> + std::marker::Unpin,
        is_initiator: bool,
    ) {
        let mut peer_state = if is_initiator {
            PeerState::WaitingForHandshake
        } else {
            PeerState::Ready
        };

        while let Some(message) = messages.next().await {
            trace!(?message);

            match peer_state {
                PeerState::WaitingForHandshake => match message {
                    CommMessage::HandshakeRequest {
                        node_type,
                        public_address,
                    } => {
                        trace!(?peer_addr, ?public_address, ?node_type, "HandshakeRequest");

                        peer_state = PeerState::Ready;

                        match self
                            .handle_handshake_request(
                                peer_addr,
                                public_address,
                                send_tx.clone(),
                                node_type,
                            )
                            .await
                        {
                            Ok(()) => {}
                            Err(CommsError::PeerDuplicate) => {
                                // Drop a duplicate connection.
                                warn!(?public_address, "duplicate peer");
                                return;
                            }
                            Err(error) => warn!(?error, "handle_handshake_request"),
                        }
                    }
                    other => {
                        warn!(
                            ?other,
                            "Received unexpected message while waiting for a handshake; ignoring"
                        );
                    }
                },
                PeerState::Ready => match message {
                    CommMessage::HandshakeResponse { contacts } => {
                        trace!(?contacts, "HandshakeResponse");

                        if let Err(error) =
                            self.handle_handshake_response(peer_addr, contacts).await
                        {
                            warn!(?error, "handle_handshake_response");
                        }
                    }
                    CommMessage::Direct {
                        payload: frame,
                        id: _,
                    } => {
                        if let Err(error) = self.event_tx.send(Event::NewFrame {
                            peer: peer_addr,
                            frame,
                        }) {
                            warn!(?error, ?peer_addr, "event_tx.send");
                        }
                    }
                    CommMessage::Gossip { payload, ttl, id } => {
                        trace!(?payload, ?ttl, ?id, "gossip");
                        if let Err(error) = self.handle_gossip(peer_addr, payload, ttl, id).await {
                            warn!(?error, "handle_gossip");
                        }
                    }
                    other => {
                        warn!(?other, "Received unexpected message; ignoring");
                    }
                },
            }
        }
    }

    async fn handle_gossip(
        &self,
        from_peer: SocketAddr,
        payload: Bytes,
        ttl: u8,
        message_id: Token,
    ) -> Result<()> {
        {
            // Have we already seen a message with this id?
            let mut seen_list = self.seen_gossip_messages.write().await;

            if seen_list.contains(&message_id) {
                // This message was already seen, do nothing.
                trace!(?ttl, ?message_id, "gossip - already seen");
                return Ok(());
            }

            let _ = seen_list.insert(message_id);
        }

        // Notify a user about the message.
        self.event_tx.send(Event::NewFrame {
            peer: from_peer,
            frame: payload.clone(),
        })?;

        // Retransmit the gossip message.
        if ttl > GOSSIP_MAX_TTL {
            // The message has exceeded its TTL.
            return Ok(());
        }

        let mut peers = self.sample_peers(FANOUT).await;
        peers.remove(&from_peer);

        if peers.is_empty() {
            // Our contact list is empty.
            trace!(?message_id, "gossip - contact list is empty");
            return Ok(());
        }

        // Create a new task for retransmission to make sure we don't block the listener.
        let node = self.clone();
        let payload = payload.clone();

        tokio::spawn(async move {
            node.send_multicast(
                peers,
                CommMessage::Gossip {
                    payload,
                    id: message_id,
                    ttl: ttl + 1,
                },
            )
            .await;
        });

        Ok(())
    }

    /// Handles an incoming handshake request.
    /// Sends a handshake response with a list of peers we know within our ring.
    async fn handle_handshake_request(
        &self,
        peer_out_addr: SocketAddr,
        peer_in_addr: SocketAddr,
        mut send_tx: mpsc::Sender<std::result::Result<Bytes, io::Error>>,
        peer_type: NodeType,
    ) -> Result<()> {
        let mut all_peers = self.peers.write().await;

        // Check if we already have a connection to this peer.
        if all_peers.contains_key(&peer_in_addr) {
            return Err(CommsError::PeerDuplicate);
        }

        let peer = all_peers
            .get_mut(&peer_out_addr)
            .ok_or(CommsError::PeerNotFound)?;

        peer.peer_type = Some(peer_type);
        peer.public_address = Some(peer_in_addr);

        // Send handshake response which will contain contacts of all valid peers within our ring.
        let contacts = self.ring_peers(&all_peers).collect();
        let message = Bytes::from(serialize(&CommMessage::HandshakeResponse { contacts })?);
        self.send_bytes(peer_out_addr, &mut send_tx, message)
            .await?;

        Ok(())
    }

    /// Handles a handshake response.
    /// Connects to all nodes that we receive in the contact list.
    async fn handle_handshake_response(
        &self,
        peer_addr: SocketAddr,
        contacts: Vec<SocketAddr>,
    ) -> Result<()> {
        let all_peers = self.peers.write().await;

        // Notify the peer about the handshake response if someone's waiting for it.
        {
            let peer = all_peers.get(&peer_addr).ok_or(CommsError::PeerNotFound)?;
            if let Some(ref notify) = peer.notify_handshake_response {
                notify.notify();
            }
        }

        // Find out if we have received new peer contacts in the response.
        let known_peers: HashSet<_> = all_peers
            .values()
            .filter_map(|p| p.public_address)
            .collect();

        let contacts = contacts.into_iter().collect::<HashSet<_>>();
        let unknown_peers = contacts.difference(&known_peers);

        trace!(?known_peers, ?unknown_peers);

        // Connect to all previously unknown peers (in the background - i.e. we don't wait for these connections to succeed).
        for &peer in unknown_peers {
            let mut node = self.clone();
            tokio::spawn(
                async move {
                    if let Err(error) = node.connect_to(peer).await {
                        warn!(?error, "connect_to failed");
                    }
                }
                .instrument(info_span!(parent: &self.span, "handshake_response_connect", ?peer)),
            );
        }

        Ok(())
    }

    /// Manages the data exchange with the peer.
    /// Accepts incoming data and decodes it to frames.
    ///
    /// ### Arguments
    ///
    /// * `socket`       - The peer's TCP socket.
    /// * `event_tx`     - A channel for user-level messages.
    /// * `span`         - The logging scope for this peer.
    /// * `is_initiator` - If `true`, this peer has connected to us. If `false`, then _we_ are
    ///                    connecting to this peer.
    ///
    /// ### Returns
    /// A new `Peer` instance.
    fn handle_peer(&self, socket: TcpStream, span: Span, is_initiator: bool) -> Peer {
        let peer_addr = socket.peer_addr().unwrap();

        let (send_tx, mut send_rx) = mpsc::channel(128);

        // Wrap the peer socket into the tokio codec which handles length-delimited frames.
        let (sock_in, sock_out) = tokio::io::split(socket);
        let sock_in = FramedRead::new(sock_in, LengthDelimitedCodec::new());
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
        let messages = get_messages_stream(sock_in);
        tokio::spawn({
            let node = self.clone();
            let send_tx = send_tx.clone();
            let peers = self.peers.clone();
            async move {
                node.handle_peer_recv(peer_addr, send_tx, messages, is_initiator)
                    .await;
                // Since we don't wait for any messages from this peer, we can drop the connection.
                let mut peers_list = peers.write().await;
                let _ = peers_list.remove(&peer_addr);
            }
            .instrument(span)
        });

        Peer {
            addr: peer_addr,
            send_tx,
            peer_type: None,
            public_address: if !is_initiator { Some(peer_addr) } else { None },
            notify_handshake_response: if is_initiator {
                None
            } else {
                Some(Arc::new(Notify::new()))
            },
        }
    }
}

/// Transforms a stream of incoming TCP frames into a stream of deserialized messages.
fn get_messages_stream(
    sock_in: FramedRead<tokio::io::ReadHalf<TcpStream>, LengthDelimitedCodec>,
) -> impl Stream<Item = CommMessage> {
    sock_in.filter_map(|frame| {
        trace!(?frame, "recv_frame");

        let frame = match frame {
            Ok(inner) => inner,
            Err(error) => {
                warn!(?error, "Could not decode frame");
                return None;
            }
        };

        match deserialize::<CommMessage>(&frame) {
            Ok(message) => Some(message),
            Err(error) => {
                warn!(?error, "Could not deserialize message; ignoring");
                return None;
            }
        }
    })
}
