//! This module provides basic networking interfaces.

mod error;
mod node;
mod stream_cancel;
pub mod tcp_tls;
#[cfg(test)]
mod tests;

pub use error::CommsError;
pub use node::Node;

use bytes::Bytes;
use std::net::SocketAddr;

pub type Result<T> = std::result::Result<T, CommsError>;

/// Events from peer.
#[derive(Debug)]
pub enum Event {
    NewFrame { peer: SocketAddr, frame: Bytes },
}
