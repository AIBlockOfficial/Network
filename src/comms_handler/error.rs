use super::Event;
use std::{error::Error, fmt, io};
use tokio::sync::mpsc;

#[derive(Debug)]
pub enum CommsError {
    /// Input/output-related communication error.
    Io(io::Error),
    /// The peers list is empty.
    PeerListEmpty,
    /// The peers list is at the limit.
    PeerListFull,
    /// No such peer found.
    PeerNotFound,
    /// Peer is in invalid state.
    PeerInvalidState,
    /// Serialization-related error.
    Serialization(bincode::Error),
    /// MPSC channel error.
    ChannelSendError(mpsc::error::SendError<Event>),
}

impl fmt::Display for CommsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommsError::Io(err) => write!(f, "I/O error: {}", err),
            CommsError::PeerListFull => write!(f, "Peer list is full"),
            CommsError::PeerListEmpty => write!(f, "Peer list is empty"),
            CommsError::PeerNotFound => write!(f, "Peer not found"),
            CommsError::PeerInvalidState => write!(f, "Peer has invalid state"),
            CommsError::Serialization(err) => write!(f, "Serialization error: {}", err),
            CommsError::ChannelSendError(err) => write!(f, "MPSC channel send error: {}", err),
        }
    }
}

impl Error for CommsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CommsError::Io(err) => Some(err),
            CommsError::PeerListFull => None,
            CommsError::PeerListEmpty => None,
            CommsError::PeerNotFound => None,
            CommsError::PeerInvalidState => None,
            CommsError::Serialization(err) => Some(err),
            CommsError::ChannelSendError(err) => Some(err),
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

impl From<mpsc::error::SendError<Event>> for CommsError {
    fn from(other: mpsc::error::SendError<Event>) -> Self {
        Self::ChannelSendError(other)
    }
}
