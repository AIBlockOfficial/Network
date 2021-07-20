use super::Event;
use std::{error::Error, fmt, io};
use tokio::sync::mpsc;
use tokio_rustls::rustls::TLSError;
use tokio_rustls::webpki;

#[derive(Debug)]
pub enum CommsError {
    /// Config error when starting up
    ConfigError(&'static str),
    /// Input/output-related communication error.
    Io(io::Error),
    /// TLS library error
    TlsError(TLSError),
    /// The peers list is empty.
    PeerListEmpty,
    /// The peers list is at the limit.
    PeerListFull,
    /// No such peer found.
    PeerNotFound,
    /// No such peer found.in TLS mapping.
    PeerNameNotFound,
    /// Peer is in invalid state.
    PeerInvalidState,
    /// This peer is already connected.
    PeerDuplicate,
    /// This peer is not compatible.
    PeerIncompatible,
    /// Serialization-related error.
    Serialization(bincode::Error),
    /// MPSC channel error.
    ChannelSendError(mpsc::error::SendError<Event>),
}

impl fmt::Display for CommsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(err) => write!(f, "Config error: {}", err),
            Self::Io(err) => write!(f, "I/O error: {}", err),
            Self::TlsError(err) => write!(f, "TLS error: {}", err),
            Self::PeerListFull => write!(f, "Peer list is full"),
            Self::PeerListEmpty => write!(f, "Peer list is empty"),
            Self::PeerNotFound => write!(f, "Peer not found"),
            Self::PeerNameNotFound => write!(f, "Peer name not found"),
            Self::PeerDuplicate => write!(f, "Peer has invalid state"),
            Self::PeerInvalidState => write!(f, "Duplicate peer"),
            Self::PeerIncompatible => write!(f, "Peer incompatible"),
            Self::Serialization(err) => write!(f, "Serialization error: {}", err),
            Self::ChannelSendError(err) => write!(f, "MPSC channel send error: {}", err),
        }
    }
}

impl Error for CommsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigError(_) => None,
            Self::Io(err) => Some(err),
            Self::TlsError(err) => Some(err),
            Self::PeerListFull => None,
            Self::PeerListEmpty => None,
            Self::PeerNotFound => None,
            Self::PeerNameNotFound => None,
            Self::PeerInvalidState => None,
            Self::PeerDuplicate => None,
            Self::PeerIncompatible => None,
            Self::Serialization(err) => Some(err),
            Self::ChannelSendError(err) => Some(err),
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

impl From<TLSError> for CommsError {
    fn from(other: TLSError) -> Self {
        Self::TlsError(other)
    }
}

impl From<webpki::Error> for CommsError {
    fn from(other: webpki::Error) -> Self {
        Self::TlsError(TLSError::WebPKIError(other))
    }
}
