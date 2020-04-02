use std::{error::Error, fmt, io};

#[derive(Debug)]
pub enum CommsError {
    /// Input/output-related communication error.
    Io(io::Error),
    /// The peers list is at the limit.
    PeerListFull,
    /// No such peer found.
    PeerNotFound,
    /// Serialization-related error.
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
