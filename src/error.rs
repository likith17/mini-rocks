use std::fmt;
use std::io;

/// Errors returned by MiniRocks.
#[derive(Debug)]
pub enum Error {
    /// An I/O error from the operating system.
    Io(io::Error),
    /// On-disk data failed validation in a way that cannot be explained by a
    /// torn write at the end of the log. MiniRocks refuses to guess.
    Corruption(String),
    /// The caller passed an invalid argument (e.g. an oversized key).
    InvalidArgument(String),
    /// An earlier WAL write or fsync failed, so the log's on-disk state is
    /// unknown. All further writes are refused; reopen the database, which
    /// re-runs recovery from what actually reached the disk.
    Poisoned(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Corruption(m) => write!(f, "corruption: {m}"),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            Error::Poisoned(m) => write!(f, "database is poisoned: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
