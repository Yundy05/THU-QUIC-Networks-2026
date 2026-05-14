use std::io;

use thiserror::Error;

mod recv;
pub use recv::*;

mod send;
pub use send::*;

mod state;
pub use state::*;

/// Error indicating that a stream has not been opened or has already been
/// finished or reset
#[derive(Debug, Default, Error, Clone, PartialEq, Eq)]
#[error("closed stream")]
pub struct ClosedStream;

impl From<ClosedStream> for io::Error {
    fn from(x: ClosedStream) -> Self {
        Self::new(io::ErrorKind::NotConnected, x)
    }
}
