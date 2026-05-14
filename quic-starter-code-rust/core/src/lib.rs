mod connection;
pub use connection::{
    Connection, ConnectionError, ConnectionHandle, ConnectionTask, Event, FinishError,
    IllegalOrderedRead, ReadError, ReadableError, StreamEvent, WriteError,
};

mod endpoint;
pub use endpoint::Endpoint;

const SUPPORTED_VERSIONS: &[u32] = &[1];
const FAKE_CID: &[u8] = b"0123456789abcdefghij";
const INITIAL_MTU: usize = 1472;

pub type TransportHandler = Box<dyn FnMut(&mut Connection, Event) + Send>;
pub type TransportHandlerFactory = fn() -> TransportHandler;
