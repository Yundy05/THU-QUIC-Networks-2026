use std::{
    fmt,
    ops::{Deref, DerefMut},
};

use bytes::{Buf, BufMut, Bytes, TryGetError};
use strum::{Display, EnumIs, VariantArray};

/// Whether an endpoint was the initiator of a connection
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Display, EnumIs)]
#[strum(serialize_all = "snake_case")]
pub enum Side {
    /// The initiator of a connection
    Client = 0,
    /// The acceptor of a connection
    Server = 1,
}

impl std::ops::Not for Side {
    type Output = Self;

    fn not(self) -> Self {
        match self {
            Self::Client => Self::Server,
            Self::Server => Self::Client,
        }
    }
}

/// Whether a stream communicates data in both directions or only from the
/// initiator
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Display, VariantArray)]
pub enum Dir {
    /// Data flows in both directions
    #[strum(to_string = "bidirectional")]
    Bi = 0,
    /// Data flows only from the stream's initiator
    #[strum(to_string = "unidirectional")]
    Uni = 1,
}

/// Identifier for a stream within a particular connection
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct StreamId(u64);

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} stream {}",
            self.initiator(),
            self.dir(),
            self.index()
        )
    }
}

impl StreamId {
    /// Create a new StreamId
    pub fn new(initiator: Side, dir: Dir, index: u64) -> Self {
        Self((index << 2) | ((dir as u64) << 1) | initiator as u64)
    }

    /// Which side of a connection initiated the stream
    pub fn initiator(self) -> Side {
        if self.0 & 0x1 == 0 {
            Side::Client
        } else {
            Side::Server
        }
    }

    /// Which directions data flows in
    pub fn dir(self) -> Dir {
        if self.0 & 0x2 == 0 {
            Dir::Bi
        } else {
            Dir::Uni
        }
    }

    /// Distinguishes streams of the same initiator and directionality
    pub fn index(self) -> u64 {
        self.0 >> 2
    }
}

impl From<u64> for StreamId {
    fn from(x: u64) -> Self {
        Self(x)
    }
}

impl Deref for StreamId {
    type Target = u64;

    fn deref(&self) -> &u64 {
        &self.0
    }
}

/// Maximum size of a connection ID
pub const MAX_CID_SIZE: usize = 20;

/// Protocol-level identifier for a connection.
///
/// Mainly useful for identifying this connection's packets on the wire with
/// tools like Wireshark.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ConnectionId {
    /// length of CID
    len: u8,
    /// CID in byte array
    bytes: [u8; MAX_CID_SIZE],
}

impl ConnectionId {
    /// Construct cid from byte array
    pub fn new(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() <= MAX_CID_SIZE);
        let mut res = Self {
            len: bytes.len() as u8,
            bytes: [0; MAX_CID_SIZE],
        };
        res.bytes[..bytes.len()].copy_from_slice(bytes);
        res
    }

    /// Constructs cid by reading `len` bytes from a `Buf`.
    pub fn from_buf<B: Buf>(buf: &mut B, len: usize) -> Result<Self, TryGetError> {
        debug_assert!(len <= MAX_CID_SIZE);
        let mut res = Self {
            len: len as u8,
            bytes: [0; MAX_CID_SIZE],
        };
        buf.try_copy_to_slice(&mut res[..len])?;
        Ok(res)
    }

    /// Decode from long header format
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, TryGetError> {
        let len = buf.try_get_u8()? as usize;
        Self::from_buf(buf, len)
    }

    /// Encode in long header format
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(self.len);
        buf.put_slice(self);
    }
}

impl fmt::Debug for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Bytes::from_static(unsafe {
            std::mem::transmute::<&[u8], &[u8]>(&self.bytes[0..self.len as usize])
        })
        .fmt(f)
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.iter() {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Deref for ConnectionId {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes[0..self.len as usize]
    }
}

impl DerefMut for ConnectionId {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[0..self.len as usize]
    }
}

/// Explicit congestion notification codepoint
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, EnumIs)]
pub enum EcnCodepoint {
    /// The ECT(0) codepoint, indicating that an endpoint is ECN-capable
    Ect0 = 0b10,
    /// The ECT(1) codepoint, indicating that an endpoint is ECN-capable
    Ect1 = 0b01,
    /// The CE codepoint, signalling that congestion was experienced
    Ce = 0b11,
}

impl EcnCodepoint {
    /// Create new object from the given bits
    pub fn from_bits(x: u8) -> Option<Self> {
        use EcnCodepoint::*;
        Some(match x & 0b11 {
            0b10 => Ect0,
            0b01 => Ect1,
            0b11 => Ce,
            _ => {
                return None;
            }
        })
    }
}

pub const RESET_TOKEN_SIZE: usize = 16;

/// Stateless reset token
///
/// Used for an endpoint to securely communicate that it has lost state for a
/// connection.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub struct ResetToken([u8; RESET_TOKEN_SIZE]);

impl ResetToken {
    // TODO: create a reset token
}

impl From<[u8; RESET_TOKEN_SIZE]> for ResetToken {
    fn from(x: [u8; RESET_TOKEN_SIZE]) -> Self {
        Self(x)
    }
}

impl Deref for ResetToken {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for ResetToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.iter() {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct IssuedCid {
    pub sequence: u64,
    pub id: ConnectionId,
    pub reset_token: ResetToken,
}
