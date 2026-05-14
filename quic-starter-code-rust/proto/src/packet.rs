use std::ops::{Index, IndexMut};

use bytes::{Buf, BufMut, Bytes, TryGetError};
use strum::{EnumIs, VariantArray};
use thiserror::Error;

use crate::{BufExt, BufMutExt, ConnectionId};

/// Packet decode error
#[derive(Error, Debug, Eq, PartialEq)]
pub enum PacketDecodeError {
    /// Packet uses a QUIC version that is not supported
    #[error("unsupported version {version:x}")]
    UnsupportedVersion {
        /// Source Connection ID
        src_cid: ConnectionId,
        /// Destination Connection ID
        dst_cid: ConnectionId,
        /// The version that was unsupported
        version: u32,
    },
    /// Unexpected end of packet header
    #[error("unexpected end of packet header (requested {} but only {} available)", .0.requested, .0.available)]
    UnexpectedEndOfHeader(#[from] TryGetError),
    /// The reserved bits in the header are set
    #[error("reserved bits set")]
    ReservedBitsSet,
    /// The fixed bit in the header is unset
    #[error("fixed bit unset")]
    FixedBitUnset,
}

pub const LONG_HEADER_FORM: u8 = 0x80;
pub const FIXED_BIT: u8 = 0x40;

/// Long packet types with uniform header structure
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LongType {
    /// Handshake packet
    Handshake,
    /// 0-RTT packet
    ZeroRtt,
}

/// Long packet type including non-uniform cases
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LongHeaderType {
    Initial,
    Retry,
    Standard(LongType),
}

impl From<u8> for LongHeaderType {
    fn from(b: u8) -> Self {
        debug_assert!(b & LONG_HEADER_FORM != 0, "not a long packet");
        use LongHeaderType::*;
        use LongType::*;
        match (b & 0x30) >> 4 {
            0x0 => Initial,
            0x1 => Standard(ZeroRtt),
            0x2 => Standard(Handshake),
            0x3 => Retry,
            _ => unreachable!(),
        }
    }
}

impl From<LongHeaderType> for u8 {
    fn from(ty: LongHeaderType) -> Self {
        use LongHeaderType::*;
        use LongType::*;
        match ty {
            Initial => LONG_HEADER_FORM | FIXED_BIT,
            Standard(ZeroRtt) => LONG_HEADER_FORM | FIXED_BIT | (0x1 << 4),
            Standard(Handshake) => LONG_HEADER_FORM | FIXED_BIT | (0x2 << 4),
            Retry => LONG_HEADER_FORM | FIXED_BIT | (0x3 << 4),
        }
    }
}

/// Packet number space identifiers
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, VariantArray)]
pub enum SpaceId {
    /// Unprotected packets, used to bootstrap the handshake
    Initial   = 0,
    Handshake = 1,
    /// Application data space, used for 0-RTT and post-handshake/1-RTT packets
    Data      = 2,
}

impl<T> Index<SpaceId> for [T; SpaceId::VARIANTS.len()] {
    type Output = T;

    fn index(&self, index: SpaceId) -> &Self::Output {
        &self[index as usize]
    }
}

impl<T> IndexMut<SpaceId> for [T; SpaceId::VARIANTS.len()] {
    fn index_mut(&mut self, index: SpaceId) -> &mut Self::Output {
        &mut self[index as usize]
    }
}

// An encoded packet number
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PacketNumber {
    U8(u8),
    U16(u16),
    U24(u32),
    U32(u32),
}

impl PacketNumber {
    pub fn new(n: u64, largest_acked: u64) -> Self {
        let range = (n - largest_acked) * 2;
        if range < 1 << 8 {
            Self::U8(n as u8)
        } else if range < 1 << 16 {
            Self::U16(n as u16)
        } else if range < 1 << 24 {
            Self::U24(n as u32)
        } else if range < 1 << 32 {
            Self::U32(n as u32)
        } else {
            panic!("packet number too large to encode")
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(self) -> usize {
        use PacketNumber::*;
        match self {
            U8(_) => 1,
            U16(_) => 2,
            U24(_) => 3,
            U32(_) => 4,
        }
    }

    pub fn encode<W: BufMut>(self, w: &mut W) {
        use PacketNumber::*;
        match self {
            U8(x) => w.put_u8(x),
            U16(x) => w.put_u16(x),
            U24(x) => w.put_uint(u64::from(x), 3),
            U32(x) => w.put_u32(x),
        }
    }

    pub fn decode<R: Buf>(len: usize, r: &mut R) -> Result<Self, PacketDecodeError> {
        use PacketNumber::*;
        let pn = match len {
            1 => U8(r.try_get_u8()?),
            2 => U16(r.try_get_u16()?),
            3 => U24(r.try_get_uint(3)? as u32),
            4 => U32(r.try_get_u32()?),
            _ => unreachable!(),
        };
        Ok(pn)
    }

    pub fn decode_len(tag: u8) -> usize {
        1 + (tag & 0x03) as usize
    }

    fn tag(self) -> u8 {
        use PacketNumber::*;
        match self {
            U8(_) => 0b00,
            U16(_) => 0b01,
            U24(_) => 0b10,
            U32(_) => 0b11,
        }
    }

    pub fn expand(self, expected: u64) -> u64 {
        // From Appendix A
        use PacketNumber::*;
        let truncated = match self {
            U8(x) => u64::from(x),
            U16(x) => u64::from(x),
            U24(x) => u64::from(x),
            U32(x) => u64::from(x),
        };
        let nbits = self.len() * 8;
        let win = 1 << nbits;
        let hwin = win / 2;
        let mask = win - 1;
        // The incoming packet number should be greater than expected - hwin and less
        // than or equal to expected + hwin
        //
        // This means we can't just strip the trailing bits from expected and add the
        // truncated because that might yield a value outside the window.
        //
        // The following code calculates a candidate value and makes sure it's within
        // the packet number window.
        let candidate = (expected & !mask) | truncated;
        if expected.checked_sub(hwin).is_some_and(|x| candidate <= x) {
            candidate + win
        } else if candidate > expected + hwin && candidate > win {
            candidate - win
        } else {
            candidate
        }
    }
}

const SPIN_BIT: u8 = 0x20;
const SHORT_RESERVED_BITS: u8 = 0x18;
const LONG_RESERVED_BITS: u8 = 0x0c;
const KEY_PHASE_BIT: u8 = 0x04;

#[derive(Debug, Clone)]
pub struct InitialHeader {
    pub dst_cid: ConnectionId,
    pub src_cid: ConnectionId,
    pub token: Bytes,
    pub number: PacketNumber,
    pub version: u32,
}

#[derive(Debug)]
pub struct InitialPacket {
    pub header: InitialHeader,
    pub payload: Bytes,
}

#[derive(Debug, EnumIs)]
pub enum Header {
    Initial(InitialHeader),
    Long {
        ty: LongType,
        dst_cid: ConnectionId,
        src_cid: ConnectionId,
        number: PacketNumber,
        version: u32,
    },
    Retry {
        dst_cid: ConnectionId,
        src_cid: ConnectionId,
        version: u32,
    },
    Short {
        spin: bool,
        key_phase: bool,
        dst_cid: ConnectionId,
        number: PacketNumber,
    },
    VersionNegotiate {
        random: u8,
        src_cid: ConnectionId,
        dst_cid: ConnectionId,
    },
}

impl Header {
    /// Whether the packet is encrypted on the wire
    pub fn is_protected(&self) -> bool {
        !matches!(self, Self::Retry { .. } | Self::VersionNegotiate { .. })
    }

    pub fn number(&self) -> Option<PacketNumber> {
        use Header::*;
        Some(match *self {
            Initial(InitialHeader { number, .. }) => number,
            Long { number, .. } => number,
            Short { number, .. } => number,
            _ => {
                return None;
            }
        })
    }

    pub fn space(&self) -> SpaceId {
        use Header::*;
        match self {
            Short { .. } => SpaceId::Data,
            Long {
                ty: LongType::ZeroRtt,
                ..
            } => SpaceId::Data,
            Long {
                ty: LongType::Handshake,
                ..
            } => SpaceId::Handshake,
            _ => SpaceId::Initial,
        }
    }

    pub fn key_phase(&self) -> bool {
        match *self {
            Self::Short { key_phase, .. } => key_phase,
            _ => false,
        }
    }

    pub fn is_1rtt(&self) -> bool {
        self.is_short()
    }

    pub fn is_0rtt(&self) -> bool {
        matches!(
            self,
            Self::Long {
                ty: LongType::ZeroRtt,
                ..
            }
        )
    }

    pub fn dst_cid(&self) -> ConnectionId {
        use Header::*;
        match *self {
            Initial(InitialHeader { dst_cid, .. }) => dst_cid,
            Long { dst_cid, .. } => dst_cid,
            Retry { dst_cid, .. } => dst_cid,
            Short { dst_cid, .. } => dst_cid,
            VersionNegotiate { dst_cid, .. } => dst_cid,
        }
    }

    /// Whether the payload of this packet contains QUIC frames
    pub fn has_frames(&self) -> bool {
        use Header::*;
        match self {
            Initial(_) | Long { .. } | Short { .. } => true,
            Retry { .. } | VersionNegotiate { .. } => false,
        }
    }

    pub fn encode<B: BufMut>(&self, out: &mut B) {
        use Header::*;
        match self {
            Initial(InitialHeader {
                dst_cid,
                src_cid,
                token,
                number,
                version,
            }) => {
                out.put_u8(u8::from(LongHeaderType::Initial) | number.tag());
                out.put_u32(*version);
                dst_cid.encode(out);
                src_cid.encode(out);
                out.put_bytes_with_varint_length(token);
                out.put_varint(number.len() as _); // Packet length
                number.encode(out);
            }
            Long {
                ty,
                dst_cid,
                src_cid,
                number,
                version,
            } => {
                out.put_u8(u8::from(LongHeaderType::Standard(*ty)) | number.tag());
                out.put_u32(*version);
                dst_cid.encode(out);
                src_cid.encode(out);
                out.put_varint(number.len() as _); // Packet length
                number.encode(out);
            }
            Retry {
                dst_cid,
                src_cid,
                version,
            } => {
                out.put_u8(u8::from(LongHeaderType::Retry));
                out.put_u32(*version);
                dst_cid.encode(out);
                src_cid.encode(out);
            }
            Short {
                spin,
                key_phase,
                dst_cid,
                number,
            } => {
                out.put_u8(
                    FIXED_BIT
                        | if *key_phase { KEY_PHASE_BIT } else { 0 }
                        | if *spin { SPIN_BIT } else { 0 }
                        | number.tag(),
                );
                out.put_slice(dst_cid);
                number.encode(out);
            }
            VersionNegotiate {
                random,
                dst_cid,
                src_cid,
            } => {
                out.put_u8(0x80u8 | random);
                out.put_u32(0); // Version
                dst_cid.encode(out);
                src_cid.encode(out);
            }
        }
    }

    pub fn decode<B: Buf>(
        buf: &mut B,
        cid_len: usize,
        supported_versions: &[u32],
    ) -> Result<Self, PacketDecodeError> {
        let first = buf.try_get_u8()?;
        if first & FIXED_BIT == 0 {
            return Err(PacketDecodeError::FixedBitUnset);
        }

        if first & LONG_HEADER_FORM == 0 {
            if first & SHORT_RESERVED_BITS != 0 {
                return Err(PacketDecodeError::ReservedBitsSet);
            }

            let spin = first & SPIN_BIT != 0;
            let key_phase = first & KEY_PHASE_BIT != 0;
            let dst_cid = ConnectionId::from_buf(buf, cid_len)?;
            let number = PacketNumber::decode(PacketNumber::decode_len(first), buf)?;
            Ok(Header::Short {
                spin,
                key_phase,
                dst_cid,
                number,
            })
        } else {
            if first & LONG_RESERVED_BITS != 0 {
                return Err(PacketDecodeError::ReservedBitsSet);
            }

            let ty = LongHeaderType::from(first);
            let version = buf.try_get_u32()?;
            let dst_cid = ConnectionId::decode(buf)?;
            let src_cid = ConnectionId::decode(buf)?;

            if version == 0 {
                let random = first & !LONG_HEADER_FORM;
                return Ok(Header::VersionNegotiate {
                    random,
                    src_cid,
                    dst_cid,
                });
            }

            if !supported_versions.contains(&version) {
                return Err(PacketDecodeError::UnsupportedVersion {
                    src_cid,
                    dst_cid,
                    version,
                });
            }

            match ty {
                LongHeaderType::Initial => {
                    let token = buf.try_get_bytes_with_varint_length()?;
                    let _ = buf.try_get_varint()?;
                    let number = PacketNumber::decode(PacketNumber::decode_len(first), buf)?;
                    Ok(Header::Initial(InitialHeader {
                        dst_cid,
                        src_cid,
                        token,
                        number,
                        version,
                    }))
                }
                LongHeaderType::Retry => Ok(Header::Retry {
                    dst_cid,
                    src_cid,
                    version,
                }),
                LongHeaderType::Standard(ty) => {
                    let _ = buf.try_get_varint()?;
                    let number = PacketNumber::decode(PacketNumber::decode_len(first), buf)?;
                    Ok(Header::Long {
                        ty,
                        dst_cid,
                        src_cid,
                        number,
                        version,
                    })
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Packet {
    pub header: Header,
    pub payload: Bytes,
}

impl From<InitialPacket> for Packet {
    fn from(packet: InitialPacket) -> Self {
        Self {
            header: Header::Initial(packet.header),
            payload: packet.payload,
        }
    }
}
