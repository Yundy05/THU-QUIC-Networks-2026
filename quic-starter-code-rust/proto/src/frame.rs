use std::{
    fmt::{self, Write},
    ops::{Deref, Range, RangeInclusive},
};

use bytes::{Buf, BufMut, Bytes, TryGetError};
use thiserror::Error;

use crate::{
    ArrayRangeSet, BufExt, BufMutExt, ConnectionId, Dir, EcnCodepoint, RESET_TOKEN_SIZE,
    ResetToken, StreamId, TransportError, TransportErrorCode, varint_len,
};

/// A QUIC frame type
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct FrameType(u64);

impl FrameType {
    fn stream(self) -> Option<StreamInfo> {
        if STREAM_TYS.contains(&self.0) {
            Some(StreamInfo(self.0 as u8))
        } else {
            None
        }
    }

    fn datagram(self) -> Option<DatagramInfo> {
        if DATAGRAM_TYS.contains(&self.0) {
            Some(DatagramInfo(self.0 as u8))
        } else {
            None
        }
    }
}

impl Deref for FrameType {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

macro_rules! frame_types {
    {$($name:ident = $val:expr,)*} => {
        impl FrameType {
            $(pub const $name: FrameType = FrameType($val);)*
        }

        impl fmt::Debug for FrameType {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self.0 {
                    $($val => f.write_str(stringify!($name)),)*
                    x => write!(f, "Type({:02x})", x)
                }
            }
        }

        impl fmt::Display for FrameType {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self.0 {
                    $($val => f.write_str(stringify!($name)),)*
                    x if STREAM_TYS.contains(&x) => f.write_str("STREAM"),
                    x if DATAGRAM_TYS.contains(&x) => f.write_str("DATAGRAM"),
                    x => write!(f, "<unknown {:02x}>", x),
                }
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct StreamInfo(u8);

impl StreamInfo {
    fn fin(self) -> bool {
        self.0 & 0x01 != 0
    }

    fn len(self) -> bool {
        self.0 & 0x02 != 0
    }

    fn off(self) -> bool {
        self.0 & 0x04 != 0
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct DatagramInfo(u8);

impl DatagramInfo {
    fn len(self) -> bool {
        self.0 & 0x01 != 0
    }
}

frame_types! {
    PADDING = 0x00,
    PING = 0x01,
    ACK = 0x02,
    ACK_ECN = 0x03,
    RESET_STREAM = 0x04,
    STOP_SENDING = 0x05,
    CRYPTO = 0x06,
    NEW_TOKEN = 0x07,
    // STREAM
    MAX_DATA = 0x10,
    MAX_STREAM_DATA = 0x11,
    MAX_STREAMS_BIDI = 0x12,
    MAX_STREAMS_UNI = 0x13,
    DATA_BLOCKED = 0x14,
    STREAM_DATA_BLOCKED = 0x15,
    STREAMS_BLOCKED_BIDI = 0x16,
    STREAMS_BLOCKED_UNI = 0x17,
    NEW_CONNECTION_ID = 0x18,
    RETIRE_CONNECTION_ID = 0x19,
    PATH_CHALLENGE = 0x1a,
    PATH_RESPONSE = 0x1b,
    TRANSPORT_CLOSE = 0x1c,
    APPLICATION_CLOSE = 0x1d,
    HANDSHAKE_DONE = 0x1e,
    // ACK Frequency
    ACK_FREQUENCY = 0xaf,
    IMMEDIATE_ACK = 0x1f,
    // DATAGRAM
}

const STREAM_TYS: RangeInclusive<u64> = RangeInclusive::new(0x08, 0x0f);
const DATAGRAM_TYS: RangeInclusive<u64> = RangeInclusive::new(0x30, 0x31);

#[derive(Debug)]
pub enum Frame {
    Padding,
    Ping,
    Ack(Ack),
    ResetStream(ResetStream),
    StopSending(StopSending),
    Crypto(Crypto),
    NewToken(NewToken),
    Stream(Stream),
    MaxData(u64),
    MaxStreamData { id: StreamId, limit: u64 },
    MaxStreams { dir: Dir, count: u64 },
    DataBlocked { limit: u64 },
    StreamDataBlocked { id: StreamId, limit: u64 },
    StreamsBlocked { dir: Dir, limit: u64 },
    NewConnectionId(NewConnectionId),
    RetireConnectionId { sequence: u64 },
    PathChallenge(u64),
    PathResponse(u64),
    Close(ConnectionClose),
    Datagram(Datagram),
    ImmediateAck,
    HandshakeDone,
}

impl Frame {
    pub fn ty(&self) -> FrameType {
        use Frame::*;
        match *self {
            Padding => FrameType::PADDING,
            ResetStream(_) => FrameType::RESET_STREAM,
            Close(ConnectionClose::Transport(_)) => FrameType::TRANSPORT_CLOSE,
            Close(ConnectionClose::Application(_)) => FrameType::APPLICATION_CLOSE,
            MaxData(_) => FrameType::MAX_DATA,
            MaxStreamData { .. } => FrameType::MAX_STREAM_DATA,
            MaxStreams { dir: Dir::Bi, .. } => FrameType::MAX_STREAMS_BIDI,
            MaxStreams { dir: Dir::Uni, .. } => FrameType::MAX_STREAMS_UNI,
            Ping => FrameType::PING,
            DataBlocked { .. } => FrameType::DATA_BLOCKED,
            StreamDataBlocked { .. } => FrameType::STREAM_DATA_BLOCKED,
            StreamsBlocked { dir: Dir::Bi, .. } => FrameType::STREAMS_BLOCKED_BIDI,
            StreamsBlocked { dir: Dir::Uni, .. } => FrameType::STREAMS_BLOCKED_UNI,
            StopSending { .. } => FrameType::STOP_SENDING,
            RetireConnectionId { .. } => FrameType::RETIRE_CONNECTION_ID,
            Ack(_) => FrameType::ACK,
            Stream(ref x) => {
                let mut ty = *STREAM_TYS.start();
                if x.fin {
                    ty |= 0x01;
                }
                if x.offset != 0 {
                    ty |= 0x04;
                }
                FrameType(ty)
            }
            PathChallenge(_) => FrameType::PATH_CHALLENGE,
            PathResponse(_) => FrameType::PATH_RESPONSE,
            NewConnectionId { .. } => FrameType::NEW_CONNECTION_ID,
            Crypto(_) => FrameType::CRYPTO,
            NewToken(_) => FrameType::NEW_TOKEN,
            Datagram(_) => FrameType(*DATAGRAM_TYS.start()),
            ImmediateAck => FrameType::IMMEDIATE_ACK,
            HandshakeDone => FrameType::HANDSHAKE_DONE,
        }
    }

    pub fn is_ack_eliciting(&self) -> bool {
        !matches!(*self, Self::Ack(_) | Self::Padding | Self::Close(_))
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct Ack {
    pub largest: u64,
    pub delay: u64,
    pub additional: Bytes,
    pub ecn: Option<EcnCounts>,
}

#[derive(Debug, Clone)]
pub struct AckIter<'a> {
    largest: u64,
    data: &'a [u8],
}

impl Iterator for AckIter<'_> {
    type Item = RangeInclusive<u64>;

    fn next(&mut self) -> Option<RangeInclusive<u64>> {
        if !self.data.has_remaining() {
            return None;
        }
        let block = self.data.try_get_varint().unwrap();
        let largest = self.largest;
        if let Ok(gap) = self.data.try_get_varint() {
            self.largest -= block + gap + 2;
        }
        Some(largest - block..=largest)
    }
}

impl<'a> IntoIterator for &'a Ack {
    type IntoIter = AckIter<'a>;
    type Item = RangeInclusive<u64>;

    fn into_iter(self) -> AckIter<'a> {
        AckIter {
            largest: self.largest,
            data: &self.additional[..],
        }
    }
}

impl fmt::Debug for Ack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut ranges = "[".to_string();
        let mut first = true;
        for range in self {
            if !first {
                ranges.push(',');
            }
            write!(ranges, "{range:?}").unwrap();
            first = false;
        }
        ranges.push(']');

        f.debug_struct("Ack")
            .field("largest", &self.largest)
            .field("delay", &self.delay)
            .field("ecn", &self.ecn)
            .field("ranges", &ranges)
            .finish()
    }
}

impl Ack {
    pub fn encode<W: BufMut>(
        delay: u64,
        ranges: &ArrayRangeSet,
        ecn: Option<&EcnCounts>,
        out: &mut W,
    ) {
        let mut rest = ranges.iter().rev();
        let first = rest.next().unwrap();
        let largest = first.end - 1;
        let first_size = first.end - first.start;
        out.put_varint(*if ecn.is_some() {
            FrameType::ACK_ECN
        } else {
            FrameType::ACK
        });
        out.put_varint(largest);
        out.put_varint(delay);
        out.put_varint(ranges.len() as u64 - 1);
        out.put_varint(first_size - 1);
        let mut prev = first.start;
        for block in rest {
            let size = block.end - block.start;
            out.put_varint(prev - block.end - 1);
            out.put_varint(size - 1);
            prev = block.start;
        }
        if let Some(x) = ecn {
            x.encode(out)
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct EcnCounts {
    pub ect0: u64,
    pub ect1: u64,
    pub ce: u64,
}

impl std::ops::AddAssign<EcnCodepoint> for EcnCounts {
    fn add_assign(&mut self, rhs: EcnCodepoint) {
        match rhs {
            EcnCodepoint::Ect0 => {
                self.ect0 += 1;
            }
            EcnCodepoint::Ect1 => {
                self.ect1 += 1;
            }
            EcnCodepoint::Ce => {
                self.ce += 1;
            }
        }
    }
}

impl EcnCounts {
    pub const ZERO: Self = Self {
        ect0: 0,
        ect1: 0,
        ce: 0,
    };

    pub fn encode<W: BufMut>(&self, out: &mut W) {
        out.put_varint(self.ect0);
        out.put_varint(self.ect1);
        out.put_varint(self.ce);
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ResetStream {
    pub id: StreamId,
    pub error_code: u64,
    pub final_offset: u64,
}

impl ResetStream {
    pub const SIZE_BOUND: usize = 1 + 8 + 8 + 8;

    pub fn encode<W: BufMut>(&self, out: &mut W) {
        out.put_varint(*FrameType::RESET_STREAM); // 1 byte
        out.put_varint(*self.id); // <= 8 bytes
        out.put_varint(self.error_code); // <= 8 bytes
        out.put_varint(self.final_offset); // <= 8 bytes
    }
}

#[derive(Debug, Copy, Clone)]
pub struct StopSending {
    pub id: StreamId,
    pub error_code: u64,
}

impl StopSending {
    pub const SIZE_BOUND: usize = 1 + 8 + 8;

    pub fn encode<W: BufMut>(&self, out: &mut W) {
        out.put_varint(*FrameType::STOP_SENDING); // 1 byte
        out.put_varint(*self.id); // <= 8 bytes
        out.put_varint(self.error_code) // <= 8 bytes
    }
}

#[derive(Debug, Clone)]
pub struct Crypto {
    pub offset: u64,
    pub data: Bytes,
}

impl Crypto {
    pub const SIZE_BOUND: usize = 17;

    pub fn encode<W: BufMut>(&self, out: &mut W) {
        out.put_varint(*FrameType::CRYPTO);
        out.put_varint(self.offset);
        out.put_varint(self.data.len() as u64);
        out.put_slice(&self.data);
    }
}

#[derive(Debug, Clone)]
pub struct NewToken {
    pub token: Bytes,
}

impl NewToken {
    pub fn encode<W: BufMut>(&self, out: &mut W) {
        out.put_varint(*FrameType::NEW_TOKEN);
        out.put_varint(self.token.len() as u64);
        out.put_slice(&self.token);
    }

    pub fn size(&self) -> usize {
        1 + varint_len(self.token.len() as u64) + self.token.len()
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    pub id: StreamId,
    pub offset: u64,
    pub fin: bool,
    pub data: Bytes,
}

impl Stream {
    pub const SIZE_BOUND: usize = 1 + 8 + 8 + 8;
}

/// Metadata from a stream frame
#[derive(Debug, Default, Clone)]
pub struct StreamMeta {
    pub id: StreamId,
    pub offsets: Range<u64>,
    pub fin: bool,
}

impl StreamMeta {
    pub fn encode<W: BufMut>(&self, length: bool, out: &mut W) {
        let mut ty = *STREAM_TYS.start();
        if self.offsets.start != 0 {
            ty |= 0x04;
        }
        if length {
            ty |= 0x02;
        }
        if self.fin {
            ty |= 0x01;
        }
        out.put_varint(ty); // 1 byte
        out.put_varint(*self.id); // <= 8 bytes
        if self.offsets.start != 0 {
            out.put_varint(self.offsets.start); // <= 8 bytes
        }
        if length {
            out.put_varint(self.offsets.end - self.offsets.start); // <= 8 bytes
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct NewConnectionId {
    pub sequence: u64,
    pub retire_prior_to: u64,
    pub id: ConnectionId,
    pub reset_token: ResetToken,
}

impl NewConnectionId {
    pub fn encode<W: BufMut>(&self, out: &mut W) {
        out.put_varint(*FrameType::NEW_CONNECTION_ID);
        out.put_varint(self.sequence);
        out.put_varint(self.retire_prior_to);
        self.id.encode(out);
        out.put_slice(&self.reset_token);
    }
}

#[derive(Clone, Debug)]
pub enum ConnectionClose {
    Transport(TransportClose),
    Application(ApplicationClose),
}

impl ConnectionClose {
    pub fn encode<W: BufMut>(&self, out: &mut W, max_len: usize) {
        match self {
            Self::Transport(x) => x.encode(out, max_len),
            Self::Application(x) => x.encode(out, max_len),
        }
    }
}

impl From<TransportError> for ConnectionClose {
    fn from(x: TransportError) -> Self {
        Self::Transport(x.into())
    }
}

impl From<TransportClose> for ConnectionClose {
    fn from(x: TransportClose) -> Self {
        Self::Transport(x)
    }
}

impl From<ApplicationClose> for ConnectionClose {
    fn from(x: ApplicationClose) -> Self {
        Self::Application(x)
    }
}

/// Reason given by the transport for closing the connection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportClose {
    /// Class of error as encoded in the specification
    pub error_code: TransportErrorCode,
    /// Type of frame that caused the close
    pub frame_type: Option<FrameType>,
    /// Human-readable reason for the close
    pub reason: Bytes,
}

impl fmt::Display for TransportClose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error_code.fmt(f)?;
        if !self.reason.as_ref().is_empty() {
            f.write_str(": ")?;
            f.write_str(&String::from_utf8_lossy(&self.reason))?;
        }
        Ok(())
    }
}

impl From<TransportError> for TransportClose {
    fn from(x: TransportError) -> Self {
        Self {
            error_code: x.code,
            frame_type: x.frame,
            reason: x.reason.into(),
        }
    }
}

impl TransportClose {
    pub const SIZE_BOUND: usize = 1 + 8 + 8 + 8;

    pub fn encode<W: BufMut>(&self, out: &mut W, max_len: usize) {
        out.put_varint(*FrameType::TRANSPORT_CLOSE); // 1 byte
        out.put_varint(*self.error_code); // <= 8 bytes
        let ty = self.frame_type.unwrap_or(FrameType::PADDING);
        out.put_varint(*ty); // <= 8 bytes
        let max_len = max_len - 3 - varint_len(ty.0) - varint_len(self.reason.len() as u64);
        let actual_len = self.reason.len().min(max_len);
        out.put_bytes_with_varint_length(&self.reason[0..actual_len]); // <= 8 bytes + whatever's left
    }
}

/// Reason given by an application for closing the connection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationClose {
    /// Application-specific reason code
    pub error_code: u64,
    /// Human-readable reason for the close
    pub reason: Bytes,
}

impl fmt::Display for ApplicationClose {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.reason.as_ref().is_empty() {
            f.write_str(&String::from_utf8_lossy(&self.reason))?;
            f.write_str(" (code ")?;
            self.error_code.fmt(f)?;
            f.write_str(")")?;
        } else {
            self.error_code.fmt(f)?;
        }
        Ok(())
    }
}

impl ApplicationClose {
    pub const SIZE_BOUND: usize = 1 + 8 + 8;

    pub fn encode<W: BufMut>(&self, out: &mut W, max_len: usize) {
        out.put_varint(*FrameType::APPLICATION_CLOSE); // 1 byte
        out.put_varint(self.error_code); // <= 8 bytes
        let max_len = max_len - 3 - varint_len(self.reason.len() as u64);
        let actual_len = self.reason.len().min(max_len);
        out.put_bytes_with_varint_length(&self.reason[0..actual_len]); // <= 8 bytes + whatever's left
    }
}

/// An unreliable datagram
#[derive(Debug, Clone)]
pub struct Datagram {
    /// Payload
    pub data: Bytes,
}

impl Datagram {
    pub const SIZE_BOUND: usize = 1 + 8;

    pub fn encode<W: BufMut>(&self, length: bool, out: &mut W) {
        out.put_varint(*DATAGRAM_TYS.start() | u64::from(length)); // 1 byte
        if length {
            out.put_varint(self.data.len() as u64); // <= 8 bytes
        }
        out.put_slice(&self.data);
    }

    pub fn size(&self, length: bool) -> usize {
        1 + if length {
            varint_len(self.data.len() as u64)
        } else {
            0
        } + self.data.len()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AckFrequency {
    pub sequence: u64,
    pub ack_eliciting_threshold: u64,
    pub request_max_ack_delay: u64,
    pub reordering_threshold: u64,
}

impl AckFrequency {
    pub fn encode<W: BufMut>(&self, buf: &mut W) {
        buf.put_varint(*FrameType::ACK_FREQUENCY);
        buf.put_varint(self.sequence);
        buf.put_varint(self.ack_eliciting_threshold);
        buf.put_varint(self.request_max_ack_delay);
        buf.put_varint(self.reordering_threshold);
    }
}

pub struct FrameParser {
    bytes: Bytes,
    last_ty: Option<FrameType>,
}

#[derive(Error, Debug, Eq, PartialEq)]
pub enum FrameParseError {
    #[error("unexpected end")]
    UnexpectedEnd(#[from] TryGetError),
    #[error("invalid frame ID")]
    InvalidFrameId,
    #[error("malformed frame")]
    Malformed,
}

impl FrameParser {
    pub fn new(payload: Bytes) -> Result<Self, TransportError> {
        if payload.is_empty() {
            // "An endpoint MUST treat receipt of a packet containing no frames as a
            // connection error of type PROTOCOL_VIOLATION."
            // https://www.rfc-editor.org/rfc/rfc9000.html#name-frames-and-frame-types
            return Err(TransportError::PROTOCOL_VIOLATION(
                "packet payload is empty",
            ));
        }

        Ok(Self {
            bytes: payload,
            last_ty: None,
        })
    }

    fn try_next(&mut self) -> Result<Frame, FrameParseError> {
        let buf = &mut self.bytes;
        let ty = FrameType(buf.try_get_varint()?);
        self.last_ty = Some(ty);
        Ok(match ty {
            FrameType::PADDING => Frame::Padding,
            FrameType::RESET_STREAM => Frame::ResetStream(ResetStream {
                id: buf.try_get_varint()?.into(),
                error_code: buf.try_get_varint()?,
                final_offset: buf.try_get_varint()?,
            }),
            FrameType::TRANSPORT_CLOSE => {
                Frame::Close(ConnectionClose::Transport(TransportClose {
                    error_code: buf.try_get_varint()?.into(),
                    frame_type: {
                        let x = buf.try_get_varint()?;
                        if x == 0 { None } else { Some(FrameType(x)) }
                    },
                    reason: buf.try_get_bytes_with_varint_length()?,
                }))
            }
            FrameType::APPLICATION_CLOSE => {
                Frame::Close(ConnectionClose::Application(ApplicationClose {
                    error_code: buf.try_get_varint()?,
                    reason: buf.try_get_bytes_with_varint_length()?,
                }))
            }
            FrameType::MAX_DATA => Frame::MaxData(buf.try_get_varint()?),
            FrameType::MAX_STREAM_DATA => Frame::MaxStreamData {
                id: buf.try_get_varint()?.into(),
                limit: buf.try_get_varint()?,
            },
            FrameType::MAX_STREAMS_BIDI => Frame::MaxStreams {
                dir: Dir::Bi,
                count: buf.try_get_varint()?,
            },
            FrameType::MAX_STREAMS_UNI => Frame::MaxStreams {
                dir: Dir::Uni,
                count: buf.try_get_varint()?,
            },
            FrameType::PING => Frame::Ping,
            FrameType::DATA_BLOCKED => Frame::DataBlocked {
                limit: buf.try_get_varint()?,
            },
            FrameType::STREAM_DATA_BLOCKED => Frame::StreamDataBlocked {
                id: buf.try_get_varint()?.into(),
                limit: buf.try_get_varint()?,
            },
            FrameType::STREAMS_BLOCKED_BIDI => Frame::StreamsBlocked {
                dir: Dir::Bi,
                limit: buf.try_get_varint()?,
            },
            FrameType::STREAMS_BLOCKED_UNI => Frame::StreamsBlocked {
                dir: Dir::Uni,
                limit: buf.try_get_varint()?,
            },
            FrameType::STOP_SENDING => Frame::StopSending(StopSending {
                id: buf.try_get_varint()?.into(),
                error_code: buf.try_get_varint()?,
            }),
            FrameType::RETIRE_CONNECTION_ID => Frame::RetireConnectionId {
                sequence: buf.try_get_varint()?,
            },
            FrameType::ACK | FrameType::ACK_ECN => {
                let largest = buf.try_get_varint()?;
                let delay = buf.try_get_varint()?;
                let extra_blocks = buf.try_get_varint()? as usize;
                let n = scan_ack_blocks(buf, largest, extra_blocks)?;
                Frame::Ack(Ack {
                    delay,
                    largest,
                    additional: buf.split_to(n),
                    ecn: if ty != FrameType::ACK_ECN {
                        None
                    } else {
                        Some(EcnCounts {
                            ect0: buf.try_get_varint()?,
                            ect1: buf.try_get_varint()?,
                            ce: buf.try_get_varint()?,
                        })
                    },
                })
            }
            FrameType::PATH_CHALLENGE => Frame::PathChallenge(buf.try_get_u64()?),
            FrameType::PATH_RESPONSE => Frame::PathResponse(buf.try_get_u64()?),
            FrameType::NEW_CONNECTION_ID => {
                let sequence = buf.try_get_varint()?;
                let retire_prior_to = buf.try_get_varint()?;
                if retire_prior_to > sequence {
                    return Err(FrameParseError::Malformed);
                }
                let id = ConnectionId::decode(buf)?;
                if id.is_empty() {
                    return Err(FrameParseError::Malformed);
                }
                let mut reset_token = [0; RESET_TOKEN_SIZE];
                buf.try_copy_to_slice(&mut reset_token)?;
                Frame::NewConnectionId(NewConnectionId {
                    sequence,
                    retire_prior_to,
                    id,
                    reset_token: reset_token.into(),
                })
            }
            FrameType::CRYPTO => Frame::Crypto(Crypto {
                offset: buf.try_get_varint()?,
                data: buf.try_get_bytes_with_varint_length()?,
            }),
            FrameType::NEW_TOKEN => Frame::NewToken(NewToken {
                token: buf.try_get_bytes_with_varint_length()?,
            }),
            FrameType::HANDSHAKE_DONE => Frame::HandshakeDone,
            FrameType::IMMEDIATE_ACK => Frame::ImmediateAck,
            _ if ty.stream().is_some() => {
                let s = ty.stream().expect("checked by guard");
                Frame::Stream(Stream {
                    id: buf.try_get_varint()?.into(),
                    offset: if s.off() { buf.try_get_varint()? } else { 0 },
                    fin: s.fin(),
                    data: if s.len() {
                        buf.try_get_bytes_with_varint_length()?
                    } else {
                        buf.split_to(buf.len())
                    },
                })
            }
            _ if ty.datagram().is_some() => {
                let d = ty.datagram().expect("checked by guard");
                Frame::Datagram(Datagram {
                    data: if d.len() {
                        buf.try_get_bytes_with_varint_length()?
                    } else {
                        buf.split_to(buf.len())
                    },
                })
            }
            _ => {
                return Err(FrameParseError::InvalidFrameId);
            }
        })
    }
}

/// Validate exactly `n` ACK ranges in `buf` and return the number of bytes they
/// cover
fn scan_ack_blocks(mut buf: &[u8], largest: u64, n: usize) -> Result<usize, FrameParseError> {
    let total_len = buf.remaining();
    let first_block = buf.try_get_varint()?;
    let mut smallest = largest
        .checked_sub(first_block)
        .ok_or(FrameParseError::Malformed)?;
    for _ in 0..n {
        let gap = buf.try_get_varint()?;
        smallest = smallest
            .checked_sub(gap + 2)
            .ok_or(FrameParseError::Malformed)?;
        let block = buf.try_get_varint()?;
        smallest = smallest
            .checked_sub(block)
            .ok_or(FrameParseError::Malformed)?;
    }
    Ok(total_len - buf.remaining())
}

impl Iterator for FrameParser {
    type Item = Result<Frame, TransportError>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.bytes.has_remaining() {
            return None;
        }
        match self.try_next() {
            Ok(x) => Some(Ok(x)),
            Err(e) => {
                // Corrupt frame, skip it and everything that follows
                self.bytes.clear();
                let mut e = TransportError::FRAME_ENCODING_ERROR(e.to_string());
                e.frame = self.last_ty;
                Some(Err(e))
            }
        }
    }
}
