use std::{
    cmp::Ordering,
    collections::{VecDeque, hash_map::Entry},
    mem,
};

use bytes::{Buf, Bytes};
use mzquic_proto::{RangeSet, StopSending, Stream, StreamId, TransportError};
use strum::EnumIs;
use thiserror::Error;
use tracing::debug;

use super::{ClosedStream, StreamsState};
use crate::connection::space::Retransmits;

#[derive(Debug, Default)]
pub(super) struct Recv {
    // NB: when adding or removing fields, remember to update `reinit`.
    state: RecvState,
    assembler: Assembler,
    pub(super) end: u64,
    pub(super) stopped: bool,
}

impl Recv {
    /// Process a STREAM frame
    ///
    /// Return value is `(number_of_new_bytes_ingested, stream_is_closed)`
    pub(super) fn ingest(
        &mut self,
        frame: Stream,
        payload_len: usize,
    ) -> Result<(u64, bool), TransportError> {
        let end = frame.offset + frame.data.len() as u64;
        if end >= 2u64.pow(62) {
            return Err(TransportError::FLOW_CONTROL_ERROR(
                "maximum stream offset too large",
            ));
        }

        if let Some(final_offset) = self.final_offset()
            && (end > final_offset || (frame.fin && end != final_offset))
        {
            debug!(end, final_offset, "final size error");
            return Err(TransportError::FINAL_SIZE_ERROR(""));
        }

        let new_bytes = end.saturating_sub(self.end);

        // Stopped streams don't need to wait for the actual data, they just need to
        // know how much there was.
        if frame.fin
            && !self.stopped
            && let RecvState::Recv { ref mut size } = self.state
        {
            *size = Some(end);
        }

        self.end = self.end.max(end);
        // Don't bother storing data or releasing stream-level flow control credit if
        // the stream's already stopped
        if !self.stopped {
            self.assembler.insert(frame.offset, frame.data, payload_len);
        }

        Ok((new_bytes, frame.fin && self.stopped))
    }

    pub(super) fn stop(&mut self) -> Result<(u64, bool), ClosedStream> {
        if self.stopped {
            return Err(ClosedStream);
        }

        self.stopped = true;
        self.assembler.clear();
        // Issue flow control credit for unread data
        let read_credits = self.end - self.assembler.bytes_read();
        // This may send a spurious STOP_SENDING if we've already received all data, but
        // it's a bit fiddly to distinguish that from the case where we've received a
        // FIN but are missing some data that the peer might still be trying to
        // retransmit, in which case a STOP_SENDING is still useful.
        Ok((read_credits, self.is_receiving()))
    }

    /// Whether the total amount of data that the peer will send on this stream
    /// is unknown
    ///
    /// True until we've received either a reset or the final frame.
    ///
    /// Implies that the sender might benefit from stream-level flow control
    /// updates, and we might need to issue connection-level flow control
    /// updates due to flow control budget use by this stream in the future,
    /// even if it's been stopped.
    pub(super) fn final_offset_unknown(&self) -> bool {
        matches!(self.state, RecvState::Recv { size: None })
    }

    /// Whether data is still being accepted from the peer
    pub(super) fn is_receiving(&self) -> bool {
        matches!(self.state, RecvState::Recv { .. })
    }

    fn final_offset(&self) -> Option<u64> {
        match self.state {
            RecvState::Recv { size } => size,
            RecvState::ResetRecvd { size, .. } => Some(size),
        }
    }

    /// Returns `false` iff the reset was redundant
    pub(super) fn reset(
        &mut self,
        error_code: u64,
        final_offset: u64,
    ) -> Result<bool, TransportError> {
        // Validate final_offset
        if let Some(offset) = self.final_offset() {
            if offset != final_offset {
                return Err(TransportError::FINAL_SIZE_ERROR("inconsistent value"));
            }
        } else if self.end > final_offset {
            return Err(TransportError::FINAL_SIZE_ERROR(
                "lower than high water mark",
            ));
        }

        if matches!(self.state, RecvState::ResetRecvd { .. }) {
            return Ok(false);
        }
        self.state = RecvState::ResetRecvd {
            size: final_offset,
            error_code,
        };
        // Nuke buffers so that future reads fail immediately, which ensures future
        // reads don't issue flow control credit redundant to that already issued. We
        // could instead special-case reset streams during read, but it's unclear if
        // there's any benefit to retaining data for reset streams.
        self.assembler.clear();
        Ok(true)
    }

    pub(super) fn reset_code(&self) -> Option<u64> {
        match self.state {
            RecvState::ResetRecvd { error_code, .. } => Some(error_code),
            _ => None,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum RecvState {
    Recv { size: Option<u64> },
    ResetRecvd { size: u64, error_code: u64 },
}

impl Default for RecvState {
    fn default() -> Self {
        Self::Recv { size: None }
    }
}

/// Helper to assemble unordered stream frames into an ordered stream
///TODO: A more efficient implementation that can handle out-of-order and missing data.
#[derive(Debug, Default)]
struct Assembler {
    state: AssemblerState,
    // Store received data.
    // TODO: more efficient data structure may be needed.
    data: VecDeque<Buffer>,
    /// Total number of buffered bytes, including duplicates in ordered mode.
    buffered: usize,
    /// Estimated number of allocated bytes, will never be less than `buffered`.
    allocated: usize,
    /// Number of bytes read by the application. When only ordered reads have
    /// been used, this is the length of the contiguous prefix of the stream
    /// which has been consumed by the application, aka the stream offset.
    bytes_read: u64,
    end: u64,
}

impl Assembler {
    fn ensure_ordering(&mut self, ordered: bool) -> Result<(), IllegalOrderedRead> {
        if ordered && !self.state.is_ordered() {
            return Err(IllegalOrderedRead);
        } else if !ordered && self.state.is_ordered() {
            // Enter unordered mode
            // TODO -- ATTEMPTING
            //(optional): Support Unordered mode of reading.
            unimplemented!("Unordered mode of recv stream");
        }
        Ok(())
    }

    /// Get the the next chunk
    fn read(&mut self, max_length: usize, ordered: bool) -> Option<Chunk> {
        if !ordered {
            unimplemented!("Unordered read is not supported");
        }

        // Ordered reads may only release the contiguous prefix.
        let front_offset = self.data.front()?.offset;
        if front_offset != self.bytes_read {
            return None;
        }

        let chunk = self.data.front_mut()?;
        if max_length < chunk.bytes.len() {
            self.bytes_read += max_length as u64;
            let offset = chunk.offset;
            chunk.offset += max_length as u64;
            self.buffered -= max_length;
            return Chunk {
                offset,
                bytes: chunk.bytes.split_to(max_length),
            }
            .into();
        } else {
            self.bytes_read += chunk.bytes.len() as u64;
            self.buffered -= chunk.bytes.len();
            self.allocated -= chunk.allocation_size;
            let chunk = self.data.pop_front()?;
            return Chunk {
                offset: chunk.offset,
                bytes: chunk.bytes,
            }
            .into();
        }
    }

    fn insert(&mut self, mut offset: u64, mut bytes: Bytes, allocation_size: usize) {
        debug_assert!(
            bytes.len() <= allocation_size,
            "allocation_size less than bytes.len(): {:?} < {:?}",
            allocation_size,
            bytes.len()
        );

        // TODO -- ATTEMPTING: make this able to handle ordered data.
        self.end = self.end.max(offset + bytes.len() as u64);

        if bytes.is_empty() {
            return;
        }

        // Drop data that the application has already consumed.
        let frame_end = offset + bytes.len() as u64;
        if frame_end <= self.bytes_read {
            return;
        }

        // Trim already-read prefix.
        if offset < self.bytes_read {
            let duplicate = (self.bytes_read - offset) as usize;

            if duplicate >= bytes.len() {
                return;
            }

            bytes.advance(duplicate);
            offset = self.bytes_read;
        }

        if bytes.is_empty() {
            return;
        }

        // Keep self.data sorted by offset, with no overlaps.
        let mut insert_at = self.data.len();
        for (i, buf) in self.data.iter().enumerate() {
            let buf_end = buf.offset + buf.bytes.len() as u64;
            if buf_end > offset {
                insert_at = i;
                break;
            }
        }

        // Trim overlap with previous segment.
        if insert_at > 0 {
            let prev = &self.data[insert_at - 1];
            let prev_end = prev.offset + prev.bytes.len() as u64;

            if prev_end > offset {
                let duplicate = (prev_end - offset) as usize;

                if duplicate >= bytes.len() {
                    return;
                }

                bytes.advance(duplicate);
                offset = prev_end;
            }
        }

        // Drop fully covered following segments, or trim suffix overlap.
        let new_end = offset + bytes.len() as u64;
        while insert_at < self.data.len() {
            let buf = &self.data[insert_at];
            let buf_end = buf.offset + buf.bytes.len() as u64;

            if buf.offset >= new_end {
                break;
            }

            if buf_end <= new_end {
                let removed = self.data.remove(insert_at).unwrap();
                self.buffered -= removed.bytes.len();
                self.allocated -= removed.allocation_size;
                continue;
            }

            let keep = (buf.offset - offset) as usize;
            bytes.truncate(keep);
            break;
        }

        if bytes.is_empty() {
            return;
        }

        let buffer = Buffer::new(offset, bytes, allocation_size);
        self.buffered += buffer.bytes.len();
        self.allocated += buffer.allocation_size;
        self.data.insert(insert_at, buffer);
    }

    /// Number of bytes consumed by the application
    fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Discard all buffered data
    fn clear(&mut self) {
        self.data.clear();
        self.buffered = 0;
        self.allocated = 0;
    }
}

/// A chunk of data from the receive stream
#[derive(Debug, PartialEq, Eq)]
pub struct Chunk {
    /// The offset in the stream
    pub offset: u64,
    /// The contents of the chunk
    pub bytes: Bytes,
}

#[derive(Debug, Eq)]
struct Buffer {
    offset: u64,
    bytes: Bytes,
    /// Size of the allocation behind `bytes`, if `defragmented == false`.
    /// Otherwise this will be set to `bytes.len()` by `try_mark_defragment`.
    /// Will never be less than `bytes.len()`.
    allocation_size: usize,
    defragmented: bool,
}

impl Buffer {
    /// Constructs a new fragmented Buffer
    fn new(offset: u64, bytes: Bytes, allocation_size: usize) -> Self {
        Self {
            offset,
            bytes,
            allocation_size,
            defragmented: false,
        }
    }

    /// Constructs a new defragmented Buffer
    fn new_defragmented(offset: u64, bytes: Bytes) -> Self {
        let allocation_size = bytes.len();
        Self {
            offset,
            bytes,
            allocation_size,
            defragmented: true,
        }
    }

    /// Discards data before `offset` and flags `self` as defragmented if it has
    /// good utilization
    fn try_mark_defragment(&mut self, offset: u64) {
        let duplicate = offset.saturating_sub(self.offset) as usize;
        self.offset = self.offset.max(offset);
        if duplicate >= self.bytes.len() {
            // All bytes are duplicate
            self.bytes = Bytes::new();
            self.defragmented = true;
            self.allocation_size = 0;
            return;
        }
        self.bytes.advance(duplicate);
        // Make sure that fragmented buffers with high utilization become defragmented
        // and defragmented buffers remain defragmented
        self.defragmented = self.defragmented || self.bytes.len() * 6 / 5 >= self.allocation_size;
        if self.defragmented {
            // Make sure that defragmented buffers do not contribute to over-allocation
            self.allocation_size = self.bytes.len();
        }
    }
}

impl Ord for Buffer {
    // Invert ordering based on offset (max-heap, min offset first), prioritize
    // longer chunks at the same offset.
    fn cmp(&self, other: &Self) -> Ordering {
        self.offset
            .cmp(&other.offset)
            .reverse()
            .then(self.bytes.len().cmp(&other.bytes.len()))
    }
}

impl PartialOrd for Buffer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        (self.offset, self.bytes.len()) == (other.offset, other.bytes.len())
    }
}

#[derive(Debug, Default, EnumIs)]
enum AssemblerState {
    #[default]
    Ordered,
    Unordered {
        /// The set of offsets that have been received from the peer, including
        /// portions not yet read by the application.
        recvd: RangeSet,
    },
}

/// Error indicating that an ordered read was performed on a stream after an
/// unordered read
#[derive(Debug)]
pub struct IllegalOrderedRead;

/// Chunks returned from [`RecvStream::read()`][crate::RecvStream::read].
///
/// If this type is leaked, the stream will remain blocked on the remote peer
/// until another read from the stream is done.
pub struct Chunks<'a> {
    id: StreamId,
    ordered: bool,
    streams: &'a mut StreamsState,
    state: ChunksState,
    read: u64,
}

impl<'a> Chunks<'a> {
    pub(super) fn new(
        id: StreamId,
        ordered: bool,
        streams: &'a mut StreamsState,
    ) -> Result<Self, ReadableError> {
        let mut entry = match streams.recv.entry(id) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(_) => return Err(ReadableError::ClosedStream),
        };

        let mut recv = match entry.get_mut().stopped {
            true => return Err(ReadableError::ClosedStream),
            false => entry.remove(),
        };

        recv.assembler.ensure_ordering(ordered)?;
        Ok(Self {
            id,
            ordered,
            streams,
            state: ChunksState::Readable(recv),
            read: 0,
        })
    }

    /// Next
    pub fn next(&mut self, max_length: usize) -> Result<Option<Chunk>, ReadError> {
        let rs = match self.state {
            ChunksState::Readable(ref mut rs) => rs,
            ChunksState::Reset(error_code) => {
                return Err(ReadError::Reset(error_code));
            }
            ChunksState::Finished => {
                return Ok(None);
            }
        };

        if let Some(chunk) = rs.assembler.read(max_length, self.ordered) {
            self.read += chunk.bytes.len() as u64;
            return Ok(Some(chunk));
        }

        match rs.state {
            RecvState::ResetRecvd { error_code, .. } => {
                debug_assert_eq!(self.read, 0, "reset streams have empty buffers");
                self.state = ChunksState::Reset(error_code);
                Err(ReadError::Reset(error_code))
            }
            RecvState::Recv { size } => {
                if size == Some(rs.end) && rs.assembler.bytes_read() == rs.end {
                    self.state = ChunksState::Finished;
                    Ok(None)
                } else {
                    // We don't need a distinct `ChunksState` variant for a blocked stream because
                    // retrying a read harmlessly re-traces our steps back to returning
                    // `Err(Blocked)` again. The buffers can't refill and the stream's own state
                    // can't change so long as this `Chunks` exists.
                    Err(ReadError::Blocked)
                }
            }
        }
    }
}

impl Drop for Chunks<'_> {
    fn drop(&mut self) {
        let state = mem::replace(&mut self.state, ChunksState::Finished);

        if let ChunksState::Readable(rs) = state {
            // Return the stream to storage for future use
            self.streams.recv.insert(self.id, rs);
        }
    }
}

enum ChunksState {
    Readable(Recv),
    Reset(u64),
    Finished,
}

/// Errors triggered when reading from a recv stream
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReadError {
    /// No more data is currently available on this stream.
    ///
    /// If more data on this stream is received from the peer, an
    /// `Event::StreamReadable` will be generated for this stream,
    /// indicating that retrying the read might succeed.
    #[error("blocked")]
    Blocked,
    /// The peer abandoned transmitting data on this stream.
    ///
    /// Carries an application-defined error code.
    #[error("reset by peer: code {0}")]
    Reset(u64),
}

/// Errors triggered when opening a recv stream for reading
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReadableError {
    /// The stream has not been opened or was already stopped, finished, or
    /// reset
    #[error("closed stream")]
    ClosedStream,
    /// Attempted an ordered read following an unordered read
    ///
    /// Performing an unordered read allows discontinuities to arise in the
    /// receive buffer of a stream which cannot be recovered, making further
    /// ordered reads impossible.
    #[error("ordered read after unordered read")]
    IllegalOrderedRead,
}

impl From<IllegalOrderedRead> for ReadableError {
    fn from(_: IllegalOrderedRead) -> Self {
        Self::IllegalOrderedRead
    }
}

/// Access to streams
pub struct RecvStream<'a> {
    pub(crate) id: StreamId,
    pub(crate) state: &'a mut StreamsState,
    pub(crate) pending: &'a mut Retransmits,
}

impl RecvStream<'_> {
    /// Read from the given recv stream
    ///
    /// Yields `Ok(None)` if the stream was finished. Otherwise, yields a
    /// segment of data and its offset in the stream. If `ordered` is `false`,
    /// segments may be received in any order, and the `Chunk`'s `offset` field
    /// can be used to determine ordering in the caller.
    ///
    /// While most applications will prefer to consume stream data in order,
    /// unordered reads can improve performance when packet loss occurs and data
    /// cannot be retransmitted before the flow control window is filled. On any
    /// given stream, you can switch from ordered to unordered reads, but
    /// ordered reads on streams that have seen previous unordered reads will
    /// return `ReadError::IllegalOrderedRead`.
    pub fn read(&mut self, ordered: bool) -> Result<Chunks<'_>, ReadableError> {
        Chunks::new(self.id, ordered, self.state)
    }

    /// Stop accepting data on the given receive stream
    ///
    /// Discards unread data and notifies the peer to stop transmitting. Once
    /// stopped, further attempts to operate on a stream will yield
    /// `ClosedStream` errors.
    pub fn stop(&mut self, error_code: u64) -> Result<(), ClosedStream> {
        tracing::warn!("Try to stop {:?} with code {}", self.id, error_code);
        let mut entry = match self.state.recv.entry(self.id) {
            Entry::Occupied(s) => s,
            Entry::Vacant(_) => return Err(ClosedStream),
        };
        let stream = entry.get_mut();

        let (_, stop_sending) = stream.stop()?;
        if stop_sending {
            self.pending.stop_sending.push(StopSending {
                id: self.id,
                error_code,
            });
        }

        // We need to keep stopped streams around until they're finished or reset so we
        // can update connection-level flow control to account for discarded data.
        // Otherwise, we can discard state immediately.
        if !stream.final_offset_unknown() {
            entry.remove();
        }

        Ok(())
    }

    /// Check whether this stream has been reset by the peer, returning the
    /// reset error code if so
    ///
    /// After returning `Ok(Some(_))` once, stream state will be discarded and
    /// all future calls will return `Err(ClosedStream)`.
    pub fn received_reset(&mut self) -> Result<Option<u64>, ClosedStream> {
        let Entry::Occupied(entry) = self.state.recv.entry(self.id) else {
            return Err(ClosedStream);
        };
        let s = entry.get();
        if s.stopped {
            return Err(ClosedStream);
        }
        let Some(code) = s.reset_code() else {
            return Ok(None);
        };

        entry.remove_entry();

        Ok(Some(code))
    }
}
