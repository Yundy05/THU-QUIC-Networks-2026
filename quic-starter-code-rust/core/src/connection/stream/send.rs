use std::{collections::VecDeque, ops::Range};

use bytes::Bytes;
use mzquic_proto::{RangeSet, StreamId, StreamMeta, varint_len};
use thiserror::Error;
use tracing::{debug, warn};

use super::{ClosedStream, StreamsState};
use crate::connection::space::Retransmits;

#[derive(Debug, Default)]
pub(super) struct Send {
    pub(super) state: SendState,
    pub(super) pending: SendBuffer,
    pub(super) priority: i32,
    /// Whether a frame containing a FIN bit must be transmitted, even if we
    /// don't have any new data
    pub(super) fin_pending: bool,
    /// The reason the peer wants us to stop, if `STOP_SENDING` was received
    pub(super) stop_reason: Option<u64>,
}

impl Send {
    /// Whether the stream has been reset
    pub(super) fn is_reset(&self) -> bool {
        matches!(self.state, SendState::ResetSent)
    }

    pub(super) fn finish(&mut self) -> Result<(), FinishError> {
        tracing::info!("Stream finish");
        if let Some(error_code) = self.stop_reason {
            Err(FinishError::Stopped(error_code))
        } else if self.state == SendState::Ready {
            self.state = SendState::DataSent {
                finish_acked: false,
            };
            self.fin_pending = true;
            Ok(())
        } else {
            Err(FinishError::ClosedStream)
        }
    }

    pub(super) fn write(&mut self, data: &[u8]) -> Result<(), WriteError> {
        if !self.is_writable() {
            return Err(WriteError::ClosedStream);
        }
        if let Some(error_code) = self.stop_reason {
            return Err(WriteError::Stopped(error_code));
        }

        let chunk = Bytes::copy_from_slice(data);
        self.pending.write(chunk);

        Ok(())
    }

    /// Update stream state due to a reset sent by the local application
    pub(super) fn reset(&mut self) {
        use SendState::*;
        if let DataSent { .. } | Ready = self.state {
            self.state = ResetSent;
        }
    }

    /// Handle STOP_SENDING
    ///
    /// Returns true if the stream was stopped due to this frame, and false
    /// if it had been stopped before
    pub(super) fn try_stop(&mut self, error_code: u64) -> bool {
        if self.stop_reason.is_none() {
            self.stop_reason = Some(error_code);
            true
        } else {
            false
        }
    }

    /// Returns whether the stream has been finished and all data has been
    /// acknowledged by the peer
    /// Apply one ACKed stream range, update FIN confirmation, and report
    /// whether the send stream is fully complete.
    pub(super) fn ack(&mut self, frame: StreamMeta) -> bool {
        self.pending.ack(frame.offsets);
        match self.state {
            SendState::DataSent {
                ref mut finish_acked,
            } => {
                *finish_acked |= frame.fin;
                *finish_acked && self.pending.is_fully_acked()
            }
            _ => false,
        }
    }

    pub(super) fn offset(&self) -> u64 {
        self.pending.offset()
    }

    pub(super) fn is_pending(&self) -> bool {
        self.pending.has_unsent_data() || self.fin_pending
    }

    pub(super) fn is_writable(&self) -> bool {
        matches!(self.state, SendState::Ready)
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub(super) enum SendState {
    #[default]
    /// Sending new data
    Ready,
    /// Stream was finished; now sending retransmits only
    DataSent { finish_acked: bool },
    /// Sent RESET
    ResetSent,
}

/// Buffer of outgoing retransmittable stream data
#[derive(Default, Debug)]
pub(super) struct SendBuffer {
    /// Data queued by the application but not yet acknowledged. May or may not
    /// have been sent.
    unacked_segments: VecDeque<Bytes>,
    /// Total size of `unacked_segments`
    unacked_len: usize,
    /// The first offset that hasn't been written by the application, i.e. the
    /// offset past the end of `unacked`
    offset: u64,
    /// The first offset that hasn't been sent
    ///
    /// Always lies in (offset - unacked.len())..offset
    unsent: u64,
    /// Acknowledged ranges which couldn't be discarded yet as they don't
    /// include the earliest offset in `unacked`
    acks: RangeSet,
    /// Previously transmitted ranges deemed lost
    retransmits: RangeSet,
}

impl SendBuffer {
    /// Append application data to the end of the stream
    fn write(&mut self, data: Bytes) {
        self.unacked_len += data.len();
        self.offset += data.len() as u64;
        self.unacked_segments.push_back(data);
    }

    /// Discard a range of acknowledged stream data
    /// Remove ACKed data from the send buffer and keep `unacked`/`acks`
    /// bookkeeping consistent.
    fn ack(&mut self, range: Range<u64>) {
        // TODO
        let _ = range;
        unimplemented!("implement SendBuffer::ack");
    }

    /// Compute the next range to transmit on this stream and update state to
    /// account for that transmission.
    ///
    /// `max_len` here includes the space which is available to transmit the
    /// offset and length of the data to send. The caller has to guarantee that
    /// there is at least enough space available to write maximum-sized metadata
    /// (8 byte offset + 8 byte length).
    ///
    /// The method returns a tuple:
    /// - The first return value indicates the range of data to send
    /// - The second return value indicates whether the length needs to be
    ///   encoded in the STREAM frames metadata (`true`), or whether it can be
    ///   omitted since the selected range will fill the whole packet.
    pub(super) fn poll_transmit(&mut self, mut max_len: usize) -> (Range<u64>, bool) {
        debug_assert!(max_len >= 8 + 8);
        let mut encode_length = false;

        if let Some(_range) = self.retransmits.pop_min() {
            // Retransmit sent data

            // When the offset is known, we know how many bytes are required to encode it.
            // Offset 0 requires no space
            //
            // TODO: Retransmit sent data.
            unimplemented!("Retransmit sent data")
        }

        // Transmit new data

        // When the offset is known, we know how many bytes are required to encode it.
        // Offset 0 requires no space
        if self.unsent != 0 {
            max_len -= varint_len(self.unsent);
        }
        if self.offset - self.unsent < max_len as u64 {
            encode_length = true;
            max_len -= 8;
        }

        let end = self
            .offset
            .min((max_len as u64).saturating_add(self.unsent));
        let result = self.unsent..end;
        self.unsent = end;
        (result, encode_length)
    }

    /// Returns data which is associated with a range
    ///
    /// This function can return a subset of the range, if the data is stored
    /// in noncontiguous fashion in the send buffer. In this case callers
    /// should call the function again with an incremented start offset to
    /// retrieve more data.
    /// Return a contiguous slice for the requested offsets from buffered
    /// stream data.
    pub(super) fn get(&self, offsets: Range<u64>) -> &[u8] {
        // TODO
        let _ = offsets;
        unimplemented!("implement SendBuffer::get");
    }

    /// Queue a range of sent but unacknowledged data to be retransmitted
    /// Mark a lost sent range for retransmission in future packet assembly.
    pub(super) fn retransmit(&mut self, range: Range<u64>) {
        debug_assert!(range.end <= self.unsent, "unsent data can't be lost");
        self.retransmits.insert(range);
    }

    /// First stream offset unwritten by the application, i.e. the offset that
    /// the next write will begin at
    pub(super) fn offset(&self) -> u64 {
        self.offset
    }

    /// Whether all sent data has been acknowledged
    pub(super) fn is_fully_acked(&self) -> bool {
        self.unacked_len == 0
    }

    /// Whether there's data to send
    ///
    /// There may be sent unacknowledged data even when this is false.
    pub(super) fn has_unsent_data(&self) -> bool {
        self.unsent != self.offset || !self.retransmits.is_empty()
    }
}

/// Errors triggered while writing to a send stream
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum WriteError {
    /// The peer is not able to accept additional data, or the connection is
    /// congested.
    ///
    /// If the peer issues additional flow control credit, a
    /// [`StreamEvent::Writable`] event will be generated, indicating that
    /// retrying the write might succeed.
    ///
    /// [`StreamEvent::Writable`]: crate::StreamEvent::Writable
    #[error("unable to accept further writes")]
    Blocked,
    /// The peer is no longer accepting data on this stream, and it has been
    /// implicitly reset. The stream cannot be finished or further written
    /// to.
    ///
    /// Carries an application-defined error code.
    ///
    /// [`StreamEvent::Finished`]: crate::StreamEvent::Finished
    #[error("stopped by peer: code {0}")]
    Stopped(u64),
    /// The stream has not been opened or has already been finished or reset
    #[error("closed stream")]
    ClosedStream,
}

/// Reasons why attempting to finish a stream might fail
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FinishError {
    /// The peer is no longer accepting data on this stream. No
    /// [`StreamEvent::Finished`] event will be emitted for this stream.
    ///
    /// Carries an application-defined error code.
    ///
    /// [`StreamEvent::Finished`]: crate::StreamEvent::Finished
    #[error("stopped by peer: code {0}")]
    Stopped(u64),
    /// The stream has not been opened or was already finished or reset
    #[error("closed stream")]
    ClosedStream,
}

/// Access to streams
pub struct SendStream<'a> {
    pub(crate) id: StreamId,
    pub(crate) state: &'a mut StreamsState,
    pub(crate) pending: &'a mut Retransmits,
    pub(crate) conn_state: &'a crate::connection::State,
}

impl SendStream<'_> {
    /// Send data on the given stream
    pub fn write(&mut self, data: &[u8]) -> Result<(), WriteError> {
        if self.conn_state.is_closed() {
            debug!(%self.id, "write blocked; connection draining");
            return Err(WriteError::Blocked);
        }

        let stream = self.state.send.entry(self.id).or_default();

        let was_pending = stream.is_pending();
        stream.write(data)?;
        if !was_pending {
            self.state.pending.push_pending(self.id, stream.priority);
        }
        Ok(())
    }

    /// Check if this stream was stopped, get the reason if it was
    pub fn stopped(&self) -> Result<Option<u64>, ClosedStream> {
        match self.state.send.get(&self.id) {
            Some(s) => Ok(s.stop_reason),
            None => Err(ClosedStream),
        }
    }

    /// Finish a send stream, signalling that no more data will be sent.
    ///
    /// If this fails, no [`StreamEvent::Finished`] will be generated.
    pub fn finish(&mut self) -> Result<(), FinishError> {
        let stream = self
            .state
            .send
            .get_mut(&self.id)
            .ok_or(FinishError::ClosedStream)?;

        let was_pending = stream.is_pending();
        stream.finish()?;
        if !was_pending {
            self.state.pending.push_pending(self.id, stream.priority);
        }

        Ok(())
    }

    /// Abandon transmitting data on a stream
    pub fn reset(&mut self, error_code: u64) -> Result<(), ClosedStream> {
        let stream = self.state.send.get_mut(&self.id).ok_or(ClosedStream)?;

        if matches!(stream.state, SendState::ResetSent) {
            // Redundant reset call
            return Err(ClosedStream);
        }
        warn!(?error_code, "Resetting stream, id = {:?}", self.id);
        stream.reset();
        self.pending.reset_stream.push((self.id, error_code));

        // Don't reopen an already-closed stream we haven't forgotten yet
        Ok(())
    }

    /// Set the priority of a stream
    pub fn set_priority(&mut self, priority: i32) -> Result<(), ClosedStream> {
        let stream = self.state.send.get_mut(&self.id).ok_or(ClosedStream)?;

        stream.priority = priority;
        Ok(())
    }

    /// Get the priority of a stream
    pub fn priority(&self) -> Result<i32, ClosedStream> {
        let stream = self.state.send.get(&self.id).ok_or(ClosedStream)?;

        Ok(stream.priority)
    }
}
