use std::collections::{BinaryHeap, HashMap, hash_map::Entry};

use bytes::BufMut;
use mzquic_proto::{
    Dir, ResetStream, Side, StopSending, Stream, StreamId, StreamMeta, TransportError,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, info, trace, warn};

use super::{Recv, Send, SendState};
use crate::{
    Event,
    connection::space::{Retransmits, ThinRetransmits},
};

#[derive(Debug)]
pub struct StreamsState {
    pub(super) side: Side,
    pub(super) send: HashMap<StreamId, Send>,
    pub(super) recv: HashMap<StreamId, Recv>,
    pub(super) next: [u64; 2],
    /// Streams with outgoing data queued, sorted by priority
    pub(super) pending: PendingStreamsQueue,
    pub(crate) events: UnboundedSender<Event>,
}

impl StreamsState {
    pub fn new(side: Side, events: UnboundedSender<Event>) -> Self {
        Self {
            side,
            send: HashMap::new(),
            recv: HashMap::new(),
            next: [0, 0],
            pending: PendingStreamsQueue::new(),
            events,
        }
    }

    pub fn send_event(&mut self, event: StreamEvent) {
        self.events.send(Event::Stream(event)).ok();
    }

    /// Open a new stream for the given directionality
    pub fn open(&mut self, dir: Dir) -> StreamId {
        let index = self.next[dir as usize];
        self.next[dir as usize] += 1;
        let id = StreamId::new(self.side, dir, index);
        self.send.insert(id, Send::default());
        if matches!(dir, Dir::Bi) {
            self.recv.insert(id, Recv::default());
        }
        id
    }

    /// Check for errors entailed by the peer's use of `id` as a send stream
    fn validate_receive_id(&mut self, id: StreamId) -> Result<(), TransportError> {
        if self.side == id.initiator() {
            match id.dir() {
                Dir::Uni => {
                    return Err(TransportError::STREAM_STATE_ERROR(
                        "illegal operation on send-only stream",
                    ));
                }
                Dir::Bi if id.index() >= self.next[Dir::Bi as usize] => {
                    return Err(TransportError::STREAM_STATE_ERROR(
                        "operation on unopened stream",
                    ));
                }
                Dir::Bi => {}
            };
        }
        Ok(())
    }

    /// Process incoming stream frame
    /// Ingest a STREAM frame, update receive-stream state, and emit
    /// readability events when application data becomes available.
    pub(crate) fn received(
        &mut self,
        frame: Stream,
        payload_len: usize,
    ) -> Result<(), TransportError> {
        let id = frame.id;
        self.validate_receive_id(id).inspect_err(|_| {
            debug!("received illegal STREAM frame");
        })?;

        tracing::trace!(
            "Received on stream{:?}, [{:?},{:?}, fin={:?})",
            id,
            frame.offset,
            frame.offset + frame.data.len() as u64,
            frame.fin
        );
        if frame.fin {
            tracing::info!(
                "FIN received on stream {} at offset {}",
                id,
                frame.offset + frame.data.len() as u64,
            );
        }

        let rs = self.recv.entry(id).or_default();

        if !rs.is_receiving() {
            trace!("dropping frame for finished stream");
            return Ok(());
        }

        let (_, closed) = rs.ingest(frame, payload_len)?;

        if !rs.stopped {
            self.send_event(StreamEvent::Readable(id));
            return Ok(());
        }

        if closed {
            self.recv.remove(&id).unwrap();
        }

        Ok(())
    }

    /// Process incoming RESET_STREAM frame
    pub fn received_reset(&mut self, frame: ResetStream) -> Result<(), TransportError> {
        let ResetStream {
            id,
            error_code,
            final_offset,
        } = frame;
        self.validate_receive_id(id).inspect_err(|_| {
            debug!("received illegal RESET_STREAM frame");
        })?;
        info!(?frame, "Received RESET_STREAM");

        let rs = self.recv.entry(id).or_default();

        // State transition
        if !rs.reset(error_code, final_offset)? {
            // Redundant reset
            return Ok(());
        }
        let stopped = rs.stopped;
        if stopped {
            // Stopped streams should be disposed immediately on reset
            self.recv.remove(&id);
        } else {
            self.send_event(StreamEvent::Readable(id));
        }

        Ok(())
    }

    /// Process incoming `STOP_SENDING` frame
    pub fn received_stop_sending(&mut self, frame: StopSending) -> Result<(), TransportError> {
        let StopSending { id, error_code } = frame;
        info!(?frame, "Received STOP_SENDING");
        if id.initiator() != self.side {
            if id.dir() == Dir::Uni {
                return Err(TransportError::STREAM_STATE_ERROR(
                    "STOP_SENDING on recv-only stream",
                ));
            }
        } else if id.index() >= self.next[id.dir() as usize] {
            return Err(TransportError::STREAM_STATE_ERROR(
                "STOP_SENDING on unopened stream",
            ));
        }

        let Some(ss) = self.send.get_mut(&id) else {
            warn!(
                "Received stop sending on, Stream {:?}, which is dropped",
                &id
            );
            self.send_event(StreamEvent::Stopped { id, error_code });
            return Ok(());
        };

        if ss.try_stop(error_code) {
            self.send_event(StreamEvent::Stopped { id, error_code });
        }

        Ok(())
    }

    pub(crate) fn reset_acked(&mut self, id: StreamId) {
        match self.send.entry(id) {
            Entry::Vacant(_) => {}
            Entry::Occupied(e) => {
                if let SendState::ResetSent = e.get().state {
                    e.remove_entry();
                }
            }
        }
    }

    /// Whether any stream data is queued, regardless of control frames
    pub(crate) fn can_send_stream_data(&self) -> bool {
        // Reset streams may linger in the pending stream list, but will never produce
        // stream frames
        self.pending
            .iter()
            .any(|stream| self.send.get(&stream.id).is_some_and(|s| !s.is_reset()))
    }

    pub(in crate::connection) fn write_control_frames<B: BufMut>(
        &mut self,
        buf: &mut B,
        pending: &mut Retransmits,
        retransmits: &mut ThinRetransmits,
    ) {
        // RESET_STREAM
        while ResetStream::SIZE_BOUND < buf.remaining_mut() {
            let (id, error_code) = match pending.reset_stream.pop() {
                Some(x) => x,
                None => break,
            };
            let stream = match self.send.get_mut(&id) {
                Some(x) => x,
                None => continue,
            };
            trace!(stream = %id, "RESET_STREAM");
            retransmits
                .get_or_create()
                .reset_stream
                .push((id, error_code));
            ResetStream {
                id,
                error_code,
                final_offset: stream.offset(),
            }
            .encode(buf);
        }

        // STOP_SENDING
        while StopSending::SIZE_BOUND < buf.remaining_mut() {
            let frame = match pending.stop_sending.pop() {
                Some(x) => x,
                None => break,
            };
            // We may need to transmit STOP_SENDING even for streams whose state we have
            // discarded, because we are able to discard local state for stopped
            // streams immediately upon receiving FIN, even if the peer still
            // has arbitrarily large amounts of data to (re)transmit due to loss
            // or unconventional sending strategy. We could fine-tune this
            // a little by dropping the frame if we specifically know the stream's been
            // reset by the peer, but we discard that information as soon as the
            // application consumes it, so it can't be relied upon regardless.
            trace!(stream = %frame.id, "STOP_SENDING");
            frame.encode(buf);
            retransmits.get_or_create().stop_sending.push(frame);
        }
    }

    /// Select sendable stream data, encode STREAM frames into the packet
    /// buffer, and return metadata for the emitted stream frames.
    pub(crate) fn write_stream_frames<B: BufMut>(
        &mut self,
        buf: &mut B,
        fair: bool,
    ) -> Vec<StreamMeta> {
        // TODO -- ATTEMPTING 
        let _ = (buf, fair);
        unimplemented!("implement StreamsState::write_stream_frames");
    }

    /// Process ACK metadata for one STREAM frame and advance send-stream
    /// lifecycle, emitting `Finished` when fully acknowledged.
    pub(crate) fn received_ack_of(&mut self, frame: StreamMeta) {
        let Entry::Occupied(mut entry) = self.send.entry(frame.id) else {
            return;
        };

        let stream = entry.get_mut();
        if stream.is_reset() {
            // We account for outstanding data on reset streams at time of reset
            return;
        }
        let id = frame.id;
        if !stream.ack(frame) {
            // The stream is unfinished or may still need retransmits
            return;
        }

        tracing::info!(
            "Sending part for stream {:?} has finished and is fully acked",
            id
        );
        entry.remove_entry();
        self.send_event(StreamEvent::Finished(id));
    }

    /// Re-queue lost STREAM frame ranges for retransmission and ensure
    /// the stream is scheduled for future sending.
    pub(crate) fn retransmit(&mut self, frame: StreamMeta) {
        // TODO -- ATTEMPTING
        let _ = frame;
        unimplemented!("implement StreamsState::retransmit");
    }
}

/// The [`StreamId`] of a stream with pending data queued, ordered by its
/// priority and recency
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingStream {
    /// The priority of the stream
    // Note that this field should be kept above the `recency` field, in order for the `Ord` derive
    // to be correct (See https://doc.rust-lang.org/stable/std/cmp/trait.Ord.html#derivable)
    priority: i32,
    /// A tie-breaker for streams of the same priority, used to improve fairness
    /// by implementing round-robin scheduling: Larger values are prioritized,
    /// so it is initialised to `u64::MAX`, and when a stream writes data, we
    /// know that it currently has the highest recency value, so it is
    /// deprioritized by setting its recency to 1 less than the previous lowest
    /// recency value, such that all other streams of this priority will get
    /// processed once before we get back round to this one
    recency: u64,
    /// The ID of the stream
    // The way this type is used ensures that every instance has a unique `recency` value, so this
    // field should be kept below the `priority` and `recency` fields, so that it does not
    // interfere with the behaviour of the `Ord` derive
    id: StreamId,
}

/// A queue of streams with pending outgoing data, sorted by priority
#[derive(Debug)]
pub(super) struct PendingStreamsQueue {
    streams: BinaryHeap<PendingStream>,
    /// The next stream to write out. This is `Some` when
    /// `TransportConfig::send_fairness(false)` and writing a stream is
    /// interrupted while the stream still has some pending data. See
    /// `reinsert_pending()`.
    next: Option<PendingStream>,
    /// A monotonically decreasing counter, used to implement round-robin
    /// scheduling for streams of the same priority. Underflowing is not a
    /// practical concern, as it is initialized to u64::MAX and only decremented
    /// by 1 in `push_pending`
    recency: u64,
}

impl PendingStreamsQueue {
    fn new() -> Self {
        Self {
            streams: BinaryHeap::new(),
            next: None,
            recency: u64::MAX,
        }
    }

    /// Reinsert a stream that was pending and still contains unsent data.
    fn reinsert_pending(&mut self, id: StreamId, priority: i32) {
        assert!(self.next.is_none());

        self.next = Some(PendingStream {
            priority,
            recency: self.recency, // the value here doesn't really matter
            id,
        });
    }

    /// Push a pending stream ID with the given priority, queued after any
    /// already-queued streams for the priority
    pub(super) fn push_pending(&mut self, id: StreamId, priority: i32) {
        // Note that in the case where fairness is disabled, if we have a reinserted
        // stream we don't bump it even if priority > next.priority. In order to
        // minimize fragmentation we always try to complete a stream once part of it has
        // been written.

        // As the recency counter is monotonically decreasing, we know that using its
        // value to sort this stream will queue it after all other queued streams of the
        // same priority. This is enough to implement round-robin scheduling for streams
        // that are still pending even after being handled, as in that case they are
        // removed from the `BinaryHeap`, handled, and then immediately reinserted.
        self.recency -= 1;
        self.streams.push(PendingStream {
            priority,
            recency: self.recency,
            id,
        });
    }

    fn pop(&mut self) -> Option<PendingStream> {
        self.next.take().or_else(|| self.streams.pop())
    }

    fn iter(&self) -> impl Iterator<Item = &PendingStream> {
        self.next.iter().chain(self.streams.iter())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum StreamEvent {
    /// A stream likely has data or errors waiting to be read
    Readable(StreamId),
    /// A finished stream has been fully acknowledged or stopped
    Finished(StreamId),
    /// The peer asked us to stop sending on an outgoing stream
    Stopped {
        /// Which stream has been stopped
        id: StreamId,
        /// Error code supplied by the peer
        error_code: u64,
    },
}
