use std::{collections::BTreeMap, mem, ops::Bound};

use mzquic_proto::{ArrayRangeSet, SpaceId, StopSending, StreamId, StreamMeta};
use tokio::time::{Duration, Instant};

#[derive(Debug)]
pub struct PacketSpace {
    pub(super) hi: bool,
    pub(super) dedup: Dedup,
    /// Highest received packet number
    pub(super) rx_packet: u64,

    /// Data to send
    pub(super) pending: Retransmits,
    /// Packet numbers to acknowledge
    pub(super) pending_acks: PendingAcks,

    /// The packet number of the next packet that will be sent, if any.
    pub(super) next_packet_number: u64,
    /// The largest packet number the remote peer acknowledged in an ACK frame.
    pub(super) largest_acked_packet: Option<u64>,
    pub(super) largest_acked_packet_sent: Instant,
    /// The highest-numbered ACK-eliciting packet we've sent
    pub(super) largest_ack_eliciting_sent: u64,
    /// Number of packets in `sent_packets` with numbers above
    /// `largest_ack_eliciting_sent`
    pub(super) unacked_non_ack_eliciting_tail: u64,
    /// Transmitted but not acked
    // We use a BTreeMap here so we can efficiently query by range on ACK and for loss detection
    pub(super) sent_packets: BTreeMap<u64, SentPacket>,

    /// The time at which the earliest sent packet in this space will be
    /// considered lost based on exceeding the reordering window in time. Only
    /// set for packets numbered prior to a packet that has been acknowledged.
    pub(super) loss_time: Option<Instant>,
    pub(super) ping_pending: bool,
    pub(super) immediate_ack_pending: bool,
    /// Number of congestion control "in flight" bytes
    pub(super) in_flight: u64,
}

impl PacketSpace {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            hi: false,
            dedup: Dedup::new(),
            rx_packet: 0,

            pending: Retransmits::default(),
            pending_acks: PendingAcks::new(),

            next_packet_number: 0,
            largest_acked_packet: None,
            largest_acked_packet_sent: now,
            largest_ack_eliciting_sent: 0,
            unacked_non_ack_eliciting_tail: 0,
            sent_packets: BTreeMap::new(),

            loss_time: None,
            ping_pending: false,
            immediate_ack_pending: false,
            in_flight: 0,
        }
    }

    /// Get the next outgoing packet number in this space
    /// Handle packet-number overflow gracefully instead of using assertion panic.
    pub(super) fn get_tx_number(&mut self) -> u64 {
        assert!(self.next_packet_number < 2u64.pow(62));
        let x = self.next_packet_number;
        self.next_packet_number += 1;
        x
    }

    pub(super) fn can_send(&self) -> SendableFrames {
        let acks = self.pending_acks.can_send();
        let other = !self.pending.is_empty() || self.ping_pending || self.immediate_ack_pending;

        SendableFrames { acks, other }
    }

    /// Stop tracking sent packet `number`, and return what we knew about it
    /// Remove one sent packet by number, update in-flight accounting, and
    /// return its transmission metadata.
    pub(super) fn take(&mut self, number: u64) -> Option<SentPacket> {
        // TODO
        let _ = number;
        unimplemented!("implement PacketSpace::take");
    }

    /// Returns the number of bytes to *remove* from the connection's in-flight
    /// count
    /// Record one newly sent packet and update sent-packet tracking and
    /// ack-eliciting in-flight counters.
    pub(super) fn sent(&mut self, packet: SentPacket) -> u64 {
        // TODO -- ATTEMPTING DONE
        let number = packet.number;
        let size = u64::from(packet.size);

        if packet.ack_eliciting {
            self.largest_ack_eliciting_sent = number;
            self.unacked_non_ack_eliciting_tail = 0;
        } else if number > self.largest_ack_eliciting_sent {
            self.unacked_non_ack_eliciting_tail += 1;
        }

        self.in_flight += size;
        self.sent_packets.insert(number, packet);

        0
        // unimplemented!("implement PacketSpace::sent");
    }
}

/// Represents one or more packets subject to retransmission
#[derive(Debug, Clone)]
pub(super) struct SentPacket {
    /// The time the packet was sent.
    pub(super) time_sent: Instant,
    /// The number of bytes sent in the packet, not including UDP or IP
    /// overhead, but including QUIC framing overhead. Zero if this packet is
    /// not counted towards congestion control, i.e. not an "in flight" packet.
    pub(super) size: u16,
    /// Whether an acknowledgement is expected directly in response to this
    /// packet.
    pub(super) ack_eliciting: bool,
    /// The largest packet number acknowledged by this packet
    pub(super) largest_acked: Option<u64>,
    /// Data which needs to be retransmitted in case the packet is lost. The
    /// data is boxed to minimize `SentPacket` size for the typical case of
    /// packets only containing ACKs and STREAM frames.
    pub(super) retransmits: ThinRetransmits,
    /// Metadata for stream frames in a packet
    /// The actual application data is stored with the stream state.
    pub(super) stream_frames: Vec<StreamMeta>,
    /// Packet number
    pub(super) number: u64,
    /// Space id
    pub(super) space_id: SpaceId,
}

/// Retransmittable data queue
#[derive(Debug, Default, Clone)]
pub struct Retransmits {
    /// Our special placeholder for a hello message
    pub(super) hello: bool,
    pub(super) reset_stream: Vec<(StreamId, u64)>,
    pub(super) stop_sending: Vec<StopSending>,
    pub(super) ack_frequency: bool,
    pub(super) handshake_done: bool,
}

impl Retransmits {
    pub(super) fn is_empty(&self) -> bool {
        !self.hello
            && self.reset_stream.is_empty()
            && self.stop_sending.is_empty()
            && !self.ack_frequency
            && !self.handshake_done
    }
}

impl ::std::ops::BitOrAssign for Retransmits {
    fn bitor_assign(&mut self, rhs: Self) {
        self.hello |= rhs.hello;
        self.reset_stream.extend_from_slice(&rhs.reset_stream);
        self.stop_sending.extend_from_slice(&rhs.stop_sending);
        self.ack_frequency |= rhs.ack_frequency;
        self.handshake_done |= rhs.handshake_done;
    }
}

impl ::std::iter::FromIterator<Self> for Retransmits {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Self>,
    {
        let mut result = Self::default();
        for packet in iter {
            result |= packet;
        }
        result
    }
}

/// A variant of `Retransmits` which only allocates storage when required
#[derive(Debug, Default, Clone)]
pub(super) struct ThinRetransmits {
    retransmits: Option<Box<Retransmits>>,
}

impl ThinRetransmits {
    /// Returns `true` if no retransmits are necessary
    pub(super) fn is_empty(&self) -> bool {
        match &self.retransmits {
            Some(retransmits) => retransmits.is_empty(),
            None => true,
        }
    }

    /// Returns a reference to the retransmits stored in this box
    pub(super) fn get(&self) -> Option<&Retransmits> {
        self.retransmits.as_deref()
    }

    /// Returns a mutable reference to the stored retransmits
    ///
    /// This function will allocate a backing storage if required.
    pub(super) fn get_or_create(&mut self) -> &mut Retransmits {
        if self.retransmits.is_none() {
            self.retransmits = Some(Box::default());
        }
        self.retransmits.as_deref_mut().unwrap()
    }
}

impl ::std::ops::BitOrAssign<ThinRetransmits> for Retransmits {
    fn bitor_assign(&mut self, rhs: ThinRetransmits) {
        if let Some(retransmits) = rhs.retransmits {
            self.bitor_assign(*retransmits)
        }
    }
}

/// RFC4303-style sliding window packet number deduplicator.
///
/// A contiguous bitfield, where each bit corresponds to a packet number and the
/// rightmost bit is always set. A set bit represents a packet that has been
/// successfully authenticated. Bits left of the window are assumed to be set.
///
/// ```text
/// ...xxxxxxxxx 1 0
///     ^        ^ ^
/// window highest next
/// ```
#[derive(Debug)]
pub(super) struct Dedup {
    window: Window,
    /// Lowest packet number higher than all yet authenticated.
    next: u64,
}

/// Inner bitfield type.
///
/// Because QUIC never reuses packet numbers, this only needs to be large enough
/// to deal with packets that are reordered but still delivered in a timely
/// manner.
type Window = u128;

/// Number of packets tracked by `Dedup`.
const WINDOW_SIZE: u64 = 1 + size_of::<Window>() as u64 * 8;

impl Dedup {
    /// Construct an empty window positioned at the start.
    pub(super) fn new() -> Self {
        Self { window: 0, next: 0 }
    }

    /// Highest packet number authenticated.
    fn highest(&self) -> u64 {
        self.next - 1
    }

    /// Record a newly authenticated packet number.
    ///
    /// Returns whether the packet might be a duplicate.
    /// Update the dedup sliding window and return whether the packet is a duplicate.
    pub(super) fn insert(&mut self, packet: u64) -> bool {
        if let Some(diff) = packet.checked_sub(self.next) {
            // Right of window
            self.window = ((self.window << 1) | 1)
                .checked_shl(diff.min(u64::from(u32::MAX)) as u32)
                .unwrap_or(0);
            self.next = packet + 1;
            false
        } else if self.highest() - packet < WINDOW_SIZE {
            // Within window
            if let Some(bit) = (self.highest() - packet).checked_sub(1) {
                // < highest
                let mask = 1 << bit;
                let duplicate = self.window & mask != 0;
                self.window |= mask;
                duplicate
            } else {
                // == highest
                true
            }
        } else {
            // Left of window
            true
        }
    }

    /// Returns the packet number of the smallest packet missing between the
    /// provided interval
    ///
    /// If there are no missing packets, returns `None`
    /// Return the smallest missing packet number in the interval, or `None`
    /// when the interval is complete.
    fn smallest_missing_in_interval(&self, lower_bound: u64, upper_bound: u64) -> Option<u64> {
        // TODO
        let _ = (lower_bound, upper_bound);
        unimplemented!("implement Dedup::smallest_missing_in_interval");
    }

    /// Returns true if there are any missing packets between the provided
    /// interval
    ///
    /// The provided packet numbers must have been received before calling this
    /// function
    /// Return whether any packet number is missing in the interval.
    fn missing_in_interval(&self, lower_bound: u64, upper_bound: u64) -> bool {
        // TODO
        let _ = (lower_bound, upper_bound);
        unimplemented!("implement Dedup::missing_in_interval");
    }
}

/// Indicates which data is available for sending
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct SendableFrames {
    pub(super) acks: bool,
    pub(super) other: bool,
}

impl SendableFrames {
    /// Whether no data is sendable
    pub(super) fn is_empty(&self) -> bool {
        !self.acks && !self.other
    }
}

#[derive(Debug)]
pub(super) struct PendingAcks {
    /// Whether we should send an ACK immediately, even if that means sending an
    /// ACK-only packet
    ///
    /// When `immediate_ack_required` is false, the normal behavior is to send
    /// ACK frames only when there is other data to send, or when the delayed ACK timeout expires.
    immediate_ack_required: bool,
    /// The number of ack-eliciting packets received since the last ACK frame
    /// was sent
    ///
    /// Once the count _exceeds_ `ack_eliciting_threshold`, an immediate ACK is
    /// required
    ack_eliciting_since_last_ack_sent: u64,
    non_ack_eliciting_since_last_ack_sent: u64,
    ack_eliciting_threshold: u64,
    /// The reordering threshold, controlling how we respond to out-of-order
    /// ack-eliciting packets
    ///
    /// Different values enable different behavior:
    ///
    /// * `0`: no special action is taken
    /// * `1`: an ACK is immediately sent if it is out-of-order according to RFC
    ///   9000
    /// * `>1`: an ACK is immediately sent if it is out-of-order according to
    ///   the ACK frequency draft
    reordering_threshold: u64,
    /// The earliest ack-eliciting packet since the last ACK was sent, used to
    /// calculate when the delayed ACK deadline elapses
    earliest_ack_eliciting_since_last_ack_sent: Option<Instant>,
    /// The packet number ranges of ack-eliciting packets the peer hasn't
    /// confirmed receipt of ACKs for
    ranges: ArrayRangeSet,
    /// The packet with the largest packet number, and the time upon which it
    /// was received (used to calculate ACK delay in
    /// [`PendingAcks::ack_delay`])
    largest_packet: Option<(u64, Instant)>,
    /// The ack-eliciting packet we have received with the largest packet number
    largest_ack_eliciting_packet: Option<u64>,
    /// The largest acknowledged packet number sent in an ACK frame
    largest_acked: Option<u64>,
}

impl PendingAcks {
    fn new() -> Self {
        Self {
            immediate_ack_required: false,
            ack_eliciting_since_last_ack_sent: 0,
            non_ack_eliciting_since_last_ack_sent: 0,
            ack_eliciting_threshold: 1,
            reordering_threshold: 1,
            earliest_ack_eliciting_since_last_ack_sent: None,
            ranges: ArrayRangeSet::default(),
            largest_packet: None,
            largest_ack_eliciting_packet: None,
            largest_acked: None,
        }
    }

    pub(super) fn set_immediate_ack_required(&mut self) {
        self.immediate_ack_required = true;
    }

    pub(super) fn on_ack_delay_timeout(&mut self) {
        self.immediate_ack_required = self.ack_eliciting_since_last_ack_sent > 0;
    }

    /// Whether any ACK frames can be sent
    pub(super) fn can_send(&self) -> bool {
        self.immediate_ack_required && !self.ranges.is_empty()
    }

    /// Returns the delay since the packet with the largest packet number was
    /// received
    pub(super) fn ack_delay(&self, now: Instant) -> Duration {
        self.largest_packet
            .map_or(Duration::default(), |(_, received)| now - received)
    }

    /// Handle receipt of a new packet
    ///
    /// Returns true if a delayed ACK timeout should be armed
    /// Update ACK-generation state after receiving one packet and decide
    /// whether a delayed ACK timeout should be armed.
    pub(super) fn packet_received(
        &mut self,
        now: Instant,
        packet_number: u64,
        ack_eliciting: bool,
        dedup: &Dedup,
    ) -> bool {
        // TODO
        let _ = (now, packet_number, ack_eliciting, dedup);
        unimplemented!("implement PendingAcks::packet_received");
    }

    /// Determine whether a packet is out of order, and whether that requires
    /// immediate ACK transmission under current reordering policy.
    fn is_out_of_order(
        &self,
        packet_number: u64,
        prev_largest_ack_eliciting: u64,
        dedup: &Dedup,
    ) -> bool {
        // TODO
        let _ = (packet_number, prev_largest_ack_eliciting, dedup);
        unimplemented!("implement PendingAcks::is_out_of_order");
    }

    /// Should be called whenever ACKs have been sent
    ///
    /// This will suppress sending further ACKs until additional ACK eliciting
    /// frames arrive
    /// Reset ACK-related counters after ACK frames are sent.
    pub(super) fn acks_sent(&mut self) {
        // TODO
        unimplemented!("implement PendingAcks::acks_sent");
    }

    /// Insert one packet that needs to be acknowledged
    /// Insert one packet number into pending ACK ranges and refresh largest-packet metadata.
    pub(super) fn insert_one(&mut self, packet: u64, now: Instant) {
        // TODO
        let _ = (packet, now);
        unimplemented!("implement PendingAcks::insert_one");
    }

    /// Remove ACKs of packets numbered at or below `max` from the set of
    /// pending ACKs
    /// Drop stale pending ACK ranges at or below the specified packet number.
    pub(super) fn subtract_below(&mut self, max: u64) {
        // TODO
        let _ = max;
        unimplemented!("implement PendingAcks::subtract_below");
    }

    /// Returns the set of currently pending ACK ranges
    pub(super) fn ranges(&self) -> &ArrayRangeSet {
        &self.ranges
    }

    // TODO: There may be some corner-cases to consider:
    // e.g., Queue an ACK if a significant number of non-ACK-eliciting packets have
    // not yet been acknowledged. This helps the peer perform timely loss detection.
}
