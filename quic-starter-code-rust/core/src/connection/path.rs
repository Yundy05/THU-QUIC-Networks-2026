use std::{collections::VecDeque, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use mzquic_proto::SpaceId;
use strum::VariantArray;
use tokio::{
    net::UdpSocket,
    time::{Duration, Instant},
};
use tracing::warn;

use super::space::{PacketSpace, SentPacket};
use crate::INITIAL_MTU;

const MIN_RTT: Duration = Duration::from_millis(333);

/// Packets that are scheduled to be sent
#[derive(Debug)]
pub(super) struct ScheduledPacket {
    pub(super) packet: Bytes,
    pub(super) meta_info: SentPacket,
}

#[derive(Debug)]
pub(super) struct PostSentInfo {
    pub(super) ack_eliciting: bool,
    pub(super) send_time: Instant,
}

/// Description of a particular network path
#[derive(Debug)]
pub(super) struct NetworkPath {
    pub(super) remote: SocketAddr,
    pub(super) socket: Arc<UdpSocket>,

    pub(super) sending_queue: VecDeque<ScheduledPacket>,

    pub(super) rtt: RttEstimator,
    pub(super) congestion: NewReno,
    pub(super) in_flight: InFlight,
    /// Number of the first packet sent on this path
    ///
    /// Used to determine whether a packet was sent on an earlier path.
    /// Insufficient to determine if a packet was sent on a later path.
    first_packet: Option<u64>,
}

impl NetworkPath {
    pub(super) fn new(remote: SocketAddr, socket: Arc<UdpSocket>, now: Instant) -> Self {
        Self {
            remote,
            socket,
            sending_queue: VecDeque::new(),
            rtt: RttEstimator::new(MIN_RTT),
            congestion: NewReno::new(now),
            in_flight: InFlight::default(),
            first_packet: None,
        }
    }

    async fn send_datagram(&self, data: impl AsRef<[u8]>) -> std::io::Result<Instant> {
        let data = data.as_ref();
        self.socket
            .send_to(data, self.remote)
            .await
            .inspect_err(|e| {
                warn!("Failed to send packet to {}: {e}", self.remote);
            })
            .map(|_| Instant::now())
    }

    /// Send the first packet at the head of `sending_queue`
    pub(super) async fn send_one(
        &mut self,
        spaces: &mut [PacketSpace; SpaceId::VARIANTS.len()],
    ) -> Option<PostSentInfo> {
        let ScheduledPacket {
            packet,
            mut meta_info,
        } = self.sending_queue.pop_front()?;

        let send_time = self.send_datagram(packet).await.unwrap();
        meta_info.time_sent = send_time;

        let info = PostSentInfo {
            ack_eliciting: meta_info.ack_eliciting,
            send_time,
        };

        self.in_flight.insert(&meta_info);
        if self.first_packet.is_none() {
            self.first_packet = Some(meta_info.number);
        }

        let space = &mut spaces[meta_info.space_id];
        self.in_flight.bytes = self.in_flight.bytes.saturating_sub(space.sent(meta_info));

        Some(info)
    }

    /// Remove `packet` with number `pn` from this path's congestion control
    /// counters, or return `false` if `pn` was sent before this path was
    /// established.
    pub(super) fn remove_in_flight(&mut self, pn: u64, packet: &SentPacket) -> bool {
        if self.first_packet.is_none_or(|first| first > pn) {
            return false;
        }
        self.in_flight.remove(packet);
        true
    }
}

/// RTT estimation for a particular network path
#[derive(Debug, Clone)]
pub struct RttEstimator {
    /// The most recent RTT measurement made when receiving an ack for a
    /// previously unacked packet
    latest: Duration,
    /// The smoothed RTT of the connection, computed as described in RFC6298
    smoothed: Option<Duration>,
    /// The RTT variance, computed as described in RFC6298
    var: Duration,
    /// The minimum RTT seen in the connection, ignoring ack delay.
    min: Duration,
}

impl RttEstimator {
    fn new(initial_rtt: Duration) -> Self {
        Self {
            latest: initial_rtt,
            smoothed: None,
            var: initial_rtt / 2,
            min: initial_rtt,
        }
    }

    /// The current best RTT estimation.
    pub fn get(&self) -> Duration {
        self.smoothed.unwrap_or(self.latest)
    }

    /// Conservative estimate of RTT
    ///
    /// Takes the maximum of smoothed and latest RTT, as recommended
    /// in 6.1.2 of the recovery spec (draft 29).
    pub fn conservative(&self) -> Duration {
        self.get().max(self.latest)
    }

    /// Update latest/smoothed/min RTT and variance from a new RTT sample and ACK delay.
    pub(super) fn update(&mut self, ack_delay: Duration, rtt: Duration) {
        // TODO -- ATTEMPTING DONE 
        self.latest = rtt;

        // Update min RTT (always ignores ack_delay per RFC 9002 §5.2)
        self.min = self.min.min(rtt);

        // Adjust for ack delay, but never go below min_rtt
        let adjusted_rtt = if rtt > self.min + ack_delay {
            rtt - ack_delay
        } else {
            rtt
        };

        match self.smoothed {
            None => {
                // First RTT sample (RFC 9002 §5.3 first measurement)
                self.smoothed = Some(adjusted_rtt);
                self.var = adjusted_rtt / 2;
            }
            Some(smoothed) => {
                // EWMA update (RFC 9002 §5.3)
                // rttvar = 3/4 * rttvar + 1/4 * |smoothed_rtt - adjusted_rtt|
                let diff = if smoothed > adjusted_rtt {
                    smoothed - adjusted_rtt
                } else {
                    adjusted_rtt - smoothed
                };
                self.var = (self.var * 3 + diff) / 4;

                // smoothed_rtt = 7/8 * smoothed_rtt + 1/8 * adjusted_rtt
                self.smoothed = Some((smoothed * 7 + adjusted_rtt) / 8);
            }
        }
        // unimplemented!("implement RttEstimator::update");
    }
}

/// Summary statistics of packets that have been sent on a particular path, but
/// which have not yet been acked or deemed lost
#[derive(Debug, Default)]
pub(super) struct InFlight {
    /// Sum of the sizes of all sent packets considered "in flight" by
    /// congestion control
    ///
    /// The size does not include IP or UDP overhead. Packets only containing
    /// ACK frames do not count towards this to ensure congestion control
    /// does not impede congestion feedback.
    pub(super) bytes: u64,
    /// Number of packets in flight containing frames other than ACK and PADDING
    ///
    /// This can be 0 even when bytes is not 0 because PADDING frames cause a
    /// packet to be considered "in flight" by congestion control. However,
    /// if this is nonzero, bytes will always also be nonzero.
    pub(super) ack_eliciting: u64,
}

impl InFlight {
    fn insert(&mut self, packet: &SentPacket) {
        self.bytes += u64::from(packet.size);
        self.ack_eliciting += u64::from(packet.ack_eliciting);
    }

    /// Update counters to account for a packet becoming acknowledged, lost, or
    /// abandoned
    fn remove(&mut self, packet: &SentPacket) {
        self.bytes -= u64::from(packet.size);
        self.ack_eliciting -= u64::from(packet.ack_eliciting);
    }
}

/// A simple, standard congestion controller
#[derive(Debug, Clone)]
pub struct NewReno {
    /// Maximum number of bytes in flight that may be sent.
    window: u64,
    /// Slow start threshold in bytes. When the congestion window is below
    /// ssthresh, the mode is slow start and the window grows by the number of
    /// bytes acknowledged.
    ssthresh: u64,
    /// The time when QUIC first detects a loss, causing it to enter recovery.
    /// When a packet sent after this time is acknowledged, QUIC exits recovery.
    recovery_start_time: Instant,
    /// Bytes which had been acked by the peer since leaving slow start
    bytes_acked: u64,
}

impl NewReno {
    fn new(now: Instant) -> Self {
        Self {
            window: 10 * INITIAL_MTU as u64,
            ssthresh: u64::MAX,
            recovery_start_time: now,
            bytes_acked: 0,
        }
    }

    /// Increase congestion window on ACK according to slow-start and
    /// congestion-avoidance rules.
    pub(super) fn on_ack(&mut self, sent: Instant, bytes: u64, app_limited: bool) {
        // TODO
        let _ = (sent, bytes, app_limited);
        unimplemented!("implement NewReno::on_ack");
    }

    /// Apply congestion response on loss and update recovery and threshold state.
    pub(super) fn on_congestion_event(&mut self, now: Instant, sent: Instant) {
        // TODO
        let _ = (now, sent);
        unimplemented!("implement NewReno::on_congestion_event");
    }

    pub(super) fn window(&self) -> u64 {
        self.window
    }
}
