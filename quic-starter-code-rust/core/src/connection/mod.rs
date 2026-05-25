use std::{mem, net::SocketAddr, str::SplitTerminator, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};
use mzquic_proto::{
    Ack, ApplicationClose, ArrayRangeSet, BufMutExt, ConnectionClose, ConnectionId, Crypto, Dir,
    Frame, FrameParser, FrameType, Header, InitialHeader, InitialPacket, LongType, Packet,
    PacketNumber, Side, SpaceId, StreamId, StreamMeta, TransportClose, TransportError,
    TransportErrorCode,
};
use path::NetworkPath;
use strum::VariantArray;
use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinHandle,
    time::{Duration, Instant},
};
use tracing::{debug, error, info, trace, trace_span};

use crate::{FAKE_CID, INITIAL_MTU, TransportHandler, connection::path::ScheduledPacket};

mod space;
use space::{PacketSpace, SendableFrames, SentPacket, ThinRetransmits};

mod stream;
pub use stream::*;

mod timer;
use timer::{Timer, TimerTable};

mod path;

pub type ConnectionHandle = usize;

const ACK_DELAY_EXP: usize = 3;
const TIMER_GRANULARITY: Duration = Duration::from_millis(1);

pub struct ConnectionTask {
    pub data_tx: UnboundedSender<(Instant, BytesMut)>,
    task: JoinHandle<()>,
    handle: ConnectionHandle,
    remote: SocketAddr,
}

impl ConnectionTask {
    pub(crate) fn new(
        handle: ConnectionHandle,
        remote: SocketAddr,
        socket: Arc<UdpSocket>,
        transport_handler: TransportHandler,
        side: Side,
    ) -> Self {
        let connection = Connection::new(remote, socket, side);
        Self::with_connection(connection, handle, remote, transport_handler)
    }

    pub(crate) fn with_connection(
        connection: Connection,
        handle: ConnectionHandle,
        remote: SocketAddr,
        transport_handler: TransportHandler,
    ) -> Self {
        let (data_tx, data_rx) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(run_connection(connection, data_rx, transport_handler));
        Self {
            data_tx,
            task,
            handle,
            remote,
        }
    }

    pub(crate) fn handle(&self) -> ConnectionHandle {
        self.handle
    }

    pub(crate) fn remote_addr(&self) -> SocketAddr {
        self.remote
    }

    pub(crate) fn is_alive(&self) -> bool {
        !self.task.is_finished()
    }
}

async fn run_connection(
    mut connection: Connection,
    mut data_rx: UnboundedReceiver<(Instant, BytesMut)>,
    mut transport_handler: TransportHandler,
) {
    while !matches!(connection.state, State::Drained) {
        // Schedule packets
        // NOTE: The burst-yield strategy below is heuristic and has no universally
        // optimal parameter choice. Different workloads, network
        // characteristics, CPU budgets, and runtime configurations may all
        // prefer different burst depths and yielding frequencies. You are encouraged to
        // treat the current thresholds only as a starting point and adjust them
        // experimentally.
        //
        // If bursts are too small, the sender may underutilize available pipeline
        // capacity, which can reduce throughput on high-capacity or very
        // low-latency links, but may improve scheduling fairness and reduce
        // runtime pressure.
        //
        // If bursts are too large, the sender may maximize short-term throughput but
        // risks longer CPU occupancy without yielding, which can increase
        // scheduling latency, runtime contention, and tail-delay variability.
        //
        // The values used in the implementation are therefore arbitrary design choices
        // rather than empirically proven optima, and alternative combinations
        // should be explored if performance characteristics need tuning.
        let now = Instant::now();
        let mut spins = 0;
        while connection.poll_transmit(now) {
            spins += 1;
            if spins % 32 == 0 {
                tokio::task::yield_now().await;
            }
            if spins >= 128 {
                break;
            }
        }

        // Send scheduled packets as soon as possible
        while let Some(post_send) = connection.path.send_one(&mut connection.spaces).await {
            if post_send.ack_eliciting {
                // TODO -- ATTEMPTING
                // You may need to handle idle timeout here.
                connection.set_loss_detection_timer(now);
            }
        }

        tokio::select! {
            biased;
            Some(event) = connection.events_rx.recv() => {
                transport_handler(&mut connection, event);
            },
            timer = &mut connection.timers => {
                connection.handle_timeout(timer, Instant::now());
            },
            Some((now, data)) = data_rx.recv() => {
                connection.handle_data(now, data);
            },
            _ = tokio::time::sleep(Duration::from_millis(1000))=>{}
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    side: Side,
    pub(crate) state: State,
    path: NetworkPath,
    /// Packet number spaces: initial, handshake, 1-RTT
    spaces: [PacketSpace; 3],
    highest_space: SpaceId,
    timers: TimerTable,
    streams: StreamsState,
    events_rx: mpsc::UnboundedReceiver<Event>,
    events_tx: mpsc::UnboundedSender<Event>,
    pending_close: bool,
    /// Whether the last `poll_transmit` call yielded no data because there was
    /// no outgoing application data.
    app_limited: bool,
}

impl Connection {
    pub fn new(remote: SocketAddr, socket: Arc<UdpSocket>, side: Side) -> Self {
        let now = Instant::now();

        let (events_tx, events_rx) = unbounded_channel();
        let mut this = Self {
            side,
            state: State::default(),
            path: NetworkPath::new(remote, socket, now),

            spaces: [
                PacketSpace::new(now),
                PacketSpace::new(now),
                PacketSpace::new(now),
            ],
            highest_space: SpaceId::Initial,

            timers: TimerTable::default(),
            streams: StreamsState::new(side, events_tx.clone()),
            events_rx,
            events_tx,
            pending_close: false,
            app_limited: false,
        };

        if side.is_client() {
            // Kick off the connection
            this.spaces[SpaceId::Initial].pending.hello = true;
        }

        this
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.path.remote
    }

    fn error(&mut self, error: ConnectionError) {
        self.events_tx.send(Event::ConnectionLost(error)).ok();
    }

    fn discard_space(&mut self, now: Instant, space_id: SpaceId) {
        debug_assert!(space_id != SpaceId::Data);
        debug!("discarding {:?}", space_id);
        let space = &mut self.spaces[space_id];
        space.loss_time = None;
        space.in_flight = 0;
        let sent_packets = mem::take(&mut space.sent_packets);
        for (pn, packet) in sent_packets.into_iter() {
            self.path.remove_in_flight(pn, &packet);
        }
        self.set_loss_detection_timer(now)
    }

    pub(crate) fn handle_first_packet(
        &mut self,
        now: Instant,
        packet: InitialPacket,
    ) -> Result<(), ConnectionError> {
        let number = packet.header.number.expand(0);

        self.on_packet_authenticated(now, SpaceId::Initial, Some(number));
        self.process_packet(now, Some(number), packet.into())?;
        Ok(())
    }

    /// Handle a datagram received from the socket
    ///
    /// Returns true if the connection has been drained
    pub(crate) fn handle_data(&mut self, now: Instant, mut data: BytesMut) {
        let packet = match Header::decode(&mut data, FAKE_CID.len(), crate::SUPPORTED_VERSIONS) {
            Ok(header) => Packet {
                header,
                payload: data.freeze(),
            },
            Err(e) => {
                error!("Error decoding header: {e}");
                return;
            }
        };

        let was_closed = self.state.is_closed();
        let was_drained = self.state.is_drained();

        let space = &mut self.spaces[packet.header.space()];

        let number = packet
            .header
            .number()
            .map(|n| n.expand(space.rx_packet + 1));

        let _span = match number {
            Some(pn) => trace_span!("recv", space = ?packet.header.space(), pn),
            None => trace_span!("recv", space = ?packet.header.space()),
        }
        .entered();

        let is_duplicate = |n| space.dedup.insert(n);
        if number.is_some_and(is_duplicate) {
            debug!("discarding possible duplicate packet");
            return;
        } else if self.state.is_handshake() && packet.header.is_short() {
            trace!("dropping short packet during handshake");
            return;
        }

        if !self.state.is_closed() {
            self.on_packet_authenticated(now, packet.header.space(), number);
        }
        let res = self.process_packet(now, number, packet);

        // State transitions for error cases
        if let Err(conn_err) = res {
            self.error(conn_err.clone());
            self.state = match conn_err {
                ConnectionError::ApplicationClosed(reason) => State::closed(reason),
                ConnectionError::TransportClosed(reason) => State::closed(reason),
                ConnectionError::Reset => State::Drained,
                ConnectionError::TransportError(err) => {
                    debug!("closing connection due to transport error: {}", err);
                    State::closed(err)
                }
                ConnectionError::VersionMismatch => State::Draining,
            };
        }

        if !was_closed && self.state.is_closed() {
            self.close_common();
            if !self.state.is_drained() {
                self.set_close_timer(now);
            }
        }
        if !was_drained && self.state.is_drained() {
            // Close timer may have been started previously, e.g. if we sent a close and got
            // a stateless reset in response
            self.timers.stop(Timer::Close);
        }
    }

    fn on_packet_authenticated(&mut self, now: Instant, space_id: SpaceId, number: Option<u64>) {
        let Some(number) = number else {
            return;
        };

        if self.side.is_server()
            && self.spaces[SpaceId::Initial].hi
            && space_id == SpaceId::Handshake
        {
            // A server stops sending and processing Initial packets when it receives its
            // first Handshake packet.
            self.discard_space(now, SpaceId::Initial);
        }
        let space = &mut self.spaces[space_id];
        let ack_eliciting = true; // treat all received packets as ack-eliciting conservatively
        let arm_timer =
            space
                .pending_acks
                .packet_received(now, number, ack_eliciting, &space.dedup);
        if number >= space.rx_packet {
            space.rx_packet = number;
        }
        if arm_timer {
            self.timers
                .set(Timer::MaxAckDelay, now + Duration::from_millis(25));
        }
    }

    fn loss_time_and_space(&self) -> Option<(Instant, SpaceId)> {
        SpaceId::VARIANTS
            .iter()
            .filter_map(|&id| Some((self.spaces[id].loss_time?, id)))
            .min_by_key(|&(time, _)| time)
    }

    fn set_loss_detection_timer(&mut self, _now: Instant) {
        if self.state.is_closed() {
            // No loss detection takes place on closed connections, and `close_common`
            // already stopped time timer. Ensure we don't restart it
            // inadvertently, e.g. in response to a reordered packet being
            // handled by state-insensitive code.
            return;
        }

        if let Some((loss_time, _)) = self.loss_time_and_space() {
            // Time threshold loss detection.
            self.timers.set(Timer::LossDetection, loss_time);
            return;
        }

        // TODO -- ATTEMPTING
        // Correctly handle this.
        // 1) Are there any ACK-eliciting packets still inflight in this path?
        // 2) If PTO is implemented, remember to
        // Arm a PTO timer if there are ACK-eliciting packets in flight.
        let has_in_flight = SpaceId::VARIANTS
            .iter()
            .any(|&id| self.spaces[id].in_flight > 0);

        if has_in_flight {
            let pto = self.path.rtt.conservative() * 3 + TIMER_GRANULARITY;
            let earliest_sent = SpaceId::VARIANTS
                .iter()
                .filter_map(|&id| self.spaces[id].sent_packets.values().next())
                .map(|p| p.time_sent)
                .min();
            if let Some(sent) = earliest_sent {
                self.timers.set(Timer::LossDetection, sent + pto);
                return;
            }
        }

        self.timers.stop(Timer::LossDetection);
    }

    /// Detect lost packets in the selected packet number space, queue retransmissions,
    /// and update congestion-control and timer state.
    fn detect_lost_packets(&mut self, now: Instant, space_id: SpaceId) {
        //TODO: -- ATTEMPTING DONE
        // 1) Determine `loss_delay`, or how long should we wait for the acks before thinking
        // the packets seem to be lost after their send time.
        // 2) Collect the lost packets in each Packet Number Space.
        // 3) Retransmit the **Frames**, not the packets, by calling `retransmit` on streams, and
        //    remove the retransmitted packet from the in flight statistics of the path.
        // 4) You may need to update the `pending` field for each space.
        let largest_acked = match self.spaces[space_id].largest_acked_packet {
            Some(pn) => pn,
            None => {
                self.spaces[space_id].loss_time = None;
                return;
            }
        };

        let loss_delay = self.path.rtt.conservative() * 9 / 8;
        let lost_send_before = now
            .checked_sub(loss_delay.max(TIMER_GRANULARITY))
            .unwrap_or(now);

        let mut lost_packets = Vec::new();
        let mut next_loss_time = None;

        {
            let space = &self.spaces[space_id];

            for (&pn, packet) in space.sent_packets.iter() {
                if pn > largest_acked {
                    break;
                }

                let lost = packet.time_sent <= lost_send_before;

                if lost {
                    lost_packets.push((pn, packet.clone()));
                } else {
                    let candidate = packet.time_sent + loss_delay.max(TIMER_GRANULARITY);
                    next_loss_time = match next_loss_time {
                        None => Some(candidate),
                        Some(prev) => Some(prev.min(candidate)),
                    };
                }
            }
        }

        self.spaces[space_id].loss_time = next_loss_time;

        if lost_packets.is_empty() {
            return;
        }

        for (pn, packet) in lost_packets {
            let removed_from_path = self.path.remove_in_flight(pn, &packet);

            if removed_from_path && packet.ack_eliciting {
                self.path
                    .congestion
                    .on_congestion_event(now, packet.time_sent);
            }

            for frame in packet.stream_frames.iter().cloned() {
                self.streams.retransmit(frame);
            }

            if let Some(retransmits) = packet.retransmits.get() {
                self.spaces[space_id].pending |= retransmits.clone();
            }

            let _ = self.spaces[space_id].take(pn);
        }

        self.set_loss_detection_timer(now);
    }

    /// Process an ACK frame, confirm sent packets, and update RTT, congestion,
    /// retransmission tracking, and stream completion state.
    fn on_ack_received(
        &mut self,
        now: Instant,
        space_id: SpaceId,
        ack: Ack,
    ) -> Result<(), TransportError> {
        // TODO -- ATTEMPTING DONE
        // Reject ACKs that acknowledge packets we have not sent yet.
        if ack.largest >= self.spaces[space_id].next_packet_number {
            return Err(TransportError::PROTOCOL_VIOLATION("ACK for unsent packet"));
        }

        // Reject ACKs that go backwards relative to the largest packet number
        // already acknowledged in this packet number space.
        if let Some(prev_largest) = self.spaces[space_id].largest_acked_packet {
            if ack.largest < prev_largest {
                return Err(TransportError::PROTOCOL_VIOLATION(
                    "ACK regressed largest acknowledged packet",
                ));
            }
        }

        let mut largest_newly_acked: Option<SentPacket> = None;
        let mut newly_acked_any = false;

        for range in &ack {
            for pn in range {
                let Some(packet) = self.spaces[space_id].take(pn) else {
                    continue;
                };

                newly_acked_any = true;

                let is_larger = match &largest_newly_acked {
                    None => true,
                    Some(largest) => packet.number > largest.number,
                };

                if is_larger {
                    largest_newly_acked = Some(packet.clone());
                }

                if let Some(largest_acked_by_packet) = packet.largest_acked {
                    self.spaces[space_id]
                        .pending_acks
                        .subtract_below(largest_acked_by_packet + 1);
                }

                for frame in packet.stream_frames.iter().cloned() {
                    self.streams.received_ack_of(frame);
                }

                let removed_from_path = self.path.remove_in_flight(packet.number, &packet);

                if removed_from_path && packet.ack_eliciting {
                    self.path.congestion.on_ack(
                        packet.time_sent,
                        u64::from(packet.size),
                        self.app_limited,
                    );
                }
            }
        }

        if !newly_acked_any {
            return Ok(());
        }

        self.spaces[space_id].largest_acked_packet = Some(
            self.spaces[space_id]
                .largest_acked_packet
                .map_or(ack.largest, |prev| prev.max(ack.largest)),
        );
        self.spaces[space_id].largest_acked_packet_sent = now;

        // RTT is updated from the largest newly acknowledged packet.
        if let Some(packet) = largest_newly_acked {
            let ack_delay_micros = ack.delay << ACK_DELAY_EXP;
            let ack_delay = Duration::from_micros(ack_delay_micros);
            let rtt = now.saturating_duration_since(packet.time_sent);
            self.path.rtt.update(ack_delay, rtt);
        }

        self.detect_lost_packets(now, space_id);
        self.set_loss_detection_timer(now);

        Ok(())
        // unimplemented!("implement on_ack_received");
    }

    /// Parse and process an incoming QUIC packet and dispatch its frames to
    /// handshake, ACK, stream, and close handlers.
    fn process_packet(
        &mut self,
        now: Instant,
        number: Option<u64>,
        packet: Packet,
    ) -> Result<(), ConnectionError> {
        // TODO -- ATTEMPTING DONE

        let space = packet.header.space();

        if let Some(pn) = number {
            if self.spaces[packet.header.space()].dedup.insert(pn) {
                debug!("discarding possible duplicate packet");
                return Ok(());
            }
        }

        // Route by packet space / packet type
        match space {
            SpaceId::Initial => {
                debug!("processing Initial packet");
                self.process_early_payload(now, packet)?;
            }
            SpaceId::Handshake => {
                debug!("processing Handshake packet");
                self.process_early_payload(now, packet)?;
            }
            SpaceId::Data => {
                let pn = number.ok_or(ConnectionError::TransportError(TransportError {
                    code: TransportErrorCode::PROTOCOL_VIOLATION,
                    frame: None,
                    reason: "missing packet number".into(),
                }))?;
                debug!("processing 1-RTT packet");
                self.process_payload(now, pn, packet)?;
            }
        }
        Ok(())
        // unimplemented!("implement process_packet");
    }

    /// Process an Initial or Handshake packet payload
    /// Decode handshake-space frames, advance handshake state, and update
    /// pending control transmissions.
    fn process_early_payload(
        &mut self,
        now: Instant,
        packet: Packet,
    ) -> Result<(), TransportError> {
        // TODO -- ATTEMPTING DONE
        let space_id = packet.header.space();

        let mut parser = FrameParser::new(packet.payload)?;
        while let Some(frame) = parser.next().transpose()? {
            match frame {
                Frame::Padding => {}
                Frame::Ping => {}
                Frame::Ack(ack) => {
                    self.on_ack_received(now, space_id, ack)?;
                }
                Frame::Crypto(_crypto) => {
                    // For this lab skeleton, we just record that handshake traffic
                    // was observed in this space.
                    self.spaces[space_id].hi = true;

                    // Advance the highest active space we have reached.
                    if space_id > self.highest_space {
                        self.highest_space = space_id;
                    }

                    // If we have handshake-space crypto, we are no longer purely in
                    // the Initial phase.
                    if space_id == SpaceId::Handshake && self.state.is_handshake() {
                        self.state = State::Established;
                        // Notify application that the connection is ready.
                        self.events_tx.send(Event::Connected).ok();
                    }
                }
                Frame::Close(close) => {
                    self.kill(ConnectionError::from(close));
                    return Ok(());
                }

                // Stream/data/application frames are not valid in Initial/Handshake
                // packet payloads for this lab.
                Frame::Stream(_)
                | Frame::ResetStream(_)
                | Frame::StopSending(_)
                | Frame::MaxData(_)
                | Frame::MaxStreamData { .. }
                | Frame::MaxStreams { .. }
                | Frame::DataBlocked { .. }
                | Frame::StreamDataBlocked { .. }
                | Frame::StreamsBlocked { .. }
                | Frame::NewConnectionId(_)
                | Frame::RetireConnectionId { .. }
                | Frame::PathChallenge(_)
                | Frame::PathResponse(_)
                | Frame::Datagram(_)
                | Frame::ImmediateAck
                | Frame::HandshakeDone
                | Frame::NewToken(_) => {
                    return Err(TransportError::PROTOCOL_VIOLATION(
                        "illegal frame in Initial/Handshake packet",
                    ));
                }
            }
        }

        Ok(())
        // unimplemented!("implement process_early_payload");
    }

    /// Process a 1-RTT packet payload.
    /// Dispatch ACK/STREAM/RESET/STOP_SENDING/CLOSE frames and update the
    /// connection state machine.
    fn process_payload(
        &mut self,
        now: Instant,
        number: u64,
        packet: Packet,
    ) -> Result<(), TransportError> {
        // TODO -- ATTEMPTING DONE
        let space_id = packet.header.space();
        let payload_len = packet.payload.len();

        let mut parser = FrameParser::new(packet.payload)?;
        while let Some(frame) = parser.next().transpose()? {
            match frame {
                Frame::Padding => {}
                Frame::Ping => {}
                Frame::ImmediateAck => {
                    self.spaces[space_id]
                        .pending_acks
                        .set_immediate_ack_required();
                }
                Frame::Ack(ack) => {
                    self.on_ack_received(now, space_id, ack)?;
                }
                Frame::Stream(stream) => {
                    self.streams.received(stream, payload_len)?;
                }
                Frame::ResetStream(reset) => {
                    self.streams.received_reset(reset)?;
                }
                Frame::StopSending(stop) => {
                    self.streams.received_stop_sending(stop)?;
                }
                Frame::Close(close) => {
                    self.kill(ConnectionError::from(close));
                    return Ok(());
                }
                _ => {}
            }
        }
        Ok(())
        // unimplemented!("implement process_payload");
    }

    /// Close a connection immediately
    pub fn close(&mut self, error_code: u64, reason: Bytes) {
        let reason = ConnectionClose::Application(ApplicationClose { error_code, reason });
        let was_closed = self.state.is_closed();
        if !was_closed {
            self.close_common();
            self.set_close_timer(Instant::now());
            self.pending_close = true;
            self.state = State::Closed(reason);
        }
    }

    fn close_common(&mut self) {
        debug!("connection closed");
        for &timer in Timer::VARIANTS {
            self.timers.stop(timer);
        }
    }

    fn set_close_timer(&mut self, now: Instant) {
        const CLOSE_TIMEOUT: Duration = Duration::from_secs(3);
        self.timers.set(Timer::Close, now + CLOSE_TIMEOUT);
    }

    /// Terminate the connection instantly, without sending a close packet
    fn kill(&mut self, reason: ConnectionError) {
        self.close_common();
        self.error(reason);
        self.state = State::Drained;
    }

    fn space_can_send(&self, space_id: SpaceId) -> SendableFrames {
        let mut can_send = self.spaces[space_id].can_send();
        if space_id == SpaceId::Data {
            can_send.other |= self.streams.can_send_stream_data();
        }
        can_send
    }

    /// Return whether this should be called again
    /// Poll all sendable spaces, compose packets, and enqueue datagrams for transmission.
    fn poll_transmit(&mut self, now: Instant) -> bool {
        // TODO -- ATTEMPTING DONE
        // Things to take into consideration:
        // 1) Whether the stream is App-limited?
        // 2) Whether the stream is congestion-blocked?
        // Schedule to send a packet, rather than actually sending one here.

        let close = match self.state {
            State::Drained => {
                return false;
            }
            State::Draining | State::Closed(_) => {
                if !self.pending_close {
                    return false;
                }
                true
            }
            _ => false,
        };

        // Check congestion window once before iterating spaces.
        let congestion_blocked = self.path.in_flight.bytes >= self.path.congestion.window();

        let mut sent_any = false;

        for &space_id in SpaceId::VARIANTS {
            // Is there data or a close message to send in this space?
            let can_send = self.space_can_send(space_id);
            if can_send.is_empty() && !(close && space_id == self.highest_space) {
                continue;
            }

            if self.side.is_client()
                && space_id == SpaceId::Handshake
                && self.spaces[SpaceId::Initial].hi
            {
                // A client stops both sending and processing Initial packets when it sends its
                // first Handshake packet.
                self.discard_space(now, SpaceId::Initial);
            }

            let space = &mut self.spaces[space_id];

            let mut ack_eliciting = can_send.other;
            if space_id == SpaceId::Data {
                ack_eliciting |= self.streams.can_send_stream_data();
            }

            // TODO -- ATTEMPTING DONE
            // proceed only if we are not blocked by congestion control here.

            // Don't send ack-eliciting packets if congestion-blocked.
            // Pure ACK-only packets (ack_eliciting=false) are always allowed through
            // since they don't consume congestion window.
            if ack_eliciting && congestion_blocked {
                // We couldn't send due to congestion, not app behaviour — not app-limited.
                self.app_limited = false;
                continue;
            }

            let mut buf = vec![0u8; INITIAL_MTU];
            let mut out = &mut buf[..];

            let exact_number = space.get_tx_number();
            let _span = trace_span!("send", space = ?space_id, pn = exact_number).entered();

            let number = PacketNumber::new(exact_number, space.largest_acked_packet.unwrap_or(0));
            let header = match space_id {
                SpaceId::Initial => Header::Initial(InitialHeader {
                    dst_cid: ConnectionId::new(FAKE_CID),
                    src_cid: ConnectionId::new(FAKE_CID),
                    token: Bytes::new(),
                    number,
                    version: crate::SUPPORTED_VERSIONS[0],
                }),
                SpaceId::Handshake => Header::Long {
                    ty: LongType::Handshake,
                    dst_cid: ConnectionId::new(FAKE_CID),
                    src_cid: ConnectionId::new(FAKE_CID),
                    number,
                    version: crate::SUPPORTED_VERSIONS[0],
                },
                SpaceId::Data => Header::Short {
                    spin: false,
                    key_phase: false,
                    dst_cid: ConnectionId::new(FAKE_CID),
                    number,
                },
            };
            header.encode(&mut out);
            trace!(?header);

            let SentFrames {
                retransmits,
                largest_acked,
                stream_frames,
            } = if close {
                debug!("sending CONNECTION_CLOSE");
                // TODO -- ATTEMPTING DONE
                // Encode ACKs before the ConnectionClose message, to give the receiver a better
                // approximate on what data has been processed. This is especially important
                // with ack delay, since the peer might not have gotten any other ACK for the
                // data earlier on.

                // Encode ACKs before the ConnectionClose so the peer gets up-to-date
                // acknowledgement info, especially important with ACK delay active.
                if !self.spaces[space_id].pending_acks.ranges().is_empty() {
                    Self::populate_acks(
                        now,
                        &mut SentFrames::default(),
                        &mut self.spaces[space_id],
                        &mut out,
                    );
                }
                // Don't send another close packet
                self.pending_close = false;

                SentFrames::default()
            } else {
                self.populate_packet(now, space_id, &mut out)
            };

            if largest_acked.is_some() {
                self.spaces[space_id].pending_acks.acks_sent();
                self.timers.stop(Timer::MaxAckDelay);
            };

            let remaining = out.remaining_mut();
            let packet_len = buf.len() - remaining;
            let size = if ack_eliciting { packet_len } else { 0 } as _;
            let meta_info = SentPacket {
                time_sent: now,
                size,
                ack_eliciting,
                largest_acked,
                retransmits,
                stream_frames,
                number: exact_number,
                space_id,
            };

            let packet = Bytes::from(buf).slice(..packet_len);

            // Schedule to send
            let scheduled_packet = ScheduledPacket { packet, meta_info };
            self.path.sending_queue.push_back(scheduled_packet);
            sent_any = true;
        }

        // If nothing was queued this round, stop spinning.
        if !sent_any {
            return false;
        }

        // Whether this function should be called again
        // TODO --ATTEMPTING DONE
        // consider whether the stream is congestion_blocked, and whether it is app_limited
        // Ask to be called again only if there is still unsent work remaining.

        let more_to_send = SpaceId::VARIANTS
            .iter()
            .any(|&id| !self.space_can_send(id).is_empty());

        // App-limited: sender has data capacity left but nothing to send —
        // i.e. the bottleneck is the application, not the network.
        self.app_limited =
            !more_to_send && self.path.in_flight.bytes < self.path.congestion.window();

        more_to_send
    }

    /// Write pending ACKs into a buffer
    fn populate_acks<B: BufMut>(
        now: Instant,
        sent: &mut SentFrames,
        space: &mut PacketSpace,
        buf: &mut B,
    ) {
        debug_assert!(!space.pending_acks.ranges().is_empty());
        sent.largest_acked = space.pending_acks.ranges().max();

        let delay_micros = space.pending_acks.ack_delay(now).as_micros() as u64;
        let delay = delay_micros >> ACK_DELAY_EXP;

        trace!(
            "ACK {:?}, Delay = {}us",
            space.pending_acks.ranges(),
            delay_micros
        );

        Ack::encode(delay, space.pending_acks.ranges(), None, buf);
    }

    /// Populate one packet with ACK/control/stream frames and return metadata
    /// required for ACK and retransmission tracking.
    fn populate_packet<B: BufMut>(
        &mut self,
        now: Instant,
        space_id: SpaceId,
        out: &mut B,
    ) -> SentFrames {
        // TODO -- ATTEMPTING DONE

        let mut sent = SentFrames::default();

        // 1. ACK frames first
        {
            let space = &mut self.spaces[space_id];
            if !space.pending_acks.ranges().is_empty() {
                Self::populate_acks(now, &mut sent, space, out);
            }
        }

        // 2. Hello / Crypto frame (drives handshake progress)
        {
            let space = &mut self.spaces[space_id];
            if space.pending.hello {
                space.pending.hello = false;
                space.hi = true;
                out.put_u8(0x06); // CRYPTO frame type
                out.put_u8(0x00); // offset = 0
                out.put_u8(0x01); // length = 1
                out.put_u8(0x00); // dummy crypto payload
                sent.retransmits.get_or_create().hello = true;
            }
            if space_id == SpaceId::Data && space.ping_pending {
                space.ping_pending = false;
                out.put_u8(0x01); // PING frame type
            }
        }

        // 3. RESET_STREAM / STOP_SENDING control frames
        if space_id == SpaceId::Data {
            let pending = &mut self.spaces[space_id].pending;
            self.streams
                .write_control_frames(out, pending, &mut sent.retransmits);
        }

        // 4. STREAM data frames
        if space_id == SpaceId::Data {
            sent.stream_frames = self.streams.write_stream_frames(out, true);
        }

        sent
        // unimplemented!("implement populate_packet");
    }

    /// Handle loss-detection timeout by running time-threshold loss processing
    /// and resetting related timers.
    fn on_loss_detection_timeout(&mut self, now: Instant) {
        // TODO -- ATTEMPTING DONE
        if let Some((_, space_id)) = self.loss_time_and_space() {
            self.detect_lost_packets(now, space_id);
        } else {
            // PTO: force retransmission in all spaces that have in-flight packets
            for &space_id in SpaceId::VARIANTS {
                if self.spaces[space_id].in_flight > 0 {
                    self.detect_lost_packets(now, space_id);
                }
            }
        }
        self.set_loss_detection_timer(now);
        // unimplemented!("implement on_loss_detection_timeout");
    }

    /// Dispatch timeout handling by timer type (Close, LossDetection).
    fn handle_timeout(&mut self, timer: Timer, now: Instant) {
        trace!(?timer, "timeout");
        match timer {
            Timer::LossDetection => self.on_loss_detection_timeout(now),
            Timer::Close => {
                self.state = State::Drained;
            }
            Timer::MaxAckDelay => {
                trace!("max ack delay reached");
                // This timer is only armed in the Data space
                self.spaces[SpaceId::Data]
                    .pending_acks
                    .on_ack_delay_timeout()
            }
            // TODO: You may need to process more kinds of timer (e.g. Idle)
            _ => {}
        }
    }

    /// Provide control over streams
    #[must_use]
    pub fn recv_stream(&mut self, id: StreamId) -> RecvStream<'_> {
        assert!(id.dir() == Dir::Bi || id.initiator() != self.side);
        RecvStream {
            id,
            state: &mut self.streams,
            pending: &mut self.spaces[SpaceId::Data].pending,
        }
    }

    /// Provide control over streams
    #[must_use]
    pub fn send_stream(&mut self, id: StreamId) -> SendStream<'_> {
        assert!(id.dir() == Dir::Bi || id.initiator() == self.side);
        SendStream {
            id,
            state: &mut self.streams,
            pending: &mut self.spaces[SpaceId::Data].pending,
            conn_state: &self.state,
        }
    }

    #[must_use]
    pub fn open_stream(&mut self, dir: Dir) -> StreamId {
        if self.state.is_closed() {
            panic!("Cannot open stream on closed connection");
        }
        self.streams.open(dir)
    }
}

#[derive(Debug, Default)]
pub enum State {
    #[default]
    Handshake,
    Established,
    Closed(ConnectionClose),
    Draining,
    /// Waiting for application to call close so we can dispose of the resources
    Drained,
}

impl State {
    fn closed<R: Into<ConnectionClose>>(reason: R) -> Self {
        Self::Closed(reason.into())
    }

    fn is_handshake(&self) -> bool {
        matches!(*self, Self::Handshake)
    }

    fn is_closed(&self) -> bool {
        matches!(*self, Self::Closed(_) | Self::Draining | Self::Drained)
    }

    pub(crate) fn is_drained(&self) -> bool {
        matches!(*self, Self::Drained)
    }
}

#[derive(Default)]
struct SentFrames {
    retransmits: ThinRetransmits,
    largest_acked: Option<u64>,
    stream_frames: Vec<StreamMeta>,
}

/// Reasons why a connection might be lost
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConnectionError {
    /// The peer doesn't implement any supported version
    #[error("peer doesn't implement any supported version")]
    VersionMismatch,
    /// The peer violated the QUIC specification as understood by this
    /// implementation
    #[error(transparent)]
    TransportError(#[from] TransportError),
    /// The peer's QUIC stack aborted the connection automatically
    #[error("aborted by peer: {0}")]
    TransportClosed(TransportClose),
    /// The peer closed the connection
    #[error("closed by peer: {0}")]
    ApplicationClosed(ApplicationClose),
    /// The peer is unable to continue processing this connection, usually due
    /// to having restarted
    #[error("reset by peer")]
    Reset,
}

impl From<ConnectionClose> for ConnectionError {
    fn from(x: ConnectionClose) -> Self {
        match x {
            ConnectionClose::Transport(reason) => Self::TransportClosed(reason),
            ConnectionClose::Application(reason) => Self::ApplicationClosed(reason),
        }
    }
}

/// Events of interest to the application
#[derive(Debug)]
pub enum Event {
    /// The connection was successfully established
    Connected,
    /// The connection was lost
    ///
    /// Emitted if the peer closes the connection or an error is encountered.
    ConnectionLost(ConnectionError),
    /// Stream events
    Stream(StreamEvent),
}
