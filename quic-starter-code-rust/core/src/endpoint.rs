use std::{
    collections::HashMap,
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};

use bytes::BytesMut;
use mzquic_proto::{Header, InitialPacket, Side};
use slab::Slab;
use tokio::{
    net::UdpSocket,
    // sync::mpsc::UnboundedReceiver,
    time::{Duration, Instant, interval},
};
use tracing::{debug, error, info, trace, warn};

use crate::{Connection, ConnectionHandle, ConnectionTask, INITIAL_MTU, TransportHandlerFactory};

pub struct Endpoint {
    handler_factory: TransportHandlerFactory,
    socket: Arc<UdpSocket>,
    connections: Slab<ConnectionTask>,
    routes: HashMap<SocketAddr, ConnectionHandle>,
    start: Instant,
    // packet_to_send: UnboundedReceiver<(SocketAddr, Bytes)>,
}

fn parse_addr(addr: impl ToSocketAddrs) -> SocketAddr {
    addr.to_socket_addrs()
        .map(|mut addr| addr.next())
        .ok()
        .flatten()
        .expect("Failed to parse socket addr")
}

impl Endpoint {
    pub async fn new(handler_factory: TransportHandlerFactory, addr: impl ToSocketAddrs) -> Self {
        let addr = parse_addr(addr);
        let socket = UdpSocket::bind(addr)
            .await
            .expect("Failed to bind UDP socket");

        let socket = Arc::new(socket);
        Self {
            handler_factory,
            socket,
            connections: Slab::new(),
            routes: HashMap::new(),
            start: Instant::now(),
        }
    }

    pub fn connect(&mut self, addr: impl ToSocketAddrs) {
        let addr = parse_addr(addr);

        if self.routes.contains_key(&addr) {
            warn!("Already connected to {addr}");
            return;
        }

        self.start_connection(addr);
    }

    fn start_connection(&mut self, addr: SocketAddr) {
        let side = Side::Client;
        let entry = self.connections.vacant_entry();
        let conn = ConnectionTask::new(
            entry.key(),
            addr,
            self.socket.clone(),
            (self.handler_factory)(),
            side,
        );
        let conn = entry.insert(conn);
        self.routes.insert(addr, conn.handle());
    }

    // Try to accept a connection as a server. If the first_packet cannot be
    // interpreted as an initial packet, drop it and do nothing.
    fn accept_connection(&mut self, addr: SocketAddr, mut first_packet: BytesMut, now: Instant) {
        let side = Side::Server;

        let header = match Header::decode(&mut first_packet, 0, crate::SUPPORTED_VERSIONS) {
            Ok(header) => header,
            Err(e) => {
                error!(?e, "Error decoding header");
                return;
            }
        };

        let Header::Initial(header) = header else {
            debug!(
                ?addr,
                "Failed to decode the header of first packet: {header:?}"
            );
            return;
        };

        debug!(?addr, "Incoming connection: {header:?}");

        let packet = InitialPacket {
            header,
            payload: first_packet.freeze(),
        };

        let mut conn = Connection::new(addr, self.socket.clone(), side);

        if let Err(e) = conn.handle_first_packet(now, packet) {
            debug!("Failed to process first packet: {}", e);
            return;
        }

        // Insert the connection
        let entry = self.connections.vacant_entry();
        let conn =
            ConnectionTask::with_connection(conn, entry.key(), addr, (self.handler_factory)());
        let conn = entry.insert(conn);
        self.routes.insert(addr, conn.handle());

        info!(?addr, "Accepting connection from");
    }

    fn remove_connection(&mut self, ch: ConnectionHandle) {
        if let Some(conn) = self.connections.try_remove(ch) {
            info!(?ch, "Removing connection to {:?}", conn.remote_addr());
            self.routes.remove(&conn.remote_addr());
        }
    }

    /// Dispatch UDP datagram to connections, or try to create a connection
    fn dispatch_datagram(&mut self, data: BytesMut, addr: SocketAddr) {
        let now = Instant::now();
        trace!(
            now = ?now.duration_since(self.start),
            ?addr,
            "Received {} bytes", data.len()
        );

        // Existing connection
        if let Some(&ch) = self.routes.get(&addr) {
            let conn = self.connections.get_mut(ch).unwrap();
            conn.data_tx.send((now, data)).ok();
            return;
        }

        // Try to create a new connection
        self.accept_connection(addr, data, now);
    }

    pub async fn run(mut self, exit: bool) {
        let mut check_close = interval(Duration::from_secs(1));

        // Receive packets here only, for all connections.
        loop {
            let mut buf = BytesMut::with_capacity(INITIAL_MTU);

            tokio::select! {
                new_packet = self.socket.recv_buf_from(&mut buf) => {
                    match new_packet {
                        Ok((size, addr)) => {
                            tracing::debug!(?size, "Receive Datagram");
                            self.dispatch_datagram(buf, addr);
                        }
                        Err(e) => {
                            warn!(?e, "Error on receiving UDP datagram")
                        }
                    }
                },
                // Check if any connections is closed every second.
                _ = check_close.tick() => {
                    let mut removed = Vec::new();
                    self.connections.retain(|id, conn| {
                        let alive = conn.is_alive();
                        if !alive {
                            removed.push(id);
                        }
                        alive
                    });
                    removed.into_iter().for_each(|id| self.remove_connection(id));
                    if exit && self.connections.is_empty() {
                        break;
                    }
                }
            }
        }

        info!("Endpoint exited");
    }
}
