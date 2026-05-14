use std::{fs::File, io::Write, path::PathBuf};

use clap::Parser;
use mzquic::file_notice_macos::AsyncFileWaiter;
use mzquic::*;
use mzquic_core::{Endpoint, Event, StreamEvent, TransportHandler, TransportHandlerFactory};
use mzquic_proto::{Dir, StreamId};
use rand::Rng;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

fn pingpong() -> TransportHandler {
    let mut remaining = 19;

    Box::new(move |conn, event| {
        debug!(?event);

        let mut recv = None;
        let mut cnt = 0;
        let mut finished = false;

        match event {
            Event::Connected => {
                info!("Connected to {}", conn.remote_addr());
                let id = conn.open_stream(Dir::Bi);
                conn.send_stream(id).write(b"ping").unwrap();
            }
            Event::Stream(StreamEvent::Readable(id)) => {
                let mut stream = conn.recv_stream(id);
                let mut chunks = stream.read(true).unwrap();

                while let Ok(chunk) = chunks.next(4) {
                    match chunk {
                        Some(data) => {
                            info!(?id, "Received data: {:?}", data);
                            recv = Some(id);
                            cnt += 1;
                        }
                        None => {
                            debug!(?id, "Stream finished");
                            finished = true;
                            break;
                        }
                    }
                }
            }
            _ => {}
        }

        if finished {
            conn.close(0, "byebye".into());
        } else if let Some(id) = recv {
            let mut stream = conn.send_stream(id);
            while cnt > 0 && remaining > 0 {
                stream.write(b"ping").unwrap();
                cnt -= 1;
                remaining -= 1;
            }
            if remaining == 0 {
                stream.finish().unwrap();
            }
        }
    })
}

fn pingpong_stop() -> TransportHandler {
    let random_error_code: u64 = (0xfff & rand::rng().next_u32()) as u64 + 0x3000;
    struct StreamState {
        id: StreamId,
        to_send: &'static str,
        send_remain: Option<usize>,
        finished: bool,
    }
    let mut stream_states: Vec<StreamState> = vec![];

    info!("Random Error Code {:x}", random_error_code);

    Box::new(move |conn, event| {
        debug!(?event);

        let mut to_send = None;
        let mut to_reset = None;
        let mut cnt = 0;

        match event {
            Event::Connected => {
                info!("Connected to {}", conn.remote_addr());
                let id = conn.open_stream(Dir::Bi);
                conn.send_stream(id).write(b"ping").unwrap();
                stream_states.push(StreamState {
                    id,
                    to_send: "ping",
                    finished: false,
                    send_remain: Some(9),
                });
            }
            Event::Stream(StreamEvent::Readable(id)) => {
                let mut stream = conn.recv_stream(id);
                let Some(state) = stream_states.iter_mut().find(|state| state.id == id) else {
                    unreachable!("Unknown stream id");
                };
                let mut chunks = stream.read(true).unwrap();
                let expecting_length = state.to_send.len();
                while let Ok(chunk) = chunks.next(expecting_length) {
                    match chunk {
                        Some(data) => {
                            info!(?id, "Received data: {:?}", data);
                            if let Some(send_remain) = state.send_remain.as_mut() {
                                if *send_remain > 0 {
                                    *send_remain -= 1;
                                    to_send = Some((id, state.to_send));
                                    cnt += 1;
                                } else {
                                    to_reset = Some(id);
                                }
                            } else {
                                to_send = Some((id, state.to_send));
                                cnt += 1;
                            }
                        }
                        None => {
                            debug!(?id, "Stream finished");
                            state.finished = true;
                            break;
                        }
                    }
                }
            }
            Event::Stream(StreamEvent::Stopped { id, error_code }) => {
                warn!(?id, "Stream stopped: {:x}", error_code);
                assert_eq!(id, stream_states[1].id);
                assert_eq!(error_code, random_error_code);
                stream_states[1].finished = true;
            }
            Event::ConnectionLost(err) => {
                error!(?err);
            }
            _ => {}
        }

        if stream_states.iter().filter(|x| x.finished).count() == 2 {
            info!("Both streams finished");
            conn.close(0, "byebye".into());
            return;
        }

        if let Some((id, message)) = to_send {
            for _ in 0..cnt {
                tracing::info!("Sending {:?} on stream {}", message.as_bytes(), id);
                let mut stream = conn.send_stream(id);
                stream.write(message.as_bytes()).unwrap();
            }
        }

        if let Some(id) = to_reset {
            let mut stream = conn.send_stream(id);
            let _ = stream.reset(random_error_code);

            // Create new stream
            if stream_states.len() >= 2 {
                return;
            }
            let id = conn.open_stream(Dir::Bi);
            conn.send_stream(id).write(b"ping ping").unwrap();
            stream_states.push(StreamState {
                id,
                to_send: "ping ping",
                finished: false,
                send_remain: None,
            });
        }
    })
}

fn cc() -> TransportHandler {
    let path = FILE_PATH
        .get()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("received.bin"));

    let mut f = File::create(path).unwrap();

    Box::new(move |conn, event| {
        debug!(?event);
        let mut finished = false;

        if let Event::Stream(StreamEvent::Readable(id)) = event {
            let mut stream = conn.recv_stream(id);
            let Ok(mut chunks) = stream.read(true) else {
                return;
            };

            while let Ok(chunk) = chunks.next(usize::MAX) {
                match chunk {
                    Some(chunk) => {
                        debug!(
                            ?id,
                            "Read [{}..{})",
                            chunk.offset,
                            chunk.offset + chunk.bytes.len() as u64
                        );
                        f.write_all(&chunk.bytes).expect("Failed to write to file");
                    }
                    None => {
                        debug!(?id, "Stream finished");
                        finished = true;
                        break;
                    }
                }
            }
        }

        if finished {
            conn.close(0, "finished".into());
        }
    })
}

fn echo() -> TransportHandler {
    struct State {
        id: StreamId,
        bytes: usize,
        finished: bool,
    }

    let mut ss = vec![];

    let large_message = vec![0; 1024000]; // 10 KiB

    Box::new(move |conn, event| {
        debug!(?event);

        match event {
            Event::Connected => {
                info!("Connected to {}", conn.remote_addr());
                for prio in [2, 1] {
                    let id = conn.open_stream(Dir::Bi);
                    let mut stream = conn.send_stream(id);
                    stream.set_priority(prio).unwrap();
                    stream.write(&large_message).unwrap();
                    stream.finish().unwrap();
                    ss.push(State {
                        id,
                        bytes: 0,
                        finished: false,
                    });
                }
            }
            Event::Stream(StreamEvent::Readable(id)) => {
                let mut stream = conn.recv_stream(id);

                let Some(state) = ss.iter_mut().find(|state| state.id == id) else {
                    unreachable!("Unknown stream id");
                };

                if let Ok(mut chunks) = stream.read(true) {
                    while let Ok(chunk) = chunks.next(usize::MAX) {
                        match chunk {
                            Some(chunk) => {
                                debug!(
                                    ?id,
                                    "Read [{}..{})",
                                    chunk.offset,
                                    chunk.offset + chunk.bytes.len() as u64
                                );
                                state.bytes += chunk.bytes.len();
                            }
                            None => {
                                warn!(?id, "Stream finished");
                                state.finished = true;
                                break;
                            }
                        }
                    }
                }
            }
            Event::Stream(StreamEvent::Stopped { id, error_code }) => {
                warn!(?id, "Stream stopped: {:x}", error_code);
                assert_eq!(id, ss[1].id);
                assert_eq!(error_code, 0x7331);
                ss[1].finished = true;
            }
            Event::ConnectionLost(err) => {
                error!(?err);
            }
            _ => {}
        }

        if !ss[0].finished {
            if ss[0].bytes > large_message.len() / 16 {
                let mut stream = conn.send_stream(ss[0].id);
                let _ = stream.reset(0x1337);
            }
        } else if !ss[1].finished {
            if ss[1].bytes > large_message.len() / 4 {
                let mut stream = conn.recv_stream(ss[1].id);
                let _ = stream.stop(0x7331);
            }
        } else {
            info!("ss[0].bytes = {}", ss[0].bytes);
            info!("ss[1].bytes = {}", ss[1].bytes);
            conn.close(0, "byebye".into());
        }
    })
}

#[tokio::main]
async fn main() {
    // console_subscriber::init();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = CliArguments::parse();

    tracing::info!("Running {:?} client", args.program);

    let handler: TransportHandlerFactory = match args.program {
        Program::PingPong => pingpong,
        Program::PingPongStop => pingpong_stop,
        Program::StreamEcho => echo,
        Program::FileTransfer { path } => {
            if let Some(path) = path {
                FILE_PATH.set(path).ok();
            }
            cc
        }
    };

    let mut endpoint = Endpoint::new(handler, "0.0.0.0:0").await;
    endpoint.connect(args.address);

    if let Some(file_lock) = args.file_lock {
        match AsyncFileWaiter::new(file_lock) {
            Ok(mut waiter) => {
                tracing::warn!("Waiting for server to create the file marker",);
                waiter.wait_until_file_marker().await.ok();
            }
            Err(mzquic::file_notice_macos::FileWaitError::AlreadyExists) => {
                tracing::warn!("Server has created the file marker");
            }
            Err(error) => {
                tracing::error!(?error, "Failed to create the file waiter");
            }
        }
    }
    // Exit after finishing
    endpoint.run(true).await;
    tracing::info!("Gracefully shutting down for client");
}
