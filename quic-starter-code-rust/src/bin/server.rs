use std::path::PathBuf;

use bytes::Bytes;
use clap::Parser;
use mzquic::*;
use mzquic::file_notice_macos::FileMarker;
use mzquic_core::{
    Connection, Endpoint, Event, ReadError, StreamEvent, TransportHandler, TransportHandlerFactory,
};
use mzquic_proto::{Dir, StreamId};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

fn pingpong(conn: &mut Connection, event: Event) {
    debug!(?event);

    let mut recv = None;
    let mut cnt = 0;
    let mut finished = false;

    if let Event::Stream(StreamEvent::Readable(id)) = event {
        let mut stream = conn.recv_stream(id);
        let mut chunks = stream.read(true).unwrap();

        while let Ok(chunk) = chunks.next(4) {
            match chunk {
                Some(chunk) => {
                    info!(?id, "Received data: {:?}", chunk);
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

    if let Some(id) = recv {
        let mut stream = conn.send_stream(id);
        for _ in 0..cnt {
            stream.write(b"pong").unwrap();
        }
        if finished {
            stream.finish().unwrap();
        }
    }
}

fn pingpong_stop() -> TransportHandler {
    let mut client_error_code = None;
    let mut ping_ping_cnt = 0;
    enum Action {
        Send(StreamId, Bytes),
        Finish(StreamId),
        Stop(StreamId, u64),
    }
    Box::new(move |conn: &mut Connection, event: Event| {
        let mut actions: Vec<Action> = vec![];

        match event {
            Event::Stream(StreamEvent::Readable(id)) => {
                let mut stream = conn.recv_stream(id);
                let to_reply = if id == StreamId::from(0) {
                    "pong"
                } else {
                    "pong pong"
                };
                let mut chunks = stream.read(true).unwrap();

                loop {
                    match chunks.next(to_reply.len()) {
                        Ok(Some(chunk)) => {
                            info!(?id, "Received data: {:?}", chunk);

                            if to_reply == "pong pong" {
                                ping_ping_cnt += 1;
                            }
                            if ping_ping_cnt < 10 {
                                actions.push(Action::Send(id, Bytes::from(to_reply)));
                                continue;
                            } else {
                                actions
                                    .push(Action::Stop(id, client_error_code.unwrap_or_default()));
                                actions.push(Action::Finish(id));
                            }
                        }
                        Ok(None) => {
                            unreachable!("Stream should not finish in this test");
                        }
                        Err(ReadError::Blocked) => {}
                        Err(ReadError::Reset(code)) => {
                            warn!(?id, "Stream reset: {:x}", code);
                            client_error_code = Some(code);
                            actions.push(Action::Finish(id));
                        }
                    }
                    break;
                }
            }
            Event::Stream(StreamEvent::Stopped { id, error_code }) => {
                warn!(?id, "Stream stopped: {:x}", error_code);
                actions.push(Action::Stop(id, error_code));
            }
            _ => {}
        }

        for action in actions {
            match action {
                Action::Send(id, content) => {
                    info!(?id, "Reply {:?}", content);
                    conn.send_stream(id).write(&content).ok();
                }
                Action::Finish(id) => {
                    info!(?id, "Finish send");
                    conn.send_stream(id).finish().ok();
                }
                Action::Stop(id, error_code) => {
                    info!(?id, "Stop recv");
                    conn.recv_stream(id).stop(error_code).ok();
                }
            }
        }
    })
}

fn cc() -> TransportHandler {
    let path = FILE_PATH
        .get()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("original.bin"));

    let content = std::fs::read(path).expect("Failed to read file");

    Box::new(move |conn: &mut Connection, event: Event| {
        debug!(?event);

        if let Event::Connected = event {
            let id = conn.open_stream(Dir::Uni);
            let mut stream = conn.send_stream(id);
            stream.write(&content).unwrap();
            stream.finish().unwrap();
        }
    })
}

fn echo(conn: &mut Connection, event: Event) {
    debug!(?event);

    enum Action {
        Send(StreamId, Bytes),
        Finish(StreamId),
        Stop(StreamId, u64),
        None,
    }

    let mut action = Action::None;

    match event {
        Event::Stream(StreamEvent::Readable(id)) => {
            let mut stream = conn.recv_stream(id);

            if let Ok(mut chunks) = stream.read(true) {
                match chunks.next(usize::MAX) {
                    Ok(chunk) => {
                        let chunk = chunk.expect("Stream should not finish");
                        debug!(
                            ?id,
                            "Read [{}..{})",
                            chunk.offset,
                            chunk.offset + chunk.bytes.len() as u64
                        );
                        action = Action::Send(id, chunk.bytes);
                    }
                    Err(ReadError::Blocked) => {}
                    Err(ReadError::Reset(code)) => {
                        warn!(?id, "Stream reset: {:x}", code);
                        action = Action::Finish(id);
                    }
                }
            }
        }
        Event::Stream(StreamEvent::Stopped { id, error_code }) => {
            warn!(?id, "Stream stopped: {:x}", error_code);
            action = Action::Stop(id, error_code);
        }
        _ => {}
    }

    match action {
        Action::Send(id, bytes) => {
            conn.send_stream(id).write(&bytes).ok();
        }
        Action::Finish(id) => {
            let mut stream = conn.send_stream(id);
            stream.finish().unwrap();
        }
        Action::Stop(id, error_code) => {
            conn.recv_stream(id).stop(error_code).unwrap();
        }
        Action::None => {}
    }
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

    tracing::info!("Running {:?} server", args.program);

    let handler: TransportHandlerFactory = match args.program {
        Program::PingPong => || Box::new(pingpong),
        Program::PingPongStop => pingpong_stop,
        Program::StreamEcho => || Box::new(echo),
        Program::FileTransfer { path } => {
            if let Some(path) = path {
                FILE_PATH.set(path).ok();
            }
            cc
        }
    };

    let endpoint = Endpoint::new(handler, args.address).await;
    let _marker = args.file_lock.map(|file_lock| {
        FileMarker::new(file_lock)
            .map_err(|error| tracing::error!(?error, "Failed to create file lock"))
    });

    tokio::select! {
        _ = endpoint.run(false) => {}
        _ = tokio::signal::ctrl_c() => {
            tracing::warn!("received Ctrl+C");
        }
    }

    tracing::info!("Gracefully shutting down for server");
}
