pub mod file_notice_macos;

use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};
use tokio::sync::OnceCell;

#[derive(Subcommand, Debug)]
#[clap(rename_all = "kebab-case")]
pub enum Program {
    StreamEcho,
    PingPong,
    PingPongStop,
    FileTransfer {
        #[arg(short, long)]
        path: Option<PathBuf>,
    },
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct CliArguments {
    pub address: SocketAddr,

    #[command(subcommand)]
    pub program: Program,

    #[arg(long)]
    pub file_lock: Option<PathBuf>,
}

pub static FILE_PATH: OnceCell<PathBuf> = OnceCell::const_new();
