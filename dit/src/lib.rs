#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links)]

use clap::{Parser, Subcommand};
use dit_core::config::GlobalConfig;
use dit_core::daemon::{ConnectionToDaemon, DaemonConfig, LocalListener, Packet};
use dit_core::peer::{DhtAddr, Runtime};
use dit_core::store::Store;
use std::fs;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use tokio::io;
use tracing::Instrument;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

pub async fn run_daemon(config: GlobalConfig) -> Result<(), io::Error> {
    let rt = Runtime::new(config.clone().peer).await?;

    let mut local_listener = LocalListener::new(&config.daemon, rt.controller).await?;
    let remote_listener = rt.listener;

    let local_listener_task = tokio::spawn(
        async move {
            loop {
                if let Some(inbound) = local_listener.accept().await? {
                    tokio::spawn(inbound.run().in_current_span());
                } else {
                    return Ok::<(), io::Error>(());
                }
            }
        }
        .instrument(tracing::debug_span!("local listener")),
    );

    let remote_listener_task = tokio::spawn(
        async move {
            loop {
                if let Some(remote_peer) = remote_listener.accept().await? {
                    tokio::spawn(remote_peer.run().in_current_span());
                } else {
                    return Ok::<(), io::Error>(());
                }
            }
        }
        .instrument(tracing::debug_span!("remote listener")),
    );

    let local_peer = tokio::spawn(
        rt.local_peer
            .run()
            .instrument(tracing::debug_span!("local peer")),
    );

    let (local_listener_result, remote_listener_result, local_peer_result) =
        tokio::join!(local_listener_task, remote_listener_task, local_peer);

    local_listener_result.unwrap().unwrap();
    remote_listener_result.unwrap().unwrap();
    local_peer_result.unwrap();

    Ok(())
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Creates the config file.
    Init {
        /// Overwrites the file if it exists.
        #[arg(long)]
        overwrite: bool,
    },
    /// Starts the daemon.
    Daemon,
    /// Pings the daemon.
    PingDaemon,
    /// Bootstrap the daemon.
    Bootstrap {
        /// The address of the peer to bootstrap the daemon to.
        address: SocketAddr,
    },
    /// Adds file to store.
    Add {
        /// Path to file to add.
        path: PathBuf,
    },
    /// Writes the content of the file from the store to the standard output.
    Cat {
        /// Hash of the file.
        hash: DhtAddr,
    },
    /// Removes a file from the store.
    Rm {
        /// Hash of the file.
        hash: DhtAddr,
    },
    /// Announces all files in the store.
    Announce {
        /// Hash of the file to announce. If omitted, all files in the store will be announced.
        hash: Option<DhtAddr>,
    },
}

#[tracing::instrument(name = "run_cli", skip(args))]
pub async fn run(args: Args) {
    tracing::debug!(?args);

    match args.command {
        Command::Init { overwrite } => {
            if let Some(config_dir) = args.config.parent() {
                if let Err(err) = fs::create_dir_all(config_dir) {
                    eprintln!(
                        "failed to create directory '{}': {err}",
                        config_dir.display()
                    );
                    return;
                }
            }
            if let Err(err) = GlobalConfig::init(args.config, overwrite) {
                eprintln!("failed to create config: {err}");
            };
        }
        Command::Daemon => {
            let Ok(config) = read_config_or_report_error(&args.config) else {
                return;
            };
            let Ok(()) = validate_config_for_running_peer(&config) else {
                return;
            };
            tracing::info!("Starting dit daemon");
            run_daemon(config).await.unwrap();
        }
        Command::PingDaemon => {
            let Ok(config) = read_config_or_report_error(&args.config) else {
                return;
            };

            // Connect to the daemon (get socket from toml)
            let Ok(mut connection) = connect_to_daemon_or_report_error(&config.daemon).await else {
                return;
            };

            // Send a message to the daemon
            connection.ping(42).await.unwrap(); // Sending a ping packet with 42 as value

            // FIXME: This should be part of the ping method.
            // If you want to receive a packet (for example a pong) after sending ping
            match connection.receive().await {
                Ok(Packet::Pong(value)) => {
                    println!("Received Pong with value: {}", value);
                }
                Ok(_) => {
                    println!("Received unexpected packet");
                }
                Err(e) => {
                    eprintln!("An error occurred while receiving a packet: {}", e);
                }
            }
        }
        Command::Bootstrap { address } => {
            let Ok(config) = read_config_or_report_error(&args.config) else {
                return;
            };

            // Connect to the daemon (get socket from toml)
            let mut connection = match ConnectionToDaemon::connect(config.daemon.socket_addr).await
            {
                Ok(ok) => ok,
                Err(err) => {
                    eprintln!("error: failed to connect to daemon: {err}");
                    return;
                }
            };

            // Send a message to the daemon
            match connection.bootstrap(address).await {
                Ok(()) => (),
                Err(err) => {
                    eprintln!("error: bootstrapping failed: {err}");
                }
            }
        }
        Command::Add { path } => {
            let Ok(config) = read_config_or_report_error(&args.config) else {
                return;
            };

            let store = Store::open(config.store).unwrap();
            let hash = store.add_file(path).unwrap();
            println!("Added file: {hash}");
        }
        Command::Cat { hash } => {
            let Ok(config) = read_config_or_report_error(&args.config) else {
                return;
            };

            let store = Store::open(config.store).unwrap();
            let mut file = match store.open_file(hash) {
                Ok(file) => file,
                Err(err) => {
                    eprintln!("error: failed to open file for hash {hash}: {err}");
                    return;
                }
            };
            std::io::copy(&mut file, &mut std::io::stdout()).unwrap();
        }
        Command::Rm { hash } => {
            let Ok(config) = read_config_or_report_error(&args.config) else {
                return;
            };

            let store = Store::open(config.store).unwrap();
            match store.remove_file(hash) {
                Ok(file) => file,
                Err(err) => {
                    eprintln!("error: failed to remove file for hash {hash}: {err}");
                }
            };
        }
        Command::Announce { hash } => {
            let Ok(config) = read_config_or_report_error(&args.config) else {
                return;
            };

            // Connect to the daemon (get socket from toml)
            let Ok(mut connection) = connect_to_daemon_or_report_error(&config.daemon).await else {
                return;
            };

            let store = Store::open(config.store).unwrap();

            if let Some(hash) = hash {
                connection.announce(hash).await.unwrap();
            } else {
                let files = store.files().unwrap();
                if files.is_empty() {
                    println!("No files to announce");
                    println!("help: try adding some files with `dit add`");
                } else {
                    for &file in &files {
                        connection.announce(file).await.unwrap();
                    }
                    println!("announced {} files", files.len());
                }
            }
        }
    }
}

pub fn install_default_tracing_subscriber() {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .with_env_var("DIT_LOG")
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(filter).init()
}

/// Indicated that an error has been reported to the user and the program may exit.
struct ErrorReported;

fn read_config_or_report_error(path: &Path) -> Result<GlobalConfig, ErrorReported> {
    GlobalConfig::read(path).map_err(|err| {
        eprintln!("error: {err}");
        ErrorReported
    })
}

async fn connect_to_daemon_or_report_error(
    config: &DaemonConfig,
) -> Result<ConnectionToDaemon, ErrorReported> {
    ConnectionToDaemon::connect(config.socket_addr)
        .await
        .map_err(|err| {
            eprintln!("error: failed to connect to daemon: {err}");
            if err.kind() == ErrorKind::ConnectionRefused {
                eprintln!("help: try starting the daemon with `dit daemon`");
            }
            ErrorReported
        })
}

fn validate_config_for_running_peer(config: &GlobalConfig) -> Result<(), ErrorReported> {
    if config.peer.addrs.socket_addr.ip().is_unspecified() {
        eprintln!("error: invalid address for `peer.listener` in config");
        eprintln!("help: replace `0.0.0.0` with your public ip address");
        return Err(ErrorReported);
    }

    Ok(())
}
