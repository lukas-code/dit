#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
#![deny(rustdoc::broken_intra_doc_links)]

use clap::{Parser, Subcommand};
use dit_core::peer::{Config, DhtAddr, DhtAndSocketAddr, Runtime};
use tokio::io;
use tracing::Instrument;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Starts the daemon.
    Daemon,
}

#[tracing::instrument(name = "run_cli", skip(args))]
pub async fn run(args: Args) {
    tracing::debug!(?args);

    match args.command {
        Command::Daemon => {
            let config1 = Config {
                addr: DhtAndSocketAddr {
                    dht_addr: DhtAddr::random(),
                    socket_addr: "127.0.0.1:6660".parse().unwrap(),
                },
                ttl: 8,
                query_queue_size: 1,
                max_packet_length: 1024,
            };

            let config2 = Config {
                addr: DhtAndSocketAddr {
                    dht_addr: DhtAddr::random(),
                    socket_addr: "127.0.0.1:6661".parse().unwrap(),
                },
                ttl: 8,
                query_queue_size: 1,
                max_packet_length: 1024,
            };

            let rt1 = Runtime::new(config1).await.unwrap();
            let rt2 = Runtime::new(config2).await.unwrap();

            let listener1 = tokio::spawn(
                async move {
                    loop {
                        let Some(remote) = rt1.listener.accept().await? else {
                            return Ok::<(), io::Error>(());
                        };

                        tokio::spawn(remote.run().in_current_span());
                    }
                }
                .instrument(tracing::debug_span!("listener 1")),
            );

            let listener2 = tokio::spawn(
                async move {
                    loop {
                        let Some(remote) = rt2.listener.accept().await? else {
                            return Ok::<(), io::Error>(());
                        };

                        tokio::spawn(remote.run().in_current_span());
                    }
                }
                .instrument(tracing::debug_span!("listener 2")),
            );

            let local1 = tokio::spawn(
                rt1.local_peer
                    .run()
                    .instrument(tracing::debug_span!("local 1")),
            );
            let local2 = tokio::spawn(
                rt2.local_peer
                    .run()
                    .instrument(tracing::debug_span!("local 2")),
            );

            rt1.controller
                .bootstrap("127.0.0.1:6661".parse().unwrap())
                .await
                .unwrap();

            rt1.controller.shutdown().await.unwrap();
            rt2.controller.shutdown().await.unwrap();

            local1.await.unwrap();
            local2.await.unwrap();
            listener1.await.unwrap().unwrap();
            listener2.await.unwrap().unwrap();
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
