use rand::Rng;
use tokio::time::Duration;

use dit_core::peer::{Controller, DhtAddr, DhtAndSocketAddr, PeerConfig, Runtime};

fn install_tracing_subscriber() {
    let filter = tracing_subscriber::EnvFilter::builder()
        // .with_default_directive(tracing::metadata::LevelFilter::TRACE.into())
        .with_env_var("DIT_LOG")
        .from_env_lossy();
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn get_config(dht_addr: DhtAddr) -> PeerConfig {
    PeerConfig {
        addrs: DhtAndSocketAddr {
            dht_addr,
            socket_addr: ([127, 0, 0, 1], 0).into(),
        },
        ttl: 16,
        connect_timeout: Duration::new(1, 0),
        response_timeout: Duration::new(1, 0),
        max_packet_length: 1024,
    }
}

#[tokio::test]
async fn ping_simple() {
    install_tracing_subscriber();

    let config = get_config(
        "1000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap(),
    );
    let rt1 = Runtime::new(config).await.unwrap();
    tokio::spawn(async move {
        while let Some(peer) = rt1.listener.accept().await.unwrap() {
            tokio::spawn(peer.run());
        }
    });
    tokio::spawn(rt1.local_peer.run());

    let config = get_config(
        "2000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap(),
    );
    let rt2 = Runtime::new(config).await.unwrap();
    tokio::spawn(async move {
        while let Some(peer) = rt2.listener.accept().await.unwrap() {
            tokio::spawn(peer.run());
        }
    });
    tokio::spawn(rt2.local_peer.run());

    rt2.controller
        .bootstrap(rt1.controller.config().addrs.socket_addr)
        .await
        .unwrap();

    rt2.controller
        .ping(rt1.controller.config().addrs.dht_addr)
        .await
        .unwrap();
    rt1.controller
        .ping(rt2.controller.config().addrs.dht_addr)
        .await
        .unwrap();

    rt1.controller.shutdown().await.unwrap();
    rt2.controller.shutdown().await.unwrap();
}

#[tokio::test]
#[cfg_attr(windows, ignore = "fails spuriously")] // FIXME
async fn ping_many_sequential() {
    install_tracing_subscriber();

    let mut controllers = Vec::<Controller>::new();

    for n in 0..64 {
        let config = get_config(DhtAddr([
            n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0,
        ]));
        let rt = Runtime::new(config).await.unwrap();
        tokio::spawn(async move {
            while let Some(peer) = rt.listener.accept().await.unwrap() {
                tokio::spawn(peer.run());
            }
        });
        tokio::spawn(rt.local_peer.run());

        if let Some(prev) = controllers.last() {
            rt.controller
                .bootstrap(prev.config().addrs.socket_addr)
                .await
                .unwrap();
        }

        for con in controllers.iter() {
            rt.controller
                .ping(con.config().addrs.dht_addr)
                .await
                .unwrap();
        }

        controllers.push(rt.controller);
    }
}

#[tokio::test]
async fn ping_many_random() {
    install_tracing_subscriber();

    let mut controllers = Vec::<Controller>::new();

    for _ in 0..64 {
        let config = get_config(DhtAddr::random());
        let rt = Runtime::new(config).await.unwrap();
        tokio::spawn(async move {
            while let Some(peer) = rt.listener.accept().await.unwrap() {
                tokio::spawn(peer.run());
            }
        });
        tokio::spawn(rt.local_peer.run());

        if !controllers.is_empty() {
            let index = rand::thread_rng().gen_range(0..controllers.len());
            rt.controller
                .bootstrap(controllers[index].config().addrs.socket_addr)
                .await
                .unwrap();
        }

        controllers.push(rt.controller);
    }

    for _ in 0..16 {
        for con in controllers.iter() {
            con.ping(DhtAddr::random()).await.unwrap();
        }
    }
}
