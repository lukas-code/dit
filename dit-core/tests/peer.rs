use std::io::{Cursor, ErrorKind};

use rand::seq::IteratorRandom;
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
async fn ping_dht_simple() {
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
        .ping_dht(rt1.controller.config().addrs.dht_addr)
        .await
        .unwrap();
    rt1.controller
        .ping_dht(rt2.controller.config().addrs.dht_addr)
        .await
        .unwrap();

    rt1.controller.shutdown().await.unwrap();
    rt2.controller.shutdown().await.unwrap();
}

#[tokio::test]
#[cfg_attr(windows, ignore = "fails spuriously")] // FIXME
async fn ping_dht_many_sequential() {
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
                .ping_dht(con.config().addrs.dht_addr)
                .await
                .unwrap();
        }

        controllers.push(rt.controller);
    }
}

#[tokio::test]
async fn ping_dht_many_random() {
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
            con.ping_dht(DhtAddr::random()).await.unwrap();
        }
    }
}

#[tokio::test]
async fn announce_fetch() {
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
        let dht_addr = DhtAddr::random();
        let mut sock_addrs = Vec::new();
        let cons = controllers
            .iter()
            .choose_multiple(&mut rand::thread_rng(), 4);
        for con in cons {
            con.announce(dht_addr).await.unwrap();
            sock_addrs.push(con.config().addrs.socket_addr);

            let fetched = controllers
                .iter()
                .choose(&mut rand::thread_rng())
                .unwrap()
                .fetch(dht_addr)
                .await
                .unwrap();
            assert_eq!(fetched, sock_addrs);
        }
    }
}

#[tokio::test]
async fn ping_socket() {
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

    // Don't bootstrap, socket pings don't need dht access.

    rt2.controller
        .ping_socket(rt1.controller.config().addrs.socket_addr)
        .await
        .unwrap();
    rt1.controller
        .ping_socket(rt2.controller.config().addrs.socket_addr)
        .await
        .unwrap();

    rt1.controller.shutdown().await.unwrap();
    rt2.controller.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_transfer() {
    install_tracing_subscriber();

    let content = b"very important data";
    let content_addr = DhtAddr::hash(content);

    let opener = move |addr| (addr == content_addr).then(|| Cursor::new(content));

    let config = get_config(
        "1000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap(),
    );
    let rt1 = Runtime::with_opener(config, opener).await.unwrap();
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

    // Transfer file from rt1 to rt2.
    let mut writer = Cursor::new(Vec::new());
    let file_addrs = DhtAndSocketAddr {
        dht_addr: content_addr,
        socket_addr: rt1.controller.config().addrs.socket_addr,
    };
    rt2.controller
        .recv_file(file_addrs, &mut writer)
        .await
        .unwrap();
    assert_eq!(writer.get_ref(), content);

    // Test file not found.
    let file_addrs = DhtAndSocketAddr {
        dht_addr: DhtAddr::zero(),
        socket_addr: rt1.controller.config().addrs.socket_addr,
    };
    let err = rt2
        .controller
        .recv_file(file_addrs, &mut writer)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);

    rt1.controller.shutdown().await.unwrap();
    rt2.controller.shutdown().await.unwrap();
}
