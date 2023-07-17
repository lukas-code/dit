//! Peer-to-peer communication protocol and distributed hash table implementation.
//!
//! The current implementation is based on chord.

mod proto;
pub mod types;

use self::proto::{Neighbors, Packet, Payload, PayloadKind};
use self::types::{Fingers, SocketAddr};
use crate::codec::Codec;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use std::collections::hash_map::{Entry, HashMap};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{self, Duration};
use tokio_util::codec::Framed;

pub use self::types::{DhtAddr, DhtAndSocketAddr};

type FramedStream = Framed<TcpStream, Codec<Packet>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerConfig {
    pub addrs: DhtAndSocketAddr,
    pub ttl: u32,
    pub connect_timeout: Duration,
    pub response_timeout: Duration,
    pub max_packet_length: u32,
}

#[derive(Debug)]
pub struct Runtime {
    pub controller: Controller,
    pub listener: RemoteListener,
    pub local_peer: LocalPeer,
}

impl Runtime {
    pub async fn new(mut config: PeerConfig) -> io::Result<Self> {
        assert!(
            !config.addrs.socket_addr.ip().is_unspecified(),
            "listener address must be specified",
        );
        let tcp_listener = TcpListener::bind(config.addrs.socket_addr).await?;

        // Replace port 0 with actual port number.
        config.addrs.socket_addr = tcp_listener.local_addr()?;

        let (query_sender, query_receiver) = mpsc::channel(1);
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);

        let config = Arc::new(config);

        let controller = Controller {
            config: config.clone(),
            query_sender,
            event_sender,
            shutdown_receiver,
        };

        let listener = RemoteListener {
            controller: controller.clone(),
            tcp_listener,
        };

        let local_peer = LocalPeer {
            config,
            query_receiver,
            event_receiver,
            shutdown_sender,
            next_connection_id: ConnectionId(0),
            inbound_connections: HashMap::default(),
            next_subscription_id: SubscriptionId(0),
            subscriptions_by_id: HashMap::default(),
            subscriptions_by_addr: HashMap::default(),
            links: Links::default(),
            data: HashMap::new(),
        };

        Ok(Self {
            controller,
            listener,
            local_peer,
        })
    }
}

/// Uniquely identifies a connection between a [`LocalPeer`] and [`RemotePeer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ConnectionId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SubscriptionId(u64);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Links {
    predecessor: Option<DhtAndSocketAddr>,
    successors: Fingers,
}

#[derive(Debug)]
pub struct LocalPeer {
    config: Arc<PeerConfig>,
    query_receiver: mpsc::Receiver<Query>,
    event_receiver: mpsc::UnboundedReceiver<Event>,
    shutdown_sender: watch::Sender<bool>,
    next_connection_id: ConnectionId,
    inbound_connections: HashMap<ConnectionId, SocketAddr>,
    next_subscription_id: SubscriptionId,
    subscriptions_by_id: HashMap<SubscriptionId, SubscriptionData>,
    subscriptions_by_addr: HashMap<DhtAddr, Vec<SubscriptionId>>,
    links: Links,
    data: HashMap<DhtAddr, Vec<SocketAddr>>,
}

impl LocalPeer {
    #[tracing::instrument(name = "run_local", skip(self))]
    pub async fn run(mut self) {
        tracing::debug!(?self.config, "starting local peer");
        loop {
            tokio::select! {
                event = self.event_receiver.recv() => {
                    tracing::trace!(?event, "processing event");
                    match event {
                        None => {
                            // Graceful shutdown completes when all event senders are dropped.
                            let _ = self.shutdown_sender.send(true);
                            tracing::debug!("stopping local peer");
                            return;
                        }
                        Some(Event::Shutdown) => {
                            // Initiate graceful shutdown by closing queries.
                            // This completes `controller.closed()`.
                            self.query_receiver.close();
                        }
                        Some(Event::RemoteDisconnect(id)) => {
                            self.process_remote_disconnect(id);
                        }
                        Some(Event::Unsubscribe(id)) => {
                            self.process_unsubscribe(id);
                        }
                    }
                }
                Some(query) = self.query_receiver.recv() => {
                    tracing::trace!(?query, "processing query");
                    #[allow(clippy::unit_arg)]
                    match query {
                        Query::RemoteConnect(response, socket_addr) => {
                            let _ = response.send(self.process_remote_connect(socket_addr));
                        }
                        Query::GetLinks(response) => {
                            let _ = response.send(self.process_get_links());
                        }
                        Query::AddLink(response, addrs) => {
                            let _ = response.send(self.process_add_link(addrs));
                        }
                        Query::RemoveLink(response, dht_addr) => {
                            let _ = response.send(self.process_remove_link(dht_addr));
                        }
                        Query::NotifySubscribers(response, packet) => {
                            let _ = response.send(self.process_notify_subscribers(packet));
                        }
                        Query::Subscribe(response, dht_addr, predicate) => {
                            let _ = response.send(self.process_subscribe(dht_addr, predicate));
                        }
                        Query::PutData(response, addrs) => {
                            let _ = response.send(self.process_put_data(addrs));
                        }
                        Query::GetData(response, dht_addr) => {
                            let _ = response.send(self.process_get_data(dht_addr));
                        }
                    }
                }
            }
        }
    }

    fn process_remote_connect(&mut self, socket_addr: SocketAddr) -> ConnectionId {
        let id = self.next_connection_id;
        self.next_connection_id =
            ConnectionId(id.0.checked_add(1).expect("connection id overflow"));

        tracing::info!(?id, ?socket_addr, "inbound connection established");

        self.inbound_connections.insert(id, socket_addr);
        id
    }

    fn process_get_links(&mut self) -> Links {
        self.links.clone()
    }

    fn process_add_link(&mut self, new: DhtAndSocketAddr) {
        let self_dht_addr = self.config.addrs.dht_addr;

        if !self.links.predecessor.is_some_and(|old| {
            self_dht_addr.wrapping_sub(old.dht_addr) <= self_dht_addr.wrapping_sub(new.dht_addr)
        }) {
            tracing::debug!(old = ?self.links.predecessor, ?new, "updating predecessor");
            self.links.predecessor = Some(new)
        }

        if let Some(old) = self
            .links
            .successors
            .insert(self.config.addrs.dht_addr, new)
        {
            tracing::debug!(?old, ?new, "updating successor");
        }
    }

    fn process_remove_link(&mut self, dht_addr: DhtAddr) {
        // FIXME: use a let chain
        if let Some(pred) = self.links.predecessor {
            if pred.dht_addr == dht_addr {
                tracing::debug!(?pred, "removing predecessor");
                self.links.predecessor = None;
            }
        }

        if let Some(old) = self
            .links
            .successors
            .remove(self.config.addrs.dht_addr, dht_addr)
        {
            tracing::debug!(?old, "removing successor");
        }
    }

    fn process_notify_subscribers(&mut self, packet: Packet) -> bool {
        if let Entry::Occupied(mut entry_by_addr) =
            self.subscriptions_by_addr.entry(packet.src.dht_addr)
        {
            let ids = entry_by_addr.get_mut();
            debug_assert!(!ids.is_empty());
            ids.retain(|&id| {
                let Entry::Occupied(mut entry_by_id) = self.subscriptions_by_id.entry(id) else {
                    panic!("missing subscription id for notify");
                };
                let matches = (entry_by_id.get_mut().predicate)(&packet.payload);
                if matches {
                    tracing::trace!(?id, "notifying subscriber");
                    let _ = entry_by_id.remove().response.send(packet.clone());
                }
                !matches
            });
            if ids.is_empty() {
                entry_by_addr.remove();
            }
            true
        } else {
            tracing::trace!("no subscribers");
            false
        }
    }

    fn process_subscribe(
        &mut self,
        src_addr: DhtAddr,
        predicate: Box<dyn FnMut(&Payload) -> bool + Send>,
    ) -> (SubscriptionId, oneshot::Receiver<Packet>) {
        let id = self.next_subscription_id;
        self.next_subscription_id =
            SubscriptionId(id.0.checked_add(1).expect("subscription id overflow"));

        let (sender, receiver) = oneshot::channel();
        self.subscriptions_by_id.insert(
            id,
            SubscriptionData {
                response: sender,
                src_addr,
                predicate,
            },
        );
        self.subscriptions_by_addr
            .entry(src_addr)
            .or_default()
            .push(id);

        (id, receiver)
    }

    fn process_remote_disconnect(&mut self, id: ConnectionId) {
        let socket_addr = self
            .inbound_connections
            .remove(&id)
            .expect("missing connection id for disconnect");
        tracing::info!(?id, ?socket_addr, "remote disconnected");
    }

    fn process_unsubscribe(&mut self, id: SubscriptionId) {
        let Some(subscription) = self.subscriptions_by_id.remove(&id) else {
            return;
        };

        let Entry::Occupied(mut entry) = self.subscriptions_by_addr.entry(subscription.src_addr)
        else {
            panic!("missing subscription entry for unsubscribe");
        };

        let vec = entry.get_mut();
        let index = vec
            .iter()
            .position(|&x| x == id)
            .expect("missing subscription id in entries for unsubscribe");
        vec.swap_remove(index);
        if vec.is_empty() {
            entry.remove();
        }
    }

    fn process_put_data(&mut self, addrs: DhtAndSocketAddr) {
        self.data
            .entry(addrs.dht_addr)
            .or_default()
            .push(addrs.socket_addr);
    }

    fn process_get_data(&mut self, dht_addr: DhtAddr) -> Vec<SocketAddr> {
        self.data.get(&dht_addr).cloned().unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct Controller {
    config: Arc<PeerConfig>,
    query_sender: mpsc::Sender<Query>,
    event_sender: mpsc::UnboundedSender<Event>,
    shutdown_receiver: watch::Receiver<bool>,
}

impl Controller {
    pub fn config(&self) -> &PeerConfig {
        &self.config
    }

    /// Completes when the connection to the [`LocalPeer`] has been closed.
    pub async fn closed(&self) {
        self.query_sender.closed().await
    }

    /// Signals the [`LocalPeer`] to shut down and completes after all [`Controller`]s have been dropped.
    pub async fn shutdown(mut self) -> Response<()> {
        self.send_event(Event::Shutdown)?;

        // Shutdown completes when all event senders are dropped.
        drop(self.event_sender);
        self.shutdown_receiver
            .wait_for(|&x| x)
            .await
            .map_err(|_| ConnectionClosed(()))?;
        Ok(())
    }

    /// Returns whether [`Self::shutdown`] has been called on any [`Controller`] belonging to the [`LocalPeer`].
    pub fn is_graceful_shutdown(&self) -> bool {
        self.query_sender.is_closed() && !self.event_sender.is_closed()
    }

    /// Bootstraps the [`LocalPeer`] by sending a `GetNeighbors` packet to its own [`SocketAddr`].
    #[tracing::instrument(skip_all)]
    pub async fn bootstrap(&self, bootstrap_addr: SocketAddr) -> io::Result<()> {
        // TODO: This isn't supported for now, but should probably just be a no-op.
        assert_ne!(bootstrap_addr, self.config.addrs.socket_addr);

        tracing::info!(?bootstrap_addr, "bootstrapping");

        let mut framed = self.connect_socket(bootstrap_addr).await?;

        let self_addr = self.config.addrs.dht_addr;
        let subscription = self
            .query_subscribe(self_addr, |payload| {
                payload.kind() == PayloadKind::NeighborsResponse
            })
            .await?;

        self.send_packet_request(&mut framed, self_addr, Payload::NeighborsRequest)
            .await?;
        self.disconnect(framed).await?;

        let response_packet = subscription.recv().await?;
        let Payload::NeighborsResponse(neighbors) = response_packet.payload else {
            unreachable!()
        };

        if let Some(pred) = neighbors.pred {
            self.query_add_link(pred).await?;
        }

        if let Some(succ) = neighbors.succ {
            self.query_add_link(succ).await?;
        }

        // Add `self` as a successor of `pred`.
        // This needs to happen after adding links so that we can actually route to `pred`.
        if let Some(pred) = neighbors.pred {
            // TODO: Handle the case where `pred` isn't reachable.
            self.ping(pred.dht_addr).await?;
        }

        Ok(())
    }

    /// Pings a [`DhtAddr`].
    #[tracing::instrument(skip_all)]
    pub async fn ping(&self, dst_addr: DhtAddr) -> io::Result<()> {
        tracing::info!(?dst_addr, "pinging");

        let links = self.query_get_links().await?;
        let mut framed = self.connect_dht(dst_addr, &links).await?;

        let ping_data = rand::thread_rng().gen();

        let subscription = self
            .query_subscribe(dst_addr, move |payload| {
                *payload == Payload::Pong(ping_data)
            })
            .await?;

        self.send_packet_request(&mut framed, dst_addr, Payload::Ping(ping_data))
            .await?;
        self.disconnect(framed).await?;

        let response_packet = subscription.recv().await?;
        debug_assert_eq!(response_packet.payload, Payload::Pong(ping_data));

        Ok(())
    }

    /// Stores the [`LocalPeer`]'s [`SocketAddr`] in the DHT at the destination [`DhtAddr`].
    pub async fn announce(&self, dst_addr: DhtAddr) -> io::Result<()> {
        tracing::info!(?dst_addr, "announcing");

        let links = self.query_get_links().await?;
        let mut framed = self.connect_dht(dst_addr, &links).await?;

        let subscription = self
            .query_subscribe(dst_addr, |payload| *payload == Payload::PutResponse)
            .await?;

        self.send_packet_request(&mut framed, dst_addr, Payload::PutRequest)
            .await?;
        self.disconnect(framed).await?;

        let response_packet = subscription.recv().await?;
        debug_assert_eq!(response_packet.payload, Payload::PutResponse);

        Ok(())
    }

    /// Retrieves [`SocketAddr`]s stored in the DHT at the destination [`DhtAddr`].
    pub async fn fetch(&self, dst_addr: DhtAddr) -> io::Result<Vec<SocketAddr>> {
        tracing::info!(?dst_addr, "fetching");

        let links = self.query_get_links().await?;
        let mut framed = self.connect_dht(dst_addr, &links).await?;

        let subscription = self
            .query_subscribe(dst_addr, |payload| {
                payload.kind() == PayloadKind::GetResponse
            })
            .await?;

        self.send_packet_request(&mut framed, dst_addr, Payload::GetRequest)
            .await?;
        self.disconnect(framed).await?;

        let response_packet = subscription.recv().await?;
        let Payload::GetResponse(addrs) = response_packet.payload else {
            unreachable!()
        };

        Ok(addrs)
    }

    async fn query_remote_connect(&self, socket_addr: SocketAddr) -> Response<ConnectionId> {
        self.send_query(|response| Query::RemoteConnect(response, socket_addr))
            .await
    }

    async fn query_get_links(&self) -> Response<Links> {
        #[allow(clippy::redundant_closure)]
        self.send_query(|response| Query::GetLinks(response)).await
    }

    async fn query_add_link(&self, addrs: DhtAndSocketAddr) -> Response<()> {
        assert_ne!(addrs.dht_addr, self.config.addrs.dht_addr);

        self.send_query(|response| Query::AddLink(response, addrs))
            .await
    }

    #[allow(dead_code)] // TODO: use this
    async fn query_remove_link(&self, dht_addr: DhtAddr) -> Response<()> {
        assert_ne!(dht_addr, self.config.addrs.dht_addr);

        self.send_query(|response| Query::RemoveLink(response, dht_addr))
            .await
    }

    async fn query_notify_subscribers(&self, packet: Packet) -> Response<bool> {
        self.send_query(|response| Query::NotifySubscribers(response, packet))
            .await
    }

    async fn query_subscribe(
        &self,
        src_addr: DhtAddr,
        predicate: impl FnMut(&Payload) -> bool + Send + 'static,
    ) -> Response<Subscription> {
        let (id, receiver) = self
            .send_query(|response| Query::Subscribe(response, src_addr, Box::new(predicate)))
            .await?;
        Ok(Subscription::new(self.clone(), id, receiver))
    }

    async fn query_put_data(&self, addrs: DhtAndSocketAddr) -> Response<()> {
        self.send_query(|response| Query::PutData(response, addrs))
            .await
    }

    async fn query_get_data(&self, dht_addr: DhtAddr) -> Response<Vec<SocketAddr>> {
        self.send_query(|response| Query::GetData(response, dht_addr))
            .await
    }

    fn send_event(&self, event: Event) -> Response<()> {
        tracing::trace!(?event, "sending event");
        self.event_sender
            .send(event)
            .map_err(|_| ConnectionClosed(()))
    }

    async fn send_query<T>(
        &self,
        mk_query: impl FnOnce(oneshot::Sender<T>) -> Query,
    ) -> Response<T> {
        let (response_sender, response_receiver) = oneshot::channel();
        let query = mk_query(response_sender);
        tracing::trace!(?query, "sending query");
        self.query_sender
            .send(query)
            .await
            .map_err(|_| ConnectionClosed(()))?;
        response_receiver.await.map_err(|_| ConnectionClosed(()))
    }

    async fn connect_socket(&self, socket_addr: SocketAddr) -> io::Result<FramedStream> {
        let stream_future = TcpStream::connect(socket_addr);
        let stream = time::timeout(self.config.connect_timeout, stream_future).await??;
        tracing::debug!(?socket_addr, "outbound connection established");
        let codec = Codec::new(self.config.max_packet_length);
        Ok(Framed::new(stream, codec))
    }

    async fn connect_dht(&self, dst_addr: DhtAddr, links: &Links) -> io::Result<FramedStream> {
        // Special case for loopback connections, for example for pinging own address.
        if dst_addr == self.config.addrs.dht_addr {
            tracing::trace!("creating loopback connection");
            return self.connect_socket(self.config.addrs.socket_addr).await;
        }

        // TODO: Handle case where peer is not reachable / connection times out.
        let Some(finger_addrs) = links
            .successors
            .get_route(self.config.addrs.dht_addr, dst_addr)
        else {
            tracing::error!("packet doesn't target self and we don't have fingers");
            return Err(NoRoute(()).into());
        };
        tracing::trace!(?finger_addrs, "connecting to finger");
        self.connect_socket(finger_addrs.socket_addr).await
    }

    async fn disconnect(&self, mut framed: FramedStream) -> io::Result<()> {
        let socket_addr = framed.get_ref().peer_addr();
        framed.close().await?;
        if let Some(packet) = framed.next().await.transpose()? {
            tracing::warn!(?packet, ?socket_addr, "received unexpected packet");
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "received unexpected packet",
            ));
        }
        tracing::debug!(?socket_addr, "outbound connection closed");
        Ok(())
    }

    async fn send_packet_request(
        &self,
        framed: &mut FramedStream,
        dst: DhtAddr,
        payload: Payload,
    ) -> io::Result<()> {
        let packet = Packet {
            src: self.config.addrs,
            dst,
            ttl: self.config.ttl,
            payload,
        };
        tracing::trace!(?packet, socket_addr = ?framed.get_ref().peer_addr(), "sending packet (request)");
        framed.send(packet).await
    }

    async fn send_packet_response(
        &self,
        framed: &mut FramedStream,
        request: &Packet,
        payload: Payload,
    ) -> io::Result<()> {
        let packet = Packet {
            src: DhtAndSocketAddr {
                dht_addr: request.dst,
                socket_addr: self.config.addrs.socket_addr,
            },
            dst: request.src.dht_addr,
            ttl: self.config.ttl,
            payload,
        };
        tracing::trace!(?packet, socket_addr = ?framed.get_ref().peer_addr(), "sending packet (response)");
        framed.send(packet).await
    }
}

/// Error returned by methods of [`Controller`] when the connection to the [`LocalPeer`] is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionClosed(());

impl fmt::Display for ConnectionClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("connection closed")
    }
}

impl Error for ConnectionClosed {}

pub type Response<T> = Result<T, ConnectionClosed>;

impl From<ConnectionClosed> for io::Error {
    fn from(err: ConnectionClosed) -> Self {
        Self::new(io::ErrorKind::ConnectionAborted, err)
    }
}

#[derive(Debug)]
pub struct RemoteListener {
    controller: Controller,
    tcp_listener: TcpListener,
}

impl RemoteListener {
    pub async fn accept(&self) -> io::Result<Option<RemotePeer>> {
        tokio::select! {
            biased;
            () = self.controller.closed() => Ok(None),
            result = self.tcp_listener.accept() => {
                let (stream, socket_addr) = result?;
                match self.controller.query_remote_connect(socket_addr).await {
                    Ok(id) => {
                        let controller = self.controller.clone();
                        Ok(Some(RemotePeer::new(controller, id, stream)))
                    }
                    Err(err) => {
                        if self.controller.is_graceful_shutdown() {
                            Ok(None)
                        } else {
                            Err(err.into())
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct RemotePeer {
    guard: RemotePeerGuard,
    stream: TcpStream,
}

#[derive(Debug)]
struct RemotePeerGuard {
    controller: Controller,
    id: ConnectionId,
}

impl Drop for RemotePeerGuard {
    fn drop(&mut self) {
        let _ = self.controller.send_event(Event::RemoteDisconnect(self.id));
    }
}

impl RemotePeer {
    fn new(controller: Controller, id: ConnectionId, stream: TcpStream) -> Self {
        Self {
            guard: RemotePeerGuard { controller, id },
            stream,
        }
    }

    #[tracing::instrument(name = "run_remote", skip(self), fields(self.id))]
    pub async fn run(self) -> io::Result<()> {
        tracing::debug!("starting remote peer");
        let codec = Codec::new(self.guard.controller.config.max_packet_length);
        let mut framed = Framed::new(self.stream, codec);
        loop {
            tokio::select! {
                biased;
                () = self.guard.controller.closed() => return Ok(()),
                packet = framed.next() => {
                    match packet {
                        Some(packet) => self.guard.process_packet(&mut framed, packet?).await?,
                        None => return Ok(()), // remote disconnected
                    }
                }
            }
        }
    }
}

impl RemotePeerGuard {
    /// Handle an inbound packet from an inbound connection.
    ///
    /// If this function returns `Err`, the connection is closed.
    async fn process_packet(
        &self,
        _src_framed: &mut FramedStream,
        mut packet: Packet,
    ) -> io::Result<()> {
        tracing::debug!(?packet, "received packet");
        let old_links = self.controller.query_get_links().await?;
        let notified = self
            .controller
            .query_notify_subscribers(packet.clone())
            .await?;

        let self_addrs = self.controller.config.addrs;
        let packet_src = packet.src;

        // If we don't have a predecessor, assume the packet targets self for now.
        let targets_self = old_links.predecessor.map_or(true, |pred| {
            Self::packet_targets_self(self_addrs.dht_addr, pred.dht_addr, packet.dst)
        });

        let mut new_links = None;
        // TODO: Check if we already have this link and if we don't then ping the peer before adding.
        // For now, we just avoid creating loopback links (from a peer to itself).
        // Loopback connections are allowed (for, e.g. self-pings), but must not change `pred`.
        // Note that the bootstrapping response has src and dst set the DhtAddr of self.
        if packet_src.dht_addr != self_addrs.dht_addr
            && packet_src.socket_addr != self_addrs.socket_addr
        {
            self.controller.query_add_link(packet_src).await?;
            new_links = Some(self.controller.query_get_links().await?);
        }

        if targets_self {
            self.process_self_packet(
                packet,
                &old_links,
                new_links.as_ref().unwrap_or(&old_links),
                notified,
            )
            .await?;
        } else if packet.ttl == 0 {
            tracing::debug!("packet reached end of TTL, discarding it");
        } else {
            packet.ttl -= 1;

            // Packet doesn't target self, forward it.
            // We route with the old links to avoid routing a packet back to it's source.
            let mut finger_framed = self.controller.connect_dht(packet.dst, &old_links).await?;
            tracing::debug!("forwarding packet");
            finger_framed.send(packet).await?;
            self.controller.disconnect(finger_framed).await?;
        }

        Ok(())
    }

    async fn process_self_packet(
        &self,
        packet: Packet,
        old_links: &Links,
        new_links: &Links,
        notified: bool,
    ) -> io::Result<()> {
        tracing::debug!("packet targets self");

        match packet.payload {
            Payload::Ping(n) => {
                let mut dst_framed = self.connect_for_response(&packet, new_links).await?;
                self.controller
                    .send_packet_response(&mut dst_framed, &packet, Payload::Pong(n))
                    .await?;
                self.controller.disconnect(dst_framed).await?;
                Ok(())
            }
            Payload::NeighborsRequest => {
                let mut dst_framed = self.connect_for_response(&packet, new_links).await?;

                let neighbors = Neighbors {
                    pred: old_links.predecessor,
                    succ: Some(self.controller.config.addrs),
                };

                self.controller
                    .send_packet_response(
                        &mut dst_framed,
                        &packet,
                        Payload::NeighborsResponse(neighbors),
                    )
                    .await?;

                self.controller.disconnect(dst_framed).await?;

                Ok(())
            }
            Payload::PutRequest => {
                self.controller
                    .query_put_data(DhtAndSocketAddr {
                        dht_addr: packet.dst,
                        socket_addr: packet.src.socket_addr,
                    })
                    .await?;

                let mut dst_framed = self.connect_for_response(&packet, new_links).await?;
                self.controller
                    .send_packet_response(&mut dst_framed, &packet, Payload::PutResponse)
                    .await?;
                self.controller.disconnect(dst_framed).await?;
                Ok(())
            }
            Payload::GetRequest => {
                let data = self.controller.query_get_data(packet.dst).await?;

                let mut dst_framed = self.connect_for_response(&packet, new_links).await?;
                self.controller
                    .send_packet_response(&mut dst_framed, &packet, Payload::GetResponse(data))
                    .await?;
                self.controller.disconnect(dst_framed).await?;
                Ok(())
            }
            _ => {
                if notified {
                    // A subscriber handled this packet.
                    Ok(())
                } else {
                    tracing::error!(?packet, "unexpected inbound packet");
                    Err(io::ErrorKind::InvalidData.into())
                }
            }
        }
    }

    async fn connect_for_response(
        &self,
        packet: &Packet,
        links: &Links,
    ) -> io::Result<FramedStream> {
        if packet.src.dht_addr == packet.dst {
            // Special case for bootstrapping.
            self.controller.connect_socket(packet.src.socket_addr).await
        } else {
            self.controller
                .connect_dht(packet.src.dht_addr, links)
                .await
        }
    }

    fn packet_targets_self(self_addr: DhtAddr, pred_addr: DhtAddr, dst_addr: DhtAddr) -> bool {
        if pred_addr < self_addr {
            dst_addr > pred_addr && dst_addr <= self_addr
        } else {
            dst_addr > pred_addr || dst_addr <= self_addr
        }
    }
}

/// A peer cannot be reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoRoute(());

impl fmt::Display for NoRoute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no route to target")
    }
}

impl Error for NoRoute {}

impl From<NoRoute> for io::Error {
    fn from(err: NoRoute) -> Self {
        Self::new(io::ErrorKind::Other, err)
    }
}

enum Query {
    RemoteConnect(oneshot::Sender<ConnectionId>, SocketAddr),
    GetLinks(oneshot::Sender<Links>),
    AddLink(oneshot::Sender<()>, DhtAndSocketAddr),
    RemoveLink(oneshot::Sender<()>, DhtAddr),
    NotifySubscribers(oneshot::Sender<bool>, Packet),
    Subscribe(
        oneshot::Sender<(SubscriptionId, oneshot::Receiver<Packet>)>,
        DhtAddr,
        Box<dyn FnMut(&Payload) -> bool + Send>,
    ),
    PutData(oneshot::Sender<()>, DhtAndSocketAddr),
    GetData(oneshot::Sender<Vec<SocketAddr>>, DhtAddr),
}

impl fmt::Debug for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RemoteConnect(_, arg1) => f.debug_tuple("RemoteConnect").field(arg1).finish(),
            Self::GetLinks(_) => f.debug_tuple("GetLinks").finish(),
            Self::AddLink(_, arg1) => f.debug_tuple("AddLink").field(arg1).finish(),
            Self::RemoveLink(_, arg1) => f.debug_tuple("RemoveLink").field(arg1).finish(),
            Self::NotifySubscribers(_, arg1) => {
                f.debug_tuple("NotifySubscribers").field(arg1).finish()
            }
            Self::Subscribe(_, arg1, _) => f.debug_tuple("Subscribe").field(arg1).finish(),
            Self::PutData(_, arg1) => f.debug_tuple("PutData").field(arg1).finish(),
            Self::GetData(_, arg1) => f.debug_tuple("GetData").field(arg1).finish(),
        }
    }
}

#[derive(Debug)]
enum Event {
    Shutdown,
    RemoteDisconnect(ConnectionId),
    Unsubscribe(SubscriptionId),
}

/// "receive" half of a subscription
#[derive(Debug)]
struct Subscription {
    guard: SubscriptionGuard,
    receiver: oneshot::Receiver<Packet>,
}

#[derive(Debug)]
struct SubscriptionGuard {
    controller: Controller,
    id: SubscriptionId,
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        let _ = self.controller.send_event(Event::Unsubscribe(self.id));
    }
}

impl Subscription {
    fn new(
        controller: Controller,
        id: SubscriptionId,
        receiver: oneshot::Receiver<Packet>,
    ) -> Self {
        Self {
            guard: SubscriptionGuard { controller, id },
            receiver,
        }
    }

    async fn recv(self) -> io::Result<Packet> {
        time::timeout(self.guard.controller.config.response_timeout, self.receiver)
            .await?
            .map_err(|_| ConnectionClosed(()).into())
    }
}

/// "send" half of a subscription
struct SubscriptionData {
    response: oneshot::Sender<Packet>,
    src_addr: DhtAddr,
    predicate: Box<dyn FnMut(&Payload) -> bool + Send>,
}

impl fmt::Debug for SubscriptionData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriptionData")
            .field("response", &self.response)
            .field("src_addr", &self.src_addr)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> DhtAddr {
        s.parse().unwrap()
    }

    #[test]
    fn packet_targets_self() {
        {
            let self_addr =
                addr("0000000000000000000000000000000000000000000000000000000000000000");
            let pred_addr =
                addr("8000000000000000000000000000000000000000000000000000000000000000");
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("0000000000000000000000000000000000000000000000000000000000000000"),
            ));
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("0000000000000000000000000000000000000000000000000000000000000001"),
            ));
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("8000000000000000000000000000000000000000000000000000000000000000"),
            ));
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("8000000000000000000000000000000000000000000000000000000000000001"),
            ));
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            ));
        }
        {
            let self_addr =
                addr("8000000000000000000000000000000000000000000000000000000000000000");
            let pred_addr =
                addr("0000000000000000000000000000000000000000000000000000000000000000");
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("0000000000000000000000000000000000000000000000000000000000000000"),
            ));
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("0000000000000000000000000000000000000000000000000000000000000001"),
            ));
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("8000000000000000000000000000000000000000000000000000000000000001"),
            ));
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("8000000000000000000000000000000000000000000000000000000000000000"),
            ));
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            ));
        }
        {
            let self_addr =
                addr("4000000000000000000000000000000000000000000000000000000000000000");
            let pred_addr =
                addr("c000000000000000000000000000000000000000000000000000000000000000");
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("0000000000000000000000000000000000000000000000000000000000000000"),
            ));
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("4000000000000000000000000000000000000000000000000000000000000000"),
            ));
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("4000000000000000000000000000000000000000000000000000000000000001"),
            ));
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("c000000000000000000000000000000000000000000000000000000000000000"),
            ));
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("c000000000000000000000000000000000000000000000000000000000000001"),
            ));
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            ));
        }
        {
            let self_addr =
                addr("c000000000000000000000000000000000000000000000000000000000000000");
            let pred_addr =
                addr("4000000000000000000000000000000000000000000000000000000000000000");
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("0000000000000000000000000000000000000000000000000000000000000000"),
            ));
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("4000000000000000000000000000000000000000000000000000000000000000"),
            ));
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("4000000000000000000000000000000000000000000000000000000000000001"),
            ));
            assert!(RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("c000000000000000000000000000000000000000000000000000000000000000"),
            ));
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("c000000000000000000000000000000000000000000000000000000000000001"),
            ));
            assert!(!RemotePeerGuard::packet_targets_self(
                self_addr,
                pred_addr,
                addr("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            ));
        }
    }
}
