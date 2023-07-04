//! Peer-to-peer communication protocol and distributed hash table implementation.
//!
//! The current implementation is based on chord.

mod proto;
pub mod types;

use crate::peer::proto::SocketPacket;

use self::proto::{Codec, DhtPacket, DhtPayload, DhtPayloadKind, Neighbors, Packet, SocketPayload};
pub use self::types::{DhtAddr, DhtAddrRange, DhtAndSocketAddr, Fingers, SocketAddr};

use futures_util::{SinkExt, StreamExt};
use std::collections::hash_map::{Entry, HashMap};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::codec::Framed;

#[derive(Debug)]
pub struct Config {
    pub addr: DhtAndSocketAddr,
    pub ttl: u32,
    pub query_queue_size: usize,
    pub max_packet_length: u32,
}

#[derive(Debug)]
pub struct Runtime {
    pub controller: Controller,
    pub listener: Listener,
    pub local_peer: LocalPeer,
}

impl Runtime {
    pub async fn new(config: Config) -> io::Result<Self> {
        let tcp_listener = TcpListener::bind(config.addr.socket_addr).await?;

        let (query_sender, query_receiver) = mpsc::channel(config.query_queue_size);
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let (shutdown_sender, shutdown_receiver) = watch::channel(false);

        let config = Arc::new(config);

        let controller = Controller {
            config: config.clone(),
            query_sender,
            event_sender,
            shutdown_receiver,
        };

        let listener = Listener {
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
            subscriptions_by_kind: HashMap::default(),
            links: Links::default(),
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

#[derive(Debug, Clone, Default)]
struct Links {
    predecessor: Option<DhtAndSocketAddr>,
    successors: Fingers,
}

#[derive(Debug)]
pub struct LocalPeer {
    config: Arc<Config>,
    query_receiver: mpsc::Receiver<Query>,
    event_receiver: mpsc::UnboundedReceiver<Event>,
    shutdown_sender: watch::Sender<bool>,
    next_connection_id: ConnectionId,
    inbound_connections: HashMap<ConnectionId, SocketAddr>,
    next_subscription_id: SubscriptionId,
    subscriptions_by_id:
        HashMap<SubscriptionId, (oneshot::Sender<DhtPacket>, DhtAddr, DhtPayloadKind)>,
    subscriptions_by_kind: HashMap<(DhtAddr, DhtPayloadKind), Vec<SubscriptionId>>,
    links: Links,
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
                        Query::Subscribe(response, dht_addr, payload_kind) => {
                            let _ = response.send(self.process_subscribe(dht_addr, payload_kind));
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
        let keep_old = |old: DhtAndSocketAddr| {
            self.config.addr.dht_addr.wrapping_distance(old.dht_addr)
                <= self.config.addr.dht_addr.wrapping_distance(new.dht_addr)
        };

        if !self.links.predecessor.is_some_and(keep_old) {
            tracing::debug!(old = ?self.links.predecessor, ?new, "updating predecessor");
            self.links.predecessor = Some(new)
        }

        let finger_index = Fingers::index_of(self.config.addr.dht_addr, new.dht_addr);
        let old = self.links.successors.insert(finger_index, new);
        if old != Some(new) {
            tracing::debug!(?old, ?new, ?finger_index, "updating successor");
        }
    }

    fn process_remove_link(&mut self, dht_addr: DhtAddr) {
        // TODO: remove pred, remove fingers
        todo!()
    }

    fn process_notify_subscribers(&mut self, packet: DhtPacket) {
        let kind = (packet.dst, packet.payload.kind());
        if let Some(ids) = self.subscriptions_by_kind.remove(&kind) {
            debug_assert!(!ids.is_empty());
            tracing::trace!("notifying {} subscribers for {kind:?}", ids.len());
            for id in ids {
                let (sender, ..) = self
                    .subscriptions_by_id
                    .remove(&id)
                    .expect("missing subscription id for notify");
                let _ = sender.send(packet.clone());
            }
        } else {
            tracing::trace!("no subscribers for {kind:?}");
        }
    }

    fn process_subscribe(
        &mut self,
        dht_addr: DhtAddr,
        payload_kind: DhtPayloadKind,
    ) -> (SubscriptionId, oneshot::Receiver<DhtPacket>) {
        let id = self.next_subscription_id;
        self.next_subscription_id =
            SubscriptionId(id.0.checked_add(1).expect("subscription id overflow"));

        let (sender, receiver) = oneshot::channel();
        self.subscriptions_by_id
            .insert(id, (sender, dht_addr, payload_kind));
        self.subscriptions_by_kind
            .entry((dht_addr, payload_kind))
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
        let Some((_, dht_addr, payload_kind)) = self
            .subscriptions_by_id
            .remove(&id)
        else {
            return;
        };

        let Entry::Occupied(mut entry) = self.subscriptions_by_kind.entry((dht_addr, payload_kind)) else {
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
}

#[derive(Debug, Clone)]
pub struct Controller {
    config: Arc<Config>,
    query_sender: mpsc::Sender<Query>,
    event_sender: mpsc::UnboundedSender<Event>,
    shutdown_receiver: watch::Receiver<bool>,
}

impl Controller {
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
    pub async fn bootstrap(&self, bootstrap_addr: SocketAddr) -> io::Result<()> {
        tracing::info!(?bootstrap_addr, "bootstrapping");

        let mut framed = self.connect(bootstrap_addr).await?;

        let self_addr = self.config.addr.dht_addr;
        let subscription = self
            .query_subscribe(self_addr, DhtPayloadKind::NeighborsResponse)
            .await?;

        self.send_dht_packet(
            &mut framed,
            self_addr,
            self_addr,
            DhtPayload::NeighborsRequest,
        )
        .await?;

        let response_packet = subscription.recv().await?; // <- TODO: timeout
        let DhtPayload::NeighborsResponse(neighbors) = response_packet.payload else {
            unreachable!()
        };

        self.query_add_link(response_packet.src).await?;

        if let Some(pred) = neighbors.pred {
            self.query_add_link(pred).await?;
        }

        if let Some(succ) = neighbors.succ {
            self.query_add_link(succ).await?;
        }

        Ok(())
    }

    /// Stores the [`LocalPeer`]'s [`SocketAddr`] in the DHT at [`DhtAddr`].
    pub async fn announce(&self, dht_addr: DhtAddr) -> io::Result<()> {
        todo!()
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
        self.send_query(|response| Query::AddLink(response, addrs))
            .await
    }

    async fn query_remove_link(&self, dht_addr: DhtAddr) -> Response<()> {
        self.send_query(|response| Query::RemoveLink(response, dht_addr))
            .await
    }

    async fn query_notify_subscribers(&self, packet: DhtPacket) -> Response<()> {
        self.send_query(|response| Query::NotifySubscribers(response, packet))
            .await
    }

    async fn query_subscribe(&self, addr: DhtAddr, kind: DhtPayloadKind) -> Response<Subscription> {
        let (id, receiver) = self
            .send_query(|response| Query::Subscribe(response, addr, kind))
            .await?;
        Ok(Subscription::new(self.clone(), id, receiver))
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

    async fn connect(&self, socket_addr: SocketAddr) -> io::Result<Framed<TcpStream, Codec>> {
        let stream = TcpStream::connect(socket_addr).await?;
        tracing::debug!(?socket_addr, "outbound connection established");
        let codec = Codec::new(self.config.clone());
        Ok(Framed::new(stream, codec))
    }

    async fn disconnect(
        &self,
        mut framed: Framed<TcpStream, Codec>,
    ) -> io::Result<()> {
        framed.close().await?;
        framed.next().await.transpose()?;
        Ok(())
    }

    async fn send_dht_packet(
        &self,
        framed: &mut Framed<TcpStream, Codec>,
        src: DhtAddr,
        dst: DhtAddr,
        payload: DhtPayload,
    ) -> io::Result<()> {
        let packet = Packet::Dht(DhtPacket {
            src: DhtAndSocketAddr {
                dht_addr: src,
                socket_addr: self.config.addr.socket_addr,
            },
            dst,
            ttl: self.config.ttl,
            payload,
        });
        tracing::trace!(?packet, socket_addr = ?framed.get_ref().peer_addr(), "sending packet");
        framed.send(packet).await
    }

    async fn send_socket_packet(
        &self,
        framed: &mut Framed<TcpStream, Codec>,
        payload: SocketPayload,
    ) -> io::Result<()> {
        let packet = Packet::Socket(SocketPacket { payload });
        tracing::trace!(?packet, socket_addr = ?framed.get_ref().peer_addr(), "sending packet");
        framed.send(packet).await
    }

    async fn recv_socket_packet(
        &self,
        framed: &mut Framed<TcpStream, Codec>,
    ) -> io::Result<SocketPacket> {
        let Some(result) = framed.next().await else {
            return Err(io::ErrorKind::UnexpectedEof.into())
        };
        let Packet::Socket(packet) = result? else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "expected socket packet, got dht packet"));
        };
        tracing::trace!(?packet, socket_addr = ?framed.get_ref().peer_addr(), "received packet");
        Ok(packet)
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
pub struct Listener {
    controller: Controller,
    tcp_listener: TcpListener,
}

impl Listener {
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
    pub async fn run(mut self) -> io::Result<()> {
        tracing::debug!("starting remote peer");
        let codec = Codec::new(self.guard.controller.config.clone());
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
    async fn process_packet(
        &mut self,
        src_framed: &mut Framed<TcpStream, Codec>,
        packet: Packet,
    ) -> io::Result<()> {
        tracing::debug!(?packet, "received packet");
        match packet {
            Packet::Dht(inner) => self.process_dht_packet(inner).await,
            Packet::Socket(inner) => self.process_socket_packet(src_framed, inner).await,
        }
    }

    async fn process_dht_packet(&mut self, packet: DhtPacket) -> io::Result<()> {
        self.controller
            .query_notify_subscribers(packet.clone())
            .await?;

        let links = self.controller.query_get_links().await?;

        // TODO:
        // * check links to see if this query is necessary
        // * ping peer to make sure it's actually reachable
        self.controller.query_add_link(packet.src).await?;

        // // TODO: do something with packet, forward it if needed, delete link if stale
        // // (the following is a placeholder to get the basics working)
        // if packet.payload.kind() == DhtPayloadKind::NeighborsRequest {
        //     let mut dst_framed = self.controller.connect(packet.src.socket_addr).await?;

        //     self.controller
        //         .send_packet(
        //             &mut dst_framed,
        //             packet.src.dht_addr,
        //             packet.src.dht_addr,
        //             DhtPayload::NeighborsResponse(neighbors),
        //         )
        //         .await?;
        // }

        Ok(())
    }

    async fn process_socket_packet(
        &mut self,
        src_framed: &mut Framed<TcpStream, Codec>,
        packet: SocketPacket,
    ) -> io::Result<()> {
        match packet.payload {
            SocketPayload::Ping(n) => {
                self.controller
                    .send_socket_packet(src_framed, SocketPayload::Pong(n))
                    .await?;
            }
            SocketPayload::Pong(_) => {}
        }
        Ok(())
    }
}

#[derive(Debug)]
enum Query {
    RemoteConnect(oneshot::Sender<ConnectionId>, SocketAddr),
    GetLinks(oneshot::Sender<Links>),
    AddLink(oneshot::Sender<()>, DhtAndSocketAddr),
    RemoveLink(oneshot::Sender<()>, DhtAddr),
    NotifySubscribers(oneshot::Sender<()>, DhtPacket),
    Subscribe(
        oneshot::Sender<(SubscriptionId, oneshot::Receiver<DhtPacket>)>,
        DhtAddr,
        DhtPayloadKind,
    ),
}

#[derive(Debug)]
enum Event {
    Shutdown,
    RemoteDisconnect(ConnectionId),
    Unsubscribe(SubscriptionId),
}

#[derive(Debug)]
struct Subscription {
    _guard: SubscriptionGuard,
    receiver: oneshot::Receiver<DhtPacket>,
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
        receiver: oneshot::Receiver<DhtPacket>,
    ) -> Self {
        Self {
            _guard: SubscriptionGuard { controller, id },
            receiver,
        }
    }

    async fn recv(self) -> Response<DhtPacket> {
        self.receiver.await.map_err(|_| ConnectionClosed(()))
    }
}
