use super::Config;
use super::{DhtAddr, DhtAndSocketAddr, SocketAddr};

use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::io;
use tokio_util::codec::{Decoder, Encoder};

/// A datagram that can be sent to a peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Packet {
    /// The packet targets a [`DhtAddr`] and should be routed though the peer-to-peer network.
    Dht(DhtPacket),
    /// The packet targets a specific peer by its [`SocketAddr`] directly.
    Socket(SocketPacket),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DhtPacket {
    pub src: DhtAndSocketAddr,
    pub dst: DhtAddr,
    pub ttl: u32,
    pub payload: DhtPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DhtPayload {
    Ping(u64),
    Pong(u64),
    NeighborsRequest,
    NeighborsResponse(Neighbors),

    /// Append the `SocketAddr` to the DHT entry at `dst`.
    PutRequest(SocketAddr),

    /// Indicates a successful put of the `SocketAddr` at `src.dht_addr`.
    PutResponse(SocketAddr),

    /// Requests the DHT entry at `dst`.
    GetRequest,

    /// Contains the DHT entry at `src.dht_addr`.
    GetResponse(Vec<SocketAddr>),
}

impl DhtPayload {
    pub fn kind(&self) -> DhtPayloadKind {
        match self {
            DhtPayload::Ping { .. } => DhtPayloadKind::Ping,
            DhtPayload::Pong { .. } => DhtPayloadKind::Pong,
            DhtPayload::NeighborsRequest { .. } => DhtPayloadKind::NeighborsRequest,
            DhtPayload::NeighborsResponse { .. } => DhtPayloadKind::NeighborsResponse,
            DhtPayload::PutRequest { .. } => DhtPayloadKind::PutRequest,
            DhtPayload::PutResponse { .. } => DhtPayloadKind::PutResponse,
            DhtPayload::GetRequest { .. } => DhtPayloadKind::GetRequest,
            DhtPayload::GetResponse { .. } => DhtPayloadKind::GetResponse,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DhtPayloadKind {
    Ping,
    Pong,
    NeighborsRequest,
    NeighborsResponse,
    PutRequest,
    PutResponse,
    GetRequest,
    GetResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SocketPacket {
    pub payload: SocketPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SocketPayload {
    Ping(u64),
    Pong(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Neighbors {
    pub pred: Option<DhtAndSocketAddr>,
    pub succ: Option<DhtAndSocketAddr>,
}

pub struct Codec {
    config: Arc<Config>,
}

impl Codec {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

impl Decoder for Codec {
    type Item = Packet;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut length_bytes = [0; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes);

        if length > self.config.max_packet_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                PacketTooLong(()),
            ));
        }

        let length = length.try_into().unwrap();

        if src.len() < length {
            src.reserve(length - src.len());
            return Ok(None);
        }

        let packet = serde_json::from_slice(&src[4..])?;
        src.advance(length);
        Ok(Some(packet))
    }
}

impl Encoder<Packet> for Codec {
    type Error = io::Error;

    fn encode(&mut self, packet: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded_packet = serde_json::to_vec(&packet)?;

        let length = match encoded_packet
            .len()
            .checked_add(4)
            .and_then(|len| u32::try_from(len).ok())
        {
            Some(length) if length <= self.config.max_packet_length => length,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    PacketTooLong(()),
                ))
            }
        };

        let length_bytes = length.to_be_bytes();
        dst.reserve(encoded_packet.len() + 4);
        dst.extend_from_slice(&length_bytes);
        dst.extend_from_slice(&encoded_packet);

        Ok(())
    }
}

/// Error returned by [`Codec::encode`] and [`codec::decode`] if a packet is longer than the
/// configured maximum length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketTooLong(());

impl fmt::Display for PacketTooLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("packet too long")
    }
}

impl Error for PacketTooLong {}

#[cfg(test)]
mod tests {
    use super::*;

    use futures_util::{SinkExt, StreamExt};
    use std::io::Cursor;
    use tokio_util::codec::Framed;

    #[tokio::test]
    async fn encode_decode_roundtrip() {
        let config = Config {
            addr: DhtAndSocketAddr {
                dht_addr: DhtAddr::random(),
                socket_addr: "1.2.3.4:5678".parse().unwrap(),
            },
            ttl: 42,
            query_queue_size: 1,
            max_packet_length: 1024,
        };

        let test_packet = Packet::Dht(DhtPacket {
            src: DhtAndSocketAddr {
                dht_addr: DhtAddr::random(),
                socket_addr: "[::1]:1".parse().unwrap(),
            },
            dst: DhtAddr::random(),
            ttl: 123,
            payload: DhtPayload::NeighborsResponse(Neighbors {
                pred: Some(DhtAndSocketAddr {
                    dht_addr: DhtAddr::hash(b"pred"),
                    socket_addr: "[FEED::]:0".parse().unwrap(),
                }),
                succ: None,
            }),
        });

        let stream = Cursor::new(Vec::new());
        let codec = Codec::new(Arc::new(config));
        let mut framed = Framed::new(stream, codec);

        framed.send(test_packet.clone()).await.unwrap();
        let bytes_sent = framed.get_ref().position();

        framed.get_mut().set_position(0);
        let received = framed.next().await.unwrap().unwrap();
        let bytes_received = framed.get_ref().position();

        assert_eq!(received, test_packet);
        assert_eq!(bytes_sent, bytes_received);
    }
}
