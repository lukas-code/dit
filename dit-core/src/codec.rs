use bytes::{Buf, BytesMut};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use tokio::io;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug)]
pub struct Codec<T> {
    max_packet_length: u32,
    marker: PhantomData<T>,
}

impl<T> Copy for Codec<T> {}
impl<T> Clone for Codec<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Codec<T> {
    pub fn new(max_packet_length: u32) -> Self {
        Self {
            max_packet_length,
            marker: PhantomData,
        }
    }
}

impl<T: DeserializeOwned> Decoder for Codec<T> {
    type Item = T;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let mut length_bytes = [0; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes);

        if length > self.max_packet_length {
            return Err(invalid_data(PacketTooLong(())));
        }

        let length = length.try_into().unwrap();

        if src.len() < length {
            src.reserve(length - src.len());
            return Ok(None);
        }

        let packet = rmp_serde::from_slice(&src[4..length]).map_err(invalid_data)?;
        src.advance(length);
        Ok(Some(packet))
    }
}

impl<T: Serialize> Encoder<T> for Codec<T> {
    type Error = io::Error;

    fn encode(&mut self, packet: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded_packet = rmp_serde::to_vec_named(&packet).map_err(invalid_data)?;

        let length = match encoded_packet
            .len()
            .checked_add(4)
            .and_then(|len| u32::try_from(len).ok())
        {
            Some(length) if length <= self.max_packet_length => length,
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

fn invalid_data(error: impl Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// Error returned by [`Codec::encode`] and [`Codec::decode`] if a packet is longer than the
/// configured maximum length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketTooLong(());

impl fmt::Display for PacketTooLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("packet too long")
    }
}

impl Error for PacketTooLong {}
