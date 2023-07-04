pub use std::net::SocketAddr;

use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::str::FromStr;

/// This address uniquely identifies peers and data stored on the distributed hash table.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct DhtAddr(pub [u8; Self::BYTE_LEN]);

impl DhtAddr {
    pub const BYTE_LEN: usize = 32;
    pub const BITS: usize = 8 * Self::BYTE_LEN;

    /// Returns a `DhtAddr` with all bits set to zero.
    pub const fn zero() -> Self {
        Self([0; Self::BYTE_LEN])
    }

    /// Returns 2<sup>`exp`</sup>.
    ///
    /// # Panics
    ///
    /// Panics if `exp >= DhtAddr::BITS`.
    #[track_caller]
    pub const fn pow2(exp: usize) -> Self {
        assert!(exp < Self::BITS, "DhtAddr::pow2: exponent too large");
        let mut value = Self::zero();
        value.0[Self::BYTE_LEN - 1 - (exp / 8)] = 1 << (exp % 8);
        value
    }

    /// Returns a random `DhtAddr`.
    pub fn random() -> Self {
        Self(rand::thread_rng().gen())
    }

    pub fn hash(data: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(data);
        Self(hasher.finalize().into())
    }

    pub fn is_zero(&self) -> bool {
        self.0.iter().all(|&byte| byte == 0)
    }

    /// Returns log<sub>2</sub>(`self`), rounded down.
    ///
    /// # Panics
    ///
    /// Panics if `self == DhtAddr::zero()`.
    #[track_caller]
    pub fn log2(&self) -> usize {
        let (index, &byte) = self
            .0
            .iter()
            .enumerate()
            .find(|(_, &byte)| byte != 0)
            .expect("DhtAddr::log2: argument was zero");
        8 * (Self::BYTE_LEN - 1 - index) + usize::try_from(byte.ilog2()).unwrap()
    }

    pub fn wrapping_sub(mut self, other: Self) -> Self {
        let mut carry = false;
        for (result_byte, other_byte) in self.0.iter_mut().zip(other.0).rev() {
            let (result_1, overflow_1) = result_byte.overflowing_sub(other_byte);
            let (result_2, overflow_2) = result_1.overflowing_sub(carry.into());
            *result_byte = result_2;
            carry = overflow_1 || overflow_2;
        }
        self
    }

    pub fn wrapping_add(mut self, other: Self) -> Self {
        let mut carry = false;
        for (result_byte, other_byte) in self.0.iter_mut().zip(other.0).rev() {
            let (result_1, overflow_1) = result_byte.overflowing_add(other_byte);
            let (result_2, overflow_2) = result_1.overflowing_add(carry.into());
            *result_byte = result_2;
            carry = overflow_1 || overflow_2;
        }
        self
    }

    pub fn wrapping_distance(self, other: Self) -> Self {
        if self >= other {
            self.wrapping_sub(other)
        } else {
            other.wrapping_sub(self)
        }
    }
}

impl fmt::Display for DhtAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for DhtAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DhtAddr({self})")
    }
}

impl FromStr for DhtAddr {
    type Err = ParseDhtAddrError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.len() != 2 * Self::BYTE_LEN {
            return Err(ParseDhtAddrError(()));
        }

        let mut output = [0; Self::BYTE_LEN];
        for (index, tuple) in input.as_bytes().chunks_exact(2).enumerate() {
            let high = match tuple[0] {
                c @ b'0'..=b'9' => c - b'0',
                c @ b'A'..=b'F' => c - (b'A' - 10),
                c @ b'a'..=b'f' => c - (b'a' - 10),
                _ => return Err(ParseDhtAddrError(())),
            };
            let low = match tuple[1] {
                c @ b'0'..=b'9' => c - b'0',
                c @ b'A'..=b'F' => c - (b'A' - 10),
                c @ b'a'..=b'f' => c - (b'a' - 10),
                _ => return Err(ParseDhtAddrError(())),
            };
            output[index] = (high << 4) | low
        }

        Ok(Self(output))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseDhtAddrError(());

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DhtAndSocketAddr {
    pub dht_addr: DhtAddr,
    pub socket_addr: SocketAddr,
}

#[derive(Debug)]
pub struct Fingers {}

impl Fingers {
    #[track_caller]
    pub fn insert(&mut self, index: usize, addrs: DhtAndSocketAddr) {
        assert!(index < DhtAddr::BITS, "Fingers: index out of bounds");
        // btmap.range

    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dht_addr_hash() {
        let addr = DhtAddr::hash(b"hello");
        assert_eq!(
            format!("{addr}"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        );
    }

    #[test]
    fn dht_addr_parse() {
        let addr = DhtAddr([
            0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0x0e, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9,
            0xe2, 0x9e, 0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x04, 0x33, 0x62,
            0x93, 0x8b, 0x98, 0x24,
        ]);
        assert_eq!(
            Ok(addr),
            "2CF24DBA5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".parse()
        );
    }

    #[test]
    fn dht_addr_add_sub() {
        let a = DhtAddr([
            0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0x0e, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9,
            0xe2, 0x9e, 0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x04, 0x33, 0x62,
            0x93, 0x8b, 0x98, 0x24,
        ]);
        let b = DhtAddr([
            0x48, 0x6e, 0xa4, 0x62, 0x24, 0xd1, 0xbb, 0x4f, 0xb6, 0x80, 0xf3, 0x4f, 0x7c, 0x9a,
            0xd9, 0x6a, 0x8f, 0x24, 0xec, 0x88, 0xbe, 0x73, 0xea, 0x8e, 0x5a, 0x6c, 0x65, 0x26,
            0x0e, 0x9c, 0xb8, 0xa7,
        ]);
        let a_minus_b = DhtAddr([
            0xe4, 0x83, 0xa9, 0x58, 0x3a, 0xde, 0xe7, 0xbe, 0x70, 0x67, 0x47, 0xdb, 0x49, 0x1f,
            0x09, 0x33, 0x8b, 0xf1, 0x31, 0xd3, 0x61, 0x33, 0x57, 0xd0, 0x18, 0x97, 0xce, 0x3c,
            0x84, 0xee, 0xdf, 0x7d,
        ]);
        let b_minus_a = DhtAddr([
            0x1b, 0x7c, 0x56, 0xa7, 0xc5, 0x21, 0x18, 0x41, 0x8f, 0x98, 0xb8, 0x24, 0xb6, 0xe0,
            0xf6, 0xcc, 0x74, 0x0e, 0xce, 0x2c, 0x9e, 0xcc, 0xa8, 0x2f, 0xe7, 0x68, 0x31, 0xc3,
            0x7b, 0x11, 0x20, 0x83,
        ]);

        assert_eq!(a.wrapping_sub(a), DhtAddr([0; DhtAddr::BYTE_LEN]));
        assert_eq!(a.wrapping_sub(b), a_minus_b);
        assert_eq!(b.wrapping_sub(a), b_minus_a);

        assert_eq!(b_minus_a.wrapping_add(a), b);
        assert_eq!(a.wrapping_add(b_minus_a), b);
        assert_eq!(a_minus_b.wrapping_add(b), a);
        assert_eq!(b.wrapping_add(a_minus_b), a);
    }

    #[test]
    fn pow2() {
        assert_eq!(
            format!("{}", DhtAddr::pow2(0)),
            "0000000000000000000000000000000000000000000000000000000000000001",
        );

        assert_eq!(
            format!("{}", DhtAddr::pow2(255)),
            "8000000000000000000000000000000000000000000000000000000000000000",
        );
    }

    #[test]
    #[should_panic = "exponent too large"]
    fn pow2_panic() {
        let _ = DhtAddr::pow2(256);
    }

    #[test]
    fn log2() {
        assert_eq!(
            DhtAddr::from_str("0000000000000000000000000000000000000000000000000000000000000001")
                .unwrap()
                .log2(),
            0,
        );

        assert_eq!(
            DhtAddr::from_str("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .unwrap()
                .log2(),
            255,
        );
    }

    #[test]
    #[should_panic = "argument was zero"]
    fn log2_panic() {
        let _ = DhtAddr::zero().log2();
    }
}
