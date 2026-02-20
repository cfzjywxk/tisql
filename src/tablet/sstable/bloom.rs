// Copyright 2024 TiSQL Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::util::error::{Result, TiSqlError};

const BLOOM_MAGIC: u32 = 0x464D4C42; // "BLMF"
const BLOOM_VERSION: u8 = 1;
const BLOOM_HEADER_LEN: usize = 16; // magic(4) + version(1) + k(1) + reserved(2) + bits(4) + bytes(4)
const BLOOM_CHECKSUM_LEN: usize = 4;

fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

fn stable_hash64(seed: u64, key: &[u8]) -> u64 {
    // Use a deterministic hash for persisted bloom filters.
    // `DefaultHasher` is not stable across toolchains and can cause
    // false negatives when old SSTs are read by a newer binary.
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    const MIX1: u64 = 0xff51_afd7_ed55_8ccd;
    const MIX2: u64 = 0xc4ce_b9fe_1a85_ec53;

    let mut h = FNV_OFFSET_BASIS ^ seed;
    for byte in key {
        h ^= *byte as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }

    // Murmur-style finalization to improve bit dispersion.
    h ^= h >> 33;
    h = h.wrapping_mul(MIX1);
    h ^= h >> 33;
    h = h.wrapping_mul(MIX2);
    h ^= h >> 33;
    h
}

#[inline]
fn bloom_hash_pair(key: &[u8]) -> (u64, u64) {
    let h1 = stable_hash64(0x9E37_79B9_7F4A_7C15, key);
    let h2 = stable_hash64(0xC2B2_AE3D_27D4_EB4F, key) | 1;
    (h1, h2)
}

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u32,
    num_hashes: u8,
}

impl BloomFilter {
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.num_bits == 0 || self.num_hashes == 0 {
            return true;
        }
        let (h1, h2) = bloom_hash_pair(key);

        for i in 0..self.num_hashes as u64 {
            let bit = (h1.wrapping_add(i.wrapping_mul(h2)) % self.num_bits as u64) as usize;
            let byte_idx = bit / 8;
            let mask = 1u8 << (bit % 8);
            if self.bits.get(byte_idx).is_none_or(|b| (*b & mask) == 0) {
                return false;
            }
        }
        true
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(BLOOM_HEADER_LEN + self.bits.len() + BLOOM_CHECKSUM_LEN);
        out.extend_from_slice(&BLOOM_MAGIC.to_le_bytes());
        out.push(BLOOM_VERSION);
        out.push(self.num_hashes);
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&self.num_bits.to_le_bytes());
        out.extend_from_slice(&(self.bits.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.bits);
        let checksum = crc32(&out);
        out.extend_from_slice(&checksum.to_le_bytes());
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < BLOOM_HEADER_LEN + BLOOM_CHECKSUM_LEN {
            return Err(TiSqlError::Storage("Bloom block too small".into()));
        }
        let payload_len = data.len() - BLOOM_CHECKSUM_LEN;
        let expected = u32::from_le_bytes(
            data[payload_len..payload_len + BLOOM_CHECKSUM_LEN]
                .try_into()
                .expect("slice length checked"),
        );
        let actual = crc32(&data[..payload_len]);
        if expected != actual {
            return Err(TiSqlError::Storage(format!(
                "Bloom checksum mismatch: expected={expected:#x}, actual={actual:#x}"
            )));
        }

        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != BLOOM_MAGIC {
            return Err(TiSqlError::Storage(format!(
                "Invalid bloom magic: expected {BLOOM_MAGIC:#x}, got {magic:#x}"
            )));
        }
        let version = data[4];
        if version != BLOOM_VERSION {
            return Err(TiSqlError::Storage(format!(
                "Unsupported bloom version: expected {BLOOM_VERSION}, got {version}"
            )));
        }
        let num_hashes = data[5];
        let num_bits = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let bits_len = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;
        if bits_len + BLOOM_HEADER_LEN + BLOOM_CHECKSUM_LEN != data.len() {
            return Err(TiSqlError::Storage(
                "Bloom block length does not match header".into(),
            ));
        }
        if num_hashes == 0 {
            return Err(TiSqlError::Storage(
                "Bloom block has invalid hash count 0".into(),
            ));
        }
        if num_bits == 0 {
            return Err(TiSqlError::Storage(
                "Bloom block has invalid bit count 0".into(),
            ));
        }
        if num_bits as usize > bits_len.saturating_mul(8) {
            return Err(TiSqlError::Storage(
                "Bloom block bit count exceeds payload size".into(),
            ));
        }

        let bits = data[BLOOM_HEADER_LEN..BLOOM_HEADER_LEN + bits_len].to_vec();
        Ok(Self {
            bits,
            num_bits,
            num_hashes,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BloomBuilder {
    bits_per_key: u32,
    hash_pairs: Vec<(u64, u64)>,
}

impl BloomBuilder {
    pub fn new(bits_per_key: u32) -> Self {
        Self {
            bits_per_key,
            hash_pairs: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hash_pairs.is_empty()
    }

    pub fn estimated_encoded_size(&self) -> u64 {
        if self.hash_pairs.is_empty() {
            return 0;
        }
        let num_bits = (self.hash_pairs.len() as u64)
            .saturating_mul(self.bits_per_key.max(1) as u64)
            .max(64)
            .min(u32::MAX as u64);
        let bits_len = (num_bits as usize).div_ceil(8) as u64;
        BLOOM_HEADER_LEN as u64 + bits_len + BLOOM_CHECKSUM_LEN as u64
    }

    pub fn add(&mut self, key: &[u8]) {
        self.hash_pairs.push(bloom_hash_pair(key));
    }

    pub fn finish(self) -> BloomFilter {
        let num_keys = self.hash_pairs.len() as u64;
        let num_bits_u64 = num_keys
            .saturating_mul(self.bits_per_key.max(1) as u64)
            .max(64);
        let num_bits = num_bits_u64.min(u32::MAX as u64) as u32;
        let mut bits = vec![0u8; (num_bits as usize).div_ceil(8)];
        let num_hashes = Self::num_hashes(self.bits_per_key);

        for (h1, h2) in self.hash_pairs {
            for i in 0..num_hashes as u64 {
                let bit = (h1.wrapping_add(i.wrapping_mul(h2)) % num_bits as u64) as usize;
                let byte_idx = bit / 8;
                let mask = 1u8 << (bit % 8);
                bits[byte_idx] |= mask;
            }
        }

        BloomFilter {
            bits,
            num_bits,
            num_hashes,
        }
    }

    fn num_hashes(bits_per_key: u32) -> u8 {
        let k = ((bits_per_key as f64) * 0.69).round() as i32;
        k.clamp(1, 30) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_roundtrip_contains_inserted_keys() {
        let mut builder = BloomBuilder::new(12);
        let keys: Vec<Vec<u8>> = (0..256)
            .map(|i| format!("key-{i:04}").into_bytes())
            .collect();
        for key in &keys {
            builder.add(key);
        }
        let filter = builder.finish();

        for key in &keys {
            assert!(filter.may_contain(key), "missing inserted key {key:?}");
        }

        let encoded = filter.encode();
        let decoded = BloomFilter::decode(&encoded).unwrap();
        for key in &keys {
            assert!(decoded.may_contain(key), "missing decoded key {key:?}");
        }
    }

    #[test]
    fn test_bloom_decode_rejects_corrupted_checksum() {
        let mut builder = BloomBuilder::new(12);
        builder.add(b"hello");
        let mut encoded = builder.finish().encode();

        // Flip one payload byte and keep trailing checksum unchanged.
        encoded[7] ^= 0x01;
        let err = BloomFilter::decode(&encoded).unwrap_err().to_string();
        assert!(err.contains("checksum"), "{err}");
    }

    #[test]
    fn test_bloom_decode_rejects_invalid_num_bits() {
        let mut builder = BloomBuilder::new(12);
        builder.add(b"hello");
        let mut encoded = builder.finish().encode();

        // Force num_bits larger than payload capacity and recompute checksum.
        encoded[8..12].copy_from_slice(&(u32::MAX).to_le_bytes());
        let payload_len = encoded.len() - BLOOM_CHECKSUM_LEN;
        let checksum = crc32(&encoded[..payload_len]);
        encoded[payload_len..].copy_from_slice(&checksum.to_le_bytes());

        let err = BloomFilter::decode(&encoded).unwrap_err().to_string();
        assert!(err.contains("bit count"), "{err}");
    }

    #[test]
    fn test_bloom_decode_rejects_zero_num_hashes() {
        let mut builder = BloomBuilder::new(12);
        builder.add(b"hello");
        let mut encoded = builder.finish().encode();

        encoded[5] = 0;
        let payload_len = encoded.len() - BLOOM_CHECKSUM_LEN;
        let checksum = crc32(&encoded[..payload_len]);
        encoded[payload_len..].copy_from_slice(&checksum.to_le_bytes());

        let err = BloomFilter::decode(&encoded).unwrap_err().to_string();
        assert!(err.contains("hash count"), "{err}");
    }

    #[test]
    fn test_bloom_decode_rejects_invalid_magic() {
        let mut builder = BloomBuilder::new(12);
        builder.add(b"hello");
        let mut encoded = builder.finish().encode();

        encoded[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        let payload_len = encoded.len() - BLOOM_CHECKSUM_LEN;
        let checksum = crc32(&encoded[..payload_len]);
        encoded[payload_len..].copy_from_slice(&checksum.to_le_bytes());

        let err = BloomFilter::decode(&encoded).unwrap_err().to_string();
        assert!(err.contains("magic"), "{err}");
    }

    #[test]
    fn test_bloom_hash_pair_stable() {
        // Guard persisted filter compatibility: hash outputs must remain stable.
        assert_eq!(
            bloom_hash_pair(b"stable-key"),
            (0xba60_731a_a6cc_e203, 0x44c0_c80d_07c8_a327)
        );
        assert_eq!(
            bloom_hash_pair(b"key-0001"),
            (0x11d9_c47f_f549_4e80, 0x8c4a_4369_8f71_6495)
        );
    }

    #[test]
    fn test_bloom_estimated_encoded_size() {
        let mut builder = BloomBuilder::new(12);
        assert_eq!(builder.estimated_encoded_size(), 0);

        builder.add(b"k1");
        builder.add(b"k2");
        let expected = builder.finish().encode().len() as u64;

        let mut builder2 = BloomBuilder::new(12);
        builder2.add(b"k1");
        builder2.add(b"k2");
        assert_eq!(builder2.estimated_encoded_size(), expected);
    }
}
