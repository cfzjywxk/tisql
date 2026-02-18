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

//! Number encoding utilities.
//!
//! This module provides two encoding schemes for numbers:
//!
//! ## Comparable Encoding (for keys)
//! - Signed integers: XOR with sign bit mask for correct ordering
//! - Unsigned integers: Big-endian encoding
//! - Always uses 8 bytes for consistent key length
//!
//! ## Compact Encoding (for row values)
//! - Uses minimum bytes needed to represent the value
//! - Little-endian encoding for efficiency
//! - 1, 2, 4, or 8 bytes depending on value size
//!
//! ## Varint Encoding (for lengths and offsets)
//! - Variable-length encoding for unsigned integers
//! - More space-efficient for small values

use crate::util::error::{Result, TiSqlError};

/// Sign bit mask for i64 -> comparable u64 conversion
const SIGN_MASK: u64 = 0x8000_0000_0000_0000;

// ============================================================================
// Comparable Encoding (for keys, preserves ordering)
// ============================================================================

/// Encode a signed 64-bit integer in comparable format.
/// The encoded bytes compare correctly when sorted lexicographically.
///
/// Algorithm: XOR with sign bit mask, then big-endian encoding.
/// This makes negative numbers sort before positive numbers.
///
/// Example:
/// - -1   -> 0x7FFFFFFFFFFFFFFF
/// - 0    -> 0x8000000000000000
/// - 1    -> 0x8000000000000001
#[inline]
pub fn encode_comparable_i64(buf: &mut Vec<u8>, v: i64) {
    let u = (v as u64) ^ SIGN_MASK;
    buf.extend_from_slice(&u.to_be_bytes());
}

/// Decode a signed 64-bit integer from comparable format.
#[inline]
pub fn decode_comparable_i64(buf: &[u8]) -> Result<(i64, &[u8])> {
    if buf.len() < 8 {
        return Err(TiSqlError::Codec("insufficient bytes for i64".into()));
    }
    let u = u64::from_be_bytes(buf[..8].try_into().unwrap());
    let v = (u ^ SIGN_MASK) as i64;
    Ok((v, &buf[8..]))
}

/// Encode an unsigned 64-bit integer in comparable format.
/// Simply uses big-endian encoding.
#[inline]
pub fn encode_comparable_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// Decode an unsigned 64-bit integer from comparable format.
#[inline]
pub fn decode_comparable_u64(buf: &[u8]) -> Result<(u64, &[u8])> {
    if buf.len() < 8 {
        return Err(TiSqlError::Codec("insufficient bytes for u64".into()));
    }
    let v = u64::from_be_bytes(buf[..8].try_into().unwrap());
    Ok((v, &buf[8..]))
}

/// Encode a signed 32-bit integer in comparable format.
const SIGN_MASK_32: u32 = 0x8000_0000;

#[inline]
pub fn encode_comparable_i32(buf: &mut Vec<u8>, v: i32) {
    let u = (v as u32) ^ SIGN_MASK_32;
    buf.extend_from_slice(&u.to_be_bytes());
}

/// Decode a signed 32-bit integer from comparable format.
#[inline]
pub fn decode_comparable_i32(buf: &[u8]) -> Result<(i32, &[u8])> {
    if buf.len() < 4 {
        return Err(TiSqlError::Codec("insufficient bytes for i32".into()));
    }
    let u = u32::from_be_bytes(buf[..4].try_into().unwrap());
    let v = (u ^ SIGN_MASK_32) as i32;
    Ok((v, &buf[4..]))
}

/// Encode a float64 in comparable format.
/// Uses IEEE 754 bit manipulation to ensure correct ordering.
#[inline]
pub fn encode_comparable_f64(buf: &mut Vec<u8>, v: f64) {
    let bits = v.to_bits();
    // For negative numbers, flip all bits
    // For positive numbers, flip only the sign bit
    let ordered = if bits & SIGN_MASK != 0 {
        !bits
    } else {
        bits ^ SIGN_MASK
    };
    buf.extend_from_slice(&ordered.to_be_bytes());
}

/// Decode a float64 from comparable format.
#[inline]
pub fn decode_comparable_f64(buf: &[u8]) -> Result<(f64, &[u8])> {
    if buf.len() < 8 {
        return Err(TiSqlError::Codec("insufficient bytes for f64".into()));
    }
    let ordered = u64::from_be_bytes(buf[..8].try_into().unwrap());
    let bits = if ordered & SIGN_MASK != 0 {
        ordered ^ SIGN_MASK
    } else {
        !ordered
    };
    Ok((f64::from_bits(bits), &buf[8..]))
}

// ============================================================================
// Compact Encoding (for row values, space-efficient)
// ============================================================================

/// Encode a signed 64-bit integer in compact format.
/// Uses the minimum number of bytes needed.
/// Little-endian encoding for efficiency.
#[inline]
pub fn encode_compact_i64(buf: &mut Vec<u8>, v: i64) {
    if v >= i8::MIN as i64 && v <= i8::MAX as i64 {
        buf.push(v as u8);
    } else if v >= i16::MIN as i64 && v <= i16::MAX as i64 {
        buf.extend_from_slice(&(v as i16).to_le_bytes());
    } else if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
        buf.extend_from_slice(&(v as i32).to_le_bytes());
    } else {
        buf.extend_from_slice(&v.to_le_bytes());
    }
}

/// Get the encoded length of a signed 64-bit integer in compact format.
#[inline]
pub fn compact_i64_len(v: i64) -> usize {
    if v >= i8::MIN as i64 && v <= i8::MAX as i64 {
        1
    } else if v >= i16::MIN as i64 && v <= i16::MAX as i64 {
        2
    } else if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
        4
    } else {
        8
    }
}

/// Decode a signed 64-bit integer from compact format.
/// The caller must provide the correct length.
#[inline]
pub fn decode_compact_i64(buf: &[u8]) -> Result<i64> {
    match buf.len() {
        1 => Ok(buf[0] as i8 as i64),
        2 => Ok(i16::from_le_bytes(buf[..2].try_into().unwrap()) as i64),
        4 => Ok(i32::from_le_bytes(buf[..4].try_into().unwrap()) as i64),
        8 => Ok(i64::from_le_bytes(buf[..8].try_into().unwrap())),
        n => Err(TiSqlError::Codec(format!(
            "invalid compact i64 length: {n}"
        ))),
    }
}

/// Encode an unsigned 64-bit integer in compact format.
#[inline]
pub fn encode_compact_u64(buf: &mut Vec<u8>, v: u64) {
    if v <= u8::MAX as u64 {
        buf.push(v as u8);
    } else if v <= u16::MAX as u64 {
        buf.extend_from_slice(&(v as u16).to_le_bytes());
    } else if v <= u32::MAX as u64 {
        buf.extend_from_slice(&(v as u32).to_le_bytes());
    } else {
        buf.extend_from_slice(&v.to_le_bytes());
    }
}

/// Get the encoded length of an unsigned 64-bit integer in compact format.
#[inline]
pub fn compact_u64_len(v: u64) -> usize {
    if v <= u8::MAX as u64 {
        1
    } else if v <= u16::MAX as u64 {
        2
    } else if v <= u32::MAX as u64 {
        4
    } else {
        8
    }
}

/// Decode an unsigned 64-bit integer from compact format.
#[inline]
pub fn decode_compact_u64(buf: &[u8]) -> Result<u64> {
    match buf.len() {
        1 => Ok(buf[0] as u64),
        2 => Ok(u16::from_le_bytes(buf[..2].try_into().unwrap()) as u64),
        4 => Ok(u32::from_le_bytes(buf[..4].try_into().unwrap()) as u64),
        8 => Ok(u64::from_le_bytes(buf[..8].try_into().unwrap())),
        n => Err(TiSqlError::Codec(format!(
            "invalid compact u64 length: {n}"
        ))),
    }
}

// ============================================================================
// Varint Encoding (for lengths and small values)
// ============================================================================

/// Maximum bytes needed for a varint-encoded u64
pub const MAX_VARINT_LEN: usize = 10;

/// Encode an unsigned integer as varint.
/// Returns the number of bytes written.
#[inline]
pub fn encode_varuint(buf: &mut Vec<u8>, mut v: u64) -> usize {
    let start_len = buf.len();
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
    buf.len() - start_len
}

/// Decode a varint-encoded unsigned integer.
#[inline]
pub fn decode_varuint(buf: &[u8]) -> Result<(u64, &[u8])> {
    let mut result: u64 = 0;
    let mut shift = 0;

    for (i, &b) in buf.iter().enumerate() {
        if i >= MAX_VARINT_LEN {
            return Err(TiSqlError::Codec("varint too long".into()));
        }
        result |= ((b & 0x7F) as u64) << shift;
        if b < 0x80 {
            return Ok((result, &buf[i + 1..]));
        }
        shift += 7;
    }

    Err(TiSqlError::Codec("incomplete varint".into()))
}

/// Encode a signed integer as varint using zigzag encoding.
#[inline]
pub fn encode_varint(buf: &mut Vec<u8>, v: i64) -> usize {
    // Zigzag encoding: (v << 1) ^ (v >> 63)
    let u = ((v << 1) ^ (v >> 63)) as u64;
    encode_varuint(buf, u)
}

/// Decode a zigzag-encoded varint.
#[inline]
pub fn decode_varint(buf: &[u8]) -> Result<(i64, &[u8])> {
    let (u, rest) = decode_varuint(buf)?;
    // Zigzag decoding: (u >> 1) ^ -(u & 1)
    let v = ((u >> 1) as i64) ^ -((u & 1) as i64);
    Ok((v, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comparable_i64_ordering() {
        let values = [-1000i64, -1, 0, 1, 1000];
        let mut encoded: Vec<Vec<u8>> = Vec::new();

        for v in &values {
            let mut buf = Vec::new();
            encode_comparable_i64(&mut buf, *v);
            encoded.push(buf);
        }

        // Verify ordering is preserved
        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "Ordering failed: {:?} should be < {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_comparable_i64_roundtrip() {
        let values = [i64::MIN, -1, 0, 1, i64::MAX];

        for v in values {
            let mut buf = Vec::new();
            encode_comparable_i64(&mut buf, v);
            let (decoded, rest) = decode_comparable_i64(&buf).unwrap();
            assert_eq!(decoded, v);
            assert!(rest.is_empty());
        }
    }

    #[test]
    fn test_comparable_f64_ordering() {
        let values = [-1000.0f64, -1.0, -0.0, 0.0, 1.0, 1000.0];
        let mut encoded: Vec<Vec<u8>> = Vec::new();

        for v in &values {
            let mut buf = Vec::new();
            encode_comparable_f64(&mut buf, *v);
            encoded.push(buf);
        }

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] <= encoded[i + 1],
                "Ordering failed: {:?} should be <= {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_compact_i64() {
        let test_cases = [
            (0i64, 1),
            (127, 1),
            (-128, 1),
            (128, 2),
            (-129, 2),
            (32767, 2),
            (-32768, 2),
            (32768, 4),
            (i32::MAX as i64, 4),
            (i32::MIN as i64, 4),
            (i32::MAX as i64 + 1, 8),
            (i64::MAX, 8),
            (i64::MIN, 8),
        ];

        for (v, expected_len) in test_cases {
            let mut buf = Vec::new();
            encode_compact_i64(&mut buf, v);
            assert_eq!(buf.len(), expected_len, "Length mismatch for {v}");
            assert_eq!(compact_i64_len(v), expected_len);
            let decoded = decode_compact_i64(&buf).unwrap();
            assert_eq!(decoded, v);
        }
    }

    #[test]
    fn test_compact_u64() {
        let test_cases = [
            (0u64, 1),
            (255, 1),
            (256, 2),
            (65535, 2),
            (65536, 4),
            (u32::MAX as u64, 4),
            (u32::MAX as u64 + 1, 8),
            (u64::MAX, 8),
        ];

        for (v, expected_len) in test_cases {
            let mut buf = Vec::new();
            encode_compact_u64(&mut buf, v);
            assert_eq!(buf.len(), expected_len, "Length mismatch for {v}");
            assert_eq!(compact_u64_len(v), expected_len);
            let decoded = decode_compact_u64(&buf).unwrap();
            assert_eq!(decoded, v);
        }
    }

    #[test]
    fn test_varint() {
        let test_cases = [0i64, 1, -1, 127, -128, 1000, -1000, i64::MAX, i64::MIN];

        for v in test_cases {
            let mut buf = Vec::new();
            encode_varint(&mut buf, v);
            let (decoded, rest) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, v);
            assert!(rest.is_empty());
        }
    }

    #[test]
    fn test_varuint() {
        let test_cases = [0u64, 1, 127, 128, 255, 256, 16383, 16384, u64::MAX];

        for v in test_cases {
            let mut buf = Vec::new();
            encode_varuint(&mut buf, v);
            let (decoded, rest) = decode_varuint(&buf).unwrap();
            assert_eq!(decoded, v);
            assert!(rest.is_empty());
        }
    }
}
