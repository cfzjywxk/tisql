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

//! Bytes encoding utilities.
//!
//! This module provides two encoding schemes for byte sequences:
//!
//! ## Comparable Encoding (for keys)
//! Uses TiDB/MyRocks memcomparable format:
//! - Bytes are grouped into 8-byte chunks
//! - Each group is followed by a marker byte (0xFF - padding_count)
//! - This ensures lexicographic comparison works correctly
//!
//! Example:
//! - []          -> [0,0,0,0,0,0,0,0,247]
//! - [1,2,3]     -> [1,2,3,0,0,0,0,0,250]
//! - [1,2,3,4,5,6,7,8] -> [1,2,3,4,5,6,7,8,255,0,0,0,0,0,0,0,0,247]
//!
//! ## Compact Encoding (for row values)
//! Simply prefixes data with its length as a varint.
//! More space-efficient but not suitable for key ordering.

use crate::codec::number::{decode_varuint, encode_varuint};
use crate::error::{Result, TiSqlError};

/// Group size for memcomparable encoding (8 bytes per group)
const ENC_GROUP_SIZE: usize = 8;

/// Marker byte (0xFF)
const ENC_MARKER: u8 = 0xFF;

/// Padding byte (0x00)
const ENC_PAD: u8 = 0x00;

// ============================================================================
// Comparable (Memcomparable) Encoding
// ============================================================================

/// Encode bytes in memcomparable format (ascending order).
///
/// Format: [group1][marker1]...[groupN][markerN]
/// - Each group is 8 bytes, padded with 0x00 if needed
/// - Marker is 0xFF - padding_count
///
/// This encoding guarantees that lexicographic comparison of encoded bytes
/// equals the comparison of original bytes.
pub fn encode_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    let data_len = data.len();
    // Preallocate space: (len/8 + 1) * 9 bytes
    let alloc_size = (data_len / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1);
    buf.reserve(alloc_size);

    let mut idx = 0;
    loop {
        let remain = data_len.saturating_sub(idx);
        let pad_count;

        if remain >= ENC_GROUP_SIZE {
            buf.extend_from_slice(&data[idx..idx + ENC_GROUP_SIZE]);
            pad_count = 0;
        } else {
            // Last group - pad with zeros
            if remain > 0 {
                buf.extend_from_slice(&data[idx..]);
            }
            pad_count = ENC_GROUP_SIZE - remain;
            buf.extend(std::iter::repeat_n(ENC_PAD, pad_count));
        }

        // Marker byte
        let marker = ENC_MARKER - (pad_count as u8);
        buf.push(marker);

        if pad_count != 0 {
            break;
        }
        idx += ENC_GROUP_SIZE;
    }
}

/// Get the encoded length of bytes in memcomparable format.
#[inline]
pub fn encoded_bytes_len(data_len: usize) -> usize {
    let mod_size = data_len % ENC_GROUP_SIZE;
    let pad_count = ENC_GROUP_SIZE - mod_size;
    data_len + pad_count + 1 + data_len / ENC_GROUP_SIZE
}

/// Decode bytes from memcomparable format.
/// Returns the remaining buffer and decoded bytes.
pub fn decode_bytes(buf: &[u8]) -> Result<(&[u8], Vec<u8>)> {
    decode_bytes_internal(buf, false)
}

/// Encode bytes in memcomparable format (descending order).
/// Bitwise inverts the encoded bytes for descending comparison.
pub fn encode_bytes_desc(buf: &mut Vec<u8>, data: &[u8]) {
    let start = buf.len();
    encode_bytes(buf, data);
    reverse_bytes(&mut buf[start..]);
}

/// Decode bytes from descending memcomparable format.
pub fn decode_bytes_desc(buf: &[u8]) -> Result<(&[u8], Vec<u8>)> {
    decode_bytes_internal(buf, true)
}

/// Internal decode function that handles both ascending and descending.
fn decode_bytes_internal(mut buf: &[u8], reverse: bool) -> Result<(&[u8], Vec<u8>)> {
    let mut result = Vec::with_capacity(buf.len());

    loop {
        if buf.len() < ENC_GROUP_SIZE + 1 {
            return Err(TiSqlError::Codec(
                "insufficient bytes to decode value".into(),
            ));
        }

        let group = &buf[..ENC_GROUP_SIZE];
        let marker = buf[ENC_GROUP_SIZE];

        let pad_count = if reverse { marker } else { ENC_MARKER - marker };

        if pad_count > ENC_GROUP_SIZE as u8 {
            return Err(TiSqlError::Codec(format!(
                "invalid marker byte: {marker:02x}"
            )));
        }

        let real_group_size = ENC_GROUP_SIZE - pad_count as usize;
        result.extend_from_slice(&group[..real_group_size]);
        buf = &buf[ENC_GROUP_SIZE + 1..];

        if pad_count != 0 {
            // Validate padding bytes
            let pad_byte = if reverse { ENC_MARKER } else { ENC_PAD };
            for &b in &group[real_group_size..] {
                if b != pad_byte {
                    return Err(TiSqlError::Codec(format!(
                        "invalid padding byte: {b:02x}, expected {pad_byte:02x}"
                    )));
                }
            }
            break;
        }
    }

    if reverse {
        reverse_bytes(&mut result);
    }

    Ok((buf, result))
}

/// Bitwise invert all bytes (for descending order encoding).
#[inline]
fn reverse_bytes(buf: &mut [u8]) {
    for b in buf.iter_mut() {
        *b = !*b;
    }
}

// ============================================================================
// Compact Encoding (for row values)
// ============================================================================

/// Encode bytes in compact format.
/// Format: [varint length][data...]
///
/// More space-efficient than memcomparable, but doesn't preserve ordering.
pub fn encode_compact_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    encode_varuint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

/// Decode bytes from compact format.
/// Returns the remaining buffer and decoded bytes.
pub fn decode_compact_bytes(buf: &[u8]) -> Result<(&[u8], Vec<u8>)> {
    let (len, rest) = decode_varuint(buf)?;
    let len = len as usize;

    if rest.len() < len {
        return Err(TiSqlError::Codec(format!(
            "insufficient bytes: expected {}, got {}",
            len,
            rest.len()
        )));
    }

    Ok((&rest[len..], rest[..len].to_vec()))
}

// ============================================================================
// String Encoding (wrappers around bytes encoding)
// ============================================================================

/// Encode a string in memcomparable format.
#[inline]
pub fn encode_string(buf: &mut Vec<u8>, s: &str) {
    encode_bytes(buf, s.as_bytes());
}

/// Decode a string from memcomparable format.
pub fn decode_string(buf: &[u8]) -> Result<(&[u8], String)> {
    let (rest, bytes) = decode_bytes(buf)?;
    let s = String::from_utf8(bytes).map_err(|e| TiSqlError::Codec(e.to_string()))?;
    Ok((rest, s))
}

/// Encode a string in compact format.
#[inline]
pub fn encode_compact_string(buf: &mut Vec<u8>, s: &str) {
    encode_compact_bytes(buf, s.as_bytes());
}

/// Decode a string from compact format.
pub fn decode_compact_string(buf: &[u8]) -> Result<(&[u8], String)> {
    let (rest, bytes) = decode_compact_bytes(buf)?;
    let s = String::from_utf8(bytes).map_err(|e| TiSqlError::Codec(e.to_string()))?;
    Ok((rest, s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_bytes_empty() {
        let mut buf = Vec::new();
        encode_bytes(&mut buf, &[]);
        assert_eq!(buf, vec![0, 0, 0, 0, 0, 0, 0, 0, 247]);
    }

    #[test]
    fn test_encode_bytes_short() {
        let mut buf = Vec::new();
        encode_bytes(&mut buf, &[1, 2, 3]);
        assert_eq!(buf, vec![1, 2, 3, 0, 0, 0, 0, 0, 250]);
    }

    #[test]
    fn test_encode_bytes_exact_group() {
        let mut buf = Vec::new();
        encode_bytes(&mut buf, &[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            buf,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
        );
    }

    #[test]
    fn test_encode_bytes_multi_group() {
        let mut buf = Vec::new();
        encode_bytes(&mut buf, &[1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(
            buf,
            vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248]
        );
    }

    #[test]
    fn test_decode_bytes_roundtrip() {
        let test_cases = [
            vec![],
            vec![1, 2, 3],
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            vec![0; 100],
            (0..=255u8).collect::<Vec<_>>(),
        ];

        for data in test_cases {
            let mut buf = Vec::new();
            encode_bytes(&mut buf, &data);
            let (rest, decoded) = decode_bytes(&buf).unwrap();
            assert!(rest.is_empty());
            assert_eq!(decoded, data);
        }
    }

    #[test]
    fn test_bytes_ordering() {
        let test_cases = [
            vec![],
            vec![0],
            vec![0, 0],
            vec![1],
            vec![1, 0],
            vec![1, 1],
            vec![255],
        ];

        let mut encoded: Vec<Vec<u8>> = Vec::new();
        for data in &test_cases {
            let mut buf = Vec::new();
            encode_bytes(&mut buf, data);
            encoded.push(buf);
        }

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "Ordering failed: {:?} should be < {:?}",
                test_cases[i],
                test_cases[i + 1]
            );
        }
    }

    #[test]
    fn test_encode_bytes_desc() {
        let mut asc = Vec::new();
        let mut desc = Vec::new();

        encode_bytes(&mut asc, b"abc");
        encode_bytes_desc(&mut desc, b"abc");

        // Descending should be bitwise inverse of ascending
        for (a, d) in asc.iter().zip(desc.iter()) {
            assert_eq!(*a, !*d);
        }
    }

    #[test]
    fn test_decode_bytes_desc_roundtrip() {
        let test_cases = [vec![], vec![1, 2, 3], b"hello world".to_vec()];

        for data in test_cases {
            let mut buf = Vec::new();
            encode_bytes_desc(&mut buf, &data);
            let (rest, decoded) = decode_bytes_desc(&buf).unwrap();
            assert!(rest.is_empty());
            assert_eq!(decoded, data);
        }
    }

    #[test]
    fn test_compact_bytes_roundtrip() {
        let test_cases = [
            vec![],
            vec![1, 2, 3],
            (0..1000u16).map(|x| x as u8).collect(),
        ];

        for data in test_cases {
            let mut buf = Vec::new();
            encode_compact_bytes(&mut buf, &data);
            let (rest, decoded) = decode_compact_bytes(&buf).unwrap();
            assert!(rest.is_empty());
            assert_eq!(decoded, data);
        }
    }

    #[test]
    fn test_string_roundtrip() {
        let test_cases = ["", "hello", "世界", "hello\0world"];

        for s in test_cases {
            let mut buf = Vec::new();
            encode_string(&mut buf, s);
            let (rest, decoded) = decode_string(&buf).unwrap();
            assert!(rest.is_empty());
            assert_eq!(decoded, s);
        }
    }

    #[test]
    fn test_compact_string_roundtrip() {
        let test_cases = ["", "hello", "世界"];

        for s in test_cases {
            let mut buf = Vec::new();
            encode_compact_string(&mut buf, s);
            let (rest, decoded) = decode_compact_string(&buf).unwrap();
            assert!(rest.is_empty());
            assert_eq!(decoded, s);
        }
    }

    #[test]
    fn test_encoded_bytes_len() {
        assert_eq!(encoded_bytes_len(0), 9); // 8 padding + 1 marker
        assert_eq!(encoded_bytes_len(1), 9); // 7 padding + 1 marker
        assert_eq!(encoded_bytes_len(8), 18); // 8 data + 1 marker + 8 padding + 1 marker
        assert_eq!(encoded_bytes_len(9), 18); // 8+1 data + 7 padding + 2 markers
    }
}
