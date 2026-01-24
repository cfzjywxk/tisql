//! Table key encoding utilities.
//!
//! This module provides key encoding following TiDB's tablecodec format:
//!
//! ## Record Key Format
//! ```text
//! 't' + tableID (8 bytes) + "_r" + handle (8+ bytes)
//! ```
//!
//! ## Index Key Format
//! ```text
//! 't' + tableID (8 bytes) + "_i" + indexID (8 bytes) + indexValues...
//! ```
//!
//! All integer components use comparable encoding (big-endian with sign bit XOR)
//! to ensure correct lexicographic ordering.

use crate::codec::bytes::encode_bytes;
use crate::codec::number::{decode_comparable_i64, encode_comparable_i64};
use crate::error::{Result, TiSqlError};
use crate::types::{IndexId, TableId, Value};

/// Table prefix byte: 't'
pub const TABLE_PREFIX: u8 = b't';

/// Meta prefix byte: 'm'
pub const META_PREFIX: u8 = b'm';

/// Record prefix separator: "_r"
pub const RECORD_PREFIX_SEP: &[u8] = b"_r";

/// Index prefix separator: "_i"
pub const INDEX_PREFIX_SEP: &[u8] = b"_i";

/// Length of table ID in bytes
const ID_LEN: usize = 8;

/// Length of the common prefix: 't' + tableID + "_r" or "_i"
const PREFIX_LEN: usize = 1 + ID_LEN + 2;

/// Length of a standard record row key with int handle
const RECORD_ROW_KEY_LEN: usize = PREFIX_LEN + ID_LEN;

/// Table prefix utilities
#[derive(Debug, Clone)]
pub struct TablePrefix;

impl TablePrefix {
    /// Get the table prefix bytes
    #[inline]
    pub fn bytes() -> &'static [u8] {
        &[TABLE_PREFIX]
    }

    /// Check if a key starts with the table prefix
    #[inline]
    pub fn has_prefix(key: &[u8]) -> bool {
        !key.is_empty() && key[0] == TABLE_PREFIX
    }
}

// ============================================================================
// Record Key Encoding
// ============================================================================

/// Encode a record key with pre-encoded handle.
///
/// Format: 't' + tableID + "_r" + encoded_handle
pub fn encode_record_key(table_id: TableId, encoded_handle: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PREFIX_LEN + encoded_handle.len());
    append_table_record_prefix(&mut buf, table_id);
    buf.extend_from_slice(encoded_handle);
    buf
}

/// Encode a record key with an integer handle.
///
/// Format: 't' + tableID + "_r" + handle (as comparable i64)
pub fn encode_record_key_with_handle(table_id: TableId, handle: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(RECORD_ROW_KEY_LEN);
    append_table_record_prefix(&mut buf, table_id);
    encode_comparable_i64(&mut buf, handle);
    buf
}

/// Decode a record key to get table ID and handle.
pub fn decode_record_key(key: &[u8]) -> Result<(TableId, Handle)> {
    if key.len() < PREFIX_LEN {
        return Err(TiSqlError::Codec("record key too short".into()));
    }

    if key[0] != TABLE_PREFIX {
        return Err(TiSqlError::Codec("invalid record key prefix".into()));
    }

    // Decode table ID
    let (table_id, rest) = decode_comparable_i64(&key[1..])?;

    // Check record prefix separator
    if rest.len() < 2 || &rest[..2] != RECORD_PREFIX_SEP {
        return Err(TiSqlError::Codec(
            "invalid record prefix separator".into(),
        ));
    }

    let handle_bytes = &rest[2..];

    // Decode handle
    if handle_bytes.len() == 8 {
        // Int handle
        let (handle, _) = decode_comparable_i64(handle_bytes)?;
        Ok((table_id as TableId, Handle::Int(handle)))
    } else {
        // Common handle (composite key)
        Ok((
            table_id as TableId,
            Handle::Common(handle_bytes.to_vec()),
        ))
    }
}

/// Append table record prefix: 't' + tableID + "_r"
fn append_table_record_prefix(buf: &mut Vec<u8>, table_id: TableId) {
    buf.push(TABLE_PREFIX);
    encode_comparable_i64(buf, table_id as i64);
    buf.extend_from_slice(RECORD_PREFIX_SEP);
}

/// Generate table record prefix for scanning.
pub fn gen_table_record_prefix(table_id: TableId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PREFIX_LEN);
    append_table_record_prefix(&mut buf, table_id);
    buf
}

/// Cut the record prefix from a key, returning just the handle portion.
#[inline]
pub fn cut_row_key_prefix(key: &[u8]) -> &[u8] {
    if key.len() > PREFIX_LEN {
        &key[PREFIX_LEN..]
    } else {
        &[]
    }
}

// ============================================================================
// Index Key Encoding
// ============================================================================

/// Encode an index seek key.
///
/// Format: 't' + tableID + "_i" + indexID + encoded_values...
pub fn encode_index_seek_key(
    table_id: TableId,
    index_id: IndexId,
    encoded_values: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PREFIX_LEN + ID_LEN + encoded_values.len());
    buf.push(TABLE_PREFIX);
    encode_comparable_i64(&mut buf, table_id as i64);
    buf.extend_from_slice(INDEX_PREFIX_SEP);
    encode_comparable_i64(&mut buf, index_id as i64);
    buf.extend_from_slice(encoded_values);
    buf
}

/// Generate index prefix for scanning.
pub fn gen_table_index_prefix(table_id: TableId, index_id: IndexId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PREFIX_LEN + ID_LEN);
    buf.push(TABLE_PREFIX);
    encode_comparable_i64(&mut buf, table_id as i64);
    buf.extend_from_slice(INDEX_PREFIX_SEP);
    encode_comparable_i64(&mut buf, index_id as i64);
    buf
}

/// Decode an index key to get table ID and index ID.
pub fn decode_index_key(key: &[u8]) -> Result<(TableId, IndexId, &[u8])> {
    if key.len() < PREFIX_LEN + ID_LEN {
        return Err(TiSqlError::Codec("index key too short".into()));
    }

    if key[0] != TABLE_PREFIX {
        return Err(TiSqlError::Codec("invalid index key prefix".into()));
    }

    // Decode table ID
    let (table_id, rest) = decode_comparable_i64(&key[1..])?;

    // Check index prefix separator
    if rest.len() < 2 || &rest[..2] != INDEX_PREFIX_SEP {
        return Err(TiSqlError::Codec("invalid index prefix separator".into()));
    }

    // Decode index ID
    let (index_id, rest) = decode_comparable_i64(&rest[2..])?;

    Ok((table_id as TableId, index_id as IndexId, rest))
}

// ============================================================================
// Handle Types
// ============================================================================

/// Handle type for record keys.
/// Int handles are the common case, Common handles are for composite primary keys.
#[derive(Debug, Clone, PartialEq)]
pub enum Handle {
    /// Integer handle (single column primary key)
    Int(i64),
    /// Common handle (composite primary key, stored as encoded bytes)
    Common(Vec<u8>),
}

impl Handle {
    /// Create an integer handle.
    #[inline]
    pub fn int(v: i64) -> Self {
        Handle::Int(v)
    }

    /// Get the encoded bytes of this handle.
    pub fn encoded(&self) -> Vec<u8> {
        match self {
            Handle::Int(v) => {
                let mut buf = Vec::with_capacity(8);
                encode_comparable_i64(&mut buf, *v);
                buf
            }
            Handle::Common(bytes) => bytes.clone(),
        }
    }

    /// Get the length of the encoded handle.
    pub fn len(&self) -> usize {
        match self {
            Handle::Int(_) => 8,
            Handle::Common(bytes) => bytes.len(),
        }
    }

    /// Check if handle is empty (only possible for Common handle).
    pub fn is_empty(&self) -> bool {
        match self {
            Handle::Int(_) => false,
            Handle::Common(bytes) => bytes.is_empty(),
        }
    }
}

// ============================================================================
// Value Encoding for Keys
// ============================================================================

/// Encode a value for use in a key (memcomparable format).
pub fn encode_value_for_key(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => {
            buf.push(0x00); // Null sorts first
        }
        Value::Boolean(b) => {
            buf.push(0x01);
            buf.push(if *b { 1 } else { 0 });
        }
        Value::TinyInt(v) => {
            buf.push(0x02);
            // Convert to comparable format
            buf.push((*v as u8) ^ 0x80);
        }
        Value::SmallInt(v) => {
            buf.push(0x03);
            let bytes = ((*v as u16) ^ 0x8000).to_be_bytes();
            buf.extend_from_slice(&bytes);
        }
        Value::Int(v) => {
            buf.push(0x04);
            let bytes = ((*v as u32) ^ 0x8000_0000).to_be_bytes();
            buf.extend_from_slice(&bytes);
        }
        Value::BigInt(v) => {
            buf.push(0x05);
            encode_comparable_i64(buf, *v);
        }
        Value::Float(v) => {
            buf.push(0x06);
            let bits = v.to_bits();
            let ordered = if bits & 0x8000_0000 != 0 {
                !bits
            } else {
                bits ^ 0x8000_0000
            };
            buf.extend_from_slice(&ordered.to_be_bytes());
        }
        Value::Double(v) => {
            buf.push(0x07);
            let bits = v.to_bits();
            let ordered = if bits & 0x8000_0000_0000_0000 != 0 {
                !bits
            } else {
                bits ^ 0x8000_0000_0000_0000
            };
            buf.extend_from_slice(&ordered.to_be_bytes());
        }
        Value::String(s) => {
            buf.push(0x08);
            encode_bytes(buf, s.as_bytes());
        }
        Value::Bytes(b) => {
            buf.push(0x09);
            encode_bytes(buf, b);
        }
        Value::Date(v) => {
            buf.push(0x0A);
            encode_comparable_i64(buf, *v as i64);
        }
        Value::Time(v) => {
            buf.push(0x0B);
            encode_comparable_i64(buf, *v);
        }
        Value::DateTime(v) => {
            buf.push(0x0C);
            encode_comparable_i64(buf, *v);
        }
        Value::Timestamp(v) => {
            buf.push(0x0D);
            encode_comparable_i64(buf, *v);
        }
        Value::Decimal(s) => {
            // For decimals, we encode as string for now
            // A proper implementation would use a comparable decimal encoding
            buf.push(0x0E);
            encode_bytes(buf, s.as_bytes());
        }
    }
}

/// Encode multiple values for use in a composite key.
pub fn encode_values_for_key(values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    for value in values {
        encode_value_for_key(&mut buf, value);
    }
    buf
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Check if a key is a record (row) key.
#[inline]
pub fn is_record_key(key: &[u8]) -> bool {
    key.len() >= RECORD_ROW_KEY_LEN
        && key[0] == TABLE_PREFIX
        && key.len() > PREFIX_LEN - 2
        && &key[1 + ID_LEN..PREFIX_LEN] == RECORD_PREFIX_SEP
}

/// Check if a key is an index key.
#[inline]
pub fn is_index_key(key: &[u8]) -> bool {
    key.len() >= PREFIX_LEN + ID_LEN
        && key[0] == TABLE_PREFIX
        && key.len() > PREFIX_LEN - 2
        && &key[1 + ID_LEN..PREFIX_LEN] == INDEX_PREFIX_SEP
}

/// Decode table ID from a key (works for both record and index keys).
pub fn decode_table_id(key: &[u8]) -> Result<TableId> {
    if key.len() < 1 + ID_LEN {
        return Err(TiSqlError::Codec("key too short for table ID".into()));
    }

    if key[0] != TABLE_PREFIX {
        return Err(TiSqlError::Codec("invalid table prefix".into()));
    }

    let (table_id, _) = decode_comparable_i64(&key[1..])?;
    Ok(table_id as TableId)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_key_roundtrip() {
        let table_id = 42u64;
        let handle = 100i64;

        let key = encode_record_key_with_handle(table_id, handle);
        let (decoded_table_id, decoded_handle) = decode_record_key(&key).unwrap();

        assert_eq!(decoded_table_id, table_id);
        assert_eq!(decoded_handle, Handle::Int(handle));
    }

    #[test]
    fn test_record_key_ordering() {
        let table_id = 1u64;

        let keys: Vec<Vec<u8>> = vec![
            encode_record_key_with_handle(table_id, -100),
            encode_record_key_with_handle(table_id, -1),
            encode_record_key_with_handle(table_id, 0),
            encode_record_key_with_handle(table_id, 1),
            encode_record_key_with_handle(table_id, 100),
        ];

        for i in 0..keys.len() - 1 {
            assert!(
                keys[i] < keys[i + 1],
                "Key ordering failed at index {}",
                i
            );
        }
    }

    #[test]
    fn test_table_ordering() {
        let keys: Vec<Vec<u8>> = vec![
            encode_record_key_with_handle(1, 0),
            encode_record_key_with_handle(2, 0),
            encode_record_key_with_handle(3, 0),
        ];

        for i in 0..keys.len() - 1 {
            assert!(keys[i] < keys[i + 1], "Table ordering failed at index {}", i);
        }
    }

    #[test]
    fn test_index_key_roundtrip() {
        let table_id = 42u64;
        let index_id = 1u64;
        let values = encode_values_for_key(&[Value::Int(100), Value::String("hello".into())]);

        let key = encode_index_seek_key(table_id, index_id, &values);
        let (decoded_table_id, decoded_index_id, decoded_values) = decode_index_key(&key).unwrap();

        assert_eq!(decoded_table_id, table_id);
        assert_eq!(decoded_index_id, index_id);
        assert_eq!(decoded_values, values.as_slice());
    }

    #[test]
    fn test_is_record_key() {
        let record_key = encode_record_key_with_handle(1, 100);
        let index_key = encode_index_seek_key(1, 1, &[]);

        assert!(is_record_key(&record_key));
        assert!(!is_record_key(&index_key));
    }

    #[test]
    fn test_is_index_key() {
        let record_key = encode_record_key_with_handle(1, 100);
        let index_key = encode_index_seek_key(1, 1, &[]);

        assert!(!is_index_key(&record_key));
        assert!(is_index_key(&index_key));
    }

    #[test]
    fn test_value_ordering() {
        // Test that values encode in correct order
        let values = [
            Value::Null,
            Value::Int(-100),
            Value::Int(-1),
            Value::Int(0),
            Value::Int(1),
            Value::Int(100),
        ];

        let encoded: Vec<Vec<u8>> = values.iter().map(|v| encode_values_for_key(&[v.clone()])).collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "Value ordering failed: {:?} should be < {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_string_ordering() {
        let values = [
            Value::String("a".into()),
            Value::String("aa".into()),
            Value::String("ab".into()),
            Value::String("b".into()),
        ];

        let encoded: Vec<Vec<u8>> = values.iter().map(|v| encode_values_for_key(&[v.clone()])).collect();

        for i in 0..encoded.len() - 1 {
            assert!(
                encoded[i] < encoded[i + 1],
                "String ordering failed: {:?} should be < {:?}",
                values[i],
                values[i + 1]
            );
        }
    }

    #[test]
    fn test_gen_table_record_prefix() {
        let prefix = gen_table_record_prefix(42);
        assert_eq!(prefix[0], TABLE_PREFIX);
        assert_eq!(&prefix[9..11], RECORD_PREFIX_SEP);
    }

    #[test]
    fn test_handle() {
        let int_handle = Handle::int(100);
        assert_eq!(int_handle.len(), 8);
        assert!(!int_handle.is_empty());

        let encoded = int_handle.encoded();
        assert_eq!(encoded.len(), 8);

        let common_handle = Handle::Common(vec![1, 2, 3]);
        assert_eq!(common_handle.len(), 3);
        assert!(!common_handle.is_empty());

        let empty_common = Handle::Common(vec![]);
        assert!(empty_common.is_empty());
    }
}
