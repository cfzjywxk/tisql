use crate::error::{Result, TiSqlError};
use crate::types::{Key, Row, TableId, Value};

/// Encode a table key: {table_id}{user_key}
/// Table ID is encoded as big-endian u64 for correct ordering
pub fn encode_key(table_id: TableId, user_key: &[u8]) -> Key {
    let mut key = Vec::with_capacity(8 + user_key.len());
    key.extend_from_slice(&table_id.to_be_bytes());
    key.extend_from_slice(user_key);
    key
}

/// Decode a table key back to (table_id, user_key)
pub fn decode_key(key: &[u8]) -> Result<(TableId, &[u8])> {
    if key.len() < 8 {
        return Err(TiSqlError::Storage("Key too short".into()));
    }
    let table_id = u64::from_be_bytes(key[..8].try_into().unwrap());
    Ok((table_id, &key[8..]))
}

/// Encode primary key values into bytes for ordering
/// Uses a simple encoding that preserves ordering for common types
pub fn encode_pk(values: &[Value]) -> Key {
    let mut key = Vec::new();
    for value in values {
        encode_value_ordered(value, &mut key);
    }
    key
}

/// Encode a value in an order-preserving way
fn encode_value_ordered(value: &Value, buf: &mut Vec<u8>) {
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
            // Flip sign bit for correct signed ordering
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
            let bytes = ((*v as u64) ^ 0x8000_0000_0000_0000).to_be_bytes();
            buf.extend_from_slice(&bytes);
        }
        Value::Float(v) => {
            buf.push(0x06);
            // IEEE 754 float encoding for ordering
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
            // Escape 0x00 bytes and terminate with 0x00 0x01
            for &b in s.as_bytes() {
                if b == 0x00 {
                    buf.push(0x00);
                    buf.push(0xFF);
                } else {
                    buf.push(b);
                }
            }
            buf.push(0x00);
            buf.push(0x01);
        }
        Value::Bytes(b) => {
            buf.push(0x09);
            for &byte in b {
                if byte == 0x00 {
                    buf.push(0x00);
                    buf.push(0xFF);
                } else {
                    buf.push(byte);
                }
            }
            buf.push(0x00);
            buf.push(0x01);
        }
        // For other types, use bincode (not order-preserving)
        _ => {
            buf.push(0xFF);
            let encoded = bincode::serialize(value).unwrap();
            buf.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            buf.extend_from_slice(&encoded);
        }
    }
}

/// Encode a row using bincode (not order-preserving, just for storage)
pub fn encode_row(row: &Row) -> Result<Vec<u8>> {
    bincode::serialize(row).map_err(|e| TiSqlError::Storage(e.to_string()))
}

/// Decode a row from bytes
pub fn decode_row(data: &[u8]) -> Result<Row> {
    bincode::deserialize(data).map_err(|e| TiSqlError::Storage(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_key() {
        let table_id = 42u64;
        let user_key = b"hello";

        let encoded = encode_key(table_id, user_key);
        let (decoded_table_id, decoded_user_key) = decode_key(&encoded).unwrap();

        assert_eq!(decoded_table_id, table_id);
        assert_eq!(decoded_user_key, user_key);
    }

    #[test]
    fn test_int_ordering() {
        let values = vec![
            Value::Int(-100),
            Value::Int(-1),
            Value::Int(0),
            Value::Int(1),
            Value::Int(100),
        ];

        let encoded: Vec<_> = values.iter().map(|v| encode_pk(&[v.clone()])).collect();

        // Verify ordering is preserved
        for i in 0..encoded.len() - 1 {
            assert!(encoded[i] < encoded[i + 1], "Ordering failed at index {}", i);
        }
    }

    #[test]
    fn test_string_ordering() {
        let values = vec![
            Value::String("a".into()),
            Value::String("aa".into()),
            Value::String("ab".into()),
            Value::String("b".into()),
        ];

        let encoded: Vec<_> = values.iter().map(|v| encode_pk(&[v.clone()])).collect();

        for i in 0..encoded.len() - 1 {
            assert!(encoded[i] < encoded[i + 1], "Ordering failed at index {}", i);
        }
    }

    #[test]
    fn test_row_encode_decode() {
        let row = Row::new(vec![
            Value::Int(42),
            Value::String("hello".into()),
            Value::Boolean(true),
        ]);

        let encoded = encode_row(&row).unwrap();
        let decoded = decode_row(&encoded).unwrap();

        assert_eq!(row.values, decoded.values);
    }
}
