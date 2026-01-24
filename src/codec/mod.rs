//! Codec module for key/value encoding.
//!
//! This module provides encoding utilities following TiDB's approach:
//!
//! ## Key Encoding (Memcomparable)
//! Keys use a memory-comparable encoding that preserves ordering when compared
//! lexicographically. This is essential for ordered key-value storage.
//!
//! ## Row Encoding (Compact Format)
//! Row values use a compact encoding format that stores column IDs and values
//! efficiently, similar to TiDB's rowcodec.
//!
//! ## Key Structure
//! ```text
//! Record Key: 't' + tableID (8 bytes) + "_r" + handle (8+ bytes)
//! Index Key:  't' + tableID (8 bytes) + "_i" + indexID (8 bytes) + indexValues...
//! ```

pub mod bytes;
pub mod key;
pub mod number;
pub mod row;

pub use bytes::{
    decode_bytes, decode_bytes_desc, decode_compact_bytes, encode_bytes, encode_bytes_desc,
    encode_compact_bytes,
};
pub use key::{
    decode_record_key, encode_index_seek_key, encode_record_key, encode_record_key_with_handle,
    TablePrefix, INDEX_PREFIX_SEP, RECORD_PREFIX_SEP, TABLE_PREFIX,
};
pub use number::{
    decode_comparable_i64, decode_comparable_u64, decode_compact_i64, decode_compact_u64,
    decode_varint, decode_varuint, encode_comparable_i64, encode_comparable_u64,
    encode_compact_i64, encode_compact_u64, encode_varint, encode_varuint,
};
pub use row::{
    decode_row, decode_row_to_values, decode_value_compact, encode_row, encode_value_compact,
    RowCodec, RowDecoder, RowEncoder,
};

/// Codec version constant (matches TiDB's CodecVer = 128)
pub const CODEC_VER: u8 = 128;

/// Type flags for encoded values (used in row encoding)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeFlag {
    Nil = 0,
    Bytes = 1,
    CompactBytes = 2,
    Int = 3,
    Uint = 4,
    Float = 5,
    Decimal = 6,
    Duration = 7,
    Varint = 8,
    Varuint = 9,
    Json = 10,
}

impl TypeFlag {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(TypeFlag::Nil),
            1 => Some(TypeFlag::Bytes),
            2 => Some(TypeFlag::CompactBytes),
            3 => Some(TypeFlag::Int),
            4 => Some(TypeFlag::Uint),
            5 => Some(TypeFlag::Float),
            6 => Some(TypeFlag::Decimal),
            7 => Some(TypeFlag::Duration),
            8 => Some(TypeFlag::Varint),
            9 => Some(TypeFlag::Varuint),
            10 => Some(TypeFlag::Json),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_flag() {
        assert_eq!(TypeFlag::from_u8(0), Some(TypeFlag::Nil));
        assert_eq!(TypeFlag::from_u8(3), Some(TypeFlag::Int));
        assert_eq!(TypeFlag::from_u8(255), None);
    }
}
