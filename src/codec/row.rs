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

//! Row encoding utilities (Compact Row Format).
//!
//! This module implements TiDB's compact row format for efficient row storage.
//!
//! ## Row Format Structure
//! ```text
//! [VER][FLAGS][NOT_NULL_COL_CNT][NULL_COL_CNT][COL_IDS][OFFSETS][DATA][CHECKSUM?]
//! ```
//!
//! - VER: Version byte (128)
//! - FLAGS: 1 byte (bit 0 = large row, bit 1 = has checksum)
//! - NOT_NULL_COL_CNT: 2 bytes (u16 LE)
//! - NULL_COL_CNT: 2 bytes (u16 LE)
//! - COL_IDS: Column IDs (1 or 4 bytes each depending on large flag)
//! - OFFSETS: End offsets for each non-null column's data
//! - DATA: Concatenated encoded column values
//! - CHECKSUM: Optional CRC32 checksum
//!
//! ## Small vs Large Row
//! - Small row: colID = 1 byte, offset = 2 bytes (for common cases)
//! - Large row: colID = 4 bytes, offset = 4 bytes (for > 255 columns or > 64KB data)

use crate::codec::number::{
    decode_compact_i64, decode_compact_u64, encode_compact_i64, encode_compact_u64,
};
use crate::codec::CODEC_VER;
use crate::error::{Result, TiSqlError};
use crate::types::{ColumnId, DataType, Value};

/// Row format flags
const ROW_FLAG_LARGE: u8 = 0x01;
#[allow(dead_code)] // Will be used when checksum support is added
const ROW_FLAG_CHECKSUM: u8 = 0x02;

/// Header size for small rows: VER + FLAGS + NOT_NULL_CNT + NULL_CNT
const SMALL_ROW_HEADER_SIZE: usize = 1 + 1 + 2 + 2;

// ============================================================================
// Row Codec Types
// ============================================================================

/// Row encoder for compact row format.
#[derive(Debug, Default)]
pub struct RowEncoder {
    /// Encoded column data
    data: Vec<u8>,
    /// Column IDs for small rows (1 byte each)
    col_ids: Vec<u8>,
    /// Column IDs for large rows (4 bytes each)
    col_ids32: Vec<u32>,
    /// Offsets for small rows (2 bytes each)
    offsets: Vec<u16>,
    /// Offsets for large rows (4 bytes each)
    offsets32: Vec<u32>,
    /// Number of non-null columns
    num_not_null_cols: u16,
    /// Number of null columns
    num_null_cols: u16,
    /// Row flags
    flags: u8,
}

impl RowEncoder {
    /// Create a new row encoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset the encoder for reuse.
    pub fn reset(&mut self) {
        self.data.clear();
        self.col_ids.clear();
        self.col_ids32.clear();
        self.offsets.clear();
        self.offsets32.clear();
        self.num_not_null_cols = 0;
        self.num_null_cols = 0;
        self.flags = 0;
    }

    /// Check if this is a large row.
    #[inline]
    fn is_large(&self) -> bool {
        self.flags & ROW_FLAG_LARGE != 0
    }

    /// Encode a row with column IDs and values.
    pub fn encode(&mut self, col_ids: &[ColumnId], values: &[Value]) -> Vec<u8> {
        self.reset();

        // First pass: categorize columns and check if large row is needed
        for (col_id, value) in col_ids.iter().zip(values.iter()) {
            if *col_id > 255 {
                self.flags |= ROW_FLAG_LARGE;
            }
            if value.is_null() {
                self.num_null_cols += 1;
            } else {
                self.num_not_null_cols += 1;
            }
        }

        // Initialize column ID and offset arrays
        let total_cols = col_ids.len();
        if self.is_large() {
            self.col_ids32.resize(total_cols, 0);
            self.offsets32.resize(self.num_not_null_cols as usize, 0);
        } else {
            self.col_ids.resize(total_cols, 0);
            self.offsets.resize(self.num_not_null_cols as usize, 0);
        }

        // Build sorted indices instead of copying data
        let mut indices: Vec<usize> = (0..col_ids.len()).collect();
        indices.sort_by_key(|&i| col_ids[i]);

        // Encode columns in sorted order
        let mut not_null_idx = 0usize;
        let mut null_idx = self.num_not_null_cols as usize;

        for &i in &indices {
            let col_id = col_ids[i];
            let value = &values[i];

            if value.is_null() {
                // Store null column ID
                if self.is_large() {
                    self.col_ids32[null_idx] = col_id;
                } else {
                    self.col_ids[null_idx] = col_id as u8;
                }
                null_idx += 1;
            } else {
                // Store non-null column ID and encode value
                if self.is_large() {
                    self.col_ids32[not_null_idx] = col_id;
                } else {
                    self.col_ids[not_null_idx] = col_id as u8;
                }

                encode_value_compact(&mut self.data, value);

                // Check if we need to convert to large row
                if self.data.len() > u16::MAX as usize && !self.is_large() {
                    self.convert_to_large();
                }

                if self.is_large() {
                    self.offsets32[not_null_idx] = self.data.len() as u32;
                } else {
                    self.offsets[not_null_idx] = self.data.len() as u16;
                }
                not_null_idx += 1;
            }
        }

        self.to_bytes()
    }

    /// Convert from small to large row format.
    fn convert_to_large(&mut self) {
        self.flags |= ROW_FLAG_LARGE;

        // Convert column IDs
        self.col_ids32 = self.col_ids.iter().map(|&id| id as u32).collect();
        self.col_ids.clear();

        // Convert offsets
        self.offsets32 = self.offsets.iter().map(|&off| off as u32).collect();
        self.offsets32.resize(self.num_not_null_cols as usize, 0);
        self.offsets.clear();
    }

    /// Serialize the row to bytes.
    fn to_bytes(&self) -> Vec<u8> {
        // Pre-calculate buffer size
        let total_cols = self.num_not_null_cols as usize + self.num_null_cols as usize;
        let col_id_size = if self.is_large() { 4 } else { 1 };
        let offset_size = if self.is_large() { 4 } else { 2 };

        let size = SMALL_ROW_HEADER_SIZE
            + total_cols * col_id_size
            + self.num_not_null_cols as usize * offset_size
            + self.data.len();

        let mut buf = Vec::with_capacity(size);

        // Version
        buf.push(CODEC_VER);

        // Flags
        buf.push(self.flags);

        // Not null column count (u16 LE)
        buf.extend_from_slice(&self.num_not_null_cols.to_le_bytes());

        // Null column count (u16 LE)
        buf.extend_from_slice(&self.num_null_cols.to_le_bytes());

        // Column IDs
        if self.is_large() {
            for &id in &self.col_ids32 {
                buf.extend_from_slice(&id.to_le_bytes());
            }
        } else {
            buf.extend_from_slice(&self.col_ids);
        }

        // Offsets (only for non-null columns)
        if self.is_large() {
            for &off in &self.offsets32 {
                buf.extend_from_slice(&off.to_le_bytes());
            }
        } else {
            for &off in &self.offsets {
                buf.extend_from_slice(&off.to_le_bytes());
            }
        }

        // Data
        buf.extend_from_slice(&self.data);

        buf
    }
}

/// Row decoder for compact row format.
#[derive(Debug)]
pub struct RowDecoder<'a> {
    data: &'a [u8],
    flags: u8,
    num_not_null_cols: u16,
    num_null_cols: u16,
    col_ids_start: usize,
    offsets_start: usize,
    data_start: usize,
}

impl<'a> RowDecoder<'a> {
    /// Create a new row decoder.
    pub fn new(data: &'a [u8]) -> Result<Self> {
        if data.len() < SMALL_ROW_HEADER_SIZE {
            return Err(TiSqlError::Codec("row data too short".into()));
        }

        // Check version
        if data[0] != CODEC_VER {
            return Err(TiSqlError::Codec(format!(
                "invalid row codec version: {}, expected {}",
                data[0], CODEC_VER
            )));
        }

        let flags = data[1];
        let num_not_null_cols = u16::from_le_bytes([data[2], data[3]]);
        let num_null_cols = u16::from_le_bytes([data[4], data[5]]);
        let total_cols = num_not_null_cols as usize + num_null_cols as usize;

        let is_large = flags & ROW_FLAG_LARGE != 0;
        let col_id_size = if is_large { 4 } else { 1 };
        let offset_size = if is_large { 4 } else { 2 };

        let col_ids_start = SMALL_ROW_HEADER_SIZE;
        let offsets_start = col_ids_start + total_cols * col_id_size;
        let data_start = offsets_start + num_not_null_cols as usize * offset_size;

        if data.len() < data_start {
            return Err(TiSqlError::Codec("row data truncated".into()));
        }

        Ok(Self {
            data,
            flags,
            num_not_null_cols,
            num_null_cols,
            col_ids_start,
            offsets_start,
            data_start,
        })
    }

    /// Check if this is a large row.
    #[inline]
    fn is_large(&self) -> bool {
        self.flags & ROW_FLAG_LARGE != 0
    }

    /// Get column ID at index.
    fn get_col_id(&self, idx: usize) -> ColumnId {
        let pos = self.col_ids_start;
        if self.is_large() {
            let offset = pos + idx * 4;
            u32::from_le_bytes([
                self.data[offset],
                self.data[offset + 1],
                self.data[offset + 2],
                self.data[offset + 3],
            ])
        } else {
            self.data[pos + idx] as u32
        }
    }

    /// Get offset at index (for non-null columns).
    fn get_offset(&self, idx: usize) -> usize {
        if self.is_large() {
            let pos = self.offsets_start + idx * 4;
            u32::from_le_bytes([
                self.data[pos],
                self.data[pos + 1],
                self.data[pos + 2],
                self.data[pos + 3],
            ]) as usize
        } else {
            let pos = self.offsets_start + idx * 2;
            u16::from_le_bytes([self.data[pos], self.data[pos + 1]]) as usize
        }
    }

    /// Get the data slice for a non-null column at index.
    fn get_col_data(&self, idx: usize) -> &'a [u8] {
        let start = if idx == 0 {
            0
        } else {
            self.get_offset(idx - 1)
        };
        let end = self.get_offset(idx);
        &self.data[self.data_start + start..self.data_start + end]
    }

    /// Find column by ID and return its value.
    pub fn find_col(&self, col_id: ColumnId) -> Option<Option<&'a [u8]>> {
        // Search in non-null columns (sorted)
        let non_null_count = self.num_not_null_cols as usize;
        for i in 0..non_null_count {
            let id = self.get_col_id(i);
            if id == col_id {
                return Some(Some(self.get_col_data(i)));
            }
            if id > col_id {
                break; // Past the target, check null columns
            }
        }

        // Search in null columns (sorted)
        let null_start = non_null_count;
        let null_count = self.num_null_cols as usize;
        for i in 0..null_count {
            let id = self.get_col_id(null_start + i);
            if id == col_id {
                return Some(None); // Found in null columns
            }
            if id > col_id {
                break;
            }
        }

        None // Column not found
    }

    /// Get number of non-null columns.
    pub fn num_not_null_cols(&self) -> usize {
        self.num_not_null_cols as usize
    }

    /// Get number of null columns.
    pub fn num_null_cols(&self) -> usize {
        self.num_null_cols as usize
    }

    /// Get total number of columns.
    pub fn num_cols(&self) -> usize {
        self.num_not_null_cols as usize + self.num_null_cols as usize
    }
}

// ============================================================================
// Value Encoding for Row Storage (Compact Format)
// ============================================================================

/// Encode a value in compact format for row storage.
pub fn encode_value_compact(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => {
            // Nulls are tracked separately, no data needed
        }
        Value::Boolean(b) => {
            buf.push(if *b { 1 } else { 0 });
        }
        Value::TinyInt(v) => {
            encode_compact_i64(buf, *v as i64);
        }
        Value::SmallInt(v) => {
            encode_compact_i64(buf, *v as i64);
        }
        Value::Int(v) => {
            encode_compact_i64(buf, *v as i64);
        }
        Value::BigInt(v) => {
            encode_compact_i64(buf, *v);
        }
        Value::Float(v) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Value::Double(v) => {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Value::String(s) => {
            buf.extend_from_slice(s.as_bytes());
        }
        Value::Bytes(b) => {
            buf.extend_from_slice(b);
        }
        Value::Date(v) => {
            encode_compact_i64(buf, *v as i64);
        }
        Value::Time(v) => {
            encode_compact_i64(buf, *v);
        }
        Value::DateTime(v) => {
            encode_compact_u64(buf, *v as u64);
        }
        Value::Timestamp(v) => {
            encode_compact_u64(buf, *v as u64);
        }
        Value::Decimal(s) => {
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

/// Decode a value from compact format given its data type.
pub fn decode_value_compact(data: &[u8], data_type: &DataType) -> Result<Value> {
    if data.is_empty() {
        // For types that might have empty representation
        match data_type {
            DataType::Varchar(_) | DataType::Text | DataType::Char(_) => {
                return Ok(Value::String(String::new()));
            }
            DataType::Blob => return Ok(Value::Bytes(Vec::new())),
            _ => {}
        }
    }

    match data_type {
        DataType::Boolean => {
            if data.is_empty() {
                return Err(TiSqlError::Codec("boolean data too short".into()));
            }
            Ok(Value::Boolean(data[0] != 0))
        }
        DataType::TinyInt => {
            let v = decode_compact_i64(data)?;
            Ok(Value::TinyInt(v as i8))
        }
        DataType::SmallInt => {
            let v = decode_compact_i64(data)?;
            Ok(Value::SmallInt(v as i16))
        }
        DataType::Int => {
            let v = decode_compact_i64(data)?;
            Ok(Value::Int(v as i32))
        }
        DataType::BigInt => {
            let v = decode_compact_i64(data)?;
            Ok(Value::BigInt(v))
        }
        DataType::Float => {
            if data.len() < 4 {
                return Err(TiSqlError::Codec("float data too short".into()));
            }
            let v = f32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            Ok(Value::Float(v))
        }
        DataType::Double => {
            if data.len() < 8 {
                return Err(TiSqlError::Codec("double data too short".into()));
            }
            let v = f64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            Ok(Value::Double(v))
        }
        DataType::Varchar(_) | DataType::Text | DataType::Char(_) => {
            let s = String::from_utf8(data.to_vec())
                .map_err(|e| TiSqlError::Codec(format!("invalid utf8: {e}")))?;
            Ok(Value::String(s))
        }
        DataType::Blob => Ok(Value::Bytes(data.to_vec())),
        DataType::Date => {
            let v = decode_compact_i64(data)?;
            Ok(Value::Date(v as i32))
        }
        DataType::Time => {
            let v = decode_compact_i64(data)?;
            Ok(Value::Time(v))
        }
        DataType::DateTime => {
            let v = decode_compact_u64(data)?;
            Ok(Value::DateTime(v as i64))
        }
        DataType::Timestamp => {
            let v = decode_compact_u64(data)?;
            Ok(Value::Timestamp(v as i64))
        }
        DataType::Decimal { .. } => {
            let s = String::from_utf8(data.to_vec())
                .map_err(|e| TiSqlError::Codec(format!("invalid decimal utf8: {e}")))?;
            Ok(Value::Decimal(s))
        }
    }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Row codec for encoding and decoding rows.
pub struct RowCodec;

impl RowCodec {
    /// Check if data is in new (compact) row format.
    #[inline]
    pub fn is_new_format(data: &[u8]) -> bool {
        !data.is_empty() && data[0] == CODEC_VER
    }
}

/// Encode a row with column IDs and values.
pub fn encode_row(col_ids: &[ColumnId], values: &[Value]) -> Vec<u8> {
    let mut encoder = RowEncoder::new();
    encoder.encode(col_ids, values)
}

/// Decode a row from bytes.
/// Returns a RowDecoder that can be used to access column values.
pub fn decode_row(data: &[u8]) -> Result<RowDecoder<'_>> {
    RowDecoder::new(data)
}

/// Decode a row to a Vec<Value> given column IDs and their data types.
/// The returned Vec will have the same order as the col_ids/data_types slices.
pub fn decode_row_to_values(
    data: &[u8],
    col_ids: &[ColumnId],
    data_types: &[DataType],
) -> Result<Vec<Value>> {
    if col_ids.len() != data_types.len() {
        return Err(TiSqlError::Codec(
            "col_ids and data_types length mismatch".into(),
        ));
    }

    let decoder = RowDecoder::new(data)?;
    let mut values = Vec::with_capacity(col_ids.len());

    for (col_id, data_type) in col_ids.iter().zip(data_types.iter()) {
        match decoder.find_col(*col_id) {
            Some(Some(col_data)) => {
                let value = decode_value_compact(col_data, data_type)?;
                values.push(value);
            }
            Some(None) => {
                // Column is null
                values.push(Value::Null);
            }
            None => {
                // Column not found in row, treat as null
                values.push(Value::Null);
            }
        }
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple_row() {
        let col_ids = vec![1, 2, 3];
        let values = vec![
            Value::Int(42),
            Value::String("hello".into()),
            Value::Boolean(true),
        ];

        let encoded = encode_row(&col_ids, &values);

        // Check version
        assert_eq!(encoded[0], CODEC_VER);

        // Should be small row (all column IDs <= 255)
        assert_eq!(encoded[1] & ROW_FLAG_LARGE, 0);

        // Check column counts
        let num_not_null = u16::from_le_bytes([encoded[2], encoded[3]]);
        let num_null = u16::from_le_bytes([encoded[4], encoded[5]]);
        assert_eq!(num_not_null, 3);
        assert_eq!(num_null, 0);
    }

    #[test]
    fn test_encode_with_nulls() {
        let col_ids = vec![1, 2, 3];
        let values = vec![Value::Int(42), Value::Null, Value::String("hello".into())];

        let encoded = encode_row(&col_ids, &values);

        // Check column counts
        let num_not_null = u16::from_le_bytes([encoded[2], encoded[3]]);
        let num_null = u16::from_le_bytes([encoded[4], encoded[5]]);
        assert_eq!(num_not_null, 2);
        assert_eq!(num_null, 1);
    }

    #[test]
    fn test_decode_simple_row() {
        let col_ids = vec![1, 2, 3];
        let values = vec![
            Value::Int(42),
            Value::String("hello".into()),
            Value::Boolean(true),
        ];

        let encoded = encode_row(&col_ids, &values);
        let decoder = decode_row(&encoded).unwrap();

        assert_eq!(decoder.num_not_null_cols(), 3);
        assert_eq!(decoder.num_null_cols(), 0);
        assert_eq!(decoder.num_cols(), 3);

        // Find columns
        assert!(decoder.find_col(1).is_some());
        assert!(decoder.find_col(2).is_some());
        assert!(decoder.find_col(3).is_some());
        assert!(decoder.find_col(4).is_none());
    }

    #[test]
    fn test_decode_with_nulls() {
        let col_ids = vec![1, 2, 3];
        let values = vec![Value::Int(42), Value::Null, Value::String("hello".into())];

        let encoded = encode_row(&col_ids, &values);
        let decoder = decode_row(&encoded).unwrap();

        // Column 2 should be found as null
        assert_eq!(decoder.find_col(2), Some(None));

        // Columns 1 and 3 should be found with data
        assert!(matches!(decoder.find_col(1), Some(Some(_))));
        assert!(matches!(decoder.find_col(3), Some(Some(_))));
    }

    #[test]
    fn test_large_row() {
        // Use column ID > 255 to force large row
        let col_ids = vec![1, 256, 1000];
        let values = vec![
            Value::Int(42),
            Value::String("hello".into()),
            Value::Boolean(true),
        ];

        let encoded = encode_row(&col_ids, &values);

        // Check large flag
        assert_ne!(encoded[1] & ROW_FLAG_LARGE, 0);

        let decoder = decode_row(&encoded).unwrap();
        assert!(decoder.is_large());
        assert_eq!(decoder.num_cols(), 3);
    }

    #[test]
    fn test_column_id_sorting() {
        // Column IDs provided out of order
        let col_ids = vec![3, 1, 2];
        let values = vec![
            Value::String("three".into()),
            Value::String("one".into()),
            Value::String("two".into()),
        ];

        let encoded = encode_row(&col_ids, &values);
        let decoder = decode_row(&encoded).unwrap();

        // Columns should be sorted by ID internally
        // Check that we can find all columns
        assert!(decoder.find_col(1).is_some());
        assert!(decoder.find_col(2).is_some());
        assert!(decoder.find_col(3).is_some());
    }

    #[test]
    fn test_row_codec_is_new_format() {
        let encoded = encode_row(&[1], &[Value::Int(42)]);
        assert!(RowCodec::is_new_format(&encoded));
        assert!(!RowCodec::is_new_format(&[0, 1, 2]));
        assert!(!RowCodec::is_new_format(&[]));
    }

    #[test]
    fn test_empty_row() {
        let encoded = encode_row(&[], &[]);
        let decoder = decode_row(&encoded).unwrap();

        assert_eq!(decoder.num_cols(), 0);
        assert_eq!(decoder.num_not_null_cols(), 0);
        assert_eq!(decoder.num_null_cols(), 0);
    }

    #[test]
    fn test_all_nulls() {
        let col_ids = vec![1, 2, 3];
        let values = vec![Value::Null, Value::Null, Value::Null];

        let encoded = encode_row(&col_ids, &values);
        let decoder = decode_row(&encoded).unwrap();

        assert_eq!(decoder.num_not_null_cols(), 0);
        assert_eq!(decoder.num_null_cols(), 3);

        // All columns should be found as null
        assert_eq!(decoder.find_col(1), Some(None));
        assert_eq!(decoder.find_col(2), Some(None));
        assert_eq!(decoder.find_col(3), Some(None));
    }

    #[test]
    fn test_decode_row_to_values_roundtrip() {
        use crate::types::DataType;

        let col_ids = vec![1, 2, 3, 4, 5];
        let data_types = vec![
            DataType::Int,
            DataType::Varchar(255),
            DataType::BigInt,
            DataType::Boolean,
            DataType::Double,
        ];
        let values = vec![
            Value::Int(42),
            Value::String("hello world".into()),
            Value::BigInt(123456789),
            Value::Boolean(true),
            Value::Double(1.23456),
        ];

        let encoded = encode_row(&col_ids, &values);
        let decoded = decode_row_to_values(&encoded, &col_ids, &data_types).unwrap();

        assert_eq!(decoded.len(), values.len());
        assert_eq!(decoded[0], Value::Int(42));
        assert_eq!(decoded[1], Value::String("hello world".into()));
        assert_eq!(decoded[2], Value::BigInt(123456789));
        assert_eq!(decoded[3], Value::Boolean(true));
        // Float comparison with tolerance
        if let Value::Double(d) = decoded[4] {
            assert!((d - 1.23456).abs() < 0.00001);
        } else {
            panic!("Expected Double");
        }
    }

    #[test]
    fn test_decode_row_to_values_with_nulls() {
        use crate::types::DataType;

        let col_ids = vec![1, 2, 3];
        let data_types = vec![DataType::Int, DataType::Varchar(100), DataType::BigInt];
        let values = vec![Value::Int(100), Value::Null, Value::BigInt(-500)];

        let encoded = encode_row(&col_ids, &values);
        let decoded = decode_row_to_values(&encoded, &col_ids, &data_types).unwrap();

        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0], Value::Int(100));
        assert_eq!(decoded[1], Value::Null);
        assert_eq!(decoded[2], Value::BigInt(-500));
    }
}
