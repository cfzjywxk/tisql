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

//! Canonical MySQL text formatting helpers used by protocol and worker paths.

use std::io;

/// Format days since Unix epoch to `YYYY-MM-DD`.
pub fn format_date_canonical(days_since_epoch: i32, out: &mut [u8; 32]) -> io::Result<usize> {
    let (year, month, day) = civil_from_days(days_since_epoch as i64);
    let mut len = 0usize;
    append_year(out, &mut len, year)?;
    append_byte(out, &mut len, b'-')?;
    append_u32_padded(out, &mut len, month, 2)?;
    append_byte(out, &mut len, b'-')?;
    append_u32_padded(out, &mut len, day, 2)?;
    Ok(len)
}

/// Format microseconds since midnight to `[-]HHH:MM:SS[.ffffff]`.
pub fn format_time_canonical(micros: i64, out: &mut [u8; 32]) -> io::Result<usize> {
    let mut len = 0usize;
    let abs = micros.unsigned_abs();
    let total_secs = abs / 1_000_000;
    let frac = (abs % 1_000_000) as u32;
    let hours = total_secs / 3_600;
    let minutes = ((total_secs % 3_600) / 60) as u32;
    let seconds = (total_secs % 60) as u32;

    if micros < 0 {
        append_byte(out, &mut len, b'-')?;
    }

    if hours < 100 {
        append_u32_padded(out, &mut len, hours as u32, 2)?;
    } else {
        append_u64(out, &mut len, hours)?;
    }
    append_byte(out, &mut len, b':')?;
    append_u32_padded(out, &mut len, minutes, 2)?;
    append_byte(out, &mut len, b':')?;
    append_u32_padded(out, &mut len, seconds, 2)?;
    if frac != 0 {
        append_byte(out, &mut len, b'.')?;
        append_u32_padded(out, &mut len, frac, 6)?;
    }
    Ok(len)
}

/// Format microseconds since epoch to `YYYY-MM-DD HH:MM:SS[.ffffff]`.
pub fn format_datetime_canonical(micros_since_epoch: i64, out: &mut [u8; 32]) -> io::Result<usize> {
    let secs = micros_since_epoch.div_euclid(1_000_000);
    let frac = micros_since_epoch.rem_euclid(1_000_000) as u32;
    let days = secs.div_euclid(86_400);
    let sec_of_day = secs.rem_euclid(86_400) as u32;

    let (year, month, day) = civil_from_days(days);
    let hour = sec_of_day / 3_600;
    let minute = (sec_of_day % 3_600) / 60;
    let second = sec_of_day % 60;

    let mut len = 0usize;
    append_year(out, &mut len, year)?;
    append_byte(out, &mut len, b'-')?;
    append_u32_padded(out, &mut len, month, 2)?;
    append_byte(out, &mut len, b'-')?;
    append_u32_padded(out, &mut len, day, 2)?;
    append_byte(out, &mut len, b' ')?;
    append_u32_padded(out, &mut len, hour, 2)?;
    append_byte(out, &mut len, b':')?;
    append_u32_padded(out, &mut len, minute, 2)?;
    append_byte(out, &mut len, b':')?;
    append_u32_padded(out, &mut len, second, 2)?;
    if frac != 0 {
        append_byte(out, &mut len, b'.')?;
        append_u32_padded(out, &mut len, frac, 6)?;
    }
    Ok(len)
}

// Convert days since Unix epoch (1970-01-01) to civil date.
fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if m <= 2 { 1 } else { 0 };
    (year as i32, m as u32, d as u32)
}

fn append_year(out: &mut [u8], len: &mut usize, year: i32) -> io::Result<()> {
    if (0..=9_999).contains(&year) {
        append_u32_padded(out, len, year as u32, 4)
    } else {
        let mut buf = itoa::Buffer::new();
        append_bytes(out, len, buf.format(year).as_bytes())
    }
}

fn append_u64(out: &mut [u8], len: &mut usize, value: u64) -> io::Result<()> {
    let mut buf = itoa::Buffer::new();
    append_bytes(out, len, buf.format(value).as_bytes())
}

fn append_u32_padded(
    out: &mut [u8],
    len: &mut usize,
    mut value: u32,
    width: usize,
) -> io::Result<()> {
    let mut tmp = [b'0'; 10];
    let start = tmp.len().checked_sub(width).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "Invalid width for padded int")
    })?;

    for idx in (start..tmp.len()).rev() {
        tmp[idx] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    if value != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Zero-padded integer width overflow",
        ));
    }
    append_bytes(out, len, &tmp[start..])
}

fn append_byte(out: &mut [u8], len: &mut usize, b: u8) -> io::Result<()> {
    append_bytes(out, len, &[b])
}

fn append_bytes(out: &mut [u8], len: &mut usize, bytes: &[u8]) -> io::Result<()> {
    let remaining = out.len().saturating_sub(*len);
    if remaining < bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Temporal formatting exceeded scratch buffer",
        ));
    }
    out[*len..*len + bytes.len()].copy_from_slice(bytes);
    *len += bytes.len();
    Ok(())
}
