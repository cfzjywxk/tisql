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

/// Stable 64-bit hash used by persisted formats and long-lived cache keys.
///
/// This is intentionally deterministic across Rust toolchains; do not replace
/// with `DefaultHasher` for data that must remain stable across versions.
pub fn stable_hash64(seed: u64, key: &[u8]) -> u64 {
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

#[cfg(test)]
mod tests {
    use super::stable_hash64;

    #[test]
    fn test_stable_hash64_known_vectors() {
        assert_eq!(
            stable_hash64(0x9E37_79B9_7F4A_7C15, b"stable-key"),
            0xba60_731a_a6cc_e203
        );
        assert_eq!(
            stable_hash64(0xC2B2_AE3D_27D4_EB4F, b"stable-key"),
            0x44c0_c80d_07c8_a327
        );
    }
}
