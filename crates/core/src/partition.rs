//! Placing a session on a log partition.

use std::num::NonZeroU32;

use twox_hash::XxHash3_64;

use crate::ClientId;

/// The seed of [`partition_of`]: `openqtt2` in ASCII. Fixed for ever.
const SEED: u64 = u64::from_be_bytes(*b"openqtt2");

/// The log partition that holds a client's session: its claim `own/{cid}`, its session state
/// and its queues (report R3). xxh3-64 of the Client Identifier's bytes with a fixed seed,
/// modulo the number of partitions, which is fixed when a cluster is created (256 by default).
///
/// This is part of every cluster's stored state: a golden test pins it, because a change would
/// move every session of a running cluster to another partition.
pub fn partition_of(client_id: &ClientId, partitions: NonZeroU32) -> u32 {
    let hash = xxh3(client_id.as_str().as_bytes());
    // The remainder is below `partitions`, a u32.
    u32::try_from(hash % u64::from(partitions.get())).unwrap_or(0)
}

fn xxh3(bytes: &[u8]) -> u64 {
    XxHash3_64::oneshot_with_seed(SEED, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(text: &str) -> ClientId {
        ClientId::new(text).unwrap()
    }

    fn count(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).unwrap()
    }

    /// Partition counts the golden table covers, the default 256 among them.
    const COUNTS: [u32; 9] = [1, 2, 3, 16, 64, 256, 1024, 65_536, u32::MAX];

    /// Each identifier, its hash, and its partition for each of `COUNTS`. Computed with
    /// twox-hash and checked against two other implementations of xxh3, xxhash-rust and the
    /// reference C library, when the table was written.
    const GOLDEN: [(&str, u64, [u32; 9]); 10] = [
        (
            "a",
            0x2690_abbe_66a6_466c,
            [0, 0, 2, 12, 44, 108, 620, 18_028, 2_369_188_394],
        ),
        (
            "client-1",
            0xceaa_67c5_f3cd_bf86,
            [0, 0, 0, 6, 6, 134, 902, 49_030, 3_262_654_284],
        ),
        (
            "Client-1",
            0x9861_3eab_21b4_a524,
            [0, 0, 2, 4, 36, 36, 292, 42_276, 3_121_996_751],
        ),
        (
            "client-2",
            0x073b_36d4_580c_2c44,
            [0, 0, 1, 4, 4, 68, 68, 11_332, 1_598_513_944],
        ),
        (
            "pump-3",
            0xf860_2c3c_bf3b_477d,
            [0, 1, 0, 13, 61, 125, 893, 18_301, 3_080_418_234],
        ),
        (
            "acme/production/pump-3",
            0x828b_f593_ff3e_89b6,
            [0, 0, 1, 6, 54, 182, 438, 35_254, 2_177_531_722],
        ),
        (
            "oq000000000000000000000",
            0x5640_cc44_97c9_482e,
            [0, 0, 1, 14, 46, 46, 46, 18_478, 3_993_638_002],
        ),
        (
            "oqZZZZZZZZZZZZZZZZZZZZZ",
            0xfe94_bf27_6848_0d29,
            [0, 1, 1, 9, 41, 41, 297, 3_369, 1_725_746_257],
        ),
        (
            "caf\u{e9}-7",
            0xaaf7_343a_55c8_c581,
            [0, 1, 1, 1, 1, 129, 385, 50_561, 12_581_308],
        ),
        (
            "\u{1f600}",
            0xa040_2893_0078_e798,
            [0, 0, 2, 8, 24, 152, 920, 59_288, 2_696_482_859],
        ),
    ];

    #[test]
    fn the_hash_is_xxh3_64() {
        // The reference's answer for no input and seed 0, from the xxHash test vectors, so the
        // table below cannot have been pinned to some other hash.
        assert_eq!(XxHash3_64::oneshot(b""), 0x2d06_8005_38d3_94c2);
        assert_eq!(SEED, 0x6f70_656e_7174_7432);
    }

    #[test]
    fn partition_of_is_pinned() {
        for (text, hash, partitions) in GOLDEN {
            assert_eq!(xxh3(text.as_bytes()), hash, "{text}");
            for (n, expected) in COUNTS.into_iter().zip(partitions) {
                assert_eq!(partition_of(&id(text), count(n)), expected, "{text} of {n}");
            }
        }
    }

    #[test]
    fn identifiers_of_every_length_are_pinned() {
        // Lengths on both sides of each size class xxh3 treats apart, up to the longest
        // Client Identifier, and their partitions of the default 256.
        let golden: [(usize, u64, u32); 12] = [
            (1, 0x2690_abbe_66a6_466c, 108),
            (3, 0x1530_4525_d6c6_4ae2, 226),
            (4, 0xb671_ad83_4b87_595a, 90),
            (8, 0xc4c0_0a6e_0adf_3d67, 103),
            (9, 0x8b82_b467_520c_7acb, 203),
            (16, 0x6245_e178_2188_844a, 74),
            (17, 0x8ec2_466d_de4d_4033, 51),
            (128, 0x26ef_fe02_fc3d_031b, 27),
            (129, 0x855c_1565_d18a_d3df, 223),
            (240, 0x4cc8_1d70_e024_6a95, 149),
            (241, 0x7a50_f036_98cc_7afe, 254),
            (256, 0x0e61_98db_2df7_690c, 12),
        ];
        let alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
        for (len, hash, partition) in golden {
            let text: String = alphabet.chars().cycle().take(len).collect();
            assert_eq!(xxh3(text.as_bytes()), hash, "length {len}");
            assert_eq!(
                partition_of(&id(&text), count(256)),
                partition,
                "length {len}"
            );
        }
    }

    #[test]
    fn every_partition_is_in_range() {
        for n in [1, 2, 7, 256, 1_000_003] {
            for i in 0..1_000 {
                let p = partition_of(&ClientId::assigned(i), count(n));
                assert!(p < n);
            }
        }
    }
}
