/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

#[cfg(any(feature = "postgres", feature = "mysql"))]
pub mod read_replica;
pub mod sharded_blob;
pub mod sharded_lookup;

/// Selects a shard index for a key by hashing it and reducing modulo the
/// number of configured shards. Shared by the sharded blob and in-memory
/// stores so that a given key always maps to the same backing store.
#[inline]
fn shard_index(key: &[u8], num_shards: usize) -> usize {
    (xxhash_rust::xxh3::xxh3_64(key) % num_shards as u64) as usize
}
