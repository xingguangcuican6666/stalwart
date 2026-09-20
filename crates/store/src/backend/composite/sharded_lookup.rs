/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::sync::Arc;

use registry::schema::structs::{InMemoryStoreBase, ShardedInMemoryStore};
use trc::AddContext;

use crate::{
    Deserialize, InMemoryStore, Value,
    dispatch::lookup::{KeyValue, LookupKey},
};

use super::shard_index;

/// An in-memory (lookup) store that distributes keys across several backing
/// stores. Only Redis-family backends are supported as shards; a given key is
/// mapped deterministically to one shard by hashing it.
#[derive(Debug)]
pub struct ShardedInMemory {
    pub stores: Vec<InMemoryStore>,
}

impl ShardedInMemory {
    pub async fn open(config: ShardedInMemoryStore) -> Result<InMemoryStore, String> {
        let mut stores = Vec::with_capacity(config.stores.len());

        for base in config.stores {
            let store = match base {
                #[cfg(feature = "redis")]
                InMemoryStoreBase::Redis(cfg) => {
                    crate::backend::redis::RedisStore::open_single(cfg).await?
                }
                #[cfg(feature = "redis")]
                InMemoryStoreBase::RedisCluster(cfg) => {
                    crate::backend::redis::RedisStore::open_cluster(cfg).await?
                }
                #[cfg(feature = "redis")]
                InMemoryStoreBase::RedisSentinel(cfg) => {
                    crate::backend::redis::RedisStore::open_sentinel(cfg).await?
                }
                #[allow(unreachable_patterns)]
                _ => {
                    return Err(
                        "A sharded in-memory store only supports Redis backends".to_string(),
                    );
                }
            };
            stores.push(store);
        }

        if stores.len() < 2 {
            return Err(
                "A sharded in-memory store requires at least two backing stores".to_string(),
            );
        }

        Ok(InMemoryStore::Sharded(Arc::new(ShardedInMemory { stores })))
    }

    #[inline]
    fn get_store(&self, key: &[u8]) -> &InMemoryStore {
        &self.stores[shard_index(key, self.stores.len())]
    }

    pub async fn key_set(&self, kv: KeyValue<Vec<u8>>) -> trc::Result<()> {
        Box::pin(self.get_store(&kv.key).key_set(kv)).await
    }

    pub async fn counter_incr(&self, kv: KeyValue<i64>) -> trc::Result<i64> {
        // Shards are Redis-only, whose atomic increment always yields the new
        // value, so request the value back for a meaningful return.
        Box::pin(self.get_store(&kv.key).counter_incr(kv, true)).await
    }

    pub async fn key_delete(&self, key: impl Into<LookupKey<'_>>) -> trc::Result<()> {
        let key = key.into();
        Box::pin(self.get_store(key.as_bytes()).key_delete(key)).await
    }

    pub async fn counter_delete(&self, key: impl Into<LookupKey<'_>>) -> trc::Result<()> {
        let key = key.into();
        Box::pin(self.get_store(key.as_bytes()).counter_delete(key)).await
    }

    pub async fn key_delete_prefix(&self, prefix: &[u8]) -> trc::Result<()> {
        // A prefix can span multiple shards, so fan the deletion out to every
        // backing store.
        for store in &self.stores {
            Box::pin(store.key_delete_prefix(prefix))
                .await
                .caused_by(trc::location!())?;
        }
        Ok(())
    }

    pub async fn key_get<T: Deserialize + From<Value<'static>> + std::fmt::Debug + 'static>(
        &self,
        key: impl Into<LookupKey<'_>>,
    ) -> trc::Result<Option<T>> {
        let key = key.into();
        Box::pin(self.get_store(key.as_bytes()).key_get(key)).await
    }

    pub async fn counter_get(&self, key: impl Into<LookupKey<'_>>) -> trc::Result<i64> {
        let key = key.into();
        Box::pin(self.get_store(key.as_bytes()).counter_get(key)).await
    }

    pub async fn key_exists(&self, key: impl Into<LookupKey<'_>>) -> trc::Result<bool> {
        let key = key.into();
        Box::pin(self.get_store(key.as_bytes()).key_exists(key)).await
    }

    pub async fn try_lock(&self, key: &[u8], duration: u64) -> trc::Result<bool> {
        // The caller passes an already-assembled `[prefix, ..rest]` key. The
        // underlying `try_lock` reassembles a key from a prefix byte plus the
        // remainder, so split the first byte back out to avoid double-prefixing;
        // this keeps the lock key identical to the one `remove_lock` (via
        // `key_delete`) will compute. `build_key` always emits at least the
        // prefix byte, so `key` is never empty here.
        let (prefix, rest) = key.split_first().expect("lock key must be non-empty");
        Box::pin(self.get_store(key).try_lock(*prefix, rest, duration)).await
    }
}
