/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use registry::schema::structs::{InMemoryStoreBase, ShardedInMemoryStore};
use trc::AddContext;

/// A type-erased, `Send` future used by every delegating method to break the
/// (spurious) recursive `Send` cycle described on the impl block below.
type BoxFuture<'x, T> = Pin<Box<dyn Future<Output = T> + Send + 'x>>;

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

    // Every delegating method type-erases its inner future into a boxed
    // `dyn Future + Send`. A shard is itself an `InMemoryStore`, so each call
    // dispatches back through the same method; without erasure the concrete
    // future type would transitively name itself and the auto-`Send` proof
    // (forced once these futures are spawned by callers) would recurse without
    // bound. Shards are `Arc`-backed, so the chosen store is cloned into the
    // future cheaply, letting it own the store and borrow only the key.

    pub fn key_set(&self, kv: KeyValue<Vec<u8>>) -> BoxFuture<'static, trc::Result<()>> {
        let store = self.get_store(&kv.key).clone();
        Box::pin(async move { store.key_set(kv).await })
    }

    pub fn counter_incr(&self, kv: KeyValue<i64>) -> BoxFuture<'static, trc::Result<i64>> {
        // Shards are Redis-only, whose atomic increment always yields the new
        // value, so request the value back for a meaningful return.
        let store = self.get_store(&kv.key).clone();
        Box::pin(async move { store.counter_incr(kv, true).await })
    }

    pub fn key_delete<'x>(
        &self,
        key: impl Into<LookupKey<'x>>,
    ) -> BoxFuture<'x, trc::Result<()>> {
        let key = key.into();
        let store = self.get_store(key.as_bytes()).clone();
        Box::pin(async move { store.key_delete(key).await })
    }

    pub fn counter_delete<'x>(
        &self,
        key: impl Into<LookupKey<'x>>,
    ) -> BoxFuture<'x, trc::Result<()>> {
        let key = key.into();
        let store = self.get_store(key.as_bytes()).clone();
        Box::pin(async move { store.counter_delete(key).await })
    }

    pub fn key_delete_prefix<'x>(&self, prefix: &'x [u8]) -> BoxFuture<'x, trc::Result<()>> {
        // A prefix can span multiple shards, so fan the deletion out to every
        // backing store.
        let stores = self.stores.clone();
        Box::pin(async move {
            for store in &stores {
                store
                    .key_delete_prefix(prefix)
                    .await
                    .caused_by(trc::location!())?;
            }
            Ok(())
        })
    }

    pub fn key_get<'x, T: Deserialize + From<Value<'static>> + std::fmt::Debug + 'static>(
        &self,
        key: impl Into<LookupKey<'x>>,
    ) -> BoxFuture<'x, trc::Result<Option<T>>> {
        let key = key.into();
        let store = self.get_store(key.as_bytes()).clone();
        Box::pin(async move { store.key_get(key).await })
    }

    pub fn counter_get<'x>(
        &self,
        key: impl Into<LookupKey<'x>>,
    ) -> BoxFuture<'x, trc::Result<i64>> {
        let key = key.into();
        let store = self.get_store(key.as_bytes()).clone();
        Box::pin(async move { store.counter_get(key).await })
    }

    pub fn key_exists<'x>(
        &self,
        key: impl Into<LookupKey<'x>>,
    ) -> BoxFuture<'x, trc::Result<bool>> {
        let key = key.into();
        let store = self.get_store(key.as_bytes()).clone();
        Box::pin(async move { store.key_exists(key).await })
    }

    pub fn try_lock<'x>(&self, key: &'x [u8], duration: u64) -> BoxFuture<'x, trc::Result<bool>> {
        // The caller passes an already-assembled `[prefix, ..rest]` key. The
        // underlying `try_lock` reassembles a key from a prefix byte plus the
        // remainder, so split the first byte back out to avoid double-prefixing;
        // this keeps the lock key identical to the one `remove_lock` (via
        // `key_delete`) will compute. `build_key` always emits at least the
        // prefix byte, so `key` is never empty here.
        let store = self.get_store(key).clone();
        Box::pin(async move {
            let (prefix, rest) = key.split_first().expect("lock key must be non-empty");
            store.try_lock(*prefix, rest, duration).await
        })
    }
}
