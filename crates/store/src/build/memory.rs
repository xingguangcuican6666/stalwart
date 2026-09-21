/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{InMemoryStore, registry::bootstrap::Bootstrap};
use registry::schema::{prelude::ObjectType, structs};

#[allow(unreachable_patterns)]
impl InMemoryStore {
    pub async fn build(bp: &mut Bootstrap) -> Option<Self> {
        let result = match bp.setting_infallible::<structs::InMemoryStore>().await {
            structs::InMemoryStore::Default => {
                return Some(InMemoryStore::Store(bp.data_store.clone()));
            }
            #[cfg(feature = "redis")]
            structs::InMemoryStore::Redis(redis_store) => {
                crate::backend::redis::RedisStore::open_single(redis_store).await
            }
            #[cfg(feature = "redis")]
            structs::InMemoryStore::RedisCluster(redis_cluster_store) => {
                crate::backend::redis::RedisStore::open_cluster(redis_cluster_store).await
            }
            #[cfg(feature = "redis")]
            structs::InMemoryStore::RedisSentinel(redis_sentinel_store) => {
                crate::backend::redis::RedisStore::open_sentinel(redis_sentinel_store).await
            }
            structs::InMemoryStore::Sharded(store) => {
                crate::backend::composite::sharded_lookup::ShardedInMemory::open(store).await
            }
            #[allow(unreachable_patterns)]
            _ => Err("Binary was not compiled with the selected in-memory backend".to_string()),
        };

        match result {
            Ok(store) => Some(store),
            Err(err) => {
                bp.build_error(ObjectType::InMemoryStore.singleton(), err);
                None
            }
        }
    }

    pub fn downgrade_store(self) -> InMemoryStore {
        match self {
            InMemoryStore::Sharded(_) => InMemoryStore::default(),
            other => other,
        }
    }

    pub fn is_enterprise(&self) -> bool {
        matches!(self, InMemoryStore::Sharded(_))
    }
}
