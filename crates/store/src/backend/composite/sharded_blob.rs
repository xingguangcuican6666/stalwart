/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::ops::Range;
use std::sync::Arc;

use registry::schema::structs::{BlobStoreBase, ShardedBlobStore};

use crate::BlobStore;

use super::shard_index;

/// A blob store that distributes objects across several backing blob stores.
///
/// Each key is mapped deterministically to one shard by hashing it, so reads
/// and writes for the same key always target the same underlying store.
pub struct ShardedBlob {
    pub stores: Vec<BlobStore>,
}

impl ShardedBlob {
    pub async fn open(config: ShardedBlobStore) -> Result<BlobStore, String> {
        let mut stores = Vec::with_capacity(config.stores.len());

        for base in config.stores {
            let store = match base {
                #[cfg(feature = "s3")]
                BlobStoreBase::S3(cfg) => crate::backend::s3::S3Store::open(cfg).await?,
                #[cfg(feature = "azure")]
                BlobStoreBase::Azure(cfg) => crate::backend::azure::AzureStore::open(cfg).await?,
                BlobStoreBase::FileSystem(cfg) => {
                    crate::backend::fs::FsStore::open(cfg).await?
                }
                #[cfg(feature = "foundation")]
                BlobStoreBase::FoundationDb(cfg) => BlobStore::Store(
                    crate::backend::foundationdb::FdbStore::open(cfg).await?,
                ),
                #[cfg(feature = "postgres")]
                BlobStoreBase::PostgreSql(cfg) => BlobStore::Store(
                    crate::backend::postgres::PostgresStore::open(cfg).await?,
                ),
                #[cfg(feature = "mysql")]
                BlobStoreBase::MySql(cfg) => {
                    BlobStore::Store(crate::backend::mysql::MysqlStore::open(cfg).await?)
                }
                #[allow(unreachable_patterns)]
                _ => {
                    return Err(
                        "Binary was not compiled with the selected sharded blob backend"
                            .to_string(),
                    );
                }
            };
            stores.push(store);
        }

        if stores.len() < 2 {
            return Err(
                "A sharded blob store requires at least two backing stores".to_string(),
            );
        }

        Ok(BlobStore::Sharded(Arc::new(ShardedBlob { stores })))
    }

    #[inline]
    fn get_store(&self, key: &[u8]) -> &BlobStore {
        &self.stores[shard_index(key, self.stores.len())]
    }

    pub async fn get_blob(
        &self,
        key: &[u8],
        range: Range<usize>,
    ) -> trc::Result<Option<Vec<u8>>> {
        Box::pin(self.get_store(key).get_blob(key, range)).await
    }

    pub async fn put_blob(&self, key: &[u8], data: &[u8]) -> trc::Result<()> {
        Box::pin(
            self.get_store(key)
                .put_blob(key, data, crate::CompressionAlgo::None),
        )
        .await
    }

    pub async fn delete_blob(&self, key: &[u8]) -> trc::Result<bool> {
        Box::pin(self.get_store(key).delete_blob(key)).await
    }
}
