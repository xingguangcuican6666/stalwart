/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use trc::AddContext;

use crate::{
    Deserialize, IterateParams, Key, Store, ValueKey,
    search::{IndexDocument, SearchComparator, SearchDocumentId, SearchFilter, SearchQuery},
    write::{AssignedIds, Batch, SearchIndex, ValueClass},
};

/// A composite SQL store that offloads reads to one or more read replicas while
/// directing every mutation to a single primary.
///
/// Consistency model:
///  - Point reads (`get_value`, `key_exists`, `get_counter`, `get_blob`) are
///    served by a replica chosen round-robin; on any error other than an
///    assertion failure they fall back to the primary. An assertion failure is
///    a semantic result of an optimistic-concurrency check and must never be
///    retried against another node, so it is propagated immediately.
///  - Range scans (`iterate`) go straight to the primary: the caller-supplied
///    callback is consumed once and cannot be safely replayed against a second
///    node after a partial replica failure.
///  - All writes, full-text queries and (un)indexing go to the primary only.
pub struct SQLReadReplica {
    primary: Store,
    replicas: Vec<Store>,
    last_used_replica: AtomicUsize,
}

impl SQLReadReplica {
    pub fn open(primary: Store, replicas: Vec<Store>) -> Result<Store, String> {
        if replicas.is_empty() {
            return Err("A read-replica store requires at least one replica".to_string());
        }

        Ok(Store::SQLReadReplica(Arc::new(SQLReadReplica {
            primary,
            replicas,
            last_used_replica: AtomicUsize::new(0),
        })))
    }

    #[inline]
    pub fn primary_store(&self) -> &Store {
        &self.primary
    }

    /// Returns the ordered list of stores to attempt for a read: the next
    /// replica in round-robin order, followed by the primary as a fallback.
    fn read_targets(&self) -> Vec<&Store> {
        if self.replicas.is_empty() {
            vec![&self.primary]
        } else {
            let idx = self.last_used_replica.fetch_add(1, Ordering::Relaxed) % self.replicas.len();
            vec![&self.replicas[idx], &self.primary]
        }
    }

    pub async fn get_value<U>(&self, key: impl Key) -> trc::Result<Option<U>>
    where
        U: Deserialize + 'static,
    {
        let mut last_err = None;
        for store in self.read_targets() {
            match Box::pin(store.get_value(key.clone())).await {
                Ok(value) => return Ok(value),
                Err(err) if err.is_assertion_failure() => return Err(err),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or_else(|| trc::StoreEvent::NotConfigured.into()))
    }

    pub async fn key_exists(&self, key: impl Key) -> trc::Result<bool> {
        let mut last_err = None;
        for store in self.read_targets() {
            match Box::pin(store.key_exists(key.clone())).await {
                Ok(value) => return Ok(value),
                Err(err) if err.is_assertion_failure() => return Err(err),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or_else(|| trc::StoreEvent::NotConfigured.into()))
    }

    pub async fn get_counter(
        &self,
        key: impl Into<ValueKey<ValueClass>> + Sync + Send,
    ) -> trc::Result<i64> {
        let key = key.into();
        let mut last_err = None;
        for store in self.read_targets() {
            match Box::pin(store.get_counter(key.clone())).await {
                Ok(value) => return Ok(value),
                Err(err) if err.is_assertion_failure() => return Err(err),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or_else(|| trc::StoreEvent::NotConfigured.into()))
    }

    pub async fn iterate<T: Key>(
        &self,
        params: IterateParams<T>,
        cb: impl for<'x> FnMut(&'x [u8], &'x [u8]) -> trc::Result<bool> + Sync + Send,
    ) -> trc::Result<()> {
        // The callback is single-use, so a range scan cannot be replayed on a
        // fallback node; serve it from the primary to guarantee correctness.
        Box::pin(self.primary.iterate(params, cb)).await
    }

    pub async fn get_blob(
        &self,
        key: &[u8],
        range: Range<usize>,
    ) -> trc::Result<Option<Vec<u8>>> {
        let mut last_err = None;
        for store in self.read_targets() {
            match Self::backend_get_blob(store, key, range.clone()).await {
                Ok(value) => return Ok(value),
                Err(err) if err.is_assertion_failure() => return Err(err),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or_else(|| trc::StoreEvent::NotConfigured.into()))
    }

    pub async fn put_blob(&self, key: &[u8], data: &[u8]) -> trc::Result<()> {
        match &self.primary {
            #[cfg(feature = "postgres")]
            Store::PostgreSQL(store) => store.put_blob(key, data).await,
            #[cfg(feature = "mysql")]
            Store::MySQL(store) => store.put_blob(key, data).await,
            _ => Err(trc::StoreEvent::NotSupported.into_err()),
        }
        .caused_by(trc::location!())
    }

    pub async fn delete_blob(&self, key: &[u8]) -> trc::Result<bool> {
        match &self.primary {
            #[cfg(feature = "postgres")]
            Store::PostgreSQL(store) => store.delete_blob(key).await,
            #[cfg(feature = "mysql")]
            Store::MySQL(store) => store.delete_blob(key).await,
            _ => Err(trc::StoreEvent::NotSupported.into_err()),
        }
        .caused_by(trc::location!())
    }

    async fn backend_get_blob(
        store: &Store,
        key: &[u8],
        range: Range<usize>,
    ) -> trc::Result<Option<Vec<u8>>> {
        match store {
            #[cfg(feature = "postgres")]
            Store::PostgreSQL(store) => store.get_blob(key, range).await,
            #[cfg(feature = "mysql")]
            Store::MySQL(store) => store.get_blob(key, range).await,
            _ => Err(trc::StoreEvent::NotSupported.into_err()),
        }
    }

    pub async fn write(&self, batch: Batch<'_>) -> trc::Result<AssignedIds> {
        Box::pin(self.primary.write(batch)).await
    }

    pub async fn purge_store(&self) -> trc::Result<()> {
        Box::pin(self.primary.purge_store()).await
    }

    pub async fn delete_range(&self, from: impl Key, to: impl Key) -> trc::Result<()> {
        Box::pin(self.primary.delete_range(from, to)).await
    }

    pub async fn query<R: SearchDocumentId>(
        &self,
        index: SearchIndex,
        filters: &[SearchFilter],
        sort: &[SearchComparator],
    ) -> trc::Result<Vec<R>> {
        match &self.primary {
            #[cfg(feature = "postgres")]
            Store::PostgreSQL(store) => store.query(index, filters, sort).await,
            #[cfg(feature = "mysql")]
            Store::MySQL(store) => store.query(index, filters, sort).await,
            _ => Err(trc::StoreEvent::NotSupported.into_err()),
        }
        .caused_by(trc::location!())
    }

    pub async fn index(&self, documents: Vec<IndexDocument>) -> trc::Result<()> {
        match &self.primary {
            #[cfg(feature = "postgres")]
            Store::PostgreSQL(store) => store.index(documents).await,
            #[cfg(feature = "mysql")]
            Store::MySQL(store) => store.index(documents).await,
            _ => Err(trc::StoreEvent::NotSupported.into_err()),
        }
        .caused_by(trc::location!())
    }

    pub async fn unindex(&self, query: SearchQuery) -> trc::Result<u64> {
        match &self.primary {
            #[cfg(feature = "postgres")]
            Store::PostgreSQL(store) => store.unindex(query).await,
            #[cfg(feature = "mysql")]
            Store::MySQL(store) => store.unindex(query).await,
            _ => Err(trc::StoreEvent::NotSupported.into_err()),
        }
        .caused_by(trc::location!())
    }
}
