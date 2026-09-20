/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{LogoCache, Server, USER_AGENT, manager::application::Resource};
use directory::Directory;
use registry::{
    schema::{
        enums::{StorageQuota, TenantStorageQuota},
        prelude::ObjectType,
        structs::{Domain, Tenant},
    },
    types::EnumImpl,
};
use std::sync::Arc;
use store::{BlobStore, InMemoryStore, RegistryStore, SearchStore, Store};
use utils::HttpLimitResponse;

pub mod archive;
pub mod blob;
pub mod dav;
pub mod document;
pub mod encryption;
pub mod index;
pub mod quota;
pub mod state;
pub mod transaction;

#[derive(Debug, Clone)]
pub struct ObjectQuota([u32; StorageQuota::COUNT - 1]);

#[derive(Debug, Clone)]
pub struct TenantQuota([u32; TenantStorageQuota::COUNT - 1]);

impl Server {
    #[inline(always)]
    pub fn registry(&self) -> &RegistryStore {
        &self.core.storage.registry
    }

    #[inline(always)]
    pub fn store(&self) -> &Store {
        &self.core.storage.data
    }

    #[inline(always)]
    pub fn blob_store(&self) -> &BlobStore {
        &self.core.storage.blob
    }

    #[inline(always)]
    pub fn search_store(&self) -> &SearchStore {
        &self.core.storage.search
    }

    #[inline(always)]
    pub fn in_memory_store(&self) -> &InMemoryStore {
        &self.core.storage.memory
    }

    #[inline(always)]
    pub fn tracing_store(&self) -> &Store {
        &self.core.storage.tracing
    }

    #[inline(always)]
    pub fn metrics_store(&self) -> &Store {
        &self.core.storage.metrics
    }

    #[inline(always)]
    pub fn get_directory(&self, id: &u32) -> Option<&Arc<Directory>> {
        self.core.storage.directories.get(id)
    }

    #[inline(always)]
    pub fn get_default_directory(&self) -> Option<&Arc<Directory>> {
        self.core.storage.directory.as_ref()
    }

    #[inline(always)]
    pub fn get_lookup_store(&self, name: &str) -> Option<InMemoryStore> {
        if !name.is_empty() && name != "*" {
            self.inner.data.lookup_stores.load().get(name).cloned()
        } else {
            self.in_memory_store().clone().into()
        }
    }

    pub async fn total_accounts(&self) -> trc::Result<usize> {
        self.registry().count_object(ObjectType::Account).await
    }

    pub async fn total_domains(&self) -> trc::Result<usize> {
        self.registry().count_object(ObjectType::Domain).await
    }

    /// Resolves the branding logo shown for `domain`, following a
    /// most-specific-first lookup: the domain's own logo, then its tenant's,
    /// then the server-wide default. The first URL found is downloaded (capped
    /// at 1 MiB) and memoised in the shared logo cache, keyed by registrable
    /// domain (or `"*"` for the default), so repeat lookups avoid the network.
    pub async fn logo_resource(&self, domain: &str) -> trc::Result<Option<Resource<Vec<u8>>>> {
        const MAX_LOGO_SIZE: usize = 1024 * 1024;

        // Normalise to the registrable domain so `mail.example.org` and
        // `example.org` share one cache entry.
        let mut cache_key = psl::domain_str(domain).unwrap_or(domain);

        if let Some(cached) = self.inner.data.logos.lock().get(cache_key).cloned() {
            return Ok(cached.data);
        }

        // Resolve the logo URL from the domain record, then its tenant.
        let mut logo_url = None;
        let mut domain_id = u32::MAX;
        let mut tenant_id = None;

        if let Some((id, id_tenant)) = self.domain(cache_key).await?.map(|d| (d.id, d.id_tenant))
            && let Some(record) = self.registry().object::<Domain>(id.into()).await?
        {
            domain_id = id;
            tenant_id = id_tenant;
            logo_url = record.logo;

            if logo_url.is_none()
                && let Some(tenant_id) = tenant_id
            {
                logo_url = self
                    .registry()
                    .object::<Tenant>(tenant_id.into())
                    .await?
                    .and_then(|tenant| tenant.logo);
            }
        } else {
            // Unknown domain: only the server-wide default can apply.
            cache_key = "*";
        }

        // Fall back to the deployment default, reusing its `"*"` cache slot.
        if logo_url.is_none()
            && let Some(default_url) = self.core.branding.logo_url.clone()
        {
            if let Some(cached) = self.inner.data.logos.lock().get("*").cloned() {
                return Ok(cached.data);
            }
            logo_url = Some(default_url);
        }

        // Download and wrap the image, if any URL resolved.
        let mut resource = None;
        if let Some(logo_url) = logo_url {
            let response = utils::http::http_client_builder(false)
                .user_agent(USER_AGENT)
                .build()
                .unwrap_or_default()
                .get(logo_url.as_str())
                .send()
                .await
                .map_err(|err| {
                    trc::ResourceEvent::DownloadExternal
                        .into_err()
                        .details("Failed to download logo")
                        .reason(err)
                })?;

            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("image/svg+xml")
                .to_string();

            let contents = response
                .bytes_with_limit(MAX_LOGO_SIZE)
                .await
                .map_err(|err| {
                    trc::ResourceEvent::DownloadExternal
                        .into_err()
                        .details("Failed to download logo")
                        .reason(err)
                })?
                .ok_or_else(|| {
                    trc::ResourceEvent::DownloadExternal
                        .into_err()
                        .details("Download exceeded maximum size")
                })?;

            resource = Some(Resource::new(content_type, contents));
        }

        self.inner.data.logos.lock().insert(
            cache_key.into(),
            LogoCache {
                domain_id,
                tenant_id,
                data: resource.clone(),
            },
        );

        Ok(resource)
    }
}
