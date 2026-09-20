/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use registry::schema::structs::Enterprise;
use store::registry::bootstrap::Bootstrap;

/// Deployment branding, currently just the fallback logo used when neither a
/// domain nor its tenant defines one of its own. Sourced from the operator
/// settings object; carries no licensing semantics.
#[derive(Debug, Clone, Default)]
pub struct BrandingConfig {
    /// URL of the server-wide default logo, downloaded and cached on demand.
    pub logo_url: Option<String>,
}

impl BrandingConfig {
    pub async fn parse(bp: &mut Bootstrap) -> Self {
        let settings = bp.setting_infallible::<Enterprise>().await;
        BrandingConfig {
            logo_url: settings
                .logo_url
                .map(|url| url.trim().to_string())
                .filter(|url| !url.is_empty()),
        }
    }
}
