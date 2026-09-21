/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use registry::schema::structs::DataRetention;
use std::time::Duration;
use store::registry::bootstrap::Bootstrap;

/// How long soft-deleted data is retained before permanent removal.
///
/// When a retention window is set, deleted messages keep their blob alive
/// (indexed as a recoverable `ArchivedItem`) and account destruction is delayed
/// by that window rather than run immediately, so both can be restored within
/// the window. `None` means no retention: deletion is immediate and permanent.
#[derive(Debug, Clone, Default)]
pub struct RetentionConfig {
    /// Retention window for deleted messages/items.
    pub deleted_items: Option<Duration>,
    /// Retention window (destruction delay) for deleted accounts.
    pub deleted_accounts: Option<Duration>,
}

impl RetentionConfig {
    pub async fn parse(bp: &mut Bootstrap) -> Self {
        let dr = bp.setting_infallible::<DataRetention>().await;
        RetentionConfig {
            deleted_items: dr.archive_deleted_items_for.map(|d| d.into_inner()),
            deleted_accounts: dr.archive_deleted_accounts_for.map(|d| d.into_inner()),
        }
    }
}
