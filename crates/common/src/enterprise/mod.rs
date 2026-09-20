/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: LicenseRef-SEL
 *
 * This file is subject to the Stalwart Enterprise License Agreement (SEL) and
 * is NOT open source software.
 *
 */

pub mod config;
pub mod license;

use crate::{
    Core, Server, config::groupware::CalendarTemplateVariable,
};
use license::LicenseKey;
use mail_parser::DateTime;
use std::{sync::Arc, time::Duration};
use trc::AddContext;
use utils::template::Template;

#[derive(Clone)]
pub struct Enterprise {
    pub license: LicenseKey,
    pub logo_url: Option<String>,
    pub deleted_items_retention: Option<Duration>,
    pub deleted_accounts_retention: Option<Duration>,
    pub template_calendar_alarm: Option<Template<CalendarTemplateVariable>>,
    pub template_scheduling_email: Option<Template<CalendarTemplateVariable>>,
    pub template_scheduling_web: Option<Arc<str>>,
}

impl Core {
    pub fn is_enterprise_edition(&self) -> bool {
        self.enterprise
            .as_ref()
            .is_some_and(|e| !e.license.is_expired())
    }
}

impl Server {
    // WARNING: TAMPERING WITH THIS FUNCTION IS STRICTLY PROHIBITED
    // Any attempt to modify, bypass, or disable this license validation mechanism
    // constitutes a severe violation of the Stalwart Enterprise License Agreement.
    // Such actions may result in immediate termination of your license, legal action,
    // and substantial financial penalties. Stalwart Labs LLC actively monitors for
    // unauthorized modifications and will pursue all available legal remedies against
    // violators to the fullest extent of the law, including but not limited to claims
    // for copyright infringement, breach of contract, and fraud.

    #[inline]
    pub fn is_enterprise_edition(&self) -> bool {
        self.core.is_enterprise_edition()
    }

    pub fn licensed_accounts(&self) -> u32 {
        self.core
            .enterprise
            .as_ref()
            .map_or(0, |e| e.license.accounts)
    }

    pub fn log_license_details(&self) {
        if let Some(enterprise) = &self.core.enterprise {
            trc::event!(
                Server(trc::ServerEvent::Licensing),
                Details = "Stalwart Enterprise Edition license key is valid",
                Domain = enterprise.license.domain.clone(),
                Total = enterprise.license.accounts,
                ValidFrom =
                    DateTime::from_timestamp(enterprise.license.valid_from as i64).to_rfc3339(),
                ValidTo = DateTime::from_timestamp(enterprise.license.valid_to as i64).to_rfc3339(),
            );
        }
    }

    pub async fn can_create_account(&self) -> trc::Result<bool> {
        if let Some(enterprise) = &self.core.enterprise {
            let total_accounts = self.total_accounts().await.caused_by(trc::location!())?;

            if total_accounts + 1 > enterprise.license.accounts as usize {
                trc::event!(
                    Server(trc::ServerEvent::Licensing),
                    Details = "Account creation not possible: license key account limit reached",
                    Domain = enterprise.license.domain.clone(),
                    Total = total_accounts,
                    Limit = enterprise.license.accounts,
                );

                return Ok(false);
            }
        }

        Ok(true)
    }

}
