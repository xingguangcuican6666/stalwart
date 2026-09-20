/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: LicenseRef-SEL
 *
 * This file is subject to the Stalwart Enterprise License Agreement (SEL) and
 * is NOT open source software.
 *
 */

use super::{Enterprise, license::LicenseKey};
use registry::schema::{
    prelude::{ObjectType, Property},
    structs::{
        self, CalendarAlarm, CalendarScheduling, DataRetention, SecretKeyOptional, SecretKeyValue,
        SystemSettings,
    },
};
use std::sync::Arc;
use store::{
    registry::{RegistryQuery, bootstrap::Bootstrap, write::RegistryWrite},
    roaring::RoaringBitmap,
};
use utils::template::Template;

impl Enterprise {
    pub async fn parse(bp: &mut Bootstrap) -> Option<Self> {
        let server_hostname = bp
            .setting_infallible::<SystemSettings>()
            .await
            .default_hostname;
        let mut update_license = None;
        let mut enterprise = bp.setting_infallible::<structs::Enterprise>().await;

        // WARNING: TAMPERING WITH THIS FUNCTION IS STRICTLY PROHIBITED
        // Any attempt to modify, bypass, or disable this license validation mechanism
        // constitutes a severe violation of the Stalwart Enterprise License Agreement.
        // Such actions may result in immediate termination of your license, legal action,
        // and substantial financial penalties. Stalwart Labs LLC actively monitors for
        // unauthorized modifications and will pursue all available legal remedies against
        // violators to the fullest extent of the law, including but not limited to claims
        // for copyright infringement, breach of contract, and fraud.

        let license_result = match (
            enterprise.license_key.secret().await,
            enterprise.api_key.secret().await,
        ) {
            (Ok(Some(license_key)), Ok(maybe_api_key)) => {
                match (
                    LicenseKey::new(license_key, &server_hostname),
                    maybe_api_key,
                ) {
                    (Ok(license), Some(api_key)) if license.is_near_expiration() => Ok(license
                        .try_renew(api_key.as_ref())
                        .await
                        .map(|result| {
                            update_license = Some(result.encoded_key);
                            result.key
                        })
                        .unwrap_or(license)),
                    (Ok(license), None) => Ok(license),
                    (Err(_), Some(api_key)) => LicenseKey::invalid(&server_hostname)
                        .try_renew(api_key.as_ref())
                        .await
                        .map(|result| {
                            update_license = Some(result.encoded_key);
                            result.key
                        }),
                    (maybe_license, _) => maybe_license,
                }
            }
            (Ok(None), Ok(Some(api_key))) => LicenseKey::invalid(&server_hostname)
                .try_renew(api_key.as_ref())
                .await
                .map(|result| {
                    update_license = Some(result.encoded_key);
                    result.key
                }),
            (Ok(None), Ok(None)) => {
                #[cfg(not(feature = "test_mode"))]
                return None;

                #[cfg(feature = "test_mode")]
                Ok(LicenseKey {
                    valid_to: store::write::now() + (86400 * 365),
                    valid_from: store::write::now() - 3600,
                    domain: server_hostname.to_string(),
                    accounts: 100,
                })
            }
            (Err(err), _) => {
                bp.build_error(ObjectType::Enterprise.singleton(), err);
                return None;
            }
            (_, Err(err)) => {
                bp.build_error(ObjectType::Enterprise.singleton(), err);
                return None;
            }
        };

        // Report error
        let license = match license_result {
            Ok(license) => license,
            Err(err) => {
                bp.build_warning(ObjectType::Enterprise.singleton(), err.to_string());
                return None;
            }
        };

        // Update the license if a new one was obtained
        let logo_url = enterprise.logo_url.clone();
        if let Some(license) = update_license {
            enterprise.license_key = SecretKeyOptional::Value(SecretKeyValue { secret: license });
            if let Err(err) = bp
                .registry
                .write(RegistryWrite::insert(&enterprise.into()))
                .await
            {
                trc::error!(
                    err.caused_by(trc::location!())
                        .details("Failed to update license key")
                );
            }
        }

        match bp
            .registry
            .query::<RoaringBitmap>(RegistryQuery::new(ObjectType::Account))
            .await
        {
            Ok(total) if total.len() > license.accounts as u64 => {
                bp.build_warning(
                    ObjectType::Enterprise.singleton(),
                    format!(
                        "License key is valid but only allows {} accounts, found {}.",
                        license.accounts,
                        total.len()
                    ),
                );
                return None;
            }
            Err(e) => {
                trc::error!(
                    e.caused_by(trc::location!())
                        .details("Failed to count total individual principals")
                );
                return None;
            }
            _ => (),
        }

        let dr = bp.setting_infallible::<DataRetention>().await;

        // Build the enterprise configuration
        let mut enterprise = Enterprise {
            license,
            deleted_items_retention: dr
                .archive_deleted_items_for
                .map(|retention| retention.into_inner()),
            deleted_accounts_retention: dr
                .archive_deleted_accounts_for
                .map(|retention| retention.into_inner()),
            logo_url,
            template_calendar_alarm: None,
            template_scheduling_email: None,
            template_scheduling_web: None,
        };

        // Parse templates
        let sched = bp.setting_infallible::<CalendarScheduling>().await;
        let alarm = bp.setting_infallible::<CalendarAlarm>().await;

        for (template, value, object, property) in [
            (
                alarm.template,
                &mut enterprise.template_calendar_alarm,
                ObjectType::CalendarAlarm.singleton(),
                Property::Template,
            ),
            (
                sched.email_template,
                &mut enterprise.template_scheduling_email,
                ObjectType::CalendarScheduling.singleton(),
                Property::EmailTemplate,
            ),
        ] {
            if let Some(template) = template {
                match Template::parse(&template) {
                    Ok(template) => *value = Some(template),
                    Err(err) => {
                        bp.invalid_property(object, property, format!("Invalid template: {err}"));
                    }
                }
            }
        }

        enterprise.template_scheduling_web = sched
            .http_rsvp_template
            .as_deref()
            .map(|page| page.trim())
            .filter(|page| !page.is_empty())
            .map(Arc::from);

        Some(enterprise)
    }
}
