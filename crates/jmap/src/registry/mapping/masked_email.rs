/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::registry::mapping::{ObjectResponse, RegistrySetResponse, ValidationResult};
use common::masked::MaskedAddress;
use jmap_proto::error::set::SetError;
use rand::{RngExt, distr::Alphanumeric};
use registry::{
    jmap::JmapValue,
    schema::{
        enums::StorageQuota,
        prelude::{ObjectType, Property},
        structs::MaskedEmail,
    },
};
use store::{
    registry::{RegistryObjectCounter, RegistryQuery},
    write::now,
};
use utils::{DomainPart, map::vec_map::VecMap};

/// Number of characters in an auto-generated prefix when the client does not
/// supply one.
const RANDOM_PREFIX_LEN: usize = 16;

/// Validates a masked-email create/update.
///
/// Masked addresses are immutable once minted, so updates only accept an
/// unchanged `email` and reject everything else. Creation enforces the account
/// quota, validates the (optional) client-supplied prefix and domain, and then
/// mints the opaque address via [`MaskedAddress::generate`].
pub(crate) async fn validate_masked_email(
    set: &RegistrySetResponse<'_>,
    addr: &mut MaskedEmail,
    is_create: bool,
    unpatched_properties: VecMap<Property, JmapValue<'_>>,
) -> ValidationResult {
    let mut response = ObjectResponse::default();

    if !is_create {
        // Updates are read-only: the only property that may appear is the
        // existing `email`, and only if it is unchanged.
        for (key, value) in unpatched_properties {
            let unchanged = matches!(
                (&key, &value),
                (Property::Email, JmapValue::Str(email)) if *email == addr.email
            );
            if !unchanged {
                return Ok(Err(SetError::invalid_properties()
                    .with_property(key)
                    .with_description("Cannot modify read-only property")));
            }
        }
        return Ok(Ok(response));
    }

    // --- Quota ---------------------------------------------------------------
    let existing = set
        .server
        .registry()
        .query::<RegistryObjectCounter>(
            RegistryQuery::new(ObjectType::MaskedEmail).with_account(set.account_id),
        )
        .await?
        .0 as u32;
    let account = set.server.account(set.account_id).await?;
    let quota = set
        .server
        .object_quota(account.object_quotas(), StorageQuota::MaxMaskedAddresses);
    if existing >= quota {
        return Ok(Err(SetError::over_quota().with_description(format!(
            "You have exceeded your quota of {quota} masked addresses."
        ))));
    }

    // --- Requested prefix / domain -------------------------------------------
    let mut requested_prefix = None;
    let mut requested_domain = None;

    for (key, value) in unpatched_properties {
        match (key, value) {
            (Property::EmailPrefix, JmapValue::Str(prefix)) if is_valid_prefix(&prefix) => {
                requested_prefix = Some(prefix.to_lowercase());
            }
            (Property::EmailDomain, JmapValue::Str(domain)) if !domain.is_empty() => {
                let domain = domain.to_lowercase();
                if !account_owns_domain(set, &account, &domain).await? {
                    return Ok(Err(SetError::forbidden()
                        .with_property(Property::EmailDomain)
                        .with_description("The specified domain is not valid for this account.")));
                }
                requested_domain = Some(domain);
            }
            // A null clears an optional property; nothing to validate.
            (_, JmapValue::Null) => {}
            (key, _) => {
                return Ok(Err(SetError::invalid_properties().with_property(key)));
            }
        }
    }

    // Fall back to the account's own domain and a random prefix when unset.
    let domain = match requested_domain {
        Some(domain) => domain,
        None => match account.name.try_domain_part() {
            Some(domain) => domain.to_string(),
            None => {
                return Ok(Err(SetError::forbidden()
                    .with_property(Property::EmailDomain)
                    .with_description("No valid domain is available for this account.")));
            }
        },
    };
    let prefix = requested_prefix.unwrap_or_else(random_prefix);

    // --- Mint the address ----------------------------------------------------
    let address_id = set.server.registry().assign_id();
    addr.email = MaskedAddress::generate(
        address_id,
        lifetime_secs(addr),
        &prefix,
        &domain,
    );

    response.id = Some(address_id.into());
    response
        .object
        .insert_unchecked(Property::Email, addr.email.clone());

    Ok(Ok(response))
}

/// A prefix is 1-64 chars of `[A-Za-z0-9_]` that does not start with `_`.
fn is_valid_prefix(prefix: &str) -> bool {
    (1..=64).contains(&prefix.len())
        && prefix.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !prefix.starts_with('_')
}

/// True when one of the account's addresses lives in `domain` and the domain
/// exists in the directory.
async fn account_owns_domain(
    set: &RegistrySetResponse<'_>,
    account: &common::auth::AccountCache,
    domain: &str,
) -> trc::Result<bool> {
    Ok(set.server.domain(domain).await?.is_some_and(|domain| {
        account
            .addresses
            .iter()
            .any(|addr| addr.domain_id == domain.id)
    }))
}

/// Converts the client-supplied absolute `expires_at` into a lifetime in
/// seconds relative to now, dropping non-positive/expired values.
fn lifetime_secs(addr: &MaskedEmail) -> Option<u32> {
    addr.expires_at
        .map(|t| (t.timestamp() as u64).saturating_sub(now()))
        .filter(|secs| *secs > 0)
        .map(|secs| secs as u32)
}

fn random_prefix() -> String {
    rand::rng()
        .sample_iter(Alphanumeric)
        .take(RANDOM_PREFIX_LEN)
        .map(|ch| char::from(ch.to_ascii_lowercase()))
        .collect()
}
