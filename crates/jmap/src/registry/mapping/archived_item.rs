/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

//! JMAP mapping for the `ArchivedItem` object: the recoverable record left
//! behind when a message (or other blob-backed item) is soft-deleted while a
//! retention window is in effect.
//!
//! Archived items are never created through JMAP — they are produced by the
//! delete path. Clients may only:
//!   * extend/shorten the retention deadline (`archivedUntil`),
//!   * request a restore (`status = requestRestore`), which schedules a
//!     `RestoreArchivedItem` task and removes the archive record, or
//!   * destroy the item, dropping the retained blob permanently.
//!
//! `get`/`query` mirror the shared registry-object idiom; with impersonation a
//! caller may reach across accounts, otherwise everything is scoped to the
//! caller's own account.

use crate::{
    api::query::QueryResponseBuilder,
    registry::{
        mapping::{RegistryGetResponse, RegistryQueryResponse, RegistrySetResponse},
        query::RegistryQueryFilters,
    },
};
use jmap_proto::{error::set::SetError, types::state::State};
use jmap_tools::{Key, Value};
use registry::{
    jmap::{IntoValue, RegistryValue},
    schema::{
        enums::{ArchivedItemStatus, Permission},
        prelude::{Object, ObjectType, Property},
        structs::{ArchivedItem, Task, TaskRestoreArchivedItem, TaskStatus},
    },
    types::{EnumImpl, ObjectImpl, datetime::UTCDateTime, id::ObjectId},
};
use std::str::FromStr;
use store::{
    SerializeInfallible, ValueKey,
    registry::RegistryQuery,
    write::{BatchBuilder, BlobLink, BlobOp, RegistryClass, ValueClass, assert::AssertValue},
};
use trc::AddContext;
use types::{blob::BlobClass, id::Id};

/// The patch fields a client supplied on an archived-item update.
#[derive(Default)]
struct ArchivedItemPatch {
    status: ArchivedItemStatus,
    archived_until: Option<UTCDateTime>,
}

/// Parses and validates an update patch. On any invalid field it records the
/// error against `id` and returns `None`, signalling the caller to skip.
fn parse_update_patch(
    set: &mut RegistrySetResponse<'_>,
    id: Id,
    value: Value<'_, Property, RegistryValue>,
    now: UTCDateTime,
) -> Option<ArchivedItemPatch> {
    let mut patch = ArchivedItemPatch::default();

    for (key, value) in value.into_expanded_object() {
        match (key, value) {
            (Key::Property(Property::Status), Value::Str(status)) => {
                match ArchivedItemStatus::parse(&status) {
                    Some(status) => patch.status = status,
                    None => {
                        set.response.not_updated.append(
                            id,
                            SetError::invalid_patch()
                                .with_property(Property::Status)
                                .with_description("Invalid value for property"),
                        );
                        return None;
                    }
                }
            }
            (Key::Property(Property::ArchivedUntil), Value::Str(archived_until)) => {
                match UTCDateTime::from_str(archived_until.as_ref())
                    .ok()
                    .filter(|when| *when > now)
                {
                    Some(when) => patch.archived_until = Some(when),
                    None => {
                        set.response.not_updated.append(
                            id,
                            SetError::invalid_patch()
                                .with_property(Property::ArchivedUntil)
                                .with_description("Invalid value for property"),
                        );
                        return None;
                    }
                }
            }
            // The id is immutable but accepted so an echoed value is a no-op.
            (Key::Property(Property::Id), _) => {}
            (key, _) => {
                set.response.not_updated.append(
                    id,
                    SetError::invalid_properties().with_property(key.into_owned()),
                );
                return None;
            }
        }
    }

    Some(patch)
}

pub(crate) async fn archived_item_set(
    mut set: RegistrySetResponse<'_>,
) -> trc::Result<RegistrySetResponse<'_>> {
    // Archived items are produced by the delete path, never by clients.
    set.fail_all_create("Archived items cannot be created");

    let mut batch = BatchBuilder::new();
    let object_id = set.object_type.to_id();

    // --- Updates -------------------------------------------------------------
    for (id, value) in std::mem::take(&mut set.update) {
        let now = UTCDateTime::now();
        let Some(patch) = parse_update_patch(&mut set, id, value, now) else {
            continue;
        };

        let wants_restore = patch.status == ArchivedItemStatus::RequestRestore;
        let item_id = id.id();
        let stored = set
            .server
            .store()
            .get_value::<Object>(ValueKey::from(ValueClass::Registry(RegistryClass::Item {
                object_id,
                item_id,
            })))
            .await?
            .filter(|item| {
                !set.is_account_filtered || item.inner.account_id() == Some(set.account_id.into())
            });

        let Some(stored) = stored else {
            set.response.not_updated.append(id, SetError::not_found());
            continue;
        };

        let revision = stored.revision;
        let item = ArchivedItem::from(stored);

        // A restore request takes precedence; otherwise a new deadline (if any)
        // reschedules retention. A patch with neither is a no-op on a record
        // that does exist, so it still reports success.
        if wants_restore {
            request_restore(&mut batch, object_id, item_id, item, revision);
            batch.commit_point();
        } else if let Some(new_until) = patch.archived_until {
            reschedule_retention(&mut batch, object_id, item_id, item, revision, new_until);
            batch.commit_point();
        }

        set.response.updated.append(id, None);
    }

    // --- Destroys ------------------------------------------------------------
    for id in std::mem::take(&mut set.destroy) {
        let item_id = id.id();

        let stored = set
            .server
            .store()
            .get_value::<ArchivedItem>(ValueKey::from(ValueClass::Registry(
                RegistryClass::Item { object_id, item_id },
            )))
            .await?
            .filter(|item| {
                !set.is_account_filtered || item.account_id().document_id() == set.account_id
            });

        let Some(item) = stored else {
            set.response.not_destroyed.append(id, SetError::not_found());
            continue;
        };

        let account_id = item.account_id().id();
        let until = item.archived_until().timestamp() as u64;
        let blob_hash = item.into_blob_id().hash;

        batch
            .with_account_id(account_id as u32)
            .clear(BlobOp::Link {
                hash: blob_hash,
                to: BlobLink::Temporary { until },
            })
            .clear(ValueClass::Registry(RegistryClass::Index {
                index_id: Property::AccountId.to_id(),
                object_id,
                item_id,
                key: account_id.serialize(),
            }))
            .clear(ValueClass::Registry(RegistryClass::Item { object_id, item_id }))
            .commit_point();

        set.response.destroyed.push(id);
    }

    if !batch.is_empty() {
        set.server
            .store()
            .write(batch.build_all())
            .await
            .caused_by(trc::location!())?;
        set.server.notify_task_queue();
    }

    Ok(set)
}

/// Moves the retained blob's expiry link from the old deadline to `new_until`
/// and persists the updated item, guarded by an optimistic-concurrency assert.
fn reschedule_retention(
    batch: &mut BatchBuilder,
    object_id: u16,
    item_id: u64,
    mut item: ArchivedItem,
    revision: u64,
    new_until: UTCDateTime,
) {
    let old_until = item.archived_until();
    if old_until == new_until {
        return;
    }

    item.set_archived_until(new_until);
    let blob_hash = item.blob_id().hash.clone();

    batch
        .with_account_id(item.account_id().document_id())
        .assert_value(
            ValueClass::Registry(RegistryClass::Item { object_id, item_id }),
            AssertValue::Hash(revision),
        )
        .clear(BlobOp::Link {
            hash: blob_hash.clone(),
            to: BlobLink::Temporary {
                until: old_until.timestamp() as u64,
            },
        })
        .set(
            BlobOp::Link {
                hash: blob_hash,
                to: BlobLink::Temporary {
                    until: new_until.timestamp() as u64,
                },
            },
            ObjectId::new(ObjectType::ArchivedItem, item_id.into()).serialize(),
        )
        .set(
            ValueClass::Registry(RegistryClass::Item { object_id, item_id }),
            item.to_pickled_vec(),
        );
}

/// Removes the archive record (index + item) and schedules the restore task
/// that reinstates the underlying object, guarded by an assert on the revision.
fn request_restore(
    batch: &mut BatchBuilder,
    object_id: u16,
    item_id: u64,
    item: ArchivedItem,
    revision: u64,
) {
    let account_id = item.account_id();

    batch
        .assert_value(
            ValueClass::Registry(RegistryClass::Item { object_id, item_id }),
            AssertValue::Hash(revision),
        )
        .clear(ValueClass::Registry(RegistryClass::Index {
            index_id: Property::AccountId.to_id(),
            object_id,
            item_id,
            key: account_id.id().serialize(),
        }))
        .clear(ValueClass::Registry(RegistryClass::Item { object_id, item_id }))
        .schedule_task(Task::RestoreArchivedItem(TaskRestoreArchivedItem {
            account_id,
            archived_item_type: item.object_type(),
            archived_until: item.archived_until(),
            blob_id: item.blob_id().clone(),
            created_at: item.created_at(),
            status: TaskStatus::now(),
        }));
}

pub(crate) async fn archived_item_get(
    mut get: RegistryGetResponse<'_>,
) -> trc::Result<RegistryGetResponse<'_>> {
    let object_id = get.object_type.to_id();
    let ids = if let Some(ids) = get.ids.take() {
        ids
    } else {
        let query = if !get.is_account_filtered {
            RegistryQuery::new(get.object_type).greater_than_or_equal(Property::AccountId, 0u64)
        } else {
            RegistryQuery::new(get.object_type).with_account(get.account_id)
        }
        .with_limit(get.server.core.jmap.get_max_objects);

        get.server.registry().query::<Vec<Id>>(query).await?
    };

    for id in ids {
        let stored = get
            .server
            .store()
            .get_value::<ArchivedItem>(ValueKey::from(ValueClass::Registry(
                RegistryClass::Item {
                    object_id,
                    item_id: id.id(),
                },
            )))
            .await?
            .filter(|item| {
                !get.is_account_filtered || item.account_id().document_id() == get.account_id
            });

        match stored {
            Some(mut item) => {
                // Expose the retained blob as downloadable to its owner.
                if get.is_account_filtered {
                    let expires = item.archived_until().timestamp() as u64;
                    item.blob_id_mut().class = BlobClass::Reserved {
                        account_id: get.account_id,
                        expires,
                    };
                }
                get.insert(id, item.into_value());
            }
            None => get.not_found(id),
        }
    }

    Ok(get)
}

pub(crate) async fn archived_item_query(
    mut req: RegistryQueryResponse<'_>,
) -> trc::Result<QueryResponseBuilder> {
    let can_impersonate = req.access_token.has_permission(Permission::Impersonate);
    let mut impersonated_account = None;

    // Only an impersonating caller may target another account; everyone else is
    // pinned to their own.
    req.request.extract_filters(|property, _, value| match property {
        Property::AccountId if can_impersonate => {
            match value.as_str().and_then(|s| Id::from_str(s).ok()) {
                Some(id) => {
                    impersonated_account = Some(id);
                    true
                }
                None => false,
            }
        }
        _ => false,
    })?;

    let mut query = match (impersonated_account, can_impersonate) {
        (Some(account_id), _) => {
            RegistryQuery::new(req.object_type).with_account(account_id.document_id())
        }
        (None, false) => {
            RegistryQuery::new(req.object_type).with_account(req.request.account_id.document_id())
        }
        (None, true) => {
            RegistryQuery::new(req.object_type).greater_than_or_equal(Property::AccountId, 0u64)
        }
    };

    let params = req
        .request
        .extract_parameters(req.server.core.jmap.query_max_results, Some(Property::Id))?;

    if let Some(limit) = params.limit {
        query = query.with_limit(limit);
        if let Some(anchor) = params.anchor {
            query = query.with_anchor(anchor);
        } else if let Some(position) = params.position {
            query = query.with_index_start(position);
        }
    }

    let mut results = req.server.registry().query::<Vec<Id>>(query).await?;

    // Only id-ordering is supported; descending is applied client-side here
    // since the registry index returns ids ascending.
    match params.sort_by {
        Property::Id => {
            if !params.sort_ascending {
                results.sort_unstable_by(|a, b| b.cmp(a));
            }
        }
        property => {
            return Err(trc::JmapEvent::UnsupportedSort
                .into_err()
                .details(format!("Property {property} is not supported for sorting")));
        }
    }

    let mut response = QueryResponseBuilder::new(
        results.len(),
        req.server.core.jmap.query_max_results,
        State::Initial,
        &req.request,
    );

    for id in results {
        if !response.add_id(id) {
            break;
        }
    }

    Ok(response)
}
