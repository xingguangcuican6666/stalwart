/*
 * SPDX-FileCopyrightText: 2026 Stalwart clean-room contributors
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Clean-room reimplementation of the telemetry read side of the registry API:
 * the JMAP `get`/`query` handlers for the `Trace` and `Metric` object types.
 * Written from the AGPL registry-mapping contract (RegistryGetResponse /
 * RegistryQueryResponse / QueryResponseBuilder), the AGPL search API, and the
 * AGPL-generated `Trace`/`Metric` schema types. The persisted byte layout is
 * the one produced by the clean-room write side (telemetry stores). No SEL
 * source was consulted.
 */

use crate::{
    api::query::QueryResponseBuilder,
    registry::{
        mapping::{RegistryGetResponse, RegistryQueryResponse},
        query::RegistryQueryFilters,
    },
};
use common::Server;
use jmap_proto::types::state::State;
use registry::{
    jmap::{IntoValue, JmapValue},
    schema::{
        prelude::Property,
        structs::{Metric, Trace, TraceEvent, TraceValue},
    },
    types::datetime::UTCDateTime,
};
use std::str::FromStr;
use store::{
    Deserialize, IterateParams, ValueKey,
    ahash::AHashSet,
    registry::RegistryFilterOp,
    search::{
        SearchComparator, SearchField, SearchFilter, SearchOperator, SearchQuery,
        TracingSearchField,
    },
    write::{SearchIndex, TelemetryClass, ValueClass, key::DeserializeBigEndian, now},
};
use trc::{AddContext, EventType, Key, MetricType};
use types::id::Id;
use utils::snowflake::SnowflakeIdGenerator;

/// Default lookback window when a query supplies no id or field constraint:
/// spans/metrics from the last 24 hours.
const DEFAULT_WINDOW_SECS: u64 = 86400;

// ---------------------------------------------------------------------------
// Trace: get
// ---------------------------------------------------------------------------

pub(crate) async fn trace_get(
    mut get: RegistryGetResponse<'_>,
) -> trc::Result<RegistryGetResponse<'_>> {
    // Explicit ids win; otherwise return the most recent spans within the
    // default window, newest first, capped at the configured object limit.
    let ids = match get.ids.take() {
        Some(ids) => ids,
        None => {
            let recent_threshold =
                SnowflakeIdGenerator::from_timestamp(now() - DEFAULT_WINDOW_SECS).unwrap_or_default();
            get.server
                .search_store()
                .query_global(
                    SearchQuery::new(SearchIndex::Tracing)
                        .with_filter(SearchFilter::gt(SearchField::Id, recent_threshold))
                        .with_comparator(SearchComparator::Field {
                            field: SearchField::Id,
                            ascending: false,
                        }),
                )
                .await?
                .into_iter()
                .take(get.server.core.jmap.get_max_objects)
                .map(Id::from)
                .collect()
        }
    };

    // Which summary fields the caller asked for (empty selection means all).
    let want = |p: Property| get.properties.is_empty() || get.properties.contains(&p);
    let want_timestamp = want(Property::Timestamp);
    let want_from = want(Property::From);
    let want_to = want(Property::To);
    let want_size = want(Property::Size);

    for id in ids {
        let Some(trace) = get
            .server
            .tracing_store()
            .get_value::<Trace>(span_key(id.id()))
            .await?
        else {
            get.not_found(id);
            continue;
        };

        // Hoist a few convenient summary fields out of the span's events so the
        // list view can show them without decoding every event. Each field is
        // taken from the first event that carries it.
        let mut summary = Vec::with_capacity(4);
        let mut need_timestamp = want_timestamp;
        let mut need_from = want_from;
        let mut need_to = want_to;
        let mut need_size = want_size;

        for event in trace.events.iter() {
            if need_timestamp {
                summary.push((Property::Timestamp, event.timestamp.into_value()));
                need_timestamp = false;
            }
            if need_from
                && let Some(value) = event_value(event, Key::From).map(trace_value_as_string)
            {
                summary.push((Property::From, value));
                need_from = false;
            }
            if need_to && let Some(value) = event_value(event, Key::To).map(trace_value_as_string) {
                summary.push((Property::To, value));
                need_to = false;
            }
            if need_size
                && let Some(value) = event_value(event, Key::Size).map(trace_value_as_number)
            {
                summary.push((Property::Size, value));
                need_size = false;
            }
        }

        let mut object = trace.into_value();
        let map = object.as_object_mut().unwrap();
        for (property, value) in summary {
            map.insert_unchecked(property, value);
        }

        get.insert(id, object);
    }

    Ok(get)
}

/// Locate the first value carried under `key` within a span event.
fn event_value(event: &TraceEvent, key: Key) -> Option<&TraceValue> {
    event
        .key_values
        .iter()
        .find_map(|kv| (kv.key == key).then_some(&kv.value))
}

/// Render a trace value as a JMAP string. Lists are flattened to a
/// "; "-separated string of their string members.
fn trace_value_as_string(value: &TraceValue) -> JmapValue<'static> {
    match value {
        TraceValue::String(s) => JmapValue::Str(s.value.clone().into()),
        TraceValue::List(list) => {
            let mut joined = String::new();
            for item in list.value.iter() {
                if let TraceValue::String(s) = item {
                    if !joined.is_empty() {
                        joined.push_str("; ");
                    }
                    joined.push_str(&s.value);
                }
            }
            JmapValue::Str(joined.into())
        }
        _ => JmapValue::Null,
    }
}

/// Render a numeric trace value as a JMAP number.
fn trace_value_as_number(value: &TraceValue) -> JmapValue<'static> {
    match value {
        TraceValue::Integer(v) => JmapValue::Number(v.value.into()),
        TraceValue::UnsignedInt(v) => JmapValue::Number(v.value.into()),
        TraceValue::Float(v) => JmapValue::Number(v.value.into_inner().into()),
        _ => JmapValue::Null,
    }
}

// ---------------------------------------------------------------------------
// Metric: get
// ---------------------------------------------------------------------------

pub(crate) async fn metric_get(
    mut get: RegistryGetResponse<'_>,
) -> trc::Result<RegistryGetResponse<'_>> {
    let ids = match get.ids.take() {
        Some(ids) => ids,
        None => metric_ids(get.server, get.server.core.jmap.get_max_objects).await?,
    };

    for id in ids {
        let item_id = id.id();
        let Some(metric) = get
            .server
            .metrics_store()
            .get_value::<Metric>(metric_key(item_id))
            .await?
        else {
            get.not_found(id);
            continue;
        };

        // The record's timestamp is encoded in the high bits of its snowflake
        // id; surface it as an explicit property.
        let timestamp =
            UTCDateTime::from_timestamp(SnowflakeIdGenerator::to_timestamp(item_id) as i64);
        let mut object = metric.into_value();
        object
            .as_object_mut()
            .unwrap()
            .insert_unchecked(Property::Timestamp, timestamp.into_value());

        get.insert(id, object);
    }

    Ok(get)
}

// ---------------------------------------------------------------------------
// Trace: query
// ---------------------------------------------------------------------------

pub(crate) async fn trace_query(
    mut req: RegistryQueryResponse<'_>,
) -> trc::Result<QueryResponseBuilder> {
    // Translate the JMAP filter conditions into a search query over the
    // tracing index.
    let mut filters = vec![SearchFilter::And];

    req.request.extract_filters(|property, op, value| {
        match property {
            Property::Timestamp => {
                let Some(id) = value
                    .as_str()
                    .and_then(|s| UTCDateTime::from_str(s).ok())
                    .and_then(|dt| SnowflakeIdGenerator::from_timestamp(dt.timestamp() as u64))
                else {
                    return false;
                };
                let Some(op) = search_operator(op) else {
                    return false;
                };
                filters.push(SearchFilter::Operator {
                    field: SearchField::Id,
                    op,
                    value: id.into(),
                });
                true
            }
            Property::Event => match value.as_str().and_then(EventType::parse) {
                Some(event_type) => {
                    filters.push(SearchFilter::eq(
                        TracingSearchField::EventType,
                        event_type.to_id() as u64,
                    ));
                    true
                }
                None => false,
            },
            Property::QueueId => match value.as_str().and_then(|s| Id::from_str(s).ok()) {
                Some(queue_id) => {
                    filters.push(SearchFilter::eq(TracingSearchField::QueueId, queue_id.id()));
                    true
                }
                None => false,
            },
            Property::Text => match value.as_str() {
                Some(text) => {
                    for keyword in tokenize_query(text) {
                        filters.push(SearchFilter::has_keyword(
                            TracingSearchField::Keywords,
                            keyword,
                        ));
                    }
                    true
                }
                None => false,
            },
            _ => false,
        }
    })?;

    // Without any selective constraint, restrict to the recent window so an
    // unfiltered query does not scan the entire history.
    let has_selective_filter = filters.iter().any(|f| {
        matches!(
            f,
            SearchFilter::Operator {
                field: SearchField::Tracing(
                    TracingSearchField::Keywords | TracingSearchField::QueueId
                ) | SearchField::Id,
                ..
            }
        )
    });
    if !has_selective_filter {
        let recent_threshold =
            SnowflakeIdGenerator::from_timestamp(now() - DEFAULT_WINDOW_SECS).unwrap_or_default();
        filters.push(SearchFilter::gt(SearchField::Id, recent_threshold));
    }
    filters.push(SearchFilter::End);

    let params = req
        .request
        .extract_parameters(req.server.core.jmap.query_max_results, None)?;

    if !matches!(params.sort_by, Property::Id | Property::Timestamp) {
        return Err(trc::JmapEvent::UnsupportedSort.into_err().details(format!(
            "Property {} is not supported for sorting",
            params.sort_by
        )));
    }

    let results = req
        .server
        .search_store()
        .query_global(
            SearchQuery::new(SearchIndex::Tracing)
                .with_filters(filters)
                .with_comparator(SearchComparator::Field {
                    field: SearchField::Id,
                    ascending: params.sort_ascending,
                }),
        )
        .await?;

    let mut response = QueryResponseBuilder::new(
        results.len(),
        req.server.core.jmap.query_max_results,
        State::Initial,
        &req.request,
    );
    for id in results {
        if !response.add_id(id.into()) {
            break;
        }
    }

    Ok(response)
}

/// Split a free-text query into keywords, treating a double-quoted run as a
/// single (space-joined) keyword.
fn tokenize_query(query: &str) -> Vec<String> {
    let mut keywords = Vec::new();
    let mut current = String::new();
    let mut quoted = false;

    for ch in query.chars() {
        match ch {
            '"' => {
                current.push(ch);
                if quoted {
                    if !current.is_empty() {
                        keywords.push(std::mem::take(&mut current));
                    }
                    quoted = false;
                } else {
                    quoted = true;
                }
            }
            c if c.is_ascii_whitespace() => {
                if quoted {
                    current.push(' ');
                } else if !current.is_empty() {
                    keywords.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        keywords.push(current);
    }

    keywords
}

fn search_operator(op: RegistryFilterOp) -> Option<SearchOperator> {
    match op {
        RegistryFilterOp::Equal => Some(SearchOperator::Equal),
        RegistryFilterOp::GreaterThan => Some(SearchOperator::GreaterThan),
        RegistryFilterOp::GreaterEqualThan => Some(SearchOperator::GreaterEqualThan),
        RegistryFilterOp::LowerThan => Some(SearchOperator::LowerThan),
        RegistryFilterOp::LowerEqualThan => Some(SearchOperator::LowerEqualThan),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Metric: query
// ---------------------------------------------------------------------------

pub(crate) async fn metric_query(
    mut req: RegistryQueryResponse<'_>,
) -> trc::Result<QueryResponseBuilder> {
    // Metrics are keyed by snowflake id (time-ordered), so a timestamp filter
    // maps directly to an id range; an optional metric-type set filters within
    // that range.
    let mut lo_ts = 0u64;
    let mut hi_ts = u64::MAX;
    let mut metric_types: Option<AHashSet<MetricType>> = None;

    req.request.extract_filters(|property, op, value| {
        match property {
            Property::Timestamp => {
                let Some(ts) = value
                    .as_str()
                    .and_then(|s| UTCDateTime::from_str(s).ok())
                    .map(|dt| dt.timestamp() as u64)
                else {
                    return false;
                };
                let (from, to) = match op {
                    RegistryFilterOp::Equal => (ts, ts),
                    RegistryFilterOp::GreaterThan => (ts + 1, u64::MAX),
                    RegistryFilterOp::GreaterEqualThan => (ts, u64::MAX),
                    RegistryFilterOp::LowerThan => (0, ts - 1),
                    RegistryFilterOp::LowerEqualThan => (0, ts),
                    _ => return false,
                };
                // Successive timestamp conditions intersect.
                lo_ts = lo_ts.max(from);
                hi_ts = hi_ts.min(to);
                true
            }
            Property::Metric => {
                let Some(types) = value
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(|s| s.as_str().and_then(MetricType::parse))
                            .collect::<AHashSet<_>>()
                    })
                    .filter(|set| !set.is_empty())
                else {
                    return false;
                };
                metric_types = Some(types);
                true
            }
            _ => false,
        }
    })?;

    let params = req
        .request
        .extract_parameters(req.server.core.jmap.query_max_results, None)?;

    // Convert the timestamp bounds into snowflake id bounds.
    if lo_ts != 0 {
        lo_ts = SnowflakeIdGenerator::from_timestamp(lo_ts).unwrap_or(0);
    }
    if hi_ts != u64::MAX {
        hi_ts = SnowflakeIdGenerator::from_timestamp(hi_ts).unwrap_or(u64::MAX);
    }

    // Honor the pagination anchor by clamping the scan range to it.
    if let Some(anchor) = req.request.anchor {
        let anchor = anchor.id();
        if params.sort_ascending {
            lo_ts = lo_ts.max(anchor);
        } else {
            hi_ts = hi_ts.min(anchor);
        }
    }

    let mut response = QueryResponseBuilder::new(
        req.server.core.jmap.query_max_results + 1,
        req.server.core.jmap.query_max_results,
        State::Initial,
        &req.request,
    );
    let mut matched = 0;

    // Only decode values when a metric-type filter is active; otherwise a
    // keys-only scan is enough to collect ids.
    let needs_values = metric_types.is_some();
    req.server
        .metrics_store()
        .iterate(
            IterateParams::new(metric_key(lo_ts), metric_key(hi_ts))
                .set_ascending(params.sort_ascending)
                .set_values(needs_values),
            |key, value| {
                let id = key.deserialize_be_u64(0)?;

                if let Some(ref types) = metric_types {
                    let metric_type = match Metric::deserialize(value)? {
                        Metric::Counter(count) => count.metric,
                        Metric::Gauge(count) => count.metric,
                        Metric::Histogram(sum) => sum.metric,
                    };
                    if !types.contains(&metric_type) {
                        return Ok(true);
                    }
                }

                matched += 1;
                if response.response.total.is_some() {
                    if !response.is_full() {
                        response.add_id(id.into());
                    }
                    Ok(true)
                } else {
                    Ok(response.add_id(id.into()))
                }
            },
        )
        .await
        .caused_by(trc::location!())?;

    if response.response.total.is_some() {
        response.response.total = Some(matched);
    }
    if let Some(limit) = response.response.limit
        && matched < limit
    {
        response.response.limit = None;
    }

    Ok(response)
}

/// Enumerate the most recent metric ids up to `max_results`, oldest first.
async fn metric_ids(server: &Server, max_results: usize) -> trc::Result<Vec<Id>> {
    let mut ids = Vec::with_capacity(8);

    server
        .metrics_store()
        .iterate(
            IterateParams::new(metric_key(0), metric_key(u64::MAX))
                .ascending()
                .no_values(),
            |key, _| {
                ids.push(key.deserialize_be_u64(0)?.into());
                Ok(ids.len() < max_results)
            },
        )
        .await
        .caused_by(trc::location!())
        .map(|_| ids)
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

fn span_key(id: u64) -> ValueKey<ValueClass> {
    ValueKey::from(ValueClass::Telemetry(TelemetryClass::Span(id)))
}

fn metric_key(id: u64) -> ValueKey<ValueClass> {
    ValueKey::from(ValueClass::Telemetry(TelemetryClass::Metric(id)))
}
