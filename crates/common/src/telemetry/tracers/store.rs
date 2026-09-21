/*
 * SPDX-FileCopyrightText: 2026 Stalwart clean-room contributors
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Clean-room reimplementation of the tracing (span) persistence layer. Written
 * from the AGPL-side call-site contract (task_manager index/maintenance,
 * telemetry dispatch) and the AGPL-generated `registry` schema types plus the
 * AGPL `TraceEvents`/`map_value` helpers in the parent module. No SEL source
 * was consulted.
 */

use crate::{config::telemetry::StoreTracer, telemetry::tracers::TraceEvents};
use ahash::AHashMap;
use registry::{
    schema::structs::{Task, TaskIndexTrace, TaskStatus, Trace, TraceValue},
    types::ObjectImpl,
};
use std::{future::Future, sync::Arc, time::Duration};
use nlp::language::Language;
use store::{
    SearchStore, Store, ValueKey,
    search::{IndexDocument, SearchField, SearchFilter, SearchQuery, TracingSearchField},
    write::{BatchBuilder, SearchIndex, TelemetryClass, ValueClass},
};
use trc::{
    Event, EventDetails, EventType, Key, TelemetryEvent,
    ipc::subscriber::SubscriberBuilder,
};
use utils::snowflake::SnowflakeIdGenerator;

impl StoreTracer {
    /// Event types the store tracer subscribes to by default: the span
    /// boundaries for every connection-oriented protocol plus delivery
    /// attempts. Individual events carried inside a span are persisted
    /// regardless; these are the events that open and close a span so the
    /// tracer knows when to flush.
    pub fn default_events() -> impl IntoIterator<Item = EventType> {
        EventType::variants()
            .iter()
            .copied()
            .filter(|event| event.is_span_start() || event.is_span_end())
            .collect::<Vec<_>>()
    }
}

/// Persist completed spans to the tracing store.
///
/// Events arrive batched from the collector. Each event carries an optional
/// reference to the span it belongs to (`inner.span`); events sharing the same
/// span id are accumulated until the span-end event is observed, at which point
/// the whole span is serialized as a `Trace` and written under
/// `TelemetryClass::Span(span_id)`. A reindex task is scheduled so the span
/// becomes searchable.
pub(crate) fn spawn_store_tracer(builder: SubscriberBuilder, settings: StoreTracer) {
    let (_, mut rx) = builder.register();
    tokio::spawn(async move {
        // Spans are written to `store`. Reindex tasks are written to the data
        // store (`data`) when it differs from the tracing store; otherwise they
        // go to the tracing store itself.
        let span_store = settings.store;
        let task_store = settings.data.unwrap_or_else(|| span_store.clone());

        // Accumulated events per open span.
        let mut spans: AHashMap<u64, Vec<Arc<Event<EventDetails>>>> = AHashMap::new();

        while let Some(events) = rx.recv().await {
            let mut span_batch = BatchBuilder::new();
            let mut task_batch = BatchBuilder::new();
            let mut wrote_any = false;

            for event in events {
                // Resolve the span this event belongs to. Events reference
                // their span-start event via `inner.span`; a span-boundary
                // event carries the span id on itself.
                let span_id = event
                    .inner
                    .span
                    .as_ref()
                    .and_then(|span| span.span_id())
                    .or_else(|| event.span_id());

                let Some(span_id) = span_id else {
                    continue;
                };

                let is_end = event.inner.typ.is_span_end();
                spans.entry(span_id).or_default().push(event);

                if is_end
                    && let Some(span_events) = spans.remove(&span_id)
                {
                    let num_events = span_events.len();
                    let trace =
                        Trace::from_events(span_events.iter().map(|e| e.as_ref()), num_events);

                    span_batch.set(
                        ValueClass::Telemetry(TelemetryClass::Span(span_id)),
                        trace.to_pickled_vec(),
                    );
                    task_batch.schedule_task(Task::IndexTrace(TaskIndexTrace {
                        trace_id: span_id.into(),
                        status: TaskStatus::now(),
                    }));
                    wrote_any = true;
                }
            }

            if !wrote_any {
                continue;
            }

            if let Err(err) = span_store.write(span_batch.build_all()).await {
                trc::event!(
                    Telemetry(TelemetryEvent::LogError),
                    Reason = err.to_string(),
                    Details = "Failed to write tracing span"
                );
                continue;
            }

            // Schedule reindex tasks so the newly written spans become
            // searchable. Tasks are written to the data store (or the tracing
            // store when they coincide).
            if let Err(err) = task_store.write(task_batch.build_all()).await {
                trc::event!(
                    Telemetry(TelemetryEvent::LogError),
                    Reason = err.to_string(),
                    Details = "Failed to schedule tracing span reindex"
                );
            }
        }
    });
}

pub trait TracingStore: Sync + Send {
    fn purge_spans(
        &self,
        period: Duration,
        search_store: Option<&SearchStore>,
    ) -> impl Future<Output = trc::Result<()>> + Send;
}

impl TracingStore for Store {
    async fn purge_spans(
        &self,
        period: Duration,
        search_store: Option<&SearchStore>,
    ) -> trc::Result<()> {
        let Some(threshold) = SnowflakeIdGenerator::from_duration(period) else {
            return Ok(());
        };

        // Remove the raw span records.
        self.delete_range(
            ValueKey::from(ValueClass::Telemetry(TelemetryClass::Span(0))),
            ValueKey::from(ValueClass::Telemetry(TelemetryClass::Span(threshold))),
        )
        .await?;

        // Remove the corresponding search index entries when a searchable
        // tracing index is configured.
        if let Some(search_store) = search_store
            && !matches!(search_store, SearchStore::Store(Store::None))
        {
            search_store
                .unindex(
                    SearchQuery::new(SearchIndex::Tracing)
                        .with_filter(SearchFilter::lt(SearchField::Id, threshold)),
                )
                .await?;
        }

        Ok(())
    }
}

/// Build a searchable document for a stored span. Indexes each event type as a
/// keyword, the queue id (when present) as an unsigned value, and any textual
/// values as free-text keywords.
pub fn build_span_document(span_id: u64, trace: Trace) -> IndexDocument {
    let mut document = IndexDocument::new(SearchIndex::Tracing).with_id(span_id);

    for event in trace.events.iter() {
        document.index_text(
            TracingSearchField::EventType,
            event.event.as_str(),
            Language::None,
        );

        for kv in event.key_values.iter() {
            match &kv.value {
                TraceValue::UnsignedInt(v) if kv.key == Key::QueueId => {
                    document.index_unsigned(TracingSearchField::QueueId, v.value);
                }
                TraceValue::String(v) => {
                    document.index_text(
                        TracingSearchField::Keywords,
                        &v.value,
                        Language::None,
                    );
                }
                _ => {}
            }
        }
    }

    document
}
