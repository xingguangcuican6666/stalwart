/*
 * SPDX-FileCopyrightText: 2026 Stalwart clean-room contributors
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Clean-room reimplementation of the metrics persistence layer. Written from
 * the AGPL-side call-site contract (task_manager scheduler/maintenance) and
 * the AGPL-generated `registry` schema types. No SEL source was consulted.
 */

use ahash::AHashMap;
use parking_lot::Mutex;
use registry::{
    schema::structs::{Metric, MetricCount, MetricSum},
    types::ObjectImpl,
};
use std::{future::Future, sync::Arc, time::Duration};
use store::{
    Store, ValueKey,
    write::{BatchBuilder, TelemetryClass, ValueClass},
};
use trc::{Collector, MetricType};
use utils::snowflake::SnowflakeIdGenerator;

/// Per-histogram running totals kept between collection cycles so that we can
/// persist the increment observed since the previous write.
#[derive(Debug, Default, Clone, Copy)]
pub struct HistogramHistory {
    pub sum: u64,
    pub count: u64,
}

/// Running state carried across metric collection cycles.
///
/// Counters and histograms are cumulative in the in-process collector, so we
/// remember the previously persisted value and only store the delta on each
/// write. Gauges are point-in-time and stored as-is.
pub struct MetricsHistory {
    events: AHashMap<MetricType, u32>,
    histograms: AHashMap<MetricType, HistogramHistory>,
    id_generator: SnowflakeIdGenerator,
}

impl Default for MetricsHistory {
    fn default() -> Self {
        Self {
            events: AHashMap::new(),
            histograms: AHashMap::new(),
            id_generator: SnowflakeIdGenerator::new(),
        }
    }
}

pub type SharedMetricHistory = Arc<Mutex<MetricsHistory>>;

pub fn init() -> SharedMetricHistory {
    Arc::new(Mutex::new(MetricsHistory::default()))
}

pub trait MetricsStore: Sync + Send {
    fn write_metrics(
        &self,
        timestamp: Option<u64>,
        history: SharedMetricHistory,
    ) -> impl Future<Output = trc::Result<()>> + Send;

    fn purge_metrics(&self, period: Duration) -> impl Future<Output = trc::Result<()>> + Send;
}

impl MetricsStore for Store {
    async fn write_metrics(
        &self,
        timestamp: Option<u64>,
        history: SharedMetricHistory,
    ) -> trc::Result<()> {
        // Snapshot the current collector state, computing deltas against the
        // previous cycle for cumulative series (counters, histograms).
        let metrics = {
            let mut history = history.lock();
            let mut metrics = Vec::new();

            // Counters: the collector reports the running total per event type.
            // Only event types that have a corresponding metric type are stored.
            for counter in Collector::collect_counters(true) {
                let Some(metric) = MetricType::parse(counter.id().as_str()) else {
                    continue;
                };
                let total = counter.value() as u32;
                let previous = history.events.get(&metric).copied().unwrap_or(0);
                let delta = total.saturating_sub(previous);
                if delta > 0 {
                    metrics.push(Metric::Counter(MetricCount {
                        count: delta as u64,
                        metric,
                    }));
                }
                history.events.insert(metric, total);
            }

            // Gauges: point-in-time values, persisted verbatim.
            for gauge in Collector::collect_gauges(true) {
                metrics.push(Metric::Gauge(MetricCount {
                    count: gauge.get(),
                    metric: gauge.id(),
                }));
            }

            // Histograms: the collector reports cumulative sum/count, store the
            // increment observed since the previous cycle.
            for histogram in Collector::collect_histograms(true) {
                let metric = histogram.id();
                let sum = histogram.sum();
                let count = histogram.count();
                let previous = history.histograms.get(&metric).copied().unwrap_or_default();
                let delta_count = count.saturating_sub(previous.count);
                if delta_count > 0 {
                    metrics.push(Metric::Histogram(MetricSum {
                        count: delta_count,
                        sum: sum.saturating_sub(previous.sum),
                        metric,
                    }));
                }
                history
                    .histograms
                    .insert(metric, HistogramHistory { sum, count });
            }

            metrics
        };

        if metrics.is_empty() {
            return Ok(());
        }

        // Assign an id to each metric. When a fixed timestamp is supplied
        // (historical backfill), derive ids from it; otherwise use the live
        // snowflake generator.
        let base_id = timestamp
            .and_then(SnowflakeIdGenerator::from_timestamp)
            .unwrap_or_else(|| history.lock().id_generator.generate());

        let mut batch = BatchBuilder::new();
        for (offset, metric) in metrics.into_iter().enumerate() {
            batch.set(
                ValueClass::Telemetry(TelemetryClass::Metric(base_id + offset as u64)),
                metric.to_pickled_vec(),
            );
        }

        self.write(batch.build_all()).await.map(|_| ())
    }

    async fn purge_metrics(&self, period: Duration) -> trc::Result<()> {
        if let Some(threshold) = SnowflakeIdGenerator::from_duration(period) {
            self.delete_range(
                ValueKey::from(ValueClass::Telemetry(TelemetryClass::Metric(0))),
                ValueKey::from(ValueClass::Telemetry(TelemetryClass::Metric(threshold))),
            )
            .await
        } else {
            Ok(())
        }
    }
}
