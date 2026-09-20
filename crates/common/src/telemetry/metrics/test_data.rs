/*
 * SPDX-FileCopyrightText: 2026 Stalwart clean-room contributors
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Clean-room reimplementation of the metrics test-data seeder. Only compiled
 * under `dev_mode`/`test_mode`. Populates the metrics store with synthetic
 * historical samples so the metrics UI can be exercised without a live server.
 * Written from the AGPL `Collector`/`MetricsStore` API; no SEL source consulted.
 */

use crate::{
    Server,
    telemetry::metrics::store::{MetricsStore, SharedMetricHistory},
};
use std::time::Duration;
use store::rand::{self, RngExt};
use trc::*;

/// Event counters seeded each cycle. Each is bumped by a random amount so the
/// resulting counter series shows plausible per-hour activity.
const SEEDED_COUNTERS: &[EventType] = &[
    EventType::Smtp(SmtpEvent::ConnectionStart),
    EventType::Imap(ImapEvent::ConnectionStart),
    EventType::Pop3(Pop3Event::ConnectionStart),
    EventType::ManageSieve(ManageSieveEvent::ConnectionStart),
    EventType::Http(HttpEvent::ConnectionStart),
    EventType::Delivery(DeliveryEvent::AttemptStart),
    EventType::Queue(QueueEvent::MessageQueued),
    EventType::Queue(QueueEvent::AuthenticatedMessageQueued),
    EventType::Queue(QueueEvent::DsnQueued),
    EventType::Queue(QueueEvent::ReportQueued),
    EventType::MessageIngest(MessageIngestEvent::Ham),
    EventType::MessageIngest(MessageIngestEvent::Spam),
    EventType::Auth(AuthEvent::Failed),
    EventType::Security(SecurityEvent::AuthenticationBan),
    EventType::Security(SecurityEvent::ScanBan),
    EventType::Security(SecurityEvent::AbuseBan),
    EventType::Security(SecurityEvent::LoiterBan),
    EventType::Security(SecurityEvent::IpBlocked),
    EventType::IncomingReport(IncomingReportEvent::DmarcReport),
    EventType::IncomingReport(IncomingReportEvent::DmarcReportWithWarnings),
    EventType::IncomingReport(IncomingReportEvent::TlsReport),
    EventType::IncomingReport(IncomingReportEvent::TlsReportWithWarnings),
];

/// Latency histograms seeded each cycle, in milliseconds.
const SEEDED_HISTOGRAMS: &[MetricType] = &[
    MetricType::MessageIngestTime,
    MetricType::MessageIngestIndexTime,
    MetricType::DeliveryTotalTime,
    MetricType::DeliveryAttemptTime,
    MetricType::DnsLookupTime,
    MetricType::StoreDataReadTime,
    MetricType::StoreDataWriteTime,
    MetricType::StoreBlobReadTime,
    MetricType::StoreBlobWriteTime,
];

impl Server {
    /// Seed the metrics store with 90 days of hourly synthetic samples.
    pub async fn insert_test_metrics(&self) {
        const SECONDS_PER_HOUR: u64 = 60 * 60;
        const DAYS_OF_HISTORY: u64 = 90;

        let mut rng = rand::rng();

        // Start from a clean slate.
        self.metrics_store()
            .purge_metrics(Duration::from_secs(0))
            .await
            .unwrap();

        let now = store::write::now();
        let mut sample_time = now - DAYS_OF_HISTORY * 24 * SECONDS_PER_HOUR;

        // Counters and histograms are cumulative, so a single shared history is
        // threaded through every write to produce per-cycle deltas.
        let history = SharedMetricHistory::default();

        while sample_time <= now {
            for &event_type in SEEDED_COUNTERS {
                Collector::update_event_counter(event_type, rng.random_range(0..=100));
            }

            Collector::update_gauge(MetricType::QueueCount, rng.random_range(0..=1000));
            Collector::update_gauge(
                MetricType::ServerMemory,
                rng.random_range(100 * 1024 * 1024..=300 * 1024 * 1024),
            );
            Collector::update_gauge(MetricType::UserCount, rng.random_range(100..=500));
            Collector::update_gauge(MetricType::DomainCount, rng.random_range(10..=50));

            for &metric_type in SEEDED_HISTOGRAMS {
                Collector::update_histogram(metric_type, rng.random_range(2..=1000));
            }
            // Add an occasional slow delivery to widen the distribution.
            Collector::update_histogram(
                MetricType::DeliveryTotalTime,
                rng.random_range(1000..=5000),
            );

            self.metrics_store()
                .write_metrics(sample_time.into(), history.clone())
                .await
                .unwrap();

            sample_time += SECONDS_PER_HOUR;
        }
    }
}
