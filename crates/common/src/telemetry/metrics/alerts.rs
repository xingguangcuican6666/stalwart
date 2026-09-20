/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Clean-room reimplementation of metric alerts: evaluate configured condition
 * expressions against the live metrics and, when a condition holds, emit an
 * e-mail and/or an internal event. Compiled unconditionally into the AGPL base.
 *
 * Note on the two distinct placeholder mechanisms this module deals with:
 *   - Condition expressions reference metrics via the `metric("name")` syntax,
 *     which the expression tokenizer bakes into a `System(Metric(..))` AST node
 *     at compile time. Evaluation therefore needs no metric-aware resolver — an
 *     empty resolver suffices, the value is read straight from the collector.
 *   - E-mail subject/body templates reference metrics via `%{name}%`, rendered
 *     at send time by the self-written tokenizer in `AlertContent` below.
 */

use std::fmt::Write;

use mail_builder::{
    MessageBuilder,
    headers::{
        HeaderType,
        address::{Address, EmailAddress},
    },
};
use registry::types::id::ObjectId;
use trc::{Collector, MetricType, TelemetryEvent};

use crate::{Server, expr::functions::EmptyResolver, expr::if_block::IfBlock};

/// A single configured alert: a compiled condition plus the actions to take
/// when it evaluates truthy.
#[derive(Clone, Debug)]
pub struct MetricAlert {
    pub id: ObjectId,
    pub condition: IfBlock,
    pub method: Vec<AlertMethod>,
}

#[derive(Clone, Debug)]
pub enum AlertMethod {
    Email {
        from_name: Option<String>,
        from_addr: String,
        to: Vec<String>,
        subject: AlertContent,
        body: AlertContent,
    },
    Event {
        message: Option<AlertContent>,
    },
}

/// A message template split into literal text and metric-value placeholders.
/// Placeholders use the `%{metric_name}%` syntax; unknown names are kept as
/// literal text.
#[derive(Clone, Debug, Default)]
pub struct AlertContent(pub Vec<AlertContentToken>);

#[derive(Clone, Debug)]
pub enum AlertContentToken {
    Text(String),
    Metric(MetricType),
}

/// A rendered alert ready to be handed to the outbound queue.
#[derive(Debug, PartialEq, Eq)]
pub struct AlertMessage {
    pub from: String,
    pub to: Vec<String>,
    pub body: Vec<u8>,
}

impl AlertContent {
    /// Parse a template string into text/metric tokens. Metric placeholders use
    /// the `%{metric_name}%` syntax; an unknown name or an unterminated
    /// placeholder is kept verbatim as literal text.
    pub fn parse(template: &str) -> Self {
        let mut tokens = Vec::new();
        let mut text = String::new();
        let mut chars = template.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '%' && chars.peek() == Some(&'{') {
                chars.next(); // consume '{'
                let mut name = String::new();
                let mut closed = false;
                for inner in chars.by_ref() {
                    if inner == '}' {
                        closed = true;
                        break;
                    }
                    name.push(inner);
                }
                // Swallow an optional trailing '%'.
                if closed && chars.peek() == Some(&'%') {
                    chars.next();
                }

                match (closed, MetricType::parse(&name)) {
                    (true, Some(metric)) => {
                        if !text.is_empty() {
                            tokens.push(AlertContentToken::Text(std::mem::take(&mut text)));
                        }
                        tokens.push(AlertContentToken::Metric(metric));
                    }
                    // Unknown metric or unterminated placeholder: keep literal.
                    _ => {
                        text.push_str("%{");
                        text.push_str(&name);
                        if closed {
                            text.push('}');
                        }
                    }
                }
            } else {
                text.push(ch);
            }
        }

        if !text.is_empty() {
            tokens.push(AlertContentToken::Text(text));
        }

        AlertContent(tokens)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Render the template against the current metric values.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for token in &self.0 {
            match token {
                AlertContentToken::Text(text) => out.push_str(text),
                AlertContentToken::Metric(metric) => {
                    let _ = write!(&mut out, "{}", Collector::read_metric(*metric));
                }
            }
        }
        out
    }
}

impl Server {
    /// Evaluate every configured alert against the current metrics and build the
    /// messages that should be sent. Returns `None` when nothing fired.
    pub async fn process_alerts(&self) -> Option<Vec<AlertMessage>> {
        let alerts = &self.core.metrics.alerts;
        if alerts.is_empty() {
            return None;
        }

        let mut messages = Vec::new();

        for alert in alerts {
            // Metric references are baked into the condition's AST as
            // `System(Metric(..))` nodes, so an empty resolver is sufficient.
            if self
                .eval_if::<bool, _>(&alert.condition, &EmptyResolver, 0)
                .await
                != Some(true)
            {
                continue;
            }

            for method in &alert.method {
                match method {
                    AlertMethod::Email {
                        from_name,
                        from_addr,
                        to,
                        subject,
                        body,
                    } => {
                        let subject = subject.render();
                        trc::event!(
                            Telemetry(TelemetryEvent::AlertMessage),
                            Id = alert.id.id().id(),
                            To = to
                                .iter()
                                .map(|t| trc::Value::from(t.to_string()))
                                .collect::<Vec<_>>(),
                            Details = subject.clone(),
                        );

                        let raw_message = MessageBuilder::new()
                            .from(Address::Address(EmailAddress {
                                name: from_name.as_ref().map(|s| s.into()),
                                email: from_addr.as_str().into(),
                            }))
                            .header(
                                "To",
                                HeaderType::Address(Address::List(
                                    to.iter()
                                        .map(|rcpt| {
                                            Address::Address(EmailAddress {
                                                name: None,
                                                email: rcpt.as_str().into(),
                                            })
                                        })
                                        .collect(),
                                )),
                            )
                            .header("Auto-Submitted", HeaderType::Text("auto-generated".into()))
                            .message_id(self.core.network.message_id())
                            .subject(subject)
                            .text_body(body.render())
                            .write_to_vec()
                            .unwrap_or_default();

                        messages.push(AlertMessage {
                            from: from_addr.clone(),
                            to: to.clone(),
                            body: raw_message,
                        });
                    }
                    AlertMethod::Event { message } => {
                        trc::event!(
                            Telemetry(TelemetryEvent::AlertEvent),
                            Id = alert.id.id().id(),
                            Details = message.as_ref().map(|m| m.render()),
                        );
                    }
                }
            }
        }

        (!messages.is_empty()).then_some(messages)
    }
}
