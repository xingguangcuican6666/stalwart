/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Clean-room reimplementation: LLM-based spam analysis. Sends the message to a
 * configured AI endpoint and turns its single-line verdict into spam tags.
 * Compiled unconditionally into the AGPL base.
 */

use std::{future::Future, time::Instant};

use common::Server;
use trc::AiEvent;

use crate::SpamFilterContext;

pub trait SpamFilterAnalyzeLlm: Sync + Send {
    fn spam_filter_analyze_llm(
        &self,
        ctx: &mut SpamFilterContext<'_>,
    ) -> impl Future<Output = ()> + Send;
}

impl SpamFilterAnalyzeLlm for Server {
    async fn spam_filter_analyze_llm(&self, ctx: &mut SpamFilterContext<'_>) {
        // No LLM classifier configured: nothing to do.
        let Some(config) = self.core.spam.llm.as_ref() else {
            return;
        };

        // Skip messages that carry no text body to classify.
        let Some(body) = ctx.text_body() else {
            return;
        };

        let started = Instant::now();
        let prompt = format!(
            "{}\n\nSubject: {}\n\n{}",
            config.prompt, ctx.output.subject, body
        );

        let response = match config.model.send_request(prompt, Some(config.temperature)).await {
            Ok(response) => response,
            Err(err) => {
                trc::error!(err.span_id(ctx.input.span_id));
                return;
            }
        };

        trc::event!(
            Ai(AiEvent::LlmResponse),
            Id = config.model.id.clone(),
            Details = response.clone(),
            Elapsed = started.elapsed(),
            SpanId = ctx.input.span_id,
        );

        // The model answers on a single line, fields joined by `separator`,
        // e.g. "SPAM,HIGH,looks like a phishing attempt". Pull out the category,
        // the (optional) confidence and the (optional) explanation by position.
        let mut category = None;
        let mut confidence = None;
        let mut explanation = None;

        for (idx, field) in response.split(config.separator).enumerate() {
            let field = field.trim();
            if field.is_empty() {
                continue;
            }

            if idx == config.index_category {
                let field = field.to_uppercase();
                if config.categories.contains(&field) {
                    category = Some(field);
                }
            } else if config.index_confidence == Some(idx) {
                let field = field.to_uppercase();
                if config.confidence.contains(&field) {
                    confidence = Some(field);
                }
            } else if config.index_explanation == Some(idx) {
                // Collapse internal whitespace and cap the length so a chatty
                // model can't blow up the header.
                let buf = explanation
                    .get_or_insert_with(|| String::with_capacity(field.len().min(255)));
                for ch in field.chars() {
                    buf.push(if ch.is_whitespace() { ' ' } else { ch });
                    if buf.len() >= 255 {
                        break;
                    }
                }
            }
        }

        // A recognised category is required to tag; confidence is optional.
        let category = match (category, confidence) {
            (Some(category), Some(confidence)) => {
                ctx.result.add_tag(format!("LLM_{category}_{confidence}"));
                category
            }
            (Some(category), None) => {
                ctx.result.add_tag(format!("LLM_{category}"));
                category
            }
            _ => return,
        };

        if let Some(explanation) = explanation {
            ctx.result.llm_result = Some((category, explanation));
        }
    }
}
