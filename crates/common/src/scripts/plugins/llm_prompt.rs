/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Clean-room reimplementation of the Sieve `llm_prompt` function: hand a prompt
 * to a configured AI endpoint and return its response. Compiled unconditionally
 * into the AGPL base. Access is gated by the `InteractAi` permission.
 */

use sieve::{FunctionMap, compiler::Number, runtime::Variable};
use std::time::Instant;
use trc::{AiEvent, SecurityEvent};

use super::PluginContext;

pub fn register(plugin_id: u32, fnc_map: &mut FunctionMap) {
    fnc_map.set_external_function("llm_prompt", plugin_id, 3);
}

pub async fn exec(ctx: PluginContext<'_>) -> trc::Result<Variable> {
    let (Variable::String(name), Variable::String(prompt)) =
        (&ctx.arguments[0], &ctx.arguments[1])
    else {
        return Ok(false.into());
    };

    #[cfg(feature = "test_mode")]
    if name.as_ref() == "echo-test" {
        return Ok(prompt.to_string().into());
    }

    // Enforce the InteractAi permission before reaching the model.
    if let Some(token) = ctx.access_token {
        use registry::schema::enums::Permission;
        use registry::types::EnumImpl;

        if !token.has_permission(Permission::InteractAi) {
            trc::event!(
                Security(SecurityEvent::Unauthorized),
                AccountId = token.account_id(),
                Details = Permission::InteractAi.as_str(),
                SpanId = ctx.session_id,
            );
            return Ok(false.into());
        }
    }

    // Resolve the endpoint: an empty name picks the sole configured model,
    // otherwise look it up by name.
    let apis = &ctx.server.core.ai.apis;
    let ai_api = if name.is_empty() && apis.len() == 1 {
        apis.values().next()
    } else {
        apis.get(name.as_ref())
    };
    let Some(ai_api) = ai_api else {
        return Ok(false.into());
    };

    let temperature = ctx.arguments[2].to_number_checked().map(|n| match n {
        Number::Integer(n) => (n as f64).clamp(0.0, 1.0),
        Number::Float(n) => n.clamp(0.0, 1.0),
    });

    let started = Instant::now();
    match ai_api.send_request(prompt.as_ref(), temperature).await {
        Ok(response) => {
            trc::event!(
                Ai(AiEvent::LlmResponse),
                Id = ai_api.id.clone(),
                Value = prompt.to_string(),
                Details = response.clone(),
                Elapsed = started.elapsed(),
                SpanId = ctx.session_id,
            );

            Ok(response.into())
        }
        Err(err) => {
            trc::error!(err.span_id(ctx.session_id));
            Ok(false.into())
        }
    }
}
