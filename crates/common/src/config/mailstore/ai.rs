/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 *
 * Clean-room reimplementation of the AI/LLM API integration. This module talks
 * to any OpenAI-compatible HTTP endpoint (chat- or text-completion) and is
 * compiled unconditionally into the AGPL base. The request/response shapes
 * below follow the public OpenAI API specification, not any proprietary source.
 */

use std::sync::Arc;

use ahash::AHashMap;
use registry::schema::{enums::AiModelType, structs::AiModel};
use store::registry::bootstrap::Bootstrap;
use trc::{AiEvent, EventType};
use utils::Client;

/// One configured AI endpoint, ready to issue requests.
#[derive(Clone, Debug)]
pub struct AiApiConfig {
    pub id: String,
    pub api_type: ApiType,
    pub url: String,
    pub model: String,
    pub default_temperature: f64,
    /// Pre-built HTTP client carrying the authentication and custom headers.
    pub client: Client,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiType {
    /// `POST /v1/chat/completions` — messages array, choices[].message.content.
    ChatCompletion,
    /// `POST /v1/completions` — single prompt, choices[].text.
    TextCompletion,
}

/// All configured AI endpoints, keyed both by their string id (used by the
/// Sieve `llm_prompt` plugin) and reachable by numeric object id during config
/// parsing.
#[derive(Clone, Default)]
pub struct AiConfig {
    pub apis: AHashMap<String, Arc<AiApiConfig>>,
}

// ----- Wire formats (OpenAI-compatible) -----

#[derive(serde::Serialize)]
struct ChatRequest<'x> {
    model: &'x str,
    messages: [ChatMessage<'x>; 1],
    temperature: f64,
}

#[derive(serde::Serialize)]
struct ChatMessage<'x> {
    role: &'x str,
    content: &'x str,
}

#[derive(serde::Deserialize)]
struct ChatResponse {
    #[serde(default)]
    choices: Vec<ChatChoice>,
}

#[derive(serde::Deserialize)]
struct ChatChoice {
    #[serde(default)]
    message: ChatChoiceMessage,
}

#[derive(serde::Deserialize, Default)]
struct ChatChoiceMessage {
    #[serde(default)]
    content: String,
}

#[derive(serde::Serialize)]
struct TextRequest<'x> {
    model: &'x str,
    prompt: &'x str,
    temperature: f64,
}

#[derive(serde::Deserialize)]
struct TextResponse {
    #[serde(default)]
    choices: Vec<TextChoice>,
}

#[derive(serde::Deserialize, Default)]
struct TextChoice {
    #[serde(default)]
    text: String,
}

impl AiApiConfig {
    /// Issue a completion request and return the assistant's text.
    pub async fn send_request(
        &self,
        prompt: impl Into<String>,
        temperature: Option<f64>,
    ) -> trc::Result<String> {
        self.post(prompt.into(), temperature.unwrap_or(self.default_temperature))
            .await
            .map_err(|reason| {
                trc::Error::new(EventType::Ai(AiEvent::ApiError))
                    .id(self.id.clone())
                    .details("AI API request failed")
                    .reason(reason)
            })
    }

    async fn post(&self, prompt: String, temperature: f64) -> Result<String, String> {
        // reqwest is built without its `json` feature, so serialize the body
        // ourselves. The `application/json` content type is already set as a
        // default header on the client (see `build_http_client`).
        let body = match self.api_type {
            ApiType::ChatCompletion => serde_json::to_vec(&ChatRequest {
                model: &self.model,
                messages: [ChatMessage {
                    role: "user",
                    content: &prompt,
                }],
                temperature,
            }),
            ApiType::TextCompletion => serde_json::to_vec(&TextRequest {
                model: &self.model,
                prompt: &prompt,
                temperature,
            }),
        }
        .map_err(|err| format!("Failed to serialize request: {err}"))?;

        let response = self
            .client
            .post(&self.url)
            .body(body)
            .send()
            .await
            .map_err(|err| format!("HTTP request failed: {err}"))?;

        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|err| format!("Failed to read response body: {err}"))?;

        if !status.is_success() {
            return Err(format!(
                "AI endpoint returned HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            ));
        }

        let text = match self.api_type {
            ApiType::ChatCompletion => serde_json::from_slice::<ChatResponse>(&bytes)
                .map_err(|err| format!("Failed to parse chat response: {err}"))?
                .choices
                .into_iter()
                .next()
                .map(|choice| choice.message.content),
            ApiType::TextCompletion => serde_json::from_slice::<TextResponse>(&bytes)
                .map_err(|err| format!("Failed to parse text response: {err}"))?
                .choices
                .into_iter()
                .next()
                .map(|choice| choice.text),
        };

        match text {
            Some(text) if !text.trim().is_empty() => Ok(text),
            _ => Err("AI endpoint returned an empty completion".to_string()),
        }
    }
}

impl AiConfig {
    /// Parse every configured `AiModel` into a ready-to-use endpoint. Returns
    /// the string-keyed map plus a numeric-id lookup used to resolve references
    /// from other settings (e.g. the spam-filter LLM model).
    pub async fn parse(bp: &mut Bootstrap) -> (Self, AHashMap<u64, Arc<AiApiConfig>>) {
        let mut apis = AHashMap::new();
        let mut by_id = AHashMap::new();

        for model in bp.list_infallible::<AiModel>().await {
            let object_id = model.id;
            let model = model.object;

            let client = match model
                .http_auth
                .build_http_client(
                    model.http_headers,
                    Some("application/json"),
                    model.timeout,
                    model.allow_invalid_certs,
                )
                .await
            {
                Ok(client) => client,
                Err(err) => {
                    bp.build_error(object_id, format!("Unable to build AI HTTP client: {err}"));
                    continue;
                }
            };

            let api = Arc::new(AiApiConfig {
                id: model.name,
                api_type: match model.model_type {
                    AiModelType::Chat => ApiType::ChatCompletion,
                    AiModelType::Text => ApiType::TextCompletion,
                },
                url: model.url,
                model: model.model,
                default_temperature: model.temperature.into_inner(),
                client,
            });

            apis.insert(api.id.clone(), api.clone());
            by_id.insert(object_id.id().id(), api);
        }

        (AiConfig { apis }, by_id)
    }
}
