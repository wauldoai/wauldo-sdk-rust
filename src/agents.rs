//! Agents API client — Wauldo Deploy deployed-agent registry.
//!
//! Standalone client that talks to the `/v1/agents` endpoints. Designed
//! to work alongside `HttpClient` without depending on its types —
//! construct `AgentsClient` with the same base URL + API key you'd pass
//! to `HttpClient`.
//!
//! # Example
//! ```no_run
//! use wauldo::agents::{AgentsClient, CreateAgentRequest};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let agents = AgentsClient::new("http://localhost:3000")
//!     .with_api_key("sk-...")
//!     .with_tenant("my-org");
//! let agent = agents.create(CreateAgentRequest {
//!     name: "sdr-bot".into(),
//!     wauldo_toml: "[agent]\nname = 'sdr-bot'\n[model]\nprovider = 'openrouter'\nname = 'qwen'".into(),
//!     description: "Outbound sales".into(),
//!     agents_md: None,
//!     mcp_json: None,
//!     preset: None,
//! }).await?;
//! let run = agents.run(&agent.id, "Qualify acme.com", None).await?;
//! println!("task: {}", run.task_id);
//! # Ok(())
//! # }
//! ```

use std::time::Duration;

use futures_util::StreamExt;
use reqwest::{header::HeaderMap, Client, Method, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Max bytes the client will accept from a single response. Protects
/// against hostile or misbehaving servers that try to stream gigabytes.
pub const MAX_RESPONSE_SIZE: usize = 10 * 1024 * 1024; // 10 MB

/// Errors returned by `AgentsClient` / `MemoryClient`.
#[derive(Debug, Error)]
pub enum AgentsError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("HTTP {status}: {body}")]
    Status { status: u16, body: String },

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("response body too large: >{0} bytes")]
    BodyTooLarge(usize),

    #[error("invalid input: {0}")]
    InvalidInput(String),
}

pub type AgentsResult<T> = std::result::Result<T, AgentsError>;

// ─── Wire types ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployedAgent {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub wauldo_toml: String,
    #[serde(default)]
    pub agents_md: Option<String>,
    #[serde(default)]
    pub mcp_json: Option<String>,
    pub model_provider: String,
    pub model_name: String,
    #[serde(default)]
    pub preset: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct CreateAgentRequest {
    pub name: String,
    pub wauldo_toml: String,
    #[serde(default)]
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agents_md: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp_json: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preset: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct UpdateAgentRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wauldo_toml: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agents_md: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp_json: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preset: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentListResponse {
    pub agents: Vec<DeployedAgent>,
    pub pagination: AgentPagination,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentPagination {
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentRunResponse {
    pub task_id: String,
    pub agent_id: String,
    pub status: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct A2aResponse {
    pub task_id: String,
    pub agent_id: String,
    pub trace: Vec<String>,
    pub depth: usize,
    pub status: String,
}

// ─── Tasks + verification types ──────────────────────────────────────

/// Verification verdict returned on completed tasks. Matches the server's
/// `wauldo-api::routes::tasks::types::TaskVerification.verdict` enum.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Verdict {
    Safe,
    Uncertain,
    Partial,
    Block,
    Conflict,
    Unverified,
    /// Fallback for server-side verdicts added after this SDK version.
    #[serde(other)]
    Unknown,
}

/// Task lifecycle status.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    #[serde(other)]
    Unknown,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskClaim {
    pub text: String,
    pub supported: bool,
    pub confidence: f64,
}

/// Verification block attached to completed tasks.
///
/// When `verification_source == "prompt_only"`, `confidence` and
/// `hallucination_rate` reflect self-consistency only — rely on
/// `verdict` + [`support_score`](Self::support_score) + `message` as authoritative.
///
/// **Naming note:** the JSON wire field is `trust_score` (kept stable
/// for backward compatibility). The public Rust method
/// [`support_score`](Self::support_score) returns the same value under
/// the public marketing name. New code should prefer that method.
#[derive(Debug, Clone, Deserialize)]
pub struct TaskVerification {
    pub verdict: Verdict,
    pub hallucination_rate: f64,
    pub confidence: f64,
    pub trust_score: f64,
    pub verification_source: String,
    #[serde(default)]
    pub claims: Vec<TaskClaim>,
    #[serde(default)]
    pub verification_retries: u32,
    /// Human-readable context for non-SAFE verdicts.
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub sources_cited: Vec<usize>,
    #[serde(default)]
    pub stripped_claims: Vec<String>,
}

impl TaskVerification {
    /// Public name for [`trust_score`](Self::trust_score).
    ///
    /// Returns the fraction of claims (0-1) that are supported by the
    /// provided sources. Identical to the `trust_score` field — the
    /// JSON wire format keeps `trust_score` for backward compatibility,
    /// but the public marketing name is *support score*. Prefer this
    /// method in new code.
    #[inline]
    pub fn support_score(&self) -> f64 {
        self.trust_score
    }
}

/// Full task record returned by `GET /v1/tasks/:id`.
#[derive(Debug, Clone, Deserialize)]
pub struct Task {
    pub task_id: String,
    #[serde(default)]
    pub tenant_id: String,
    pub status: TaskStatus,
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub created_at: u64,
    #[serde(default)]
    pub updated_at: u64,
    #[serde(default)]
    pub preset: Option<String>,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub partial_result: Option<String>,
    #[serde(default)]
    pub verification: Option<TaskVerification>,
    #[serde(default)]
    pub journal: Option<serde_json::Value>,
}

impl Task {
    pub fn is_done(&self) -> bool {
        self.status.is_terminal()
    }
}

/// Single event yielded by `GET /v1/tasks/:id/stream`. Each SSE `data:`
/// line is a JSON-serialised StateTransition emitted when a workflow
/// state completes.
#[derive(Debug, Clone, Deserialize)]
pub struct StateTransition {
    pub state_name: String,
    #[serde(default)]
    pub to_state: Option<String>,
    pub condition: String,
    pub raw_output: String,
    #[serde(default)]
    pub validation_notes: Vec<String>,
    pub timestamp: u64,
    pub success: bool,
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub prompt_tokens: usize,
    #[serde(default)]
    pub completion_tokens: usize,
    #[serde(default)]
    pub repair_count: u32,
    #[serde(default)]
    pub cache_hit: bool,
}

// ─── Shared helper: bounded body reader ───────────────────────────────

/// Stream a `reqwest::Response` body in chunks, aborting if it exceeds
/// `limit`. Used by both `AgentsClient` and `MemoryClient`.
pub(crate) async fn bounded_read(
    mut response: reqwest::Response,
    limit: usize,
) -> AgentsResult<Vec<u8>> {
    let mut buf = Vec::new();
    let mut stream = std::pin::pin!(async_stream::stream! {
        while let Some(chunk) = response.chunk().await.transpose() {
            yield chunk;
        }
    });
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(AgentsError::Http)?;
        if buf.len() + chunk.len() > limit {
            return Err(AgentsError::BodyTooLarge(limit));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

// ─── Client ───────────────────────────────────────────────────────────

pub struct AgentsClient {
    base_url: String,
    api_key: Option<String>,
    tenant: Option<String>,
    client: Client,
}

impl AgentsClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: None,
            tenant: None,
            client: Client::builder()
                .timeout(Duration::from_secs(120))
                .build()
                .expect("reqwest client"),
        }
    }

    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    fn headers(&self, extra: Option<HeaderMap>) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("Content-Type", "application/json".parse().unwrap());
        if let Some(key) = &self.api_key {
            if let Ok(val) = format!("Bearer {key}").parse() {
                h.insert("Authorization", val);
            }
        }
        if let Some(t) = &self.tenant {
            if let Ok(val) = t.parse() {
                h.insert("x-rapidapi-user", val);
            }
        }
        if let Some(extra_map) = extra {
            for (k, v) in extra_map.iter() {
                h.insert(k.clone(), v.clone());
            }
        }
        h
    }

    async fn request<T: for<'de> Deserialize<'de>>(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
        extra_headers: Option<HeaderMap>,
    ) -> AgentsResult<Option<T>> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self
            .client
            .request(method, &url)
            .headers(self.headers(extra_headers));
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if status == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        let bytes = bounded_read(resp, MAX_RESPONSE_SIZE).await?;
        if !status.is_success() {
            let body = String::from_utf8_lossy(&bytes).into_owned();
            return Err(AgentsError::Status {
                status: status.as_u16(),
                body,
            });
        }
        if bytes.is_empty() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    // ── CRUD ─────────────────────────────────────────────────────

    pub async fn create(&self, req: CreateAgentRequest) -> AgentsResult<DeployedAgent> {
        self.request::<DeployedAgent>(Method::POST, "/v1/agents", Some(&req), None)
            .await
            .map(|o| o.expect("server returned empty body for create"))
    }

    pub async fn list(&self, limit: usize, offset: usize) -> AgentsResult<AgentListResponse> {
        let path = format!("/v1/agents?limit={limit}&offset={offset}");
        self.request::<AgentListResponse>(Method::GET, &path, Option::<&()>::None, None)
            .await
            .map(|o| o.expect("server returned empty body for list"))
    }

    pub async fn get(&self, agent_id: &str) -> AgentsResult<DeployedAgent> {
        self.request::<DeployedAgent>(
            Method::GET,
            &format!("/v1/agents/{agent_id}"),
            Option::<&()>::None,
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for get"))
    }

    pub async fn update(
        &self,
        agent_id: &str,
        patch: UpdateAgentRequest,
    ) -> AgentsResult<DeployedAgent> {
        self.request::<DeployedAgent>(
            Method::PATCH,
            &format!("/v1/agents/{agent_id}"),
            Some(&patch),
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for update"))
    }

    pub async fn delete(&self, agent_id: &str) -> AgentsResult<()> {
        let _: Option<serde_json::Value> = self
            .request(
                Method::DELETE,
                &format!("/v1/agents/{agent_id}"),
                Option::<&()>::None,
                None,
            )
            .await?;
        Ok(())
    }

    // ── Runs ─────────────────────────────────────────────────────

    pub async fn run(
        &self,
        agent_id: &str,
        input: &str,
        verification_mode: Option<&str>,
    ) -> AgentsResult<AgentRunResponse> {
        self.run_with_fact_check(agent_id, input, verification_mode, None)
            .await
    }

    /// `POST /v1/agents/:id/runs` with explicit fact-checker mode.
    ///
    /// `fact_check_mode` accepts `"lexical"` (default, fastest), `"hybrid"`
    /// (lexical + embeddings, ~3-5s), or `"semantic"` (embeddings + LLM-judge,
    /// ~5-15s). Hybrid/semantic silently fall back to lexical when the
    /// server's BGE cache is unavailable.
    pub async fn run_with_fact_check(
        &self,
        agent_id: &str,
        input: &str,
        verification_mode: Option<&str>,
        fact_check_mode: Option<&str>,
    ) -> AgentsResult<AgentRunResponse> {
        if input.is_empty() {
            return Err(AgentsError::InvalidInput("input is required".into()));
        }
        let mut body = serde_json::json!({ "input": input });
        if let Some(v) = verification_mode {
            body["verification_mode"] = serde_json::Value::String(v.to_string());
        }
        if let Some(f) = fact_check_mode {
            body["fact_check_mode"] = serde_json::Value::String(f.to_string());
        }
        self.request::<AgentRunResponse>(
            Method::POST,
            &format!("/v1/agents/{agent_id}/runs"),
            Some(&body),
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for run"))
    }

    pub async fn a2a_invoke(
        &self,
        agent_id: &str,
        input: &str,
        trace: &[&str],
    ) -> AgentsResult<A2aResponse> {
        self.a2a_invoke_with_fact_check(agent_id, input, trace, None)
            .await
    }

    /// `POST /v1/a2a/:agent_id` with explicit fact-checker mode.
    pub async fn a2a_invoke_with_fact_check(
        &self,
        agent_id: &str,
        input: &str,
        trace: &[&str],
        fact_check_mode: Option<&str>,
    ) -> AgentsResult<A2aResponse> {
        if input.is_empty() {
            return Err(AgentsError::InvalidInput("input is required".into()));
        }
        let mut body = serde_json::json!({ "input": input });
        if let Some(f) = fact_check_mode {
            body["fact_check_mode"] = serde_json::Value::String(f.to_string());
        }
        let mut extra = HeaderMap::new();
        if !trace.is_empty() {
            if let Ok(val) = trace.join(",").parse() {
                extra.insert("x-a2a-trace", val);
            }
        }
        self.request::<A2aResponse>(
            Method::POST,
            &format!("/v1/a2a/{agent_id}"),
            Some(&body),
            Some(extra),
        )
        .await
        .map(|o| o.expect("server returned empty body for a2a_invoke"))
    }

    // ── Tasks (poll + stream) ───────────────────────────────────────

    /// `GET /v1/tasks/:id` — fetch the current state of a task.
    pub async fn get_task(&self, task_id: &str) -> AgentsResult<Task> {
        self.request::<Task>(
            Method::GET,
            &format!("/v1/tasks/{task_id}"),
            Option::<&()>::None,
            None,
        )
        .await
        .map(|o| o.expect("server returned empty body for get_task"))
    }

    /// `DELETE /v1/tasks/:id` — cancel a queued or running task.
    pub async fn cancel_task(&self, task_id: &str) -> AgentsResult<()> {
        let _: Option<serde_json::Value> = self
            .request(
                Method::DELETE,
                &format!("/v1/tasks/{task_id}"),
                Option::<&()>::None,
                None,
            )
            .await?;
        Ok(())
    }

    /// Poll [`Self::get_task`] until the task reaches a terminal status.
    ///
    /// Returns the final [`Task`] snapshot. Returns `AgentsError::InvalidInput`
    /// if the task is still running after `timeout`. Use [`Self::stream_task`]
    /// when you need per-state progress events instead of a single final
    /// snapshot.
    pub async fn wait_for_task(
        &self,
        task_id: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> AgentsResult<Task> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let task = self.get_task(task_id).await?;
            if task.is_done() {
                return Ok(task);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AgentsError::InvalidInput(format!(
                    "task {task_id} still in status {:?} after {:?}",
                    task.status, timeout
                )));
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Subscribe to `GET /v1/tasks/:id/stream` and return a `Stream` of
    /// typed [`StateTransition`] events. The stream closes when the
    /// upstream SSE stream closes (task reached terminal status) or on
    /// connection error.
    ///
    /// # Example
    /// ```no_run
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use futures_util::StreamExt;
    /// use wauldo::agents::AgentsClient;
    ///
    /// let agents = AgentsClient::new("https://api.wauldo.com").with_api_key("sk");
    /// let stream = agents.stream_task("task_xyz").await?;
    /// futures_util::pin_mut!(stream);
    /// while let Some(ev) = stream.next().await {
    ///     let ev = ev?;
    ///     println!("{}: {}ms", ev.state_name, ev.duration_ms);
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn stream_task(
        &self,
        task_id: &str,
    ) -> AgentsResult<impl futures_util::Stream<Item = AgentsResult<StateTransition>>> {
        let url = format!("{}/v1/tasks/{task_id}/stream", self.base_url);
        let mut headers = self.headers(None);
        headers.insert("Accept", "text/event-stream".parse().unwrap());
        let resp = self
            .client
            .request(Method::GET, &url)
            .headers(headers)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(AgentsError::Status {
                status: status.as_u16(),
                body,
            });
        }
        let stream = async_stream::try_stream! {
            let mut bytes = resp.bytes_stream();
            let mut buf: Vec<u8> = Vec::new();
            while let Some(chunk) = bytes.next().await {
                let chunk = chunk.map_err(AgentsError::Http)?;
                buf.extend_from_slice(&chunk);
                while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    let line_bytes = buf.drain(..=pos).collect::<Vec<u8>>();
                    let line = std::str::from_utf8(&line_bytes)
                        .unwrap_or("")
                        .trim_end_matches(['\n', '\r']);
                    if !line.starts_with("data:") {
                        continue;
                    }
                    let payload = line[5..].trim();
                    if payload.is_empty() {
                        continue;
                    }
                    match serde_json::from_str::<StateTransition>(payload) {
                        Ok(ev) => yield ev,
                        Err(_) => continue, // keep-alive or partial frame
                    }
                }
            }
        };
        Ok(stream)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn stub_agent_server() -> MockServer {
        MockServer::start().await
    }

    fn sample_agent_json() -> serde_json::Value {
        serde_json::json!({
            "id": "a1",
            "tenant_id": "t",
            "name": "bot",
            "description": "",
            "wauldo_toml": "[agent]\n[model]",
            "model_provider": "openrouter",
            "model_name": "qwen",
            "created_at": 0u64,
            "updated_at": 0u64,
        })
    }

    #[tokio::test]
    async fn test_create_sends_full_body_and_headers() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents"))
            .and(header("authorization", "Bearer k"))
            .and(header("x-rapidapi-user", "t"))
            .and(body_partial_json(serde_json::json!({
                "name": "bot",
                "preset": "general_task",
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(sample_agent_json()))
            .mount(&server)
            .await;

        let client = AgentsClient::new(server.uri())
            .with_api_key("k")
            .with_tenant("t");
        let out = client
            .create(CreateAgentRequest {
                name: "bot".into(),
                wauldo_toml: "[agent]\n[model]".into(),
                preset: Some("general_task".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(out.id, "a1");
    }

    #[tokio::test]
    async fn test_list_builds_query_string() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "agents": [],
                "pagination": { "total": 0, "limit": 10, "offset": 5 },
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let out = client.list(10, 5).await.unwrap();
        assert_eq!(out.pagination.limit, 10);
        assert_eq!(out.pagination.offset, 5);
    }

    #[tokio::test]
    async fn test_get_round_trip() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents/abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_agent_json()))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let out = client.get("abc").await.unwrap();
        assert_eq!(out.id, "a1");
    }

    #[tokio::test]
    async fn test_delete_returns_unit_on_204() {
        let server = stub_agent_server().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/agents/xyz"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        client.delete("xyz").await.unwrap();
    }

    #[tokio::test]
    async fn test_run_forwards_verification_mode() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .and(body_partial_json(serde_json::json!({
                "input": "Hello",
                "verification_mode": "strict",
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "tk1",
                "agent_id": "bot",
                "status": "queued",
                "created_at": 0u64,
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let out = client.run("bot", "Hello", Some("strict")).await.unwrap();
        assert_eq!(out.task_id, "tk1");
    }

    #[tokio::test]
    async fn test_run_rejects_empty_input() {
        let client = AgentsClient::new("http://localhost:1");
        let err = client.run("bot", "", None).await.unwrap_err();
        assert!(matches!(err, AgentsError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn test_run_forwards_fact_check_mode() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            .and(body_partial_json(serde_json::json!({
                "input": "Hello",
                "fact_check_mode": "hybrid",
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "tk1",
                "agent_id": "bot",
                "status": "queued",
                "created_at": 0u64,
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        client
            .run_with_fact_check("bot", "Hello", None, Some("hybrid"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_run_omits_fact_check_mode_when_none() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/agents/bot/runs"))
            // body_partial_json matches a subset — we assert the field is
            // absent by inspecting the actual request in the then-branch.
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "tk1",
                "agent_id": "bot",
                "status": "queued",
                "created_at": 0u64,
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        client.run("bot", "Hello", None).await.unwrap();
        let received = &server.received_requests().await.unwrap()[0];
        let body: serde_json::Value = serde_json::from_slice(&received.body).unwrap();
        assert!(
            body.get("fact_check_mode").is_none(),
            "fact_check_mode should be omitted when None, got: {}",
            body
        );
    }

    #[tokio::test]
    async fn test_a2a_invoke_sends_trace_header() {
        let server = stub_agent_server().await;
        Mock::given(method("POST"))
            .and(path("/v1/a2a/target"))
            .and(header("x-a2a-trace", "caller"))
            .and(body_partial_json(serde_json::json!({"input": "do"})))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "task_id": "tk",
                "agent_id": "target",
                "trace": ["caller", "target"],
                "depth": 2u64,
                "status": "queued",
            })))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let out = client
            .a2a_invoke("target", "do", &["caller"])
            .await
            .unwrap();
        assert_eq!(out.depth, 2);
        assert_eq!(out.trace, vec!["caller", "target"]);
    }

    #[tokio::test]
    async fn test_a2a_rejects_empty_input() {
        let client = AgentsClient::new("http://localhost:1");
        let err = client.a2a_invoke("target", "", &[]).await.unwrap_err();
        assert!(matches!(err, AgentsError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn test_non_2xx_becomes_status_error() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/agents/missing"))
            .respond_with(ResponseTemplate::new(404).set_body_string("{\"error\":\"not found\"}"))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let err = client.get("missing").await.unwrap_err();
        match err {
            AgentsError::Status { status, body } => {
                assert_eq!(status, 404);
                assert!(body.contains("not found"));
            }
            other => panic!("expected Status, got {other:?}"),
        }
    }

    // ── Tasks + SSE tests ────────────────────────────────────────

    fn sample_task_json(status: &str) -> serde_json::Value {
        serde_json::json!({
            "task_id": "t1",
            "tenant_id": "tn",
            "status": status,
            "prompt": "hi",
            "created_at": 1u64,
            "updated_at": 2u64,
            "result": "hello",
            "verification": {
                "verdict": "UNVERIFIED",
                "hallucination_rate": 0.0,
                "confidence": 1.0,
                "trust_score": 0.0,
                "verification_source": "prompt_only",
                "claims": [],
                "verification_retries": 0u32,
                "message": "No source documents uploaded.",
            }
        })
    }

    #[tokio::test]
    async fn test_task_status_is_terminal() {
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Cancelled.is_terminal());
        assert!(!TaskStatus::Queued.is_terminal());
        assert!(!TaskStatus::Running.is_terminal());
    }

    #[tokio::test]
    async fn test_get_task_parses_typed_task() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_task_json("completed")))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri()).with_api_key("k");
        let task = client.get_task("t1").await.unwrap();
        assert_eq!(task.task_id, "t1");
        assert_eq!(task.status, TaskStatus::Completed);
        assert!(task.is_done());
        let verif = task.verification.as_ref().expect("verif present");
        assert_eq!(verif.verdict, Verdict::Unverified);
        assert_eq!(verif.trust_score, 0.0);
        assert_eq!(
            verif.message.as_deref(),
            Some("No source documents uploaded.")
        );
    }

    #[tokio::test]
    async fn test_cancel_task_sends_delete() {
        let server = stub_agent_server().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        client.cancel_task("t1").await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_task_returns_on_terminal() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_task_json("completed")))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        let task = client
            .wait_for_task("t1", Duration::from_secs(5), Duration::from_millis(10))
            .await
            .unwrap();
        assert_eq!(task.status, TaskStatus::Completed);
    }

    #[tokio::test]
    async fn test_stream_task_parses_sse_frames() {
        use futures_util::StreamExt;
        let events = [
            serde_json::json!({
                "state_name": "Analysis",
                "to_state": "Tradeoffs",
                "condition": "Sequential execution",
                "raw_output": "",
                "validation_notes": [],
                "timestamp": 1u64,
                "success": true,
                "retry_count": 0u32,
                "duration_ms": 1000u64,
                "prompt_tokens": 10usize,
                "completion_tokens": 200usize,
                "repair_count": 0u32,
                "cache_hit": false,
            }),
            serde_json::json!({
                "state_name": "Tradeoffs",
                "to_state": null,
                "condition": "Sequential execution",
                "raw_output": "",
                "validation_notes": [],
                "timestamp": 2u64,
                "success": true,
                "retry_count": 0u32,
                "duration_ms": 2000u64,
                "prompt_tokens": 10usize,
                "completion_tokens": 300usize,
                "repair_count": 0u32,
                "cache_hit": false,
            }),
        ];
        let body = events
            .iter()
            .map(|e| format!("data: {}\n\n", e))
            .collect::<String>()
            + ": keep-alive\n\n";

        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1/stream"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(body),
            )
            .mount(&server)
            .await;

        let client = AgentsClient::new(server.uri());
        let stream = client.stream_task("t1").await.unwrap();
        futures_util::pin_mut!(stream);
        let mut got = Vec::new();
        while let Some(ev) = stream.next().await {
            got.push(ev.unwrap());
        }
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].state_name, "Analysis");
        assert_eq!(got[1].state_name, "Tradeoffs");
        assert_eq!(got[0].duration_ms, 1000);
    }

    #[tokio::test]
    async fn test_stream_task_surfaces_http_errors() {
        let server = stub_agent_server().await;
        Mock::given(method("GET"))
            .and(path("/v1/tasks/t1/stream"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;
        let client = AgentsClient::new(server.uri());
        match client.stream_task("t1").await {
            Err(AgentsError::Status { status, .. }) => assert_eq!(status, 500),
            Ok(_) => panic!("expected Err, got Ok(stream)"),
            Err(other) => panic!("expected Status, got {other:?}"),
        }
    }
}
