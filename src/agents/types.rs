//! Wire types, error types, and shared helpers for the agents API surface.

use futures_util::StreamExt;
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

// ─── Revisions (ECS-style versioning) ────────────────────────────────

/// Immutable snapshot of an agent's `AgentContractV2` payload.
///
/// Each revision is content-addressed via `sha256` and identified by a
/// monotone `rev` integer. Mirrors AWS ECS task-definition revisions:
/// append-only, with O(1) rollback via [`super::AgentsClient::set_active_revision`].
#[derive(Debug, Clone, Deserialize)]
pub struct AgentRevision {
    pub rev: u32,
    pub sha256: String,
    pub contract_json: String,
    pub created_at: u64,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateRevisionRequest {
    /// Full `AgentContractV2` JSON payload.
    pub custom_preset: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// When `true` (default) the new revision becomes active immediately.
    pub set_active: bool,
}

impl Default for CreateRevisionRequest {
    fn default() -> Self {
        Self {
            custom_preset: serde_json::Value::Null,
            message: None,
            set_active: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateRevisionResponse {
    pub rev: u32,
    pub sha256: String,
    pub active_rev: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListRevisionsResponse {
    pub revisions: Vec<AgentRevision>,
    pub active_rev: u32,
    pub head_rev: u32,
    pub count: usize,
}

/// Result of [`super::AgentsClient::share_task`] / [`super::AgentsClient::unshare_task`].
///
/// `expires_at` is epoch milliseconds, or `None` for paid tenants
/// (no expiration). Free-tier shares default to a 30-day TTL ; once
/// elapsed, the public `GET /v1/runs/<share_id>` returns 404.
#[derive(Debug, Clone, Deserialize)]
pub struct ShareResponse {
    pub share_id: String,
    pub url: String,
    #[serde(default)]
    pub expires_at: Option<u64>,
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
