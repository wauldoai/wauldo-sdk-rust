//! Wire types, error types, and shared helpers for the agents API surface.

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::verification::{RaceResult, TaskVerification};

/// Max bytes the client will accept from a single response. Protects
/// against hostile or misbehaving servers that try to stream gigabytes.
pub const MAX_RESPONSE_SIZE: usize = 10 * 1024 * 1024; // 10 MB

/// Errors returned by `AgentsClient` / `MemoryClient` / `WorkflowsClient`.
///
/// `#[non_exhaustive]`: a `match` needs a wildcard arm, so a variant added
/// later is not a breaking change.
///
/// Converts into the crate's [`crate::Error`] (`?` works in a function that
/// returns `wauldo::Result`): `Status` becomes `Error::Server` with the HTTP
/// status as `code` and the response body as `message`. A failed task is
/// not an error: read [`Task::error_info`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AgentsError {
    #[error("HTTP request failed: {0}{}", timed_out_note(.0))]
    Http(#[from] reqwest::Error),

    #[error("HTTP {status}: {body}")]
    Status { status: u16, body: String },

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("response body too large: >{0} bytes")]
    BodyTooLarge(usize),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// A wait ran out before the task settled (`wait_for_task`,
    /// `run_and_wait`, the fallback of `stream_task`). The task itself is
    /// not affected: it may still be running on the server. Call
    /// `wait_for_task(&task_id, ..)` again to keep waiting, or
    /// `get_task(&task_id)` to read it.
    #[error(
        "task {task_id} still in status {last_status:?} after {waited:?}; the task may still be \
         running on the server: call wait_for_task(\"{task_id}\", ..) to keep waiting, or \
         get_task(\"{task_id}\") to read it"
    )]
    Timeout {
        task_id: String,
        /// The task's status at the last read.
        last_status: TaskStatus,
        /// The wait that ran out.
        waited: std::time::Duration,
    },
}

pub type AgentsResult<T> = std::result::Result<T, AgentsError>;

/// reqwest's message for a stalled stream is "error decoding response
/// body": say that it timed out.
fn timed_out_note(e: &reqwest::Error) -> &'static str {
    if e.is_timeout() {
        " (timed out)"
    } else {
        ""
    }
}

// ─── Wire types ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployedAgent {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub wauldo_toml: String,
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
    pub preset: Option<String>,
    /// Inline workflow (the `AgentContractV2` JSON that
    /// [`CreateRevisionRequest::custom_preset`] takes), validated by the
    /// server at creation: the agent's first run already uses it, with no
    /// `create_revision` call in between. Takes precedence over `preset`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_preset: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct UpdateAgentRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wauldo_toml: Option<String>,
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
    /// The agent that ran. Empty for a task created by
    /// [`crate::AgentsClient::resume_task`] (that response names no agent).
    #[serde(default)]
    pub agent_id: String,
    pub status: String,
    pub created_at: u64,
    /// Filled only when the run was requested with a wait
    /// ([`RunOptions::wait`]): the task as it stood when the server
    /// answered — settled, with its result and verification, if the run
    /// finished within the wait.
    #[serde(default)]
    pub task: Option<Task>,
    /// Set when the run was requested with [`RunOptions::auto_share`] and a
    /// link was registered.
    #[serde(default)]
    pub share_id: Option<String>,
    /// The public URL of the run, with `share_id`.
    #[serde(default)]
    pub share_url: Option<String>,
}

/// A sensible `timeout` for [`crate::AgentsClient::wait_for_task`] and
/// [`crate::AgentsClient::run_and_wait`]. It covers ordinary runs, queueing
/// and verification included; it is not a bound on how long the server may
/// take. For a long run (many workflow states, tools, retries), pass a larger
/// timeout, follow it with [`crate::AgentsClient::stream_task`], or give the
/// run a `webhook_url`. When the wait runs out the task keeps running:
/// [`AgentsError::Timeout`] carries its id, to wait again. Only the wait is
/// long: each request inside it stays a [`LONG_POLL_SECS`] long poll.
pub const DEFAULT_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(360);

/// Longest the server holds a request open for a wait, in seconds. Larger
/// values are clamped server-side, not refused. Well under the client's own
/// 120 s request timeout, so a held request never aborts on our side first.
pub const MAX_WAIT_SECS: u64 = 50;

/// Length of each long-poll request issued by `wait_for_task`. Kept under
/// [`MAX_WAIT_SECS`] and well under the client timeout so a slow run still
/// reports progress every half minute.
pub const LONG_POLL_SECS: u64 = 30;

/// Options for [`crate::AgentsClient::run_with_options`].
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    /// `"strict"`, `"balanced"` or `"permissive"` (the server default).
    pub verification_mode: Option<String>,
    /// `"lexical"` (the server default), `"hybrid"` or `"semantic"`.
    pub fact_check_mode: Option<String>,
    /// Hold the response until the run settles or this delay elapses
    /// (clamped to [`MAX_WAIT_SECS`]). The task then comes back in
    /// [`AgentRunResponse::task`].
    pub wait: Option<std::time::Duration>,
    /// The server POSTs the task record here when the run completes or
    /// fails. A public http(s) URL: a private or internal destination is
    /// refused with a 400.
    pub webhook_url: Option<String>,
    /// Retries of the run on the server (default 3, capped at 10) — not the
    /// HTTP retries of this client ([`crate::AgentsClient::with_max_retries`]).
    pub max_retries: Option<u32>,
    /// Publish the run at creation: [`AgentRunResponse::share_url`] is then
    /// set.
    pub auto_share: Option<bool>,
}

/// `?wait=N` for a positive wait, the empty string otherwise. Rounded up: the
/// server takes whole seconds, and asking for one more is better than
/// returning before the caller's own deadline.
pub(crate) fn wait_query(wait: Option<std::time::Duration>) -> String {
    match wait {
        Some(d) if !d.is_zero() => {
            let secs = d.as_secs_f64().ceil() as u64;
            format!("?wait={}", secs.clamp(1, MAX_WAIT_SECS))
        }
        _ => String::new(),
    }
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
/// `expires_at` is epoch milliseconds: a share expires 30 days after it is
/// published (`None` would mean no expiry; no plan has that today). Once
/// elapsed, the public `GET /v1/runs/<share_id>` returns 404. The shared run
/// stays available for as long as its link is valid: until `expires_at`,
/// `unshare_task`, or the task's deletion.
#[derive(Debug, Clone, Deserialize)]
pub struct ShareResponse {
    pub share_id: String,
    pub url: String,
    #[serde(default)]
    pub expires_at: Option<u64>,
}

// ─── Tasks + verification types ──────────────────────────────────────

/// Task lifecycle status.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Running,
    /// Waiting for a human approve/reject decision — nothing moves until
    /// the caller acts.
    PausedForApproval,
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

    /// True when nothing will change until the caller acts: the run is
    /// over, or it is paused for approval. This is what the server's wait
    /// waits for, and what `wait_for_task` returns on.
    pub fn is_settled(self) -> bool {
        self.is_terminal() || self == TaskStatus::PausedForApproval
    }
}

/// Full task record returned by `GET /v1/tasks/:id`.
#[derive(Debug, Clone, Deserialize)]
pub struct Task {
    pub task_id: String,
    /// Not sent by `GET /v1/tasks/:id`: empty unless the server adds it.
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
    /// True when the run's stream withheld its output (injection detected):
    /// the stream's terminal event carries [`WITHHELD_RESULT`] in place of
    /// `result`. Absent on the wire (false) otherwise.
    #[serde(default)]
    pub output_withheld: bool,
    /// Human decisions taken on the task (pause, approve, reject), oldest
    /// first, with the reviewer's note when one was sent. Only the 20 most
    /// recent are kept: past that, the oldest is dropped. Empty when none.
    #[serde(default)]
    pub decisions: Vec<super::hitl::TaskDecision>,
    /// Id of the public share of the run, when it was published
    /// (`auto_share` or `share_task`).
    #[serde(default)]
    pub share_id: Option<String>,
    /// Public URL of the run, when it was published.
    #[serde(default)]
    pub share_url: Option<String>,
    /// Outcome of a fact-check race, for runs that raced several models.
    #[serde(default)]
    pub race_result: Option<RaceResult>,
}

/// What the stream says in place of `result` when it withheld the run's
/// output ([`Task::output_withheld`]).
pub const WITHHELD_RESULT: &str = "Output withheld: injection detected";

impl Task {
    pub fn is_done(&self) -> bool {
        self.status.is_terminal()
    }

    /// Done, or paused for approval: see [`TaskStatus::is_settled`].
    pub fn is_settled(&self) -> bool {
        self.status.is_settled()
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

/// State names of the stream's last event, one per terminal status (exact
/// names, not a prefix: a workflow state may be called anything).
pub const TERMINAL_STATE_NAMES: [&str; 3] = ["TASK_COMPLETED", "TASK_FAILED", "TASK_CANCELLED"];

/// `condition` of the stream's last event: `"terminal"` when the run ends on
/// the stream, `"Already finished"` when it was over at subscription.
pub const TERMINAL_CONDITIONS: [&str; 2] = ["terminal", "Already finished"];

/// `state_name` of the notice the server sends when the stream fell behind
/// the run. Not a workflow state and not terminal: the run goes on and the
/// terminal event still comes. `validation_notes` says how many transitions
/// were skipped; the ones after it are withheld (their `condition` says so) up
/// to the terminal event, and the task record (`get_task`) holds the full run.
pub const STREAM_LAGGED: &str = "STREAM_LAGGED";

/// What the stream's terminal event carries: the same fields as
/// `GET /v1/tasks/:id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalPayload {
    pub task_id: String,
    pub status: TaskStatus,
    #[serde(default)]
    pub result: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub verification: Option<TaskVerification>,
}

impl StateTransition {
    /// True for the stream's last event (`TASK_COMPLETED`, `TASK_FAILED`,
    /// `TASK_CANCELLED`): the run is over and [`Self::terminal_payload`]
    /// holds its result, so there is nothing left to fetch. Exact names and a
    /// terminal `condition`: a workflow state may be called anything,
    /// `TASK_COMPLETED` included.
    pub fn is_terminal(&self) -> bool {
        self.to_state.is_none()
            && TERMINAL_STATE_NAMES.contains(&self.state_name.as_str())
            && TERMINAL_CONDITIONS.contains(&self.condition.as_str())
    }

    /// The terminal event's content; `None` on an ordinary transition, or
    /// when the server withheld it.
    pub fn terminal_payload(&self) -> Option<TerminalPayload> {
        if !self.is_terminal() || self.raw_output.is_empty() {
            return None;
        }
        serde_json::from_str(&self.raw_output).ok()
    }
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
